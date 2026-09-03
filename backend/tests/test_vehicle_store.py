"""Vehicle profile store (SPEC 15.1): upsert keys, atomic writes, load errors."""

from __future__ import annotations

import json
import os
from pathlib import Path

import pytest
from pydantic import ValidationError

from diagcore.loghub import LogFilter, LogHub
from diagcore.vehicles import (
    DEFAULT_DATA_DIR,
    VehicleProfile,
    VehicleProfileInput,
    VehicleStore,
    canonical_identifiers,
    profile_key,
    resolve_data_dir,
)

ECUS = [
    {"id": "engine", "name": "Engine", "address": "0x10", "protocol": "UDS", "props": {"sw": "1"}},
    {"id": "body", "name": "Body", "address": "0x26", "protocol": "KWP"},
]


def incoming(**kw) -> VehicleProfileInput:
    data = {"name": "Car", "vin": "VIN0000000000001", "identifiers": {"serial": "abc"}, "ecus": ECUS}
    data.update(kw)
    return VehicleProfileInput.model_validate(data)


@pytest.fixture
def logs() -> LogHub:
    return LogHub()


@pytest.fixture
def store(tmp_path: Path, logs: LogHub) -> VehicleStore:
    return VehicleStore(tmp_path / "data", loghub=logs)


def vehicle_events(logs: LogHub) -> list[dict]:
    return logs.history(LogFilter(categories=["vehicle"]))


# -- data dir resolution ------------------------------------------------------


def test_resolve_data_dir_precedence(tmp_path: Path) -> None:
    assert resolve_data_dir(tmp_path / "x", {"DIAGCORE_DATA_DIR": "/elsewhere"}) == tmp_path / "x"
    assert resolve_data_dir(None, {"DIAGCORE_DATA_DIR": str(tmp_path / "env")}) == tmp_path / "env"
    assert resolve_data_dir(None, {"DIAGCORE_DATA_DIR": "  "}) == DEFAULT_DATA_DIR
    assert resolve_data_dir(None, {}) == DEFAULT_DATA_DIR
    assert DEFAULT_DATA_DIR.name == "data" and DEFAULT_DATA_DIR.parent.name == "diAgnostic-framework"


# -- keys ----------------------------------------------------------------------


def test_profile_key_prefers_vin_else_canonical_identifiers() -> None:
    assert profile_key("p", "VIN1", {"a": 1}) == "p|vin:VIN1"
    assert profile_key("p", "  VIN1 ", {}) == "p|vin:VIN1"  # normalized
    assert profile_key("p", None, {"b": 2, "a": [1, {"z": True}]}) == 'p|ids:{"a":[1,{"z":true}],"b":2}'
    assert profile_key("p", "", {"b": 2, "a": 1}) == profile_key("p", None, {"a": 1, "b": 2})  # key order irrelevant
    assert canonical_identifiers({}) == "{}"


# -- upsert ---------------------------------------------------------------------


def test_upsert_creates_file_and_updates_by_vin(store: VehicleStore, logs: LogHub) -> None:
    profile, created = store.upsert("demo", incoming(), now=1000.0)
    assert created
    assert profile.id.startswith("veh_") and len(profile.id) == 16
    assert profile.pluginId == "demo" and profile.firstSeen == profile.lastSeen == 1000.0
    path = store.path_for(profile.id)
    assert path == store.dir / f"{profile.id}.json" and path.is_file()
    on_disk = json.loads(path.read_text(encoding="utf-8"))
    assert on_disk == profile.model_dump(mode="json")
    assert list(on_disk) == sorted(on_disk)  # stable, sorted keys
    assert [p.name for p in store.list()] == ["Car"] and len(store) == 1
    assert not [p for p in store.dir.iterdir() if p.suffix == ".tmp"]  # no temp files left behind

    # same plugin + VIN => update: id kept, lastSeen bumped, ecus replaced,
    # identifiers merged (new keys win), name/notes (user-owned) kept
    store.rename(profile.id, "My car")
    updated, created = store.upsert(
        "demo",
        incoming(name="Car (rescanned)", identifiers={"serial": "abc", "color": "red"}, ecus=ECUS[:1]),
        now=2000.0,
    )
    assert not created and updated.id == profile.id
    assert updated.name == "My car"
    assert updated.firstSeen == 1000.0 and updated.lastSeen == 2000.0
    assert updated.identifiers == {"serial": "abc", "color": "red"}
    assert [e.id for e in updated.ecus] == ["engine"]
    assert len(store) == 1
    assert json.loads(path.read_text(encoding="utf-8"))["lastSeen"] == 2000.0

    # a different VIN (same plugin) or the same VIN from another plugin => new profiles
    other, created = store.upsert("demo", incoming(vin="VIN0000000000002"), now=3000.0)
    assert created and other.id != profile.id
    foreign, created = store.upsert("psa", incoming(), now=4000.0)
    assert created and foreign.id not in (profile.id, other.id)
    assert [p.id for p in store.list()] == [foreign.id, other.id, profile.id]  # lastSeen desc

    # every change is logged (category vehicle) with the full profile in data
    messages = [e["message"] for e in vehicle_events(logs)]
    assert any(m.startswith(f"vehicle profile {profile.id} created:") and "VIN0000000000001" in m for m in messages)
    assert any(m.startswith(f"vehicle profile {profile.id} renamed: 'Car' -> 'My car'") for m in messages)
    assert any(m.startswith(f"vehicle profile {profile.id} updated:") for m in messages)
    created_event = next(e for e in vehicle_events(logs) if e["message"].startswith(f"vehicle profile {profile.id} created"))
    assert created_event["data"]["profile"]["ecus"][0]["id"] == "engine"
    assert created_event["data"]["created"] is True and created_event["data"]["written"] is True
    assert created_event["nodeId"] == "vehicles"


def test_upsert_without_vin_keys_on_canonical_identifiers(store: VehicleStore) -> None:
    first, created = store.upsert("demo", incoming(vin=None, identifiers={"b": 2, "a": 1}), now=1.0)
    assert created and first.vin is None
    again, created = store.upsert("demo", incoming(vin="", identifiers={"a": 1, "b": 2}), now=2.0)
    assert not created and again.id == first.id and again.lastSeen == 2.0
    other, created = store.upsert("demo", incoming(vin=None, identifiers={"a": 1}), now=3.0)
    assert created and other.id != first.id
    assert len(store) == 2


def test_vin_learned_later_updates_the_vinless_profile(store: VehicleStore, logs: LogHub) -> None:
    """The M1.5 resolution recorded in SPEC 15.1: a same-plugin VIN-less
    profile with identical identifiers absorbs an incoming profile that now
    carries a VIN — and the audit event says so."""
    vinless, _ = store.upsert("demo", incoming(vin=None, identifiers={"serial": "abc"}), now=1.0)
    assert vehicle_events(logs)[-1]["data"]["vinLearned"] is False
    learned, created = store.upsert("demo", incoming(vin="VIN0000000000009", identifiers={"serial": "abc"}), now=2.0)
    assert not created and learned.id == vinless.id and learned.vin == "VIN0000000000009"
    event = vehicle_events(logs)[-1]
    assert event["message"].startswith(f"vehicle profile {vinless.id} updated (VIN learned: the profile was stored without one)")
    assert event["data"]["vinLearned"] is True and event["data"]["previousVin"] is None
    assert event["data"]["profile"]["vin"] == "VIN0000000000009"
    # a plain update of a VIN'd profile is not marked
    store.upsert("demo", incoming(vin="VIN0000000000009", identifiers={"serial": "abc", "more": 1}), now=3.0)
    event = vehicle_events(logs)[-1]
    assert event["data"]["vinLearned"] is False and event["data"]["previousVin"] == "VIN0000000000009"
    # from now on the profile is keyed by its VIN
    assert store.find("demo", "VIN0000000000009", {}) is not None
    assert store.find("demo", None, {"serial": "abc"}) is None
    assert len(store) == 1
    # a VIN'd profile never absorbs a VIN-less incoming with empty identifiers
    fresh, created = store.upsert("demo", incoming(vin=None, identifiers={}), now=3.0)
    assert created and fresh.id != vinless.id


def test_profile_input_validation() -> None:
    with pytest.raises(ValidationError):
        VehicleProfileInput.model_validate({"vin": "x"})  # name required
    with pytest.raises(ValidationError):
        VehicleProfileInput.model_validate({"name": "n", "ecus": "nope"})
    with pytest.raises(ValidationError, match="duplicate ecu id"):
        VehicleProfileInput.model_validate({"name": "n", "ecus": [ECUS[0], ECUS[0]]})
    # the core-owned keys (SPEC 15.1 id/firstSeen/lastSeen) are ignored, not a
    # reason to drop the profile: a plugin may re-emit the full profile it
    # received in vehicle.attach (SPEC 15.4 "may be sent again")
    resent = VehicleProfileInput.model_validate({"name": "n", "id": "veh_x", "firstSeen": 1.0, "lastSeen": 2.0})
    assert set(resent.model_dump()) == {"name", "vin", "identifiers", "pluginId", "ecus", "notes"}
    with pytest.raises(ValidationError):
        VehicleProfileInput.model_validate({"name": "n", "color": "red"})  # unknown keys still rejected
    assert VehicleProfileInput.model_validate({"name": "n", "vin": "  "}).vin is None
    with pytest.raises(ValidationError):
        VehicleProfile.model_validate({"id": "nope", "name": "n", "pluginId": "p", "firstSeen": 1, "lastSeen": 1})


# -- atomic writes ---------------------------------------------------------------


def test_write_failure_keeps_old_file_and_is_reported(store: VehicleStore, logs: LogHub, monkeypatch) -> None:
    profile, _ = store.upsert("demo", incoming(), now=1.0)
    path = store.path_for(profile.id)
    before = path.read_text(encoding="utf-8")

    def broken_replace(src, dst):
        raise OSError(28, "No space left on device")

    monkeypatch.setattr(os, "replace", broken_replace)
    updated, created = store.upsert("demo", incoming(name="ignored", identifiers={"serial": "abc", "x": 1}), now=2.0)
    assert not created and updated.id == profile.id
    assert store.last_write_error is not None and "No space left" in store.last_write_error
    assert path.read_text(encoding="utf-8") == before  # the old file is intact (atomic)
    assert not [p for p in store.dir.iterdir() if p.suffix == ".tmp"]  # temp file cleaned up
    assert store.get(profile.id).lastSeen == 2.0  # in memory the update took effect
    errors = logs.history(LogFilter(categories=["vehicle"], levels=["error"]))
    assert len(errors) == 1 and "write failed" in errors[0]["message"] and profile.id in errors[0]["message"]
    updated_event = vehicle_events(logs)[-1]
    assert updated_event["message"].endswith("(WRITE FAILED)") and updated_event["data"]["written"] is False

    monkeypatch.undo()
    store.touch(profile.id, now=3.0)
    assert store.last_write_error is None  # the next successful write clears it
    assert json.loads(path.read_text(encoding="utf-8"))["lastSeen"] == 3.0


# -- loading ----------------------------------------------------------------------


def test_load_collects_errors_and_keeps_good_profiles(tmp_path: Path, logs: LogHub) -> None:
    first = VehicleStore(tmp_path / "data", loghub=logs)
    good, _ = first.upsert("demo", incoming(), now=1.0)
    vehicles_dir = first.dir
    (vehicles_dir / "veh_broken.json").write_text("{ not json", encoding="utf-8")
    mismatch = VehicleProfile(id="veh_aaaa", name="n", pluginId="p", firstSeen=1, lastSeen=1)
    (vehicles_dir / "veh_bbbb.json").write_text(json.dumps(mismatch.model_dump(mode="json")), encoding="utf-8")
    (vehicles_dir / "veh_shape.json").write_text(json.dumps({"id": "veh_shape", "name": "x"}), encoding="utf-8")

    second = VehicleStore(tmp_path / "data", loghub=logs)
    second.load()
    assert [p.id for p in second.list()] == [good.id]
    assert len(second.load_errors) == 3
    assert any("veh_broken.json" in e and "JSONDecodeError" in e for e in second.load_errors)
    assert any("veh_bbbb.json" in e and "does not match the file name" in e for e in second.load_errors)
    assert any("veh_shape.json" in e and "ValidationError" in e for e in second.load_errors)
    load_errors = [e for e in logs.history(LogFilter(categories=["vehicle"], levels=["error"])) if "unusable" in e["message"]]
    assert len(load_errors) == 3 and all(e["nodeId"] == "vehicles" for e in load_errors)
    loaded = vehicle_events(logs)[-1]
    assert loaded["message"].startswith("vehicle store loaded 1 profile(s)") and "(3 unusable file(s))" in loaded["message"]
    assert loaded["data"]["errors"] == second.load_errors

    # load() of a missing directory is not an error
    empty = VehicleStore(tmp_path / "nothing")
    empty.load()
    assert empty.load_errors == [] and len(empty) == 0 and not empty.dir.exists()


def test_forget_deletes_file(store: VehicleStore, logs: LogHub) -> None:
    profile, _ = store.upsert("demo", incoming(), now=1.0)
    path = store.path_for(profile.id)
    assert path.exists()
    gone = store.forget(profile.id)
    assert gone.id == profile.id and not path.exists() and len(store) == 0
    with pytest.raises(KeyError):
        store.forget(profile.id)
    assert any(m.startswith(f"vehicle profile {profile.id} forgotten:") for m in (e["message"] for e in vehicle_events(logs)))


def test_summary_props_omit_full_ecus(store: VehicleStore) -> None:
    profile, _ = store.upsert("demo", incoming(), now=1.0)
    props = profile.summary_props()
    assert props["ecuCount"] == 2
    assert props["ecus"] == [
        {"id": "engine", "name": "Engine", "address": "0x10"},
        {"id": "body", "name": "Body", "address": "0x26"},
    ]
    assert set(props) == {"id", "name", "vin", "identifiers", "pluginId", "firstSeen", "lastSeen", "ecus", "notes", "ecuCount"}
