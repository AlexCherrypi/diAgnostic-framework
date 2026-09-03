"""Vehicle session manager + mount grants against a scripted fake vehicle
plugin (SPEC 15.2-15.4), through the real composition root and command bus."""

from __future__ import annotations

import asyncio
import contextlib
import json
import shutil
from pathlib import Path
from typing import Any, AsyncIterator

import pytest

from diagcore.app import AppCore
from diagcore.loghub import LogFilter
from diagcore.model import Node
from diagcore.vehicles import VehicleProfile

from util import wait_until

FIXTURES = Path(__file__).parent / "fixtures"
FAKE_DIR = FIXTURES / "fake_vehicle"

PLUGIN_ID = "fake_vehicle"
HOST_ID = f"plugin:{PLUGIN_ID}"
CTL_ID = f"{HOST_ID}:ctl"
SESSION = "vehicle"
LIBRARY = "vehicles"

FAKE_DETECT_PARAMS = [
    {
        "name": "depth",
        "label": "Depth",
        "type": "select",
        "required": True,
        "default": "quick",
        "placeholder": None,
        "help": None,
        "min": None,
        "max": None,
        "options": [{"value": "quick", "label": "Quick"}, {"value": "full", "label": "Full"}],
        "when": None,
    },
    {
        "name": "retries",
        "label": "Retries",
        "type": "number",
        "required": False,
        "default": 1,
        "placeholder": None,
        "help": None,
        "min": 0.0,
        "max": 5.0,
        "options": [],
        "when": None,
    },
]


@contextlib.asynccontextmanager
async def running_core(
    tmp_path: Path,
    *,
    env: dict[str, str] | None = None,
    plugins_dir: Path = FIXTURES,
    data_dir: Path | None = None,
) -> AsyncIterator[AppCore]:
    core = AppCore(
        plugins_dir=plugins_dir,
        log_dir=tmp_path / "logs",
        data_dir=data_dir or tmp_path / "data",
        plugin_env=env,
    )
    core.vehicles.profile_grace = 0.1
    await core.start()
    try:
        await wait_plugin(core)
        yield core
    finally:
        await core.stop()


async def wait_plugin(core: AppCore, host_id: str = HOST_ID) -> None:
    await wait_until(
        lambda: core.hosts[host_id].healthy and core.tree.get(f"{host_id}:ctl") is not None,
        timeout=15.0,
        message="fake vehicle plugin healthy",
    )
    await wait_until(
        lambda: [a.name for a in core.tree.get(SESSION).actions] == ["detect"]
        and core.tree.get(SESSION).actions[0].params,
        message="detect action recomputed with the plugin's detectParams",
    )


def session(core: AppCore) -> Node:
    node = core.tree.get(SESSION)
    assert node is not None
    return node


def state(core: AppCore) -> str:
    return str(session(core).props["state"])


def action_names(node: Node) -> list[str]:
    return [a.name for a in node.actions]


async def ctl(core: AppCore, action: str, params: dict[str, Any] | None = None) -> Any:
    result = await core.bus.dispatch(CTL_ID, action, params or {})
    assert result.ok, result.error
    return result.data


async def detect(core: AppCore, params: dict[str, Any] | None = None) -> str:
    result = await core.bus.dispatch(SESSION, "detect", params or {})
    assert result.ok, result.error
    assert state(core) == "detecting"
    return str(result.data["jobNodeId"])


async def connect(core: AppCore, profile_overrides: dict[str, Any] | None = None) -> str:
    """detect + finish => connected; returns the profile id."""
    await detect(core)
    await ctl(core, "finish", profile_overrides)
    await wait_until(lambda: state(core) == "connected", message="session connected")
    return str(session(core).props["profileId"])


def vehicle_log(core: AppCore, **kw) -> list[str]:
    return [e["message"] for e in core.logs.history(LogFilter(categories=["vehicle"], **kw))]


def dropped(core: AppCore) -> list[dict]:
    return core.logs.history(LogFilter(categories=["plugin"], levels=["warn"], search="dropped"))


# -- well-known nodes ------------------------------------------------------------


async def test_well_known_vehicle_nodes_shape(tmp_path: Path) -> None:
    async with running_core(tmp_path) as core:
        node = session(core)
        assert node.type == "vehicle.session" and node.parentId == "root" and node.owner == "core"
        assert node.status == "idle" and node.error is None and node.name == "Vehicle"
        assert node.props == {
            "state": "none",
            "profileId": None,
            "name": None,
            "vin": None,
            "pluginId": None,
            "ecuCount": 0,
            "since": None,
        }
        # exactly one capable plugin => no `plugin` select, just its detectParams
        assert [a.model_dump(mode="json") for a in node.actions] == [
            {"name": "detect", "label": "Detect vehicle", "confirm": None, "params": FAKE_DETECT_PARAMS}
        ]
        library = core.tree.get(LIBRARY)
        assert library.type == "vehicle.library" and library.status == "idle" and library.children == []
        assert library.actions == [] and library.props == {"dir": str(core.data_dir / "vehicles"), "profileCount": 0}
        assert str(core.data_dir) == core.tree.get("system").props["dataDir"]


# -- detect => connected => disconnect ------------------------------------------------


async def test_detect_connect_profile_and_disconnect(tmp_path: Path) -> None:
    async with running_core(tmp_path) as core:
        host = core.hosts[HOST_ID]
        result = await core.bus.dispatch(SESSION, "detect", {"depth": "full"})
        assert result.ok, result.error
        job_id = result.data["jobNodeId"]
        assert result.data["pluginId"] == PLUGIN_ID and result.data["state"] == "detecting"

        node = session(core)
        assert node.status == "running" and node.props["state"] == "detecting"
        assert node.props["pluginId"] == PLUGIN_ID and node.props["profileId"] is None
        assert action_names(node) == ["disconnect"]
        job = core.tree.get(job_id)
        assert job.parentId == SESSION and job.owner == HOST_ID and job.type == "job" and job.status == "running"
        assert job.props["detectParams"] == {"depth": "full", "retries": 1}  # defaults filled in by the core
        assert host.has_grant(SESSION) and host.grants == frozenset({SESSION})

        await ctl(core, "finish")
        await wait_until(lambda: state(core) == "connected", message="connected")
        node = session(core)
        props = node.props
        profile_id = props["profileId"]
        assert node.status == "idle" and node.error is None and node.name == "Vehicle: Fake Car"
        assert props["name"] == "Fake Car" and props["vin"] == "FAKEVIN000000001"
        assert props["pluginId"] == PLUGIN_ID and props["ecuCount"] == 2
        assert isinstance(props["since"], float) and props["since"] > 1e12
        assert action_names(node) == ["detect", "disconnect"]
        ecus = [core.tree.get(cid) for cid in node.children if core.tree.get(cid).type == "ecu"]
        assert [e.name for e in ecus] == ["Fake Engine", "Fake Body"]
        assert all(e.owner == HOST_ID and e.props["ecuId"] for e in ecus)
        assert core.tree.get(job_id).status == "done"  # the job stays visible while connected

        # library: one vehicle.profile child with the SPEC 15.3 props/actions
        library = core.tree.get(LIBRARY)
        assert library.children == [f"vehicle:{profile_id}"] and library.props["profileCount"] == 1
        card = core.tree.get(f"vehicle:{profile_id}")
        assert card.type == "vehicle.profile" and card.status == "idle" and card.name == "Fake Car"
        assert card.props["id"] == profile_id and card.props["vin"] == "FAKEVIN000000001"
        assert card.props["pluginId"] == PLUGIN_ID and card.props["ecuCount"] == 2
        assert card.props["ecus"] == [
            {"id": "engine", "name": "Fake Engine", "address": "0x10"},
            {"id": "body", "name": "Fake Body", "address": "0x26"},
        ]
        assert card.props["identifiers"] == {"serial": "abc"} and card.props["notes"] == ""
        assert card.props["firstSeen"] == card.props["lastSeen"] > 1e12
        actions = [a.model_dump(mode="json") for a in card.actions]
        assert [a["name"] for a in actions] == ["select", "rename", "forget"]
        assert actions[1]["params"][0]["name"] == "name" and actions[1]["params"][0]["default"] == "Fake Car"
        assert "Forget vehicle 'Fake Car'" in actions[2]["confirm"]
        # the profile file exists in <data dir>/vehicles
        path = core.data_dir / "vehicles" / f"{profile_id}.json"
        stored = VehicleProfile.model_validate(json.loads(path.read_text(encoding="utf-8")))
        assert stored.pluginId == PLUGIN_ID and [e.id for e in stored.ecus] == ["engine", "body"]
        assert stored.ecus[0].props == {"sw": "1.0"}

        # audit trail (category vehicle): grant, transitions, profile events
        messages = vehicle_log(core)
        assert any(m.startswith(f"mount grant {SESSION} -> plugin {PLUGIN_ID} (vehicle.detect)") for m in messages)
        assert any(m.startswith("vehicle session none -> detecting: plugin=fake_vehicle params=") for m in messages)
        assert any(m.startswith(f"vehicle.profile from plugin {PLUGIN_ID} for {SESSION}: name='Fake Car'") for m in messages)
        assert any(m.startswith(f"vehicle profile {profile_id} created:") for m in messages)
        assert any(m.startswith(f"vehicle session detecting -> connected: profile={profile_id}") for m in messages)
        for event in core.logs.history(LogFilter(categories=["vehicle"], search="-> connected")):
            assert event["nodeId"] == SESSION and event["data"]["profile"]["vin"] == "FAKEVIN000000001"

        # disconnect: vehicle.detach, subtree removed, grant revoked, none
        result = await core.bus.dispatch(SESSION, "disconnect", {})
        assert result.ok and result.data == {"previousState": "connected", "state": "none"}
        node = session(core)
        assert node.props["state"] == "none" and node.children == [] and node.status == "idle"
        assert node.props["profileId"] is None and node.props["ecuCount"] == 0 and node.props["since"] is None
        assert action_names(node) == ["detect"] and node.name == "Vehicle"
        assert host.grants == frozenset()
        await wait_until(lambda: core.tree.get(CTL_ID).props.get("detached") == SESSION, message="plugin saw detach")
        messages = vehicle_log(core)
        assert any(m.startswith(f"mount grant {SESSION} revoked from plugin {PLUGIN_ID} (vehicle.detach)") for m in messages)
        assert any(m.startswith("vehicle session connected -> none: disconnect requested") for m in messages)
        # the library keeps the profile
        assert core.tree.get(LIBRARY).children == [f"vehicle:{profile_id}"]
        result = await core.bus.dispatch(SESSION, "disconnect", {})
        assert not result.ok and "no vehicle session" in result.error.message


# -- failed / cancelled detect unwinds ---------------------------------------------------


async def test_failed_detect_returns_to_none_and_logs_why(tmp_path: Path) -> None:
    async with running_core(tmp_path) as core:
        host = core.hosts[HOST_ID]
        job_id = await detect(core)
        await ctl(core, "fail")
        await wait_until(lambda: state(core) == "none", message="unwound")
        node = session(core)
        assert node.children == [] and core.tree.get(job_id) is None and node.status == "idle"
        assert host.grants == frozenset() and action_names(node) == ["detect"]
        errors = vehicle_log(core, levels=["error"])
        assert any(
            m.startswith(f"vehicle session detecting -> none: detection job {job_id} 'Fake detection' failed: scan failed")
            for m in errors
        )
        assert any(m.startswith(f"mount grant {SESSION} revoked from plugin {PLUGIN_ID} (detection job") for m in vehicle_log(core))
        assert core.tree.get(LIBRARY).children == []  # nothing was stored
        # the session is usable again
        await connect(core)


async def test_cancelled_detect_via_job_cancel_returns_to_none(tmp_path: Path) -> None:
    async with running_core(tmp_path) as core:
        job_id = await detect(core)
        result = await core.bus.dispatch(job_id, "cancel", {})  # routed to the plugin (owner)
        assert result.ok
        await wait_until(lambda: state(core) == "none", message="unwound after cancel")
        assert session(core).children == []
        assert any(
            m.startswith(f"vehicle session detecting -> none: detection job {job_id} 'Fake detection' cancelled")
            for m in vehicle_log(core, levels=["warn"])
        )


async def test_detect_job_done_without_profile_fails_after_grace(tmp_path: Path) -> None:
    async with running_core(tmp_path) as core:
        job_id = await detect(core)
        await ctl(core, "done_no_profile")
        await asyncio.sleep(0.02)
        assert state(core) == "detecting"  # grace period: the profile may still be on its way
        await wait_until(lambda: state(core) == "none", timeout=2.0, message="unwound after grace")
        assert any(
            m.startswith(f"vehicle session detecting -> none: detection job {job_id} finished without a vehicle.profile")
            for m in vehicle_log(core, levels=["error"])
        )


async def test_detect_rejected_by_plugin(tmp_path: Path) -> None:
    async with running_core(tmp_path, env={"FAKE_DETECT_REJECT": "1"}) as core:
        result = await core.bus.dispatch(SESSION, "detect", {})
        assert not result.ok and result.error.message == "vehicle.detect failed: detect refused"
        assert result.error.detail == "FAKE_DETECT_REJECT"
        assert state(core) == "none" and core.hosts[HOST_ID].grants == frozenset()
        assert any(
            "-> none: vehicle.detect rejected by plugin fake_vehicle: detect refused" in m
            for m in vehicle_log(core, levels=["error"])
        )


async def test_detect_param_validation_and_busy_rules(tmp_path: Path) -> None:
    async with running_core(tmp_path) as core:
        result = await core.bus.dispatch(SESSION, "detect", {"depth": "deep"})
        assert not result.ok and "param 'depth' must be one of" in result.error.message
        result = await core.bus.dispatch(SESSION, "detect", {"retries": 9})
        assert not result.ok and "param 'retries' must be <= 5" in result.error.message
        result = await core.bus.dispatch(SESSION, "detect", {"plugin": "nope"})
        assert not result.ok and result.error.message == "unknown plugin 'nope'"
        assert state(core) == "none"
        await detect(core, {"plugin": PLUGIN_ID})  # naming the only plugin is fine
        result = await core.bus.dispatch(SESSION, "detect", {})
        assert not result.ok and "already detecting" in result.error.message


async def test_disconnect_while_detecting_cancels_job_then_detaches(tmp_path: Path) -> None:
    async with running_core(tmp_path) as core:
        job_id = await detect(core)
        result = await core.bus.dispatch(SESSION, "disconnect", {})
        assert result.ok and result.data["previousState"] == "detecting"
        assert state(core) == "none" and session(core).children == []
        control = core.tree.get(CTL_ID).props
        assert control["cancelled"] == job_id and control["detached"] == SESSION
        messages = vehicle_log(core)
        assert any(f"cancel of job {job_id} ok" in m for m in messages)
        assert any(f"vehicle.detach by plugin {PLUGIN_ID} ok" in m for m in messages)


async def test_redetect_while_connected_tears_down_first(tmp_path: Path) -> None:
    async with running_core(tmp_path) as core:
        first = await connect(core)
        job_id = await detect(core)  # allowed from connected: disconnect + fresh detect
        control = core.tree.get(CTL_ID).props
        assert control["detached"] == SESSION
        assert [c for c in session(core).children] == [job_id]  # old ECUs gone
        await ctl(core, "finish")
        await wait_until(lambda: state(core) == "connected")
        assert session(core).props["profileId"] == first  # same vehicle => same profile (upsert)
        assert core.tree.get(LIBRARY).children == [f"vehicle:{first}"]


# -- grants ------------------------------------------------------------------------------


async def test_mutations_under_session_need_a_grant(tmp_path: Path) -> None:
    async with running_core(tmp_path) as core:
        host = core.hosts[HOST_ID]
        # no grant yet: dropped + logged (SPEC 6 / 15.4)
        await ctl(core, "add_under_session")
        assert core.tree.get(f"{SESSION}:extra") is None
        assert len(dropped(core)) == 1 and "'add'" in dropped(core)[0]["message"]
        assert "outside the plugin subtree" in dropped(core)[0]["message"]
        await ctl(core, "io_session")
        assert core.logs.history(LogFilter(categories=["io"])) == []
        assert len(dropped(core)) == 2

        # with the grant (implied by vehicle.detect): allowed, owner stamped
        await detect(core)
        assert host.has_grant(SESSION)
        await ctl(core, "add_under_session")
        extra = core.tree.get(f"{SESSION}:extra")
        assert extra is not None and extra.owner == HOST_ID and extra.parentId == SESSION
        await ctl(core, "io_session")
        io_events = core.logs.history(LogFilter(categories=["io"]))
        assert len(io_events) == 1 and io_events[0]["nodeId"] == SESSION and io_events[0]["source"] == HOST_ID
        assert len(dropped(core)) == 2
        # ...but never elsewhere in the tree
        await ctl(core, "add_under_session", {"session": "jobs"})
        assert core.tree.get("jobs:extra") is None and len(dropped(core)) == 3

        # revoked with the session: dropped again
        assert (await core.bus.dispatch(SESSION, "disconnect", {})).ok
        assert not host.has_grant(SESSION)
        await ctl(core, "add_under_session")
        assert core.tree.get(f"{SESSION}:extra") is None and len(dropped(core)) == 4


async def test_bad_action_params_from_plugin_are_dropped(tmp_path: Path) -> None:
    async with running_core(tmp_path) as core:
        await ctl(core, "bad_action_params")
        assert core.tree.get(f"{HOST_ID}:badparams") is None
        events = dropped(core)
        assert len(events) == 1
        assert "'add'" in events[0]["message"] and "ValidationError" in events[0]["message"]
        assert "at least one option" in events[0]["message"]
        assert core.hosts[HOST_ID].healthy  # the reader survived


# -- plugin death: lost, restart: none -----------------------------------------------------


async def test_plugin_death_marks_session_lost_and_restart_returns_none(tmp_path: Path) -> None:
    async with running_core(tmp_path) as core:
        host = core.hosts[HOST_ID]
        profile_id = await connect(core)
        ecu_ids = list(session(core).children)
        await core.bus.dispatch(CTL_ID, "die", {})
        await wait_until(lambda: state(core) == "lost", message="lost")
        node = session(core)
        assert node.status == "error" and node.error.message == "plugin process died"
        assert "died while the session was connected" in node.error.detail
        assert node.props["profileId"] == profile_id and node.props["pluginId"] == PLUGIN_ID
        assert action_names(node) == ["detect", "disconnect"]
        # the plugin-owned evidence stays, marked error like the plugin's own subtree
        assert node.children == ecu_ids
        for ecu_id in ecu_ids:
            ecu = core.tree.get(ecu_id)
            assert ecu.status == "error" and ecu.error.message == "plugin process died"
        assert host.grants == frozenset()
        assert any("vehicle session connected -> lost: plugin fake_vehicle died" in m for m in vehicle_log(core, levels=["error"]))
        assert any(m.startswith(f"mount grant {SESSION} revoked from plugin {PLUGIN_ID} (plugin died") for m in vehicle_log(core))
        # while dead: the detect action carries no plugin params, select fails with a reason
        await wait_until(lambda: session(core).actions[0].params == [], message="detect params recomputed")
        result = await core.bus.dispatch(f"vehicle:{profile_id}", "select", {})
        assert not result.ok and result.error.message == "plugin 'fake_vehicle' is not running"

        # automatic restart (1 s backoff): the old session subtree is removed, state none
        await wait_until(lambda: state(core) == "none", timeout=15.0, message="none after restart")
        assert session(core).children == [] and session(core).error is None
        assert any("-> none: plugin fake_vehicle restarted" in m for m in vehicle_log(core, levels=["warn"]))
        await wait_plugin(core)
        assert session(core).actions[0].params  # params are back with the healthy plugin
        # and a fresh detect works (grant re-issued to the new incarnation)
        await connect(core)
        assert session(core).props["profileId"] == profile_id


async def test_disconnect_in_lost_state_skips_detach(tmp_path: Path) -> None:
    async with running_core(tmp_path) as core:
        await connect(core)
        await core.bus.dispatch(CTL_ID, "die", {})
        await wait_until(lambda: state(core) == "lost")
        result = await core.bus.dispatch(SESSION, "disconnect", {})
        assert result.ok and result.data["previousState"] == "lost"
        assert state(core) == "none" and session(core).children == []
        assert any("is not running; vehicle.detach skipped" in m for m in vehicle_log(core, levels=["warn"]))


# -- detach timeout ----------------------------------------------------------------------------


async def test_detach_timeout_is_logged_and_session_still_ends(tmp_path: Path) -> None:
    async with running_core(tmp_path, env={"FAKE_DETACH_HANG": "1"}) as core:
        core.vehicles.detach_timeout = 0.3
        await connect(core)
        loop = asyncio.get_running_loop()
        started = loop.time()
        result = await core.bus.dispatch(SESSION, "disconnect", {})
        elapsed = loop.time() - started
        assert result.ok and 0.25 <= elapsed < 3.0
        assert state(core) == "none" and session(core).children == []
        assert core.hosts[HOST_ID].grants == frozenset()
        warns = vehicle_log(core, levels=["warn"])
        assert any("vehicle.detach vehicle by plugin fake_vehicle did not succeed: vehicle.detach timed out after 0.3 s" in m for m in warns)
        assert any("vehicle.detach by plugin fake_vehicle failed: vehicle.detach timed out" in m for m in warns)
        assert core.hosts[HOST_ID].healthy  # a slow detach is not a plugin death


# -- vehicle.profile validation ---------------------------------------------------------------


async def test_vehicle_profile_notification_validation(tmp_path: Path) -> None:
    async with running_core(tmp_path) as core:
        await detect(core)
        await ctl(core, "profile_bad")
        await ctl(core, "profile_nogrant")
        await ctl(core, "profile_wrong_plugin")
        assert state(core) == "detecting" and core.tree.get(LIBRARY).children == []
        events = dropped(core)
        assert len(events) == 3 and all("'vehicle.profile'" in e["message"] for e in events)
        assert "ValidationError" in events[0]["message"] and events[0]["data"]["params"]["profile"] == {"vin": "X", "ecus": "nope"}
        assert "'jobs' is not a session this plugin holds a mount grant for" in events[1]["message"]
        assert "pluginId 'other' does not match this plugin" in events[2]["message"]
        assert all(e["source"] == HOST_ID for e in events)
        # a valid profile afterwards still connects
        await ctl(core, "finish")
        await wait_until(lambda: state(core) == "connected")


async def test_profile_resent_upserts_same_profile(tmp_path: Path) -> None:
    async with running_core(tmp_path) as core:
        profile_id = await connect(core)
        await ctl(core, "profile", {"identifiers": {"serial": "abc", "color": "blue"}, "name": "renamed by plugin"})
        await wait_until(lambda: core.tree.get(f"vehicle:{profile_id}").props["identifiers"].get("color") == "blue")
        assert state(core) == "connected" and session(core).props["profileId"] == profile_id
        card = core.tree.get(f"vehicle:{profile_id}")
        assert card.props["name"] == "Fake Car"  # the display name is user-owned: kept
        assert core.tree.get(LIBRARY).children == [f"vehicle:{profile_id}"]
        on_disk = json.loads((core.data_dir / "vehicles" / f"{profile_id}.json").read_text(encoding="utf-8"))
        assert on_disk["identifiers"] == {"serial": "abc", "color": "blue"}
        assert sum(1 for m in vehicle_log(core) if f"vehicle profile {profile_id} updated" in m) == 1


# -- library: select / rename / forget / reload ------------------------------------------------


async def test_select_attach_rename_forget(tmp_path: Path) -> None:
    async with running_core(tmp_path) as core:
        profile_id = await connect(core)
        card_id = f"vehicle:{profile_id}"
        first_seen = core.tree.get(card_id).props["lastSeen"]
        assert (await core.bus.dispatch(SESSION, "disconnect", {})).ok

        # select => attaching (plugin re-declares ECUs + verification job) => connected
        result = await core.bus.dispatch(card_id, "select", {})
        assert result.ok, result.error
        job_id = result.data["jobNodeId"]
        node = session(core)
        assert node.props["state"] == "attaching" and node.status == "running"
        assert node.props["profileId"] == profile_id and node.props["name"] == "Fake Car"
        assert action_names(node) == ["disconnect"]
        assert core.tree.get(job_id).parentId == SESSION and core.tree.get(job_id).props["attachedProfile"] == profile_id
        result = await core.bus.dispatch(card_id, "select", {})
        assert not result.ok and "busy (attaching)" in result.error.message
        await ctl(core, "finish_attach")
        await wait_until(lambda: state(core) == "connected", message="connected after attach")
        assert session(core).props["ecuCount"] == 2
        assert core.tree.get(card_id).props["lastSeen"] > first_seen  # seen again
        assert any(m.startswith("vehicle session none -> attaching: profile=") for m in vehicle_log(core))
        assert any(f"attaching -> connected: profile={profile_id}" in m and "attach job" in m for m in vehicle_log(core))

        # rename: node, props and the live session follow
        result = await core.bus.dispatch(card_id, "rename", {"name": "  My Fake Car "})
        assert result.ok and result.data == {"profileId": profile_id, "name": "My Fake Car"}
        card = core.tree.get(card_id)
        assert card.name == "My Fake Car" and card.props["name"] == "My Fake Car"
        assert card.actions[1].params[0].default == "My Fake Car"
        assert session(core).props["name"] == "My Fake Car" and session(core).name == "Vehicle: My Fake Car"
        result = await core.bus.dispatch(card_id, "rename", {"name": "   "})
        assert not result.ok and "non-empty string" in result.error.message

        # forget is refused while the profile is the active session's
        result = await core.bus.dispatch(card_id, "forget", {})
        assert not result.ok and "disconnect first" in result.error.message
        assert (await core.bus.dispatch(SESSION, "disconnect", {})).ok
        result = await core.bus.dispatch(card_id, "forget", {})
        assert result.ok and result.data == {"profileId": profile_id}
        assert core.tree.get(card_id) is None and core.tree.get(LIBRARY).children == []
        assert not (core.data_dir / "vehicles" / f"{profile_id}.json").exists()
        assert core.tree.get(LIBRARY).props["profileCount"] == 0
        result = await core.bus.dispatch(LIBRARY, "select", {})
        assert not result.ok and "unknown action" in result.error.message


async def test_library_loads_profiles_on_second_start_and_attach_without_job(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    async with running_core(tmp_path, data_dir=data_dir) as core:
        profile_id = await connect(core)
    # a second core start with the same data dir shows the profile before any plugin runs
    core2 = AppCore(plugins_dir=FIXTURES, log_dir=tmp_path / "logs2", data_dir=data_dir, plugin_env={"FAKE_ATTACH_NO_JOB": "1"})
    try:
        library = core2.tree.get(LIBRARY)
        assert library.children == [f"vehicle:{profile_id}"] and library.status == "idle"
        assert core2.tree.get(f"vehicle:{profile_id}").props["vin"] == "FAKEVIN000000001"
        assert any(m.startswith("vehicle store loaded 1 profile(s)") for m in vehicle_log(core2))
        await core2.start()
        await wait_plugin(core2)
        # attach without a verification job => connected right away
        result = await core2.bus.dispatch(f"vehicle:{profile_id}", "select", {})
        assert result.ok and result.data["jobNodeId"] is None and result.data["state"] == "connected"
        node = session(core2)
        assert node.props["state"] == "connected" and node.props["ecuCount"] == 2
        assert any("without verification job" in m for m in vehicle_log(core2))
    finally:
        await core2.stop()


async def test_library_load_error_marks_library_error_with_reload(tmp_path: Path) -> None:
    vehicles_dir = tmp_path / "data" / "vehicles"
    vehicles_dir.mkdir(parents=True)
    good = VehicleProfile(id="veh_good", name="Good", vin="V1", pluginId=PLUGIN_ID, firstSeen=1.0, lastSeen=1.0)
    (vehicles_dir / "veh_good.json").write_text(json.dumps(good.model_dump(mode="json")), encoding="utf-8")
    bad = vehicles_dir / "veh_bad.json"
    bad.write_text("{ nope", encoding="utf-8")
    async with running_core(tmp_path) as core:
        library = core.tree.get(LIBRARY)
        assert library.status == "error" and library.error.message == "vehicle profile store error"
        assert "veh_bad.json" in library.error.detail and "JSONDecodeError" in library.error.detail
        assert action_names(library) == ["reload"] and library.props["profileCount"] == 1
        assert library.children == ["vehicle:veh_good"]  # good files are still served
        result = await core.bus.dispatch(LIBRARY, "reload", {})
        assert result.ok and result.data["profiles"] == 1 and len(result.data["errors"]) == 1
        assert core.tree.get(LIBRARY).status == "error"  # still broken
        bad.unlink()
        result = await core.bus.dispatch(LIBRARY, "reload", {})
        assert result.ok and result.data == {"profiles": 1, "errors": []}
        library = core.tree.get(LIBRARY)
        assert library.status == "idle" and library.error is None and library.actions == []
        assert library.children == ["vehicle:veh_good"]
        # `reload` is only OFFERED while in error, but a re-read is always harmless
        result = await core.bus.dispatch(LIBRARY, "reload", {})
        assert result.ok and core.tree.get(LIBRARY).status == "idle" and core.tree.get(LIBRARY).actions == []


# -- several capable plugins: `plugin` select ---------------------------------------------------


async def test_two_capable_plugins_add_plugin_select(tmp_path: Path) -> None:
    plugins = tmp_path / "plugins"
    for plugin_id in ("fake_vehicle", "fake_vehicle_b"):
        target = plugins / plugin_id
        shutil.copytree(FAKE_DIR, target)
        manifest = json.loads((target / "manifest.json").read_text(encoding="utf-8"))
        manifest["id"] = plugin_id
        manifest["name"] = f"Fake {plugin_id}"
        if plugin_id == "fake_vehicle_b":
            manifest["vehicle"]["detectParams"] = [{"name": "bus", "label": "Bus", "type": "string"}, {"name": "depth", "label": "D"}]
        (target / "manifest.json").write_text(json.dumps(manifest), encoding="utf-8")
    async with running_core(tmp_path, plugins_dir=plugins) as core:
        await wait_plugin(core, "plugin:fake_vehicle_b")
        await wait_until(lambda: session(core).actions[0].params[0].name == "plugin", message="plugin select present")
        params = [p.model_dump(mode="json") for p in session(core).actions[0].params]
        assert params[0]["type"] == "select" and params[0]["required"] is True
        assert params[0]["options"] == [
            {"value": "fake_vehicle", "label": "Fake fake_vehicle"},
            {"value": "fake_vehicle_b", "label": "Fake fake_vehicle_b"},
        ]
        # union of the plugins' detectParams, each tagged with its plugin (SPEC 3.1.1 `when`,
        # 15.2 resolution): same-named params of different plugins coexist, nothing is dropped
        assert [p["name"] for p in params[1:]] == ["depth", "retries", "bus", "depth"]
        assert [p["when"] for p in params[1:]] == [
            {"plugin": "fake_vehicle"},
            {"plugin": "fake_vehicle"},
            {"plugin": "fake_vehicle_b"},
            {"plugin": "fake_vehicle_b"},
        ]
        assert params[1]["type"] == "select" and params[4]["type"] == "string"

        result = await core.bus.dispatch(SESSION, "detect", {"depth": "quick"})
        assert not result.ok and "param 'plugin' is required" in result.error.message
        # what the generic form sends for plugin b: only b's params (a's `depth`
        # select is hidden by its `when`), plus whatever a client adds — the
        # core forwards exactly the SELECTED plugin's declared params
        result = await core.bus.dispatch(
            SESSION, "detect", {"plugin": "fake_vehicle_b", "bus": "can0", "depth": "deep", "retries": 3}
        )
        assert result.ok and result.data["pluginId"] == "fake_vehicle_b"
        assert session(core).props["pluginId"] == "fake_vehicle_b"
        assert core.hosts["plugin:fake_vehicle_b"].has_grant(SESSION)
        assert not core.hosts["plugin:fake_vehicle"].has_grant(SESSION)
        job = core.tree.get(result.data["jobNodeId"])
        # validated against b's own `depth` (a plain string, so 'deep' is fine), a's `retries` stripped
        assert job.owner == "plugin:fake_vehicle_b" and job.props["detectParams"] == {"bus": "can0", "depth": "deep"}
        detecting = [e for e in core.logs.history(LogFilter(categories=["vehicle"], search="none -> detecting"))]
        assert detecting[-1]["data"]["params"] == {"bus": "can0", "depth": "deep"}  # audited as b received it
        # the other plugin cannot mutate under the session
        await ctl(core, "add_under_session")
        assert core.tree.get(f"{SESSION}:extra") is None and len(dropped(core)) == 1
        assert (await core.bus.dispatch(SESSION, "disconnect", {})).ok
        assert state(core) == "none" and session(core).actions[0].params[0].name == "plugin"
        # one plugin dying makes the select disappear (one capable plugin left => its params only)
        await core.bus.dispatch("plugin:fake_vehicle:ctl", "die", {})
        await wait_until(
            lambda: [p.name for p in session(core).actions[0].params] == ["bus", "depth"],
            timeout=5.0,
            message="select dropped after plugin death",
        )
        assert session(core).actions[0].params[1].type == "string"  # plugin b's own `depth` now


# -- the granted root itself stays core-owned (SPEC 15.2/15.4, SPEC 6) -----------------------


async def test_grant_never_covers_the_session_node_itself(tmp_path: Path) -> None:
    async with running_core(tmp_path) as core:
        host = core.hosts[HOST_ID]
        job_id = await detect(core)
        assert host.has_grant(SESSION)
        before = session(core).model_dump(mode="json")
        dropped_before = len(dropped(core))

        await ctl(
            core,
            "raw_notify",
            {
                "method": "update",
                "params": {
                    "id": SESSION,
                    "changes": {"status": "done", "type": "hijacked", "props": {"state": "none"}, "actions": []},
                },
            },
        )
        await ctl(core, "raw_notify", {"method": "merge_props", "params": {"id": SESSION, "props": {"state": "connected"}}})
        await ctl(core, "raw_notify", {"method": "remove", "params": {"id": SESSION}})
        await asyncio.sleep(0.05)

        # untouched: type, status, props, actions, children - and the manager still detects
        assert session(core).model_dump(mode="json") == before
        assert state(core) == "detecting" and session(core).children == [job_id]
        events = dropped(core)[dropped_before:]
        assert [e["data"]["method"] for e in events] == ["update", "merge_props", "remove"]
        assert all("granted root (core-owned)" in e["message"] and e["source"] == HOST_ID for e in events)
        assert all(e["data"]["params"]["id"] == SESSION for e in events)
        # ...while the plugin's own nodes under the session stay mutable (the job it declared)
        await ctl(core, "raw_notify", {"method": "merge_props", "params": {"id": job_id, "props": {"note": "mine"}}})
        await wait_until(lambda: core.tree.get(job_id).props.get("note") == "mine", message="own node under grant mutable")
        assert len(dropped(core)) == dropped_before + 3
        # the session is still fully operable
        assert (await core.bus.dispatch(SESSION, "disconnect", {})).ok
        assert state(core) == "none" and not host.has_grant(SESSION)


# -- a cancelled command task never leaves the session half-transitioned (SPEC 1.5) ----------


async def test_cancelled_disconnect_command_still_ends_the_session(tmp_path: Path) -> None:
    async with running_core(tmp_path, env={"FAKE_DETACH_HANG": "1"}) as core:
        core.vehicles.detach_timeout = 0.3
        host = core.hosts[HOST_ID]
        await connect(core)
        task = asyncio.create_task(core.bus.dispatch(SESSION, "disconnect", {}))
        await asyncio.sleep(0.05)  # vehicle.detach is pending (the plugin never answers)
        task.cancel()  # what /ws/state does when the client disconnects mid-command
        with pytest.raises(asyncio.CancelledError):
            await task
        assert state(core) == "connected"  # the transition is still running (detach cap not reached)
        await wait_until(lambda: state(core) == "none", timeout=2.0, message="session ended after the cancelled command")
        node = session(core)
        assert node.children == [] and node.status == "idle" and action_names(node) == ["detect"]
        assert host.grants == frozenset()
        assert any("command 'disconnect' abandoned by its caller" in m for m in vehicle_log(core, levels=["warn"]))
        assert any(m.startswith("vehicle session connected -> none: disconnect requested") for m in vehicle_log(core))
        assert host.healthy
        # a later mutation under the session by the plugin is dropped as usual (grant gone)
        await ctl(core, "add_under_session")
        assert core.tree.get(f"{SESSION}:extra") is None


async def test_cancelled_detect_command_still_settles_the_session(tmp_path: Path) -> None:
    async with running_core(tmp_path, env={"FAKE_DETECT_HANG": "1"}) as core:
        core.vehicles.start_timeout = 0.3
        host = core.hosts[HOST_ID]
        task = asyncio.create_task(core.bus.dispatch(SESSION, "detect", {}))
        await wait_until(lambda: state(core) == "detecting", message="detecting")
        assert host.has_grant(SESSION)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        # the transition continues: the plugin never answers, the start timeout unwinds it
        await wait_until(lambda: state(core) == "none", timeout=2.0, message="unwound after the start timeout")
        assert host.grants == frozenset() and session(core).children == []
        assert any(
            "detecting -> none: vehicle.detect rejected by plugin fake_vehicle: vehicle.detect timed out after 0.3 s" in m
            for m in vehicle_log(core, levels=["error"])
        )
        assert any("command 'detect' abandoned by its caller" in m for m in vehicle_log(core, levels=["warn"]))


# -- the job stays watched after an early vehicle.profile (SPEC 15.4) -------------------------


async def test_job_failing_after_early_profile_returns_to_none(tmp_path: Path) -> None:
    async with running_core(tmp_path) as core:
        host = core.hosts[HOST_ID]
        job_id = await detect(core)
        await ctl(core, "profile")  # the profile precedes the job's end
        await wait_until(lambda: state(core) == "connected", message="connected on the early profile")
        assert core.vehicles.job_id == job_id and core.tree.get(job_id).status == "running"
        await ctl(core, "fail")
        await wait_until(lambda: state(core) == "none", message="unwound after the late failure")
        node = session(core)
        assert node.children == [] and core.tree.get(job_id) is None and host.grants == frozenset()
        assert any(
            m.startswith(f"vehicle session connected -> none: detection job {job_id} 'Fake detection' failed: scan failed")
            for m in vehicle_log(core, levels=["error"])
        )
        # the profile itself was stored (the vehicle WAS identified); only the session ended
        assert len(core.tree.get(LIBRARY).children) == 1


async def test_attach_verification_failing_after_early_profile_returns_to_none(tmp_path: Path) -> None:
    async with running_core(tmp_path) as core:
        profile_id = await connect(core)
        assert (await core.bus.dispatch(SESSION, "disconnect", {})).ok
        result = await core.bus.dispatch(f"vehicle:{profile_id}", "select", {})
        assert result.ok, result.error
        job_id = result.data["jobNodeId"]
        await ctl(core, "profile", {"identifiers": {"serial": "abc", "learned": "yes"}})  # re-sent early: upsert
        await wait_until(lambda: state(core) == "connected", message="connected on the early profile during verify")
        assert session(core).props["ecuCount"] == 2 and core.vehicles.job_id == job_id
        await ctl(core, "fail")  # the verification then fails: profile mismatch => like a failed detect
        await wait_until(lambda: state(core) == "none", message="unwound after the failed verification")
        assert session(core).children == [] and session(core).props["ecuCount"] == 0
        assert any(
            m.startswith(f"vehicle session connected -> none: attach job {job_id} 'Fake verify' failed")
            for m in vehicle_log(core, levels=["error"])
        )
        assert core.tree.get(f"vehicle:{profile_id}").props["identifiers"]["learned"] == "yes"


async def test_job_done_after_early_profile_stays_connected(tmp_path: Path) -> None:
    async with running_core(tmp_path) as core:
        job_id = await detect(core)
        await ctl(core, "profile")
        await wait_until(lambda: state(core) == "connected")
        await ctl(core, "done_no_profile")
        await wait_until(lambda: core.vehicles.job_id is None, message="job no longer watched once done")
        await asyncio.sleep(0.3)  # > profile_grace: no grace timer runs while connected
        assert state(core) == "connected" and core.tree.get(job_id).status == "done"
        assert any(f"connected: detection job {job_id} 'Fake detection' done; no longer watched" in m for m in vehicle_log(core))


# -- a job removed by the plugin counts as vanished --------------------------------------------


async def test_job_removed_by_plugin_unwinds_the_session(tmp_path: Path) -> None:
    async with running_core(tmp_path) as core:
        host = core.hosts[HOST_ID]
        job_id = await detect(core)
        await ctl(core, "remove_job")
        await wait_until(lambda: state(core) == "none", message="unwound after the job vanished")
        assert host.grants == frozenset() and session(core).children == []
        assert any(
            m.startswith(f"vehicle session detecting -> none: detection job {job_id} vanished from the tree")
            for m in vehicle_log(core, levels=["error"])
        )
        await connect(core)  # usable again


# -- vehicle.profile may carry the core-owned keys (re-emitting the attach payload) ---------------


async def test_profile_resent_with_core_owned_keys_is_upserted(tmp_path: Path) -> None:
    async with running_core(tmp_path) as core:
        profile_id = await connect(core)
        assert (await core.bus.dispatch(SESSION, "disconnect", {})).ok
        assert (await core.bus.dispatch(f"vehicle:{profile_id}", "select", {})).ok
        await ctl(core, "finish_attach")
        await wait_until(lambda: state(core) == "connected")
        card = core.tree.get(f"vehicle:{profile_id}")
        last_seen = card.props["lastSeen"]
        dropped_before = len(dropped(core))
        updated_before = sum(1 for m in vehicle_log(core) if f"vehicle profile {profile_id} updated" in m)

        sent = (await ctl(core, "profile_full", {"identifiers": {"learned": "later"}}))["sent"]
        assert {"id", "firstSeen", "lastSeen", "pluginId"} <= set(sent)  # the exact attach payload, plus one identifier
        await wait_until(
            lambda: core.tree.get(f"vehicle:{profile_id}").props["identifiers"].get("learned") == "later",
            message="profile updated from the re-sent full profile",
        )
        card = core.tree.get(f"vehicle:{profile_id}")
        assert card.props["identifiers"] == {"serial": "abc", "learned": "later"} and card.props["lastSeen"] > last_seen
        assert card.props["id"] == profile_id and state(core) == "connected"
        assert len(dropped(core)) == dropped_before
        assert sum(1 for m in vehicle_log(core) if f"vehicle profile {profile_id} updated" in m) == updated_before + 1


# -- vehicle.detect answered without a job ------------------------------------------------------


async def test_detect_without_job_needs_a_profile_within_grace(tmp_path: Path) -> None:
    async with running_core(tmp_path, env={"FAKE_DETECT_NO_JOB": "1"}) as core:
        host = core.hosts[HOST_ID]
        result = await core.bus.dispatch(SESSION, "detect", {})
        assert result.ok and result.data == {"jobNodeId": None, "pluginId": PLUGIN_ID, "state": "detecting"}
        assert state(core) == "detecting" and host.has_grant(SESSION)
        await wait_until(lambda: state(core) == "none", timeout=2.0, message="unwound after the grace period")
        assert host.grants == frozenset()
        assert any(
            "detecting -> none: vehicle.detect by plugin fake_vehicle answered without a job and no vehicle.profile"
            " arrived within" in m
            for m in vehicle_log(core, levels=["error"])
        )
        # the happy path: a synchronous detection that does send its profile
        result = await core.bus.dispatch(SESSION, "detect", {})
        assert result.ok
        await ctl(core, "profile")
        await wait_until(lambda: state(core) == "connected", message="connected without a job")
        await asyncio.sleep(0.3)
        assert state(core) == "connected"


# -- a detect whose job already failed answers ok:false -----------------------------------------


async def test_detect_whose_job_already_failed_answers_ok_false(tmp_path: Path) -> None:
    async with running_core(tmp_path, env={"FAKE_DETECT_INSTANT_FAIL": "1"}) as core:
        result = await core.bus.dispatch(SESSION, "detect", {}, cmd_id="c9")
        assert not result.ok
        assert result.error.message.startswith(
            "vehicle.detect failed: detection job vehicle:job:1 'Fake detection' failed: instant fail"
        )
        assert state(core) == "none" and core.hosts[HOST_ID].grants == frozenset()
        assert any("detecting -> none: detection job vehicle:job:1" in m for m in vehicle_log(core, levels=["error"]))
        cmd_events = core.logs.history(LogFilter(categories=["cmd"], search="c9"))
        assert cmd_events[-1]["message"].startswith("cmd 'detect' on 'vehicle' [c9] failed: vehicle.detect failed")
        assert cmd_events[-1]["data"]["result"]["ok"] is False


# -- job milestone bookkeeping does not outlive the session subtree -------------------------------


async def test_session_subtree_removal_forgets_job_bookkeeping(tmp_path: Path) -> None:
    async with running_core(tmp_path) as core:
        host = core.hosts[HOST_ID]
        first = await detect(core)
        assert first in host._job_milestones
        await ctl(core, "finish")
        await wait_until(lambda: state(core) == "connected")
        assert (await core.bus.dispatch(SESSION, "disconnect", {})).ok
        assert first not in host._job_milestones
        second = await detect(core)
        await ctl(core, "fail")
        await wait_until(lambda: state(core) == "none")
        assert second not in host._job_milestones
        third = await detect(core)
        assert (await core.bus.dispatch(third, "cancel", {})).ok
        await wait_until(lambda: state(core) == "none")
        assert third not in host._job_milestones and host._job_milestones == {}


# -- a hung plugin never makes Disconnect wait for the command timeout -----------------------------


async def test_disconnect_preempts_a_hung_detect(tmp_path: Path) -> None:
    async with running_core(tmp_path, env={"FAKE_DETECT_HANG": "1"}) as core:
        core.vehicles.start_timeout = 5.0  # the RPC would block the session lock for this long
        core.vehicles.detach_timeout = 0.5
        host = core.hosts[HOST_ID]
        detect_task = asyncio.create_task(core.bus.dispatch(SESSION, "detect", {}))
        await wait_until(lambda: state(core) == "detecting", message="detecting (RPC pending)")
        loop = asyncio.get_running_loop()
        started = loop.time()
        result = await core.bus.dispatch(SESSION, "disconnect", {})
        elapsed = loop.time() - started
        assert result.ok and result.data == {"previousState": "detecting", "state": "none", "abandoned": True}
        assert elapsed < 1.0, f"disconnect waited {elapsed:.2f} s for the hung vehicle.detect"
        detect_result = await detect_task
        assert not detect_result.ok and "vehicle.detect abandoned: disconnect requested" in detect_result.error.message
        assert state(core) == "none" and host.grants == frozenset() and session(core).children == []
        await wait_until(lambda: core.tree.get(CTL_ID).props.get("detached") == SESSION, message="plugin told to let go")
        messages = vehicle_log(core)
        assert any("disconnect pre-empts the pending vehicle.detect" in m for m in messages)
        assert any(
            "vehicle.detect by plugin fake_vehicle abandoned: disconnect requested" in m
            for m in vehicle_log(core, levels=["warn"])
        )
        assert any("detecting -> none: disconnect requested while awaiting vehicle.detect" in m for m in messages)
        assert host.healthy  # a slow plugin is not a dead plugin


async def test_disconnect_is_bounded_when_the_job_cancel_hangs(tmp_path: Path) -> None:
    async with running_core(tmp_path, env={"FAKE_CANCEL_HANG": "1"}) as core:
        core.vehicles.detach_timeout = 0.3
        job_id = await detect(core)
        loop = asyncio.get_running_loop()
        started = loop.time()
        result = await core.bus.dispatch(SESSION, "disconnect", {})
        elapsed = loop.time() - started
        assert result.ok and result.data["previousState"] == "detecting"
        assert 0.25 <= elapsed < 1.5, f"disconnect took {elapsed:.2f} s"
        assert state(core) == "none" and session(core).children == []
        assert any(
            f"cancel of job {job_id} not answered by plugin fake_vehicle within 0.3 s" in m
            for m in vehicle_log(core, levels=["warn"])
        )
        assert any(f"vehicle.detach by plugin {PLUGIN_ID} ok" in m for m in vehicle_log(core))
