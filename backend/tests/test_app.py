"""Composition root: well-known nodes, plugin discovery, core routing."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import httpx

from diagcore.app import AppCore
from diagcore.loghub import LogFilter
from diagcore.server import create_app


async def test_well_known_nodes(tmp_path: Path) -> None:
    core = AppCore(plugins_dir=tmp_path / "none", log_dir=tmp_path / "logs")
    tree = core.tree
    assert tree.root_id == "root"
    assert tree.get("root").type == "root"
    assert tree.get("root").children == ["system", "plugins", "jobs", "vehicle", "vehicles"]
    assert tree.get("system").type == "system"
    assert tree.get("plugins").type == "plugins"
    assert tree.get("jobs").type == "jobs"
    assert tree.get("vehicle").type == "vehicle.session"  # M1.5, SPEC 15.2
    assert tree.get("vehicles").type == "vehicle.library"  # M1.5, SPEC 15.3
    props = tree.get("system").props
    assert set(props) == {"version", "startedAt", "pythonVersion", "auditFile", "dataDir"}
    assert props["auditFile"] == str(core.logs.session_path)  # where the trail goes (SPEC 3.2/14.1)
    assert props["dataDir"] == str(core.data_dir)  # where profiles go (SPEC 15.1)
    assert tree.get("system").status == "idle"


# -- audit sink failure ends up on the `system` node (SPEC 1.5 / 14.1) ----------


def system_patches(core: AppCore) -> list[dict]:
    return core.logs.history(LogFilter(nodeId="system", categories=["state"]))


async def test_audit_write_failure_marks_system_node_once(tmp_path: Path) -> None:
    core = AppCore(plugins_dir=tmp_path / "none", log_dir=tmp_path / "logs")
    before = len(system_patches(core))
    core.logs._file.close()  # the file becomes unwritable behind the hub's back
    core.logs.emit("info", "cmd", "a coding write to the car")  # the failing write
    await asyncio.sleep(0)  # the node update is deferred to the loop (seq order)

    system = core.tree.get("system")
    assert system.status == "error"
    assert system.error.message == "audit session file disabled"
    assert "closed file" in system.error.detail
    assert system.props["auditFile"] == str(core.logs.session_path)
    assert system.props["auditFileError"] == core.logs.sink_error
    assert system.props["version"]  # existing props kept
    assert len(system_patches(core)) == before + 1  # exactly one patch
    # the outage itself is an error event in the ring, and further emits do
    # not repeat either the event or the patch
    assert len(core.logs.history(LogFilter(levels=["error"], categories=["system"]))) == 1
    core.logs.emit("info", "cmd", "more")
    core.logs.emit("info", "cmd", "and more")
    await asyncio.sleep(0)
    assert len(system_patches(core)) == before + 1
    assert len(core.logs.history(LogFilter(levels=["error"], categories=["system"]))) == 1
    # /api/health mirrors it
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(core)), base_url="http://t") as c:
        body = (await c.get("/api/health")).json()
    assert body["ok"] is False
    assert body["auditFile"] == str(core.logs.session_path)
    assert body["auditFileError"] == core.logs.sink_error


async def test_audit_open_failure_at_startup_marks_system_node(tmp_path: Path) -> None:
    log_dir = tmp_path / "logs"
    log_dir.write_text("not a directory", encoding="utf-8")
    core = AppCore(plugins_dir=tmp_path / "none", log_dir=log_dir)
    system = core.tree.get("system")
    assert system.status == "error"
    assert system.error.message == "audit session file disabled"
    assert system.props["auditFile"] is None
    assert system.props["auditFileError"]
    started = core.logs.history(LogFilter(categories=["system"], search="session started"))[0]
    assert started["data"]["sessionFile"] is None  # JSON null, not the string "None"
    failure = core.logs.history(LogFilter(levels=["error"], categories=["system"]))
    assert len(failure) == 1 and failure[0]["n"] == started["n"] + 1


async def test_audit_failure_inside_tree_subscriber_keeps_seq_order(tmp_path: Path) -> None:
    """The state seam emits from inside a tree subscriber. If the write fails
    there, the resulting `system` update must be broadcast AFTER the patch
    that triggered it (a nested seq N+1 before N would force a UI resync)."""
    core = AppCore(plugins_dir=tmp_path / "none", log_dir=tmp_path / "logs")
    seen: list[tuple[int, str]] = []
    core.tree.subscribe(lambda seq, op: seen.append((seq, op["id"] if "id" in op else op["node"]["id"])))
    core.logs._file.close()
    core.tree.update("jobs", {"status": "running"})  # its state event is the failing write
    await asyncio.sleep(0)
    assert [node_id for _, node_id in seen] == ["jobs", "system"]
    seqs = [seq for seq, _ in seen]
    assert seqs == [seqs[0], seqs[0] + 1]
    assert core.tree.get("system").status == "error"


async def test_broken_manifest_yields_error_node(tmp_path: Path) -> None:
    plugins = tmp_path / "plugins"
    bad = plugins / "broken_one"
    bad.mkdir(parents=True)
    (bad / "manifest.json").write_text("{ this is not json", encoding="utf-8")

    core = AppCore(plugins_dir=plugins, log_dir=tmp_path / "logs")
    await core.start()
    try:
        node = core.tree.get("plugin:broken_one")
        assert node is not None
        assert node.status == "error"
        assert node.error.message == "invalid plugin manifest"
        assert core.hosts == {}  # no host spawned for it
    finally:
        await core.stop()


async def test_duplicate_plugin_id_yields_error_node(tmp_path: Path) -> None:
    plugins = tmp_path / "plugins"
    manifest = '{"id": "dup", "name": "Dup", "version": "0.0.1", "entry": "plugin.py"}'
    for dir_name in ("dup_a", "dup_b"):
        plugin_dir = plugins / dir_name
        plugin_dir.mkdir(parents=True)
        (plugin_dir / "manifest.json").write_text(manifest, encoding="utf-8")
        (plugin_dir / "plugin.py").write_text("", encoding="utf-8")

    core = AppCore(plugins_dir=plugins, log_dir=tmp_path / "logs")
    core._load_plugins_from_disk()  # no start(): don't spawn processes

    # first (sorted) directory wins; the duplicate is flagged, not silent
    assert set(core.hosts) == {"plugin:dup"}
    assert core.hosts["plugin:dup"].plugin_dir.name == "dup_a"
    broken = core.tree.get("plugin:dup_b")
    assert broken is not None
    assert broken.status == "error"
    assert "duplicate plugin id" in broken.error.message
    assert "dup_a" in broken.error.detail


async def test_core_command_unknown_action(tmp_path: Path) -> None:
    core = AppCore(plugins_dir=tmp_path / "none", log_dir=tmp_path / "logs")
    result = await core.bus.dispatch("system", "frobnicate", {})
    assert not result.ok
    assert "unknown action" in result.error.message


async def test_state_patch_seam_logging(tmp_path: Path) -> None:
    """SPEC 14.1: every tree patch is logged (category state, level trace) —
    including the well-known node adds at construction time."""
    core = AppCore(plugins_dir=tmp_path / "none", log_dir=tmp_path / "logs")
    adds = [e for e in core.logs.history(LogFilter(categories=["state"]))]
    assert len(adds) >= 6  # root, system, plugins, jobs, vehicle, vehicles
    assert all(e["level"] == "trace" for e in adds)
    assert any("state add root" in e["message"] for e in adds)

    core.tree.update("system", {"status": "running", "progress": 0.5})
    last = core.logs.history(LogFilter(categories=["state"]))[-1]
    assert last["nodeId"] == "system"
    assert last["level"] == "trace"
    # compact diff of the changed keys, with values
    assert "status=" in last["message"] and "running" in last["message"]
    assert "progress=" in last["message"] and "0.5" in last["message"]
    assert last["data"]["op"]["op"] == "update"

    core.tree.merge_props("system", {"note": "hi"})
    last = core.logs.history(LogFilter(categories=["state"]))[-1]
    assert "merge_props" in last["message"] and "note=" in last["message"] and "hi" in last["message"]

    core.tree.remove("jobs")
    last = core.logs.history(LogFilter(categories=["state"]))[-1]
    assert last["message"] == "state remove jobs (and descendants)"
    # the remove event itself still matches a subtree filter on the removed
    # node (ancestry resolved at emit time)
    assert core.logs.history(LogFilter(nodeId="jobs", categories=["state"]))[-1] == last
    core.logs.close()


async def test_lifecycle_events_carry_values_and_close_the_session_file(tmp_path: Path) -> None:
    """Messages are never bare labels (SPEC 14.1): start/stop carry version,
    uptime, event count and the session file; the stop event is the last
    line of the session file (flushed before close)."""
    core = AppCore(plugins_dir=tmp_path / "none", log_dir=tmp_path / "logs")
    await core.start()
    await core.stop()

    system = core.logs.history(LogFilter(categories=["system"]))
    assert system[0]["message"].startswith("session started: version=")
    assert system[0]["data"]["sessionFile"] == str(core.logs.session_path)
    stopped = system[-1]
    assert stopped["message"].startswith("core stopped after ")
    assert "uptime" in stopped["message"] and "events logged" in stopped["message"]
    assert "DISABLED" not in stopped["message"]
    assert stopped["nodeId"] == "system"
    assert stopped["data"]["uptimeSeconds"] >= 0.0
    assert stopped["data"]["events"] == stopped["n"] - 1  # everything before it
    assert stopped["data"]["sessionFile"] == str(core.logs.session_path)
    assert stopped["data"]["sessionFileError"] is None
    # a deliberate stop is not a sink failure
    assert core.logs.sink_error is None
    assert core.tree.get("system").status == "idle"

    lines = core.logs.session_path.read_text(encoding="utf-8").splitlines()
    assert json.loads(lines[-1])["n"] == stopped["n"]
    assert len(lines) == stopped["n"]  # complete: one line per event
