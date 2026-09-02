"""Plugin host supervision against a scripted fake plugin process (SPEC 6)."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path

import pytest

from diagcore.loghub import LogFilter, LogHub
from diagcore.plugins.host import PluginHost
from diagcore.plugins.manifest import Manifest
from diagcore.telemetry import TelemetryHub
from diagcore.tree import StateTree

from util import make_base_tree, wait_until

FAKE_DIR = Path(__file__).parent / "fixtures" / "fake_plugin"

MANIFEST = Manifest(id="fake", name="Fake plugin", version="0.0.1", entry="plugin.py")

NODE_ID = "plugin:fake"
CHILD_ID = f"{NODE_ID}:child"


def make_host(
    tree: StateTree,
    hub: TelemetryHub,
    *,
    env: dict[str, str] | None = None,
    **overrides,
) -> PluginHost:
    settings = {
        "ping_interval": 0.1,
        "ping_timeout": 0.4,
        "init_timeout": 5.0,
        "restart_backoff": (0.1, 0.1, 0.1),
        "max_restarts": 3,
    }
    settings.update(overrides)
    return PluginHost(
        tree=tree,
        hub=hub,
        manifest=MANIFEST,
        plugin_dir=FAKE_DIR,
        env=env,
        **settings,
    )


@pytest.fixture
def tree() -> StateTree:
    return make_base_tree()


@pytest.fixture
def hub() -> TelemetryHub:
    return TelemetryHub()


def status(tree: StateTree, node_id: str) -> str | None:
    node = tree.get(node_id)
    return node.status if node else None


async def wait_healthy(host: PluginHost, tree: StateTree, timeout: float = 10.0) -> None:
    await wait_until(
        lambda: status(tree, NODE_ID) == "idle" and tree.get(CHILD_ID) is not None,
        timeout=timeout,
        message="plugin idle with declared child",
    )


async def test_initialize_declare_and_owner_stamping(tree: StateTree, hub: TelemetryHub) -> None:
    host = make_host(tree, hub)
    await host.start()
    try:
        await wait_healthy(host, tree)
        plugin_node = tree.get(NODE_ID)
        assert plugin_node.owner == "core"  # supervisor owns the plugin node
        assert plugin_node.parentId == "plugins"
        assert [a.name for a in plugin_node.actions] == ["restart"]
        child = tree.get(CHILD_ID)
        # the fake plugin claimed owner "core"; the host must stamp plugin:fake
        assert child.owner == "plugin:fake"
        assert child.parentId == NODE_ID
    finally:
        await host.stop()


async def test_command_roundtrip_and_error_result(tree: StateTree, hub: TelemetryHub) -> None:
    host = make_host(tree, hub)
    await host.start()
    try:
        await wait_healthy(host, tree)
        child = tree.get(CHILD_ID)
        res = await host.handle_command(child, "echo", {"x": 42})
        assert res.ok
        assert res.data == {"echo": {"x": 42}}

        res = await host.handle_command(child, "boom", {})
        assert not res.ok
        assert res.error.message == "it went boom"

        res = await host.handle_command(child, "no_such_action", {})
        assert not res.ok
        assert "unknown action" in res.error.message
    finally:
        await host.stop()


async def test_crash_marks_subtree_error_then_restarts(tree: StateTree, hub: TelemetryHub) -> None:
    host = make_host(tree, hub)
    await host.start()
    try:
        await wait_healthy(host, tree)
        child = tree.get(CHILD_ID)
        res = await host.handle_command(child, "die", {})
        assert not res.ok  # process died before responding

        # subtree => error with the spec'd message
        await wait_until(
            lambda: status(tree, NODE_ID) == "error" and status(tree, CHILD_ID) == "error",
            message="plugin subtree error",
        )
        assert tree.get(NODE_ID).error.message == "plugin process died"
        assert tree.get(CHILD_ID).error.message == "plugin process died"

        # automatic restart: old subtree removed, re-declared, idle again
        await wait_healthy(host, tree)
        assert tree.get(NODE_ID).children == [CHILD_ID]
        assert status(tree, CHILD_ID) == "idle"

        # and it still answers commands
        res = await host.handle_command(tree.get(CHILD_ID), "echo", {"back": True})
        assert res.ok
    finally:
        await host.stop()


async def test_heartbeat_timeout_triggers_restart(tree: StateTree, hub: TelemetryHub) -> None:
    host = make_host(tree, hub)
    await host.start()
    try:
        await wait_healthy(host, tree)
        res = await host.handle_command(tree.get(CHILD_ID), "freeze", {})
        assert res.ok
        await wait_until(
            lambda: status(tree, NODE_ID) == "error",
            timeout=5.0,
            message="heartbeat timeout => error",
        )
        assert "heartbeat timeout" in tree.get(NODE_ID).error.detail
        await wait_healthy(host, tree)  # restarted, unfrozen
    finally:
        await host.stop()


async def test_max_restarts_then_manual_restart(tree: StateTree, hub: TelemetryHub, tmp_path: Path) -> None:
    flag = tmp_path / "die.flag"
    flag.write_text("die", encoding="utf-8")
    host = make_host(tree, hub, env={"FAKE_EXIT_IF_FLAG": str(flag)}, init_timeout=3.0)
    await host.start()
    try:
        # 1 initial attempt + 3 restarts all fail => stays error
        await wait_until(
            lambda: host.restart_attempts > 3 and status(tree, NODE_ID) == "error",
            timeout=20.0,
            message="restart attempts exhausted",
        )
        node = tree.get(NODE_ID)
        assert node.error.message == "plugin process died"
        assert [a.name for a in node.actions] == ["restart"]  # manual restart offered

        # commands are rejected while dead
        res = await host.handle_command(node, "echo", {})
        assert not res.ok

        # manual restart after the cause is fixed
        flag.unlink()
        await host.restart()
        await wait_healthy(host, tree)
    finally:
        await host.stop()


async def test_illegal_mutation_outside_subtree_is_dropped(tree: StateTree, hub: TelemetryHub) -> None:
    logs = LogHub(tree)
    host = make_host(tree, hub, loghub=logs)
    await host.start()
    try:
        await wait_healthy(host, tree)
        root_children_before = list(tree.get("root").children)
        res = await host.handle_command(tree.get(CHILD_ID), "illegal_add", {})
        assert res.ok  # the plugin thinks it worked...
        await asyncio.sleep(0.2)  # give the notification time to arrive
        assert tree.get("evil") is None  # ...but the host dropped it
        assert tree.get("root").children == root_children_before
        # ...and audited the violation (SPEC 14: nothing happens without a
        # log): a warn-level plugin event carrying ids and the full params
        dropped = logs.history(LogFilter(categories=["plugin"], levels=["warn"], search="dropped"))
        assert len(dropped) == 1
        message = dropped[0]["message"]
        assert "'add'" in message and "'root'" in message and "'evil'" in message
        assert dropped[0]["source"] == "plugin:fake"
        assert dropped[0]["data"]["method"] == "add"
        assert dropped[0]["data"]["params"]["node"]["id"] == "evil"
    finally:
        await host.stop()


async def test_bad_notification_params_do_not_kill_reader(tree: StateTree, hub: TelemetryHub) -> None:
    """A JSON-RPC notification with positional (array) params must be dropped,
    not kill the stdout reader (which would fabricate a heartbeat death)."""
    logs = LogHub(tree)
    host = make_host(tree, hub, loghub=logs)
    await host.start()
    try:
        await wait_healthy(host, tree)
        res = await host.handle_command(tree.get(CHILD_ID), "bad_params", {})
        assert res.ok
        # the reader survived: a follow-up command still round-trips
        res = await host.handle_command(tree.get(CHILD_ID), "echo", {"still": "alive"})
        assert res.ok
        assert status(tree, NODE_ID) == "idle"  # no fake "plugin process died"
        dropped = logs.history(LogFilter(categories=["plugin"], levels=["warn"], search="dropped"))
        assert len(dropped) == 1
        assert "'log'" in dropped[0]["message"] and "positional" in dropped[0]["message"]
        assert dropped[0]["data"]["params"] == ["positional", "params"]
    finally:
        await host.stop()


async def test_garbage_stdout_is_audited_and_dropped(tree: StateTree, hub: TelemetryHub) -> None:
    """A stray print() in a plugin (stdout is the protocol channel, SPEC 6)
    must be dropped without killing the reader — and must be visible in the
    audit trail as warn-level plugin events carrying the offending lines."""
    logs = LogHub(tree)
    host = make_host(tree, hub, loghub=logs)
    await host.start()
    try:
        await wait_healthy(host, tree)
        res = await host.handle_command(tree.get(CHILD_ID), "garbage_stdout", {})
        assert res.ok  # the response after the garbage lines still arrived
        warns = logs.history(LogFilter(categories=["plugin"], levels=["warn"]))
        assert any("not JSON" in e["message"] and "this is not json" in e["message"] for e in warns)
        assert any("not a JSON object" in e["message"] and "[1, 2, 3]" in e["message"] for e in warns)
        assert all(e["source"] == "plugin:fake" for e in warns)
        assert status(tree, NODE_ID) == "idle"
    finally:
        await host.stop()


async def test_failed_respawn_keeps_error_subtree_visible(tree: StateTree, hub: TelemetryHub) -> None:
    """SPEC 6: the old subtree is removed on SUCCESSFUL restart only. When
    every respawn fails, the error-marked child nodes must stay visible."""
    host = make_host(tree, hub, restart_backoff=(0.1,), max_restarts=1)
    await host.start()
    try:
        await wait_healthy(host, tree)
        # make every subsequent spawn fail, then crash the plugin
        host._python_exe = str(FAKE_DIR / "no_such_python_dir" / "python.exe")
        await host.handle_command(tree.get(CHILD_ID), "die", {})
        await wait_until(
            lambda: host.restart_attempts > 1 and status(tree, NODE_ID) == "error",
            timeout=10.0,
            message="restart attempts exhausted on spawn failure",
        )
        child = tree.get(CHILD_ID)
        assert child is not None, "error subtree was wiped by a failed restart"
        assert child.status == "error"
        assert child.error.message == "plugin process died"
    finally:
        await host.stop()


async def test_stop_is_graceful(tree: StateTree, hub: TelemetryHub) -> None:
    host = make_host(tree, hub)
    await host.start()
    await wait_healthy(host, tree)
    await host.stop()
    # no error status smeared onto the tree by a graceful stop
    assert status(tree, NODE_ID) == "idle"


# -- plugin seam logging (SPEC 14.1) -----------------------------------------


async def test_plugin_seam_logs_lifecycle_rpc_and_stderr(tree: StateTree, hub: TelemetryHub) -> None:
    logs = LogHub(tree)
    host = make_host(tree, hub, loghub=logs)
    await host.start()
    try:
        await wait_healthy(host, tree)

        def plugin_messages(**kw) -> list[str]:
            return [e["message"] for e in logs.history(LogFilter(categories=["plugin"], **kw))]

        # lifecycle: spawn (with pid) and successful initialize
        assert any("spawned pid=" in m for m in plugin_messages())
        assert any(f"plugin fake initialized (node {NODE_ID})" in m for m in plugin_messages())
        # every RPC line in/out at trace level
        trace = plugin_messages(levels=["trace"])
        assert any(m.startswith("rpc tx [fake] initialize:") for m in trace)
        assert any("rpc rx [fake]:" in m and '"result"' in m for m in trace)
        # heartbeat pings are RPC lines too — the heartbeat evidence
        await wait_until(
            lambda: any(m.startswith("rpc tx [fake] ping:") for m in plugin_messages(levels=["trace"])),
            message="ping rpc line logged",
        )
        # stderr lines land in the hub (source = the plugin)
        await wait_until(
            lambda: any("stderr: fake plugin ready" in m for m in plugin_messages()),
            message="stderr line logged",
        )
        stderr_event = next(
            e
            for e in logs.history(LogFilter(categories=["plugin"]))
            if "stderr: fake plugin ready" in e["message"]
        )
        assert stderr_event["source"] == "plugin:fake"
        # `log` notifications are forwarded with their level
        assert any(m == "plugin fake: fake plugin initialized" for m in plugin_messages())
    finally:
        await host.stop()


async def test_plugin_seam_logs_crash_exit_code_and_restart(tree: StateTree, hub: TelemetryHub) -> None:
    logs = LogHub(tree)
    host = make_host(tree, hub, loghub=logs)
    await host.start()
    try:
        await wait_healthy(host, tree)
        await host.handle_command(tree.get(CHILD_ID), "die", {})

        def plugin_messages(**kw) -> list[str]:
            return [e["message"] for e in logs.history(LogFilter(categories=["plugin"], **kw))]

        await wait_until(
            lambda: any(
                "plugin fake died" in m and "exited with code 1" in m
                for m in plugin_messages(levels=["error"])
            ),
            message="death with exit code logged",
        )
        await wait_until(
            lambda: any("restart attempt 1/3" in m for m in plugin_messages(levels=["warn"])),
            message="restart attempt logged",
        )
        await wait_healthy(host, tree)  # restarted and re-declared
    finally:
        await host.stop()


# -- io notifications (SPEC 14.2) --------------------------------------------


async def test_io_notification_feeds_loghub(tree: StateTree, hub: TelemetryHub) -> None:
    logs = LogHub(tree)
    host = make_host(tree, hub, loghub=logs)
    await host.start()
    try:
        await wait_healthy(host, tree)
        res = await host.handle_command(tree.get(CHILD_ID), "io_good", {})
        assert res.ok
        # the notifications precede the command response on stdout, so they
        # are fully processed by now — no waiting needed
        events = logs.history(LogFilter(categories=["io"]))
        assert len(events) == 2
        tx, rx = events
        assert tx["level"] == "info"
        assert tx["source"] == "plugin:fake"
        assert tx["nodeId"] == CHILD_ID
        assert tx["raw"] == "22F190"
        assert tx["data"]["direction"] == "tx"
        assert tx["data"]["decoded"] == {"service": "ReadDataByIdentifier", "did": "0xF190"}
        assert tx["data"]["note"] == "vin request"
        assert CHILD_ID in tx["message"] and "22F190" in tx["message"]
        assert rx["raw"] == "62F190564643"
        assert rx["data"]["direction"] == "rx"
        # subtree filtering: the io events match a filter on the plugin node
        assert len(logs.history(LogFilter(nodeId=NODE_ID, categories=["io"]))) == 2
        # io lines are NOT double-logged as raw rpc rx trace lines
        assert not any(
            '"method": "io"' in e["message"]
            for e in logs.history(LogFilter(categories=["plugin"], levels=["trace"]))
        )
    finally:
        await host.stop()


async def test_io_raw_must_be_whole_byte_hex(tree: StateTree, hub: TelemetryHub) -> None:
    """SPEC 14.2: `raw` is the frame as hex of whole bytes so the trail replays
    byte-exact; '0x' prefixes and odd lengths are dropped (audited), while
    whitespace between bytes is tolerated and stripped."""
    logs = LogHub(tree)
    host = make_host(tree, hub, loghub=logs)
    await host.start()
    try:
        await wait_healthy(host, tree)
        for action in ("io_nonhex", "io_oddlen", "io_spaced"):
            res = await host.handle_command(tree.get(CHILD_ID), action, {})
            assert res.ok
        io_events = logs.history(LogFilter(categories=["io"]))
        assert [e["raw"] for e in io_events] == ["22F442"]  # only the spaced one, normalized
        assert bytes.fromhex(io_events[0]["raw"]) == b"\x22\xf4\x42"
        assert "raw=22F442" in io_events[0]["message"]
        dropped = logs.history(LogFilter(categories=["plugin"], levels=["warn"], search="'io' dropped"))
        assert len(dropped) == 2
        assert all("ValueError" in e["message"] and "even-length hex" in e["message"] for e in dropped)
        assert dropped[0]["data"]["params"]["raw"] == "0x22F442"
        assert "'0x22F442'" in dropped[0]["message"]
        assert dropped[1]["data"]["params"]["raw"] == "22F44"
        assert status(tree, NODE_ID) == "idle"  # the reader survived
    finally:
        await host.stop()


async def test_io_outside_subtree_or_invalid_is_dropped(tree: StateTree, hub: TelemetryHub) -> None:
    logs = LogHub(tree)
    host = make_host(tree, hub, loghub=logs)
    await host.start()
    try:
        await wait_healthy(host, tree)
        res = await host.handle_command(tree.get(CHILD_ID), "io_evil", {})
        assert res.ok  # the plugin thinks it worked...
        res = await host.handle_command(tree.get(CHILD_ID), "io_bad", {})
        assert res.ok
        # ...but the host dropped both: no io event ever reached the hub, and
        # the reader survived (the follow-up command above round-tripped).
        assert logs.history(LogFilter(categories=["io"])) == []
        assert status(tree, NODE_ID) == "idle"
        # both drops are audited as warn-level plugin events (with the reason,
        # the offending node id / direction and the full params)
        dropped = logs.history(LogFilter(categories=["plugin"], levels=["warn"], search="'io' dropped"))
        assert len(dropped) == 2
        outside, invalid = dropped
        assert "outside the plugin subtree" in outside["message"] and "'root'" in outside["message"]
        assert outside["data"]["params"]["raw"] == "DEADBEEF"
        assert "ValueError" in invalid["message"] and "'sideways'" in invalid["message"]
        assert invalid["data"]["params"]["nodeId"] == CHILD_ID
    finally:
        await host.stop()


# -- plugin death ends its running jobs with `job` error events (SPEC 14.1) ---


async def test_plugin_death_ends_running_jobs_with_error_events(tree: StateTree, hub: TelemetryHub) -> None:
    """A plugin that dies sends no notification for its running jobs; the host
    must derive the terminal `job` error events itself, so a job-subtree log
    view does not show the job running forever. Already-terminal jobs are
    flipped to error (SPEC 6) but not reported as fresh failures."""
    logs = LogHub(tree)
    host = make_host(tree, hub, loghub=logs)
    job_id = f"{CHILD_ID}:job"
    step_id = f"{job_id}:step:1"
    done_id = f"{CHILD_ID}:job-done"
    await host.start()
    try:
        await wait_healthy(host, tree)
        res = await host.handle_command(tree.get(CHILD_ID), "job_running", {})
        assert res.ok and res.data == {"jobId": job_id}
        assert status(tree, job_id) == "running" and status(tree, step_id) == "running"
        assert job_id in host._job_milestones
        await host.handle_command(tree.get(CHILD_ID), "die", {})
        await wait_until(lambda: status(tree, job_id) == "error", message="job marked error after death")

        assert tree.get(job_id).error.message == "plugin process died"
        assert status(tree, step_id) == "error" and status(tree, done_id) == "error"
        # terminal job events for the job AND its running step, in the job's subtree
        failures = logs.history(LogFilter(nodeId=job_id, categories=["job"], levels=["error"]))
        assert [e["nodeId"] for e in failures] == [job_id, step_id]
        assert failures[0]["message"].startswith(f"job {job_id} 'Fake job' failed: plugin process died (")
        assert "exited with code 1" in failures[0]["message"]
        assert failures[1]["message"].startswith(f"job step {step_id} 'Fake step' failed: plugin process died")
        for event in failures:
            assert event["source"] == "plugin:fake"
            assert event["data"]["error"]["message"] == "plugin process died"
            assert event["data"]["previousStatus"] == "running"
        # the already-done job is not reported as a fresh failure
        assert logs.history(LogFilter(nodeId=done_id, categories=["job"], levels=["error"])) == []
        # the plugin-side death event comes first, then the job events
        died = logs.history(LogFilter(categories=["plugin"], levels=["error"], search="died"))
        assert died and died[0]["n"] < failures[0]["n"]
        assert job_id not in host._job_milestones
    finally:
        await host.stop()


# -- lone surrogates from a plugin never reach tree/log/sockets unescaped ------


async def test_lone_surrogates_from_plugin_are_escaped_and_audited(tree: StateTree, hub: TelemetryHub) -> None:
    logs = LogHub(tree)
    host = make_host(tree, hub, loghub=logs)
    await host.start()
    try:
        await wait_healthy(host, tree)
        res = await host.handle_command(tree.get(CHILD_ID), "surrogate_line", {})
        assert res.ok
        # the node rename landed, escaped (byte value visible), and the whole
        # snapshot stays encodable => /ws/state hello cannot be poisoned
        assert tree.get(CHILD_ID).name == "bad " + "\\" + "udcff name"
        json.dumps(tree.snapshot(), ensure_ascii=False).encode("utf-8")
        # the plugin log message landed, escaped
        assert any(
            e["message"] == "plugin fake: poison " + "\\" + "ud800 text"
            for e in logs.history(LogFilter(categories=["plugin"]))
        )
        # ...and both lines were audited as warn events carrying the raw line
        warns = logs.history(LogFilter(categories=["plugin"], levels=["warn"], search="lone UTF-16 surrogates"))
        assert len(warns) == 2
        assert all(e["source"] == "plugin:fake" and "\\" + "ud" in e["data"]["line"].lower() for e in warns)
        for event in logs.history():
            json.dumps(event, ensure_ascii=False).encode("utf-8")
        assert status(tree, NODE_ID) == "idle"
    finally:
        await host.stop()
