"""Plugin host supervision against a scripted fake plugin process (SPEC 6)."""

from __future__ import annotations

import asyncio
from pathlib import Path

import pytest

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
    host = make_host(tree, hub)
    await host.start()
    try:
        await wait_healthy(host, tree)
        root_children_before = list(tree.get("root").children)
        res = await host.handle_command(tree.get(CHILD_ID), "illegal_add", {})
        assert res.ok  # the plugin thinks it worked...
        await asyncio.sleep(0.2)  # give the notification time to arrive
        assert tree.get("evil") is None  # ...but the host dropped it
        assert tree.get("root").children == root_children_before
    finally:
        await host.stop()


async def test_bad_notification_params_do_not_kill_reader(tree: StateTree, hub: TelemetryHub) -> None:
    """A JSON-RPC notification with positional (array) params must be dropped,
    not kill the stdout reader (which would fabricate a heartbeat death)."""
    host = make_host(tree, hub)
    await host.start()
    try:
        await wait_healthy(host, tree)
        res = await host.handle_command(tree.get(CHILD_ID), "bad_params", {})
        assert res.ok
        # the reader survived: a follow-up command still round-trips
        res = await host.handle_command(tree.get(CHILD_ID), "echo", {"still": "alive"})
        assert res.ok
        assert status(tree, NODE_ID) == "idle"  # no fake "plugin process died"
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
