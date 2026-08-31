"""Command routing by node owner (SPEC 4)."""

from __future__ import annotations

from typing import Any

import pytest

from diagcore.bus import CommandBus
from diagcore.model import CmdResult, Node

from util import make_base_tree


@pytest.fixture
def setup() -> tuple[Any, ...]:
    tree = make_base_tree()
    tree.add(Node(id="corenode", type="system", name="c", status="idle", owner="core", parentId="root"))
    tree.add(Node(id="pnode", type="device.sim", name="p", status="idle", owner="plugin:x", parentId="root"))
    bus = CommandBus(tree)
    return tree, bus


async def test_routes_to_owner_handler(setup) -> None:
    tree, bus = setup
    calls: list[tuple[str, str, dict]] = []

    async def core_handler(node, action, params):
        calls.append((node.id, action, params))
        return CmdResult(ok=True, data={"handled": "core"})

    async def plugin_handler(node, action, params):
        calls.append((node.id, action, params))
        return CmdResult(ok=True, data={"handled": "plugin"})

    bus.register("core", core_handler)
    bus.register("plugin:x", plugin_handler)

    res1 = await bus.dispatch("corenode", "poke", {"a": 1})
    res2 = await bus.dispatch("pnode", "selftest", {})
    assert res1.ok and res1.data == {"handled": "core"}
    assert res2.ok and res2.data == {"handled": "plugin"}
    assert calls == [("corenode", "poke", {"a": 1}), ("pnode", "selftest", {})]


async def test_unknown_target(setup) -> None:
    tree, bus = setup
    res = await bus.dispatch("does-not-exist", "x", {})
    assert not res.ok
    assert "unknown target" in res.error.message


async def test_missing_target_and_action(setup) -> None:
    tree, bus = setup
    res = await bus.dispatch(None, "x", {})
    assert not res.ok and "missing command target" in res.error.message
    res = await bus.dispatch("corenode", None, {})
    assert not res.ok and "missing command action" in res.error.message


async def test_no_handler_for_owner(setup) -> None:
    tree, bus = setup
    res = await bus.dispatch("pnode", "selftest", {})
    assert not res.ok
    assert "no command handler" in res.error.message
    assert "plugin:x" in res.error.message


async def test_handler_exception_becomes_ok_false(setup) -> None:
    tree, bus = setup

    async def bad_handler(node, action, params):
        raise RuntimeError("kapow")

    bus.register("core", bad_handler)
    res = await bus.dispatch("corenode", "poke", {})
    assert not res.ok
    assert "kapow" in res.error.message
    assert "RuntimeError" in (res.error.detail or "")


async def test_params_default_to_empty_dict(setup) -> None:
    tree, bus = setup
    seen: dict[str, Any] = {}

    async def handler(node, action, params):
        seen["params"] = params
        return CmdResult(ok=True)

    bus.register("core", handler)
    res = await bus.dispatch("corenode", "poke", None)
    assert res.ok
    assert seen["params"] == {}
