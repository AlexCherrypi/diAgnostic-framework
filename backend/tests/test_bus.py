"""Command routing by node owner (SPEC 4)."""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from diagcore.bus import CommandBus
from diagcore.loghub import LogFilter, LogHub
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


# -- command seam logging (SPEC 14.1) ----------------------------------------


async def test_cmd_seam_logs_received_command_and_result() -> None:
    tree = make_base_tree()
    tree.add(Node(id="corenode", type="system", name="c", status="idle", owner="core", parentId="root"))
    logs = LogHub(tree)
    bus = CommandBus(tree, loghub=logs)

    async def handler(node, action, params):
        return CmdResult(ok=True, data={"answer": 42})

    bus.register("core", handler)
    await bus.dispatch("corenode", "poke", {"a": 1})

    events = logs.history(LogFilter(categories=["cmd"]))
    assert len(events) == 2  # received + result
    received, finished = events
    assert received["level"] == "info"
    assert received["nodeId"] == "corenode"  # subtree filtering targets the node
    # message carries ids + values, never just labels: target, action, params
    assert "'poke'" in received["message"] and "'corenode'" in received["message"]
    assert '"a":1' in received["message"]
    assert received["data"] == {"target": "corenode", "action": "poke", "params": {"a": 1}}
    assert finished["data"]["result"] == {"ok": True, "data": {"answer": 42}, "error": None}
    assert "ok" in finished["message"] and "42" in finished["message"]


async def test_cmd_seam_logs_failed_dispatch() -> None:
    tree = make_base_tree()
    logs = LogHub(tree)
    bus = CommandBus(tree, loghub=logs)

    result = await bus.dispatch("does-not-exist", "x", {})
    assert not result.ok
    events = logs.history(LogFilter(categories=["cmd"]))
    assert len(events) == 2  # even a failing dispatch logs received + result
    assert "failed" in events[1]["message"]
    assert "unknown target" in events[1]["message"]
    assert events[1]["data"]["result"]["ok"] is False


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


@pytest.mark.parametrize("params", [[1, 2], "abc", 5, True])
async def test_non_dict_params_are_rejected_not_raised(params: Any) -> None:
    """The bus never raises into the caller: malformed params yield ok:false
    and BOTH cmd audit events (SPEC 14.1: every received command)."""
    tree = make_base_tree()
    tree.add(Node(id="corenode", type="system", name="c", status="idle", owner="core", parentId="root"))
    logs = LogHub(tree)
    bus = CommandBus(tree, loghub=logs)
    calls: list[Any] = []

    async def handler(node, action, params):
        calls.append(params)
        return CmdResult(ok=True)

    bus.register("core", handler)
    result = await bus.dispatch("corenode", "poke", params)
    assert not result.ok
    assert "invalid params" in result.error.message
    assert type(params).__name__ in result.error.message
    assert calls == []  # the handler never sees garbage
    events = logs.history(LogFilter(categories=["cmd"]))
    assert len(events) == 2
    assert events[0]["data"]["params"] == params  # the raw value is audited
    assert "invalid params" in events[1]["message"]


async def test_cmd_events_carry_correlation_ids_and_pair_up_out_of_order() -> None:
    """cmdId + client are stamped into the received AND the result event, so
    two concurrent identical commands finishing in reverse order still pair."""
    tree = make_base_tree()
    tree.add(Node(id="dev", type="device.sim", name="d", status="idle", owner="core", parentId="root"))
    logs = LogHub(tree)
    bus = CommandBus(tree, loghub=logs)
    gates = {"c1": asyncio.Event(), "c2": asyncio.Event()}

    async def handler(node, action, params):
        await gates[params["who"]].wait()
        return CmdResult(ok=True, data={"who": params["who"]})

    bus.register("core", handler)
    t1 = asyncio.create_task(bus.dispatch("dev", "cancel", {"who": "c1"}, cmd_id="c1", client="127.0.0.1:1"))
    t2 = asyncio.create_task(bus.dispatch("dev", "cancel", {"who": "c2"}, cmd_id="c2", client="127.0.0.1:2"))
    await asyncio.sleep(0.02)
    gates["c2"].set()  # second command completes first
    await t2
    gates["c1"].set()
    await t1

    events = logs.history(LogFilter(categories=["cmd"]))
    assert len(events) == 4
    received = [e for e in events if "params" in e["data"]]
    results = [e for e in events if "result" in e["data"]]
    assert [e["data"]["cmdId"] for e in received] == ["c1", "c2"]
    assert [e["data"]["cmdId"] for e in results] == ["c2", "c1"]  # reversed completion, still paired
    for event in results:
        assert event["data"]["result"]["data"]["who"] == event["data"]["cmdId"]
    assert received[0]["data"]["client"] == "127.0.0.1:1"
    assert results[0]["data"]["client"] == "127.0.0.1:2"
    # searchable in the message text too (the /ws/logs `search` filter)
    assert "[c1 from 127.0.0.1:1]" in received[0]["message"]
    assert len(logs.history(LogFilter(categories=["cmd"], search="[c2 from"))) == 2
    # without correlation info the data shape stays exactly as before
    await bus.dispatch("dev", "cancel", {"who": "c1"})
    plain = logs.history(LogFilter(categories=["cmd"]))[-2]
    assert set(plain["data"]) == {"target", "action", "params"}
