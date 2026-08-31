"""Shared helpers for the test suite."""

from __future__ import annotations

import asyncio
from typing import Any, Callable

from diagcore.model import Node
from diagcore.tree import StateTree


async def wait_until(
    predicate: Callable[[], Any],
    timeout: float = 5.0,
    interval: float = 0.01,
    message: str = "condition",
) -> Any:
    """Poll ``predicate`` until it returns a truthy value or ``timeout``."""
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout
    while True:
        value = predicate()
        if value:
            return value
        if loop.time() > deadline:
            raise AssertionError(f"timed out after {timeout}s waiting for {message}")
        await asyncio.sleep(interval)


def make_base_tree() -> StateTree:
    """A tree with the well-known root/plugins/jobs skeleton."""
    tree = StateTree()
    tree.add(Node(id="root", type="root", name="root", status="idle", owner="core"))
    tree.add(Node(id="plugins", type="plugins", name="Plugins", status="idle", owner="core", parentId="root"))
    tree.add(Node(id="jobs", type="jobs", name="Jobs", status="idle", owner="core", parentId="root"))
    return tree


def apply_ops(state: dict[str, Any], ops: list[dict[str, Any]]) -> None:
    """Apply patch ops to a mirror ``{"rootId":..., "nodes": {...}}`` exactly
    as a frontend mirror store would (SPEC 3.3)."""
    nodes: dict[str, dict[str, Any]] = state["nodes"]
    for op in ops:
        kind = op["op"]
        if kind == "add":
            node = dict(op["node"])
            nodes[node["id"]] = node
            parent_id = node.get("parentId")
            if parent_id is None:
                state["rootId"] = node["id"]
            else:
                children = nodes[parent_id].setdefault("children", [])
                index = op.get("index")
                if index is None:
                    children.append(node["id"])
                else:
                    children.insert(index, node["id"])
        elif kind == "update":
            nodes[op["id"]].update(op["changes"])
        elif kind == "merge_props":
            target = nodes[op["id"]]
            target["props"] = {**target.get("props", {}), **op["props"]}
        elif kind == "remove":
            node_id = op["id"]
            stack = [node_id]
            doomed = []
            while stack:
                nid = stack.pop()
                node = nodes.get(nid)
                if node is None:
                    continue
                doomed.append(nid)
                stack.extend(node.get("children", []))
            removed = nodes.get(node_id)
            if removed is not None and removed.get("parentId") in nodes:
                parent_children = nodes[removed["parentId"]].get("children", [])
                if node_id in parent_children:
                    parent_children.remove(node_id)
            for nid in doomed:
                nodes.pop(nid, None)
            if state.get("rootId") == node_id:
                state["rootId"] = None
        else:
            raise AssertionError(f"unknown op: {kind!r}")
