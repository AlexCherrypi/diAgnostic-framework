"""The state tree: flat node storage, sequence numbering and patch broadcast.

Implements SPEC 3.3: every mutation increments the global ``seq`` and is
broadcast to subscribers as exactly one patch op (``add`` / ``update`` /
``merge_props`` / ``remove``).
"""

from __future__ import annotations

import copy
import logging
from typing import Any, Callable, Mapping

from .model import Node

logger = logging.getLogger(__name__)

#: Callback signature: (seq, op) where ``op`` is a JSON-ready patch op dict.
PatchCallback = Callable[[int, dict[str, Any]], None]

#: Top-level node keys that ``update`` may replace. ``id``, ``parentId`` and
#: ``children`` are maintained by the tree itself and must not be patched
#: directly (that would desync the parent/children bookkeeping).
_UPDATABLE_KEYS: frozenset[str] = frozenset(
    {"type", "name", "status", "error", "progress", "props", "actions", "owner"}
)


class StateTree:
    """Flat store ``{rootId, nodes}`` with seq-numbered patch broadcast."""

    def __init__(self) -> None:
        self._nodes: dict[str, Node] = {}
        self.root_id: str | None = None
        self.seq: int = 0
        self._subscribers: list[PatchCallback] = []

    # -- read access -------------------------------------------------------

    @property
    def nodes(self) -> Mapping[str, Node]:
        return self._nodes

    def get(self, node_id: str | None) -> Node | None:
        if node_id is None:
            return None
        return self._nodes.get(node_id)

    def __contains__(self, node_id: str) -> bool:
        return node_id in self._nodes

    def snapshot(self) -> dict[str, Any]:
        """JSON-ready snapshot ``{rootId, nodes}`` (SPEC 3.3 / hello)."""
        return {
            "rootId": self.root_id,
            "nodes": {nid: node.model_dump(mode="json") for nid, node in self._nodes.items()},
        }

    def subtree_ids(self, node_id: str) -> list[str]:
        """The node itself plus all descendants (depth-first, parents first)."""
        result: list[str] = []
        stack = [node_id]
        while stack:
            nid = stack.pop()
            node = self._nodes.get(nid)
            if node is None:
                continue
            result.append(nid)
            # push reversed so the traversal yields children in order
            stack.extend(reversed(node.children))
        return result

    # -- subscriptions -----------------------------------------------------

    def subscribe(self, callback: PatchCallback) -> PatchCallback:
        self._subscribers.append(callback)
        return callback

    def unsubscribe(self, callback: PatchCallback) -> None:
        try:
            self._subscribers.remove(callback)
        except ValueError:
            pass

    def _emit(self, op: dict[str, Any]) -> None:
        self.seq += 1
        for callback in list(self._subscribers):
            try:
                callback(self.seq, op)
            except Exception:
                logger.exception("state tree subscriber raised; continuing")

    # -- mutations ---------------------------------------------------------

    def add(self, node: Node, index: int | None = None) -> Node:
        """Insert ``node``; append its id to the parent's children (or insert
        at ``index`` if given)."""
        if node.id in self._nodes:
            raise ValueError(f"node id already exists: {node.id!r}")
        if index is not None and index < 0:
            raise ValueError(f"negative insert index: {index}")

        stored = node.model_copy(deep=True)
        if stored.children:
            raise ValueError("a freshly added node must not carry children")

        if stored.parentId is None:
            if self.root_id is not None:
                raise ValueError("root node already exists; parentId must not be null")
            self.root_id = stored.id
        else:
            parent = self._nodes.get(stored.parentId)
            if parent is None:
                raise ValueError(f"unknown parent: {stored.parentId!r}")
            if index is None:
                parent.children.append(stored.id)
            else:
                parent.children.insert(index, stored.id)

        self._nodes[stored.id] = stored

        op: dict[str, Any] = {"op": "add", "node": stored.model_dump(mode="json")}
        if index is not None:
            op["index"] = index
        self._emit(op)
        return stored

    def update(self, node_id: str, changes: dict[str, Any]) -> Node:
        """Replace top-level node keys with the values in ``changes``.

        Per SPEC 3.3 the values REPLACE the node's keys ("props" replaces all
        props). ``id`` / ``parentId`` / ``children`` cannot be updated.
        """
        node = self._nodes.get(node_id)
        if node is None:
            raise ValueError(f"unknown node: {node_id!r}")
        bad_keys = set(changes) - _UPDATABLE_KEYS
        if bad_keys:
            raise ValueError(f"keys not updatable via 'update': {sorted(bad_keys)}")

        data = node.model_dump()
        data.update(copy.deepcopy(changes))
        new_node = Node.model_validate(data)
        self._nodes[node_id] = new_node

        dumped = new_node.model_dump(mode="json")
        normalized_changes = {key: dumped[key] for key in changes}
        self._emit({"op": "update", "id": node_id, "changes": normalized_changes})
        return new_node

    def merge_props(self, node_id: str, props: dict[str, Any]) -> Node:
        """Shallow-merge ``props`` into ``node.props`` (SPEC 3.3)."""
        node = self._nodes.get(node_id)
        if node is None:
            raise ValueError(f"unknown node: {node_id!r}")

        node.props.update(copy.deepcopy(props))

        dumped_props = node.model_dump(mode="json")["props"]
        normalized = {key: dumped_props[key] for key in props}
        self._emit({"op": "merge_props", "id": node_id, "props": normalized})
        return node

    def remove(self, node_id: str) -> None:
        """Remove the node and all descendants; detach from parent."""
        node = self._nodes.get(node_id)
        if node is None:
            raise ValueError(f"unknown node: {node_id!r}")

        for nid in self.subtree_ids(node_id):
            self._nodes.pop(nid, None)

        if node.parentId is not None:
            parent = self._nodes.get(node.parentId)
            if parent is not None and node_id in parent.children:
                parent.children.remove(node_id)
        if self.root_id == node_id:
            self.root_id = None

        self._emit({"op": "remove", "id": node_id})
