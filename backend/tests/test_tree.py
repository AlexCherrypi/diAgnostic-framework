"""State tree patch semantics (SPEC 3.3)."""

from __future__ import annotations

import pytest

from diagcore.model import Node
from diagcore.tree import StateTree


def make_node(node_id: str, parent: str | None = None, **kwargs) -> Node:
    defaults = {"type": "device.sim", "name": node_id, "status": "idle", "owner": "core"}
    defaults.update(kwargs)
    return Node(id=node_id, parentId=parent, **defaults)


@pytest.fixture
def tree() -> StateTree:
    t = StateTree()
    t.add(make_node("root", None, type="root"))
    return t


class Recorder:
    def __init__(self, tree: StateTree) -> None:
        self.events: list[tuple[int, dict]] = []
        tree.subscribe(lambda seq, op: self.events.append((seq, op)))


# -- add --------------------------------------------------------------------


def test_add_sets_root_and_children(tree: StateTree) -> None:
    tree.add(make_node("a", "root"))
    tree.add(make_node("b", "root"))
    tree.add(make_node("a1", "a"))
    assert tree.root_id == "root"
    assert tree.get("root").children == ["a", "b"]
    assert tree.get("a").children == ["a1"]
    assert tree.get("a1").parentId == "a"


def test_add_with_index_inserts(tree: StateTree) -> None:
    tree.add(make_node("a", "root"))
    tree.add(make_node("b", "root"))
    tree.add(make_node("c", "root"), index=0)
    assert tree.get("root").children == ["c", "a", "b"]


def test_add_duplicate_id_raises(tree: StateTree) -> None:
    tree.add(make_node("a", "root"))
    with pytest.raises(ValueError, match="already exists"):
        tree.add(make_node("a", "root"))


def test_add_unknown_parent_raises(tree: StateTree) -> None:
    with pytest.raises(ValueError, match="unknown parent"):
        tree.add(make_node("a", "nope"))


def test_is_within_matches_subtree_ids_membership(tree: StateTree) -> None:
    tree.add(make_node("a", "root"))
    tree.add(make_node("a1", "a"))
    tree.add(make_node("a1x", "a1"))
    tree.add(make_node("b", "root"))
    for node_id in ("a", "a1", "a1x"):
        assert tree.is_within(node_id, "a")
        assert node_id in tree.subtree_ids("a")
    assert not tree.is_within("b", "a") and "b" not in tree.subtree_ids("a")
    assert not tree.is_within("root", "a")
    assert tree.is_within("a1x", "root")
    assert not tree.is_within(None, "a")
    assert not tree.is_within("ghost", "a")
    assert not tree.is_within("a", "ghost")  # ancestor must exist too
    tree.remove("a1")
    assert not tree.is_within("a1x", "a")  # removed subtree is gone
    with pytest.raises(TypeError):
        tree.is_within(["unhashable"], "a")  # type: ignore[arg-type]


def test_add_second_root_raises(tree: StateTree) -> None:
    with pytest.raises(ValueError, match="root node already exists"):
        tree.add(make_node("root2", None, type="root"))


def test_add_broadcasts_full_node(tree: StateTree) -> None:
    rec = Recorder(tree)
    tree.add(make_node("a", "root"), index=0)
    (seq, op), = rec.events
    assert op["op"] == "add"
    assert op["index"] == 0
    assert op["node"]["id"] == "a"
    assert op["node"]["parentId"] == "root"
    assert op["node"]["status"] == "idle"


# -- update -----------------------------------------------------------------


def test_update_replaces_top_level_keys(tree: StateTree) -> None:
    tree.add(make_node("a", "root", props={"x": 1, "y": 2}))
    tree.update("a", {"status": "running", "props": {"z": 3}})
    node = tree.get("a")
    assert node.status == "running"
    assert node.props == {"z": 3}  # props REPLACED, not merged
    assert node.name == "a"  # untouched keys stay


def test_update_error_and_progress(tree: StateTree) -> None:
    tree.add(make_node("a", "root"))
    tree.update("a", {"status": "error", "error": {"message": "boom", "detail": "d"}, "progress": 0.5})
    node = tree.get("a")
    assert node.error.message == "boom"
    assert node.progress == 0.5


def test_update_rejects_structural_keys(tree: StateTree) -> None:
    tree.add(make_node("a", "root"))
    for key in ("id", "parentId", "children", "bogus"):
        with pytest.raises(ValueError, match="not updatable"):
            tree.update("a", {key: "x"})


def test_update_unknown_node_raises(tree: StateTree) -> None:
    with pytest.raises(ValueError, match="unknown node"):
        tree.update("nope", {"status": "idle"})


def test_update_broadcasts_normalized_changes(tree: StateTree) -> None:
    tree.add(make_node("a", "root"))
    rec = Recorder(tree)
    tree.update("a", {"status": "error", "error": {"message": "boom"}})
    (seq, op), = rec.events
    assert op == {
        "op": "update",
        "id": "a",
        "changes": {"status": "error", "error": {"message": "boom", "detail": None}},
    }


# -- merge_props ------------------------------------------------------------


def test_merge_props_shallow_merges(tree: StateTree) -> None:
    tree.add(make_node("a", "root", props={"x": 1, "y": 2}))
    rec = Recorder(tree)
    tree.merge_props("a", {"y": 20, "z": 30})
    assert tree.get("a").props == {"x": 1, "y": 20, "z": 30}
    (seq, op), = rec.events
    assert op == {"op": "merge_props", "id": "a", "props": {"y": 20, "z": 30}}


def test_merge_props_unknown_node_raises(tree: StateTree) -> None:
    with pytest.raises(ValueError, match="unknown node"):
        tree.merge_props("nope", {"x": 1})


# -- remove -----------------------------------------------------------------


def test_remove_leaf_maintains_children(tree: StateTree) -> None:
    tree.add(make_node("a", "root"))
    tree.add(make_node("b", "root"))
    tree.remove("a")
    assert tree.get("a") is None
    assert tree.get("root").children == ["b"]


def test_remove_subtree_removes_all_descendants(tree: StateTree) -> None:
    tree.add(make_node("a", "root"))
    tree.add(make_node("a1", "a"))
    tree.add(make_node("a1x", "a1"))
    tree.add(make_node("b", "root"))
    rec = Recorder(tree)
    tree.remove("a")
    assert tree.get("a") is None
    assert tree.get("a1") is None
    assert tree.get("a1x") is None
    assert tree.get("b") is not None
    assert tree.get("root").children == ["b"]
    # a single remove op is broadcast; descendants are implied
    (seq, op), = rec.events
    assert op == {"op": "remove", "id": "a"}


def test_remove_unknown_raises(tree: StateTree) -> None:
    with pytest.raises(ValueError, match="unknown node"):
        tree.remove("nope")


# -- seq & broadcast --------------------------------------------------------


def test_seq_increments_once_per_mutation(tree: StateTree) -> None:
    start = tree.seq
    rec = Recorder(tree)
    tree.add(make_node("a", "root"))
    tree.update("a", {"status": "running"})
    tree.merge_props("a", {"x": 1})
    tree.remove("a")
    assert tree.seq == start + 4
    assert [seq for seq, _ in rec.events] == [start + 1, start + 2, start + 3, start + 4]
    assert [op["op"] for _, op in rec.events] == ["add", "update", "merge_props", "remove"]


def test_unsubscribe_stops_callbacks(tree: StateTree) -> None:
    events: list[int] = []
    cb = tree.subscribe(lambda seq, op: events.append(seq))
    tree.add(make_node("a", "root"))
    tree.unsubscribe(cb)
    tree.add(make_node("b", "root"))
    assert len(events) == 1


def test_snapshot_shape(tree: StateTree) -> None:
    tree.add(make_node("a", "root", props={"x": 1}))
    snap = tree.snapshot()
    assert snap["rootId"] == "root"
    assert set(snap["nodes"]) == {"root", "a"}
    assert snap["nodes"]["a"]["props"] == {"x": 1}
    assert snap["nodes"]["root"]["children"] == ["a"]


def test_subtree_ids(tree: StateTree) -> None:
    tree.add(make_node("a", "root"))
    tree.add(make_node("a1", "a"))
    tree.add(make_node("a2", "a"))
    tree.add(make_node("a1x", "a1"))
    assert tree.subtree_ids("a") == ["a", "a1", "a1x", "a2"]
