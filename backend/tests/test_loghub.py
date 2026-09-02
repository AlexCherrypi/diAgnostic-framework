"""LogHub unit tests (SPEC 14.1): ring, filters, session file, live feed."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest
from pydantic import ValidationError

from diagcore.loghub import HISTORY_LIMIT, LogEvent, LogFilter, LogHub, sanitize_surrogates
from diagcore.model import Node
from diagcore.tree import StateTree

from util import make_base_tree, wait_until

#: What plugins/api.py's ensure_ascii=True `_send` produces for a
#: surrogateescape'd str (e.g. raw bus bytes), re-materialized by json.loads.
POISON = json.loads('"poison \\ud800 text"')


def serialized_size(event: dict[str, Any]) -> int:
    return len(json.dumps(event, ensure_ascii=False, default=repr).encode("utf-8"))


class BrokenFile:
    """A file object whose writes fail like a full disk (ENOSPC)."""

    def write(self, _: str) -> int:
        raise OSError(28, "No space left on device")

    def flush(self) -> None:
        pass

    def close(self) -> None:
        pass


@pytest.fixture
def tree() -> StateTree:
    tree = make_base_tree()
    tree.add(Node(id="a", type="device.sim", name="a", status="idle", owner="core", parentId="root"))
    tree.add(Node(id="b", type="telemetry.channel", name="b", status="idle", owner="core", parentId="a"))
    return tree


@pytest.fixture
def hub(tree: StateTree) -> LogHub:
    return LogHub(tree)


# -- ring & numbering --------------------------------------------------------


def test_event_numbering_and_ring_cap(tree: StateTree) -> None:
    hub = LogHub(tree, ring_size=5)
    for i in range(8):
        event = hub.emit("info", "system", f"event {i}")
        assert event.n == i + 1
        assert event.ts > 1e12  # unix epoch milliseconds
    assert hub.event_count == 8
    history = hub.history()
    assert [e["message"] for e in history] == [f"event {i}" for i in range(3, 8)]  # oldest dropped
    assert [e["n"] for e in history] == [4, 5, 6, 7, 8]  # monotonic, gap-free


def test_history_limit_returns_newest_matching(hub: LogHub) -> None:
    for i in range(20):
        hub.emit("info", "system", f"event {i}")
    history = hub.history(LogFilter(), limit=5)
    assert [e["message"] for e in history] == [f"event {i}" for i in range(15, 20)]
    assert HISTORY_LIMIT == 500  # the /ws/logs default per SPEC 14.3


# -- filters -----------------------------------------------------------------


def test_level_category_and_search_filters(hub: LogHub) -> None:
    hub.emit("info", "cmd", "cmd 'selftest' -> 'a'")
    hub.emit("error", "job", "job j1 'Self-test' failed: kaboom")
    hub.emit("trace", "state", "state update a: status='running'")
    hub.emit("info", "io", "io tx a raw=22F190", raw="22F190")

    assert [e["category"] for e in hub.history(LogFilter(levels=["error"]))] == ["job"]
    assert [e["level"] for e in hub.history(LogFilter(categories=["cmd", "io"]))] == ["info", "info"]
    assert [e["message"] for e in hub.history(LogFilter(search="KABOOM"))] == [
        "job j1 'Self-test' failed: kaboom"
    ]  # search is case-insensitive
    assert hub.history(LogFilter(search="no such text")) == []
    # empty lists mean "no constraint" (SPEC 14.3: fields are optional)
    assert len(hub.history(LogFilter(levels=[], categories=[]))) == 4
    # combined constraints
    assert hub.history(LogFilter(levels=["info"], categories=["io"], search="22F1")) != []
    assert hub.history(LogFilter(levels=["warn"], categories=["io"])) == []


def test_subtree_filter_via_ancestry(hub: LogHub) -> None:
    hub.emit("info", "io", "on b", node_id="b")
    hub.emit("info", "system", "no node")

    assert [e["message"] for e in hub.history(LogFilter(nodeId="b"))] == ["on b"]
    assert [e["message"] for e in hub.history(LogFilter(nodeId="a"))] == ["on b"]  # subtree
    assert [e["message"] for e in hub.history(LogFilter(nodeId="root"))] == ["on b"]
    assert hub.history(LogFilter(nodeId="jobs")) == []
    assert len(hub.history(LogFilter())) == 2  # no nodeId constraint => both


def test_subtree_filter_survives_node_removal(tree: StateTree, hub: LogHub) -> None:
    hub.emit("info", "io", "while alive", node_id="b")
    tree.remove("a")  # removes a and b
    # stored events keep matching subtree filters (ancestry stored at emit time)
    assert [e["message"] for e in hub.history(LogFilter(nodeId="a"))] == ["while alive"]
    # late events for the removed node resolve via the emit-time cache
    hub.emit("info", "io", "after removal", node_id="b")
    assert [e["message"] for e in hub.history(LogFilter(nodeId="a"))] == [
        "while alive",
        "after removal",
    ]


def test_unknown_node_ancestry_is_itself(hub: LogHub) -> None:
    hub.emit("info", "system", "ghost event", node_id="ghost")
    assert [e["message"] for e in hub.history(LogFilter(nodeId="ghost"))] == ["ghost event"]
    assert hub.history(LogFilter(nodeId="root")) == []


# -- session file ------------------------------------------------------------


def test_session_file_complete_and_valid(tree: StateTree, tmp_path: Path) -> None:
    hub = LogHub(tree, session_dir=tmp_path / "logs")
    assert hub.session_path is not None
    assert hub.session_path.parent == tmp_path / "logs"  # directory created
    assert hub.session_path.name.startswith("session-")
    assert hub.session_path.suffix == ".jsonl"

    hub.emit("info", "cmd", "first", node_id="b", data={"params": {"x": 1}})
    hub.emit("trace", "state", "second ünïcode")
    hub.emit("info", "io", "third", node_id="b", raw="22F190", data={"direction": "tx"})
    # flushed promptly: readable NOW, without closing (crash tolerance)
    lines = hub.session_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 3
    events = [json.loads(line) for line in lines]  # every line is valid JSON
    assert [e["n"] for e in events] == [1, 2, 3]
    assert events[0]["data"] == {"params": {"x": 1}}
    assert events[1]["message"] == "second ünïcode"
    assert events[2]["raw"] == "22F190"
    for event in events:
        assert set(event) == {"ts", "n", "level", "source", "nodeId", "category", "message", "data", "raw"}
    hub.close()


def test_two_hubs_get_distinct_session_files(tree: StateTree, tmp_path: Path) -> None:
    hub1 = LogHub(tree, session_dir=tmp_path)
    hub2 = LogHub(tree, session_dir=tmp_path)
    assert hub1.session_path != hub2.session_path
    hub1.close()
    hub2.close()


def test_emit_survives_closed_file(tree: StateTree, tmp_path: Path) -> None:
    hub = LogHub(tree, session_dir=tmp_path)
    hub.emit("info", "system", "one")
    hub.close()
    hub.emit("info", "system", "two")  # must not raise; ring keeps serving
    assert [e["message"] for e in hub.history()] == ["one", "two"]
    # a deliberate close is not a sink failure: nothing flagged, no error event
    assert hub.sink_error is None
    assert hub.history(LogFilter(levels=["error"])) == []


# -- lone surrogates (not valid text) must not break any sink ----------------


def test_sanitize_surrogates_escapes_only_what_is_broken() -> None:
    clean = {"a": ["x", 1, None, {"b": "ünïcode"}], "c": "d"}
    assert sanitize_surrogates(clean) is clean  # untouched => same object
    dirty = {"msg": POISON, "nested": [{"k": POISON}, "fine"], POISON: 1}
    out = sanitize_surrogates(dirty)
    assert out is not dirty
    assert out["msg"] == "poison \\ud800 text"  # escape text, byte value visible
    assert out["nested"] == [{"k": "poison \\ud800 text"}, "fine"]
    assert "poison \\ud800 text" in out  # keys too
    json.dumps(out, ensure_ascii=False).encode("utf-8")  # now transport-safe


def test_lone_surrogate_in_event_keeps_file_alive_and_is_escaped(tree: StateTree, tmp_path: Path) -> None:
    """One bad string (plugin log text, node props, cmd params...) must
    neither disable the session file for the rest of the session nor leave
    the ring holding an event no WebSocket transport can encode."""
    hub = LogHub(tree, session_dir=tmp_path / "logs")
    hub.emit("info", "system", "clean before")
    poison = hub.emit("info", "plugin", POISON, source="plugin:x", node_id="b", data={"line": POISON, "nested": [POISON]})
    hub.emit("info", "system", "clean after")
    hub.emit("trace", "state", "another", node_id="b")

    assert hub.sink_error is None  # file still healthy
    lines = hub.session_path.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 4  # every event written, nothing lost
    events = [json.loads(line) for line in lines]
    assert [e["n"] for e in events] == [1, 2, 3, 4]
    assert events[1]["message"] == "poison \\ud800 text"
    assert events[1]["data"] == {"line": "poison \\ud800 text", "nested": ["poison \\ud800 text"]}
    # the returned event and the ring carry the same escaped text
    assert poison.message == "poison \\ud800 text"
    history = hub.history()
    assert [e["message"] for e in history][:3] == ["clean before", "poison \\ud800 text", "clean after"]
    for event in history:  # what /ws/logs would send: must be UTF-8 encodable
        json.dumps(event, ensure_ascii=False).encode("utf-8")
    assert len(hub.history(LogFilter(nodeId="a"))) == 2  # subtree filtering intact
    hub.close()


def test_raw_must_be_whole_byte_hex(hub: LogHub) -> None:
    """SPEC 14.1/14.2: `raw` is hex of whole bytes, reconstructible via bytes.fromhex."""
    LogEvent(ts=1.0, n=1, level="info", source="core", category="io", message="m", raw="22f190")
    for bad in ("0x22F442", "22F4 42", "22F44", "", "hello"):
        with pytest.raises(ValidationError):
            LogEvent(ts=1.0, n=1, level="info", source="core", category="io", message="m", raw=bad)
    # an emitter violating it is recorded as a system error, never raised
    event = hub.emit("info", "io", "io tx b raw=0x22", node_id="b", raw="0x22")
    assert event.level == "error" and event.category == "system"
    assert event.data["original"]["raw"] == "0x22"


# -- ring byte budget ---------------------------------------------------------


def test_ring_byte_budget_evicts_oldest_first(tree: StateTree) -> None:
    budget = 20_000
    hub = LogHub(tree, ring_size=1000, ring_bytes=budget)
    payload = "x" * 1000
    for i in range(200):
        hub.emit("trace", "state", f"event {i}", node_id="b", data={"op": {"props": {"blob": payload}}})
    history = hub.history()
    assert 0 < len(history) < 200  # the byte cap bit long before the count cap
    assert hub.ring_bytes <= budget
    assert hub.ring_bytes == sum(serialized_size(e) for e in history)  # exact accounting
    ns = [e["n"] for e in history]
    assert ns == list(range(ns[0], 201))  # newest kept, gap-free, oldest evicted
    assert history[-1]["message"] == "event 199"
    # history/filter semantics unchanged
    assert hub.history(LogFilter(nodeId="a"), limit=3) == history[-3:]


def test_ring_byte_budget_keeps_newest_even_if_oversized(tree: StateTree) -> None:
    hub = LogHub(tree, ring_size=10, ring_bytes=500)
    hub.emit("info", "system", "small")
    hub.emit("info", "system", "big " + "y" * 2000)
    assert [e["message"][:3] for e in hub.history()] == ["big"]  # never empty
    assert hub.ring_bytes == serialized_size(hub.history()[0])


def test_ring_count_cap_keeps_byte_accounting_exact(tree: StateTree) -> None:
    hub = LogHub(tree, ring_size=5)
    for i in range(8):
        hub.emit("info", "system", f"event {i} " + "z" * i)
    history = hub.history()
    assert len(history) == 5
    assert hub.ring_bytes == sum(serialized_size(e) for e in history)


# -- session-file sink failure is never silent --------------------------------


def test_write_failure_is_reported_once_and_ring_keeps_serving(tree: StateTree, tmp_path: Path) -> None:
    hub = LogHub(tree, session_dir=tmp_path / "logs")
    reported: list[str] = []
    hub.on_sink_failure = reported.append
    hub.emit("info", "system", "one")
    hub._file = BrokenFile()  # the disk fills up
    hub.emit("info", "cmd", "two")  # the failing write
    hub.emit("info", "cmd", "three")
    hub.emit("info", "cmd", "four")

    assert hub.sink_error is not None and "No space left" in hub.sink_error
    assert reported == [hub.sink_error]  # exactly one report
    messages = [e["message"] for e in hub.history()]
    assert messages[:2] == ["one", "two"]
    assert messages[2].startswith("audit session file disabled:")  # right after the failing event
    assert messages[3:] == ["three", "four"]  # ring keeps serving, no repeat
    failure = hub.history(LogFilter(levels=["error"], categories=["system"]))
    assert len(failure) == 1
    assert failure[0]["nodeId"] == "system"
    assert failure[0]["data"]["sessionFile"] == str(hub.session_path)
    assert "No space left" in failure[0]["data"]["error"]
    assert [e["n"] for e in hub.history()] == [1, 2, 3, 4, 5]  # gap-free numbering
    # the file holds what was written before the outage; nothing crashed
    assert len(hub.session_path.read_text(encoding="utf-8").splitlines()) == 1
    hub.close()


def test_open_failure_at_startup_is_reported(tree: StateTree, tmp_path: Path) -> None:
    not_a_dir = tmp_path / "logs"
    not_a_dir.write_text("I am a file", encoding="utf-8")
    hub = LogHub(tree, session_dir=not_a_dir)
    assert hub.session_path is None
    assert hub.sink_error is not None
    hub.emit("info", "system", "session started")  # what AppCore emits first
    history = hub.history()
    assert [e["message"][:22] for e in history] == ["session started", "audit session file dis"]
    assert history[1]["level"] == "error" and history[1]["category"] == "system"
    assert history[1]["data"]["sessionFile"] is None
    hub.emit("info", "system", "later")
    assert len(hub.history(LogFilter(levels=["error"]))) == 1  # reported once


# -- live subscriptions ------------------------------------------------------


async def test_live_subscribe_batches_and_filters(tree: StateTree) -> None:
    hub = LogHub(tree, flush_interval=0.01)
    await hub.start()
    try:
        batches: list[list[dict]] = []

        def callback(events: list[dict]) -> None:
            batches.append(events)

        history = hub.subscribe(callback, LogFilter(categories=["io"]))
        assert history == []
        hub.emit("info", "io", "io one", node_id="b")
        hub.emit("info", "cmd", "not io")
        hub.emit("info", "io", "io two", node_id="b")
        await wait_until(lambda: sum(len(b) for b in batches) >= 2, message="live batch delivery")
        delivered = [e for batch in batches for e in batch]
        assert [e["message"] for e in delivered] == ["io one", "io two"]
        hub.unsubscribe(callback)
    finally:
        await hub.stop()


async def test_history_and_live_do_not_overlap_and_unsubscribe_stops(tree: StateTree) -> None:
    hub = LogHub(tree)
    hub.emit("info", "system", "old 1")
    hub.emit("info", "system", "old 2")

    batches: list[list[dict]] = []

    def callback(events: list[dict]) -> None:
        batches.append(events)

    history = hub.subscribe(callback, LogFilter())
    assert [e["message"] for e in history] == ["old 1", "old 2"]
    hub.emit("info", "system", "new 1")
    hub.flush()
    delivered = [e for batch in batches for e in batch]
    assert [e["message"] for e in delivered] == ["new 1"]  # history not re-delivered

    hub.unsubscribe(callback)
    hub.emit("info", "system", "new 2")
    hub.flush()
    assert sum(len(b) for b in batches) == 1  # nothing after unsubscribe


# -- event model contract & emit robustness ----------------------------------


def test_log_event_model_is_the_wire_contract() -> None:
    """SPEC 14.1: exactly these fields, these literals, no extras."""
    event = LogEvent(
        ts=1735e12,
        n=1,
        level="info",
        source="plugin:demo_vci",
        nodeId="x",
        category="io",
        message="io tx x raw=22F190",
        data={"direction": "tx"},
        raw="22F190",
    )
    assert set(event.model_dump()) == {"ts", "n", "level", "source", "nodeId", "category", "message", "data", "raw"}
    with pytest.raises(ValidationError):
        LogEvent(ts=1.0, n=1, level="verbose", source="core", category="cmd", message="m")
    with pytest.raises(ValidationError):
        LogEvent(ts=1.0, n=1, level="info", source="core", category="telemetry", message="m")
    with pytest.raises(ValidationError):
        LogEvent(ts=1.0, n=1, level="info", source="frontend", category="cmd", message="m")
    with pytest.raises(ValidationError):
        LogEvent(ts=1.0, n=1, level="info", source="core", category="cmd", message="m", extra="no")


def test_invalid_emit_is_recorded_as_system_error_not_raised(hub: LogHub) -> None:
    """A logging bug at a seam must neither raise into the seam (that could
    swallow a cmd_result or kill a tree subscriber) nor vanish silently."""

    class Opaque:  # not JSON-serializable
        pass

    bad_source = hub.emit("info", "cmd", "cmd 'x' -> 'b'", source="Frontend", node_id="b")
    bad_data = hub.emit("info", "io", "io tx b", node_id="b", data={"obj": Opaque()})

    assert (bad_source.level, bad_source.category, bad_source.source) == ("error", "system", "core")
    assert "Frontend" in bad_source.message and "cmd 'x' -> 'b'" in bad_source.message
    assert bad_source.data["original"]["source"] == "Frontend"
    assert bad_source.data["original"]["message"] == "cmd 'x' -> 'b'"
    assert bad_data.level == "error" and "Opaque" in bad_data.data["original"]["data"]
    # numbering stays gap-free and both stay filterable by the node's subtree
    assert [e["n"] for e in hub.history()] == [1, 2]
    assert len(hub.history(LogFilter(nodeId="a"))) == 2
