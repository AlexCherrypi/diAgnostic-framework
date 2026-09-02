"""Live end-to-end smoke test against a RUNNING diAgnostic core.

Standalone: start the core first (``python -m diagcore`` from the backend
venv), then run this script with the same interpreter:

    C:\\diAgnostic-framework\\backend\\.venv\\Scripts\\python.exe tests\\smoke_live.py

Exercises the full contract over real WebSockets:

1. /ws/state hello snapshot (root/system/plugins/jobs + demo_vci subtree)
2. /ws/telemetry sub => history first, then fresh sample batches
3. selftest command => job runs (progress increases, steps appear) => done
4. selftest_fail => job AND the failing step end in status error
5. crash => plugin subtree goes error => supervisor restarts => idle again
6. one more selftest, cancelled mid-flight => status cancelled
7. /ws/logs (M0.5, SPEC 14.3): sub with a subtree+io filter => history of the
   earlier self-tests' raw frames, then live tx/rx batches while a job runs
8. /ws/logs unfiltered: history arrives; a selftest then shows up live as
   cmd events (target id + full params + result), job lifecycle events
   (created/started/steps/milestones/done) and io frames (tx AND rx, valid
   hex, decoded present)
9. /ws/logs with filter {nodeId: plugin:demo_vci}: history AND live carry
   only events of that subtree (several categories, nothing from outside)
10. session file logs/session-<ts>.jsonl exists, is valid gap-free JSONL and
    contains the very io frames (n + raw hex) seen live in check 8
11. every state patch is audited: category `state` / level `trace` events
    exist for the check-8 job's add and its status=done update

The port defaults to ``DIAGCORE_PORT`` (env) or 8765; ``--port`` overrides.
Exit code 0 = all green; any assertion failure exits 1 with a message.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path
from typing import Any, Callable

import websockets

PLUGIN_ID = "plugin:demo_vci"
DEVICE_ID = f"{PLUGIN_ID}:device"
CHANNEL_IDS = [f"{DEVICE_ID}:ch:{key}" for key in ("sine", "noise", "saw")]

LOG_LEVELS = {"trace", "debug", "info", "warn", "error"}
LOG_CATEGORIES = {"cmd", "state", "io", "job", "plugin", "system"}
LOG_EVENT_KEYS = {"ts", "n", "level", "source", "nodeId", "category", "message", "data", "raw"}

#: /ws/logs history is up to 500 events with payloads: allow large frames.
LOGS_WS_MAX_SIZE = 8 * 1024 * 1024


class SmokeFailure(AssertionError):
    pass


def check(cond: bool, message: str) -> None:
    if not cond:
        raise SmokeFailure(message)


# --------------------------------------------------------------------------
# Tree mirror (frontend-equivalent patch application)
# --------------------------------------------------------------------------


class Mirror:
    def __init__(self) -> None:
        self.seq = -1
        self.root_id = ""
        self.nodes: dict[str, dict[str, Any]] = {}

    def apply_hello(self, msg: dict[str, Any]) -> None:
        self.seq = msg["seq"]
        self.root_id = msg["snapshot"]["rootId"]
        self.nodes = {nid: dict(node) for nid, node in msg["snapshot"]["nodes"].items()}

    def apply_patch(self, msg: dict[str, Any]) -> None:
        seq = msg["seq"]
        check(seq == self.seq + 1, f"seq gap: mirror at {self.seq}, patch has {seq}")
        self.seq = seq
        for op in msg["ops"]:
            self._apply_op(op)

    def _apply_op(self, op: dict[str, Any]) -> None:
        kind = op["op"]
        if kind == "add":
            node = dict(op["node"])
            self.nodes[node["id"]] = node
            parent = self.nodes.get(node.get("parentId") or "")
            if parent is not None:
                children = list(parent.get("children") or [])
                if node["id"] not in children:
                    index = op.get("index")
                    if index is None:
                        children.append(node["id"])
                    else:
                        children.insert(index, node["id"])
                parent["children"] = children
        elif kind == "update":
            node = self.nodes.get(op["id"])
            check(node is not None, f"update for unknown node {op['id']!r}")
            assert node is not None
            node.update(op["changes"])
        elif kind == "merge_props":
            node = self.nodes.get(op["id"])
            check(node is not None, f"merge_props for unknown node {op['id']!r}")
            assert node is not None
            props = dict(node.get("props") or {})
            props.update(op["props"])
            node["props"] = props
        elif kind == "remove":
            self._remove(op["id"])
        else:
            raise SmokeFailure(f"unknown op {kind!r}")

    def _remove(self, node_id: str) -> None:
        node = self.nodes.pop(node_id, None)
        if node is None:
            return
        parent = self.nodes.get(node.get("parentId") or "")
        if parent is not None and node_id in (parent.get("children") or []):
            parent["children"] = [c for c in parent["children"] if c != node_id]
        for child in list(node.get("children") or []):
            self._remove(child)

    def children_of_type(self, node_id: str, node_type: str) -> list[dict[str, Any]]:
        node = self.nodes.get(node_id)
        if node is None:
            return []
        out = []
        for cid in node.get("children") or []:
            child = self.nodes.get(cid)
            if child is not None and child.get("type") == node_type:
                out.append(child)
        return out


# --------------------------------------------------------------------------
# State client: single reader that applies patches and stashes cmd_results
# --------------------------------------------------------------------------


class StateClient:
    def __init__(self, ws: websockets.ClientConnection) -> None:
        self.ws = ws
        self.mirror = Mirror()
        self.cmd_results: dict[str, dict[str, Any]] = {}
        self._cmd_counter = 0

    async def wait_hello(self, timeout: float = 5.0) -> None:
        raw = await asyncio.wait_for(self.ws.recv(), timeout)
        msg = json.loads(raw)
        check(msg.get("t") == "hello", f"first message must be hello, got {msg.get('t')!r}")
        self.mirror.apply_hello(msg)

    def _dispatch(self, msg: dict[str, Any]) -> None:
        t = msg.get("t")
        if t == "patch":
            self.mirror.apply_patch(msg)
        elif t == "cmd_result":
            self.cmd_results[msg["cmdId"]] = msg
        elif t == "hello":
            self.mirror.apply_hello(msg)
        else:
            raise SmokeFailure(f"unexpected /ws/state message type {t!r}")

    async def pump_until(
        self, predicate: Callable[[], bool], timeout: float, what: str
    ) -> None:
        """Receive and apply messages until predicate() is true."""
        deadline = time.monotonic() + timeout
        while not predicate():
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise SmokeFailure(f"timeout ({timeout:.0f}s) waiting for: {what}")
            try:
                raw = await asyncio.wait_for(self.ws.recv(), remaining)
            except asyncio.TimeoutError:
                raise SmokeFailure(f"timeout ({timeout:.0f}s) waiting for: {what}") from None
            self._dispatch(json.loads(raw))

    async def send_cmd(self, target: str, action: str, params: dict[str, Any] | None = None) -> str:
        self._cmd_counter += 1
        cmd_id = f"smoke-{self._cmd_counter}"
        await self.ws.send(
            json.dumps(
                {"t": "cmd", "cmdId": cmd_id, "target": target, "action": action, "params": params or {}}
            )
        )
        return cmd_id

    async def cmd(
        self, target: str, action: str, params: dict[str, Any] | None = None, timeout: float = 10.0
    ) -> dict[str, Any]:
        cmd_id = await self.send_cmd(target, action, params)
        await self.pump_until(
            lambda: cmd_id in self.cmd_results, timeout, f"cmd_result for {action} on {target}"
        )
        return self.cmd_results.pop(cmd_id)

    def node(self, node_id: str) -> dict[str, Any] | None:
        return self.mirror.nodes.get(node_id)

    def status(self, node_id: str) -> str | None:
        node = self.node(node_id)
        return None if node is None else node.get("status")


# --------------------------------------------------------------------------
# Checks
# --------------------------------------------------------------------------


async def check_hello(sc: StateClient) -> None:
    m = sc.mirror
    root = m.nodes.get(m.root_id)
    check(root is not None and root["type"] == "root", "hello: root node missing")

    def well_known(node_type: str) -> dict[str, Any]:
        matches = m.children_of_type(m.root_id, node_type)
        check(len(matches) == 1, f"hello: expected exactly one {node_type!r} child of root")
        return matches[0]

    system = well_known("system")
    for key in ("version", "startedAt", "pythonVersion", "auditFile"):
        check(key in (system.get("props") or {}), f"hello: system.props missing {key!r}")
    check(
        system.get("status") != "error",
        f"hello: system node in error (audit trail disabled?): {system.get('error')}",
    )
    check(isinstance(system["props"]["auditFile"], str), "hello: system.props.auditFile must name the session file")
    well_known("jobs")
    plugins = well_known("plugins")
    check(PLUGIN_ID in (plugins.get("children") or []), f"hello: {PLUGIN_ID} not under plugins")

    # The demo plugin declares its subtree shortly after spawn; wait for it.
    def declared() -> bool:
        if sc.status(DEVICE_ID) != "idle":
            return False
        return all(cid in m.nodes for cid in CHANNEL_IDS)

    await sc.pump_until(declared, 15.0, "demo_vci device idle + 3 channels declared")
    device = m.nodes[DEVICE_ID]
    check(device["type"] == "device.sim", "device node has wrong type")
    action_names = {a["name"] for a in device.get("actions") or []}
    check(
        {"selftest", "selftest_fail", "crash"} <= action_names,
        f"device actions incomplete: {sorted(action_names)}",
    )
    for cid in CHANNEL_IDS:
        ch = m.nodes[cid]
        check(ch["type"] == "telemetry.channel", f"{cid} has wrong type")
        for key in ("unit", "label", "sampleRateHz"):
            check(key in (ch.get("props") or {}), f"{cid} props missing {key!r}")
    print("OK  1. hello snapshot: root/system/plugins/jobs + demo_vci device + 3 channels")


async def check_telemetry(base_ws_url: str) -> None:
    channel = CHANNEL_IDS[0]
    sub_time_ms = time.time() * 1000.0
    async with websockets.connect(f"{base_ws_url}/ws/telemetry") as ws:
        await ws.send(json.dumps({"t": "sub", "channel": channel}))
        first = json.loads(await asyncio.wait_for(ws.recv(), 5.0))
        check(first.get("t") == "history", f"first telemetry message must be history, got {first.get('t')!r}")
        check(first.get("channel") == channel, "history for wrong channel")
        history = first.get("samples")
        check(isinstance(history, list), "history samples must be a list")
        # Fresh live samples: at least one batch with a timestamp after sub time.
        fresh = 0
        deadline = time.monotonic() + 5.0
        while fresh == 0 and time.monotonic() < deadline:
            msg = json.loads(await asyncio.wait_for(ws.recv(), 5.0))
            check(msg.get("t") == "samples", f"expected samples batch, got {msg.get('t')!r}")
            check(msg.get("channel") == channel, "samples for wrong channel")
            for t_ms, value in msg.get("samples") or []:
                check(isinstance(t_ms, (int, float)) and isinstance(value, (int, float)),
                      "sample must be [tMs, value] numbers")
                if t_ms >= sub_time_ms - 1000.0:
                    fresh += 1
        check(fresh > 0, "no fresh telemetry samples within 5 s of subscribing")
        await ws.send(json.dumps({"t": "unsub", "channel": channel}))
    print(f"OK  2. telemetry: history ({len(history)} samples) then {fresh} fresh live samples")


async def start_selftest(sc: StateClient, action: str) -> str:
    result = await sc.cmd(DEVICE_ID, action, timeout=10.0)
    check(result["ok"] is True, f"{action} cmd_result not ok: {result.get('error')}")
    job_id = (result.get("data") or {}).get("jobId")
    check(isinstance(job_id, str) and job_id, f"{action} cmd_result carries no jobId")
    await sc.pump_until(lambda: sc.node(job_id) is not None, 5.0, f"job node {job_id}")
    return job_id


def job_steps(sc: StateClient, job_id: str) -> list[dict[str, Any]]:
    steps: list[dict[str, Any]] = []

    def collect(node_id: str) -> None:
        node = sc.node(node_id)
        if node is None:
            return
        for cid in node.get("children") or []:
            child = sc.node(cid)
            if child is not None and child.get("type") == "job.step":
                steps.append(child)
                collect(cid)

    collect(job_id)
    return steps


async def check_selftest_done(sc: StateClient) -> None:
    job_id = await start_selftest(sc, "selftest")
    progress_seen: list[float] = []

    def track() -> bool:
        node = sc.node(job_id)
        if node is None:
            raise SmokeFailure(f"job node {job_id} disappeared mid-run")
        p = node.get("progress")
        if isinstance(p, (int, float)) and (not progress_seen or p > progress_seen[-1]):
            progress_seen.append(float(p))
        return node.get("status") == "done"

    await sc.pump_until(track, 60.0, "selftest job reaching status done")
    check(len(progress_seen) >= 2, f"progress did not increase (saw {progress_seen})")
    check(progress_seen == sorted(progress_seen), "progress was not monotonically increasing")
    steps = job_steps(sc, job_id)
    check(len(steps) == 4, f"expected 4 job.step nodes, found {len(steps)}")
    bad = [s["id"] for s in steps if s.get("status") != "done"]
    check(not bad, f"steps not done after job done: {bad}")
    job = sc.node(job_id)
    assert job is not None
    check(job.get("actions") == [], "terminal job must expose no actions")
    print(
        f"OK  3. selftest: running with increasing progress "
        f"({len(progress_seen)} updates, max {progress_seen[-1]:.2f}), 4 steps, job done"
    )


async def check_selftest_fail(sc: StateClient) -> None:
    job_id = await start_selftest(sc, "selftest_fail")

    def failed() -> bool:
        node = sc.node(job_id)
        if node is None:
            raise SmokeFailure(f"job node {job_id} disappeared mid-run")
        status = node.get("status")
        check(status != "done", "selftest_fail job unexpectedly succeeded")
        return status == "error"

    await sc.pump_until(failed, 60.0, "selftest_fail job reaching status error")
    job = sc.node(job_id)
    assert job is not None
    check((job.get("error") or {}).get("message"), "failed job carries no error.message")
    steps = job_steps(sc, job_id)
    error_steps = [s for s in steps if s.get("status") == "error"]
    check(len(error_steps) == 1, f"expected exactly 1 failing step, found {len(error_steps)}")
    check(
        (error_steps[0].get("error") or {}).get("message"),
        "failing step carries no error.message",
    )
    print(
        f"OK  4. selftest_fail: job error ({(job.get('error') or {}).get('message')!r}) "
        f"+ failing step {error_steps[0]['name']!r} error"
    )


async def check_crash_and_recover(sc: StateClient) -> None:
    # The crash handler calls os._exit(1) and never answers the RPC; the host
    # fails the pending command, so expect an ok:false result (or none if the
    # death races the response) -- do not require it, just fire the command.
    await sc.send_cmd(DEVICE_ID, "crash")

    def subtree_error() -> bool:
        plugin = sc.node(PLUGIN_ID)
        if plugin is None or plugin.get("status") != "error":
            return False
        descendants = [nid for nid in sc.mirror.nodes if nid.startswith(PLUGIN_ID + ":")]
        return bool(descendants) and all(
            sc.status(nid) == "error" for nid in descendants
        )

    await sc.pump_until(subtree_error, 15.0, "plugin subtree in status error after crash")
    plugin = sc.node(PLUGIN_ID)
    assert plugin is not None
    check(
        (plugin.get("error") or {}).get("message") == "plugin process died",
        f"plugin error.message wrong: {plugin.get('error')}",
    )

    def recovered() -> bool:
        if sc.status(PLUGIN_ID) not in ("idle", "running"):
            return False
        if sc.status(DEVICE_ID) != "idle":
            return False
        return all(cid in sc.mirror.nodes for cid in CHANNEL_IDS)

    await sc.pump_until(recovered, 30.0, "supervisor restart re-declaring the subtree")
    print("OK  5. crash: subtree error ('plugin process died') then auto-restart, device idle again")


async def check_cancel(sc: StateClient) -> None:
    job_id = await start_selftest(sc, "selftest")
    await sc.pump_until(
        lambda: sc.status(job_id) in ("running", "init"), 10.0, "job running before cancel"
    )
    result = await sc.cmd(job_id, "cancel", timeout=10.0)
    check(result["ok"] is True, f"cancel cmd_result not ok: {result.get('error')}")
    await sc.pump_until(
        lambda: sc.status(job_id) == "cancelled", 15.0, "cancelled status on job"
    )
    job = sc.node(job_id)
    assert job is not None
    check(job.get("actions") == [], "cancelled job must expose no actions")
    print("OK  6. cancel: second selftest cancelled, job status cancelled")


async def check_logs_feed(sc: StateClient, base_ws_url: str) -> None:
    """/ws/logs: one filter per connection, history first, then live batches."""
    async with websockets.connect(f"{base_ws_url}/ws/logs", max_size=8 * 1024 * 1024) as ws:
        await ws.send(json.dumps({"t": "sub", "filter": {"nodeId": DEVICE_ID, "categories": ["io"]}}))
        first = json.loads(await asyncio.wait_for(ws.recv(), 5.0))
        check(first.get("t") == "history", f"first /ws/logs message must be history, got {first.get('t')!r}")
        history = first.get("events")
        check(isinstance(history, list), "history events must be a list")
        check(len(history) > 0, "expected io history from the earlier self-tests")
        check(all(e.get("category") == "io" for e in history), "history contains non-io events despite the filter")
        check(all(e.get("nodeId") == DEVICE_ID for e in history), "history contains events outside the subtree")

        # a fresh job => the demo plugin simulates UDS-ish tx/rx frames live
        job_id = await start_selftest(sc, "selftest")
        live: list[dict[str, Any]] = []
        deadline = time.monotonic() + 15.0
        while len(live) < 6 and time.monotonic() < deadline:
            msg = json.loads(await asyncio.wait_for(ws.recv(), 5.0))
            check(msg.get("t") == "events", f"expected a live events batch, got {msg.get('t')!r}")
            check(isinstance(msg.get("events"), list), "live events must be batched in a list")
            live.extend(msg["events"])
        check(len(live) >= 6, f"only {len(live)} live io events within 15 s")
        for event in live:
            check(event.get("category") == "io" and event.get("nodeId") == DEVICE_ID, "live event outside the filter")
            check((event.get("data") or {}).get("direction") in ("tx", "rx"), "io event without tx/rx direction")
            check(isinstance(event.get("raw"), str) and event["raw"], "io event without raw hex")
            int(event["raw"], 16)
        check(live[0]["data"]["direction"] == "tx", "the first frame of a job must be a request (tx)")
        ns = [e["n"] for e in history + live]
        check(ns == sorted(ns) and len(set(ns)) == len(ns), "event numbers must be strictly increasing")
        await ws.send(json.dumps({"t": "unsub"}))
    await sc.pump_until(lambda: sc.status(job_id) == "done", 60.0, "selftest (logs check) reaching done")
    print(
        f"OK  7. logs: /ws/logs history ({len(history)} io events) then {len(live)} live tx/rx "
        f"frames for {DEVICE_ID}"
    )


# --------------------------------------------------------------------------
# M0.5 audit-log checks (SPEC 14.1 / 14.2 / 14.3)
# --------------------------------------------------------------------------


def check_log_event_shape(event: dict[str, Any]) -> None:
    """One LogEvent exactly per SPEC 14.1 (wire field names and types)."""
    check(isinstance(event, dict), f"log event must be an object, got {type(event).__name__}")
    check(set(event) <= LOG_EVENT_KEYS, f"log event has unknown keys: {sorted(set(event) - LOG_EVENT_KEYS)}")
    for key in ("ts", "n", "level", "source", "category", "message"):
        check(key in event, f"log event #{event.get('n')} missing {key!r}")
    check(isinstance(event["ts"], (int, float)), "log event ts must be a number (unix ms)")
    check(isinstance(event["n"], int), "log event n must be an int")
    check(event["level"] in LOG_LEVELS, f"log event level invalid: {event['level']!r}")
    check(event["category"] in LOG_CATEGORIES, f"log event category invalid: {event['category']!r}")
    source = event["source"]
    check(source == "core" or source.startswith("plugin:"), f"log event source invalid: {source!r}")
    check(event.get("nodeId") is None or isinstance(event["nodeId"], str), "nodeId must be string|null")
    check(isinstance(event["message"], str) and event["message"], "log event message must be non-empty")
    check(event.get("data") is None or isinstance(event["data"], dict), "data must be object|null")
    check(event.get("raw") is None or isinstance(event["raw"], str), "raw must be string|null")


def check_strictly_increasing(events: list[dict[str, Any]], what: str) -> None:
    ns = [e["n"] for e in events]
    check(ns == sorted(ns) and len(set(ns)) == len(ns), f"{what}: event numbers must be strictly increasing")


def check_hex(raw: Any, what: str) -> None:
    check(isinstance(raw, str) and raw, f"{what}: io event without raw hex")
    check(len(raw) % 2 == 0, f"{what}: raw hex {raw!r} is not whole bytes")
    try:
        int(raw, 16)
    except ValueError:
        raise SmokeFailure(f"{what}: raw {raw!r} is not a hex string") from None


def in_plugin_subtree(node_id: Any) -> bool:
    return isinstance(node_id, str) and (node_id == PLUGIN_ID or node_id.startswith(PLUGIN_ID + ":"))


class LogCollector:
    """Background reader for one /ws/logs connection after its history: keeps
    every live batch so the state socket can be pumped concurrently."""

    def __init__(self, ws: websockets.ClientConnection) -> None:
        self.ws = ws
        self.events: list[dict[str, Any]] = []
        self.batches = 0

    async def run(self) -> None:
        while True:
            msg = json.loads(await self.ws.recv())
            check(msg.get("t") == "events", f"expected a live events batch, got {msg.get('t')!r}")
            events = msg.get("events")
            check(isinstance(events, list) and events, "live events must be a non-empty list")
            self.batches += 1
            self.events.extend(events)


async def logs_sub(ws: websockets.ClientConnection, flt: dict[str, Any]) -> list[dict[str, Any]]:
    """Send a sub and return the history (the mandatory first reply)."""
    await ws.send(json.dumps({"t": "sub", "filter": flt}))
    first = json.loads(await asyncio.wait_for(ws.recv(), 5.0))
    check(first.get("t") == "history", f"first /ws/logs message must be history, got {first.get('t')!r}")
    history = first.get("events")
    check(isinstance(history, list), "history events must be a list")
    check(len(history) <= 500, f"history must be capped at 500 events, got {len(history)}")
    for event in history:
        check_log_event_shape(event)
    check_strictly_increasing(history, "history")
    return history


async def collect_live(ws: websockets.ClientConnection, seconds: float) -> list[dict[str, Any]]:
    """Collect live batches for ``seconds`` (no other socket to pump)."""
    collector = LogCollector(ws)
    task = asyncio.create_task(collector.run())
    await asyncio.sleep(seconds)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    return collector.events


async def check_logs_unfiltered(sc: StateClient, base_ws_url: str) -> tuple[list[dict[str, Any]], str]:
    """Check 8: no filter => history, then a selftest seen live at every seam.

    Returns (io events seen live, job id) for the later checks.
    """
    async with websockets.connect(f"{base_ws_url}/ws/logs", max_size=LOGS_WS_MAX_SIZE) as ws:
        history = await logs_sub(ws, {})
        check(len(history) > 0, "unfiltered history must not be empty on a running core")

        collector = LogCollector(ws)
        task = asyncio.create_task(collector.run())
        params = {"note": "smoke check 8"}
        result = await sc.cmd(DEVICE_ID, "selftest", params, timeout=10.0)
        check(result["ok"] is True, f"selftest cmd_result not ok: {result.get('error')}")
        job_id = (result.get("data") or {}).get("jobId")
        check(isinstance(job_id, str) and job_id, "selftest cmd_result carries no jobId")
        await sc.pump_until(lambda: sc.node(job_id) is not None, 5.0, f"job node {job_id}")
        await sc.pump_until(lambda: sc.status(job_id) == "done", 60.0, "selftest (check 8) reaching done")
        await asyncio.sleep(0.5)  # let the last 100 ms batch flush
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        await ws.send(json.dumps({"t": "unsub"}))

    live = collector.events
    check(collector.batches >= 2, f"expected several live batches, got {collector.batches}")
    for event in live:
        check_log_event_shape(event)
    check_strictly_increasing(history + live, "history + live")

    # -- cmd seam: the received command (target id, action, full params) ...
    received = [
        e for e in live
        if e["category"] == "cmd" and (e.get("data") or {}).get("action") == "selftest"
        and "result" not in (e.get("data") or {})
    ]
    check(len(received) >= 1, "no cmd event for the received selftest command")
    cmd_event = received[-1]
    check(cmd_event["data"]["target"] == DEVICE_ID, f"cmd event target wrong: {cmd_event['data']}")
    check(cmd_event["data"]["params"] == params, f"cmd event must carry the full params, got {cmd_event['data']}")
    check(cmd_event["nodeId"] == DEVICE_ID, "cmd event must be attributed to the target node")
    check(DEVICE_ID in cmd_event["message"] and "selftest" in cmd_event["message"], "cmd message lacks target/action")
    check(cmd_event["source"] == "core", "cmd events are core (bus) events")
    # ... correlated to this very request (cmdId + issuing client) ...
    check(cmd_event["data"].get("cmdId") == result["cmdId"], f"cmd event must carry our cmdId, got {cmd_event['data']}")
    check(isinstance(cmd_event["data"].get("client"), str) and cmd_event["data"]["client"], "cmd event lacks the client")
    check(result["cmdId"] in cmd_event["message"], "cmd message must carry the cmdId (searchable)")
    # ... and its result with the jobId, carrying the same correlation
    results = [
        e for e in live
        if e["category"] == "cmd" and (e.get("data") or {}).get("action") == "selftest"
        and "result" in (e.get("data") or {}) and (e.get("data") or {}).get("cmdId") == result["cmdId"]
    ]
    check(len(results) == 1, f"expected exactly one cmd result event for cmdId {result['cmdId']}, got {len(results)}")
    cmd_result = results[-1]["data"]["result"]
    check(cmd_result.get("ok") is True, f"cmd result event not ok: {cmd_result}")
    check((cmd_result.get("data") or {}).get("jobId") == job_id, "cmd result event must carry the jobId")
    check(results[-1]["data"].get("client") == cmd_event["data"]["client"], "result event must carry the same client")
    check(cmd_event["n"] < results[-1]["n"], "cmd received must precede cmd result")

    # -- job seam: lifecycle of the plugin-declared job + its 4 steps
    job_events = [e for e in live if e["category"] == "job" and e.get("nodeId") == job_id]
    job_msgs = [e["message"] for e in job_events]
    check(any("created" in m for m in job_msgs), f"no 'created' job event for {job_id}: {job_msgs}")
    check(any(m.endswith(" started") for m in job_msgs), f"no 'started' job event for {job_id}: {job_msgs}")
    check(any(m.endswith(" done") for m in job_msgs), f"no 'done' job event for {job_id}: {job_msgs}")
    milestones = {
        int(m.split("progress ")[1].split("%")[0]) for m in job_msgs if "progress " in m and "%" in m
    }
    check({25, 50, 75} <= milestones, f"progress milestones incomplete: {sorted(milestones)}")
    check(all(e["source"] == PLUGIN_ID for e in job_events), "plugin job events must carry the plugin source")
    step_events = [
        e for e in live
        if e["category"] == "job" and isinstance(e.get("nodeId"), str) and e["nodeId"].startswith(job_id + ":")
    ]
    started_steps = {e["nodeId"] for e in step_events if e["message"].endswith(" started")
                     or " started (" in e["message"]}
    done_steps = {e["nodeId"] for e in step_events if e["message"].endswith(" done")}
    check(len(started_steps) == 4, f"expected 4 started steps, saw {len(started_steps)}: {sorted(started_steps)}")
    check(started_steps == done_steps, f"steps started != steps done: {sorted(started_steps ^ done_steps)}")
    check(all(job_id in m for m in job_msgs), "job messages must always carry the job id (never bare labels)")

    # -- io seam (SPEC 14.2): tx AND rx, valid hex, decoded present
    io_events = [e for e in live if e["category"] == "io"]
    check(len(io_events) >= 6, f"only {len(io_events)} io events during the self-test")
    directions = set()
    for event in io_events:
        check(event["nodeId"] == DEVICE_ID, f"io event on unexpected node {event['nodeId']!r}")
        check(event["source"] == PLUGIN_ID, "io events must carry the plugin source")
        check(event["level"] == "info", "io events are level info")
        data = event.get("data") or {}
        check(data.get("direction") in ("tx", "rx"), f"io event without tx/rx direction: {data}")
        directions.add(data["direction"])
        check_hex(event.get("raw"), f"io #{event['n']}")
        check(isinstance(data.get("decoded"), dict) and data["decoded"], f"io #{event['n']} without decoded dict")
        check(event["raw"] in event["message"], "io message must carry the raw hex")
    check(directions == {"tx", "rx"}, f"expected both tx and rx frames, saw {sorted(directions)}")
    check(io_events[0]["data"]["direction"] == "tx", "the first frame of a job must be a request (tx)")
    # a request is answered: every tx is followed by an rx before the next tx
    for previous, current in zip(io_events, io_events[1:]):
        if previous["data"]["direction"] == "tx":
            check(current["data"]["direction"] == "rx", f"tx #{previous['n']} not followed by an rx")

    # the command, the job and the frames were all bracketed by the state seam
    state_events = [e for e in live if e["category"] == "state"]
    check(all(e["level"] == "trace" for e in state_events), "state events must be level trace")
    check(any(e.get("nodeId") == job_id for e in state_events), f"no state patch event for job {job_id}")

    print(
        f"OK  8. logs unfiltered: history ({len(history)} events) then {len(live)} live events in "
        f"{collector.batches} batches: cmd (target+params+result), {len(job_events)} job + "
        f"{len(step_events)} step events, {len(io_events)} io frames (tx/rx, hex, decoded)"
    )
    return io_events, job_id


async def check_logs_subtree(base_ws_url: str) -> None:
    """Check 9: filter {nodeId: plugin:demo_vci} => only that subtree, but the
    whole subtree (device, channels, jobs, steps) across categories."""
    async with websockets.connect(f"{base_ws_url}/ws/logs", max_size=LOGS_WS_MAX_SIZE) as ws:
        history = await logs_sub(ws, {"nodeId": PLUGIN_ID})
        check(len(history) > 0, "subtree history must not be empty after the self-tests")
        outside = [e for e in history if not in_plugin_subtree(e.get("nodeId"))]
        check(not outside, f"subtree history contains foreign events: {[(e['n'], e.get('nodeId')) for e in outside[:5]]}")
        categories = {e["category"] for e in history}
        check(
            {"io", "job", "state"} <= categories,
            f"subtree history should span categories (io/job/state), saw {sorted(categories)}",
        )
        node_ids = {e["nodeId"] for e in history}
        check(
            len(node_ids) >= 3 and any(nid != PLUGIN_ID for nid in node_ids),
            f"subtree filter must match descendants, not just the node itself: {sorted(node_ids)[:6]}",
        )
        # live: the heartbeat (rpc tx/rx ping, trace) arrives within ~2 s and is
        # attributed to the plugin node; nothing from outside may slip through
        live = await collect_live(ws, 3.0)
        check(len(live) > 0, "no live subtree events within 3 s (heartbeat pings expected)")
        for event in live:
            check_log_event_shape(event)
            check(in_plugin_subtree(event.get("nodeId")), f"live event outside the subtree: {event['nodeId']!r}")
        check_strictly_increasing(history + live, "subtree history + live")
        await ws.send(json.dumps({"t": "unsub"}))
    # The same ring holds foreign events (e.g. this script's own /ws/state
    # connection, audited on the leaf node `system`): a filter on that node
    # must return exactly those and none of the dominating plugin traffic.
    async with websockets.connect(f"{base_ws_url}/ws/logs", max_size=LOGS_WS_MAX_SIZE) as ws:
        system_history = await logs_sub(ws, {"nodeId": "system"})
        await ws.send(json.dumps({"t": "unsub"}))
    check(len(system_history) > 0, "no events for node 'system' (expected /ws/state connection audits)")
    leaked = [e for e in system_history if e["nodeId"] != "system"]
    check(not leaked, f"'system' filter leaked events of other nodes: {[(e['n'], e['nodeId']) for e in leaked[:5]]}")
    check(
        any("/ws/state client" in e["message"] for e in system_history),
        "no /ws/state connection audit event attributed to 'system'",
    )
    print(
        f"OK  9. logs subtree filter {PLUGIN_ID}: {len(history)} history + {len(live)} live events, "
        f"all inside the subtree ({len(node_ids)} distinct nodes, categories {sorted(categories)}); "
        f"filter 'system' => {len(system_history)} events, none foreign"
    )


async def check_session_file(base_ws_url: str, host: str, live_io: list[dict[str, Any]]) -> None:
    """Check 10: the session JSONL exists, is valid and gap-free, and carries
    the io frames seen live (same n, same raw hex, same direction)."""
    async with websockets.connect(f"{base_ws_url}/ws/logs", max_size=LOGS_WS_MAX_SIZE) as ws:
        history = await logs_sub(ws, {"categories": ["system"], "search": "session started"})
        await ws.send(json.dumps({"t": "unsub"}))
    check(len(history) >= 1, "no 'session started' system event in the log history")
    started = history[0]
    check(started["category"] == "system" and "session started" in started["message"], "search filter mismatch")
    session_file = (started.get("data") or {}).get("sessionFile")
    check(isinstance(session_file, str) and session_file, "session-started event carries no data.sessionFile")
    # the audit sink is healthy: no "audit session file disabled" event anywhere
    async with websockets.connect(f"{base_ws_url}/ws/logs", max_size=LOGS_WS_MAX_SIZE) as ws:
        disabled = await logs_sub(ws, {"categories": ["system"], "levels": ["error"], "search": "audit session file"})
        await ws.send(json.dumps({"t": "unsub"}))
    check(not disabled, f"audit session file was disabled during the session: {disabled[0]['message'] if disabled else ''}")
    if host not in ("127.0.0.1", "localhost", "::1"):
        print(f"SKIP 10. session file {session_file}: core is remote ({host}), cannot read it")
        return
    path = Path(session_file)
    check(path.is_file(), f"session file does not exist: {path}")
    check(path.name.startswith("session-") and path.suffix == ".jsonl", f"unexpected session file name {path.name}")
    check(path.parent.name == "logs", f"session file must live in logs/, got {path.parent}")

    text = path.read_text(encoding="utf-8")
    lines = text.split("\n")
    trailing = lines.pop()  # "" after the final newline, or an in-progress partial line
    events: list[dict[str, Any]] = []
    for index, line in enumerate(lines, start=1):
        try:
            event = json.loads(line)
        except json.JSONDecodeError as exc:
            raise SmokeFailure(f"session file line {index} is not valid JSON: {exc}") from None
        check_log_event_shape(event)
        events.append(event)
    if trailing:
        try:
            events.append(json.loads(trailing))
        except json.JSONDecodeError:
            pass  # a partial line is still being written; tolerated
    check(len(events) > 0, "session file is empty")
    ns = [e["n"] for e in events]
    check(ns[0] == 1, f"session file must start at n=1, starts at {ns[0]}")
    check(ns == list(range(1, len(ns) + 1)), "session file is not gap-free (every event must be written)")
    check(events[0]["message"].startswith("session started"), "first line must be the session-started event")
    by_n = {e["n"]: e for e in events}
    check(len(live_io) > 0, "internal: no live io events to compare against")
    for live in live_io:
        stored = by_n.get(live["n"])
        check(stored is not None, f"live io event #{live['n']} missing from the session file")
        assert stored is not None
        check(stored["category"] == "io", f"file event #{live['n']} is not category io")
        check(stored["raw"] == live["raw"], f"file event #{live['n']} raw {stored['raw']!r} != live {live['raw']!r}")
        check(
            (stored.get("data") or {}).get("direction") == live["data"]["direction"],
            f"file event #{live['n']} direction differs from live",
        )
        check(
            (stored.get("data") or {}).get("decoded") == live["data"]["decoded"],
            f"file event #{live['n']} decoded differs from live",
        )
    file_raws = {e["raw"] for e in events if e["category"] == "io"}
    live_raws = {e["raw"] for e in live_io}
    check(live_raws <= file_raws, f"raw hex seen live but not in the file: {sorted(live_raws - file_raws)}")
    print(
        f"OK 10. session file {path.name}: {len(events)} valid gap-free JSONL events, "
        f"all {len(live_io)} live io frames present with identical raw hex"
    )


async def check_state_trace(base_ws_url: str, job_id: str) -> None:
    """Check 11: state patches are audited as category state / level trace —
    the check-8 job's add and its status=done update are in the history."""
    async with websockets.connect(f"{base_ws_url}/ws/logs", max_size=LOGS_WS_MAX_SIZE) as ws:
        history = await logs_sub(ws, {"nodeId": job_id, "categories": ["state"]})
        await ws.send(json.dumps({"t": "unsub"}))
    check(len(history) > 0, f"no state events for job {job_id}")
    for event in history:
        check(event["category"] == "state" and event["level"] == "trace", f"state event #{event['n']} not trace")
        op = (event.get("data") or {}).get("op")
        check(isinstance(op, dict) and "op" in op, f"state event #{event['n']} lacks data.op")
        check(isinstance((event.get("data") or {}).get("seq"), int), f"state event #{event['n']} lacks data.seq")
    adds = [e for e in history if e["data"]["op"]["op"] == "add" and e["data"]["op"]["node"]["id"] == job_id]
    # Plugin job ids are unique per process incarnation (SPEC 3.2 resolution),
    # so even after crashes/restarts and earlier smoke runs against the same
    # core this id has exactly one lifecycle in the ring.
    check(len(adds) == 1, f"expected exactly one 'add' patch event for {job_id}, got {len(adds)}")
    add = adds[0]
    check(add["nodeId"] == job_id and job_id in add["message"], "add event must name the node")
    done_updates = [
        e for e in history
        if e["nodeId"] == job_id and e["data"]["op"]["op"] == "update"
        and e["data"]["op"].get("changes", {}).get("status") == "done"
    ]
    check(len(done_updates) == 1, f"expected exactly one status=done update for {job_id}, got {len(done_updates)}")
    check('status="done"' in done_updates[0]["message"], f"update diff missing from message: {done_updates[0]['message']}")
    step_adds = [e for e in history if e["data"]["op"]["op"] == "add" and e["nodeId"] != job_id]
    check(len(step_adds) == 4, f"expected the 4 step adds in the job subtree history, got {len(step_adds)}")
    seqs = [e["data"]["seq"] for e in history]
    check(seqs == sorted(seqs), "state events must be in seq order")
    print(
        f"OK 11. state trace: {len(history)} trace/state events for {job_id} incl. its add, "
        f"4 step adds and the status=done update diff"
    )


# --------------------------------------------------------------------------


def default_port() -> int:
    raw = os.environ.get("DIAGCORE_PORT", "").strip()
    if raw:
        try:
            return int(raw)
        except ValueError:
            print(f"ignoring non-numeric DIAGCORE_PORT={raw!r}; using 8765", file=sys.stderr)
    return 8765


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument(
        "--port", type=int, default=default_port(), help="core port (default: $DIAGCORE_PORT or 8765)"
    )
    args = parser.parse_args()
    base = f"ws://{args.host}:{args.port}"

    print(f"Smoke test against {base} ...")
    async with websockets.connect(f"{base}/ws/state") as ws:
        sc = StateClient(ws)
        await sc.wait_hello()
        await check_hello(sc)
        await check_telemetry(base)
        await check_selftest_done(sc)
        await check_selftest_fail(sc)
        await check_crash_and_recover(sc)
        await check_cancel(sc)
        await check_logs_feed(sc, base)
        live_io, job_id = await check_logs_unfiltered(sc, base)
        await check_logs_subtree(base)
        await check_session_file(base, args.host, live_io)
        await check_state_trace(base, job_id)
    print("SMOKE PASSED: all 11 end-to-end checks green")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(asyncio.run(main()))
    except SmokeFailure as exc:
        print(f"SMOKE FAILED: {exc}", file=sys.stderr)
        sys.exit(1)
    except (ConnectionRefusedError, OSError) as exc:
        print(
            f"SMOKE FAILED: cannot connect ({exc}). Is the core running "
            "(python -m diagcore)?",
            file=sys.stderr,
        )
        sys.exit(1)
