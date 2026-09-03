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
12. vehicle hello (M1.5, SPEC 15): `vehicle` (vehicle.session, state none)
    offers `detect` with the demo plugin's `depth` select and NO `plugin`
    param (exactly one capable plugin); `vehicles` (vehicle.library) holds no
    demo profile; system.props.dataDir names the profile store
13. detect {depth: quick} => detecting/running, the plugin's job appears
    UNDER the session with 4 steps and climbing progress, io frames flow at
    the vehicle, 5 `ecu` nodes appear one by one, then connected with
    profileId/name/VIN/ecuCount=5 + the `ecu.probe` tool; the library gains
    one `vehicle.profile` card (first, lastSeen desc); the profile JSON in
    <dataDir>/vehicles/ matches SPEC 15.1; `vehicle`-category log events
    cover the mount grant, the profile upsert (full profile in data) and
    every state change, in order
14. probe via the ecu.probe tool: 0x10/UDS answers (data + tx/rx frames),
    0x2A/UDS times out after ~1.5 s (ok:false with the reason), 'zz' is
    rejected; both probes are audited as cmd events with their raw params
15. read_ids on one ecu => short job under that ecu node ends done and fills
    props.identification / lastReadMs
16. forget is refused while the profile is in use; disconnect => none, the
    whole session subtree removed, the mount grant revoked (logged after the
    grant, plugin answered vehicle.detach), the card untouched
17. select on the library card => attaching job under the session => connected
    again in < 4 s (no full scan), a second select meanwhile is refused as
    busy, lastSeen bumped (card + file)
18. crash the plugin while connected => session lost (status error, evidence
    marked error), then the supervisor restart returns it to none with an
    empty subtree, detect offered again, the library still has the profile
19. forget => card gone, profileCount back, file deleted, audited

The port defaults to ``DIAGCORE_PORT`` (env) or 8765; ``--port`` overrides.
Start the core with a throwaway ``DIAGCORE_DATA_DIR`` for a clean library;
against a reused core the M1.5 checks first end a leftover session and
forget leftover demo profiles so their assertions stay deterministic.
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
LOG_CATEGORIES = {"cmd", "state", "io", "job", "plugin", "system", "vehicle"}
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
# M1.5 vehicle session (SPEC 15) — checks 12-19
# --------------------------------------------------------------------------

SESSION_ID = "vehicle"
LIBRARY_ID = "vehicles"
VEHICLE_PLUGIN = "demo_vci"
VEHICLE_OWNER = f"plugin:{VEHICLE_PLUGIN}"
DEMO_NAME = "Demo Vehicle"
DEMO_VIN = "DEM0VEH1CLE000042"
DEMO_ECU_IDS = {"engine", "body", "brakes", "airbag", "cluster"}
#: SPEC 15.1 profile file shape.
PROFILE_KEYS = {"id", "name", "vin", "identifiers", "pluginId", "firstSeen", "lastSeen", "ecus", "notes"}
PROFILE_ECU_KEYS = {"id", "name", "address", "protocol", "props"}


class VehicleRun:
    """What the M1.5 checks hand to each other. Every id is taken from the
    tree (SPEC 3.1: ids are opaque) — never built by this script."""

    def __init__(self, local: bool) -> None:
        #: the core runs on this machine, so its files can be inspected
        self.local = local
        self.data_dir: str | None = None
        #: cards in the library before the first detect (foreign profiles stay)
        self.library_before = 0
        self.profile_id = ""
        self.card_id = ""
        self.tool_id = ""
        #: n of the detect's mount-grant event (the revoke must follow it)
        self.grant_n = 0


def session_props(sc: StateClient) -> dict[str, Any]:
    return dict((sc.node(SESSION_ID) or {}).get("props") or {})


def session_state(sc: StateClient) -> Any:
    return session_props(sc).get("state")


def action_names(node: dict[str, Any] | None) -> list[str]:
    return [a["name"] for a in ((node or {}).get("actions") or [])]


def profile_cards(sc: StateClient) -> list[dict[str, Any]]:
    return sc.mirror.children_of_type(LIBRARY_ID, "vehicle.profile")


def subtree_ids(sc: StateClient, node_id: str) -> list[str]:
    """Mirror-side descendants of ``node_id`` (without the node itself)."""
    out: list[str] = []
    stack = list((sc.node(node_id) or {}).get("children") or [])
    while stack:
        nid = stack.pop()
        out.append(nid)
        stack.extend((sc.node(nid) or {}).get("children") or [])
    return out


def profile_path(run: VehicleRun) -> Path | None:
    if not run.local or run.data_dir is None or not run.profile_id:
        return None
    return Path(run.data_dir) / "vehicles" / f"{run.profile_id}.json"


async def logs_history(base_ws_url: str, flt: dict[str, Any]) -> list[dict[str, Any]]:
    """One-shot /ws/logs history for ``flt`` (shape-checked, unsubscribed)."""
    async with websockets.connect(f"{base_ws_url}/ws/logs", max_size=LOGS_WS_MAX_SIZE) as ws:
        history = await logs_sub(ws, flt)
        await ws.send(json.dumps({"t": "unsub"}))
    return history


def detect_ready(sc: StateClient) -> bool:
    """State none: exactly `detect`, carrying the running plugin's params."""
    actions = (sc.node(SESSION_ID) or {}).get("actions") or []
    return len(actions) == 1 and actions[0]["name"] == "detect" and bool(actions[0].get("params"))


async def check_vehicle_hello(sc: StateClient, run: VehicleRun) -> None:
    """Check 12: the two well-known M1.5 nodes and the `detect` action shape."""
    m = sc.mirror
    session = sc.node(SESSION_ID)
    check(session is not None and session["type"] == "vehicle.session", "hello: `vehicle` node (vehicle.session) missing")
    assert session is not None
    check(session["parentId"] == m.root_id and session["owner"] == "core", "`vehicle` must be a core-owned child of root")
    library = sc.node(LIBRARY_ID)
    check(library is not None and library["type"] == "vehicle.library", "hello: `vehicles` node (vehicle.library) missing")
    assert library is not None
    check(library["parentId"] == m.root_id and library["owner"] == "core", "`vehicles` must be a core-owned child of root")
    system = m.children_of_type(m.root_id, "system")[0]
    data_dir = (system.get("props") or {}).get("dataDir")
    check(isinstance(data_dir, str) and data_dir, "system.props.dataDir must name the profile data dir (SPEC 15.1)")
    run.data_dir = data_dir
    lib_dir = (library.get("props") or {}).get("dir")
    check(isinstance(lib_dir, str) and Path(lib_dir) == Path(data_dir) / "vehicles", f"library props.dir must be <dataDir>/vehicles, got {lib_dir!r}")

    if session_state(sc) != "none":
        # a previous run (or a user) left a session behind: end it first
        result = await sc.cmd(SESSION_ID, "disconnect", timeout=15.0)
        check(result["ok"] is True, f"disconnect of the leftover session failed: {result.get('error')}")
        await sc.pump_until(lambda: session_state(sc) == "none", 10.0, "leftover session ending")
    # Leftover demo profiles (a core reused across runs) would make the
    # "library gains exactly one card" assertions ambiguous: forget them.
    for card in [c for c in profile_cards(sc) if (c.get("props") or {}).get("vin") == DEMO_VIN]:
        result = await sc.cmd(card["id"], "forget", timeout=10.0)
        check(result["ok"] is True, f"forget of leftover profile {card['id']} failed: {result.get('error')}")
        await sc.pump_until(lambda cid=card["id"]: sc.node(cid) is None, 10.0, f"leftover card {card['id']} removed")

    await sc.pump_until(lambda: detect_ready(sc), 15.0, "`detect` (and nothing else) with the demo plugin's detectParams")
    session = sc.node(SESSION_ID)
    assert session is not None
    check(session["status"] == "idle" and session.get("error") is None, f"session none must be idle without error: {session.get('error')}")
    props = session["props"]
    check(props.get("state") == "none", f"session state must be none, got {props.get('state')!r}")
    for key in ("profileId", "name", "vin", "pluginId", "since"):
        check(key in props and props[key] is None, f"session prop {key!r} must be null in state none: {props}")
    check(props.get("ecuCount") == 0, f"ecuCount must be 0 in state none: {props}")
    detect = session["actions"][0]
    names = [p["name"] for p in detect["params"]]
    check("plugin" not in names, f"one vehicle-capable plugin runs, so `detect` must carry no `plugin` select: {names}")
    check(names == ["depth"], f"detect params must be the demo plugin's `depth` only, got {names}")
    depth = detect["params"][0]
    check(
        depth["type"] == "select" and depth["required"] is True and depth.get("default") == "quick",
        f"depth param shape (select, required, default quick): {depth}",
    )
    check([o["value"] for o in depth["options"]] == ["quick", "full"], "depth options must be quick/full")
    check(all(isinstance(o.get("label"), str) and o["label"] for o in depth["options"]), "depth options need labels")
    check(detect.get("confirm") is None, "detect must not ask for confirmation")

    library = sc.node(LIBRARY_ID)
    assert library is not None
    cards = profile_cards(sc)
    check(library["status"] == "idle" and action_names(library) == [], f"library must be idle without actions: {library.get('error')}")
    check((library.get("props") or {}).get("profileCount") == len(cards), "library profileCount must equal its cards")
    check(len(cards) == len(library.get("children") or []), "library children must all be vehicle.profile nodes")
    check(not any((c.get("props") or {}).get("vin") == DEMO_VIN for c in cards), "no demo profile may exist before the first detect")
    run.library_before = len(cards)
    print(
        "OK 12. vehicle hello: `vehicle` none (detect with the `depth` select only, no `plugin` param), "
        f"`vehicles` {'empty' if not cards else f'{len(cards)} foreign card(s), no demo profile'}, dataDir {data_dir}"
    )


async def check_vehicle_detect(sc: StateClient, base_ws_url: str, run: VehicleRun) -> None:
    """Check 13: detect => job under the session (steps, progress, io frames),
    5 ecu nodes, connected + library card + profile file + vehicle log events."""
    async with websockets.connect(f"{base_ws_url}/ws/logs", max_size=LOGS_WS_MAX_SIZE) as ws:
        await logs_sub(ws, {"nodeId": SESSION_ID, "categories": ["io"]})
        collector = LogCollector(ws)
        task = asyncio.create_task(collector.run())

        started = time.monotonic()
        result = await sc.cmd(SESSION_ID, "detect", {"depth": "quick"}, timeout=15.0)
        check(result["ok"] is True, f"detect cmd_result not ok: {result.get('error')}")
        data = result.get("data") or {}
        job_id = data.get("jobNodeId")
        check(isinstance(job_id, str) and job_id, f"detect cmd_result carries no jobNodeId: {data}")
        check(data.get("pluginId") == VEHICLE_PLUGIN and data.get("state") == "detecting", f"detect result data: {data}")
        await sc.pump_until(lambda: sc.node(job_id) is not None, 5.0, f"detection job node {job_id}")
        job = sc.node(job_id)
        assert job is not None
        check(job["parentId"] == SESSION_ID and job["type"] == "job", f"detection job must be a `job` UNDER the session, parent is {job['parentId']!r}")
        check(job["owner"] == VEHICLE_OWNER, f"detection job must be plugin-owned (mount grant), owner is {job['owner']!r}")
        session = sc.node(SESSION_ID)
        assert session is not None
        check(session_state(sc) == "detecting" and session["status"] == "running", f"session must be detecting/running: {session['props']}")
        check(action_names(session) == ["disconnect"], f"while detecting only `disconnect` is offered, got {action_names(session)}")
        check(session["props"].get("pluginId") == VEHICLE_PLUGIN, "session pluginId must be set while detecting")

        progress_seen: list[float] = []
        ecu_counts: list[int] = []
        states: list[Any] = []
        connected_at: list[float] = []

        def track() -> bool:
            node = sc.node(job_id)
            if node is None:
                raise SmokeFailure(f"detection job {job_id} disappeared mid-run")
            p = node.get("progress")
            if isinstance(p, (int, float)) and (not progress_seen or p > progress_seen[-1]):
                progress_seen.append(float(p))
            n = len(sc.mirror.children_of_type(SESSION_ID, "ecu"))
            if not ecu_counts or n != ecu_counts[-1]:
                ecu_counts.append(n)
            state = session_state(sc)
            if not states or state != states[-1]:
                states.append(state)
            if state == "connected" and not connected_at:
                connected_at.append(time.monotonic())
            # The session turns `connected` on `vehicle.profile`, which the
            # plugin sends INSIDE its last step — the job's final progress and
            # `done` follow a moment later, so keep tracking until both are in.
            return state == "connected" and node.get("status") == "done"

        await sc.pump_until(track, 60.0, "session connected and detection job done after the quick detection")
        elapsed = connected_at[0] - started
        await asyncio.sleep(0.5)  # let the last 100 ms batch flush
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        await ws.send(json.dumps({"t": "unsub"}))

    check(states == ["detecting", "connected"], f"states seen during detection: {states}")
    check(len(progress_seen) >= 4 and progress_seen[-1] >= 0.99, f"progress did not climb to 1: {progress_seen}")
    check(progress_seen == sorted(progress_seen), "progress must be monotonically increasing")
    check(ecu_counts[-1] == 5 and len(ecu_counts) >= 3, f"ecu nodes must appear one by one during the scan: {ecu_counts}")
    steps = job_steps(sc, job_id)
    check(len(steps) == 4 and all(s["status"] == "done" for s in steps), f"expected 4 done steps, got {[(s['name'], s['status']) for s in steps]}")
    check(steps[0]["name"] == "Wake diagnostic bus" and steps[-1]["name"] == "Build profile", f"step names: {[s['name'] for s in steps]}")
    job = sc.node(job_id)
    assert job is not None
    check(job.get("actions") == [], "terminal detection job must expose no actions")

    session = sc.node(SESSION_ID)
    assert session is not None
    props = session["props"]
    check(session["status"] == "idle" and session.get("error") is None, "connected session must be idle without error")
    check(props["name"] == DEMO_NAME and props["vin"] == DEMO_VIN and props["pluginId"] == VEHICLE_PLUGIN, f"unexpected vehicle props: {props}")
    check(props["ecuCount"] == 5, f"expected ecuCount 5, got {props['ecuCount']}")
    check(isinstance(props.get("since"), (int, float)), f"connected session must carry `since`: {props}")
    profile_id = props.get("profileId")
    check(isinstance(profile_id, str) and profile_id, f"connected session must carry profileId: {props}")
    check(action_names(session) == ["detect", "disconnect"], f"connected session actions: {action_names(session)}")
    check(DEMO_NAME in session["name"], f"session node name should show the vehicle, got {session['name']!r}")

    ecus = sc.mirror.children_of_type(SESSION_ID, "ecu")
    tools = sc.mirror.children_of_type(SESSION_ID, "ecu.probe")
    check(len(ecus) == 5 and len(tools) == 1, f"expected 5 ecu + 1 ecu.probe children, got {len(ecus)} + {len(tools)}")
    for ecu in ecus:
        check(ecu["owner"] == VEHICLE_OWNER and ecu["status"] == "idle", f"ecu {ecu['id']} must be plugin-owned and idle")
        eprops = ecu.get("props") or {}
        for key in ("ecuId", "address", "protocol"):
            check(isinstance(eprops.get(key), str) and eprops[key], f"ecu {ecu['id']} props missing {key!r}: {eprops}")
        ident = eprops.get("identification")
        check(isinstance(ident, dict) and ident.get("vin") == DEMO_VIN, f"ecu {ecu['id']} identification not read during detection: {eprops}")
        check("read_ids" in action_names(ecu), f"ecu {ecu['id']} must offer read_ids")
    check({(e.get("props") or {}).get("ecuId") for e in ecus} == DEMO_ECU_IDS, f"ecu ids: {[(e.get('props') or {}).get('ecuId') for e in ecus]}")
    tool = tools[0]
    check(tool["owner"] == VEHICLE_OWNER, "probe tool must be plugin-owned")
    probe = next((a for a in tool.get("actions") or [] if a["name"] == "probe"), None)
    check(probe is not None, "probe tool must offer `probe`")
    assert probe is not None
    pnames = {p["name"]: p for p in probe["params"]}
    check(set(pnames) == {"address", "protocol"}, f"probe params must be address + protocol, got {sorted(pnames)}")
    check(pnames["address"]["type"] == "string" and pnames["address"]["required"] is True, f"address param: {pnames['address']}")
    check(
        pnames["protocol"]["type"] == "select" and [o["value"] for o in pnames["protocol"]["options"]] == ["UDS", "KWP"],
        f"protocol param: {pnames['protocol']}",
    )

    # -- the library gained exactly one card, first in line (lastSeen desc)
    cards = profile_cards(sc)
    check(len(cards) == run.library_before + 1, f"library must gain exactly one card: {run.library_before} -> {len(cards)}")
    card = next((c for c in cards if (c.get("props") or {}).get("id") == profile_id), None)
    check(card is not None, f"no vehicle.profile card for profile {profile_id}")
    assert card is not None
    check(card["id"] == f"vehicle:{profile_id}", f"profile node id must be vehicle:<profileId> (SPEC 15.3), got {card['id']}")
    check(cards[0]["id"] == card["id"], "the newest profile must be the first library child (lastSeen desc)")
    check(card["owner"] == "core" and card["status"] == "idle", "profile cards are core-owned and idle")
    cprops = card["props"]
    check(card["name"] == DEMO_NAME and cprops.get("vin") == DEMO_VIN and cprops.get("pluginId") == VEHICLE_PLUGIN, f"card props: {cprops}")
    check(cprops.get("ecuCount") == 5 and len(cprops.get("ecus") or []) == 5, f"card ecu summary: {cprops}")
    check(all(set(e) == {"id", "name", "address"} for e in cprops["ecus"]), "card ecus must be the compact {id, name, address} summary")
    check(isinstance(cprops.get("identifiers"), dict) and cprops["identifiers"], "card must carry the plugin's identifiers")
    check(isinstance(cprops.get("firstSeen"), (int, float)) and isinstance(cprops.get("lastSeen"), (int, float)), f"card firstSeen/lastSeen: {cprops}")
    check(action_names(card) == ["select", "rename", "forget"], f"card actions: {action_names(card)}")
    rename = card["actions"][1]
    check(
        [p["name"] for p in rename["params"]] == ["name"]
        and rename["params"][0]["type"] == "string"
        and rename["params"][0].get("default") == DEMO_NAME,
        f"rename must take a `name` string prefilled with the current name: {rename}",
    )
    forget = card["actions"][2]
    check(isinstance(forget.get("confirm"), str) and DEMO_NAME in forget["confirm"], f"forget must carry a confirm text naming the vehicle: {forget}")
    library = sc.node(LIBRARY_ID)
    assert library is not None
    check(library["props"].get("profileCount") == len(cards) and library["status"] == "idle", f"library after detect: {library['props']} {library['status']}")

    # -- io frames flowed at the vehicle: scan frames on the session node,
    #    identification frames on the ecu nodes (subtree filter)
    io_events = collector.events
    check(collector.batches >= 2 and len(io_events) >= 20, f"expected a stream of io frames, got {len(io_events)} in {collector.batches} batches")
    directions: set[Any] = set()
    io_nodes: set[Any] = set()
    for event in io_events:
        check_log_event_shape(event)
        check(event["category"] == "io" and event["source"] == VEHICLE_OWNER, f"io #{event['n']} category/source: {event['category']}/{event['source']}")
        check_hex(event.get("raw"), f"io #{event['n']}")
        directions.add((event.get("data") or {}).get("direction"))
        io_nodes.add(event["nodeId"])
    check(directions == {"tx", "rx"}, f"both tx and rx frames expected, saw {sorted(map(str, directions))}")
    ecu_ids = {e["id"] for e in ecus}
    check(SESSION_ID in io_nodes and io_nodes & ecu_ids, f"scan frames on the session and identification frames on the ecus expected, saw {sorted(io_nodes)}")
    tx = sum(1 for e in io_events if e["data"]["direction"] == "tx")
    rx = sum(1 for e in io_events if e["data"]["direction"] == "rx")
    check(tx >= 128 and rx >= 6, f"a quick scan sends one request per address (128) and 5 ECUs answer, saw {tx} tx / {rx} rx")

    # -- the profile file (SPEC 15.1)
    run.profile_id = profile_id
    run.card_id = card["id"]
    run.tool_id = tool["id"]
    path = profile_path(run)
    if path is not None:
        check(path.is_file(), f"profile file missing: {path}")
        stored = json.loads(path.read_text(encoding="utf-8"))
        check(set(stored) == PROFILE_KEYS, f"profile file keys must be exactly SPEC 15.1: {sorted(stored)}")
        check(
            stored["id"] == profile_id and stored["name"] == DEMO_NAME and stored["vin"] == DEMO_VIN and stored["pluginId"] == VEHICLE_PLUGIN,
            f"profile file content: {stored}",
        )
        check(isinstance(stored["identifiers"], dict) and isinstance(stored["notes"], str), "profile identifiers/notes types")
        check(
            isinstance(stored["firstSeen"], (int, float)) and isinstance(stored["lastSeen"], (int, float)) and stored["lastSeen"] >= stored["firstSeen"],
            f"profile timestamps: {stored['firstSeen']} / {stored['lastSeen']}",
        )
        check(len(stored["ecus"]) == 5 and all(set(e) == PROFILE_ECU_KEYS for e in stored["ecus"]), f"profile ecus must be full SPEC 15.1 entries: {stored['ecus']}")
        check({e["id"] for e in stored["ecus"]} == DEMO_ECU_IDS and all(isinstance(e["props"], dict) for e in stored["ecus"]), "profile ecu ids/props")
        check(stored["lastSeen"] == cprops["lastSeen"], "card lastSeen must mirror the file")
        check(not list(path.parent.glob(".*.tmp")), "no temp file may be left behind by the atomic write")
        file_note = f"file {path.name} = SPEC 15.1"
    else:
        file_note = "file check skipped (remote core)"

    # -- the vehicle category audited grant, profile upsert and state changes
    vehicle_events = await logs_history(base_ws_url, {"categories": ["vehicle"]})
    check(vehicle_events and all(e["category"] == "vehicle" for e in vehicle_events), "vehicle-category history expected")
    tail = [e["message"] for e in vehicle_events[-12:]]
    grants = [e for e in vehicle_events if e["message"].startswith(f"mount grant {SESSION_ID} -> plugin {VEHICLE_PLUGIN} (vehicle.detect)")]
    check(grants, f"no mount-grant event for the detect: {tail}")
    grant = grants[-1]
    check(grant["nodeId"] == SESSION_ID and (grant.get("data") or {}).get("pluginId") == VEHICLE_PLUGIN, f"grant event attribution: {grant}")
    check((grant.get("data") or {}).get("grants") == [SESSION_ID], f"grant event must list the plugin's grants: {grant.get('data')}")
    run.grant_n = grant["n"]
    upserts = [e for e in vehicle_events if e["message"].startswith(f"vehicle profile {profile_id} created")]
    check(upserts, f"no profile-created event for {profile_id}: {tail}")
    upsert = upserts[-1]
    check(upsert["nodeId"] == LIBRARY_ID, "profile upsert events belong to the library node")
    logged_profile = (upsert.get("data") or {}).get("profile")
    check(
        isinstance(logged_profile, dict) and set(logged_profile) == PROFILE_KEYS and logged_profile["id"] == profile_id,
        f"profile upsert event must carry the full profile in data: {upsert.get('data')}",
    )
    check((upsert.get("data") or {}).get("written") is True, "profile upsert event must confirm the file write")
    received = [e for e in vehicle_events if e["message"].startswith(f"vehicle.profile from plugin {VEHICLE_PLUGIN} for {SESSION_ID}")]
    check(received and received[-1]["source"] == VEHICLE_OWNER, f"no vehicle.profile-received event: {tail}")
    detecting = [e for e in vehicle_events if e["nodeId"] == SESSION_ID and "vehicle session none -> detecting" in e["message"]]
    check(detecting, f"no none -> detecting transition event: {tail}")
    check((detecting[-1].get("data") or {}).get("params") == {"depth": "quick"}, f"transition event must carry the detect params: {detecting[-1].get('data')}")
    connected = [e for e in vehicle_events if e["nodeId"] == SESSION_ID and "vehicle session detecting -> connected" in e["message"]]
    check(connected, f"no detecting -> connected transition event: {tail}")
    check((connected[-1].get("data") or {}).get("profileId") == profile_id, "connected event must carry the profileId")
    check(
        detecting[-1]["n"] < grant["n"] < received[-1]["n"] < upsert["n"] < connected[-1]["n"],
        "vehicle events out of order (expected: none->detecting, grant, profile received, upsert, ->connected)",
    )
    print(
        f"OK 13. detect (quick, {elapsed:.1f} s): job under `vehicle` (4 steps, progress {len(progress_seen)} updates), "
        f"{len(io_events)} io frames ({tx} tx / {rx} rx), ecus {ecu_counts} => connected ({DEMO_NAME}, VIN {DEMO_VIN}, "
        f"profile {profile_id}); card {card['id']} first in the library; {file_note}; "
        f"{len(vehicle_events)} vehicle events (grant, upsert, transitions in order)"
    )


async def check_vehicle_probe(sc: StateClient, base_ws_url: str, run: VehicleRun) -> None:
    """Check 14: the ecu.probe tool — a known address answers with data and
    a tx/rx pair, an unknown one times out (~1.5 s, ok:false with the reason),
    an invalid address is rejected; both probes are audited as cmd events."""
    known = {"address": "0x10", "protocol": "UDS"}
    unknown = {"address": "0x2A", "protocol": "UDS"}
    t0 = time.monotonic()
    ok_result = await sc.cmd(run.tool_id, "probe", known, timeout=15.0)
    known_elapsed = time.monotonic() - t0
    check(ok_result["ok"] is True, f"probe 0x10/UDS failed: {ok_result.get('error')}")
    data = ok_result.get("data") or {}
    check(data.get("ecuId") == "engine" and data.get("address") == "0x10" and data.get("protocol") == "UDS", f"probe data: {data}")
    check(isinstance(data.get("identification"), dict) and data["identification"].get("vin") == DEMO_VIN, f"probe identification: {data}")
    check(isinstance(data.get("responseTimeMs"), (int, float)), f"probe data must carry responseTimeMs: {data}")
    check(isinstance(data.get("name"), str) and data["name"], f"probe data must name the ECU: {data}")

    t0 = time.monotonic()
    fail_result = await sc.cmd(run.tool_id, "probe", unknown, timeout=15.0)
    unknown_elapsed = time.monotonic() - t0
    check(fail_result["ok"] is False, f"probe 0x2A must fail, got {fail_result}")
    message = (fail_result.get("error") or {}).get("message", "")
    check("no response from 0x2A (UDS)" in message, f"probe 0x2A error message: {message!r}")
    check(1.2 <= unknown_elapsed <= 6.0, f"probe timeout should take ~1.5 s, took {unknown_elapsed:.2f} s")
    check(known_elapsed < unknown_elapsed, "a known address must answer faster than the timeout")

    invalid = await sc.cmd(run.tool_id, "probe", {"address": "zz", "protocol": "UDS"}, timeout=15.0)
    check(invalid["ok"] is False and "invalid address" in (invalid.get("error") or {}).get("message", ""), f"probe 'zz': {invalid}")

    tool = sc.node(run.tool_id)
    assert tool is not None
    last = (tool.get("props") or {}).get("lastProbe")
    check(isinstance(last, dict) and last.get("address") == "0x2A" and last.get("ok") is False, f"tool props.lastProbe after the timeout: {last}")

    cmd_events = await logs_history(base_ws_url, {"nodeId": run.tool_id, "categories": ["cmd"]})
    for label, result, params, expect_ok in (("0x10", ok_result, known, True), ("0x2A", fail_result, unknown, False)):
        mine = [e for e in cmd_events if (e.get("data") or {}).get("cmdId") == result["cmdId"]]
        received = [e for e in mine if "result" not in (e.get("data") or {})]
        results = [e for e in mine if "result" in (e.get("data") or {})]
        check(len(received) == 1, f"probe {label}: expected one received cmd event, got {len(received)}")
        rx_data = received[0]["data"]
        check(rx_data["target"] == run.tool_id and rx_data["action"] == "probe", f"probe {label} cmd event target/action: {rx_data}")
        check(rx_data["params"] == params, f"probe {label} cmd event must carry the raw params {params}, got {rx_data.get('params')}")
        check(len(results) == 1, f"probe {label}: expected one cmd result event, got {len(results)}")
        logged = results[0]["data"]["result"]
        check(logged.get("ok") is expect_ok, f"probe {label} result event ok mismatch: {logged}")
        if not expect_ok:
            check("no response" in (logged.get("error") or {}).get("message", ""), f"timeout result event must carry the reason: {logged}")
        check(received[0]["n"] < results[0]["n"], f"probe {label}: received must precede result")
    io_events = await logs_history(base_ws_url, {"nodeId": run.tool_id, "categories": ["io"]})
    check(len(io_events) >= 4, f"probe frames on the tool node: expected tx/rx pairs, got {len(io_events)}")
    for event in io_events:
        check_hex(event.get("raw"), f"probe io #{event['n']}")
        check(event["nodeId"] == run.tool_id and event["source"] == VEHICLE_OWNER, f"probe io #{event['n']} attribution")
    dirs = [e["data"]["direction"] for e in io_events[-4:]]
    check(dirs == ["tx", "rx", "tx", "rx"], f"known probe = tx/rx, timeout = tx + timeout rx: {dirs}")
    check((io_events[-1]["data"].get("decoded") or {}).get("timeout") is True, "the timeout must be recorded as an rx frame with decoded.timeout")
    print(
        f"OK 14. probe: 0x10/UDS ok ({data['name']}, {known_elapsed * 1000:.0f} ms, tx/rx), 0x2A/UDS ok:false after "
        f"{unknown_elapsed:.2f} s ({message!r}), 'zz' rejected; both audited as cmd events with raw params"
    )


async def check_vehicle_read_ids(sc: StateClient, base_ws_url: str, run: VehicleRun) -> None:
    """Check 15: read_ids => short job under the ecu node, props filled."""
    ecu = next((e for e in sc.mirror.children_of_type(SESSION_ID, "ecu") if (e.get("props") or {}).get("ecuId") == "body"), None)
    check(ecu is not None, "ecu 'body' missing under the session")
    assert ecu is not None
    ecu_id = ecu["id"]
    before = (ecu.get("props") or {}).get("lastReadMs")
    result = await sc.cmd(ecu_id, "read_ids", timeout=15.0)
    check(result["ok"] is True, f"read_ids failed: {result.get('error')}")
    data = result.get("data") or {}
    job_id = data.get("jobId")
    check(isinstance(job_id, str) and job_id and data.get("ecuId") == "body", f"read_ids result data: {data}")
    await sc.pump_until(lambda: sc.node(job_id) is not None, 5.0, f"read_ids job node {job_id}")
    job = sc.node(job_id)
    assert job is not None
    check(job["parentId"] == ecu_id and job["type"] == "job", f"read_ids job must live under its ecu node, parent is {job['parentId']!r}")
    await sc.pump_until(lambda: sc.status(job_id) == "done", 20.0, "read_ids job done")
    steps = job_steps(sc, job_id)
    check(len(steps) == 1 and steps[0]["status"] == "done", f"read_ids job steps: {[(s['name'], s['status']) for s in steps]}")
    ecu = sc.node(ecu_id)
    assert ecu is not None
    props = ecu.get("props") or {}
    ident = props.get("identification")
    check(isinstance(ident, dict) and ident.get("vin") == DEMO_VIN and ident.get("partNumber"), f"identification not filled: {props}")
    last = props.get("lastReadMs")
    check(isinstance(last, (int, float)) and (before is None or last > before), f"lastReadMs must be filled by read_ids: {before} -> {last}")
    check(ecu["status"] == "idle", f"ecu must be idle after read_ids, is {ecu['status']}")
    io_events = await logs_history(base_ws_url, {"nodeId": ecu_id, "categories": ["io"]})
    check(len(io_events) >= 4, f"expected the detection + read_ids identification frames on the ecu, got {len(io_events)}")
    check([e["data"]["direction"] for e in io_events[-2:]] == ["tx", "rx"], "read_ids must be one tx/rx pair on the ecu node")
    print(f"OK 15. read_ids on {ecu_id}: job {job_id} under the ecu done, identification + lastReadMs filled, tx/rx audited")


async def check_vehicle_disconnect(sc: StateClient, base_ws_url: str, run: VehicleRun) -> None:
    """Check 16: forget refused while in use; disconnect => none, subtree
    removed, grant revoked (logged), card untouched."""
    refused = await sc.cmd(run.card_id, "forget", timeout=10.0)
    check(refused["ok"] is False and "disconnect first" in (refused.get("error") or {}).get("message", ""), f"forget while connected: {refused}")
    doomed = subtree_ids(sc, SESSION_ID)
    check(len(doomed) >= 8, f"expected the job, 5 ecus and the tool under the session, found {len(doomed)} nodes")
    result = await sc.cmd(SESSION_ID, "disconnect", timeout=15.0)
    check(result["ok"] is True, f"disconnect failed: {result.get('error')}")
    data = result.get("data") or {}
    check(data.get("previousState") == "connected" and data.get("state") == "none", f"disconnect result data: {data}")
    await sc.pump_until(
        lambda: session_state(sc) == "none" and not (sc.node(SESSION_ID) or {}).get("children"),
        10.0,
        "session none with an empty subtree",
    )
    gone = [nid for nid in doomed if sc.node(nid) is None]
    check(len(gone) == len(doomed), f"the whole session subtree must be removed, still present: {[n for n in doomed if sc.node(n)]}")
    session = sc.node(SESSION_ID)
    assert session is not None
    check(session["status"] == "idle" and session.get("error") is None, f"session none must be idle: {session['status']} {session.get('error')}")
    check(action_names(session) == ["detect"], f"session none must offer detect only, got {action_names(session)}")
    props = session["props"]
    check(
        all(props[k] is None for k in ("profileId", "name", "vin", "pluginId", "since")) and props["ecuCount"] == 0,
        f"session props must be cleared: {props}",
    )
    check(sc.node(run.card_id) is not None, "disconnect must not touch the stored profile")
    events = await logs_history(base_ws_url, {"nodeId": SESSION_ID, "categories": ["vehicle"]})
    revoked = [e for e in events if e["message"].startswith(f"mount grant {SESSION_ID} revoked from plugin {VEHICLE_PLUGIN} (vehicle.detach)")]
    check(revoked, f"no grant-revoked event for the disconnect: {[e['message'] for e in events[-8:]]}")
    check(revoked[-1]["n"] > run.grant_n, "the revoke must follow the detect's grant")
    check((revoked[-1].get("data") or {}).get("grants") == [], f"after the revoke the plugin must hold no grants: {revoked[-1].get('data')}")
    detached = [e for e in events if f"vehicle.detach by plugin {VEHICLE_PLUGIN} ok" in e["message"]]
    check(detached and detached[-1]["n"] > revoked[-1]["n"], "vehicle.detach must be answered ok by the demo plugin (after the revoke)")
    ended = [e for e in events if "vehicle session connected -> none: disconnect requested" in e["message"]]
    check(ended and ended[-1]["n"] > detached[-1]["n"], "no connected -> none transition event after the detach")
    print(
        f"OK 16. disconnect: forget refused while in use; => none, {len(doomed)} subtree nodes removed, grant revoked "
        f"(event #{revoked[-1]['n']} > grant #{run.grant_n}; post-revoke mutations are dropped per "
        "tests/test_vehicle_session.py::test_mutations_under_session_need_a_grant), card kept"
    )


async def check_vehicle_select(sc: StateClient, run: VehicleRun) -> None:
    """Check 17: select on the card => attaching job => connected without a
    full scan (< 4 s), busy rule, lastSeen bumped."""
    card = sc.node(run.card_id)
    assert card is not None
    last_seen_before = card["props"]["lastSeen"]
    started = time.monotonic()
    result = await sc.cmd(run.card_id, "select", timeout=15.0)
    check(result["ok"] is True, f"select failed: {result.get('error')}")
    data = result.get("data") or {}
    job_id = data.get("jobNodeId")
    check(isinstance(job_id, str) and job_id, f"select result must carry the attach job: {data}")
    check(data.get("pluginId") == VEHICLE_PLUGIN and data.get("state") == "attaching", f"select result data: {data}")
    session = sc.node(SESSION_ID)
    assert session is not None
    check(session_state(sc) == "attaching" and session["status"] == "running", f"session must be attaching/running: {session['props']}")
    props = session["props"]
    check(props["profileId"] == run.profile_id and props["name"] == DEMO_NAME and props["vin"] == DEMO_VIN, f"attaching session must carry the known profile: {props}")
    check(action_names(session) == ["disconnect"], f"while attaching only `disconnect` is offered, got {action_names(session)}")
    busy = await sc.cmd(run.card_id, "select", timeout=15.0)
    check(busy["ok"] is False and "busy" in (busy.get("error") or {}).get("message", ""), f"a second select while attaching must be refused: {busy}")
    await sc.pump_until(lambda: sc.node(job_id) is not None, 5.0, f"attach job node {job_id}")
    job = sc.node(job_id)
    assert job is not None
    check(job["parentId"] == SESSION_ID and job["type"] == "job" and job["owner"] == VEHICLE_OWNER, f"attach job must be plugin-owned under the session: {job['parentId']} {job['owner']}")
    states: list[Any] = []

    def track() -> bool:
        state = session_state(sc)
        if not states or state != states[-1]:
            states.append(state)
        return state == "connected"

    await sc.pump_until(track, 20.0, "session connected after the attach")
    elapsed = time.monotonic() - started
    check(elapsed < 4.0, f"a known vehicle must attach without a full scan (took {elapsed:.1f} s)")
    check(states == ["attaching", "connected"], f"states seen during attach: {states}")
    await sc.pump_until(lambda: sc.status(job_id) == "done", 10.0, "attach job done")
    props = session_props(sc)
    check(props["ecuCount"] == 5 and props["profileId"] == run.profile_id and props["pluginId"] == VEHICLE_PLUGIN, f"connected session props after attach: {props}")
    ecus = sc.mirror.children_of_type(SESSION_ID, "ecu")
    tools = sc.mirror.children_of_type(SESSION_ID, "ecu.probe")
    check(len(ecus) == 5 and len(tools) == 1, f"attach must re-declare 5 ecus + the tool, got {len(ecus)} + {len(tools)}")
    check(all(e["status"] == "idle" for e in ecus), "re-declared ecus must be idle")
    run.tool_id = tools[0]["id"]
    card = sc.node(run.card_id)
    assert card is not None
    check(card["props"]["lastSeen"] > last_seen_before, f"lastSeen must be bumped on attach: {last_seen_before} -> {card['props']['lastSeen']}")
    check(profile_cards(sc)[0]["id"] == run.card_id, "the attached profile must stay first in the library")
    path = profile_path(run)
    if path is not None:
        stored = json.loads(path.read_text(encoding="utf-8"))
        check(stored["lastSeen"] == card["props"]["lastSeen"], "the file must carry the bumped lastSeen")
    print(
        f"OK 17. select {run.card_id}: attaching (job {job['name']!r} under `vehicle`, second select refused as busy) "
        f"=> connected in {elapsed:.1f} s without a scan, 5 ecus re-declared, lastSeen bumped"
    )


async def check_vehicle_lost_and_recover(sc: StateClient, base_ws_url: str, run: VehicleRun) -> None:
    """Check 18: plugin crash while connected => lost (error), restart => none
    with an empty subtree; the library keeps the profile."""
    ecu_ids = [e["id"] for e in sc.mirror.children_of_type(SESSION_ID, "ecu")]
    check(len(ecu_ids) == 5 and session_state(sc) == "connected", "precondition: connected with 5 ecus")
    await sc.send_cmd(DEVICE_ID, "crash")
    await sc.pump_until(lambda: session_state(sc) == "lost", 15.0, "session lost after the plugin crash")
    session = sc.node(SESSION_ID)
    assert session is not None
    check(session["status"] == "error", f"lost session must be status error, is {session['status']}")
    check((session.get("error") or {}).get("message") == "plugin process died", f"lost session error: {session.get('error')}")
    check(session["props"]["profileId"] == run.profile_id and session["props"]["name"] == DEMO_NAME, "lost session keeps the profile reference")
    check(all(sc.status(eid) == "error" for eid in ecu_ids), f"plugin-owned evidence under the session must be marked error: {[sc.status(e) for e in ecu_ids]}")
    check(action_names(session) == ["detect", "disconnect"], f"lost session must offer the ways out, got {action_names(session)}")
    states: list[Any] = ["lost"]

    def recovered() -> bool:
        state = session_state(sc)
        if state != states[-1]:
            states.append(state)
        if state != "none" or sc.status(PLUGIN_ID) not in ("idle", "running"):
            return False
        return sc.status(DEVICE_ID) == "idle" and all(cid in sc.mirror.nodes for cid in CHANNEL_IDS)

    await sc.pump_until(recovered, 30.0, "supervisor restart returning the session to none")
    check(states == ["lost", "none"], f"states seen after the crash: {states}")
    session = sc.node(SESSION_ID)
    assert session is not None
    check(not session.get("children"), "the old session subtree must be removed after the restart")
    check(session["status"] == "idle" and session.get("error") is None, f"session none after restart: {session['status']} {session.get('error')}")
    check(all(sc.node(eid) is None for eid in ecu_ids), "the error-marked ecus must be gone after the restart")
    await sc.pump_until(lambda: detect_ready(sc), 10.0, "`detect` offered again with params after the restart")
    library = sc.node(LIBRARY_ID)
    assert library is not None
    check(sc.node(run.card_id) is not None, "the library must still hold the profile after the crash")
    check(library["props"].get("profileCount") == run.library_before + 1 and library["status"] == "idle", f"library after the crash: {library['props']}")
    events = await logs_history(base_ws_url, {"nodeId": SESSION_ID, "categories": ["vehicle"]})
    lost = [e for e in events if f"vehicle session connected -> lost: plugin {VEHICLE_PLUGIN} died" in e["message"]]
    check(lost and lost[-1]["level"] == "error", f"no connected -> lost error event: {[e['message'] for e in events[-8:]]}")
    revoked = [e for e in events if e["message"].startswith(f"mount grant {SESSION_ID} revoked from plugin {VEHICLE_PLUGIN} (plugin died")]
    check(revoked and revoked[-1]["n"] < lost[-1]["n"], "the death must revoke the grant before the session is marked lost")
    back = [e for e in events if f"vehicle session lost -> none: plugin {VEHICLE_PLUGIN} restarted" in e["message"]]
    check(back and back[-1]["n"] > lost[-1]["n"], "no lost -> none event after the restart")
    print(
        f"OK 18. crash while connected: session lost (error 'plugin process died', 5 ecus marked error, grant revoked) "
        f"=> restart => none with an empty subtree, detect offered again, library keeps {run.card_id}"
    )


async def check_vehicle_forget(sc: StateClient, base_ws_url: str, run: VehicleRun) -> None:
    """Check 19: forget => card gone, file deleted, audited."""
    path = profile_path(run)
    if path is not None:
        check(path.is_file(), f"precondition: profile file exists: {path}")
    result = await sc.cmd(run.card_id, "forget", timeout=10.0)
    check(result["ok"] is True, f"forget failed: {result.get('error')}")
    check((result.get("data") or {}).get("profileId") == run.profile_id, f"forget result data: {result.get('data')}")
    await sc.pump_until(lambda: sc.node(run.card_id) is None, 10.0, f"card {run.card_id} removed")
    library = sc.node(LIBRARY_ID)
    assert library is not None
    check(
        library["props"].get("profileCount") == run.library_before and len(profile_cards(sc)) == run.library_before,
        f"library must be back to {run.library_before} card(s): {library['props']}",
    )
    check(library["status"] == "idle" and action_names(library) == [], f"library after forget: {library['status']} {library.get('error')}")
    if path is not None:
        check(not path.exists(), f"profile file must be deleted: {path}")
    events = await logs_history(base_ws_url, {"nodeId": LIBRARY_ID, "categories": ["vehicle"], "search": "forgotten"})
    forgotten = [
        e for e in events
        if e["message"].startswith(f"vehicle profile {run.profile_id} forgotten")
        and ((e.get("data") or {}).get("profile") or {}).get("id") == run.profile_id
    ]
    check(forgotten, f"no forgotten event with the full profile for {run.profile_id}")
    print(
        f"OK 19. forget {run.card_id}: card gone, library back to {run.library_before}, "
        f"{'file deleted' if path is not None else 'file check skipped (remote core)'}, audited with the full profile"
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
        run = VehicleRun(local=args.host in ("127.0.0.1", "localhost", "::1"))
        await check_vehicle_hello(sc, run)
        await check_vehicle_detect(sc, base, run)
        await check_vehicle_probe(sc, base, run)
        await check_vehicle_read_ids(sc, base, run)
        await check_vehicle_disconnect(sc, base, run)
        await check_vehicle_select(sc, run)
        await check_vehicle_lost_and_recover(sc, base, run)
        await check_vehicle_forget(sc, base, run)
    print("SMOKE PASSED: all 19 end-to-end checks green")
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
