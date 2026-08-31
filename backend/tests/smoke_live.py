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

Exit code 0 = all green; any assertion failure exits 1 with a message.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from typing import Any, Callable

import websockets

PLUGIN_ID = "plugin:demo_vci"
DEVICE_ID = f"{PLUGIN_ID}:device"
CHANNEL_IDS = [f"{DEVICE_ID}:ch:{key}" for key in ("sine", "noise", "saw")]


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
    for key in ("version", "startedAt", "pythonVersion"):
        check(key in (system.get("props") or {}), f"hello: system.props missing {key!r}")
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


# --------------------------------------------------------------------------


async def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
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
    print("SMOKE PASSED: all 6 end-to-end checks green")
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
