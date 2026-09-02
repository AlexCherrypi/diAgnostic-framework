"""End-to-end: real app + real demo_vci plugin over real WebSockets.

Starts uvicorn on an ephemeral port with the actual composition root, which
spawns the demo_vci plugin process. Asserts the hello snapshot, drives the
self-test job via /ws/state commands (running => done via patches, including
seq-gap checking), the failing self-test, cancellation, and telemetry
subscription on /ws/telemetry.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any, AsyncIterator, Callable

import httpx
import pytest
import uvicorn
from websockets.asyncio.client import connect

from diagcore.app import AppCore
from diagcore.loghub import LogFilter
from diagcore.server import create_app

from util import apply_ops

PLUGIN_ID = "plugin:demo_vci"
DEVICE_ID = f"{PLUGIN_ID}:device"
CHANNEL_IDS = [f"{DEVICE_ID}:ch:{key}" for key in ("sine", "noise", "saw")]

#: Speed the demo self-test up for the test run (4 steps x 0.4 s ~= 1.6 s).
FAST_STEP_SECONDS = "0.4"


class StateClient:
    """A minimal frontend mirror: applies hello/patch, checks seq continuity."""

    def __init__(self, ws) -> None:
        self._ws = ws
        self.state: dict[str, Any] = {"rootId": None, "nodes": {}}
        self.seq: int | None = None
        self.results: dict[str, dict[str, Any]] = {}
        self._cmd_counter = 0

    @property
    def nodes(self) -> dict[str, dict[str, Any]]:
        return self.state["nodes"]

    def node(self, node_id: str) -> dict[str, Any]:
        return self.nodes.get(node_id, {})

    async def send_cmd(self, target: str, action: str, params: dict | None = None) -> str:
        self._cmd_counter += 1
        cmd_id = f"c{self._cmd_counter}"
        await self._ws.send(
            json.dumps(
                {"t": "cmd", "cmdId": cmd_id, "target": target, "action": action, "params": params or {}}
            )
        )
        return cmd_id

    async def recv_until(
        self,
        predicate: Callable[["StateClient"], Any],
        timeout: float = 15.0,
        what: str = "condition",
    ) -> Any:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while True:
            value = predicate(self)
            if value:
                return value
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise AssertionError(f"e2e timeout waiting for {what}")
            try:
                raw = await asyncio.wait_for(self._ws.recv(), timeout=remaining)
            except asyncio.TimeoutError:
                raise AssertionError(f"e2e timeout waiting for {what}") from None
            self._apply(json.loads(raw))

    async def wait_result(self, cmd_id: str, timeout: float = 15.0) -> dict[str, Any]:
        return await self.recv_until(
            lambda c: c.results.get(cmd_id), timeout=timeout, what=f"cmd_result {cmd_id}"
        )

    def _apply(self, message: dict[str, Any]) -> None:
        kind = message.get("t")
        if kind == "hello":
            self.seq = message["seq"]
            snapshot = message["snapshot"]
            self.state = {"rootId": snapshot["rootId"], "nodes": dict(snapshot["nodes"])}
        elif kind == "patch":
            assert self.seq is not None, "patch before hello"
            assert message["seq"] == self.seq + 1, (
                f"seq gap: had {self.seq}, got {message['seq']}"
            )
            self.seq = message["seq"]
            apply_ops(self.state, message["ops"])
        elif kind == "cmd_result":
            self.results[message["cmdId"]] = message
        else:
            raise AssertionError(f"unexpected message type: {kind!r}")


@pytest.fixture
async def running_server(monkeypatch, tmp_path) -> AsyncIterator[tuple[AppCore, int]]:
    monkeypatch.setenv("DEMO_VCI_STEP_SECONDS", FAST_STEP_SECONDS)
    # default plugins dir => backend/plugins => demo_vci
    core = AppCore(log_dir=tmp_path / "logs")
    app = create_app(core)
    config = uvicorn.Config(app, host="127.0.0.1", port=0, log_level="warning", lifespan="on")
    server = uvicorn.Server(config)
    serve_task = asyncio.create_task(server.serve())
    try:
        deadline = asyncio.get_running_loop().time() + 15.0
        while not server.started:
            if serve_task.done() or asyncio.get_running_loop().time() > deadline:
                raise AssertionError("uvicorn did not start")
            await asyncio.sleep(0.02)
        port = server.servers[0].sockets[0].getsockname()[1]
        yield core, port
    finally:
        server.should_exit = True
        await asyncio.wait_for(serve_task, timeout=20.0)


def job_steps(client: StateClient, job_id: str) -> list[dict[str, Any]]:
    return [client.node(cid) for cid in client.node(job_id).get("children", [])]


async def test_full_state_flow(running_server: tuple[AppCore, int]) -> None:
    core, port = running_server
    async with connect(f"ws://127.0.0.1:{port}/ws/state", max_size=8 * 1024 * 1024) as ws:
        client = StateClient(ws)

        # -- hello snapshot -------------------------------------------------
        await client.recv_until(lambda c: c.seq is not None, what="hello")
        assert client.state["rootId"] == "root"
        for well_known in ("root", "system", "plugins", "jobs"):
            assert well_known in client.nodes, f"missing well-known node {well_known}"
        assert client.node("root")["children"] == ["system", "plugins", "jobs"]
        assert PLUGIN_ID in client.nodes
        assert client.node(PLUGIN_ID)["parentId"] == "plugins"

        # -- plugin declares its subtree ------------------------------------
        await client.recv_until(
            lambda c: c.node(PLUGIN_ID).get("status") == "idle"
            and c.node(DEVICE_ID).get("status") == "idle"
            and all(ch in c.nodes for ch in CHANNEL_IDS),
            what="demo_vci declared and idle",
        )
        device = client.node(DEVICE_ID)
        assert device["owner"] == "plugin:demo_vci"
        assert [a["name"] for a in device["actions"]] == ["selftest", "selftest_fail", "crash"]
        for ch in CHANNEL_IDS:
            props = client.node(ch)["props"]
            assert props["sampleRateHz"] == 20.0
            assert props["unit"]
        assert client.node(CHANNEL_IDS[0])["props"]["unit"] == "V"

        # -- selftest: running => done via patches --------------------------
        cmd_id = await client.send_cmd(DEVICE_ID, "selftest")
        result = await client.wait_result(cmd_id)
        assert result["ok"] is True
        job_id = result["data"]["jobId"]

        await client.recv_until(
            lambda c: c.node(job_id).get("status") == "running", what="job running"
        )
        assert client.node(job_id)["parentId"] == DEVICE_ID
        await client.recv_until(
            lambda c: c.node(job_id).get("status") == "done", timeout=30.0, what="job done"
        )
        job = client.node(job_id)
        assert job["progress"] == 1.0
        steps = job_steps(client, job_id)
        assert len(steps) == 4
        assert all(s["status"] == "done" for s in steps)
        assert all(s["type"] == "job.step" for s in steps)

        # -- selftest_fail: error on job AND step 3 -------------------------
        cmd_id = await client.send_cmd(DEVICE_ID, "selftest_fail")
        result = await client.wait_result(cmd_id)
        assert result["ok"] is True
        fail_job_id = result["data"]["jobId"]
        await client.recv_until(
            lambda c: c.node(fail_job_id).get("status") == "error", timeout=30.0, what="job error"
        )
        fail_job = client.node(fail_job_id)
        assert "garbage" in fail_job["error"]["message"]
        steps = job_steps(client, fail_job_id)
        assert len(steps) == 3  # step 4 never started
        assert [s["status"] for s in steps] == ["done", "done", "error"]
        assert "garbage" in steps[2]["error"]["message"]

        # -- cancel: cancelled on job and running step ----------------------
        cmd_id = await client.send_cmd(DEVICE_ID, "selftest")
        result = await client.wait_result(cmd_id)
        cancel_job_id = result["data"]["jobId"]
        await client.recv_until(
            lambda c: c.node(cancel_job_id).get("status") == "running", what="job running"
        )
        cancel_cmd = await client.send_cmd(cancel_job_id, "cancel")
        cancel_result = await client.wait_result(cancel_cmd)
        assert cancel_result["ok"] is True
        await client.recv_until(
            lambda c: c.node(cancel_job_id).get("status") == "cancelled",
            timeout=30.0,
            what="job cancelled",
        )
        steps = job_steps(client, cancel_job_id)
        assert steps, "cancelled job should have at least one step"
        assert steps[-1]["status"] == "cancelled"

        # -- unknown action goes through the plugin and comes back ok:false -
        cmd_id = await client.send_cmd(DEVICE_ID, "no_such_action")
        result = await client.wait_result(cmd_id)
        assert result["ok"] is False
        assert "unknown action" in result["error"]["message"]

        # -- resync ---------------------------------------------------------
        await ws.send(json.dumps({"t": "resync"}))
        client.seq = None  # accept the next hello
        await client.recv_until(lambda c: c.seq is not None, what="hello after resync")
        assert client.node(job_id)["status"] == "done"


async def test_ws_logs_history_live_and_raw_io_feed(running_server: tuple[AppCore, int]) -> None:
    """/ws/logs against the real app (SPEC 14.3): sub => history, live batched
    events, subtree+category filtering, filter replacement, unsub — and the
    demo plugin's simulated io frames + the session JSONL file (SPEC 14.2)."""
    core, port = running_server
    async with connect(f"ws://127.0.0.1:{port}/ws/state", max_size=8 * 1024 * 1024) as state_ws:
        client = StateClient(state_ws)
        await client.recv_until(
            lambda c: c.node(DEVICE_ID).get("status") == "idle", what="demo_vci declared"
        )

        async with connect(f"ws://127.0.0.1:{port}/ws/logs", max_size=8 * 1024 * 1024) as logs_ws:
            # sub to the device subtree's raw io feed BEFORE the job runs
            await logs_ws.send(
                json.dumps({"t": "sub", "filter": {"nodeId": DEVICE_ID, "categories": ["io"]}})
            )
            first = json.loads(await asyncio.wait_for(logs_ws.recv(), timeout=5.0))
            assert first["t"] == "history"
            assert first["events"] == []  # nothing exchanged yet

            # run a self-test => the plugin simulates UDS-ish tx/rx frames
            cmd_id = await client.send_cmd(DEVICE_ID, "selftest")
            result = await client.wait_result(cmd_id)
            assert result["ok"] is True
            job_id = result["data"]["jobId"]

            io_events: list[dict] = []
            deadline = asyncio.get_running_loop().time() + 15.0
            while len(io_events) < 8:
                remaining = deadline - asyncio.get_running_loop().time()
                assert remaining > 0, "no io events within 15 s"
                message = json.loads(await asyncio.wait_for(logs_ws.recv(), timeout=remaining))
                assert message["t"] == "events"
                assert isinstance(message["events"], list)  # live events are batched
                io_events.extend(message["events"])

            for event in io_events:
                assert event["category"] == "io"
                assert event["level"] == "info"
                assert event["source"] == "plugin:demo_vci"
                assert event["nodeId"] == DEVICE_ID
                assert event["data"]["direction"] in ("tx", "rx")
                int(event["raw"], 16)  # raw payload is a hex string
            # tx/rx pairs: request first, response second
            assert io_events[0]["data"]["direction"] == "tx"
            assert io_events[1]["data"]["direction"] == "rx"
            assert any((e["data"].get("decoded") or {}).get("service") for e in io_events)
            ns = [e["n"] for e in io_events]
            assert ns == sorted(ns)  # monotonic event numbers, in order

            # let the job finish, then replace the filter (ONE per connection):
            # the job subtree's category-job history shows the full lifecycle
            await client.recv_until(
                lambda c: c.node(job_id).get("status") == "done", timeout=30.0, what="job done"
            )
            await logs_ws.send(
                json.dumps({"t": "sub", "filter": {"nodeId": job_id, "categories": ["job"]}})
            )
            history = None
            while history is None:
                message = json.loads(await asyncio.wait_for(logs_ws.recv(), timeout=5.0))
                if message["t"] == "history":  # skip io batches still in flight
                    history = message
            job_messages = [e["message"] for e in history["events"]]
            assert any("created" in m and job_id in m for m in job_messages)
            assert any("started" in m for m in job_messages)
            assert any("progress 25%" in m for m in job_messages)
            assert any("progress 75%" in m for m in job_messages)
            assert any(job_id in m and "done" in m for m in job_messages)
            assert any("step" in m for m in job_messages)  # step lifecycle included

            # unsub stops the feed: a second self-test (noisy: io + job + cmd
            # + state events) must deliver nothing on this connection
            await logs_ws.send(json.dumps({"t": "unsub"}))
            await asyncio.sleep(0.3)  # let the unsub land and in-flight batches drain
            while True:  # drain anything already queued
                try:
                    await asyncio.wait_for(logs_ws.recv(), timeout=0.2)
                except asyncio.TimeoutError:
                    break
            cmd_id = await client.send_cmd(DEVICE_ID, "selftest")
            result = await client.wait_result(cmd_id)
            assert result["ok"] is True
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(logs_ws.recv(), timeout=1.0)

    # the session JSONL file is the complete audit trail: valid line-by-line
    # JSON, promptly flushed, io events carrying the raw hex payloads
    session_path = core.logs.session_path
    assert session_path is not None and session_path.exists()
    lines = session_path.read_text(encoding="utf-8").splitlines()
    events = []
    for i, line in enumerate(lines):
        try:
            events.append(json.loads(line))
        except json.JSONDecodeError:
            # the server is still writing: at most one truncated trailing line
            assert i == len(lines) - 1, f"corrupt session file line {i}"
    assert [e["n"] for e in events] == list(range(1, len(events) + 1))  # gap-free
    file_io = [e for e in events if e["category"] == "io"]
    assert len(file_io) >= len(io_events)
    assert all(e["raw"] for e in file_io)
    assert any(e["category"] == "cmd" and "'selftest'" in e["message"] for e in events)
    assert any(e["category"] == "plugin" and "spawned pid=" in e["message"] for e in events)
    assert any(e["category"] == "state" for e in events)


async def test_plugin_crash_ends_running_job_and_restart_gets_fresh_job_ids(
    running_server: tuple[AppCore, int],
) -> None:
    """SPEC 14.1/14.4 with the real demo plugin: a job killed with its plugin
    gets a terminal `job` error event in ITS subtree, and jobs created by the
    restarted plugin never reuse a pre-crash id (so a per-node raw feed never
    merges two unrelated jobs)."""
    core, port = running_server
    async with connect(f"ws://127.0.0.1:{port}/ws/state", max_size=8 * 1024 * 1024) as ws:
        client = StateClient(ws)
        await client.recv_until(
            lambda c: c.node(DEVICE_ID).get("status") == "idle", what="demo_vci declared"
        )
        cmd_id = await client.send_cmd(DEVICE_ID, "selftest")
        first_job = (await client.wait_result(cmd_id))["data"]["jobId"]
        await client.recv_until(
            lambda c: c.node(first_job).get("status") == "running" and job_steps(c, first_job),
            what="first job running with a step",
        )
        running_step = job_steps(client, first_job)[-1]["id"]

        # crash mid-job: the job and its running step end in error, with
        # terminal category-job events in the job's own subtree
        await client.send_cmd(DEVICE_ID, "crash")
        await client.recv_until(
            lambda c: c.node(first_job).get("status") == "error"
            and c.node(PLUGIN_ID).get("status") == "error",
            what="subtree error after crash",
        )
        failures = core.logs.history(LogFilter(nodeId=first_job, categories=["job"], levels=["error"]))
        assert [e["nodeId"] for e in failures] == [first_job, running_step]
        assert all("failed: plugin process died" in e["message"] for e in failures)

        # restart (1 s backoff) => re-declared; a new self-test gets a fresh id
        await client.recv_until(
            lambda c: c.node(PLUGIN_ID).get("status") == "idle"
            and c.node(DEVICE_ID).get("status") == "idle",
            timeout=30.0,
            what="plugin restarted",
        )
        cmd_id = await client.send_cmd(DEVICE_ID, "selftest")
        second_job = (await client.wait_result(cmd_id))["data"]["jobId"]
        assert second_job != first_job
        assert second_job.startswith(f"{PLUGIN_ID}:job:")
        await client.recv_until(
            lambda c: c.node(second_job).get("status") == "done", timeout=30.0, what="second job done"
        )
        created = [
            e for e in core.logs.history(LogFilter(nodeId=second_job, categories=["job"])) if "created" in e["message"]
        ]
        assert len(created) == 1  # the new job's feed is not merged with the pre-crash one
        assert core.logs.history(LogFilter(nodeId=second_job, categories=["job"], levels=["error"])) == []


async def test_telemetry_and_health(running_server: tuple[AppCore, int]) -> None:
    core, port = running_server

    # wait (via the state socket) until the channels are declared
    async with connect(f"ws://127.0.0.1:{port}/ws/state", max_size=8 * 1024 * 1024) as ws:
        client = StateClient(ws)
        await client.recv_until(
            lambda c: all(ch in c.nodes for ch in CHANNEL_IDS),
            what="channels declared",
        )

    channel = CHANNEL_IDS[0]
    async with connect(f"ws://127.0.0.1:{port}/ws/telemetry") as ws:
        await ws.send(json.dumps({"t": "sub", "channel": channel}))
        first = json.loads(await asyncio.wait_for(ws.recv(), timeout=5.0))
        assert first["t"] == "history"
        assert first["channel"] == channel

        # 20 Hz channel, 100 ms flush => batches must arrive quickly
        collected: list[list[float]] = []
        deadline = asyncio.get_running_loop().time() + 5.0
        while len(collected) < 5:
            remaining = deadline - asyncio.get_running_loop().time()
            assert remaining > 0, "no telemetry samples within 5 s"
            message = json.loads(await asyncio.wait_for(ws.recv(), timeout=remaining))
            assert message["t"] == "samples"
            assert message["channel"] == channel
            collected.extend(message["samples"])

        for t_ms, value in collected:
            assert t_ms > 1e12  # Unix epoch milliseconds
            assert 10.0 <= value <= 14.0  # sine V stays in its declared range
        # timestamps monotonically increase
        times = [s[0] for s in collected]
        assert times == sorted(times)

        await ws.send(json.dumps({"t": "unsub", "channel": channel}))

    async with httpx.AsyncClient(base_url=f"http://127.0.0.1:{port}") as http:
        res = await http.get("/api/health")
        assert res.status_code == 200
        assert res.json()["ok"] is True
        res = await http.get("/api/snapshot")
        assert res.status_code == 200
        assert PLUGIN_ID in res.json()["snapshot"]["nodes"]
