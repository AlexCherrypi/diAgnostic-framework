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
async def running_server(monkeypatch) -> AsyncIterator[tuple[AppCore, int]]:
    monkeypatch.setenv("DEMO_VCI_STEP_SECONDS", FAST_STEP_SECONDS)
    core = AppCore()  # default plugins dir => backend/plugins => demo_vci
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
