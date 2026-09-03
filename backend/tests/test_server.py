"""FastAPI endpoints and /ws/state protocol basics (SPEC 4)."""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import AsyncIterator

import httpx
import pytest
import uvicorn
import websockets
from fastapi.testclient import TestClient
from websockets.asyncio.client import connect

from diagcore import __version__
from diagcore.app import AppCore
from diagcore.loghub import LogFilter
from diagcore.model import Node
from diagcore.server import create_app

#: A lone surrogate as produced by json.loads of a `\ud800` escape (what a
#: surrogateescape'd str becomes on the plugin wire).
POISON = json.loads('"poison \\ud800"')


@pytest.fixture
def core(tmp_path: Path) -> AppCore:
    # empty plugins dir => no plugin processes in these tests
    return AppCore(plugins_dir=tmp_path / "no_plugins", log_dir=tmp_path / "logs")


@pytest.fixture
async def live_server(core: AppCore) -> AsyncIterator[int]:
    """The real ASGI server on an ephemeral port: transport-level behaviour
    (frame encoding, close frames) is invisible to the in-process TestClient."""
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
        yield server.servers[0].sockets[0].getsockname()[1]
    finally:
        server.should_exit = True
        await asyncio.wait_for(serve_task, timeout=20.0)


async def test_health_and_snapshot(core: AppCore) -> None:
    app = create_app(core)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        res = await client.get("/api/health")
        assert res.status_code == 200
        body = res.json()
        assert body["ok"] is True
        assert body["version"] == __version__
        assert body["auditFile"] == str(core.logs.session_path)
        assert body["auditFileError"] is None

        res = await client.get("/api/snapshot")
        assert res.status_code == 200
        body = res.json()
        assert body["snapshot"]["rootId"] == "root"
        nodes = body["snapshot"]["nodes"]
        assert set(nodes["root"]["children"]) == {"system", "plugins", "jobs", "vehicle", "vehicles"}
        assert nodes["system"]["props"]["version"] == __version__
        assert nodes["system"]["props"]["pythonVersion"]
        assert nodes["system"]["props"]["startedAt"]
        assert body["seq"] == core.tree.seq


def test_ws_state_hello_resync_and_bad_cmd(core: AppCore) -> None:
    app = create_app(core)
    with TestClient(app) as client:
        with client.websocket_connect("/ws/state") as ws:
            hello = ws.receive_json()
            assert hello["t"] == "hello"
            assert hello["seq"] == core.tree.seq
            assert hello["snapshot"]["rootId"] == "root"
            assert "system" in hello["snapshot"]["nodes"]

            # resync => a fresh hello
            ws.send_json({"t": "resync"})
            hello2 = ws.receive_json()
            assert hello2["t"] == "hello"
            assert hello2["snapshot"]["rootId"] == "root"

            # unknown target => cmd_result ok:false
            ws.send_json({"t": "cmd", "cmdId": "c1", "target": "nope", "action": "x", "params": {}})
            result = ws.receive_json()
            assert result == {
                "t": "cmd_result",
                "cmdId": "c1",
                "ok": False,
                "error": {"message": "unknown target: 'nope'", "detail": None},
            }

            # unknown action on a core node => cmd_result ok:false
            ws.send_json({"t": "cmd", "cmdId": "c2", "target": "system", "action": "warp", "params": {}})
            result = ws.receive_json()
            assert result["t"] == "cmd_result"
            assert result["cmdId"] == "c2"
            assert result["ok"] is False
            assert "unknown action" in result["error"]["message"]


def test_ws_state_connection_events_are_audited(core: AppCore) -> None:
    """UI connect, resync (seq-gap evidence) and disconnect on /ws/state land
    in the LogHub as debug system events carrying the client and the seq."""
    app = create_app(core)
    with TestClient(app) as client:
        with client.websocket_connect("/ws/state") as ws:
            assert ws.receive_json()["t"] == "hello"
            ws.send_json({"t": "resync"})
            assert ws.receive_json()["t"] == "hello"

        def events() -> list[dict]:
            return core.logs.history(LogFilter(categories=["system"], levels=["debug"], search="/ws/state"))

        deadline = time.time() + 5.0  # the handler's teardown may still be finishing
        while not any(" disconnected (" in e["message"] for e in events()) and time.time() < deadline:
            time.sleep(0.02)
        messages = [e["message"] for e in events()]
        assert sum(1 for m in messages if " connected (" in m and "disconnected" not in m) == 1
        assert sum(1 for m in messages if " requested resync (" in m) == 1
        assert sum(1 for m in messages if " disconnected (" in m) == 1
        for event in events():
            assert event["nodeId"] == "system"
            assert event["data"]["endpoint"] == "/ws/state"
            assert isinstance(event["data"]["seq"], int)
            assert event["data"]["client"] and event["data"]["client"] in event["message"]


def test_ws_state_cmd_with_non_dict_params_still_gets_a_result(core: AppCore) -> None:
    """Malformed params must yield cmd_result ok:false for THAT cmdId (never a
    command pending forever) and leave both cmd audit events behind."""
    app = create_app(core)
    with TestClient(app) as client:
        with client.websocket_connect("/ws/state") as ws:
            assert ws.receive_json()["t"] == "hello"
            for cmd_id, params in (("c2", [1, 2]), ("c3", "abc"), ("c4", 5), ("c5", True)):
                ws.send_json({"t": "cmd", "cmdId": cmd_id, "target": "system", "action": "nope", "params": params})
                result = ws.receive_json()
                assert result["t"] == "cmd_result" and result["cmdId"] == cmd_id
                assert result["ok"] is False
                assert "invalid params" in result["error"]["message"]
            ws.send_json({"t": "cmd", "cmdId": "c6", "target": "system", "action": "nope", "params": {}})
            result = ws.receive_json()
            assert result["cmdId"] == "c6" and "unknown action" in result["error"]["message"]
    cmd_events = core.logs.history(LogFilter(categories=["cmd"]))
    assert len(cmd_events) == 10  # received + result for each of the 5 commands
    assert [e["data"]["cmdId"] for e in cmd_events] == [c for c in ("c2", "c3", "c4", "c5", "c6") for _ in (0, 1)]
    assert all(e["data"]["client"] for e in cmd_events)  # the issuing connection is recorded


def test_ws_state_survives_binary_frame(core: AppCore) -> None:
    """A binary frame makes starlette's receive_json raise KeyError; it must
    be ignored like invalid JSON, not tear down the connection."""
    app = create_app(core)
    with TestClient(app) as client:
        with client.websocket_connect("/ws/state") as ws:
            assert ws.receive_json()["t"] == "hello"
            ws.send_bytes(b"\x00")
            ws.send_json({"t": "resync"})
            hello = ws.receive_json()
            assert hello["t"] == "hello"
            assert hello["snapshot"]["rootId"] == "root"


def test_ws_telemetry_survives_binary_frame(core: AppCore) -> None:
    app = create_app(core)
    with TestClient(app) as client:
        with client.websocket_connect("/ws/telemetry") as ws:
            ws.send_bytes(b"\x00")
            ws.send_json({"t": "sub", "channel": "ch"})
            first = ws.receive_json()
            assert first == {"t": "history", "channel": "ch", "samples": []}


def test_ws_logs_sub_history_live_and_filter_replacement(core: AppCore) -> None:
    """/ws/logs (SPEC 14.3): sub sends matching history first, then live
    batches; a new sub replaces the connection's single active filter."""
    app = create_app(core)
    with TestClient(app) as client:
        with client.websocket_connect("/ws/logs") as logs_ws:
            logs_ws.send_json({"t": "sub", "filter": {"categories": ["cmd"]}})
            history = logs_ws.receive_json()
            assert history == {"t": "history", "events": []}  # no commands yet

            # a command on /ws/state generates live cmd events at the seam
            with client.websocket_connect("/ws/state") as state_ws:
                assert state_ws.receive_json()["t"] == "hello"
                state_ws.send_json(
                    {"t": "cmd", "cmdId": "c1", "target": "nope", "action": "x", "params": {}}
                )
                assert state_ws.receive_json()["t"] == "cmd_result"

            collected: list[dict] = []
            while len(collected) < 2:  # received + result, batched
                message = logs_ws.receive_json()
                assert message["t"] == "events"
                collected.extend(message["events"])
            assert [e["category"] for e in collected] == ["cmd", "cmd"]
            assert "'x'" in collected[0]["message"] and "'nope'" in collected[0]["message"]
            assert "failed" in collected[1]["message"]

            # ONE active filter per connection: re-sub replaces it...
            logs_ws.send_json(
                {"t": "sub", "filter": {"categories": ["cmd"], "search": "no such text"}}
            )
            assert logs_ws.receive_json() == {"t": "history", "events": []}
            # ...and a matching re-sub returns the earlier events as history
            logs_ws.send_json({"t": "sub", "filter": {"categories": ["cmd"]}})
            history = logs_ws.receive_json()
            assert [e["message"] for e in history["events"]] == [e["message"] for e in collected]


def test_ws_logs_survives_invalid_filter_and_binary_frame(core: AppCore) -> None:
    app = create_app(core)
    with TestClient(app) as client:
        with client.websocket_connect("/ws/logs") as ws:
            ws.send_bytes(b"\x00")  # ignored like invalid JSON
            ws.send_json({"t": "sub", "filter": {"levels": "not-a-list"}})  # ignored
            ws.send_json({"t": "sub", "filter": {"categories": ["system"]}})
            history = ws.receive_json()
            assert history["t"] == "history"
            # AppCore emitted system events at construction/startup
            assert any("session started" in e["message"] for e in history["events"])
            assert all(e["category"] == "system" for e in history["events"])


async def recv_json(ws, timeout: float = 5.0) -> dict:
    return json.loads(await asyncio.wait_for(ws.recv(), timeout))


async def test_ws_logs_poison_event_is_delivered_escaped_and_feed_stays_alive(
    core: AppCore, live_server: int
) -> None:
    """A lone surrogate in a logged string used to kill the /ws/logs sender in
    uvicorn's encode and leave the client connected but deaf forever (and
    the history replay poisoned every reconnect). Now the event arrives
    escaped and the feed keeps flowing."""
    async with connect(f"ws://127.0.0.1:{live_server}/ws/logs") as ws:
        await ws.send(json.dumps({"t": "sub", "filter": {"categories": ["plugin"]}}))
        assert (await recv_json(ws))["t"] == "history"
        core.logs.emit("info", "plugin", POISON, source="plugin:x", node_id="plugins", data={"line": POISON})
        core.logs.emit("info", "plugin", "clean after", source="plugin:x", node_id="plugins")
        received: list[dict] = []
        while len(received) < 2:
            message = await recv_json(ws)
            assert message["t"] == "events"
            received.extend(message["events"])
        assert received[0]["message"] == "poison \\ud800"  # escaped, byte value visible
        assert received[0]["data"] == {"line": "poison \\ud800"}
        assert received[1]["message"] == "clean after"
        # a later event still arrives: the connection is alive, not a zombie
        core.logs.emit("warn", "plugin", "still alive", source="plugin:x", node_id="plugins")
        message = await recv_json(ws)
        assert [e["message"] for e in message["events"]] == ["still alive"]
    # a fresh connection replays the (escaped) history and works from frame one
    async with connect(f"ws://127.0.0.1:{live_server}/ws/logs") as ws:
        await ws.send(json.dumps({"t": "sub", "filter": {"categories": ["plugin"]}}))
        history = await recv_json(ws)
        assert [e["message"] for e in history["events"]] == ["poison \\ud800", "clean after", "still alive"]
        core.logs.emit("info", "plugin", "after reconnect", source="plugin:x", node_id="plugins")
        assert [e["message"] for e in (await recv_json(ws))["events"]] == ["after reconnect"]
    # the session file got every line
    lines = core.logs.session_path.read_text(encoding="utf-8").splitlines()
    assert core.logs.sink_error is None
    assert any(json.loads(line)["message"] == "poison \\ud800" for line in lines)


async def test_ws_state_sender_failure_closes_connection_instead_of_zombie(
    core: AppCore, live_server: int
) -> None:
    """If a frame cannot be sent (here: a tree node whose name the transport
    cannot encode), the client must be dropped with a close frame and the
    drop audited — never left connected and silently receiving nothing."""
    async with connect(f"ws://127.0.0.1:{live_server}/ws/state") as ws:
        assert (await recv_json(ws))["t"] == "hello"
        core.tree.add(Node(id="poison", type="device.sim", name=POISON, status="idle", owner="core", parentId="root"))
        with pytest.raises(websockets.exceptions.ConnectionClosed) as info:
            await asyncio.wait_for(ws.recv(), timeout=5.0)
        assert info.value.rcvd is not None and info.value.rcvd.code == 1011  # a proper close frame
    deadline = asyncio.get_running_loop().time() + 5.0
    while asyncio.get_running_loop().time() < deadline:
        dropped = core.logs.history(LogFilter(categories=["system"], levels=["error"], search="send failed"))
        if dropped:
            break
        await asyncio.sleep(0.02)
    assert len(dropped) == 1
    assert dropped[0]["nodeId"] == "system"
    assert dropped[0]["data"]["endpoint"] == "/ws/state"
    assert "UnicodeEncodeError" in dropped[0]["message"]
    # the tree subscription was released: no subscriber leak for the dead client
    # (only the state seam's and the vehicle session manager's permanent
    # subscribers remain; bound methods compare by ==)
    assert core.tree._subscribers == [core._log_state_patch, core.vehicles._on_patch]


def test_serves_frontend_dist_when_present(core: AppCore, tmp_path: Path) -> None:
    dist = tmp_path / "dist"
    dist.mkdir()
    (dist / "index.html").write_text("<html><body>frontend</body></html>", encoding="utf-8")
    app = create_app(core, frontend_dist=dist)
    with TestClient(app) as client:
        res = client.get("/")
        assert res.status_code == 200
        assert "frontend" in res.text
        # API routes still win over the static mount
        assert client.get("/api/health").json()["ok"] is True
