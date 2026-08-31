"""FastAPI endpoints and /ws/state protocol basics (SPEC 4)."""

from __future__ import annotations

from pathlib import Path

import httpx
import pytest
from fastapi.testclient import TestClient

from diagcore import __version__
from diagcore.app import AppCore
from diagcore.server import create_app


@pytest.fixture
def core(tmp_path: Path) -> AppCore:
    # empty plugins dir => no plugin processes in these tests
    return AppCore(plugins_dir=tmp_path / "no_plugins")


async def test_health_and_snapshot(core: AppCore) -> None:
    app = create_app(core)
    transport = httpx.ASGITransport(app=app)
    async with httpx.AsyncClient(transport=transport, base_url="http://test") as client:
        res = await client.get("/api/health")
        assert res.status_code == 200
        body = res.json()
        assert body["ok"] is True
        assert body["version"] == __version__

        res = await client.get("/api/snapshot")
        assert res.status_code == 200
        body = res.json()
        assert body["snapshot"]["rootId"] == "root"
        nodes = body["snapshot"]["nodes"]
        assert set(nodes["root"]["children"]) == {"system", "plugins", "jobs"}
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
