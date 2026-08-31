"""FastAPI app: /ws/state, /ws/telemetry, /api/health, /api/snapshot.

Serves ``../frontend/dist`` at ``/`` when it exists (single-process prod) and
allows CORS from the Vite dev server at http://localhost:5173.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import platform
from pathlib import Path
from typing import Any, AsyncIterator

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

from . import __version__
from .app import AppCore
from .model import ErrorInfo

logger = logging.getLogger(__name__)

FRONTEND_DIST = Path(__file__).resolve().parents[2] / "frontend" / "dist"

#: Per-connection send-queue cap. A connected-but-stalled client (stuck TCP
#: window, frozen tab) must not grow core memory without bound; on overflow
#: the state client is treated as dead and dropped (it resyncs on reconnect,
#: SPEC 4), while telemetry batches are simply dropped by the hub.
SEND_QUEUE_MAX = 512


def create_app(core: AppCore, *, frontend_dist: Path | None = None) -> FastAPI:
    @contextlib.asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        await core.start()
        try:
            yield
        finally:
            await core.stop()

    app = FastAPI(title="diAgnostic core", version=__version__, lifespan=lifespan)
    app.state.core = core

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:5173", "http://127.0.0.1:5173"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # -- REST --------------------------------------------------------------

    @app.get("/api/health")
    async def health() -> dict[str, Any]:
        return {
            "ok": True,
            "version": __version__,
            "pythonVersion": platform.python_version(),
            "seq": core.tree.seq,
        }

    @app.get("/api/snapshot")
    async def snapshot() -> dict[str, Any]:
        return {"seq": core.tree.seq, "snapshot": core.tree.snapshot()}

    # -- /ws/state ---------------------------------------------------------

    @app.websocket("/ws/state")
    async def ws_state(ws: WebSocket) -> None:
        await ws.accept()
        out: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=SEND_QUEUE_MAX)
        cmd_tasks: set[asyncio.Task[None]] = set()
        overflowed = False
        recv_task: asyncio.Task[None] | None = None

        def safe_put(message: dict[str, Any]) -> None:
            nonlocal overflowed
            try:
                out.put_nowait(message)
            except asyncio.QueueFull:
                # The client stopped draining its socket. Detach immediately
                # (nothing further is retained for it) and end the handler;
                # the client resyncs via hello on reconnect.
                if not overflowed:
                    overflowed = True
                    logger.warning("/ws/state send queue full; dropping stalled client")
                    core.tree.unsubscribe(on_patch)
                    if recv_task is not None:
                        recv_task.cancel()

        def on_patch(seq: int, op: dict[str, Any]) -> None:
            safe_put({"t": "patch", "seq": seq, "ops": [op]})

        def enqueue_hello() -> None:
            # No await between reading seq and snapshot: consistent pair.
            safe_put({"t": "hello", "seq": core.tree.seq, "snapshot": core.tree.snapshot()})

        enqueue_hello()
        core.tree.subscribe(on_patch)

        async def sender() -> None:
            while True:
                await ws.send_json(await out.get())

        async def run_cmd(raw: dict[str, Any]) -> None:
            cmd_id = raw.get("cmdId")
            result = await core.bus.dispatch(raw.get("target"), raw.get("action"), raw.get("params") or {})
            message: dict[str, Any] = {"t": "cmd_result", "cmdId": cmd_id, "ok": result.ok}
            if result.ok:
                message["data"] = result.data or {}
            else:
                error = result.error or ErrorInfo(message="command failed")
                message["error"] = error.model_dump(mode="json")
            safe_put(message)

        async def receiver() -> None:
            while True:
                try:
                    raw = await ws.receive_json()
                except (json.JSONDecodeError, ValueError, KeyError):
                    # KeyError: starlette's receive_json does message["text"],
                    # which a binary frame does not carry.
                    logger.warning("ignoring invalid frame on /ws/state")
                    continue
                if not isinstance(raw, dict):
                    continue
                kind = raw.get("t")
                if kind == "cmd":
                    task = asyncio.create_task(run_cmd(raw))
                    cmd_tasks.add(task)
                    task.add_done_callback(cmd_tasks.discard)
                elif kind == "resync":
                    enqueue_hello()
                else:
                    logger.warning("ignoring unknown /ws/state message type: %r", kind)

        send_task = asyncio.create_task(sender())
        recv_task = asyncio.create_task(receiver())
        try:
            await recv_task
        except WebSocketDisconnect:
            pass
        except asyncio.CancelledError:
            # Overflow cancels recv_task to drop the stalled client (handled
            # here, clean return); anything else — e.g. server shutdown
            # cancelling this handler — must propagate.
            if not overflowed:
                raise
        finally:
            # Sync-only teardown: unsubscribe and cancel, but do not await —
            # cancelled tasks settle on their own and a stalled socket must
            # not keep this handler (and its queue) alive.
            core.tree.unsubscribe(on_patch)  # idempotent
            send_task.cancel()
            recv_task.cancel()
            for task in list(cmd_tasks):
                task.cancel()

    # -- /ws/telemetry -----------------------------------------------------

    @app.websocket("/ws/telemetry")
    async def ws_telemetry(ws: WebSocket) -> None:
        await ws.accept()
        # Bounded: TelemetryHub._deliver drops batches on QueueFull, so a
        # stalled client caps out instead of growing core memory forever.
        out: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=SEND_QUEUE_MAX)
        subscribed: set[str] = set()

        async def sender() -> None:
            while True:
                await ws.send_json(await out.get())

        send_task = asyncio.create_task(sender())
        try:
            while True:
                try:
                    raw = await ws.receive_json()
                except (json.JSONDecodeError, ValueError, KeyError):
                    # KeyError: starlette's receive_json does message["text"],
                    # which a binary frame does not carry.
                    logger.warning("ignoring invalid frame on /ws/telemetry")
                    continue
                if not isinstance(raw, dict):
                    continue
                kind = raw.get("t")
                channel = raw.get("channel")
                if kind == "sub" and isinstance(channel, str) and channel not in subscribed:
                    # subscribe() and the history enqueue happen without an
                    # await in between => history is the first message.
                    history = core.hub.subscribe(channel, out)
                    try:
                        out.put_nowait({"t": "history", "channel": channel, "samples": history})
                    except asyncio.QueueFull:
                        logger.warning("/ws/telemetry send queue full; dropping history for %r", channel)
                    subscribed.add(channel)
                elif kind == "unsub" and isinstance(channel, str) and channel in subscribed:
                    core.hub.unsubscribe(channel, out)
                    subscribed.discard(channel)
        except WebSocketDisconnect:
            pass
        finally:
            for channel in subscribed:
                core.hub.unsubscribe(channel, out)
            send_task.cancel()
            await asyncio.gather(send_task, return_exceptions=True)

    # -- static frontend (prod) --------------------------------------------

    dist = frontend_dist if frontend_dist is not None else FRONTEND_DIST
    if dist.is_dir():
        app.mount("/", StaticFiles(directory=dist, html=True), name="frontend")

    return app
