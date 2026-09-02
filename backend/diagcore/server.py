"""FastAPI app: /ws/state, /ws/telemetry, /ws/logs, /api/health, /api/snapshot.

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
from pydantic import ValidationError

from . import __version__
from .app import AppCore
from .loghub import LogFilter
from .model import CmdResult, ErrorInfo

logger = logging.getLogger(__name__)

FRONTEND_DIST = Path(__file__).resolve().parents[2] / "frontend" / "dist"

#: Per-connection send-queue cap. A connected-but-stalled client (stuck TCP
#: window, frozen tab) must not grow core memory without bound; on overflow
#: the state client is treated as dead and dropped (it resyncs on reconnect,
#: SPEC 4), while telemetry batches are simply dropped by the hub.
SEND_QUEUE_MAX = 512

#: Close code for a connection dropped because the core could not send to it.
_CLOSE_INTERNAL_ERROR = 1011
_CLOSE_TIMEOUT = 2.0


async def _serve_socket(send_task: asyncio.Task[None], recv_task: asyncio.Task[None]) -> BaseException | None:
    """Run a socket's sender/receiver pair until either side ends.

    Returns the exception that killed ``sender()`` — the caller then closes
    the socket and treats it as a disconnect. Awaiting only the receiver
    would leave a connected-but-deaf zombie client whenever a frame cannot
    be sent (SPEC 1.5: never silent). A receiver ending on client disconnect
    (or cancelled by the overflow policy) returns None; any other receiver
    failure propagates as before. Cancellation of the handler itself (server
    shutdown) propagates out of the ``await``.
    """
    done, _ = await asyncio.wait({send_task, recv_task}, return_when=asyncio.FIRST_COMPLETED)
    if send_task in done and not send_task.cancelled():
        exc = send_task.exception()  # retrieved: no "exception was never retrieved"
        if exc is not None and not isinstance(exc, WebSocketDisconnect):
            return exc
    if recv_task in done and not recv_task.cancelled():
        exc = recv_task.exception()
        if exc is not None and not isinstance(exc, WebSocketDisconnect):
            raise exc
    return None


async def _close_after_send_failure(ws: WebSocket) -> None:
    """Best-effort close frame for a client whose sender died; bounded, never raises."""
    try:
        await asyncio.wait_for(ws.close(code=_CLOSE_INTERNAL_ERROR, reason="send failed"), _CLOSE_TIMEOUT)
    except (RuntimeError, OSError, WebSocketDisconnect, asyncio.TimeoutError) as exc:
        logger.debug("closing websocket after send failure: %r", exc)


def _client_label(ws: WebSocket) -> str:
    return f"{ws.client.host}:{ws.client.port}" if ws.client is not None else "unknown"


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
        # ok is False once the audit session file is disabled (SPEC 14.1: the
        # complete trail is the whole point of M0.5; losing it is not healthy).
        return {
            "ok": core.logs.sink_error is None,
            "version": __version__,
            "pythonVersion": platform.python_version(),
            "seq": core.tree.seq,
            "auditFile": str(core.logs.session_path) if core.logs.session_path is not None else None,
            "auditFileError": core.logs.sink_error,
        }

    def audit_send_failure(endpoint: str, client: str, exc: BaseException) -> None:
        """A socket sender died (e.g. a frame the transport could not encode):
        the client is dropped, and that is audit evidence (SPEC 14)."""
        detail = f"{type(exc).__name__}: {exc}"
        logger.error("%s send failed; dropping client %s: %s", endpoint, client, detail)
        core.logs.emit(
            "error",
            "system",
            f"{endpoint} client {client} dropped: send failed ({detail})",
            node_id="system",
            data={"endpoint": endpoint, "client": client, "error": detail},
        )

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
        # UI (re)connects and resyncs are audit evidence (SPEC 14): a resync
        # means the client saw a seq gap — patches were lost or the client
        # was dropped — and "was the UI even connected?" matters for "why is
        # it stuck?" reconstructions. Debug level: routine, but never silent.
        client = _client_label(ws)

        def audit(what: str) -> None:
            core.logs.emit(
                "debug",
                "system",
                f"/ws/state client {client} {what} (seq={core.tree.seq})",
                node_id="system",
                data={"endpoint": "/ws/state", "client": client, "seq": core.tree.seq},
            )

        audit("connected")

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
            try:
                result = await core.bus.dispatch(
                    raw.get("target"),
                    raw.get("action"),
                    raw.get("params"),
                    cmd_id=str(cmd_id) if cmd_id is not None else None,
                    client=client,
                )
            except Exception as exc:
                # The bus contract is "never raises"; should a future bus bug
                # break it, the client must still get its cmd_result (SPEC 4 /
                # 1.5) instead of a command pending forever.
                logger.exception("command dispatch raised; reporting ok:false to the client")
                result = CmdResult(ok=False, error=ErrorInfo(message=f"command failed: {exc}"))
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
                    audit("requested resync")
                    enqueue_hello()
                else:
                    logger.warning("ignoring unknown /ws/state message type: %r", kind)

        send_task = asyncio.create_task(sender())
        recv_task = asyncio.create_task(receiver())
        sender_error: BaseException | None = None
        try:
            # Either side ending ends the connection: a receiver ending means
            # client disconnect (or the overflow policy cancelling it); a dead
            # sender is treated as a disconnect too — never a zombie client.
            sender_error = await _serve_socket(send_task, recv_task)
        finally:
            # Sync-only teardown: unsubscribe and cancel, but do not await the
            # tasks — cancelled tasks settle on their own and a stalled socket
            # must not keep this handler (and its queue) alive.
            core.tree.unsubscribe(on_patch)  # idempotent
            send_task.cancel()
            recv_task.cancel()
            for task in list(cmd_tasks):
                task.cancel()
            if sender_error is not None:
                audit_send_failure("/ws/state", client, sender_error)
                await _close_after_send_failure(ws)
            else:
                audit("dropped: send queue full (stalled client)" if overflowed else "disconnected")

    # -- /ws/telemetry -----------------------------------------------------

    @app.websocket("/ws/telemetry")
    async def ws_telemetry(ws: WebSocket) -> None:
        await ws.accept()
        # Bounded: TelemetryHub._deliver drops batches on QueueFull, so a
        # stalled client caps out instead of growing core memory forever.
        out: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=SEND_QUEUE_MAX)
        subscribed: set[str] = set()
        client = _client_label(ws)

        async def sender() -> None:
            while True:
                await ws.send_json(await out.get())

        async def receiver() -> None:
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

        send_task = asyncio.create_task(sender())
        recv_task = asyncio.create_task(receiver())
        sender_error: BaseException | None = None
        try:
            sender_error = await _serve_socket(send_task, recv_task)
        finally:
            # Sync-only teardown (see /ws/state): the cancelled tasks settle on
            # their own; awaiting them here would race the server's own
            # cancellation of this handler on a client disconnect.
            for channel in subscribed:
                core.hub.unsubscribe(channel, out)
            send_task.cancel()
            recv_task.cancel()
            if sender_error is not None:
                audit_send_failure("/ws/telemetry", client, sender_error)
                await _close_after_send_failure(ws)

    # -- /ws/logs (SPEC 14.3) ----------------------------------------------

    @app.websocket("/ws/logs")
    async def ws_logs(ws: WebSocket) -> None:
        await ws.accept()
        # Bounded send queue with the /ws/state dead-client policy: a stalled
        # client is dropped (it re-subs with fresh history on reconnect)
        # rather than growing core memory or silently losing single batches.
        out: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=SEND_QUEUE_MAX)
        overflowed = False
        recv_task: asyncio.Task[None] | None = None
        client = _client_label(ws)

        def safe_put(message: dict[str, Any]) -> None:
            nonlocal overflowed
            try:
                out.put_nowait(message)
            except asyncio.QueueFull:
                if not overflowed:
                    overflowed = True
                    logger.warning("/ws/logs send queue full; dropping stalled client")
                    core.logs.unsubscribe(on_events)
                    if recv_task is not None:
                        recv_task.cancel()

        def on_events(events: list[dict[str, Any]]) -> None:
            safe_put({"t": "events", "events": events})

        async def sender() -> None:
            while True:
                await ws.send_json(await out.get())

        async def receiver() -> None:
            while True:
                try:
                    raw = await ws.receive_json()
                except (json.JSONDecodeError, ValueError, KeyError):
                    # KeyError: starlette's receive_json does message["text"],
                    # which a binary frame does not carry.
                    logger.warning("ignoring invalid frame on /ws/logs")
                    continue
                if not isinstance(raw, dict):
                    continue
                kind = raw.get("t")
                if kind == "sub":
                    flt_raw = raw.get("filter")
                    try:
                        flt = LogFilter.model_validate(flt_raw if isinstance(flt_raw, dict) else {})
                    except ValidationError as exc:
                        logger.warning("ignoring /ws/logs sub with invalid filter: %s", exc)
                        continue
                    # ONE active filter per connection (SPEC 14.3): a new sub
                    # replaces the previous one. subscribe() returns history
                    # and registers atomically (no await in between), so the
                    # history message precedes every live batch, gap-free.
                    core.logs.unsubscribe(on_events)
                    history = core.logs.subscribe(on_events, flt)
                    safe_put({"t": "history", "events": history})
                elif kind == "unsub":
                    core.logs.unsubscribe(on_events)
                else:
                    logger.warning("ignoring unknown /ws/logs message type: %r", kind)

        send_task = asyncio.create_task(sender())
        recv_task = asyncio.create_task(receiver())
        sender_error: BaseException | None = None
        try:
            sender_error = await _serve_socket(send_task, recv_task)
        finally:
            core.logs.unsubscribe(on_events)  # idempotent
            send_task.cancel()
            recv_task.cancel()
            if sender_error is not None:
                audit_send_failure("/ws/logs", client, sender_error)
                await _close_after_send_failure(ws)

    # -- static frontend (prod) --------------------------------------------

    dist = frontend_dist if frontend_dist is not None else FRONTEND_DIST
    if dist.is_dir():
        app.mount("/", StaticFiles(directory=dist, html=True), name="frontend")

    return app
