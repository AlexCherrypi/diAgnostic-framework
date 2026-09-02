"""Helper library for plugin authors (runs inside the plugin process).

A plugin is a separate OS process talking line-delimited JSON-RPC 2.0 on
stdio (SPEC 6). This module hides the plumbing:

.. code-block:: python

    from diagcore.plugins.api import Plugin

    plugin = Plugin()

    @plugin.on_initialized
    def declare() -> None:
        plugin.add(plugin.make_node("my:dev", "device.sim", "My device"))

    @plugin.on_command("selftest")
    def selftest(target: str, params: dict) -> dict:
        job_id = plugin.start_job("Self-test", run_selftest, locks=["bus:my"])
        return {"jobId": job_id}

    plugin.run()

Never print to stdout from a plugin — stdout is the protocol channel. Use
``plugin.log(...)`` or stderr.

Because the plugin protocol restricts tree mutations to the plugin's own
subtree (and offers no job-creation RPC), jobs triggered by plugin commands
live *inside the plugin subtree*: :meth:`Plugin.start_job` mirrors the core
job engine semantics (job/step nodes, progress, sorted named locks,
``cancel`` action) using notifications. The named locks are plugin-local.
This is the recorded M0 resolution of the SPEC 3.2/7-vs-6 contradiction —
see the M0 notes in SPEC.md sections 3.2, 6 and 7.

Job ids are ``<pluginNodeId>:job:<incarnation>.<n>``: the incarnation salt is
drawn once per plugin process, so a job created after a crash/restart never
reuses the id of a pre-crash job — the core's log ring keeps events (and
their emit-time ancestry) across restarts, and a reused id would merge two
unrelated jobs in a per-node raw feed (SPEC 14.4).
"""

from __future__ import annotations

import asyncio
import inspect
import json
import secrets
import sys
import threading
import traceback
from typing import Any, Awaitable, Callable, Sequence

#: A command handler: (target_node_id, params) -> result data (dict or None).
CommandHandler = Callable[[str, dict[str, Any]], Any]

#: A job body: an async callable receiving the PluginJobContext.
JobFactory = Callable[["PluginJobContext"], Awaitable[Any]]

_CANCEL_ACTION = {"name": "cancel", "label": "Cancel", "params": []}

#: Terminal jobs kept in the plugin subtree; older ones are removed
#: (mirrors the core job engine's retention cap, SPEC 7).
JOB_RETENTION = 50


class _PluginJob:
    def __init__(self, job_id: str) -> None:
        self.id = job_id
        self.task: asyncio.Task[None] | None = None


class _PluginStep:
    def __init__(self, ctx: PluginJobContext, name: str) -> None:
        self._ctx = ctx
        self._name = name
        self.id: str | None = None

    async def __aenter__(self) -> _PluginStep:
        ctx = self._ctx
        ctx._step_counter += 1
        step_id = f"{ctx.job_id}:step:{ctx._step_counter}"
        ctx._plugin.add(
            {
                "id": step_id,
                "type": "job.step",
                "name": self._name,
                "status": "running",
                "parentId": ctx._stack[-1],
            }
        )
        ctx._stack.append(step_id)
        self.id = step_id
        return self

    async def __aexit__(self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: object) -> bool:
        ctx = self._ctx
        ctx._stack.pop()
        assert self.id is not None
        if exc_type is None:
            ctx._plugin.update(self.id, {"status": "done"})
        elif issubclass(exc_type, asyncio.CancelledError):
            ctx._plugin.update(self.id, {"status": "cancelled"})
        else:
            message = str(exc) or exc_type.__name__
            ctx._plugin.update(self.id, {"status": "error", "error": {"message": message}})
        return False


class PluginJobContext:
    """Mirror of the core ``JobContext`` for plugin-side jobs."""

    def __init__(self, plugin: Plugin, job_id: str) -> None:
        self._plugin = plugin
        self.job_id = job_id
        self._stack: list[str] = [job_id]
        self._step_counter = 0

    def step(self, name: str) -> _PluginStep:
        return _PluginStep(self, name)

    def set_progress(self, value: float) -> None:
        clamped = min(1.0, max(0.0, float(value)))
        self._plugin.update(self.job_id, {"progress": clamped})


class Plugin:
    def __init__(self) -> None:
        self.node_id: str | None = None
        self.config: dict[str, Any] = {}
        self._handlers: dict[str, CommandHandler] = {}
        self._init_cb: Callable[[], Any] | None = None
        self._loop: asyncio.AbstractEventLoop | None = None
        self._queue: asyncio.Queue[str | None] | None = None
        self._stdout_lock = threading.Lock()
        self._jobs: dict[str, _PluginJob] = {}
        self._finished_jobs: list[str] = []  # terminal job ids, oldest first
        self._job_counter = 0
        #: Per-process-incarnation salt for job ids (see module docstring).
        self._job_epoch = secrets.token_hex(4)
        self._locks: dict[str, asyncio.Lock] = {}
        self._tasks: set[asyncio.Task[Any]] = set()

    # -- registration ------------------------------------------------------

    def on_initialized(self, fn: Callable[[], Any]) -> Callable[[], Any]:
        """Decorator: called (in the event loop) after the ``initialize``
        handshake — declare your nodes here."""
        self._init_cb = fn
        return fn

    def on_command(self, action: str) -> Callable[[CommandHandler], CommandHandler]:
        """Decorator registering a handler for a command action. The handler
        receives ``(target_node_id, params)`` and may be sync or async; its
        return value (a dict, or None) becomes the command result data."""

        def decorator(fn: CommandHandler) -> CommandHandler:
            self._handlers[action] = fn
            return fn

        return decorator

    # -- outgoing notifications --------------------------------------------

    def add(self, node: dict[str, Any], index: int | None = None) -> None:
        params: dict[str, Any] = {"node": node}
        if index is not None:
            params["index"] = index
        self._notify("add", params)

    def update(self, node_id: str, changes: dict[str, Any]) -> None:
        self._notify("update", {"id": node_id, "changes": changes})

    def merge_props(self, node_id: str, props: dict[str, Any]) -> None:
        self._notify("merge_props", {"id": node_id, "props": props})

    def remove(self, node_id: str) -> None:
        self._notify("remove", {"id": node_id})

    def samples(self, channel: str, samples: Sequence[Sequence[float]]) -> None:
        self._notify("samples", {"channel": channel, "samples": [list(s) for s in samples]})

    def log(self, level: str, message: str) -> None:
        self._notify("log", {"level": level, "message": message})

    def io(
        self,
        node_id: str,
        direction: str,
        raw: str,
        decoded: dict[str, Any] | None = None,
        note: str | None = None,
    ) -> None:
        """Report one raw vehicle frame (SPEC 14.2).

        **Law:** every byte exchanged with a vehicle MUST be reported here —
        this one-liner feeds the audit LogHub and the per-node raw feed.
        ``direction`` is ``"tx"`` (written to the vehicle) or ``"rx"`` (read),
        ``raw`` is the frame as whole bytes in hex — ``frame.hex()``: no
        ``0x`` prefix, no separators (whitespace between bytes is tolerated),
        even length — so the trail replays byte-exact; anything else is
        dropped by the core with a ``warn`` event. ``decoded`` is an optional
        structured interpretation, ``note`` an optional free-text remark.
        """
        params: dict[str, Any] = {"nodeId": node_id, "direction": direction, "raw": raw}
        if decoded is not None:
            params["decoded"] = decoded
        if note is not None:
            params["note"] = note
        self._notify("io", params)

    def make_node(
        self,
        node_id: str,
        node_type: str,
        name: str,
        *,
        parent_id: str | None = None,
        status: str = "idle",
        props: dict[str, Any] | None = None,
        actions: list[dict[str, Any]] | None = None,
        progress: float | None = None,
    ) -> dict[str, Any]:
        """Convenience builder for a node dict (parent defaults to the plugin
        node)."""
        return {
            "id": node_id,
            "type": node_type,
            "name": name,
            "status": status,
            "parentId": parent_id or self.node_id,
            "props": props or {},
            "actions": actions or [],
            "progress": progress,
        }

    # -- background tasks --------------------------------------------------

    def create_task(self, coro: Awaitable[Any]) -> asyncio.Task[Any]:
        assert self._loop is not None, "create_task() requires the running plugin loop"
        task = self._loop.create_task(coro)  # type: ignore[arg-type]
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    # -- plugin-side jobs --------------------------------------------------

    def start_job(
        self,
        name: str,
        factory: JobFactory,
        *,
        locks: Sequence[str] = (),
        parent_id: str | None = None,
    ) -> str:
        """Create a ``job`` node in the plugin subtree and run ``factory``.

        Mirrors the core job engine: sorted lock acquisition with
        ``props.waitingFor``, running/done/error/cancelled transitions, and a
        ``cancel`` action handled automatically by the API. The returned id is
        unique per process incarnation (see module docstring), so LogHub
        subtree filters never merge two jobs across a plugin restart.
        """
        self._job_counter += 1
        job_id = f"{self.node_id}:job:{self._job_epoch}.{self._job_counter}"
        self.add(
            {
                "id": job_id,
                "type": "job",
                "name": name,
                "status": "init",
                "parentId": parent_id or self.node_id,
                "actions": [dict(_CANCEL_ACTION)],
            }
        )
        job = _PluginJob(job_id)
        self._jobs[job_id] = job
        job.task = self.create_task(self._run_job(job, factory, tuple(sorted(set(locks)))))
        return job_id

    def cancel_job(self, job_id: str) -> bool:
        job = self._jobs.get(job_id)
        if job is None or job.task is None or job.task.done():
            return False
        job.task.cancel()
        return True

    async def _run_job(self, job: _PluginJob, factory: JobFactory, lock_names: tuple[str, ...]) -> None:
        held: list[asyncio.Lock] = []
        ctx = PluginJobContext(self, job.id)
        try:
            if lock_names:
                self.merge_props(job.id, {"waitingFor": list(lock_names)})
                for lock_name in lock_names:
                    lock = self._locks.setdefault(lock_name, asyncio.Lock())
                    await lock.acquire()
                    held.append(lock)
                self.merge_props(job.id, {"waitingFor": []})
            self.update(job.id, {"status": "running", "progress": 0.0})
            await factory(ctx)
            self.update(job.id, {"status": "done", "progress": 1.0, "actions": []})
        except asyncio.CancelledError:
            self.update(job.id, {"status": "cancelled", "actions": []})
        except Exception as exc:
            message = str(exc) or type(exc).__name__
            self.update(
                job.id,
                {
                    "status": "error",
                    "error": {"message": message, "detail": traceback.format_exc()},
                    "actions": [],
                },
            )
        finally:
            for lock in reversed(held):
                lock.release()
            job.task = None  # release the finished Task object
            # Retention cap (mirrors the core engine): drop the oldest
            # terminal jobs (node + bookkeeping) beyond JOB_RETENTION.
            self._finished_jobs.append(job.id)
            while len(self._finished_jobs) > JOB_RETENTION:
                old_id = self._finished_jobs.pop(0)
                self._jobs.pop(old_id, None)
                self.remove(old_id)

    # -- main loop ---------------------------------------------------------

    def run(self) -> None:
        """Blocking entry point: read stdin until EOF or ``shutdown``."""
        try:
            asyncio.run(self._main())
        except KeyboardInterrupt:
            pass

    async def _main(self) -> None:
        self._loop = asyncio.get_running_loop()
        self._queue = asyncio.Queue()
        reader = threading.Thread(
            target=self._stdin_reader, args=(self._loop, self._queue), daemon=True
        )
        reader.start()
        while True:
            line = await self._queue.get()
            if line is None:
                break
            self._dispatch_line(line)
        for task in list(self._tasks):
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)

    def _stdin_reader(self, loop: asyncio.AbstractEventLoop, queue: asyncio.Queue[str | None]) -> None:
        try:
            for line in sys.stdin:
                loop.call_soon_threadsafe(queue.put_nowait, line)
        except (ValueError, OSError):
            pass
        try:
            loop.call_soon_threadsafe(queue.put_nowait, None)
        except RuntimeError:
            pass  # loop already closed

    # -- incoming messages -------------------------------------------------

    def _dispatch_line(self, line: str) -> None:
        text = line.strip()
        if not text:
            return
        try:
            message = json.loads(text)
        except json.JSONDecodeError:
            print(f"diagcore.plugins.api: invalid JSON from host: {text[:200]}", file=sys.stderr)
            return
        if not isinstance(message, dict):
            return
        method = message.get("method")
        msg_id = message.get("id")
        if method is None:
            return  # the host sends no responses to our notifications

        if method == "ping":
            self._respond(msg_id, {})
        elif method == "initialize":
            params = message.get("params") or {}
            self.node_id = params.get("pluginNodeId")
            config = params.get("config")
            self.config = config if isinstance(config, dict) else {}
            self._respond(msg_id, {"ok": True})
            if self._init_cb is not None:
                self._invoke_callback(self._init_cb)
        elif method == "command":
            self.create_task(self._run_command(msg_id, message.get("params") or {}))
        elif method == "shutdown":
            self._respond(msg_id, {})
            assert self._queue is not None
            self._queue.put_nowait(None)
        else:
            if msg_id is not None:
                self._send(
                    {
                        "jsonrpc": "2.0",
                        "id": msg_id,
                        "error": {"code": -32601, "message": f"method not found: {method}"},
                    }
                )

    def _invoke_callback(self, fn: Callable[[], Any]) -> None:
        try:
            result = fn()
            if inspect.isawaitable(result):
                self.create_task(result)
        except Exception:
            self.log("error", f"initialized callback failed:\n{traceback.format_exc()}")

    async def _run_command(self, msg_id: Any, params: dict[str, Any]) -> None:
        target = params.get("target")
        action = params.get("action")
        cmd_params = params.get("params") or {}
        try:
            if action == "cancel" and target in self._jobs:
                cancelled = self.cancel_job(str(target))
                if cancelled:
                    self._respond(msg_id, {"ok": True, "data": {}})
                else:
                    self._respond(
                        msg_id,
                        {"ok": False, "error": {"message": f"job {target!r} is not cancellable"}},
                    )
                return
            handler = self._handlers.get(str(action))
            if handler is None:
                self._respond(
                    msg_id,
                    {"ok": False, "error": {"message": f"unknown action {action!r} for {target!r}"}},
                )
                return
            result = handler(str(target), dict(cmd_params))
            if inspect.isawaitable(result):
                result = await result
            self._respond(msg_id, {"ok": True, "data": result if isinstance(result, dict) else {}})
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._respond(
                msg_id,
                {
                    "ok": False,
                    "error": {
                        "message": str(exc) or type(exc).__name__,
                        "detail": traceback.format_exc(),
                    },
                },
            )

    # -- low-level output --------------------------------------------------

    def _respond(self, msg_id: Any, result: dict[str, Any]) -> None:
        if msg_id is None:
            return
        self._send({"jsonrpc": "2.0", "id": msg_id, "result": result})

    def _notify(self, method: str, params: dict[str, Any]) -> None:
        self._send({"jsonrpc": "2.0", "method": method, "params": params})

    def _send(self, payload: dict[str, Any]) -> None:
        line = json.dumps(payload, ensure_ascii=True, separators=(",", ":"))
        with self._stdout_lock:
            sys.stdout.write(line + "\n")
            sys.stdout.flush()
