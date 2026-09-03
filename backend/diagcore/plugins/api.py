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

M1.5 (SPEC 15.4) — vehicle plugins (manifest capability ``vehicle``):

.. code-block:: python

    plugin.register_vehicle_handlers(detect=on_detect, attach=on_attach, detach=on_detach)

    async def on_detect(session_node_id: str, params: dict) -> str:
        # declare the detection job UNDER the session node (mount grant)
        return plugin.start_job("Detect vehicle", scan, parent_id=session_node_id)

The core grants the plugin the session subtree with ``vehicle.detect`` /
``vehicle.attach`` (``sessionNodeId`` in the request params is the granted
node id); ``add`` / ``update`` / ``start_job`` under that node then work
like in the plugin's own subtree. :meth:`Plugin.emit_vehicle_profile`
sends the SPEC 15.1 profile (without id/firstSeen/lastSeen). The core
removes the session subtree itself when the session ends, so the job
retention cap never touches jobs declared under a granted node.
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

#: Vehicle handlers (SPEC 15.4), sync or async:
#: ``detect(session_node_id, params) -> jobNodeId | None``,
#: ``attach(session_node_id, profile) -> jobNodeId | None``,
#: ``detach(session_node_id) -> None``.
VehicleDetectHandler = Callable[[str, dict[str, Any]], Any]
VehicleAttachHandler = Callable[[str, dict[str, Any]], Any]
VehicleDetachHandler = Callable[[str], Any]

_CANCEL_ACTION = {"name": "cancel", "label": "Cancel", "params": []}

_VEHICLE_METHODS = ("vehicle.detect", "vehicle.attach", "vehicle.detach")

#: Profile keys the core fills itself (SPEC 15.1); never sent by a plugin.
_CORE_OWNED_PROFILE_KEYS = frozenset({"id", "firstSeen", "lastSeen"})

#: Terminal jobs kept in the plugin subtree; older ones are removed
#: (mirrors the core job engine's retention cap, SPEC 7).
JOB_RETENTION = 50


class _PluginJob:
    def __init__(self, job_id: str, *, under_grant: bool = False) -> None:
        self.id = job_id
        self.task: asyncio.Task[None] | None = None
        #: Declared under a granted (session) subtree: the core removes it.
        self.under_grant = under_grant


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
        self._vehicle_handlers: dict[str, Callable[..., Any]] = {}
        #: Mount grants the core passed along (SPEC 15.4): granted root ids,
        #: and node ids this plugin declared below them -> their root.
        self._granted_roots: set[str] = set()
        self._granted_nodes: dict[str, str] = {}

    # -- registration ------------------------------------------------------

    def register_vehicle_handlers(
        self,
        *,
        detect: VehicleDetectHandler | None = None,
        attach: VehicleAttachHandler | None = None,
        detach: VehicleDetachHandler | None = None,
    ) -> None:
        """Register the SPEC 15.4 vehicle RPC handlers (sync or async).

        ``detect(session_node_id, params)`` and ``attach(session_node_id,
        profile)`` return the id of the job they started under the session
        node (or ``None``); ``detach(session_node_id)`` releases the vehicle.
        An exception becomes ``{ok: false, error}`` for the core.
        """
        for method, handler in (("vehicle.detect", detect), ("vehicle.attach", attach), ("vehicle.detach", detach)):
            if handler is not None:
                self._vehicle_handlers[method] = handler
            else:
                self._vehicle_handlers.pop(method, None)

    # -- mount grants (SPEC 15.4) -------------------------------------------

    @property
    def granted_roots(self) -> frozenset[str]:
        """Session node ids the core currently lets this plugin mutate."""
        return frozenset(self._granted_roots)

    def is_granted(self, node_id: str | None) -> bool:
        """True for a granted root or a node this plugin declared below one."""
        return node_id is not None and (node_id in self._granted_roots or node_id in self._granted_nodes)

    def _grant_root_of(self, node_id: str | None) -> str | None:
        if node_id is None:
            return None
        if node_id in self._granted_roots:
            return node_id
        return self._granted_nodes.get(node_id)

    def _forget_grant(self, root_id: str) -> None:
        self._granted_roots.discard(root_id)
        for node_id, root in list(self._granted_nodes.items()):
            if root == root_id:
                del self._granted_nodes[node_id]

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
        root = self._grant_root_of(node.get("parentId"))
        if root is not None and isinstance(node.get("id"), str):
            self._granted_nodes[node["id"]] = root
        self._notify("add", params)

    def update(self, node_id: str, changes: dict[str, Any]) -> None:
        self._notify("update", {"id": node_id, "changes": changes})

    def merge_props(self, node_id: str, props: dict[str, Any]) -> None:
        self._notify("merge_props", {"id": node_id, "props": props})

    def remove(self, node_id: str) -> None:
        self._granted_nodes.pop(node_id, None)
        self._notify("remove", {"id": node_id})

    def emit_vehicle_profile(self, session_node_id: str, profile: dict[str, Any]) -> None:
        """Send ``vehicle.profile`` (SPEC 15.4): ``profile`` is the SPEC 15.1
        profile without ``id`` / ``firstSeen`` / ``lastSeen`` — ``name``,
        optional ``vin``, ``identifiers``, ``ecus`` (``[{id, name, address,
        protocol, props}]``), ``notes``; ``pluginId`` may be omitted (the core
        stamps it). The core upserts the file and sets the session
        ``connected``; send it again when more identifiers are learned —
        re-emitting the full profile received in ``vehicle.attach`` is fine,
        the core-owned keys are stripped here (and ignored by the core)."""
        clean = {key: value for key, value in profile.items() if key not in _CORE_OWNED_PROFILE_KEYS}
        self._notify("vehicle.profile", {"sessionNodeId": session_node_id, "profile": clean})

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
        parent = parent_id or self.node_id
        self.add(
            {
                "id": job_id,
                "type": "job",
                "name": name,
                "status": "init",
                "parentId": parent,
                "actions": [dict(_CANCEL_ACTION)],
            }
        )
        job = _PluginJob(job_id, under_grant=self.is_granted(parent))
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
            # terminal jobs (node + bookkeeping) beyond JOB_RETENTION. Jobs
            # under a granted session subtree are removed by the core with
            # the session (SPEC 15.4), never by us.
            self._finished_jobs.append(job.id)
            while len(self._finished_jobs) > JOB_RETENTION:
                old_id = self._finished_jobs.pop(0)
                old_job = self._jobs.pop(old_id, None)
                if old_job is None or not old_job.under_grant:
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
        elif method in _VEHICLE_METHODS:
            self.create_task(self._run_vehicle_request(msg_id, str(method), message.get("params") or {}))
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

    async def _run_vehicle_request(self, msg_id: Any, method: str, params: dict[str, Any]) -> None:
        """``vehicle.detect`` / ``vehicle.attach`` / ``vehicle.detach`` (SPEC
        15.4): record the mount grant the core passed along, run the
        registered handler, answer ``{ok, jobNodeId?}`` or ``{ok: false, error}``."""
        session_id = params.get("sessionNodeId")
        handler = self._vehicle_handlers.get(method)
        if handler is None:
            self._send(
                {
                    "jsonrpc": "2.0",
                    "id": msg_id,
                    "error": {"code": -32601, "message": f"method not found: {method} (no handler registered)"},
                }
            )
            return
        if not isinstance(session_id, str) or not session_id:
            self._respond(msg_id, {"ok": False, "error": {"message": f"{method}: missing sessionNodeId"}})
            return
        try:
            if method == "vehicle.detect":
                self._granted_roots.add(session_id)
                result = handler(session_id, dict(params.get("params") or {}))
            elif method == "vehicle.attach":
                self._granted_roots.add(session_id)
                result = handler(session_id, dict(params.get("profile") or {}))
            else:
                result = handler(session_id)
            if inspect.isawaitable(result):
                result = await result
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if method != "vehicle.detach":
                self._forget_grant(session_id)
            self._respond(
                msg_id,
                {
                    "ok": False,
                    "error": {"message": str(exc) or type(exc).__name__, "detail": traceback.format_exc()},
                },
            )
            return
        if method == "vehicle.detach":
            self._forget_grant(session_id)
            self._respond(msg_id, {"ok": True})
            return
        response: dict[str, Any] = {"ok": True}
        if isinstance(result, str) and result:
            response["jobNodeId"] = result
        self._respond(msg_id, response)

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
