"""Plugin supervisor: spawn, JSON-RPC over stdio, heartbeat, restart (SPEC 6).

One :class:`PluginHost` per configured plugin. It owns the plugin node
(``plugin:<id>``, itself core-owned) and supervises the out-of-process plugin:

- spawn via asyncio subprocess (cwd = plugin dir, line-delimited JSON-RPC 2.0)
- ``initialize`` handshake, then the plugin declares its subtree via
  notifications (``add``/``update``/``merge_props``/``remove``), which are
  restricted to the plugin's own subtree — violations are logged and dropped
- heartbeat ``ping`` every 2 s with a 5 s timeout
- process exit or heartbeat timeout => plugin node + all descendants
  ``status: error`` (``plugin process died``), automatic restart with backoff
  1 s / 3 s / 10 s, max 3 attempts, then the node stays ``error`` and the
  ``restart`` action remains available
- stderr lines are logged by the host

M0.5 (SPEC 14.1/14.2): the host is the plugin seam of the audit LogHub —
plugin lifecycle (spawn, initialize, heartbeat timeout, exit code, restart
attempts), every plugin RPC line in/out (level ``trace``), and stderr lines
are logged automatically. ``samples`` notifications are exempt from RPC-line
logging: telemetry is deliberately NOT routed through the LogHub (it would
flood it). ``io`` notifications (raw vehicle frames) are validated against
the plugin subtree and land in the LogHub as category ``io`` — their content
is fully captured there, so their raw RPC line is not logged a second time.
``io.raw`` must be whole bytes as hex (whitespace between bytes is
tolerated and stripped; ``0x`` prefixes, odd lengths or non-hex characters
are dropped with a ``warn`` event) — the trail must replay byte-exact.
Plugin-declared job/step nodes (the M0 resolution in SPEC 3.2) additionally
get category ``job`` lifecycle events derived from their notifications — and
a terminal ``failed: plugin process died`` event when they die with their
plugin — so plugin authors get job auditing for free.

Plugin stdout lines whose JSON escapes materialize lone UTF-16 surrogates
(``\\uD800``-``\\uDFFF`` without a partner, e.g. bytes decoded with
``errors="surrogateescape"``) are not valid text: the host escapes them
(``backslashreplace``) before they can reach the tree, the LogHub or a
WebSocket, and audits the line.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import re
import sys
from pathlib import Path
from typing import Any

from pydantic import ValidationError

from ..loghub import LogCategory, LogHub, LogLevel, compact_json, sanitize_surrogates
from ..model import Action, CmdResult, ErrorInfo, Node
from ..telemetry import TelemetryHub
from ..tree import StateTree
from .manifest import Manifest

logger = logging.getLogger(__name__)

_LOG_LEVELS = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
    "critical": logging.CRITICAL,
}

#: Plugin ``log`` notification levels -> LogHub levels.
_HUB_LEVELS: dict[str, LogLevel] = {
    "trace": "trace",
    "debug": "debug",
    "info": "info",
    "warn": "warn",
    "warning": "warn",
    "error": "error",
    "critical": "error",
}

#: RPC lines carrying these notification methods are not logged as raw lines:
#: ``samples`` is telemetry (never routed through the LogHub), ``io`` gets its
#: own richer category-``io`` event instead (see module docstring).
_RPC_LINE_EXEMPT = frozenset({"samples", "io"})

_STREAM_LIMIT = 1024 * 1024  # max line length on plugin stdout/stderr

#: Cheap pre-filter for lines that MAY carry a lone surrogate: a JSON
#: ``\uD800``-``\uDFFF`` escape is the only way one can enter (the UTF-8
#: decoder with errors="replace" never produces surrogates). Legit surrogate
#: pairs (ensure_ascii-escaped emoji) also match; the sanitizer then finds
#: nothing to change and no event is emitted.
_SURROGATE_ESCAPE_RE = re.compile(r"\\u[dD][89a-fA-F]")

#: ``io.raw`` is whole bytes as hex (SPEC 14.2), checked after stripping
#: whitespace between byte pairs.
_HEX_BYTES_RE = re.compile(r"^(?:[0-9a-fA-F]{2})+$")

#: Node statuses in which a job/step is still in flight (a plugin death then
#: ends it with a terminal `job` error event).
_ACTIVE_JOB_STATUSES = frozenset({"init", "running"})


class PluginDiedError(RuntimeError):
    """The plugin process died (or was never running) during an RPC."""


class PluginRpcError(RuntimeError):
    """The plugin answered a request with a JSON-RPC error object."""


class PluginHost:
    def __init__(
        self,
        *,
        tree: StateTree,
        hub: TelemetryHub,
        manifest: Manifest,
        plugin_dir: Path | str,
        config: dict[str, Any] | None = None,
        parent_node_id: str = "plugins",
        python_exe: str | None = None,
        env: dict[str, str] | None = None,
        ping_interval: float = 2.0,
        ping_timeout: float = 5.0,
        init_timeout: float = 10.0,
        command_timeout: float = 30.0,
        restart_backoff: tuple[float, ...] = (1.0, 3.0, 10.0),
        max_restarts: int = 3,
        loghub: LogHub | None = None,
    ) -> None:
        self.tree = tree
        self.hub = hub
        self.loghub = loghub
        self.manifest = manifest
        self.plugin_dir = Path(plugin_dir)
        self.config = dict(config or {})
        self.parent_node_id = parent_node_id
        self.node_id = f"plugin:{manifest.id}"
        #: Owner string stamped onto every node the plugin declares.
        self.owner = self.node_id

        self._python_exe = python_exe
        self._extra_env = dict(env or {})
        self._ping_interval = ping_interval
        self._ping_timeout = ping_timeout
        self._init_timeout = init_timeout
        self._command_timeout = command_timeout
        self._backoff = restart_backoff
        self._max_restarts = max_restarts

        self._log = logging.getLogger(f"diagcore.plugin.{manifest.id}")
        self._proc: asyncio.subprocess.Process | None = None
        self._pending: dict[int, asyncio.Future[Any]] = {}
        self._rpc_id = 0
        self._healthy = False
        self._stopping = False
        self._attempts = 0
        self._death: asyncio.Future[str] | None = None
        self._supervisor: asyncio.Task[None] | None = None
        self._stop_event: asyncio.Event | None = None
        #: Last logged progress quarter (0..4) per plugin-declared job node,
        #: for 25 % milestone events derived from update notifications.
        self._job_milestones: dict[str, int] = {}

    def _emit(
        self,
        level: LogLevel,
        category: LogCategory,
        message: str,
        *,
        source: str | None = None,
        node_id: str | None = None,
        data: dict[str, Any] | None = None,
        raw: str | None = None,
    ) -> None:
        """Audit-log helper; no-op without a LogHub (e.g. bare unit tests)."""
        if self.loghub is not None:
            self.loghub.emit(
                level,
                category,
                message,
                source=source or "core",
                node_id=node_id if node_id is not None else self.node_id,
                data=data,
                raw=raw,
            )

    def _drop_notification(self, method: str | None, reason: str, params: Any) -> None:
        """Refuse a plugin notification (outside the plugin's own subtree, or
        malformed): logged and dropped (SPEC 6) — into the stdlib log AND the
        audit LogHub (level ``warn``, category ``plugin``, source = the
        plugin), so a misbehaving plugin is never invisible (SPEC 14: nothing
        happens without a log). ``params`` is kept in full in ``data``."""
        reason = " ".join(reason.split())
        self._log.warning("dropped %r notification: %s", method, reason)
        self._emit(
            "warn",
            "plugin",
            f"plugin {self.manifest.id} notification {method!r} dropped: {reason[:300]}"
            f" params={compact_json(params, 240)}",
            source=self.owner,
            data={"method": method, "reason": reason, "params": params},
        )

    # -- public state ------------------------------------------------------

    @property
    def healthy(self) -> bool:
        return self._healthy

    @property
    def restart_attempts(self) -> int:
        return self._attempts

    # -- lifecycle ---------------------------------------------------------

    def create_node(self) -> None:
        """Create the plugin node under ``plugins`` (idempotent)."""
        if self.tree.get(self.node_id) is None:
            self.tree.add(
                Node(
                    id=self.node_id,
                    type="plugin",
                    name=self.manifest.name,
                    status="init",
                    owner="core",
                    parentId=self.parent_node_id,
                    actions=[Action(name="restart", label="Restart plugin")],
                    props={"pluginId": self.manifest.id, "version": self.manifest.version},
                )
            )

    async def start(self) -> None:
        self.create_node()
        self._stop_event = asyncio.Event()
        self._supervisor = asyncio.get_running_loop().create_task(
            self._supervise(), name=f"diagcore-plugin-{self.manifest.id}"
        )

    async def stop(self) -> None:
        """Graceful shutdown: ask the plugin to exit, then reap everything."""
        if not self._stopping:
            self._emit("info", "plugin", f"plugin {self.manifest.id} host stopping")
        self._stopping = True
        if self._stop_event is not None:
            self._stop_event.set()
        if self._healthy:
            try:
                await asyncio.wait_for(self._request("shutdown", {}), 2.0)
            except (PluginDiedError, PluginRpcError, asyncio.TimeoutError):
                pass
        self._signal_death("host stopped")
        supervisor = self._supervisor
        if supervisor is not None and not supervisor.done():
            try:
                await asyncio.wait_for(asyncio.shield(supervisor), 10.0)
            except asyncio.TimeoutError:
                supervisor.cancel()
                await asyncio.gather(supervisor, return_exceptions=True)

    async def restart(self) -> None:
        """Manual restart (the plugin node's ``restart`` action)."""
        if self._stopping:
            return
        self._emit("info", "plugin", f"plugin {self.manifest.id} manual restart requested")
        self._attempts = 0
        if self._supervisor is None or self._supervisor.done():
            if self._stop_event is None:
                self._stop_event = asyncio.Event()
            self._supervisor = asyncio.get_running_loop().create_task(
                self._supervise(), name=f"diagcore-plugin-{self.manifest.id}"
            )
        elif self._healthy:
            self._signal_death("manual restart requested")

    # -- command forwarding (bus handler for plugin-owned nodes) -----------

    async def handle_command(self, node: Node, action: str, params: dict[str, Any]) -> CmdResult:
        if not self._healthy:
            return CmdResult(
                ok=False,
                error=ErrorInfo(message=f"plugin {self.manifest.id!r} is not running"),
            )
        try:
            result = await asyncio.wait_for(
                self._request("command", {"target": node.id, "action": action, "params": params}),
                self._command_timeout,
            )
        except PluginDiedError as exc:
            return CmdResult(ok=False, error=ErrorInfo(message="plugin process died", detail=str(exc)))
        except PluginRpcError as exc:
            return CmdResult(ok=False, error=ErrorInfo(message=f"plugin rejected command: {exc}"))
        except asyncio.TimeoutError:
            return CmdResult(ok=False, error=ErrorInfo(message=f"command {action!r} timed out"))

        if not isinstance(result, dict):
            return CmdResult(ok=False, error=ErrorInfo(message=f"invalid command result: {result!r}"))
        if result.get("ok"):
            data = result.get("data")
            return CmdResult(ok=True, data=data if isinstance(data, dict) else {})
        error = result.get("error")
        if not isinstance(error, dict):
            error = {"message": "command failed"}
        return CmdResult(
            ok=False,
            error=ErrorInfo(
                message=str(error.get("message", "command failed")),
                detail=error.get("detail"),
            ),
        )

    # -- supervision -------------------------------------------------------

    async def _supervise(self) -> None:
        while not self._stopping:
            reason = await self._run_once()
            if self._stopping:
                return
            self._log.error("plugin %s died: %s", self.manifest.id, reason)
            self._emit(
                "error",
                "plugin",
                f"plugin {self.manifest.id} died: {reason}",
                data={"pluginId": self.manifest.id, "reason": reason},
            )
            self._mark_subtree_error(reason)
            self._attempts += 1
            if self._attempts > self._max_restarts:
                self._log.error(
                    "plugin %s: giving up after %d restart attempts; awaiting manual restart",
                    self.manifest.id,
                    self._max_restarts,
                )
                self._emit(
                    "error",
                    "plugin",
                    f"plugin {self.manifest.id} giving up after {self._max_restarts} restart attempts;"
                    " awaiting manual restart",
                    data={"pluginId": self.manifest.id, "maxRestarts": self._max_restarts},
                )
                return
            delay = self._backoff[min(self._attempts - 1, len(self._backoff) - 1)]
            self._log.info("plugin %s: restart attempt %d in %.1fs", self.manifest.id, self._attempts, delay)
            self._emit(
                "warn",
                "plugin",
                f"plugin {self.manifest.id} restart attempt {self._attempts}/{self._max_restarts}"
                f" in {delay:.1f}s",
                data={"pluginId": self.manifest.id, "attempt": self._attempts, "delaySeconds": delay},
            )
            assert self._stop_event is not None
            try:
                await asyncio.wait_for(self._stop_event.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass

    async def _run_once(self) -> str:
        """One process lifecycle; returns the death reason."""
        loop = asyncio.get_running_loop()
        self._death = loop.create_future()
        try:
            self.tree.update(self.node_id, {"status": "init", "error": None, "progress": None})
        except ValueError:
            self._log.warning("plugin node %s missing while (re)starting", self.node_id)

        if self.manifest.python == "default":
            python = self._python_exe or sys.executable
        else:
            python = self.manifest.python
        entry = self.plugin_dir / self.manifest.entry
        env = {
            **os.environ,
            "PYTHONUNBUFFERED": "1",
            "PYTHONIOENCODING": "utf-8",
            **self._extra_env,
        }

        try:
            proc = await asyncio.create_subprocess_exec(
                python,
                str(entry),
                cwd=str(self.plugin_dir),
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
                env=env,
                limit=_STREAM_LIMIT,
            )
        except OSError as exc:
            self._emit(
                "error",
                "plugin",
                f"plugin {self.manifest.id} spawn failed: {exc} (python={python} entry={entry})",
                data={"pluginId": self.manifest.id, "python": python, "entry": str(entry)},
            )
            return f"spawn failed: {exc}"

        self._emit(
            "info",
            "plugin",
            f"plugin {self.manifest.id} spawned pid={proc.pid} (python={python} entry={entry})",
            data={"pluginId": self.manifest.id, "pid": proc.pid, "python": python, "entry": str(entry)},
        )
        self._proc = proc
        # SPEC 6: the old subtree is removed only once a restart attempt has a
        # new process running — a failed spawn keeps the error-marked subtree
        # visible (the evidence of WHAT died). The plugin declares nodes only
        # after `initialize`, and the stdout reader has not started yet, so
        # removing here is race-free.
        self._remove_declared_subtree()
        io_tasks = [
            loop.create_task(self._read_stdout(proc), name=f"diagcore-plugin-{self.manifest.id}-stdout"),
            loop.create_task(self._read_stderr(proc), name=f"diagcore-plugin-{self.manifest.id}-stderr"),
            loop.create_task(self._watch_exit(proc), name=f"diagcore-plugin-{self.manifest.id}-exit"),
        ]
        heartbeat_task: asyncio.Task[None] | None = None
        reason = "plugin process ended"
        try:
            try:
                result = await asyncio.wait_for(
                    self._request("initialize", {"pluginNodeId": self.node_id, "config": self.config}),
                    self._init_timeout,
                )
            except asyncio.TimeoutError:
                self._signal_death("initialize timed out")
            except (PluginDiedError, PluginRpcError) as exc:
                self._signal_death(f"initialize failed: {exc}")
            else:
                if not (isinstance(result, dict) and result.get("ok") is True):
                    self._signal_death(f"initialize rejected: {result!r}")
                else:
                    try:
                        self.tree.update(self.node_id, {"status": "idle"})
                    except ValueError:
                        pass
                    self._attempts = 0
                    self._healthy = True
                    self._log.info("plugin %s initialized", self.manifest.id)
                    self._emit(
                        "info",
                        "plugin",
                        f"plugin {self.manifest.id} initialized (node {self.node_id})",
                        data={"pluginId": self.manifest.id, "version": self.manifest.version},
                    )
                    heartbeat_task = loop.create_task(self._heartbeat())
            reason = await self._death
        finally:
            self._healthy = False
            if heartbeat_task is not None:
                heartbeat_task.cancel()
            for task in io_tasks:
                task.cancel()
            if proc.returncode is None:
                try:
                    proc.kill()
                except ProcessLookupError:
                    pass
            try:
                await asyncio.shield(proc.wait())
            except asyncio.CancelledError:
                pass
            self._fail_pending(PluginDiedError(reason))
            self._proc = None
            io_results = await asyncio.gather(*io_tasks, return_exceptions=True)
            for task, result in zip(io_tasks, io_results):
                # CancelledError is BaseException, so expected cancellations
                # are excluded here; anything else is a real bug — never
                # swallow it silently (SPEC 1.5).
                if isinstance(result, Exception):
                    self._log.error("io task %s failed: %r", task.get_name(), result)
            if heartbeat_task is not None:
                await asyncio.gather(heartbeat_task, return_exceptions=True)
        return reason

    async def _heartbeat(self) -> None:
        while True:
            await asyncio.sleep(self._ping_interval)
            try:
                await asyncio.wait_for(self._request("ping", {}), self._ping_timeout)
            except asyncio.TimeoutError:
                self._signal_death("heartbeat timeout")
                return
            except (PluginDiedError, PluginRpcError) as exc:
                self._signal_death(f"heartbeat failed: {exc}")
                return

    def _signal_death(self, reason: str) -> None:
        fut = self._death
        if fut is not None and not fut.done():
            fut.set_result(reason)
        self._fail_pending(PluginDiedError(reason))

    def _fail_pending(self, exc: Exception) -> None:
        pending, self._pending = self._pending, {}
        for fut in pending.values():
            if not fut.done():
                fut.set_exception(exc)

    async def _watch_exit(self, proc: asyncio.subprocess.Process) -> None:
        returncode = await proc.wait()
        if self._stopping:
            self._signal_death("host stopped")
        else:
            self._signal_death(f"process exited with code {returncode}")

    # -- tree bookkeeping --------------------------------------------------

    def _mark_subtree_error(self, reason: str) -> None:
        """SPEC 6: plugin node + all descendants => ``error``.

        Plugin-declared jobs/steps that were still in flight additionally get
        their terminal category-``job`` error event here (SPEC 14.1 "job
        lifecycle incl. errors"): a dead plugin sends no notification, so the
        derived-event seam in ``_notify_update`` never fires for them, and a
        job-subtree log view would otherwise show a job running forever.
        Already-terminal jobs are flipped to ``error`` too (SPEC 6) but not
        re-reported as failures.
        """
        error = {"message": "plugin process died", "detail": reason}
        for node_id in self.tree.subtree_ids(self.node_id):
            node = self.tree.get(node_id)
            if node is None:
                continue
            previous_status = node.status
            try:
                self.tree.update(node_id, {"status": "error", "error": error})
            except ValueError:
                continue
            if node.type not in ("job", "job.step"):
                continue
            self._job_milestones.pop(node_id, None)
            if previous_status in _ACTIVE_JOB_STATUSES:
                label = "job" if node.type == "job" else "job step"
                self._emit(
                    "error",
                    "job",
                    f"{label} {node_id} {node.name!r} failed: plugin process died ({reason})",
                    source=self.owner,
                    node_id=node_id,
                    data={"error": error, "previousStatus": previous_status},
                )

    def _remove_declared_subtree(self) -> None:
        node = self.tree.get(self.node_id)
        if node is None:
            return
        for child_id in list(node.children):
            removed_ids = self.tree.subtree_ids(child_id)
            try:
                self.tree.remove(child_id)
            except ValueError:
                continue
            # Drop telemetry rings for removed nodes so dead channel ids do
            # not pin buffers and a re-declared channel starts with fresh
            # history instead of replaying pre-crash samples.
            for removed_id in removed_ids:
                self.hub.drop(removed_id)
        # The whole declared subtree is gone; plugin-job milestone tracking
        # starts fresh with the re-declared nodes.
        self._job_milestones.clear()

    # -- RPC plumbing ------------------------------------------------------

    async def _request(self, method: str, params: dict[str, Any]) -> Any:
        proc = self._proc
        if proc is None or proc.stdin is None:
            raise PluginDiedError("plugin process is not running")
        self._rpc_id += 1
        rpc_id = self._rpc_id
        fut: asyncio.Future[Any] = asyncio.get_running_loop().create_future()
        self._pending[rpc_id] = fut
        line = json.dumps({"jsonrpc": "2.0", "id": rpc_id, "method": method, "params": params})
        # Every RPC line out at trace (SPEC 14.1); `ping` included — it is the
        # heartbeat evidence.
        self._emit(
            "trace",
            "plugin",
            f"rpc tx [{self.manifest.id}] {method}: {line[:240]}",
            data={"line": line},
        )
        try:
            proc.stdin.write(line.encode("utf-8") + b"\n")
            await proc.stdin.drain()
        except (ConnectionError, BrokenPipeError, OSError) as exc:
            self._pending.pop(rpc_id, None)
            raise PluginDiedError(f"stdin write failed: {exc}") from exc
        try:
            return await fut
        finally:
            self._pending.pop(rpc_id, None)

    async def _read_stdout(self, proc: asyncio.subprocess.Process) -> None:
        assert proc.stdout is not None
        while True:
            try:
                line = await proc.stdout.readline()
            except (ValueError, OSError) as exc:  # over-long line / closed pipe
                self._log.warning("stdout read error: %s", exc)
                continue
            if not line:
                return  # EOF; _watch_exit signals the death
            text = line.decode("utf-8", errors="replace").strip()
            if not text:
                continue
            try:
                message = json.loads(text)
            except json.JSONDecodeError as exc:
                # stdout is the protocol channel (SPEC 6); a stray print() in
                # a plugin is a bug worth seeing in the audit trail.
                self._log.warning("invalid JSON from plugin: %.200s", text)
                self._emit(
                    "warn",
                    "plugin",
                    f"plugin {self.manifest.id} stdout line is not JSON ({exc.msg} at column"
                    f" {exc.colno}); dropped: {text[:240]}",
                    source=self.owner,
                    data={"line": text, "error": str(exc)},
                )
                continue
            if not isinstance(message, dict):
                self._log.warning("non-object JSON from plugin: %.200s", text)
                self._emit(
                    "warn",
                    "plugin",
                    f"plugin {self.manifest.id} stdout line is not a JSON object; dropped: {text[:240]}",
                    source=self.owner,
                    data={"line": text},
                )
                continue
            if _SURROGATE_ESCAPE_RE.search(text):
                clean = sanitize_surrogates(message)
                if clean is not message:
                    # Not valid text (see module docstring): escaped so it can
                    # neither poison the tree (every /ws/state hello would
                    # fail to encode) nor the log sinks — and audited.
                    message = clean
                    self._log.warning("plugin line carried lone surrogates; escaped: %.200s", text)
                    self._emit(
                        "warn",
                        "plugin",
                        f"plugin {self.manifest.id} stdout line carried lone UTF-16 surrogates (not valid"
                        f" text, e.g. bytes decoded with errors='surrogateescape'); escaped: {text[:240]}",
                        source=self.owner,
                        data={"line": text},
                    )
            # Every RPC line in at trace (SPEC 14.1) — except `samples`
            # (telemetry, never routed through the LogHub) and `io` (fully
            # captured as its own category-`io` event below).
            if message.get("method") not in _RPC_LINE_EXEMPT:
                self._emit(
                    "trace",
                    "plugin",
                    f"rpc rx [{self.manifest.id}]: {text[:240]}",
                    source=self.owner,
                    data={"line": text},
                )
            try:
                if "id" in message and ("result" in message or "error" in message):
                    self._handle_response(message)
                elif "method" in message:
                    params = message.get("params")
                    if params is None:
                        params = {}
                    if not isinstance(params, dict):
                        # JSON-RPC 2.0 permits positional params (an array),
                        # but every method of this protocol takes an object.
                        self._drop_notification(
                            message.get("method"),
                            "params must be a JSON object (positional params are not supported)",
                            params,
                        )
                        continue
                    self._handle_notification(message.get("method"), params)
                else:
                    self._log.warning("unroutable message from plugin: %.200s", text)
                    self._emit(
                        "warn",
                        "plugin",
                        f"plugin {self.manifest.id} sent an unroutable JSON-RPC message"
                        f" (neither response nor notification); dropped: {text[:240]}",
                        source=self.owner,
                        data={"line": text},
                    )
            except Exception:
                # Last-resort guard (SPEC 12): one bad line must never kill
                # the reader task — that would silently take down heartbeat
                # handling for a healthy plugin.
                self._log.exception("error handling plugin line; dropped: %.200s", text)

    def _handle_response(self, message: dict[str, Any]) -> None:
        msg_id = message.get("id")
        try:
            fut = self._pending.get(msg_id)  # type: ignore[arg-type]
        except TypeError:  # unhashable id (e.g. a list)
            self._log.warning("dropped response with unhashable id: %.100r", msg_id)
            return
        if fut is None or fut.done():
            return
        if "error" in message:
            fut.set_exception(PluginRpcError(str(message["error"])))
        else:
            fut.set_result(message.get("result"))

    def _handle_notification(self, method: str | None, params: dict[str, Any]) -> None:
        try:
            if method == "add":
                self._notify_add(params)
            elif method == "update":
                self._notify_update(params)
            elif method == "merge_props":
                self._notify_merge_props(params)
            elif method == "remove":
                self._notify_remove(params)
            elif method == "samples":
                # Telemetry samples deliberately bypass the LogHub (SPEC 14
                # design decision: they would flood it; `io` is the audited
                # vehicle-frame channel).
                self.hub.publish(params["channel"], params.get("samples") or [])
            elif method == "io":
                self._notify_io(params)
            elif method == "log":
                raw_level = str(params.get("level", "info")).lower()
                text = str(params.get("message", ""))
                self._log.log(_LOG_LEVELS.get(raw_level, logging.INFO), "plugin: %s", text)
                self._emit(
                    _HUB_LEVELS.get(raw_level, "info"),
                    "plugin",
                    f"plugin {self.manifest.id}: {text}",
                    source=self.owner,
                )
            else:
                self._drop_notification(method, "unknown notification method", params)
        except (ValidationError, ValueError, KeyError, TypeError) as exc:
            self._drop_notification(method, f"{type(exc).__name__}: {exc}", params)
        except Exception:
            # Last resort (SPEC 12): drop the notification with full context
            # instead of letting the exception kill the stdout reader.
            self._log.exception("dropped %r notification after unexpected error", method)

    def _in_subtree(self, node_id: Any) -> bool:
        """Membership test for the plugin's own subtree, O(depth) and
        allocation-free — this runs once per notification, including every
        `io` frame at bus rate. An unhashable id raises TypeError, which
        ``_handle_notification`` turns into a dropped notification."""
        return self.tree.is_within(node_id, self.node_id)

    def _notify_add(self, params: dict[str, Any]) -> None:
        raw = params.get("node")
        if not isinstance(raw, dict):
            raise ValueError("add: missing node object")
        data = dict(raw)
        data.setdefault("parentId", self.node_id)
        data["owner"] = self.owner  # enforced, whatever the plugin claims
        if not self._in_subtree(data.get("parentId")):
            self._drop_notification(
                "add",
                f"parentId {data.get('parentId')!r} is outside the plugin subtree (node id {data.get('id')!r})",
                params,
            )
            return
        index = params.get("index")
        stored = self.tree.add(Node.model_validate(data), index=index if isinstance(index, int) else None)
        self._log_job_add(stored)

    def _notify_update(self, params: dict[str, Any]) -> None:
        node_id = params.get("id")
        if not self._in_subtree(node_id):
            self._drop_notification("update", f"node {node_id!r} is outside the plugin subtree", params)
            return
        changes = params.get("changes")
        if not isinstance(changes, dict):
            raise ValueError("update: missing changes object")
        changes.pop("owner", None)  # plugins cannot re-own nodes
        self.tree.update(node_id, changes)
        self._log_job_update(node_id, changes)

    def _notify_merge_props(self, params: dict[str, Any]) -> None:
        node_id = params.get("id")
        if not self._in_subtree(node_id):
            self._drop_notification("merge_props", f"node {node_id!r} is outside the plugin subtree", params)
            return
        props = params.get("props")
        if not isinstance(props, dict):
            raise ValueError("merge_props: missing props object")
        self.tree.merge_props(node_id, props)

    def _notify_remove(self, params: dict[str, Any]) -> None:
        node_id = params.get("id")
        if node_id == self.node_id or not self._in_subtree(node_id):
            self._drop_notification(
                "remove",
                f"node {node_id!r} is the plugin node itself or outside the plugin subtree",
                params,
            )
            return
        removed_ids = self.tree.subtree_ids(node_id)
        self.tree.remove(node_id)
        for removed_id in removed_ids:
            self.hub.drop(removed_id)
            self._job_milestones.pop(removed_id, None)

    def _notify_io(self, params: dict[str, Any]) -> None:
        """Raw vehicle I/O notification (SPEC 14.2): validate, feed the LogHub.

        Like the tree notifications, the node must belong to the plugin's own
        subtree — violations are logged and dropped. ``raw`` must be whole
        bytes as hex (``frame.hex()``; whitespace between bytes is stripped):
        the event is the audit evidence and must replay via ``bytes.fromhex``.
        """
        node_id = params.get("nodeId")
        if not self._in_subtree(node_id):
            self._drop_notification("io", f"nodeId {node_id!r} is outside the plugin subtree", params)
            return
        direction = params.get("direction")
        if direction not in ("tx", "rx"):
            raise ValueError(f"io: invalid direction {direction!r} (expected 'tx' or 'rx')")
        raw = params.get("raw")
        if not isinstance(raw, str) or not raw:
            raise ValueError("io: missing raw hex string")
        raw = "".join(raw.split())
        if not _HEX_BYTES_RE.fullmatch(raw):
            raise ValueError(
                f"io: raw must be an even-length hex string (whole bytes, no '0x' prefix), got {params.get('raw')!r}"
            )
        decoded = params.get("decoded")
        if decoded is not None and not isinstance(decoded, dict):
            raise ValueError("io: decoded must be an object")
        note = params.get("note")
        if note is not None and not isinstance(note, str):
            raise ValueError("io: note must be a string")

        message = f"io {direction} {node_id} raw={raw[:96]}"
        if decoded is not None:
            message += f" decoded={compact_json(decoded, 160)}"
        if note:
            message += f" note={note!r}"
        self._emit(
            "info",
            "io",
            message,
            source=self.owner,
            node_id=node_id,
            data={"direction": direction, "decoded": decoded, "note": note},
            raw=raw,
        )

    # -- derived job auditing for plugin-declared jobs (M0 resolution) ------

    def _log_job_add(self, node: Node) -> None:
        if self.loghub is None:
            return
        if node.type == "job":
            self._job_milestones[node.id] = 0
            self._emit(
                "info",
                "job",
                f"job {node.id} {node.name!r} created by plugin {self.manifest.id}",
                source=self.owner,
                node_id=node.id,
                data={"jobId": node.id, "name": node.name},
            )
        elif node.type == "job.step":
            verb = "started" if node.status == "running" else f"created ({node.status})"
            self._emit(
                "info",
                "job",
                f"job step {node.id} {node.name!r} {verb} (parent {node.parentId})",
                source=self.owner,
                node_id=node.id,
            )

    def _log_job_update(self, node_id: str, changes: dict[str, Any]) -> None:
        if self.loghub is None:
            return
        node = self.tree.get(node_id)
        if node is None or node.type not in ("job", "job.step"):
            return
        label = "job" if node.type == "job" else "job step"
        if "status" in changes:
            if node.status == "error":
                error_message = node.error.message if node.error else "unknown error"
                self._emit(
                    "error",
                    "job",
                    f"{label} {node_id} {node.name!r} failed: {error_message}",
                    source=self.owner,
                    node_id=node_id,
                    data={"error": node.error.model_dump(mode="json") if node.error else None},
                )
            elif node.status == "running":
                self._emit(
                    "info",
                    "job",
                    f"{label} {node_id} {node.name!r} started",
                    source=self.owner,
                    node_id=node_id,
                )
            elif node.status in ("done", "cancelled"):
                self._emit(
                    "info",
                    "job",
                    f"{label} {node_id} {node.name!r} {node.status}",
                    source=self.owner,
                    node_id=node_id,
                )
        if node.type == "job" and "progress" in changes and node.progress is not None:
            quarter = int(node.progress * 4)
            if quarter > self._job_milestones.get(node_id, 0):
                self._job_milestones[node_id] = quarter
                # 100 % coincides with the `done` status event; skip it here.
                if 1 <= quarter <= 3:
                    self._emit(
                        "info",
                        "job",
                        f"job {node_id} {node.name!r} progress {quarter * 25}% ({node.progress:.2f})",
                        source=self.owner,
                        node_id=node_id,
                        data={"progress": node.progress},
                    )

    async def _read_stderr(self, proc: asyncio.subprocess.Process) -> None:
        assert proc.stderr is not None
        while True:
            try:
                line = await proc.stderr.readline()
            except (ValueError, OSError):
                continue
            if not line:
                return
            text = line.decode("utf-8", errors="replace").rstrip()
            self._log.info("[stderr] %s", text)
            self._emit(
                "info",
                "plugin",
                f"plugin {self.manifest.id} stderr: {text}",
                source=self.owner,
                data={"line": text},
            )
