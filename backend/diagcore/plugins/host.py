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
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from pathlib import Path
from typing import Any

from pydantic import ValidationError

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

_STREAM_LIMIT = 1024 * 1024  # max line length on plugin stdout/stderr


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
    ) -> None:
        self.tree = tree
        self.hub = hub
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
            self._mark_subtree_error(reason)
            self._attempts += 1
            if self._attempts > self._max_restarts:
                self._log.error(
                    "plugin %s: giving up after %d restart attempts; awaiting manual restart",
                    self.manifest.id,
                    self._max_restarts,
                )
                return
            delay = self._backoff[min(self._attempts - 1, len(self._backoff) - 1)]
            self._log.info("plugin %s: restart attempt %d in %.1fs", self.manifest.id, self._attempts, delay)
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
            return f"spawn failed: {exc}"

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
        error = {"message": "plugin process died", "detail": reason}
        for node_id in self.tree.subtree_ids(self.node_id):
            try:
                self.tree.update(node_id, {"status": "error", "error": error})
            except ValueError:
                pass

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
            except json.JSONDecodeError:
                self._log.warning("invalid JSON from plugin: %.200s", text)
                continue
            if not isinstance(message, dict):
                self._log.warning("non-object JSON from plugin: %.200s", text)
                continue
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
                        self._log.warning("dropped notification with non-object params: %.200s", text)
                        continue
                    self._handle_notification(message.get("method"), params)
                else:
                    self._log.warning("unroutable message from plugin: %.200s", text)
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
                self.hub.publish(params["channel"], params.get("samples") or [])
            elif method == "log":
                level = _LOG_LEVELS.get(str(params.get("level", "info")).lower(), logging.INFO)
                self._log.log(level, "plugin: %s", params.get("message", ""))
            else:
                self._log.warning("unknown notification from plugin: %r", method)
        except (ValidationError, ValueError, KeyError, TypeError) as exc:
            self._log.warning("dropped invalid %r notification: %s", method, exc)
        except Exception:
            # Last resort (SPEC 12): drop the notification with full context
            # instead of letting the exception kill the stdout reader.
            self._log.exception("dropped %r notification after unexpected error", method)

    def _subtree(self) -> set[str]:
        return set(self.tree.subtree_ids(self.node_id))

    def _notify_add(self, params: dict[str, Any]) -> None:
        raw = params.get("node")
        if not isinstance(raw, dict):
            raise ValueError("add: missing node object")
        data = dict(raw)
        data.setdefault("parentId", self.node_id)
        data["owner"] = self.owner  # enforced, whatever the plugin claims
        if data.get("parentId") not in self._subtree():
            self._log.warning(
                "dropped add outside plugin subtree: id=%r parentId=%r",
                data.get("id"),
                data.get("parentId"),
            )
            return
        index = params.get("index")
        self.tree.add(Node.model_validate(data), index=index if isinstance(index, int) else None)

    def _notify_update(self, params: dict[str, Any]) -> None:
        node_id = params.get("id")
        if node_id not in self._subtree():
            self._log.warning("dropped update outside plugin subtree: id=%r", node_id)
            return
        changes = params.get("changes")
        if not isinstance(changes, dict):
            raise ValueError("update: missing changes object")
        changes.pop("owner", None)  # plugins cannot re-own nodes
        self.tree.update(node_id, changes)

    def _notify_merge_props(self, params: dict[str, Any]) -> None:
        node_id = params.get("id")
        if node_id not in self._subtree():
            self._log.warning("dropped merge_props outside plugin subtree: id=%r", node_id)
            return
        props = params.get("props")
        if not isinstance(props, dict):
            raise ValueError("merge_props: missing props object")
        self.tree.merge_props(node_id, props)

    def _notify_remove(self, params: dict[str, Any]) -> None:
        node_id = params.get("id")
        if node_id == self.node_id or node_id not in self._subtree():
            self._log.warning("dropped remove outside plugin subtree: id=%r", node_id)
            return
        removed_ids = self.tree.subtree_ids(node_id)
        self.tree.remove(node_id)
        for removed_id in removed_ids:
            self.hub.drop(removed_id)

    async def _read_stderr(self, proc: asyncio.subprocess.Process) -> None:
        assert proc.stderr is not None
        while True:
            try:
                line = await proc.stderr.readline()
            except (ValueError, OSError):
                continue
            if not line:
                return
            self._log.info("[stderr] %s", line.decode("utf-8", errors="replace").rstrip())
