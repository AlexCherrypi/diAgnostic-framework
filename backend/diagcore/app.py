"""Composition root: well-known nodes, job engine, telemetry hub, plugins.

``AppCore`` wires the pieces together (SPEC 3.2, 7, 14): it builds the state
tree with ``root`` / ``system`` / ``plugins`` / ``jobs``, owns the command bus,
the job engine and the audit LogHub (SPEC 14.1 — every state patch is logged
as category ``state`` at trace level via a tree subscription installed before
any node exists), and loads every plugin found under ``backend/plugins/*``.
"""

from __future__ import annotations

import asyncio
import logging
import platform
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import __version__
from .bus import CommandBus
from .jobs import JobEngine
from .loghub import DEFAULT_LOG_DIR, LogHub, compact_json
from .model import CmdResult, ErrorInfo, Node
from .plugins.host import PluginHost
from .plugins.manifest import load_manifest
from .telemetry import TelemetryHub
from .tree import StateTree

logger = logging.getLogger(__name__)

DEFAULT_PLUGINS_DIR = Path(__file__).resolve().parent.parent / "plugins"


class AppCore:
    def __init__(
        self,
        *,
        plugins_dir: Path | str | None = None,
        plugin_env: dict[str, str] | None = None,
        log_dir: Path | str | None = None,
    ) -> None:
        self.tree = StateTree()
        # LogHub first, subscribed before any node exists: even the well-known
        # node adds below are audited (SPEC 14: nothing happens without a log).
        self.logs = LogHub(
            self.tree,
            session_dir=Path(log_dir) if log_dir is not None else DEFAULT_LOG_DIR,
        )
        self.tree.subscribe(self._log_state_patch)
        self.logs.emit(
            "info",
            "system",
            f"session started: version={__version__} python={platform.python_version()}"
            f" log={self.logs.session_path}",
            data={"version": __version__, "sessionFile": self._session_file()},
        )
        self.hub = TelemetryHub()
        self.jobs = JobEngine(self.tree, loghub=self.logs)
        self.bus = CommandBus(self.tree, loghub=self.logs)
        self.bus.register("core", self._handle_core_command)
        self.hosts: dict[str, PluginHost] = {}
        self.plugins_dir = Path(plugins_dir) if plugins_dir is not None else DEFAULT_PLUGINS_DIR
        self._plugin_env = plugin_env
        self._started = False
        self._started_monotonic: float | None = None
        self._build_well_known_nodes()
        # SPEC 1.5 for the audit trail itself: a dead session file must show
        # up as an error on a node. Wire the one-shot report now (the
        # `system` node exists) and cover a failure that already happened
        # while opening the file at construction time.
        self.logs.on_sink_failure = self._on_audit_sink_failure
        if self.logs.sink_error is not None:
            self._mark_audit_sink_failure(self.logs.sink_error)

    def _session_file(self) -> str | None:
        return str(self.logs.session_path) if self.logs.session_path is not None else None

    # -- audit sink failure => visible on the `system` node (SPEC 1.5) -------

    def _on_audit_sink_failure(self, error: str) -> None:
        # Called from inside LogHub.emit, which frequently runs inside a tree
        # subscriber (the state seam). A nested tree mutation there would
        # broadcast seq N+1 before seq N and force every /ws/state client to
        # resync — so defer to the loop when one is running.
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            self._mark_audit_sink_failure(error)
        else:
            loop.call_soon(self._mark_audit_sink_failure, error)

    def _mark_audit_sink_failure(self, error: str) -> None:
        node = self.tree.get("system")
        if node is None:
            return
        props = {**node.props, "auditFile": self._session_file(), "auditFileError": error}
        self.tree.update(
            "system",
            {
                "status": "error",
                "error": {"message": "audit session file disabled", "detail": error},
                "props": props,
            },
        )

    # -- state seam (SPEC 14.1: every patch, category `state`, level trace) --

    def _log_state_patch(self, seq: int, op: dict[str, Any]) -> None:
        kind = op["op"]
        source = "core"
        if kind == "add":
            node = op["node"]
            node_id = node["id"]
            source = node.get("owner") or "core"
            message = (
                f"state add {node_id} type={node['type']} parent={node['parentId']}"
                f" status={node['status']}"
            )
        elif kind in ("update", "merge_props"):
            node_id = op["id"]
            node = self.tree.get(node_id)
            if node is not None:
                source = node.owner
            changed = op["changes"] if kind == "update" else op["props"]
            # compact diff: only the changed keys with their new values
            diff = " ".join(f"{key}={compact_json(value, 80)}" for key, value in changed.items())
            message = f"state {kind} {node_id}: {diff}"
        else:  # remove
            node_id = op["id"]
            message = f"state remove {node_id} (and descendants)"
        self.logs.emit(
            "trace",
            "state",
            message,
            source=source,
            node_id=node_id,
            data={"seq": seq, "op": op},
        )

    # -- tree bootstrap ----------------------------------------------------

    def _build_well_known_nodes(self) -> None:
        self.tree.add(Node(id="root", type="root", name="diAgnostic", status="idle", owner="core"))
        self.tree.add(
            Node(
                id="system",
                type="system",
                name="System",
                status="idle",
                owner="core",
                parentId="root",
                props={
                    "version": __version__,
                    "startedAt": datetime.now(timezone.utc).isoformat(),
                    "pythonVersion": platform.python_version(),
                    # where the audit trail goes (SPEC 14.1); null if it could
                    # not be opened — see _mark_audit_sink_failure
                    "auditFile": self._session_file(),
                },
            )
        )
        self.tree.add(Node(id="plugins", type="plugins", name="Plugins", status="idle", owner="core", parentId="root"))
        self.tree.add(Node(id="jobs", type="jobs", name="Jobs", status="idle", owner="core", parentId="root"))

    # -- lifecycle ---------------------------------------------------------

    async def start(self) -> None:
        if self._started:
            return
        self._started = True
        self._started_monotonic = time.monotonic()
        await self.logs.start()
        self.logs.emit("info", "system", f"core starting: version={__version__}", node_id="system")
        await self.hub.start()
        self._load_plugins_from_disk()
        for host in self.hosts.values():
            await host.start()

    async def stop(self) -> None:
        if not self._started:
            return
        self._started = False
        await self.jobs.shutdown()
        for host in self.hosts.values():
            await host.stop()
        await self.hub.stop()
        uptime = time.monotonic() - (self._started_monotonic or time.monotonic())
        sink_error = self.logs.sink_error
        file_note = f"session file {self.logs.session_path}"
        if sink_error is not None:
            file_note += f" DISABLED: {sink_error}"
        self.logs.emit(
            "info",
            "system",
            f"core stopped after {uptime:.1f}s uptime ({self.logs.event_count} events logged so far,"
            f" {file_note})",
            node_id="system",
            data={
                "uptimeSeconds": round(uptime, 3),
                "events": self.logs.event_count,
                "sessionFile": self._session_file(),
                "sessionFileError": sink_error,
            },
        )
        await self.logs.stop()

    # -- plugin loading ----------------------------------------------------

    def _load_plugins_from_disk(self) -> None:
        if not self.plugins_dir.is_dir():
            logger.info("no plugins directory at %s", self.plugins_dir)
            return
        for manifest_path in sorted(self.plugins_dir.glob("*/manifest.json")):
            plugin_dir = manifest_path.parent
            try:
                manifest = load_manifest(manifest_path)
            except Exception as exc:
                logger.exception("invalid plugin manifest: %s", manifest_path)
                self._add_broken_plugin_node(plugin_dir.name, str(exc))
                continue
            node_id = f"plugin:{manifest.id}"
            existing = self.hosts.get(node_id)
            if existing is not None:
                # Two plugin directories sharing one manifest id must not
                # silently overwrite each other (SPEC 1.5): the first
                # (sorted) directory wins, the duplicate gets an error node.
                logger.warning(
                    "duplicate plugin id %r in %s (already provided by %s); ignoring this directory",
                    manifest.id,
                    plugin_dir,
                    existing.plugin_dir,
                )
                self._add_broken_plugin_node(
                    plugin_dir.name,
                    f"already provided by {existing.plugin_dir}",
                    message=f"duplicate plugin id {manifest.id!r}",
                )
                continue
            host = PluginHost(
                tree=self.tree,
                hub=self.hub,
                manifest=manifest,
                plugin_dir=plugin_dir,
                env=self._plugin_env,
                loghub=self.logs,
            )
            self.hosts[host.node_id] = host
            self.bus.register(host.owner, host.handle_command)
            logger.info("configured plugin %s from %s", manifest.id, plugin_dir)

    def _add_broken_plugin_node(
        self, dir_name: str, detail: str, *, message: str = "invalid plugin manifest"
    ) -> None:
        # Never fail (or vanish) silently: if the natural id is taken (e.g.
        # the directory name equals another plugin's id), pick a distinct one.
        node_id = f"plugin:{dir_name}"
        suffix = 2
        while self.tree.get(node_id) is not None:
            node_id = f"plugin:{dir_name}#{suffix}"
            suffix += 1
        self.tree.add(
            Node(
                id=node_id,
                type="plugin",
                name=dir_name,
                status="error",
                owner="core",
                parentId="plugins",
                error=ErrorInfo(message=message, detail=detail),
            )
        )
        self.logs.emit(
            "error",
            "plugin",
            f"plugin directory {dir_name!r} unusable: {message} ({detail})",
            node_id=node_id,
            data={"directory": dir_name, "message": message, "detail": detail},
        )

    # -- core command handling ---------------------------------------------

    async def _handle_core_command(self, node: Node, action: str, params: dict[str, Any]) -> CmdResult:
        if node.type == "job" or node.type.startswith("job."):
            return await self.jobs.handle_command(node, action, params)
        if node.type == "plugin":
            host = self.hosts.get(node.id)
            if host is not None and action == "restart":
                await host.restart()
                return CmdResult(ok=True, data={})
        return CmdResult(
            ok=False,
            error=ErrorInfo(message=f"unknown action {action!r} for target {node.id!r}"),
        )
