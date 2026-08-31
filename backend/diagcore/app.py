"""Composition root: well-known nodes, job engine, telemetry hub, plugins.

``AppCore`` wires the pieces together (SPEC 3.2, 7): it builds the state tree
with ``root`` / ``system`` / ``plugins`` / ``jobs``, owns the command bus and
the job engine, and loads every plugin found under ``backend/plugins/*``.
"""

from __future__ import annotations

import logging
import platform
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from . import __version__
from .bus import CommandBus
from .jobs import JobEngine
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
    ) -> None:
        self.tree = StateTree()
        self.hub = TelemetryHub()
        self.jobs = JobEngine(self.tree)
        self.bus = CommandBus(self.tree)
        self.bus.register("core", self._handle_core_command)
        self.hosts: dict[str, PluginHost] = {}
        self.plugins_dir = Path(plugins_dir) if plugins_dir is not None else DEFAULT_PLUGINS_DIR
        self._plugin_env = plugin_env
        self._started = False
        self._build_well_known_nodes()

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
