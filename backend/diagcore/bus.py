"""Command routing by node owner (SPEC 4).

Commands target a node id; the bus looks up the node and forwards the command
to the handler registered for the node's ``owner`` ("core" or
"plugin:<pluginId>"). Unknown target / missing handler / handler failure all
yield ``ok: false`` — the bus never raises into the caller.
"""

from __future__ import annotations

import logging
import traceback
from typing import Any, Awaitable, Callable

from .model import CmdResult, ErrorInfo, Node
from .tree import StateTree

logger = logging.getLogger(__name__)

#: A command handler: (node, action, params) -> CmdResult.
CommandHandler = Callable[[Node, str, dict[str, Any]], Awaitable[CmdResult]]


def _fail(message: str, detail: str | None = None) -> CmdResult:
    return CmdResult(ok=False, error=ErrorInfo(message=message, detail=detail))


class CommandBus:
    def __init__(self, tree: StateTree) -> None:
        self._tree = tree
        self._handlers: dict[str, CommandHandler] = {}

    def register(self, owner: str, handler: CommandHandler) -> None:
        self._handlers[owner] = handler

    def unregister(self, owner: str) -> None:
        self._handlers.pop(owner, None)

    async def dispatch(
        self,
        target: str | None,
        action: str | None,
        params: dict[str, Any] | None = None,
    ) -> CmdResult:
        if not target:
            return _fail("missing command target")
        node = self._tree.get(target)
        if node is None:
            return _fail(f"unknown target: {target!r}")
        if not action:
            return _fail("missing command action")

        handler = self._handlers.get(node.owner)
        if handler is None:
            return _fail(f"no command handler registered for owner {node.owner!r}")

        try:
            return await handler(node, action, dict(params or {}))
        except Exception as exc:
            logger.exception("command handler failed: target=%s action=%s", target, action)
            return _fail(f"command failed: {exc}", detail=traceback.format_exc())
