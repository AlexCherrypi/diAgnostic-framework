"""Command routing by node owner (SPEC 4).

Commands target a node id; the bus looks up the node and forwards the command
to the handler registered for the node's ``owner`` ("core" or
"plugin:<pluginId>"). Unknown target / missing handler / handler failure all
yield ``ok: false`` — the bus never raises into the caller.

M0.5 (SPEC 14.1): the bus is the command seam — every received command (target
id, action, full raw params) and its result is logged automatically, category
``cmd``, so plugin authors get command auditing for free. When the caller
passes them, ``cmd_id`` (the client's ``cmdId``) and ``client`` (the issuing
connection) are stamped into both events so concurrent commands on the same
target/action pair up unambiguously in the audit trail.
"""

from __future__ import annotations

import logging
import traceback
from typing import Any, Awaitable, Callable

from .loghub import LogHub, compact_json
from .model import CmdResult, ErrorInfo, Node
from .tree import StateTree

logger = logging.getLogger(__name__)

#: A command handler: (node, action, params) -> CmdResult.
CommandHandler = Callable[[Node, str, dict[str, Any]], Awaitable[CmdResult]]


def _fail(message: str, detail: str | None = None) -> CmdResult:
    return CmdResult(ok=False, error=ErrorInfo(message=message, detail=detail))


class CommandBus:
    def __init__(self, tree: StateTree, *, loghub: LogHub | None = None) -> None:
        self._tree = tree
        self._handlers: dict[str, CommandHandler] = {}
        self._loghub = loghub

    def register(self, owner: str, handler: CommandHandler) -> None:
        self._handlers[owner] = handler

    def unregister(self, owner: str) -> None:
        self._handlers.pop(owner, None)

    async def dispatch(
        self,
        target: str | None,
        action: str | None,
        params: Any = None,
        *,
        cmd_id: str | None = None,
        client: str | None = None,
    ) -> CmdResult:
        """Route one command; never raises (SPEC 4: unknown/invalid => ok:false).

        ``params`` is whatever the client sent: ``None`` means ``{}``, anything
        but a JSON object is rejected as ``ok:false`` — after the received
        command has been audited with the raw value (SPEC 14.1: every received
        command, full raw params).
        """
        correlation = {key: value for key, value in (("cmdId", cmd_id), ("client", client)) if value is not None}
        tag = ""
        if cmd_id is not None or client is not None:
            tag = " [" + " from ".join(part for part in (cmd_id, client) if part is not None) + "]"
        if params is None:
            params = {}
        if self._loghub is not None:
            self._loghub.emit(
                "info",
                "cmd",
                f"cmd {action!r} -> {target!r}{tag} params={compact_json(params)}",
                node_id=target if isinstance(target, str) else None,
                data={"target": target, "action": action, "params": params, **correlation},
            )
        if isinstance(params, dict):
            result = await self._dispatch(target, action, dict(params))
        else:
            result = _fail(f"invalid params: expected a JSON object, got {type(params).__name__}")
        if self._loghub is not None:
            if result.ok:
                message = f"cmd {action!r} on {target!r}{tag} ok data={compact_json(result.data or {})}"
            else:
                error_message = result.error.message if result.error else "command failed"
                message = f"cmd {action!r} on {target!r}{tag} failed: {error_message}"
            self._loghub.emit(
                "info",
                "cmd",
                message,
                node_id=target if isinstance(target, str) else None,
                data={
                    "target": target,
                    "action": action,
                    "result": result.model_dump(mode="json"),
                    **correlation,
                },
            )
        return result

    async def _dispatch(
        self,
        target: str | None,
        action: str | None,
        params: dict[str, Any],
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
            return await handler(node, action, dict(params))
        except Exception as exc:
            logger.exception("command handler failed: target=%s action=%s", target, action)
            return _fail(f"command failed: {exc}", detail=traceback.format_exc())
