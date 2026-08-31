"""Pydantic models for the state tree and the WebSocket wire protocol.

This module is the Python mirror of SPEC.md sections 3-5: node model, patch
ops and the message envelopes exchanged on /ws/state and /ws/telemetry.
"""

from __future__ import annotations

from typing import Any, Literal, Union

from pydantic import BaseModel, ConfigDict, Field

NodeStatus = Literal["init", "idle", "running", "done", "error", "cancelled", "gone"]

#: Statuses in which a node is considered finished (no further transitions
#: expected without an explicit reset).
TERMINAL_STATUSES: frozenset[str] = frozenset({"done", "error", "cancelled"})


class ErrorInfo(BaseModel):
    """Structured error info attached to a node or a command result."""

    model_config = ConfigDict(extra="forbid")

    message: str
    detail: str | None = None


class Action(BaseModel):
    """A command the UI may offer generically on a node."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    label: str
    params: list[Any] = Field(default_factory=list)


class Node(BaseModel):
    """One node of the state tree (SPEC 3.1).

    Field names deliberately match the JSON wire format (camelCase for
    ``parentId``) so a plain ``model_dump()`` is the wire representation.
    """

    model_config = ConfigDict(extra="forbid")

    id: str = Field(min_length=1)
    type: str = Field(pattern=r"^[a-z0-9_]+(\.[a-z0-9_]+)*$")
    name: str
    status: NodeStatus = "init"
    error: ErrorInfo | None = None
    progress: float | None = Field(default=None, ge=0.0, le=1.0)
    props: dict[str, Any] = Field(default_factory=dict)
    actions: list[Action] = Field(default_factory=list)
    parentId: str | None = None
    children: list[str] = Field(default_factory=list)
    owner: str = "core"


class CmdResult(BaseModel):
    """Result of dispatching a command to a node owner."""

    model_config = ConfigDict(extra="forbid")

    ok: bool
    data: dict[str, Any] | None = None
    error: ErrorInfo | None = None


# --------------------------------------------------------------------------
# Patch ops (SPEC 3.3)
# --------------------------------------------------------------------------


class AddOp(BaseModel):
    model_config = ConfigDict(extra="forbid")

    op: Literal["add"] = "add"
    node: Node
    index: int | None = None


class UpdateOp(BaseModel):
    model_config = ConfigDict(extra="forbid")

    op: Literal["update"] = "update"
    id: str
    changes: dict[str, Any]


class MergePropsOp(BaseModel):
    model_config = ConfigDict(extra="forbid")

    op: Literal["merge_props"] = "merge_props"
    id: str
    props: dict[str, Any]


class RemoveOp(BaseModel):
    model_config = ConfigDict(extra="forbid")

    op: Literal["remove"] = "remove"
    id: str


PatchOp = Union[AddOp, UpdateOp, MergePropsOp, RemoveOp]


# --------------------------------------------------------------------------
# /ws/state messages (SPEC 4)
# --------------------------------------------------------------------------


class Snapshot(BaseModel):
    model_config = ConfigDict(extra="forbid")

    rootId: str | None
    nodes: dict[str, Node]


class HelloMessage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    t: Literal["hello"] = "hello"
    seq: int
    snapshot: Snapshot


class PatchMessage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    t: Literal["patch"] = "patch"
    seq: int
    ops: list[dict[str, Any]]


class CmdResultMessage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    t: Literal["cmd_result"] = "cmd_result"
    cmdId: str | None
    ok: bool
    data: dict[str, Any] | None = None
    error: ErrorInfo | None = None


class CmdMessage(BaseModel):
    """Client -> server command intent."""

    model_config = ConfigDict(extra="ignore")

    t: Literal["cmd"] = "cmd"
    cmdId: str | None = None
    target: str | None = None
    action: str | None = None
    params: dict[str, Any] = Field(default_factory=dict)


class ResyncMessage(BaseModel):
    model_config = ConfigDict(extra="ignore")

    t: Literal["resync"] = "resync"


# --------------------------------------------------------------------------
# /ws/telemetry messages (SPEC 5)
# --------------------------------------------------------------------------


class SubMessage(BaseModel):
    model_config = ConfigDict(extra="ignore")

    t: Literal["sub"] = "sub"
    channel: str | None = None


class UnsubMessage(BaseModel):
    model_config = ConfigDict(extra="ignore")

    t: Literal["unsub"] = "unsub"
    channel: str | None = None


class HistoryMessage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    t: Literal["history"] = "history"
    channel: str
    samples: list[list[float]]


class SamplesMessage(BaseModel):
    model_config = ConfigDict(extra="forbid")

    t: Literal["samples"] = "samples"
    channel: str
    samples: list[list[float]]
