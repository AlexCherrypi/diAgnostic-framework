"""Pydantic models for the state tree and the WebSocket wire protocol.

This module is the Python mirror of SPEC.md sections 3-5: node model, patch
ops and the message envelopes exchanged on /ws/state and /ws/telemetry.
"""

from __future__ import annotations

from typing import Any, Literal, Mapping, Union

from pydantic import BaseModel, ConfigDict, Field, model_validator

NodeStatus = Literal["init", "idle", "running", "done", "error", "cancelled", "gone"]

#: Statuses in which a node is considered finished (no further transitions
#: expected without an explicit reset).
TERMINAL_STATUSES: frozenset[str] = frozenset({"done", "error", "cancelled"})

#: Action parameter input types (SPEC 3.1.1).
ParamType = Literal["string", "number", "boolean", "select"]

#: A JSON scalar: what a parameter value / option value / default may be.
ParamScalar = Union[str, int, float, bool]


class ErrorInfo(BaseModel):
    """Structured error info attached to a node or a command result."""

    model_config = ConfigDict(extra="forbid")

    message: str
    detail: str | None = None


class ActionParamOption(BaseModel):
    """One choice of a ``select`` parameter. The UI sends ``value``, never
    ``label`` (SPEC 3.1.1: the audit trail records the raw id behind every
    dropdown choice)."""

    model_config = ConfigDict(extra="forbid")

    value: ParamScalar
    label: str


class ActionParam(BaseModel):
    """One input the UI collects before sending a command (SPEC 3.1.1).

    Field names match the wire. ``min``/``max`` are number-only, ``options``
    select-only; a malformed declaration is a validation error — for a
    plugin-declared node that means the notification is dropped and logged
    like every other bad notification (SPEC 6), never a crash.

    ``when`` (optional) makes the param conditional: ``{"<selectName>":
    <optionValue>}`` — it is collected, validated and sent only while every
    named ``select`` of the same action holds that value (SPEC 3.1.1). This
    is how the core folds several plugins' ``detectParams`` into the one
    static ``detect`` form (SPEC 15.2): each plugin's params are tagged
    ``when: {plugin: <id>}``, so same-named params of different plugins
    never clash and a required param of a plugin that is not selected
    never blocks the form.
    """

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    label: str = ""  # defaults to ``name``
    type: ParamType = "string"
    required: bool = False
    default: ParamScalar | None = None
    placeholder: str | None = None
    help: str | None = None
    min: float | None = None
    max: float | None = None
    options: list[ActionParamOption] = Field(default_factory=list)
    when: dict[str, ParamScalar] | None = None

    @model_validator(mode="after")
    def _check_shape(self) -> ActionParam:
        if not self.label:
            self.label = self.name
        if self.when is not None:
            if not self.when:
                raise ValueError(f"param {self.name!r}: 'when' must name at least one select param")
            if self.name in self.when:
                raise ValueError(f"param {self.name!r}: 'when' cannot depend on the param itself")
            for key in self.when:
                if not key:
                    raise ValueError(f"param {self.name!r}: 'when' keys must be param names")
        if self.type == "select":
            if not self.options:
                raise ValueError(f"select param {self.name!r} needs at least one option")
            values = [option.value for option in self.options]
            if self.default is not None and self.default not in values:
                raise ValueError(f"select param {self.name!r}: default {self.default!r} is not an option value")
        elif self.options:
            raise ValueError(f"param {self.name!r}: options are only valid for type 'select'")
        if self.type != "number" and (self.min is not None or self.max is not None):
            raise ValueError(f"param {self.name!r}: min/max are only valid for type 'number'")
        if self.min is not None and self.max is not None and self.min > self.max:
            raise ValueError(f"param {self.name!r}: min {self.min} > max {self.max}")
        if self.default is not None:
            problem = _check_value(self, self.default)
            if problem is not None:
                raise ValueError(f"param {self.name!r}: default {problem}")
        return self


def _check_value(param: ActionParam, value: Any) -> str | None:
    """Why ``value`` does not fit ``param`` (None when it does)."""
    if param.type == "string":
        if not isinstance(value, str):
            return f"must be a string, got {type(value).__name__}"
    elif param.type == "number":
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            return f"must be a number, got {type(value).__name__}"
        if param.min is not None and value < param.min:
            return f"must be >= {param.min}, got {value}"
        if param.max is not None and value > param.max:
            return f"must be <= {param.max}, got {value}"
    elif param.type == "boolean":
        if not isinstance(value, bool):
            return f"must be a boolean, got {type(value).__name__}"
    elif param.type == "select":
        values = [option.value for option in param.options]
        if value not in values:
            return f"must be one of {values!r}, got {value!r}"
    return None


def param_applies(param: ActionParam, values: Mapping[str, Any]) -> bool:
    """Whether a (possibly conditional) param is in effect for ``values``:
    unconditional params always are; a ``when`` param only while every
    named select currently holds the given option value (SPEC 3.1.1)."""
    if param.when is None:
        return True
    return all(values.get(key) == expected for key, expected in param.when.items())


def resolve_param_ref(params: list[ActionParam], param: ActionParam, key: str) -> ActionParam | None:
    """The param named ``key`` that ``param.when`` refers to. With several
    same-named params (the detect form's union of plugin params, each tagged
    with its plugin) the one whose own ``when`` is compatible with — a subset
    of — the referencing param's condition wins, so ``{"depth": "deep",
    "plugin": "b"}`` resolves to plugin b's ``depth``, never plugin a's."""
    condition = param.when or {}
    for candidate in params:
        if candidate.name != key or candidate is param:
            continue
        if candidate.when is None or all(condition.get(k) == v for k, v in candidate.when.items()):
            return candidate
    return None


def check_param_conditions(params: list[ActionParam], *, reserved: Mapping[str, bool] | None = None) -> None:
    """Validate the ``when`` references of a params list: every key must
    name a ``select`` param of the same list (see :func:`resolve_param_ref`)
    or one of ``reserved`` (``True`` = allowed without a declaration, e.g.
    the core-added ``plugin`` select of the detect form; ``False`` = must not
    be referenced at all), and every value must be one of that select's
    option values. Raises ``ValueError``."""
    for param in params:
        if param.when is None:
            continue
        for key, expected in param.when.items():
            if reserved is not None and key in reserved:
                if not reserved[key]:
                    raise ValueError(f"param {param.name!r}: 'when' must not reference the reserved param {key!r}")
                continue
            target = resolve_param_ref(params, param, key)
            if target is None:
                raise ValueError(f"param {param.name!r}: 'when' references unknown param {key!r}")
            if target.type != "select":
                raise ValueError(f"param {param.name!r}: 'when' references {key!r}, which is not a select")
            if expected not in [option.value for option in target.options]:
                raise ValueError(f"param {param.name!r}: 'when' value {expected!r} is not an option of {key!r}")


def apply_param_defaults(params: list[ActionParam], values: Mapping[str, Any]) -> dict[str, Any]:
    """``values`` with the declared defaults filled in for every missing or
    empty param that is in effect (a select default may bring further
    conditional params into effect, so this iterates to a fixed point)."""
    filled = dict(values)
    changed = True
    while changed:
        changed = False
        for param in params:
            if param.default is None or filled.get(param.name) not in (None, ""):
                continue
            if param_applies(param, filled):
                filled[param.name] = param.default
                changed = True
    return filled


def validate_action_params(params: list[ActionParam], values: dict[str, Any]) -> list[str]:
    """Check command ``values`` against a declared ``params`` list.

    Returns a list of human-readable problems (empty when the values fit):
    a required parameter that is missing/null/empty, or a value of the wrong
    type, out of range or not an option value. Keys not declared in
    ``params`` are ignored (they still land in the audit trail as sent);
    params whose ``when`` condition does not hold for ``values`` are skipped
    (the UI never collects them either).
    """
    problems: list[str] = []
    for param in params:
        if not param_applies(param, values):
            continue
        value = values.get(param.name)
        if value is None or value == "":
            if param.required and param.default is None:
                problems.append(f"missing required param {param.name!r}")
            continue
        problem = _check_value(param, value)
        if problem is not None:
            problems.append(f"param {param.name!r} {problem}")
    return problems


class Action(BaseModel):
    """A command the UI may offer generically on a node.

    A non-empty ``params`` list opens the generic action form; ``confirm``
    (a question) makes the UI ask before sending (SPEC 3.1.1). A param's
    ``when`` must reference a ``select`` of the same action.
    """

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    label: str
    confirm: str | None = None
    params: list[ActionParam] = Field(default_factory=list)

    @model_validator(mode="after")
    def _check_conditions(self) -> Action:
        check_param_conditions(self.params)
        return self


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
