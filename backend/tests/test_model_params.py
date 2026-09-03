"""Action params schema (SPEC 3.1.1) and manifest capabilities (SPEC 6, M1.5)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from diagcore.model import (
    Action,
    ActionParam,
    ActionParamOption,
    Node,
    apply_param_defaults,
    param_applies,
    resolve_param_ref,
    validate_action_params,
)
from diagcore.plugins.manifest import Manifest, load_manifest


def param(**kw) -> ActionParam:
    return ActionParam(**{"name": "p", **kw})


# -- ActionParam -------------------------------------------------------------


def test_param_defaults_and_label_fallback() -> None:
    p = ActionParam(name="address")
    assert p.model_dump() == {
        "name": "address",
        "label": "address",  # label defaults to the name
        "type": "string",
        "required": False,
        "default": None,
        "placeholder": None,
        "help": None,
        "min": None,
        "max": None,
        "options": [],
        "when": None,
    }


def test_param_wire_shape_matches_spec_example() -> None:
    raw = {
        "name": "address",
        "label": "ECU address",
        "type": "select",
        "required": True,
        "default": "uds",
        "placeholder": "0x10",
        "help": "which stack",
        "options": [{"value": "uds", "label": "UDS on CAN"}],
    }
    p = ActionParam.model_validate(raw)
    assert p.model_dump(mode="json") == {**raw, "min": None, "max": None, "when": None}
    with pytest.raises(ValidationError):
        ActionParam.model_validate({**raw, "extra": 1})  # extra keys forbidden
    with pytest.raises(ValidationError):
        ActionParam.model_validate({**raw, "type": "date"})


def test_select_needs_options_and_default_must_be_an_option() -> None:
    with pytest.raises(ValidationError, match="at least one option"):
        param(type="select")
    with pytest.raises(ValidationError, match="not an option value"):
        param(type="select", default="x", options=[{"value": "a", "label": "A"}])
    ok = param(type="select", default="a", options=[{"value": "a", "label": "A"}, {"value": 2, "label": "Two"}])
    assert [o.value for o in ok.options] == ["a", 2]
    with pytest.raises(ValidationError, match="only valid for type 'select'"):
        param(type="string", options=[{"value": "a", "label": "A"}])


def test_min_max_only_for_numbers_and_default_type_checked() -> None:
    with pytest.raises(ValidationError, match="only valid for type 'number'"):
        param(type="string", min=0)
    with pytest.raises(ValidationError, match="min 5.0 > max 1.0"):
        param(type="number", min=5, max=1)
    with pytest.raises(ValidationError, match="default must be a number"):
        param(type="number", default="7")
    with pytest.raises(ValidationError, match="default must be >= 1.0"):
        param(type="number", min=1, default=0)
    with pytest.raises(ValidationError, match="default must be a boolean"):
        param(type="boolean", default="yes")
    with pytest.raises(ValidationError, match="default must be a string"):
        param(type="string", default=3)
    assert param(type="number", min=0, max=255, default=16).default == 16


def test_action_confirm_and_params_on_a_node() -> None:
    node = Node(
        id="x",
        type="ecu.probe",
        name="Probe ECU",
        status="idle",
        owner="plugin:demo",
        parentId=None,
        actions=[
            Action(name="probe", label="Probe", confirm="Really?", params=[param(name="address", required=True)]),
            Action(name="plain", label="Plain"),
        ],
    )
    dumped = node.model_dump(mode="json")["actions"]
    assert dumped[0]["confirm"] == "Really?" and dumped[0]["params"][0]["name"] == "address"
    assert dumped[1] == {"name": "plain", "label": "Plain", "confirm": None, "params": []}
    # a malformed param inside a node's actions is a validation error (the
    # host drops + logs such a notification instead of crashing)
    with pytest.raises(ValidationError):
        Node.model_validate(
            {**node.model_dump(), "actions": [{"name": "x", "label": "X", "params": [{"name": "m", "type": "select"}]}]}
        )


def test_validate_action_params() -> None:
    schema = [
        param(name="address", required=True),
        param(name="depth", type="select", required=True, default="quick", options=[{"value": "quick", "label": "Q"}]),
        param(name="retries", type="number", min=0, max=5),
        param(name="verbose", type="boolean"),
    ]
    assert validate_action_params(schema, {"address": "0x10"}) == []  # required select satisfied by its default
    problems = validate_action_params(schema, {"address": "", "depth": "deep", "retries": 9, "verbose": "no"})
    assert problems == [
        "missing required param 'address'",
        "param 'depth' must be one of ['quick'], got 'deep'",
        "param 'retries' must be <= 5.0, got 9",
        "param 'verbose' must be a boolean, got str",
    ]
    assert validate_action_params(schema, {"address": 5}) == ["param 'address' must be a string, got int"]
    assert validate_action_params(schema, {"address": "x", "retries": True}) == [
        "param 'retries' must be a number, got bool"
    ]
    assert validate_action_params(schema, {"address": "x", "unknown": 1}) == []  # undeclared keys are ignored


# -- Manifest ---------------------------------------------------------------


def test_manifest_capabilities_and_vehicle_block(tmp_path: Path) -> None:
    raw = {
        "id": "demo_vci",
        "name": "Demo",
        "version": "0.1.0",
        "entry": "plugin.py",
        "capabilities": ["vehicle"],
        "vehicle": {"detectParams": [{"name": "depth", "type": "select", "options": [{"value": "q", "label": "Q"}]}]},
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(raw), encoding="utf-8")
    manifest = load_manifest(path)
    assert manifest.is_vehicle_plugin
    assert [p.name for p in manifest.detect_params] == ["depth"]
    assert manifest.detect_params[0].label == "depth"


def test_manifest_without_capabilities_is_not_a_vehicle_plugin() -> None:
    manifest = Manifest(id="old", name="Old", version="1", entry="plugin.py")
    assert manifest.capabilities == [] and manifest.vehicle is None
    assert not manifest.is_vehicle_plugin and manifest.detect_params == []


def test_manifest_rejects_inconsistent_vehicle_declarations() -> None:
    base = {"id": "x", "name": "X", "version": "1", "entry": "plugin.py"}
    with pytest.raises(ValidationError, match="not in 'capabilities'"):
        Manifest.model_validate({**base, "vehicle": {"detectParams": []}})
    with pytest.raises(ValidationError, match="invalid capability name"):
        Manifest.model_validate({**base, "capabilities": ["Vehicle!"]})
    with pytest.raises(ValidationError, match="reserved by the core"):
        Manifest.model_validate(
            {**base, "capabilities": ["vehicle"], "vehicle": {"detectParams": [{"name": "plugin", "label": "P"}]}}
        )
    with pytest.raises(ValidationError):  # malformed detect param
        Manifest.model_validate(
            {**base, "capabilities": ["vehicle"], "vehicle": {"detectParams": [{"name": "d", "type": "select"}]}}
        )
    with pytest.raises(ValidationError):  # unknown vehicle-block key
        Manifest.model_validate({**base, "capabilities": ["vehicle"], "vehicle": {"attachParams": []}})


def test_demo_manifest_declares_vehicle_capability() -> None:
    manifest = load_manifest(Path(__file__).parent.parent / "plugins" / "demo_vci" / "manifest.json")
    assert manifest.is_vehicle_plugin
    depth = manifest.detect_params[0]
    assert depth.name == "depth" and depth.type == "select" and depth.default == "quick"
    assert [o.value for o in depth.options] == ["quick", "full"]
    assert isinstance(depth.options[0], ActionParamOption)


# -- conditional params (`when`, SPEC 3.1.1) ---------------------------------


def select(name: str, *values: str, **kw) -> ActionParam:
    return ActionParam(name=name, type="select", options=[{"value": v, "label": v.upper()} for v in values], **kw)


def test_when_must_reference_a_select_of_the_same_action() -> None:
    mode = select("mode", "plain", "secure", required=True, default="plain")
    pin = param(name="pin", required=True, when={"mode": "secure"})
    action = Action(name="unlock", label="Unlock", params=[mode, pin])
    assert action.params[1].when == {"mode": "secure"}
    with pytest.raises(ValidationError, match="references unknown param 'nope'"):
        Action(name="x", label="X", params=[param(name="pin", when={"nope": "secure"})])
    with pytest.raises(ValidationError, match="is not a select"):
        Action(name="x", label="X", params=[param(name="mode"), param(name="pin", when={"mode": "secure"})])
    with pytest.raises(ValidationError, match="is not an option of 'mode'"):
        Action(name="x", label="X", params=[mode, param(name="pin", when={"mode": "deep"})])
    with pytest.raises(ValidationError, match="cannot depend on the param itself"):
        param(name="pin", when={"pin": "x"})
    with pytest.raises(ValidationError, match="at least one select"):
        param(name="pin", when={})
    # a manifest may not reference the core's reserved `plugin` select
    with pytest.raises(ValidationError, match="reserved param 'plugin'"):
        Manifest(
            id="p",
            name="P",
            version="1",
            entry="p.py",
            capabilities=["vehicle"],
            vehicle={"detectParams": [param(name="pin", when={"plugin": "p"}).model_dump()]},
        )


def test_when_resolves_same_named_params_by_compatible_condition() -> None:
    # the detect form's union: plugin a's `depth` select and plugin b's `depth` select
    plugin = select("plugin", "a", "b", required=True)
    depth_a = select("depth", "quick", "full", when={"plugin": "a"})
    depth_b = select("depth", "fast", "deep", when={"plugin": "b"})
    pin_b = param(name="pin", required=True, when={"depth": "deep", "plugin": "b"})
    params = [plugin, depth_a, depth_b, pin_b]
    assert resolve_param_ref(params, pin_b, "depth") is depth_b  # never a's `depth`
    Action(name="detect", label="Detect", params=params)  # 'deep' is an option of b's depth
    with pytest.raises(ValidationError, match="not an option of 'depth'"):
        Action(
            name="detect",
            label="Detect",
            params=[plugin, depth_a, depth_b, param(name="pin", when={"depth": "full", "plugin": "b"})],
        )


def test_validate_and_defaults_skip_params_whose_condition_does_not_hold() -> None:
    mode = select("mode", "plain", "secure", required=True, default="plain")
    pin = param(name="pin", required=True, when={"mode": "secure"})
    level = param(name="level", type="number", default=3, when={"mode": "secure"})
    schema = [mode, pin, level]
    assert validate_action_params(schema, {"mode": "plain"}) == []  # pin not in effect
    assert validate_action_params(schema, {"mode": "secure"}) == ["missing required param 'pin'"]
    assert validate_action_params(schema, {}) == []  # mode's default keeps pin out of effect
    assert param_applies(pin, {"mode": "secure"}) and not param_applies(pin, {"mode": "plain"}) and param_applies(mode, {})
    assert apply_param_defaults(schema, {}) == {"mode": "plain"}
    # a select default may bring conditional params into effect (fixed point)
    secure = select("mode", "plain", "secure", required=True, default="secure")
    assert apply_param_defaults([secure, pin, level], {}) == {"mode": "secure", "level": 3}
    assert apply_param_defaults([secure, pin, level], {"mode": "plain", "level": ""}) == {"mode": "plain", "level": ""}
