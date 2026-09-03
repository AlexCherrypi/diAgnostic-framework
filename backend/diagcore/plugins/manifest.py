"""Plugin manifest model (SPEC 6).

M1.5: ``capabilities`` (``"vehicle"`` ⇒ the plugin implements the SPEC 15.4
RPCs) and the optional ``vehicle`` block whose ``detectParams`` (SPEC 3.1.1
params) are appended to the session node's ``detect`` action.
"""

from __future__ import annotations

import json
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field, model_validator

from ..model import ActionParam, check_param_conditions

#: The capability a plugin declares to take part in vehicle sessions.
VEHICLE_CAPABILITY = "vehicle"

#: The detect form's plugin select is added by the core (SPEC 15.2): a
#: manifest may neither declare it nor reference it in a ``when``.
_RESERVED_DETECT_PARAMS = {"plugin": False}


class VehicleManifest(BaseModel):
    """The optional ``vehicle`` manifest block (SPEC 6 / 15.4)."""

    model_config = ConfigDict(extra="forbid")

    detectParams: list[ActionParam] = Field(default_factory=list)

    @model_validator(mode="after")
    def _check_conditions(self) -> VehicleManifest:
        # A `when` may only reference the plugin's own detect selects; the
        # core tags every param with the plugin it belongs to itself.
        check_param_conditions(self.detectParams, reserved=_RESERVED_DETECT_PARAMS)
        return self


class Manifest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(pattern=r"^[a-z0-9_][a-z0-9_\-]*$")
    name: str
    version: str
    entry: str
    python: str = "default"
    frontend: str | None = None  # reserved for M1: plugin-shipped component bundles
    capabilities: list[str] = Field(default_factory=list)
    vehicle: VehicleManifest | None = None

    @model_validator(mode="after")
    def _check_vehicle_block(self) -> Manifest:
        for capability in self.capabilities:
            if not capability or not capability.replace("_", "").isalnum() or not capability.islower():
                raise ValueError(f"invalid capability name {capability!r} (lowercase identifier expected)")
        if self.vehicle is not None and VEHICLE_CAPABILITY not in self.capabilities:
            # An author who writes a `vehicle` block means the capability; a
            # silently ignored block would be the worst outcome (SPEC 1.5).
            raise ValueError("manifest has a 'vehicle' block but 'vehicle' is not in 'capabilities'")
        if "plugin" in {param.name for param in self.detect_params}:
            raise ValueError("vehicle.detectParams must not declare a param named 'plugin' (reserved by the core)")
        return self

    @property
    def is_vehicle_plugin(self) -> bool:
        return VEHICLE_CAPABILITY in self.capabilities

    @property
    def detect_params(self) -> list[ActionParam]:
        return list(self.vehicle.detectParams) if self.vehicle is not None else []


def load_manifest(path: Path | str) -> Manifest:
    """Load and validate a ``manifest.json``."""
    manifest_path = Path(path)
    raw = manifest_path.read_text(encoding="utf-8")
    return Manifest.model_validate(json.loads(raw))
