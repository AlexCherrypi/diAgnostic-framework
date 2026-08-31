"""Plugin manifest model (SPEC 6)."""

from __future__ import annotations

import json
from pathlib import Path

from pydantic import BaseModel, ConfigDict, Field


class Manifest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    id: str = Field(pattern=r"^[a-z0-9_][a-z0-9_\-]*$")
    name: str
    version: str
    entry: str
    python: str = "default"
    frontend: str | None = None  # reserved for M1: plugin-shipped component bundles


def load_manifest(path: Path | str) -> Manifest:
    """Load and validate a ``manifest.json``."""
    manifest_path = Path(path)
    raw = manifest_path.read_text(encoding="utf-8")
    return Manifest.model_validate(json.loads(raw))
