from __future__ import annotations

import asyncio
import sys
from pathlib import Path

import pytest

# asyncio subprocesses need the Proactor event loop on Windows. It is the
# default on Python 3.13, but pin it explicitly so pytest-asyncio contexts
# can never end up on a selector loop.
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())


@pytest.fixture(autouse=True)
def isolated_data_dir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Every test gets its own vehicle data dir (SPEC 15.1 ``DIAGCORE_DATA_DIR``)
    so no test can ever write profiles into the repo's ``data/`` folder."""
    data_dir = tmp_path / "data"
    monkeypatch.setenv("DIAGCORE_DATA_DIR", str(data_dir))
    return data_dir
