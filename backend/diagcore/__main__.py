"""``python -m diagcore`` — run the core on 127.0.0.1:8765."""

from __future__ import annotations

import logging

import uvicorn

from .app import AppCore
from .server import create_app


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )
    core = AppCore()
    app = create_app(core)
    uvicorn.run(app, host="127.0.0.1", port=8765, log_level="info")


if __name__ == "__main__":
    main()
