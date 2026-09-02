"""``python -m diagcore`` — run the core on 127.0.0.1 (default port 8765).

The port can be overridden with ``--port`` or the ``DIAGCORE_PORT``
environment variable (the CLI flag wins); without either, the SPEC 2 default
of 8765 applies unchanged.
"""

from __future__ import annotations

import argparse
import logging
import os
from typing import Mapping, Sequence

import uvicorn

from .app import AppCore
from .server import create_app

logger = logging.getLogger(__name__)

DEFAULT_PORT = 8765


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(prog="diagcore", description="diAgnostic core backend")
    parser.add_argument(
        "--port",
        type=int,
        default=None,
        help=f"TCP port to listen on (overrides DIAGCORE_PORT; default {DEFAULT_PORT})",
    )
    return parser.parse_args(argv)


def resolve_port(cli_port: int | None, env: Mapping[str, str] = os.environ) -> int:
    """``--port`` beats ``DIAGCORE_PORT`` beats the default 8765."""
    if cli_port is not None:
        return cli_port
    raw = env.get("DIAGCORE_PORT")
    if raw:
        try:
            return int(raw)
        except ValueError:
            logger.warning("ignoring non-numeric DIAGCORE_PORT=%r; using %d", raw, DEFAULT_PORT)
    return DEFAULT_PORT


def main(argv: Sequence[str] | None = None) -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s: %(message)s",
    )
    args = parse_args(argv)
    port = resolve_port(args.port)
    core = AppCore()
    app = create_app(core)
    uvicorn.run(app, host="127.0.0.1", port=port, log_level="info")


if __name__ == "__main__":
    main()
