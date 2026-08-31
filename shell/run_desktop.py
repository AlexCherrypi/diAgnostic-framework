"""Optional desktop shell: uvicorn in a thread + a native pywebview window.

Requires the backend venv (diagcore + deps) and ``pywebview``::

    C:\\diAgnostic-framework\\backend\\.venv\\Scripts\\python.exe -m pip install pywebview
    C:\\diAgnostic-framework\\backend\\.venv\\Scripts\\python.exe C:\\diAgnostic-framework\\shell\\run_desktop.py
"""

from __future__ import annotations

import sys
import threading
import time

HOST = "127.0.0.1"
PORT = 8765

try:
    import webview  # pywebview
except ImportError:
    print(
        "pywebview is not installed. The desktop shell needs it:\n"
        "    pip install pywebview\n"
        "(run this inside the backend venv, e.g. "
        "C:\\diAgnostic-framework\\backend\\.venv\\Scripts\\python.exe -m pip install pywebview)\n"
        "Alternatively run 'python -m diagcore' and open http://127.0.0.1:8765 in a browser.",
        file=sys.stderr,
    )
    sys.exit(1)

try:
    import uvicorn

    from diagcore.app import AppCore
    from diagcore.server import create_app
except ImportError as exc:
    print(
        f"diagcore (or a dependency) is not importable: {exc}\n"
        "Run this script with the backend venv python:\n"
        "    C:\\diAgnostic-framework\\backend\\.venv\\Scripts\\python.exe shell\\run_desktop.py",
        file=sys.stderr,
    )
    sys.exit(1)


def main() -> None:
    core = AppCore()
    app = create_app(core)
    config = uvicorn.Config(app, host=HOST, port=PORT, log_level="info")
    server = uvicorn.Server(config)

    thread = threading.Thread(target=server.run, name="uvicorn", daemon=True)
    thread.start()

    deadline = time.monotonic() + 15.0
    while not server.started:
        if not thread.is_alive():
            print("uvicorn failed to start (see log above)", file=sys.stderr)
            sys.exit(1)
        if time.monotonic() > deadline:
            print("uvicorn did not start within 15 s", file=sys.stderr)
            sys.exit(1)
        time.sleep(0.05)

    webview.create_window("diAgnostic", f"http://{HOST}:{PORT}", width=1280, height=800)
    try:
        webview.start()
    finally:
        server.should_exit = True
        thread.join(timeout=10.0)


if __name__ == "__main__":
    main()
