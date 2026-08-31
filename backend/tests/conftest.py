from __future__ import annotations

import asyncio
import sys

# asyncio subprocesses need the Proactor event loop on Windows. It is the
# default on Python 3.13, but pin it explicitly so pytest-asyncio contexts
# can never end up on a selector loop.
if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
