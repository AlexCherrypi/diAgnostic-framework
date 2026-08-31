"""Scripted fake plugin used by the plugin-host tests.

Deliberately does NOT use diagcore.plugins.api: it is a minimal, synchronous,
hand-rolled implementation of the stdio JSON-RPC protocol so the host is
tested against the wire contract, not against our own helper library.

Behaviours (driven by environment variables):
- FAKE_EXIT_IMMEDIATELY=1     -> exit(3) before speaking any protocol
- FAKE_EXIT_IF_FLAG=<path>    -> exit(3) at startup while <path> exists
- command "echo"              -> respond ok with the params as data
- command "die"               -> os._exit(1) (hard crash)
- command "freeze"            -> stop responding to pings (heartbeat timeout)
- command "illegal_add"       -> try to add a node outside our subtree
- command "boom"              -> respond ok:false with an error object
- command "bad_params"        -> emit a notification with positional (array)
                                 params before responding ok (JSON-RPC allows
                                 it; the host must drop it, not die)
"""

from __future__ import annotations

import json
import os
import sys

if os.environ.get("FAKE_EXIT_IMMEDIATELY") == "1":
    sys.exit(3)

flag_path = os.environ.get("FAKE_EXIT_IF_FLAG")
if flag_path and os.path.exists(flag_path):
    sys.exit(3)


def send(obj: dict) -> None:
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()


def respond(msg_id, result) -> None:
    send({"jsonrpc": "2.0", "id": msg_id, "result": result})


def notify(method: str, params: dict) -> None:
    send({"jsonrpc": "2.0", "method": method, "params": params})


node_id = None
frozen = False

for line in sys.stdin:
    line = line.strip()
    if not line:
        continue
    msg = json.loads(line)
    method = msg.get("method")
    msg_id = msg.get("id")
    params = msg.get("params") or {}

    if method == "initialize":
        node_id = params["pluginNodeId"]
        respond(msg_id, {"ok": True})
        notify(
            "add",
            {
                "node": {
                    "id": f"{node_id}:child",
                    "type": "device.sim",
                    "name": "Fake child",
                    "status": "idle",
                    "parentId": node_id,
                    # deliberately claims a bogus owner; the host must stamp it
                    "owner": "core",
                }
            },
        )
        notify("log", {"level": "info", "message": "fake plugin initialized"})
    elif method == "ping":
        if not frozen:
            respond(msg_id, {})
    elif method == "command":
        action = params.get("action")
        if action == "echo":
            respond(msg_id, {"ok": True, "data": {"echo": params.get("params") or {}}})
        elif action == "die":
            os._exit(1)
        elif action == "freeze":
            frozen = True
            respond(msg_id, {"ok": True, "data": {}})
        elif action == "illegal_add":
            notify(
                "add",
                {
                    "node": {
                        "id": "evil",
                        "type": "device.sim",
                        "name": "Evil",
                        "status": "idle",
                        "parentId": "root",
                    }
                },
            )
            respond(msg_id, {"ok": True, "data": {}})
        elif action == "bad_params":
            send({"jsonrpc": "2.0", "method": "log", "params": ["positional", "params"]})
            respond(msg_id, {"ok": True, "data": {}})
        elif action == "boom":
            respond(msg_id, {"ok": False, "error": {"message": "it went boom"}})
        else:
            respond(msg_id, {"ok": False, "error": {"message": f"unknown action {action!r}"}})
    elif method == "shutdown":
        respond(msg_id, {})
        break
    else:
        send({"jsonrpc": "2.0", "id": msg_id, "error": {"code": -32601, "message": "method not found"}})
