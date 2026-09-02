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
- command "io_good"           -> emit a valid io tx/rx pair for our child node
- command "io_evil"           -> emit an io notification for a node outside
                                 our subtree (must be dropped by the host)
- command "io_bad"            -> emit an io notification with an invalid
                                 direction (must be dropped by the host)
- command "garbage_stdout"    -> write a non-JSON line and a JSON array line
                                 to stdout (a stray print(); the host must
                                 audit and drop both, and keep reading)
- command "io_nonhex"         -> io with raw "0x22F442" (must be dropped)
- command "io_oddlen"         -> io with raw "22F44" (odd length; dropped)
- command "io_spaced"         -> io with raw "22 F4 42" (whitespace between
                                 bytes is tolerated: stored as "22F442")
- command "job_running"       -> declare a running job + running step under
                                 our child, plus an already-done job (the
                                 host derives the terminal `job` events when
                                 we die afterwards)
- command "surrogate_line"    -> emit a log message and a node rename that
                                 carry lone UTF-16 surrogates (\\ud800 /
                                 \\udcff JSON escapes); the host must escape
                                 them instead of poisoning tree/log/sockets
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

# One stderr line at startup: the host must capture it (SPEC 14.1).
sys.stderr.write("fake plugin ready\n")
sys.stderr.flush()


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
        elif action == "io_good":
            notify(
                "io",
                {
                    "nodeId": f"{node_id}:child",
                    "direction": "tx",
                    "raw": "22F190",
                    "decoded": {"service": "ReadDataByIdentifier", "did": "0xF190"},
                    "note": "vin request",
                },
            )
            notify(
                "io",
                {
                    "nodeId": f"{node_id}:child",
                    "direction": "rx",
                    "raw": "62F190564643",
                    "decoded": {"service": "ReadDataByIdentifier", "did": "0xF190", "vin": "VFC"},
                },
            )
            respond(msg_id, {"ok": True, "data": {}})
        elif action == "io_evil":
            notify("io", {"nodeId": "root", "direction": "rx", "raw": "DEADBEEF"})
            respond(msg_id, {"ok": True, "data": {}})
        elif action == "io_bad":
            notify("io", {"nodeId": f"{node_id}:child", "direction": "sideways", "raw": "00"})
            respond(msg_id, {"ok": True, "data": {}})
        elif action == "garbage_stdout":
            sys.stdout.write("this is not json\n")
            sys.stdout.write("[1, 2, 3]\n")
            sys.stdout.flush()
            respond(msg_id, {"ok": True, "data": {}})
        elif action == "io_nonhex":
            notify("io", {"nodeId": f"{node_id}:child", "direction": "tx", "raw": "0x22F442"})
            respond(msg_id, {"ok": True, "data": {}})
        elif action == "io_oddlen":
            notify("io", {"nodeId": f"{node_id}:child", "direction": "rx", "raw": "22F44"})
            respond(msg_id, {"ok": True, "data": {}})
        elif action == "io_spaced":
            notify("io", {"nodeId": f"{node_id}:child", "direction": "tx", "raw": "22 F4 42"})
            respond(msg_id, {"ok": True, "data": {}})
        elif action == "job_running":
            job_id = f"{node_id}:child:job"
            notify(
                "add",
                {
                    "node": {
                        "id": job_id,
                        "type": "job",
                        "name": "Fake job",
                        "status": "running",
                        "progress": 0.3,
                        "parentId": f"{node_id}:child",
                    }
                },
            )
            notify(
                "add",
                {
                    "node": {
                        "id": f"{job_id}:step:1",
                        "type": "job.step",
                        "name": "Fake step",
                        "status": "running",
                        "parentId": job_id,
                    }
                },
            )
            notify(
                "add",
                {
                    "node": {
                        "id": f"{node_id}:child:job-done",
                        "type": "job",
                        "name": "Finished job",
                        "status": "done",
                        "progress": 1.0,
                        "parentId": f"{node_id}:child",
                    }
                },
            )
            respond(msg_id, {"ok": True, "data": {"jobId": job_id}})
        elif action == "surrogate_line":
            # json.dumps (ensure_ascii=True) turns the lone surrogates into the
            # \ud800 / \udcff escapes a surrogateescape'd str produces on the wire.
            notify("log", {"level": "info", "message": "poison \ud800 text"})
            notify("update", {"id": f"{node_id}:child", "changes": {"name": "bad \udcff name"}})
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
