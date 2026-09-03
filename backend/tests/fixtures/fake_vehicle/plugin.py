"""Scripted fake VEHICLE plugin used by the vehicle-session tests (SPEC 15).

Like ``fake_plugin`` it deliberately does NOT use diagcore.plugins.api: a
minimal, synchronous, hand-rolled stdio JSON-RPC loop, so the core is tested
against the wire contract of SPEC 15.4 rather than against our helper lib.
Nothing happens on its own — every state change is driven by a command on
the control node ``<pluginNodeId>:ctl`` so tests stay deterministic.

Requests from the core:
- ``vehicle.detect {sessionNodeId, params}`` -> declares a running ``job``
  node under the session (props.detectParams = the params received) and
  answers ``{ok, jobNodeId}``; with ``FAKE_DETECT_REJECT=1`` answers
  ``{ok: false, error}`` instead; with ``FAKE_DETECT_HANG=1`` it never
  answers; with ``FAKE_DETECT_NO_JOB=1`` it answers ``{ok}`` without a job;
  with ``FAKE_DETECT_INSTANT_FAIL=1`` the job is declared and ends
  ``error`` BEFORE the answer is written.
- ``vehicle.attach {sessionNodeId, profile}`` -> declares the profile's
  ``ecu`` nodes under the session and a running "Verify" job, answers
  ``{ok, jobNodeId}``; with ``FAKE_ATTACH_NO_JOB=1`` no job (``{ok}`` only).
- ``vehicle.detach {sessionNodeId}`` -> ``{ok: true}`` (records
  ``props.detached`` on the control node); with ``FAKE_DETACH_HANG=1`` it
  never answers.
- ``command cancel`` on the current job -> job status ``cancelled``; with
  ``FAKE_CANCEL_HANG=1`` it never answers (and the job keeps running).

Control-node commands (all answer ok):
- ``finish``          -> declare 2 ecu nodes, send ``vehicle.profile`` (params
                         override name/vin/identifiers), job -> done
- ``finish_attach``   -> job -> done (no profile)
- ``profile``         -> send ``vehicle.profile`` only (params override)
- ``profile_full``    -> re-send the FULL profile received in vehicle.attach
                         (incl. id/firstSeen/lastSeen) with params.identifiers
                         merged in
- ``fail``            -> job -> error
- ``done_no_profile`` -> job -> done without any profile
- ``remove_job``      -> remove the current job node
- ``profile_bad``     -> malformed profile (no name, ecus not a list)
- ``profile_nogrant`` -> profile for sessionNodeId "jobs" (no grant)
- ``profile_wrong_plugin`` -> profile claiming pluginId "other"
- ``add_under_session`` -> add node "<session>:extra" under params.session
                         (default "vehicle")
- ``io_session``      -> an io frame on the session node
- ``raw_notify``      -> send the notification params.method with
                         params.params verbatim (grant/ownership tests)
- ``bad_action_params`` -> add a node in our own subtree whose action
                         declares a select param without options
- ``die``             -> os._exit(1)
"""

from __future__ import annotations

import json
import os
import sys

REJECT_DETECT = os.environ.get("FAKE_DETECT_REJECT") == "1"
DETECT_HANG = os.environ.get("FAKE_DETECT_HANG") == "1"
DETECT_NO_JOB = os.environ.get("FAKE_DETECT_NO_JOB") == "1"
DETECT_INSTANT_FAIL = os.environ.get("FAKE_DETECT_INSTANT_FAIL") == "1"
ATTACH_NO_JOB = os.environ.get("FAKE_ATTACH_NO_JOB") == "1"
DETACH_HANG = os.environ.get("FAKE_DETACH_HANG") == "1"
CANCEL_HANG = os.environ.get("FAKE_CANCEL_HANG") == "1"


def send(obj: dict) -> None:
    sys.stdout.write(json.dumps(obj) + "\n")
    sys.stdout.flush()


def respond(msg_id, result) -> None:
    send({"jsonrpc": "2.0", "id": msg_id, "result": result})


def notify(method: str, params: dict) -> None:
    send({"jsonrpc": "2.0", "method": method, "params": params})


node_id = None
ctl_id = None
session_id = None
job_id = None
job_counter = 0
attached_profile: dict | None = None  # the full profile received in vehicle.attach

ECUS = [
    {"id": "engine", "name": "Fake Engine", "address": "0x10", "protocol": "UDS", "props": {"sw": "1.0"}},
    {"id": "body", "name": "Fake Body", "address": "0x26", "protocol": "KWP", "props": {}},
]


def declare_ecus(session: str, ecus: list[dict]) -> None:
    for ecu in ecus:
        notify(
            "add",
            {
                "node": {
                    "id": f"{session}:ecu:{ecu['id']}",
                    "type": "ecu",
                    "name": ecu["name"],
                    "status": "idle",
                    "parentId": session,
                    "props": {"ecuId": ecu["id"], "address": ecu["address"], "protocol": ecu["protocol"]},
                    "actions": [{"name": "read_ids", "label": "Read ids", "params": []}],
                }
            },
        )


def start_job(session: str, name: str, props: dict) -> str:
    global job_counter, job_id
    job_counter += 1
    job_id = f"{session}:job:{job_counter}"
    notify(
        "add",
        {
            "node": {
                "id": job_id,
                "type": "job",
                "name": name,
                "status": "running",
                "progress": 0.0,
                "parentId": session,
                "props": props,
                "actions": [{"name": "cancel", "label": "Cancel", "params": []}],
            }
        },
    )
    return job_id


def profile(overrides: dict) -> dict:
    data = {
        "name": "Fake Car",
        "vin": "FAKEVIN000000001",
        "identifiers": {"serial": "abc"},
        "ecus": ECUS,
        "notes": "",
    }
    for key in ("name", "vin", "identifiers", "notes", "pluginId"):
        if key in overrides:
            data[key] = overrides[key]
    return data


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
        ctl_id = f"{node_id}:ctl"
        respond(msg_id, {"ok": True})
        notify(
            "add",
            {
                "node": {
                    "id": ctl_id,
                    "type": "device.sim",
                    "name": "Fake vehicle control",
                    "status": "idle",
                    "parentId": node_id,
                }
            },
        )
    elif method == "ping":
        respond(msg_id, {})
    elif method == "vehicle.detect":
        if REJECT_DETECT:
            respond(msg_id, {"ok": False, "error": {"message": "detect refused", "detail": "FAKE_DETECT_REJECT"}})
            continue
        session_id = params["sessionNodeId"]
        if DETECT_HANG:
            continue  # never answers: the core must not wait for the generic command timeout
        if DETECT_NO_JOB:
            respond(msg_id, {"ok": True})  # synchronous detection: no job, the profile follows later (or not)
            continue
        started = start_job(session_id, "Fake detection", {"detectParams": params.get("params") or {}})
        if DETECT_INSTANT_FAIL:
            notify(
                "update",
                {"id": started, "changes": {"status": "error", "error": {"message": "instant fail"}, "actions": []}},
            )
        respond(msg_id, {"ok": True, "jobNodeId": started})
    elif method == "vehicle.attach":
        session_id = params["sessionNodeId"]
        attached_profile = dict(params.get("profile") or {})
        declare_ecus(session_id, list((params.get("profile") or {}).get("ecus") or []))
        if ATTACH_NO_JOB:
            respond(msg_id, {"ok": True})
        else:
            started = start_job(session_id, "Fake verify", {"attachedProfile": (params.get("profile") or {}).get("id")})
            respond(msg_id, {"ok": True, "jobNodeId": started})
    elif method == "vehicle.detach":
        if DETACH_HANG:
            continue  # never answers: the core must proceed after its timeout
        notify("merge_props", {"id": ctl_id, "props": {"detached": params.get("sessionNodeId")}})
        session_id = None
        job_id = None
        respond(msg_id, {"ok": True})
    elif method == "command":
        action = params.get("action")
        target = params.get("target")
        cmd_params = params.get("params") or {}
        if action == "cancel" and target == job_id:
            if CANCEL_HANG:
                continue  # never answers, job keeps running
            notify("update", {"id": job_id, "changes": {"status": "cancelled", "actions": []}})
            notify("merge_props", {"id": ctl_id, "props": {"cancelled": job_id}})
            respond(msg_id, {"ok": True, "data": {}})
        elif action == "profile_full":
            full = dict(attached_profile or profile({}))
            full["identifiers"] = {**(full.get("identifiers") or {}), **(cmd_params.get("identifiers") or {})}
            notify("vehicle.profile", {"sessionNodeId": session_id, "profile": full})
            respond(msg_id, {"ok": True, "data": {"sent": full}})
        elif action == "remove_job":
            notify("remove", {"id": job_id})
            respond(msg_id, {"ok": True, "data": {"removed": job_id}})
        elif action == "raw_notify":
            notify(str(cmd_params.get("method")), dict(cmd_params.get("params") or {}))
            respond(msg_id, {"ok": True, "data": {}})
        elif action == "finish":
            declare_ecus(session_id, ECUS)
            notify("vehicle.profile", {"sessionNodeId": session_id, "profile": profile(cmd_params)})
            notify("update", {"id": job_id, "changes": {"status": "done", "progress": 1.0, "actions": []}})
            respond(msg_id, {"ok": True, "data": {"jobId": job_id}})
        elif action == "finish_attach":
            notify("update", {"id": job_id, "changes": {"status": "done", "progress": 1.0, "actions": []}})
            respond(msg_id, {"ok": True, "data": {"jobId": job_id}})
        elif action == "profile":
            notify("vehicle.profile", {"sessionNodeId": session_id, "profile": profile(cmd_params)})
            respond(msg_id, {"ok": True, "data": {}})
        elif action == "fail":
            notify(
                "update",
                {
                    "id": job_id,
                    "changes": {"status": "error", "error": {"message": "scan failed: bus silent"}, "actions": []},
                },
            )
            respond(msg_id, {"ok": True, "data": {}})
        elif action == "done_no_profile":
            notify("update", {"id": job_id, "changes": {"status": "done", "progress": 1.0, "actions": []}})
            respond(msg_id, {"ok": True, "data": {}})
        elif action == "profile_bad":
            notify("vehicle.profile", {"sessionNodeId": session_id, "profile": {"vin": "X", "ecus": "nope"}})
            respond(msg_id, {"ok": True, "data": {}})
        elif action == "profile_nogrant":
            notify("vehicle.profile", {"sessionNodeId": "jobs", "profile": profile({})})
            respond(msg_id, {"ok": True, "data": {}})
        elif action == "profile_wrong_plugin":
            notify("vehicle.profile", {"sessionNodeId": session_id, "profile": profile({"pluginId": "other"})})
            respond(msg_id, {"ok": True, "data": {}})
        elif action == "add_under_session":
            session = cmd_params.get("session") or "vehicle"
            notify(
                "add",
                {
                    "node": {
                        "id": f"{session}:extra",
                        "type": "device.sim",
                        "name": "Extra",
                        "status": "idle",
                        "parentId": session,
                    }
                },
            )
            respond(msg_id, {"ok": True, "data": {}})
        elif action == "io_session":
            notify("io", {"nodeId": cmd_params.get("session") or "vehicle", "direction": "tx", "raw": "3E80"})
            respond(msg_id, {"ok": True, "data": {}})
        elif action == "bad_action_params":
            notify(
                "add",
                {
                    "node": {
                        "id": f"{node_id}:badparams",
                        "type": "device.sim",
                        "name": "Bad params",
                        "status": "idle",
                        "parentId": node_id,
                        "actions": [
                            {"name": "x", "label": "X", "params": [{"name": "mode", "label": "Mode", "type": "select"}]}
                        ],
                    }
                },
            )
            respond(msg_id, {"ok": True, "data": {}})
        elif action == "die":
            os._exit(1)
        else:
            respond(msg_id, {"ok": False, "error": {"message": f"unknown action {action!r}"}})
    elif method == "shutdown":
        respond(msg_id, {})
        break
    else:
        send({"jsonrpc": "2.0", "id": msg_id, "error": {"code": -32601, "message": "method not found"}})
