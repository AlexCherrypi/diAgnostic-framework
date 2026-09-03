"""Demo VCI plugin (SPEC 9 + 15.6): proves the skeleton, still car-unspecific.

Declares a simulated device with ``selftest`` / ``selftest_fail`` / ``crash``
actions and three 20 Hz telemetry channels (sine V, noise rpm, sawtooth degC).

Runs out-of-process, spawned by the core; talks JSON-RPC 2.0 over stdio via
:mod:`diagcore.plugins.api`.

M0.5 (SPEC 14.2): while a self-test job runs, the plugin simulates bus
traffic — plausible UDS-ish request/response frame pairs reported via the
``io`` notification (tx then rx, hex payloads, decoded dicts, one pair per
job tick, i.e. ~10 io events/s at the default step duration) — so the raw
feed is demonstrable without hardware.

M1.5 (SPEC 15.6): capability ``vehicle``. ``vehicle.detect`` runs a
detection job under the session node (steps "Wake diagnostic bus", "Scan
addresses 0x00-0x7F" with per-address ``io`` frames of which 5 addresses
answer, "Read identification", "Build profile"), declares 5 ``ecu`` nodes
(each with a ``read_ids`` job action) plus one ``ecu.probe`` tool node whose
``probe`` action answers for known addresses and times out for unknown ones,
and always yields the same fictive vehicle ("Demo Vehicle", VIN
``DEM0VEH1CLE000042``) so the second run demonstrates the library's
"known vehicle -> attach" path (``vehicle.attach`` = ~2 s "Verify ECUs
present" job).

Timings are overridable for fast test runs: ``DEMO_VCI_STEP_SECONDS``
(self-test step), ``DEMO_VCI_DETECT_SECONDS`` (quick detection total),
``DEMO_VCI_ATTACH_SECONDS``, ``DEMO_VCI_READ_IDS_SECONDS`` and
``DEMO_VCI_PROBE_TIMEOUT_SECONDS``.
"""

from __future__ import annotations

import asyncio
import math
import os
import random
import time
from typing import Any

from diagcore.plugins.api import Plugin, PluginJobContext

plugin = Plugin()

#: Seconds per self-test step (4 steps => ~8 s by default). Overridable for
#: fast test runs.
STEP_SECONDS = float(os.environ.get("DEMO_VCI_STEP_SECONDS", "2.0"))
#: Total duration of a quick detection (a full one takes twice as long).
DETECT_SECONDS = float(os.environ.get("DEMO_VCI_DETECT_SECONDS", "6.0"))
ATTACH_SECONDS = float(os.environ.get("DEMO_VCI_ATTACH_SECONDS", "2.0"))
READ_IDS_SECONDS = float(os.environ.get("DEMO_VCI_READ_IDS_SECONDS", "1.0"))
PROBE_TIMEOUT_SECONDS = float(os.environ.get("DEMO_VCI_PROBE_TIMEOUT_SECONDS", "1.5"))

SAMPLE_RATE_HZ = 20.0

STEP_NAMES = [
    "Check power supply",
    "Check bus transceiver",
    "Check protocol stack",
    "Compile report",
]

CHANNELS = [
    # (key, label, unit, min, max)
    ("sine", "Battery voltage (sine)", "V", 10.0, 14.0),
    ("noise", "Engine speed (noise)", "rpm", 700.0, 900.0),
    ("saw", "Coolant temp (sawtooth)", "°C", 60.0, 100.0),
]

# -- the fictive vehicle (SPEC 15.6) ------------------------------------------

VEHICLE_NAME = "Demo Vehicle"
VEHICLE_VIN = "DEM0VEH1CLE000042"
SCAN_FIRST, SCAN_LAST = 0x00, 0x7F
PROTOCOLS = ("UDS", "KWP")

#: (ecu id, name, address, protocol, part number, software version)
ECUS: list[tuple[str, str, int, str, str, str]] = [
    ("engine", "Engine ECU", 0x10, "UDS", "DEMO-ENG-4711", "1.4.2"),
    ("body", "Body Control Module", 0x26, "UDS", "DEMO-BCM-0815", "3.0.1"),
    ("brakes", "Brake / ABS ECU", 0x28, "UDS", "DEMO-ABS-2201", "2.7.0"),
    ("airbag", "Airbag ECU", 0x15, "KWP", "DEMO-SRS-0042", "1.0.9"),
    ("cluster", "Instrument Cluster", 0x17, "KWP", "DEMO-CLU-1337", "5.2.3"),
]
ECU_BY_ADDRESS: dict[int, tuple[str, str, int, str, str, str]] = {ecu[2]: ecu for ecu in ECUS}
ECU_BY_ID: dict[str, tuple[str, str, int, str, str, str]] = {ecu[0]: ecu for ecu in ECUS}

READ_IDS_ACTION = {"name": "read_ids", "label": "Read identification", "params": []}
PROBE_ACTION = {
    "name": "probe",
    "label": "Probe",
    "params": [
        {
            "name": "address",
            "label": "ECU address",
            "type": "string",
            "required": True,
            "placeholder": "0x2A",
            "help": "Hex diagnostic address 0x00-0x7F; known ones answer, unknown ones time out",
        },
        {
            "name": "protocol",
            "label": "Protocol",
            "type": "select",
            "required": True,
            "default": "UDS",
            "options": [{"value": "UDS", "label": "UDS on CAN"}, {"value": "KWP", "label": "KWP2000"}],
        },
    ],
}


class VehicleState:
    """What the plugin currently holds of the (single) vehicle session."""

    session_id: str | None = None
    attached: bool = False
    tool_id: str | None = None


vehicle = VehicleState()


def device_id() -> str:
    return f"{plugin.node_id}:device"


def channel_id(key: str) -> str:
    return f"{device_id()}:ch:{key}"


def ecu_node_id(session_id: str, ecu_id: str) -> str:
    return f"{session_id}:ecu:{ecu_id}"


def tool_node_id(session_id: str) -> str:
    return f"{session_id}:tool:probe"


def hex_address(address: int) -> str:
    return f"0x{address:02X}"


@plugin.on_initialized
def declare() -> None:
    dev = device_id()
    plugin.add(
        {
            "id": dev,
            "type": "device.sim",
            "name": "Simulated VCI",
            "status": "idle",
            "parentId": plugin.node_id,
            "actions": [
                {"name": "selftest", "label": "Run self-test", "params": []},
                {"name": "selftest_fail", "label": "Run failing self-test", "params": []},
                {"name": "crash", "label": "Crash plugin process", "params": []},
            ],
        }
    )
    for key, label, unit, lo, hi in CHANNELS:
        plugin.add(
            {
                "id": channel_id(key),
                "type": "telemetry.channel",
                "name": label,
                "status": "running",
                "parentId": dev,
                "props": {
                    "unit": unit,
                    "label": label,
                    "sampleRateHz": SAMPLE_RATE_HZ,
                    "min": lo,
                    "max": hi,
                },
            }
        )
    plugin.create_task(telemetry_loop())
    plugin.log("info", "demo_vci declared device and channels")


async def telemetry_loop() -> None:
    period = 1.0 / SAMPLE_RATE_HZ
    while True:
        now = time.time()
        t_ms = now * 1000.0
        sine = 12.0 + 2.0 * math.sin(2.0 * math.pi * 0.2 * now)
        noise = 800.0 + random.uniform(-100.0, 100.0)
        saw = 60.0 + 40.0 * ((now % 8.0) / 8.0)
        plugin.samples(channel_id("sine"), [[t_ms, sine]])
        plugin.samples(channel_id("noise"), [[t_ms, noise]])
        plugin.samples(channel_id("saw"), [[t_ms, saw]])
        await asyncio.sleep(period)


# -- self-test (SPEC 9 / 14.2) --------------------------------------------------


def _emit_selftest_io(step_index: int, tick: int) -> None:
    """One simulated UDS request/response pair (tx then rx) for a job tick."""
    dev = device_id()
    step_name = STEP_NAMES[step_index]
    if step_index == 0:
        # ReadDataByIdentifier: battery voltage
        mv = int((12000 + 500 * math.sin(tick)) + random.uniform(-50, 50))
        plugin.io(
            dev,
            "tx",
            "22F442",
            decoded={"service": "ReadDataByIdentifier", "serviceId": "0x22", "did": "0xF442"},
            note=step_name,
        )
        plugin.io(
            dev,
            "rx",
            f"62F442{mv:04X}",
            decoded={
                "service": "ReadDataByIdentifier",
                "serviceId": "0x62",
                "did": "0xF442",
                "batteryVoltageMv": mv,
            },
        )
    elif step_index == 1:
        # TesterPresent keeps the bus transceiver talking
        plugin.io(
            dev,
            "tx",
            "3E00",
            decoded={"service": "TesterPresent", "serviceId": "0x3E", "subFunction": "0x00"},
            note=step_name,
        )
        plugin.io(
            dev,
            "rx",
            "7E00",
            decoded={"service": "TesterPresent", "serviceId": "0x7E", "subFunction": "0x00"},
        )
    elif step_index == 2:
        # DiagnosticSessionControl: extended session
        plugin.io(
            dev,
            "tx",
            "1003",
            decoded={"service": "DiagnosticSessionControl", "serviceId": "0x10", "session": "0x03"},
            note=step_name,
        )
        plugin.io(
            dev,
            "rx",
            "500300320190",
            decoded={
                "service": "DiagnosticSessionControl",
                "serviceId": "0x50",
                "session": "0x03",
                "p2ServerMaxMs": 50,
                "p2StarServerMaxMs": 4000,
            },
        )
    else:
        # ReadDTCInformation: report DTCs by status mask (none set)
        plugin.io(
            dev,
            "tx",
            "1902AF",
            decoded={"service": "ReadDTCInformation", "serviceId": "0x19", "subFunction": "0x02", "statusMask": "0xAF"},
            note=step_name,
        )
        plugin.io(
            dev,
            "rx",
            "5902FF",
            decoded={
                "service": "ReadDTCInformation",
                "serviceId": "0x59",
                "subFunction": "0x02",
                "availabilityMask": "0xFF",
                "dtcCount": 0,
            },
        )


def _emit_fail_io() -> None:
    """The failing exchange preceding the simulated step-3 fault."""
    dev = device_id()
    plugin.io(
        dev,
        "tx",
        "1003",
        decoded={"service": "DiagnosticSessionControl", "serviceId": "0x10", "session": "0x03"},
        note=STEP_NAMES[2],
    )
    plugin.io(
        dev,
        "rx",
        "7F1031",
        decoded={
            "service": "NegativeResponse",
            "serviceId": "0x7F",
            "rejectedService": "0x10",
            "nrc": "0x31",
            "nrcName": "requestOutOfRange",
        },
        note="simulated fault",
    )


async def run_selftest(ctx: PluginJobContext, *, fail: bool = False) -> None:
    ticks = 10
    total = len(STEP_NAMES)
    for step_index, step_name in enumerate(STEP_NAMES):
        async with ctx.step(step_name):
            for tick in range(ticks):
                if fail and step_index == 2 and tick == ticks // 2:
                    _emit_fail_io()
                    raise RuntimeError("bus transceiver returned garbage")
                _emit_selftest_io(step_index, tick)
                await asyncio.sleep(STEP_SECONDS / ticks)
                ctx.set_progress((step_index + (tick + 1) / ticks) / total)


@plugin.on_command("selftest")
def selftest(target: str, params: dict) -> dict:
    job_id = plugin.start_job(
        "Self-test",
        lambda ctx: run_selftest(ctx, fail=False),
        locks=["bus:demo_vci"],
        parent_id=device_id(),
    )
    return {"jobId": job_id}


@plugin.on_command("selftest_fail")
def selftest_fail(target: str, params: dict) -> dict:
    job_id = plugin.start_job(
        "Self-test (failing)",
        lambda ctx: run_selftest(ctx, fail=True),
        locks=["bus:demo_vci"],
        parent_id=device_id(),
    )
    return {"jobId": job_id}


@plugin.on_command("crash")
def crash(target: str, params: dict) -> dict:
    # Demonstrates supervision: hard-kill without any cleanup or response.
    os._exit(1)


# -- simulated ECU conversations (SPEC 15.6) ------------------------------------


def _ident_request(node_id: str, ecu: tuple[str, str, int, str, str, str]) -> None:
    """The identification request (tx frame) for ``ecu`` on its protocol."""
    _ecu_id, name, address, protocol, _part_number, _sw_version = ecu
    if protocol == "UDS":
        plugin.io(
            node_id,
            "tx",
            "22F190",
            decoded={"service": "ReadDataByIdentifier", "serviceId": "0x22", "did": "0xF190", "address": hex_address(address)},
            note=f"read identification of {name}",
        )
    else:
        plugin.io(
            node_id,
            "tx",
            "1A80",
            decoded={"service": "ReadEcuIdentification", "serviceId": "0x1A", "option": "0x80", "address": hex_address(address)},
            note=f"read identification of {name}",
        )


def _ident_response(node_id: str, ecu: tuple[str, str, int, str, str, str]) -> dict[str, Any]:
    """The identification answer (rx frame) and the decoded identification."""
    _ecu_id, _name, address, protocol, part_number, sw_version = ecu
    if protocol == "UDS":
        payload = VEHICLE_VIN.encode("ascii").hex().upper()
        plugin.io(
            node_id,
            "rx",
            f"62F190{payload}",
            decoded={"service": "ReadDataByIdentifier", "serviceId": "0x62", "did": "0xF190", "vin": VEHICLE_VIN},
        )
    else:
        payload = part_number.encode("ascii").hex().upper()
        plugin.io(
            node_id,
            "rx",
            f"5A80{payload}",
            decoded={"service": "ReadEcuIdentification", "serviceId": "0x5A", "option": "0x80", "partNumber": part_number},
        )
    return {
        "vin": VEHICLE_VIN,
        "partNumber": part_number,
        "swVersion": sw_version,
        "hwVersion": f"H{address:02d}",
        "serial": f"SN{address:02X}{random.randint(100000, 999999)}",
    }


def _ident_frames(node_id: str, ecu: tuple[str, str, int, str, str, str]) -> dict[str, Any]:
    """ReadDataByIdentifier of the identification block: one tx/rx pair."""
    _ident_request(node_id, ecu)
    return _ident_response(node_id, ecu)


def _ecu_props(ecu: tuple[str, str, int, str, str, str]) -> dict[str, Any]:
    ecu_id, name, address, protocol, part_number, _sw = ecu
    return {"ecuId": ecu_id, "address": hex_address(address), "protocol": protocol, "partNumber": part_number}


def _declare_ecu(session_id: str, ecu: tuple[str, str, int, str, str, str], *, status: str = "idle") -> str:
    node_id = ecu_node_id(session_id, ecu[0])
    plugin.add(
        {
            "id": node_id,
            "type": "ecu",
            "name": ecu[1],
            "status": status,
            "parentId": session_id,
            "props": _ecu_props(ecu),
            "actions": [dict(READ_IDS_ACTION)],
        }
    )
    return node_id


def _declare_probe_tool(session_id: str) -> str:
    node_id = tool_node_id(session_id)
    plugin.add(
        {
            "id": node_id,
            "type": "ecu.probe",
            "name": "Probe ECU",
            "status": "idle",
            "parentId": session_id,
            "props": {"addressRange": f"{hex_address(SCAN_FIRST)}-{hex_address(SCAN_LAST)}", "protocols": list(PROTOCOLS)},
            "actions": [dict(PROBE_ACTION)],
        }
    )
    vehicle.tool_id = node_id
    return node_id


def _profile() -> dict[str, Any]:
    return {
        "name": VEHICLE_NAME,
        "vin": VEHICLE_VIN,
        "identifiers": {
            "bus": "demo-can-500k",
            "ecuAddresses": [hex_address(ecu[2]) for ecu in ECUS],
        },
        "ecus": [
            {
                "id": ecu[0],
                "name": ecu[1],
                "address": hex_address(ecu[2]),
                "protocol": ecu[3],
                "props": {"partNumber": ecu[4], "swVersion": ecu[5]},
            }
            for ecu in ECUS
        ],
        "notes": "",
    }


# -- vehicle.detect: the detection job (SPEC 15.6) -------------------------------


async def run_detect(ctx: PluginJobContext, session_id: str, depth: str) -> None:
    # quick: one protocol-agnostic TesterPresent per address (every ECU that
    # exists answers); full: one request per protocol per address (an ECU
    # answers on its own protocol only) => same 5 ECUs, twice the frames/time.
    passes: list[str | None] = list(PROTOCOLS) if depth == "full" else [None]
    total = DETECT_SECONDS * (2.0 if depth == "full" else 1.0)
    addresses = list(range(SCAN_FIRST, SCAN_LAST + 1))
    # time budget per step: 10 % wake, 60 % scan, 20 % identification, 10 % profile
    wake_time, scan_time, ident_time, build_time = 0.1 * total, 0.6 * total, 0.2 * total, 0.1 * total

    async with ctx.step("Wake diagnostic bus"):
        plugin.io(
            session_id,
            "tx",
            "3E80",
            decoded={"service": "TesterPresent", "serviceId": "0x3E", "subFunction": "0x80", "note": "functional wake-up"},
            note="wake diagnostic bus",
        )
        await asyncio.sleep(wake_time / 2)
        plugin.io(
            session_id,
            "rx",
            "7E80",
            decoded={"service": "TesterPresent", "serviceId": "0x7E", "subFunction": "0x80"},
        )
        await asyncio.sleep(wake_time / 2)
        ctx.set_progress(0.1)

    found: list[tuple[str, str, int, str, str, str]] = []
    async with ctx.step(f"Scan addresses {hex_address(SCAN_FIRST)}-{hex_address(SCAN_LAST)}"):
        per_request = scan_time / (len(addresses) * len(passes))
        request_index = 0
        for address in addresses:
            for protocol in passes:
                request_index += 1
                addr = hex_address(address)
                if protocol is None:
                    plugin.io(
                        session_id,
                        "tx",
                        f"{address:02X}3E80",
                        decoded={"address": addr, "protocol": "any", "service": "TesterPresent", "subFunction": "0x80"},
                        note=f"probe {addr}",
                    )
                elif protocol == "UDS":
                    plugin.io(
                        session_id,
                        "tx",
                        f"{address:02X}1001",
                        decoded={"address": addr, "protocol": protocol, "service": "DiagnosticSessionControl", "session": "0x01"},
                        note=f"probe {addr} ({protocol})",
                    )
                else:
                    plugin.io(
                        session_id,
                        "tx",
                        f"{address:02X}81",
                        decoded={"address": addr, "protocol": protocol, "service": "StartCommunication"},
                        note=f"probe {addr} ({protocol})",
                    )
                ecu = ECU_BY_ADDRESS.get(address)
                if ecu is not None and (protocol is None or ecu[3] == protocol):
                    if protocol is None:
                        plugin.io(
                            session_id,
                            "rx",
                            f"{address:02X}7E80",
                            decoded={"address": addr, "protocol": ecu[3], "service": "TesterPresent", "subFunction": "0x80"},
                            note=f"{ecu[1]} answered ({ecu[3]})",
                        )
                    elif protocol == "UDS":
                        plugin.io(
                            session_id,
                            "rx",
                            f"{address:02X}500100320190",
                            decoded={"address": addr, "protocol": protocol, "service": "DiagnosticSessionControl", "session": "0x01"},
                            note=f"{ecu[1]} answered",
                        )
                    else:
                        plugin.io(
                            session_id,
                            "rx",
                            f"{address:02X}C1EF8F",
                            decoded={"address": addr, "protocol": protocol, "service": "StartCommunication", "keyBytes": "EF8F"},
                            note=f"{ecu[1]} answered",
                        )
                    if ecu not in found:
                        found.append(ecu)
                        _declare_ecu(session_id, ecu)
                await asyncio.sleep(per_request)
                ctx.set_progress(0.1 + 0.6 * request_index / (len(addresses) * len(passes)))
        if not found:
            raise RuntimeError("no ECU answered on the diagnostic bus")

    async with ctx.step("Read identification"):
        for index, ecu in enumerate(found):
            node_id = ecu_node_id(session_id, ecu[0])
            plugin.update(node_id, {"status": "running"})
            identification = _ident_frames(node_id, ecu)
            await asyncio.sleep(ident_time / len(found))
            plugin.merge_props(node_id, {"identification": identification})
            plugin.update(node_id, {"status": "idle"})
            ctx.set_progress(0.7 + 0.2 * (index + 1) / len(found))

    async with ctx.step("Build profile"):
        await asyncio.sleep(build_time)
        _declare_probe_tool(session_id)
        vehicle.attached = True
        plugin.emit_vehicle_profile(session_id, _profile())
        ctx.set_progress(1.0)
    plugin.log("info", f"demo_vci detected {VEHICLE_NAME} (VIN {VEHICLE_VIN}) with {len(found)} ECUs")


def on_vehicle_detect(session_id: str, params: dict[str, Any]) -> str:
    depth = str(params.get("depth") or "quick")
    if depth not in ("quick", "full"):
        raise ValueError(f"unknown depth {depth!r} (expected 'quick' or 'full')")
    vehicle.session_id = session_id
    vehicle.attached = False
    vehicle.tool_id = None
    return plugin.start_job(
        f"Detect vehicle ({depth} scan)",
        lambda ctx: run_detect(ctx, session_id, depth),
        locks=["bus:demo_vci"],
        parent_id=session_id,
    )


# -- vehicle.attach: the verification job ---------------------------------------


async def run_attach(ctx: PluginJobContext, session_id: str, profile: dict[str, Any]) -> None:
    ecus = profile.get("ecus") or []
    if not isinstance(ecus, list) or not ecus:
        raise RuntimeError("profile carries no ECUs to verify")
    async with ctx.step("Verify ECUs present"):
        for index, entry in enumerate(ecus):
            ecu = ECU_BY_ID.get(str(entry.get("id")))
            addr = str(entry.get("address"))
            if ecu is None or hex_address(ecu[2]) != addr:
                raise RuntimeError(f"ECU {entry.get('id')!r} at {addr} is not part of the demo vehicle (profile mismatch)")
            plugin.io(
                session_id,
                "tx",
                f"{ecu[2]:02X}3E00",
                decoded={"address": addr, "protocol": ecu[3], "service": "TesterPresent"},
                note=f"verify {ecu[1]}",
            )
            await asyncio.sleep(ATTACH_SECONDS / len(ecus))
            plugin.io(
                session_id,
                "rx",
                f"{ecu[2]:02X}7E00",
                decoded={"address": addr, "protocol": ecu[3], "service": "TesterPresent"},
                note=f"{ecu[1]} present",
            )
            _declare_ecu(session_id, ecu)
            ctx.set_progress((index + 1) / len(ecus))
    _declare_probe_tool(session_id)
    vehicle.attached = True
    plugin.log("info", f"demo_vci attached {profile.get('name')!r} ({len(ecus)} ECUs verified)")


def on_vehicle_attach(session_id: str, profile: dict[str, Any]) -> str:
    if profile.get("vin") != VEHICLE_VIN:
        raise ValueError(f"profile VIN {profile.get('vin')!r} is not the demo vehicle ({VEHICLE_VIN})")
    vehicle.session_id = session_id
    vehicle.attached = False
    vehicle.tool_id = None
    return plugin.start_job(
        "Verify ECUs present",
        lambda ctx: run_attach(ctx, session_id, profile),
        locks=["bus:demo_vci"],
        parent_id=session_id,
    )


def on_vehicle_detach(session_id: str) -> None:
    vehicle.session_id = None
    vehicle.attached = False
    vehicle.tool_id = None
    plugin.log("info", f"demo_vci released the vehicle of session {session_id}")


plugin.register_vehicle_handlers(detect=on_vehicle_detect, attach=on_vehicle_attach, detach=on_vehicle_detach)


# -- ECU actions (SPEC 15.6): read_ids on ecu nodes, probe on the tool node ------


def _ecu_of_target(target: str) -> tuple[str, str, int, str, str, str]:
    session_id = vehicle.session_id
    if session_id is None:
        raise RuntimeError("no vehicle session is active")
    for ecu in ECUS:
        if ecu_node_id(session_id, ecu[0]) == target:
            return ecu
    raise RuntimeError(f"target {target!r} is not one of the demo ECUs")


async def run_read_ids(ctx: PluginJobContext, target: str, ecu: tuple[str, str, int, str, str, str]) -> None:
    async with ctx.step(f"Read identification of {ecu[1]}"):
        identification = _ident_frames(target, ecu)
        await asyncio.sleep(READ_IDS_SECONDS)
        plugin.merge_props(target, {"identification": identification, "lastReadMs": time.time() * 1000.0})
        ctx.set_progress(1.0)


@plugin.on_command("read_ids")
def read_ids(target: str, params: dict) -> dict:
    ecu = _ecu_of_target(target)
    job_id = plugin.start_job(
        f"Read identification ({ecu[1]})",
        lambda ctx: run_read_ids(ctx, target, ecu),
        locks=["bus:demo_vci"],
        parent_id=target,
    )
    return {"jobId": job_id, "ecuId": ecu[0]}


def _parse_address(raw: Any) -> int:
    text = str(raw).strip()
    if not text:
        raise ValueError("address is required (e.g. 0x2A)")
    try:
        value = int(text, 16)  # hex, optional 0x prefix
    except ValueError:
        raise ValueError(f"invalid address {text!r}: expected hex like 0x2A") from None
    if not SCAN_FIRST <= value <= SCAN_LAST:
        raise ValueError(f"address {hex_address(value)} is outside {hex_address(SCAN_FIRST)}-{hex_address(SCAN_LAST)}")
    return value


@plugin.on_command("probe")
async def probe(target: str, params: dict) -> dict:
    if vehicle.session_id is None or target != vehicle.tool_id:
        raise RuntimeError("no vehicle session is active")
    address = _parse_address(params.get("address"))
    protocol = str(params.get("protocol") or "UDS").upper()
    if protocol not in PROTOCOLS:
        raise ValueError(f"unknown protocol {protocol!r} (expected UDS or KWP)")
    addr = hex_address(address)
    started = time.monotonic()
    if protocol == "UDS":
        plugin.io(
            target,
            "tx",
            f"{address:02X}22F190",
            decoded={"address": addr, "protocol": protocol, "service": "ReadDataByIdentifier", "did": "0xF190"},
            note=f"probe {addr} ({protocol})",
        )
    else:
        plugin.io(
            target,
            "tx",
            f"{address:02X}1A80",
            decoded={"address": addr, "protocol": protocol, "service": "ReadEcuIdentification", "option": "0x80"},
            note=f"probe {addr} ({protocol})",
        )
    ecu = ECU_BY_ADDRESS.get(address)
    if ecu is None or ecu[3] != protocol:
        await asyncio.sleep(PROBE_TIMEOUT_SECONDS)
        plugin.io(
            target,
            "rx",
            f"{address:02X}00",
            decoded={"address": addr, "protocol": protocol, "timeout": True},
            note=f"no response from {addr} ({protocol}) after {PROBE_TIMEOUT_SECONDS:g} s",
        )
        plugin.merge_props(target, {"lastProbe": {"address": addr, "protocol": protocol, "ok": False}})
        raise RuntimeError(f"no response from {addr} ({protocol}) after {PROBE_TIMEOUT_SECONDS:g} s")
    # The probe request above IS the identification request on the ECU's
    # protocol; only its answer follows, so the trail shows one tx/rx pair.
    await asyncio.sleep(0.05)
    identification = _ident_response(target, ecu)
    elapsed_ms = round((time.monotonic() - started) * 1000.0, 1)
    result = {
        "address": addr,
        "protocol": protocol,
        "ecuId": ecu[0],
        "name": ecu[1],
        "identification": identification,
        "responseTimeMs": elapsed_ms,
    }
    plugin.merge_props(target, {"lastProbe": {**result, "ok": True}})
    return result


if __name__ == "__main__":
    plugin.run()
