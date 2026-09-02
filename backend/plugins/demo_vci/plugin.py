"""Demo VCI plugin (SPEC 9): proves the skeleton, still car-unspecific.

Declares a simulated device with ``selftest`` / ``selftest_fail`` / ``crash``
actions and three 20 Hz telemetry channels (sine V, noise rpm, sawtooth degC).

Runs out-of-process, spawned by the core; talks JSON-RPC 2.0 over stdio via
:mod:`diagcore.plugins.api`.

M0.5 (SPEC 14.2): while a self-test job runs, the plugin simulates bus
traffic — plausible UDS-ish request/response frame pairs reported via the
``io`` notification (tx then rx, hex payloads, decoded dicts, one pair per
job tick, i.e. ~10 io events/s at the default step duration) — so the raw
feed is demonstrable without hardware.
"""

from __future__ import annotations

import asyncio
import math
import os
import random
import time

from diagcore.plugins.api import Plugin, PluginJobContext

plugin = Plugin()

#: Seconds per self-test step (4 steps => ~8 s by default). Overridable for
#: fast test runs.
STEP_SECONDS = float(os.environ.get("DEMO_VCI_STEP_SECONDS", "2.0"))

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


def device_id() -> str:
    return f"{plugin.node_id}:device"


def channel_id(key: str) -> str:
    return f"{device_id()}:ch:{key}"


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


if __name__ == "__main__":
    plugin.run()
