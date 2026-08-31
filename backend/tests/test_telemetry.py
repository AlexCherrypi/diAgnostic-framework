"""Telemetry hub: ring buffers and 100 ms batching (SPEC 5)."""

from __future__ import annotations

import asyncio

from diagcore.telemetry import TelemetryHub


def drain(queue: asyncio.Queue) -> list[dict]:
    out = []
    while not queue.empty():
        out.append(queue.get_nowait())
    return out


# -- ring buffer ------------------------------------------------------------


async def test_ring_buffer_caps_history() -> None:
    hub = TelemetryHub(ring_size=5)
    hub.publish("ch", [[float(i), float(i * 10)] for i in range(8)])
    assert hub.history("ch") == [[float(i), float(i * 10)] for i in range(3, 8)]


async def test_default_ring_size_is_10k() -> None:
    hub = TelemetryHub()
    hub.publish("ch", [[float(i), 0.0] for i in range(10_005)])
    history = hub.history("ch")
    assert len(history) == 10_000
    assert history[0][0] == 5.0


async def test_history_of_unknown_channel_is_empty() -> None:
    hub = TelemetryHub()
    assert hub.history("nope") == []


# -- subscribe / batching ---------------------------------------------------


async def test_no_pending_batches_without_subscribers() -> None:
    hub = TelemetryHub()
    hub.publish("ch", [[1.0, 2.0]])
    hub.flush()  # nothing to deliver, nothing buffered
    assert hub.history("ch") == [[1.0, 2.0]]


async def test_subscribe_returns_history_and_batches_follow() -> None:
    hub = TelemetryHub()
    hub.publish("ch", [[1.0, 10.0]])
    q: asyncio.Queue = asyncio.Queue()
    history = hub.subscribe("ch", q)
    assert history == [[1.0, 10.0]]

    hub.publish("ch", [[2.0, 20.0]])
    hub.publish("ch", [[3.0, 30.0]])
    assert drain(q) == []  # nothing until flush
    hub.flush()
    messages = drain(q)
    assert messages == [{"t": "samples", "channel": "ch", "samples": [[2.0, 20.0], [3.0, 30.0]]}]
    hub.flush()
    assert drain(q) == []  # pending cleared


async def test_new_subscriber_never_gets_overlap_with_history() -> None:
    hub = TelemetryHub()
    qa: asyncio.Queue = asyncio.Queue()
    hub.subscribe("ch", qa)
    hub.publish("ch", [[1.0, 1.0]])  # pending for qa, not yet flushed

    qb: asyncio.Queue = asyncio.Queue()
    history_b = hub.subscribe("ch", qb)  # forces early flush to qa
    assert history_b == [[1.0, 1.0]]
    assert drain(qa) == [{"t": "samples", "channel": "ch", "samples": [[1.0, 1.0]]}]

    hub.publish("ch", [[2.0, 2.0]])
    hub.flush()
    assert drain(qa) == [{"t": "samples", "channel": "ch", "samples": [[2.0, 2.0]]}]
    assert drain(qb) == [{"t": "samples", "channel": "ch", "samples": [[2.0, 2.0]]}]


async def test_unsubscribe_stops_delivery() -> None:
    hub = TelemetryHub()
    q: asyncio.Queue = asyncio.Queue()
    hub.subscribe("ch", q)
    hub.unsubscribe("ch", q)
    hub.publish("ch", [[1.0, 1.0]])
    hub.flush()
    assert drain(q) == []


async def test_drop_clears_ring_and_pending_but_keeps_subscribers() -> None:
    hub = TelemetryHub()
    q: asyncio.Queue = asyncio.Queue()
    hub.subscribe("ch", q)
    hub.publish("ch", [[1.0, 1.0]])
    hub.drop("ch")
    assert hub.history("ch") == []  # ring gone
    hub.flush()
    assert drain(q) == []  # pending batch gone too
    # a re-declared channel starts fresh; live subscribers stay attached
    hub.publish("ch", [[2.0, 2.0]])
    hub.flush()
    assert drain(q) == [{"t": "samples", "channel": "ch", "samples": [[2.0, 2.0]]}]


async def test_drop_unknown_channel_is_a_noop() -> None:
    hub = TelemetryHub()
    hub.drop("nope")  # must not raise
    assert hub.history("nope") == []


async def test_flush_loop_runs_on_interval() -> None:
    hub = TelemetryHub(flush_interval=0.03)
    await hub.start()
    try:
        q: asyncio.Queue = asyncio.Queue()
        hub.subscribe("ch", q)
        hub.publish("ch", [[1.0, 1.0]])
        message = await asyncio.wait_for(q.get(), timeout=1.0)
        assert message["t"] == "samples"
        assert message["samples"] == [[1.0, 1.0]]
    finally:
        await hub.stop()


async def test_publish_coerces_to_float_pairs() -> None:
    hub = TelemetryHub()
    hub.publish("ch", [(1, 2), [3, 4]])
    assert hub.history("ch") == [[1.0, 2.0], [3.0, 4.0]]
