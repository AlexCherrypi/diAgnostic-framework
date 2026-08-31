"""Telemetry hub (SPEC 5).

High-rate numeric samples bypass the state tree. The hub keeps a ring buffer
per channel (default 10 000 samples) and flushes batched samples to
subscribers every 100 ms. Subscribers are plain asyncio queues receiving
JSON-ready ``{"t": "samples", ...}`` dicts; the ``history`` message is
returned from ``subscribe()`` so the caller can enqueue it first.
"""

from __future__ import annotations

import asyncio
import logging
from collections import deque
from typing import Any, Iterable, Sequence

logger = logging.getLogger(__name__)

DEFAULT_RING_SIZE = 10_000
DEFAULT_FLUSH_INTERVAL = 0.1  # seconds


class TelemetryHub:
    def __init__(
        self,
        *,
        ring_size: int = DEFAULT_RING_SIZE,
        flush_interval: float = DEFAULT_FLUSH_INTERVAL,
    ) -> None:
        self._ring_size = ring_size
        self._flush_interval = flush_interval
        self._rings: dict[str, deque[list[float]]] = {}
        self._pending: dict[str, list[list[float]]] = {}
        self._subscribers: dict[str, set[asyncio.Queue[dict[str, Any]]]] = {}
        self._flush_task: asyncio.Task[None] | None = None

    # -- lifecycle ---------------------------------------------------------

    async def start(self) -> None:
        if self._flush_task is None or self._flush_task.done():
            self._flush_task = asyncio.get_running_loop().create_task(
                self._flush_loop(), name="diagcore-telemetry-flush"
            )

    async def stop(self) -> None:
        task = self._flush_task
        self._flush_task = None
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self.flush()

    # -- publishing --------------------------------------------------------

    def publish(self, channel: str, samples: Iterable[Sequence[float]]) -> None:
        """Append ``[tMs, value]`` pairs to the ring buffer and, if anyone is
        subscribed, to the pending batch for the next flush."""
        pairs = [[float(t), float(v)] for t, v in samples]
        if not pairs:
            return
        ring = self._rings.setdefault(channel, deque(maxlen=self._ring_size))
        ring.extend(pairs)
        if self._subscribers.get(channel):
            self._pending.setdefault(channel, []).extend(pairs)

    def history(self, channel: str) -> list[list[float]]:
        return [list(pair) for pair in self._rings.get(channel, ())]

    def drop(self, channel: str) -> None:
        """Forget a channel's buffered data (ring buffer + pending batch).

        Called when the corresponding ``telemetry.channel`` node is removed,
        so dead channel ids do not pin ring buffers for the life of the
        process and a re-declared channel starts with fresh history instead
        of replaying pre-removal samples. Live subscriber queues stay
        attached: they simply receive whatever a re-declared channel
        publishes next.
        """
        self._rings.pop(channel, None)
        self._pending.pop(channel, None)

    # -- subscriptions -----------------------------------------------------

    def subscribe(self, channel: str, queue: asyncio.Queue[dict[str, Any]]) -> list[list[float]]:
        """Register ``queue`` for ``channel``; returns the ring buffer content
        for the ``history`` message.

        Any samples still pending for existing subscribers are flushed to them
        first, so the new subscriber never receives a batch that overlaps its
        history.
        """
        pending = self._pending.pop(channel, None)
        if pending:
            self._deliver(channel, pending)
        self._subscribers.setdefault(channel, set()).add(queue)
        return self.history(channel)

    def unsubscribe(self, channel: str, queue: asyncio.Queue[dict[str, Any]]) -> None:
        subs = self._subscribers.get(channel)
        if subs is None:
            return
        subs.discard(queue)
        if not subs:
            del self._subscribers[channel]
            self._pending.pop(channel, None)

    # -- flushing ----------------------------------------------------------

    def flush(self) -> None:
        """Deliver all pending batches now (also called by the 100 ms loop)."""
        if not self._pending:
            return
        pending, self._pending = self._pending, {}
        for channel, samples in pending.items():
            if samples:
                self._deliver(channel, samples)

    async def _flush_loop(self) -> None:
        while True:
            await asyncio.sleep(self._flush_interval)
            try:
                self.flush()
            except Exception:
                logger.exception("telemetry flush failed; continuing")

    def _deliver(self, channel: str, samples: list[list[float]]) -> None:
        subs = self._subscribers.get(channel)
        if not subs:
            return
        message = {"t": "samples", "channel": channel, "samples": samples}
        for queue in list(subs):
            try:
                queue.put_nowait(message)
            except asyncio.QueueFull:
                logger.warning("telemetry subscriber queue full; dropping batch for %s", channel)
