"""Audit LogHub (SPEC 14.1): structured events, session JSONL file, ring.

Every action in the program ends up here as a :class:`LogEvent`. Two sinks:

- **Session file** ``logs/session-<start-ts>.jsonl`` — complete, every event,
  unbounded, flushed promptly after each line (crash-tolerant: a crash leaves
  at most one truncated trailing line; every fully written line is valid JSON).
- **In-memory ring** serving the UI via ``/ws/logs``: 50 000 events or
  :data:`DEFAULT_RING_BYTES` of serialized events, whichever cap is hit
  first — the byte budget keeps a plugin pushing large props at high rate
  from driving the core into hundreds of MB resident (SPEC 14.1).

Subtree filtering: each event's node ancestry is resolved (and cached) **at
emit time**, so ``nodeId`` filters keep matching events of nodes that have
since been removed from the tree.

Robustness of the sinks:

- Every event is serialized exactly once. Text that is not valid Unicode
  (lone UTF-16 surrogates, e.g. raw bus bytes decoded with
  ``errors="surrogateescape"`` that crossed the plugin JSON boundary) is
  escaped with ``backslashreplace`` before it reaches any sink, so one bad
  string can neither disable the session file nor kill a WebSocket sender;
  the original byte value stays visible in the escape.
- When the session file fails (disk full, directory gone, open error) the hub
  keeps serving from the ring, records :attr:`LogHub.sink_error`, appends ONE
  ring-only ``system``/``error`` event and calls :attr:`LogHub.on_sink_failure`
  once — so the outage is visible in every log view and the composition root
  can flag it on the ``system`` node (SPEC 1.5: never silent).

Two laws the hub itself obeys:

- Telemetry samples are NOT routed through the LogHub (they would flood it);
  the ``io`` category is exclusively for vehicle communication frames.
- LogHub activity never generates further log events (no self-amplification):
  the hub only writes to its own sinks and reports internal trouble via the
  stdlib logger. The single sink-failure event above is the one deliberate
  exception, guarded so it fires once per session.
"""

from __future__ import annotations

import asyncio
import json
import logging
import re
import time
from collections import deque
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Literal

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator

from .tree import StateTree

logger = logging.getLogger(__name__)

LogLevel = Literal["trace", "debug", "info", "warn", "error"]
LogCategory = Literal["cmd", "state", "io", "job", "plugin", "system"]

DEFAULT_RING_SIZE = 50_000
#: Ring byte budget (serialized JSON size; Python object overhead is roughly
#: 2-3x on top). Whichever of the count and byte caps is hit first evicts the
#: oldest events. The session file is unaffected (complete, unbounded).
DEFAULT_RING_BYTES = 64 * 1024 * 1024
DEFAULT_FLUSH_INTERVAL = 0.1  # seconds => at most 10 live batches/s (SPEC 14.3)
HISTORY_LIMIT = 500  # recent matching events sent on /ws/logs sub

#: ``raw`` payloads are whole bytes as hex (SPEC 14.1/14.2): the audit trail
#: must be reconstructible byte-exact via ``bytes.fromhex``.
_RAW_HEX_RE = re.compile(r"^(?:[0-9a-fA-F]{2})+$")

#: Default session-file directory: ``<repo>/logs`` (git-ignored).
DEFAULT_LOG_DIR = Path(__file__).resolve().parents[2] / "logs"

#: Cap for the ancestry cache (see :meth:`LogHub._ancestry`). Entries are tiny
#: (id -> tuple of ids) but a very long session with heavy node churn must not
#: grow it without bound; when full, the oldest quarter is evicted.
_ANCESTRY_CACHE_MAX = 200_000

#: A live-event subscriber: receives JSON-ready event dicts, batched.
EventCallback = Callable[[list[dict[str, Any]]], None]


class LogEvent(BaseModel):
    """One structured audit event (SPEC 14.1). Field names match the wire."""

    model_config = ConfigDict(extra="forbid")

    ts: float  # unix epoch milliseconds
    n: int  # monotonic event number (per session)
    level: LogLevel
    source: str = Field(pattern=r"^(core|plugin:[a-z0-9_][a-z0-9_\-]*)$")
    nodeId: str | None = None
    category: LogCategory
    message: str
    data: dict[str, Any] | None = None
    raw: str | None = None  # raw bytes as hex, when applicable

    @field_validator("raw")
    @classmethod
    def _raw_is_whole_byte_hex(cls, value: str | None) -> str | None:
        if value is not None and not _RAW_HEX_RE.fullmatch(value):
            raise ValueError("raw must be a non-empty even-length hex string (whole bytes, no separators)")
        return value


class LogFilter(BaseModel):
    """One /ws/logs subscription filter (SPEC 14.3).

    ``nodeId`` matches the whole subtree (via the ancestry stored at emit
    time). ``search`` is a case-insensitive substring match on ``message``.

    Empty/absent lists mean "no constraint" — ``levels: []`` and
    ``categories: []`` match every level / category exactly like omitting the
    key (this is the wire contract; a client that wants "nothing" must not
    subscribe, see SPEC 14.3).
    """

    model_config = ConfigDict(extra="ignore")

    nodeId: str | None = None
    levels: list[str] | None = None
    categories: list[str] | None = None
    search: str | None = None


@dataclass(frozen=True)
class _StoredEvent:
    """Ring entry: the JSON-ready event dict plus its emit-time ancestry."""

    event: dict[str, Any]
    ancestry: tuple[str, ...]  # the event's node id + all its ancestors
    size: int  # serialized (UTF-8 JSON) size, for the ring byte budget


def sanitize_surrogates(value: Any) -> Any:
    """Escape lone UTF-16 surrogates in every ``str`` inside ``value``.

    Strings that are not UTF-8 encodable (e.g. ``b"\\xff".decode("utf-8",
    "surrogateescape")`` re-materialized from a ``\\udcff`` JSON escape) are
    rewritten with ``backslashreplace`` so the byte value stays visible.
    Dicts (keys too) and lists are walked; anything untouched is returned as
    the very same object, so ``sanitize_surrogates(x) is x`` means "clean".
    """
    if isinstance(value, str):
        try:
            value.encode("utf-8")
        except UnicodeEncodeError:
            return value.encode("utf-8", "backslashreplace").decode("utf-8")
        return value
    if isinstance(value, dict):
        changed = False
        out: dict[Any, Any] = {}
        for key, item in value.items():
            clean_key = sanitize_surrogates(key)
            clean_item = sanitize_surrogates(item)
            if clean_key is not key or clean_item is not item:
                changed = True
            out[clean_key] = clean_item
        return out if changed else value
    if isinstance(value, list):
        items = [sanitize_surrogates(item) for item in value]
        return items if any(a is not b for a, b in zip(items, value)) else value
    return value


class _Subscription:
    __slots__ = ("callback", "filter", "pending")

    def __init__(self, callback: EventCallback, flt: LogFilter) -> None:
        self.callback = callback
        self.filter = flt
        self.pending: list[dict[str, Any]] = []


def compact_json(value: Any, limit: int = 300) -> str:
    """Compact single-line JSON for log messages, truncated to ``limit``."""
    try:
        text = json.dumps(value, ensure_ascii=False, separators=(",", ":"), default=repr)
    except (TypeError, ValueError):
        text = repr(value)
    if len(text) > limit:
        text = text[: limit - 1] + "…"
    return text


class LogHub:
    """In-memory ring + session JSONL file + filtered live subscribers."""

    def __init__(
        self,
        tree: StateTree | None = None,
        *,
        session_dir: Path | str | None = None,
        ring_size: int = DEFAULT_RING_SIZE,
        ring_bytes: int = DEFAULT_RING_BYTES,
        flush_interval: float = DEFAULT_FLUSH_INTERVAL,
    ) -> None:
        self._tree = tree
        self._ring: deque[_StoredEvent] = deque(maxlen=ring_size)
        self._ring_budget = ring_bytes
        self._ring_bytes = 0
        self._flush_interval = flush_interval
        self._counter = 0
        self._subscriptions: dict[EventCallback, _Subscription] = {}
        self._flush_task: asyncio.Task[None] | None = None
        self._ancestry_cache: dict[str, tuple[str, ...]] = {}
        self._file: Any | None = None
        self._file_broken = False
        self._sink_failure_pending = False
        #: Why the session file is unavailable (None while it is healthy).
        #: Set once, at the first failure; the ring keeps serving afterwards.
        self.sink_error: str | None = None
        #: Called once (with :attr:`sink_error`) right after the sink-failure
        #: event has been recorded, from inside ``emit()`` — the composition
        #: root uses it to flag the ``system`` node. Deliberate ``close()`` /
        #: ``stop()`` is not a failure and never triggers it.
        self.on_sink_failure: Callable[[str], None] | None = None
        self.session_path: Path | None = None
        if session_dir is not None:
            self._open_session_file(Path(session_dir))

    # -- session file ------------------------------------------------------

    def _open_session_file(self, session_dir: Path) -> None:
        """Create ``session_dir`` and open a unique ``session-<ts>.jsonl``."""
        try:
            session_dir.mkdir(parents=True, exist_ok=True)
            stamp = time.strftime("%Y%m%d-%H%M%S") + f"-{int(time.time() * 1000) % 1000:03d}"
            path = session_dir / f"session-{stamp}.jsonl"
            suffix = 2
            while path.exists():
                path = session_dir / f"session-{stamp}#{suffix}.jsonl"
                suffix += 1
            # backslashreplace: belt and braces under the emit-time
            # sanitization — an unencodable string can never disable the file.
            self._file = path.open("a", encoding="utf-8", errors="backslashreplace", newline="\n")
            self.session_path = path
        except OSError as exc:
            # The audit file failing must not take down the core (SPEC 1.5);
            # the in-memory ring keeps serving the UI — and the failure is
            # recorded as an event + reported via on_sink_failure (never silent).
            self._file = None
            self._sink_failed(f"could not open session log file in {session_dir}: {type(exc).__name__}: {exc}")

    def _sink_failed(self, detail: str) -> None:
        """First failure of the file sink: remember it, arm the one-shot
        report (drained at the end of the current/next ``emit()``) and close
        the file. Idempotent for later failures."""
        if not self._file_broken:
            self._file_broken = True
            self.sink_error = detail
            self._sink_failure_pending = True
            logger.error("session log file disabled; events continue in the ring only: %s", detail)
        self.close()

    def close(self) -> None:
        """Close the session file (idempotent). Later events stay in-memory."""
        file = self._file
        self._file = None
        if file is not None:
            try:
                file.close()
            except OSError:
                logger.exception("closing session log file failed")

    # -- lifecycle (live-batch flushing) -----------------------------------

    async def start(self) -> None:
        if self._flush_task is None or self._flush_task.done():
            self._flush_task = asyncio.get_running_loop().create_task(
                self._flush_loop(), name="diagcore-loghub-flush"
            )

    async def stop(self) -> None:
        task = self._flush_task
        self._flush_task = None
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        self.flush()
        self.close()

    async def _flush_loop(self) -> None:
        while True:
            await asyncio.sleep(self._flush_interval)
            try:
                self.flush()
            except Exception:
                logger.exception("log flush failed; continuing")

    def flush(self) -> None:
        """Deliver all pending live batches now (also called every 100 ms)."""
        for sub in list(self._subscriptions.values()):
            if not sub.pending:
                continue
            batch, sub.pending = sub.pending, []
            try:
                sub.callback(batch)
            except Exception:
                logger.exception("log subscriber raised; continuing")

    # -- emitting ----------------------------------------------------------

    def emit(
        self,
        level: LogLevel,
        category: LogCategory,
        message: str,
        *,
        source: str = "core",
        node_id: str | None = None,
        data: dict[str, Any] | None = None,
        raw: str | None = None,
    ) -> LogEvent:
        """Record one event in the session file, the ring and live feeds.

        Never raises into the calling seam: an invalid emit (bad ``source``
        id, non-JSON-serializable ``data``) is recorded as a sanitized
        ``system``/``error`` event describing the problem instead (SPEC 1.5:
        neither silent nor fatal — a logging bug must not swallow a command
        result or kill a tree subscriber).

        The event is serialized once; text with lone surrogates is escaped
        (see :func:`sanitize_surrogates`) so every sink and every WebSocket
        transport can carry it.
        """
        self._counter += 1
        ts = time.time() * 1000.0
        try:
            event = LogEvent(
                ts=ts,
                n=self._counter,
                level=level,
                source=source,
                nodeId=node_id,
                category=category,
                message=message,
                data=data,
                raw=raw,
            )
            dumped = event.model_dump(mode="json")
            text = json.dumps(dumped, ensure_ascii=False, default=repr)
        except (ValidationError, ValueError, TypeError) as exc:
            # ValueError also covers pydantic's serialization errors.
            logger.warning("invalid log event from %r/%r dropped: %s", source, category, exc)
            reason = " ".join(str(exc).split())
            original = {
                "level": str(level),
                "source": str(source),
                "nodeId": str(node_id) if node_id is not None else None,
                "category": str(category),
                "message": str(message),
                "data": compact_json(data, 1000) if data is not None else None,
                "raw": str(raw) if raw is not None else None,
            }
            node_id = node_id if isinstance(node_id, str) else None
            event = LogEvent(
                ts=ts,
                n=self._counter,
                level="error",
                source="core",
                nodeId=node_id,
                category="system",
                message=(
                    f"invalid log event from {original['source']}/{original['category']} dropped:"
                    f" {type(exc).__name__}: {reason[:300]} original={compact_json(original, 400)}"
                ),
                data={"error": reason[:2000], "original": original},
            )
            dumped = event.model_dump(mode="json")
            text = json.dumps(dumped, ensure_ascii=False, default=repr)
        try:
            size = len(text.encode("utf-8"))
        except UnicodeEncodeError:
            # Lone surrogates in some string: escape them (byte values stay
            # visible) so the file, the ring and every ws transport can carry
            # the event. Rare path — the common case pays only the encode.
            dumped = sanitize_surrogates(dumped)
            event = LogEvent.model_validate(dumped)
            text = json.dumps(dumped, ensure_ascii=False, default=repr)
            size = len(text.encode("utf-8"))
        stored = _StoredEvent(event=dumped, ancestry=self._ancestry(node_id), size=size)
        self._write_line(text)
        self._append(stored)
        for sub in self._subscriptions.values():
            if _matches(stored, sub.filter):
                sub.pending.append(dumped)
        if self._sink_failure_pending:
            # Drained AFTER this event is fully recorded (n-order stays
            # intact) and outside _write_line (no re-entrant file access).
            self._sink_failure_pending = False
            self._report_sink_failure()
        return event

    def _append(self, stored: _StoredEvent) -> None:
        """Ring append honouring both caps: the deque's count limit and the
        serialized byte budget (oldest evicted first; the newest event is
        always kept, even if it alone exceeds the budget)."""
        ring = self._ring
        if ring.maxlen is not None and len(ring) == ring.maxlen:
            self._ring_bytes -= ring[0].size  # the deque evicts it on append
        ring.append(stored)
        self._ring_bytes += stored.size
        while self._ring_bytes > self._ring_budget and len(ring) > 1:
            self._ring_bytes -= ring.popleft().size

    def _report_sink_failure(self) -> None:
        """One-shot: record the file-sink outage as a ring-only event (the
        file is already closed, so this nested emit cannot recurse) and tell
        the composition root via ``on_sink_failure``."""
        error = self.sink_error or "unknown error"
        session_file = str(self.session_path) if self.session_path is not None else None
        self.emit(
            "error",
            "system",
            f"audit session file disabled: {error} — events continue in the in-memory ring only"
            f" (sessionFile={session_file})",
            node_id="system",
            data={"sessionFile": session_file, "error": error},
        )
        callback = self.on_sink_failure
        if callback is not None:
            try:
                callback(error)
            except Exception:
                logger.exception("on_sink_failure callback raised; continuing")

    def _write_line(self, text: str) -> None:
        file = self._file
        if file is None:
            return
        try:
            file.write(text + "\n")
            file.flush()  # promptly flushed: the audit trail survives a crash
        except UnicodeEncodeError:
            # Unreachable with errors="backslashreplace" + emit-time
            # sanitization; if it ever happens, lose this one line — never
            # the whole audit file.
            logger.warning("session log line not encodable; line skipped")
        except (OSError, ValueError) as exc:
            self._sink_failed(f"session log write failed: {type(exc).__name__}: {exc}")

    def _ancestry(self, node_id: str | None) -> tuple[str, ...]:
        """The node's id plus all ancestor ids, resolved at emit time.

        Resolved against the live tree when the node exists; the result is
        cached so events referencing a node *after* its removal (e.g. the
        ``remove`` patch itself, or a late job event) still carry the full
        ancestry and keep matching subtree filters.
        """
        if node_id is None:
            return ()
        tree = self._tree
        if tree is not None and node_id in tree:
            chain: list[str] = []
            current: str | None = node_id
            while current is not None:
                node = tree.get(current)
                if node is None:
                    break
                chain.append(current)
                current = node.parentId
            ancestry = tuple(chain)
            if len(self._ancestry_cache) >= _ANCESTRY_CACHE_MAX:
                for stale in list(self._ancestry_cache)[: _ANCESTRY_CACHE_MAX // 4]:
                    del self._ancestry_cache[stale]
            self._ancestry_cache[node_id] = ancestry
            return ancestry
        return self._ancestry_cache.get(node_id, (node_id,))

    # -- reading / subscribing ---------------------------------------------

    @property
    def event_count(self) -> int:
        """Total events emitted this session (``n`` of the newest event)."""
        return self._counter

    @property
    def ring_bytes(self) -> int:
        """Serialized size of everything currently held in the ring."""
        return self._ring_bytes

    def history(self, flt: LogFilter | None = None, limit: int = HISTORY_LIMIT) -> list[dict[str, Any]]:
        """The last ``limit`` ring events matching ``flt`` (JSON-ready dicts,
        oldest first)."""
        flt = flt or LogFilter()
        result: list[dict[str, Any]] = []
        for stored in reversed(self._ring):
            if _matches(stored, flt):
                result.append(stored.event)
                if len(result) >= limit:
                    break
        result.reverse()
        return result

    def subscribe(self, callback: EventCallback, flt: LogFilter | None = None) -> list[dict[str, Any]]:
        """Register ``callback`` for live events matching ``flt``; returns the
        matching history so the caller can enqueue it first (no event emitted
        between the history snapshot and the subscription — both happen in
        this one synchronous call)."""
        flt = flt or LogFilter()
        self._subscriptions[callback] = _Subscription(callback, flt)
        return self.history(flt)

    def unsubscribe(self, callback: EventCallback) -> None:
        self._subscriptions.pop(callback, None)


def _matches(stored: _StoredEvent, flt: LogFilter) -> bool:
    event = stored.event
    if flt.levels and event["level"] not in flt.levels:
        return False
    if flt.categories and event["category"] not in flt.categories:
        return False
    if flt.nodeId is not None and flt.nodeId not in stored.ancestry:
        return False
    if flt.search and flt.search.lower() not in str(event["message"]).lower():
        return False
    return True
