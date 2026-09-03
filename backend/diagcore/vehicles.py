"""Generic vehicle session (SPEC 15): profile store + session manager.

The core owns the vehicle *concepts* and their *persistence*; a plugin with
capability ``vehicle`` supplies the manufacturer-specific behavior through
the SPEC 15.4 RPCs (``vehicle.detect`` / ``vehicle.attach`` /
``vehicle.detach`` and the ``vehicle.profile`` notification).

Two pieces live here:

- :class:`VehicleStore` — one JSON file per profile in
  ``<data dir>/vehicles/<id>.json`` (SPEC 15.1): atomic writes (temp file +
  ``os.replace``), the upsert key (``pluginId`` + VIN when known, else
  ``pluginId`` + canonical JSON of ``identifiers``), load errors collected
  instead of raised, every create/update/forget logged (category
  ``vehicle``) with the full profile in ``data``.
- :class:`VehicleSessionManager` — the two well-known nodes ``vehicle``
  (type ``vehicle.session``, SPEC 15.2) and ``vehicles`` (type
  ``vehicle.library``, SPEC 15.3) with their command handling and the
  session state machine::

      none --detect--> detecting --vehicle.profile--> connected
      none --select--> attaching --job done / profile--> connected
      detecting/attaching --job error|cancelled|vanished--> none   (subtree removed, grant revoked, logged)
      connected --job error|cancelled (profile came early)--> none (same: "handled like a failed detect")
      detecting --job done / no job, no profile within grace--> none
      any active --plugin died--> lost                    (error; evidence stays)
      lost --plugin restarted--> none                     (old subtree removed)
      any active --disconnect--> none                     (cancel job, vehicle.detach, subtree removed;
                                                           a refused/timed-out detach is a warn, still none)
      detecting/attaching, plugin not answering --disconnect--> none  (the wait is abandoned at once)

  The detection/attach job is the *plugin's* job declared under the session
  node (SPEC 15.2 children); the manager watches that job node's status via
  a tree subscription — deferred to the loop, never mutating the tree from
  inside a subscriber (that would broadcast seq N+1 before N). The job stays
  watched until it is terminal, also after ``vehicle.profile`` already made
  the session ``connected``.

  Every transition runs as its own task that the calling command merely
  awaits (shielded): ``/ws/state`` cancels in-flight commands when their
  client disconnects, and a session left half-transitioned would assert a
  state that is no longer true (SPEC 1.5).

Data dir: default ``<repo>/data`` (resolved like ``logs/``), override with
``DIAGCORE_DATA_DIR``.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import secrets
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Callable, Coroutine, Literal, Mapping

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from .loghub import LogHub, LogLevel, compact_json
from .model import (
    Action,
    ActionParam,
    ActionParamOption,
    CmdResult,
    ErrorInfo,
    Node,
    apply_param_defaults,
    param_applies,
    validate_action_params,
)
from .telemetry import TelemetryHub
from .tree import StateTree

if TYPE_CHECKING:
    from .plugins.host import PluginHost

logger = logging.getLogger(__name__)

#: Default data directory: ``<repo>/data`` (git-ignored), like ``logs/``.
DEFAULT_DATA_DIR = Path(__file__).resolve().parents[2] / "data"
DATA_DIR_ENV = "DIAGCORE_DATA_DIR"

#: Well-known node ids (SPEC 3.2).
SESSION_NODE_ID = "vehicle"
LIBRARY_NODE_ID = "vehicles"
SESSION_NODE_TYPE = "vehicle.session"
LIBRARY_NODE_TYPE = "vehicle.library"
PROFILE_NODE_TYPE = "vehicle.profile"
ECU_NODE_TYPE = "ecu"

SessionState = Literal["none", "detecting", "attaching", "connected", "lost"]

_ACTIVE_JOB_STATUSES = frozenset({"init", "running"})
_ACTIVE_SESSION_STATES = frozenset({"detecting", "attaching", "connected", "lost"})


def resolve_data_dir(explicit: Path | str | None = None, env: Mapping[str, str] = os.environ) -> Path:
    """An explicit path beats ``DIAGCORE_DATA_DIR`` beats ``<repo>/data``."""
    if explicit is not None:
        return Path(explicit)
    raw = env.get(DATA_DIR_ENV)
    if raw and raw.strip():
        return Path(raw.strip())
    return DEFAULT_DATA_DIR


def now_ms() -> float:
    return time.time() * 1000.0


# --------------------------------------------------------------------------
# Profile models (SPEC 15.1 / 15.4)
# --------------------------------------------------------------------------


class VehicleEcu(BaseModel):
    """One ECU of a profile; ``id`` is stable within the profile."""

    model_config = ConfigDict(extra="forbid")

    id: str = Field(min_length=1)
    name: str
    address: str
    protocol: str
    props: dict[str, Any] = Field(default_factory=dict)


def _normalize_vin(value: str | None) -> str | None:
    if value is None:
        return None
    text = value.strip()
    return text or None


def _unique_ecu_ids(ecus: list[VehicleEcu]) -> list[VehicleEcu]:
    seen: set[str] = set()
    for ecu in ecus:
        if ecu.id in seen:
            raise ValueError(f"duplicate ecu id {ecu.id!r}")
        seen.add(ecu.id)
    return ecus


#: Profile fields the core owns (SPEC 15.1); a plugin never sets them. A
#: plugin that re-emits the full profile the core handed it in
#: ``vehicle.attach`` (more identifiers learned, SPEC 15.4) carries them
#: along — they are ignored, never a reason to drop the notification.
CORE_OWNED_PROFILE_KEYS = frozenset({"id", "firstSeen", "lastSeen"})


class VehicleProfileInput(BaseModel):
    """What a plugin sends in ``vehicle.profile`` (SPEC 15.4): the 15.1
    profile without ``id`` / ``firstSeen`` / ``lastSeen`` — the core fills
    those (and ignores them when a plugin sends them anyway). ``pluginId``
    is optional; the host stamps the sending plugin."""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1)
    vin: str | None = None
    identifiers: dict[str, Any] = Field(default_factory=dict)
    pluginId: str | None = None
    ecus: list[VehicleEcu] = Field(default_factory=list)
    notes: str = ""

    _vin = field_validator("vin")(_normalize_vin)
    _ecus = field_validator("ecus")(_unique_ecu_ids)

    @model_validator(mode="before")
    @classmethod
    def _ignore_core_owned_keys(cls, data: Any) -> Any:
        if isinstance(data, dict) and any(key in data for key in CORE_OWNED_PROFILE_KEYS):
            return {key: value for key, value in data.items() if key not in CORE_OWNED_PROFILE_KEYS}
        return data


class VehicleProfile(BaseModel):
    """A persisted vehicle profile (SPEC 15.1). Field names are the file
    and wire format."""

    model_config = ConfigDict(extra="forbid")

    id: str = Field(pattern=r"^veh_[a-z0-9]+$")
    name: str = Field(min_length=1)
    vin: str | None = None
    identifiers: dict[str, Any] = Field(default_factory=dict)
    pluginId: str = Field(min_length=1)
    firstSeen: float
    lastSeen: float
    ecus: list[VehicleEcu] = Field(default_factory=list)
    notes: str = ""

    _vin = field_validator("vin")(_normalize_vin)
    _ecus = field_validator("ecus")(_unique_ecu_ids)

    @property
    def key(self) -> str:
        return profile_key(self.pluginId, self.vin, self.identifiers)

    def summary_props(self) -> dict[str, Any]:
        """Library node props (SPEC 15.3): the profile without full ``ecus``,
        plus ``ecuCount`` and a compact ``ecus`` summary."""
        data = self.model_dump(mode="json")
        data["ecuCount"] = len(self.ecus)
        data["ecus"] = [{"id": ecu.id, "name": ecu.name, "address": ecu.address} for ecu in self.ecus]
        return data


def canonical_identifiers(identifiers: Mapping[str, Any]) -> str:
    """Canonical JSON (sorted keys, no whitespace) — the identifiers half of
    the upsert key."""
    return json.dumps(identifiers, sort_keys=True, separators=(",", ":"), ensure_ascii=False, default=str)


def profile_key(plugin_id: str, vin: str | None, identifiers: Mapping[str, Any]) -> str:
    """Upsert key (SPEC 15.1): ``pluginId`` + VIN when a VIN is known, else
    ``pluginId`` + canonical JSON of ``identifiers``."""
    vin = _normalize_vin(vin)
    if vin:
        return f"{plugin_id}|vin:{vin}"
    return f"{plugin_id}|ids:{canonical_identifiers(identifiers)}"


# --------------------------------------------------------------------------
# Store
# --------------------------------------------------------------------------


class VehicleStore:
    """JSON-file-per-profile store with atomic writes (SPEC 15.1).

    Never raises for I/O trouble: load problems are collected in
    :attr:`load_errors` (good files are still loaded), a failed write is
    recorded in :attr:`last_write_error` (the profile stays in memory so the
    live session keeps working) — the session manager turns both into the
    library node's ``error`` status with the ``reload`` action.
    """

    def __init__(
        self,
        data_dir: Path | str,
        *,
        loghub: LogHub | None = None,
        node_id: str = LIBRARY_NODE_ID,
    ) -> None:
        self.data_dir = Path(data_dir)
        self.dir = self.data_dir / "vehicles"
        self._loghub = loghub
        self._node_id = node_id
        self._profiles: dict[str, VehicleProfile] = {}
        #: Per-file problems of the last :meth:`load` (``"<file>: <why>"``).
        self.load_errors: list[str] = []
        #: Why the last write failed (None while writes succeed).
        self.last_write_error: str | None = None

    # -- logging -------------------------------------------------------------

    def _log(self, level: LogLevel, message: str, *, data: dict[str, Any] | None = None) -> None:
        if self._loghub is not None:
            self._loghub.emit(level, "vehicle", message, node_id=self._node_id, data=data)

    # -- paths ---------------------------------------------------------------

    def path_for(self, profile_id: str) -> Path:
        return self.dir / f"{profile_id}.json"

    # -- loading -------------------------------------------------------------

    def load(self) -> None:
        """(Re)read every ``<id>.json``; problems land in :attr:`load_errors`."""
        self._profiles = {}
        self.load_errors = []
        self.last_write_error = None
        if not self.dir.is_dir():
            self._log("info", f"vehicle store: no profile directory yet at {self.dir}", data={"dir": str(self.dir)})
            return
        try:
            paths = sorted(self.dir.glob("*.json"))
        except OSError as exc:
            self.load_errors.append(f"{self.dir}: {type(exc).__name__}: {exc}")
            self._log("error", f"vehicle store: cannot list {self.dir}: {exc}", data={"dir": str(self.dir)})
            return
        for path in paths:
            try:
                raw = json.loads(path.read_text(encoding="utf-8"))
                profile = VehicleProfile.model_validate(raw)
                if profile.id != path.stem:
                    raise ValueError(f"profile id {profile.id!r} does not match the file name")
            except (OSError, ValueError, ValidationError) as exc:  # JSONDecodeError is a ValueError
                reason = " ".join(str(exc).split())
                self.load_errors.append(f"{path.name}: {type(exc).__name__}: {reason[:300]}")
                self._log(
                    "error",
                    f"vehicle store: profile file {path.name} unusable: {type(exc).__name__}: {reason[:300]}",
                    data={"file": str(path), "error": reason[:2000]},
                )
                continue
            self._profiles[profile.id] = profile
        self._log(
            "info",
            f"vehicle store loaded {len(self._profiles)} profile(s) from {self.dir}"
            f" ({len(self.load_errors)} unusable file(s))",
            data={
                "dir": str(self.dir),
                "profiles": [p.id for p in self.list()],
                "errors": list(self.load_errors),
            },
        )

    # -- reading -------------------------------------------------------------

    def list(self) -> list[VehicleProfile]:
        """All profiles, ``lastSeen`` desc (SPEC 15.3 ordering)."""
        return sorted(self._profiles.values(), key=lambda p: (-p.lastSeen, p.name, p.id))

    def get(self, profile_id: str | None) -> VehicleProfile | None:
        if profile_id is None:
            return None
        return self._profiles.get(profile_id)

    def __len__(self) -> int:
        return len(self._profiles)

    def find(self, plugin_id: str, vin: str | None, identifiers: Mapping[str, Any]) -> VehicleProfile | None:
        """The profile an incoming (pluginId, vin, identifiers) upserts into.

        Exact key match first (SPEC 15.1). One extension (the M1.5
        resolution recorded in SPEC 15.1): a profile stored *before* its VIN
        was known (VIN-less, same non-empty identifiers) is matched by an
        incoming profile that now carries a VIN, so learning the VIN later
        does not fork the vehicle into two profiles — which is why a
        plugin's ``identifiers`` must be vehicle-unique. The upsert marks
        such a merge in its audit event (``vinLearned`` / ``previousVin``).
        """
        key = profile_key(plugin_id, vin, identifiers)
        for profile in self.list():
            if profile.key == key:
                return profile
        vin = _normalize_vin(vin)
        if vin and identifiers:
            canonical = canonical_identifiers(identifiers)
            for profile in self.list():
                if (
                    profile.pluginId == plugin_id
                    and profile.vin is None
                    and canonical_identifiers(profile.identifiers) == canonical
                ):
                    return profile
        return None

    # -- writing -------------------------------------------------------------

    def _new_id(self) -> str:
        while True:
            profile_id = f"veh_{secrets.token_hex(6)}"
            if profile_id not in self._profiles and not self.path_for(profile_id).exists():
                return profile_id

    def upsert(self, plugin_id: str, incoming: VehicleProfileInput, *, now: float | None = None) -> tuple[VehicleProfile, bool]:
        """Create or update the profile for ``incoming`` (SPEC 15.1 key).

        On update the user-owned fields ``name`` / ``notes`` are kept (the
        display name is user-renamable), ``identifiers`` are merged (new keys
        win), a newly learned VIN is taken over and ``ecus`` are replaced by
        the fresh scan. Returns ``(profile, created)``.
        """
        stamp = now_ms() if now is None else now
        existing = self.find(plugin_id, incoming.vin, incoming.identifiers)
        vin_learned = existing is not None and existing.vin is None and bool(incoming.vin)
        if existing is None:
            profile = VehicleProfile(
                id=self._new_id(),
                name=incoming.name,
                vin=incoming.vin,
                identifiers=dict(incoming.identifiers),
                pluginId=plugin_id,
                firstSeen=stamp,
                lastSeen=stamp,
                ecus=list(incoming.ecus),
                notes=incoming.notes,
            )
            created = True
        else:
            profile = existing.model_copy(
                update={
                    "vin": incoming.vin or existing.vin,
                    "identifiers": {**existing.identifiers, **incoming.identifiers},
                    "ecus": list(incoming.ecus),
                    "lastSeen": stamp,
                },
                deep=True,
            )
            created = False
        self._profiles[profile.id] = profile
        written = self._write(profile)
        verb = "created" if created else "updated"
        note = " (VIN learned: the profile was stored without one)" if vin_learned else ""
        self._log(
            "info",
            f"vehicle profile {profile.id} {verb}{note}: name={profile.name!r} vin={profile.vin} plugin={profile.pluginId}"
            f" ecus={len(profile.ecus)} identifiers={compact_json(profile.identifiers, 120)}"
            f" file={self.path_for(profile.id)}{'' if written else ' (WRITE FAILED)'}",
            data={
                "profileId": profile.id,
                "created": created,
                "written": written,
                "vinLearned": vin_learned,
                "previousVin": existing.vin if existing is not None else None,
                "file": str(self.path_for(profile.id)),
                "profile": profile.model_dump(mode="json"),
            },
        )
        return profile, created

    def rename(self, profile_id: str, name: str) -> VehicleProfile:
        existing = self._profiles[profile_id]  # KeyError for unknown ids
        profile = existing.model_copy(update={"name": name}, deep=True)
        self._profiles[profile_id] = profile
        written = self._write(profile)
        self._log(
            "info",
            f"vehicle profile {profile_id} renamed: {existing.name!r} -> {name!r}{'' if written else ' (WRITE FAILED)'}",
            data={
                "profileId": profile_id,
                "previousName": existing.name,
                "written": written,
                "profile": profile.model_dump(mode="json"),
            },
        )
        return profile

    def touch(self, profile_id: str, *, now: float | None = None) -> VehicleProfile:
        """Bump ``lastSeen`` (a known vehicle was attached again)."""
        existing = self._profiles[profile_id]
        profile = existing.model_copy(update={"lastSeen": now_ms() if now is None else now}, deep=True)
        self._profiles[profile_id] = profile
        written = self._write(profile)
        self._log(
            "info",
            f"vehicle profile {profile_id} seen again: name={profile.name!r} lastSeen={profile.lastSeen:.0f}"
            f"{'' if written else ' (WRITE FAILED)'}",
            data={"profileId": profile_id, "written": written, "profile": profile.model_dump(mode="json")},
        )
        return profile

    def forget(self, profile_id: str) -> VehicleProfile:
        """Delete the profile and its file. Raises ``OSError`` if the file
        cannot be removed (the profile then stays)."""
        profile = self._profiles[profile_id]
        path = self.path_for(profile_id)
        try:
            path.unlink(missing_ok=True)
        except OSError as exc:
            self._log(
                "error",
                f"vehicle profile {profile_id} forget failed: cannot delete {path}: {exc}",
                data={"profileId": profile_id, "file": str(path), "error": str(exc)},
            )
            raise
        self._profiles.pop(profile_id, None)
        self._log(
            "info",
            f"vehicle profile {profile_id} forgotten: name={profile.name!r} vin={profile.vin} plugin={profile.pluginId}"
            f" file={path} deleted",
            data={"profileId": profile_id, "file": str(path), "profile": profile.model_dump(mode="json")},
        )
        return profile

    def _write(self, profile: VehicleProfile) -> bool:
        """Atomic write: temp file in the same directory, fsync, ``os.replace``.
        Returns False (and records :attr:`last_write_error`) on failure."""
        path = self.path_for(profile.id)
        text = json.dumps(profile.model_dump(mode="json"), indent=2, ensure_ascii=False, sort_keys=True) + "\n"
        tmp = path.with_name(f".{path.name}.{secrets.token_hex(4)}.tmp")
        try:
            self.dir.mkdir(parents=True, exist_ok=True)
            with tmp.open("w", encoding="utf-8", newline="\n") as handle:
                handle.write(text)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp, path)
        except OSError as exc:
            try:
                tmp.unlink(missing_ok=True)
            except OSError:
                pass
            detail = f"{path}: {type(exc).__name__}: {exc}"
            self.last_write_error = detail
            logger.error("vehicle profile write failed: %s", detail)
            self._log(
                "error",
                f"vehicle profile {profile.id} write failed: {detail}",
                data={"profileId": profile.id, "file": str(path), "error": str(exc)},
            )
            return False
        self.last_write_error = None
        return True


# --------------------------------------------------------------------------
# Session manager
# --------------------------------------------------------------------------


def _fail(message: str, detail: str | None = None) -> CmdResult:
    return CmdResult(ok=False, error=ErrorInfo(message=message, detail=detail))


class VehicleSessionManager:
    """Owns the ``vehicle`` and ``vehicles`` nodes and the session state
    machine (SPEC 15.2-15.4). See the module docstring for the transitions.
    """

    def __init__(
        self,
        tree: StateTree,
        store: VehicleStore,
        hosts: Mapping[str, "PluginHost"],
        *,
        hub: TelemetryHub | None = None,
        loghub: LogHub | None = None,
        detach_timeout: float = 5.0,
        profile_grace: float = 1.0,
        start_timeout: float = 10.0,
    ) -> None:
        self._tree = tree
        self.store = store
        #: Live mapping ``plugin node id -> host`` (the composition root's dict).
        self._hosts = hosts
        self._hub = hub
        self._loghub = loghub
        #: How long ``vehicle.detach`` (and the job ``cancel`` of a teardown)
        #: may take before the core proceeds without the plugin's answer
        #: (SPEC 15.4: 5 s ⇒ ``warn``).
        self.detach_timeout = detach_timeout
        #: Grace period after a detection job ends ``done`` (or a detect was
        #: answered without a job) for the plugin's ``vehicle.profile`` to
        #: arrive before the detect counts as failed.
        self.profile_grace = profile_grace
        #: How long the plugin may take to *answer* ``vehicle.detect`` /
        #: ``vehicle.attach`` (the scan itself belongs in the job, SPEC 15.4).
        #: A hung plugin must not hold the session lock for the generic 30 s
        #: command timeout; a ``disconnect`` abandons the wait right away.
        self.start_timeout = start_timeout

        self._lock = asyncio.Lock()
        self._state: SessionState = "none"
        self._host: PluginHost | None = None
        self._profile_id: str | None = None
        self._name: str | None = None
        self._vin: str | None = None
        self._job_id: str | None = None
        #: "detection" / "attach": how the watched job is called in messages.
        self._job_verb = "detection"
        self._since: float | None = None
        self._error: dict[str, Any] | None = None
        #: Bumped on every session transition; deferred callbacks carry the
        #: epoch they were scheduled for and are ignored when it is stale.
        self._epoch = 0
        self._subscribed = False
        self._refresh_pending = False
        self._grace_handle: asyncio.TimerHandle | None = None
        #: In-flight transition tasks (see :meth:`_run_transition`).
        self._transitions: set[asyncio.Task[CmdResult]] = set()
        #: Set while a start RPC is awaited; ``disconnect`` sets the event to
        #: abandon the wait (see :meth:`_start_rpc`).
        self._preempt: asyncio.Event | None = None
        self._preempt_verb = ""

    # -- public state ----------------------------------------------------------

    @property
    def state(self) -> SessionState:
        return self._state

    @property
    def job_id(self) -> str | None:
        return self._job_id

    @property
    def profile_id(self) -> str | None:
        return self._profile_id

    @property
    def plugin_id(self) -> str | None:
        return self._host.manifest.id if self._host is not None else None

    # -- logging ---------------------------------------------------------------

    def _log(
        self,
        level: LogLevel,
        message: str,
        *,
        node_id: str = SESSION_NODE_ID,
        data: dict[str, Any] | None = None,
    ) -> None:
        if self._loghub is not None:
            self._loghub.emit(level, "vehicle", message, node_id=node_id, data=data)

    def _session_data(self, **extra: Any) -> dict[str, Any]:
        return {
            "state": self._state,
            "pluginId": self.plugin_id,
            "profileId": self._profile_id,
            "jobNodeId": self._job_id,
            **extra,
        }

    # -- bootstrap -------------------------------------------------------------

    def create_nodes(self) -> None:
        """Add the two well-known nodes under ``root`` (idempotent)."""
        if self._tree.get(SESSION_NODE_ID) is None:
            self._tree.add(
                Node(
                    id=SESSION_NODE_ID,
                    type=SESSION_NODE_TYPE,
                    name="Vehicle",
                    status="idle",
                    owner="core",
                    parentId="root",
                    props=self._session_props(),
                    actions=self._session_actions(),
                )
            )
        if self._tree.get(LIBRARY_NODE_ID) is None:
            self._tree.add(
                Node(
                    id=LIBRARY_NODE_ID,
                    type=LIBRARY_NODE_TYPE,
                    name="Known vehicles",
                    status="idle",
                    owner="core",
                    parentId="root",
                    props={"dir": str(self.store.dir), "profileCount": 0},
                )
            )

    def load_library(self) -> None:
        """Load the store and mirror it into the library node."""
        self.store.load()
        self._refresh_library()

    def attach_host(self, host: "PluginHost") -> None:
        """Wire a plugin host's vehicle seams to this manager."""
        host.on_plugin_died = self._on_plugin_died
        host.on_plugin_respawned = self._on_plugin_respawned
        host.on_vehicle_profile = self._on_vehicle_profile
        self._schedule_refresh()

    def start(self) -> None:
        """Subscribe to the tree (job watching, plugin availability, ECU count)."""
        if not self._subscribed:
            self._tree.subscribe(self._on_patch)
            self._subscribed = True
        self._refresh_session_node()

    async def shutdown(self, timeout: float = 2.0) -> None:
        """Let in-flight transitions finish (bounded), cancel the rest —
        called by the composition root after the plugin hosts stopped, so
        pending plugin RPCs have already failed fast."""
        self._cancel_grace()
        tasks = [task for task in self._transitions if not task.done()]
        if not tasks:
            return
        _done, pending = await asyncio.wait(tasks, timeout=timeout)
        for task in pending:
            task.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)

    # -- tree subscription (never mutates the tree synchronously) ----------------

    def _on_patch(self, seq: int, op: dict[str, Any]) -> None:
        kind = op["op"]
        if kind == "update":
            node_id = op["id"]
            if "status" not in op["changes"]:
                return
            if node_id == self._job_id:
                self._defer(self._check_job, self._epoch)
            elif node_id in self._hosts:
                self._schedule_refresh()
        elif kind == "remove":
            # The tree emits only the removed root's id: compare against the
            # tree, so a job nested under a removed container is caught too.
            # The core's own teardown paths bump the epoch before removing
            # the subtree, so their deferred check is a no-op.
            if self._job_id is not None and self._tree.get(self._job_id) is None:
                self._defer(self._check_job, self._epoch)
            self._schedule_refresh()
        elif kind == "add":
            self._schedule_refresh()  # ecuCount / plugin availability may have changed

    def _defer(self, fn: Callable[..., None], *args: Any) -> None:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            fn(*args)
        else:
            loop.call_soon(fn, *args)

    def _schedule_refresh(self) -> None:
        if self._refresh_pending:
            return
        self._refresh_pending = True
        self._defer(self._run_refresh)

    def _run_refresh(self) -> None:
        self._refresh_pending = False
        try:
            self._refresh_session_node()
        except Exception:  # SPEC 12: never let a bookkeeping bug escape a loop callback
            logger.exception("vehicle session refresh failed")

    # -- session node --------------------------------------------------------------

    def _count_ecus(self) -> int:
        count = 0
        for node_id in self._tree.subtree_ids(SESSION_NODE_ID):
            node = self._tree.get(node_id)
            if node is not None and node.type == ECU_NODE_TYPE:
                count += 1
        return count

    def _session_props(self) -> dict[str, Any]:
        return {
            "state": self._state,
            "profileId": self._profile_id,
            "name": self._name,
            "vin": self._vin,
            "pluginId": self.plugin_id,
            "ecuCount": self._count_ecus() if self._tree.get(SESSION_NODE_ID) is not None else 0,
            "since": self._since,
        }

    def _session_status(self) -> str:
        if self._state in ("detecting", "attaching"):
            return "running"
        if self._state == "lost":
            return "error"
        return "idle"

    def _capable_hosts(self) -> list["PluginHost"]:
        """Running plugins with capability ``vehicle``, by id."""
        hosts = [host for host in self._hosts.values() if host.manifest.is_vehicle_plugin and host.healthy]
        return sorted(hosts, key=lambda host: host.manifest.id)

    def _detect_action(self) -> Action:
        """The static ``detect`` form (SPEC 15.2 resolution): with several
        capable plugins the ``plugin`` select plus every plugin's
        ``detectParams`` tagged ``when: {plugin: <id>}`` — the generic form
        shows, validates and sends only the selected plugin's params, so
        same-named params never clash and a required param of another
        plugin never blocks the way in."""
        capable = self._capable_hosts()
        params: list[ActionParam] = []
        if len(capable) > 1:
            params.append(
                ActionParam(
                    name="plugin",
                    label="Vehicle plugin",
                    type="select",
                    required=True,
                    options=[ActionParamOption(value=host.manifest.id, label=host.manifest.name) for host in capable],
                )
            )
            for host in capable:
                for param in host.manifest.detect_params:
                    condition = {**(param.when or {}), "plugin": host.manifest.id}
                    params.append(param.model_copy(update={"when": condition}))
        elif len(capable) == 1:
            params.extend(capable[0].manifest.detect_params)
        return Action(name="detect", label="Detect vehicle", params=params)

    def _session_actions(self) -> list[Action]:
        actions: list[Action] = []
        if self._state in ("none", "connected", "lost"):
            actions.append(self._detect_action())
        if self._state in ("detecting", "attaching", "connected", "lost"):
            actions.append(Action(name="disconnect", label="Disconnect"))
        return actions

    def _refresh_session_node(self) -> None:
        node = self._tree.get(SESSION_NODE_ID)
        if node is None:
            return
        desired: dict[str, Any] = {
            "name": f"Vehicle: {self._name}" if self._name else "Vehicle",
            "status": self._session_status(),
            "error": self._error,
            "props": self._session_props(),
            "actions": [action.model_dump(mode="json") for action in self._session_actions()],
        }
        current = node.model_dump(mode="json")
        changes = {key: value for key, value in desired.items() if current.get(key) != value}
        if changes:
            self._tree.update(SESSION_NODE_ID, changes)

    # -- library node ----------------------------------------------------------------

    @staticmethod
    def profile_node_id(profile_id: str) -> str:
        return f"vehicle:{profile_id}"

    def _profile_actions(self, profile: VehicleProfile) -> list[dict[str, Any]]:
        actions = [
            Action(name="select", label="Connect"),
            Action(
                name="rename",
                label="Rename",
                params=[ActionParam(name="name", label="Name", type="string", required=True, default=profile.name)],
            ),
            Action(
                name="forget",
                label="Forget",
                confirm=f"Forget vehicle {profile.name!r} (VIN {profile.vin or 'unknown'})? Its stored profile file is deleted.",
            ),
        ]
        return [action.model_dump(mode="json") for action in actions]

    def _refresh_library(self) -> None:
        """Mirror the store into ``vehicles``: one ``vehicle.profile`` child per
        profile, ``lastSeen`` desc, props/actions up to date; then the
        library's own status (load / write errors ⇒ ``error`` + ``reload``)."""
        library = self._tree.get(LIBRARY_NODE_ID)
        if library is None:
            return
        profiles = self.store.list()
        desired_ids = [self.profile_node_id(profile.id) for profile in profiles]
        for child_id in list(library.children):
            if child_id not in desired_ids:
                try:
                    self._tree.remove(child_id)
                except ValueError:
                    pass
        for index, profile in enumerate(profiles):
            node_id = self.profile_node_id(profile.id)
            props = profile.summary_props()
            actions = self._profile_actions(profile)
            node = self._tree.get(node_id)
            children = self._tree.get(LIBRARY_NODE_ID).children  # type: ignore[union-attr]
            if node is not None and children[index] != node_id:
                self._tree.remove(node_id)  # wrong position: re-insert at the right one
                node = None
            if node is None:
                self._tree.add(
                    Node(
                        id=node_id,
                        type=PROFILE_NODE_TYPE,
                        name=profile.name,
                        status="idle",
                        owner="core",
                        parentId=LIBRARY_NODE_ID,
                        props=props,
                        actions=actions,
                    ),
                    index=index,
                )
                continue
            current = node.model_dump(mode="json")
            changes: dict[str, Any] = {}
            if current["name"] != profile.name:
                changes["name"] = profile.name
            if current["props"] != props:
                changes["props"] = props
            if current["actions"] != actions:
                changes["actions"] = actions
            if changes:
                self._tree.update(node_id, changes)
        self._refresh_library_status()

    def _refresh_library_status(self) -> None:
        library = self._tree.get(LIBRARY_NODE_ID)
        if library is None:
            return
        problems = list(self.store.load_errors)
        if self.store.last_write_error is not None:
            problems.append(f"write failed: {self.store.last_write_error}")
        desired: dict[str, Any] = {
            "props": {"dir": str(self.store.dir), "profileCount": len(self.store)},
        }
        if problems:
            desired["status"] = "error"
            desired["error"] = {
                "message": "vehicle profile store error",
                "detail": "\n".join(problems),
            }
            desired["actions"] = [Action(name="reload", label="Reload profiles").model_dump(mode="json")]
        else:
            desired["status"] = "idle"
            desired["error"] = None
            desired["actions"] = []
        current = library.model_dump(mode="json")
        changes = {key: value for key, value in desired.items() if current.get(key) != value}
        if changes:
            self._tree.update(LIBRARY_NODE_ID, changes)

    # -- commands ----------------------------------------------------------------------

    async def handle_command(self, node: Node, action: str, params: dict[str, Any]) -> CmdResult:
        """Bus handler for ``vehicle.session`` / ``vehicle.library`` /
        ``vehicle.profile`` nodes."""
        if node.type == SESSION_NODE_TYPE:
            if action == "detect":
                return await self._run_transition(self._cmd_detect(params), "detect")
            if action == "disconnect":
                return await self._run_transition(self._cmd_disconnect(), "disconnect")
        elif node.type == LIBRARY_NODE_TYPE:
            if action == "reload":
                return self._cmd_reload()
        elif node.type == PROFILE_NODE_TYPE:
            profile_id = node.props.get("id")
            if not isinstance(profile_id, str):
                return _fail(f"profile node {node.id!r} carries no profile id")
            if action == "select":
                return await self._run_transition(self._cmd_select(profile_id), "select")
            if action == "rename":
                return self._cmd_rename(profile_id, params)
            if action == "forget":
                return self._cmd_forget(profile_id)
        return _fail(f"unknown action {action!r} for target {node.id!r}")

    async def _run_transition(self, coro: Coroutine[Any, Any, CmdResult], label: str) -> CmdResult:
        """Run a session transition to completion even when the *calling*
        command task is cancelled: ``/ws/state`` cancels every in-flight
        command when its client disconnects (page reload, stalled client),
        and a session left half-transitioned would assert a state that is no
        longer true (SPEC 1.5). The transition task owns the lock and the
        plugin RPCs; the caller only waits for it."""
        loop = asyncio.get_running_loop()
        task = loop.create_task(coro, name=f"diagcore-vehicle-{label}")
        self._transitions.add(task)
        task.add_done_callback(self._transitions.discard)
        task.add_done_callback(self._reap_transition)
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            if not task.done():
                self._log(
                    "warn",
                    f"vehicle command {label!r} abandoned by its caller (client gone?);"
                    " the session transition continues to completion",
                    data=self._session_data(command=label),
                )
            raise

    @staticmethod
    def _reap_transition(task: asyncio.Task[CmdResult]) -> None:
        # The bus catches handler exceptions, but a caller that was cancelled
        # no longer awaits this task — never let a failure go unseen (SPEC 1.5).
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            logger.error("vehicle session transition %s failed: %r", task.get_name(), exc)

    async def _cmd_detect(self, params: dict[str, Any]) -> CmdResult:
        async with self._lock:
            if self._state in ("detecting", "attaching"):
                return _fail(f"vehicle session is already {self._state}")
            capable = self._capable_hosts()
            requested = params.get("plugin")
            if requested is not None and not isinstance(requested, str):
                return _fail("param 'plugin' must be a plugin id string")
            host: PluginHost | None = None
            if requested:
                host = next((h for h in capable if h.manifest.id == requested), None)
                if host is None:
                    configured = next((h for h in self._hosts.values() if h.manifest.id == requested), None)
                    if configured is None:
                        return _fail(f"unknown plugin {requested!r}")
                    if not configured.manifest.is_vehicle_plugin:
                        return _fail(f"plugin {requested!r} has no 'vehicle' capability")
                    return _fail(f"plugin {requested!r} is not running")
            elif len(capable) == 1:
                host = capable[0]
            elif not capable:
                return _fail("no vehicle-capable plugin is running")
            else:
                return _fail(
                    "param 'plugin' is required: several vehicle-capable plugins are running"
                    f" ({', '.join(h.manifest.id for h in capable)})"
                )
            # SPEC 15.2 resolution: the selected plugin receives exactly its
            # own declared params (defaults filled in) — never the `plugin`
            # select or what another plugin's half of the union form carried.
            declared = host.manifest.detect_params
            declared_names = {param.name for param in declared}
            detect_params = apply_param_defaults(
                declared, {key: value for key, value in params.items() if key in declared_names}
            )
            problems = validate_action_params(declared, detect_params)
            if problems:
                return _fail("invalid detect params: " + "; ".join(problems))
            in_effect = {param.name for param in declared if param_applies(param, detect_params)}
            detect_params = {key: value for key, value in detect_params.items() if key in in_effect}
            if self._state != "none":
                await self._teardown(f"detect requested while {self._state}")
            self._begin("detecting", host)
            self._log(
                "info",
                f"vehicle session none -> detecting: plugin={host.manifest.id} params={compact_json(detect_params)}",
                data=self._session_data(params=detect_params),
            )
            result = await self._start_rpc(
                host, "detect", host.vehicle_detect(SESSION_NODE_ID, detect_params, timeout=self.start_timeout)
            )
            if result is None:
                return await self._abandon_start("detect")
            return self._after_start_rpc(host, result, "detect")

    async def _cmd_select(self, profile_id: str) -> CmdResult:
        async with self._lock:
            profile = self.store.get(profile_id)
            if profile is None:
                return _fail(f"unknown vehicle profile {profile_id!r}")
            if self._state in ("detecting", "attaching"):
                return _fail(f"vehicle session is busy ({self._state}); wait or disconnect first")
            host = self._hosts.get(f"plugin:{profile.pluginId}")
            if host is None:
                return _fail(f"plugin {profile.pluginId!r} of profile {profile_id!r} is not configured")
            if not host.manifest.is_vehicle_plugin:
                return _fail(f"plugin {profile.pluginId!r} has no 'vehicle' capability")
            if not host.healthy:
                return _fail(f"plugin {profile.pluginId!r} is not running")
            if self._state != "none":
                await self._teardown(f"select {profile_id} requested while {self._state}")
            self._begin("attaching", host, profile=profile)
            self._log(
                "info",
                f"vehicle session none -> attaching: profile={profile.id} name={profile.name!r} vin={profile.vin}"
                f" plugin={host.manifest.id} ecus={len(profile.ecus)}",
                data=self._session_data(profile=profile.model_dump(mode="json")),
            )
            result = await self._start_rpc(
                host,
                "attach",
                host.vehicle_attach(SESSION_NODE_ID, profile.model_dump(mode="json"), timeout=self.start_timeout),
            )
            if result is None:
                return await self._abandon_start("attach")
            return self._after_start_rpc(host, result, "attach")

    async def _start_rpc(
        self, host: "PluginHost", verb: str, rpc_coro: Coroutine[Any, Any, CmdResult]
    ) -> CmdResult | None:
        """Await the plugin's ``vehicle.detect`` / ``vehicle.attach`` answer
        (bounded by :attr:`start_timeout`), pre-emptible: a ``disconnect``
        issued meanwhile sets :attr:`_preempt` and the wait is abandoned at
        once (``None``; the RPC is cancelled, a late answer is discarded by
        the host) — a hung plugin never makes Disconnect wait for the RPC
        timeout. Cancellation of the transition itself (core shutdown)
        unwinds the pending session before propagating."""
        loop = asyncio.get_running_loop()
        rpc = loop.create_task(rpc_coro)
        preempt = self._preempt = asyncio.Event()
        self._preempt_verb = verb
        waiter = loop.create_task(preempt.wait())
        try:
            await asyncio.wait({rpc, waiter}, return_when=asyncio.FIRST_COMPLETED)
        except asyncio.CancelledError:
            rpc.cancel()
            waiter.cancel()
            await asyncio.gather(rpc, waiter, return_exceptions=True)
            if self._host is host and self._state in ("detecting", "attaching"):
                self._unwind(f"vehicle.{verb} cancelled while awaiting the plugin (core shutting down?)", level="warn")
            raise
        finally:
            self._preempt = None
            self._preempt_verb = ""
        if rpc.done():
            waiter.cancel()
            return rpc.result()
        rpc.cancel()
        await asyncio.gather(rpc, return_exceptions=True)
        self._log(
            "warn",
            f"vehicle.{verb} by plugin {host.manifest.id} abandoned: disconnect requested while the"
            " plugin had not answered yet",
            data=self._session_data(),
        )
        return None

    async def _abandon_start(self, verb: str) -> CmdResult:
        """A pre-empted start: end the session like a ``disconnect`` (the
        plugin may have started something — ``vehicle.detach`` under its
        cap tells it to let go), answer the start command ``ok:false``."""
        await self._teardown(f"disconnect requested while awaiting vehicle.{verb}")
        return _fail(f"vehicle.{verb} abandoned: disconnect requested while awaiting the plugin")

    def _after_start_rpc(self, host: "PluginHost", result: CmdResult, verb: str) -> CmdResult:
        """Common tail of detect/select after the plugin answered."""
        expected = "detecting" if verb == "detect" else "attaching"
        if not result.ok:
            message = result.error.message if result.error else f"vehicle.{verb} failed"
            if self._state == expected and self._host is host:
                self._unwind(f"vehicle.{verb} rejected by plugin {host.manifest.id}: {message}", level="error")
            return _fail(f"vehicle.{verb} failed: {message}", detail=result.error.detail if result.error else None)
        # `connected` is fine too: the plugin's vehicle.profile may precede
        # its answer — the job it names is still watched until terminal.
        if self._state not in (expected, "connected") or self._host is not host:
            return _fail(f"vehicle session changed during vehicle.{verb}: state is now {self._state!r}")
        job_id = (result.data or {}).get("jobNodeId")
        if isinstance(job_id, str) and job_id:
            if not self._tree.is_within(job_id, SESSION_NODE_ID):
                self._unwind(
                    f"plugin {host.manifest.id} answered vehicle.{verb} with jobNodeId {job_id!r},"
                    " which is not a node under the session",
                    level="error",
                )
                return _fail(f"vehicle.{verb} failed: plugin returned an unknown job node {job_id!r}")
            self._job_id = job_id
            self._log(
                "info",
                f"vehicle session {self._state}: watching job {job_id} of plugin {host.manifest.id}",
                data=self._session_data(),
            )
            reason = self._check_job(self._epoch)  # it may already be terminal
            if reason is not None:
                return _fail(f"vehicle.{verb} failed: {reason}")
        elif self._state == "connected":
            pass  # profile already in, nothing to watch
        elif verb == "attach":
            # No verification job: the plugin re-declared the ECUs synchronously.
            profile = self.store.get(self._profile_id)
            if profile is None:
                self._unwind("profile vanished during attach", level="error")
                return _fail("vehicle.attach failed: profile vanished")
            self._set_connected(self._touch(profile), reason="vehicle.attach without verification job")
        else:
            # detect answered without a job (SPEC 15.4 `jobNodeId?`): the
            # profile must still arrive — same grace as after a job's `done`,
            # so a plugin that answers ok and then fails silently cannot leave
            # the session `detecting` forever.
            self._start_grace(self._epoch)
        return CmdResult(
            ok=True,
            data={"jobNodeId": job_id if isinstance(job_id, str) else None, "pluginId": host.manifest.id, "state": self._state},
        )

    async def _cmd_disconnect(self) -> CmdResult:
        preempted = self._preempt_start()
        async with self._lock:
            if self._state not in _ACTIVE_SESSION_STATES:
                if preempted is not None:
                    # The pre-empted start already ended the session (SPEC 15.4
                    # teardown, detach under its cap): this disconnect did its job.
                    return CmdResult(ok=True, data={"previousState": preempted, "state": self._state, "abandoned": True})
                return _fail(f"no vehicle session to disconnect (state {self._state!r})")
            previous = self._state
            await self._teardown("disconnect requested")
            return CmdResult(ok=True, data={"previousState": previous, "state": self._state})

    def _preempt_start(self) -> SessionState | None:
        """Wake a start RPC that is still waiting for the plugin so this
        disconnect does not queue behind it for the whole RPC timeout.
        Returns the pre-empted state, or None when nothing was waiting."""
        event = self._preempt
        if event is None or event.is_set():
            return None
        event.set()
        self._log(
            "info",
            f"vehicle session {self._state}: disconnect pre-empts the pending vehicle.{self._preempt_verb}"
            f" of plugin {self.plugin_id}",
            data=self._session_data(),
        )
        return self._state

    def _cmd_reload(self) -> CmdResult:
        self.store.load()
        self._refresh_library()
        if self._profile_id is not None and self.store.get(self._profile_id) is None:
            self._log(
                "warn",
                f"vehicle session profile {self._profile_id} is gone after reload; session keeps running without it",
                data=self._session_data(),
            )
        return CmdResult(ok=True, data={"profiles": len(self.store), "errors": list(self.store.load_errors)})

    def _cmd_rename(self, profile_id: str, params: dict[str, Any]) -> CmdResult:
        name = params.get("name")
        if not isinstance(name, str) or not name.strip():
            return _fail("param 'name' must be a non-empty string")
        name = name.strip()
        if self.store.get(profile_id) is None:
            return _fail(f"unknown vehicle profile {profile_id!r}")
        profile = self.store.rename(profile_id, name)
        self._refresh_library()
        if self._profile_id == profile_id:
            self._name = profile.name
            self._refresh_session_node()
        if self.store.last_write_error is not None:
            return _fail(f"profile renamed in memory but not saved: {self.store.last_write_error}")
        return CmdResult(ok=True, data={"profileId": profile_id, "name": profile.name})

    def _cmd_forget(self, profile_id: str) -> CmdResult:
        if self.store.get(profile_id) is None:
            return _fail(f"unknown vehicle profile {profile_id!r}")
        if self._profile_id == profile_id and self._state != "none":
            return _fail(f"profile {profile_id!r} belongs to the active vehicle session ({self._state}); disconnect first")
        try:
            self.store.forget(profile_id)
        except OSError as exc:
            return _fail(f"could not delete profile {profile_id!r}: {exc}")
        self._refresh_library()
        return CmdResult(ok=True, data={"profileId": profile_id})

    # -- transitions ---------------------------------------------------------------------

    def _begin(self, state: SessionState, host: "PluginHost", *, profile: VehicleProfile | None = None) -> None:
        self._epoch += 1
        self._cancel_grace()
        self._state = state
        self._host = host
        self._job_id = None
        self._job_verb = "detection" if state == "detecting" else "attach"
        self._error = None
        self._since = None
        if profile is not None:
            self._profile_id, self._name, self._vin = profile.id, profile.name, profile.vin
        else:
            self._profile_id = self._name = self._vin = None
        self._refresh_session_node()

    def _reset_to_none(self) -> None:
        self._state = "none"
        self._host = None
        self._profile_id = self._name = self._vin = None
        self._job_id = None
        self._since = None
        self._error = None
        self._refresh_session_node()

    def _touch(self, profile: VehicleProfile) -> VehicleProfile:
        touched = self.store.touch(profile.id)
        self._refresh_library()
        return touched

    def _set_connected(self, profile: VehicleProfile, *, reason: str) -> None:
        # `_job_id` is deliberately kept: a detect/attach job that is still
        # running when the profile arrives stays watched (SPEC 15.4 — its
        # error/cancel still ends the session), see `_check_job`.
        previous = self._state
        self._cancel_grace()
        self._state = "connected"
        self._error = None
        self._profile_id, self._name, self._vin = profile.id, profile.name, profile.vin
        if self._since is None:
            self._since = now_ms()
        self._refresh_session_node()
        self._log(
            "info",
            f"vehicle session {previous} -> connected: profile={profile.id} name={profile.name!r} vin={profile.vin}"
            f" plugin={self.plugin_id} ecus={self._count_ecus()} ({reason})",
            data=self._session_data(reason=reason, profile=profile.model_dump(mode="json")),
        )

    def _unwind(self, reason: str, *, level: LogLevel = "warn") -> None:
        """Failed/cancelled detect or attach (SPEC 15.4): back to ``none``,
        subtree removed, grant revoked, reason logged. Synchronous."""
        previous = self._state
        host = self._host
        self._epoch += 1
        self._cancel_grace()
        self._log(level, f"vehicle session {previous} -> none: {reason}", data=self._session_data(reason=reason))
        if host is not None:
            host.revoke(SESSION_NODE_ID, reason=reason)
        self._remove_session_children()
        self._reset_to_none()

    async def _teardown(self, reason: str) -> None:
        """Orderly end of the session: cancel a running job, ``vehicle.detach``
        (both bounded by :attr:`detach_timeout`), remove the subtree, ``none``.
        The bookkeeping tail runs whatever happens to the plugin RPCs — a
        refused or timed-out detach is a ``warn`` and the session still ends
        in ``none`` (SPEC 15.2/15.4), and a cancelled teardown (core
        shutdown) never leaves the tree asserting a session that is over."""
        previous = self._state
        host = self._host
        job_id = self._job_id
        self._epoch += 1
        self._cancel_grace()
        self._log("info", f"vehicle session {previous} ending: {reason}", data=self._session_data(reason=reason))
        try:
            if job_id is not None and host is not None and host.healthy:
                job = self._tree.get(job_id)
                if job is not None and job.status in _ACTIVE_JOB_STATUSES:
                    try:
                        result = await asyncio.wait_for(host.handle_command(job, "cancel", {}), self.detach_timeout)
                    except asyncio.TimeoutError:
                        self._log(
                            "warn",
                            f"vehicle session: cancel of job {job_id} not answered by plugin {host.manifest.id}"
                            f" within {self.detach_timeout:g} s; proceeding with vehicle.detach",
                            data=self._session_data(timeoutSeconds=self.detach_timeout),
                        )
                    else:
                        outcome = "ok" if result.ok else f"failed: {result.error.message if result.error else 'unknown'}"
                        self._log(
                            "info",
                            f"vehicle session: cancel of job {job_id} {outcome}",
                            data=self._session_data(cancelResult=result.model_dump(mode="json")),
                        )
            if host is not None:
                if host.healthy:
                    result = await host.vehicle_detach(SESSION_NODE_ID, timeout=self.detach_timeout)
                    outcome = "ok" if result.ok else f"failed: {result.error.message if result.error else 'unknown'}"
                    self._log(
                        "info" if result.ok else "warn",
                        f"vehicle session: vehicle.detach by plugin {host.manifest.id} {outcome}",
                        data=self._session_data(detachResult=result.model_dump(mode="json")),
                    )
                else:
                    host.revoke(SESSION_NODE_ID, reason=f"{reason} (plugin not running, vehicle.detach skipped)")
                    self._log(
                        "warn",
                        f"vehicle session: plugin {host.manifest.id} is not running; vehicle.detach skipped",
                        data=self._session_data(),
                    )
        except asyncio.CancelledError:
            self._log(
                "warn",
                f"vehicle session {previous} teardown interrupted ({reason}); ending the session without"
                " waiting for the plugin",
                data=self._session_data(reason=reason),
            )
            raise
        finally:
            if host is not None:
                host.revoke(SESSION_NODE_ID, reason=f"{reason} (session ended)")  # idempotent safety net
            self._remove_session_children()
            self._reset_to_none()
            self._log("info", f"vehicle session {previous} -> none: {reason}", data=self._session_data(reason=reason))

    def _remove_session_children(self) -> None:
        session = self._tree.get(SESSION_NODE_ID)
        if session is None:
            return
        host = self._host  # every node under the session belongs to the granted plugin
        for child_id in list(session.children):
            removed_ids = self._tree.subtree_ids(child_id)
            try:
                self._tree.remove(child_id)
            except ValueError:
                continue
            if host is not None:
                host.forget_nodes(removed_ids)  # telemetry rings + job milestone tracking
            elif self._hub is not None:
                for removed_id in removed_ids:
                    self._hub.drop(removed_id)

    # -- job watching --------------------------------------------------------------------

    def _check_job(self, epoch: int) -> str | None:
        """Act on the watched job's status. Returns the reason when this
        unwound the session (the job failed, was cancelled or vanished), else
        None — a ``done`` job connects (attach) or starts the profile grace
        (detect), and while already ``connected`` it simply stops being
        watched."""
        if epoch != self._epoch or self._state not in ("detecting", "attaching", "connected") or self._job_id is None:
            return None
        job_id = self._job_id
        verb = self._job_verb
        job = self._tree.get(job_id)
        if job is None:
            reason = f"{verb} job {job_id} vanished from the tree"
            self._unwind(reason, level="error")
            return reason
        if job.status == "error":
            message = job.error.message if job.error else "unknown error"
            reason = f"{verb} job {job_id} {job.name!r} failed: {message}"
            self._unwind(reason, level="error")
            return reason
        if job.status == "cancelled":
            reason = f"{verb} job {job_id} {job.name!r} cancelled"
            self._unwind(reason, level="warn")
            return reason
        if job.status == "done":
            if self._state == "connected":
                self._job_id = None
                self._log(
                    "info",
                    f"vehicle session connected: {verb} job {job_id} {job.name!r} done; no longer watched",
                    data=self._session_data(jobNodeId=job_id),
                )
            elif self._state == "attaching":
                profile = self.store.get(self._profile_id)
                if profile is None:
                    reason = f"profile {self._profile_id} vanished during attach"
                    self._unwind(reason, level="error")
                    return reason
                self._set_connected(self._touch(profile), reason=f"attach job {job_id} done")
            else:
                # The profile normally precedes the job's `done`; give a slow
                # plugin a grace period before calling the detect failed.
                self._start_grace(epoch)
        return None

    def _start_grace(self, epoch: int) -> None:
        self._cancel_grace()
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            self._grace_expired(epoch)
        else:
            self._grace_handle = loop.call_later(self.profile_grace, self._grace_expired, epoch)

    def _grace_expired(self, epoch: int) -> None:
        self._grace_handle = None
        if epoch != self._epoch or self._state != "detecting":
            return
        if self._job_id is None:
            reason = (
                f"vehicle.detect by plugin {self.plugin_id} answered without a job and no vehicle.profile"
                f" arrived within {self.profile_grace:g} s"
            )
        else:
            reason = f"detection job {self._job_id} finished without a vehicle.profile from plugin {self.plugin_id}"
        self._unwind(reason, level="error")

    def _cancel_grace(self) -> None:
        handle = self._grace_handle
        self._grace_handle = None
        if handle is not None:
            handle.cancel()

    # -- plugin seams (called by the host) ------------------------------------------------

    def _on_vehicle_profile(self, host: "PluginHost", session_node_id: str, incoming: VehicleProfileInput) -> None:
        if session_node_id != SESSION_NODE_ID or host is not self._host or self._state not in (
            "detecting",
            "attaching",
            "connected",
        ):
            self._log(
                "warn",
                f"vehicle.profile from plugin {host.manifest.id} for {session_node_id!r} ignored:"
                f" session state={self._state} plugin={self.plugin_id}",
                data=self._session_data(profile=incoming.model_dump(mode="json")),
            )
            return
        profile, created = self.store.upsert(host.manifest.id, incoming)
        self._refresh_library()
        self._set_connected(profile, reason=f"vehicle.profile ({'created' if created else 'updated'})")

    def _on_plugin_died(self, host: "PluginHost", reason: str) -> None:
        if self._host is not host or self._state == "none":
            self._schedule_refresh()
            return
        previous = self._state
        self._epoch += 1
        self._cancel_grace()
        self._state = "lost"
        self._job_id = None
        self._error = {
            "message": "plugin process died",
            "detail": f"plugin {host.manifest.id} died while the session was {previous}: {reason}",
        }
        self._refresh_session_node()
        self._log(
            "error",
            f"vehicle session {previous} -> lost: plugin {host.manifest.id} died ({reason})",
            data=self._session_data(reason=reason, previousState=previous),
        )

    def _on_plugin_respawned(self, host: "PluginHost") -> None:
        if self._host is host and self._state != "none":
            previous = self._state
            self._epoch += 1
            self._cancel_grace()
            self._remove_session_children()
            self._reset_to_none()
            self._log(
                "warn",
                f"vehicle session {previous} -> none: plugin {host.manifest.id} restarted;"
                " the old session subtree was removed",
                data=self._session_data(previousState=previous),
            )
        self._schedule_refresh()
