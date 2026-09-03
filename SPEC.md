# diAgnostic-framework — Skeleton Specification (M0)

A manufacturer-independent platform skeleton for building automotive diagnostic
tools. This document is the **binding contract** between backend, frontend and
plugins. Anything ambiguous here must be resolved by updating this file first.

Scope of M0: everything visual, organizational and backend-generic.
**Nothing vehicle-specific.** Vehicle/VCI/database specifics arrive later as
plugins (e.g. the existing `oleodiag` PSA library becomes a plugin bundle).

---

## 1. Core principles

1. **The backend is the single source of truth.** The frontend is a pure
   renderer: it mirrors backend state and sends *intents* (commands). It never
   computes domain state itself.
2. **Every backend object is visible.** All meaningful backend state lives in
   one **state tree** of typed nodes. If it isn't in the tree, the frontend
   cannot show it — so put it in the tree.
3. **Every node type has a frontend component** (or falls back to a generic
   renderer). Every component must render **all** node statuses, including
   `error` and `gone`. A component that crashes while rendering must degrade to
   an inline error card, never take down the app.
4. **Fault isolation.** Plugins run out-of-process. A crashing plugin marks its
   subtree `error`; the core and the UI keep running.
5. **No silent failure.** No bare `except:`. Every failure ends up as an
   `error` status + structured error info on some node.

## 2. Process model

```
┌────────────────────┐  WebSocket (state + telemetry)   ┌──────────────────┐
│  Frontend (web)    │◄────────────────────────────────►│  Core (Python    │
│  React + TS, pure  │      commands →                  │  3.14, asyncio,  │
│  renderer          │                                  │  FastAPI)        │
└────────────────────┘                                  └───┬──────────────┘
   runs in browser (dev) or pywebview window (desktop)      │ JSON-RPC over stdio
                                                 ┌──────────┴───────────┐
                                                 │ Plugin processes     │
                                                 │ (any Python, even    │
                                                 │  32-bit for old DLLs)│
                                                 └──────────────────────┘
```

- **Core**: Python ≥3.13 (64-bit, developed on 3.14), asyncio, FastAPI + uvicorn, pydantic v2.
  Owns the state tree, job engine, telemetry hub, plugin supervisor.
- **Plugins**: separate OS processes, spawned and supervised by the core,
  speaking JSON-RPC 2.0 (line-delimited) over stdio. stderr = plugin log.
- **Frontend**: Vite + React + TypeScript (strict), zustand mirror store,
  uPlot charts. Dev: browser at Vite dev server. Prod: built `dist/` served by
  the core; optional pywebview shell opens a native window.

Ports: core listens on `127.0.0.1:8765` (override for a second instance, e.g.
tests next to the running desktop app: `python -m diagcore --port <n>` or env
`DIAGCORE_PORT`; the flag wins). Vite dev server `5173`, proxying `/ws` and
`/api` (with `ws: true`) to the core.

## 3. State tree

### 3.1 Node model

```jsonc
{
  "id": "string, unique, opaque",
  "type": "dotted.lowercase, e.g. 'plugin', 'device.sim', 'telemetry.channel', 'job', 'job.step'",
  "name": "human-readable label",
  "status": "init | idle | running | done | error | cancelled | gone",
  "error": { "message": "string", "detail": "string?" },   // or null
  "progress": 0.0,          // 0..1 or null
  "props": { },             // type-specific data, JSON object
  "actions": [              // commands the UI may offer generically
    { "name": "selftest", "label": "Run self-test", "params": [] },
    { "name": "probe", "label": "Probe ECU", "confirm": "string?",  // optional confirmation text
      "params": [ /* see 3.1.1 */ ] }
  ],
  "parentId": "string|null (null only for root)",
  "children": ["ordered", "child", "ids"],
  "owner": "core | plugin:<pluginId>"
}
```

#### 3.1.1 Action parameters (M1.5)

Each entry of `actions[].params` describes one input the UI collects before
sending the command:

```jsonc
{ "name": "address", "label": "ECU address",
  "type": "string | number | boolean | select",
  "required": true, "default": null, "placeholder": "0x10", "help": "string?",
  "min": 0, "max": 255,                                        // number only
  "options": [ { "value": "uds", "label": "UDS on CAN" } ],    // select only
  "when": { "protocol": "uds" } }   // optional: in effect only while that select holds this value
```

Rules: the UI sends `params` as `{ name: value }` using the option **value**,
never its label — so the audit trail (§14) records the raw value/id behind
every dropdown choice. A non-empty `params` list opens the generic action
form (§8); an action with `confirm` asks before sending; an action without
params sends `{}`.

`when` (optional, M1.5) makes a param **conditional**: every key names a
`select` param of the same action, and the param is shown, validated
(`required` included) and sent only while each named select holds the given
option value — otherwise it is neither collected nor on the wire, and the
core skips it in its own validation and default-filling. A `when` that
references an unknown or non-select param, or a value that is not one of
its options, is a declaration error (dropped and logged like any malformed
node). Several params may share a name when their `when` conditions differ;
a reference then resolves to the same-named select whose own `when` is
compatible with the referencing condition. This is how the session's
`detect` form folds several plugins' params into one static form (§15.2).

Status semantics:

| status      | meaning                                                       |
|-------------|---------------------------------------------------------------|
| `init`      | created, not ready yet (e.g. job queued, plugin starting)     |
| `idle`      | ready, nothing running                                        |
| `running`   | actively doing something (may carry `progress`)               |
| `done`      | finished successfully (terminal)                              |
| `error`     | failed / crashed; `error` field is set (terminal until reset) |
| `cancelled` | stopped by user request (terminal)                            |
| `gone`      | no longer available; UI shows it dimmed or removes it         |

### 3.2 Well-known nodes (created by the core at startup)

```
root (type "root")
├── system  (type "system")   props: { version, startedAt, pythonVersion, auditFile }
├── plugins (type "plugins")  children: one "plugin" node per configured plugin
├── jobs    (type "jobs")     children: "job" nodes, newest first
├── vehicle  (type "vehicle.session")  the active vehicle — M1.5, §15.2
└── vehicles (type "vehicle.library")  known vehicle profiles — M1.5, §15.3
```

`auditFile` (M0.5) is the path of the session audit file (§14.1), or `null`
if it could not be opened. If the audit file fails at any time (open error,
disk full, directory removed) the `system` node goes `status: "error"` with
error `{message: "audit session file disabled", detail}` and gains
`props.auditFileError` — the trail's outage is itself visible in the UI
(§1.5); `/api/health` then reports `ok: false` plus `auditFile` /
`auditFileError`.

Plugin nodes have id `plugin:<pluginId>` and own their subtree: everything a
plugin declares is attached under its plugin node (devices, channels, …).
Jobs are owned by the core job engine and live under `jobs`.

> **M0 resolution (recorded per the rule at the top of this file):** §6
> restricts plugin mutations to the plugin's own subtree and offers no
> job-creation RPC, while §9 requires the demo plugin to run jobs. Jobs
> *triggered by a plugin* therefore live as `job` / `job.step` nodes *inside
> that plugin's subtree*, declared via ordinary §6 notifications and
> mirroring the §7 semantics (steps, progress, sorted named locks, `cancel`).
> The `jobs` node holds core-created jobs only. A core-side job RPC that
> moves plugin-triggered jobs under `jobs` (and unifies the lock table, see
> §7) is deferred to a later milestone.
> **M0.5 addendum:** plugin-side job ids are unique per plugin *process
> incarnation* (`<pluginNodeId>:job:<incarnation>.<n>`), never reused after a
> crash/restart — the log ring (§14.1) keeps events across restarts, and a
> reused id would merge two unrelated jobs in a per-node feed (§14.4). Ids
> stay opaque (§3.1); nothing may parse them.

### 3.3 Storage & patches

The tree is stored flat: `{ rootId: string, nodes: { [id]: Node } }`.
Every mutation increments a global sequence number `seq` (monotonic int) and
is broadcast as one of three ops:

```jsonc
{ "op": "add",    "node": { ...full Node incl. parentId } }
  // insert node; append id to parent's children (or at "index" if given)
{ "op": "update", "id": "x", "changes": { ...partial Node } }
  // top-level keys in changes REPLACE the node's keys ("props" replaces all props)
{ "op": "merge_props", "id": "x", "props": { ...partial } }
  // shallow-merge into node.props (cheap single-value updates)
{ "op": "remove", "id": "x" }
  // remove node and all descendants; remove id from parent's children
```

## 4. State WebSocket — `/ws/state`

Server → client:

```jsonc
{ "t": "hello", "seq": 42, "snapshot": { "rootId": "root", "nodes": { } } }
{ "t": "patch", "seq": 43, "ops": [ ...ops from 3.3 ] }
{ "t": "cmd_result", "cmdId": "c1", "ok": true, "data": { } }
{ "t": "cmd_result", "cmdId": "c1", "ok": false, "error": { "message": "..." } }
```

Client → server:

```jsonc
{ "t": "cmd", "cmdId": "c1", "target": "<nodeId>", "action": "selftest", "params": { } }
{ "t": "resync" }   // ask for a fresh hello (sent after a detected seq gap)
```

Rules:
- On connect the server immediately sends `hello`.
- Patches carry consecutive `seq`; if the client sees a gap it sends `resync`.
- Commands are routed by the **owner** of the target node: `core` handles its
  own (e.g. `cancel` on jobs, `restart` on dead plugin nodes), plugin-owned
  nodes are forwarded to the plugin via RPC. Unknown target/action → `ok:false`.
- The frontend reconnects automatically (exponential backoff, 0.5 s → 10 s cap)
  and shows connection state in the UI at all times.

## 5. Telemetry WebSocket — `/ws/telemetry`

High-rate numeric samples bypass the state tree. A node of type
`telemetry.channel` *describes* a channel (props: `unit`, `label`,
`sampleRateHz`, optional `min`/`max`); the samples flow here.

Client → server: `{ "t": "sub", "channel": "<nodeId>" }` / `{ "t": "unsub", ... }`

Server → client:

```jsonc
{ "t": "history", "channel": "<id>", "samples": [[tMs, value], ...] }  // on sub, ring buffer content
{ "t": "samples", "channel": "<id>", "samples": [[tMs, value], ...] }  // batched, flushed every 100 ms
```

Timestamps are Unix epoch **milliseconds** (float ok). The core keeps a ring
buffer per channel (default 10 000 samples).

## 6. Plugin protocol (JSON-RPC 2.0, line-delimited, over stdio)

Core spawns: `<python> <entry from manifest>` with cwd = plugin dir.
One JSON object per line on stdin/stdout. stderr lines are logged by the host.

Core → plugin (requests):
- `initialize {pluginNodeId, config}` → result `{ok: true}` — plugin then declares its nodes
- `command {target, action, params}` → result `{ok, data?, error?}`
- `ping {}` → result `{}` (heartbeat; every 2 s, timeout 5 s)
- `shutdown {}` → result `{}` then process exits
- `vehicle.detect {sessionNodeId, params}` / `vehicle.attach {sessionNodeId, profile}`
  / `vehicle.detach {sessionNodeId}` — M1.5, only for plugins with capability
  `vehicle`; see §15.4

Plugin → core (notifications):
- `add {node}` / `update {id, changes}` / `merge_props {id, props}` / `remove {id}`
  — mutations restricted to the plugin's own subtree **plus subtrees explicitly
  granted to it (mount grants, §15.4)**; violations are logged and dropped.
  The granted root itself stays core-owned: `add` may name it as `parentId`
  and `io` may report frames at it, but `update` / `merge_props` / `remove`
  of the root — or of a node under it the plugin did not declare — are
  violations
- `samples {channel, samples: [[tMs, v], ...]}`
- `log {level, message}`
- `io {...}` (M0.5, §14.2) · `vehicle.profile {sessionNodeId, profile}` (M1.5, §15.4)

There is deliberately no job-creation RPC in M0: jobs triggered by a plugin
are ordinary nodes in the plugin's subtree — see the M0 resolution in §3.2.

Supervision policy:
- Heartbeat timeout or process exit ⇒ plugin node + all descendants get
  `status: "error"`, error `{message: "plugin process died"}`.
- Automatic restart with backoff (1 s, 3 s, 10 s), max 3 attempts, then the
  plugin node stays `error` and offers action `restart`.
- On successful restart the old subtree is removed and re-declared.

Manifest (`manifest.json` in the plugin directory):

```jsonc
{
  "id": "demo_vci",
  "name": "Demo VCI (simulated)",
  "version": "0.1.0",
  "entry": "plugin.py",
  "python": "default",        // or an absolute path, e.g. a 32-bit interpreter
  "frontend": null,           // reserved for M1: plugin-shipped component bundles
  "capabilities": ["vehicle"],          // M1.5: optional; "vehicle" ⇒ implements §15.4
  "vehicle": { "detectParams": [] }     // M1.5: optional extra params for `detect` (§3.1.1)
}
```

## 7. Job engine (core)

- Anything long-running is a **job**: a node of type `job` under `jobs`, with
  child `job.step` nodes for nested/complex flows (steps may nest further).
- API (backend-internal): `jobs.create(name, coro_factory, locks=[...]) -> Job`.
  The coro receives a `JobContext` with `ctx.step(name)` (async context manager
  creating a running step node), `ctx.set_progress(0..1)`, and honours
  `asyncio.CancelledError` for cancellation.
- **Resource locks**: named exclusive locks (e.g. `"bus:demo_vci"`). Acquired
  in sorted order before the job starts (deadlock-free). While waiting, the job
  node is `init` with `props.waitingFor = [lockNames]`.
  **M0 scope:** locks are exclusive *per owner process* — the core engine's
  lock table and each plugin process's lock table (see §3.2) are separate
  namespaces, so a core job and a plugin job using the same lock name do
  **not** exclude each other. Cross-owner exclusivity arrives with the job
  RPC deferred in §3.2.
- Every job node has action `cancel` while `init`/`running` ⇒ status `cancelled`.
- Job failure ⇒ `error` on the job node **and** on the step that failed; the
  core never crashes because of a job.
- **Retention**: the engine keeps the newest 50 *terminal* jobs and removes
  older terminal job nodes (with their steps), so a long-running core does not
  accumulate them forever; plugin-side jobs apply the same cap per plugin.

## 8. Frontend architecture

- **Mirror store** (zustand): `{ connection, seq, rootId, nodes }` — mutated
  *only* by `hello`/`patch` messages. Hook `useNode(id)` selects one node.
- **Component registry**: maps `type` → React component. Resolution: exact
  match, then longest dotted prefix (`job.step` → `job`), then `GenericNode`.
  `GenericNode` renders name, type, status badge, error, progress, props table,
  action buttons and nested children — so *any* backend object is usable in
  the UI with zero frontend work.
- **StatusFrame**: every node component is wrapped in a frame that renders the
  status (badge/border), the error card for `error`, dimming for `gone`, and a
  per-node React error boundary (render crash ⇒ inline error card).
- **Actions**: buttons generated from `node.actions`, sending `cmd` and
  reflecting pending/result state.
- **Charts**: uPlot. `ChannelChart` subscribes to `/ws/telemetry` on mount,
  unsubscribes on unmount, keeps a client-side ring buffer, renders a rolling
  time window (default 60 s) plus a large current-value readout. Updates are
  rAF-throttled. Current values are always shown *with* their recent history.
- **Layout**: header (app name, connection state, system info) · left sidebar
  (full tree explorer) · main panel (selected node rendered via registry;
  default selection = root).
- **Design**: clean dev-tool aesthetic. CSS custom properties, dark theme
  default, light via `prefers-color-scheme`. System font stack (offline-safe).
  Status colors: idle=neutral, running=blue (subtle animation), done=green,
  error=red, cancelled=amber, gone=dimmed. UI strings in English (i18n is a
  later milestone).

## 9. Demo plugin `demo_vci` (proves the skeleton, still car-unspecific)

Declares under its plugin node:
- Device node `device.sim` "Simulated VCI", status `idle`, with actions:
  - `selftest` — job with 4 nested steps, ~8 s total, progress, cancellable,
    uses lock `bus:demo_vci`
  - `selftest_fail` — same but step 3 raises ⇒ demonstrates error propagation
  - `crash` — hard-kills the plugin process (`os._exit(1)`) ⇒ demonstrates
    supervisor + subtree `error` + restart
- Three `telemetry.channel` children at 20 Hz: sine (`V`), noise (`rpm`),
  sawtooth (`°C`).

## 10. Repository layout

```
C:\diAgnostic-framework\
  SPEC.md                    ← this contract
  README.md
  .gitignore
  logs\                      ← M0.5 session audit files session-<ts>.jsonl (git-ignored, §14.1)
  backend\
    pyproject.toml           ← project "diagcore", deps: fastapi, uvicorn[standard],
    .venv\                     pydantic>=2, websockets; dev: pytest, pytest-asyncio, httpx
    diagcore\
      __init__.py  model.py  tree.py  bus.py  jobs.py  telemetry.py
      loghub.py                                ← M0.5 audit LogHub (§14.1)
      server.py    app.py    __main__.py      ← `python -m diagcore`
      plugins\ host.py  api.py  manifest.py   ← api.py = helper lib for plugin authors
    plugins\demo_vci\ manifest.json  plugin.py
    tests\                   ← pytest; includes a real-WebSocket smoke test
  frontend\
    package.json  vite.config.ts  tsconfig*  index.html
    src\ main.tsx App.tsx
         lib\ types.ts ws.ts store.ts commands.ts telemetry.ts
         registry\ registry.tsx
         components\ StatusFrame.tsx GenericNode.tsx TreeExplorer.tsx
                     JobView.tsx DeviceView.tsx ChannelChart.tsx ...
  shell\run_desktop.py       ← optional pywebview window (serves built dist)
  scripts\ dev-backend.ps1 dev-frontend.ps1 build.ps1 test.ps1
  tools\node\                ← portable Node 24 LTS (git-ignored)
```

The core serves `frontend/dist` at `/` when it exists (single-process prod).

## 11. Toolchain on this machine

- Python 3.14 (64-bit): launcher `py -3.14`. A 32-bit 3.12 exists for future
  legacy-DLL plugins — **not** used by the core.
- Node: **not on PATH.** Portable install at `C:\diAgnostic-framework\tools\node`
  (node.exe, npm.cmd). Prepend it to PATH in every shell that needs npm/npx.
- Backend venv: `C:\diAgnostic-framework\backend\.venv` (create with `py -3.14 -m venv`).

## 12. Conventions

- Code, comments, identifiers, commit messages: English.
- Python: pydantic v2 models, full type hints, `from __future__ import annotations`,
  no bare `except:` (narrow exceptions; last resort `except Exception` with
  structured error reporting to the tree).
- TypeScript: `strict: true`, no `any` unless unavoidable at the WS boundary
  (parse into typed messages immediately).
- Tests must pass via `scripts\test.ps1`; frontend must typecheck + build via
  `scripts\build.ps1`.

## 13. Milestones (beyond M0 — out of scope now, design for them)

- **M0.5**: audit logging & raw feeds — see §14. **Implemented as of v0.2.**
- **M1**: plugin-shipped frontend bundles (manifest `frontend` field; dynamic
  import; e.g. camera stream with overlay), served at `/plugin-assets/<id>/`.
- **M1.5**: generic vehicle-session concepts (manufacturer-agnostic): vehicle
  profile store (detect a new vehicle vs. reuse a previously detected one),
  generic ECU-discovery flow, "probe arbitrary ECU" — as node types + generic
  components; plugins fill in the manufacturer-specific behavior.
  **Specified in §15, pulled ahead of M1. Implemented as of v0.3 (backend
  `diagcore` 0.3.0).**
- **M2**: persistence (sessions, recorded telemetry), bug-report export
  (zipped session logs).
- **M3**: packaging/installer (Tauri or PyInstaller + pywebview), auto-update.
- **M4**: i18n; first real vehicle plugin (wrap `oleodiag` as `plugin:psa`).

---

## 14. M0.5 — Audit logging & raw feeds  ✅ implemented as of v0.2 (backend `diagcore` 0.2.0)

Two platform laws, added to §1 in spirit and enforced by this layer:

- **Nothing is invisible.** Every backend actor — down to an adapter driver —
  has a node in the tree and therefore a place in the UI where its crash or
  state change is *seen* (§1.2 already guarantees this; M0.5 adds the history).
- **Nothing happens without a log.** Every action in the program, and above
  all every exchange with the vehicle, is logged automatically and completely,
  **always including the raw data** (read *and* write), so any session can be
  reconstructed 1:1 afterwards (wrong coding written to a car, bug reports,
  "why is it stuck at 10 %?").

### 14.1 LogHub (core)

Structured event:

```jsonc
{
  "ts": 1735e12,            // unix ms
  "n": 123,                 // monotonic event number (per session)
  "level": "trace | debug | info | warn | error",
  "source": "core | plugin:<id>",
  "nodeId": "string|null",  // the node this event belongs to (subtree filtering)
  "category": "cmd | state | io | job | plugin | system | vehicle",   // vehicle: M1.5 session lifecycle & grants
  "message": "human-readable, but NEVER only a label — always ids + values",
  "data": { },              // decoded/structured payload (optional)
  "raw": "hex string"       // raw bytes when applicable (optional)
}
```

Sinks:
- **Session file** `logs/session-<start-ts>.jsonl` — *complete*, every event,
  unbounded, flushed promptly. This is the audit trail. Its failure is never
  silent: the core keeps running on the ring, records one ring-only
  `system`/`error` event and flags the `system` node (§3.2).
- **In-memory ring** serving the UI: **50 000 events or 64 MiB of serialized
  events, whichever cap is hit first** (oldest evicted first) — a plugin
  pushing large props at high rate must not grow the core without bound.

Text hygiene: strings are Unicode. Lone UTF-16 surrogates (not valid text;
typically bytes decoded with `errors="surrogateescape"` that crossed the
plugin's JSON boundary as `\uDxxx` escapes) are escaped (`backslashreplace`,
byte value stays visible) at the plugin ingress and again at `emit`, so one
bad string can neither disable the session file nor kill a WebSocket sender.
A socket whose sender fails is closed (code 1011) and the drop is audited —
never a connected-but-deaf client.

Logged **automatically by the core at the seams** (plugin authors get this for
free): every received command (target id, action, full raw params — any JSON
value, a non-object is rejected `ok:false` *after* being logged — result,
plus `cmdId` and the issuing `client` connection in both events so concurrent
commands pair up unambiguously); every state patch (category `state`, level
`trace`, update diffs included); job lifecycle incl. steps, progress
milestones, cancellation, errors — for plugin-side jobs also the terminal
`failed: plugin process died` event when the plugin dies mid-job; plugin
lifecycle (spawn, heartbeat timeout, exit code, restart attempts); every
plugin RPC line in/out (level `trace`), **except two incoming
notifications**: `samples` (telemetry is never routed through the LogHub — it
would flood it; the `io` category is exclusively for vehicle frames) and `io`
(captured once as its category-`io` event with nodeId, direction, raw hex,
decoded and note; the JSON-RPC envelope is not trace-logged a second time).

### 14.2 Raw vehicle I/O

New plugin → core notification:

```jsonc
{ "method": "io", "params": { "nodeId": "<device/channel node>",
  "direction": "tx | rx", "raw": "hex", "decoded": { }, "note": "string?" } }
```

**Law:** every byte exchanged with a vehicle MUST be reported via `io` —
the plugin helper lib makes this a one-liner. `io` events land in the LogHub
(category `io`) and in the per-node raw feed. The demo plugin simulates bus
traffic during its jobs so the feed is demonstrable without hardware.

Rules (M0.5 resolution, recorded per the rule at the top of this file):
- `nodeId` must lie in the plugin's own subtree (same rule as the §6
  mutations); violations are logged (level `warn`, category `plugin`, source
  `plugin:<id>`, full params in `data`) and dropped.
- `raw` is the frame as **whole bytes in hex** (`frame.hex()`: even length,
  no `0x` prefix, no separators; whitespace between bytes is tolerated and
  stripped) so the trail replays byte-exact via `bytes.fromhex`. Anything
  else is dropped with the same `warn` event. The core's own `LogEvent.raw`
  is held to the same rule.

### 14.3 Log WebSocket — `/ws/logs`

Client → server: `{ "t": "sub", "filter": { "nodeId": "x"?, "levels": []?,
"categories": []?, "search": "..."? } }` (nodeId filters the whole subtree),
`{ "t": "unsub" }`. Server → client: `{ "t": "history", "events": [...] }`
(recent matching events on sub), `{ "t": "events", "events": [...] }` (live,
batched ≤10/s).

Filter semantics: every field is optional and **an empty list is the same as
an absent field — "no constraint"** (`categories: []` matches every
category). A client that wants to show nothing must not subscribe; the
global log panel therefore never lets the last category be deselected.

### 14.4 Frontend

- **Global log panel**: collapsible bottom drawer; live tail with pause/resume
  autoscroll; filters (level, category, subtree, text search); monospace; raw
  hex shown alongside decoded data; copyable.
- **Per-node raw feed**: every node component (via StatusFrame) gets a "Raw"
  expander showing the log/io feed filtered to that node's subtree — the
  "is it looping or actually working?" view.
- **Never a bare wait state**: long operations show progress/ETA when known;
  when unknowable, the raw feed is always one click away.

---

## 15. M1.5 — Generic vehicle session  ✅ implemented as of v0.3 (backend `diagcore` 0.3.0)

Manufacturer-agnostic answers to "which vehicle am I talking to?": detect a
new vehicle, reuse a previously detected one, see its ECUs, probe an
arbitrary ECU. The **core owns the concepts and the persistence**; a plugin
with capability `vehicle` supplies the manufacturer-specific **behavior**
(how to scan a bus, how to talk to an ECU). Everything stays visible (§1.2),
logged (§14) and job-shaped (§7).

### 15.1 Vehicle profile (persisted by the core)

```jsonc
{
  "id": "veh_<random>",              // core-assigned, opaque
  "name": "Demo Vehicle",            // display name (plugin-proposed, user-renamable)
  "vin": "string|null",
  "identifiers": { },                // plugin-defined, opaque to the core
  "pluginId": "demo_vci",            // the plugin able to attach this vehicle
  "firstSeen": 1735e12, "lastSeen": 1735e12,          // unix ms
  "ecus": [ { "id": "stable within the profile", "name": "Engine ECU",
              "address": "string", "protocol": "string", "props": { } } ],
  "notes": ""
}
```

Stored as one JSON file per profile in `<data dir>/vehicles/<id>.json`
(data dir default `<repo>/data`, override `DIAGCORE_DATA_DIR`; git-ignored).
Upsert key: `pluginId` + `vin` when a VIN is known, else `pluginId` +
canonical JSON of `identifiers`. Writes are atomic (temp file + replace).
Load errors mark the library node `error` (with action `reload`) — never
crash the core. Every profile create/update/forget is logged (category
`vehicle`) with the full profile in `data`.

> **M1.5 resolution (recorded per the rule at the top of this file):** an
> incoming profile that carries a VIN also matches a stored profile of the
> same plugin that has **no** VIN yet but identical, non-empty
> `identifiers` — so a VIN learned on a later `vehicle.profile` (§15.4)
> does not fork the vehicle into two profiles. Consequence for plugin
> authors: `identifiers` must be vehicle-unique per plugin (a layout
> shared by many vehicles would merge them). The audit event of such an
> upsert says so (`data.vinLearned: true`, `data.previousVin: null`, and
> the message notes the learned VIN).

### 15.2 Session node `vehicle` (core-owned, type `vehicle.session`)

props `{ state: "none | detecting | attaching | connected | lost", profileId,
name, vin, pluginId, ecuCount, since }`. Status mapping: `idle` for
none/connected, `running` for detecting/attaching, `error` for lost (plugin
died; `error` set).

Actions — only the applicable ones are present at any time:
- `detect` (states none/connected/lost): params = `plugin` (select over the
  plugins with capability `vehicle`; omitted when exactly one) + that plugin's
  manifest `vehicle.detectParams`. Starts detection (§15.4).
- `disconnect` (states detecting/attaching/connected/lost): cancels a running
  detect/attach, sends `vehicle.detach`, removes the session subtree.

> **M1.5 resolution (recorded per the rule at the top of this file):** the
> `detect` action is one static form, so with *several* capable plugins its
> params are the required `plugin` select plus **every** capable plugin's
> `detectParams`, each tagged `when: {plugin: <id>}` (§3.1.1; a
> `detectParam` named `plugin`, or one whose `when` references `plugin`, is
> a manifest error) — the generic form shows, validates and sends only the
> selected plugin's params, so same-named params of different plugins never
> clash and a required param of another plugin never blocks the form. The
> core validates the values against the *selected* plugin's params, fills in
> that plugin's defaults and forwards **exactly that plugin's declared
> params** in `vehicle.detect` (anything else a client sent is dropped, so
> the audited `params` are what the plugin received). With no capable plugin
> running `detect` is still offered (empty params) and answers `ok:false`
> with the reason — the UI never hides the way in. `detect`/`select` from
> connected/lost first tear the current session down like `disconnect`. A
> detect/attach that fails or is cancelled does **not** send
> `vehicle.detach` (the plugin's job has already ended; the grant is revoked
> and the subtree removed); `disconnect` does.
>
> `disconnect` always ends in `none`, never `lost`: a `vehicle.detach` that
> answers `ok:false` or times out is logged `warn` (category `vehicle`, the
> result in `data`) and the subtree is still removed and the grant revoked
> (§15.4) — `lost` is reserved for a plugin that died, where the evidence
> stays. A `disconnect` issued while the plugin has not yet answered
> `vehicle.detect` / `vehicle.attach` pre-empts that wait at once (the start
> command answers `ok:false` "abandoned", the disconnect `ok:true`), so a
> hung plugin never makes Disconnect wait for an RPC timeout.
>
> Commands on the session run as core-owned transitions that a client's
> command merely awaits: a client vanishing mid-command (page reload, stalled
> socket) never leaves the session half-transitioned (§1.5) — the transition
> completes and is logged as usual.

Children: while detecting/attaching, the plugin's job (`job`/`job.step`)
declared **under the session node** so progress, steps and the raw feed appear
*at the vehicle*; while connected, the plugin's `ecu` nodes and tool nodes.
All of these are plugin-owned via a mount grant (§15.4).

### 15.3 Library node `vehicles` (core-owned, type `vehicle.library`)

One child `vehicle.profile` node per stored profile (id `vehicle:<profileId>`,
ordered by `lastSeen` desc). props = the profile without full `ecus`
(`ecuCount` + compact `ecus` summary `[{id, name, address}]`). Actions:
`select` (attach via the profile's plugin; `ok:false` with reason while a
session is detecting/attaching or the plugin is not running), `rename`
(params `name` string), `forget` (`confirm` set; deletes the file). Library
status `idle`; `error` with action `reload` if the store failed to load.

### 15.4 Plugin side — capability `vehicle`

Manifest: `"capabilities": ["vehicle"]`, optional
`"vehicle": { "detectParams": [ …§3.1.1 params… ] }`.

**Mount grants.** The core grants the plugin the right to mutate under the
session node: implied by `vehicle.detect` / `vehicle.attach` for
`sessionNodeId`, revoked by `vehicle.detach`, by the core removing the session
subtree, or by a plugin restart. The host enforces §6 as "own subtree ∪
granted subtrees"; grants and revocations are logged (category `vehicle`).

Core → plugin requests:
- `vehicle.detect {sessionNodeId, params}` → `{ok, jobNodeId?, error?}`.
  The plugin runs a detection **job under the session node** (steps,
  progress, `io` frames, cancellable via the job's `cancel`), declares `ecu`
  nodes under the session node as they are found, and finally sends
  `vehicle.profile`. If the job ends `error`/`cancelled`, the core returns the
  session to `none`, removes the subtree, revokes the grant, and logs why.
  The job stays watched until it is terminal — also once an early
  `vehicle.profile` already made the session `connected` (a late
  error/cancel then ends the session the same way; `done` just stops the
  watch); a job the plugin *removes* while watched counts as failed
  ("vanished"). A detect answered without `jobNodeId` (synchronous
  detection) must still deliver its `vehicle.profile` within the same short
  grace period the core grants after a job's `done`, else it counts as
  failed. If the job the plugin names is already terminal when the answer
  is processed, the `detect`/`select` command itself answers `ok:false`.
- `vehicle.attach {sessionNodeId, profile}` → `{ok, jobNodeId?, error?}`:
  re-declare the profile's `ecu` nodes (optionally after a short verification
  job) so a known vehicle is reused without a full scan; a mismatch is
  reported through the job's error and handled like a failed detect.
- The *answer* to `vehicle.detect` / `vehicle.attach` is expected within
  10 s (the scan itself belongs in the job); a plugin that does not answer
  in time is treated like one that refused (session back to `none`,
  logged). A `disconnect` issued meanwhile abandons the wait immediately
  (§15.2).
- `vehicle.detach {sessionNodeId}` → `{ok}`: the plugin releases the vehicle.
  The core removes the subtree and revokes the grant even if the plugin does
  not answer (timeout 5 s ⇒ logged `warn`) or answers `ok:false`; the same
  5 s bound applies to the job `cancel` a teardown sends first.

Plugin → core notification:
- `vehicle.profile {sessionNodeId, profile}` — profile per §15.1 without
  `id`/`firstSeen`/`lastSeen` (the core fills them; a plugin that re-emits
  the full profile it received in `vehicle.attach` may carry them along —
  they are ignored, never a reason to drop the notification). The core
  upserts the file, refreshes the library node and sets the session
  `connected` with `profileId`. May be sent again later (more identifiers
  learned) ⇒ upsert.

`ecu` node (plugin-owned, type `ecu`): props `{ ecuId (= profile ecu id),
address, protocol, … }`, status `idle` when reachable, `error` when not;
plugin-defined actions (e.g. `read_ids`, `probe`) are ordinary §6 commands,
so the audit trail has target id, params and result for every one of them.

### 15.5 Frontend

- `VehicleSessionView` (type `vehicle.session`) — the hero of the app.
  none → prominent "Detect vehicle" (action form when params exist) plus a
  compact "Known vehicles" list with one-click connect; detecting/attaching →
  the job rendered inline (progress, steps, raw feed) with cancel; connected →
  summary (name, VIN, plugin, since, ECU count), ECU grid and tool nodes via
  the registry, disconnect; lost → error card with the ways out.
- `VehicleLibraryView` / `VehicleProfileView`: cards (name, VIN, last seen,
  ECU count, plugin) with Connect · Rename · Forget (confirm).
- `EcuView` (type `ecu`): compact card — name, address, protocol, status,
  actions; the Raw expander comes from StatusFrame as everywhere.
- **Generic action form** (§3.1.1): generated from `params` (text, number with
  min/max, checkbox, select from options), shows only the params in effect
  (`when`), validates `required`, sends `{ name: value }`; `confirm` prompts
  first. Used by GenericNode and every specific view alike. A finished
  command result flashes briefly and is then dropped by the command layer
  itself, so it never resurfaces on a control mounted later.
- The `vehicle` node is the default selection on connect (instead of root), so
  the first screen answers "which vehicle?". An explicit selection whose
  node leaves the tree (session subtree removed, plugin restart) is cleared,
  not just shadowed — a node re-declared later under the same id never
  hijacks the main panel without a click.

### 15.6 Demo plugin additions (fictive, manufacturer-free)

- Capability `vehicle`; detect param `depth` (select `quick` / `full`).
- Detection job (~6 s quick, ~12 s full): steps "Wake diagnostic bus", "Scan
  addresses 0x00–0x7F" (progress per address, simulated request/response `io`
  frames, 5 addresses answer), "Read identification" (per ECU), "Build
  profile". Always yields the same fictive vehicle (name "Demo Vehicle", VIN
  `DEM0VEH1CLE000042`), so the *second* run demonstrates "known vehicle →
  attach" through the library.
- Declares 5 `ecu` nodes (Engine, Body, Brakes, Airbag, Cluster), each with
  action `read_ids` (short job filling props), plus one tool node `ecu.probe`
  "Probe ECU" with action `probe` — params `address` (string, e.g. `0x2A`) and
  `protocol` (select UDS/KWP): known addresses answer (result data + `io`
  frames), unknown ones time out after ~1.5 s ⇒ `ok:false` with reason.
- `attach` runs a ~2 s "Verify ECUs present" job.
