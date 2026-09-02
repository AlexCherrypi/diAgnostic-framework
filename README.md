# diAgnostic-framework

Herstellerunabhängiges Plattform-**Skelett** für Kfz-Diagnosewerkzeuge
(Milestones M0 + M0.5, Backend `diagcore` v0.2.0).
Enthält alles Visuelle, Organisatorische und Backend-Generische — **nichts
Fahrzeugspezifisches**. Fahrzeug-, VCI- und Datenbank-Logik kommt später als Plugins.

Der **verbindliche Vertrag** zwischen Backend, Frontend und Plugins steht in
[`SPEC.md`](SPEC.md). Bei Unklarheiten gilt: erst SPEC.md ändern, dann Code.

## Quickstart

Voraussetzungen (siehe SPEC.md §11): Python 3.14 (64-bit, `py -3.14`), portables
Node unter `tools\node` (nicht im PATH). Backend-venv einmalig anlegen:

```powershell
py -3.14 -m venv backend\.venv
backend\.venv\Scripts\python.exe -m pip install -e "backend[dev]"
```

### Dev-Modus (zwei Terminals, Hot-Reload im Frontend)

```powershell
.\scripts\dev-backend.ps1    # Core auf http://127.0.0.1:8765
.\scripts\dev-frontend.ps1   # Vite auf http://localhost:5173 (proxied /ws + /api)
```

Browser: <http://localhost:5173>

### Prod-Modus (ein Prozess)

```powershell
.\scripts\build.ps1          # Frontend nach frontend\dist bauen
.\scripts\dev-backend.ps1    # Core serviert dist\ auf http://127.0.0.1:8765/
```

### Desktop-Shell (natives Fenster)

Einfachster Weg: **`START.bat` doppelklicken** (startet Core + Fenster).
Manuell:

```powershell
backend\.venv\Scripts\python.exe -m pip install -e "backend[desktop]"
backend\.venv\Scripts\python.exe shell\run_desktop.py
```

### Tests

```powershell
.\scripts\test.ps1           # Backend-pytest + Frontend-Typecheck/Build
```

Zusätzlich gibt es einen Live-Smoke-Test gegen einen **laufenden** Core
(Hello-Snapshot, Telemetrie, Selftest-Job, Fehlerpfad, Plugin-Crash+Restart,
Cancel sowie seit M0.5 der Audit-Log: `/ws/logs`-History und Live-Events,
cmd-/job-/io-/state-Events, Teilbaum-Filter, Session-JSONL-Datei):

```powershell
cd backend
.venv\Scripts\python.exe tests\smoke_live.py              # Core auf 8765
.venv\Scripts\python.exe tests\smoke_live.py --port 8766  # oder $env:DIAGCORE_PORT
```

## Audit-Log & Roh-Feed (M0.5, ab v0.2)

Plattform-Gesetz aus SPEC §14: **Nichts passiert ohne Log.** Der Core
protokolliert automatisch an seinen Nahtstellen — Plugin-Autoren müssen dafür
nichts tun:

- jedes empfangene Kommando: Ziel-Knoten-ID, Action, vollständige Parameter,
  das Ergebnis sowie `cmdId` und die auslösende UI-Verbindung, damit sich
  gleichzeitige Kommandos eindeutig zuordnen lassen (Kategorie `cmd`)
- jeden State-Patch (`add`/`update`/`merge_props`/`remove`) inkl. Diff der
  geänderten Werte (Kategorie `state`, Level `trace`)
- den Job-Lebenszyklus: angelegt, Warten auf Locks, gestartet, Steps,
  25/50/75-%-Meilensteine, Abbruch, Fehler, fertig (Kategorie `job`) — auch
  für Plugin-seitige Jobs, inklusive `failed: plugin process died`, wenn das
  Plugin mitten im Job stirbt
- den Plugin-Lebenszyklus: Spawn (PID), Initialize, Heartbeat-Timeout,
  Exit-Code, Restart-Versuche, verworfene Notifications, stderr-Zeilen sowie
  jede JSON-RPC-Zeile rein/raus auf Level `trace` (Kategorie `plugin`) —
  ausgenommen die eingehenden Notifications `samples` (Telemetrie) und `io`
  (landet einmal als `io`-Event, nicht zusätzlich als Roh-Zeile)
- **jeden Fahrzeug-Frame** (Kategorie `io`, immer mit Roh-Hex): ein Plugin
  meldet ihn per `plugin.io(node_id, "tx" | "rx", raw_hex, decoded=..., note=...)`
  — Gesetz aus SPEC §14.2: jedes Byte zum/vom Fahrzeug wird gemeldet, lesend
  wie schreibend. `raw_hex` ist `frame.hex()` (ganze Bytes, kein `0x`, keine
  Trenner); anderes wird verworfen und als `warn` geloggt. Das Demo-Plugin
  simuliert dafür während seiner Self-Tests UDS-ähnliche Request/Response-Paare.
- Core-Start/-Stopp und UI-Verbindungen zu `/ws/state` (Kategorie `system`)

Telemetrie-Samples laufen bewusst **nicht** durch das Log (sie würden es
fluten); die Kategorie `io` ist ausschließlich für Fahrzeug-Kommunikation.

**Session-Dateien:** `logs\session-<Startzeit>.jsonl` (Verzeichnis wird beim
Start angelegt, git-ignoriert) — ein JSON-Objekt pro Zeile, sofort geflusht,
vollständig und unbegrenzt: der Audit-Trail, mit dem sich jede Sitzung 1:1
rekonstruieren lässt (falsche Codierung, Bug-Report, „warum hängt es bei
10 %?“). Der Pfad steht in `system.props.auditFile`; fällt die Datei aus
(Platte voll, Verzeichnis weg), geht der `system`-Knoten auf `error`
(`auditFileError`), `/api/health` meldet `ok: false`, und im Log erscheint
genau ein `system`/`error`-Event — der Core läuft mit dem Ring weiter.
Zusätzlich hält der Core einen Ring der letzten 50 000 Events (max. 64 MiB
serialisiert) für die UI; Zugriff über den WebSocket `/ws/logs` (SPEC §14.3:
`sub` mit Filter ⇒ `history` + gebündelte Live-`events`, ein Filter pro
Verbindung; leere Listen bedeuten „keine Einschränkung“).

**UI:** Das **Log-Panel** (einklappbare Leiste am unteren Rand) zeigt den
Live-Tail mit Pause/Resume-Autoscroll und Filtern (Mindest-Level, Kategorien,
Teilbaum = aktuell ausgewählter Knoten, Textsuche); jede Zeile lässt sich
aufklappen (Meta, Daten-JSON, Hex-Block, Kopieren). Jeder Knoten hat in seinem
StatusFrame einen **„Raw“-Expander** mit dem Log-/io-Feed seines Teilbaums —
die „hängt es oder arbeitet es?“-Ansicht, immer einen Klick entfernt.

**Port-Override:** Standard bleibt `127.0.0.1:8765`. Für eine zweite Instanz
(z. B. Tests neben der laufenden Desktop-App): `python -m diagcore --port 8766`
oder Umgebungsvariable `DIAGCORE_PORT=8766` (das Flag gewinnt). `smoke_live.py`
liest dieselbe Variable (Fallback 8765).

## Architektur-Überblick

Details und alle Protokolle: [`SPEC.md`](SPEC.md).

- **Core** (Python 3.14, asyncio, FastAPI, `python -m diagcore`): einzige
  Quelle der Wahrheit. Hält den **State-Tree** (flacher Baum typisierter
  Knoten, SPEC §3), die **Job-Engine** (SPEC §7), den **Telemetry-Hub**
  (SPEC §5) und den **Plugin-Supervisor** (SPEC §6).
- **Frontend** (Vite + React + TS strict, zustand, uPlot): reiner Renderer.
  Spiegelt den Tree über `/ws/state` (hello/patch/resync), sendet Kommandos
  als Intents, zeichnet Telemetrie über `/ws/telemetry`. Jeder Knotentyp hat
  eine Komponente oder fällt auf `GenericNode` zurück (SPEC §8).
- **Plugins**: eigene OS-Prozesse (auch 32-bit-Python für Alt-DLLs möglich),
  JSON-RPC 2.0 zeilenweise über stdio. Ein Plugin darf nur seinen eigenen
  Teilbaum mutieren. Crash ⇒ Teilbaum `error`, automatischer Restart mit
  Backoff; Core und UI laufen weiter.

## Projektlayout

```
SPEC.md                      Vertrag (verbindlich)
backend\
  diagcore\                  Core: model, tree, bus, jobs, telemetry, loghub, server, app
    plugins\                 host.py (Supervisor), api.py (Lib für Plugin-Autoren)
  plugins\demo_vci\          Demo-Plugin (simuliertes VCI)
  tests\                     pytest-Suite + smoke_live.py (Live-E2E)
frontend\
  src\lib\                   types, store (zustand-Mirror), ws, commands, telemetry
  src\registry\              Typ → Komponente-Registry
  src\components\            StatusFrame, GenericNode, TreeExplorer, JobView, ...
shell\run_desktop.py         optionales pywebview-Fenster
scripts\                     dev-backend / dev-frontend / build / test (PS 5.1)
tools\node\                  portables Node (nicht im PATH, git-ignoriert)
logs\                        Session-Audit-Dateien session-<ts>.jsonl (git-ignoriert)
```

## Eigenes Plugin schreiben (am Beispiel `demo_vci`)

Ein Plugin ist ein Verzeichnis unter `backend\plugins\<id>\` mit zwei Dateien.
Vorlage: `backend\plugins\demo_vci\`.

**1. `manifest.json`** (SPEC §6):

```jsonc
{
  "id": "mein_plugin",
  "name": "Mein Plugin",
  "version": "0.1.0",
  "entry": "plugin.py",
  "python": "default",   // oder absoluter Pfad, z. B. 32-bit-Interpreter
  "frontend": null       // reserviert für M1
}
```

**2. `plugin.py`** mit der Helfer-Lib `diagcore.plugins.api`:

```python
from diagcore.plugins.api import Plugin, PluginJobContext

plugin = Plugin()

@plugin.on_initialized
def declare() -> None:
    # Knoten deklarieren — nur unterhalb des eigenen Plugin-Knotens!
    dev = f"{plugin.node_id}:device"
    plugin.add({
        "id": dev, "type": "device.sim", "name": "Mein Gerät",
        "status": "idle", "parentId": plugin.node_id,
        "actions": [{"name": "selftest", "label": "Run self-test", "params": []}],
    })
    # Telemetrie-Kanal: type "telemetry.channel", props unit/label/sampleRateHz
    # Samples dann per plugin.samples(kanal_id, [[t_ms, wert], ...]) senden.

async def run_selftest(ctx: PluginJobContext) -> None:
    async with ctx.step("Schritt 1"):
        ctx.set_progress(0.5)   # 0..1; CancelledError sauber durchlassen

@plugin.on_command("selftest")
def selftest(target: str, params: dict) -> dict:
    job_id = plugin.start_job("Self-test", run_selftest, locks=["bus:mein_plugin"])
    return {"jobId": job_id}    # wird als cmd_result-data an die UI gereicht

if __name__ == "__main__":
    plugin.run()
```

Bausteine der API:

- `plugin.add / update / merge_props / remove` — Tree-Mutationen im eigenen
  Teilbaum (Verstöße werden vom Core verworfen und geloggt).
- `plugin.on_command("name")` — Handler (sync oder async) für Actions der
  eigenen Knoten; Rückgabe-Dict landet in `cmd_result.data`, Exceptions werden
  zu `ok:false`.
- `plugin.start_job(name, coro_factory, locks=[...], parent_id=...)` — Job mit
  `job`/`job.step`-Knoten, Fortschritt, benannten Locks und automatischer
  `cancel`-Action. Semantik spiegelt SPEC §7, aber: die Job-Knoten liegen im
  Plugin-Teilbaum (nicht unter `jobs`) und die benannten Locks sind nur
  Plugin-lokal exklusiv — siehe die M0-Hinweise in SPEC §3.2/§6/§7.
- `plugin.samples(channel_id, [[tMs, v], ...])` — Telemetrie in den Hub
  (bewusst nicht im Audit-Log).
- `plugin.io(node_id, "tx" | "rx", raw_hex, decoded=None, note=None)` — jeden
  Fahrzeug-Frame melden (Pflicht, SPEC §14.2); landet im Audit-Log
  (Kategorie `io`, mit Roh-Hex) und im „Raw“-Feed des Knotens.
- `plugin.log(level, message)` — Log über den Host (Audit-Log-Kategorie
  `plugin`); stderr wird ebenfalls vom Host geloggt.
- `plugin.create_task(coro)` — Hintergrund-Task (z. B. Telemetrie-Schleife).

Der Core entdeckt das Plugin beim Start automatisch, spawnt den Prozess
(cwd = Plugin-Verzeichnis), überwacht ihn per Heartbeat (2 s / Timeout 5 s)
und startet ihn bei Absturz mit Backoff neu — der Plugin-Knoten `plugin:<id>`
zeigt den Zustand und bietet immer die Action `restart`.

Frontend-seitig ist **nichts** nötig: unbekannte Typen rendert `GenericNode`
(Name, Status, Props, Action-Buttons, Kinder). Eigene Komponenten können per
`registerNodeComponent(type, component)` in der Registry ergänzt werden.
