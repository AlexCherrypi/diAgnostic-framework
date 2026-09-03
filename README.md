# diAgnostic-framework

Herstellerunabhängiges Plattform-**Skelett** für Kfz-Diagnosewerkzeuge
(Milestones M0 + M0.5 + M1.5, Backend `diagcore` v0.3.0).
Enthält alles Visuelle, Organisatorische und Backend-Generische sowie seit M1.5
die herstellerunabhängigen **Fahrzeug-Session-Konzepte** (Erkennen, bekanntes
Fahrzeug wiederverwenden, ECUs, Probe) — **nichts Herstellerspezifisches**.
Fahrzeug-, VCI- und Datenbank-Logik kommt als Plugins.

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
Cancel; seit M0.5 der Audit-Log: `/ws/logs`-History und Live-Events,
cmd-/job-/io-/state-Events, Teilbaum-Filter, Session-JSONL-Datei; seit M1.5
die Fahrzeug-Session: detect ⇒ connected + Profil-Datei, probe, read_ids,
disconnect/Grant-Entzug, select ⇒ attach ohne Scan, Crash ⇒ lost ⇒ none, forget):

```powershell
cd backend
$env:DIAGCORE_DATA_DIR = "$env:TEMP\diag-smoke"                 # Wegwerf-Profilspeicher
.venv\Scripts\python.exe -m diagcore --port 8766                 # zweites Terminal
.venv\Scripts\python.exe tests\smoke_live.py --port 8766         # oder $env:DIAGCORE_PORT
```

Gegen einen wiederverwendeten Core räumen die M1.5-Checks eine hinterlassene
Session und Demo-Profile zuerst weg; `data\` im Repo wird von Tests nie
beschrieben (die pytest-Suite setzt `DIAGCORE_DATA_DIR` automatisch auf ein
temporäres Verzeichnis).

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

## Fahrzeug-Session (M1.5, ab v0.3)

SPEC §15: Der Core besitzt die herstellerunabhängigen **Fahrzeug-Konzepte**
und ihre **Persistenz**; ein Plugin mit der Capability `vehicle` liefert nur
das herstellerspezifische **Verhalten** (wie ein Bus gescannt, wie ein
Steuergerät angesprochen wird). Alles bleibt sichtbar (Tree), geloggt
(Audit-Log) und Job-förmig. Zwei neue Well-known-Knoten unter `root`:

- **`vehicle`** (Typ `vehicle.session`) — das aktive Fahrzeug und in der UI
  der Startbildschirm („Welches Fahrzeug?“). `props.state` ist
  `none | detecting | attaching | connected | lost`, dazu `profileId`, `name`,
  `vin`, `pluginId`, `ecuCount`, `since`. Status `idle` (none/connected),
  `running` (detecting/attaching), `error` (lost, mit `error`).
- **`vehicles`** (Typ `vehicle.library`, „Known vehicles“) — ein
  `vehicle.profile`-Kind pro gespeichertem Profil (`vehicle:<profileId>`,
  nach `lastSeen` absteigend) mit den Actions `select` (in der UI „Connect“),
  `rename` (Param `name`) und `forget` (mit Bestätigungstext, löscht die Datei).
  Ladefehler machen den Knoten `error` mit Action `reload` — der Core läuft weiter.

### Erkennen vs. bekanntes Fahrzeug

- **`detect`** auf dem Session-Knoten (Zustände none/connected/lost): Params =
  die `vehicle.detectParams` des Plugins (Demo: `depth` = `quick`/`full`) plus —
  nur wenn mehrere vehicle-fähige Plugins laufen — ein `plugin`-Select; dann
  trägt jeder Plugin-Param `when: {plugin: <id>}` (SPEC §3.1.1), das Formular
  zeigt und sendet nur die Params des gewählten Plugins, und der Core reicht
  genau dessen deklarierte Params weiter. Der Core ruft `vehicle.detect` im
  Plugin auf (Antwort binnen 10 s — der Scan selbst gehört in den Job); das
  Plugin legt seinen Erkennungs-Job **unter dem Session-Knoten** ab (Steps,
  Fortschritt, `io`-Frames, `cancel`), deklariert `ecu`-Knoten, sobald sie
  gefunden sind, und meldet zum Schluss `vehicle.profile`. Der Core speichert
  das Profil (Upsert), aktualisiert die Bibliothek und setzt die Session auf
  `connected`. Endet der Job mit `error`/`cancelled` (oder ohne Profil), geht
  die Session zurück auf `none`: Teilbaum entfernt, Grant entzogen, Grund
  geloggt — auch dann, wenn das Profil schon vor dem Job-Ende kam (der Job
  bleibt bis zum Ende beobachtet) oder das Plugin seinen Job-Knoten entfernt.
- **`select`** auf einer Profil-Karte: `vehicle.attach` mit dem gespeicherten
  Profil — das Plugin verifiziert kurz (Demo: ~2 s „Verify ECUs present“) und
  deklariert die ECUs erneut, **ohne** vollen Scan; `lastSeen` wird gesetzt.
  Abgelehnt (`ok:false` mit Grund), solange die Session detecting/attaching
  ist oder das Plugin nicht läuft.
- **`disconnect`** (detecting/attaching/connected/lost): bricht einen laufenden
  Job ab, sendet `vehicle.detach` (je 5 s Timeout ⇒ `warn`, der Core macht
  trotzdem weiter und endet immer in `none`, nie in `lost`), entfernt den
  Teilbaum, entzieht den Grant. Wartet der Core gerade noch auf die Antwort
  auf `vehicle.detect`/`vehicle.attach`, bricht `disconnect` dieses Warten
  sofort ab. Session-Kommandos laufen als Core-eigene Übergänge zu Ende, auch
  wenn der Client mittendrin verschwindet (Reload) — nie ein halber Zustand.
- Stirbt das Plugin während einer Session ⇒ `lost` (Status `error`, die
  plugin-eigenen Knoten bleiben als `error` markiert sichtbar); nach dem
  erfolgreichen Neustart des Plugins ⇒ `none`, alter Teilbaum weg, die
  Bibliothek behält das Profil.

Das Demo-Plugin liefert immer dasselbe fiktive Fahrzeug („Demo Vehicle“, VIN
`DEM0VEH1CLE000042`, 5 ECUs) — der zweite Durchlauf zeigt so den Weg
„bekanntes Fahrzeug → Connect“ über die Bibliothek.

### ECUs und Probe-Tool

Unter der verbundenen Session liegen die plugin-eigenen **`ecu`-Knoten**
(`props.ecuId`, `address`, `protocol`, …; Demo-Action `read_ids` = kurzer Job
unter dem ECU-Knoten, füllt `identification`/`lastReadMs`) und **Tool-Knoten**
wie `ecu.probe` „Probe ECU“ des Demo-Plugins: Action `probe` mit den Params
`address` (String, z. B. `0x2A`) und `protocol` (Select UDS/KWP). Bekannte
Adressen antworten (Ergebnisdaten + tx/rx-Frames), unbekannte laufen nach
~1,5 s in den Timeout ⇒ `ok:false` mit Grund. Jede dieser Actions ist ein
gewöhnliches Kommando: Ziel, Roh-Parameter (Option-*Value*, nie das Label)
und Ergebnis stehen im Audit-Log (Kategorie `cmd`), die Frames als `io`.

### Generisches Action-Formular (SPEC §3.1.1)

Actions deklarieren `params` (`string | number | boolean | select`, `required`,
`default`, `placeholder`, `help`, `min`/`max` nur bei Zahlen, `options` nur bei
`select`, optional `when: {"<select>": <optionValue>}` für Params, die nur bei
einer bestimmten Auswahl gelten) und optional `confirm`. Das Frontend erzeugt
daraus das Formular (Text, Zahl mit Bereich, Checkbox, Auswahl; `when`-Params
erscheinen nur, solange ihre Bedingung gilt), validiert `required` und sendet
`{ name: value }`; `confirm` fragt vorher nach. Das ist der **einzige** Weg,
Parameter einzusammeln — in den Fahrzeug-Views genauso wie in `GenericNode`.
Eine fehlerhafte Param-Deklaration eines Plugins wird wie jede ungültige
Notification verworfen und geloggt.

### Datenverzeichnis

Profile liegen als je eine JSON-Datei unter `<data dir>\vehicles\<id>.json`
(SPEC §15.1: `id`, `name`, `vin`, `identifiers`, `pluginId`, `firstSeen`,
`lastSeen`, `ecus`, `notes`). Standard ist `<repo>\data` (git-ignoriert),
Override per Umgebungsvariable **`DIAGCORE_DATA_DIR`** (oder
`AppCore(data_dir=...)`); der Pfad steht in `system.props.dataDir` und
`vehicles.props.dir`. Schreiben atomar (Temp-Datei + `os.replace`).
Upsert-Schlüssel: `pluginId` + VIN, sonst `pluginId` + kanonisches JSON der
`identifiers`; ein Profil mit VIN trifft zusätzlich ein VIN-loses Profil
desselben Plugins mit identischen `identifiers` (VIN später gelernt ⇒ kein
zweites Profil; deshalb müssen `identifiers` je Plugin fahrzeug-eindeutig
sein — das Audit-Event markiert so einen Merge mit `vinLearned`). Bei einem
Update bleiben `name`/`notes` Nutzer-eigen (Rename überlebt den nächsten
Detect), `identifiers` werden gemergt, `ecus` ersetzt. Alle Zustandswechsel, Mount-Grants und -Entzüge, Profil-Upserts (mit
vollem Profil in `data`), `forget` und Detach-Timeouts landen im Audit-Log mit
Kategorie `vehicle`.

### Capability `vehicle` im eigenen Plugin

`manifest.json` — Capability plus optionale Detect-Params:

```jsonc
{
  "id": "mein_vci", "name": "Mein VCI", "version": "0.1.0", "entry": "plugin.py",
  "python": "default", "frontend": null,
  "capabilities": ["vehicle"],
  "vehicle": {
    "detectParams": [
      { "name": "depth", "label": "Scan depth", "type": "select", "required": true,
        "default": "quick",
        "options": [ { "value": "quick", "label": "Quick" }, { "value": "full", "label": "Full" } ] }
    ]
  }
}
```

`plugin.py` — die drei Handler und das Profil, alles über `diagcore.plugins.api`:

```python
from diagcore.plugins.api import Plugin, PluginJobContext

plugin = Plugin()

async def scan(ctx: PluginJobContext, session_id: str, depth: str) -> None:
    async with ctx.step("Scan bus"):
        plugin.io(session_id, "tx", "3e80", decoded={"service": "TesterPresent"})   # Pflicht: jedes Byte
        plugin.io(session_id, "rx", "7e80")
        plugin.add({                                    # ECU-Knoten UNTER der Session (Mount-Grant)
            "id": f"{session_id}:ecu:engine", "type": "ecu", "name": "Engine ECU",
            "status": "idle", "parentId": session_id,
            "props": {"ecuId": "engine", "address": "0x10", "protocol": "UDS"},
            "actions": [{"name": "read_ids", "label": "Read identification", "params": []}],
        })
        ctx.set_progress(0.8)
    async with ctx.step("Build profile"):
        plugin.emit_vehicle_profile(session_id, {      # SPEC 15.1 ohne id/firstSeen/lastSeen
            "name": "Mein Fahrzeug", "vin": "WXX00000000000000",
            "identifiers": {"bus": "can-500k"},
            "ecus": [{"id": "engine", "name": "Engine ECU", "address": "0x10",
                      "protocol": "UDS", "props": {}}],
            "notes": "",
        })

def on_detect(session_id: str, params: dict) -> str:
    depth = str(params.get("depth") or "quick")
    return plugin.start_job("Detect vehicle", lambda ctx: scan(ctx, session_id, depth),
                            locks=["bus:mein_vci"], parent_id=session_id)

def on_attach(session_id: str, profile: dict) -> str | None:
    # ECUs des gespeicherten Profils erneut deklarieren — optional nach einem
    # kurzen Verify-Job (Job-ID zurückgeben) oder synchron (None zurückgeben).
    ...

def on_detach(session_id: str) -> None:
    ...  # Fahrzeug freigeben

plugin.register_vehicle_handlers(detect=on_detect, attach=on_attach, detach=on_detach)
```

Regeln dazu:

- `plugin.register_vehicle_handlers(detect=, attach=, detach=)` — Handler sind
  sync oder async. `detect(session_node_id, params)` und
  `attach(session_node_id, profile)` geben die ID des unter dem Session-Knoten
  gestarteten Jobs zurück (oder `None`, wenn die ECUs synchron deklariert
  wurden); `detach(session_node_id)` gibt das Fahrzeug frei. Eine Exception
  wird zu `{ok: false, error}` und lässt den Core die Session auf `none`
  zurücksetzen.
- **Mount-Grant** (SPEC §15.4): Mit `vehicle.detect`/`vehicle.attach` erlaubt
  der Core dem Plugin, unter `sessionNodeId` zu mutieren — `add`/`update`/
  `merge_props`/`remove`, `io` und `start_job(..., parent_id=session_id)`
  funktionieren dort wie im eigenen Teilbaum („eigener Teilbaum ∪ gewährte
  Teilbäume“). Der Session-Knoten selbst bleibt Core-eigen: `add` darunter und
  `io` daran sind erlaubt, `update`/`merge_props`/`remove` des Knotens werden
  verworfen und geloggt. Der Grant endet mit `vehicle.detach`, dem Entfernen
  des Session-Teilbaums oder einem Plugin-Neustart; danach werden Mutationen
  unter der Session verworfen und als `warn` geloggt.
- `plugin.emit_vehicle_profile(session_node_id, profile)` — das Profil nach
  SPEC §15.1 ohne `id`/`firstSeen`/`lastSeen` (`pluginId` darf fehlen, der Core
  stempelt es; das volle Profil aus `vehicle.attach` darf unverändert erneut
  gesendet werden, die Core-Felder werden ignoriert). Darf später erneut
  gesendet werden (weitere Identifikatoren gelernt ⇒ Upsert desselben
  Profils).
- Jobs unter der Session entfernt der Core zusammen mit der Session — der
  Retention-Cap des Plugins fasst sie nicht an.
- Vollständige Referenz: `backend\plugins\demo_vci\plugin.py` (Detect-Job mit
  vier Steps und per-Adresse-Frames, Attach-Verify, `read_ids`, `probe`).
  Timings für schnelle Tests: `DEMO_VCI_DETECT_SECONDS`, `DEMO_VCI_ATTACH_SECONDS`,
  `DEMO_VCI_READ_IDS_SECONDS`, `DEMO_VCI_PROBE_TIMEOUT_SECONDS`.

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
  Teilbaum mutieren — plus per Mount-Grant den Fahrzeug-Session-Teilbaum,
  solange es die Session hält (SPEC §15.4). Crash ⇒ Teilbaum `error`,
  automatischer Restart mit Backoff; Core und UI laufen weiter.

## Projektlayout

```
SPEC.md                      Vertrag (verbindlich)
backend\
  diagcore\                  Core: model, tree, bus, jobs, telemetry, loghub, vehicles, server, app
    plugins\                 host.py (Supervisor + Mount-Grants), api.py (Lib für Plugin-Autoren), manifest.py
  plugins\demo_vci\          Demo-Plugin (simuliertes VCI, Capability vehicle)
  tests\                     pytest-Suite + smoke_live.py (Live-E2E)
frontend\
  src\lib\                   types, store (zustand-Mirror), ws, commands, telemetry, actionform
  src\registry\              Typ → Komponente-Registry
  src\components\            StatusFrame, GenericNode, TreeExplorer, JobView, ActionForm,
                             VehicleSessionView, VehicleLibraryView, VehicleProfileView, EcuView, ...
shell\run_desktop.py         optionales pywebview-Fenster
scripts\                     dev-backend / dev-frontend / build / test (PS 5.1)
tools\node\                  portables Node (nicht im PATH, git-ignoriert)
logs\                        Session-Audit-Dateien session-<ts>.jsonl (git-ignoriert)
data\vehicles\               Fahrzeugprofile <id>.json (git-ignoriert; Override DIAGCORE_DATA_DIR)
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
  "frontend": null,      // reserviert für M1
  "capabilities": []     // optional; ["vehicle"] ⇒ Fahrzeug-Session, siehe oben
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
- `plugin.register_vehicle_handlers(detect=, attach=, detach=)` und
  `plugin.emit_vehicle_profile(session_id, profile)` — nur mit Capability
  `vehicle`, siehe „Fahrzeug-Session“ oben.
- Actions dürfen `params` (SPEC §3.1.1) und `confirm` deklarieren; die UI baut
  daraus das generische Formular und sendet `{ name: value }`.

Der Core entdeckt das Plugin beim Start automatisch, spawnt den Prozess
(cwd = Plugin-Verzeichnis), überwacht ihn per Heartbeat (2 s / Timeout 5 s)
und startet ihn bei Absturz mit Backoff neu — der Plugin-Knoten `plugin:<id>`
zeigt den Zustand und bietet immer die Action `restart`.

Frontend-seitig ist **nichts** nötig: unbekannte Typen rendert `GenericNode`
(Name, Status, Props, Action-Buttons, Kinder). Eigene Komponenten können per
`registerNodeComponent(type, component)` in der Registry ergänzt werden.
