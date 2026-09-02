/**
 * App shell (SPEC section 8, "Layout"):
 * header (app name, connection state, system info) · left sidebar (tree
 * explorer) · main panel (selected node via the registry, default = root)
 * · collapsible log drawer under the main area (SPEC 14.4).
 * With no backend reachable the UI stands alone showing a clear
 * connecting state while the socket retries in the background.
 */

import { useMirrorStore } from './lib/store'
import { useEffectiveSelection } from './lib/selection'
import type { ConnectionState, DiagNode } from './lib/types'
import { TreeExplorer } from './components/TreeExplorer'
import { LogPanel, LogPanelToggle } from './components/LogPanel'
import { NodeRenderer } from './registry/registry'

const CONNECTION_LABEL: Record<ConnectionState, string> = {
  connecting: 'connecting…',
  connected: 'connected',
  disconnected: 'reconnecting…',
}

function ConnectionPill() {
  const connection = useMirrorStore((s) => s.connection)
  return (
    <span className={`conn-pill conn-${connection}`}>
      <span className="conn-dot" />
      {CONNECTION_LABEL[connection]}
    </span>
  )
}

function useSystemNode(): DiagNode | undefined {
  return useMirrorStore((s) => {
    const root = s.rootId !== null ? s.nodes[s.rootId] : undefined
    if (root === undefined) return undefined
    for (const childId of root.children) {
      const child = s.nodes[childId]
      if (child !== undefined && child.type === 'system') return child
    }
    return undefined
  })
}

function SystemInfo() {
  const system = useSystemNode()
  if (system === undefined) return null
  const version = system.props['version']
  const python = system.props['pythonVersion']
  // The system node carries core-level failures (e.g. the audit session
  // file becoming unwritable, SPEC 14.1) — surface them in the header, where
  // they are seen even with the system node not selected (SPEC 1.5).
  const failure = system.status === 'error' && system.error !== null ? system.error : null
  return (
    <span className="sysinfo">
      {typeof version === 'string' ? `core v${version}` : null}
      {typeof python === 'string' ? ` · Python ${python}` : null}
      {failure !== null ? (
        <span
          className="status-badge st-error sysinfo-error"
          role="alert"
          title={failure.detail ?? failure.message}
        >
          <span className="status-dot" />
          {failure.message}
        </span>
      ) : null}
    </span>
  )
}

function ConnectingPanel() {
  const connection = useMirrorStore((s) => s.connection)
  return (
    <div className="connecting-panel">
      <span className="spinner spinner-large" />
      <h2>Connecting to core…</h2>
      <p>
        Waiting for the backend at <code>/ws/state</code>
        {connection === 'disconnected' ? ' — retrying automatically.' : '.'}
      </p>
    </div>
  )
}

export default function App() {
  const selectedId = useEffectiveSelection()
  return (
    <div className="app">
      <header className="app-header">
        <span className="brand">
          <span className="brand-mark" aria-hidden="true" />
          diAgnostic
        </span>
        <span className="header-spacer" />
        <SystemInfo />
        <LogPanelToggle />
        <ConnectionPill />
      </header>
      <aside className="sidebar">
        <TreeExplorer />
      </aside>
      <main className="main">
        {selectedId !== null ? <NodeRenderer id={selectedId} /> : <ConnectingPanel />}
      </main>
      <LogPanel />
    </div>
  )
}
