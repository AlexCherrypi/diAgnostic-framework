/**
 * App shell (SPEC section 8, "Layout"):
 * header (app name, connection state, system info) · left sidebar (tree
 * explorer) · main panel (selected node via the registry, default = root).
 * With no backend reachable the UI stands alone showing a clear
 * connecting state while the socket retries in the background.
 */

import { useMirrorStore } from './lib/store'
import { useEffectiveSelection } from './lib/selection'
import type { ConnectionState, DiagNode } from './lib/types'
import { TreeExplorer } from './components/TreeExplorer'
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
  return (
    <span className="sysinfo">
      {typeof version === 'string' ? `core v${version}` : null}
      {typeof python === 'string' ? ` · Python ${python}` : null}
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
        <ConnectionPill />
      </header>
      <aside className="sidebar">
        <TreeExplorer />
      </aside>
      <main className="main">
        {selectedId !== null ? <NodeRenderer id={selectedId} /> : <ConnectingPanel />}
      </main>
    </div>
  )
}
