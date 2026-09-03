/**
 * VehicleSessionView (SPEC 15.2 / 15.5): renderer for the core's `vehicle`
 * node — the hero of the app and its default selection, answering "which
 * vehicle am I talking to?" in every session state:
 *
 * - none                 → a prominent "Detect vehicle" (the generic action
 *                          form when the action carries params) plus the
 *                          compact known-vehicles list with one-click connect
 *                          (`select` on the profile node)
 * - detecting / attaching → the plugin's job, declared under this node, is
 *                          rendered inline via the registry: progress, steps,
 *                          raw feed and its own cancel; disconnect abandons
 *                          the session
 * - connected            → summary (name, VIN, plugin, since, ECU count), the
 *                          ECU grid and the tool nodes via the registry,
 *                          disconnect / detect again
 * - lost                 → StatusFrame shows the error card; the body offers
 *                          the ways out (detect again / disconnect)
 *
 * Every child stays visible whatever the state (SPEC 1.2): children are
 * grouped by type (jobs, `ecu` nodes, everything else = tools), never
 * filtered away. An unknown `state` degrades to the generic layout.
 */

import { useMemo } from 'react'
import { useShallow } from 'zustand/react/shallow'
import { defaultVariant, findAction } from '../lib/actionform'
import { formatDateTime, formatRelative, useNow } from '../lib/format'
import { propNumber, propString } from '../lib/props'
import { selectNode } from '../lib/selection'
import { useMirrorStore, useNode, useRootChild } from '../lib/store'
import type { DiagNode, NodeAction } from '../lib/types'
import { presentProfileAction } from '../lib/vehicle'
import { NodeRenderer } from '../registry/registry'
import type { NodeComponentProps } from '../registry/registry'
import { ActionControl } from './ActionForm'
import { ActionButtons, NodeChildren, PropsTable } from './bits'
import { ErrorCard } from './StatusFrame'

type SessionState = 'none' | 'detecting' | 'attaching' | 'connected' | 'lost'

const SESSION_STATES: readonly SessionState[] = [
  'none',
  'detecting',
  'attaching',
  'connected',
  'lost',
]

function sessionState(node: DiagNode): SessionState | null {
  const s = node.props['state']
  return SESSION_STATES.includes(s as SessionState) ? (s as SessionState) : null
}

// ---------------------------------------------------------------------------
// Children by kind
// ---------------------------------------------------------------------------

interface ChildGroups {
  jobs: string[]
  ecus: string[]
  tools: string[]
}

function isJobType(type: string): boolean {
  return type === 'job' || type.startsWith('job.')
}

/** Child ids grouped by node type; re-computed only when a child's type or the child list changes. */
function useChildGroups(node: DiagNode): ChildGroups {
  const children = node.children
  const types = useMirrorStore(useShallow((s) => children.map((id) => s.nodes[id]?.type ?? '')))
  return useMemo(() => {
    const groups: ChildGroups = { jobs: [], ecus: [], tools: [] }
    children.forEach((id, i) => {
      const type = types[i] ?? ''
      if (isJobType(type)) groups.jobs.push(id)
      else if (type === 'ecu') groups.ecus.push(id)
      else groups.tools.push(id)
    })
    return groups
  }, [children, types])
}

function ChildSection({
  title,
  ids,
  grid = false,
  empty,
}: {
  title: string
  ids: string[]
  grid?: boolean
  empty?: string
}) {
  if (ids.length === 0 && empty === undefined) return null
  return (
    <section className="vh-section">
      <header className="vh-section-head">
        <h3>{title}</h3>
        <span className="vh-count">{ids.length}</span>
      </header>
      {ids.length === 0 ? (
        <p className="vh-empty">{empty}</p>
      ) : (
        <div className={grid ? 'ecu-grid' : 'node-children'}>
          {ids.map((id) => (
            <NodeRenderer key={id} id={id} />
          ))}
        </div>
      )}
    </section>
  )
}

/** The three child groups in their natural order for a state. */
function ChildSections({ groups, state }: { groups: ChildGroups; state: SessionState }) {
  const busy = state === 'detecting' || state === 'attaching'
  return (
    <>
      {busy ? (
        <ChildSection
          title={state === 'detecting' ? 'Detection job' : 'Attach job'}
          ids={groups.jobs}
          empty="Waiting for the plugin to declare its job — the Raw feed below shows what is happening."
        />
      ) : null}
      <ChildSection
        title={busy ? 'ECUs found so far' : 'ECUs'}
        ids={groups.ecus}
        grid
        empty={state === 'connected' ? 'The plugin has not declared any ECU nodes.' : undefined}
      />
      <ChildSection title="Tools" ids={groups.tools} />
      {busy ? null : <ChildSection title="Jobs" ids={groups.jobs} />}
    </>
  )
}

// ---------------------------------------------------------------------------
// Detect / disconnect controls
// ---------------------------------------------------------------------------

function DetectControl({
  node,
  detect,
  inline,
}: {
  node: DiagNode
  detect: NodeAction | undefined
  inline: boolean
}) {
  if (detect === undefined) {
    return (
      <p className="vh-hint">
        Detection is not available: no running plugin has the <code>vehicle</code> capability.
      </p>
    )
  }
  return (
    <ActionControl
      target={node.id}
      action={detect}
      variant="primary"
      size={inline ? 'large' : 'default'}
      inline={inline}
    />
  )
}

function DisconnectControl({
  node,
  disconnect,
}: {
  node: DiagNode
  disconnect: NodeAction | undefined
}) {
  if (disconnect === undefined) return null
  return <ActionControl target={node.id} action={disconnect} variant={defaultVariant(disconnect)} />
}

/**
 * The session's action row: disconnect, optionally detect (as a toggle
 * button — the hero states show it inline instead), and every other action
 * the backend offers on the node, so nothing is ever hidden (SPEC 1.2).
 */
function SessionActions({ node, withDetect }: { node: DiagNode; withDetect: boolean }) {
  const disconnect = findAction(node, 'disconnect')
  const detect = withDetect ? findAction(node, 'detect') : undefined
  const extras = node.actions.filter((a) => a.name !== 'detect' && a.name !== 'disconnect')
  if (disconnect === undefined && detect === undefined && extras.length === 0) return null
  return (
    <div className="actions">
      <DisconnectControl node={node} disconnect={disconnect} />
      {detect !== undefined ? <DetectControl node={node} detect={detect} inline={false} /> : null}
      {extras.map((a) => (
        <ActionControl key={a.name} target={node.id} action={a} variant={defaultVariant(a)} />
      ))}
    </div>
  )
}

// ---------------------------------------------------------------------------
// Known vehicles (compact list, state none)
// ---------------------------------------------------------------------------

function KnownVehicleRow({ id, now }: { id: string; now: number }) {
  const node = useNode(id)
  if (node === undefined) return null
  const vin = propString(node.props, 'vin')
  const plugin = propString(node.props, 'pluginId')
  const lastSeen = propNumber(node.props, 'lastSeen')
  const ecuCount = propNumber(node.props, 'ecuCount')
  const select = findAction(node, 'select')
  return (
    <div className={`vh-known-row${node.status === 'gone' ? ' is-gone' : ''}`}>
      <div className="vh-known-main">
        <button
          type="button"
          className="vh-known-name"
          onClick={() => selectNode(id)}
          title="Show the stored profile"
        >
          {node.name}
        </button>
        <code className="vh-known-vin">{vin ?? 'no VIN'}</code>
      </div>
      <div className="vh-known-meta">
        {plugin !== undefined ? (
          <span title="plugin">
            <code>{plugin}</code>
          </span>
        ) : null}
        {ecuCount !== undefined ? (
          <span>
            {ecuCount} ECU{ecuCount === 1 ? '' : 's'}
          </span>
        ) : null}
        {lastSeen !== undefined ? (
          <span title={formatDateTime(lastSeen)}>seen {formatRelative(lastSeen, now)}</span>
        ) : null}
      </div>
      {select !== undefined ? (
        <ActionControl
          target={id}
          action={presentProfileAction(select)}
          variant="primary"
          size="small"
        />
      ) : (
        <button
          type="button"
          className="btn btn-small"
          disabled
          title="This profile offers no connect action right now"
        >
          Connect
        </button>
      )}
    </div>
  )
}

function KnownVehicles() {
  const library = useRootChild('vehicle.library')
  const now = useNow()
  if (library === undefined) return null
  const count = library.children.length
  return (
    <section className="vh-section vh-known">
      <header className="vh-section-head">
        <h3>Known vehicles</h3>
        <span className="vh-count">{count}</span>
        <span className="vh-spacer" />
        <button
          type="button"
          className="btn btn-small"
          onClick={() => selectNode(library.id)}
          title="Open the vehicle library"
        >
          Open library
        </button>
      </header>
      {library.status === 'error' && library.error !== null ? (
        <>
          <ErrorCard title="Vehicle library" error={library.error} />
          <ActionButtons node={library} />
        </>
      ) : null}
      {count === 0 ? (
        <p className="vh-empty">
          Nothing stored yet — the first successful detection creates a profile here, so the
          next time this vehicle connects with one click.
        </p>
      ) : (
        <div className="vh-known-list">
          {library.children.map((id) => (
            <KnownVehicleRow key={id} id={id} now={now} />
          ))}
        </div>
      )}
    </section>
  )
}

// ---------------------------------------------------------------------------
// States
// ---------------------------------------------------------------------------

function NoneState({ node, groups }: { node: DiagNode; groups: ChildGroups }) {
  return (
    <>
      <div className="vh-hero">
        <h2 className="vh-title">No vehicle connected</h2>
        <p className="vh-sub">
          Detect the vehicle on the bus, or reconnect one you have seen before.
        </p>
        <div className="vh-detect">
          <DetectControl node={node} detect={findAction(node, 'detect')} inline />
        </div>
        <SessionActions node={node} withDetect={false} />
      </div>
      <KnownVehicles />
      <ChildSections groups={groups} state="none" />
    </>
  )
}

function BusyState({
  node,
  state,
  groups,
}: {
  node: DiagNode
  state: 'detecting' | 'attaching'
  groups: ChildGroups
}) {
  const name = propString(node.props, 'name')
  const plugin = propString(node.props, 'pluginId')
  return (
    <>
      <div className="vh-hero">
        <h2 className="vh-title">
          {state === 'detecting' ? 'Detecting vehicle…' : `Attaching ${name ?? 'vehicle'}…`}
        </h2>
        <p className="vh-sub">
          {plugin !== undefined ? (
            <>
              <code>{plugin}</code>{' '}
              {state === 'detecting' ? 'is scanning the bus' : 'is verifying the known ECUs'}.{' '}
            </>
          ) : null}
          Progress, steps and the raw bus traffic appear below — cancel on the job, or disconnect
          to abandon the session.
        </p>
        <SessionActions node={node} withDetect={false} />
      </div>
      <ChildSections groups={groups} state={state} />
    </>
  )
}

function ConnectedState({ node, groups }: { node: DiagNode; groups: ChildGroups }) {
  const now = useNow()
  const name = propString(node.props, 'name')
  const vin = propString(node.props, 'vin')
  const plugin = propString(node.props, 'pluginId')
  const since = propNumber(node.props, 'since')
  const ecuCount = propNumber(node.props, 'ecuCount') ?? groups.ecus.length
  const profileId = propString(node.props, 'profileId')
  // Profile nodes are `vehicle:<profileId>` (SPEC 15.3) — link to it when it exists.
  const profileNode = useNode(profileId !== undefined ? `vehicle:${profileId}` : null)
  return (
    <>
      <div className="vh-summary">
        <div className="vh-summary-main">
          <h2 className="vh-title">{name ?? node.name}</h2>
          <code className="vh-vin" title="VIN">
            {vin ?? 'VIN unknown'}
          </code>
        </div>
        <dl className="vh-facts">
          <div>
            <dt>Plugin</dt>
            <dd>{plugin !== undefined ? <code>{plugin}</code> : '—'}</dd>
          </div>
          <div>
            <dt>Connected</dt>
            <dd title={since !== undefined ? formatDateTime(since) : undefined}>
              {since !== undefined ? formatRelative(since, now) : '—'}
              {since !== undefined ? <span className="vh-abs">{formatDateTime(since)}</span> : null}
            </dd>
          </div>
          <div>
            <dt>ECUs</dt>
            <dd>{ecuCount}</dd>
          </div>
          <div>
            <dt>Profile</dt>
            <dd>
              {profileNode !== undefined ? (
                <button
                  type="button"
                  className="btn-link"
                  onClick={() => selectNode(profileNode.id)}
                  title="Show the stored profile"
                >
                  {profileId}
                </button>
              ) : (
                (profileId ?? '—')
              )}
            </dd>
          </div>
        </dl>
        <SessionActions node={node} withDetect />
      </div>
      <ChildSections groups={groups} state="connected" />
    </>
  )
}

function LostState({ node, groups }: { node: DiagNode; groups: ChildGroups }) {
  const name = propString(node.props, 'name')
  return (
    <>
      <div className="vh-hero">
        <h2 className="vh-title">Connection to {name ?? 'the vehicle'} lost</h2>
        <p className="vh-sub">
          The plugin died — see the error above. Detect again to start a fresh session, or
          disconnect to clear this one.
        </p>
        <div className="vh-ways">
          <DetectControl node={node} detect={findAction(node, 'detect')} inline />
          <SessionActions node={node} withDetect={false} />
        </div>
      </div>
      <ChildSections groups={groups} state="lost" />
    </>
  )
}

// ---------------------------------------------------------------------------

export function VehicleSessionView({ node }: NodeComponentProps) {
  const state = sessionState(node)
  const groups = useChildGroups(node)

  if (state === null) {
    // Unknown or missing `state`: never hide anything — the generic layout.
    return (
      <>
        <PropsTable props={node.props} />
        <ActionButtons node={node} />
        <NodeChildren node={node} />
      </>
    )
  }

  switch (state) {
    case 'none':
      return <NoneState node={node} groups={groups} />
    case 'detecting':
    case 'attaching':
      return <BusyState node={node} state={state} groups={groups} />
    case 'connected':
      return <ConnectedState node={node} groups={groups} />
    case 'lost':
      return <LostState node={node} groups={groups} />
  }
}
