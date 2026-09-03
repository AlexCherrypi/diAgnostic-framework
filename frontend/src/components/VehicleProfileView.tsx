/**
 * VehicleProfileView (SPEC 15.3 / 15.5): card for one stored vehicle profile
 * (`vehicle.profile`, child of the library). Shows VIN, plugin, last/first
 * seen (relative, absolute on hover and below), ECU count with the compact
 * ECU summary chips and the notes; the backend's actions — `select`
 * (rendered as Connect, primary), `rename` (action form), `forget`
 * (confirm) — go through the generic action form. Plugin-defined
 * `identifiers` stay reachable in a collapsed details block.
 */

import { defaultVariant } from '../lib/actionform'
import { formatDateTime, formatRelative, useNow } from '../lib/format'
import { propNumber, propRecord, propString } from '../lib/props'
import type { DiagNode } from '../lib/types'
import { presentProfileAction, profileEcus } from '../lib/vehicle'
import type { NodeComponentProps } from '../registry/registry'
import { ActionControl } from './ActionForm'
import { NodeChildren, PropsTable } from './bits'

function ProfileActions({ node }: { node: DiagNode }) {
  if (node.actions.length === 0) return null
  return (
    <div className="actions">
      {node.actions.map((action) => (
        <ActionControl
          key={action.name}
          target={node.id}
          action={presentProfileAction(action)}
          variant={defaultVariant(action, 'select')}
        />
      ))}
    </div>
  )
}

export function VehicleProfileView({ node }: NodeComponentProps) {
  const now = useNow()
  const vin = propString(node.props, 'vin')
  const plugin = propString(node.props, 'pluginId')
  const lastSeen = propNumber(node.props, 'lastSeen')
  const firstSeen = propNumber(node.props, 'firstSeen')
  const ecus = profileEcus(node.props)
  const ecuCount = propNumber(node.props, 'ecuCount') ?? ecus.length
  const notes = propString(node.props, 'notes')
  const identifiers = propRecord(node.props, 'identifiers')

  return (
    <>
      <div className="vp-body">
        <code className="vp-vin" title="VIN">
          {vin ?? 'no VIN'}
        </code>
        <dl className="vh-facts">
          <div>
            <dt>Plugin</dt>
            <dd>{plugin !== undefined ? <code>{plugin}</code> : '—'}</dd>
          </div>
          <div>
            <dt>Last seen</dt>
            <dd title={lastSeen !== undefined ? formatDateTime(lastSeen) : undefined}>
              {lastSeen !== undefined ? formatRelative(lastSeen, now) : '—'}
              {lastSeen !== undefined ? (
                <span className="vh-abs">{formatDateTime(lastSeen)}</span>
              ) : null}
            </dd>
          </div>
          <div>
            <dt>First seen</dt>
            <dd title={firstSeen !== undefined ? formatDateTime(firstSeen) : undefined}>
              {firstSeen !== undefined ? formatRelative(firstSeen, now) : '—'}
            </dd>
          </div>
          <div>
            <dt>ECUs</dt>
            <dd>{ecuCount}</dd>
          </div>
        </dl>
        {ecus.length > 0 ? (
          <div className="vp-ecus">
            {ecus.map((e) => (
              <span key={e.id} className="chip" title={`ecu id ${e.id}`}>
                {e.name}
                {e.address !== undefined ? <code>{e.address}</code> : null}
              </span>
            ))}
          </div>
        ) : null}
        {notes !== undefined ? <p className="vp-notes">{notes}</p> : null}
        {identifiers !== undefined && Object.keys(identifiers).length > 0 ? (
          <details className="vp-more">
            <summary>identifiers</summary>
            <PropsTable props={identifiers} />
          </details>
        ) : null}
      </div>
      <ProfileActions node={node} />
      <NodeChildren node={node} />
    </>
  )
}
