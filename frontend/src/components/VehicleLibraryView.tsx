/**
 * VehicleLibraryView (SPEC 15.3 / 15.5): the `vehicles` node — one
 * VehicleProfileView card per stored profile (children, newest `lastSeen`
 * first as ordered by the backend) in a responsive grid. The library's own
 * actions (`reload` while its store failed to load — the error card comes
 * from StatusFrame) render through the generic action buttons.
 */

import { selectNode } from '../lib/selection'
import { useRootChild } from '../lib/store'
import { NodeRenderer } from '../registry/registry'
import type { NodeComponentProps } from '../registry/registry'
import { ActionButtons } from './bits'

export function VehicleLibraryView({ node }: NodeComponentProps) {
  const session = useRootChild('vehicle.session')
  const count = node.children.length
  return (
    <>
      <div className="vl-head">
        <span className="vl-summary">
          {count === 0
            ? 'No known vehicles yet'
            : `${count} known vehicle${count === 1 ? '' : 's'} · most recently seen first`}
        </span>
        <ActionButtons node={node} />
      </div>
      {count === 0 ? (
        <p className="vh-empty">
          A profile is stored by the first successful detection.
          {session !== undefined ? (
            <>
              {' '}
              <button type="button" className="btn-link" onClick={() => selectNode(session.id)}>
                Go to the vehicle page
              </button>
            </>
          ) : null}
        </p>
      ) : (
        <div className="vl-grid">
          {node.children.map((id) => (
            <NodeRenderer key={id} id={id} />
          ))}
        </div>
      )}
    </>
  )
}
