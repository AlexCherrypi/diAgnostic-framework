/**
 * Small shared building blocks used by the node components:
 * progress bar, props table, action buttons (the generic action form with
 * pending/result state, see ActionForm.tsx) and the nested-children renderer.
 */

import { defaultVariant } from '../lib/actionform'
import type { DiagNode } from '../lib/types'
import { NodeRenderer } from '../registry/registry'
import { ActionControl } from './ActionForm'

export function ProgressBar({
  value,
  indeterminate = false,
}: {
  value: number | null
  indeterminate?: boolean
}) {
  if (value === null && !indeterminate) return null
  const pct = value === null ? 0 : Math.max(0, Math.min(1, value)) * 100
  return (
    <div className="progress-row">
      <div
        className={`progress${indeterminate && value === null ? ' indeterminate' : ''}`}
        role="progressbar"
        aria-valuemin={0}
        aria-valuemax={100}
        aria-valuenow={value === null ? undefined : Math.round(pct)}
      >
        <div className="progress-fill" style={{ width: `${pct}%` }} />
      </div>
      {value !== null ? <span className="progress-label">{Math.round(pct)}%</span> : null}
    </div>
  )
}

function formatPropValue(value: unknown): string {
  if (value === null || value === undefined) return '—'
  if (typeof value === 'string') return value
  if (typeof value === 'number' || typeof value === 'boolean') return String(value)
  try {
    return JSON.stringify(value)
  } catch {
    return String(value)
  }
}

export function PropsTable({ props }: { props: Record<string, unknown> }) {
  const entries = Object.entries(props)
  if (entries.length === 0) return null
  return (
    <table className="props-table">
      <tbody>
        {entries.map(([key, value]) => (
          <tr key={key}>
            <td className="props-key">{key}</td>
            <td className="props-value">{formatPropValue(value)}</td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}

/**
 * Every action of a node as an ActionControl (button, or button + inline
 * form/confirm panel per SPEC 3.1.1). `primary` names the action rendered
 * as the emphasized one (e.g. `select` on a vehicle profile).
 */
export function ActionButtons({ node, primary }: { node: DiagNode; primary?: string }) {
  if (node.actions.length === 0) return null
  return (
    <div className="actions">
      {node.actions.map((action) => (
        <ActionControl
          key={action.name}
          target={node.id}
          action={action}
          variant={defaultVariant(action, primary)}
        />
      ))}
    </div>
  )
}

export function NodeChildren({ node }: { node: DiagNode }) {
  if (node.children.length === 0) return null
  return (
    <div className="node-children">
      {node.children.map((id) => (
        <NodeRenderer key={id} id={id} />
      ))}
    </div>
  )
}
