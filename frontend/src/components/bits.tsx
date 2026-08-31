/**
 * Small shared building blocks used by the node components:
 * progress bar, props table, action buttons (with pending/result state)
 * and the nested-children renderer.
 */

import { useEffect } from 'react'
import { clearCommandState, sendCommand, useCommandState } from '../lib/commands'
import type { DiagNode, NodeAction } from '../lib/types'
import { NodeRenderer } from '../registry/registry'

const RESULT_FLASH_MS = 4000

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

function ActionButton({ node, action }: { node: DiagNode; action: NodeAction }) {
  const cmd = useCommandState(node.id, action.name)
  const pending = cmd?.status === 'pending'

  // Clear finished results after a short flash.
  useEffect(() => {
    if (cmd === undefined || cmd.status === 'pending') return
    const timer = window.setTimeout(
      () => clearCommandState(node.id, action.name),
      RESULT_FLASH_MS,
    )
    return () => window.clearTimeout(timer)
  }, [cmd, node.id, action.name])

  return (
    <span className="action">
      <button
        type="button"
        className="btn"
        disabled={pending}
        onClick={() => {
          void sendCommand(node.id, action.name, {})
        }}
      >
        {pending ? <span className="spinner spinner-inline" /> : null}
        {action.label}
      </button>
      {cmd?.status === 'ok' ? <span className="action-result ok">ok</span> : null}
      {cmd?.status === 'error' ? (
        <span className="action-result err" title={cmd.error?.detail ?? undefined}>
          {cmd.error?.message ?? 'failed'}
        </span>
      ) : null}
    </span>
  )
}

export function ActionButtons({ node }: { node: DiagNode }) {
  if (node.actions.length === 0) return null
  return (
    <div className="actions">
      {node.actions.map((action) => (
        <ActionButton key={action.name} node={node} action={action} />
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
