/**
 * StatusFrame (SPEC section 8): every node component is wrapped in a frame
 * that renders the status (badge + colored border), the error card for
 * `error`, dimming for `gone`, and a per-node React error boundary — a
 * render crash degrades to an inline error card, never takes down the app.
 * Since M0.5 it also gives every node a "Raw" expander (SPEC 14.4) showing
 * the log/io feed filtered to the node's subtree.
 */

import { Component } from 'react'
import type { ReactNode } from 'react'
import type { DiagNode, NodeError, NodeStatus } from '../lib/types'
import { RawExpander } from './RawFeed'

const STATUS_LABEL: Record<NodeStatus, string> = {
  init: 'init',
  idle: 'idle',
  running: 'running',
  done: 'done',
  error: 'error',
  cancelled: 'cancelled',
  gone: 'gone',
}

export function StatusBadge({ status }: { status: NodeStatus }) {
  return (
    <span className={`status-badge st-${status}`}>
      <span className="status-dot" />
      {STATUS_LABEL[status]}
    </span>
  )
}

export function ErrorCard({ title, error }: { title?: string; error: NodeError }) {
  return (
    <div className="error-card" role="alert">
      <div className="error-card-title">{title ?? 'Error'}</div>
      <div className="error-card-message">{error.message}</div>
      {error.detail ? <pre className="error-card-detail">{error.detail}</pre> : null}
    </div>
  )
}

interface BoundaryProps {
  nodeId: string
  children: ReactNode
}

interface BoundaryState {
  error: Error | null
}

/** Per-node error boundary: a component crash becomes an inline error card. */
class NodeErrorBoundary extends Component<BoundaryProps, BoundaryState> {
  state: BoundaryState = { error: null }

  static getDerivedStateFromError(error: Error): BoundaryState {
    return { error }
  }

  componentDidCatch(error: Error): void {
    console.error(`[render] component for node "${this.props.nodeId}" crashed`, error)
  }

  render(): ReactNode {
    if (this.state.error !== null) {
      return (
        <div className="error-card" role="alert">
          <div className="error-card-title">Component crashed</div>
          <div className="error-card-message">{this.state.error.message}</div>
          <button
            type="button"
            className="btn btn-small"
            onClick={() => this.setState({ error: null })}
          >
            Retry render
          </button>
        </div>
      )
    }
    return this.props.children
  }
}

export function StatusFrame({ node, children }: { node: DiagNode; children: ReactNode }) {
  const gone = node.status === 'gone'
  return (
    <section className={`node-card st-${node.status}${gone ? ' is-gone' : ''}`}>
      <header className="node-head">
        <span className="node-name">{node.name}</span>
        <code className="node-type">{node.type}</code>
        <span className="node-head-spacer" />
        <StatusBadge status={node.status} />
      </header>
      {node.status === 'error' && node.error !== null ? <ErrorCard error={node.error} /> : null}
      <div className="node-body">
        <NodeErrorBoundary nodeId={node.id}>{children}</NodeErrorBoundary>
      </div>
      <RawExpander nodeId={node.id} />
    </section>
  )
}
