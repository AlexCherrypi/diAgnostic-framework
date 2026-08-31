/**
 * GenericNode (SPEC section 8): the universal fallback renderer. Together
 * with the surrounding StatusFrame (name, type, status badge, error card)
 * it shows progress, the props table, action buttons generated from
 * node.actions and nested children via the registry — so any backend object
 * is usable in the UI with zero frontend work.
 */

import type { NodeComponentProps } from '../registry/registry'
import { ActionButtons, NodeChildren, ProgressBar, PropsTable } from './bits'

export function GenericNode({ node }: NodeComponentProps) {
  return (
    <>
      <ProgressBar value={node.progress} />
      <PropsTable props={node.props} />
      <ActionButtons node={node} />
      <NodeChildren node={node} />
    </>
  )
}
