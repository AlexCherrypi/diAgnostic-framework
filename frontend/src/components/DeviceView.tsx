/**
 * DeviceView (SPEC section 8): renderer for `device` and `device.*` nodes.
 * Device properties, its actions (e.g. selftest) and its children — typically
 * telemetry channels rendered as live charts.
 */

import type { NodeComponentProps } from '../registry/registry'
import { ActionButtons, NodeChildren, ProgressBar, PropsTable } from './bits'

export function DeviceView({ node }: NodeComponentProps) {
  return (
    <>
      <ProgressBar value={node.progress} />
      <PropsTable props={node.props} />
      <ActionButtons node={node} />
      <NodeChildren node={node} />
    </>
  )
}
