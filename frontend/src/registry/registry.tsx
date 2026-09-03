/**
 * Component registry (SPEC section 8).
 *
 * Maps node `type` → React component. Resolution order:
 *   1. exact match ("telemetry.channel")
 *   2. longest dotted prefix ("job.step" → "job", "device.sim" → "device")
 *   3. GenericNode fallback
 *
 * NodeRenderer is the one entry point for rendering any node: it subscribes
 * to exactly that node (per-node granularity), resolves the component and
 * wraps it in StatusFrame (status badge/border, error card, gone dimming,
 * per-node error boundary).
 */

import { memo } from 'react'
import type { ComponentType } from 'react'
import { useNode } from '../lib/store'
import type { DiagNode } from '../lib/types'
import { StatusFrame } from '../components/StatusFrame'
import { GenericNode } from '../components/GenericNode'
import { JobView } from '../components/JobView'
import { DeviceView } from '../components/DeviceView'
import { ChannelChart } from '../components/ChannelChart'
import { VehicleSessionView } from '../components/VehicleSessionView'
import { VehicleLibraryView } from '../components/VehicleLibraryView'
import { VehicleProfileView } from '../components/VehicleProfileView'
import { EcuView } from '../components/EcuView'

export interface NodeComponentProps {
  node: DiagNode
}

export type NodeComponent = ComponentType<NodeComponentProps>

const registry = new Map<string, NodeComponent>([
  ['job', JobView], // job.step resolves here via prefix
  ['device', DeviceView], // device.sim resolves here via prefix
  ['telemetry.channel', ChannelChart],
  // M1.5 vehicle session (SPEC 15)
  ['vehicle.session', VehicleSessionView],
  ['vehicle.library', VehicleLibraryView],
  ['vehicle.profile', VehicleProfileView],
  ['ecu', EcuView], // ecu.* tool nodes (e.g. ecu.probe) resolve here via prefix
])

/** Register or override a component for a node type (M1 plugin bundles hook in here). */
export function registerNodeComponent(type: string, component: NodeComponent): void {
  registry.set(type, component)
}

/** Resolve a node type to a component: exact, longest dotted prefix, GenericNode. */
export function resolveComponent(type: string): NodeComponent {
  let key = type
  for (;;) {
    const component = registry.get(key)
    if (component) return component
    const dot = key.lastIndexOf('.')
    if (dot === -1) return GenericNode
    key = key.slice(0, dot)
  }
}

/** Render one node (by id) through the registry, wrapped in its StatusFrame. */
export const NodeRenderer = memo(function NodeRenderer({ id }: { id: string }) {
  const node = useNode(id)
  if (!node) return null
  const Component = resolveComponent(node.type)
  return (
    <StatusFrame key={node.id} node={node}>
      <Component node={node} />
    </StatusFrame>
  )
})
