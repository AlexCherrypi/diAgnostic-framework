/**
 * EcuView (SPEC 15.4 / 15.5): compact card for plugin-owned `ecu` nodes —
 * address, protocol and profile ecu id up front, every other prop (e.g. what
 * `read_ids` filled in) in the props table, the plugin's actions through the
 * generic action form, nested children (a per-ECU job) via the registry. The
 * name, status badge, error card and the Raw expander come from StatusFrame.
 * Tool nodes typed `ecu.*` (e.g. the demo's `ecu.probe`) resolve here via
 * prefix and simply have no meta line.
 */

import { propText } from '../lib/props'
import type { NodeComponentProps } from '../registry/registry'
import { ActionButtons, NodeChildren, ProgressBar, PropsTable } from './bits'

const META_KEYS = ['address', 'protocol', 'ecuId'] as const

export function EcuView({ node }: NodeComponentProps) {
  const address = propText(node.props, 'address')
  const protocol = propText(node.props, 'protocol')
  const ecuId = propText(node.props, 'ecuId')
  const rest: Record<string, unknown> = {}
  for (const [key, value] of Object.entries(node.props)) {
    if (!(META_KEYS as readonly string[]).includes(key)) rest[key] = value
  }
  return (
    <>
      {address !== undefined || protocol !== undefined || ecuId !== undefined ? (
        <div className="ecu-meta">
          {address !== undefined ? (
            <span className="ecu-fact">
              <span className="ecu-k">address</span>
              <code>{address}</code>
            </span>
          ) : null}
          {protocol !== undefined ? (
            <span className="ecu-fact">
              <span className="ecu-k">protocol</span>
              <span>{protocol}</span>
            </span>
          ) : null}
          {ecuId !== undefined ? (
            <span className="ecu-fact">
              <span className="ecu-k">id</span>
              <code>{ecuId}</code>
            </span>
          ) : null}
        </div>
      ) : null}
      <ProgressBar value={node.progress} />
      <PropsTable props={rest} />
      <ActionButtons node={node} />
      <NodeChildren node={node} />
    </>
  )
}
