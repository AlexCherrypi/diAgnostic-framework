/**
 * JobView (SPEC section 8 / 7): renderer for `job` nodes and, via prefix
 * resolution, `job.step`. Shows the progress bar, lock-wait info, the action
 * buttons from the backend (`cancel` while init/running) and the step list
 * (children rendered through the registry, so steps nest arbitrarily).
 */

import type { NodeComponentProps } from '../registry/registry'
import { ActionButtons, NodeChildren, ProgressBar } from './bits'

export function JobView({ node }: NodeComponentProps) {
  const waitingFor = node.props['waitingFor']
  return (
    <>
      <ProgressBar value={node.progress} indeterminate={node.status === 'running'} />
      {node.status === 'init' && Array.isArray(waitingFor) && waitingFor.length > 0 ? (
        <div className="job-waiting">
          waiting for lock{waitingFor.length > 1 ? 's' : ''}:{' '}
          {waitingFor.map((l) => (
            <code key={String(l)} className="lock-chip">
              {String(l)}
            </code>
          ))}
        </div>
      ) : null}
      <ActionButtons node={node} />
      <NodeChildren node={node} />
    </>
  )
}
