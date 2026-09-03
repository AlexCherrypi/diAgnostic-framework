/**
 * TreeExplorer (SPEC section 8): the sidebar rendering the full state tree.
 * Every row subscribes only to its own node, so a patch touching one node
 * re-renders one row, not the whole tree. Clicking a row selects it for the
 * main panel. The `vehicle` session node (SPEC 15.2) keeps the backend's
 * order but reads as the primary entry: emphasized label plus its session
 * state as a hint.
 */

import { memo } from 'react'
import { useMirrorStore, useNode } from '../lib/store'
import { selectNode, useIsSelected } from '../lib/selection'

const TreeItem = memo(function TreeItem({ id, depth }: { id: string; depth: number }) {
  const node = useNode(id)
  const selected = useIsSelected(id)
  if (node === undefined) return null
  const primary = node.type === 'vehicle.session'
  const state = primary ? node.props['state'] : undefined
  const hint = typeof state === 'string' && state !== '' && state !== 'none' ? state : null
  return (
    <>
      <button
        type="button"
        className={`tree-row${selected ? ' selected' : ''}${node.status === 'gone' ? ' is-gone' : ''}${primary ? ' tree-row-primary' : ''}`}
        style={{ paddingLeft: `${depth * 14 + 10}px` }}
        onClick={() => selectNode(id)}
        title={`${node.type} · ${node.status}`}
      >
        <span className={`tree-dot st-${node.status}`} />
        <span className="tree-name">{node.name}</span>
        {hint !== null ? <span className="tree-hint">{hint}</span> : null}
      </button>
      {node.children.map((childId) => (
        <TreeItem key={childId} id={childId} depth={depth + 1} />
      ))}
    </>
  )
})

export function TreeExplorer() {
  const rootId = useMirrorStore((s) => s.rootId)
  return (
    <nav className="tree" aria-label="State tree">
      {rootId !== null ? (
        <TreeItem id={rootId} depth={0} />
      ) : (
        <div className="tree-empty">no state yet</div>
      )}
    </nav>
  )
}
