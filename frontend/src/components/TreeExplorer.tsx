/**
 * TreeExplorer (SPEC section 8): the sidebar rendering the full state tree.
 * Every row subscribes only to its own node, so a patch touching one node
 * re-renders one row, not the whole tree. Clicking a row selects it for the
 * main panel.
 */

import { memo } from 'react'
import { useMirrorStore, useNode } from '../lib/store'
import { selectNode, useIsSelected } from '../lib/selection'

const TreeItem = memo(function TreeItem({ id, depth }: { id: string; depth: number }) {
  const node = useNode(id)
  const selected = useIsSelected(id)
  if (node === undefined) return null
  return (
    <>
      <button
        type="button"
        className={`tree-row${selected ? ' selected' : ''}${node.status === 'gone' ? ' is-gone' : ''}`}
        style={{ paddingLeft: `${depth * 14 + 10}px` }}
        onClick={() => selectNode(id)}
        title={`${node.type} · ${node.status}`}
      >
        <span className={`tree-dot st-${node.status}`} />
        <span className="tree-name">{node.name}</span>
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
