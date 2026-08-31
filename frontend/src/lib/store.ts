/**
 * Zustand mirror store — the frontend's copy of the backend state tree.
 *
 * Per SPEC section 8 the tree data ({ seq, rootId, nodes }) is mutated ONLY by
 * `hello` / `patch` messages (applied by src/lib/ws.ts). `connection` reflects
 * the /ws/state socket state.
 *
 * Re-render granularity: every op copies only the nodes it touches, so a
 * `useNode(id)` subscriber re-renders only when its own node object changes.
 */

import { create } from 'zustand'
import type {
  ConnectionState,
  DiagNode,
  HelloMsg,
  PatchOp,
} from './types'

export interface MirrorState {
  connection: ConnectionState
  seq: number
  rootId: string | null
  nodes: Record<string, DiagNode>
}

export const useMirrorStore = create<MirrorState>(() => ({
  connection: 'connecting',
  seq: -1,
  rootId: null,
  nodes: {},
}))

/** Select one node with per-node subscription granularity. */
export function useNode(id: string | null | undefined): DiagNode | undefined {
  return useMirrorStore((s) => (id ? s.nodes[id] : undefined))
}

// ---------------------------------------------------------------------------
// Mutators — called exclusively by the /ws/state client (ws.ts).
// ---------------------------------------------------------------------------

export function setConnectionState(connection: ConnectionState): void {
  useMirrorStore.setState({ connection })
}

/** Replace the whole mirror with a fresh snapshot. */
export function applyHello(msg: HelloMsg): void {
  useMirrorStore.setState({
    seq: msg.seq,
    rootId: msg.snapshot.rootId,
    nodes: msg.snapshot.nodes,
  })
}

/**
 * Apply one patch (already gap-checked by the caller).
 * Returns false if an op referenced unknown state — the caller should resync.
 */
export function applyPatchOps(seq: number, ops: PatchOp[]): boolean {
  const state = useMirrorStore.getState()
  const nodes: Record<string, DiagNode> = { ...state.nodes }
  let ok = true

  for (const op of ops) {
    switch (op.op) {
      case 'add': {
        const node = op.node
        nodes[node.id] = node
        if (node.parentId !== null) {
          const parent = nodes[node.parentId]
          if (!parent) {
            ok = false
            break
          }
          if (!parent.children.includes(node.id)) {
            const children = [...parent.children]
            if (op.index !== undefined && op.index >= 0 && op.index <= children.length) {
              children.splice(op.index, 0, node.id)
            } else {
              children.push(node.id)
            }
            nodes[parent.id] = { ...parent, children }
          }
        }
        break
      }
      case 'update': {
        const existing = nodes[op.id]
        if (!existing) {
          ok = false
          break
        }
        // Top-level keys REPLACE the node's keys ("props" replaces all props).
        nodes[op.id] = { ...existing, ...op.changes, id: existing.id }
        break
      }
      case 'merge_props': {
        const existing = nodes[op.id]
        if (!existing) {
          ok = false
          break
        }
        nodes[op.id] = { ...existing, props: { ...existing.props, ...op.props } }
        break
      }
      case 'remove': {
        const target = nodes[op.id]
        if (!target) {
          ok = false
          break
        }
        // Remove the node and all descendants.
        const stack = [op.id]
        while (stack.length > 0) {
          const id = stack.pop() as string
          const n = nodes[id]
          if (n) stack.push(...n.children)
          delete nodes[id]
        }
        // Remove the id from its parent's children.
        if (target.parentId !== null) {
          const parent = nodes[target.parentId]
          if (parent && parent.children.includes(op.id)) {
            nodes[parent.id] = {
              ...parent,
              children: parent.children.filter((c) => c !== op.id),
            }
          }
        }
        break
      }
    }
    if (!ok) break
  }

  if (ok) {
    useMirrorStore.setState({ seq, nodes })
  }
  return ok
}
