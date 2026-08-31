/**
 * UI-local selection state (which node the main panel shows). Kept separate
 * from the mirror store, which only hello/patch may mutate.
 */

import { create } from 'zustand'
import { useMirrorStore } from './store'

interface SelectionState {
  /** null → fall back to the root node */
  selectedId: string | null
}

export const useSelectionStore = create<SelectionState>(() => ({ selectedId: null }))

export function selectNode(id: string): void {
  useSelectionStore.setState({ selectedId: id })
}

/**
 * The node id the main panel should render: the explicit selection while it
 * still exists in the tree, otherwise the root (default selection).
 */
export function useEffectiveSelection(): string | null {
  const selectedId = useSelectionStore((s) => s.selectedId)
  return useMirrorStore((s) =>
    selectedId !== null && selectedId in s.nodes ? selectedId : s.rootId,
  )
}

/** Whether `id` is the effective selection (for tree row highlighting). */
export function useIsSelected(id: string): boolean {
  const selectedId = useSelectionStore((s) => s.selectedId)
  const rootId = useMirrorStore((s) => s.rootId)
  return (selectedId ?? rootId) === id
}
