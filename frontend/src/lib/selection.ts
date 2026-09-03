/**
 * UI-local selection state (which node the main panel shows). Kept separate
 * from the mirror store, which only hello/patch may mutate.
 *
 * Default selection (nothing selected yet, or the selected node vanished):
 * the `vehicle` session node when the tree has one — the first screen
 * answers "which vehicle?" (SPEC 15.5) — else the root (SPEC 8). An explicit
 * selection always wins while its node exists and survives reconnects (a
 * hello that still carries the node keeps it); the default is never applied
 * over it.
 *
 * An explicit selection whose node leaves the tree is cleared, not merely
 * shadowed: session children are re-declared under stable ids on every
 * detect/attach (`vehicle:ecu:<id>`, tool nodes), and a stale id that
 * silently came back would hijack the main panel — e.g. jump from the
 * detection progress to an ECU card seconds into a re-detect — without any
 * click. After the node is gone the panel stays on the fallback (vehicle)
 * page until the user selects again; this also applies to a device node
 * re-declared after a plugin restart.
 */

import { create } from 'zustand'
import { findRootChild, useMirrorStore } from './store'
import type { MirrorState } from './store'

interface SelectionState {
  /** null → fall back to the default selection */
  selectedId: string | null
}

export const useSelectionStore = create<SelectionState>(() => ({ selectedId: null }))

export function selectNode(id: string): void {
  useSelectionStore.setState({ selectedId: id })
}

// Every patch commits all its ops in one setState (store.ts), so a remove+add
// of the same id inside one patch never counts as "gone".
useMirrorStore.subscribe((state, prev) => {
  const id = useSelectionStore.getState().selectedId
  if (id !== null && id in prev.nodes && !(id in state.nodes)) {
    useSelectionStore.setState({ selectedId: null })
  }
})

function defaultSelectionId(state: MirrorState): string | null {
  const vehicle = findRootChild(state, 'vehicle.session')
  return vehicle !== undefined ? vehicle.id : state.rootId
}

/**
 * The node id the main panel should render: the explicit selection while it
 * still exists in the tree, otherwise the default selection.
 */
export function useEffectiveSelection(): string | null {
  const selectedId = useSelectionStore((s) => s.selectedId)
  return useMirrorStore((s) =>
    selectedId !== null && selectedId in s.nodes ? selectedId : defaultSelectionId(s),
  )
}

/** Whether `id` is the effective selection (for tree row highlighting). */
export function useIsSelected(id: string): boolean {
  return useEffectiveSelection() === id
}
