/**
 * UI-local state of the global log drawer (SPEC 14.4): open/collapsed and
 * the live event rate shown in the header toggle. Kept separate from the
 * mirror store, which only hello/patch may mutate — same pattern as
 * selection.ts.
 */

import { create } from 'zustand'

interface LogPanelUiState {
  /** drawer expanded? collapsed by default */
  open: boolean
  /** matching live events per second (sampled over 1 s), for the header hint */
  rate: number
}

export const useLogPanelStore = create<LogPanelUiState>(() => ({ open: false, rate: 0 }))

export function toggleLogPanel(): void {
  useLogPanelStore.setState((s) => ({ open: !s.open }))
}

export function setLogPanelRate(rate: number): void {
  if (useLogPanelStore.getState().rate !== rate) useLogPanelStore.setState({ rate })
}
