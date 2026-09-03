/**
 * Vehicle-domain helpers shared by the session, library and profile views
 * (SPEC 15): reading the compact ECU summary of a profile node and the
 * presentation of its `select` action.
 */

import type { NodeAction } from './types'

export interface EcuSummary {
  id: string
  name: string
  address: string | undefined
}

/** The compact `ecus` summary `[{id, name, address}]` of a profile node (SPEC 15.3). */
export function profileEcus(props: Record<string, unknown>): EcuSummary[] {
  const raw = props['ecus']
  if (!Array.isArray(raw)) return []
  const out: EcuSummary[] = []
  for (const e of raw) {
    if (typeof e !== 'object' || e === null || Array.isArray(e)) continue
    const rec = e as Record<string, unknown>
    const id = typeof rec['id'] === 'string' ? rec['id'] : String(out.length)
    const name = typeof rec['name'] === 'string' && rec['name'] !== '' ? rec['name'] : id
    const address =
      typeof rec['address'] === 'string' || typeof rec['address'] === 'number'
        ? String(rec['address'])
        : undefined
    out.push({ id, name, address })
  }
  return out
}

/** `select` reads as "Connect" in the UI — the spec's own word for it (15.5); the wire name stays `select`. */
export function presentProfileAction(action: NodeAction): NodeAction {
  return action.name === 'select' ? { ...action, label: 'Connect' } : action
}
