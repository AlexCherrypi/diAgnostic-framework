/**
 * Typed accessors for `node.props` (a JSON object per SPEC 3.1). Views read
 * the well-known keys through these so a missing or mistyped prop renders as
 * "absent" instead of crashing the component.
 */

/** A non-empty string prop, or undefined. */
export function propString(props: Record<string, unknown>, key: string): string | undefined {
  const v = props[key]
  return typeof v === 'string' && v !== '' ? v : undefined
}

/** A string or number prop as display text (e.g. an ECU address), or undefined. */
export function propText(props: Record<string, unknown>, key: string): string | undefined {
  const v = props[key]
  if (typeof v === 'string') return v !== '' ? v : undefined
  if (typeof v === 'number' && Number.isFinite(v)) return String(v)
  return undefined
}

/** A finite number prop, or undefined. */
export function propNumber(props: Record<string, unknown>, key: string): number | undefined {
  const v = props[key]
  return typeof v === 'number' && Number.isFinite(v) ? v : undefined
}

/** A JSON-object prop, or undefined. */
export function propRecord(
  props: Record<string, unknown>,
  key: string,
): Record<string, unknown> | undefined {
  const v = props[key]
  return typeof v === 'object' && v !== null && !Array.isArray(v)
    ? (v as Record<string, unknown>)
    : undefined
}
