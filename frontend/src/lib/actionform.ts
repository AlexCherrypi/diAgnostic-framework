/**
 * Generic action form logic (SPEC 3.1.1): initial field values from the
 * param defaults, validation (required, number bounds, option membership)
 * and the wire `params` object — the option VALUE for selects, never the
 * label, so the audit trail (SPEC 14) records the raw value behind every
 * dropdown choice. Pure functions, so components/ActionForm.tsx stays a thin
 * renderer; plus the hook binding one (target, action) to the command
 * layer's pending/result state.
 *
 * Conditional params (`when`, SPEC 3.1.1): a param tagged
 * `{ "<selectName>": <optionValue> }` is shown, validated and sent only while
 * that select holds the value. The session's `detect` form is the case that
 * matters: with several vehicle plugins it carries every plugin's params
 * tagged `when: {plugin: <id>}` (SPEC 15.2), so params of the plugin that is
 * not selected neither block the submit nor go on the wire. Fields are keyed
 * by `paramKey`, not by name, because the union may carry same-named params
 * of different plugins.
 */

import { useCallback } from 'react'
import { sendCommand, useCommandState } from './commands'
import type { CommandState } from './commands'
import type { ActionParam, ActionParamValue, DiagNode, NodeAction } from './types'

export { RESULT_FLASH_MS } from './commands'

/**
 * Field state as the inputs hold it: strings for text/number, booleans for
 * checkboxes, and for selects the option INDEX as a string ('' = none) — the
 * index maps back to the option object on submit, so a numeric or boolean
 * option value survives the round trip through a DOM `<select>` intact.
 * Keyed by `paramKey(param)`.
 */
export type FieldValues = Record<string, string | boolean>

/** The action named `name` on a node, if it is currently offered. */
export function findAction(node: DiagNode, name: string): NodeAction | undefined {
  return node.actions.find((a) => a.name === name)
}

/**
 * Field key of a param: its name, disambiguated by its `when` condition —
 * the detect form's union may carry a `depth` of plugin a and a `depth` of
 * plugin b, each keeping its own field state while the user switches.
 */
export function paramKey(p: ActionParam): string {
  return p.when === undefined ? p.name : `${p.name}@${JSON.stringify(p.when)}`
}

/** The option a select field currently holds, or undefined when empty. */
function selectedOption(p: ActionParam, values: FieldValues): ActionParamValue | undefined {
  const options = p.options ?? []
  const v = values[paramKey(p)]
  const idx = typeof v === 'string' && v !== '' ? Number(v) : -1
  const option = Number.isInteger(idx) && idx >= 0 ? options[idx] : undefined
  return option?.value
}

/**
 * The param named `key` that `param.when` refers to: with several same-named
 * params the one whose own `when` is compatible with (a subset of) the
 * referencing condition wins — `{depth: 'deep', plugin: 'b'}` resolves to
 * plugin b's `depth`, never plugin a's. Mirrors the backend's rule.
 */
function resolveRef(params: ActionParam[], param: ActionParam, key: string): ActionParam | undefined {
  const condition = param.when ?? {}
  for (const candidate of params) {
    if (candidate.name !== key || candidate === param) continue
    const w = candidate.when
    if (w === undefined || Object.entries(w).every(([k, v]) => condition[k] === v)) return candidate
  }
  return undefined
}

/**
 * Whether a param is in effect for the current field values. A reference the
 * backend would have rejected (unknown name, not a select) never hides a
 * param — better one field too many than a way in that vanished. A select
 * that is itself out of effect satisfies nothing.
 */
function paramApplies(
  params: ActionParam[],
  param: ActionParam,
  values: FieldValues,
  seen: Set<ActionParam> = new Set(),
): boolean {
  if (param.when === undefined) return true
  if (seen.has(param)) return false
  seen.add(param)
  for (const [key, expected] of Object.entries(param.when)) {
    const target = resolveRef(params, param, key)
    if (target === undefined || target.type !== 'select') continue
    if (!paramApplies(params, target, values, seen)) return false
    if (selectedOption(target, values) !== expected) return false
  }
  return true
}

/** The params to show and collect for the current field values, in declaration order. */
export function activeParams(params: ActionParam[], values: FieldValues): ActionParam[] {
  return params.filter((p) => paramApplies(params, p, values))
}

/** Whether an action needs a panel (fields and/or a confirmation) before it can be sent. */
export function actionNeedsPanel(action: NodeAction): boolean {
  return action.params.length > 0 || action.confirm !== undefined
}

export type ButtonVariant = 'default' | 'primary' | 'danger'
export type ButtonSize = 'default' | 'small' | 'large'

/** CSS classes of an action button. */
export function buttonClass(variant: ButtonVariant, size: ButtonSize, extra = ''): string {
  let cls = 'btn'
  if (variant !== 'default') cls += ` btn-${variant}`
  if (size !== 'default') cls += ` btn-${size}`
  if (extra !== '') cls += ` ${extra}`
  return cls
}

/** The variant an action gets by default: `primary` for the named one, `danger` when it asks for confirmation. */
export function defaultVariant(action: NodeAction, primary?: string): ButtonVariant {
  if (primary !== undefined && action.name === primary) return 'primary'
  return action.confirm !== undefined ? 'danger' : 'default'
}

/** Initial field values from the param defaults (every param, conditional ones included). */
export function initialValues(params: ActionParam[]): FieldValues {
  const values: FieldValues = {}
  for (const p of params) {
    const key = paramKey(p)
    switch (p.type) {
      case 'boolean':
        values[key] = p.default === true
        break
      case 'select': {
        const options = p.options ?? []
        let idx = p.default === null ? -1 : options.findIndex((o) => o.value === p.default)
        // A required select without a usable default starts on the first
        // option: one click less, and the value sent is still an option value.
        if (idx === -1 && p.required && options.length > 0) idx = 0
        values[key] = idx === -1 ? '' : String(idx)
        break
      }
      case 'number':
      case 'string':
        values[key] = p.default === null ? '' : String(p.default)
        break
    }
  }
  return values
}

export interface ValidationResult {
  /** wire params `{ name: value }` — meaningful only when `errors` is empty */
  params: Record<string, unknown>
  /** per-field messages, keyed by param name */
  errors: Record<string, string>
}

/**
 * Validate the field values against the param descriptors and build the wire
 * params. Only params in effect (`when`) take part; empty optional
 * text/number/select fields are omitted (the backend applies its own
 * defaults); checkboxes always send true/false. Errors are keyed by param
 * name like the wire params.
 */
export function validateValues(params: ActionParam[], values: FieldValues): ValidationResult {
  const out: Record<string, unknown> = {}
  const errors: Record<string, string> = {}
  for (const p of activeParams(params, values)) {
    const v = values[paramKey(p)]
    switch (p.type) {
      case 'boolean':
        out[p.name] = v === true
        break
      case 'string': {
        const text = typeof v === 'string' ? v.trim() : ''
        if (text === '') {
          if (p.required) errors[p.name] = 'required'
          break
        }
        out[p.name] = text
        break
      }
      case 'number': {
        const text = typeof v === 'string' ? v.trim() : ''
        if (text === '') {
          if (p.required) errors[p.name] = 'required'
          break
        }
        const n = Number(text)
        if (!Number.isFinite(n)) {
          errors[p.name] = 'must be a number'
          break
        }
        if (p.min !== undefined && n < p.min) {
          errors[p.name] = `must be at least ${p.min}`
          break
        }
        if (p.max !== undefined && n > p.max) {
          errors[p.name] = `must be at most ${p.max}`
          break
        }
        out[p.name] = n
        break
      }
      case 'select': {
        const options = p.options ?? []
        const idx = typeof v === 'string' && v !== '' ? Number(v) : -1
        const option = Number.isInteger(idx) && idx >= 0 ? options[idx] : undefined
        if (option === undefined) {
          if (p.required) errors[p.name] = options.length === 0 ? 'no options available' : 'required'
          break
        }
        out[p.name] = option.value
        break
      }
    }
  }
  return { params: out, errors }
}

/** "address=0x2A protocol=uds" — what is about to be sent, for the confirm step. */
export function summarizeParams(params: Record<string, unknown>): string {
  const parts: string[] = []
  for (const [key, value] of Object.entries(params)) {
    let text: string
    if (typeof value === 'string') text = value
    else {
      try {
        text = JSON.stringify(value) ?? String(value)
      } catch {
        text = String(value)
      }
    }
    parts.push(`${key}=${text}`)
  }
  return parts.join('  ')
}

export interface ActionCommand {
  /** latest command state for this (target, action), if any */
  cmd: CommandState | undefined
  pending: boolean
  /** send the command with the given wire params */
  send: (params: Record<string, unknown>) => void
}

/**
 * Bind one action of one node to the command layer: pending/result state
 * (shared by every control for the same target + action) and a stable
 * `send`. Finished results flash for RESULT_FLASH_MS and are cleared by the
 * command layer itself — not by this hook, whose control has typically
 * unmounted before the result even arrives (the state patch precedes it).
 */
export function useActionCommand(target: string, actionName: string): ActionCommand {
  const cmd = useCommandState(target, actionName)

  const send = useCallback(
    (params: Record<string, unknown>) => {
      void sendCommand(target, actionName, params)
    },
    [target, actionName],
  )

  return { cmd, pending: cmd?.status === 'pending', send }
}
