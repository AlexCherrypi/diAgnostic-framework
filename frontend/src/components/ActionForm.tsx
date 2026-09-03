/**
 * Generic action form (SPEC 3.1.1 / 15.5) — the ONLY way action parameters
 * are collected. Generated from `action.params`: text, number (min/max),
 * checkbox, select (the option VALUE goes on the wire, never the label);
 * shows only the params in effect (`when`), validates `required`, applies
 * defaults, shows help text; `confirm` asks before sending — also for
 * param-less actions. Everything renders inline
 * (an expanding panel under the button row, never a blocking modal), and the
 * pending/result state is the command layer's, shared by every control for
 * the same (target, action).
 *
 * - ActionControl: the button every node's actions get (via bits.tsx
 *   ActionButtons, hence GenericNode and every specific view). Param-less
 *   without confirm ⇒ sends `{}` on click; otherwise toggles the panel — or,
 *   with `inline`, shows the form right away (the session hero's detect).
 * - ActionForm: the fields + submit/confirm footer, usable directly where a
 *   view wants the form visible without a toggle.
 */

import { useId, useState } from 'react'
import type { FormEvent, KeyboardEvent, ReactNode } from 'react'
import {
  actionNeedsPanel,
  activeParams,
  buttonClass,
  initialValues,
  paramKey,
  summarizeParams,
  useActionCommand,
  validateValues,
} from '../lib/actionform'
import type { ButtonSize, ButtonVariant, FieldValues } from '../lib/actionform'
import type { CommandState } from '../lib/commands'
import type { ActionParam, NodeAction } from '../lib/types'

/** The ok / error chip shown after a command finished (cleared by the flash timer). */
export function CommandResult({ cmd }: { cmd: CommandState | undefined }) {
  if (cmd === undefined || cmd.status === 'pending') return null
  if (cmd.status === 'ok') return <span className="action-result ok">ok</span>
  return (
    <span className="action-result err" title={cmd.error?.detail ?? undefined}>
      {cmd.error?.message ?? 'failed'}
    </span>
  )
}

// ---------------------------------------------------------------------------
// Fields
// ---------------------------------------------------------------------------

interface FieldProps {
  param: ActionParam
  value: string | boolean | undefined
  error: string | undefined
  idPrefix: string
  autoFocus: boolean
  onChange: (value: string | boolean) => void
}

function Field({ param, value, error, idPrefix, autoFocus, onChange }: FieldProps) {
  const id = `${idPrefix}-${param.name}`
  const errId = `${id}-err`
  const invalid = error !== undefined
  const help = param.help !== undefined ? <span className="af-help">{param.help}</span> : null
  const err = invalid ? (
    <span className="af-error" id={errId} role="alert">
      {error}
    </span>
  ) : null

  if (param.type === 'boolean') {
    return (
      <div className="af-field af-field-check">
        <label className="af-check" htmlFor={id}>
          <input
            id={id}
            type="checkbox"
            checked={value === true}
            autoFocus={autoFocus}
            onChange={(e) => onChange(e.target.checked)}
          />
          <span>{param.label}</span>
        </label>
        {help}
        {err}
      </div>
    )
  }

  const text = typeof value === 'string' ? value : ''
  const inputClass = `af-input${invalid ? ' invalid' : ''}`
  let control: ReactNode
  if (param.type === 'select') {
    const options = param.options ?? []
    control = (
      <select
        id={id}
        className={inputClass}
        value={text}
        autoFocus={autoFocus}
        disabled={options.length === 0}
        aria-required={param.required}
        aria-invalid={invalid}
        aria-describedby={invalid ? errId : undefined}
        onChange={(e) => onChange(e.target.value)}
      >
        {options.length === 0 ? (
          <option value="">no options available</option>
        ) : !param.required || text === '' ? (
          <option value="">{param.placeholder ?? '— select —'}</option>
        ) : null}
        {options.map((o, i) => (
          <option key={i} value={String(i)}>
            {o.label}
          </option>
        ))}
      </select>
    )
  } else {
    const isNumber = param.type === 'number'
    control = (
      <input
        id={id}
        className={inputClass}
        type={isNumber ? 'number' : 'text'}
        value={text}
        placeholder={param.placeholder}
        min={isNumber ? param.min : undefined}
        max={isNumber ? param.max : undefined}
        step={isNumber ? 'any' : undefined}
        inputMode={isNumber ? 'decimal' : undefined}
        autoFocus={autoFocus}
        autoComplete="off"
        spellCheck={false}
        aria-required={param.required}
        aria-invalid={invalid}
        aria-describedby={invalid ? errId : undefined}
        onChange={(e) => onChange(e.target.value)}
      />
    )
  }

  const range =
    param.type === 'number' && (param.min !== undefined || param.max !== undefined)
      ? `${param.min ?? '…'} – ${param.max ?? '…'}`
      : null

  return (
    <div className="af-field">
      <label className="af-label" htmlFor={id}>
        {param.label}
        {param.required ? (
          <span className="af-req" aria-hidden="true">
            *
          </span>
        ) : null}
        {range !== null ? <span className="af-range">{range}</span> : null}
      </label>
      {control}
      {help}
      {err}
    </div>
  )
}

// ---------------------------------------------------------------------------
// The form
// ---------------------------------------------------------------------------

export interface ActionFormProps {
  /** node id the command targets */
  target: string
  action: NodeAction
  variant?: ButtonVariant
  size?: ButtonSize
  /** offered as a Cancel button and on Escape */
  onCancel?: () => void
  /** called right after the command was sent */
  onSent?: () => void
  /** focus the first field on mount (not for forms open at page load) */
  autoFocus?: boolean
  /** the parent shows the result chip itself */
  hideResult?: boolean
}

export function ActionForm({
  target,
  action,
  variant = 'default',
  size = 'default',
  onCancel,
  onSent,
  autoFocus = false,
  hideResult = false,
}: ActionFormProps) {
  const { cmd, pending, send } = useActionCommand(target, action.name)
  const [values, setValues] = useState<FieldValues>(() => initialValues(action.params))
  const [errors, setErrors] = useState<Record<string, string>>({})
  /** validated wire params awaiting the user's confirmation, or null */
  const [confirming, setConfirming] = useState<Record<string, unknown> | null>(null)
  const idPrefix = useId()

  const hasFields = action.params.length > 0
  // The params in effect for the current values (`when`, SPEC 3.1.1) — a
  // select change may reveal or hide dependent fields; their values persist.
  const fields = activeParams(action.params, values)
  // A param-less action with `confirm` has nothing to fill in: straight to the question.
  const confirmStage = confirming !== null || (!hasFields && action.confirm !== undefined)
  const submitClass = buttonClass(variant, size)
  const spinner = pending ? <span className="spinner spinner-inline" /> : null
  const result = hideResult ? null : <CommandResult cmd={cmd} />

  const setValue = (key: string, name: string, value: string | boolean) => {
    setValues((prev) => ({ ...prev, [key]: value }))
    if (name in errors) {
      setErrors((prev) => {
        const next = { ...prev }
        delete next[name]
        return next
      })
    }
  }

  const submit = (e: FormEvent<HTMLFormElement>) => {
    e.preventDefault()
    if (pending) return
    const validation = validateValues(action.params, values)
    if (Object.keys(validation.errors).length > 0) {
      setErrors(validation.errors)
      return
    }
    setErrors({})
    if (action.confirm !== undefined) {
      setConfirming(validation.params)
      return
    }
    send(validation.params)
    onSent?.()
  }

  const confirm = () => {
    if (pending) return
    send(confirming ?? {})
    setConfirming(null)
    onSent?.()
  }

  const back = () => {
    if (confirming !== null) setConfirming(null)
    else onCancel?.()
  }

  const onKeyDown = (e: KeyboardEvent<HTMLElement>) => {
    if (e.key === 'Escape' && onCancel !== undefined) {
      e.preventDefault()
      onCancel()
    }
  }

  if (confirmStage) {
    const summary = confirming !== null ? summarizeParams(confirming) : ''
    return (
      <div className="af af-confirm" role="group" aria-label={`Confirm ${action.label}`} onKeyDown={onKeyDown}>
        <span className="af-confirm-text">{action.confirm ?? `${action.label}?`}</span>
        {summary !== '' ? <code className="af-confirm-params">{summary}</code> : null}
        <div className="af-footer">
          <button type="button" className={submitClass} disabled={pending} onClick={confirm} autoFocus>
            {spinner}
            {action.label}
          </button>
          {hasFields || onCancel !== undefined ? (
            <button type="button" className={buttonClass('default', size)} onClick={back}>
              {hasFields ? 'Back' : 'Cancel'}
            </button>
          ) : null}
          {result}
        </div>
      </div>
    )
  }

  return (
    <form className="af" onSubmit={submit} onKeyDown={onKeyDown} noValidate>
      {hasFields ? (
        <div className="af-fields">
          {fields.map((param, i) => (
            <Field
              key={paramKey(param)}
              param={param}
              value={values[paramKey(param)]}
              error={errors[param.name]}
              idPrefix={idPrefix}
              autoFocus={autoFocus && i === 0}
              onChange={(v) => setValue(paramKey(param), param.name, v)}
            />
          ))}
        </div>
      ) : null}
      <div className="af-footer">
        <button type="submit" className={submitClass} disabled={pending}>
          {spinner}
          {action.label}
        </button>
        {onCancel !== undefined ? (
          <button type="button" className={buttonClass('default', size)} onClick={onCancel}>
            Cancel
          </button>
        ) : null}
        {result}
      </div>
    </form>
  )
}

// ---------------------------------------------------------------------------
// The button (+ panel)
// ---------------------------------------------------------------------------

export interface ActionControlProps {
  /** node id the command targets */
  target: string
  action: NodeAction
  variant?: ButtonVariant
  size?: ButtonSize
  /**
   * Show the form right away instead of behind a toggle (only matters for
   * actions with params or confirm) — e.g. the session hero's detect form.
   */
  inline?: boolean
}

/**
 * One action of one node. Rendered inside an `.actions` row: the panel is a
 * sibling that wraps onto its own full-width line under the buttons, so the
 * row stays intact and nothing blocks the rest of the page.
 */
export function ActionControl({
  target,
  action,
  variant = 'default',
  size = 'default',
  inline = false,
}: ActionControlProps) {
  const { cmd, pending, send } = useActionCommand(target, action.name)
  const [open, setOpen] = useState(false)
  const needsPanel = actionNeedsPanel(action)

  if (!needsPanel) {
    return (
      <span className="action">
        <button
          type="button"
          className={buttonClass(variant, size)}
          disabled={pending}
          onClick={() => send({})}
        >
          {pending ? <span className="spinner spinner-inline" /> : null}
          {action.label}
        </button>
        <CommandResult cmd={cmd} />
      </span>
    )
  }

  if (inline) {
    return (
      <div className="action action-inline">
        <ActionForm target={target} action={action} variant={variant} size={size} />
      </div>
    )
  }

  return (
    <>
      <span className="action">
        <button
          type="button"
          className={buttonClass(variant, size, open ? 'active' : '')}
          disabled={pending}
          aria-expanded={open}
          onClick={() => setOpen((v) => !v)}
        >
          {pending ? <span className="spinner spinner-inline" /> : null}
          {action.label}
          <span className="btn-ellipsis" aria-hidden="true">
            …
          </span>
        </button>
        {open ? null : <CommandResult cmd={cmd} />}
      </span>
      {open ? (
        <div className="action-panel">
          <div className="action-panel-title">{action.label}</div>
          <ActionForm
            target={target}
            action={action}
            variant={variant}
            size={size}
            autoFocus
            onCancel={() => setOpen(false)}
            onSent={() => setOpen(false)}
          />
        </div>
      ) : null}
    </>
  )
}
