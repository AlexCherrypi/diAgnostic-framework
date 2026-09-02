/**
 * Global log panel (SPEC 14.4): a collapsible bottom drawer across the main
 * area showing the LogHub live tail.
 *
 * - Collapsed by default; the header toggle (LogPanelToggle) shows a live
 *   event-rate hint. The panel component stays mounted while collapsed so
 *   its /ws/logs subscription keeps feeding the rate hint and expanding the
 *   drawer is instant.
 * - Live tail with autoscroll. Scrolling up auto-pauses (scrolling back to
 *   the bottom resumes); the pause button and expanding a row pause
 *   manually; a banner shows how many live events arrived while paused.
 * - Filters (minimum level, category multi-toggle, free-text search,
 *   selected-subtree toggle) are applied server-side: every change reopens
 *   the subscription (one active filter per connection, SPEC 14.3) and the
 *   fresh view starts live — a pause taken in the previous view is
 *   discarded, never resurrected when the same filter comes back.
 *   At least one category always stays selected: the server treats an
 *   empty `categories` list as "no constraint" (SPEC 14.3), so sending `[]`
 *   would show everything under a filter bar that shows nothing selected.
 * - Every row expands to the full message + meta, the pretty-printed data
 *   JSON and the raw hex block — all copyable.
 * - Rendering stays smooth at ~100 events/s: rows are memoized and keyed by
 *   the event number, the live tail renders a capped slice, and the full
 *   client-side buffer (2000 events) is only materialized while paused.
 */

import { memo, useCallback, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react'
import type { UIEvent } from 'react'
import {
  formatHexBlock,
  formatLogTime,
  hexByteCount,
  levelsAtOrAbove,
  logFilterKey,
  safeJson,
  useLogFeed,
} from '../lib/logs'
import { setLogPanelRate, toggleLogPanel, useLogPanelStore } from '../lib/logpanel'
import { useEffectiveSelection } from '../lib/selection'
import { useNode } from '../lib/store'
import { LOG_CATEGORIES, LOG_LEVELS } from '../lib/types'
import type { LogCategory, LogEvent, LogFilter, LogLevel } from '../lib/types'

/** Rows rendered while live-tailing (the full 2000-event buffer shows when paused). */
const LIVE_RENDER_CAP = 400
/** Distance from the bottom (px) still counting as "at the bottom". */
const AT_BOTTOM_SLACK_PX = 48
/** Typing in the search box reopens the subscription only after this quiet period. */
const SEARCH_DEBOUNCE_MS = 300
const RATE_SAMPLE_MS = 1000

// ---------------------------------------------------------------------------
// Header toggle
// ---------------------------------------------------------------------------

/** Header button: toggles the drawer and shows the live event-rate hint. */
export function LogPanelToggle() {
  const open = useLogPanelStore((s) => s.open)
  const rate = useLogPanelStore((s) => s.rate)
  return (
    <button
      type="button"
      className={`log-toggle${open ? ' active' : ''}`}
      onClick={toggleLogPanel}
      aria-expanded={open}
      title={rate > 0 ? `Toggle the log panel — ${rate} events/s` : 'Toggle the log panel'}
    >
      Logs
      {rate > 0 ? <span className="log-toggle-rate">{rate}/s</span> : null}
      <span className="log-toggle-caret" aria-hidden="true">
        {open ? '▾' : '▴'}
      </span>
    </button>
  )
}

// ---------------------------------------------------------------------------
// Rows
// ---------------------------------------------------------------------------

const LEVEL_ABBR: Record<LogLevel, string> = {
  trace: 'TRC',
  debug: 'DBG',
  info: 'INF',
  warn: 'WRN',
  error: 'ERR',
}

function CopyButton({ text, label }: { text: string; label: string }) {
  const [copied, setCopied] = useState(false)
  return (
    <button
      type="button"
      className="btn btn-small"
      onClick={() => {
        void navigator.clipboard?.writeText(text).then(
          () => {
            setCopied(true)
            window.setTimeout(() => setCopied(false), 1500)
          },
          () => {
            // clipboard unavailable — text stays selectable by hand
          },
        )
      }}
    >
      {copied ? 'copied' : label}
    </button>
  )
}

function LogRowDetail({ ev }: { ev: LogEvent }) {
  const dataJson = ev.data !== undefined ? safeJson(ev.data) : null
  return (
    <div className="log-detail">
      <div className="log-detail-block log-detail-wide">
        <div className="log-detail-head">
          <span>
            #{ev.n} · {ev.level} · {ev.category} · <code>{ev.source}</code>
            {ev.nodeId !== null ? (
              <>
                {' · '}
                <code>{ev.nodeId}</code>
              </>
            ) : null}
          </span>
          <span className="log-detail-actions">
            <CopyButton text={ev.message} label="copy message" />
            <CopyButton text={safeJson(ev)} label="copy event" />
          </span>
        </div>
        <pre className="log-detail-pre">{ev.message}</pre>
      </div>
      {dataJson !== null ? (
        <div className="log-detail-block">
          <div className="log-detail-head">
            data
            <span className="log-detail-actions">
              <CopyButton text={dataJson} label="copy" />
            </span>
          </div>
          <pre className="log-detail-pre">{dataJson}</pre>
        </div>
      ) : null}
      {ev.raw !== undefined ? (
        <div className="log-detail-block">
          <div className="log-detail-head">
            raw · {hexByteCount(ev.raw)} bytes
            <span className="log-detail-actions">
              <CopyButton text={ev.raw} label="copy hex" />
            </span>
          </div>
          <pre className="log-detail-pre log-hex">{formatHexBlock(ev.raw)}</pre>
        </div>
      ) : null}
    </div>
  )
}

interface LogRowProps {
  ev: LogEvent
  /** called when the row is expanded (the panel pauses so the row stays put) */
  onExpand: () => void
}

const LogRow = memo(function LogRow({ ev, onExpand }: LogRowProps) {
  const [expanded, setExpanded] = useState(false)
  return (
    <div className={`log-row lv-${ev.level}${expanded ? ' expanded' : ''}`} data-n={ev.n}>
      <button
        type="button"
        className="log-line"
        aria-expanded={expanded}
        onClick={() => {
          if (!expanded) onExpand()
          setExpanded(!expanded)
        }}
      >
        <span className="log-caret" aria-hidden="true">
          {expanded ? '▾' : '▸'}
        </span>
        <span className="log-time">{formatLogTime(ev.ts)}</span>
        <span className={`log-chip lv-${ev.level}`}>{LEVEL_ABBR[ev.level]}</span>
        <span className="log-cat">{ev.category}</span>
        <span className="log-source" title={ev.source}>
          {ev.source}
        </span>
        <span className="log-node" title={ev.nodeId ?? undefined}>
          {ev.nodeId ?? '—'}
        </span>
        <span className="log-msg" title={ev.message}>
          {ev.message}
        </span>
        {ev.data !== undefined ? <span className="log-flag">data</span> : null}
        {ev.raw !== undefined ? <span className="log-flag">{hexByteCount(ev.raw)} B</span> : null}
      </button>
      {expanded ? <LogRowDetail ev={ev} /> : null}
    </div>
  )
})

// ---------------------------------------------------------------------------
// The drawer
// ---------------------------------------------------------------------------

type PauseMode = 'auto' | 'manual'

interface PauseState {
  /** `auto`: scrolled up — scrolling back down resumes. `manual`: button / row expand. */
  mode: PauseMode
  /** the filter key this pause belongs to — a filter change opens a fresh view, which starts live */
  key: string
  /** the buffer frozen at pause time (full client-side cap, oldest first) */
  frozen: LogEvent[]
  /** liveReceived at pause time — the banner counts live events since */
  liveAt: number
}

/** Latest render values for the (stable) event handlers. */
interface Latest {
  events: LogEvent[]
  liveReceived: number
  filterKey: string
  pauseMode: PauseMode | null
}

/** A row to keep in place across a re-layout: its event number + viewport offset. */
interface ScrollAnchor {
  n: number
  offset: number
}

/** The first row visible in the scroller, or null when there is none. */
function captureAnchor(el: HTMLElement): ScrollAnchor | null {
  const top = el.getBoundingClientRect().top
  for (const child of el.children) {
    if (!(child instanceof HTMLElement) || child.dataset['n'] === undefined) continue
    const rect = child.getBoundingClientRect()
    if (rect.bottom > top) return { n: Number(child.dataset['n']), offset: rect.top - top }
  }
  return null
}

/** Scroll so the anchored row sits where it was captured (no-op if it is gone). */
function restoreAnchor(el: HTMLElement, anchor: ScrollAnchor): void {
  const row = el.querySelector<HTMLElement>(`[data-n="${anchor.n}"]`)
  if (row === null) return
  const top = el.getBoundingClientRect().top
  el.scrollTop += row.getBoundingClientRect().top - top - anchor.offset
}

export function LogPanel() {
  const open = useLogPanelStore((s) => s.open)

  // --- filter state -------------------------------------------------------
  const [minLevel, setMinLevel] = useState<LogLevel>('trace')
  const [cats, setCats] = useState<ReadonlySet<LogCategory>>(() => new Set(LOG_CATEGORIES))
  const [searchInput, setSearchInput] = useState('')
  const [search, setSearch] = useState('')
  const [subtreeOnly, setSubtreeOnly] = useState(false)

  const selectedId = useEffectiveSelection()
  const selectedNode = useNode(subtreeOnly ? selectedId : null)

  useEffect(() => {
    const t = window.setTimeout(() => setSearch(searchInput.trim()), SEARCH_DEBOUNCE_MS)
    return () => window.clearTimeout(t)
  }, [searchInput])

  const filter = useMemo<LogFilter>(() => {
    const f: LogFilter = {}
    const levels = levelsAtOrAbove(minLevel)
    if (levels !== undefined) f.levels = levels
    // Never send an empty list: the server reads `[]` as "no constraint"
    // (loghub LogFilter / SPEC 14.3). toggleCat keeps the set non-empty; this
    // guard makes the wire filter honest even if that invariant ever slips.
    if (cats.size > 0 && cats.size < LOG_CATEGORIES.length) {
      f.categories = LOG_CATEGORIES.filter((c) => cats.has(c))
    }
    if (search !== '') f.search = search
    if (subtreeOnly && selectedId !== null) f.nodeId = selectedId
    return f
  }, [minLevel, cats, search, subtreeOnly, selectedId])
  const filterKey = logFilterKey(filter)

  const { events, liveReceived, connected } = useLogFeed(filter)

  // --- pause / autoscroll -------------------------------------------------
  const [pauseState, setPauseState] = useState<PauseState | null>(null)
  // A pause belongs to the view it was taken in; a filter change DISCARDS it
  // (adjust-state-during-render): the frozen buffer and the `liveAt`
  // baseline came from a LogSubscription that is disposed with the old
  // filter, so keeping the pause around would resurrect stale rows with a
  // wrong "new events" count if the same filter key ever came back.
  if (pauseState !== null && pauseState.key !== filterKey) setPauseState(null)
  const pause = pauseState !== null && pauseState.key === filterKey ? pauseState : null
  const paused = pause !== null

  // Latest values for the event handlers below, which are kept referentially
  // stable so the memoized rows do not re-render on every batch.
  const latest = useRef<Latest>({ events, liveReceived, filterKey, pauseMode: null })
  useEffect(() => {
    latest.current = { events, liveReceived, filterKey, pauseMode: pause?.mode ?? null }
  })

  const scrollRef = useRef<HTMLDivElement | null>(null)
  /** Captured right before freezing, so the rows being read stay in place. */
  const anchorRef = useRef<ScrollAnchor | null>(null)

  const doPause = useCallback((mode: PauseMode) => {
    const cur = latest.current
    if (cur.pauseMode === 'manual' || cur.pauseMode === mode) return
    if (cur.pauseMode === 'auto') {
      // already auto-paused (scrolled up) → keep the frozen view, stop auto-resuming
      cur.pauseMode = mode
      setPauseState((p) => (p === null ? p : { ...p, mode }))
      return
    }
    cur.pauseMode = mode // synchronously: the anchor scroll below must not re-trigger a pause
    const el = scrollRef.current
    if (el !== null) anchorRef.current = captureAnchor(el)
    setPauseState({ mode, key: cur.filterKey, frozen: cur.events, liveAt: cur.liveReceived })
  }, [])

  const doResume = useCallback(() => {
    latest.current.pauseMode = null
    setPauseState(null)
  }, [])

  /** Expanding a row while live pauses, so the row does not scroll away and unmount. */
  const onRowExpand = useCallback(() => doPause('manual'), [doPause])

  const liveEvents = useMemo(() => events.slice(-LIVE_RENDER_CAP), [events])
  const displayEvents = pause !== null ? pause.frozen : liveEvents
  const newWhilePaused = pause !== null ? Math.max(0, liveReceived - pause.liveAt) : 0

  // Live tail sticks to the bottom; freezing keeps the viewed rows in place
  // even though the full buffer (rows above) gets materialized or a row
  // expands; reopening the drawer lands on the newest rows either way.
  const prevOpenRef = useRef(open)
  useLayoutEffect(() => {
    const reopened = open && !prevOpenRef.current
    prevOpenRef.current = open
    const el = scrollRef.current
    if (el === null) return
    const anchor = anchorRef.current
    if (anchor !== null) {
      anchorRef.current = null
      restoreAnchor(el, anchor)
      return
    }
    if (!paused || reopened) el.scrollTop = el.scrollHeight
  }, [displayEvents, paused, open])

  const onScroll = useCallback(
    (e: UIEvent<HTMLDivElement>) => {
      const el = e.currentTarget
      const atBottom = el.scrollHeight - el.scrollTop - el.clientHeight < AT_BOTTOM_SLACK_PX
      const mode = latest.current.pauseMode
      if (!atBottom && mode === null) {
        doPause('auto') // the user scrolled up to read — stop the tail
      } else if (atBottom && mode === 'auto') {
        doResume() // scrolled back down — resume the tail
      }
    },
    [doPause, doResume],
  )

  // --- event-rate hint for the header toggle ------------------------------
  useEffect(() => {
    let prev = latest.current.liveReceived
    const timer = window.setInterval(() => {
      const cur = latest.current.liveReceived
      setLogPanelRate(Math.max(0, cur - prev)) // a reopened view restarts at 0
      prev = cur
    }, RATE_SAMPLE_MS)
    return () => window.clearInterval(timer)
  }, [])

  const toggleCat = useCallback((c: LogCategory) => {
    setCats((prev) => {
      const next = new Set(prev)
      if (next.has(c)) {
        // The last active category cannot be switched off: an empty set has
        // no server-side meaning ("no constraint", see the filter memo).
        if (next.size === 1) return prev
        next.delete(c)
      } else {
        next.add(c)
      }
      return next
    })
  }, [])

  if (!open) return null

  return (
    <section className="log-panel" aria-label="Log panel">
      <div className="log-filters">
        <select
          className="log-select"
          value={minLevel}
          onChange={(e) => setMinLevel(e.target.value as LogLevel)}
          title="Minimum level"
          aria-label="Minimum level"
        >
          {LOG_LEVELS.map((l) => (
            <option key={l} value={l}>
              ≥ {l}
            </option>
          ))}
        </select>
        <div className="log-cats" role="group" aria-label="Categories">
          {LOG_CATEGORIES.map((c) => (
            <button
              key={c}
              type="button"
              className={`log-cat-toggle${cats.has(c) ? ' active' : ''}`}
              aria-pressed={cats.has(c)}
              disabled={cats.size === 1 && cats.has(c)}
              title={
                cats.size === 1 && cats.has(c)
                  ? 'At least one category stays selected'
                  : `Toggle category ${c}`
              }
              onClick={() => toggleCat(c)}
            >
              {c}
            </button>
          ))}
        </div>
        <input
          className="log-search"
          type="search"
          placeholder="search messages…"
          value={searchInput}
          onChange={(e) => setSearchInput(e.target.value)}
          aria-label="Search log messages"
        />
        <label
          className="log-subtree"
          title="Only events from the subtree of the node selected in the tree"
        >
          <input
            type="checkbox"
            checked={subtreeOnly}
            onChange={(e) => setSubtreeOnly(e.target.checked)}
          />
          subtree
          {subtreeOnly && selectedNode !== undefined ? (
            <code className="log-subtree-node" title={selectedNode.id}>
              {selectedNode.name}
            </code>
          ) : null}
        </label>
        <span className="log-filters-spacer" />
        <button
          type="button"
          className="btn btn-small"
          onClick={() => (paused ? doResume() : doPause('manual'))}
          title={paused ? 'Resume the live tail' : 'Freeze the view (new events keep buffering)'}
        >
          {paused ? 'Resume' : 'Pause'}
        </button>
        <span className="log-status">
          {events.length} buffered · {connected ? (paused ? 'paused' : 'live') : 'reconnecting…'}
        </span>
      </div>
      <div className="log-scroll" ref={scrollRef} onScroll={onScroll}>
        {displayEvents.length === 0 ? (
          <div className="log-empty">
            {connected ? 'no events match the current filter' : 'connecting to /ws/logs…'}
          </div>
        ) : (
          displayEvents.map((ev) => <LogRow key={ev.n} ev={ev} onExpand={onRowExpand} />)
        )}
      </div>
      {paused ? (
        <div className="log-paused-banner">
          <span>
            paused — {newWhilePaused} new event{newWhilePaused === 1 ? '' : 's'}
          </span>
          <button type="button" className="btn btn-small" onClick={doResume}>
            Resume ↓
          </button>
        </div>
      ) : null}
    </section>
  )
}
