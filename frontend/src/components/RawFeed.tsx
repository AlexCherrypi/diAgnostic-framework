/**
 * Per-node raw feed (SPEC 14.4): the "Raw" expander every node card gets via
 * StatusFrame — the "is it looping or actually working?" view.
 *
 * Lazy: the /ws/logs subscription (filter `{ nodeId }`, i.e. this node's
 * whole subtree) exists only while the expander is open and is disposed on
 * collapse/unmount. The compact feed emphasizes `io` events — direction
 * arrow (tx/rx), hex payload, decoded summary, note — while other categories
 * render as dimmed one-liners. A small indicator lights up while live events
 * are flowing.
 */

import { memo, useEffect, useLayoutEffect, useMemo, useRef, useState } from 'react'
import {
  formatDecodedInline,
  formatHexInline,
  formatLogTime,
  hexByteCount,
  useLogFeed,
} from '../lib/logs'
import type { LogEvent, LogFilter } from '../lib/types'

/** Newest rows rendered in the compact feed. */
const RENDER_CAP = 100
/** How long the live indicator stays lit after a batch arrives. */
const FLOW_LIT_MS = 1600

function dataField(ev: LogEvent, key: string): unknown {
  return ev.data !== undefined ? ev.data[key] : undefined
}

const RawRow = memo(function RawRow({ ev }: { ev: LogEvent }) {
  if (ev.category === 'io') {
    const dirRaw = dataField(ev, 'direction')
    const dir = dirRaw === 'tx' || dirRaw === 'rx' ? dirRaw : null
    const noteRaw = dataField(ev, 'note')
    const note = typeof noteRaw === 'string' ? noteRaw : ''
    const decoded = formatDecodedInline(dataField(ev, 'decoded'))
    const title =
      ev.raw !== undefined ? `${ev.message}\n${hexByteCount(ev.raw)} byte(s)` : ev.message
    return (
      <div className={`raw-row io${dir !== null ? ` dir-${dir}` : ''}`} title={title}>
        <span className="raw-time">{formatLogTime(ev.ts)}</span>
        <span className="raw-dir" aria-hidden="true">
          {dir === 'tx' ? '→' : dir === 'rx' ? '←' : '·'}
        </span>
        {ev.raw !== undefined ? <code className="raw-hex">{formatHexInline(ev.raw)}</code> : null}
        <span className="raw-summary">
          {decoded !== '' ? <span className="raw-decoded">{decoded}</span> : null}
          {note !== '' ? <span className="raw-note">{note}</span> : null}
          {decoded === '' && note === '' ? ev.message : null}
        </span>
      </div>
    )
  }
  return (
    <div className={`raw-row other lv-${ev.level}`} title={ev.message}>
      <span className="raw-time">{formatLogTime(ev.ts)}</span>
      <span className="raw-dir" aria-hidden="true">
        ·
      </span>
      <span className="raw-summary">
        <span className="raw-cat">{ev.category}</span> {ev.message}
      </span>
    </div>
  )
})

function RawFeed({ nodeId }: { nodeId: string }) {
  const filter = useMemo<LogFilter>(() => ({ nodeId }), [nodeId])
  const { events, lastLiveAt, connected } = useLogFeed(filter)

  // Live indicator: lit from a live batch until FLOW_LIT_MS after the last
  // one. `litExpiredAt` is the lastLiveAt whose lit period ran out, so the
  // flag is derived — the effect only arms the expiry timer.
  const [litExpiredAt, setLitExpiredAt] = useState(0)
  const flowing = lastLiveAt > litExpiredAt
  useEffect(() => {
    if (lastLiveAt === 0) return
    const t = window.setTimeout(() => setLitExpiredAt(lastLiveAt), FLOW_LIT_MS)
    return () => window.clearTimeout(t)
  }, [lastLiveAt])

  // Tail sticks to the bottom unless the user scrolled up.
  const scrollRef = useRef<HTMLDivElement | null>(null)
  const atBottomRef = useRef(true)
  useLayoutEffect(() => {
    const el = scrollRef.current
    if (el !== null && atBottomRef.current) el.scrollTop = el.scrollHeight
  }, [events])

  const ioCount = useMemo(() => {
    let n = 0
    for (const ev of events) if (ev.category === 'io') n += 1
    return n
  }, [events])

  const shown = events.slice(-RENDER_CAP)
  return (
    <div className="raw-feed">
      <div className="raw-feed-head">
        <span className={`raw-live${flowing ? ' on' : ''}`} aria-hidden="true" />
        {connected ? (flowing ? 'events flowing' : 'quiet') : 'reconnecting…'}
        <span className="raw-feed-count">
          {ioCount} frame{ioCount === 1 ? '' : 's'} · {events.length} event
          {events.length === 1 ? '' : 's'} buffered
        </span>
      </div>
      <div
        className="raw-scroll"
        ref={scrollRef}
        onScroll={(e) => {
          const el = e.currentTarget
          atBottomRef.current = el.scrollHeight - el.scrollTop - el.clientHeight < 32
        }}
      >
        {shown.length === 0 ? (
          <div className="raw-empty">
            {connected ? 'no events for this subtree yet' : 'connecting to /ws/logs…'}
          </div>
        ) : (
          shown.map((ev) => <RawRow key={ev.n} ev={ev} />)
        )}
      </div>
    </div>
  )
}

/** The collapsed-by-default "Raw" footer on every node card. */
export function RawExpander({ nodeId }: { nodeId: string }) {
  const [open, setOpen] = useState(false)
  return (
    <div className="raw-section">
      <button
        type="button"
        className="raw-toggle"
        onClick={() => setOpen((v) => !v)}
        aria-expanded={open}
        title="Live log / raw I/O feed for this node's subtree"
      >
        <span className="raw-caret" aria-hidden="true">
          {open ? '▾' : '▸'}
        </span>
        Raw
      </button>
      {open ? <RawFeed nodeId={nodeId} /> : null}
    </div>
  )
}
