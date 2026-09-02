/**
 * /ws/logs client (SPEC section 14.3).
 *
 * One LogSubscription per active log view (global log panel, per-node raw
 * feed): each owns its own WebSocket carrying exactly ONE active filter,
 * sent as `{ t: "sub", filter }` right after the socket opens. Changing a
 * filter means disposing the subscription and opening a new one (the
 * `useLogFeed` hook below does exactly that).
 *
 * - `history` replaces the buffer (recent matching events on every (re)sub,
 *   so reconnects never duplicate events), `events` batches append
 * - client-side cap: the newest 2000 events are kept per view
 * - auto-reconnect with the same 0.5 s → 10 s backoff as the other sockets,
 *   re-subscribing with the same filter on reconnect
 * - dispose() sends `unsub` and closes the socket cleanly
 *
 * Dev note: the socket connects same-origin, so in dev it goes through the
 * Vite `/ws` proxy to the core on 8765, like /ws/state and /ws/telemetry.
 */

import { useMemo, useSyncExternalStore } from 'react'
import { LOG_LEVELS, parseLogsServerMsg } from './types'
import type { LogCategory, LogEvent, LogFilter, LogLevel, LogsClientMsg } from './types'

const BACKOFF_INITIAL_MS = 500
const BACKOFF_CAP_MS = 10_000
/** Client-side cap: newest events kept per view. */
const EVENT_CAP = 2000

function wsUrl(path: string): string {
  const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
  return `${proto}//${window.location.host}${path}`
}

export class LogSubscription {
  readonly filter: LogFilter
  /**
   * Capped event buffer, oldest first. Replaced (fresh array identity) on
   * every change, so it doubles as a useSyncExternalStore snapshot.
   */
  events: LogEvent[] = []
  /** Total events delivered (history + live). */
  totalReceived = 0
  /**
   * Live events delivered (`events` batches only, never `history`) — drives
   * the rate hint, the "new while paused" counter and flow indicators, so a
   * (re)sub's history burst does not register as a spike of activity.
   */
  liveReceived = 0
  /** Wall-clock ms of the last live batch, 0 if none arrived yet. */
  lastLiveAt = 0
  connected = false

  private ws: WebSocket | null = null
  private backoffMs = BACKOFF_INITIAL_MS
  private reconnectTimer: number | null = null
  private disposed = false
  private listeners = new Set<() => void>()

  constructor(filter: LogFilter) {
    this.filter = filter
    this.connect()
  }

  /** Listener fires after every buffer/connection change. Returns unsubscribe. */
  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener)
    return () => {
      this.listeners.delete(listener)
    }
  }

  /** Unsubscribe, close the socket and stop reconnecting. Idempotent. */
  dispose(): void {
    if (this.disposed) return
    this.disposed = true
    if (this.reconnectTimer !== null) {
      window.clearTimeout(this.reconnectTimer)
      this.reconnectTimer = null
    }
    const ws = this.ws
    this.ws = null
    if (ws !== null) {
      if (ws.readyState === WebSocket.OPEN) {
        const unsub: LogsClientMsg = { t: 'unsub' }
        try {
          ws.send(JSON.stringify(unsub))
        } catch {
          // socket is being torn down anyway
        }
      }
      ws.close()
    }
    this.listeners.clear()
  }

  private notify(): void {
    for (const listener of this.listeners) listener()
  }

  private send(msg: LogsClientMsg): void {
    if (this.ws !== null && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(msg))
    }
  }

  private connect(): void {
    if (this.disposed) return
    let ws: WebSocket
    try {
      ws = new WebSocket(wsUrl('/ws/logs'))
    } catch (err) {
      console.warn('[ws/logs] failed to open socket', err)
      this.scheduleReconnect()
      return
    }
    this.ws = ws

    ws.onopen = () => {
      if (this.ws !== ws) return
      this.backoffMs = BACKOFF_INITIAL_MS
      this.connected = true
      // ONE active filter per connection (SPEC 14.3); re-sent on reconnect.
      this.send({ t: 'sub', filter: this.filter })
      this.notify()
    }

    ws.onmessage = (ev: MessageEvent) => {
      if (this.ws !== ws) return
      const msg = parseLogsServerMsg(ev.data)
      if (msg === null) {
        console.warn('[ws/logs] malformed frame ignored', ev.data)
        return
      }
      if (msg.t === 'history') {
        // Recent matching events on (re)sub — replaces the buffer, so a
        // reconnect never duplicates events.
        this.events = msg.events.slice(-EVENT_CAP)
      } else {
        if (msg.events.length === 0) return
        const merged = this.events.concat(msg.events)
        this.events =
          merged.length > EVENT_CAP ? merged.slice(merged.length - EVENT_CAP) : merged
        this.liveReceived += msg.events.length
        this.lastLiveAt = Date.now()
      }
      this.totalReceived += msg.events.length
      this.notify()
    }

    ws.onclose = () => {
      if (this.ws !== ws) return
      this.ws = null
      this.connected = false
      this.notify()
      this.scheduleReconnect()
    }

    ws.onerror = () => {
      // onclose follows; nothing to do here.
    }
  }

  private scheduleReconnect(): void {
    if (this.disposed || this.reconnectTimer !== null) return
    const delay = this.backoffMs
    this.backoffMs = Math.min(this.backoffMs * 2, BACKOFF_CAP_MS)
    this.reconnectTimer = window.setTimeout(() => {
      this.reconnectTimer = null
      this.connect()
    }, delay)
  }
}

// ---------------------------------------------------------------------------
// Filter helpers
// ---------------------------------------------------------------------------

/** All levels at or above `min` severity, or undefined when unrestricted. */
export function levelsAtOrAbove(min: LogLevel): LogLevel[] | undefined {
  const idx = LOG_LEVELS.indexOf(min)
  return idx <= 0 ? undefined : LOG_LEVELS.slice(idx)
}

/** Canonical (fully populated) shape a filter key serializes. */
interface FilterKeyShape {
  nodeId: string | null
  levels: LogLevel[] | null
  categories: LogCategory[] | null
  search: string | null
}

/** Stable identity key for a filter (by value) — drives the reopen-on-change behavior. */
export function logFilterKey(filter: LogFilter): string {
  const shape: FilterKeyShape = {
    nodeId: filter.nodeId ?? null,
    levels: filter.levels ?? null,
    categories: filter.categories ?? null,
    search: filter.search ?? null,
  }
  return JSON.stringify(shape)
}

/** Inverse of logFilterKey (our own serialization, so the cast is safe). */
export function parseLogFilterKey(key: string): LogFilter {
  const shape = JSON.parse(key) as FilterKeyShape
  const filter: LogFilter = {}
  if (shape.nodeId !== null) filter.nodeId = shape.nodeId
  if (shape.levels !== null) filter.levels = shape.levels
  if (shape.categories !== null) filter.categories = shape.categories
  if (shape.search !== null) filter.search = shape.search
  return filter
}

// ---------------------------------------------------------------------------
// React binding
// ---------------------------------------------------------------------------

export interface LogFeed {
  /** capped matching events, oldest first */
  events: LogEvent[]
  /** total events delivered to this view (history + live batches) */
  totalReceived: number
  /** live events delivered to this view (excluding history) */
  liveReceived: number
  /** wall-clock ms of the last live batch, 0 if none yet */
  lastLiveAt: number
  connected: boolean
}

const EMPTY_FEED: LogFeed = {
  events: [],
  totalReceived: 0,
  liveReceived: 0,
  lastLiveAt: 0,
  connected: false,
}

interface FeedStore {
  subscribe: (onChange: () => void) => () => void
  getSnapshot: () => LogFeed
}

/**
 * useSyncExternalStore adapter around one LogSubscription. React subscribes
 * on commit, so the socket opens with the first subscriber (a StrictMode
 * double render opens nothing twice) and is disposed when the last one
 * leaves — on unmount, or when the hook swaps to a new store because the
 * filter changed. The snapshot object is replaced only when the
 * subscription reports a change, as useSyncExternalStore requires.
 */
function createFeedStore(filter: LogFilter): FeedStore {
  const listeners = new Set<() => void>()
  let sub: LogSubscription | null = null
  let snapshot: LogFeed = EMPTY_FEED

  const refresh = () => {
    if (sub === null) return
    snapshot = {
      events: sub.events,
      totalReceived: sub.totalReceived,
      liveReceived: sub.liveReceived,
      lastLiveAt: sub.lastLiveAt,
      connected: sub.connected,
    }
    for (const listener of listeners) listener()
  }

  return {
    subscribe(onChange) {
      listeners.add(onChange)
      if (sub === null) {
        sub = new LogSubscription(filter)
        sub.subscribe(refresh)
      }
      return () => {
        listeners.delete(onChange)
        if (listeners.size === 0 && sub !== null) {
          sub.dispose()
          sub = null
          snapshot = EMPTY_FEED
        }
      }
    },
    getSnapshot: () => snapshot,
  }
}

/**
 * Feed one LogSubscription for `filter` while mounted; reopen it whenever
 * the filter changes by value; dispose it on unmount. The store is keyed by
 * the filter *value* (round-tripped through its key), so an equal-but-new
 * filter object never reopens the socket.
 */
export function useLogFeed(filter: LogFilter): LogFeed {
  const key = logFilterKey(filter)
  const store = useMemo(() => createFeedStore(parseLogFilterKey(key)), [key])
  return useSyncExternalStore(store.subscribe, store.getSnapshot)
}

// ---------------------------------------------------------------------------
// Formatting helpers shared by the log views
// ---------------------------------------------------------------------------

function pad2(v: number): string {
  return String(v).padStart(2, '0')
}

/** HH:MM:SS.mmm in local time. */
export function formatLogTime(tsMs: number): string {
  const d = new Date(tsMs)
  const ms = String(d.getMilliseconds()).padStart(3, '0')
  return `${pad2(d.getHours())}:${pad2(d.getMinutes())}:${pad2(d.getSeconds())}.${ms}`
}

function hexPairs(raw: string): string[] {
  const clean = raw.replace(/\s+/g, '')
  const pairs: string[] = []
  for (let i = 0; i < clean.length; i += 2) {
    pairs.push(clean.slice(i, i + 2).toUpperCase())
  }
  return pairs
}

/** Number of bytes a hex string encodes (a trailing nibble counts as one). */
export function hexByteCount(raw: string): number {
  return Math.ceil(raw.replace(/\s+/g, '').length / 2)
}

/** "0af322…" → "0A F3 22 …" on one line (for compact feed rows). */
export function formatHexInline(raw: string): string {
  return hexPairs(raw).join(' ')
}

/** "0af322…" → spaced byte pairs, wrapped every 16 bytes (for detail blocks). */
export function formatHexBlock(raw: string): string {
  const pairs = hexPairs(raw)
  const lines: string[] = []
  for (let i = 0; i < pairs.length; i += 16) {
    lines.push(pairs.slice(i, i + 16).join(' '))
  }
  return lines.join('\n')
}

function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null && !Array.isArray(v)
}

/**
 * One-line `key=value key=value` rendering of a decoded payload (nested
 * values fall back to compact JSON) — for the compact raw feed, where a
 * pretty-printed object would not fit on a row.
 */
export function formatDecodedInline(decoded: unknown): string {
  if (decoded === undefined || decoded === null) return ''
  if (!isRecord(decoded)) {
    try {
      return JSON.stringify(decoded)
    } catch {
      return String(decoded)
    }
  }
  const parts: string[] = []
  for (const [key, value] of Object.entries(decoded)) {
    let text: string
    if (typeof value === 'string') text = value
    else if (typeof value === 'number' || typeof value === 'boolean') text = String(value)
    else if (value === null || value === undefined) text = '—'
    else {
      try {
        text = JSON.stringify(value)
      } catch {
        text = String(value)
      }
    }
    parts.push(`${key}=${text}`)
  }
  return parts.join(' ')
}

/** Pretty JSON of a value, never throwing (for copy buttons and detail blocks). */
export function safeJson(value: unknown): string {
  try {
    return JSON.stringify(value, null, 2) ?? String(value)
  } catch {
    return String(value)
  }
}
