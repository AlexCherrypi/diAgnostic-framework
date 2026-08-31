/**
 * /ws/telemetry client (SPEC section 5).
 *
 * - sub/unsub per channel with refcounting (many charts may share a channel)
 * - `history` resets the per-channel ring buffer, `samples` append to it
 * - listeners are notified after each buffer change (charts rAF-throttle
 *   their redraws themselves)
 * - auto-reconnect with the same 0.5 s → 10 s backoff; on reconnect every
 *   refcounted channel is resubscribed (the server answers with `history`,
 *   which replaces the buffer, so no duplicates accumulate)
 */

import { parseTelemetryServerMsg } from './types'
import type { Sample, TelemetryClientMsg } from './types'

const BACKOFF_INITIAL_MS = 500
const BACKOFF_CAP_MS = 10_000
const RING_CAPACITY = 10_000 // matches the core's default per-channel buffer

/** Fixed-capacity ring buffer of (tMs, value) samples. */
export class RingBuffer {
  readonly capacity: number
  private ts: Float64Array
  private vs: Float64Array
  private start = 0
  private count = 0

  constructor(capacity: number = RING_CAPACITY) {
    this.capacity = capacity
    this.ts = new Float64Array(capacity)
    this.vs = new Float64Array(capacity)
  }

  get size(): number {
    return this.count
  }

  clear(): void {
    this.start = 0
    this.count = 0
  }

  push(tMs: number, value: number): void {
    const idx = (this.start + this.count) % this.capacity
    this.ts[idx] = tMs
    this.vs[idx] = value
    if (this.count < this.capacity) {
      this.count += 1
    } else {
      this.start = (this.start + 1) % this.capacity
    }
  }

  pushMany(samples: Sample[]): void {
    for (const [t, v] of samples) this.push(t, v)
  }

  /** Newest sample or undefined. */
  last(): Sample | undefined {
    if (this.count === 0) return undefined
    const idx = (this.start + this.count - 1) % this.capacity
    return [this.ts[idx] as number, this.vs[idx] as number]
  }

  /** Copy out all samples with tMs >= sinceMs, oldest first. */
  since(sinceMs: number): { ts: number[]; vs: number[] } {
    const ts: number[] = []
    const vs: number[] = []
    for (let i = 0; i < this.count; i++) {
      const idx = (this.start + i) % this.capacity
      const t = this.ts[idx] as number
      if (t >= sinceMs) {
        ts.push(t)
        vs.push(this.vs[idx] as number)
      }
    }
    return { ts, vs }
  }
}

type ChannelListener = () => void

interface ChannelSub {
  refs: number
  buffer: RingBuffer
  listeners: Set<ChannelListener>
}

class TelemetryClient {
  private ws: WebSocket | null = null
  private channels = new Map<string, ChannelSub>()
  private backoffMs = BACKOFF_INITIAL_MS
  private reconnectTimer: number | null = null
  private started = false

  /**
   * Subscribe to a channel. The listener fires after every buffer change.
   * Returns an unsubscribe function (refcounted: the wire `unsub` is only
   * sent when the last local subscriber releases the channel).
   */
  subscribe(channel: string, listener: ChannelListener): () => void {
    let sub = this.channels.get(channel)
    if (!sub) {
      sub = { refs: 0, buffer: new RingBuffer(), listeners: new Set() }
      this.channels.set(channel, sub)
    }
    sub.refs += 1
    sub.listeners.add(listener)
    if (sub.refs === 1) this.send({ t: 'sub', channel })
    this.ensureStarted()

    let released = false
    return () => {
      if (released) return
      released = true
      const s = this.channels.get(channel)
      if (!s) return
      s.refs -= 1
      s.listeners.delete(listener)
      if (s.refs <= 0) {
        this.channels.delete(channel)
        this.send({ t: 'unsub', channel })
      }
    }
  }

  /** The ring buffer for a channel, if currently subscribed. */
  getBuffer(channel: string): RingBuffer | undefined {
    return this.channels.get(channel)?.buffer
  }

  private ensureStarted(): void {
    if (this.started) return
    this.started = true
    this.connect()
  }

  private send(msg: TelemetryClientMsg): void {
    if (this.ws !== null && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(msg))
    }
    // Not open: sub state is re-established from `channels` on (re)connect,
    // and an unsub for a dead socket is a no-op server-side.
  }

  private connect(): void {
    const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
    let ws: WebSocket
    try {
      ws = new WebSocket(`${proto}//${window.location.host}/ws/telemetry`)
    } catch (err) {
      console.warn('[ws/telemetry] failed to open socket', err)
      this.scheduleReconnect()
      return
    }
    this.ws = ws

    ws.onopen = () => {
      this.backoffMs = BACKOFF_INITIAL_MS
      // Resubscribe everything that is still referenced.
      for (const channel of this.channels.keys()) {
        this.send({ t: 'sub', channel })
      }
    }

    ws.onmessage = (ev: MessageEvent) => {
      const msg = parseTelemetryServerMsg(ev.data)
      if (msg === null) {
        console.warn('[ws/telemetry] malformed frame ignored', ev.data)
        return
      }
      const sub = this.channels.get(msg.channel)
      if (!sub) return // late frames after unsub
      if (msg.t === 'history') {
        sub.buffer.clear()
      }
      sub.buffer.pushMany(msg.samples)
      for (const listener of sub.listeners) listener()
    }

    ws.onclose = () => {
      if (this.ws !== ws) return
      this.ws = null
      this.scheduleReconnect()
    }

    ws.onerror = () => {
      // onclose follows.
    }
  }

  private scheduleReconnect(): void {
    if (this.reconnectTimer !== null) return
    const delay = this.backoffMs
    this.backoffMs = Math.min(this.backoffMs * 2, BACKOFF_CAP_MS)
    this.reconnectTimer = window.setTimeout(() => {
      this.reconnectTimer = null
      this.connect()
    }, delay)
  }
}

/** App-wide singleton /ws/telemetry client. Connects lazily on first subscribe. */
export const telemetry = new TelemetryClient()
