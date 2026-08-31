/**
 * /ws/state client (SPEC section 4).
 *
 * - `hello`      → replace the mirror store with the snapshot
 * - `patch`      → apply ops if `seq` is consecutive, otherwise send `resync`
 * - `cmd_result` → forwarded to the command layer (commands.ts registers a hook)
 * - auto-reconnect with exponential backoff 0.5 s → 10 s cap
 * - connection state exposed via the mirror store
 */

import { applyHello, applyPatchOps, setConnectionState, useMirrorStore } from './store'
import { parseStateServerMsg } from './types'
import type { CmdResultMsg, StateClientMsg } from './types'

const BACKOFF_INITIAL_MS = 500
const BACKOFF_CAP_MS = 10_000

function wsUrl(path: string): string {
  const proto = window.location.protocol === 'https:' ? 'wss:' : 'ws:'
  return `${proto}//${window.location.host}${path}`
}

/**
 * True when a raw frame is a patch that failed full parsing. Such a frame
 * must be treated like a seq gap (SPEC section 4): silently dropping it
 * would leave the mirror stale under a green "connected" pill, and the gap
 * would only surface when (if ever) the next patch arrives.
 */
function looksLikePatchFrame(data: unknown): boolean {
  let raw: unknown = data
  if (typeof data === 'string') {
    try {
      raw = JSON.parse(data)
    } catch {
      return false
    }
  }
  return (
    typeof raw === 'object' &&
    raw !== null &&
    !Array.isArray(raw) &&
    (raw as Record<string, unknown>)['t'] === 'patch'
  )
}

class StateSocket {
  /** Set by commands.ts; receives every cmd_result frame. */
  onCmdResult: ((msg: CmdResultMsg) => void) | null = null
  /** Set by commands.ts; called when the connection drops (pending cmds fail). */
  onConnectionLost: (() => void) | null = null

  private ws: WebSocket | null = null
  private backoffMs = BACKOFF_INITIAL_MS
  private reconnectTimer: number | null = null
  private started = false
  /** true after a detected gap until the next hello arrives */
  private awaitingResync = false

  start(): void {
    if (this.started) return
    this.started = true
    this.connect()
  }

  /** Send a client frame; returns false if the socket is not open. */
  send(msg: StateClientMsg): boolean {
    if (this.ws !== null && this.ws.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(msg))
      return true
    }
    return false
  }

  private connect(): void {
    setConnectionState('connecting')
    let ws: WebSocket
    try {
      ws = new WebSocket(wsUrl('/ws/state'))
    } catch (err) {
      console.warn('[ws/state] failed to open socket', err)
      this.scheduleReconnect()
      return
    }
    this.ws = ws

    ws.onopen = () => {
      this.backoffMs = BACKOFF_INITIAL_MS
      this.awaitingResync = false
      setConnectionState('connected')
    }

    ws.onmessage = (ev: MessageEvent) => {
      const msg = parseStateServerMsg(ev.data)
      if (msg === null) {
        console.warn('[ws/state] malformed frame ignored', ev.data)
        if (!this.awaitingResync && looksLikePatchFrame(ev.data)) {
          this.requestResync('unparseable patch')
        }
        return
      }
      switch (msg.t) {
        case 'hello': {
          this.awaitingResync = false
          applyHello(msg)
          break
        }
        case 'patch': {
          if (this.awaitingResync) return // stale patches until the fresh hello
          const expected = useMirrorStore.getState().seq + 1
          if (msg.seq !== expected) {
            this.requestResync(`seq gap (expected ${expected}, got ${msg.seq})`)
            return
          }
          if (!applyPatchOps(msg.seq, msg.ops)) {
            this.requestResync('patch referenced unknown node')
          }
          break
        }
        case 'cmd_result': {
          this.onCmdResult?.(msg)
          break
        }
      }
    }

    ws.onclose = () => {
      if (this.ws !== ws) return
      this.ws = null
      setConnectionState('disconnected')
      this.onConnectionLost?.()
      this.scheduleReconnect()
    }

    ws.onerror = () => {
      // onclose follows; nothing to do here.
    }
  }

  private requestResync(reason: string): void {
    console.warn(`[ws/state] resync: ${reason}`)
    this.awaitingResync = true
    this.send({ t: 'resync' })
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

/** App-wide singleton /ws/state client. Call start() once at boot. */
export const stateSocket = new StateSocket()
