/**
 * Wire types mirroring SPEC.md sections 3.1 (node model), 3.3 (patch ops),
 * 4 (state WebSocket) and 5 (telemetry WebSocket).
 *
 * Field names are the contract — do not rename.
 */

// ---------------------------------------------------------------------------
// 3.1 Node model
// ---------------------------------------------------------------------------

export type NodeStatus =
  | 'init'
  | 'idle'
  | 'running'
  | 'done'
  | 'error'
  | 'cancelled'
  | 'gone'

export interface NodeError {
  message: string
  detail?: string | null
}

export interface NodeAction {
  name: string
  label: string
  /** Parameter descriptors — shape reserved by the spec, empty in M0. */
  params: unknown[]
}

export type NodeOwner = 'core' | `plugin:${string}`

export interface DiagNode {
  /** unique, opaque */
  id: string
  /** dotted.lowercase, e.g. "plugin", "device.sim", "telemetry.channel", "job", "job.step" */
  type: string
  /** human-readable label */
  name: string
  status: NodeStatus
  error: NodeError | null
  /** 0..1 or null */
  progress: number | null
  /** type-specific data */
  props: Record<string, unknown>
  /** commands the UI may offer generically */
  actions: NodeAction[]
  /** null only for root */
  parentId: string | null
  /** ordered child ids */
  children: string[]
  owner: NodeOwner
}

// ---------------------------------------------------------------------------
// 3.3 Patch ops
// ---------------------------------------------------------------------------

export interface AddOp {
  op: 'add'
  node: DiagNode
  /** optional insertion index into the parent's children */
  index?: number
}

export interface UpdateOp {
  op: 'update'
  id: string
  /** top-level keys REPLACE the node's keys ("props" replaces all props) */
  changes: Partial<DiagNode>
}

export interface MergePropsOp {
  op: 'merge_props'
  id: string
  /** shallow-merged into node.props */
  props: Record<string, unknown>
}

export interface RemoveOp {
  op: 'remove'
  id: string
}

export type PatchOp = AddOp | UpdateOp | MergePropsOp | RemoveOp

// ---------------------------------------------------------------------------
// 4. State WebSocket — /ws/state
// ---------------------------------------------------------------------------

export interface Snapshot {
  rootId: string
  nodes: Record<string, DiagNode>
}

export interface HelloMsg {
  t: 'hello'
  seq: number
  snapshot: Snapshot
}

export interface PatchMsg {
  t: 'patch'
  seq: number
  ops: PatchOp[]
}

export interface CmdResultOkMsg {
  t: 'cmd_result'
  cmdId: string
  ok: true
  data?: Record<string, unknown>
}

export interface CmdResultErrMsg {
  t: 'cmd_result'
  cmdId: string
  ok: false
  error: NodeError
}

export type CmdResultMsg = CmdResultOkMsg | CmdResultErrMsg

export type StateServerMsg = HelloMsg | PatchMsg | CmdResultMsg

export interface CmdMsg {
  t: 'cmd'
  cmdId: string
  target: string
  action: string
  params: Record<string, unknown>
}

export interface ResyncMsg {
  t: 'resync'
}

export type StateClientMsg = CmdMsg | ResyncMsg

// ---------------------------------------------------------------------------
// 5. Telemetry WebSocket — /ws/telemetry
// ---------------------------------------------------------------------------

/** [tMs (Unix epoch milliseconds, float ok), value] */
export type Sample = [number, number]

export interface SubMsg {
  t: 'sub'
  channel: string
}

export interface UnsubMsg {
  t: 'unsub'
  channel: string
}

export type TelemetryClientMsg = SubMsg | UnsubMsg

export interface HistoryMsg {
  t: 'history'
  channel: string
  samples: Sample[]
}

export interface SamplesMsg {
  t: 'samples'
  channel: string
  samples: Sample[]
}

export type TelemetryServerMsg = HistoryMsg | SamplesMsg

// ---------------------------------------------------------------------------
// Frontend-local: connection state
// ---------------------------------------------------------------------------

export type ConnectionState = 'connecting' | 'connected' | 'disconnected'

// ---------------------------------------------------------------------------
// Boundary parsing — raw JSON from the sockets is validated/normalized here,
// so the rest of the app only ever sees the typed shapes above.
// ---------------------------------------------------------------------------

const NODE_STATUSES: readonly NodeStatus[] = [
  'init',
  'idle',
  'running',
  'done',
  'error',
  'cancelled',
  'gone',
]

function isRecord(v: unknown): v is Record<string, unknown> {
  return typeof v === 'object' && v !== null && !Array.isArray(v)
}

function asString(v: unknown, fallback = ''): string {
  return typeof v === 'string' ? v : fallback
}

function normalizeError(v: unknown): NodeError | null {
  if (!isRecord(v)) return null
  const message = asString(v['message'], 'unknown error')
  const detail = typeof v['detail'] === 'string' ? v['detail'] : null
  return { message, detail }
}

function normalizeActions(v: unknown): NodeAction[] {
  if (!Array.isArray(v)) return []
  const out: NodeAction[] = []
  for (const a of v) {
    if (!isRecord(a) || typeof a['name'] !== 'string') continue
    out.push({
      name: a['name'],
      label: asString(a['label'], a['name']),
      params: Array.isArray(a['params']) ? a['params'] : [],
    })
  }
  return out
}

/** Normalize one raw node object from the wire into a DiagNode, or null if unusable. */
export function parseNode(raw: unknown): DiagNode | null {
  if (!isRecord(raw)) return null
  if (typeof raw['id'] !== 'string' || raw['id'] === '') return null
  const status = NODE_STATUSES.includes(raw['status'] as NodeStatus)
    ? (raw['status'] as NodeStatus)
    : 'init'
  const owner = asString(raw['owner'], 'core')
  return {
    id: raw['id'],
    type: asString(raw['type'], 'unknown'),
    name: asString(raw['name'], raw['id']),
    status,
    error: normalizeError(raw['error']),
    progress: typeof raw['progress'] === 'number' ? raw['progress'] : null,
    props: isRecord(raw['props']) ? raw['props'] : {},
    actions: normalizeActions(raw['actions']),
    parentId: typeof raw['parentId'] === 'string' ? raw['parentId'] : null,
    children: Array.isArray(raw['children'])
      ? raw['children'].filter((c): c is string => typeof c === 'string')
      : [],
    owner: (owner === 'core' || owner.startsWith('plugin:') ? owner : 'core') as NodeOwner,
  }
}

/**
 * Normalize the `changes` object of an `update` op. Only the spec-updatable
 * keys may appear (mirroring the backend's `_UPDATABLE_KEYS`); `id`,
 * `parentId`, `children` and unknown keys are dropped — the tree maintains
 * those itself, and letting them through would poison the mirror (e.g. a
 * non-array `children` crashes the TreeExplorer outside any error boundary).
 * Returns null when a present value has an invalid shape: the caller then
 * distrusts the whole patch, same as for any other malformed op.
 */
function parsePartialNode(raw: unknown): Partial<DiagNode> | null {
  if (!isRecord(raw)) return null
  const out: Partial<DiagNode> = {}
  for (const [key, v] of Object.entries(raw)) {
    switch (key) {
      case 'type':
        if (typeof v !== 'string') return null
        out.type = v
        break
      case 'name':
        if (typeof v !== 'string') return null
        out.name = v
        break
      case 'status':
        if (!NODE_STATUSES.includes(v as NodeStatus)) return null
        out.status = v as NodeStatus
        break
      case 'error':
        if (v !== null && !isRecord(v)) return null
        out.error = normalizeError(v)
        break
      case 'progress':
        if (v !== null && typeof v !== 'number') return null
        out.progress = v
        break
      case 'props':
        if (!isRecord(v)) return null
        out.props = v
        break
      case 'actions':
        if (!Array.isArray(v)) return null
        out.actions = normalizeActions(v)
        break
      case 'owner':
        if (typeof v !== 'string') return null
        out.owner = (v === 'core' || v.startsWith('plugin:') ? v : 'core') as NodeOwner
        break
      default:
        break // id/parentId/children and unknown keys are never patchable
    }
  }
  return out
}

function parseOp(raw: unknown): PatchOp | null {
  if (!isRecord(raw)) return null
  switch (raw['op']) {
    case 'add': {
      const node = parseNode(raw['node'])
      if (!node) return null
      const op: AddOp = { op: 'add', node }
      if (typeof raw['index'] === 'number') op.index = raw['index']
      return op
    }
    case 'update': {
      if (typeof raw['id'] !== 'string') return null
      const changes = parsePartialNode(raw['changes'])
      if (changes === null) return null
      return { op: 'update', id: raw['id'], changes }
    }
    case 'merge_props': {
      if (typeof raw['id'] !== 'string' || !isRecord(raw['props'])) return null
      return { op: 'merge_props', id: raw['id'], props: raw['props'] }
    }
    case 'remove': {
      if (typeof raw['id'] !== 'string') return null
      return { op: 'remove', id: raw['id'] }
    }
    default:
      return null
  }
}

/** Parse a raw /ws/state server frame. Returns null for malformed frames. */
export function parseStateServerMsg(data: unknown): StateServerMsg | null {
  let raw: unknown = data
  if (typeof data === 'string') {
    try {
      raw = JSON.parse(data)
    } catch {
      return null
    }
  }
  if (!isRecord(raw)) return null
  switch (raw['t']) {
    case 'hello': {
      if (typeof raw['seq'] !== 'number' || !isRecord(raw['snapshot'])) return null
      const snap = raw['snapshot']
      if (typeof snap['rootId'] !== 'string' || !isRecord(snap['nodes'])) return null
      const nodes: Record<string, DiagNode> = {}
      for (const [id, n] of Object.entries(snap['nodes'])) {
        const node = parseNode(n)
        if (node) nodes[id] = node
      }
      return { t: 'hello', seq: raw['seq'], snapshot: { rootId: snap['rootId'], nodes } }
    }
    case 'patch': {
      if (typeof raw['seq'] !== 'number' || !Array.isArray(raw['ops'])) return null
      const ops: PatchOp[] = []
      for (const o of raw['ops']) {
        const op = parseOp(o)
        if (op === null) return null // a malformed op means we cannot trust the patch
        ops.push(op)
      }
      return { t: 'patch', seq: raw['seq'], ops }
    }
    case 'cmd_result': {
      if (typeof raw['cmdId'] !== 'string' || typeof raw['ok'] !== 'boolean') return null
      if (raw['ok']) {
        return {
          t: 'cmd_result',
          cmdId: raw['cmdId'],
          ok: true,
          data: isRecord(raw['data']) ? raw['data'] : {},
        }
      }
      return {
        t: 'cmd_result',
        cmdId: raw['cmdId'],
        ok: false,
        error: normalizeError(raw['error']) ?? { message: 'command failed' },
      }
    }
    default:
      return null
  }
}

/** Parse a raw /ws/telemetry server frame. Returns null for malformed frames. */
export function parseTelemetryServerMsg(data: unknown): TelemetryServerMsg | null {
  let raw: unknown = data
  if (typeof data === 'string') {
    try {
      raw = JSON.parse(data)
    } catch {
      return null
    }
  }
  if (!isRecord(raw)) return null
  if (raw['t'] !== 'history' && raw['t'] !== 'samples') return null
  if (typeof raw['channel'] !== 'string' || !Array.isArray(raw['samples'])) return null
  const samples: Sample[] = []
  for (const s of raw['samples']) {
    if (
      Array.isArray(s) &&
      s.length >= 2 &&
      typeof s[0] === 'number' &&
      typeof s[1] === 'number'
    ) {
      samples.push([s[0], s[1]])
    }
  }
  return { t: raw['t'], channel: raw['channel'], samples }
}
