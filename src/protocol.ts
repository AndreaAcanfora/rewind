/** Protocol version implemented by this package. */
export const VERSION = '0.1'

/** One event of a stream. */
export interface StreamEvent {
  /** Position in the stream: 1, 2, 3… without gaps. */
  id: number
  /** Event type; `message` by default. */
  event: string
  /** Application-defined text, JSON recommended. */
  data: string
}

export type StreamStatus = 'live' | 'complete' | 'cancelled' | 'error'

/** Body of a snapshot response (spec section 4.2). */
export interface Snapshot {
  status: StreamStatus
  last_id: number
  events: StreamEvent[]
}

export const END = 'stream.end'
export const ERROR = 'stream.error'

export interface EndData {
  reason: 'complete' | 'cancelled'
}

export interface ErrorData {
  code: string
  message: string
}

const TYPE_PATTERN = /^[A-Za-z0-9._-]{1,64}$/

/** Reports whether `type` is a valid event type. */
export function isValidType(type: string): boolean {
  return TYPE_PATTERN.test(type)
}

/** Reports whether `type` is reserved by the protocol. */
export function isReservedType(type: string): boolean {
  return type.startsWith('stream.')
}

/** Reports whether the event closes its stream. */
export function isTerminal(e: Pick<StreamEvent, 'event'>): boolean {
  return e.event === END || e.event === ERROR
}

/** Status of a finished stream, from its terminal event. */
export function statusOf(terminal: StreamEvent): StreamStatus {
  if (terminal.event === ERROR) return 'error'
  try {
    return (JSON.parse(terminal.data) as EndData).reason === 'cancelled' ? 'cancelled' : 'complete'
  } catch {
    return 'complete'
  }
}

/**
 * Parses a cursor (Last-Event-ID header or `after` parameter).
 * Returns undefined when the value is not a non-negative integer.
 */
export function parseCursor(value: string): number | undefined {
  if (!/^(0|[1-9][0-9]{0,14})$/.test(value)) return undefined
  return Number(value)
}
