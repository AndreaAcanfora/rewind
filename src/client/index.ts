import { isTerminal, type Snapshot, type StreamEvent } from '../protocol.ts'
import { SSEParser } from '../sse.ts'

export interface ReadOptions {
  /** ID of the last event already received. Default 0. */
  cursor?: number
  /** Extra request headers (e.g. Authorization), or a function returning them for each attempt. */
  headers?: HeadersInit | (() => HeadersInit | Promise<HeadersInit>)
  signal?: AbortSignal
  fetch?: typeof fetch
  /** Consecutive failed attempts before giving up. Default 8. */
  maxAttempts?: number
  /** First reconnection delay when the server sent no `retry`. Default 1000 ms. */
  initialDelayMs?: number
  /** Upper bound of the reconnection delay. Default 30 000 ms. */
  maxDelayMs?: number
  /** Called after each event, e.g. to save the cursor in sessionStorage. */
  onCursor?: (cursor: number) => void
  /** Called before each reconnection. */
  onReconnect?: (info: ReconnectInfo) => void
}

export interface ReconnectInfo {
  cursor: number
  attempt: number
  delayMs: number
  reason: string
}

/** The stream does not exist, or has expired (HTTP 404). */
export class StreamNotFoundError extends Error {
  constructor() {
    super('stream not found')
    this.name = 'StreamNotFoundError'
  }
}

/** The server rejected the request with a non-retryable status. */
export class StreamHTTPError extends Error {
  status: number
  constructor(status: number) {
    super(`server responded ${status}`)
    this.name = 'StreamHTTPError'
    this.status = status
  }
}

/** Every reconnection attempt failed. */
export class StreamUnavailableError extends Error {
  constructor(reason: string) {
    super(`stream unavailable: ${reason}`)
    this.name = 'StreamUnavailableError'
  }
}

/**
 * Reads a stream from its URL, reconnecting from the last event after any
 * interruption, until the terminal event. Yields every event once, in order,
 * including the terminal `stream.end` or `stream.error` event.
 *
 * ```ts
 * for await (const e of readStream(url)) {
 *   if (e.event === 'delta') output.textContent += JSON.parse(e.data).text
 * }
 * ```
 */
export function readStream(url: string | URL, opts: ReadOptions = {}): AsyncGenerator<StreamEvent, void, undefined> {
  return follow(String(url), opts)
}

export interface StartedStream {
  /** Absolute URL of the stream: save it to resume after a reload. */
  url: string
  /** Events of the stream, resumed across interruptions. */
  events: AsyncGenerator<StreamEvent, void, undefined>
}

/**
 * Calls an endpoint that starts a stream (spec section 3.1) and follows the
 * stream, whichever of the two response styles the endpoint uses.
 */
export async function startStream(input: string | URL, init: RequestInit = {}, opts: ReadOptions = {}): Promise<StartedStream> {
  const fetchImpl = opts.fetch ?? globalThis.fetch
  const headers = new Headers(init.headers)
  if (!headers.has('accept')) headers.set('accept', 'text/event-stream')
  const res = await fetchImpl(input, { ...init, headers, signal: opts.signal ?? init.signal ?? null })
  const base = res.url || String(input)
  const inline = res.headers.get('stream-location')
  if (res.status === 200 && inline) {
    const url = new URL(inline, base).href
    return { url, events: follow(url, opts, res) }
  }
  const location = res.headers.get('location')
  if (res.status === 201 && location) {
    await res.body?.cancel()
    const url = new URL(location, base).href
    return { url, events: follow(url, opts) }
  }
  await res.body?.cancel()
  throw new StreamHTTPError(res.status)
}

/** Fetches a snapshot of the stream (spec section 4.2) without waiting. */
export async function fetchSnapshot(url: string | URL, after = 0, opts: Pick<ReadOptions, 'headers' | 'signal' | 'fetch'> = {}): Promise<Snapshot> {
  const fetchImpl = opts.fetch ?? globalThis.fetch
  const headers = new Headers(await resolveHeaders(opts.headers))
  headers.set('accept', 'application/json')
  const target = new URL(url)
  target.searchParams.set('after', String(after))
  const res = await fetchImpl(target, { headers, signal: opts.signal ?? null })
  if (res.status === 404) throw new StreamNotFoundError()
  if (!res.ok) throw new StreamHTTPError(res.status)
  return (await res.json()) as Snapshot
}

async function resolveHeaders(h: ReadOptions['headers']): Promise<HeadersInit | undefined> {
  return typeof h === 'function' ? await h() : h
}

async function* follow(url: string, opts: ReadOptions, initial?: Response): AsyncGenerator<StreamEvent, void, undefined> {
  const fetchImpl = opts.fetch ?? globalThis.fetch
  const signal = opts.signal
  const maxAttempts = opts.maxAttempts ?? 8
  const maxDelay = opts.maxDelayMs ?? 30_000
  let cursor = opts.cursor ?? 0
  let failures = 0
  let retryMs: number | undefined
  let pending = initial

  for (;;) {
    signal?.throwIfAborted()
    let progressed = false
    let reason: string
    try {
      let res = pending
      pending = undefined
      if (!res) {
        const headers = new Headers(await resolveHeaders(opts.headers))
        headers.set('accept', 'text/event-stream')
        headers.set('last-event-id', String(cursor))
        res = await fetchImpl(url, { headers, signal: signal ?? null })
      }
      if (res.status === 204) {
        await res.body?.cancel()
        return
      }
      if (res.status === 404) {
        await res.body?.cancel()
        throw new StreamNotFoundError()
      }
      if (res.status !== 200 || !res.body) {
        await res.body?.cancel()
        if (res.status < 500 && res.status !== 408 && res.status !== 429) throw new StreamHTTPError(res.status)
        reason = `server responded ${res.status}`
      } else {
        const parser = new SSEParser()
        const reader = res.body.pipeThrough(new TextDecoderStream()).getReader()
        reason = 'connection closed before the end of the stream'
        try {
          read: for (;;) {
            const { done, value } = await reader.read()
            if (done) break
            for (const raw of parser.feed(value)) {
              const id = raw.id === undefined ? NaN : Number(raw.id)
              if (!Number.isSafeInteger(id) || id < 1) {
                reason = `event without a valid id after ${cursor}`
                break read
              }
              if (id <= cursor) continue
              if (id > cursor + 1) {
                reason = `gap: expected event ${cursor + 1}, got ${id}`
                break read
              }
              cursor = id
              progressed = true
              failures = 0
              opts.onCursor?.(cursor)
              const event: StreamEvent = { id, event: raw.event, data: raw.data }
              yield event
              if (isTerminal(event)) return
            }
            if (parser.retry !== undefined) retryMs = parser.retry
          }
        } finally {
          reader.cancel().catch(() => {})
        }
      }
    } catch (err) {
      if (err instanceof StreamNotFoundError || err instanceof StreamHTTPError) throw err
      signal?.throwIfAborted()
      reason = err instanceof Error ? err.message : String(err)
    }

    if (!progressed) failures++
    if (failures >= maxAttempts) throw new StreamUnavailableError(reason)
    const base = retryMs ?? opts.initialDelayMs ?? 1000
    const delayMs = Math.min(maxDelay, base * 2 ** Math.max(0, failures - 1))
    opts.onReconnect?.({ cursor, attempt: failures + 1, delayMs, reason })
    await sleep(delayMs, signal)
  }
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    if (signal?.aborted) return reject(signal.reason)
    const timer = setTimeout(() => {
      signal?.removeEventListener('abort', onAbort)
      resolve()
    }, ms)
    const onAbort = () => {
      clearTimeout(timer)
      reject(signal!.reason)
    }
    signal?.addEventListener('abort', onAbort, { once: true })
  })
}
