import { parseCursor, type StreamStatus } from '../protocol.ts'
import { encodeEvent } from '../sse.ts'
import type { Store } from './store.ts'

export interface HandlerOptions {
  store: Store
  /** Extracts the stream ID from a request. Default: the last path segment. */
  streamId?: (req: Request) => string | undefined
  /** Idle interval between keep-alive comments. Default 15 s, the spec maximum. */
  keepAliveMs?: number
  /** `retry` value sent to clients, in milliseconds. Default 1000. */
  retryMs?: number
  /** Whether DELETE cancels streams. Default true. */
  allowCancel?: boolean
}

/**
 * Returns a Fetch API handler that serves stream URLs: GET (SSE or snapshot)
 * and DELETE (cancellation). Authorization is the caller's job: check the
 * request before passing it on.
 */
export function createHandler(opts: HandlerOptions): (req: Request) => Promise<Response> {
  const getId = opts.streamId ?? lastSegment
  return async (req) => {
    const id = getId(req)
    if (!id) return text(404, 'stream not found')
    switch (req.method) {
      case 'GET':
      case 'HEAD':
        return read(opts, id, req)
      case 'DELETE': {
        if (opts.allowCancel === false) return text(405, 'cancellation not supported', { Allow: 'GET, HEAD' })
        const result = await opts.store.cancel(id)
        if (result === 'missing') return text(404, 'stream not found')
        return result === 'finished' ? new Response(null, { status: 204 }) : text(202, 'cancelling')
      }
      default:
        return text(405, 'method not allowed', { Allow: 'GET, HEAD, DELETE' })
    }
  }
}

async function read(opts: HandlerOptions, id: string, req: Request): Promise<Response> {
  const header = req.headers.get('last-event-id')
  const param = new URL(req.url).searchParams.get('after')
  const raw = header ?? param ?? '0'
  const after = parseCursor(raw.trim())
  if (after === undefined) return text(400, 'invalid cursor')

  const snap = await opts.store.snapshot(id, after)
  if (!snap) return text(404, 'stream not found')

  const accept = req.headers.get('accept') ?? ''
  if (accept.includes('application/json') && !accept.includes('text/event-stream')) {
    return new Response(req.method === 'HEAD' ? null : JSON.stringify(snap), {
      headers: { 'Content-Type': 'application/json', 'Cache-Control': 'no-store', 'Stream-Status': snap.status },
    })
  }
  if (snap.status !== 'live' && after >= snap.last_id) {
    return new Response(null, { status: 204, headers: { 'Stream-Status': snap.status } })
  }
  if (req.method === 'HEAD') return new Response(null, { headers: sseHeaders(snap.status) })
  return sseResponse(opts, id, after, snap.status, req.signal)
}

/**
 * Streams a stream as SSE from `after`. Exposed for endpoints that start a
 * generation and answer with the stream itself (spec section 3.1, option 2).
 */
export function sseResponse(
  opts: Pick<HandlerOptions, 'store' | 'keepAliveMs' | 'retryMs'>,
  streamId: string,
  after: number,
  status: StreamStatus,
  signal?: AbortSignal,
  extraHeaders: Record<string, string> = {},
): Response {
  const encoder = new TextEncoder()
  const abort = new AbortController()
  signal?.addEventListener('abort', () => abort.abort(), { once: true })
  const keepAliveMs = opts.keepAliveMs ?? 15_000
  let timer: ReturnType<typeof setInterval> | undefined

  const body = new ReadableStream<Uint8Array>({
    async start(controller) {
      let lastWrite = Date.now()
      const write = (s: string) => {
        if (abort.signal.aborted) return
        try {
          controller.enqueue(encoder.encode(s))
          lastWrite = Date.now()
        } catch {
          abort.abort()
        }
      }
      write(`retry: ${opts.retryMs ?? 1000}\n\n`)
      timer = setInterval(() => {
        if (Date.now() - lastWrite >= keepAliveMs) write(': keep-alive\n\n')
      }, Math.max(10, Math.floor(keepAliveMs / 3)))
      try {
        for await (const e of opts.store.follow(streamId, after, abort.signal)) write(encodeEvent(e))
      } finally {
        clearInterval(timer)
        if (!abort.signal.aborted) {
          try {
            controller.close()
          } catch {
            // Already closed by the consumer.
          }
        }
      }
    },
    cancel() {
      clearInterval(timer)
      abort.abort()
    },
  })
  return new Response(body, { headers: { ...sseHeaders(status), ...extraHeaders } })
}

function sseHeaders(status: StreamStatus): Record<string, string> {
  return {
    'Content-Type': 'text/event-stream; charset=utf-8',
    'Cache-Control': 'no-store',
    'X-Accel-Buffering': 'no',
    'Stream-Status': status,
  }
}

function lastSegment(req: Request): string | undefined {
  const path = new URL(req.url).pathname.replace(/\/+$/, '')
  const seg = path.slice(path.lastIndexOf('/') + 1)
  return seg ? decodeURIComponent(seg) : undefined
}

function text(status: number, body: string, headers: Record<string, string> = {}): Response {
  return new Response(body + '\n', { status, headers: { 'Content-Type': 'text/plain; charset=utf-8', ...headers } })
}
