import { createServer, type Server } from 'node:http'
import type { AddressInfo } from 'node:net'
import { createHandler, MemoryStore, produce, start, type Chunk } from '../src/server/index.ts'
import { toNodeListener } from '../src/server/node.ts'

export interface TestServer {
  base: string
  store: MemoryStore
  server: Server
  /** Creates a stream fed by `source` and returns its URL. */
  stream(source: AsyncIterable<Chunk> | ((signal: AbortSignal) => AsyncIterable<Chunk>)): Promise<string>
  close(): Promise<void>
}

let counter = 0

/** Starts an HTTP server with /streams/:id and a /chat endpoint that starts streams. */
export async function serve(opts: { keepAliveMs?: number; mode?: 'inline' | 'location'; tokens?: number } = {}): Promise<TestServer> {
  const store = new MemoryStore()
  const handler = createHandler({ store, keepAliveMs: opts.keepAliveMs ?? 15_000, retryMs: 10 })
  const server = createServer(
    toNodeListener(async (req) => {
      const url = new URL(req.url)
      if (url.pathname === '/chat' && req.method === 'POST') {
        const n = opts.tokens ?? 5
        return start(words(n, 5), { store, url: (id) => `/streams/${id}`, mode: opts.mode ?? 'inline', signal: req.signal, retryMs: 10 })
      }
      if (url.pathname.startsWith('/streams/')) return handler(req)
      return new Response('not found', { status: 404 })
    }),
  )
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve))
  const base = `http://127.0.0.1:${(server.address() as AddressInfo).port}`
  return {
    base,
    store,
    server,
    async stream(source) {
      const id = `s${++counter}`
      await store.create(id)
      void produce(store, id, source)
      return `${base}/streams/${id}`
    },
    close: () =>
      new Promise<void>((resolve) => {
        server.closeAllConnections()
        server.close(() => resolve())
      }),
  }
}

/** Yields `n` delta events, `delayMs` apart. */
export async function* words(n: number, delayMs = 0, signal?: AbortSignal): AsyncGenerator<Chunk> {
  for (let i = 1; i <= n; i++) {
    if (delayMs) await sleep(delayMs, signal)
    yield { event: 'delta', data: { text: `w${i} ` } }
  }
}

export function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve, reject) => {
    const t = setTimeout(resolve, ms)
    signal?.addEventListener('abort', () => {
      clearTimeout(t)
      reject(signal.reason)
    })
  })
}

/** Waits until the stream finishes. */
export async function finished(store: MemoryStore, id: string): Promise<void> {
  for await (const _ of store.follow(id, 0)) {
    // drain
  }
}

/**
 * A fetch that cuts every SSE response after `cutAfter` events, like a
 * flaky network, and counts the requests.
 */
export function flakyFetch(cutAfter: number) {
  const calls: { lastEventId: string | null }[] = []
  const f: typeof fetch = async (input, init) => {
    calls.push({ lastEventId: new Headers(init?.headers).get('last-event-id') })
    const res = await fetch(input, init)
    if (!res.body || !(res.headers.get('content-type') ?? '').startsWith('text/event-stream')) return res
    let seen = 0
    let cutting = false
    let text = ''
    const decoder = new TextDecoder()
    const encoder = new TextEncoder()
    const cut = new TransformStream<Uint8Array, Uint8Array>({
      transform(chunk, controller) {
        if (cutting) return
        text += decoder.decode(chunk, { stream: true })
        let i: number
        while ((i = text.indexOf('\n\n')) >= 0) {
          const block = text.slice(0, i + 2)
          text = text.slice(i + 2)
          controller.enqueue(encoder.encode(block))
          if (block.includes('\nid:') || block.startsWith('id:')) seen++
          if (seen >= cutAfter) {
            // Error after the reader has taken the events already sent:
            // erroring now would discard them.
            cutting = true
            setTimeout(() => controller.error(new TypeError('network connection lost')), 5)
            return
          }
        }
      },
    })
    return new Response(res.body.pipeThrough(cut), { status: res.status, headers: res.headers })
  }
  return { fetch: f, calls }
}
