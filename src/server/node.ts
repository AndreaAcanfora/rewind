import type { IncomingMessage, ServerResponse } from 'node:http'
import { Readable } from 'node:stream'

/**
 * Adapts a Fetch API handler to `node:http`:
 *
 * ```ts
 * http.createServer(toNodeListener(createHandler({ store }))).listen(3000)
 * ```
 */
export function toNodeListener(handler: (req: Request) => Promise<Response>) {
  return async (req: IncomingMessage, res: ServerResponse): Promise<void> => {
    const abort = new AbortController()
    res.on('close', () => abort.abort())
    try {
      const response = await handler(toRequest(req, abort.signal))
      await writeResponse(response, res, abort.signal)
    } catch (err) {
      if (!res.headersSent) {
        res.writeHead(500, { 'Content-Type': 'text/plain; charset=utf-8' })
        res.end('internal error\n')
      } else {
        res.destroy(err as Error)
      }
    }
  }
}

export function toRequest(req: IncomingMessage, signal: AbortSignal): Request {
  const host = req.headers.host ?? 'localhost'
  const proto = (req.socket as { encrypted?: boolean }).encrypted ? 'https' : 'http'
  const headers = new Headers()
  for (const [name, value] of Object.entries(req.headers)) {
    if (Array.isArray(value)) value.forEach((v) => headers.append(name, v))
    else if (value !== undefined) headers.set(name, value)
  }
  const hasBody = req.method !== 'GET' && req.method !== 'HEAD'
  return new Request(new URL(req.url ?? '/', `${proto}://${host}`), {
    method: req.method,
    headers,
    signal,
    body: hasBody ? (Readable.toWeb(req) as ReadableStream<Uint8Array>) : null,
    // Required by Node for streaming request bodies.
    ...(hasBody ? { duplex: 'half' } : {}),
  } as RequestInit)
}

async function writeResponse(response: Response, res: ServerResponse, signal: AbortSignal): Promise<void> {
  const headers: Record<string, string> = {}
  response.headers.forEach((v, k) => (headers[k] = v))
  res.writeHead(response.status, headers)
  if (!response.body) {
    res.end()
    return
  }
  // Send headers now: SSE clients wait for them before the first event.
  res.flushHeaders()
  const reader = response.body.getReader()
  signal.addEventListener('abort', () => void reader.cancel().catch(() => {}), { once: true })
  try {
    for (;;) {
      const { done, value } = await reader.read()
      if (done) break
      if (!res.write(value)) await new Promise((r) => res.once('drain', r))
    }
  } finally {
    res.end()
  }
}
