import { produce, type ProduceOptions, type Source } from './produce.ts'
import { sseResponse, type HandlerOptions } from './handler.ts'

/** Returns a random stream ID with 128 bits of entropy, safe in URLs. */
export function newStreamId(): string {
  const bytes = crypto.getRandomValues(new Uint8Array(16))
  return btoa(String.fromCharCode(...bytes)).replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '')
}

export interface StartOptions extends Pick<HandlerOptions, 'store' | 'keepAliveMs' | 'retryMs'>, ProduceOptions {
  /** Builds the stream URL from the stream ID, e.g. id => `/streams/${id}`. */
  url: (streamId: string) => string
  /**
   * 'inline' (default) answers with the stream itself and a Stream-Location
   * header; 'location' answers 201 Created with a Location header.
   */
  mode?: 'inline' | 'location'
  /** Signal of the starting request; only stops the inline response, never the generation. */
  signal?: AbortSignal
}

/**
 * Starts a generation in the background and returns the response for the
 * request that started it (spec section 3.1).
 *
 * ```ts
 * app.post('/chat', (req) => start(async function* (signal) {
 *   for await (const token of model.stream(prompt, { signal })) yield { event: 'delta', data: { text: token } }
 * }, { store, url: (id) => `/streams/${id}`, signal: req.signal }))
 * ```
 */
export async function start(source: Source, opts: StartOptions): Promise<Response> {
  const id = newStreamId()
  await opts.store.create(id)
  void produce(opts.store, id, source, opts)
  const url = opts.url(id)
  if (opts.mode === 'location') {
    return new Response(JSON.stringify({ stream: url }), {
      status: 201,
      headers: { Location: url, 'Content-Type': 'application/json', 'Cache-Control': 'no-store' },
    })
  }
  return sseResponse(opts, id, 0, 'live', opts.signal, { 'Stream-Location': url })
}
