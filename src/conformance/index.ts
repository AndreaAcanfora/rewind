/**
 * Wire conformance suite for the Resumable Stream Protocol. It reads an
 * existing, finished stream (it never modifies it) and checks the server's
 * responses against the spec.
 */
import { END, ERROR, isTerminal, type Snapshot, type StreamEvent } from '../protocol.ts'
import { SSEParser } from '../sse.ts'

export type Level = 'MUST' | 'SHOULD'

export interface Result {
  name: string
  level: Level
  section: string
  passed: boolean
  detail?: string
}

export interface RunOptions {
  fetch?: typeof fetch
  /** Extra headers sent with every request, e.g. Authorization. */
  headers?: Record<string, string>
  /** Per-request timeout. Default 10 s. */
  timeoutMs?: number
}

interface Read {
  status: number
  headers: Headers
  events: StreamEvent[]
  retry?: number
  closed: boolean
}

class Tester {
  url: string
  opts: RunOptions
  constructor(url: string, opts: RunOptions) {
    this.url = url
    this.opts = opts
  }

  async request(target: string, headers: Record<string, string>, method = 'GET'): Promise<Response> {
    const f = this.opts.fetch ?? globalThis.fetch
    return f(target, {
      method,
      headers: { ...this.opts.headers, ...headers },
      signal: AbortSignal.timeout(this.opts.timeoutMs ?? 10_000),
    })
  }

  /** Reads an SSE response to its end. `closed` is false if the server kept it open. */
  async sse(headers: Record<string, string> = {}, query = ''): Promise<Read> {
    const res = await this.request(this.url + query, { accept: 'text/event-stream', ...headers })
    const out: Read = { status: res.status, headers: res.headers, events: [], closed: true }
    if (!res.body) return out
    const parser = new SSEParser()
    const reader = res.body.pipeThrough(new TextDecoderStream()).getReader()
    // A finished stream must close after its terminal event.
    const deadline = setTimeout(() => {
      out.closed = false
      reader.cancel().catch(() => {})
    }, 5_000)
    try {
      for (;;) {
        const { done, value } = await reader.read()
        if (done) break
        for (const e of parser.feed(value)) out.events.push({ id: Number(e.id), event: e.event, data: e.data })
      }
    } catch {
      out.closed = false
    } finally {
      clearTimeout(deadline)
    }
    if (parser.retry !== undefined) out.retry = parser.retry
    return out
  }

  async snapshot(after: number): Promise<{ status: number; body?: Snapshot; headers: Headers }> {
    const res = await this.request(`${this.url}${this.url.includes('?') ? '&' : '?'}after=${after}`, { accept: 'application/json' })
    if (res.status !== 200) return { status: res.status, headers: res.headers }
    return { status: res.status, headers: res.headers, body: (await res.json()) as Snapshot }
  }

  sibling(): string {
    const u = new URL(this.url)
    const i = u.pathname.lastIndexOf('/')
    const rand = Array.from(crypto.getRandomValues(new Uint8Array(16)), (b) => b.toString(16).padStart(2, '0')).join('')
    u.pathname = u.pathname.slice(0, i + 1) + rand
    return u.href
  }
}

type Case = { name: string; level: Level; section: string; run: (t: Tester, full: StreamEvent[]) => Promise<void> }

function assert(cond: unknown, msg: string | undefined = 'assertion failed'): asserts cond {
  if (!cond) throw new Error(msg)
}

function sameEvents(got: StreamEvent[], want: StreamEvent[]): string | undefined {
  if (got.length !== want.length) return `got ${got.length} events, want ${want.length}`
  for (let i = 0; i < want.length; i++) {
    const g = got[i]!
    const w = want[i]!
    if (g.id !== w.id || g.event !== w.event || g.data !== w.data) return `event ${i}: got ${JSON.stringify(g)}, want ${JSON.stringify(w)}`
  }
  return undefined
}

const middle = (full: StreamEvent[]) => Math.floor(full.length / 2)

const cases: Case[] = [
  {
    name: 'resume with Last-Event-ID returns exactly the following events',
    level: 'MUST',
    section: '4.1',
    async run(t, full) {
      const k = middle(full)
      const r = await t.sse({ 'last-event-id': String(k) })
      assert(r.status === 200, `status ${r.status}`)
      const diff = sameEvents(r.events, full.slice(k))
      assert(!diff, diff)
    },
  },
  {
    name: 'resume with the after parameter',
    level: 'MUST',
    section: '4',
    async run(t, full) {
      const k = middle(full)
      const r = await t.sse({}, `${t.url.includes('?') ? '&' : '?'}after=${k}`)
      const diff = sameEvents(r.events, full.slice(k))
      assert(!diff, diff)
    },
  },
  {
    name: 'Last-Event-ID wins over after',
    level: 'MUST',
    section: '4',
    async run(t, full) {
      const k = full.length - 1
      const r = await t.sse({ 'last-event-id': String(k) }, `${t.url.includes('?') ? '&' : '?'}after=0`)
      const diff = sameEvents(r.events, full.slice(k))
      assert(!diff, diff)
    },
  },
  {
    name: 'cursor at the terminal event returns 204',
    level: 'MUST',
    section: '4.1',
    async run(t, full) {
      const r = await t.sse({ 'last-event-id': String(full.length) })
      assert(r.status === 204, `status ${r.status}`)
    },
  },
  {
    name: 'cursor beyond the terminal event returns 204',
    level: 'MUST',
    section: '4.1',
    async run(t, full) {
      const r = await t.sse({ 'last-event-id': String(full.length + 10) })
      assert(r.status === 204, `status ${r.status}`)
    },
  },
  {
    name: 'invalid cursors are rejected with 400',
    level: 'MUST',
    section: '4',
    async run(t) {
      for (const bad of ['abc', '-1', '01', '1.5']) {
        const r = await t.request(t.url, { accept: 'text/event-stream', 'last-event-id': bad })
        await r.body?.cancel()
        assert(r.status === 400, `cursor ${JSON.stringify(bad)}: status ${r.status}`)
      }
    },
  },
  {
    name: 'snapshot matches the SSE events',
    level: 'MUST',
    section: '4.2',
    async run(t, full) {
      const s = await t.snapshot(0)
      assert(s.status === 200 && s.body, `status ${s.status}`)
      assert(['complete', 'cancelled', 'error'].includes(s.body.status), `status field ${s.body.status}`)
      assert(s.body.last_id === full.length, `last_id ${s.body.last_id}, want ${full.length}`)
      const diff = sameEvents(s.body.events, full)
      assert(!diff, diff)
    },
  },
  {
    name: 'snapshot honours the cursor',
    level: 'MUST',
    section: '4.2',
    async run(t, full) {
      const k = middle(full)
      const s = await t.snapshot(k)
      assert(s.body, `status ${s.status}`)
      const diff = sameEvents(s.body.events, full.slice(k))
      assert(!diff, diff)
    },
  },
  {
    name: 'responses are not cacheable',
    level: 'MUST',
    section: '8',
    async run(t) {
      const r = await t.sse({ 'last-event-id': '0' })
      assert(/no-store/.test(r.headers.get('cache-control') ?? ''), `SSE Cache-Control: ${r.headers.get('cache-control')}`)
      const s = await t.snapshot(0)
      assert(/no-store/.test(s.headers.get('cache-control') ?? ''), `snapshot Cache-Control: ${s.headers.get('cache-control')}`)
    },
  },
  {
    name: 'unknown stream returns 404',
    level: 'SHOULD',
    section: '3.2',
    async run(t) {
      const r = await t.request(t.sibling(), { accept: 'text/event-stream' })
      await r.body?.cancel()
      assert(r.status === 404, `status ${r.status}`)
    },
  },
  {
    name: 'SSE response sends retry',
    level: 'SHOULD',
    section: '4.1',
    async run(t) {
      const r = await t.sse()
      assert(r.retry !== undefined, 'no retry field')
    },
  },
  {
    name: 'Stream-Status header',
    level: 'SHOULD',
    section: '4.1',
    async run(t) {
      const r = await t.sse()
      const s = r.headers.get('stream-status')
      assert(s && ['complete', 'cancelled', 'error'].includes(s), `Stream-Status: ${s}`)
    },
  },
  {
    name: 'DELETE on a finished stream returns 204 or 405',
    level: 'MUST',
    section: '6',
    async run(t) {
      const r = await t.request(t.url, {}, 'DELETE')
      await r.body?.cancel()
      assert(r.status === 204 || r.status === 405, `status ${r.status}`)
    },
  },
]

/** Runs the suite against `url`, the URL of a finished stream. */
export async function run(url: string, opts: RunOptions = {}): Promise<Result[]> {
  const t = new Tester(url, opts)
  const results: Result[] = []
  const record = (name: string, level: Level, section: string, err?: unknown) =>
    results.push({ name, level, section, passed: !err, ...(err ? { detail: err instanceof Error ? err.message : String(err) } : {}) })

  // The baseline read checks the stream itself; the other cases compare against it.
  let full: StreamEvent[] = []
  try {
    const r = await t.sse()
    assert(r.status === 200, `status ${r.status}`)
    assert((r.headers.get('content-type') ?? '').startsWith('text/event-stream'), `Content-Type: ${r.headers.get('content-type')}`)
    assert(r.events.length >= 2, `the stream needs at least one event and a terminal event, got ${r.events.length} events`)
    r.events.forEach((e, i) => assert(e.id === i + 1, `event ${i} has id ${e.id}, want ${i + 1}`))
    const last = r.events.at(-1)!
    assert(isTerminal(last), `the stream is not finished: last event is ${JSON.stringify(last.event)}`)
    assert(r.events.slice(0, -1).every((e) => !isTerminal(e)), 'terminal event before the end')
    assert(r.closed, 'the server kept the response open after the terminal event')
    const data = JSON.parse(last.data) as Record<string, unknown>
    if (last.event === END) assert(data.reason === 'complete' || data.reason === 'cancelled', `stream.end reason ${String(data.reason)}`)
    if (last.event === ERROR) assert(typeof data.code === 'string' && typeof data.message === 'string', 'stream.error needs code and message')
    full = r.events
    record('full read: ids 1..N, one terminal event, response closed', 'MUST', '4.1')
  } catch (err) {
    record('full read: ids 1..N, one terminal event, response closed', 'MUST', '4.1', err)
    return results
  }

  for (const c of cases) {
    try {
      await c.run(t, full)
      record(c.name, c.level, c.section)
    } catch (err) {
      record(c.name, c.level, c.section, err)
    }
  }
  return results
}

/** Reports whether every MUST case passed. */
export function passed(results: Result[]): boolean {
  return results.every((r) => r.passed || r.level !== 'MUST')
}
