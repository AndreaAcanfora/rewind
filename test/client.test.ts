import assert from 'node:assert/strict'
import { after, before, test } from 'node:test'
import type { StreamEvent } from '../src/protocol.ts'
import { encodeEvent } from '../src/sse.ts'
import {
  fetchSnapshot,
  readStream,
  startStream,
  StreamHTTPError,
  StreamNotFoundError,
  StreamUnavailableError,
  type ReadOptions,
} from '../src/client/index.ts'
import { flakyFetch, serve, sleep, words, type TestServer } from './helpers.ts'

let srv: TestServer
before(async () => {
  srv = await serve()
})
after(() => srv.close())

async function collect(url: string, opts: ReadOptions = {}): Promise<StreamEvent[]> {
  const out: StreamEvent[] = []
  for await (const e of readStream(url, { initialDelayMs: 1, ...opts })) out.push(e)
  return out
}

function text(events: StreamEvent[]): string {
  return events
    .filter((e) => e.event === 'delta')
    .map((e) => JSON.parse(e.data).text)
    .join('')
}

test('reads a stream to the end', async () => {
  const url = await srv.stream(words(5, 2))
  const events = await collect(url)
  assert.equal(text(events), 'w1 w2 w3 w4 w5 ')
  assert.equal(events.at(-1)!.event, 'stream.end')
})

test('resumes after every dropped connection, without duplicates or losses', async () => {
  const url = await srv.stream(words(20, 2))
  const flaky = flakyFetch(3)
  const reconnects: number[] = []
  const events = await collect(url, { fetch: flaky.fetch, onReconnect: (i) => reconnects.push(i.cursor) })
  assert.deepEqual(events.map((e) => e.id), Array.from({ length: 21 }, (_, i) => i + 1))
  assert.equal(text(events), Array.from({ length: 20 }, (_, i) => `w${i + 1} `).join(''))
  assert.ok(flaky.calls.length >= 7, `only ${flaky.calls.length} connections`)
  assert.deepEqual(
    flaky.calls.map((c) => c.lastEventId),
    ['0', ...reconnects.map(String)],
  )
})

test('resumes from a saved cursor', async () => {
  const url = await srv.stream(words(4))
  await sleep(10)
  const cursors: number[] = []
  const events = await collect(url, { cursor: 2, onCursor: (c) => cursors.push(c) })
  assert.deepEqual(events.map((e) => e.id), [3, 4, 5])
  assert.deepEqual(cursors, [3, 4, 5])
})

test('ends quietly when the stream already finished (204)', async () => {
  const url = await srv.stream(words(1))
  await sleep(10)
  assert.deepEqual(await collect(url, { cursor: 2 }), [])
})

test('throws StreamNotFoundError on 404', async () => {
  await assert.rejects(collect(`${srv.base}/streams/nope`), StreamNotFoundError)
})

/** A fetch that serves fixed SSE bodies, one per call. */
function scripted(bodies: (string | number)[]) {
  let i = 0
  const lastIds: (string | null)[] = []
  const f: typeof fetch = async (_input, init) => {
    lastIds.push(new Headers(init?.headers).get('last-event-id'))
    const b = bodies[Math.min(i++, bodies.length - 1)]!
    if (typeof b === 'number') return new Response(null, { status: b })
    return new Response(b, { headers: { 'content-type': 'text/event-stream' } })
  }
  return { fetch: f, lastIds }
}

const ev = (id: number, event = 'delta', data = `d${id}`) => encodeEvent({ id, event, data })
const end = (id: number) => ev(id, 'stream.end', '{"reason":"complete"}')

test('drops duplicates replayed by the server', async () => {
  const s = scripted([ev(1) + ev(2), ev(1) + ev(2) + ev(3) + end(4)])
  const events = await collect('http://x/s', { fetch: s.fetch })
  assert.deepEqual(events.map((e) => e.id), [1, 2, 3, 4])
})

test('reconnects from its cursor when it detects a gap', async () => {
  const s = scripted([ev(1) + ev(3), ev(2) + ev(3) + end(4)])
  const events = await collect('http://x/s', { fetch: s.fetch })
  assert.deepEqual(events.map((e) => e.id), [1, 2, 3, 4])
  assert.deepEqual(s.lastIds, ['0', '1'])
})

test('retries 5xx and 429 but not other 4xx', async () => {
  const ok = scripted([503, 429, ev(1) + end(2)])
  assert.equal((await collect('http://x/s', { fetch: ok.fetch })).length, 2)
  const denied = scripted([401])
  await assert.rejects(collect('http://x/s', { fetch: denied.fetch }), (e: unknown) => e instanceof StreamHTTPError && e.status === 401)
})

test('gives up after maxAttempts consecutive failures', async () => {
  const s = scripted([500])
  await assert.rejects(collect('http://x/s', { fetch: s.fetch, maxAttempts: 3 }), StreamUnavailableError)
  assert.equal(s.lastIds.length, 3)
})

test('backs off exponentially and resets after progress', async () => {
  const delays: number[] = []
  const s = scripted([500, 500, ev(1), 500, end(2)])
  await collect('http://x/s', { fetch: s.fetch, initialDelayMs: 1, onReconnect: (i) => delays.push(i.delayMs) })
  assert.deepEqual(delays, [1, 2, 1, 1])
})

test('stops when aborted', async () => {
  const url = await srv.stream(words(1000, 5))
  const ac = new AbortController()
  const got: StreamEvent[] = []
  setTimeout(() => ac.abort(), 30)
  await assert.rejects(async () => {
    for await (const e of readStream(url, { signal: ac.signal })) got.push(e)
  })
  assert.ok(got.length > 0 && got.length < 1000)
})

test('startStream follows an inline start, across drops', async () => {
  const flaky = flakyFetch(2)
  const s = await startStream(`${srv.base}/chat`, { method: 'POST' }, { fetch: flaky.fetch, initialDelayMs: 1 })
  assert.match(s.url, /^http:\/\/127\.0\.0\.1:\d+\/streams\//)
  const events: StreamEvent[] = []
  for await (const e of s.events) events.push(e)
  assert.equal(text(events), 'w1 w2 w3 w4 w5 ')
})

test('startStream follows a 201 Location start', async () => {
  const other = await serve({ mode: 'location' })
  try {
    const s = await startStream(`${other.base}/chat`, { method: 'POST' })
    const events: StreamEvent[] = []
    for await (const e of s.events) events.push(e)
    assert.equal(text(events), 'w1 w2 w3 w4 w5 ')
  } finally {
    await other.close()
  }
})

test('fetchSnapshot', async () => {
  const url = await srv.stream(words(3))
  await sleep(10)
  const snap = await fetchSnapshot(url, 2)
  assert.equal(snap.status, 'complete')
  assert.deepEqual(snap.events.map((e) => e.id), [3, 4])
})
