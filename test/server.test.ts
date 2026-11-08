import assert from 'node:assert/strict'
import { after, before, test } from 'node:test'
import { SSEParser } from '../src/sse.ts'
import { serve, sleep, words, type TestServer } from './helpers.ts'

let srv: TestServer
before(async () => {
  srv = await serve({ keepAliveMs: 30 })
})
after(() => srv.close())

async function readAll(res: Response) {
  const p = new SSEParser()
  const text = await res.text()
  return { text, events: p.feed(text), retry: p.retry }
}

test('serves a finished stream as SSE, from any cursor', async () => {
  const url = await srv.stream(words(3))
  await sleep(10)
  const full = await fetch(url)
  assert.equal(full.status, 200)
  assert.match(full.headers.get('content-type')!, /^text\/event-stream/)
  assert.equal(full.headers.get('cache-control'), 'no-store')
  assert.equal(full.headers.get('stream-status'), 'complete')
  const all = await readAll(full)
  assert.deepEqual(all.events.map((e) => e.id), ['1', '2', '3', '4'])
  assert.equal(all.events[3]!.event, 'stream.end')
  assert.equal(all.retry, 10)

  const tail = await readAll(await fetch(url, { headers: { 'last-event-id': '2' } }))
  assert.deepEqual(tail.events.map((e) => e.id), ['3', '4'])
  const viaQuery = await readAll(await fetch(url + '?after=3'))
  assert.deepEqual(viaQuery.events.map((e) => e.id), ['4'])
  const headerWins = await readAll(await fetch(url + '?after=0', { headers: { 'last-event-id': '3' } }))
  assert.deepEqual(headerWins.events.map((e) => e.id), ['4'])
})

test('204 at the end, 400 on bad cursors, 404 on unknown streams', async () => {
  const url = await srv.stream(words(1))
  await sleep(10)
  assert.equal((await fetch(url, { headers: { 'last-event-id': '2' } })).status, 204)
  assert.equal((await fetch(url, { headers: { 'last-event-id': '99' } })).status, 204)
  assert.equal((await fetch(url, { headers: { 'last-event-id': 'x' } })).status, 400)
  assert.equal((await fetch(`${srv.base}/streams/nope`)).status, 404)
})

test('follows a live stream and sends keep-alive comments while idle', async () => {
  const url = await srv.stream(words(2, 80))
  const r = await readAll(await fetch(url))
  assert.deepEqual(r.events.map((e) => e.event), ['delta', 'delta', 'stream.end'])
  assert.match(r.text, /^: keep-alive$/m)
})

test('several readers get the same events', async () => {
  const url = await srv.stream(words(5, 5))
  const [a, b] = await Promise.all([readAll(await fetch(url)), readAll(await fetch(url))])
  assert.deepEqual(a.events, b.events)
  assert.equal(a.events.length, 6)
})

test('a live cursor beyond the end waits for later events', async () => {
  const url = await srv.stream(words(4, 10))
  const r = await readAll(await fetch(url, { headers: { 'last-event-id': '3' } }))
  assert.deepEqual(r.events.map((e) => e.id), ['4', '5'])
})

test('JSON snapshot', async () => {
  const url = await srv.stream(words(2))
  await sleep(10)
  const res = await fetch(url + '?after=1', { headers: { accept: 'application/json' } })
  assert.equal(res.headers.get('cache-control'), 'no-store')
  const body = await res.json()
  assert.equal(body.status, 'complete')
  assert.equal(body.last_id, 3)
  assert.deepEqual(body.events.map((e: { id: number }) => e.id), [2, 3])
})

test('DELETE cancels a live stream, then returns 204', async () => {
  const url = await srv.stream((signal) => words(1000, 5, signal))
  await sleep(20)
  assert.equal((await fetch(url, { method: 'DELETE' })).status, 202)
  await sleep(20)
  const snap = await (await fetch(url, { headers: { accept: 'application/json' } })).json()
  assert.equal(snap.status, 'cancelled')
  assert.equal((await fetch(url, { method: 'DELETE' })).status, 204)
  assert.equal((await fetch(`${srv.base}/streams/nope`, { method: 'DELETE' })).status, 404)
  assert.equal((await fetch(url, { method: 'PUT' })).status, 405)
})

test('a client disconnecting does not stop the generation', async () => {
  const url = await srv.stream(words(5, 10))
  const res = await fetch(url)
  const reader = res.body!.getReader()
  await reader.read()
  await reader.cancel()
  await sleep(100)
  const snap = await (await fetch(url, { headers: { accept: 'application/json' } })).json()
  assert.equal(snap.status, 'complete')
  assert.equal(snap.last_id, 6)
})

test('start answers inline with Stream-Location', async () => {
  const res = await fetch(`${srv.base}/chat`, { method: 'POST', body: 'hi' })
  assert.equal(res.status, 200)
  const loc = res.headers.get('stream-location')!
  assert.match(loc, /^\/streams\/[A-Za-z0-9_-]{22}$/)
  const r = await readAll(res)
  assert.equal(r.events.length, 6)
})

test('start answers 201 with Location', async () => {
  const s = await serve({ mode: 'location' })
  try {
    const res = await fetch(`${s.base}/chat`, { method: 'POST' })
    assert.equal(res.status, 201)
    const loc = res.headers.get('location')!
    assert.deepEqual(await res.json(), { stream: loc })
    const r = await readAll(await fetch(new URL(loc, s.base)))
    assert.equal(r.events.at(-1)!.event, 'stream.end')
  } finally {
    await s.close()
  }
})
