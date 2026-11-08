import assert from 'node:assert/strict'
import { test } from 'node:test'
import { MemoryStore, produce, StreamClosedError } from '../src/server/index.ts'
import { sleep, words } from './helpers.ts'

async function collect<T>(it: AsyncIterable<T>): Promise<T[]> {
  const out: T[] = []
  for await (const x of it) out.push(x)
  return out
}

test('numbers events and finishes on the terminal event', async () => {
  const store = new MemoryStore()
  await store.create('a')
  assert.equal((await store.append('a', 'delta', 'x')).id, 1)
  assert.equal((await store.append('a', 'stream.end', '{"reason":"complete"}')).id, 2)
  await assert.rejects(store.append('a', 'delta', 'late'), StreamClosedError)
  const snap = await store.snapshot('a', 1)
  assert.deepEqual(snap, { status: 'complete', last_id: 2, events: [{ id: 2, event: 'stream.end', data: '{"reason":"complete"}' }] })
  assert.equal(await store.snapshot('missing', 0), undefined)
  await assert.rejects(store.create('a'))
})

test('follow replays, then waits for live events', async () => {
  const store = new MemoryStore()
  await store.create('a')
  await store.append('a', 'delta', '1')
  const got = collect(store.follow('a', 0))
  await sleep(5)
  await store.append('a', 'delta', '2')
  await sleep(5)
  await store.append('a', 'stream.end', '{"reason":"complete"}')
  assert.deepEqual((await got).map((e) => e.id), [1, 2, 3])
  // From a cursor, after the end.
  assert.deepEqual((await collect(store.follow('a', 2))).map((e) => e.id), [3])
  assert.deepEqual(await collect(store.follow('a', 3)), [])
})

test('follow stops when its signal aborts', async () => {
  const store = new MemoryStore()
  await store.create('a')
  const ac = new AbortController()
  const got = collect(store.follow('a', 0, ac.signal))
  setTimeout(() => ac.abort(), 10)
  assert.deepEqual(await got, [])
})

test('produce writes chunks and the terminal event', async () => {
  const store = new MemoryStore()
  await store.create('a')
  assert.equal(await produce(store, 'a', words(3)), 'complete')
  const snap = await store.snapshot('a', 0)
  assert.deepEqual(snap?.events.map((e) => e.event), ['delta', 'delta', 'delta', 'stream.end'])
  assert.equal(snap?.events[0]?.data, '{"text":"w1 "}')
})

test('produce hides error details by default', async () => {
  const store = new MemoryStore()
  await store.create('a')
  async function* failing() {
    yield 'partial'
    throw new Error('database password is hunter2')
  }
  assert.equal(await produce(store, 'a', failing()), 'error')
  const last = (await store.snapshot('a', 0))!.events.at(-1)!
  assert.equal(last.event, 'stream.error')
  assert.doesNotMatch(last.data, /hunter2/)
  assert.equal(JSON.parse(last.data).code, 'generation_failed')
})

test('produce rejects reserved event types from the source', async () => {
  const store = new MemoryStore()
  await store.create('a')
  async function* sneaky() {
    yield { event: 'stream.end', data: { reason: 'complete' } }
  }
  assert.equal(await produce(store, 'a', sneaky()), 'error')
})

test('cancel aborts the producer signal', async () => {
  const store = new MemoryStore()
  await store.create('a')
  let aborted = false
  const done = produce(store, 'a', async function* (signal) {
    signal.addEventListener('abort', () => (aborted = true))
    yield* words(1000, 2, signal)
  })
  await sleep(15)
  assert.equal(await store.cancel('a'), 'requested')
  assert.equal(await done, 'cancelled')
  assert.ok(aborted)
  const snap = await store.snapshot('a', 0)
  assert.equal(snap?.status, 'cancelled')
  assert.ok(snap!.last_id < 1000)
  assert.equal(await store.cancel('a'), 'finished')
  assert.equal(await store.cancel('nope'), 'missing')
})

test('cancel without a producer ends the stream itself', async () => {
  const store = new MemoryStore()
  await store.create('a')
  assert.equal(await store.cancel('a'), 'requested')
  assert.equal((await store.snapshot('a', 0))?.status, 'cancelled')
})

test('finished streams expire after the retention time', async () => {
  const store = new MemoryStore({ retentionMs: 20 })
  await store.create('a')
  await store.append('a', 'stream.end', '{"reason":"complete"}')
  assert.equal(store.size, 1)
  await sleep(40)
  assert.equal(store.size, 0)
})

test('live streams end with an error after their maximum lifetime', async () => {
  const store = new MemoryStore({ maxLifetimeMs: 20 })
  await store.create('a')
  await sleep(40)
  const snap = await store.snapshot('a', 0)
  assert.equal(snap?.status, 'error')
  assert.equal(JSON.parse(snap!.events[0]!.data).code, 'timeout')
})
