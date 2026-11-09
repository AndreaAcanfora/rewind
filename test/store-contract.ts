// Behaviour every Store must have. Run for MemoryStore and RedisStore.
import assert from 'node:assert/strict'
import { test } from 'node:test'
import { produce, StreamClosedError, type Store } from '../src/server/index.ts'
import { sleep, words } from './helpers.ts'

async function collect<T>(it: AsyncIterable<T>): Promise<T[]> {
  const out: T[] = []
  for await (const x of it) out.push(x)
  return out
}

let n = 0
const fresh = () => `contract-${process.pid}-${Date.now()}-${++n}`

export function storeContract(name: string, make: () => Store | Promise<Store>) {
  test(`${name}: numbers events and finishes on the terminal event`, async () => {
    const store = await make()
    const id = fresh()
    await store.create(id)
    assert.equal((await store.append(id, 'delta', 'x')).id, 1)
    assert.equal((await store.append(id, 'stream.end', '{"reason":"complete"}')).id, 2)
    await assert.rejects(store.append(id, 'delta', 'late'), StreamClosedError)
    assert.deepEqual(await store.snapshot(id, 1), { status: 'complete', last_id: 2, events: [{ id: 2, event: 'stream.end', data: '{"reason":"complete"}' }] })
    assert.equal(await store.snapshot(fresh(), 0), undefined)
    await assert.rejects(store.create(id))
  })

  test(`${name}: data survives the round trip unchanged`, async () => {
    const store = await make()
    const id = fresh()
    await store.create(id)
    const data = 'line one\nline "two" — ünïcødé 🚀'
    await store.append(id, 'delta', data)
    assert.equal((await store.snapshot(id, 0))!.events[0]!.data, data)
  })

  test(`${name}: follow replays, then waits for live events`, async () => {
    const store = await make()
    const id = fresh()
    await store.create(id)
    await store.append(id, 'delta', '1')
    const got = collect(store.follow(id, 0))
    await sleep(30)
    await store.append(id, 'delta', '2')
    await sleep(30)
    await store.append(id, 'stream.end', '{"reason":"complete"}')
    assert.deepEqual((await got).map((e) => e.id), [1, 2, 3])
    assert.deepEqual((await collect(store.follow(id, 2))).map((e) => e.id), [3])
    assert.deepEqual(await collect(store.follow(id, 3)), [])
    assert.deepEqual(await collect(store.follow(fresh(), 0)), [])
  })

  test(`${name}: follow stops when its signal aborts`, async () => {
    const store = await make()
    const id = fresh()
    await store.create(id)
    const ac = new AbortController()
    const got = collect(store.follow(id, 0, ac.signal))
    setTimeout(() => ac.abort(), 30)
    assert.deepEqual(await got, [])
  })

  test(`${name}: produce writes chunks and the terminal event`, async () => {
    const store = await make()
    const id = fresh()
    await store.create(id)
    assert.equal(await produce(store, id, words(3)), 'complete')
    const snap = await store.snapshot(id, 0)
    assert.deepEqual(snap?.events.map((e) => e.event), ['delta', 'delta', 'delta', 'stream.end'])
  })

  test(`${name}: cancel stops the producer`, async () => {
    const store = await make()
    const id = fresh()
    await store.create(id)
    const done = produce(store, id, (signal) => words(1000, 5, signal))
    await sleep(60)
    assert.equal(await store.cancel(id), 'requested')
    assert.equal(await done, 'cancelled')
    const snap = await store.snapshot(id, 0)
    assert.equal(snap?.status, 'cancelled')
    assert.ok(snap!.last_id < 1000)
    assert.equal(await store.cancel(id), 'finished')
    assert.equal(await store.cancel(fresh()), 'missing')
  })

  test(`${name}: writes racing the terminal event stay invisible`, async () => {
    const store = await make()
    const id = fresh()
    await store.create(id)
    await store.append(id, 'delta', 'a')
    await Promise.allSettled([store.append(id, 'stream.end', '{"reason":"complete"}'), store.append(id, 'delta', 'b'), store.append(id, 'delta', 'c')])
    const snap = (await store.snapshot(id, 0))!
    assert.equal(snap.events.at(-1)!.event, 'stream.end')
    assert.deepEqual(snap.events.map((e) => e.id), Array.from({ length: snap.last_id }, (_, i) => i + 1))
  })
}
