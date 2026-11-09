// RedisStore against a real server. Set REDIS_URL (e.g. redis://localhost:6379)
// to run these tests; they are skipped otherwise.
import assert from 'node:assert/strict'
import { createServer } from 'node:http'
import type { AddressInfo } from 'node:net'
import { after, before, describe, test } from 'node:test'
import { readStream } from '../src/client/index.ts'
import { passed, run } from '../src/conformance/index.ts'
import { createHandler, produce, RedisStore } from '../src/server/index.ts'
import { toNodeListener } from '../src/server/node.ts'
import { sleep, words } from './helpers.ts'
import { RespClient } from './resp.ts'
import { storeContract } from './store-contract.ts'

const url = process.env.REDIS_URL

describe('RedisStore', { skip: url ? false : 'REDIS_URL is not set' }, () => {
  const clients: RespClient[] = []
  const store = async (opts = {}) => {
    const c = await RespClient.open(url!)
    clients.push(c)
    return new RedisStore(c, { pollMs: 20, cancelGraceMs: 300, prefix: `rewind-test-${process.pid}:`, ...opts })
  }
  after(() => clients.forEach((c) => c.close()))

  storeContract('RedisStore', () => store())

  test('a stream produced on one instance is followed from another', async () => {
    const [a, b] = [await store(), await store()]
    await a.create('shared')
    const done = produce(a, 'shared', words(5, 10))
    const ids: number[] = []
    for await (const e of b.follow('shared', 0)) ids.push(e.id)
    assert.deepEqual(ids, [1, 2, 3, 4, 5, 6])
    assert.equal(await done, 'complete')
  })

  test('cancel on one instance stops the producer on another', async () => {
    const [a, b] = [await store(), await store()]
    await a.create('cancel-across')
    const done = produce(a, 'cancel-across', (signal) => words(1000, 5, signal))
    await sleep(60)
    assert.equal(await b.cancel('cancel-across'), 'requested')
    assert.equal(await done, 'cancelled')
    assert.equal((await b.snapshot('cancel-across', 0))?.status, 'cancelled')
  })

  test('cancel ends a stream whose producer is gone', async () => {
    const s = await store()
    await s.create('abandoned')
    await s.append('abandoned', 'delta', 'x')
    assert.equal(await s.cancel('abandoned'), 'requested')
    await sleep(500)
    assert.equal((await s.snapshot('abandoned', 0))?.status, 'cancelled')
  })

  test('finished streams expire after the retention time', async () => {
    const s = await store({ retentionMs: 150 })
    await s.create('short')
    await s.append('short', 'stream.end', '{"reason":"complete"}')
    assert.ok(await s.snapshot('short', 0))
    await sleep(300)
    assert.equal(await s.snapshot('short', 0), undefined)
  })

  describe('over HTTP', () => {
    let base = ''
    let producerStore: RedisStore
    const servers: ReturnType<typeof createServer>[] = []
    before(async () => {
      producerStore = await store()
      // The reading server uses its own store instance: like a second instance behind a load balancer.
      const handler = createHandler({ store: await store(), keepAliveMs: 1000, retryMs: 10 })
      const srv = createServer(toNodeListener(handler))
      servers.push(srv)
      await new Promise<void>((r) => srv.listen(0, '127.0.0.1', r))
      base = `http://127.0.0.1:${(srv.address() as AddressInfo).port}`
    })
    after(() => servers.forEach((s) => { s.closeAllConnections(); s.close() }))

    test('the client follows a stream produced elsewhere', async () => {
      await producerStore.create('http-1')
      void produce(producerStore, 'http-1', words(8, 10))
      const ids: number[] = []
      for await (const e of readStream(`${base}/streams/http-1`, { initialDelayMs: 1 })) ids.push(e.id)
      assert.equal(ids.length, 9)
    })

    test('the server conforms with RedisStore', async () => {
      await producerStore.create('http-conform')
      await produce(producerStore, 'http-conform', words(4))
      const results = await run(`${base}/streams/http-conform`)
      for (const r of results) assert.ok(r.passed, `${r.name}: ${r.detail}`)
      assert.ok(passed(results))
    })
  })
})
