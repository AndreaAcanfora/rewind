import assert from 'node:assert/strict'
import { createServer } from 'node:http'
import type { AddressInfo } from 'node:net'
import { after, before, test } from 'node:test'
import { passed, run } from '../src/conformance/index.ts'
import { encodeEvent } from '../src/sse.ts'
import { serve, sleep, words, type TestServer } from './helpers.ts'

let srv: TestServer
before(async () => {
  srv = await serve()
})
after(() => srv.close())

test('the reference server conforms', async () => {
  for (const source of [words(6), words(1)]) {
    const url = await srv.stream(source)
    await sleep(10)
    const results = await run(url)
    for (const r of results) assert.ok(r.passed, `${r.name}: ${r.detail}`)
  }
})

test('a naive SSE server that ignores cursors does not conform', async () => {
  const body = [1, 2, 3].map((id) => encodeEvent({ id, event: 'delta', data: 'x' })).join('') +
    encodeEvent({ id: 4, event: 'stream.end', data: '{"reason":"complete"}' })
  const naive = createServer((_req, res) => {
    res.writeHead(200, { 'content-type': 'text/event-stream' })
    res.end(body)
  })
  await new Promise<void>((r) => naive.listen(0, '127.0.0.1', r))
  try {
    const url = `http://127.0.0.1:${(naive.address() as AddressInfo).port}/streams/x`
    const results = await run(url)
    assert.equal(passed(results), false)
    const failed = results.filter((r) => !r.passed).map((r) => r.name)
    assert.ok(failed.includes('resume with Last-Event-ID returns exactly the following events'))
    assert.ok(failed.includes('cursor at the terminal event returns 204'))
  } finally {
    naive.closeAllConnections()
    naive.close()
  }
})
