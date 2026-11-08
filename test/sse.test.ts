import assert from 'node:assert/strict'
import { test } from 'node:test'
import { parseCursor, statusOf } from '../src/protocol.ts'
import { encodeEvent, SSEParser } from '../src/sse.ts'

test('encodes events', () => {
  assert.equal(encodeEvent({ id: 1, event: 'message', data: 'hi' }), 'id: 1\ndata: hi\n\n')
  assert.equal(encodeEvent({ id: 2, event: 'delta', data: 'a\nb' }), 'id: 2\nevent: delta\ndata: a\ndata: b\n\n')
})

test('round-trips events split at every position', () => {
  const events = [
    { id: 1, event: 'message', data: 'plain' },
    { id: 2, event: 'delta', data: '{"text":"multi\\nline"}' },
    { id: 3, event: 'delta', data: 'line one\nline two\n\nafter blank' },
    { id: 4, event: 'stream.end', data: '{"reason":"complete"}' },
  ]
  const wire = 'retry: 500\n\n: comment\n' + events.map(encodeEvent).join('')
  for (let cut = 0; cut <= wire.length; cut++) {
    const p = new SSEParser()
    const got = [...p.feed(wire.slice(0, cut)), ...p.feed(wire.slice(cut))]
    assert.deepEqual(
      got,
      events.map((e) => ({ id: String(e.id), event: e.event, data: e.data })),
      `cut at ${cut}`,
    )
    assert.equal(p.retry, 500)
  }
})

test('parses CRLF and CR line endings and field edge cases', () => {
  const p = new SSEParser()
  const got = p.feed('id: 1\r\nevent: x\r\ndata:no-space\r\n\r\nid: 2\rdata\r\r\n')
  assert.deepEqual(got, [
    { id: '1', event: 'x', data: 'no-space' },
    { id: '2', event: 'message', data: '' },
  ])
})

test('a CR at the end of a chunk waits for a possible LF', () => {
  const p = new SSEParser()
  assert.deepEqual(p.feed('id: 1\r\ndata: a\r'), [])
  assert.deepEqual(p.feed('\n\r\n'), [{ id: '1', event: 'message', data: 'a' }])
})

test('parses cursors strictly', () => {
  assert.equal(parseCursor('0'), 0)
  assert.equal(parseCursor('42'), 42)
  for (const bad of ['', '-1', '01', '1.5', 'abc', ' 1', '1e3', '9999999999999999']) assert.equal(parseCursor(bad), undefined, bad)
})

test('derives the status from the terminal event', () => {
  assert.equal(statusOf({ id: 1, event: 'stream.end', data: '{"reason":"complete"}' }), 'complete')
  assert.equal(statusOf({ id: 1, event: 'stream.end', data: '{"reason":"cancelled"}' }), 'cancelled')
  assert.equal(statusOf({ id: 1, event: 'stream.error', data: '{}' }), 'error')
})
