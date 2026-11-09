# Rewind: the Resumable Stream Protocol

[![CI](https://github.com/AndreaAcanfora/rewind/actions/workflows/ci.yml/badge.svg)](https://github.com/AndreaAcanfora/rewind/actions/workflows/ci.yml)
[![npm](https://img.shields.io/npm/v/rewind-stream)](https://www.npmjs.com/package/rewind-stream)

LLM answers that survive dropped connections, page reloads and multiple tabs.
An open wire protocol on top of Server-Sent Events, with a TypeScript server,
client and conformance suite. No runtime dependencies.

- **Specification:** [SPEC.md](SPEC.md) (version 0.1, draft)
- **Works with the browser's native `EventSource`**, no library needed to read a stream.
- **Framework-neutral:** the server is a Fetch API handler (Node, Deno, Bun,
  Cloudflare Workers, Next.js, Hono…), and the store is an interface.

## The problem

A model streams an answer for 30 seconds. Halfway through, the phone switches
from Wi-Fi to 4G, or the user reloads the page, or a proxy closes an idle
connection. In most apps the answer is gone: the user sees half a message,
and the app either gives up or pays for the whole generation again.

The fix is simple, and many teams build it in-house: run the generation
independently of the connection, number the events, keep them for a while, and
let the client reconnect from the last number it saw. Every framework and
vendor does it differently, so nothing is interoperable. Rewind writes it
down as a small protocol.

## How it works

```
POST /chat                 →  200 text/event-stream, Stream-Location: /streams/Xy…
                              id: 1  event: delta  data: {"text":"Hel"}
                              id: 2  event: delta  data: {"text":"lo"}
        ✂ connection lost
GET /streams/Xy…           →  id: 3  event: delta  data: {"text":" world"}
Last-Event-ID: 2              id: 4  event: stream.end  data: {"reason":"complete"}
GET /streams/Xy…           →  204 No Content   (finished: EventSource stops)
Last-Event-ID: 4
GET /streams/Xy…?after=0   →  {"status":"complete","last_id":4,"events":[…]}
Accept: application/json      (snapshot: render at once after a page reload)
DELETE /streams/Xy…        →  202 Accepted   (stop the generation)
```

Events are numbered 1, 2, 3… without gaps, so clients drop duplicates and
notice losses. The stream ends with exactly one terminal event, `stream.end`
or `stream.error`, which is replayed like any other event. The full rules are
in [SPEC.md](SPEC.md).

## Server

```ts
import { createHandler, MemoryStore, start } from 'rewind-stream/server'
import Anthropic from '@anthropic-ai/sdk'

const store = new MemoryStore()
const streams = createHandler({ store }) // GET (SSE or snapshot) and DELETE
const anthropic = new Anthropic()

// Any Fetch API router. This is a Next.js-style pair of route handlers.
export async function POST(req: Request) {
  const { prompt } = await req.json()
  return start(
    async function* (signal) {
      const stream = anthropic.messages.stream(
        { model: 'claude-sonnet-5-5', max_tokens: 1024, messages: [{ role: 'user', content: prompt }] },
        { signal }, // cancelling the stream really stops the model
      )
      for await (const e of stream) {
        if (e.type === 'content_block_delta' && e.delta.type === 'text_delta') {
          yield { event: 'delta', data: { text: e.delta.text } }
        }
      }
    },
    { store, url: (id) => `/streams/${id}`, signal: req.signal },
  )
}

export const GET = streams    // /streams/[id]
export const DELETE = streams
```

The generation runs in the background: a client disconnecting never stops
it. Errors thrown by the source end the stream with `stream.error`, without
leaking the error message unless you pass `formatError`.

For plain `node:http`, wrap the handler:

```ts
import { toNodeListener } from 'rewind-stream/server/node'
http.createServer(toNodeListener(handler)).listen(3000)
```

`MemoryStore` keeps streams in the process, which suits a single server.
With several instances, or serverless functions, use `RedisStore`: a
generation started on one instance can be read and resumed through any
other.

```ts
import { createClient } from 'redis'
import { RedisStore, nodeRedis } from 'rewind-stream/server'

const redis = await createClient({ url: process.env.REDIS_URL }).connect()
const store = new RedisStore(nodeRedis(redis))
```

`ioredis(client)` adapts ioredis; any other client works by implementing
`{ command(args: string[]): Promise<unknown> }`. The store uses only basic
commands (no Lua, no streams, no blocking reads), keeps finished streams for
5 minutes and lets readers poll every 100 ms (`retentionMs`, `pollMs`).
Cancelling from any instance stops the producer wherever it runs.

**Authorization is up to you:** check the request before calling the handler,
with the same rules as the request that started the stream. Stream IDs from
`start` have 128 random bits.

## Python server

[`rewind-stream` for Python](python/) is an ASGI implementation (FastAPI,
Starlette, Django, Litestar) with memory and Redis stores. It passes the same
conformance suite, and its Redis layout matches the TypeScript one, so Python
and Node instances can share streams.

## Client

```ts
import { startStream, readStream, fetchSnapshot } from 'rewind-stream/client'

const { url, events } = await startStream('/chat', { method: 'POST', body: JSON.stringify({ prompt }) })
for await (const e of events) {
  if (e.event === 'delta') output.textContent += JSON.parse(e.data).text
  sessionStorage.setItem('stream', JSON.stringify({ url, cursor: e.id }))
}
```

The client reconnects by itself with backoff, ignores duplicates, detects
gaps and stops at the terminal event. To resume after a reload:

```ts
const { url, cursor } = JSON.parse(sessionStorage.getItem('stream'))
const snap = await fetchSnapshot(url, cursor)         // render what was missed, at once
snap.events.forEach(render)
for await (const e of readStream(url, { cursor: snap.last_id })) render(e)
```

Custom headers (for example `Authorization`) go in the `headers` option, as an
object or a function called before each attempt.

### Or no library at all

```js
const es = new EventSource(streamUrl)
es.addEventListener('delta', (e) => render(JSON.parse(e.data)))
es.addEventListener('stream.end', () => es.close())
```

`EventSource` already reconnects with `Last-Event-ID`; the protocol makes the
server answer it correctly, and the final `204` stops it.

## Conformance

```sh
npx rewind-conform https://api.example.com/streams/<id-of-a-finished-stream>
npx rewind-conform -H "Authorization: Bearer …" https://…
```

Reads a finished stream (never changes it) and checks the server's answers:
resuming from any cursor, `204` at the end, snapshots, rejected cursors,
headers.

## Demo

```sh
npm install
npm run demo        # http://localhost:3000, fake model
ANTHROPIC_API_KEY=… npm run demo   # real answers from Claude
```

Ask something, then press **Drop the connection** or **Reload the page**
while it streams, or open a second tab: the answer continues from where it
was, without gaps or repeats. `npm run e2e` checks this in a real Chromium,
along with the native `EventSource`.

## Install

```sh
npm install rewind-stream
```

Node 22.18 or later. Releases are published from GitHub Actions with npm
trusted publishing and provenance. Browsers: any with `fetch` streams and `TextDecoderStream`.

## Related work

Resumable streaming is not new, and the spec borrows from what exists:
SSE's own `Last-Event-ID`, Vercel's
[`resumable-stream`](https://www.npmjs.com/package/resumable-stream) for the
AI SDK (Redis pub/sub), the chunk index of Vercel's Workflow DevKit, and
commercial transports such as Ably. Rewind's goal is narrower: a
vendor-neutral wire format any client and server can implement, testable with
a conformance suite.

## Roadmap

- Optional compaction: merge many small deltas when replaying a long stream.

Feedback on the spec is the most useful contribution right now: open an issue.

## License

MIT
