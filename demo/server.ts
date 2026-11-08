// Demo: a chat whose answers survive dropped connections and page reloads.
// Run with `npm run demo`, then open http://localhost:3000.
//
// Without ANTHROPIC_API_KEY the answers come from a fake model. With it, they
// come from Claude through the Messages API.
import { readFile } from 'node:fs/promises'
import { createServer } from 'node:http'
import { extname, join } from 'node:path'
import { createHandler, MemoryStore, start, type Chunk } from '../dist/server/index.js'
import { toNodeListener } from '../dist/server/node.js'

const root = new URL('..', import.meta.url).pathname
const store = new MemoryStore()
const streams = createHandler({ store })
const port = Number(process.env.PORT ?? 3000)

const types: Record<string, string> = { '.html': 'text/html; charset=utf-8', '.js': 'text/javascript; charset=utf-8' }

async function file(path: string): Promise<Response> {
  try {
    return new Response(await readFile(join(root, path)), { headers: { 'Content-Type': types[extname(path)] ?? 'application/octet-stream' } })
  } catch {
    return new Response('not found\n', { status: 404 })
  }
}

const handler = async (req: Request): Promise<Response> => {
  const { pathname } = new URL(req.url)
  if (pathname === '/') return file('demo/index.html')
  if (pathname.startsWith('/lib/') && !pathname.includes('..')) return file('dist/' + pathname.slice(5))
  if (pathname.startsWith('/streams/')) return streams(req)
  if (pathname === '/chat' && req.method === 'POST') {
    const { prompt } = (await req.json()) as { prompt: string }
    const source = process.env.ANTHROPIC_API_KEY ? (signal: AbortSignal) => claude(prompt, signal) : (signal: AbortSignal) => fake(prompt, signal)
    return start(source, { store, url: (id) => `/streams/${id}`, signal: req.signal })
  }
  return new Response('not found\n', { status: 404 })
}

const sleep = (ms: number, signal: AbortSignal) =>
  new Promise<void>((resolve, reject) => {
    const t = setTimeout(resolve, ms)
    signal.addEventListener('abort', () => (clearTimeout(t), reject(signal.reason)), { once: true })
  })

async function* fake(prompt: string, signal: AbortSignal): AsyncGenerator<Chunk> {
  const answer =
    `You asked: "${prompt}". This answer is generated slowly, one word at a time, by a fake model. ` +
    'Try the buttons while it streams: drop the connection, or reload the page. The generation keeps going on the server, ' +
    'and the client picks up from the last word it received, without losing or repeating anything. ' +
    'Open this page in a second tab and you will see the same answer, live, from the same stream. ' +
    'That is the whole idea of the Resumable Stream Protocol: number the events, keep them, and let clients resume.'
  for (const word of answer.split(/(?<= )/)) {
    await sleep(60, signal)
    yield { event: 'delta', data: { text: word } }
  }
}

async function* claude(prompt: string, signal: AbortSignal): AsyncGenerator<Chunk> {
  const res = await fetch('https://api.anthropic.com/v1/messages', {
    method: 'POST',
    signal,
    headers: {
      'content-type': 'application/json',
      'x-api-key': process.env.ANTHROPIC_API_KEY!,
      'anthropic-version': '2023-06-01',
    },
    body: JSON.stringify({
      model: process.env.ANTHROPIC_MODEL ?? 'claude-sonnet-5-5',
      max_tokens: 1024,
      stream: true,
      messages: [{ role: 'user', content: prompt }],
    }),
  })
  if (!res.ok || !res.body) throw new Error(`Anthropic API responded ${res.status}`)
  const { SSEParser } = await import('../dist/index.js')
  const parser = new SSEParser()
  const reader = res.body.pipeThrough(new TextDecoderStream()).getReader()
  for (let r = await reader.read(); !r.done; r = await reader.read()) {
    for (const e of parser.feed(r.value)) {
      if (e.event !== 'content_block_delta') continue
      const delta = JSON.parse(e.data).delta
      if (delta?.type === 'text_delta') yield { event: 'delta', data: { text: delta.text } }
    }
  }
}

createServer(toNodeListener(handler)).listen(port, () => {
  console.log(`demo on http://localhost:${port} (${process.env.ANTHROPIC_API_KEY ? 'Claude' : 'fake model'})`)
})
