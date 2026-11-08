// End-to-end test in a real browser (Chromium via Playwright): the demo page
// survives a dropped connection and a reload, and the native EventSource
// follows a stream and stops on the final 204. Run with `npm run e2e`.
import assert from 'node:assert/strict'
import { spawn } from 'node:child_process'
import { chromium } from 'playwright'

const port = 3000 + Math.floor(Math.random() * 1000)
const base = `http://127.0.0.1:${port}`

const server = spawn(process.execPath, ['demo/server.ts'], { env: { ...process.env, PORT: String(port), ANTHROPIC_API_KEY: '' }, stdio: ['ignore', 'pipe', 'inherit'] })
await new Promise<void>((resolve) => server.stdout!.once('data', () => resolve()))
// CHROMIUM_PATH lets the test use a browser already on the machine.
const browser = await chromium.launch(process.env.CHROMIUM_PATH ? { executablePath: process.env.CHROMIUM_PATH } : {})

try {
  // The reference answer, read in one go.
  const res = await fetch(`${base}/chat`, { method: 'POST', body: JSON.stringify({ prompt: 'e2e' }) })
  const { SSEParser } = await import('../../dist/index.js')
  const parser = new SSEParser()
  const expected = parser.feed(await res.text()).filter((e) => e.event === 'delta').map((e) => JSON.parse(e.data).text).join('')
  assert.ok(expected.length > 200)

  const page = await browser.newPage()
  await page.goto(base)
  await page.fill('#prompt', 'e2e')
  await page.click('#ask button')
  await page.waitForTimeout(800)
  await page.click('#drop')
  await page.waitForTimeout(800)
  assert.match((await page.textContent('#log'))!, /connection dropped after event \d+; resuming/)
  await page.click('#reload')
  await page.waitForLoadState('load')
  await page.waitForTimeout(800)
  await page.reload()
  await page.waitForFunction(() => document.getElementById('status')!.textContent === 'complete', null, { timeout: 30_000 })
  const answer = await page.textContent('#answer')
  assert.equal(answer, expected, 'the answer after a drop and two reloads must match the reference exactly')
  const log = await page.textContent('#log')
  assert.match(log!, /reloaded: had \d+ events, caught up to \d+/)
  console.log('ok - demo survives a dropped connection and two reloads')

  // Native EventSource, no library.
  const native = await page.evaluate(async () => {
    const res = await fetch('/chat', { method: 'POST', body: JSON.stringify({ prompt: 'e2e' }) })
    const url = res.headers.get('stream-location')!
    await res.body!.cancel()
    return new Promise<{ text: string; ids: string[]; readyState: number }>((resolve) => {
      const es = new EventSource(url)
      let text = ''
      const ids: string[] = []
      es.addEventListener('delta', (e) => {
        ids.push((e as MessageEvent).lastEventId)
        text += JSON.parse((e as MessageEvent).data).text
      })
      // Deliberately not closing on stream.end: the server's 204 on the
      // automatic reconnection must stop EventSource by itself.
      es.onerror = () => {
        if (es.readyState === EventSource.CLOSED) resolve({ text, ids, readyState: es.readyState })
      }
    })
  })
  assert.equal(native.text, expected)
  assert.equal(new Set(native.ids).size, native.ids.length, 'no duplicate events')
  assert.equal(native.readyState, 2)
  console.log('ok - native EventSource reads the stream and stops on 204')
} finally {
  await browser.close()
  server.kill()
}
