#!/usr/bin/env node
import { passed, run } from './index.ts'

const args = process.argv.slice(2)
const headers: Record<string, string> = {}
let url: string | undefined
for (let i = 0; i < args.length; i++) {
  const a = args[i]!
  if (a === '-H' || a === '--header') {
    const h = args[++i] ?? ''
    const colon = h.indexOf(':')
    if (colon < 1) {
      console.error(`invalid header ${JSON.stringify(h)}`)
      process.exit(2)
    }
    headers[h.slice(0, colon).trim()] = h.slice(colon + 1).trim()
  } else if (!url) {
    url = a
  } else {
    url = undefined
    break
  }
}
if (!url) {
  console.error('usage: rewind-conform [-H "Name: value"]... <url-of-a-finished-stream>')
  process.exit(2)
}

const results = await run(url, { headers })
for (const r of results) {
  console.log(`${r.passed ? 'PASS' : 'FAIL'}  ${r.level.padEnd(6)} §${r.section.padEnd(4)} ${r.name}${r.detail ? ` (${r.detail})` : ''}`)
}
if (passed(results)) {
  console.log('\nconforming to the Resumable Stream Protocol 0.1')
} else {
  console.log('\nnot conforming: at least one MUST case failed')
  process.exitCode = 1
}
