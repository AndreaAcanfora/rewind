// A minimal RESP2 client for the tests: enough to drive RedisStore against a
// real server (REDIS_URL) without adding a dependency to the package.
import { connect, type Socket } from 'node:net'
import type { RedisCommander } from '../src/server/redis.ts'

type Reply = string | number | null | Reply[] | Error

export class RespClient implements RedisCommander {
  #socket: Socket
  #buffer = Buffer.alloc(0)
  #waiting: { resolve: (r: Reply) => void; reject: (e: Error) => void }[] = []

  private constructor(socket: Socket) {
    this.#socket = socket
    socket.on('data', (chunk) => {
      this.#buffer = Buffer.concat([this.#buffer, chunk])
      for (;;) {
        const parsed = parse(this.#buffer, 0)
        if (!parsed) break
        this.#buffer = this.#buffer.subarray(parsed.end)
        const w = this.#waiting.shift()
        if (parsed.value instanceof Error) w?.reject(parsed.value)
        else w?.resolve(parsed.value)
      }
    })
    socket.on('error', (err) => {
      for (const w of this.#waiting.splice(0)) w.reject(err)
    })
  }

  static open(url: string): Promise<RespClient> {
    const u = new URL(url)
    return new Promise((resolve, reject) => {
      const s = connect(Number(u.port || 6379), u.hostname, () => resolve(new RespClient(s)))
      s.once('error', reject)
    })
  }

  command(args: string[]): Promise<unknown> {
    let out = `*${args.length}\r\n`
    for (const a of args) out += `$${Buffer.byteLength(a)}\r\n${a}\r\n`
    return new Promise((resolve, reject) => {
      this.#waiting.push({ resolve, reject })
      this.#socket.write(out)
    })
  }

  close(): void {
    this.#socket.end()
  }
}

function parse(buf: Buffer, at: number): { value: Reply; end: number } | undefined {
  const nl = buf.indexOf('\r\n', at)
  if (nl < 0) return undefined
  const line = buf.toString('utf8', at + 1, nl)
  const next = nl + 2
  switch (String.fromCharCode(buf[at]!)) {
    case '+':
      return { value: line, end: next }
    case '-':
      return { value: new Error(line), end: next }
    case ':':
      return { value: Number(line), end: next }
    case '$': {
      const n = Number(line)
      if (n < 0) return { value: null, end: next }
      if (buf.length < next + n + 2) return undefined
      return { value: buf.toString('utf8', next, next + n), end: next + n + 2 }
    }
    case '*': {
      const n = Number(line)
      if (n < 0) return { value: null, end: next }
      const items: Reply[] = []
      let pos = next
      for (let i = 0; i < n; i++) {
        const item = parse(buf, pos)
        if (!item) return undefined
        items.push(item.value)
        pos = item.end
      }
      return { value: items, end: pos }
    }
    default:
      throw new Error(`unexpected RESP reply: ${line}`)
  }
}
