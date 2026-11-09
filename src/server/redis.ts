import { END, isTerminal, statusOf, type Snapshot, type StreamEvent } from '../protocol.ts'
import { StreamClosedError, type Store } from './store.ts'

/**
 * The one method RedisStore needs from a Redis client: send a command and
 * return its reply. Use `nodeRedis(client)` or `ioredis(client)`, or wrap any
 * other client (Upstash, Valkey Glide…).
 */
export interface RedisCommander {
  command(args: string[]): Promise<unknown>
}

/** Adapts a node-redis (v4 or later) client. */
export function nodeRedis(client: { sendCommand(args: string[]): Promise<unknown> }): RedisCommander {
  return { command: (args) => client.sendCommand(args) }
}

/** Adapts an ioredis client. */
export function ioredis(client: { call(command: string, ...args: string[]): Promise<unknown> }): RedisCommander {
  return { command: ([cmd, ...args]) => client.call(cmd!, ...args) }
}

export interface RedisStoreOptions {
  /** Prefix of every key. Default "rewind:". */
  prefix?: string
  /** How long a finished stream is kept. Default 5 minutes (spec section 3.2). */
  retentionMs?: number
  /** Live streams expire after this long. Default 1 hour. */
  maxLifetimeMs?: number
  /** How often readers poll for new events. Default 100 ms. */
  pollMs?: number
  /** How long cancel() waits for the producer before ending the stream itself. Default 3 s. */
  cancelGraceMs?: number
}

const LIVE = 'live'
const FINISHED = 'finished'

/**
 * A Store in Redis, so that any server instance can serve any stream: a
 * generation started on one instance can be resumed through another.
 *
 * It uses only basic commands (SET NX PX, GET, RPUSH, LRANGE, PEXPIRE, DEL):
 * no Lua, no streams and no blocking reads, so it suits Redis-compatible
 * servers and request/response clients alike. Readers poll for new events.
 *
 * Layout per stream: `<prefix><id>:state` ("live" or "finished"),
 * `<prefix><id>:events` (a list; event N is at index N-1) and
 * `<prefix><id>:cancel` (set when cancellation is requested). Event IDs come
 * from RPUSH, so they are contiguous even with several writers. If a write
 * races with the terminal event, it lands after it and stays invisible:
 * readers stop at the first terminal event.
 */
export class RedisStore implements Store {
  #r: RedisCommander
  #prefix: string
  #retentionMs: number
  #lifetimeMs: number
  #pollMs: number
  #graceMs: number

  constructor(redis: RedisCommander, opts: RedisStoreOptions = {}) {
    this.#r = redis
    this.#prefix = opts.prefix ?? 'rewind:'
    this.#retentionMs = opts.retentionMs ?? 5 * 60_000
    this.#lifetimeMs = opts.maxLifetimeMs ?? 60 * 60_000
    this.#pollMs = opts.pollMs ?? 100
    this.#graceMs = opts.cancelGraceMs ?? 3_000
  }

  #key(id: string, part: 'state' | 'events' | 'cancel'): string {
    return `${this.#prefix}${id}:${part}`
  }

  #cmd(...args: (string | number)[]): Promise<unknown> {
    return this.#r.command(args.map(String))
  }

  async create(streamId: string): Promise<void> {
    const ttl = this.#lifetimeMs + this.#retentionMs
    const ok = await this.#cmd('SET', this.#key(streamId, 'state'), LIVE, 'NX', 'PX', ttl)
    if (ok === null || ok === undefined) throw new Error(`stream ${streamId} already exists`)
  }

  async append(streamId: string, event: string, data: string): Promise<StreamEvent> {
    const state = await this.#cmd('GET', this.#key(streamId, 'state'))
    if (state !== LIVE) throw new StreamClosedError(streamId)
    const events = this.#key(streamId, 'events')
    const id = Number(await this.#cmd('RPUSH', events, JSON.stringify([event, data])))
    const e: StreamEvent = { id, event, data }
    if (id === 1) await this.#cmd('PEXPIRE', events, this.#lifetimeMs + this.#retentionMs)
    if (isTerminal(e)) await this.#finish(streamId)
    return e
  }

  async #finish(streamId: string): Promise<void> {
    const state = this.#key(streamId, 'state')
    await this.#cmd('SET', state, FINISHED, 'PX', this.#retentionMs)
    await this.#cmd('PEXPIRE', this.#key(streamId, 'events'), this.#retentionMs)
    await this.#cmd('DEL', this.#key(streamId, 'cancel'))
  }

  /** Events from index `from` (0-based), cut after the first terminal event. */
  async #read(streamId: string, from: number): Promise<{ events: StreamEvent[]; ended: boolean }> {
    const raw = (await this.#cmd('LRANGE', this.#key(streamId, 'events'), from, -1)) as string[] | null
    const events: StreamEvent[] = []
    for (const [i, item] of (raw ?? []).entries()) {
      const [event, data] = JSON.parse(item) as [string, string]
      const e = { id: from + i + 1, event, data }
      events.push(e)
      if (isTerminal(e)) return { events, ended: true }
    }
    return { events, ended: false }
  }

  async snapshot(streamId: string, after: number): Promise<Snapshot | undefined> {
    const state = await this.#cmd('GET', this.#key(streamId, 'state'))
    if (state !== LIVE && state !== FINISHED) return undefined
    const { events, ended } = await this.#read(streamId, 0)
    const last = events.at(-1)
    return {
      status: ended && last ? statusOf(last) : 'live',
      last_id: events.length,
      events: events.slice(after),
    }
  }

  async *follow(streamId: string, after: number, signal?: AbortSignal): AsyncIterable<StreamEvent> {
    // A cursor at or past the end of a finished stream has nothing to read.
    const snap = await this.snapshot(streamId, after)
    if (!snap) return
    if (snap.status !== 'live' && after >= snap.last_id) return
    let cursor = after
    for (;;) {
      if (signal?.aborted) return
      const { events, ended } = await this.#read(streamId, cursor)
      for (const e of events) {
        cursor = e.id
        yield e
        if (signal?.aborted) return
      }
      if (ended) return
      if (events.length === 0) {
        const state = await this.#cmd('GET', this.#key(streamId, 'state'))
        if (state !== LIVE && state !== FINISHED) return // expired
        await sleep(this.#pollMs, signal)
      }
    }
  }

  async cancel(streamId: string): Promise<'requested' | 'finished' | 'missing'> {
    const state = await this.#cmd('GET', this.#key(streamId, 'state'))
    if (state === FINISHED) return 'finished'
    if (state !== LIVE) return 'missing'
    await this.#cmd('SET', this.#key(streamId, 'cancel'), '1', 'PX', this.#lifetimeMs)
    // The producer may run on another instance: give it time to stop and end
    // the stream itself, then end it here if nobody did (a crashed producer).
    void this.#endIfAbandoned(streamId)
    return 'requested'
  }

  async #endIfAbandoned(streamId: string): Promise<void> {
    await sleep(this.#graceMs)
    try {
      await this.append(streamId, END, JSON.stringify({ reason: 'cancelled' }))
    } catch {
      // Already finished by the producer.
    }
  }

  onCancel(streamId: string, listener: () => void): () => void {
    let stopped = false
    const timer = setInterval(async () => {
      try {
        if (!stopped && (await this.#cmd('GET', this.#key(streamId, 'cancel'))) === '1') {
          stopped = true
          clearInterval(timer)
          listener()
        }
      } catch {
        // Transient Redis error: try again on the next tick.
      }
    }, Math.max(this.#pollMs, 50))
    timer.unref?.()
    return () => {
      stopped = true
      clearInterval(timer)
    }
  }
}

function sleep(ms: number, signal?: AbortSignal): Promise<void> {
  return new Promise((resolve) => {
    if (signal?.aborted) return resolve()
    const t = setTimeout(done, ms)
    t.unref?.()
    function done() {
      clearTimeout(t)
      signal?.removeEventListener('abort', done)
      resolve()
    }
    signal?.addEventListener('abort', done, { once: true })
  })
}
