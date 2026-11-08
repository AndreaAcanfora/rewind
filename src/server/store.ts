import { END, isTerminal, statusOf, type Snapshot, type StreamEvent } from '../protocol.ts'

/**
 * Where streams live. The memory store suits a single process; a shared store
 * (Redis Streams, a database) lets any instance serve any stream.
 */
export interface Store {
  /** Creates an empty live stream. Fails if the ID is taken. */
  create(streamId: string): Promise<void>
  /** Appends an event and returns it with its ID. Fails if the stream is finished or missing. */
  append(streamId: string, event: string, data: string): Promise<StreamEvent>
  /** Returns the events after `after`, or undefined if the stream does not exist. */
  snapshot(streamId: string, after: number): Promise<Snapshot | undefined>
  /**
   * Yields the events after `after`, then new events as they are appended,
   * and returns after the terminal event or when `signal` aborts.
   */
  follow(streamId: string, after: number, signal?: AbortSignal): AsyncIterable<StreamEvent>
  /**
   * Asks the producer of a live stream to stop. Returns 'requested' if the
   * stream is live, 'finished' if it already ended, 'missing' otherwise.
   */
  cancel(streamId: string): Promise<'requested' | 'finished' | 'missing'>
  /** Calls `listener` when cancellation is requested. Returns an unsubscribe function. */
  onCancel(streamId: string, listener: () => void): () => void
}

export class StreamClosedError extends Error {
  constructor(streamId: string) {
    super(`stream ${streamId} is finished or missing`)
    this.name = 'StreamClosedError'
  }
}

interface Entry {
  events: StreamEvent[]
  finished: boolean
  waiters: Set<() => void>
  cancelListeners: Set<() => void>
  expiry?: ReturnType<typeof setTimeout>
}

export interface MemoryStoreOptions {
  /** How long a finished stream is kept. Default 5 minutes (spec section 3.2). */
  retentionMs?: number
  /** Live streams older than this are ended with an error. Default 1 hour. */
  maxLifetimeMs?: number
}

/** In-process store. Streams are lost when the process exits. */
export class MemoryStore implements Store {
  #streams = new Map<string, Entry>()
  #retentionMs: number
  #maxLifetimeMs: number

  constructor(opts: MemoryStoreOptions = {}) {
    this.#retentionMs = opts.retentionMs ?? 5 * 60_000
    this.#maxLifetimeMs = opts.maxLifetimeMs ?? 60 * 60_000
  }

  /** Number of streams currently held. */
  get size(): number {
    return this.#streams.size
  }

  async create(streamId: string): Promise<void> {
    if (this.#streams.has(streamId)) throw new Error(`stream ${streamId} already exists`)
    const entry: Entry = { events: [], finished: false, waiters: new Set(), cancelListeners: new Set() }
    entry.expiry = setTimeout(() => {
      this.#append(streamId, entry, 'stream.error', JSON.stringify({ code: 'timeout', message: 'stream exceeded its maximum lifetime' }))
    }, this.#maxLifetimeMs)
    entry.expiry.unref?.()
    this.#streams.set(streamId, entry)
  }

  async append(streamId: string, event: string, data: string): Promise<StreamEvent> {
    const entry = this.#streams.get(streamId)
    if (!entry || entry.finished) throw new StreamClosedError(streamId)
    return this.#append(streamId, entry, event, data)
  }

  #append(streamId: string, entry: Entry, event: string, data: string): StreamEvent {
    const e: StreamEvent = { id: entry.events.length + 1, event, data }
    entry.events.push(e)
    if (isTerminal(e)) {
      entry.finished = true
      entry.cancelListeners.clear()
      clearTimeout(entry.expiry)
      entry.expiry = setTimeout(() => this.#streams.delete(streamId), this.#retentionMs)
      entry.expiry.unref?.()
    }
    for (const wake of [...entry.waiters]) wake()
    return e
  }

  async snapshot(streamId: string, after: number): Promise<Snapshot | undefined> {
    const entry = this.#streams.get(streamId)
    if (!entry) return undefined
    const last = entry.events.at(-1)
    return {
      status: entry.finished && last ? statusOf(last) : 'live',
      last_id: entry.events.length,
      events: entry.events.slice(after),
    }
  }

  async *follow(streamId: string, after: number, signal?: AbortSignal): AsyncIterable<StreamEvent> {
    let cursor = after
    for (;;) {
      const entry = this.#streams.get(streamId)
      if (!entry || signal?.aborted) return
      while (cursor < entry.events.length) {
        const e = entry.events[cursor++]!
        yield e
        if (isTerminal(e)) return
        if (signal?.aborted) return
      }
      if (entry.finished) return
      await new Promise<void>((resolve) => {
        const wake = () => {
          entry.waiters.delete(wake)
          signal?.removeEventListener('abort', wake)
          resolve()
        }
        entry.waiters.add(wake)
        signal?.addEventListener('abort', wake, { once: true })
      })
    }
  }

  async cancel(streamId: string): Promise<'requested' | 'finished' | 'missing'> {
    const entry = this.#streams.get(streamId)
    if (!entry) return 'missing'
    if (entry.finished) return 'finished'
    if (entry.cancelListeners.size === 0) {
      // No producer is attached: end the stream here.
      this.#append(streamId, entry, END, JSON.stringify({ reason: 'cancelled' }))
    } else {
      for (const listener of [...entry.cancelListeners]) listener()
    }
    return 'requested'
  }

  onCancel(streamId: string, listener: () => void): () => void {
    const entry = this.#streams.get(streamId)
    if (!entry || entry.finished) return () => {}
    entry.cancelListeners.add(listener)
    return () => entry.cancelListeners.delete(listener)
  }
}
