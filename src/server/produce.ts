import { END, ERROR, isReservedType, isValidType, type StreamStatus } from '../protocol.ts'
import type { Store } from './store.ts'

/** What a source yields: a string (type `message`) or an event with a type. */
export type Chunk = string | { event?: string; data: unknown }

export type Source = AsyncIterable<Chunk> | ((signal: AbortSignal) => AsyncIterable<Chunk>)

export interface ProduceOptions {
  /**
   * Turns an exception from the source into the data of `stream.error`.
   * The default hides the message, which may contain internal details.
   */
  formatError?: (err: unknown) => { code: string; message: string }
}

const defaultFormatError = () => ({ code: 'generation_failed', message: 'The generation failed.' })

/**
 * Copies a source into a stream, detached from any client connection, and
 * ends the stream with the right terminal event. A function source receives
 * an AbortSignal that fires when a client cancels the stream; pass it to the
 * model API call so that the generation really stops.
 *
 * The stream must already exist. The returned promise never rejects.
 */
export async function produce(store: Store, streamId: string, source: Source, opts: ProduceOptions = {}): Promise<StreamStatus> {
  const controller = new AbortController()
  const unsubscribe = store.onCancel(streamId, () => controller.abort())
  const finish = async (event: string, data: object) => {
    try {
      await store.append(streamId, event, JSON.stringify(data))
    } catch {
      // Already finished (for example by the store's lifetime limit).
    }
  }
  try {
    const iterable = typeof source === 'function' ? source(controller.signal) : source
    for await (const chunk of iterable) {
      if (controller.signal.aborted) break
      const { event, data } = normalize(chunk)
      await store.append(streamId, event, data)
    }
    if (controller.signal.aborted) {
      await finish(END, { reason: 'cancelled' })
      return 'cancelled'
    }
    await finish(END, { reason: 'complete' })
    return 'complete'
  } catch (err) {
    if (controller.signal.aborted) {
      await finish(END, { reason: 'cancelled' })
      return 'cancelled'
    }
    await finish(ERROR, (opts.formatError ?? defaultFormatError)(err))
    return 'error'
  } finally {
    unsubscribe()
  }
}

function normalize(chunk: Chunk): { event: string; data: string } {
  if (typeof chunk === 'string') return { event: 'message', data: chunk }
  const event = chunk.event ?? 'message'
  if (!isValidType(event) || isReservedType(event)) throw new TypeError(`invalid event type ${JSON.stringify(event)}`)
  const data = typeof chunk.data === 'string' ? chunk.data : JSON.stringify(chunk.data)
  return { event, data }
}
