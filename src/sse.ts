import type { StreamEvent } from './protocol.ts'

/** Encodes one event in the SSE format of spec section 4.1. */
export function encodeEvent(e: StreamEvent): string {
  let out = `id: ${e.id}\n`
  if (e.event !== 'message') out += `event: ${e.event}\n`
  for (const line of e.data.split(/\r\n|\r|\n/)) out += `data: ${line}\n`
  return out + '\n'
}

/** A raw SSE event as parsed from the wire. */
export interface ParsedEvent {
  id?: string
  event: string
  data: string
}

/**
 * Incremental parser for the text/event-stream format, following the WHATWG
 * HTML specification. Feed it chunks of text; it returns complete events.
 */
export class SSEParser {
  #buffer = ''
  #data: string[] = []
  #event = ''
  #id: string | undefined
  #hasData = false
  /** Last `retry` value received, in milliseconds. */
  retry: number | undefined

  feed(chunk: string): ParsedEvent[] {
    this.#buffer += chunk
    const events: ParsedEvent[] = []
    for (;;) {
      const match = /\r\n|\r|\n/.exec(this.#buffer)
      if (!match) break
      // A lone \r at the end of the buffer may be the start of \r\n.
      if (match[0] === '\r' && match.index === this.#buffer.length - 1) break
      const line = this.#buffer.slice(0, match.index)
      this.#buffer = this.#buffer.slice(match.index + match[0].length)
      const event = this.#line(line)
      if (event) events.push(event)
    }
    return events
  }

  #line(line: string): ParsedEvent | undefined {
    if (line === '') {
      if (!this.#hasData) {
        this.#event = ''
        return undefined
      }
      const e: ParsedEvent = { event: this.#event || 'message', data: this.#data.join('\n') }
      if (this.#id !== undefined) e.id = this.#id
      this.#data = []
      this.#event = ''
      this.#hasData = false
      return e
    }
    if (line.startsWith(':')) return undefined
    const colon = line.indexOf(':')
    const field = colon < 0 ? line : line.slice(0, colon)
    let value = colon < 0 ? '' : line.slice(colon + 1)
    if (value.startsWith(' ')) value = value.slice(1)
    switch (field) {
      case 'data':
        this.#data.push(value)
        this.#hasData = true
        break
      case 'event':
        this.#event = value
        break
      case 'id':
        if (!value.includes('\0')) this.#id = value
        break
      case 'retry':
        if (/^[0-9]+$/.test(value)) this.retry = Number(value)
        break
    }
    return undefined
  }
}
