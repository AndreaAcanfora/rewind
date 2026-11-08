# Resumable Stream Protocol — version 0.1

Status: draft. Feedback welcome through GitHub issues.

The key words MUST, MUST NOT, SHOULD, SHOULD NOT and MAY are to be
interpreted as described in [RFC 2119](https://www.rfc-editor.org/rfc/rfc2119).

## 1. Purpose

LLM responses are streamed token by token for tens of seconds or minutes. When
the connection drops (a mobile network change, a page reload, a laptop going to
sleep, a proxy idle timeout, a server deploy), most applications lose the
response: the user sees a broken message, and the application either gives up
or pays for the whole generation again.

The fix is well known: decouple the generation from the connection, number
the events, and let the client reconnect from the last event it saw. Several
frameworks and vendors implement it, each in its own way, so clients and
servers from different stacks cannot resume each other's streams.

This specification defines that mechanism on the wire, on top of
[Server-Sent Events](https://html.spec.whatwg.org/multipage/server-sent-events.html)
(SSE), so that:

- any client can resume any conforming stream, including the browser's native
  `EventSource`, which already reconnects with `Last-Event-ID`;
- several readers (browser tabs, devices) can follow the same stream;
- a reloaded page can render what was already generated, then follow the rest.

The protocol does not define how a generation is started, what the events
contain, or how they are stored. Those are application concerns.

## 2. Terminology

- **Stream**: the ordered, append-only sequence of events of one generation.
- **Stream URL**: the URL that identifies a stream.
- **Event**: one item of a stream: an ID, a type and a data string.
- **Event ID**: the position of the event in the stream: `1` for the first
  event, then `2`, `3`, and so on, without gaps.
- **Cursor**: the ID of the last event a client has received; `0` before the
  first event.
- **Terminal event**: the last event of a stream (section 5).
- **Producer**: the server-side process that appends events to a stream.

## 3. Streams

A stream is created by the application, for example when it receives a chat
message. From then on the producer appends events to it, independently of any
client connection: a client disconnecting MUST NOT stop the generation.

Events are immutable. Once an event has an ID, every reader of the stream
MUST receive the same type and data for that ID.

A stream is **live** until its terminal event is appended, and **finished**
afterwards.

### 3.1 Starting a stream

An application endpoint that starts a stream (for example `POST /chat`) MUST
tell the client the stream URL, in one of two ways:

1. Respond `201 Created` with a `Location` header holding the stream URL. The
   client then reads the stream from cursor `0`.
2. Respond `200 OK` with the stream itself as an SSE response, as in section
   4.1, starting from event `1`, and with a `Stream-Location` header holding
   the stream URL. This saves a round trip.

Relative URLs in `Location` and `Stream-Location` are resolved against the
request URL.

### 3.2 Retention

A server MUST keep every event of a live stream. After the terminal event, it
SHOULD keep the stream for at least 5 minutes, so that clients that were
disconnected at the end can still read it. When it no longer has the stream,
it responds `404 Not Found`.

## 4. Reading a stream

A client reads a stream with `GET` on the stream URL. The response format
depends on the `Accept` header:

- `application/json` without `text/event-stream`: a snapshot (section 4.2);
- anything else, including no `Accept` header: SSE (section 4.1).

The client gives its cursor with the `Last-Event-ID` request header or the
`after` query parameter. Both hold a non-negative decimal integer. When both
are present the header wins, because `EventSource` sets it on reconnection
while keeping the original URL. A missing cursor means `0`. A cursor that is
not a non-negative integer MUST be rejected with `400 Bad Request`.

Any number of clients MAY read the same stream at the same time, each from its
own cursor.

### 4.1 SSE response

If the stream is finished and the cursor is greater than or equal to the ID of
the terminal event, the server MUST respond `204 No Content`. This makes
`EventSource` stop reconnecting.

Otherwise the server responds `200 OK` with `Content-Type: text/event-stream`
and `Cache-Control: no-store`, and then:

1. sends every event whose ID is greater than the cursor, in order;
2. if the stream is live, keeps the response open and sends new events as
   they are appended;
3. closes the response after sending the terminal event.

A cursor greater than the last event ID of a live stream is valid: the server
sends events once their ID exceeds the cursor.

Each event is encoded as an SSE event with:

- an `id` field holding the event ID;
- an `event` field holding the type, omitted when the type is `message`;
- one `data` field per line of the data.

```
id: 7
event: delta
data: {"text":" world"}

```

While the response is open and idle, the server MUST send an SSE comment
(a line starting with `:`) at least every 15 seconds, so that proxies do not
close it. The server SHOULD send a `retry` field at the start of the response.

The response SHOULD carry a `Stream-Status` header with the stream status at
the time of the response (`live`, `complete`, `cancelled` or `error`).

### 4.2 Snapshot response

A snapshot returns what the stream holds now, without waiting:

```json
{
  "status": "live",
  "last_id": 42,
  "events": [
    { "id": 41, "event": "delta", "data": "{\"text\":\"Hel\"}" },
    { "id": 42, "event": "delta", "data": "{\"text\":\"lo\"}" }
  ]
}
```

`events` holds the events with an ID greater than the cursor, in order.
`status` is `live`, or the reason of the terminal event when the stream is
finished (section 5). `last_id` is the ID of the last event in the stream.

A page that reloads in the middle of a generation can render a snapshot
immediately, then open an SSE response from `last_id`.

## 5. Event types

The type of an event is a string of 1 to 64 characters from
`A-Z a-z 0-9 . _ -`. Applications choose their own types (for example
`delta`, `tool_call`, `usage`). The default type is `message`.

Types that start with `stream.` are reserved. This version defines two
terminal types, which close the stream:

| Type           | Data                                                  |
|----------------|-------------------------------------------------------|
| `stream.end`   | JSON: `{"reason": "complete"}` or `{"reason": "cancelled"}` |
| `stream.error` | JSON: `{"code": "<string>", "message": "<string>"}`   |

A stream has exactly one terminal event, and it is the last event. It has an
ID like any other event, so it is replayed to clients that missed it.

The data of other events is application-defined text. JSON is RECOMMENDED.

## 6. Cancelling a stream (optional)

A server MAY let clients stop a generation with `DELETE` on the stream URL.
If the stream is live, the server stops the producer, appends
`stream.end` with `{"reason": "cancelled"}` and responds `202 Accepted`. If
the stream is already finished, it responds `204 No Content`. A server that
does not support cancellation responds `405 Method Not Allowed`.

Cancellation MUST require the same authorization as reading.

## 7. Client behaviour

A client:

- keeps its cursor: the ID of the last event received;
- MUST ignore events whose ID is less than or equal to its cursor (they are
  duplicates);
- MUST treat an event whose ID is greater than cursor + 1 as a gap: it
  discards the event, closes the response and reconnects from its cursor;
- stops after a terminal event, a `204` response, a `404` response, or any
  other `4xx` response except `408` and `429`;
- after a network error, a response that ends without a terminal event, a
  `5xx`, `408` or `429` response, reconnects with its cursor in
  `Last-Event-ID`, waiting first. It SHOULD use the `retry` value sent by the
  server, or 1 second if there is none, doubling the wait after each failed
  attempt up to 30 seconds, and resetting it after an attempt that received
  at least one event;
- SHOULD give up after a bounded number of consecutive failed attempts.

To survive page reloads, a browser client SHOULD store the stream URL and its
cursor (for example in `sessionStorage`) and resume from them.

### 7.1 Native EventSource

A browser can follow a stream with the built-in `EventSource`:

```js
const es = new EventSource(streamUrl)
es.addEventListener('delta', (e) => render(JSON.parse(e.data)))
es.addEventListener('stream.end', () => es.close())
es.addEventListener('stream.error', () => es.close())
```

`EventSource` reconnects by itself with `Last-Event-ID`, and the `204`
response of section 4.1 stops it for good. It does not detect gaps and cannot
send custom headers, so applications that need authorization headers or
gap detection should use a client library.

## 8. Security considerations

- Reading a stream MUST require the same authorization as the request that
  created it. If the stream URL is the only credential (a capability URL), it
  MUST hold at least 128 bits of randomness and MUST NOT be logged.
- Events can contain personal data. Responses MUST carry
  `Cache-Control: no-store`, and servers SHOULD delete finished streams
  once retention expires.
- A server SHOULD limit the number of concurrent readers per stream and per
  client.

## 9. Conformance

A **conforming server** implements sections 3 to 5, and section 6 if it
supports cancellation. A **conforming client** implements section 7.

The reference implementation ships a conformance runner
(`rewind-conform <stream-url>`) that checks a server's behaviour on the wire
against a stream that has finished.

## 10. Versioning

Minor versions (0.x) add optional features and stay compatible on the wire.
Breaking changes require a new major version.
