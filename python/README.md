# rewind-stream for Python

The [Resumable Stream Protocol](https://github.com/AndreaAcanfora/rewind/blob/master/SPEC.md)
for Python servers: LLM answers that survive dropped connections, page
reloads and multiple tabs. An ASGI app (FastAPI, Starlette, Django,
Litestar…), memory and Redis stores, no required dependencies.

Clients are the same as for the TypeScript server: the browser's native
`EventSource`, or the [`rewind-stream`](https://www.npmjs.com/package/rewind-stream)
npm client with automatic resume.

## FastAPI

```python
from anthropic import AsyncAnthropic
from fastapi import FastAPI, Request
from rewind_stream import MemoryStore, StreamApp
from rewind_stream.starlette import start

app = FastAPI()
client = AsyncAnthropic()
store = MemoryStore()                      # RedisStore(...) for several instances
app.mount("/streams", StreamApp(store))    # GET (SSE or snapshot), DELETE (cancel)

@app.post("/chat")
async def chat(request: Request):
    prompt = (await request.json())["prompt"]

    async def answer(cancelled):           # set when a client cancels
        async with client.messages.stream(model="claude-sonnet-5-5", max_tokens=1024,
                                          messages=[{"role": "user", "content": prompt}]) as s:
            async for text in s.text_stream:
                if cancelled.is_set():
                    return
                yield {"event": "delta", "data": {"text": text}}

    return await start(answer, store=store, url=lambda sid: f"/streams/{sid}")
```

`start` runs the generation in the background, detached from the request:
a client that disconnects can resume from `/streams/<id>` with
`Last-Event-ID`, or read a JSON snapshot with `Accept: application/json`.
Errors raised by the source end the stream with `stream.error` without
exposing their message (pass `format_error` to change that).

## Several instances: RedisStore

```python
import redis.asyncio as redis
from rewind_stream import RedisStore

store = RedisStore(redis.Redis.from_url("redis://localhost:6379", decode_responses=True))
```

Any client with redis-py's `execute_command` works. The store uses only
basic commands (no Lua, no streams, no blocking reads), and its layout is
the same as the TypeScript `RedisStore`: Python and Node servers can share
one Redis and resume each other's streams.

## Without a framework

`StreamApp(store)` is a plain ASGI app, and `start_stream(store, source)`
starts a generation and returns its ID, for any ASGI framework.

## Tests

```sh
pip install -e ".[test]"
python -m unittest discover -s tests -t .           # REDIS_URL=redis://… adds the RedisStore tests
```

The TypeScript conformance suite (`npx rewind-conform <url>`) passes against
this server; CI runs it on every change.
