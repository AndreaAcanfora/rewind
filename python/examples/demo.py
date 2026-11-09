"""A chat endpoint whose answers can be resumed, with Starlette.

    pip install "rewind-stream[starlette]" uvicorn
    uvicorn demo:app

POST /chat starts an answer and streams it; GET /streams/<id> resumes it
(with Last-Event-ID), from this or any other instance when using RedisStore.
The fake model below stands in for an LLM client.
"""

import asyncio

from starlette.applications import Starlette
from starlette.requests import Request
from starlette.routing import Mount, Route

from rewind_stream import MemoryStore, StreamApp
from rewind_stream.starlette import start

store = MemoryStore()  # or RedisStore(redis.asyncio.Redis.from_url(...)) for several instances


async def fake_model(prompt: str, cancelled: asyncio.Event):
    for word in f"You asked: {prompt}. This answer arrives one word at a time and survives reconnections.".split(" "):
        if cancelled.is_set():
            return
        await asyncio.sleep(0.05)
        yield {"event": "delta", "data": {"text": word + " "}}


async def chat(request: Request):
    body = await request.json() if await request.body() else {}
    prompt = body.get("prompt", "hello")
    return await start(lambda cancelled: fake_model(prompt, cancelled), store=store, url=lambda sid: f"/streams/{sid}")


app = Starlette(routes=[Route("/chat", chat, methods=["POST"]), Mount("/streams", StreamApp(store))])
