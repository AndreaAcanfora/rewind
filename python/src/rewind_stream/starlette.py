"""Starlette and FastAPI helpers (needs `pip install rewind-stream[starlette]`)."""

from __future__ import annotations

from collections.abc import Callable
from typing import Any, Literal

from starlette.responses import JSONResponse, StreamingResponse

from .asgi import sse_body, sse_headers, start_stream
from .produce import Source
from .store import Store


async def start(
    source: Source,
    *,
    store: Store,
    url: Callable[[str], str],
    mode: Literal["inline", "location"] = "inline",
    keep_alive: float = 15.0,
    retry_ms: int = 1000,
    **produce_kwargs: Any,
):
    """Starts a generation in the background and returns the response for the
    request that started it (spec section 3.1): the stream itself with a
    Stream-Location header, or 201 Created with Location.

        @app.post("/chat")
        async def chat(body: dict):
            async def tokens(cancelled):
                async for text in model_stream(body["prompt"]):
                    yield {"event": "delta", "data": {"text": text}}
            return await start(tokens, store=store, url=lambda id: f"/streams/{id}")
    """
    stream_id = await start_stream(store, source, **produce_kwargs)
    location = url(stream_id)
    if mode == "location":
        return JSONResponse({"stream": location}, status_code=201, headers={"Location": location, "Cache-Control": "no-store"})
    headers = {k.decode(): v.decode() for k, v in sse_headers("live")}
    headers["Stream-Location"] = location
    return StreamingResponse(sse_body(store, stream_id, 0, keep_alive, retry_ms), headers=headers, media_type=headers.pop("content-type"))
