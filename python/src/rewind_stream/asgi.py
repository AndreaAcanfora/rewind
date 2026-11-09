"""ASGI application serving stream URLs (GET as SSE or snapshot, DELETE to cancel).

Framework-agnostic: mount it in FastAPI or Starlette (`app.mount("/streams",
StreamApp(store))`), Django, Litestar, or run it directly with any ASGI server.
"""

from __future__ import annotations

import asyncio
import json
import secrets
from collections.abc import AsyncIterator, Callable
from typing import Any
from urllib.parse import parse_qs, unquote

from .produce import Source, produce
from .protocol import StreamStatus, encode_event, parse_cursor
from .store import Store

Scope = dict[str, Any]


def new_stream_id() -> str:
    """A random stream ID with 128 bits of entropy, safe in URLs."""
    return secrets.token_urlsafe(16)


def sse_headers(status: StreamStatus) -> list[tuple[bytes, bytes]]:
    return [
        (b"content-type", b"text/event-stream; charset=utf-8"),
        (b"cache-control", b"no-store"),
        (b"x-accel-buffering", b"no"),
        (b"stream-status", status.encode()),
    ]


async def sse_body(store: Store, stream_id: str, after: int, keep_alive: float = 15.0, retry_ms: int = 1000) -> AsyncIterator[bytes]:
    """The SSE body of a stream from cursor `after` (spec section 4.1),
    with keep-alive comments while idle. Ends after the terminal event."""
    yield f"retry: {retry_ms}\n\n".encode()
    queue: asyncio.Queue = asyncio.Queue()
    done = object()

    async def pump() -> None:
        try:
            async for e in store.follow(stream_id, after):
                await queue.put(e)
        finally:
            await queue.put(done)

    task = asyncio.ensure_future(pump())
    try:
        while True:
            try:
                item = await asyncio.wait_for(queue.get(), keep_alive)
            except asyncio.TimeoutError:
                yield b": keep-alive\n\n"
                continue
            if item is done:
                return
            yield encode_event(item).encode()
    finally:
        task.cancel()


def _text(status: int, body: str, extra: list[tuple[bytes, bytes]] | None = None) -> tuple[int, list[tuple[bytes, bytes]], bytes]:
    return status, [(b"content-type", b"text/plain; charset=utf-8"), *(extra or [])], (body + "\n").encode()


class StreamApp:
    """Serves `<mount point>/<stream id>`. Authorization is up to you: wrap it,
    or check the request in middleware, with the same rules as the request
    that started the stream."""

    def __init__(
        self,
        store: Store,
        stream_id: Callable[[Scope], str | None] | None = None,
        keep_alive: float = 15.0,
        retry_ms: int = 1000,
        allow_cancel: bool = True,
    ) -> None:
        self.store = store
        self.keep_alive = keep_alive
        self.retry_ms = retry_ms
        self.allow_cancel = allow_cancel
        self._stream_id = stream_id or self._last_segment

    @staticmethod
    def _last_segment(scope: Scope) -> str | None:
        path = scope.get("path", "").rstrip("/")
        segment = unquote(path.rsplit("/", 1)[-1]) if path else ""
        return segment or None

    async def __call__(self, scope: Scope, receive: Callable, send: Callable) -> None:
        if scope["type"] != "http":
            return
        method = scope["method"]
        stream_id = self._stream_id(scope)
        if not stream_id:
            return await self._send(send, *_text(404, "stream not found"))
        if method in ("GET", "HEAD"):
            return await self._read(scope, receive, send, stream_id, head=method == "HEAD")
        if method == "DELETE":
            if not self.allow_cancel:
                return await self._send(send, *_text(405, "cancellation not supported", [(b"allow", b"GET, HEAD")]))
            result = await self.store.cancel(stream_id)
            if result == "missing":
                return await self._send(send, *_text(404, "stream not found"))
            if result == "finished":
                return await self._send(send, 204, [], b"")
            return await self._send(send, *_text(202, "cancelling"))
        await self._send(send, *_text(405, "method not allowed", [(b"allow", b"GET, HEAD, DELETE")]))

    async def _read(self, scope: Scope, receive: Callable, send: Callable, stream_id: str, head: bool) -> None:
        headers = {k.decode().lower(): v.decode() for k, v in scope.get("headers", [])}
        query = parse_qs(scope.get("query_string", b"").decode())
        raw = headers.get("last-event-id")
        if raw is None:
            raw = query.get("after", ["0"])[0]
        after = parse_cursor(raw.strip())
        if after is None:
            return await self._send(send, *_text(400, "invalid cursor"))
        snap = await self.store.snapshot(stream_id, after)
        if snap is None:
            return await self._send(send, *_text(404, "stream not found"))
        accept = headers.get("accept", "")
        if "application/json" in accept and "text/event-stream" not in accept:
            body = b"" if head else json.dumps(snap.to_dict()).encode()
            return await self._send(
                send,
                200,
                [(b"content-type", b"application/json"), (b"cache-control", b"no-store"), (b"stream-status", snap.status.encode())],
                body,
            )
        if snap.status != "live" and after >= snap.last_id:
            return await self._send(send, 204, [(b"stream-status", snap.status.encode())], b"")
        await send({"type": "http.response.start", "status": 200, "headers": sse_headers(snap.status)})
        if head:
            return await send({"type": "http.response.body", "body": b""})
        await stream_to(send, receive, sse_body(self.store, stream_id, after, self.keep_alive, self.retry_ms))

    @staticmethod
    async def _send(send: Callable, status: int, headers: list[tuple[bytes, bytes]], body: bytes) -> None:
        await send({"type": "http.response.start", "status": status, "headers": headers})
        await send({"type": "http.response.body", "body": body})


async def stream_to(send: Callable, receive: Callable, body: AsyncIterator[bytes]) -> None:
    """Writes an async body to an ASGI response, stopping if the client goes away."""
    disconnected = asyncio.Event()

    async def watch() -> None:
        while True:
            message = await receive()
            if message["type"] == "http.disconnect":
                disconnected.set()
                return

    watcher = asyncio.ensure_future(watch())
    try:
        async for chunk in body:
            if disconnected.is_set():
                break
            await send({"type": "http.response.body", "body": chunk, "more_body": True})
        if not disconnected.is_set():
            await send({"type": "http.response.body", "body": b""})
    finally:
        watcher.cancel()
        await body.aclose()


async def start_stream(store: Store, source: Source, **produce_kwargs: Any) -> str:
    """Creates a stream, starts the generation in the background and returns the stream ID."""
    stream_id = new_stream_id()
    await store.create(stream_id)
    task = asyncio.ensure_future(produce(store, stream_id, source, **produce_kwargs))
    _background.add(task)
    task.add_done_callback(_background.discard)
    return stream_id


_background: set[asyncio.Task] = set()
