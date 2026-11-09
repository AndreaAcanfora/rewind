"""Copying a generation into a stream, detached from any client connection."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterable, Callable
from typing import Any

from .protocol import END, ERROR, StreamStatus, is_reserved_type, is_valid_type
from .store import Store, StreamClosedError

#: A source yields strings (type "message") or {"event": ..., "data": ...}.
Chunk = str | dict
#: Either an async iterable, or a function given an asyncio.Event that is set
#: when a client cancels the stream (pass it on to stop the model call).
Source = AsyncIterable[Chunk] | Callable[[asyncio.Event], AsyncIterable[Chunk]]


def _default_format_error(_: BaseException) -> dict:
    return {"code": "generation_failed", "message": "The generation failed."}


def _normalize(chunk: Chunk) -> tuple[str, str]:
    if isinstance(chunk, str):
        return "message", chunk
    event = chunk.get("event", "message")
    if not isinstance(event, str) or not is_valid_type(event) or is_reserved_type(event):
        raise TypeError(f"invalid event type {event!r}")
    data: Any = chunk.get("data", "")
    return event, data if isinstance(data, str) else json.dumps(data, ensure_ascii=False, separators=(",", ":"))


async def produce(
    store: Store,
    stream_id: str,
    source: Source,
    format_error: Callable[[BaseException], dict] = _default_format_error,
) -> StreamStatus:
    """Copies source into the stream and ends it with the right terminal event.

    The stream must exist. A client cancelling the stream sets the event given
    to a function source and stops the iteration. Errors in the source end the
    stream with stream.error; by default their message is not exposed.
    Never raises.
    """
    cancelled = asyncio.Event()

    async def pump() -> None:
        iterable = source(cancelled) if callable(source) else source
        async for chunk in iterable:
            if cancelled.is_set():
                return
            event, data = _normalize(chunk)
            await store.append(stream_id, event, data)

    task = asyncio.ensure_future(pump())

    def on_cancel() -> None:
        cancelled.set()
        task.cancel()

    unsubscribe = store.on_cancel(stream_id, on_cancel)

    async def finish(event: str, data: dict) -> None:
        try:
            await store.append(stream_id, event, json.dumps(data, separators=(",", ":")))
        except StreamClosedError:  # already finished, e.g. by the store's lifetime limit
            return

    try:
        await task
        if cancelled.is_set():
            await finish(END, {"reason": "cancelled"})
            return "cancelled"
        await finish(END, {"reason": "complete"})
        return "complete"
    except asyncio.CancelledError:
        if not cancelled.is_set():
            raise
        await finish(END, {"reason": "cancelled"})
        return "cancelled"
    except Exception as err:  # noqa: BLE001 - the source may raise anything
        await finish(ERROR, format_error(err))
        return "error"
    finally:
        unsubscribe()
