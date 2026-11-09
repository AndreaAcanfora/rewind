"""Where streams live: the Store interface and the in-process MemoryStore."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Callable
from typing import Literal, Protocol

from .protocol import END, Snapshot, StreamEvent, is_terminal, status_of

CancelResult = Literal["requested", "finished", "missing"]


class StreamClosedError(Exception):
    """Raised when appending to a stream that is finished or missing."""


class Store(Protocol):
    async def create(self, stream_id: str) -> None: ...
    async def append(self, stream_id: str, event: str, data: str) -> StreamEvent: ...
    async def snapshot(self, stream_id: str, after: int) -> Snapshot | None: ...
    def follow(self, stream_id: str, after: int) -> AsyncIterator[StreamEvent]: ...
    async def cancel(self, stream_id: str) -> CancelResult: ...
    def on_cancel(self, stream_id: str, listener: Callable[[], None]) -> Callable[[], None]: ...


class _Entry:
    def __init__(self) -> None:
        self.events: list[StreamEvent] = []
        self.finished = False
        self.changed = asyncio.Condition()
        self.cancel_listeners: list[Callable[[], None]] = []
        self.expiry: asyncio.TimerHandle | None = None


class MemoryStore:
    """Streams in this process; lost when it exits. Suits a single server."""

    def __init__(self, retention: float = 300.0, max_lifetime: float = 3600.0) -> None:
        self._streams: dict[str, _Entry] = {}
        self._retention = retention
        self._max_lifetime = max_lifetime

    def __len__(self) -> int:
        return len(self._streams)

    async def create(self, stream_id: str) -> None:
        if stream_id in self._streams:
            raise ValueError(f"stream {stream_id} already exists")
        entry = _Entry()
        loop = asyncio.get_running_loop()
        entry.expiry = loop.call_later(self._max_lifetime, lambda: asyncio.ensure_future(self._timeout(stream_id, entry)))
        self._streams[stream_id] = entry

    async def _timeout(self, stream_id: str, entry: _Entry) -> None:
        if not entry.finished:
            await self._append(
                stream_id, entry, "stream.error", json.dumps({"code": "timeout", "message": "stream exceeded its maximum lifetime"})
            )

    async def append(self, stream_id: str, event: str, data: str) -> StreamEvent:
        entry = self._streams.get(stream_id)
        if entry is None or entry.finished:
            raise StreamClosedError(stream_id)
        return await self._append(stream_id, entry, event, data)

    async def _append(self, stream_id: str, entry: _Entry, event: str, data: str) -> StreamEvent:
        e = StreamEvent(len(entry.events) + 1, event, data)
        entry.events.append(e)
        if is_terminal(event):
            entry.finished = True
            entry.cancel_listeners.clear()
            if entry.expiry:
                entry.expiry.cancel()
            entry.expiry = asyncio.get_running_loop().call_later(self._retention, lambda: self._streams.pop(stream_id, None))
        async with entry.changed:
            entry.changed.notify_all()
        return e

    async def snapshot(self, stream_id: str, after: int) -> Snapshot | None:
        entry = self._streams.get(stream_id)
        if entry is None:
            return None
        last = entry.events[-1] if entry.events else None
        status = status_of(last) if entry.finished and last else "live"
        return Snapshot(status, len(entry.events), entry.events[after:])

    async def follow(self, stream_id: str, after: int) -> AsyncIterator[StreamEvent]:
        cursor = after
        while True:
            entry = self._streams.get(stream_id)
            if entry is None:
                return
            while cursor < len(entry.events):
                e = entry.events[cursor]
                cursor += 1
                yield e
                if is_terminal(e.event):
                    return
            if entry.finished:
                return
            async with entry.changed:
                await entry.changed.wait_for(lambda e=entry, c=cursor: c < len(e.events) or e.finished)

    async def cancel(self, stream_id: str) -> CancelResult:
        entry = self._streams.get(stream_id)
        if entry is None:
            return "missing"
        if entry.finished:
            return "finished"
        if not entry.cancel_listeners:
            await self._append(stream_id, entry, END, json.dumps({"reason": "cancelled"}, separators=(",", ":")))
        else:
            for listener in list(entry.cancel_listeners):
                listener()
        return "requested"

    def on_cancel(self, stream_id: str, listener: Callable[[], None]) -> Callable[[], None]:
        entry = self._streams.get(stream_id)
        if entry is None or entry.finished:
            return lambda: None
        entry.cancel_listeners.append(listener)

        def unsubscribe() -> None:
            if listener in entry.cancel_listeners:
                entry.cancel_listeners.remove(listener)

        return unsubscribe
