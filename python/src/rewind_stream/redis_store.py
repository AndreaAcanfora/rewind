"""A Store in Redis, shared by every server instance (same layout as the TypeScript RedisStore)."""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Callable
from typing import Any, Protocol

from .protocol import END, Snapshot, StreamEvent, is_terminal, status_of
from .store import CancelResult, StreamClosedError

LIVE, FINISHED = "live", "finished"


class RedisCommander(Protocol):
    """Anything with redis-py's `execute_command`, e.g. `redis.asyncio.Redis(decode_responses=True)`."""

    async def execute_command(self, *args: Any) -> Any: ...


def _text(v: Any) -> Any:
    return v.decode() if isinstance(v, bytes) else v


class RedisStore:
    """Streams in Redis, readable and cancellable from any instance.

    Uses only SET NX PX, GET, RPUSH, LRANGE, PEXPIRE and DEL: no Lua, no
    streams, no blocking reads. Readers poll for new events. Keys:
    `<prefix><id>:state`, `<prefix><id>:events`, `<prefix><id>:cancel`.
    Compatible with rewind's TypeScript RedisStore: both can share a Redis.
    """

    def __init__(
        self,
        redis: RedisCommander,
        prefix: str = "rewind:",
        retention: float = 300.0,
        max_lifetime: float = 3600.0,
        poll: float = 0.1,
        cancel_grace: float = 3.0,
    ) -> None:
        self._r = redis
        self._prefix = prefix
        self._retention_ms = int(retention * 1000)
        self._lifetime_ms = int(max_lifetime * 1000)
        self._poll = poll
        self._grace = cancel_grace
        self._tasks: set[asyncio.Task] = set()

    def _key(self, stream_id: str, part: str) -> str:
        return f"{self._prefix}{stream_id}:{part}"

    async def _cmd(self, *args: Any) -> Any:
        return _text(await self._r.execute_command(*[str(a) for a in args]))

    async def create(self, stream_id: str) -> None:
        ok = await self._cmd("SET", self._key(stream_id, "state"), LIVE, "NX", "PX", self._lifetime_ms + self._retention_ms)
        if ok is None:
            raise ValueError(f"stream {stream_id} already exists")

    async def append(self, stream_id: str, event: str, data: str) -> StreamEvent:
        if await self._cmd("GET", self._key(stream_id, "state")) != LIVE:
            raise StreamClosedError(stream_id)
        events = self._key(stream_id, "events")
        n = int(await self._cmd("RPUSH", events, json.dumps([event, data], ensure_ascii=False)))
        if n == 1:
            await self._cmd("PEXPIRE", events, self._lifetime_ms + self._retention_ms)
        if is_terminal(event):
            await self._cmd("SET", self._key(stream_id, "state"), FINISHED, "PX", self._retention_ms)
            await self._cmd("PEXPIRE", events, self._retention_ms)
            await self._cmd("DEL", self._key(stream_id, "cancel"))
        return StreamEvent(n, event, data)

    async def _read(self, stream_id: str, start: int) -> tuple[list[StreamEvent], bool]:
        raw = await self._r.execute_command("LRANGE", self._key(stream_id, "events"), str(start), "-1") or []
        out: list[StreamEvent] = []
        for i, item in enumerate(raw):
            event, data = json.loads(_text(item))
            e = StreamEvent(start + i + 1, event, data)
            out.append(e)
            if is_terminal(event):
                return out, True
        return out, False

    async def snapshot(self, stream_id: str, after: int) -> Snapshot | None:
        if await self._cmd("GET", self._key(stream_id, "state")) not in (LIVE, FINISHED):
            return None
        events, ended = await self._read(stream_id, 0)
        status = status_of(events[-1]) if ended and events else "live"
        return Snapshot(status, len(events), events[after:])

    async def follow(self, stream_id: str, after: int) -> AsyncIterator[StreamEvent]:
        snap = await self.snapshot(stream_id, after)
        if snap is None or (snap.status != "live" and after >= snap.last_id):
            return
        cursor = after
        while True:
            events, ended = await self._read(stream_id, cursor)
            for e in events:
                cursor = e.id
                yield e
            if ended:
                return
            if not events:
                if await self._cmd("GET", self._key(stream_id, "state")) not in (LIVE, FINISHED):
                    return
                await asyncio.sleep(self._poll)

    async def cancel(self, stream_id: str) -> CancelResult:
        state = await self._cmd("GET", self._key(stream_id, "state"))
        if state == FINISHED:
            return "finished"
        if state != LIVE:
            return "missing"
        await self._cmd("SET", self._key(stream_id, "cancel"), "1", "PX", self._lifetime_ms)
        task = asyncio.ensure_future(self._end_if_abandoned(stream_id))
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return "requested"

    async def _end_if_abandoned(self, stream_id: str) -> None:
        await asyncio.sleep(self._grace)
        try:
            await self.append(stream_id, END, json.dumps({"reason": "cancelled"}, separators=(",", ":")))
        except StreamClosedError:
            pass

    def on_cancel(self, stream_id: str, listener: Callable[[], None]) -> Callable[[], None]:
        async def watch() -> None:
            while True:
                await asyncio.sleep(max(self._poll, 0.05))
                try:
                    if await self._cmd("GET", self._key(stream_id, "cancel")) == "1":
                        listener()
                        return
                except (OSError, RuntimeError):  # transient Redis error: try again
                    continue

        task = asyncio.ensure_future(watch())
        return task.cancel
