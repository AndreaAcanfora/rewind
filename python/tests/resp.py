"""A minimal asyncio RESP2 client for the tests (no redis-py needed)."""

from __future__ import annotations

import asyncio
from urllib.parse import urlparse


class RespClient:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        self._r, self._w = reader, writer
        self._lock = asyncio.Lock()

    @classmethod
    async def open(cls, url: str) -> RespClient:
        u = urlparse(url)
        return cls(*await asyncio.open_connection(u.hostname, u.port or 6379))

    async def execute_command(self, *args):
        out = f"*{len(args)}\r\n"
        for a in args:
            a = str(a)
            out += f"${len(a.encode())}\r\n{a}\r\n"
        async with self._lock:
            self._w.write(out.encode())
            await self._w.drain()
            return await self._reply()

    async def _reply(self):
        line = (await self._r.readline()).decode().rstrip("\r\n")
        kind, rest = line[0], line[1:]
        if kind == "+":
            return rest
        if kind == "-":
            raise RuntimeError(rest)
        if kind == ":":
            return int(rest)
        if kind == "$":
            n = int(rest)
            if n < 0:
                return None
            data = await self._r.readexactly(n + 2)
            return data[:-2].decode()
        if kind == "*":
            n = int(rest)
            return None if n < 0 else [await self._reply() for _ in range(n)]
        raise RuntimeError(f"unexpected reply {line!r}")

    async def close(self) -> None:
        self._w.close()
