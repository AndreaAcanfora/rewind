import asyncio
import os
import socket
import threading
import time
import unittest

import httpx
import uvicorn

from rewind_stream import (
    END,
    MemoryStore,
    RedisStore,
    StreamApp,
    StreamClosedError,
    StreamEvent,
    encode_event,
    parse_cursor,
    produce,
    status_of,
)
from tests.resp import RespClient

REDIS_URL = os.environ.get("REDIS_URL")


async def words(n, delay=0.0, cancelled=None):
    for i in range(1, n + 1):
        if delay:
            await asyncio.sleep(delay)
        yield {"event": "delta", "data": {"text": f"w{i} "}}


async def collect(it):
    return [e async for e in it]


class ProtocolTest(unittest.TestCase):
    def test_encode_and_cursor(self):
        self.assertEqual(encode_event(StreamEvent(2, "delta", "a\nb")), "id: 2\nevent: delta\ndata: a\ndata: b\n\n")
        self.assertEqual(encode_event(StreamEvent(1, "message", "hi")), "id: 1\ndata: hi\n\n")
        self.assertEqual(parse_cursor("42"), 42)
        for bad in ["", "-1", "01", "1.5", "x"]:
            self.assertIsNone(parse_cursor(bad))
        self.assertEqual(status_of(StreamEvent(1, END, '{"reason":"cancelled"}')), "cancelled")


class StoreContract:
    """Behaviour every store must have; mixed into a TestCase with make_store()."""

    n = 0

    def fresh(self):
        StoreContract.n += 1
        return f"py-{os.getpid()}-{time.time_ns()}-{StoreContract.n}"

    async def test_numbering_and_terminal(self):
        store = await self.make_store()
        sid = self.fresh()
        await store.create(sid)
        self.assertEqual((await store.append(sid, "delta", "x")).id, 1)
        self.assertEqual((await store.append(sid, END, '{"reason":"complete"}')).id, 2)
        with self.assertRaises(StreamClosedError):
            await store.append(sid, "delta", "late")
        snap = await store.snapshot(sid, 1)
        self.assertEqual((snap.status, snap.last_id, [e.id for e in snap.events]), ("complete", 2, [2]))
        self.assertIsNone(await store.snapshot(self.fresh(), 0))
        with self.assertRaises(ValueError):
            await store.create(sid)

    async def test_follow_live(self):
        store = await self.make_store()
        sid = self.fresh()
        await store.create(sid)
        await store.append(sid, "delta", "1")
        task = asyncio.ensure_future(collect(store.follow(sid, 0)))
        await asyncio.sleep(0.05)
        await store.append(sid, "delta", "2 — ünïcødé 🚀")
        await asyncio.sleep(0.05)
        await store.append(sid, END, '{"reason":"complete"}')
        events = await task
        self.assertEqual([e.id for e in events], [1, 2, 3])
        self.assertEqual(events[1].data, "2 — ünïcødé 🚀")
        self.assertEqual([e.id for e in await collect(store.follow(sid, 2))], [3])
        self.assertEqual(await collect(store.follow(sid, 3)), [])

    async def test_produce_and_cancel(self):
        store = await self.make_store()
        sid = self.fresh()
        await store.create(sid)
        self.assertEqual(await produce(store, sid, words(3)), "complete")
        sid2 = self.fresh()
        await store.create(sid2)
        task = asyncio.ensure_future(produce(store, sid2, lambda cancelled: words(1000, 0.005, cancelled)))
        await asyncio.sleep(0.1)
        self.assertEqual(await store.cancel(sid2), "requested")
        self.assertEqual(await task, "cancelled")
        snap = await store.snapshot(sid2, 0)
        self.assertEqual(snap.status, "cancelled")
        self.assertLess(snap.last_id, 1000)
        self.assertEqual(await store.cancel(sid2), "finished")
        self.assertEqual(await store.cancel(self.fresh()), "missing")

    async def test_errors_are_hidden(self):
        store = await self.make_store()
        sid = self.fresh()
        await store.create(sid)

        async def failing():
            yield "partial"
            raise RuntimeError("password hunter2")

        self.assertEqual(await produce(store, sid, failing()), "error")
        last = (await store.snapshot(sid, 0)).events[-1]
        self.assertEqual(last.event, "stream.error")
        self.assertNotIn("hunter2", last.data)


class MemoryStoreTest(StoreContract, unittest.IsolatedAsyncioTestCase):
    async def make_store(self):
        return MemoryStore()

    async def test_retention(self):
        store = MemoryStore(retention=0.05)
        await store.create("a")
        await store.append("a", END, '{"reason":"complete"}')
        await asyncio.sleep(0.1)
        self.assertEqual(len(store), 0)


@unittest.skipUnless(REDIS_URL, "REDIS_URL is not set")
class RedisStoreTest(StoreContract, unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.clients = []

    async def asyncTearDown(self):
        for c in self.clients:
            await c.close()

    async def make_store(self):
        c = await RespClient.open(REDIS_URL)
        self.clients.append(c)
        return RedisStore(c, prefix=f"rewind-py-{os.getpid()}:", poll=0.02, cancel_grace=0.3)

    async def test_two_instances(self):
        a, b = await self.make_store(), await self.make_store()
        sid = self.fresh()
        await a.create(sid)
        task = asyncio.ensure_future(produce(a, sid, words(5, 0.01)))
        self.assertEqual([e.id for e in await collect(b.follow(sid, 0))], [1, 2, 3, 4, 5, 6])
        self.assertEqual(await task, "complete")


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Server:
    """Runs an ASGI app with uvicorn in a thread."""

    def __init__(self, app):
        self.port = free_port()
        self.server = uvicorn.Server(uvicorn.Config(app, host="127.0.0.1", port=self.port, log_level="warning"))
        self.thread = threading.Thread(target=self.server.run, daemon=True)

    def __enter__(self):
        self.thread.start()
        while not self.server.started:
            time.sleep(0.01)
        return f"http://127.0.0.1:{self.port}"

    def __exit__(self, *exc):
        self.server.should_exit = True
        self.thread.join(5)


def parse_sse(text):
    events, cur = [], {}
    for line in text.split("\n"):
        if line == "":
            if "data" in cur:
                events.append(cur)
            cur = {}
        elif not line.startswith(":") and ":" in line:
            k, v = line.split(":", 1)
            v = v.removeprefix(" ")
            cur[k] = cur.get(k, "") + ("\n" if k == "data" and k in cur else "") + v
    return events


class HTTPTest(unittest.TestCase):
    def test_stream_app(self):
        loop_holder = {}
        store = MemoryStore()

        async def app(scope, receive, send):
            if scope["type"] == "lifespan":
                while True:
                    m = await receive()
                    if m["type"] == "lifespan.startup":
                        loop_holder["loop"] = asyncio.get_running_loop()
                        await send({"type": "lifespan.startup.complete"})
                    else:
                        await send({"type": "lifespan.shutdown.complete"})
                        return
            return await StreamApp(store, keep_alive=0.05, retry_ms=10)(scope, receive, send)

        with Server(app) as base:
            loop = loop_holder["loop"]

            def run(coro):
                return asyncio.run_coroutine_threadsafe(coro, loop).result(5)

            run(store.create("s1"))
            asyncio.run_coroutine_threadsafe(produce(store, "s1", words(3, 0.1)), loop)
            with httpx.Client(base_url=base, timeout=10) as http:
                r = http.get("/s1")  # live: follows until the end, with keep-alive comments
                self.assertEqual(r.headers["content-type"], "text/event-stream; charset=utf-8")
                self.assertEqual(r.headers["cache-control"], "no-store")
                self.assertIn(": keep-alive", r.text)
                self.assertEqual([e["id"] for e in parse_sse(r.text)], ["1", "2", "3", "4"])
                tail = parse_sse(http.get("/s1", headers={"Last-Event-ID": "2"}).text)
                self.assertEqual([e["id"] for e in tail], ["3", "4"])
                self.assertEqual(http.get("/s1", headers={"Last-Event-ID": "4"}).status_code, 204)
                self.assertEqual(http.get("/s1", headers={"Last-Event-ID": "x"}).status_code, 400)
                self.assertEqual(http.get("/nope").status_code, 404)
                snap = http.get("/s1?after=1", headers={"Accept": "application/json"}).json()
                self.assertEqual((snap["status"], snap["last_id"], len(snap["events"])), ("complete", 4, 3))
                self.assertEqual(http.delete("/s1").status_code, 204)
                self.assertEqual(http.put("/s1").status_code, 405)

    def test_starlette_start(self):
        from starlette.applications import Starlette
        from starlette.routing import Mount, Route

        from rewind_stream.starlette import start

        store = MemoryStore()

        async def chat(request):
            return await start(words(4, 0.02), store=store, url=lambda sid: f"/streams/{sid}")

        app = Starlette(routes=[Route("/chat", chat, methods=["POST"]), Mount("/streams", StreamApp(store))])
        with Server(app) as base, httpx.Client(base_url=base, timeout=10) as http:
            r = http.post("/chat")
            loc = r.headers["stream-location"]
            self.assertTrue(loc.startswith("/streams/"))
            self.assertEqual(len(parse_sse(r.text)), 5)
            again = http.get(loc, headers={"Last-Event-ID": "3"})
            self.assertEqual([e["id"] for e in parse_sse(again.text)], ["4", "5"])


if __name__ == "__main__":
    unittest.main()
