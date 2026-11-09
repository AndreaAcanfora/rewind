"""Resumable Stream Protocol for Python: LLM token streams that survive
dropped connections, page reloads and multiple tabs.

See https://github.com/AndreaAcanfora/rewind/blob/master/SPEC.md
"""

from .asgi import StreamApp, new_stream_id, sse_body, start_stream
from .produce import produce
from .protocol import END, ERROR, VERSION, Snapshot, StreamEvent, encode_event, is_terminal, parse_cursor, status_of
from .redis_store import RedisStore
from .store import MemoryStore, Store, StreamClosedError

__version__ = "0.1.0"

__all__ = [
    "END",
    "ERROR",
    "VERSION",
    "MemoryStore",
    "RedisStore",
    "Snapshot",
    "Store",
    "StreamApp",
    "StreamClosedError",
    "StreamEvent",
    "encode_event",
    "is_terminal",
    "new_stream_id",
    "parse_cursor",
    "produce",
    "sse_body",
    "start_stream",
    "status_of",
]
