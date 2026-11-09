"""Shared definitions of the Resumable Stream Protocol (SPEC.md)."""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from typing import Literal

VERSION = "0.1"
END = "stream.end"
ERROR = "stream.error"

StreamStatus = Literal["live", "complete", "cancelled", "error"]

_TYPE = re.compile(r"^[A-Za-z0-9._-]{1,64}$")
_CURSOR = re.compile(r"^(0|[1-9][0-9]{0,14})$")


@dataclass(frozen=True)
class StreamEvent:
    """One event: its position in the stream (1, 2, 3…), type and data."""

    id: int
    event: str
    data: str

    def to_dict(self) -> dict:
        return {"id": self.id, "event": self.event, "data": self.data}


@dataclass(frozen=True)
class Snapshot:
    status: StreamStatus
    last_id: int
    events: list[StreamEvent]

    def to_dict(self) -> dict:
        return {"status": self.status, "last_id": self.last_id, "events": [e.to_dict() for e in self.events]}


def is_terminal(event: str) -> bool:
    return event in (END, ERROR)


def is_valid_type(event: str) -> bool:
    return bool(_TYPE.match(event))


def is_reserved_type(event: str) -> bool:
    return event.startswith("stream.")


def status_of(terminal: StreamEvent) -> StreamStatus:
    """Status of a finished stream, from its terminal event."""
    if terminal.event == ERROR:
        return "error"
    try:
        return "cancelled" if json.loads(terminal.data).get("reason") == "cancelled" else "complete"
    except (ValueError, AttributeError):
        return "complete"


def parse_cursor(value: str) -> int | None:
    """Parses a Last-Event-ID header or `after` parameter; None if invalid."""
    return int(value) if _CURSOR.match(value) else None


def encode_event(e: StreamEvent) -> str:
    """Encodes an event as SSE (spec section 4.1)."""
    out = f"id: {e.id}\n"
    if e.event != "message":
        out += f"event: {e.event}\n"
    for line in re.split(r"\r\n|\r|\n", e.data):
        out += f"data: {line}\n"
    return out + "\n"
