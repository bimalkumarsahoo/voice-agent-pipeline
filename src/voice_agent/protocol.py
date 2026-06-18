"""
WebSocket wire protocol for the Media Streams audio contract.

Parses inbound messages (start / media / stop) into typed objects and builds
outbound messages (media / mark / clear). Keeping the wire format isolated here
means the rest of the pipeline works with typed objects and never touches raw
JSON or base64.
"""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from typing import Optional


# --- Inbound messages ------------------------------------------------------


@dataclass
class StartMessage:
    """Stream start: carries the stream id and negotiated media format."""
    stream_sid: str
    encoding: str
    sample_rate: int

    @classmethod
    def from_dict(cls, d: dict) -> "StartMessage":
        start = d["start"]
        fmt = start.get("mediaFormat", {})
        return cls(
            stream_sid=start["streamSid"],
            encoding=fmt.get("encoding", "audio/x-mulaw"),
            sample_rate=int(fmt.get("sampleRate", 8000)),
        )


@dataclass
class MediaMessage:
    """An inbound audio frame. `audio` is the decoded raw mu-law bytes."""
    timestamp: int
    audio: bytes

    @classmethod
    def from_dict(cls, d: dict) -> "MediaMessage":
        media = d["media"]
        return cls(
            timestamp=int(media.get("timestamp", 0)),
            audio=base64.b64decode(media["payload"]),  # decode base64 -> raw mu-law
        )


@dataclass
class StopMessage:
    """Stream end."""
    @classmethod
    def from_dict(cls, d: dict) -> "StopMessage":
        return cls()


def parse_inbound(raw: str):
    """Parse a raw inbound JSON string into a typed message.

    Returns a StartMessage / MediaMessage / StopMessage, or None for an unknown
    event (unknown events are ignored rather than raising).
    """
    d = json.loads(raw)
    event = d.get("event")
    if event == "start":
        return StartMessage.from_dict(d)
    if event == "media":
        return MediaMessage.from_dict(d)
    if event == "stop":
        return StopMessage.from_dict(d)
    return None


# --- Outbound message builders --------------------------------------------


def outbound_media(stream_sid: str, audio: bytes) -> str:
    """Build an outbound media message from raw mu-law bytes."""
    return json.dumps({
        "event": "media",
        "streamSid": stream_sid,
        "media": {"payload": base64.b64encode(audio).decode("ascii")},
    })


def outbound_mark(stream_sid: str, name: str) -> str:
    """Build a mark message: a labeled checkpoint in the outbound audio stream."""
    return json.dumps({
        "event": "mark",
        "streamSid": stream_sid,
        "mark": {"name": name},
    })


def outbound_clear(stream_sid: str) -> str:
    """Build a clear message: tells the far side to discard buffered, unplayed audio."""
    return json.dumps({"event": "clear", "streamSid": stream_sid})