"""
Message types for the Twilio Media Streams-style WebSocket protocol.

This is a faithful subset of the Twilio Media Streams contract. Keeping the
wire format isolated here means the rest of the pipeline never touches raw
JSON or base64 -- it works with typed objects.

Inbound (simulator/telephony -> our service):
    start  : stream begins, carries streamSid + media format
    media  : a ~20ms frame of base64-encoded 8kHz mu-law audio
    stop   : stream ends

Outbound (our service -> simulator/telephony):
    media  : audio to play back to the caller (base64 mu-law)
    mark   : a labeled marker we can use to know when playback reaches a point
    clear  : flush any buffered outbound audio NOT yet played (barge-in)
"""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from typing import Optional


# --- Inbound messages ------------------------------------------------------


@dataclass
class StartMessage:
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
    audio: bytes  # raw mu-law bytes (already base64-decoded)

    @classmethod
    def from_dict(cls, d: dict) -> "MediaMessage":
        media = d["media"]
        return cls(
            timestamp=int(media.get("timestamp", 0)),
            audio=base64.b64decode(media["payload"]),
        )


@dataclass
class StopMessage:
    @classmethod
    def from_dict(cls, d: dict) -> "StopMessage":
        return cls()


# --- Parsing entry point ---------------------------------------------------


def parse_inbound(raw: str):
    """Parse a raw inbound JSON string into a typed message, or None if unknown."""
    d = json.loads(raw)
    event = d.get("event")
    if event == "start":
        return StartMessage.from_dict(d)
    if event == "media":
        return MediaMessage.from_dict(d)
    if event == "stop":
        return StopMessage.from_dict(d)
    return None  # unknown event -- ignore, but don't crash


# --- Outbound message builders --------------------------------------------


def outbound_media(stream_sid: str, audio: bytes) -> str:
    """Build an outbound media message. `audio` is raw mu-law bytes."""
    return json.dumps(
        {
            "event": "media",
            "streamSid": stream_sid,
            "media": {"payload": base64.b64encode(audio).decode("ascii")},
        }
    )


def outbound_mark(stream_sid: str, name: str) -> str:
    """Build a mark message -- a labeled checkpoint in the outbound audio."""
    return json.dumps(
        {
            "event": "mark",
            "streamSid": stream_sid,
            "mark": {"name": name},
        }
    )


def outbound_clear(stream_sid: str) -> str:
    """Build a clear message -- tells the far side to flush buffered audio.

    This is the barge-in primitive: when the caller interrupts, we send this
    so any already-queued agent audio is discarded instead of played.
    """
    return json.dumps({"event": "clear", "streamSid": stream_sid})