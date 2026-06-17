"""
Minimal WebSocket server stub (Phase 0 smoke test).

Right now it just accepts a connection, parses inbound messages into typed
objects, and logs them. The real pipeline (STT->LLM->TTS, endpointing,
barge-in) gets wired into the `handle_connection` loop in later phases.

Run:  uv run python -m voice_agent.server
"""

from __future__ import annotations

import asyncio
import logging

import websockets

from voice_agent.protocol import (
    StartMessage,
    MediaMessage,
    StopMessage,
    parse_inbound,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("voice_agent")

HOST = "0.0.0.0"
PORT = 8080


async def handle_connection(websocket):
    """One coroutine per call. Reads inbound frames and dispatches by type."""
    stream_sid = None
    frame_count = 0

    async for raw in websocket:
        msg = parse_inbound(raw)

        if isinstance(msg, StartMessage):
            stream_sid = msg.stream_sid
            log.info(
                "START stream=%s encoding=%s rate=%d",
                msg.stream_sid, msg.encoding, msg.sample_rate,
            )

        elif isinstance(msg, MediaMessage):
            frame_count += 1
            # Phase 2+: decode mu-law -> resample -> feed VAD/STT here.
            if frame_count % 50 == 0:  # log every ~1s of audio
                log.info("MEDIA frames=%d (%d bytes last)",
                         frame_count, len(msg.audio))

        elif isinstance(msg, StopMessage):
            log.info("STOP stream=%s total_frames=%d", stream_sid, frame_count)
            break

        else:
            log.warning("Unknown inbound message ignored")

    log.info("Connection closed (stream=%s)", stream_sid)


async def main():
    log.info("Voice agent server listening on ws://%s:%d", HOST, PORT)
    async with websockets.serve(handle_connection, HOST, PORT):
        await asyncio.Future()  # run forever


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Shutting down.")