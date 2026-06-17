"""
Server wired with the real ConversationPipeline (Phase 3).

The pipeline (STT->LLM->TTS, streamed) replaces the Phase 1 EchoResponder,
plugging into the same OutboundSender seam and the same on_audio /
on_caller_stopped interface -- so handle_connection barely changed.

NB: on_caller_stopped is currently fired on the `stop` event (whole-call =
one turn). Phase 4 replaces that with real endpointing (silence detection)
so a multi-turn conversation works mid-call.

Run:  uv run python -m voice_agent.server
"""

from __future__ import annotations

import asyncio
import logging

import websockets

from voice_agent.protocol import (
    StartMessage, MediaMessage, StopMessage,
    parse_inbound, outbound_media, outbound_mark,
)
from voice_agent.stages import MockSTT, MockLLM, MockTTS
from voice_agent.pipeline import ConversationPipeline

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("voice_agent")

HOST, PORT = "0.0.0.0", 8080


class OutboundSender:
    def __init__(self, websocket, stream_sid: str):
        self._ws = websocket
        self._stream_sid = stream_sid

    async def send_audio(self, mulaw: bytes) -> None:
        await self._ws.send(outbound_media(self._stream_sid, mulaw))

    async def send_mark(self, name: str) -> None:
        await self._ws.send(outbound_mark(self._stream_sid, name))


def build_pipeline(sender: OutboundSender) -> ConversationPipeline:
    """One place to wire stages. Swap a Mock* for a real provider here."""
    return ConversationPipeline(sender, MockSTT(), MockLLM(), MockTTS())


async def handle_connection(websocket):
    sender = None
    pipeline = None
    stream_sid = None
    frame_count = 0

    async for raw in websocket:
        msg = parse_inbound(raw)

        if isinstance(msg, StartMessage):
            stream_sid = msg.stream_sid
            sender = OutboundSender(websocket, stream_sid)
            pipeline = build_pipeline(sender)
            log.info("START stream=%s rate=%d", msg.stream_sid, msg.sample_rate)

        elif isinstance(msg, MediaMessage):
            if pipeline is None:
                continue
            frame_count += 1
            await pipeline.on_audio(msg.audio)

        elif isinstance(msg, StopMessage):
            log.info("STOP stream=%s frames=%d -> caller turn ended", stream_sid, frame_count)
            if pipeline is not None:
                await pipeline.on_caller_stopped()
            break

    log.info("Connection closed (stream=%s)", stream_sid)


async def main():
    log.info("Voice agent server (pipeline) on ws://%s:%d", HOST, PORT)
    async with websockets.serve(handle_connection, HOST, PORT):
        await asyncio.Future()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Shutting down.")