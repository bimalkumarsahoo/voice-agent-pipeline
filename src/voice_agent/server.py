"""
Server wired with the full-duplex ConversationPipeline + barge-in (Phase 5).

Run:  uv run python -m voice_agent.server
"""

from __future__ import annotations

import asyncio
import logging

import websockets

from voice_agent.protocol import (
    StartMessage, MediaMessage, StopMessage,
    parse_inbound, outbound_media, outbound_mark, outbound_clear,
)
from voice_agent.stages import MockSTT, MockLLM, MockTTS
from voice_agent.pipeline import ConversationPipeline

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("voice_agent")

HOST, PORT = "0.0.0.0", 8080


class OutboundSender:
    """Sends audio/marks/clear to the caller.

    Outbound audio is sent through a bounded queue drained by a background
    task. This gives barge-in a real "flush queued audio" operation: on clear()
    we empty the queue so frames already produced but not yet sent are DROPPED,
    then emit the `clear` event so the far side discards what it has buffered.
    Without the queue, "flush" would be a no-op because frames go straight out.
    """

    def __init__(self, websocket, stream_sid: str):
        self._ws = websocket
        self._stream_sid = stream_sid
        self._queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=400)
        self._drain_task = asyncio.create_task(self._drain())

    async def _drain(self):
        try:
            while True:
                mulaw = await self._queue.get()
                await self._ws.send(outbound_media(self._stream_sid, mulaw))
        except asyncio.CancelledError:
            raise

    async def send_audio(self, mulaw: bytes) -> None:
        try:
            self._queue.put_nowait(mulaw)
        except asyncio.QueueFull:
            # Bounded buffer: drop oldest, keep freshest (stale audio is useless).
            try:
                self._queue.get_nowait()
                self._queue.put_nowait(mulaw)
            except (asyncio.QueueEmpty, asyncio.QueueFull):
                pass

    async def send_mark(self, name: str) -> None:
        await self._ws.send(outbound_mark(self._stream_sid, name))

    async def clear(self) -> None:
        """Barge-in flush: drop all queued outbound audio, then send `clear`."""
        dropped = 0
        while not self._queue.empty():
            try:
                self._queue.get_nowait()
                dropped += 1
            except asyncio.QueueEmpty:
                break
        await self._ws.send(outbound_clear(self._stream_sid))
        log.info("CLEAR sent (dropped %d queued frames)", dropped)

    async def aclose(self):
        self._drain_task.cancel()
        try:
            await self._drain_task
        except asyncio.CancelledError:
            pass


def build_pipeline(sender: OutboundSender) -> ConversationPipeline:
    """One place to wire stages. Swap a Mock* for a real provider here."""
    return ConversationPipeline(sender, MockSTT(), MockLLM(), MockTTS())


async def handle_connection(websocket):
    sender = None
    pipeline = None
    stream_sid = None
    frame_count = 0

    try:
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
                log.info("STOP stream=%s frames=%d -> call ended", stream_sid, frame_count)
                if pipeline is not None:
                    await pipeline.on_stop()
                break
    finally:
        if sender is not None:
            await sender.aclose()

    log.info("Connection closed (stream=%s)", stream_sid)


async def main():
    log.info("Voice agent server (full-duplex + barge-in) on ws://%s:%d", HOST, PORT)
    async with websockets.serve(handle_connection, HOST, PORT):
        await asyncio.Future()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Shutting down.")