"""
WebSocket server entry point.

Accepts call connections and runs one ConversationPipeline per call. Outbound
audio goes through OutboundSender, which buffers frames in a bounded queue
drained by a background task so barge-in can flush undelivered audio.

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
    """Sends audio / marks / clear to the caller.

    Audio is queued and drained by a background task so that, on barge-in,
    clear() can drop frames that were produced but not yet sent. The queue is
    bounded; under pressure the oldest frame is dropped in favour of the newest.

    websocket:  the open connection.
    stream_sid: id echoed back in every outbound message.
    """

    def __init__(self, websocket, stream_sid: str):
        self._ws = websocket
        self._stream_sid = stream_sid
        # maxsize is in FRAMES (each ~20ms); 400 frames ~= 8s of headroom.
        self._queue: asyncio.Queue[bytes] = asyncio.Queue(maxsize=400)
        self._drain_task = asyncio.create_task(self._drain())

    async def _drain(self):
        """Continuously send queued audio frames over the WebSocket."""
        try:
            while True:
                mulaw = await self._queue.get()
                await self._ws.send(outbound_media(self._stream_sid, mulaw))
        except asyncio.CancelledError:
            raise

    async def send_audio(self, mulaw: bytes) -> None:
        """Enqueue one outbound audio frame (drop-oldest if the queue is full)."""
        try:
            self._queue.put_nowait(mulaw)
        except asyncio.QueueFull:
            try:
                self._queue.get_nowait()       # drop oldest, keep freshest
                self._queue.put_nowait(mulaw)
            except (asyncio.QueueEmpty, asyncio.QueueFull):
                pass

    async def send_mark(self, name: str) -> None:
        await self._ws.send(outbound_mark(self._stream_sid, name))

    async def clear(self) -> None:
        """Drop all queued outbound audio, then send the clear event."""
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
        """Stop the drain task."""
        self._drain_task.cancel()
        try:
            await self._drain_task
        except asyncio.CancelledError:
            pass


def build_pipeline(sender: OutboundSender) -> ConversationPipeline:
    """Wire the pipeline stages. Swap a Mock* for a real provider here."""
    return ConversationPipeline(sender, MockSTT(), MockLLM(), MockTTS())


async def handle_connection(websocket):
    """One coroutine per call: dispatch inbound events to the pipeline."""
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
                log.info("STOP stream=%s frames=%d", stream_sid, frame_count)
                if pipeline is not None:
                    await pipeline.on_stop()
                break
    finally:
        if sender is not None:
            await sender.aclose()

    log.info("Connection closed (stream=%s)", stream_sid)


async def main():
    log.info("Voice agent server on ws://%s:%d", HOST, PORT)
    async with websockets.serve(handle_connection, HOST, PORT):
        await asyncio.Future()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log.info("Shutting down.")