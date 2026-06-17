"""
Phase 1-2: mock_twilio -- a faithful Twilio Media Streams *client* that
bridges your real microphone and speakers to the WebSocket.

This stands in for the telephony provider. It is a SEPARATE process from your
service (exactly as real Twilio would be). You talk into your mic and hear the
agent through your speakers; your service never knows a human is involved.

Topology:
    [your mic]      -> capture -> 8kHz mu-law 20ms frames -> WS  -> server
    [your speakers] <- playback <- 8kHz mu-law frames       <- WS <- server

The hard part: sounddevice runs its audio callbacks on PortAudio's OWN thread,
while the WebSocket lives in the asyncio event loop. We bridge the two worlds
with thread-safe queues, never blocking either side.

Barge-in: when the server sends {"event":"clear"}, we immediately drop every
queued outbound frame, so the agent's voice stops the instant you interrupt.

USE HEADPHONES. With open speakers, the mic hears the agent and can self-trigger
barge-in (acoustic echo). Headphones make inbound (you) and outbound (agent)
physically separate channels -- which is the clean model this assessment wants.

Requires PortAudio on the host:
    macOS:   (bundled with the sounddevice wheel; if not: brew install portaudio)
    Linux:   sudo apt install libportaudio2
    Windows: (bundled with the sounddevice wheel)

Run (server must be running):
    uv run python simulator/mock_twilio.py
Press Ctrl+C to hang up.
"""

from __future__ import annotations

import argparse
import asyncio
import audioop
import base64
import json
import logging
import queue
import signal
import sys
import threading

import numpy as np
import sounddevice as sd
import websockets

logging.basicConfig(level=logging.INFO, format="%(asctime)s TWILIO %(message)s")
log = logging.getLogger("mock_twilio")

SERVER_URI = "ws://localhost:8080"
TELE_RATE = 8000          # telephony side: 8kHz mu-law
DEVICE_RATE = 16000       # mic/speaker side: 16kHz linear PCM (clean, widely supported)
FRAME_MS = 20
FRAME_SAMPLES_TELE = TELE_RATE * FRAME_MS // 1000     # 160 samples per outbound frame
FRAME_SAMPLES_DEV = DEVICE_RATE * FRAME_MS // 1000    # 320 samples per device frame
SAMPLE_WIDTH = 2          # 16-bit PCM


class MockTwilio:
    def __init__(self, uri: str, stream_sid: str):
        self.uri = uri
        self.stream_sid = stream_sid

        # Cross-thread queues bridging PortAudio threads <-> asyncio loop.
        # mic_q: filled by mic callback (audio thread), drained by WS sender (loop).
        self.mic_q: queue.Queue[bytes] = queue.Queue(maxsize=100)
        # play_q: filled by WS receiver (loop), drained by speaker callback (audio thread).
        # Bounded so a slow consumer can't grow memory without limit.
        self.play_q: queue.Queue[bytes] = queue.Queue(maxsize=200)

        # ratecv keeps resampler state across calls; one state object per direction.
        self._mic_resample_state = None
        self._play_resample_state = None

        self._running = threading.Event()
        self._running.set()

        # Leftover PCM that didn't fill a complete speaker frame.
        self._play_residual = b""

    # --- PortAudio callbacks (run on the audio thread, NOT the event loop) ---

    def _mic_callback(self, indata, frames, time_info, status):
        """Mic -> 8kHz mu-law -> mic_q. Runs on PortAudio's input thread."""
        if status:
            log.debug("mic status: %s", status)
        # indata is float32 [-1,1]; convert to 16-bit PCM bytes.
        pcm16 = (indata[:, 0] * 32767).astype(np.int16).tobytes()
        # Resample 16kHz -> 8kHz, carrying state across callbacks.
        pcm8, self._mic_resample_state = audioop.ratecv(
            pcm16, SAMPLE_WIDTH, 1, DEVICE_RATE, TELE_RATE, self._mic_resample_state
        )
        mulaw = audioop.lin2ulaw(pcm8, SAMPLE_WIDTH)
        try:
            self.mic_q.put_nowait(mulaw)
        except queue.Full:
            pass  # drop if sender is behind -- fresh audio matters more than complete

    def _speaker_callback(self, outdata, frames, time_info, status):
        """play_q -> speaker. Runs on PortAudio's output thread.

        Pulls mu-law frames the server sent, decodes+resamples to device rate,
        and fills the output buffer. On underrun (nothing queued) -> silence.
        """
        if status:
            log.debug("speaker status: %s", status)
        needed = frames * SAMPLE_WIDTH  # bytes of 16-bit PCM the device wants
        buf = self._play_residual
        while len(buf) < needed:
            try:
                mulaw = self.play_q.get_nowait()
            except queue.Empty:
                break
            pcm8 = audioop.ulaw2lin(mulaw, SAMPLE_WIDTH)
            pcm16, self._play_resample_state = audioop.ratecv(
                pcm8, SAMPLE_WIDTH, 1, TELE_RATE, DEVICE_RATE, self._play_resample_state
            )
            buf += pcm16
        take, self._play_residual = buf[:needed], buf[needed:]
        if len(take) < needed:
            take = take + b"\x00" * (needed - len(take))  # pad with silence
        outdata[:] = np.frombuffer(take, dtype=np.int16).reshape(-1, 1)

    def flush_playback(self):
        """Barge-in: drop all queued + residual outbound audio immediately."""
        dropped = 0
        try:
            while True:
                self.play_q.get_nowait()
                dropped += 1
        except queue.Empty:
            pass
        self._play_residual = b""
        log.info("CLEAR -> flushed playback (%d frames dropped)", dropped)

    # --- asyncio coroutines (run on the event loop) ---

    async def _sender(self, ws):
        """Drain mic_q -> WS as media frames. Yields to the loop between frames."""
        await ws.send(json.dumps({
            "event": "start",
            "start": {
                "streamSid": self.stream_sid,
                "mediaFormat": {"encoding": "audio/x-mulaw", "sampleRate": TELE_RATE},
            },
        }))
        log.info("-> START sent")
        ts = 0
        while self._running.is_set():
            try:
                mulaw = await asyncio.get_event_loop().run_in_executor(
                    None, self.mic_q.get, True, 0.1
                )
            except queue.Empty:
                continue
            await ws.send(json.dumps({
                "event": "media",
                "media": {"timestamp": str(ts), "payload": base64.b64encode(mulaw).decode()},
            }))
            ts += FRAME_MS

    async def _receiver(self, ws):
        """WS -> play_q (media) / flush (clear) / log (mark)."""
        async for raw in ws:
            try:
                msg = json.loads(raw)
            except json.JSONDecodeError:
                continue
            event = msg.get("event")
            if event == "media":
                mulaw = base64.b64decode(msg["media"]["payload"])
                try:
                    self.play_q.put_nowait(mulaw)
                except queue.Full:
                    # Speaker behind -> drop oldest, keep newest (low-latency bias).
                    try:
                        self.play_q.get_nowait()
                        self.play_q.put_nowait(mulaw)
                    except queue.Empty:
                        pass
            elif event == "clear":
                self.flush_playback()
            elif event == "mark":
                log.info("<- MARK: %s", msg.get("mark", {}).get("name"))

    async def run(self):
        # Open mic + speaker streams; their callbacks run on PortAudio threads.
        mic = sd.InputStream(
            samplerate=DEVICE_RATE, channels=1, dtype="float32",
            blocksize=FRAME_SAMPLES_DEV, callback=self._mic_callback,
        )
        speaker = sd.OutputStream(
            samplerate=DEVICE_RATE, channels=1, dtype="int16",
            blocksize=FRAME_SAMPLES_DEV, callback=self._speaker_callback,
        )
        with mic, speaker:
            log.info("Mic + speaker open. Talk now. Ctrl+C to hang up.")
            async with websockets.connect(self.uri) as ws:
                log.info("Connected to %s", self.uri)
                send_task = asyncio.create_task(self._sender(ws))
                recv_task = asyncio.create_task(self._receiver(ws))
                done, pending = await asyncio.wait(
                    {send_task, recv_task}, return_when=asyncio.FIRST_COMPLETED
                )
                for t in pending:
                    t.cancel()
                try:
                    await ws.send(json.dumps({"event": "stop"}))
                except Exception:
                    pass
        log.info("Hung up.")


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--uri", default=SERVER_URI)
    parser.add_argument("--sid", default="MOCK_TWILIO_001")
    args = parser.parse_args()

    bridge = MockTwilio(args.uri, args.sid)

    loop = asyncio.get_event_loop()
    stop = asyncio.Event()

    def _hangup(*_):
        bridge._running.clear()
        stop.set()

    try:
        loop.add_signal_handler(signal.SIGINT, _hangup)
    except NotImplementedError:
        pass  # Windows

    await bridge.run()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)