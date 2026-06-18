"""
Interactive telephony client: bridges the local microphone and speakers to the
WebSocket, standing in for a real telephony provider.

Captures mic audio, converts to 8kHz mu-law 20ms frames, and streams it to the
server; receives outbound audio and plays it through the speakers. Handles the
clear event by flushing queued playback, so the agent stops immediately on
barge-in.

sounddevice callbacks run on PortAudio threads while the WebSocket runs on the
asyncio loop; thread-safe queues bridge the two without blocking either.

Use headphones: with open speakers the mic can pick up the agent's voice and
falsely trigger barge-in (the inbound and outbound channels would no longer be
acoustically separate).

Requires PortAudio on the host (bundled with the sounddevice wheel on
macOS/Windows; `apt install libportaudio2` on Linux).

Run:  python simulator/mock_twilio.py   (Ctrl+C to hang up)
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
TELE_RATE = 8000           # telephony side: 8kHz mu-law
DEVICE_RATE = 16000        # mic/speaker side: 16kHz linear PCM
FRAME_MS = 20
FRAME_SAMPLES_DEV = DEVICE_RATE * FRAME_MS // 1000
SAMPLE_WIDTH = 2


class MockTwilio:
    """Bridges local audio devices to the WebSocket.

    uri:        server WebSocket URI.
    stream_sid: id sent in the start message and echoed in outbound frames.
    """

    def __init__(self, uri: str, stream_sid: str):
        self.uri = uri
        self.stream_sid = stream_sid

        # mic_q: mic thread -> WS sender.  play_q: WS receiver -> speaker thread.
        self.mic_q: queue.Queue[bytes] = queue.Queue(maxsize=100)
        self.play_q: queue.Queue[bytes] = queue.Queue(maxsize=200)  # bounded playback buffer

        self._mic_resample_state = None
        self._play_resample_state = None

        self._running = threading.Event()
        self._running.set()
        self._play_residual = b""   # leftover PCM that didn't fill a speaker block

    # --- PortAudio callbacks (audio threads) -------------------------------

    def _mic_callback(self, indata, frames, time_info, status):
        """Mic -> 8kHz mu-law -> mic_q."""
        pcm16 = (indata[:, 0] * 32767).astype(np.int16).tobytes()
        pcm8, self._mic_resample_state = audioop.ratecv(
            pcm16, SAMPLE_WIDTH, 1, DEVICE_RATE, TELE_RATE, self._mic_resample_state
        )
        mulaw = audioop.lin2ulaw(pcm8, SAMPLE_WIDTH)
        try:
            self.mic_q.put_nowait(mulaw)
        except queue.Full:
            pass  # drop if the sender is behind; fresh audio matters more

    def _speaker_callback(self, outdata, frames, time_info, status):
        """play_q -> speaker, decoding/resampling to the device rate."""
        needed = frames * SAMPLE_WIDTH
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
            take = take + b"\x00" * (needed - len(take))   # pad underrun with silence
        outdata[:] = np.frombuffer(take, dtype=np.int16).reshape(-1, 1)

    def flush_playback(self):
        """Drop all queued + residual outbound audio (handles the clear event)."""
        dropped = 0
        try:
            while True:
                self.play_q.get_nowait()
                dropped += 1
        except queue.Empty:
            pass
        self._play_residual = b""
        log.info("CLEAR -> flushed playback (%d frames dropped)", dropped)

    # --- asyncio coroutines (event loop) -----------------------------------

    async def _sender(self, ws):
        """Drain mic_q to the WebSocket as media frames."""
        await ws.send(json.dumps({
            "event": "start",
            "start": {"streamSid": self.stream_sid,
                      "mediaFormat": {"encoding": "audio/x-mulaw", "sampleRate": TELE_RATE}},
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
        """Route outbound messages: media -> play_q, clear -> flush, mark -> log."""
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
                    try:
                        self.play_q.get_nowait()       # drop oldest, keep newest
                        self.play_q.put_nowait(mulaw)
                    except queue.Empty:
                        pass
            elif event == "clear":
                self.flush_playback()
            elif event == "mark":
                log.info("<- MARK: %s", msg.get("mark", {}).get("name"))

    async def run(self):
        """Open audio devices and run the send/receive loops until hang-up."""
        mic = sd.InputStream(samplerate=DEVICE_RATE, channels=1, dtype="float32",
                             blocksize=FRAME_SAMPLES_DEV, callback=self._mic_callback)
        speaker = sd.OutputStream(samplerate=DEVICE_RATE, channels=1, dtype="int16",
                                  blocksize=FRAME_SAMPLES_DEV, callback=self._speaker_callback)
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

    def _hangup(*_):
        bridge._running.clear()

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