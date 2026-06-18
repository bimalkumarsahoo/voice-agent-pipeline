"""
WAV-file driver: a telephony client that streams a .wav over the WebSocket at
real-time pace, for deterministic, repeatable testing.

Reads a WAV, converts it to 8kHz mono mu-law, chunks it into 20ms frames, and
sends them one every 20ms (mimicking a live call). Also listens for outbound
messages and logs them.

Run:  python simulator/stream_wav.py samples/clip.wav
"""

from __future__ import annotations

import argparse
import asyncio
import audioop
import base64
import json
import logging
import sys
import time
import wave

import websockets

logging.basicConfig(level=logging.INFO, format="%(asctime)s SIM %(message)s")
log = logging.getLogger("simulator")

SERVER_URI = "ws://localhost:8080"
FRAME_MS = 20
TARGET_RATE = 8000
SAMPLE_WIDTH = 2


def load_wav_as_mulaw_frames(path: str) -> list[bytes]:
    """Load a WAV and return it as a list of 20ms 8kHz mu-law frames.

    Handles mono/stereo, any bit depth, and any sample rate by normalising to
    8kHz mono mu-law.
    """
    with wave.open(path, "rb") as wf:
        n_channels = wf.getnchannels()
        sample_width = wf.getsampwidth()
        frame_rate = wf.getframerate()
        pcm = wf.readframes(wf.getnframes())

    log.info("Loaded %s: %dch %dbit %dHz", path, n_channels, sample_width * 8, frame_rate)

    if n_channels == 2:
        pcm = audioop.tomono(pcm, sample_width, 0.5, 0.5)
    if sample_width != SAMPLE_WIDTH:
        pcm = audioop.lin2lin(pcm, sample_width, SAMPLE_WIDTH)
        sample_width = SAMPLE_WIDTH
    if frame_rate != TARGET_RATE:
        pcm, _ = audioop.ratecv(pcm, sample_width, 1, frame_rate, TARGET_RATE, None)
    mulaw = audioop.lin2ulaw(pcm, sample_width)

    samples_per_frame = TARGET_RATE * FRAME_MS // 1000  # 160 mu-law bytes / 20ms
    frames = [mulaw[i : i + samples_per_frame] for i in range(0, len(mulaw), samples_per_frame)]
    log.info("Prepared %d frames (~%.1fs)", len(frames), len(frames) * FRAME_MS / 1000)
    return frames


async def receive_loop(ws):
    """Log outbound messages (media / mark / clear) from the server."""
    out_frames = 0
    async for raw in ws:
        try:
            msg = json.loads(raw)
        except json.JSONDecodeError:
            continue
        event = msg.get("event")
        if event == "media":
            out_frames += 1
            if out_frames % 50 == 0:
                log.info("<- received %d outbound media frames", out_frames)
        elif event == "mark":
            log.info("<- MARK: %s", msg.get("mark", {}).get("name"))
        elif event == "clear":
            log.info("<- CLEAR (flush queued audio)")


async def send_loop(ws, frames: list[bytes], stream_sid: str):
    """Send start, stream frames at real-time pace, then send stop."""
    await ws.send(json.dumps({
        "event": "start",
        "start": {"streamSid": stream_sid,
                  "mediaFormat": {"encoding": "audio/x-mulaw", "sampleRate": TARGET_RATE}},
    }))
    log.info("-> START sent (stream=%s)", stream_sid)

    frame_interval = FRAME_MS / 1000.0
    start_t = time.monotonic()
    for i, frame in enumerate(frames):
        await ws.send(json.dumps({
            "event": "media",
            "media": {"timestamp": str(i * FRAME_MS),
                      "payload": base64.b64encode(frame).decode("ascii")},
        }))
        # Pace against an absolute schedule so the stream doesn't drift.
        next_t = start_t + (i + 1) * frame_interval
        sleep_for = next_t - time.monotonic()
        if sleep_for > 0:
            await asyncio.sleep(sleep_for)

    await ws.send(json.dumps({"event": "stop"}))
    log.info("-> STOP sent (%d frames)", len(frames))


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("wav", help="Path to a .wav file to stream")
    parser.add_argument("--uri", default=SERVER_URI)
    parser.add_argument("--sid", default="SIM_STREAM_001")
    parser.add_argument("--keepalive", type=float, default=3.0,
                        help="Seconds to keep listening after stop.")
    args = parser.parse_args()

    frames = load_wav_as_mulaw_frames(args.wav)
    async with websockets.connect(args.uri) as ws:
        log.info("Connected to %s", args.uri)
        receiver = asyncio.create_task(receive_loop(ws))
        await send_loop(ws, frames, args.sid)
        try:
            await asyncio.sleep(args.keepalive)
        finally:
            receiver.cancel()
    log.info("Simulator done.")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(0)