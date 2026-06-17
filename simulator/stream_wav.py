"""
Phase 1-1: WAV-file simulator (a fake Twilio).

Reads a .wav file, converts it to 8kHz mono mu-law, chunks it into 20ms
frames, and streams them over the WebSocket at REAL-TIME pace -- one frame
every 20ms -- exactly as a telephony provider would. Concurrently listens
for outbound messages from the server (media to "play", marks, and clears)
and logs them.

Why real-time pace matters: the whole assessment is about behaviour under
the constraint that you cannot pause the caller. If we dumped all frames
instantly, we'd never exercise the real-time path. So we sleep 20ms between
frames to mimic a live call.

Run (from project root, with the server already running):
    uv run python simulator/stream_wav.py samples/hello.wav
"""

from __future__ import annotations

import argparse
import asyncio
import audioop
import json
import logging
import sys
import time
import wave

import websockets

logging.basicConfig(level=logging.INFO, format="%(asctime)s SIM %(message)s")
log = logging.getLogger("simulator")

SERVER_URI = "ws://localhost:8080"
FRAME_MS = 20          # 20ms frames, the telephony standard
TARGET_RATE = 8000     # 8kHz, telephony standard
SAMPLE_WIDTH = 2       # 16-bit PCM intermediate before mu-law encode


def load_wav_as_mulaw_frames(path: str) -> list[bytes]:
    """Load a WAV, convert to 8kHz mono mu-law, split into 20ms frames.

    Returns a list of raw mu-law byte chunks, each ~20ms of audio.
    """
    with wave.open(path, "rb") as wf:
        n_channels = wf.getnchannels()
        sample_width = wf.getsampwidth()
        frame_rate = wf.getframerate()
        pcm = wf.readframes(wf.getnframes())

    log.info("Loaded %s: %dch %dbit %dHz", path, n_channels, sample_width * 8, frame_rate)

    # 1. Downmix stereo -> mono if needed.
    if n_channels == 2:
        pcm = audioop.tomono(pcm, sample_width, 0.5, 0.5)

    # 2. Normalise sample width to 16-bit (mu-law encode expects linear PCM).
    if sample_width != SAMPLE_WIDTH:
        pcm = audioop.lin2lin(pcm, sample_width, SAMPLE_WIDTH)
        sample_width = SAMPLE_WIDTH

    # 3. Resample to 8kHz.
    if frame_rate != TARGET_RATE:
        pcm, _ = audioop.ratecv(pcm, sample_width, 1, frame_rate, TARGET_RATE, None)

    # 4. Encode to mu-law (1 byte per sample).
    mulaw = audioop.lin2ulaw(pcm, sample_width)

    # 5. Chunk into 20ms frames. At 8kHz, 20ms = 160 samples = 160 mu-law bytes.
    samples_per_frame = TARGET_RATE * FRAME_MS // 1000  # 160
    frames = [
        mulaw[i : i + samples_per_frame]
        for i in range(0, len(mulaw), samples_per_frame)
    ]
    log.info("Prepared %d frames (~%.1fs of audio)", len(frames), len(frames) * FRAME_MS / 1000)
    return frames


async def receive_loop(ws):
    """Listen for outbound messages from the server and log them.

    This is how we'll verify the agent's replies and, in Phase 5, observe
    barge-in: a 'clear' event means the server told us to flush queued audio.
    """
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
            log.info("<- CLEAR (barge-in: flush queued audio)")


async def send_loop(ws, frames: list[bytes], stream_sid: str):
    """Stream frames at real-time pace, then send stop."""
    # start
    await ws.send(json.dumps({
        "event": "start",
        "start": {
            "streamSid": stream_sid,
            "mediaFormat": {"encoding": "audio/x-mulaw", "sampleRate": TARGET_RATE},
        },
    }))
    log.info("-> START sent (stream=%s)", stream_sid)

    # media frames, paced to real time
    frame_interval = FRAME_MS / 1000.0
    start_t = time.monotonic()
    for i, frame in enumerate(frames):
        import base64
        await ws.send(json.dumps({
            "event": "media",
            "media": {
                "timestamp": str(i * FRAME_MS),
                "payload": base64.b64encode(frame).decode("ascii"),
            },
        }))
        # Pace to real time: sleep until this frame's scheduled send time.
        next_t = start_t + (i + 1) * frame_interval
        sleep_for = next_t - time.monotonic()
        if sleep_for > 0:
            await asyncio.sleep(sleep_for)

    # stop
    await ws.send(json.dumps({"event": "stop"}))
    log.info("-> STOP sent (%d frames streamed)", len(frames))


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("wav", help="Path to a .wav file to stream")
    parser.add_argument("--uri", default=SERVER_URI)
    parser.add_argument("--sid", default="SIM_STREAM_001")
    parser.add_argument(
        "--keepalive", type=float, default=3.0,
        help="Seconds to keep listening after stop, to catch the agent's reply.",
    )
    args = parser.parse_args()

    frames = load_wav_as_mulaw_frames(args.wav)

    async with websockets.connect(args.uri) as ws:
        log.info("Connected to %s", args.uri)
        receiver = asyncio.create_task(receive_loop(ws))
        await send_loop(ws, frames, args.sid)
        # Keep listening briefly so we can observe the server's response.
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