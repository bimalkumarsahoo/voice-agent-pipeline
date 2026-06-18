"""
Audio format helpers: conversion between the telephony wire format and the model
format used by STT/TTS.

    Wire:   8 kHz, mono, mu-law (G.711), 1 byte/sample
    Model:  16 kHz, mono, linear PCM, 16-bit (2 bytes/sample)

Provides one-shot conversion for complete buffers and stateful streaming codecs
for continuous frame-by-frame audio.
"""

from __future__ import annotations

import audioop
from dataclasses import dataclass

TELEPHONY_RATE = 8000
MODEL_RATE = 16000
SAMPLE_WIDTH = 2          # 16-bit linear PCM
CHANNELS = 1
FRAME_MS = 20
TELEPHONY_FRAME_BYTES = TELEPHONY_RATE * FRAME_MS // 1000        # 160 mu-law bytes / 20ms
MODEL_FRAME_BYTES = MODEL_RATE * FRAME_MS // 1000 * SAMPLE_WIDTH  # 640 PCM bytes / 20ms


# --- One-shot conversions (stateless; for complete buffers) ----------------

def mulaw_to_pcm(mulaw: bytes) -> bytes:
    """Convert a complete 8kHz mu-law buffer to 16kHz 16-bit PCM."""
    pcm8 = audioop.ulaw2lin(mulaw, SAMPLE_WIDTH)
    pcm16, _ = audioop.ratecv(pcm8, SAMPLE_WIDTH, CHANNELS, TELEPHONY_RATE, MODEL_RATE, None)
    return pcm16


def pcm_to_mulaw(pcm: bytes) -> bytes:
    """Convert a complete 16kHz 16-bit PCM buffer to 8kHz mu-law."""
    pcm8, _ = audioop.ratecv(pcm, SAMPLE_WIDTH, CHANNELS, MODEL_RATE, TELEPHONY_RATE, None)
    return audioop.lin2ulaw(pcm8, SAMPLE_WIDTH)


# --- Streaming resamplers (stateful; for frame-by-frame) -------------------

class StreamResampler:
    """Resamples a continuous stream frame-by-frame.

    audioop.ratecv carries filter state between calls; this threads that state so
    a stream resampled in chunks matches resampling the whole buffer at once
    (otherwise artifacts appear at every frame boundary). One instance per
    stream/direction.
    """

    def __init__(self, from_rate: int, to_rate: int):
        self._from = from_rate
        self._to = to_rate
        self._state = None  # ratecv state, threaded across calls

    def process(self, pcm: bytes) -> bytes:
        out, self._state = audioop.ratecv(
            pcm, SAMPLE_WIDTH, CHANNELS, self._from, self._to, self._state
        )
        return out


class InboundDecoder:
    """Streaming inbound codec: mu-law 8kHz frames -> 16kHz PCM, artifact-free."""

    def __init__(self):
        self._resampler = StreamResampler(TELEPHONY_RATE, MODEL_RATE)

    def decode(self, mulaw_frame: bytes) -> bytes:
        pcm8 = audioop.ulaw2lin(mulaw_frame, SAMPLE_WIDTH)
        return self._resampler.process(pcm8)


class OutboundEncoder:
    """Streaming outbound codec: 16kHz PCM -> mu-law 8kHz frames, artifact-free."""

    def __init__(self):
        self._resampler = StreamResampler(MODEL_RATE, TELEPHONY_RATE)

    def encode(self, pcm_frame: bytes) -> bytes:
        pcm8 = self._resampler.process(pcm_frame)
        return audioop.lin2ulaw(pcm8, SAMPLE_WIDTH)


# --- Energy / reframing ----------------------------------------------------

def frame_energy(pcm: bytes) -> int:
    """RMS energy of a 16-bit PCM buffer; used for silence/speech detection."""
    if not pcm:
        return 0
    return audioop.rms(pcm, SAMPLE_WIDTH)


@dataclass
class Reframer:
    """Buffers a byte stream and emits fixed-size frames.

    Codec / resampler output rarely lands on clean frame boundaries; this
    accumulates bytes, yields complete frames of `frame_bytes`, and holds the
    remainder for next time.
    """

    frame_bytes: int
    _buf: bytes = b""

    def push(self, data: bytes) -> list[bytes]:
        """Add bytes; return any complete frames now available."""
        self._buf += data
        frames = []
        while len(self._buf) >= self.frame_bytes:
            frames.append(self._buf[: self.frame_bytes])
            self._buf = self._buf[self.frame_bytes :]
        return frames

    def flush(self) -> bytes:
        """Return any partial remainder and clear the buffer."""
        rem, self._buf = self._buf, b""
        return rem