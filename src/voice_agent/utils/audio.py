"""
Phase 2: audio plumbing -- the single boundary between the telephony format
(8kHz mono mu-law, what the wire speaks) and the model format (16kHz linear
PCM, what STT/TTS want).

Everything audio-format-related lives here so the rest of the pipeline never
touches audioop directly. Two worlds:

    TELEPHONY (wire):  8000 Hz, mono, mu-law (G.711), 1 byte/sample
    MODEL (STT/TTS):   16000 Hz, mono, linear PCM, 16-bit (2 bytes/sample)

Why two resampling paths:
    audioop.ratecv is STATEFUL -- it carries filter state between calls. For a
    continuous stream resampled frame-by-frame, you MUST thread that state
    through or you get audible clicks at every frame boundary. So we expose:
      - convert_* one-shot helpers for whole buffers (e.g. a full TTS utterance)
      - StreamResampler for frame-by-frame streaming (e.g. the live inbound feed)
"""

from __future__ import annotations

import audioop
from dataclasses import dataclass

# --- Canonical formats -----------------------------------------------------

TELEPHONY_RATE = 8000
MODEL_RATE = 16000
SAMPLE_WIDTH = 2          # 16-bit linear PCM
CHANNELS = 1
FRAME_MS = 20
# 20ms frame sizes in each world
TELEPHONY_FRAME_BYTES = TELEPHONY_RATE * FRAME_MS // 1000        # 160 mu-law bytes
MODEL_FRAME_BYTES = MODEL_RATE * FRAME_MS // 1000 * SAMPLE_WIDTH  # 640 PCM bytes


# --- One-shot conversions (stateless; for complete buffers) ----------------

def mulaw_to_pcm(mulaw: bytes) -> bytes:
    """8kHz mu-law -> 16kHz 16-bit PCM. Use for a complete buffer."""
    pcm8 = audioop.ulaw2lin(mulaw, SAMPLE_WIDTH)
    pcm16, _ = audioop.ratecv(pcm8, SAMPLE_WIDTH, CHANNELS, TELEPHONY_RATE, MODEL_RATE, None)
    return pcm16


def pcm_to_mulaw(pcm: bytes) -> bytes:
    """16kHz 16-bit PCM -> 8kHz mu-law. Use for a complete buffer."""
    pcm8, _ = audioop.ratecv(pcm, SAMPLE_WIDTH, CHANNELS, MODEL_RATE, TELEPHONY_RATE, None)
    return audioop.lin2ulaw(pcm8, SAMPLE_WIDTH)


# --- Streaming resamplers (stateful; for frame-by-frame) -------------------

class StreamResampler:
    """Stateful resampler for a continuous stream. Create ONE per stream/direction
    and feed frames in order; it threads ratecv state to avoid boundary clicks.
    """

    def __init__(self, from_rate: int, to_rate: int):
        self._from = from_rate
        self._to = to_rate
        self._state = None

    def process(self, pcm: bytes) -> bytes:
        out, self._state = audioop.ratecv(
            pcm, SAMPLE_WIDTH, CHANNELS, self._from, self._to, self._state
        )
        return out


class InboundDecoder:
    """Streaming inbound path: mu-law 8kHz frames -> 16kHz PCM, click-free."""

    def __init__(self):
        self._resampler = StreamResampler(TELEPHONY_RATE, MODEL_RATE)

    def decode(self, mulaw_frame: bytes) -> bytes:
        pcm8 = audioop.ulaw2lin(mulaw_frame, SAMPLE_WIDTH)
        return self._resampler.process(pcm8)


class OutboundEncoder:
    """Streaming outbound path: 16kHz PCM -> mu-law 8kHz frames, click-free."""

    def __init__(self):
        self._resampler = StreamResampler(MODEL_RATE, TELEPHONY_RATE)

    def encode(self, pcm_frame: bytes) -> bytes:
        pcm8 = self._resampler.process(pcm_frame)
        return audioop.lin2ulaw(pcm8, SAMPLE_WIDTH)


# --- WAV / arbitrary-source normalization ----------------------------------

def normalize_to_telephony_mulaw(
        pcm: bytes, src_rate: int, src_width: int, src_channels: int
) -> bytes:
    """Convert arbitrary linear PCM (any rate/width/channels) to 8kHz mono mu-law.

    Used when ingesting a source whose format we don't control (e.g. a WAV
    file a user drops in). Steps: downmix -> 16-bit -> 8kHz -> mu-law.
    Each step is no-op if already in the target form.
    """
    if src_channels == 2:
        pcm = audioop.tomono(pcm, src_width, 0.5, 0.5)
    if src_width != SAMPLE_WIDTH:
        pcm = audioop.lin2lin(pcm, src_width, SAMPLE_WIDTH)
    if src_rate != TELEPHONY_RATE:
        pcm, _ = audioop.ratecv(pcm, SAMPLE_WIDTH, CHANNELS, src_rate, TELEPHONY_RATE, None)
    return audioop.lin2ulaw(pcm, SAMPLE_WIDTH)


def chunk_frames(data: bytes, frame_bytes: int) -> list[bytes]:
    """Split a buffer into fixed-size frames. A trailing short frame is kept."""
    return [data[i : i + frame_bytes] for i in range(0, len(data), frame_bytes)]


# --- Energy / silence measurement (Phase 4 endpointing will use this) ------

def frame_energy(pcm: bytes) -> int:
    """RMS energy of a 16-bit PCM buffer. Used by the VAD for silence detection."""
    if not pcm:
        return 0
    return audioop.rms(pcm, SAMPLE_WIDTH)


# --- Reframing helper ------------------------------------------------------

@dataclass
class Reframer:
    """Buffers a byte stream and emits fixed-size frames.

    STT/TTS chunks and resampler outputs rarely land on clean 20ms boundaries.
    This accumulates bytes and yields complete frames of `frame_bytes`, holding
    the remainder for next time.
    """

    frame_bytes: int
    _buf: bytes = b""

    def push(self, data: bytes) -> list[bytes]:
        self._buf += data
        frames = []
        while len(self._buf) >= self.frame_bytes:
            frames.append(self._buf[: self.frame_bytes])
            self._buf = self._buf[self.frame_bytes :]
        return frames

    def flush(self) -> bytes:
        """Return any partial remainder (pad-and-send at end of utterance)."""
        rem, self._buf = self._buf, b""
        return rem