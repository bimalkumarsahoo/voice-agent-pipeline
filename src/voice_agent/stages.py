"""
Phase 3: pipeline stage interfaces + faithful mock implementations.

Each stage (STT, LLM, TTS) is defined behind an abstract interface so a real
provider (Deepgram, OpenAI, ElevenLabs, ...) drops in by implementing the same
methods -- a one-line swap in the pipeline wiring.

The mocks honour the assessment rule: "fake the latency honestly." They sleep
with realistic per-stage / per-token / per-chunk delays and STREAM their output
(async generators), never return instantly. This is what makes the latency
numbers and the streaming behaviour meaningful.

Formats (see utils.audio):
    STT input:  16kHz 16-bit PCM (model format)
    TTS output: 16kHz 16-bit PCM (model format) -> pipeline encodes to mu-law
"""

from __future__ import annotations

import abc
import asyncio
import math
import struct
from typing import AsyncIterator

from voice_agent.utils import audio


# --- Interfaces ------------------------------------------------------------

class STT(abc.ABC):
    @abc.abstractmethod
    async def transcribe(self, pcm: bytes) -> str:
        """Turn a buffer of 16kHz PCM (one caller turn) into text."""


class LLM(abc.ABC):
    @abc.abstractmethod
    def generate(self, prompt: str) -> AsyncIterator[str]:
        """Stream the reply as text chunks (tokens/words) over time."""


class TTS(abc.ABC):
    @abc.abstractmethod
    def synthesize(self, text: str) -> AsyncIterator[bytes]:
        """Stream synthesized audio as 16kHz PCM chunks over time."""


# --- Mock STT --------------------------------------------------------------

class MockSTT(STT):
    """Pretends to recognise speech. Latency scales a little with audio length,
    as a real model's would. Returns a canned transcript (content isn't graded).
    """

    def __init__(self, base_latency_s: float = 0.15):
        self._base = base_latency_s

    async def transcribe(self, pcm: bytes) -> str:
        seconds_of_audio = len(pcm) / audio.SAMPLE_WIDTH / audio.MODEL_RATE
        # A little processing time proportional to audio, plus a fixed cost.
        await asyncio.sleep(self._base + 0.02 * seconds_of_audio)
        # Canned: we don't grade content. Pretend the caller asked something.
        return "I'd like to book an appointment for next week."


# --- Mock LLM --------------------------------------------------------------

class MockLLM(LLM):
    """Streams a canned reply token-by-token with realistic inter-token delay,
    so downstream sentence-streaming into TTS can be exercised honestly.
    """

    def __init__(self, per_token_s: float = 0.04, first_token_s: float = 0.25):
        self._per_token = per_token_s
        self._first_token = first_token_s  # time-to-first-token (model "thinking")

    async def generate(self, prompt: str) -> AsyncIterator[str]:
        reply = (
            "Sure, I can help with that. "
            "We have openings on Tuesday and Thursday afternoon. "
            "Which day works better for you?"
        )
        tokens = reply.split(" ")
        first = True
        for tok in tokens:
            await asyncio.sleep(self._first_token if first else self._per_token)
            first = False
            # Yield the token with a trailing space, like a real tokenizer stream.
            yield tok + " "


# --- Mock TTS --------------------------------------------------------------

class MockTTS(TTS):
    """Synthesizes audio for a text chunk, streamed as 16kHz PCM sub-chunks with
    realistic delay. Produces an actual tone burst so there's real audio to hear
    and to measure bytes on (content/quality isn't graded).
    """

    def __init__(self, per_chunk_s: float = 0.05, chunk_ms: int = 40):
        self._per_chunk = per_chunk_s
        self._chunk_ms = chunk_ms

    async def synthesize(self, text: str) -> AsyncIterator[bytes]:
        # Roughly 60ms of audio per character, a loose stand-in for speech rate.
        total_ms = max(self._chunk_ms, len(text) * 60)
        chunks = total_ms // self._chunk_ms
        for i in range(chunks):
            await asyncio.sleep(self._per_chunk)
            yield self._tone_pcm(self._chunk_ms, freq=200 + (i % 5) * 40)

    @staticmethod
    def _tone_pcm(ms: int, freq: int) -> bytes:
        n = audio.MODEL_RATE * ms // 1000
        return b"".join(
            struct.pack("<h", int(6000 * math.sin(2 * math.pi * freq * i / audio.MODEL_RATE)))
            for i in range(n)
        )