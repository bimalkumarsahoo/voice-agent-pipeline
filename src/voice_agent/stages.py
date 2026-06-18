"""
Pipeline stage interfaces (STT / LLM / TTS) and mock implementations.

Each stage is defined behind an abstract interface so a real provider
(Deepgram, OpenAI, ElevenLabs, ...) can drop in by implementing the same
methods. The mocks stream their output with incremental delays so streaming
and latency behaviour can be exercised without external services.

    STT input:  16kHz 16-bit PCM
    TTS output: 16kHz 16-bit PCM (the pipeline encodes it to mu-law)

============================================================================
MOCK LATENCY VALUES (tune here to model different provider speeds)
----------------------------------------------------------------------------
  MockSTT  base_latency_s   = 0.15   fixed transcription cost
           + 0.02 * seconds_of_audio (scales mildly with turn length)
  MockLLM  first_token_s    = 0.25   time-to-first-token
           per_token_s      = 0.04   delay between subsequent tokens
  MockTTS  per_chunk_s      = 0.05   delay before each audio chunk
           chunk_ms         = 40     audio duration per chunk
============================================================================
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
        """Transcribe a buffer of 16kHz PCM (one caller turn) into text."""


class LLM(abc.ABC):
    @abc.abstractmethod
    def generate(self, prompt: str) -> AsyncIterator[str]:
        """Stream the reply as text chunks (tokens) over time."""


class TTS(abc.ABC):
    @abc.abstractmethod
    def synthesize(self, text: str) -> AsyncIterator[bytes]:
        """Stream synthesized audio as 16kHz PCM chunks over time."""


# --- Mock STT --------------------------------------------------------------

class MockSTT(STT):
    """Returns a canned transcript after a latency that scales mildly with the
    audio length, mimicking a real model's behaviour.

    base_latency_s: fixed processing cost per transcription.
    """

    def __init__(self, base_latency_s: float = 0.15):   # MOCK VALUE
        self._base = base_latency_s

    async def transcribe(self, pcm: bytes) -> str:
        seconds_of_audio = len(pcm) / audio.SAMPLE_WIDTH / audio.MODEL_RATE
        await asyncio.sleep(self._base + 0.02 * seconds_of_audio)  # MOCK VALUE: 0.02/s
        return "I'd like to book an appointment for next week."


# --- Mock LLM --------------------------------------------------------------

class MockLLM(LLM):
    """Streams a canned reply token-by-token with realistic inter-token delay.

    per_token_s:   delay between tokens after the first.
    first_token_s: initial delay before the first token (model "thinking").
    """

    def __init__(self, per_token_s: float = 0.04, first_token_s: float = 0.25):  # MOCK VALUES
        self._per_token = per_token_s
        self._first_token = first_token_s

    async def generate(self, prompt: str) -> AsyncIterator[str]:
        reply = (
            "Sure, I can help with that. "
            "We have openings on Tuesday and Thursday afternoon. "
            "Which day works better for you?"
        )
        first = True
        for tok in reply.split(" "):
            await asyncio.sleep(self._first_token if first else self._per_token)
            first = False
            yield tok + " "   # trailing space mimics a tokenizer stream


# --- Mock TTS --------------------------------------------------------------

class MockTTS(TTS):
    """Synthesizes a tone burst for a text chunk, streamed as 16kHz PCM
    sub-chunks with realistic delay (audio content is a placeholder tone).

    per_chunk_s: delay before emitting each audio sub-chunk.
    chunk_ms:    audio duration of each sub-chunk.
    """

    def __init__(self, per_chunk_s: float = 0.05, chunk_ms: int = 40):  # MOCK VALUES
        self._per_chunk = per_chunk_s
        self._chunk_ms = chunk_ms

    async def synthesize(self, text: str) -> AsyncIterator[bytes]:
        total_ms = max(self._chunk_ms, len(text) * 60)  # ~60ms of audio per char
        for i in range(total_ms // self._chunk_ms):
            await asyncio.sleep(self._per_chunk)
            yield self._tone_pcm(self._chunk_ms, freq=200 + (i % 5) * 40)

    @staticmethod
    def _tone_pcm(ms: int, freq: int) -> bytes:
        n = audio.MODEL_RATE * ms // 1000
        return b"".join(
            struct.pack("<h", int(6000 * math.sin(2 * math.pi * freq * i / audio.MODEL_RATE)))
            for i in range(n)
        )