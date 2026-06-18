"""
Endpointing: detects when the caller has finished their turn.

Uses energy-based silence detection on the inbound PCM stream. A turn ends after
a configurable span of continuous silence following detected speech.

Tradeoff on the silence span:
  too short -> the agent interrupts on a mid-sentence breath
  too long  -> the agent feels sluggish and inflates perceived latency
500-800ms is the usual conversational range, so it is configurable.
"""

from __future__ import annotations

from dataclasses import dataclass

from voice_agent.utils import audio


@dataclass
class EndpointConfig:
    silence_rms_threshold: int = 500   # RMS below this counts as silence
    silence_hangover_ms: int = 600     # continuous silence after speech that ends a turn
    min_speech_ms: int = 200           # minimum speech before a turn can end
    frame_ms: int = 20


class Endpointer:
    """Consumes decoded PCM frames and reports when the caller's turn ends.

    update(frame) returns True exactly once, on the frame where the turn ends.
    """

    def __init__(self, config: EndpointConfig | None = None):
        self.cfg = config or EndpointConfig()
        self._reset()

    def _reset(self) -> None:
        self._speech_ms = 0
        self._silence_ms = 0
        self._speech_started = False
        self._fired = False

    def update(self, pcm_frame: bytes) -> bool:
        """Process one PCM frame; return True the moment the turn ends."""
        if self._fired:
            return False

        energy = audio.frame_energy(pcm_frame)
        is_speech = energy >= self.cfg.silence_rms_threshold

        if is_speech:
            self._speech_ms += self.cfg.frame_ms
            self._silence_ms = 0
            if self._speech_ms >= self.cfg.min_speech_ms:
                self._speech_started = True
        else:
            # Only count silence once the caller has actually started speaking,
            # otherwise leading silence would end the turn immediately.
            if self._speech_started:
                self._silence_ms += self.cfg.frame_ms

        if self._speech_started and self._silence_ms >= self.cfg.silence_hangover_ms:
            self._fired = True
            return True
        return False

    def new_turn(self) -> None:
        """Reset state for the next turn."""
        self._reset()