"""
Phase 5: barge-in detection.

Watches the INBOUND stream while the agent is speaking. If the caller produces
sustained speech (not a blip), it signals an interruption so the pipeline can
cancel the in-flight reply, flush queued audio, and send `clear`.

Why "sustained" (the anti-self-trigger guard):
  Triggering on a single loud frame would false-fire on a click, a cough, line
  noise, or -- in a shared-acoustic setup -- the agent's own voice bleeding into
  the mic. Requiring N consecutive voiced frames (~200ms) means only a real
  caller interruption trips it.

Why this is separate from the Endpointer:
  The Endpointer answers "has the caller FINISHED talking?" (silence after
  speech). The BargeInDetector answers "has the caller STARTED talking while the
  agent is mid-reply?" (onset of speech). Opposite edges, different jobs.
"""

from __future__ import annotations

from dataclasses import dataclass

from voice_agent.utils import audio


@dataclass
class BargeInConfig:
    # Energy at/above this counts as voiced (same scale as Endpointer).
    speech_rms_threshold: int = 500
    # Consecutive voiced ms required to declare a real interruption.
    trigger_ms: int = 200
    frame_ms: int = 20


class BargeInDetector:
    """Feed inbound PCM frames *while the agent is speaking*. Returns True once
    when sustained caller speech indicates a genuine barge-in."""

    def __init__(self, config: BargeInConfig | None = None):
        self.cfg = config or BargeInConfig()
        self._voiced_ms = 0
        self._fired = False

    def update(self, pcm_frame: bytes) -> bool:
        if self._fired:
            return False
        energy = audio.frame_energy(pcm_frame)
        if energy >= self.cfg.speech_rms_threshold:
            self._voiced_ms += self.cfg.frame_ms
        else:
            self._voiced_ms = 0  # must be CONTINUOUS speech; reset on any gap
        if self._voiced_ms >= self.cfg.trigger_ms:
            self._fired = True
            return True
        return False

    def reset(self) -> None:
        self._voiced_ms = 0
        self._fired = False