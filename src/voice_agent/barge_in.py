"""
Barge-in detection: detects the caller starting to speak while the agent is
mid-reply, so the reply can be interrupted.

Requires sustained speech (not a single frame) before signalling an
interruption. This avoids false triggers on clicks, coughs, line noise, or the
agent's own audio bleeding into the input.

Distinct from the Endpointer: the Endpointer detects the END of a caller turn
(silence after speech); this detects the START of one (onset of speech during
agent output).
"""

from __future__ import annotations

from dataclasses import dataclass

from voice_agent.utils import audio


@dataclass
class BargeInConfig:
    speech_rms_threshold: int = 500   # RMS at/above this counts as voiced
    trigger_ms: int = 200             # continuous voiced span that signals barge-in
    frame_ms: int = 20


class BargeInDetector:
    """Consumes inbound PCM frames while the agent is speaking; reports a genuine
    interruption once sustained caller speech is seen."""

    def __init__(self, config: BargeInConfig | None = None):
        self.cfg = config or BargeInConfig()
        self._voiced_ms = 0
        self._fired = False

    def update(self, pcm_frame: bytes) -> bool:
        """Process one PCM frame; return True once on sustained caller speech."""
        if self._fired:
            return False
        energy = audio.frame_energy(pcm_frame)
        if energy >= self.cfg.speech_rms_threshold:
            self._voiced_ms += self.cfg.frame_ms
        else:
            self._voiced_ms = 0  # speech must be continuous; any gap resets
        if self._voiced_ms >= self.cfg.trigger_ms:
            self._fired = True
            return True
        return False

    def reset(self) -> None:
        """Clear state (call when the agent starts a new reply)."""
        self._voiced_ms = 0
        self._fired = False