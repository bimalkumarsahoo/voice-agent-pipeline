"""
Phase 4: endpointing / turn-taking.

Decides when the caller has FINISHED their turn so the agent can reply.
Approach: energy-based silence detection on the inbound PCM stream.

The decision, and its tradeoff (the assessment asks us to state this):
  We declare the turn over after SILENCE_HANGOVER_MS of continuous silence
  *following* detected speech.
    - too short -> agent interrupts on a mid-sentence breath (false endpoint)
    - too long  -> agent feels sluggish, inflates perceived latency
  500-800ms is the usual conversational sweet spot; it's a UX dial, not a
  correct-value problem, so it's configurable.

Two robustness refinements over naive "energy < threshold for N ms":
  1. Require speech to have STARTED before silence can end a turn -- otherwise
     the leading silence before the caller speaks would trigger instantly.
  2. Count consecutive silent frames; any genuine speech frame resets the
     counter -- so a turn only ends on real sustained silence.

Limitation (README): the energy threshold is fixed. Production systems estimate
an adaptive noise floor (or use a trained VAD like webrtcvad / Silero) so the
threshold tracks line noise. A fixed RMS threshold is the honest take-home scope.
"""

from __future__ import annotations

from dataclasses import dataclass

from voice_agent.utils import audio


@dataclass
class EndpointConfig:
    # RMS energy below this = "silence". 16-bit PCM RMS ranges ~0..32767.
    silence_rms_threshold: int = 500
    # Continuous silence after speech that ends the turn.
    silence_hangover_ms: int = 600
    # Minimum speech before a turn can end (guards against blips counting as a turn).
    min_speech_ms: int = 200
    # Frame size the caller stream arrives in (post-decode, model rate).
    frame_ms: int = 20


class Endpointer:
    """Feed it decoded PCM frames; it tells you when the caller's turn ends.

    Usage per frame:
        if endpointer.update(pcm_frame):   # returns True exactly once per turn
            # caller finished -> trigger the agent reply
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
        """Process one PCM frame. Returns True the moment the turn ends."""
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
            # Only accumulate silence once the caller has actually spoken.
            if self._speech_started:
                self._silence_ms += self.cfg.frame_ms

        # Turn ends: speech happened, then enough continuous silence followed.
        if self._speech_started and self._silence_ms >= self.cfg.silence_hangover_ms:
            self._fired = True
            return True
        return False

    def new_turn(self) -> None:
        """Reset for the next turn (call after the agent finishes replying)."""
        self._reset()