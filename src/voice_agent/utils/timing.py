"""
Phase 6: per-turn latency instrumentation.

Requirement 5 asks for per-stage timings, especially time-to-first-audio-byte
(caller stops speaking -> caller hears first sound). This records named marks
during a turn and reports the breakdown as one clean log line.

Marks recorded per turn:
    endpoint      caller's turn ended (this is t=0 for time-to-first-audio)
    stt_done      transcript ready
    first_token   first LLM token arrived
    first_audio   first outbound audio frame handed to the sender  <-- TTFA
    reply_done    agent finished the whole reply

Honest notes (for the README):
  - Numbers reflect the MOCK stage delays (faked deliberately, per the spec), so
    they characterise the ARCHITECTURE's timing behaviour, not real-model speed.
  - time-to-first-audio measured here is from endpoint (caller stopped). The
    ~600ms endpoint hangover is a separate, configurable turn-taking dial; the
    report shows TTFA from endpoint so the hangover is excluded from these stage
    numbers (the hangover happens before 'endpoint' is marked).
"""

from __future__ import annotations

import logging
import time

log = logging.getLogger("voice_agent.timing")


class TurnTimer:
    """Records monotonic timestamps for named marks within one turn."""

    def __init__(self):
        self._marks: dict[str, float] = {}

    def mark(self, name: str) -> None:
        # Record only the FIRST occurrence of a mark (e.g. first_token/first_audio).
        if name not in self._marks:
            self._marks[name] = time.monotonic()

    def _delta_ms(self, a: str, b: str) -> int | None:
        if a in self._marks and b in self._marks:
            return round((self._marks[b] - self._marks[a]) * 1000)
        return None

    def report(self) -> str:
        """One-line per-stage breakdown anchored at 'endpoint'."""
        def seg(a, b):
            d = self._delta_ms(a, b)
            return f"{d}ms" if d is not None else "n/a"

        ttfa = self._delta_ms("endpoint", "first_audio")
        full = self._delta_ms("endpoint", "reply_done")
        return (
            "TURN TIMING | "
            f"endpoint->STT: {seg('endpoint','stt_done')} | "
            f"STT->1st-token: {seg('stt_done','first_token')} | "
            f"1st-token->1st-audio: {seg('first_token','first_audio')} | "
            f"TIME-TO-FIRST-AUDIO: {ttfa if ttfa is not None else 'n/a'}ms | "
            f"full-reply: {full if full is not None else 'n/a'}ms"
        )

    def log_report(self) -> None:
        log.info(self.report())