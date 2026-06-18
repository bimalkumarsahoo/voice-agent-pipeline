"""
Per-turn latency instrumentation.

Records named timestamps during a turn and reports the per-stage breakdown,
including time-to-first-audio (caller stops speaking -> caller hears first sound).

Marks recorded per turn:
    endpoint     caller's turn ended (t=0 for time-to-first-audio)
    stt_done     transcript ready
    first_token  first LLM token arrived
    first_audio  first outbound audio frame handed to the sender
    reply_done   agent finished the whole reply
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
        """Record the first occurrence of a named mark (later repeats ignored)."""
        if name not in self._marks:
            self._marks[name] = time.monotonic()

    def _delta_ms(self, a: str, b: str) -> int | None:
        if a in self._marks and b in self._marks:
            return round((self._marks[b] - self._marks[a]) * 1000)
        return None

    def report(self) -> str:
        """One-line per-stage breakdown anchored at the endpoint mark."""
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