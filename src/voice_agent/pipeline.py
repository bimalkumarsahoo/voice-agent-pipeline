"""
Phase 5: full-duplex conversational pipeline with barge-in.

caller audio -> endpointer (turn end) -> STT -> LLM (streamed) -> sentence
chunking -> TTS (streamed) -> encode -> 20ms mu-law -> caller.

Phase 5 changes (vs Phase 4):
  * FULL-DUPLEX: we no longer `await` the reply inline. The reply runs as a
    background task while on_audio keeps processing inbound frames -- so the
    caller can interrupt mid-reply.
  * BARGE-IN: while the agent is speaking, inbound frames feed a BargeInDetector.
    On sustained caller speech we (1) cancel the reply task, (2) flush queued
    outbound audio, (3) send `clear` to the telephony side. The agent stops
    immediately and does not finish its old sentence.
  * Anti-self-trigger: barge-in requires SUSTAINED speech (not one frame), and
    inbound/outbound are separate streams, so the agent never barges in on
    itself. (Real shared-acoustic deployments would add echo cancellation.)

Seam unchanged: on_audio(frame), on_stop().
"""

from __future__ import annotations

import asyncio
import logging
import re

from voice_agent.utils import audio
from voice_agent.stages import STT, LLM, TTS
from voice_agent.vad import Endpointer, EndpointConfig
from voice_agent.barge_in import BargeInDetector, BargeInConfig
from voice_agent.utils.timing import TurnTimer

log = logging.getLogger("voice_agent.pipeline")

_SENTENCE_END = re.compile(r"[.?!]+")


class ConversationPipeline:
    def __init__(self, sender, stt: STT, llm: LLM, tts: TTS,
                 endpoint_config: EndpointConfig | None = None,
                 bargein_config: BargeInConfig | None = None):
        self._sender = sender
        self._stt = stt
        self._llm = llm
        self._tts = tts

        self._inbound = audio.InboundDecoder()
        self._endpointer = Endpointer(endpoint_config)
        self._bargein = BargeInDetector(bargein_config)
        self._turn_pcm = bytearray()

        self._encoder = audio.OutboundEncoder()
        self._reframer = audio.Reframer(audio.TELEPHONY_FRAME_BYTES)

        self._reply_task: asyncio.Task | None = None
        self._agent_speaking = False
        self._timer: TurnTimer | None = None

    # --- inbound: now full-duplex ------------------------------------------

    async def on_audio(self, mulaw_frame: bytes) -> None:
        pcm = self._inbound.decode(mulaw_frame)

        if self._agent_speaking:
            # Agent is talking: watch ONLY for a barge-in (caller interrupting).
            if self._bargein.update(pcm):
                await self._handle_barge_in()
                # The interrupting speech is the START of the caller's next turn;
                # begin accumulating it so we don't lose the first words.
                self._turn_pcm.extend(pcm)
            return

        # Agent idle: normal turn accumulation + endpointing.
        self._turn_pcm.extend(pcm)
        if self._endpointer.update(pcm):
            self._start_reply()  # fire-and-forget; do NOT await (full-duplex)

    async def on_stop(self) -> None:
        """Call ended. Cancel any in-flight reply and stop cleanly."""
        if self._reply_task is not None and not self._reply_task.done():
            self._reply_task.cancel()
            try:
                await self._reply_task
            except asyncio.CancelledError:
                pass

    # --- barge-in handling -------------------------------------------------

    async def _handle_barge_in(self) -> None:
        log.info("BARGE-IN detected -> cancelling reply, flushing, clearing")
        # 1. Cancel in-flight generation/synthesis.
        if self._reply_task is not None and not self._reply_task.done():
            self._reply_task.cancel()
            try:
                await self._reply_task
            except asyncio.CancelledError:
                pass
        # 2 + 3. Flush queued outbound audio and tell the far side to drop its
        #        buffered audio (the `clear` event).
        await self._sender.clear()
        # _agent_speaking is cleared by _run_reply's finally; ensure it here too.
        self._agent_speaking = False
        # Reset detectors for the caller's new turn.
        self._bargein.reset()
        self._endpointer.new_turn()

    # --- reply trigger (fire-and-forget) -----------------------------------

    def _start_reply(self) -> None:
        turn = bytes(self._turn_pcm)
        self._turn_pcm.clear()
        if not turn:
            self._endpointer.new_turn()
            return
        self._bargein.reset()
        self._timer = TurnTimer()
        self._timer.mark("endpoint")
        self._reply_task = asyncio.create_task(self._run_reply(turn))

    async def _run_reply(self, turn_pcm: bytes) -> None:
        try:
            self._agent_speaking = True
            transcript = await self._stt.transcribe(turn_pcm)
            if self._timer: self._timer.mark("stt_done")
            log.info("STT: %r", transcript)

            sentence = ""
            async for token in self._llm.generate(transcript):
                if self._timer: self._timer.mark("first_token")
                sentence += token
                if _SENTENCE_END.search(token):
                    await self._speak(sentence.strip())
                    sentence = ""
            if sentence.strip():
                await self._speak(sentence.strip())

            await self._sender.send_mark("agent_turn_complete")
            if self._timer:
                self._timer.mark("reply_done")
                self._timer.log_report()
            log.info("Reply complete")
        except asyncio.CancelledError:
            log.info("Reply cancelled (barge-in)")
            raise
        finally:
            self._agent_speaking = False
            self._endpointer.new_turn()

    async def _speak(self, sentence: str) -> None:
        if not sentence:
            return
        log.info("TTS <- %r", sentence)
        async for pcm_chunk in self._tts.synthesize(sentence):
            mulaw = self._encoder.encode(pcm_chunk)
            for frame in self._reframer.push(mulaw):
                if self._timer: self._timer.mark("first_audio")
                await self._sender.send_audio(frame)
        rem = self._reframer.flush()
        if rem:
            await self._sender.send_audio(rem)