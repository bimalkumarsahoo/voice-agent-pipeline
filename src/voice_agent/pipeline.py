"""
Conversational pipeline: orchestrates a full-duplex voice turn.

Flow per turn:
    caller audio -> endpointing -> STT -> LLM (streamed) -> sentence chunking
    -> TTS (streamed) -> encode to mu-law -> 20ms frames -> caller.

Full-duplex: the reply runs as a background task (not awaited inline) so inbound
frames keep being processed during the reply and the caller can interrupt.

Barge-in: while the agent speaks, inbound frames feed a BargeInDetector; on
sustained caller speech the reply is cancelled, queued outbound audio is flushed,
and a clear event is sent. The agent stops immediately.

Inbound interface:
    on_audio(frame)  -- one inbound 20ms mu-law frame
    on_stop()        -- the call ended
"""

from __future__ import annotations

import asyncio
import logging
import re

from voice_agent.utils import audio
from voice_agent.utils.timing import TurnTimer
from voice_agent.stages import STT, LLM, TTS
from voice_agent.vad import Endpointer, EndpointConfig
from voice_agent.barge_in import BargeInDetector, BargeInConfig

log = logging.getLogger("voice_agent.pipeline")

_SENTENCE_END = re.compile(r"[.?!]+")  # sentence boundary for chunking the LLM stream


class ConversationPipeline:
    """Drives one call: turn detection, streamed reply, and barge-in.

    sender:          OutboundSender (send_audio / send_mark / clear).
    stt / llm / tts: pipeline stage implementations.
    endpoint_config / bargein_config: optional detector tuning.
    """

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
        self._turn_pcm = bytearray()         # caller audio accumulated for the current turn

        self._encoder = audio.OutboundEncoder()
        self._reframer = audio.Reframer(audio.TELEPHONY_FRAME_BYTES)

        self._reply_task: asyncio.Task | None = None  # in-flight reply (cancellable)
        self._agent_speaking = False                  # routes inbound frames (turn vs barge-in)
        self._timer: TurnTimer | None = None

    # --- inbound -----------------------------------------------------------

    async def on_audio(self, mulaw_frame: bytes) -> None:
        """Decode one inbound frame and route it based on agent state."""
        pcm = self._inbound.decode(mulaw_frame)

        if self._agent_speaking:
            # Agent is talking: only watch for the caller interrupting.
            if self._bargein.update(pcm):
                await self._handle_barge_in()
                self._turn_pcm.extend(pcm)  # interrupting speech begins the next turn
            return

        # Agent idle: accumulate the turn and check whether it has ended.
        self._turn_pcm.extend(pcm)
        if self._endpointer.update(pcm):
            self._start_reply()  # fire-and-forget; not awaited (full-duplex)

    async def on_stop(self) -> None:
        """Call ended: cancel any in-flight reply and finish."""
        if self._reply_task is not None and not self._reply_task.done():
            self._reply_task.cancel()
            try:
                await self._reply_task
            except asyncio.CancelledError:
                pass

    # --- barge-in ----------------------------------------------------------

    async def _handle_barge_in(self) -> None:
        """Cancel the in-flight reply, flush queued audio, send clear."""
        log.info("BARGE-IN detected -> cancelling reply, flushing, clearing")
        if self._reply_task is not None and not self._reply_task.done():
            self._reply_task.cancel()
            try:
                await self._reply_task
            except asyncio.CancelledError:
                pass
        await self._sender.clear()          # drop queued frames + send clear event
        self._agent_speaking = False
        self._bargein.reset()
        self._endpointer.new_turn()

    # --- reply -------------------------------------------------------------

    def _start_reply(self) -> None:
        """Snapshot the turn audio and launch the reply as a background task."""
        turn = bytes(self._turn_pcm)
        self._turn_pcm.clear()
        if not turn:
            self._endpointer.new_turn()
            return
        self._bargein.reset()
        self._timer = TurnTimer()
        self._timer.mark("endpoint")        # t=0 for time-to-first-audio
        self._reply_task = asyncio.create_task(self._run_reply(turn))

    async def _run_reply(self, turn_pcm: bytes) -> None:
        """STT -> streamed LLM -> sentence-chunked TTS. Cancellable for barge-in."""
        try:
            self._agent_speaking = True
            transcript = await self._stt.transcribe(turn_pcm)
            if self._timer:
                self._timer.mark("stt_done")
            log.info("STT: %r", transcript)

            sentence = ""
            async for token in self._llm.generate(transcript):
                if self._timer:
                    self._timer.mark("first_token")
                sentence += token
                if _SENTENCE_END.search(token):   # speak each sentence as soon as it completes
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
            # Cleared on every exit path so the next turn isn't misread.
            self._agent_speaking = False
            self._endpointer.new_turn()

    async def _speak(self, sentence: str) -> None:
        """Stream one sentence through TTS -> encode -> 20ms frames -> sender."""
        if not sentence:
            return
        log.info("TTS <- %r", sentence)
        async for pcm_chunk in self._tts.synthesize(sentence):
            mulaw = self._encoder.encode(pcm_chunk)
            for frame in self._reframer.push(mulaw):
                if self._timer:
                    self._timer.mark("first_audio")  # first frame out = time-to-first-audio
                await self._sender.send_audio(frame)
        rem = self._reframer.flush()
        if rem:
            await self._sender.send_audio(rem)