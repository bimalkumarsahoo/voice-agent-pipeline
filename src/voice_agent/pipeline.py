"""
Phase 3: the conversational pipeline -- replaces EchoResponder.

Orchestrates: caller audio -> STT -> LLM (streamed) -> sentence-chunking ->
TTS (streamed) -> OutboundEncoder -> 20ms mu-law frames -> caller.

The streaming property (assessment requirement 4): we do NOT wait for the full
LLM reply. We accumulate streamed tokens until we have a complete sentence, then
fire it into TTS immediately and stream that audio out -- so the caller hears
sentence 1 while the LLM is still producing sentence 2.

This class exposes the SAME interface EchoResponder did:
    on_audio(frame)        -- inbound 20ms mu-law frame (caller speaking)
    on_caller_stopped()    -- endpoint detected: caller's turn ended, agent replies
so swapping it into the server is a one-line change.

NB: turn-taking (when on_caller_stopped fires) is Phase 4. Cancellation /
barge-in (interrupting a reply in progress) is Phase 5. Here we build the clean
forward path and structure the reply as a single cancellable task so Phase 5 can
cancel it without restructuring.
"""

from __future__ import annotations

import asyncio
import logging
import re

from voice_agent.utils import audio
from voice_agent.stages import STT, LLM, TTS

log = logging.getLogger("voice_agent.pipeline")

# A sentence boundary: ., ?, ! optionally followed by space. Good enough to
# chunk an LLM stream into speakable units without waiting for the whole reply.
_SENTENCE_END = re.compile(r"[.?!]+")


class ConversationPipeline:
    def __init__(self, sender, stt: STT, llm: LLM, tts: TTS):
        self._sender = sender          # OutboundSender (send_audio / send_mark)
        self._stt = stt
        self._llm = llm
        self._tts = tts

        # Inbound audio for the current turn, decoded to 16kHz PCM for STT.
        self._inbound = audio.InboundDecoder()
        self._turn_pcm = bytearray()

        # Reuse one outbound encoder + reframer for the whole call (state-continuous).
        self._encoder = audio.OutboundEncoder()
        self._reframer = audio.Reframer(audio.TELEPHONY_FRAME_BYTES)

        # Handle to the in-flight reply task (Phase 5 will cancel this).
        self._reply_task: asyncio.Task | None = None

    # --- inbound -----------------------------------------------------------

    async def on_audio(self, mulaw_frame: bytes) -> None:
        """Caller audio frame: decode to PCM and accumulate for this turn."""
        pcm = self._inbound.decode(mulaw_frame)
        self._turn_pcm.extend(pcm)

    async def on_caller_stopped(self) -> None:
        """Endpoint: the caller finished. Run STT -> LLM -> TTS for the reply.

        Structured as one cancellable task so Phase 5 barge-in can cancel it.
        """
        turn = bytes(self._turn_pcm)
        self._turn_pcm.clear()
        if not turn:
            return
        self._reply_task = asyncio.create_task(self._run_reply(turn))
        await self._reply_task

    # --- the reply path ----------------------------------------------------

    async def _run_reply(self, turn_pcm: bytes) -> None:
        try:
            transcript = await self._stt.transcribe(turn_pcm)
            log.info("STT: %r", transcript)

            sentence = ""
            async for token in self._llm.generate(transcript):
                sentence += token
                # When we hit a sentence boundary, ship that sentence to TTS now.
                if _SENTENCE_END.search(token):
                    await self._speak(sentence.strip())
                    sentence = ""
            # Flush any trailing partial sentence.
            if sentence.strip():
                await self._speak(sentence.strip())

            # Mark end of the agent's turn so the client knows playback is done.
            await self._sender.send_mark("agent_turn_complete")
            log.info("Reply complete")
        except asyncio.CancelledError:
            # Phase 5: barge-in cancels us here. Re-raise after any cleanup.
            log.info("Reply cancelled (barge-in)")
            raise

    async def _speak(self, sentence: str) -> None:
        """Stream one sentence through TTS -> encode -> 20ms frames -> out."""
        if not sentence:
            return
        log.info("TTS <- %r", sentence)
        async for pcm_chunk in self._tts.synthesize(sentence):
            mulaw = self._encoder.encode(pcm_chunk)
            for frame in self._reframer.push(mulaw):
                await self._sender.send_audio(frame)
        # Push any remainder as a final (sub-20ms) frame for this sentence.
        rem = self._reframer.flush()
        if rem:
            await self._sender.send_audio(rem)