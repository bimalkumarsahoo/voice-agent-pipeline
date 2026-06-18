# Real-Time Voice Agent Pipeline — Design Reference

Architecture, module responsibilities, data flow, and event sequence diagrams
(normal turn + barge-in) for the voice-agent service. ASCII diagrams render
anywhere with no tooling.

---

## 1. System Overview

The service is the brain behind a phone-based voice assistant. A telephony
provider (Twilio/Vonage, or the bundled mock) terminates the real call and
bridges audio over a WebSocket using a Media Streams-style protocol. The
service runs the caller's audio through STT -> LLM -> TTS and streams
synthesized audio back, fast enough to feel like a phone call and able to stop
the instant the caller interrupts.

```
            ┌──────────────────────────────────────────────────────────┐
            │                  TELEPHONY SIDE (client)                   │
            │   real Twilio  /  mock_twilio (mic+speaker)  /  WAV sim    │
            │                                                            │
            │   caller mic  ──► 8kHz µ-law 20ms frames ──┐               │
            │   caller ear  ◄── 8kHz µ-law frames ───────┼──┐            │
            └────────────────────────────────────────────┼──┼───────────┘
                                                          │  │
                                            WebSocket (Media Streams subset)
                                                          │  │
            ┌─────────────────────────────────────────────▼──┴───────────┐
            │                   OUR SERVICE (server)                      │
            │                                                             │
            │   server.py ── handle_connection (one coroutine per call)   │
            │        │                                                    │
            │        ▼                                                    │
            │   ConversationPipeline  ── orchestrates the whole turn      │
            │        │                                                    │
            │   ┌────┴─────┬──────────┬──────────┬──────────┐            │
            │   ▼          ▼          ▼          ▼          ▼            │
            │ Endpointer BargeIn   STT/LLM/TTS  Audio    TurnTimer        │
            │ (vad.py)  (barge_in) (stages.py) (utils/   (utils/          │
            │                                   audio.py) timing.py)      │
            │                                                             │
            │   OutboundSender ── queued, flushable audio back to caller  │
            └─────────────────────────────────────────────────────────────┘
```

---

## 2. Module Responsibilities

| Module                  | Responsibility                                                        |
|-------------------------|-----------------------------------------------------------------------|
| `protocol.py`           | Wire format: parse inbound start/media/stop; build media/mark/clear.   |
| `server.py`             | WS server; one coroutine per call; `OutboundSender` (queued + clear).  |
| `pipeline.py`           | Orchestrator: turn logic, streaming reply, barge-in, full-duplex.      |
| `vad.py`                | `Endpointer` — detects caller turn END (silence after speech).         |
| `barge_in.py`           | `BargeInDetector` — detects caller turn START during agent speech.     |
| `stages.py`             | STT/LLM/TTS interfaces + faithful streaming mocks.                     |
| `utils/audio.py`        | µ-law↔PCM, stateful resampling, reframing, energy.                     |
| `utils/timing.py`       | `TurnTimer` — per-stage latency marks + report.                        |
| `simulator/*`           | mock_twilio (mic/speaker) + stream_wav (file) — fake telephony clients.|

---

## 3. The Protocol (Media Streams subset)

```
INBOUND  (telephony -> us)            OUTBOUND (us -> telephony)
─────────────────────────             ──────────────────────────
{event:"start", start:{               {event:"media",
   streamSid, mediaFormat}}              streamSid, media:{payload}}  // audio out
{event:"media", media:{               {event:"mark",
   timestamp, payload}}   // µ-law       streamSid, mark:{name}}      // checkpoint
{event:"stop"}                        {event:"clear", streamSid}      // FLUSH (barge-in)
```

`clear` is the barge-in primitive: it tells the far side to discard audio it has
buffered but not yet played.

---

## 4. Audio Format Boundary

Two format worlds meet in this service; `utils/audio.py` owns the conversion.

```
   WIRE (telephony)                         MODEL (STT/TTS)
   8 kHz, mono, µ-law          <──────►     16 kHz, mono, 16-bit PCM
   1 byte/sample                            2 bytes/sample
   160 bytes / 20ms                         640 bytes / 20ms

   inbound:  µ-law ─► ulaw2lin ─► ratecv(8k→16k) ─► PCM   (InboundDecoder)
   outbound: PCM   ─► ratecv(16k→8k) ─► lin2ulaw ─► µ-law (OutboundEncoder)

   NB: ratecv is STATEFUL — one resampler per stream/direction, state threaded
       across frames, or you get clicks at every 20ms boundary.
```

---

## 5. High-Level Data Flow (one turn)

```
  caller speaks                              caller hears reply
       │                                            ▲
       ▼                                            │
  ┌─────────┐   µ-law    ┌──────────────┐   µ-law   ┌──────────────┐
  │ inbound │  frames    │  on_audio()  │  frames   │OutboundSender│
  │  media  │ ─────────► │  (pipeline)  │ ◄──────── │ queue+drain  │
  └─────────┘            └──────┬───────┘           └──────▲───────┘
                                │ decode µ-law→PCM          │ µ-law frames
                                ▼                           │
                         ┌─────────────┐                   │
                         │ Endpointer  │  turn end?         │
                         │ (silence    │ ───┐               │
                         │  after      │    │ yes           │
                         │  speech)    │    ▼               │
                         └─────────────┘  _start_reply()    │
                                            │               │
                                            ▼               │
                  ┌──────────────────────────────────────┐  │
                  │            _run_reply (task)          │  │
                  │                                       │  │
                  │  STT(turn_pcm) ─► transcript          │  │
                  │     │                                 │  │
                  │     ▼                                 │  │
                  │  LLM.generate() ─► token stream       │  │
                  │     │  accumulate into sentences      │  │
                  │     ▼  (on . ? !)                     │  │
                  │  _speak(sentence):                    │  │
                  │     TTS.synthesize() ─► PCM chunks     │  │
                  │       ─► OutboundEncoder (PCM→µ-law)   │──┘
                  │       ─► Reframer (clean 20ms)         │
                  │       ─► sender.send_audio()           │
                  └───────────────────────────────────────┘

  STREAMING: sentence 1's audio flows out while the LLM is still generating
  sentence 2. Time-to-first-audio is driven by the first sentence, not the whole
  reply.
```

---

## 6. Full-Duplex Routing in on_audio()

Every inbound frame is routed by whether the agent is currently speaking. This
is the heart of barge-in.

```
                      ┌──────────────────────────┐
   inbound µ-law ───► │   on_audio(frame)        │
                      │   decode µ-law → PCM      │
                      └────────────┬─────────────┘
                                   │
                     _agent_speaking ?
                      ┌────────────┴────────────┐
                  YES │                         │ NO
                      ▼                         ▼
            ┌──────────────────┐      ┌──────────────────────┐
            │ BargeInDetector  │      │ accumulate _turn_pcm  │
            │ .update(pcm)     │      │ Endpointer.update(pcm)│
            │ sustained speech?│      │ turn ended?           │
            └────────┬─────────┘      └──────────┬───────────┘
                 YES │ (≥200ms)              YES │ (silence ≥600ms)
                     ▼                           ▼
            _handle_barge_in()            _start_reply()
            • cancel reply task           • snapshot+clear turn buf
            • sender.clear()  (flush+      • new TurnTimer, mark endpoint
              send clear event)           • create_task(_run_reply)
            • reset detectors                (fire-and-forget; NOT awaited)
            • start new turn buffer
```

Key point: we do NOT await the reply task. It runs in the background so on_audio
keeps consuming inbound frames and can detect an interruption. Half-duplex
(awaiting the reply) literally cannot barge-in.

---

## 7. SEQUENCE DIAGRAM — Normal Call (no interruption)

```
Caller/Telephony      server.py            pipeline            Endpointer      stages(STT/LLM/TTS)   OutboundSender
      │                  │                     │                   │                  │                  │
      │ ws connect       │                     │                   │                  │                  │
      ├─────────────────►│                     │                   │                  │                  │
      │ {start}          │                     │                   │                  │                  │
      ├─────────────────►│ build pipeline      │                   │                  │                  │
      │                  ├────────────────────►│ create            │                  │                  │
      │                  │                     ├──────────────────►│ (ready)          │                  │
      │ {media} x N      │                     │                   │                  │                  │
      │ (caller talking) │                     │                   │                  │                  │
      ├─────────────────►│ on_audio(frame)     │                   │                  │                  │
      │                  ├────────────────────►│ decode, accumulate│                  │                  │
      │                  │                     ├──────────────────►│ update(pcm)      │                  │
      │                  │                     │                   │ speech... no end │                  │
      │       ... repeats for each 20ms frame while caller speaks ...                 │                  │
      │ {media} silence  │                     │                   │                  │                  │
      ├─────────────────►│ on_audio(frame)     ├──────────────────►│ update(pcm)      │                  │
      │                  │                     │                   │ silence ≥600ms   │                  │
      │                  │                     │                   │ ──► TURN END     │                  │
      │                  │                     │◄──────────────────┤ returns True     │                  │
      │                  │                     │ _start_reply()    │                  │                  │
      │                  │                     │ TurnTimer.mark(endpoint)             │                  │
      │                  │                     │ create_task(_run_reply)  [async]     │                  │
      │                  │                     │                   │                  │                  │
      │                  │                     │ STT.transcribe(turn_pcm) ───────────►│                  │
      │                  │                     │◄─────────────────────── transcript ──┤  mark(stt_done)  │
      │                  │                     │ LLM.generate(transcript) ───────────►│                  │
      │                  │                     │◄──── token ──── token ──── token ────┤  mark(first_token)│
      │                  │                     │ accumulate → sentence (on . ? !)     │                  │
      │                  │                     │ _speak(sentence):                    │                  │
      │                  │                     │ TTS.synthesize(sentence) ───────────►│                  │
      │                  │                     │◄──── pcm chunk ──── pcm chunk ───────┤                  │
      │                  │                     │ encode µ-law, reframe 20ms           │                  │
      │                  │                     │ send_audio(frame) ──────────────────────────────────────►│ mark(first_audio)
      │                  │                     │                   │                  │      enqueue ────►│ drain→ws
      │ {media} out ◄────┼─────────────────────┼───────────────────────────────────────────────────────  │ (audio to caller)
      │ (caller HEARS    │                     │  ... streams sentence by sentence ...                    │
      │  sentence 1 while │                    │                   │                  │                  │
      │  LLM makes #2)   │                     │                   │                  │                  │
      │                  │                     │ send_mark("agent_turn_complete") ───────────────────────►│
      │ {mark} ◄─────────┼─────────────────────┤ TurnTimer.mark(reply_done); log_report()                 │
      │                  │                     │ Endpointer.new_turn()  ──► ready for caller's next turn   │
      │                  │                     │                   │                  │                  │
      │  (caller can now speak again → next turn, same flow)       │                  │                  │
      │ {stop}           │                     │                   │                  │                  │
      ├─────────────────►│ on_stop()           │                   │                  │                  │
      │                  ├────────────────────►│ cancel any in-flight reply, close    │                  │
      │                  │ sender.aclose()     │                   │                  │                  │
```

---

## 8. SEQUENCE DIAGRAM — Barge-In (caller interrupts mid-reply)

```
Caller/Telephony      server.py            pipeline           BargeInDetector   stages         OutboundSender
      │                  │                     │                   │              │                │
      │  (agent is mid-reply: _agent_speaking == True, reply task running,        │                │
      │   audio frames streaming OUT to caller)                                   │                │
      │ {media} out ◄────┼─────────────────────┼──────────────────────────────────────────────────┤ (agent talking)
      │                  │                     │                   │              │                │
      │ CALLER STARTS    │                     │                   │              │                │
      │ TALKING again    │                     │                   │              │                │
      │ {media} speech   │                     │                   │              │                │
      ├─────────────────►│ on_audio(frame)     │                   │              │                │
      │                  ├────────────────────►│ decode → PCM      │              │                │
      │                  │                     │ _agent_speaking?  │              │                │
      │                  │                     │   YES ──► route to barge-in      │                │
      │                  │                     ├──────────────────►│ update(pcm)  │                │
      │                  │                     │                   │ voiced 20ms  │                │
      │       ... a few more inbound speech frames ...             │ voiced 40ms  │                │
      │ {media} speech   │                     ├──────────────────►│ ...          │                │
      ├─────────────────►│ on_audio(frame)     │                   │ voiced ≥200ms│                │
      │                  │                     │                   │ ──► BARGE-IN │                │
      │                  │                     │◄──────────────────┤ returns True │                │
      │                  │                     │                   │              │                │
      │                  │                     │ _handle_barge_in():              │                │
      │                  │                     │                   │              │                │
      │                  │                     │ 1. reply_task.cancel() ─────────►│ CancelledError │
      │                  │                     │    (LLM/TTS stop immediately)    │ propagates     │
      │                  │                     │    await task → swallow Cancelled │                │
      │                  │                     │                   │              │                │
      │                  │                     │ 2+3. sender.clear() ────────────────────────────►│
      │                  │                     │      • drop all queued µ-law frames               │ (flush queue)
      │                  │                     │      • send {clear} event                         │
      │ {clear} ◄────────┼─────────────────────┼───────────────────────────────────────────────────┤
      │ (far side flushes│                     │                   │              │                │
      │  buffered audio; │                     │ _agent_speaking = False           │                │
      │  agent goes      │                     │ bargein.reset()   │              │                │
      │  SILENT at once) │                     │ endpointer.new_turn()             │                │
      │                  │                     │ _turn_pcm.extend(interrupt pcm) ──► caller's new turn begins
      │                  │                     │                   │              │                │
      │  NO more agent audio frames are sent. Agent did NOT finish its sentence.   │                │
      │                  │                     │                   │              │                │
      │  caller continues speaking → endpointing → next reply (normal flow)        │                │
```

### Barge-in trigger conditions (exact)

```
Barge-in fires WHEN ALL of:
  • _agent_speaking == True        (a reply is in progress)
  • inbound frame energy ≥ speech_rms_threshold (default 500)  ... AND
  • that holds for ≥ trigger_ms continuous (default 200ms = ~10 frames)

Barge-in does NOT fire on:
  • a single loud frame (click/cough)      → needs 200ms sustained
  • silence/noise during agent speech       → below energy threshold
  • the agent's own voice                    → separate in/out channels +
                                               sustained gate (anti-self-trigger)
```

### What happens on barge-in

```
  1. CANCEL in-flight generation/synthesis  → reply_task.cancel()
  2. FLUSH already-queued outbound audio     → sender drops queued frames
  3. SEND clear                               → {clear} event to far side
  4. Agent does NOT finish its old sentence   → no audio frames after clear
```

---

## 9. Concurrency Model (summary)

```
  • One asyncio event loop, single thread (server side). No locks, no races.
  • One coroutine per call (handle_connection). Per-call isolated state.
  • Reply runs as a background Task (not awaited) → full-duplex.
  • Everything slow is awaited (STT/LLM/TTS, sends) → loop never blocks.
  • async generators (LLM/TTS) yield between tokens/chunks → cooperative,
    other calls keep being served mid-reply.
  • Bounded queues (OutboundSender, mock_twilio play_q): drop oldest under
    pressure — stale audio is worthless, and you can't slow the caller.

  WHERE IT BREAKS FIRST:
    In-process CPU-heavy work (e.g. real STT model inference) would block the
    single loop thread and stall all calls. Fix: offload to run_in_executor /
    a worker pool / a separate STT service. Current design delegates heavy work
    to external STT/TTS, keeping the loop responsive.
```

---

## 10. Latency (measured, mock stages)

```
TURN TIMING | endpoint→STT: 174ms | STT→1st-token: 251ms |
              1st-token→1st-audio: 252ms | TIME-TO-FIRST-AUDIO: 677ms |
              full-reply: 9397ms

  • TTFA 677ms = caller stops → first sound.
  • full-reply 9.4s ≫ TTFA 677ms  ⇒ proves streaming (caller hears sentence 1
    while later sentences are still being synthesized).
  • numbers reflect the mock stage delays → they characterise the architecture's
    timing behaviour, not real-model speed.
  • endpoint hangover (~600ms) sits BEFORE the endpoint mark; perceived caller
    latency = hangover + TTFA.
```

---

## 11. Known Limitations & Future Enhancements

Honest scope boundaries in the current implementation:

**Barge-in onset clipping.** Barge-in fires only after a sustained-speech window
(~200ms). The inbound frames consumed during that detection window are used to
*decide* an interruption occurred, but only the triggering frame is carried into
the next turn's audio — so the first word or two of an interrupting utterance can
be clipped before STT sees it. The clean fix is a short rolling **pre-roll
buffer** (~300ms) maintained even during agent speech, used to seed the next
turn so the onset is recovered. Left as a future enhancement.

**Trailing silence in the STT buffer.** A turn is buffered and sent to STT as one
batch, so the endpoint-hangover silence is included in the input. Real STT
tolerates leading/trailing silence, so this is a minor efficiency cost rather than
a correctness issue; trimming trailing silence using the per-frame energy signal
would tighten it.

**Batch (not streaming) STT.** The turn is accumulated and transcribed in one
call. The production-correct approach is to stream audio continuously to a
streaming STT with internal VAD/endpointing, which removes both the onset
clipping and the buffering. This is the larger architectural direction.

**Mark ordering.** `agent_turn_complete` is sent directly rather than through the
outbound audio queue, so on a busy drain it could reach the wire just ahead of the
last queued audio frames. Routing the mark through the same queue would make it a
strict playback boundary.

**Single-process scale.** One asyncio loop serves many I/O-bound calls well, but
in-process CPU-heavy work (e.g. a real STT/TTS model running locally) would block
the loop. Heavy stages should be offloaded to a worker pool or run as separate
services; horizontal scale-out would put multiple processes behind a load
balancer with sticky per-call routing.