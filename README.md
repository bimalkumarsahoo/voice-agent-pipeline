# Real-Time Voice Agent Pipeline

A Python service that runs a real-time, two-way voice conversation over a
streaming audio connection. Caller audio streams in over a WebSocket, runs
through a speech-to-text → LLM → text-to-speech loop, and synthesized audio
streams back — fast enough to feel like a phone call, and able to stop the
instant the caller interrupts.

The telephony provider (Twilio/Vonage) is simulated; the service speaks a
faithful subset of the Twilio Media Streams protocol, so a real number could be
wired in without changing the service.

> **Look at first:** `src/voice_agent/pipeline.py` (orchestration + barge-in) and
> `DESIGN.md` (architecture + sequence diagrams).

---

## Run it (fresh machine)

Requires Python 3.12 (the audio code uses the stdlib `audioop`, removed in 3.13).

```bash
# 1. install deps
uv python pin 3.12
uv add "websockets>=12.0" numpy sounddevice
uv add --dev pytest

# 2. start the server (terminal 1)
uv run python -m voice_agent.server

# 3a. deterministic driver — stream a WAV (terminal 2)
uv run python simulator/stream_wav.py samples/tone.wav

# 3b. OR interactive — talk via mic/speaker (terminal 2). USE HEADPHONES.
uv run python simulator/mock_twilio.py
```

`mock_twilio` needs PortAudio (bundled with the `sounddevice` wheel on
macOS/Windows; `sudo apt install libportaudio2` on Linux).

Run the tests:

```bash
uv run pytest -q
```

---

## What works

- **Multi-turn conversation** — caller speaks, service transcribes, replies, and
  the loop continues across turns within one call.
- **Turn-taking / endpointing** — energy-based silence detection decides when the
  caller has finished.
- **Barge-in** — if the caller speaks while the agent is talking, the agent stops
  immediately: in-flight generation is cancelled, queued outbound audio is
  flushed, and a `clear` is sent.
- **Streaming, not batch** — LLM output is chunked into sentences and streamed
  into TTS, so the caller hears the first words while the rest is still being
  generated.
- **Latency instrumentation** — per-turn, per-stage timings including
  time-to-first-audio.
- **Audio plumbing** — µ-law/8 kHz ↔ 16 kHz PCM conversion with stateful
  streaming resamplers and bounded buffering.

STT / LLM / TTS are mocks behind interfaces (`stages.py`); they stream output
with realistic incremental delays so streaming and latency behave as they would
with real providers. Swapping in a real provider is a one-line change in
`build_pipeline()`.

---

## Key design decisions & tradeoffs

- The server.py implements the two methods - *on_audio, on_stop*. The server 
  owns the transport and the pipeline orchestrates the conversational logic.
  So, at START the server builds the conversation pipeline, then it forwards the
  inbound (MEDIA) frames to the pipeline and the stop event, nothing else. So the
  pipeline implementation can be swapped independently.

- The conversation reply runs as a `asyncio.Task` rather than being awaited inline
  so that the inbound audio is processed during the reply. This helps us implement
  the barge-in feature. Once there is a barge-in, the task is canceled.

- VAD Endpointer - detection of silence after speech. A caller turn ends after 
  ~600 ms of continuous silence. This is a decision we need to look into as if the 
  too short, the agent might interrupt mid-sentence and too long means it feels slow.

- Barge-in threshold - An interruption requires ~200ms of continuous audio. This
  eliminates clicks, coughs and external short noises. Separate inbound and 
  outbound channel prevents the barge in triggering on agents voice.

- First audio streaming - LLM response is accumulated till a sentence boundary. 
  Then the sentence is synthesised immediately while the LLM generates successive
  responses. Then passed on to the TTS -> send_audio. This helps reduce the time-to-first-audio, 
  thus implementing LLM response as stream instead of batches.

- Outbound audio buffer - So, the outbound audio flows though a bounded queue of 400 frames (~ 8s buffer).
  This implementation drains the oldest audio frame in case the
  consumer falls behind prioritizing the newest frames. The key idea is that stale audio
  is less valuable in case of lag as the caller cant be slowed. During barge-in, the stale audio
  is flushed using the clear call.

---

## Latency (one turn, mock stages)

```
TURN TIMING | endpoint→STT: 175ms | STT→1st-token: 251ms |
              1st-token→1st-audio: 252ms | TIME-TO-FIRST-AUDIO: 677ms |
              full-reply: 9397ms
```

**Time-to-first-audio is ~677 ms** from the moment the caller stops speaking. The
gap between full-reply (~9.4 s) and time-to-first-audio (~677 ms) is the streaming
payoff: the caller hears sentence one while later sentences are still being
synthesized.

How it's measured: a `TurnTimer` records monotonic timestamps at endpoint,
STT-done, first-token, first-audio, and reply-done; the per-stage deltas are
logged once per turn. Numbers reflect the mock stage delays (tunable in
`stages.py`), so they characterise the architecture's timing, not real-model
speed. Separately, the ~600 ms endpoint hangover is a VAD that sits
before the endpoint mark so the perceived caller latency = *hangover +
time-to-first-audio*.

---

## Concurrency / scale

One asyncio event loop, one coroutine per call, isolated per-call state. Many
I/O-bound calls share the loop because every slow stage is awaited and the LLM/TTS
mocks are async generators that yield between chunks. Nothing on the server side
blocks the loop.

**Where it breaks first:** in-process CPU-heavy work — e.g. running a real STT/TTS
model locally — would block the single loop thread and stall every call. The fix
is to offload heavy stages to a worker pool or separate
services; the current design keeps the loop responsive by delegating heavy work to
external STT/TTS.

---

## Known limitations

- **Barge-in onset clipping.** The ~200 ms detection window consumes the first
  word or two of an interrupting utterance before the audio is sent to STT. 
  - Fix: We can implement a pre-roll buffer which can cover the lost audio.
- **mark event ordering.** For simplicity, `agent_turn_complete` is sent directly rather than through
  the outbound queue, so it could reach the wire just before the last queued
  audio frames. 
  - Fix: Route the events in a single audio queue for the specific call.
- **Real providers.** STT/LLM/TTS are faithful mocks; wiring real streaming
  providers is a one-line swap per stage.

---

## Layout

```
src/voice_agent/
  protocol.py     WebSocket message parse/build
  server.py       WS server, OutboundSender (queued + flushable)
  pipeline.py     orchestration: turns, streaming reply, barge-in
  stages.py       STT/LLM/TTS interfaces + mocks (mock delays marked here)
  vad.py          endpointing (turn end)
  barge_in.py     barge-in detection (turn start during agent speech)
  tts_real.py     optional pyttsx3 stage for audible demo (not default)
  utils/
    audio.py      µ-law/PCM, stateful resampling, reframing, energy
    timing.py     per-turn latency marks
simulator/
  stream_wav.py   deterministic WAV driver
  mock_twilio.py  interactive mic/speaker driver
tests/
  test_audio.py   audio conversion unit tests
DESIGN.md         architecture + sequence diagrams
```

Time spent: roughly a focused day. Deliberately skipped: real provider
integrations, the stretch goals (answering-machine detection, tool calls,
reconnect, metrics dashboard), and broad test coverage. Mainly focused on the
real-time pipeline, barge-in, and streaming.

Video links:
Short demo: https://drive.google.com/file/d/1PnJnxHgBr6Czdn5IlIvjOodiPBkGd6J5/view?usp=sharing
Full video: https://drive.google.com/file/d/1NeGA7IHtI55EUnnKRgwoZD00f6XV73MY/view?usp=sharing
