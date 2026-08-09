# Phase 2 Contracts — Architecture

Companion to `DESIGN_PHASE2.md`. Covers the two service boundaries and the two contract files:
`converse.proto` (streaming) and `retrieval_api.md` (one-shot).

## The two boundaries, and why they differ

```
  media-orchestrator  ──[ gRPC bidi stream ]──►  inference-gateway  ──[ HTTP POST ]──►  retrieval-svc
       (Boundary 1: converse.proto)                   (Boundary 2: retrieval_api.md)
```

| | Boundary 1 (proto) | Boundary 2 (HTTP) |
|---|---|---|
| Shape | continuous, bidirectional, per turn | one request/response, once per turn |
| Transport | gRPC bidi streaming | HTTP/JSON |
| Why | audio + events interleave; cancellation must be instant | single round-trip; codegen buys nothing |
| Cancel | native gRPC (no message) | n/a — just a ~150ms timeout |

 **use the heavy tool (gRPC streaming) only where continuous flow +
cancellation demand it. A one-shot lookup is plain HTTP.**

---

## Boundary 1 — `converse.proto`

One bidi stream **per turn**. Orchestrator opens it at endpoint, streams audio, gets events back.

**Orchestrator → gateway (`ClientEvent`)**
- `Start` — first message only. Carries `call_sid`, `codec`, `rag_enabled`. The turn's envelope
  header; not forwarded to LLM/TTS.
- `AudioFrame` — bare µ-law frames after Start. `call_sid` not repeated (set once).

**Gateway → orchestrator (`ServerEvent`, a `oneof`)**
- `PartialTranscript` → interim STT
- `FinalTranscript` → STT done; **triggers RAG + LLM**
- `FirstToken` → empty timing marker (LLM started)
- `AudioFrame` → TTS audio streamed back
- `TurnComplete { reason }` → "complete" | "error" | "cancelled"

**Barge-in:** orchestrator cancels the gRPC call natively → gateway's context fires, it stops
providers. No cancel message. The caller stops hearing audio because the orchestrator flushes its
**local** queue (design 4). `TurnComplete.reason = "cancelled"`.

**Timing:** all latency marks use the orchestrator's single monotonic clock (`FirstToken` timed on
receipt, ~2ms hop). No cross-service clock sync — see design discussion.

---

## Boundary 2 — `retrieval_api.md`

`POST /retrieve {call_sid, query, k}` → `{chunks[]}`, ordered by score. Called once per turn on the
final transcript.

**The one hard rule — timeout budget (~150ms):** on timeout/5xx/empty, the gateway generates
**without** context rather than stalling. Retrieval never blocks the turn (design 5a).

---

## One full turn, both boundaries

Caller: *"what are your hours?"*

1. Orchestrator opens gRPC stream → `Start{call_sid, mulaw_8000, rag=true}` + `AudioFrame`s.
2. Gateway STT → `PartialTranscript` × N → `FinalTranscript{"what are your hours?"}`.
3. Gateway → `POST /retrieve` → `chunks:[{"open 9-5..."}]` (within 150ms, else skip).
4. Gateway injects chunks → LLM → `FirstToken{}` → `AudioFrame`s of *"We're open nine to five."*
5. Gateway → `TurnComplete{"complete"}`; stream closes.
6. If caller interrupts at step 4: orchestrator cancels the call + flushes local queue →
   `TurnComplete{"cancelled"}`.

---

## Files
| File | Boundary | Format |
|---|---|---|
| `converse.proto` | orchestrator ↔ gateway | gRPC proto3 |
| `retrieval_api.md` | gateway → retrieval | HTTP/JSON |
