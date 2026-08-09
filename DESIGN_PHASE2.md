# Voice Agent — Phase 2 POC Design

**Goal of phase 2:** take the working real-time voice pipeline (phase 1) and turn it into a
containerized, multi-service system with **real AI models** and **RAG-grounded answers** —
while staying buildable by one person and demo-able with a single `docker compose up`.

**Explicit non-goals (deferred to phase 3):** GPU self-hosting, autoscaling, Kubernetes,
speculative retrieval, metrics dashboards, data-residency/privacy hardening. This is a POC that
*demonstrates the productionization path*, not a production deployment.

---

## 1. Design principle

Every split must buy **independent scaling** or **independent failure isolation**, and be worth
the **latency** it costs. The end-to-end budget is ~800 ms per turn; each network hop competes
with it. So: split only where it pays, and spend complexity almost entirely on **barge-in**,
which is the hardest and most distinctive real-time feature.

---

## 2. Architecture (split-by-concern)

Four containers. Orchestration stays coherent in one service; only inference and retrieval —
the two things that genuinely scale differently — are pulled out.

```
                    ┌──────────────────────────────┐
   Twilio sim  ───► │  media-orchestrator           │  (phase-1 server.py + pipeline.py)
   (WebSocket)      │  • WS transport                │
                    │  • VAD endpoint / barge-in     │  ◄── SINGLE cancellation authority
                    │  • turn state, outbound queue  │
                    └───────┬──────────────┬─────────┘
                     gRPC   │              │  HTTP
                  (streaming)│              │ (req/resp)
              ┌─────────────▼──┐    ┌───────▼──────────┐
              │ inference-gw    │    │ retrieval-svc     │
              │ • STT stream    │    │ • embed query     │
              │ • LLM stream    │◄───┤ • vector search   │
              │ • TTS stream    │    │ • return top-k    │
              └────┬────┬────┬──┘    └───────┬──────────┘
                   │    │    │               │
              Deepgram  │  Cartesia    ┌─────▼──────┐
                   Claude Haiku        │ vector-db   │ (pgvector)
                                       └─────────────┘
```

**Services**
| Service | Responsibility | Scales on |
|---|---|---|
| `media-orchestrator` | WS transport, turn state, VAD/endpoint, **barge-in authority**, outbound queue | concurrent calls |
| `inference-gateway` | fan-out to STT / LLM / TTS providers, sentence chunking, streaming | inference load |
| `retrieval-svc` | embed query, vector search, return top-k chunks | corpus size / QPS |
| `vector-db` | pgvector store | data volume |

---

## 3. Decisions, with trade-offs and the "why"

### 3.1 Decomposition — **split-by-concern** ✅

| | Split-by-stage (rejected) | Split-by-concern (chosen) |
|---|---|---|
| **+** | granular per-stage scaling, textbook microservices | fewer hot-path hops, single barge-in authority, retrieval isolated |
| **−** | barge-in cancellation scattered across 3 boundaries; 3 hops/turn | gateway is fatter, coarser scaling |

**Why:** for a POC, the barge-in coherence matters far more than scaling granularity we won't
exercise. Keeping "stop talking NOW" inside one service is the deciding factor.

### 3.2 Transport — **gRPC on streaming path, HTTP on retrieval** ✅

| | gRPC everywhere (rejected) | Mixed (chosen) |
|---|---|---|
| **+** | uniform | right tool per path: gRPC streaming where cancellation matters, HTTP where it's one-shot |
| **−** | proto overhead on a path that doesn't need it | two transports to reason about |

**Why:** orchestrator↔gateway is bidirectional streaming and cancellation-sensitive → gRPC
earns its keep (and you already know protobuf). Retrieval is a single request/response → gRPC
adds codegen for no benefit; plain HTTP/JSON is simpler and debuggable with `curl`.

### 3.3 RAG timing — **blocking retrieve-then-generate** ✅

| | Blocking (chosen) | Speculative on partial (rejected) |
|---|---|---|
| **+** | simple, correct, easy to reason about | hides 50–150 ms of retrieval latency |
| **−** | adds ~50–150 ms before first token | retrieves on incomplete queries, dedup, wasted calls |

**Why:** "make it work, then make it fast." The 50–150 ms is not yet proven to hurt. Speculation
is a phase-3 optimization once tracing shows retrieval is on the critical path.

### 3.4 Vector DB — **pgvector** ✅

**Why:** $0, one container, SQL you already know, comfortable to ~1M vectors — a POC will not
exceed that. Qdrant's better filtering/latency solves a scale problem that doesn't exist yet.
Migration later is a swap behind `retrieval-svc`, not a rewrite.

### 3.5 LLM serving — **managed (Claude Haiku)** ✅

**Why:** one-line streaming call, no GPU, no batching/autoscaling. Self-hosted vLLM/TGI is a
phase-3 project in itself. **Known limitation:** this defers the data-privacy/PII story — stated
here deliberately so it isn't a surprise later.

### 3.6 Local orchestration — **Docker Compose** ✅

**Why:** one `up` brings all four services + DB live; trivial to demo. K8s shows the prod story
but raises setup cost without making the POC demonstrate more. Structure services cleanly so a
later K8s migration is just manifests.

### 3.7 Observability — **OpenTelemetry tracing** ✅

**Why:** the thing you'll actually need is to see *where the ms go across four services* on a slow
turn — that's distributed tracing, extending your existing `TurnTimer` marks into spans.
Prometheus/Grafana dashboards are for ongoing ops you don't have yet (phase 3).

### 3.8 Two cheap conventions adopted now (scale-forward, zero behavior change) ✅

These cost almost nothing in the POC and prevent an expensive retrofit later. Both are adopted
now because they touch **message contracts** or **every call site** — the things that get
exponentially harder to change once code is built on top of them.

- **`callSid` on every message.** A stable per-call identifier threaded through every gRPC/HTTP
  message. In the POC it powers distributed tracing; at scale it is what pins a call to one
  orchestrator pod (see §9). One field now, painful to retrofit into every proto later.
- **Endpoints + timeouts in config, never hardcoded.** Read Compose DNS names and per-hop
  timeouts from env/config. This makes the eventual separate-servers move a *config* change,
  not a *code* change.

> Note: a per-turn `turnId` was considered and **deferred** — see §10. With a call pinned to one
> pod and in-process cancellation, it is not needed for correctness now.

---

## 4. The critical design point — barge-in across services

In phase 1, barge-in is an in-process `task.cancel()`. Once TTS lives in another container,
"stop talking NOW" becomes a **network** signal. The rule:

- The **outbound queue and the `clear`/flush stay in `media-orchestrator`** — never in the gateway.
- On barge-in the orchestrator (a) **cancels the in-flight LLM+TTS gRPC streams** (gRPC cancellation
  propagates cleanly) and (b) **flushes its own outbound queue locally**.

This guarantees the flush is **always local and instant**, even if the gateway is mid-generation.
This single decision is *why* we chose split-by-concern over split-by-stage.

---

## 5. Per-turn flow (happy path)

1. Caller audio streams into `media-orchestrator` over WebSocket (µ-law/8 kHz).
2. VAD detects end-of-turn (~600 ms silence hangover).
3. Orchestrator opens a gRPC stream to `inference-gateway`; audio → **STT**.
4. On **final transcript**, gateway calls `retrieval-svc` over HTTP → top-k chunks.
5. Chunks injected into the **LLM** system prompt; generation streams back token-by-token.
6. Gateway chunks tokens at sentence boundaries → **TTS** → audio frames stream to orchestrator.
7. Orchestrator queues outbound audio (bounded ~400 frames) and sends to the caller.
8. **If caller speaks during step 5–7:** barge-in fires → orchestrator cancels gRPC streams +
   flushes queue + sends `clear`.

---

## 5a. Error handling (bounded recovery)

Every failure keeps the caller-facing audio path recoverable, because the orchestrator owns the
outbound queue and `clear`. Three cases:

1. **Barge-in (caller interrupts):** ~200 ms detection → orchestrator cancels the in-flight
   gRPC streams and flushes its queue locally. The flush is instant regardless of gateway state.
2. **Retrieval fails / times out:** `retrieval-svc` is given a **~150 ms timeout budget**. On
   timeout, the gateway **falls back to generating without retrieved context** rather than
   stalling the turn. Retrieval never blocks the caller-facing path.
3. **Provider error (STT/LLM/TTS 5xx or stream drop):** **retry once** (fast), and if it still
   fails, play a **spoken fallback** ("one moment — could you say that again?"). The turn degrades
   gracefully instead of going silent.

---

## 6. Container inventory

| Container | Base | Key deps | Ports |
|---|---|---|---|
| `media-orchestrator` | python:3.12-slim | websockets, grpcio, opentelemetry | WS in; gRPC out |
| `inference-gateway` | python:3.12-slim | grpcio, provider SDKs (Deepgram/Anthropic/Cartesia), httpx | gRPC in; HTTP out |
| `retrieval-svc` | python:3.12-slim | fastapi/uvicorn, sentence-transformers, psycopg | HTTP |
| `vector-db` | pgvector/pgvector:pg16 | — | 5432 |

> Python pinned to **3.12** (phase-1 audio code uses stdlib `audioop`, removed in 3.13).

---

## 7. Chosen phase-2 stack (summary)

**4 containers · split-by-concern · gRPC (streaming) + HTTP (retrieval) · blocking RAG ·
pgvector · managed Claude Haiku · Docker Compose · OpenTelemetry tracing.**

Everything above is picked as the **simplest option that still demonstrates the productionization
story**, with complexity spent deliberately — and only — on barge-in.

---

## 8. Open questions to resolve before building

1. **gRPC proto contracts** — exact `service`/`message` definitions for the streaming path.
2. **Barge-in cancellation protocol** — precise ordering of cancel / flush / `clear` across the wire.
3. **RAG chunking + embedding model** — chunk size, top-k, which embedder in `retrieval-svc`.
4. **Prompt-cache strategy** — cache system prompt + stable chunks to blunt per-turn RAG token cost.

Suggested next dive: **(2) barge-in protocol** (the hard, distinctive part) or **(1) proto contracts**
(the foundation everything else builds on).

---

## 9. Path to scale (planning only — not built in the POC)

The POC runs on one Compose host where every hop is ~1–3 ms loopback. This section records what
changes when services span real nodes, so the POC doesn't paint us into a corner. Nothing here is
built now; the core architecture already holds because split-by-concern and orchestrator-owns-the-
queue were the scale-right calls.

**Chosen scaling approach: call sharding with sticky routing.** The orchestrator stays the
stateful unit — turn state, outbound queue, and barge-in context live in one pod for the call's
life. A session-aware L7 router pins each `callSid` to one orchestrator replica; gateway and
retrieval scale as **stateless** pools behind normal load balancing. Barge-in stays an in-process
`task.cancel()` inside the pinned pod — no cross-pod cancellation race.

*Rejected alternative — externalize call state to Redis:* enables any replica to handle any frame
and survives pod death, but round-tripping high-rate audio frames through an external store fights
real-time latency. Wrong trade for voice. The `callSid` convention keeps this door open if it's
ever needed.

**Foreseen bottlenecks (in priority order):**
1. **Stateful call affinity** — can't round-robin a caller's frames; sticky routing solves it.
2. **Hot-path latency** — gRPC hops go ~1–3 ms → ~10–50 ms cross-node; two hops/turn add
   ~40–100 ms. Budget absorbs it, but it's no longer free.
3. **Provider rate limits** — Deepgram/Cartesia/Anthropic account concurrency caps bite before
   own-compute does; one shared key across N replicas hits account-level limits.
4. **pgvector contention** — concurrent embed + search is CPU-heavy; the retrieval tier bottlenecks
   first. Migrate to Qdrant or a read-replica setup when it does.

**Separate-servers hardening checklist (additive, not a redesign):**
service discovery (replaces Compose DNS) · per-hop timeouts / retries / circuit breakers ·
gRPC channel pooling + keep-alive · mTLS between services (PII/healthcare) · explicit backpressure
(gRPC flow control) · health / readiness endpoints + graceful drain · trace-context propagated
across every hop.

**Deferred deliberately** (trade away POC latency/simplicity for problems we don't have yet):
external state store, K8s / service mesh, per-provider gateway split. The `callSid` groundwork
keeps the scale doors open cheaply.

---

## 10. Future improvements (noted, not scheduled)

- **Per-turn `turnId` for intra-pod stale-frame rejection.** A local correctness detail, *not* a
  contract-level change. On a barge-in, a late audio/TTS chunk from the cancelled turn N can, in
  principle, leak into turn N+1's outbound stream (same class as the phase-1 README limitation
  about `agent_turn_complete` ordering). Stamping outbound frames with a `turnId` lets the
  orchestrator drop anything not matching the current turn. Reach for this **only if the
  stale-frame symptom is actually observed** — it's in-pod frame hygiene, not a message-contract
  requirement, and pinning + in-process cancel removes the distributed version of the race entirely.
- **Speculative retrieval** on partial transcripts (hide the ~50–150 ms) — phase 3, once tracing
  proves retrieval is on the critical path.
- **Self-hosted inference** (vLLM/TGI + local STT/TTS) for the data-privacy/PII story — phase 3.
- **Prometheus + Grafana dashboards** on top of the OTel traces — when there's ongoing ops to watch.
