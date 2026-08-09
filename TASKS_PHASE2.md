# Voice Agent — Phase 2 POC Task Plan

Companion to `DESIGN_PHASE2.md`. This is the build order: dependency-sequenced milestones, each
with concrete tasks and a "done when" check. The guiding rule from the design doc — *pick the
simplest thing that demonstrates the productionization story, spend complexity only on barge-in* —
applies to scope decisions here too.

**Conventions adopted from the start (design §3.8):**
- Thread `callSid` through every gRPC/HTTP message.
- All service endpoints + timeouts come from env/config, never hardcoded.
- Python pinned to 3.12 (phase-1 `audioop` dependency).

---

## Milestone 0 — Scaffold & contracts

The foundation everything builds on. Get the skeleton talking before wiring real models.

- Repo/module layout for four services; shared `proto/` and `config/` at the root.
- Define the **gRPC proto contracts** for the streaming path (orchestrator ↔ gateway): audio-in
  stream, transcript/token/audio-out messages, and an explicit `Cancel` message. Include `callSid`.
- Define the **retrieval HTTP contract** (gateway → retrieval): `POST /retrieve {query, k}` →
  `{chunks[]}`.
- `docker-compose.yml` with all four services + `vector-db`, on one network, config via env.
- Health/readiness stubs on each service.

**Done when:** `docker compose up` starts all four containers; a stub message flows
orchestrator → gateway → retrieval → back; `callSid` is present end-to-end in logs.

---

## Milestone 1 — Orchestrator port (phase-1 logic, now a service)

Move the working phase-1 pipeline behind the new service boundary without changing behavior.

- Port `server.py` (WS transport, OutboundSender) and `pipeline.py` (turn state, barge-in) into
  `media-orchestrator`.
- Keep the outbound queue and `clear`/flush **local to the orchestrator** (design §4).
- Replace the in-process stage calls with gRPC calls to the gateway (still mock stages on the
  other side for now).
- Barge-in stays in-process `task.cancel()` + local flush; verify it still fires cleanly across
  the new gRPC boundary.

**Done when:** the phase-1 WAV driver and mock-Twilio driver run against the containerized
orchestrator; barge-in still cuts the agent off; latency marks still log per turn.

---

## Milestone 2 — Real STT + TTS in the gateway

Swap two of the three mock stages for streaming providers. Do STT/TTS before LLM+RAG because they
have no retrieval dependency.

- Implement `inference-gateway` streaming stages behind the phase-1 `stages.py` interfaces.
- **STT:** Deepgram (or AssemblyAI) streaming — partial + final transcripts.
- **TTS:** Cartesia Sonic streaming (sub-100 ms first-audio class).
- Keep sentence-boundary chunking exactly as in phase 1.
- Provider keys + endpoints from config; apply the **retry-once** rule (design §5a).

**Done when:** a real spoken turn transcribes and speaks back through the containers; time-to-
first-audio measured against the phase-1 baseline; a forced provider 5xx triggers retry then
spoken fallback.

---

## Milestone 3 — RAG (retrieval-svc + vector-db)

Add grounded answers. Retrieval fires **once per turn**, on the final transcript, before the LLM
(design §3.3, §5).

- Stand up `vector-db` (pgvector) and a schema for chunks + embeddings.
- Build an **ingestion script**: docs → chunk → embed → store. Pick chunk size + embedder
  (open question #3).
- Implement `retrieval-svc`: embed query → vector search → return top-k.
- Wire the gateway to call retrieval on final transcript, inject top-k into the LLM prompt.
- Apply the **~150 ms retrieval timeout → generate-without-context fallback** (design §5a).

**Done when:** a question answerable only from the ingested corpus is answered correctly; a forced
retrieval timeout falls back to un-grounded generation without stalling the turn.

---

## Milestone 4 — Real LLM + prompt caching

Close the loop with a real streaming model and blunt the per-turn RAG token cost.

- Swap the mock LLM for streaming **Claude Haiku** behind the stage interface.
- Verify sentence-boundary streaming into TTS still holds (first-audio payoff preserved).
- Implement **prompt caching** for the system prompt + stable RAG chunks (open question #4).
- Confirm barge-in cancels a real in-flight LLM+TTS stream cleanly.

**Done when:** full STT → RAG → LLM → TTS runs end-to-end with real providers; barge-in mid-
generation cuts off cleanly; cached-prefix token savings visible in logs.

---

## Milestone 5 — Observability (OpenTelemetry tracing)

Make a slow turn debuggable across all four services (design §3.7).

- Add OTel spans extending the phase-1 `TurnTimer` marks (endpoint, STT, retrieval, first-token,
  first-audio, reply-done).
- Propagate trace context across gRPC metadata and HTTP headers (also readies §9 hardening).
- Stand up a local collector/viewer (e.g. Jaeger) in Compose.

**Done when:** one turn produces a single trace spanning orchestrator → gateway → retrieval →
providers, with per-stage latency visible.

---

## Milestone 6 — Demo hardening & docs

Make it reliably demo-able and record what was built.

- End-to-end run-through on a fresh machine from the README.
- Tune VAD hangover / barge-in threshold if real-provider timing shifted them.
- Short demo script + updated architecture notes; capture measured latency numbers.
- Sanity-check the error paths (barge-in, retrieval timeout, provider error) on camera.

**Done when:** `docker compose up` → a clean spoken, grounded, interruptible conversation, with
the three failure modes gracefully handled.

---

## Dependency order (quick view)

```
M0 scaffold+contracts
      │
M1 orchestrator port ──► M2 real STT+TTS ──► M3 RAG ──► M4 real LLM+cache
                                                             │
                                              M5 tracing ◄───┘
                                                             │
                                                     M6 demo hardening
```

M5 can start in parallel once M1 lands (spans over mocks), but it's most useful after M4.

---

## Explicitly out of scope for the POC (from design §9–§10)

Per-turn `turnId` frame hygiene · speculative retrieval · self-hosted inference · external state
store · K8s / service mesh · per-provider gateway split · Prometheus/Grafana dashboards.
These are planning-tracked, not build-tracked.
