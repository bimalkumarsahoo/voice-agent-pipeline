# Barge-in Cancel/Flush Protocol

Companion to `DESIGN_PHASE2.md` (§4) and `CONTRACTS_PHASE2.md`. Specifies the exact ordering when
a caller interrupts the agent mid-reply.

## The one principle

**Kill local (caller-facing) first, clean up remote after.** The caller only hears the
orchestrator's outbound queue — they cannot tell whether the gateway is still generating. So the
only step that must be instant is flushing that local queue. Everything else is cleanup and can
take a network hop.

## What's in flight at the moment of barge-in

1. Outbound queue in the orchestrator — synthesized frames waiting to send. *(local)*
2. Frames already on the wire toward the caller. *(in transport)*
3. The gRPC stream — gateway still sending `AudioFrame`s. *(remote)*
4. Provider streams inside the gateway — LLM + TTS still working. *(remote)*

## The ordering

**Step 0 — detect.** ~200 ms of continuous inbound audio on the separate inbound channel
(prevents triggering on the agent's own voice, clicks, coughs). Same threshold as phase 1.

**Step 1 — stop local playback (instant, ~0 ms, LOCAL).**
Flush the outbound queue and send `clear` to the transport. The caller goes silent *now*. This is
the only latency-critical step. It works regardless of what the gateway is doing.

**Step 2 — cancel the gRPC call (REMOTE, ~one hop).**
The orchestrator cancels the `Converse` call. gRPC's native cancellation fires the gateway's
context; it stops the LLM and TTS provider streams. No cancel *message* — native gRPC cancellation
is the mechanism (see `CONTRACTS_PHASE2.md`). `TurnComplete.reason` would read `"cancelled"`.

**Step 3 — mark the turn cancelled (LOCAL).**
Bump the orchestrator's current-turn marker so any late frame from the cancelled turn is dropped
rather than played into the next turn. See the race below.

**Then:** turn N+1 starts fresh — new stream, clean queue.

## The surviving race (and the guard)

Steps 1–2 don't eliminate one race: an `AudioFrame` from turn N may already be on the wire when
the gateway observes the cancel, and it can arrive *after* the orchestrator has started turn N+1.
Without a guard, that stale frame plays into the new turn — the caller hears a fragment of the
abandoned reply. (Same class as the phase-1 README limitation about `agent_turn_complete` ordering.)

**Guard:** the orchestrator tags each outbound frame with the turn it belongs to and drops any
frame whose tag != the current turn.

### Important: this guard is LOCAL — it is NOT a contract change

The tag lives entirely inside the orchestrator, on its own outbound frames. The gateway never sees
it. So this does **not** put a `turnId` in the gRPC proto — consistent with the deferral in design
§10. Two consequences:

- **POC:** you can ship without the tag. With a call pinned to one pod and instant local flush, the
  stale-frame window is small. Add the tag only if you actually observe the symptom.
- **If added:** it's a local field on the orchestrator's queue items, not a proto field. No
  contract change, no gateway change.

## Why this ordering is correct

- Local flush first → caller-perceived latency is ~0, independent of network or gateway state.
- Native gRPC cancel → no bespoke cancel message; cancellation propagates cleanly to providers.
- Local turn marker → the one race that survives a network cancel is closed without a contract change.
- Queue + clear stay in the orchestrator (design §4) → this is *why* split-by-concern beats
  split-by-stage: a single, local cancellation authority.

## Sequence (text)

```
caller speaks (mid-reply, turn N)
        │  ~200ms detect
        ▼
[1] flush queue + clear      (LOCAL, ~0ms)   ── caller goes silent
        │
        ▼
[2] cancel gRPC Converse     (REMOTE, ~hop)  ── gateway stops LLM+TTS
        │
        ▼
[3] bump current-turn marker (LOCAL)         ── late turn-N frames now dropped
        │
        ▼
turn N+1 starts: new stream, clean queue
```
