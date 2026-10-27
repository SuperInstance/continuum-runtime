# SLICE1-REPORT — the stream core

Branch `slice-1` (base: main @ 21d2f86). Scope: BRIEF.md, "slice 1".
Everything below was built and checked on this branch; `cargo build` and
`cargo test` (16 unit + 8 integration tests) pass from a clean clone of
the branch.

## What was built

An append-only transmission stream; patch processes as real supervised
child OS processes exchanging transmissions over that stream, each
keeping its own active log and ledger side; an unattended kill test; and
playback as an event-for-event positional diff. One binary, four
subcommands:

```
continuum-runtime run      --run-dir D --input F --manifest-dir M [--step-delay-ms N]
continuum-runtime patch    --id ID --run-dir D            # child mode; spawned by `run`
continuum-runtime verify   --run-dir D [--input F]        # positional re-derivation
continuum-runtime playback --a D1 --b D2                  # event-for-event stream diff
```

Modules (scaffold vocabulary followed; additions flagged below):

| Module | What it is |
|---|---|
| `src/seams/envelope.rs` | The transmission record: sender, receiver, typed intent, payload + `payload_ref` (content hash), OTel-style span context (`trace_id`/`span_id`/`parent_span_id`), declared cost, `causal_pos`. Ingests the chrono-tensor 4-tuple `[tick, channel, intent, velocity]` whole (`SourceEvent`), preserved in the payload. |
| `src/stream.rs` | **Added — the scaffold named no stream module.** Append-only JSONL; positions are the ordering (the negotiated/sequence clock — records carry no wall time at all); read-back by position; named, persistent, read-without-consume tailers (the Chronicle Queue adaptation from FOUNDATIONS-APPLICATION, rebuilt in Rust). |
| `src/nodes/sovereign.rs` | The patch wrapper with the pulled BEAM/OTP semantics: a real child process per patch (per-process heap; envelopes are *copied* across the seam as serialized lines on stdin — no shared memory); supervision with MaxR=5/MaxT=10s restart intensity; at-least-once delivery. |
| `src/nodes/patch.rs` | The patch itself: its own active log (SPEC P4 — one entry per wake, durable before the reply) and its own ledger side (SPEC P5 — RECEIVE/REFUSED + SEND bookings); receiver-side refusal policy; decisions derived purely from (envelope, manifest, own log). **Added — the scaffold's node list had no home for patch behaviour.** |
| `src/engine/loop.rs` | The trigger loop: ingest → match against enrolled manifests → wake exactly the routed patch → append its outcome. No polling, no work a patch didn't subscribe to. Resume after restart re-derives pending deliveries from the stream alone. |
| `src/engine/compiler.rs` | Admission-time manifest validation (structure, duplicate ids, satisfiability-lite with warnings for unknown trigger senders). |
| `src/engine/playback.rs` | **Added — unnamed in the scaffold.** `verify` (re-derives routing, causal linkage, trace/span continuity, payload refs, declared costs, input coverage, and P5-lite log/ledger pairing, positionally) and `diff_streams` (playback). The Temporal discipline adaptation. |
| `src/core/manifold.rs` | The enrolment index and deterministic (sender, intent) → patch routing the loop schedules by. `trajectory` and `weigh` remain README stubs (later slices; they are views over logs this slice now produces). |

`seams/deadband` and `seams/entrainment`, `nodes/jepa` and
`nodes/boundary` remain README stubs, per BUILD-BRIEF non-goals and the
pending deadband ruling (see Gaps).

**Run-dir layout** (self-contained; `verify` needs nothing outside it):

```
run-dir/
  stream.jsonl                    # the append-only transmission stream
  tailers/ingest.pos              # named persistent tailer position
  patches/<id>/manifest.json      # enrolment copy
  patches/<id>/active-log.jsonl   # the patch's P4 log (patch-written)
  patches/<id>/ledger.jsonl       # the patch's P5 side (patch-written)
  patches/<id>/pid                # live child pid (kill test uses it)
  patches/<id>/stderr.log
```

### How the pieces satisfy the slice scope

1. **Stream (scope 1).** Records carry sender, receiver, payload +
   payload reference, and span-context provenance; append assigns the
   position; read-back is by position; the position *is* the sequence
   time. Each append is one `write_all` of a complete line + flush
   (O_APPEND), so a SIGKILL cannot tear a record.
2. **Two patches exchanging (scope 2).** The demo scenario: `alpha`
   triggers on `(ingest, ping)`; `beta` on `(alpha, relay)`; `alpha`
   again on `(beta, relay)`. A ping cascades ingest → alpha → beta →
   alpha(hold) — four stream records per ping, each patch woken from
   its own manifest trigger, each keeping its own log. Chain
   termination is itself P4 in action: a patch relays only the first
   envelope it sees on a trace and holds thereafter, *reading that
   fact from its own persisted log* — which is why the kill test below
   genuinely depends on log persistence.
3. **Kill test (scope 3)** — below.
4. **Playback (scope 4)** — below.

### Choices made where the scaffold was silent

- **`src/stream.rs` placement** (top level): the stream is the medium
  everything crosses — not bookkeeping (`core`), not a process
  (`nodes`), not the crossing (`seams`), not the loop (`engine`).
- **Provenance minting**: trace ids derive from source-event content;
  span ids from (trace, position, sender). All ids are pure functions
  of content and position — no wall clock, randomness, or pids — which
  is what makes two runs byte-comparable. 64-bit FNV-1a hex (in-house;
  std's hasher is not stability-guaranteed), documented as OTel-*style*,
  not OTel-compatible.
- **Single-writer stream**: only the engine appends to `stream.jsonl`;
  patches write only their own two files. This is what makes
  append-atomicity and positional re-derivation tractable in this
  slice.
- **At-least-once delivery, log-idempotent patches**: if a child dies
  or a delivery times out after the envelope was written, the
  supervisor restarts and re-delivers (the unacknowledged-mailbox
  window). A patch that finds an exact (trace, position) wake already
  in its log re-emits the logged outcome and books a `redelivery: true`
  wake entry — never a silent duplicate. Consequence, honestly stated:
  after a kill, a patch log may gain *flagged* redelivery entries; the
  stream never gains duplicates (the kill test asserts this).
- **Depth-first cascades**: a triggered record is fully answered before
  the next source event is ingested, so record layout is a
  deterministic function of (input, manifests) — the property playback
  leans on.
- **Dependency added**: `serde` + `serde_json` — JSONL stream/log
  records and line-delimited child IPC, with stable field order and
  inspectable output for diffs. Nothing else; no external services.

## The kill test — result and evidence

BUILD-BRIEF criterion 3. Implemented twice, both unattended:

- `tests/slice1.rs::kill_patch_and_runtime_then_resume_loses_nothing`
  (part of `cargo test`), and
- `scripts/kill-check.sh` (same procedure, human-readable transcript).

Procedure: run the 60-ping two-patch scenario twice (clean + paced);
once the paced run's stream passes 40 records, SIGKILL the `alpha`
child, then SIGKILL the runtime; restart the runtime on the same run
dir; then check. Transcript (`scripts/kill-check.sh`, verbatim):

```
== clean reference run ==
{"records":240,"refused":{},"restarts":{"alpha":0,"beta":0},"resumed_from":0,"unrouted":60,"wakes":{"alpha":120,"beta":60}}

== paced run, killed mid-flight ==
stream at kill: 42 records; alpha pid 10897; runtime pid 10895
snapshotted: 42 stream records, 21 alpha log entries, 11 beta log entries

== restart the runtime on the same run dir ==
{"records":240,"refused":{},"restarts":{"alpha":0,"beta":0},"resumed_from":42,"unrouted":60,"wakes":{"alpha":99,"beta":50}}

== checks ==
verify: 240 records checked
verify: PASS (no findings)
PASS  verify: nothing lost, everything re-derives
PASS  alpha log: pre-kill entries are a byte prefix of the post-restart log
PASS  beta log: pre-kill entries are a byte prefix of the post-restart log
PASS  stream: pre-kill records stand unchanged
playback: identical, 240 records (A=240, B=240)
PASS  playback: resumed stream is event-for-event identical to the clean run

== result ==
KILL CHECK: PASS
```

What each line proves:

- **No stream events lost**: `verify` re-derives input coverage (all 60
  source indices ingested exactly once), routing, causal linkage, and
  every routed record answered exactly once — 240 records, no findings.
- **No patch log entries lost**: both patches' pre-kill logs are a
  byte-for-byte prefix of their post-restart logs.
- **Nothing duplicated either**: the strongest check — the entire
  killed-and-resumed stream is **event-for-event identical to a clean
  run's stream** (`playback: identical, 240 records`). A lost or
  duplicated event anywhere would diverge the diff.
- The `resumed_from: 42` field shows the restarted runtime picked up
  exactly where the stream stood, deriving it from the stream itself.

The integration-test variant additionally asserts the wake-level
invariant (every log entry states whether it is a redelivery; observed
in runs: at most one flagged redelivery wake per kill, on the patch
whose in-flight envelope lost its outcome append).

Related, also automated: `supervisor_restarts_killed_patch_without_
runtime_restart` — SIGKILL the patch child only; the runtime's
sovereign restarts it within MaxR/MaxT, redelivers, and the run still
converges to a stream identical to the clean run's (the OTP pull
working as specified).

## Playback — behaviour

Per the corrected BUILD-BRIEF wording: playback plus positionally
checkable re-derivation; **no claim of deterministic re-execution of
model-driven decisions**. Slice-1 patches make no model calls — their
decisions are pure functions of (envelope, manifest, own log) — so two
runs over the same input *do* produce identical streams, and the slice
demonstrates exactly that:

- `playback --a run1 --b run2` compares position by position; identical
  runs report `identical, N records`; a divergence reports the first
  differing position with both records (`playback_detects_tampering`
  flips one record's cost and asserts the diff names `position 1`).
- The kill test's strongest assertion is a playback: killed-and-resumed
  ≡ clean, event for event.
- `verify` is the re-derivation half: routing, causal/span linkage,
  payload references, declared costs, coverage, and P5-lite ledger
  pairing, all positionally, from the run dir alone. Tampering is
  caught by both tools.

When a later slice enrols model-driven patches, this tooling keeps its
word: the diff would report positional divergence rather than pretend
to re-execute anything — that is chrono-flight's replay limit, enforced
from the emitter side.

## Boundary driver — no privilege (BUILD-BRIEF criterion 4)

`boundary_driver_has_no_privilege`: a `dj` patch enrolled with
`role: "boundary"` (it amplifies — its emit intent is `amplify`), a
`strict` patch that triggers on amplifications from two senders and
accepts only `alpha` (who never speaks). Result: `dj`'s envelope and
`beta`'s envelope are both refused by `strict`, same intent, same zero
cost, same terminal routing, both booked as `refused` on `strict`'s
ledger — identical treatment, because the boundary driver has no
delivery path other than the stream and no rule other than the
receiver's own. Enrolment bookkeeping only; `nodes/boundary` stays a
stub (no selection mechanism — correctly still open per
FOUNDATIONS-APPLICATION).

## Reproduce

```
cargo build && cargo test          # everything above, unattended
./scripts/kill-check.sh            # the kill test transcript
```

## Gaps — precise

1. **Deadband not built** (`seams/deadband` stub). Pending Casey's
   middle-behaviour ruling (BUILD-BRIEF open item: build the band with
   the middle behaviour behind one switch). Nothing in this slice
   pre-commits its shape; refusal here is policy-only, not band
   arithmetic. Next slice needs: the ruling, then (band, yaw-time) as
   receiver-held context-set parameters with Hold/HoldWatch/Act
   outcomes on the seam.
2. **P5 payload-transfer agreement is simplified** (quilt-rooms parity
   is *time*; payload agreement is beam-quilt gap 2). Slice 1 books
   both sides (SEND by sender, RECEIVE/REFUSED by receiver) and
   `verify` pairs them content-wise, but there is no pending→agreed
   lifecycle and agreement is asserted post-hoc by the runtime's
   single-writer append, not negotiated by the two sides.
3. **At-least-once delivery** (unacked-mailbox window): correct under
   idempotent patches, but a later slice wanting exactly-once needs
   delivery acknowledgements booked on the stream.
4. **Restart policy is one-shape** (fixed MaxR/MaxT, no escalation
   tree, no hot code swap — the OTP pull is the per-process semantics,
   not the full supervision tree).
5. **Chronicle adaptation is positional, not mmap**: named persistent
   tailers yes; cycle-rolling/mmap cycles no (slice-1 logs are small;
   the retention seam where chrono-tensor downmix hooks in remains
   future work).
6. **`verify`'s coverage check needs the input file** (optional flag);
   the run dir alone proves everything else. Recording the input
   digest into the run dir would close this.
7. **Single host, point-to-point** per v0 non-goals; no lattice
   routing, no entrainment, no JEPA, no self-modification — all
   intentionally absent, not missed.
