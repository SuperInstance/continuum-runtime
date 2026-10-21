# Foundations application — continuum-runtime

What beam-quilt's verified foundations (beam-quilt `FOUNDATIONS.md`, Lane A
pull list) mean for this repo specifically. This repo's confidence is
deliberately the programme's lowest; tags below use **SETTLED / PROPOSED /
SPECULATIVE** (mapping to this repo's settled / plausible / speculative).

## PULL here

### PULL — BEAM process & supervision semantics (Erlang/OTP, Apache-2.0) → `nodes/sovereign`, `engine/loop`
- **Replaces:** inventing patch lifecycle and failure semantics from
  scratch. ARCHITECTURE.md currently gives `sovereign` only "restart on
  crash (supervisor pattern)" — the foundations replace that placeholder
  with the whole OTP model: per-process heaps with copied messages,
  selective-receive mailboxes (a patch's inbox is the natural
  seam-arbitration chokepoint), supervision trees with MaxR/MaxT
  restart-intensity escalation, monitors/links, two-generation hot code
  swap.
- **Explicitly NOT pulled:** distributed Erlang's full-mesh topology,
  cookie trust, and kill-the-loser `global` name resolution — they assume
  the uniform-trust cluster that edge hardware is not. Node discovery and
  trust here stay enrolment + keys (SPEC C1).
- Tag: **SETTLED** as the reference model (this is also the "BEAM process
  model" ARCHITECTURE.md already cites for `sovereign`); the Rust
  realisation is **PROPOSED** — take the semantics, not the VM.

## ADAPT here

### ADAPT — Temporal's position-matched replay discipline (docs; platform not run) → `engine/loop`, stream writes
- **Replaces:** any naive notion of replaying a run. Log transmissions so
  re-derivation is positionally checkable; treat every model call as an
  activity — recorded, never re-executed; detect drift as a positional
  mismatch instead of pretending determinism.
- This *enforces* chrono-flight's replay limit from the emitter side: this
  runtime must never emit a stream that implies re-executability.
- Tag: **PROPOSED**.

### ADAPT — Chronicle Queue named tailers + mmap append-only cycles (Apache-2.0) → P5 patch-local logs
- **Replaces:** unspecified log mechanics under BUILD-BRIEF's persistence
  requirement (SPEC P4/P5). Named, persistent, read-without-consume
  tailers = each sovereign reader keeps its own position; cycle-rolling
  is the retention seam where chrono-tensor downmix/archive hooks in.
- **Rebuild in Rust** (this repo); do not vendor the Java original.
- Tag: **PROPOSED**.

### STUDY patterns — churn answers (Petals; Hivemind/SWARM) → `nodes/`, `core/manifold`
- The two proven churn sequences: blacklist → reroute → regenerate caches
  (Petals), and rebalance the topology around the departed node
  (Hivemind/SWARM). Neither system is pulled — both shard one shared
  model, which sovereignty forbids — but their *recovery sequences* are
  the reference for what `sovereign` + supervision should do when a patch
  or node disappears mid-stream. Tag: **PROPOSED** as design input.

## BUILD OUR OWN

- **Sovereign, churn-tolerant pooling.** No surveyed pooled-inference
  system fits: all shard one shared model, none let a receiver refuse,
  none log influence. The runtime's trigger-driven, refusal-respecting
  scheduling is ours to build; prima.cpp's heterogeneity-aware placement
  is the STUDY reference for placement only.
- **The boundary-driver selection rule.** The constraint (amplify, never
  command) is **SETTLED** in ARCHITECTURE.md; no source anywhere specifies
  how "the pattern the fleet already exhibits" is selected. Still an open
  design problem — foundations found no prior art that closes it.
  Tag: **SPECULATIVE**, unchanged.

## Open decisions — NOT settled by the foundations

### State model: stored sovereign state vs trajectory-only (Casey's ruling)
This is the repo's headline open decision (ARCHITECTURE.md, "OPEN
DECISION") and the foundations pass deliberately does not settle it — it
verified the *stakes*: a trajectory-only runtime cannot keep P5's logs,
cannot feed chrono-flight, and cannot produce auditable records.

Sections that change under each ruling:
- **Stored state (current position; SPEC P4/P5 as written):**
  `core/trajectory` stays a derived view over stored history;
  `core/manifold` stays bookkeeping; the Chronicle adaptation above
  applies as written; BUILD-BRIEF's persistence acceptance test stands.
- **Trajectory-only (requires a beam-quilt SPEC amendment FIRST — the
  amendment leads, this repo follows):** `core/trajectory` becomes the
  primary state representation and `core/manifold` must be redesigned
  around it; P5 logging, the disagreement record, and the C3 stop test
  lose their substrate and must be re-specified upstream; the Chronicle
  and Temporal adaptations shrink to stream-only roles (no patch-local
  stored log to tail or positionally check); chrono-flight's emitter
  assumptions break (see its FOUNDATIONS-APPLICATION.md). Do not prototype
  this branch in code before the ruling.

### Deadband semantics (Casey's ruling: clarify vs abstain as fourth outcome)
`seams/deadband` implements the band; the ruling decides its middle.
- **Clarify-only:** an in-band event emits a clarification request onto
  the stream; `core/weigh` counts holds-that-clarified.
- **Abstain added:** an in-band event may also resolve as coast/no-act,
  which `engine/loop` must treat as a terminal outcome for that trigger
  (not a pending wake), `core/weigh` gains an abstain rate alongside
  deadband/refusal rates, and `seams/entrainment` must not read a coasting
  node's silence as cadence drift. None of this is built until ruled.

## Conflicts found against the verified foundations

1. **BUILD-BRIEF.md wording — "the full run can be replayed through
   chrono-flight."** Read literally, this promises re-execution, which the
   verified foundations (Temporal discipline, above) and chrono-flight's
   own replay limit both reject. The compliant reading is *playback +
   positionally checkable re-derivation*. Flagged as a wording conflict to
   fix when BUILD-BRIEF is next revised; this file does not edit it.
2. **ARCHITECTURE.md:167 parked-vocabulary row** naming "Laman rigidity =
   truth" in order to reject it — checked in the foundations pass,
   correct as written, no change.
3. `seams/entrainment` is consistent with the foundations' Kuramoto
   verdict (MISLEADING-SCOPE as a mechanism claim): the doc already
   reduces it to periodic timing pulses, no oscillators coupled. No
   conflict; do not let the parked term re-enter as a mechanism.
