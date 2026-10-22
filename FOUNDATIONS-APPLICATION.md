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

## Rulings and remaining open decisions

### State model — SUPERSEDED by the two-active-items foundation (Casey, 2026-10-21)
The stored-state vs trajectory-only question is **closed — not by
picking a side, but by Casey's restatement of the foundation**, which
reframed what "state" is. (Full record: beam-quilt `FOUNDATIONS.md`,
"Shelf foundations — the two active items"; SPEC v1.2 P4/P5.) Each
patch holds an **active log** — its own dated readings from its own
perspective, first-class truth *for it*, never another patch's to
read — and crossings are **active ledger** entries booked SEND by the
sender and RECEIVE by the receiver, with shared state existing only in
the two sides' agreement. There is no stored state to defend and no
trajectory-only store to adopt: `core/trajectory` is a reading of the
active log over time, and `core/manifold` is bookkeeping over logs and
agreed entries. Prior art for the split: **quilt-rooms** (2026-10-08).

What this settles for the adaptations above:
- The Chronicle adaptation applies as written, now with its object
  named correctly: the patch-local records its named tailers serve are
  the patch's **active log** and its own side of the **active ledger**.
- The Temporal discipline applies to the **agreed ledger entries**:
  re-derivation is positionally checkable over entries both sides
  booked, not over a state store.
- BUILD-BRIEF's persistence acceptance test stands, re-read: what must
  persist is each patch's log and ledger side — which is also exactly
  what chrono-flight exists to record (see its
  FOUNDATIONS-APPLICATION.md).

### Deadband semantics — RULED (Casey, 2026-10-21; SPEC v1.1)
The clarify-vs-abstain framing was rejected by Casey; the mechanism is
the autopilot one. `seams/deadband` implements a receiver-held
**(band, yaw-time)** pair: **Hold** inside the band, **HoldWatch** on an
excursion within yaw-time, **Act(response)** only when an excursion
persists beyond yaw-time. Clarify/narrow is one Act response among
several, chosen by policy; coasting is inside-band Hold, not an
outcome. `core/weigh` tallies excursions with durations and outcomes;
`seams/entrainment` must not read a holding node's silence as cadence
drift. Band edges are set by conformal calibration; band and yaw-time
are context-set and learnable.

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
