# continuum-runtime — architecture

Confidence marks used throughout: **settled** = follows from the beam-quilt
SPEC or a plain engineering fact; **plausible** = a reasonable design
choice, not yet argued or tested; **speculative** = an idea from the source
conversations with no mechanism or evidence behind it yet. Speculative items
are kept visible so following agents don't have to rediscover them — and
don't mistake them for decisions.

## Module map

Layout synthesized from the Rust scaffold in `gemini21.md`.

### `src/core/`

- **`manifold`** — the runtime's bookkeeping view of "where things stand":
  which patches exist, which seams are open, what the recent stream history
  is. *Need to know:* this is an index the runtime keeps for scheduling; it
  is **not** a shared state store patches read from — patches hold their own
  active log (P4). Confidence: **plausible** (the name is parked vocabulary; the
  bookkeeping need is real).
  *Foundation mapping:* **log-side (derived)** — an index over patches'
  active logs and the stream history; it holds no readings of its own.
- **`trajectory`** — a patch's recent history of states and actions, kept
  as an ordered record so "where is this patch heading" (rate and direction
  of change) can be computed from what it has done. *Need to know:* this is
  a reading of the patch's active log over time, **not** a rival store and
  not a replacement for the log — see the foundation section below.
  Confidence: **plausible** as a derived view; the trajectory-only ontology
  is **superseded** (below).
  *Foundation mapping:* **log-side** — a view over the active log, never
  a store of its own.
- **`weigh`** — per-seam and per-patch disagreement accounting: how often
  proposals land in the deadband, get refused, or get disputed, accumulated
  over time. *Need to know:* this is counters and rates over recorded seam
  outcomes. It is an input to scheduling priority and to operator reports —
  nothing more mystical. Confidence: **plausible**.
  *Foundation mapping:* **ledger-side** — its inputs are booked seam
  outcomes (agreed entries, refusals, disputes), not private readings.

### `src/nodes/`

- **`sovereign`** — the patch process wrapper: spawn, isolate, deliver an
  envelope, collect the outcome, restart on crash (supervisor pattern).
  *Need to know:* no shared memory with any other patch; the wrapper can
  deliver and observe, never reach inside. Confidence: **settled**
  (this is SPEC P4 + the BEAM process model from beam-quilt docs/03).
- **`jepa`** — an optional local predictive model inside a node: the node
  keeps a small model of what it expects to happen next locally, and the
  gap between prediction and observation is itself a signal worth emitting.
  *Need to know:* "JEPA" here names that pattern (a local predictor whose
  surprise is data), not a specific trained architecture; nothing in the
  source material specifies a model. Confidence: **speculative**.
- **`boundary`** — the boundary driver node. See "The boundary driver"
  below: a **settled constraint** with a **speculative mechanism**.
  Confidence: mixed, as marked there.

### `src/seams/`

- **`envelope`** — the chrono-tensor event / beam-quilt transmission
  envelope as the one thing that crosses between processes: typed intent,
  timestamp/tick, provenance, declared cost. *Need to know:* the format is
  owned by `chrono-tensor` and constrained by SPEC T1–T3; this module is a
  consumer, not a designer, of the format. Confidence: **settled**.
- **`deadband`** — the seam tolerance mechanism (beam-quilt SPEC §3,
  v1.1; ruled by Casey 2026-10-21 on the autopilot doctrine): a
  context-set, learnable **(band, yaw-time)** pair around the receiver's
  predicted course. Inside the band → **Hold** (no adjustment; coasting
  is this, not a separate outcome). Outside the band but within
  yaw-time → **HoldWatch** (waves swing you out and back; do not steer
  to them). Outside beyond yaw-time → **Act(response)**, the response
  chosen by policy (accept-best, clarify/narrow, escalate). Confidence:
  **settled**.
  *Foundation mapping:* **ledger-side in effect** — its verdicts are seam
  events at a crossing; its band/yaw-time parameters are held log-side,
  in the receiver's own readings and calibration.
- **`entrainment`** — cadence coordination. In plain words: **nodes
  periodically exchange small timing pulses and nudge their local cycle
  toward their neighbours'**, so a fleet that needs a shared rhythm can
  converge on one without a master clock. *Need to know:* a pulse is just
  an event on the stream; no node is obliged to match anyone. Confidence:
  **plausible** as a mechanism (periodic sync pulses are ordinary
  engineering); **speculative** that quilt scheduling needs it in v0.
  *Foundation mapping:* **ledger-side** — pulses are transmissions booked
  at the seam; any convergence is read afterwards from each node's log.

### `src/engine/`

- **`loop`** — the trigger loop: consume the event stream, match events
  against patches' declared trigger conditions, wake the matching patch
  processes, append outcomes to the stream. *Need to know:* event-driven,
  never a polling timer and never a scheduler that assigns work a patch
  didn't subscribe to. Confidence: **plausible** (it is the runtime's
  defining choice; unbuilt).
  *Foundation mapping:* **ledger-side** — it operates the ledger
  discipline: it moves transmissions and appends their outcomes; it does
  not read any patch's active log.
- **`compiler`** — admission-time checking: when a patch is enrolled, verify
  its declared envelope types, trigger conditions, and constraint set are
  complete and satisfiable before it joins. *Need to know:* in plain words
  this is manifest validation plus a check that a patch's declared
  constraints don't contradict each other. The source material's stronger
  claim ("a fully-determined constraint graph = truth") is parked below.
  Confidence: **plausible** as validation; the truth claim is **rejected**,
  not merely speculative.

## The boundary driver (the "DJ")

Source: `gemini20.md`. The useful content of the DJ material is a **limit
on orchestrators**, and that limit is adopted here as a constraint:

- A boundary driver is a node with a larger voice at the edge of a fleet —
  it can broadcast, suggest, set tempo, and amplify.
- **It may only amplify a pattern the fleet already exhibits.** It reads
  what is happening and reinforces it; it does not originate marching
  orders.
- **It may never command.** No patch is obliged to obey a boundary driver.
  A patch's receiver-side judgement (its Jev, SPEC §3) applies to the
  driver's proposals exactly as to anyone else's.
- If the fleet rejects the driver's amplification, the energy dies — that
  is the design working, not a failure to be patched over with authority.

Confidence: the *constraint* (amplify, never command) is **settled** as a
design rule for this repo — it is beam-quilt's sovereignty applied to the
orchestrator role. The *mechanism* in the source (phase-space attractor,
argmax over a resonance kernel) is **speculative**: no selection rule for
"the pattern the fleet already has" is specified anywhere, and building one
is an open design problem, not a formula waiting to be transcribed.

## Environmental staging (a design input)

Source: `gemini20.md` ("staging over prompting"). The claim worth keeping,
in plain words: **what a node perceives is weighted by its situation —
where it is, when an observation arrives, who else is present and active,
and what it has been doing.** The runtime should therefore treat presence,
timing, and neighbourhood as first-class inputs to trigger matching and to
the weight an observation carries, rather than pretending every event
arrives in a vacuum with only its payload mattering.

Confidence: **plausible** as a design input (it is how the source
conversations' strongest material — the fishing-fleet chapters — actually
works). No weighting scheme is specified; any concrete formula is future
work and must be argued, not inherited from the prose.

## FOUNDATION — the two active items (supersedes the stored-vs-trajectory question)

**The old A/B question — stored sovereign state vs. trajectory-only
state — is SUPERSEDED.** Casey's restatement of the SuperInstance
foundation (2026-10-21; recorded in beam-quilt `FOUNDATIONS.md`,
"Shelf foundations — the two active items," and absorbed into SPEC
v1.2 P4/P5) dissolves the framing: state was never a store *or* a
trajectory — a system is represented by **two active items**:

- **Active log (SPEC P4).** Each patch/item holds its own readings of
  every cell *from its own perspective*, each reading dated. Its
  readings are first-class truth *for it*; there is no God's-eye view
  and no shared state store. `core/trajectory` is a **reading of that
  log over time** — rate and direction computed from the dated
  readings — not a rival store and not a replacement for the log.
- **Active ledger (SPEC P5).** What is shared exists **only** as
  agreed ledger entries: a transmission is booked by the sender as a
  SEND and by the receiver as a RECEIVE, under the contract
  (algorithm) that governed the crossing, and only entries both sides
  agree on count as shared. Shared state is never stored anywhere; it
  exists in the agreement.

Consequences for this runtime:

- **This runtime persists logs and ledger sides, not "state."** Every
  patch's active log and its own side of the active ledger are
  persisted by the patch (via the `sovereign` wrapper's guarantees),
  and the stream itself is append-only. The records chrono-flight
  replays and an operator audits are the log and the agreed entries —
  which is why the trajectory-only branch failed on its own terms: a
  velocity has no memory, and there was never a store to defend
  either.
- **Prior art: `quilt-rooms`.** The named pair (`ACTIVE-LOG.md` /
  `ACTIVE-LEDGER.md`), sovereign local sequence time, both-sides
  record, and pending→agreed lifecycle are already implemented there
  (2026-10-08) and are cited as the source of the split — tap the
  vocabulary and sovereignty rules; its ledger agrees on *time*, so
  the payload-transfer agreement (SPEC P5) remains this programme's
  own work (beam-quilt FOUNDATIONS.md, gap 2).

## Parked vocabulary

Terms from the source conversations that are **not** used in this repo's
design. Each is parked with what it actually reduces to, so a following
agent can skip re-deriving it.

| Parked term | Reduces to |
|---|---|
| Self-weighing / system weight (W, τ, ω) | Accumulated disagreement counts and rates at a patch's seams over time (`core/weigh`) |
| Phase-space / manifold | The set of patches' current states plus their recent history (`core/manifold` as bookkeeping) |
| Standing wave | A stable, recurring pattern of fleet activity — an observation, not a mechanism |
| Kuramoto entrainment | Nodes adjusting their cadence toward neighbours' via periodic timing pulses (`seams/entrainment`) |
| Boundary driver as phase-space attractor | A high-amplitude node whose proposals get adopted because they match what the fleet is already doing |
| Laman rigidity = truth | Checking a constraint graph is fully determined. A fully determined graph can still be wrong; determination is not truth |
| Hodge decomposition of friction | Sorting seam disagreements into three buckets: noise, cycles, structural conflicts |
| Spectral Identity (SIA) | Identifying a node by a behavioural signature instead of a key. Parked: identity here is enrolment + keys (SPEC C1); behavioural identity is unproven and spoofable |
| Genome / self-evolving code | A mutation-and-selection loop over node code. Parked as a non-goal (BUILD-BRIEF.md) |
| Lattice routing (Eisenstein A₂) | Routing messages over a hexagonal coordinate grid. Belongs to chrono-tensor/snapkit exploration, not this runtime |
| Wu Wei / "the room weighs nothing" | A fleet in agreement needs little coordination traffic. A symptom, not a target to optimise directly |
