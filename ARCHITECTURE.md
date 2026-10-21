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
  state (P4). Confidence: **plausible** (the name is parked vocabulary; the
  bookkeeping need is real).
- **`trajectory`** — a patch's recent history of states and actions, kept
  as an ordered record so "where is this patch heading" (rate and direction
  of change) can be computed from what it has done. *Need to know:* this is
  a view over stored history, **not** a replacement for stored state — see
  the open decision below. Confidence: **plausible** as a derived view;
  **speculative** as an ontology.
- **`weigh`** — per-seam and per-patch disagreement accounting: how often
  proposals land in the deadband, get refused, or get disputed, accumulated
  over time. *Need to know:* this is counters and rates over recorded seam
  outcomes. It is an input to scheduling priority and to operator reports —
  nothing more mystical. Confidence: **plausible**.

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
- **`deadband`** — the seam decision band: accept above the high mark,
  refuse below the low mark, and in between, hold for clarification
  (beam-quilt SPEC §3). *Need to know:* what "in between" triggers is
  awaiting Casey's ruling (clarify-by-asking vs. abstain-and-coast are both
  in the source material; v0 says clarify). This module implements the
  band; the ruling decides its middle behaviour. Confidence: **settled**
  that the band exists; **open** what the middle does.
- **`entrainment`** — cadence coordination. In plain words: **nodes
  periodically exchange small timing pulses and nudge their local cycle
  toward their neighbours'**, so a fleet that needs a shared rhythm can
  converge on one without a master clock. *Need to know:* a pulse is just
  an event on the stream; no node is obliged to match anyone. Confidence:
  **plausible** as a mechanism (periodic sync pulses are ordinary
  engineering); **speculative** that quilt scheduling needs it in v0.

### `src/engine/`

- **`loop`** — the trigger loop: consume the event stream, match events
  against patches' declared trigger conditions, wake the matching patch
  processes, append outcomes to the stream. *Need to know:* event-driven,
  never a polling timer and never a scheduler that assigns work a patch
  didn't subscribe to. Confidence: **plausible** (it is the runtime's
  defining choice; unbuilt).
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

## OPEN DECISION — trajectory state vs. stored state

**This conflict is unresolved. Do not resolve it in code. It is Casey's
ruling to make.**

- The continuum sources (`gemini20.md`, `gemini21.md`) claim state *is*
  trajectory: a node is its velocity through time; stored static state is
  declared undefined; "there is no database."
- beam-quilt SPEC **P4** requires a patch to hold sovereign state, and
  **P5** requires it to keep its own log of transmissions and decisions.
  The seam rules, the disagreement record, and the C3 stop test all depend
  on that stored record existing.

A trajectory-only runtime **cannot keep P5's logs** — a velocity has no
memory — and cannot produce the records chrono-flight exists to replay or
an operator exists to audit. Accordingly, until Casey rules otherwise:

- **This runtime stores.** Every patch's state and log are persisted by the
  patch (via the `sovereign` wrapper's guarantees), and the stream itself
  is append-only.
- `core/trajectory` is a *derived view* over that stored history, useful
  for scheduling and reporting — nothing more.
- If a future ruling adopts trajectory-only state, P4/P5 and everything
  built on them must be amended in beam-quilt first. The amendment leads;
  this repo follows.

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
