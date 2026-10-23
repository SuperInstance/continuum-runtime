# continuum-runtime — build brief (smallest first build)

**For the agent who builds this:** you need this brief, `ARCHITECTURE.md`,
and the beam-quilt SPEC. You do not need the Gemini conversations; the
need-to-knows are extracted here. Blinders on, build the small thing.

## The build

A single-host event loop that:

1. **Ingests** a chrono-tensor event stream (a file or local socket of
   events in the `[time/tick, channel/agent, intent, velocity/pressure]`
   format owned by `chrono-tensor`).
2. **Matches** each incoming event against the trigger conditions declared
   in the enrolled patches' manifests.
3. **Wakes** the matching patch process (one patch is enough for v0),
   delivers the triggering envelope via `seams/envelope`, and waits for
   its outcome without inspecting its state.
4. **Records** the outcome back onto the stream as a new event
   (patch id, outcome type, declared cost, timestamp), so the run is fully
   reconstructible from the stream alone.
5. **Persists** each patch's state and log across restarts (SPEC P4/P5 —
   see the open decision in ARCHITECTURE.md; the ruling for now is: store).

Scheduling is by trigger only. There is no polling interval, no work
queue a patch didn't subscribe to, and no component that can instruct a
patch to do anything other than by delivering an envelope its receiver
side may refuse.

## Acceptance criteria

- Given a stream containing one event matching patch A's trigger and one
  matching nothing: A wakes exactly once; no other process is started; the
  no-match event produces no wake and no error.
- The outcome event written back carries the same provenance chain as the
  triggering event plus A's declared cost.
- Killing the patch process mid-run and restarting the runtime loses no
  stream events and no patch log entries.
- A boundary-driver node (if enrolled) can be demonstrated to have no
  privilege: an envelope from it is refused by a patch whose rules reject
  it, identically to any other sender.
- The full run can be played back through chrono-flight (or, until it
  exists, diffed against the recorded stream file) event for event —
  playback plus positionally checkable re-derivation of derived state.
  Deterministic re-execution of decisions made by model-driven nodes
  is not promised and not implied (chrono-flight's replay limit).

## Non-goals (do not build these)

- **No self-modification.** Patches do not rewrite themselves, each other,
  or the runtime.
- **No genome mutation loop.** The `gemini21.md` material about genetic
  encoding and self-evolving nodes is speculation; it is parked, not a
  roadmap item.
- **No lattice routing.** No Eisenstein/A₂ coordinate routing; messages go
  point to point on one host in v0.
- **No entrainment requirement.** Cadence pulses (`seams/entrainment`) are
  not needed for the first build; triggers alone must work.
- **No boundary-driver intelligence.** v0 needs the *constraint* enforced
  (amplify-only, never command — enforced by giving the driver no special
  delivery path), not a resonance-selection algorithm.
- **No latent-vector seams.** Crossings are typed envelopes (SPEC T1),
  not raw model activations.

## Open items the builder must not decide alone

- The deadband's middle behaviour (clarify vs. abstain) — Casey's ruling,
  pending; build the band with the middle behaviour behind one switch.
- Trajectory-only state — rejected for v0 by the standing ruling in
  ARCHITECTURE.md; do not "optimise away" stored state.
