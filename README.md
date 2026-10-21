# continuum-runtime

**A Rust runtime that runs beam-quilt patches as sovereign processes over a
shared chrono-tensor event stream, waking patches on event triggers rather
than polling them.**

## Who it's for

Builders working one layer below the beam-quilt design study: people who
want to know what an *executable* quilt looks like — patches as real
processes, seams as real message crossings, scheduling driven by what
actually happened on the stream — without giving any process a view into
another's state, and without a central scheduler that commands anyone.

## Status

**Scaffolding / speculative.** This repo is a synthesized scaffold, not a
design Casey has ratified and not working code. Its confidence is
deliberately marked **lower than quilt-engine's**: quilt-engine implements
reference semantics that were argued out in the open; this runtime
extrapolates from the most speculative stretch of the source conversations
(the "continuum" material in beam-quilt's `docs/proposed/gemini20–22.md`),
including one conflict with the beam-quilt SPEC that is **unresolved** —
see ARCHITECTURE.md, "Open decision: trajectory state vs. stored state."

Nothing here overrides the SPEC. Where this repo and the SPEC disagree, the
SPEC wins until Casey rules otherwise.

## Relation map

| Role | Repo |
|---|---|
| **Spec** — what a patch is, what may cross a seam, the rules (P1–P6, T1–T3, G1–G3, C1–C3) | `beam-quilt` |
| **Event format** — the 4D event stream this runtime schedules over: `[time/tick, channel/agent, intent, velocity/pressure]` | `chrono-tensor` |
| **Reference semantics** — the small, argued-out implementation of envelope + Jev pair that this runtime's seam behaviour must match | `quilt-engine` |
| **This repo** — process execution, trigger scheduling, persistence of patch state and logs on top of those three | `continuum-runtime` |
| **Recorder** — captures this runtime's event streams for later examination | `chrono-flight` |

## The one-paragraph version

Patches run as separate processes, each holding its own state (SPEC P4)
and its own log (P5). They never read each other. Everything that happens —
a transmission sent, a seam decision, a patch waking, a patch finishing —
is appended to the shared event stream in chrono-tensor format. The
runtime's loop does not poll patches on a timer; it watches the stream,
and when an event matches a patch's declared trigger conditions, it wakes
that patch and hands it the event. What the patch does with it is the
patch's business. Outcomes go back onto the stream as new events. The only
privileged node type, the *boundary driver*, is a constraint on
orchestration, not a commander: it may amplify a pattern the fleet already
has; it may never issue an instruction a patch is obliged to obey (see
ARCHITECTURE.md).

## Layout

- `ARCHITECTURE.md` — module map, need-to-knows, confidence marks, the
  open state conflict, and the parked-vocabulary table.
- `BUILD-BRIEF.md` — the smallest first build and its non-goals.
- `src/` — module skeleton: `core/`, `nodes/`, `seams/`, `engine/`, each
  with a one-line README. No implementation yet.

## Source material

Synthesized from `beam-quilt/docs/proposed/gemini21.md` (the
`continuum-runtime` scaffold — one of three rival scaffolds in that file;
the Python `continuum-manifold`/`phase-quilt` variant was not used),
`gemini20.md` (boundary driver, environmental staging, self-weighing), and
`gemini22.md` (continuum execution lifecycle). Those files are raw
conversation exports — iterations of prompts Casey retried and selected
from — and are cited here as *leads with prior work behind them*, not as
specifications.
