//! `nodes/patch` — the patch process (the thing `nodes::sovereign`
//! wraps). One patch = one child OS process with its own heap; it holds
//! its **active log** (SPEC P4: its own dated readings, first-class
//! truth *for it*) and its **side of the active ledger** (SPEC P5:
//! RECEIVE bookings for envelopes it accepted, SEND bookings for the
//! outcomes it emitted). Neither is ever read by the runtime or another
//! patch — the runtime delivers envelopes and collects outcomes, that
//! is all it can see.
//!
//! The decision function is a pure function of (envelope, manifest,
//! own active log) — no wall clock, no randomness, no shared state:
//!
//! - **refuse** if the sender fails the patch's receiver-side policy
//!   (identically for every rejected sender — a boundary driver has no
//!   other path to be rejected by, BUILD-BRIEF criterion 4);
//! - **emit** (relay, in the patch's declared emit intent) when this
//!   patch has never emitted on the envelope's trace;
//! - **hold** when it has (the chain ends there), or when an exact
//!   (trace, position) re-delivery arrives — in which case the logged
//!   outcome is re-emitted unchanged, which is what makes at-least-once
//!   delivery converge (`nodes::sovereign`).
//!
//! "Already emitted on this trace" is read from the patch's own
//! persisted log, so the patch's behaviour genuinely depends on P4
//! persistence surviving kills — the property the kill test exercises.

use std::io::{BufRead, Read, Write};
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::core::manifold::Manifest;
use crate::seams::envelope::{payload_ref, Payload, Transmission};

// ---------------------------------------------------------------------------
// Child-process protocol (one JSON object per line; see nodes::sovereign)
// ---------------------------------------------------------------------------

/// runtime → patch, on the child's stdin.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum RuntimeToPatch {
    Deliver { record: Transmission },
}

/// patch → runtime, on the child's stdout.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum PatchToRuntime {
    Ready { id: String, log_len: u64, ledger_len: u64 },
    Outcome(Outcome),
}

/// A patch's answer to a delivered envelope.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Outcome {
    pub intent: String,
    pub payload: Payload,
    pub cost: u64,
}

// ---------------------------------------------------------------------------
// Active log (P4) and ledger side (P5), slice-1 record shapes
// ---------------------------------------------------------------------------

/// One entry in the patch's active log: a dated (by sequence number)
/// reading of a wake, what crossed, and what this patch decided.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct LogEntry {
    pub seq: u64,
    /// Trace of the delivered envelope.
    pub trace_id: String,
    /// Stream position of the envelope that woke this patch.
    pub trigger_pos: u64,
    pub sender: String,
    pub intent: String,
    /// True when this exact (trace, trigger_pos) had been processed
    /// before — an at-least-once redelivery, not a new crossing.
    pub redelivery: bool,
    /// What this patch did. `None` would mean a crash between the wake
    /// and the decision; the append order (wake entry first, then
    /// decision folded into the same entry before reply) prevents it.
    pub decision: Decision,
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Decision {
    /// `refuse` | the emitted intent | `hold`.
    pub outcome_intent: String,
    pub body: String,
    pub cost: u64,
}

/// One entry in the patch's side of the active ledger. Booked at the
/// seam crossing: RECEIVE/REFUSED when an envelope arrives (its stream
/// position), SEND when this patch emits an outcome answering it (the
/// parent position, plus the content `verify` matches against the
/// stream record).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct LedgerEntry {
    pub seq: u64,
    /// `receive` | `refused` | `send`.
    pub kind: String,
    /// Stream position the entry concerns: the envelope's own position
    /// for receive/refused, the answered envelope's position for send.
    pub pos: u64,
    pub counterparty: String,
    pub intent: String,
    /// Content fields for send entries (empty for receive/refused,
    /// whose identity is (pos, kind) already).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload_ref: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cost: Option<u64>,
}

pub const LEDGER_RECEIVE: &str = "receive";
pub const LEDGER_REFUSED: &str = "refused";
pub const LEDGER_SEND: &str = "send";

// ---------------------------------------------------------------------------
// The decision function
// ---------------------------------------------------------------------------

/// The patch's whole behaviour, pure: given the envelope, its manifest
/// and its own log, decide. See the module docs for the rules.
pub fn decide(record: &Transmission, manifest: &Manifest, log: &[LogEntry]) -> (Decision, bool) {
    let trace = record.provenance.trace_id.as_str();

    // Exact (trace, position) redelivery: re-emit the logged outcome.
    if let Some(prior) = log.iter().find(|e| e.trace_id == trace && e.trigger_pos == record.pos) {
        return (prior.decision.clone(), true);
    }

    // Receiver-side judgement: the sender fails the policy — refuse,
    // identically for every rejected sender.
    if !accepts(manifest, &record.sender) {
        return (
            Decision {
                outcome_intent: crate::seams::envelope::intent::REFUSE.into(),
                body: record.payload.body.clone(),
                cost: 0,
            },
            false,
        );
    }

    // This patch has already emitted on this trace: hold (coast).
    let already_emitted = log.iter().any(|e| {
        e.trace_id == trace
            && e.decision.outcome_intent != crate::seams::envelope::intent::REFUSE
            && e.decision.outcome_intent != crate::seams::envelope::intent::HOLD
    });
    if already_emitted {
        return (
            Decision {
                outcome_intent: crate::seams::envelope::intent::HOLD.into(),
                body: record.payload.body.clone(),
                cost: manifest.cost,
            },
            false,
        );
    }

    // First crossing on this trace: relay it onward.
    (
        Decision {
            outcome_intent: manifest.emit_intent.clone(),
            body: record.payload.body.clone(),
            cost: manifest.cost,
        },
        false,
    )
}

fn accepts(manifest: &Manifest, sender: &str) -> bool {
    manifest.policy.accept_senders.iter().any(|s| s == "*" || s == sender)
}

// ---------------------------------------------------------------------------
// Persistence helpers (single write_all + flush per entry: a SIGKILL
// cannot tear an entry — see stream.rs for the same argument)
// ---------------------------------------------------------------------------

fn append_json_line<T: Serialize>(path: &Path, value: &T) -> std::io::Result<()> {
    use std::fs::OpenOptions;
    let mut f = OpenOptions::new().create(true).append(true).open(path)?;
    let mut line = serde_json::to_string(value).map_err(std::io::Error::other)?;
    line.push('\n');
    f.write_all(line.as_bytes())?;
    f.flush()
}

fn read_json_lines<T: serde::de::DeserializeOwned>(path: &Path) -> std::io::Result<Vec<T>> {
    let mut file = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
        Err(e) => return Err(e),
    };
    let mut text = String::new();
    file.read_to_string(&mut text)?;
    let lines: Vec<&str> = text.lines().filter(|l| !l.trim().is_empty()).collect();
    let mut out = Vec::new();
    for (i, line) in lines.iter().enumerate() {
        match serde_json::from_str(line) {
            Ok(v) => out.push(v),
            // A torn final line (power-loss window) is skipped — every
            // complete entry before it stands; anything bad mid-file is
            // corruption, not a torn tail, and refuses to load.
            Err(e) if i == lines.len() - 1 => {
                eprintln!("patch log {}: torn final line skipped ({e})", path.display())
            }
            Err(e) => {
                return Err(std::io::Error::other(format!("{}: bad line {}: {e}", path.display(), i)))
            }
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Child main
// ---------------------------------------------------------------------------

pub struct PatchPaths {
    pub manifest: PathBuf,
    pub active_log: PathBuf,
    pub ledger: PathBuf,
}

pub fn paths(run_dir: &Path, id: &str) -> PatchPaths {
    let dir = run_dir.join("patches").join(id);
    PatchPaths {
        manifest: dir.join("manifest.json"),
        active_log: dir.join("active-log.jsonl"),
        ledger: dir.join("ledger.jsonl"),
    }
}

/// Run the patch child. Exit codes: 0 clean, 2 enrolment/manifest
/// trouble, 3 runtime (protocol) trouble.
pub fn run_patch(id: &str, run_dir: &Path, step_delay_ms: u64) -> i32 {
    let paths = paths(run_dir, id);
    let manifest: Manifest = match std::fs::read_to_string(&paths.manifest) {
        Ok(text) => match serde_json::from_str(&text) {
            Ok(m) => m,
            Err(e) => {
                eprintln!("patch[{id}]: bad manifest {}: {e}", paths.manifest.display());
                return 2;
            }
        },
        Err(e) => {
            eprintln!("patch[{id}]: cannot read manifest {}: {e}", paths.manifest.display());
            return 2;
        }
    };

    let mut log: Vec<LogEntry> = match read_json_lines(&paths.active_log) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("patch[{id}]: cannot read active log: {e}");
            return 2;
        }
    };
    let ledger: Vec<LedgerEntry> = match read_json_lines(&paths.ledger) {
        Ok(l) => l,
        Err(e) => {
            eprintln!("patch[{id}]: cannot read ledger: {e}");
            return 2;
        }
    };

    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    let ready = PatchToRuntime::Ready {
        id: id.to_string(),
        log_len: log.len() as u64,
        ledger_len: ledger.len() as u64,
    };
    if write_line(&mut out, &ready).is_err() {
        return 3;
    }

    let mut ledger_seq = ledger.len() as u64;
    let stdin = std::io::stdin();
    let mut lines = stdin.lock().lines();
    while let Some(Ok(line)) = lines.next() {
        let msg: RuntimeToPatch = match serde_json::from_str(&line) {
            Ok(m) => m,
            Err(e) => {
                eprintln!("patch[{id}]: undecodable delivery: {e}");
                return 3;
            }
        };
        let RuntimeToPatch::Deliver { record } = msg;

        if step_delay_ms > 0 {
            std::thread::sleep(std::time::Duration::from_millis(step_delay_ms));
        }

        let (decision, redelivery) = decide(&record, &manifest, &log);
        let seq = log.len() as u64;
        let entry = LogEntry {
            seq,
            trace_id: record.provenance.trace_id.clone(),
            trigger_pos: record.pos,
            sender: record.sender.clone(),
            intent: record.intent.clone(),
            redelivery,
            decision: decision.clone(),
        };
        // Durable log first, reply second: a kill in between leaves the
        // reading on disk, and the redelivery path re-emits it.
        if append_json_line(&paths.active_log, &entry).is_err() {
            eprintln!("patch[{id}]: cannot append active log");
            return 3;
        }
        log.push(entry);

        let refused = decision.outcome_intent == crate::seams::envelope::intent::REFUSE;
        let ledger_entry = LedgerEntry {
            seq: ledger_seq,
            kind: if refused { LEDGER_REFUSED.into() } else { LEDGER_RECEIVE.into() },
            pos: record.pos,
            counterparty: record.sender.clone(),
            intent: record.intent.clone(),
            payload_ref: None,
            cost: None,
        };
        if append_json_line(&paths.ledger, &ledger_entry).is_err() {
            eprintln!("patch[{id}]: cannot append ledger");
            return 3;
        }
        ledger_seq += 1;
        if !refused {
            // The SEND side of the crossing: what this patch emitted,
            // keyed to the envelope it answers (verify matches on
            // (pos, intent, payload_ref, cost)).
            let send = LedgerEntry {
                seq: ledger_seq,
                kind: LEDGER_SEND.into(),
                pos: record.pos,
                counterparty: record.sender.clone(),
                intent: decision.outcome_intent.clone(),
                payload_ref: Some(payload_ref(&emitted_payload(&decision))),
                cost: Some(decision.cost),
            };
            if append_json_line(&paths.ledger, &send).is_err() {
                eprintln!("patch[{id}]: cannot append ledger");
                return 3;
            }
            ledger_seq += 1;
        }

        let outcome = Outcome {
            intent: decision.outcome_intent.clone(),
            payload: emitted_payload(&decision),
            cost: decision.cost,
        };
        if write_line(&mut out, &PatchToRuntime::Outcome(outcome)).is_err() {
            return 3;
        }
    }

    0 // stdin EOF: the supervisor closed the seam
}

/// The payload a patch emits: the body it decided on, nothing else —
/// the source event's identifying fields stay on the ingest record that
/// minted them; downstream records point back through `causal_pos`.
fn emitted_payload(decision: &Decision) -> Payload {
    Payload { body: decision.body.clone(), ..Default::default() }
}

fn write_line<T: Serialize>(out: &mut impl Write, value: &T) -> std::io::Result<()> {
    let mut line = serde_json::to_string(value).map_err(std::io::Error::other)?;
    line.push('\n');
    out.write_all(line.as_bytes())?;
    out.flush()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::manifold::{Policy, Trigger, ROLE_PATCH};
    use crate::seams::envelope::{SpanContext, INGEST, UNROUTED};

    fn manifest(id: &str, accept: &[&str], emit: &str) -> Manifest {
        Manifest {
            id: id.into(),
            role: ROLE_PATCH.into(),
            triggers: vec![Trigger { sender: INGEST.into(), intent: "ping".into() }],
            policy: Policy { accept_senders: accept.iter().map(|s| s.to_string()).collect() },
            emit_intent: emit.into(),
            cost: 3,
        }
    }

    fn record(pos: u64, sender: &str, intent: &str, trace: &str) -> Transmission {
        let payload = Payload { body: "fleet".into(), ..Default::default() };
        Transmission {
            pos,
            sender: sender.into(),
            receiver: UNROUTED.into(),
            intent: intent.into(),
            payload_ref: payload_ref(&payload),
            payload,
            provenance: SpanContext {
                trace_id: trace.into(),
                span_id: format!("span{pos}"),
                parent_span_id: None,
            },
            causal_pos: None,
            cost: 0,
        }
    }

    fn log_entry(trace: &str, trigger_pos: u64, outcome_intent: &str) -> LogEntry {
        LogEntry {
            seq: 0,
            trace_id: trace.into(),
            trigger_pos,
            sender: INGEST.into(),
            intent: "ping".into(),
            redelivery: false,
            decision: Decision { outcome_intent: outcome_intent.into(), body: "fleet".into(), cost: 3 },
        }
    }

    #[test]
    fn first_crossing_relays_with_declared_cost() {
        let m = manifest("alpha", &["*"], "relay");
        let r = record(0, INGEST, "ping", "t1");
        let (d, redelivery) = decide(&r, &m, &[]);
        assert_eq!(d.outcome_intent, "relay");
        assert_eq!(d.cost, 3, "declared cost rides the outcome");
        assert!(!redelivery);
    }

    #[test]
    fn rejected_sender_is_refused_at_zero_cost() {
        let m = manifest("strict", &["alpha"], "relay");
        let r = record(0, "dj", "amplify", "t9");
        let (d, _) = decide(&r, &m, &[]);
        assert_eq!(d.outcome_intent, "refuse");
        assert_eq!(d.cost, 0);
    }

    #[test]
    fn second_crossing_on_same_trace_holds() {
        let m = manifest("alpha", &["*"], "relay");
        let log = vec![log_entry("t1", 0, "relay")];
        let r = record(4, "beta", "relay", "t1"); // new envelope, same trace
        let (d, redelivery) = decide(&r, &m, &log);
        assert_eq!(d.outcome_intent, "hold");
        assert!(!redelivery, "new envelope on the trace, not a redelivery");
    }

    #[test]
    fn exact_redelivery_reemits_the_logged_outcome() {
        let m = manifest("alpha", &["*"], "relay");
        let logged = log_entry("t1", 0, "relay");
        let r = record(0, INGEST, "ping", "t1");
        let (d, redelivery) = decide(&r, &m, &[logged.clone()]);
        assert_eq!(d, logged.decision, "re-emits what the log already booked");
        assert!(redelivery);
    }
}
