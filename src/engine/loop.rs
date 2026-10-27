//! `engine/loop` — the trigger loop (ARCHITECTURE.md): consume the
//! event stream, match events against patches' declared trigger
//! conditions, wake the matching patch processes, append outcomes to
//! the stream. Event-driven: there is no polling interval and no
//! scheduler that assigns work a patch didn't subscribe to — the only
//! things that move the loop forward are a source event arriving and an
//! outcome coming back from a woken patch. (Delivery timeouts and
//! child-exit checks are supervision, not scheduling: they guard the
//! seam, they never originate work.)
//!
//! Foundation mapping: ledger-side. The loop moves transmissions and
//! appends their outcomes; it never reads any patch's active log. Every
//! decision it makes (routing, the next pending delivery) is re-derived
//! from the stream plus the enrolled manifests, which is what makes a
//! restarted runtime resume with nothing lost and nothing duplicated:
//!
//! - an ingest record is one whose payload carries `source_index`;
//!   the run's input coverage is exactly those indices, so a restart
//!   re-derives how far ingestion got (cross-checked against the
//!   persistent `ingest` tailer, `stream::Tailer`);
//! - a routed record (receiver ≠ unrouted) is *pending* while no record
//!   with `causal_pos` = its position exists; a restart re-derives the
//!   pending set and re-delivers exactly it, at least once, and the
//!   patch's log-idempotence collapses the redelivery.
//!
//! Cascades are depth-first (a triggered record is fully answered
//! before the next source event is ingested), so the record layout is a
//! deterministic function of (input, manifests) — which is what
//! playback's event-for-event diff relies on.

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::time::Duration;

use serde::Serialize;
use serde_json::Value;

use crate::core::manifold::{Manifold, Manifest};
use crate::engine::compiler;
use crate::nodes::patch::Outcome;
use crate::nodes::sovereign::Sovereign;
use crate::seams::envelope::{payload_ref, SpanContext, Transmission};
use crate::stream::{self, Stream, Tailer};

#[derive(Clone, Debug)]
pub struct LoopConfig {
    pub run_dir: PathBuf,
    pub input_path: PathBuf,
    pub manifest_dir: PathBuf,
    /// Test pacing handed to patch children (ms per wake). Zero in
    /// production runs.
    pub step_delay_ms: u64,
    /// Supervision delivery timeout (ms).
    pub delivery_timeout_ms: u64,
}

#[derive(Serialize, Clone, Debug, PartialEq)]
pub struct RunSummary {
    pub records: u64,
    pub resumed_from: u64,
    pub unrouted: u64,
    pub wakes: BTreeMap<String, u64>,
    pub restarts: BTreeMap<String, u64>,
    pub refused: BTreeMap<String, u64>,
}

/// The whole run: enrol, resume if the stream exists, drive to quiescence.
pub fn run(config: &LoopConfig) -> Result<RunSummary, String> {
    std::fs::create_dir_all(&config.run_dir).map_err(|e| format!("run-dir: {e}"))?;

    // --- enrolment (engine::compiler admits each manifest) -----------
    let (manifold, _manifests) = enroll(&config.manifest_dir, &config.run_dir)?;

    // --- stream + resume state ---------------------------------------
    let stream_path = config.run_dir.join("stream.jsonl");
    let mut stream_file = Stream::open(&stream_path).map_err(|e| e.to_string())?;
    let mut records = stream::read_all(&stream_path).map_err(|e| e.to_string())?;
    let resumed_from = records.len() as u64;

    // Children of each routed record, keyed by parent position.
    let mut answered: BTreeMap<u64, Transmission> = BTreeMap::new();
    for record in &records {
        if let Some(parent) = record.causal_pos {
            if answered.insert(parent, record.clone()).is_some() {
                return Err(format!(
                    "stream corrupt: position {parent} has more than one answering record"
                ));
            }
        }
    }

    // Input coverage from the stream alone; cross-checked with the
    // persistent named tailer.
    let ingested: BTreeSet<u64> = records
        .iter()
        .filter_map(|r| r.payload.source_index)
        .collect();
    let mut ingest_tailer =
        Tailer::open(&config.run_dir.join("tailers"), "ingest").map_err(|e| e.to_string())?;
    let mut cursor = ingested.iter().next_back().map(|max| max + 1).unwrap_or(0);
    if ingest_tailer.next > cursor {
        return Err(format!(
            "ingest tailer is at {} but the stream only covers {} source events — \
             the tailer file points past the stream; refusing to silently skip input",
            ingest_tailer.next, cursor
        ));
    }
    if ingest_tailer.next < cursor {
        // Crash window between append and tailer commit: catch up.
        ingest_tailer.commit(cursor - 1).map_err(|e| e.to_string())?;
    }

    let source = load_source(&config.input_path)?;

    // --- sovereigns ---------------------------------------------------
    let exe = std::env::current_exe().map_err(|e| format!("current_exe: {e}"))?;
    let mut sovereigns: BTreeMap<String, Sovereign> = BTreeMap::new();
    for id in manifold.ids() {
        let mut s = Sovereign::new(
            &id,
            exe.clone(),
            config.run_dir.clone(),
            config.step_delay_ms,
            Duration::from_millis(config.delivery_timeout_ms),
        );
        s.start().map_err(|e| format!("start patch {id}: {e}"))?;
        sovereigns.insert(id, s);
    }

    // --- the loop ------------------------------------------------------
    let mut summary = RunSummary {
        records: records.len() as u64,
        resumed_from,
        unrouted: records.iter().filter(|r| !r.is_routed()).count() as u64,
        wakes: BTreeMap::new(),
        restarts: BTreeMap::new(),
        refused: BTreeMap::new(),
    };

    loop {
        // 1. Any routed record still unanswered? Deliver it (lowest
        //    position first — deterministic).
        let pending = records
            .iter()
            .filter(|r| r.is_routed() && !answered.contains_key(&r.pos))
            .map(|r| r.clone())
            .next();
        if let Some(trigger) = pending {
            let patch_id = trigger.receiver.clone();
            let sovereign = sovereigns
                .get_mut(&patch_id)
                .ok_or_else(|| format!("record {} routed to unknown patch {patch_id}", trigger.pos))?;
            let (outcome, _restarts_incurred) = sovereign
                .deliver(&trigger)
                .map_err(|e| format!("delivery to {patch_id} (record {}): {e}", trigger.pos))?;
            // Restart accounting reads the supervisor's own counter — a
            // restart can also happen in ensure_alive() before the
            // delivery proper, which deliver()'s return does not count.
            *summary.restarts.entry(patch_id.clone()).or_insert(0) =
                sovereign.restart_count;
            *summary.wakes.entry(patch_id.clone()).or_insert(0) += 1;
            if outcome.intent == crate::seams::envelope::intent::REFUSE {
                *summary.refused.entry(patch_id.clone()).or_insert(0) += 1;
            }
            let record =
                outcome_record(&manifold, records.len() as u64, &patch_id, &trigger, &outcome);
            append(&mut stream_file, &mut records, &mut answered, record)?;
            continue;
        }

        // 2. Nothing pending: ingest the next source event, if any.
        if (cursor as usize) < source.len() {
            let event = &source[cursor as usize];
            let index = cursor;
            let mut record = Transmission::from_source(records.len() as u64, index, event);
            record.receiver = manifold.route(&record.sender, &record.intent);
            append(&mut stream_file, &mut records, &mut answered, record)?;
            ingest_tailer.commit(index).map_err(|e| e.to_string())?;
            cursor += 1;
            continue;
        }

        // 3. Quiescent: every source event ingested, every routed
        //    record answered. Nothing may poll for more.
        break;
    }

    for (_, mut sovereign) in sovereigns {
        sovereign.shutdown();
    }

    summary.records = records.len() as u64;
    summary.unrouted = records.iter().filter(|r| !r.is_routed()).count() as u64;
    Ok(summary)
}

fn append(
    stream_file: &mut Stream,
    records: &mut Vec<Transmission>,
    answered: &mut BTreeMap<u64, Transmission>,
    record: Transmission,
) -> Result<(), String> {
    let pos = stream_file.append(record.clone()).map_err(|e| e.to_string())?;
    if let Some(parent) = record.causal_pos {
        answered.insert(parent, record.clone());
    }
    records.push(record);
    debug_assert_eq!(pos, records.len() as u64 - 1);
    Ok(())
}

/// Build the outcome record for a delivery: same trace as the trigger,
/// fresh span, `causal_pos` = trigger position, receiver = whatever the
/// manifests route the emitted intent to (or unrouted). Carries the
/// patch's declared cost — the provenance chain plus cost is BUILD-BRIEF
/// acceptance criterion 2.
fn outcome_record(
    manifold: &Manifold,
    pos: u64,
    patch_id: &str,
    trigger: &Transmission,
    outcome: &Outcome,
) -> Transmission {
    let mut provenance = SpanContext::outcome(&trigger.provenance.trace_id, pos, patch_id);
    provenance.parent_span_id = Some(trigger.provenance.span_id.clone());
    let receiver = manifold.route(patch_id, &outcome.intent);
    Transmission {
        pos,
        sender: patch_id.to_string(),
        receiver,
        intent: outcome.intent.clone(),
        payload_ref: payload_ref(&outcome.payload),
        payload: outcome.payload.clone(),
        provenance,
        causal_pos: Some(trigger.pos),
        cost: outcome.cost,
    }
}

fn enroll(manifest_dir: &PathBuf, run_dir: &PathBuf) -> Result<(Manifold, Vec<Manifest>), String> {
    let dir = std::fs::read_dir(manifest_dir)
        .map_err(|e| format!("manifest dir {}: {e}", manifest_dir.display()))?;
    let mut paths: Vec<PathBuf> = dir
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().map(|x| x == "json").unwrap_or(false))
        .collect();
    paths.sort(); // deterministic enrolment order

    // Read everything first, then validate each manifest against the
    // full id set (trigger senders may point at patches enrolled in any
    // file order).
    let mut manifests = Vec::new();
    for path in &paths {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("manifest {}: {e}", path.display()))?;
        let manifest: Manifest =
            serde_json::from_str(&text).map_err(|e| format!("manifest {}: {e}", path.display()))?;
        manifests.push((manifest, text));
    }
    if manifests.is_empty() {
        return Err(format!("no manifests found in {}", manifest_dir.display()));
    }
    let all_ids: Vec<String> = manifests.iter().map(|(m, _)| m.id.clone()).collect();

    let mut manifold = Manifold::new();
    for (manifest, text) in &manifests {
        let peers: Vec<String> =
            all_ids.iter().filter(|id| **id != manifest.id).cloned().collect();
        compiler::validate(manifest, &peers).map_err(|problems| {
            format!("manifest for '{}' refused: {}", manifest.id, problems.join("; "))
        })?;
        // Copy into the run dir so the run is self-contained (verify
        // re-derives routing from these, not from the source manifest dir).
        let dest = run_dir.join("patches").join(&manifest.id);
        std::fs::create_dir_all(&dest).map_err(|e| format!("run-dir patches: {e}"))?;
        std::fs::write(dest.join("manifest.json"), text)
            .map_err(|e| format!("copy manifest for '{}': {e}", manifest.id))?;
        manifold.enrol(manifest.clone());
    }
    Ok((manifold, manifests.into_iter().map(|(m, _)| m).collect()))
}

fn load_source(path: &PathBuf) -> Result<Vec<crate::seams::envelope::SourceEvent>, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("input {}: {e}", path.display()))?;
    let mut events = Vec::new();
    for (n, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let event: crate::seams::envelope::SourceEvent = serde_json::from_str(line)
            .map_err(|e| format!("input {} line {}: {e}", path.display(), n + 1))?;
        events.push(event);
    }
    if events.is_empty() {
        return Err(format!("input {} contains no events", path.display()));
    }
    Ok(events)
}

/// Format a run summary as one JSON line on stdout (the run's only
/// stdout output; tests and the report parse it).
pub fn print_summary(summary: &RunSummary) {
    let value = serde_json::to_value(summary).unwrap_or(Value::Null);
    println!("{value}");
}
