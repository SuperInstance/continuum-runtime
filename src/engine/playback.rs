//! `engine/playback` — playback and positional re-derivation (the
//! Temporal discipline, FOUNDATIONS-APPLICATION "ADAPT"). The scaffold
//! did not name this module; it lives beside the loop because it checks
//! exactly what the loop derives. See SLICE1-REPORT.md.
//!
//! Two operations, both position-matched:
//!
//! - [`verify`] re-derives everything derivable from a run dir —
//!   routing, causal linkage, trace continuity, payload references,
//!   declared costs, input coverage, and the P5-lite pairing of stream
//!   crossings against the participating patches' own ledger sides and
//!   active logs — and reports each check positionally.
//! - [`diff_streams`] plays one recorded stream against another event
//!   for event. This is playback plus positionally checkable
//!   re-derivation, **not** deterministic re-execution: slice-1 patches
//!   happen to be deterministic (no model calls), so two runs over the
//!   same input diff clean — but the claim made and checked here is
//!   positional agreement, never replay of decisions.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::core::manifold::{Manifold, Manifest};
use crate::nodes::patch::{self, LedgerEntry, LogEntry};
use crate::seams::envelope::{payload_ref, Transmission};
use crate::stream;

/// One named check outcome; `pos` is the position the finding is about
/// (None for whole-run checks).
#[derive(Debug, Clone, PartialEq)]
pub struct Finding {
    pub check: &'static str,
    pub pos: Option<u64>,
    pub detail: String,
}

pub struct VerifyReport {
    pub records: u64,
    pub findings: Vec<Finding>,
}

impl VerifyReport {
    pub fn ok(&self) -> bool {
        self.findings.is_empty()
    }
}

/// Load the manifests a run was enrolled with (the run dir carries its
/// own copies, so verification needs nothing outside it).
pub fn load_run_manifests(run_dir: &Path) -> Result<Vec<Manifest>, String> {
    let patches_dir = run_dir.join("patches");
    let dir = std::fs::read_dir(&patches_dir)
        .map_err(|e| format!("{}: {e}", patches_dir.display()))?;
    let mut manifests = Vec::new();
    for entry in dir.filter_map(|e| e.ok()) {
        let manifest_path = entry.path().join("manifest.json");
        let Ok(text) = std::fs::read_to_string(&manifest_path) else {
            continue;
        };
        let manifest: Manifest = serde_json::from_str(&text)
            .map_err(|e| format!("{}: {e}", manifest_path.display()))?;
        manifests.push(manifest);
    }
    manifests.sort_by(|a, b| a.id.cmp(&b.id));
    Ok(manifests)
}

/// Run every positional check over a run dir. `input` (the source file)
/// is optional; when given, input coverage is checked too.
pub fn verify(run_dir: &Path, input: Option<&Path>) -> Result<VerifyReport, String> {
    let mut findings = Vec::new();

    let stream_path = run_dir.join("stream.jsonl");
    let records = match stream::read_all_strict(&stream_path) {
        Ok(r) => r,
        Err(e) => return Err(format!("stream unreadable: {e}")),
    };

    let manifests = load_run_manifests(run_dir)?;
    let mut manifold = Manifold::new();
    for m in &manifests {
        manifold.enrol(m.clone());
    }
    let cost_of: BTreeMap<&str, u64> =
        manifests.iter().map(|m| (m.id.as_str(), m.cost)).collect();

    // Children of each position (for uniqueness + unrouted silence).
    let mut children: BTreeMap<u64, Vec<&Transmission>> = BTreeMap::new();
    for record in &records {
        if let Some(parent) = record.causal_pos {
            children.entry(parent).or_default().push(record);
        }
    }

    for record in &records {
        let pos = record.pos;

        // payload reference re-derivation
        let reference = payload_ref(&record.payload);
        if reference != record.payload_ref {
            findings.push(Finding {
                check: "payload_ref",
                pos: Some(pos),
                detail: format!("record says {} but payload hashes to {}", record.payload_ref, reference),
            });
        }

        // routing re-derivation from the enrolled manifests
        let derived_receiver = manifold.route(&record.sender, &record.intent);
        if derived_receiver != record.receiver {
            findings.push(Finding {
                check: "routing",
                pos: Some(pos),
                detail: format!(
                    "receiver is {} but manifests route ({}, {}) to {}",
                    record.receiver, record.sender, record.intent, derived_receiver
                ),
            });
        }

        // declared cost
        if let Some(declared) = cost_of.get(record.sender.as_str()) {
            let expected = if record.intent == crate::seams::envelope::intent::REFUSE {
                0
            } else {
                *declared
            };
            if record.cost != expected {
                findings.push(Finding {
                    check: "declared_cost",
                    pos: Some(pos),
                    detail: format!("cost is {} but {} declares {}", record.cost, record.sender, expected),
                });
            }
        } else if record.cost != 0 {
            findings.push(Finding {
                check: "declared_cost",
                pos: Some(pos),
                detail: format!("sender {} is not an enrolled patch; cost should be 0", record.sender),
            });
        }

        // causal linkage and trace continuity
        match record.causal_pos {
            Some(parent_pos) => {
                let parent = records.get(parent_pos as usize);
                match parent {
                    None => findings.push(Finding {
                        check: "causal",
                        pos: Some(pos),
                        detail: format!("causal_pos {parent_pos} points past the stream"),
                    }),
                    Some(parent) => {
                        if parent_pos >= pos {
                            findings.push(Finding {
                                check: "causal",
                                pos: Some(pos),
                                detail: format!("causal_pos {parent_pos} not before own position"),
                            });
                        }
                        if record.provenance.trace_id != parent.provenance.trace_id {
                            findings.push(Finding {
                                check: "trace",
                                pos: Some(pos),
                                detail: format!(
                                    "trace {} does not continue parent trace {}",
                                    record.provenance.trace_id, parent.provenance.trace_id
                                ),
                            });
                        }
                        if record.provenance.parent_span_id.as_deref()
                            != Some(parent.provenance.span_id.as_str())
                        {
                            findings.push(Finding {
                                check: "span",
                                pos: Some(pos),
                                detail: "parent_span_id does not name the parent record's span".into(),
                            });
                        }
                    }
                }
            }
            None => {
                if record.provenance.parent_span_id.is_some() {
                    findings.push(Finding {
                        check: "span",
                        pos: Some(pos),
                        detail: "root record (no causal_pos) must not carry a parent span".into(),
                    });
                }
            }
        }
    }

    // Every routed record answered exactly once; unrouted records wake
    // nobody (BUILD-BRIEF criterion 1, from the stream side).
    for record in &records {
        let answers = children.get(&record.pos).map(Vec::as_slice).unwrap_or(&[]);
        if record.is_routed() {
            match answers.len() {
                1 => {}
                0 => findings.push(Finding {
                    check: "answer",
                    pos: Some(record.pos),
                    detail: format!(
                        "routed to {} but no record answers position {}",
                        record.receiver, record.pos
                    ),
                }),
                n => findings.push(Finding {
                    check: "answer",
                    pos: Some(record.pos),
                    detail: format!("position {} answered {n} times", record.pos),
                }),
            }
        } else if !answers.is_empty() {
            findings.push(Finding {
                check: "answer",
                pos: Some(record.pos),
                detail: "unrouted record must wake nobody".into(),
            });
        }
        for child in answers {
            if child.sender != record.receiver {
                findings.push(Finding {
                    check: "answer",
                    pos: Some(record.pos),
                    detail: format!(
                        "answered by {} but was routed to {}",
                        child.sender, record.receiver
                    ),
                });
            }
        }
    }

    // Input coverage: every source event ingested exactly once.
    let mut seen_indices: BTreeMap<u64, usize> = BTreeMap::new();
    for record in &records {
        if let Some(index) = record.payload.source_index {
            *seen_indices.entry(index).or_insert(0) += 1;
        }
    }
    for (index, count) in &seen_indices {
        if *count > 1 {
            findings.push(Finding {
                check: "coverage",
                pos: None,
                detail: format!("source event {index} ingested {count} times"),
            });
        }
    }
    if let Some(input_path) = input {
        let expected = count_source_events(input_path)?;
        for index in 0..expected {
            if !seen_indices.contains_key(&index) {
                findings.push(Finding {
                    check: "coverage",
                    pos: None,
                    detail: format!("source event {index} never ingested"),
                });
            }
        }
        for index in seen_indices.keys() {
            if *index >= expected {
                findings.push(Finding {
                    check: "coverage",
                    pos: None,
                    detail: format!("source event {index} is past the input's {expected} events"),
                });
            }
        }
    }

    // P5-lite: every crossing booked on both of the receiver's own
    // files — active log (wake) and ledger side (receive/refused, send).
    let logs = load_patch_logs(run_dir, &manifests)?;
    for record in &records {
        if !record.is_routed() {
            continue;
        }
        let answer = children
            .get(&record.pos)
            .and_then(|v| v.first().map(|r| (*r).clone()));
        let Some(answer) = answer else { continue };
        let Some((log, ledger)) = logs.get(&record.receiver) else {
            findings.push(Finding {
                check: "patch_log",
                pos: Some(record.pos),
                detail: format!("no active log/ledger for routed patch {}", record.receiver),
            });
            continue;
        };
        let woke = log.iter().any(|e| e.trigger_pos == record.pos && e.trace_id == answer.provenance.trace_id);
        if !woke {
            findings.push(Finding {
                check: "patch_log",
                pos: Some(record.pos),
                detail: format!(
                    "patch {} has no wake entry for position {} in its active log",
                    record.receiver, record.pos
                ),
            });
        }
        let booked_crossing = ledger.iter().any(|e| {
            e.pos == record.pos
                && (e.kind == patch::LEDGER_RECEIVE || e.kind == patch::LEDGER_REFUSED)
        });
        if !booked_crossing {
            findings.push(Finding {
                check: "ledger_pair",
                pos: Some(record.pos),
                detail: format!(
                    "patch {} booked no receive/refused entry for position {}",
                    record.receiver, record.pos
                ),
            });
        }
        if answer.intent != crate::seams::envelope::intent::REFUSE {
            let booked_send = ledger.iter().any(|e| {
                e.kind == patch::LEDGER_SEND
                    && e.pos == record.pos
                    && e.intent == answer.intent
                    && e.payload_ref.as_deref() == Some(answer.payload_ref.as_str())
                    && e.cost == Some(answer.cost)
            });
            if !booked_send {
                findings.push(Finding {
                    check: "ledger_pair",
                    pos: Some(record.pos),
                    detail: format!(
                        "patch {} booked no send entry matching its outcome at position {}",
                        answer.sender, answer.pos
                    ),
                });
            }
        }
    }

    Ok(VerifyReport { records: records.len() as u64, findings })
}

fn count_source_events(path: &Path) -> Result<u64, String> {
    let text = std::fs::read_to_string(path).map_err(|e| format!("input {}: {e}", path.display()))?;
    Ok(text.lines().filter(|l| !l.trim().is_empty()).count() as u64)
}

fn load_patch_logs(
    run_dir: &Path,
    manifests: &[Manifest],
) -> Result<BTreeMap<String, (Vec<LogEntry>, Vec<LedgerEntry>)>, String> {
    let mut out = BTreeMap::new();
    for manifest in manifests {
        let paths = patch::paths(run_dir, &manifest.id);
        let log: Vec<LogEntry> = read_lines_or_empty(&paths.active_log)?;
        let ledger: Vec<LedgerEntry> = read_lines_or_empty(&paths.ledger)?;
        out.insert(manifest.id.clone(), (log, ledger));
    }
    Ok(out)
}

fn read_lines_or_empty<T: serde::de::DeserializeOwned>(path: &PathBuf) -> Result<Vec<T>, String> {
    match std::fs::read_to_string(path) {
        Ok(text) => {
            let mut out = Vec::new();
            for line in text.lines().filter(|l| !l.trim().is_empty()) {
                out.push(serde_json::from_str(line).map_err(|e| {
                    format!("{}: {e}", path.display())
                })?);
            }
            Ok(out)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(Vec::new()),
        Err(e) => Err(format!("{}: {e}", path.display())),
    }
}

/// Playback: diff stream A against stream B event for event. Returns
/// the divergence report (empty = identical). The first differing
/// position is reported with both records; unequal lengths report the
/// first extra position and the surplus count.
pub fn diff_streams(run_a: &Path, run_b: &Path) -> Result<DiffReport, String> {
    for run in [run_a, run_b] {
        let path = run.join("stream.jsonl");
        if !path.exists() {
            return Err(format!("no stream to play back at {}", path.display()));
        }
    }
    let a = stream::read_all_strict(&run_a.join("stream.jsonl"))
        .map_err(|e| format!("run A: {e}"))?;
    let b = stream::read_all_strict(&run_b.join("stream.jsonl"))
        .map_err(|e| format!("run B: {e}"))?;
    let common = a.len().min(b.len());
    for index in 0..common {
        if a[index] != b[index] {
            return Ok(DiffReport {
                identical: false,
                records_a: a.len() as u64,
                records_b: b.len() as u64,
                first_divergence: Some(index as u64),
                detail: Some(format!(
                    "position {index}:\n  A: {}\n  B: {}",
                    serde_json::to_string(&a[index]).unwrap_or_default(),
                    serde_json::to_string(&b[index]).unwrap_or_default()
                )),
            });
        }
    }
    if a.len() != b.len() {
        return Ok(DiffReport {
            identical: false,
            records_a: a.len() as u64,
            records_b: b.len() as u64,
            first_divergence: Some(common as u64),
            detail: Some(format!(
                "identical for the first {common} records, then lengths differ ({} vs {})",
                a.len(),
                b.len()
            )),
        });
    }
    Ok(DiffReport {
        identical: true,
        records_a: a.len() as u64,
        records_b: b.len() as u64,
        first_divergence: None,
        detail: None,
    })
}

#[derive(Debug, Clone)]
pub struct DiffReport {
    pub identical: bool,
    pub records_a: u64,
    pub records_b: u64,
    pub first_divergence: Option<u64>,
    pub detail: Option<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn diff_of_missing_run_is_an_error_not_a_silent_pass() {
        let missing = std::env::temp_dir().join("cr-diff-definitely-missing-run");
        assert!(diff_streams(&missing, &missing).is_err());
    }
}
