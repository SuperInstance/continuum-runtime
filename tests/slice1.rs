//! Slice-1 integration tests. These drive the real binary end to end —
//! the runtime, its patch children as real OS processes, and the
//! verify/playback subcommands — against BUILD-BRIEF's acceptance
//! criteria:
//!
//! 1. `unmatched_event_wakes_nobody` — criterion 1 (wake exactly once;
//!    no-match event wakes nobody, errors nobody)
//! 2. `provenance_chain_and_declared_cost` — criterion 2 (outcome
//!    carries the trigger's provenance chain plus declared cost)
//! 3. `kill_patch_and_runtime_then_resume_loses_nothing` — criterion 3
//!    (the kill test, unattended: SIGKILL a patch child mid-run, SIGKILL
//!    the runtime, restart, and prove nothing is lost — stream complete
//!    by verify, patch logs prefix-stable, and the resumed stream
//!    playback-identical to a clean run's)
//! 4. `supervisor_restarts_killed_patch_without_runtime_restart` — the
//!    OTP supervisor semantics pulled per FOUNDATIONS-APPLICATION
//! 5. `playback_identical_for_two_clean_runs` /
//!    `playback_detects_tampering` — criterion 5's compliant reading:
//!    event-for-event diff, positionally checkable
//! 6. `boundary_driver_has_no_privilege` — criterion 4 (an envelope from
//!    the boundary driver is refused by a patch whose rules reject it,
//!    identically to any other sender)

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use continuum_runtime::seams::envelope::Transmission;

const BIN: &str = env!("CARGO_BIN_EXE_continuum-runtime");

/// The two kill tests pace a real run and must not fight each other for
/// the machine's attention (their kill windows are timing-based).
static TIMING_TESTS: Mutex<()> = Mutex::new(());

fn timing_guard() -> std::sync::MutexGuard<'static, ()> {
    TIMING_TESTS.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Count complete records (each ends with a newline); a final partial
/// line — the power-loss window a SIGKILL cannot normally produce — is
/// not a record yet.
fn complete_records(path: &Path) -> usize {
    let bytes = std::fs::read(path).unwrap_or_default();
    bytes.iter().filter(|&&b| b == b'\n').count()
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

struct RunFixture {
    root: PathBuf,
}

impl RunFixture {
    fn new(tag: &str) -> RunFixture {
        let root = std::env::temp_dir().join(format!(
            "cr-slice1-{}-{tag}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(root.join("manifests")).unwrap();
        std::fs::create_dir_all(root.join("run")).unwrap();
        RunFixture { root }
    }

    fn manifests_dir(&self) -> PathBuf {
        self.root.join("manifests")
    }

    fn run_dir(&self) -> PathBuf {
        self.root.join("run")
    }

    fn fresh_run_dir(&self, name: &str) -> PathBuf {
        let dir = self.root.join(name);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn write_manifest(&self, body: &str) {
        let value: serde_json::Value = serde_json::from_str(body).unwrap();
        let name = format!("{}.json", value["id"].as_str().unwrap());
        std::fs::write(self.manifests_dir().join(name), body).unwrap();
    }

    fn write_input(&self, name: &str, events: &[serde_json::Value]) -> PathBuf {
        let path = self.root.join(name);
        let mut text = String::new();
        for event in events {
            text.push_str(&event.to_string());
            text.push('\n');
        }
        std::fs::write(&path, text).unwrap();
        path
    }
}

fn event(tick: u64, channel: &str, intent: &str, velocity: f64) -> serde_json::Value {
    serde_json::json!([tick, channel, intent, velocity])
}

fn alpha_beta_manifests(f: &RunFixture) {
    f.write_manifest(
        r#"{"id":"alpha","role":"patch","triggers":[
             {"sender":"ingest","intent":"ping"},
             {"sender":"beta","intent":"relay"}],
           "policy":{"accept_senders":["*"]},
           "emit_intent":"relay","cost":2}"#,
    );
    f.write_manifest(
        r#"{"id":"beta","role":"patch","triggers":[
             {"sender":"alpha","intent":"relay"}],
           "policy":{"accept_senders":["*"]},
           "emit_intent":"relay","cost":3}"#,
    );
}

fn runtime_command(run_dir: &Path, input: &Path, manifests: &Path) -> Command {
    let mut cmd = Command::new(BIN);
    cmd.arg("run")
        .arg("--run-dir")
        .arg(run_dir)
        .arg("--input")
        .arg(input)
        .arg("--manifest-dir")
        .arg(manifests)
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    cmd
}

/// Run the runtime to completion; returns (exit-ok, summary JSON).
fn run_to_completion(run_dir: &Path, input: &Path, manifests: &Path) -> (bool, serde_json::Value) {
    let output = runtime_command(run_dir, input, manifests).output().unwrap();
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let ok = output.status.success();
    if !ok {
        eprintln!("stderr: {}", String::from_utf8_lossy(&output.stderr));
    }
    let summary = stdout
        .lines()
        .filter_map(|l| serde_json::from_str::<serde_json::Value>(l).ok())
        .next()
        .expect("run must print one JSON summary line");
    (ok, summary)
}

fn stream_of(run_dir: &Path) -> Vec<Transmission> {
    let path = run_dir.join("stream.jsonl");
    let text = std::fs::read_to_string(&path).unwrap_or_default();
    text.lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).unwrap())
        .collect()
}

fn lines_of(path: &Path) -> Vec<String> {
    let text = std::fs::read_to_string(path).unwrap_or_default();
    text.lines().filter(|l| !l.trim().is_empty()).map(String::from).collect()
}

fn verify(run_dir: &Path, input: Option<&Path>) -> (bool, String) {
    let mut cmd = Command::new(BIN);
    cmd.arg("verify").arg("--run-dir").arg(run_dir);
    if let Some(input) = input {
        cmd.arg("--input").arg(input);
    }
    let output = cmd.output().unwrap();
    (
        output.status.success(),
        format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ),
    )
}

fn playback(a: &Path, b: &Path) -> (bool, String) {
    let output = Command::new(BIN)
        .arg("playback")
        .arg("--a")
        .arg(a)
        .arg("--b")
        .arg(b)
        .output()
        .unwrap();
    (
        output.status.success(),
        format!(
            "{}{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        ),
    )
}

fn wait_for_lines(path: &Path, at_least: usize, deadline: Duration) -> usize {
    let start = Instant::now();
    loop {
        let count = lines_of(path).len();
        if count >= at_least || start.elapsed() > deadline {
            return count;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
}

fn kill_hard(pid: u32) {
    let _ = Command::new("kill").arg("-9").arg(pid.to_string()).output();
}

fn wait_with_deadline(child: &mut Child, deadline: Duration) -> std::process::ExitStatus {
    let start = Instant::now();
    loop {
        match child.try_wait().unwrap() {
            Some(status) => return status,
            None if start.elapsed() > deadline => {
                let _ = child.kill();
                panic!("child outlived the deadline");
            }
            None => std::thread::sleep(Duration::from_millis(20)),
        }
    }
}

fn pings(n: u64) -> Vec<serde_json::Value> {
    (0..n).map(|i| event(i, "ingest", "ping", 0.5)).collect()
}

// ---------------------------------------------------------------------------
// criterion 1 — wake exactly once; no-match wakes nobody
// ---------------------------------------------------------------------------

#[test]
fn unmatched_event_wakes_nobody() {
    let f = RunFixture::new("trigger");
    // One patch only, so "no other process is started" is checkable
    // as "the only wake is alpha's, exactly once".
    f.write_manifest(
        r#"{"id":"alpha","role":"patch","triggers":[
             {"sender":"ingest","intent":"ping"}],
           "policy":{"accept_senders":["*"]},
           "emit_intent":"relay","cost":2}"#,
    );
    let input = f.write_input(
        "input.ctev",
        &[event(0, "ingest", "ping", 0.5), event(1, "ingest", "noise", 0.1)],
    );

    let (ok, summary) = run_to_completion(&f.run_dir(), &input, &f.manifests_dir());
    assert!(ok, "run must exit cleanly with an unmatched event present");

    let records = stream_of(&f.run_dir());
    assert_eq!(
        records.len(),
        3,
        "ping, its single outcome, and the inert noise event — nothing else"
    );

    let ping = &records[0];
    assert_eq!(ping.receiver, "alpha", "the matching event routes to alpha");
    let noise = &records[2];
    assert_eq!(noise.receiver, "unrouted", "the no-match event is unrouted");
    assert_eq!(noise.intent, "noise");
    assert!(
        !records.iter().any(|r| r.causal_pos == Some(1) || r.causal_pos == Some(2)),
        "unrouted records must wake nobody — no record answers either"
    );

    let wakes = summary["wakes"].as_object().unwrap();
    assert_eq!(wakes.len(), 1, "no other process is started");
    assert_eq!(wakes["alpha"].as_u64(), Some(1), "alpha wakes exactly once");

    let alpha_log = lines_of(&f.run_dir().join("patches/alpha/active-log.jsonl"));
    assert_eq!(alpha_log.len(), 1, "one wake entry, one entry only");

    let (verdict, report) = verify(&f.run_dir(), Some(&input));
    assert!(verdict, "verify must pass:\n{report}");
}

// ---------------------------------------------------------------------------
// criterion 2 — provenance chain + declared cost on outcomes
// ---------------------------------------------------------------------------

#[test]
fn provenance_chain_and_declared_cost() {
    let f = RunFixture::new("provenance");
    alpha_beta_manifests(&f);
    let input = f.write_input("input.ctev", &[event(0, "ingest", "ping", 0.5)]);

    let (ok, _) = run_to_completion(&f.run_dir(), &input, &f.manifests_dir());
    assert!(ok);

    let records = stream_of(&f.run_dir());
    assert_eq!(records.len(), 4, "ingest → alpha relay → beta relay → alpha hold");

    let root = &records[0];
    assert!(root.causal_pos.is_none(), "ingest record is the trace root");
    let trace = root.provenance.trace_id.clone();

    let expected = [
        ("alpha", "relay", "beta", 2u64),
        ("beta", "relay", "alpha", 3),
        ("alpha", "hold", "unrouted", 2),
    ];
    for (i, (sender, intent, receiver, cost)) in expected.iter().enumerate() {
        let record = &records[i + 1];
        assert_eq!(record.sender, *sender, "record {i}");
        assert_eq!(record.intent, *intent, "record {i}");
        assert_eq!(record.receiver, *receiver, "record {i}");
        assert_eq!(record.cost, *cost, "record {i} carries the sender's declared cost");
        assert_eq!(record.provenance.trace_id, trace, "record {i} stays on the trigger's trace");
        assert_eq!(
            record.causal_pos,
            Some(records[i].pos),
            "record {i} answers the record before it"
        );
        assert_eq!(
            record.provenance.parent_span_id.as_deref(),
            Some(records[i].provenance.span_id.as_str()),
            "record {i}'s parent span is the trigger's span"
        );
    }

    let (verdict, report) = verify(&f.run_dir(), Some(&input));
    assert!(verdict, "verify must pass:\n{report}");
}

// ---------------------------------------------------------------------------
// criterion 3 — the kill test (unattended)
// ---------------------------------------------------------------------------

#[test]
fn kill_patch_and_runtime_then_resume_loses_nothing() {
    let _guard = timing_guard();
    let f = RunFixture::new("kill");
    alpha_beta_manifests(&f);
    let n_pings = 60u64;
    let input = f.write_input("input.ctev", &pings(n_pings));
    let kill_run = f.fresh_run_dir("killrun");
    let clean_run = f.fresh_run_dir("cleanrun");

    // A clean reference run of the same input, first: playback will
    // hold the killed-and-resumed run to it, event for event.
    let (ok, _) = run_to_completion(&clean_run, &input, &f.manifests_dir());
    assert!(ok);
    let clean_records = stream_of(&clean_run);

    // The run to kill: paced by a per-wake delay so the kill lands
    // mid-run deterministically enough for an unattended check.
    let mut child = runtime_command(&kill_run, &input, &f.manifests_dir())
        .arg("--step-delay-ms")
        .arg("8")
        .spawn()
        .unwrap();
    let runtime_pid = child.id();

    let stream_path = kill_run.join("stream.jsonl");
    let lines_at_kill = wait_for_lines(&stream_path, 40, Duration::from_secs(30));
    assert!(
        lines_at_kill >= 40,
        "the run should be visibly mid-flight before the kill (saw {lines_at_kill})"
    );
    assert!(
        (lines_at_kill as usize) < clean_records.len(),
        "the kill must land mid-run, not at the end"
    );

    // THE KILL: the patch process first (mid-delivery window), then the
    // runtime itself — both SIGKILL, no chance to flush anything.
    let alpha_pid: u32 = std::fs::read_to_string(kill_run.join("patches/alpha/pid"))
        .unwrap()
        .trim()
        .parse()
        .expect("runtime publishes the live child pid");
    kill_hard(alpha_pid);
    kill_hard(runtime_pid);
    let _ = child.wait();

    // Snapshot everything durable the dead processes left behind.
    let stream_before = std::fs::read_to_string(&stream_path).unwrap();
    let complete_at_kill = complete_records(&stream_path);
    let alpha_before = std::fs::read(kill_run.join("patches/alpha/active-log.jsonl")).unwrap();
    let beta_before = std::fs::read(kill_run.join("patches/beta/active-log.jsonl")).unwrap();
    let alpha_before_lines = lines_of(&kill_run.join("patches/alpha/active-log.jsonl")).len();
    let beta_before_lines = lines_of(&kill_run.join("patches/beta/active-log.jsonl")).len();
    assert!(alpha_before_lines > 0 && beta_before_lines > 0, "both patches were mid-run");

    // RESTART the runtime on the same run dir. It must resume from the
    // stream + tailers alone, re-derive pending deliveries, and finish.
    // The exact resume point is the number of complete records in the
    // post-kill snapshot (the run may append a little between the poll
    // that triggered the kill and the SIGKILLs landing).
    assert!(complete_at_kill >= lines_at_kill);
    let (ok, summary) = run_to_completion(&kill_run, &input, &f.manifests_dir());
    assert!(ok, "resumed run must complete cleanly");
    assert_eq!(
        summary["resumed_from"].as_u64(),
        Some(complete_at_kill as u64),
        "the resumed runtime picks up exactly where the stream stands"
    );

    // (a) No stream events lost: full input coverage, every routed
    //     record answered, everything re-derives — this is `verify`.
    let (verdict, report) = verify(&kill_run, Some(&input));
    assert!(verdict, "verify over the killed-and-resumed run:\n{report}");

    // (b) No patch log entries lost: the snapshot is a byte prefix of
    //     the post-restart logs.
    let alpha_after = std::fs::read(kill_run.join("patches/alpha/active-log.jsonl")).unwrap();
    let beta_after = std::fs::read(kill_run.join("patches/beta/active-log.jsonl")).unwrap();
    assert!(
        alpha_after.starts_with(&alpha_before),
        "alpha's log must keep every pre-kill entry, byte for byte"
    );
    assert!(
        beta_after.starts_with(&beta_before),
        "beta's log must keep every pre-kill entry, byte for byte"
    );
    let alpha_after_lines = lines_of(&kill_run.join("patches/alpha/active-log.jsonl")).len();
    let beta_after_lines = lines_of(&kill_run.join("patches/beta/active-log.jsonl")).len();
    assert!(alpha_after_lines >= alpha_before_lines);
    assert!(beta_after_lines >= beta_before_lines);
    // Redeliveries may add log entries (an in-flight envelope whose
    // outcome was not yet appended is re-delivered after restart), but
    // they must be flagged as redeliveries in the patch's own log,
    // never silent duplicates.
    for log in ["alpha", "beta"] {
        for line in lines_of(&kill_run.join(format!("patches/{log}/active-log.jsonl"))) {
            let entry: serde_json::Value = serde_json::from_str(&line).unwrap();
            assert!(
                entry["redelivery"].is_boolean(),
                "every wake entry states whether it is a redelivery: {line}"
            );
        }
    }

    // (c) The strongest check: the killed-and-resumed run's stream is
    //     event-for-event identical to the clean reference run — the
    //     kill left no gap and no duplicate, positionally.
    let (same, detail) = playback(&kill_run, &clean_run);
    assert!(
        same,
        "the resumed stream must be playback-identical to the clean run:\n{detail}"
    );
    assert_eq!(stream_of(&kill_run).len(), clean_records.len());

    // The pre-kill prefix of the stream is also untouched.
    let stream_after = std::fs::read_to_string(&stream_path).unwrap();
    assert!(
        stream_after.starts_with(&stream_before),
        "records appended before the kill must stand unchanged"
    );
}

// ---------------------------------------------------------------------------
// supervisor restart without runtime restart (OTP semantics pulled)
// ---------------------------------------------------------------------------

#[test]
fn supervisor_restarts_killed_patch_without_runtime_restart() {
    let _guard = timing_guard();
    let f = RunFixture::new("supervise");
    alpha_beta_manifests(&f);
    let input = f.write_input("input.ctev", &pings(60));
    let kill_run = f.fresh_run_dir("killrun");
    let clean_run = f.fresh_run_dir("cleanrun");

    let (ok, _) = run_to_completion(&clean_run, &input, &f.manifests_dir());
    assert!(ok);

    let mut child = runtime_command(&kill_run, &input, &f.manifests_dir())
        .arg("--step-delay-ms")
        .arg("8")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();

    let stream_path = kill_run.join("stream.jsonl");
    wait_for_lines(&stream_path, 30, Duration::from_secs(30));

    // SIGKILL the patch child only; the runtime must notice, restart it
    // under MaxR/MaxT, redeliver, and carry the run to completion.
    let alpha_pid: u32 = std::fs::read_to_string(kill_run.join("patches/alpha/pid"))
        .unwrap()
        .trim()
        .parse()
        .unwrap();
    kill_hard(alpha_pid);

    let status = wait_with_deadline(&mut child, Duration::from_secs(60));
    assert!(status.success(), "the runtime survives the patch kill and finishes the run");

    let stdout = {
        let mut buf = String::new();
        if let Some(mut out) = child.stdout.take() {
            use std::io::Read;
            out.read_to_string(&mut buf).unwrap();
        }
        buf
    };
    let summary: serde_json::Value = stdout
        .lines()
        .filter_map(|l| serde_json::from_str(l).ok())
        .next()
        .expect("summary line");
    let restarts = summary["restarts"]["alpha"].as_u64().unwrap_or(0);
    assert!(restarts >= 1, "sovereign must have restarted the killed child (restarts: {restarts})");

    let (verdict, report) = verify(&kill_run, Some(&input));
    assert!(verdict, "verify must pass after supervisor restart:\n{report}");

    let (same, detail) = playback(&kill_run, &clean_run);
    assert!(same, "restart-and-redeliver must converge to the clean stream:\n{detail}");
}

// ---------------------------------------------------------------------------
// criterion 5 (playback, compliant reading)
// ---------------------------------------------------------------------------

#[test]
fn playback_identical_for_two_clean_runs() {
    let f = RunFixture::new("playback");
    alpha_beta_manifests(&f);
    let input = f.write_input(
        "input.ctev",
        &[event(0, "ingest", "ping", 0.5), event(1, "ingest", "noise", 0.1), event(2, "ingest", "ping", 0.9)],
    );
    let run_a = f.fresh_run_dir("a");
    let run_b = f.fresh_run_dir("b");

    let (ok_a, _) = run_to_completion(&run_a, &input, &f.manifests_dir());
    let (ok_b, _) = run_to_completion(&run_b, &input, &f.manifests_dir());
    assert!(ok_a && ok_b);

    let (same, detail) = playback(&run_a, &run_b);
    assert!(same, "two runs over the same input must be event-for-event identical:\n{detail}");
    assert!(detail.contains("identical"), "{detail}");
}

#[test]
fn playback_detects_tampering() {
    let f = RunFixture::new("tamper");
    alpha_beta_manifests(&f);
    let input = f.write_input("input.ctev", &pings(3));
    let run_a = f.fresh_run_dir("a");
    let run_b = f.fresh_run_dir("b");

    run_to_completion(&run_a, &input, &f.manifests_dir());
    run_to_completion(&run_b, &input, &f.manifests_dir());

    // Tamper with run B's record at position 1: flip the declared cost.
    let stream_path = run_b.join("stream.jsonl");
    let mut lines = lines_of(&stream_path);
    let mut record: serde_json::Value = serde_json::from_str(&lines[1]).unwrap();
    record["cost"] = serde_json::json!(999);
    lines[1] = record.to_string();
    let mut file = std::fs::File::create(&stream_path).unwrap();
    for line in &lines {
        writeln!(file, "{line}").unwrap();
    }
    drop(file);

    let (same, detail) = playback(&run_a, &run_b);
    assert!(!same, "a tampered stream must not play back clean");
    assert!(detail.contains("position 1"), "the divergence is reported positionally:\n{detail}");

    // And verify catches it too, from the run's own manifests (cost 999
    // contradicts alpha's declared cost).
    let (verdict, report) = verify(&run_b, None::<&Path>);
    assert!(!verdict, "verify must catch the tampered cost:\n{report}");
}

// ---------------------------------------------------------------------------
// criterion 4 — the boundary driver has no privilege
// ---------------------------------------------------------------------------

#[test]
fn boundary_driver_has_no_privilege() {
    let f = RunFixture::new("boundary");
    // `dj` is enrolled as a boundary driver: it may amplify (emit
    // `amplify`), and that is all — enrolment gives it no delivery path
    // a patch's receiver-side rules cannot refuse.
    f.write_manifest(
        r#"{"id":"dj","role":"boundary","triggers":[
             {"sender":"ingest","intent":"tempo"}],
           "policy":{"accept_senders":["*"]},
           "emit_intent":"amplify","cost":1}"#,
    );
    // `beta` relays pings as amplifications too, so strict sees the
    // same envelope from two different senders.
    f.write_manifest(
        r#"{"id":"beta","role":"patch","triggers":[
             {"sender":"ingest","intent":"ping"}],
           "policy":{"accept_senders":["*"]},
           "emit_intent":"amplify","cost":3}"#,
    );
    // `strict` triggers on amplifications from *either* sender — and
    // its policy accepts only alpha, who never speaks here. Both dj and
    // beta must be refused, identically.
    f.write_manifest(
        r#"{"id":"strict","role":"patch","triggers":[
             {"sender":"dj","intent":"amplify"},
             {"sender":"beta","intent":"amplify"}],
           "policy":{"accept_senders":["alpha"]},
           "emit_intent":"relay","cost":5}"#,
    );
    let input = f.write_input(
        "input.ctev",
        &[event(0, "ingest", "tempo", 0.5), event(1, "ingest", "ping", 0.7)],
    );

    let (ok, summary) = run_to_completion(&f.run_dir(), &input, &f.manifests_dir());
    assert!(ok);

    let records = stream_of(&f.run_dir());
    let refusals: Vec<&Transmission> =
        records.iter().filter(|r| r.intent == "refuse").collect();
    assert_eq!(refusals.len(), 2, "strict refuses both senders");
    assert!(
        refusals.iter().all(|r| r.sender == "strict"),
        "refusals come from the refusing patch"
    );
    let refused_senders: Vec<&str> = refusals
        .iter()
        .map(|r| records[r.causal_pos.unwrap() as usize].sender.as_str())
        .collect();
    assert!(refused_senders.contains(&"dj"), "the boundary driver is refused: {refused_senders:?}");
    assert!(refused_senders.contains(&"beta"), "an ordinary patch is refused the same way: {refused_senders:?}");
    assert!(
        refusals.iter().all(|r| r.cost == 0),
        "a refusal is a refusal, whatever the sender's role"
    );
    assert_eq!(
        summary["refused"]["strict"].as_u64(),
        Some(2),
        "both refusals booked to strict"
    );

    // The two refusal records differ *only* by their causal chain —
    // same shape, same intent, same zero cost. That is "identically to
    // any other sender".
    let r0 = refusals[0];
    let r1 = refusals[1];
    assert_eq!(r0.intent, r1.intent);
    assert_eq!(r0.cost, r1.cost);
    assert_eq!(r0.receiver, r1.receiver, "both refusals are terminal (unrouted)");

    // dj itself: woken once by the tempo event, refused nothing, and
    // never addressed strict by any path other than the stream.
    let dj_log = lines_of(&f.run_dir().join("patches/dj/active-log.jsonl"));
    assert_eq!(dj_log.len(), 1, "dj woke once and its amplification was its only act");

    let strict_ledger = lines_of(&f.run_dir().join("patches/strict/ledger.jsonl"));
    let refused_bookings = strict_ledger
        .iter()
        .filter(|l| l.contains("\"refused\""))
        .count();
    assert_eq!(refused_bookings, 2, "strict booked both refusals on its ledger side");

    let (verdict, report) = verify(&f.run_dir(), Some(&input));
    assert!(verdict, "verify must pass:\n{report}");
}

// ---------------------------------------------------------------------------
// resume sanity without killing anything (restart a finished run)
// ---------------------------------------------------------------------------

#[test]
fn rerunning_a_completed_run_is_a_noop() {
    let f = RunFixture::new("noop");
    alpha_beta_manifests(&f);
    let input = f.write_input("input.ctev", &pings(5));

    let (ok1, summary1) = run_to_completion(&f.run_dir(), &input, &f.manifests_dir());
    assert!(ok1);
    let (ok2, summary2) = run_to_completion(&f.run_dir(), &input, &f.manifests_dir());
    assert!(ok2);
    assert_eq!(
        summary1["records"], summary2["records"],
        "a completed run re-invoked appends nothing"
    );
    assert_eq!(summary2["resumed_from"], summary2["records"]);
    assert_eq!(summary2["wakes"].as_object().unwrap().len(), 0, "nothing wakes twice");

    let (verdict, report) = verify(&f.run_dir(), Some(&input));
    assert!(verdict, "{report}");
}
