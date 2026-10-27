//! `nodes/sovereign` — the patch process wrapper (ARCHITECTURE.md,
//! settled): spawn, isolate, deliver an envelope, collect the outcome,
//! restart on crash.
//!
//! Semantics pulled from BEAM/OTP (FOUNDATIONS-APPLICATION, "PULL"):
//! per-process heaps (a patch is a real child OS process; nothing is
//! shared with it), messages are *copied* across the seam (the envelope
//! is serialized to the child's stdin — no shared memory, no reach-ins),
//! and supervision restarts a crashed child under a MaxR/MaxT
//! restart-intensity budget. Not pulled: distributed Erlang's
//! uniform-trust cluster machinery. The Rust realisation takes the
//! semantics, not the VM.
//!
//! Delivery is at-least-once (the unacknowledged-mailbox window): if the
//! child dies or the delivery times out after the envelope was written,
//! the supervisor restarts the child and delivers again. Patches are
//! idempotent against their own logs (`nodes::patch`: an exact
//! (trace, position) re-delivery re-emits the logged outcome), so
//! at-least-once delivery plus log-derived decisions converges without
//! duplicate stream records.

use std::io::{BufRead, BufReader, Write};
use std::process::{Child, ChildStdin, Command, Stdio};
use std::sync::mpsc::{channel, Receiver, RecvTimeoutError};
use std::time::{Duration, Instant};

use crate::nodes::patch::{self, Outcome, PatchToRuntime, RuntimeToPatch};
use crate::seams::envelope::Transmission;

/// MaxR: restarts allowed within the MaxT window before escalation.
const MAX_RESTARTS: usize = 5;
/// MaxT: the restart-intensity window.
const RESTART_WINDOW: Duration = Duration::from_secs(10);
/// How long a respawned child may take to say `ready`.
const READY_TIMEOUT: Duration = Duration::from_secs(10);
/// Backoff before a respawn (BEAM supervisors sleep between restarts).
const RESTART_BACKOFF: Duration = Duration::from_millis(50);

#[derive(Debug)]
pub enum SovereignError {
    RestartIntensityExceeded { id: String, restarts: usize, window_secs: u64 },
    Io(String),
    Protocol(String),
}

impl std::fmt::Display for SovereignError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SovereignError::RestartIntensityExceeded { id, restarts, window_secs } => write!(
                f,
                "patch '{id}': restart intensity exceeded ({restarts} restarts in {window_secs}s) — giving up"
            ),
            SovereignError::Io(m) => write!(f, "io: {m}"),
            SovereignError::Protocol(m) => write!(f, "protocol: {m}"),
        }
    }
}

/// One live child: its stdin (the delivery seam) and the channel fed by
/// its stdout reader thread.
struct ChildHandle {
    child: Child,
    stdin: ChildStdin,
    rx: Receiver<PatchToRuntime>,
    pid: u32,
}

impl ChildHandle {
    fn kill(mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// The supervisor for one enrolled patch.
pub struct Sovereign {
    id: String,
    exe: std::path::PathBuf,
    run_dir: std::path::PathBuf,
    step_delay_ms: u64,
    delivery_timeout: Duration,
    handle: Option<ChildHandle>,
    restart_instants: Vec<Instant>,
    /// Total restarts performed (reported in the run summary).
    pub restart_count: u64,
}

impl Sovereign {
    pub fn new(
        id: &str,
        exe: std::path::PathBuf,
        run_dir: std::path::PathBuf,
        step_delay_ms: u64,
        delivery_timeout: Duration,
    ) -> Sovereign {
        Sovereign {
            id: id.to_string(),
            exe,
            run_dir,
            step_delay_ms,
            delivery_timeout,
            handle: None,
            restart_instants: Vec::new(),
            restart_count: 0,
        }
    }

    /// Spawn the child and wait for its `ready` line. Public entry used
    /// by the engine loop at run start.
    pub fn start(&mut self) -> Result<(), SovereignError> {
        self.spawn_child()
    }

    fn spawn_child(&mut self) -> Result<(), SovereignError> {
        let stderr_path = self.run_dir.join(format!("patches/{}/stderr.log", self.id));
        if let Some(parent) = stderr_path.parent() {
            std::fs::create_dir_all(parent).map_err(|e| SovereignError::Io(e.to_string()))?;
        }
        let stderr_log = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(&stderr_path)
            .map_err(|e| SovereignError::Io(e.to_string()))?;

        let mut child = Command::new(&self.exe)
            .arg("patch")
            .arg("--id")
            .arg(&self.id)
            .arg("--run-dir")
            .arg(&self.run_dir)
            .arg("--step-delay-ms")
            .arg(self.step_delay_ms.to_string())
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::from(stderr_log))
            .spawn()
            .map_err(|e| SovereignError::Io(format!("spawn patch {}: {e}", self.id)))?;

        let pid = child.id();
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| SovereignError::Protocol("child stdout not piped".into()))?;
        let stdin = child
            .stdin
            .take()
            .ok_or_else(|| SovereignError::Protocol("child stdin not piped".into()))?;

        // Publish the pid so an operator (or the kill test) can address
        // the live child process directly.
        let _ = std::fs::write(self.run_dir.join(format!("patches/{}/pid", self.id)), pid.to_string());

        let (tx, rx) = channel();
        let reader_id = self.id.clone();
        std::thread::spawn(move || {
            let reader = BufReader::new(stdout);
            for line in reader.lines() {
                match line {
                    Ok(text) => {
                        let decoded: Result<PatchToRuntime, _> = serde_json::from_str(&text);
                        match decoded {
                            Ok(msg) => {
                                if tx.send(msg).is_err() {
                                    break; // supervisor went away
                                }
                            }
                            Err(e) => {
                                eprintln!("sovereign[{reader_id}]: bad child line: {e}: {text}");
                            }
                        }
                    }
                    Err(_) => break, // EOF / pipe gone: child exited
                }
            }
        });

        self.handle = Some(ChildHandle { child, stdin, rx, pid });

        // Wait for the handshake before accepting deliveries.
        match self.rx_recv(READY_TIMEOUT) {
            Ok(PatchToRuntime::Ready { id, .. }) if id == self.id => Ok(()),
            Ok(other) => Err(SovereignError::Protocol(format!(
                "patch {}: expected ready handshake, got {other:?}",
                self.id
            ))),
            Err(e) => Err(SovereignError::Protocol(format!(
                "patch {}: no ready handshake: {e}",
                self.id
            ))),
        }
    }

    fn rx_recv(&mut self, timeout: Duration) -> Result<PatchToRuntime, RecvTimeoutError> {
        self.handle
            .as_mut()
            .map(|h| h.rx.recv_timeout(timeout))
            .unwrap_or(Err(RecvTimeoutError::Disconnected))
    }

    /// Note a restart; escalate when MaxR restarts land inside MaxT.
    fn note_restart(&mut self) -> Result<(), SovereignError> {
        let now = Instant::now();
        self.restart_instants.retain(|t| now.duration_since(*t) < RESTART_WINDOW);
        if self.restart_instants.len() >= MAX_RESTARTS {
            return Err(SovereignError::RestartIntensityExceeded {
                id: self.id.clone(),
                restarts: self.restart_instants.len(),
                window_secs: RESTART_WINDOW.as_secs(),
            });
        }
        self.restart_instants.push(now);
        self.restart_count += 1;
        Ok(())
    }

    /// Restart the child: kill whatever remains, respect the intensity
    /// budget, back off, respawn.
    fn restart(&mut self) -> Result<(), SovereignError> {
        if let Some(handle) = self.handle.take() {
            handle.kill();
        }
        self.note_restart()?;
        std::thread::sleep(RESTART_BACKOFF);
        eprintln!(
            "sovereign[{}]: child crashed; restarting (restart #{})",
            self.id, self.restart_count
        );
        self.spawn_child()
    }

    /// Ensure a live child: restart if it exited while idle. Called
    /// before each delivery, so an idle-window crash is healed at the
    /// next trigger rather than discovered mid-write.
    fn ensure_alive(&mut self) -> Result<(), SovereignError> {
        let exited = match self.handle.as_mut() {
            None => true,
            Some(handle) => match handle.child.try_wait() {
                Ok(Some(_)) => true,
                Ok(None) => false,
                Err(_) => true,
            },
        };
        if exited {
            self.restart()?;
        }
        Ok(())
    }

    /// Deliver an envelope and collect the outcome, restarting the
    /// child as needed (at-least-once). Returns the outcome plus the
    /// number of restarts this one delivery incurred.
    pub fn deliver(&mut self, record: &Transmission) -> Result<(Outcome, u64), SovereignError> {
        let mut restarts_incurred = 0u64;
        let msg = RuntimeToPatch::Deliver { record: record.clone() };
        let mut line = serde_json::to_string(&msg)
            .map_err(|e| SovereignError::Protocol(e.to_string()))?;
        line.push('\n');
        loop {
            self.ensure_alive()?;
            let sent = self
                .handle
                .as_mut()
                .map(|h| h.stdin.write_all(line.as_bytes()).and_then(|_| h.stdin.flush()))
                .unwrap_or_else(|| Err(std::io::Error::other("no child handle")));

            match sent {
                Err(e) => {
                    eprintln!("sovereign[{}]: delivery write failed ({e}); restarting", self.id);
                    self.restart()?;
                    restarts_incurred += 1;
                    continue;
                }
                Ok(()) => {}
            }

            match self.rx_recv(self.delivery_timeout) {
                Ok(PatchToRuntime::Outcome(outcome)) => {
                    return Ok((outcome, restarts_incurred));
                }
                Ok(PatchToRuntime::Ready { .. }) => {
                    // A respawned child raced its ready line into this
                    // delivery's wait; it is live, just early. Wait on.
                    continue;
                }
                Err(RecvTimeoutError::Timeout) => {
                    eprintln!(
                        "sovereign[{}]: delivery timed out after {}ms; restarting",
                        self.id,
                        self.delivery_timeout.as_millis()
                    );
                    self.restart()?;
                    restarts_incurred += 1;
                    continue;
                }
                Err(RecvTimeoutError::Disconnected) => {
                    eprintln!("sovereign[{}]: child exited mid-delivery; restarting", self.id);
                    self.restart()?;
                    restarts_incurred += 1;
                    continue;
                }
            }
        }
    }

    /// Graceful shutdown: close the seam (stdin EOF is the child's stop
    /// signal), wait, then kill any straggler. Never escalates.
    pub fn shutdown(&mut self) {
        if let Some(mut handle) = self.handle.take() {
            drop(handle.stdin); // close the seam: EOF → child exits 0
            let _ = handle.child.wait();
        }
    }

    /// Current child pid, for diagnostics and tests.
    pub fn pid(&self) -> Option<u32> {
        self.handle.as_ref().map(|h| h.pid)
    }
}

// Re-exported for the engine loop's summary types.
pub use patch::Outcome as PatchOutcome;

#[cfg(test)]
mod tests {
    #[test]
    fn sovereign_constants_match_otp_shape() {
        // MaxR within MaxT; the numbers themselves are policy, the shape
        // (bounded restarts inside a sliding window) is the pulled
        // semantics. Asserting here keeps the policy visible.
        assert_eq!(super::MAX_RESTARTS, 5);
        assert_eq!(super::RESTART_WINDOW.as_secs(), 10);
    }
}
