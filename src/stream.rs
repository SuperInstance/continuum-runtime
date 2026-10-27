//! `stream` — the append-only transmission stream.
//!
//! The one module the scaffold's map did not name; it lives at the top
//! level because the stream is the medium everything else crosses — it
//! is not runtime bookkeeping (`core`), not a process (`nodes`), not the
//! crossing itself (`seams`), not the loop (`engine`). See
//! SLICE1-REPORT.md.
//!
//! Mechanics (FOUNDATIONS-APPLICATION, "ADAPT — Chronicle Queue named
//! tailers", rebuilt in Rust, nothing vendored):
//!
//! - **Append-only.** One JSON record per line, opened with `O_APPEND`;
//!   each append is a single `write_all` of the complete line followed by
//!   a flush, so a `SIGKILL` anywhere cannot tear a record — the line is
//!   either fully in the file or absent. Record `pos` is written
//!   explicitly and must equal its line index; read-back checks it.
//! - **Positions are the ordering** (the negotiated/sequence clock, per
//!   the scaffold's design). Read-back is by position.
//! - **Named, persistent, read-without-consume tailers.** A [`Tailer`]
//!   keeps its own `next` position and persists it under the run dir, so
//!   a restarted consumer resumes exactly where its predecessor stopped.
//!
//! The stream is single-writer: only the engine loop appends. Patches
//! never touch it — they hold their own active logs and ledger sides
//! (`nodes::patch`), which is the P4/P5 split.

use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Read, Write};
use std::path::{Path, PathBuf};

use crate::seams::envelope::Transmission;

/// A record that could not be read back. `TornTail` covers a final
/// partial line (power-loss window; a SIGKILL cannot produce one given
/// single-`write_all` appends) — it is reported, and everything before
/// it stands.
#[derive(Debug)]
pub enum StreamError {
    TornTail { line_no: u64 },
    BadRecord { line_no: u64, cause: String },
    PosMismatch { line_no: u64, found: u64 },
    Io(std::io::Error),
}

impl std::fmt::Display for StreamError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StreamError::TornTail { line_no } => {
                write!(f, "torn final line at line {line_no} (partial write; ignored)")
            }
            StreamError::BadRecord { line_no, cause } => {
                write!(f, "unparseable record at line {line_no}: {cause}")
            }
            StreamError::PosMismatch { line_no, found } => write!(
                f,
                "pos field {found} does not match line index {line_no}"
            ),
            StreamError::Io(e) => write!(f, "io error: {e}"),
        }
    }
}

impl From<std::io::Error> for StreamError {
    fn from(e: std::io::Error) -> Self {
        StreamError::Io(e)
    }
}

/// An open append-only stream file.
pub struct Stream {
    path: PathBuf,
    file: File,
}

impl Stream {
    /// Open (creating if absent) the stream at `path` for appending.
    pub fn open(path: &Path) -> Result<Stream, StreamError> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(path)?;
        Ok(Stream { path: path.to_path_buf(), file })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Append a record at position = current length, stamping `pos`
    /// into the record. Single buffered-then-flushed write of the whole
    /// line; returns the position assigned.
    pub fn append(&mut self, mut record: Transmission) -> Result<u64, StreamError> {
        let pos = self.len()?;
        record.pos = pos;
        let mut line = serde_json::to_string(&record)
            .map_err(|e| StreamError::BadRecord { line_no: pos, cause: e.to_string() })?;
        line.push('\n');
        self.file.write_all(line.as_bytes())?;
        self.file.flush()?;
        Ok(pos)
    }

    /// Number of complete records currently in the file.
    pub fn len(&self) -> Result<u64, StreamError> {
        Ok(read_all(self.path.as_path())?.len() as u64)
    }

    pub fn is_empty(&self) -> Result<bool, StreamError> {
        Ok(self.len()? == 0)
    }
}

/// Read the whole stream back, positions 0..n, validating that each
/// record's `pos` equals its line index. A torn (unterminated) final
/// line stops the read and is reported inside the result: the records
/// before it are returned, and the caller decides whether a torn tail is
/// acceptable in context (`verify` fails on it; the engine loop, which
/// resumes from complete records only, tolerates it by construction —
/// it never re-appends what it can see).
pub fn read_all(path: &Path) -> Result<Vec<Transmission>, StreamError> {
    read_all_with_tail(path).map(|(records, tail)| {
        if let Some(err) = tail {
            // Surface the torn tail through a debug-side channel: the
            // records stand; callers wanting strictness use
            // `read_all_strict`.
            eprintln!("stream: {err}");
        }
        records
    })
}

/// Strict variant: any torn tail or bad record is an error.
pub fn read_all_strict(path: &Path) -> Result<Vec<Transmission>, StreamError> {
    match read_all_with_tail(path)? {
        (records, None) => Ok(records),
        (_, Some(err)) => Err(err),
    }
}

fn read_all_with_tail(path: &Path) -> Result<(Vec<Transmission>, Option<StreamError>), StreamError> {
    let mut file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok((Vec::new(), None)),
        Err(e) => return Err(e.into()),
    };
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)?;
    if bytes.is_empty() {
        return Ok((Vec::new(), None));
    }
    let torn = !bytes.ends_with(b"\n");
    let mut records = Vec::new();
    let mut line_no: u64 = 0;
    let segments: Vec<&[u8]> = bytes.split(|&b| b == b'\n').collect();
    for (i, raw) in segments.iter().enumerate() {
        if raw.is_empty() {
            continue;
        }
        // A non-terminated final segment is the torn tail: report it
        // as such rather than as unparseable.
        if torn && i == segments.len() - 1 {
            return Ok((records, Some(StreamError::TornTail { line_no })));
        }
        let rec: Transmission = match serde_json::from_slice(raw) {
            Ok(r) => r,
            Err(e) => {
                let err = StreamError::BadRecord { line_no, cause: e.to_string() };
                return if torn { Ok((records, Some(err))) } else { Err(err) };
            }
        };
        if rec.pos != line_no {
            let err = StreamError::PosMismatch { line_no, found: rec.pos };
            return if torn { Ok((records, Some(err))) } else { Err(err) };
        }
        records.push(rec);
        line_no += 1;
    }
    if torn {
        return Ok((records, Some(StreamError::TornTail { line_no })));
    }
    Ok((records, None))
}

/// A named, persistent, read-without-consume cursor over a line file
/// (the stream, the ingest source, or a patch log). Position survives
/// process death: it is persisted to `<dir>/<name>.pos` after every
/// advance, one small write + flush.
pub struct Tailer {
    pos_file: PathBuf,
    pub next: u64,
}

impl Tailer {
    /// Open (or resume) the tailer named `name` rooted at `dir`.
    pub fn open(dir: &Path, name: &str) -> Result<Tailer, StreamError> {
        std::fs::create_dir_all(dir)?;
        let pos_file = dir.join(format!("{name}.pos"));
        let next = match std::fs::read_to_string(&pos_file) {
            Ok(text) => text
                .trim()
                .parse::<u64>()
                .map_err(|e| StreamError::Io(std::io::Error::other(format!(
                    "bad tailer file {}: {e}",
                    pos_file.display()
                ))))?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => 0,
            Err(e) => return Err(e.into()),
        };
        Ok(Tailer { pos_file, next })
    }

    fn persist(&self) -> Result<(), StreamError> {
        let mut f = File::create(&self.pos_file)?;
        f.write_all(self.next.to_string().as_bytes())?;
        f.flush()?;
        Ok(())
    }

    /// Advance past record `pos`. Persists immediately.
    pub fn commit(&mut self, pos: u64) -> Result<(), StreamError> {
        self.next = pos + 1;
        self.persist()
    }
}

/// Read line `pos` (0-based) from a JSONL file, if present. Used by
/// tailers that consume one record at a time without loading the file.
pub fn read_line_as_json<T: serde::de::DeserializeOwned>(
    path: &Path,
    pos: u64,
) -> Result<Option<T>, StreamError> {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    let mut reader = BufReader::new(file);
    let mut line = String::new();
    let mut index: u64 = 0;
    loop {
        line.clear();
        let n = read_line(&mut reader, &mut line)?;
        if n == 0 {
            return Ok(None); // EOF before pos
        }
        if index == pos {
            if line.ends_with('\n') {
                line.pop();
            }
            if line.is_empty() {
                return Ok(None);
            }
            let value = serde_json::from_str(line.trim_end())
                .map_err(|e| StreamError::BadRecord { line_no: pos, cause: e.to_string() })?;
            return Ok(Some(value));
        }
        index += 1;
    }
}

/// BufRead::read_line without the dependency on the trait being in
/// scope for generic readers we construct here.
fn read_line<R: Read>(reader: &mut BufReader<R>, buf: &mut String) -> Result<usize, StreamError> {
    let mut bytes = Vec::new();
    loop {
        let available = reader.fill_buf()?;
        if available.is_empty() {
            break;
        }
        if let Some(newline) = available.iter().position(|&b| b == b'\n') {
            bytes.extend_from_slice(&available[..=newline]);
            reader.consume(newline + 1);
            break;
        }
        bytes.extend_from_slice(available);
        let len = available.len();
        reader.consume(len);
    }
    let text = String::from_utf8_lossy(&bytes);
    buf.push_str(&text);
    Ok(bytes.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::seams::envelope::{Payload, SpanContext, Transmission, UNROUTED};

    fn record(pos_hint: u64, body: &str) -> Transmission {
        let payload = Payload { body: body.into(), ..Default::default() };
        Transmission {
            pos: pos_hint,
            sender: "ingest".into(),
            receiver: UNROUTED.into(),
            intent: "ping".into(),
            payload_ref: crate::seams::envelope::payload_ref(&payload),
            payload,
            provenance: SpanContext {
                trace_id: "t".repeat(16),
                span_id: format!("{pos_hint:016x}"),
                parent_span_id: None,
            },
            causal_pos: None,
            cost: 0,
        }
    }

    fn tmpdir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "cr-stream-test-{}-{}-{tag}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn append_assigns_positions_and_read_back_roundtrips() {
        let dir = tmpdir("append");
        let path = dir.join("stream.jsonl");
        let mut s = Stream::open(&path).unwrap();
        assert_eq!(s.append(record(99, "a")).unwrap(), 0, "pos assigned by append");
        assert_eq!(s.append(record(99, "b")).unwrap(), 1);
        assert_eq!(s.append(record(99, "c")).unwrap(), 2);
        drop(s);
        let back = read_all_strict(&path).unwrap();
        assert_eq!(back.len(), 3);
        assert_eq!(back[0].payload.body, "a");
        assert_eq!(back[2].pos, 2);
        assert_eq!(back[1].payload.body, "b");
    }

    #[test]
    fn torn_tail_is_reported_and_prefix_stands() {
        let dir = tmpdir("torn");
        let path = dir.join("stream.jsonl");
        let mut s = Stream::open(&path).unwrap();
        s.append(record(0, "a")).unwrap();
        drop(s);
        // Simulate a torn final line (power-loss window).
        let mut f = OpenOptions::new().append(true).open(&path).unwrap();
        write!(f, "{{\"pos\":1,\"sender\":\"ing").unwrap();
        drop(f);

        let (records, tail) = read_all_with_tail(&path).unwrap();
        assert_eq!(records.len(), 1, "complete prefix stands");
        assert!(matches!(tail, Some(StreamError::TornTail { .. })));
        assert!(read_all_strict(&path).is_err(), "strict read refuses the torn tail");
    }

    #[test]
    fn tailer_persists_position_across_reopen() {
        let dir = tmpdir("tailer");
        let mut t = Tailer::open(&dir, "ingest").unwrap();
        assert_eq!(t.next, 0);
        t.commit(4).unwrap();
        drop(t);
        let t2 = Tailer::open(&dir, "ingest").unwrap();
        assert_eq!(t2.next, 5, "position resumed from disk");
    }
}
