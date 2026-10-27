//! `seams/envelope` — the transmission envelope (ARCHITECTURE.md,
//! settled): typed intent, timestamp/tick, provenance, declared cost.
//!
//! A [`Transmission`] is the one thing that crosses between processes.
//! It is a *consumer* of the chrono-tensor event format — the 4-tuple
//! `[time/tick, channel/agent, intent, velocity/pressure]` — which this
//! module ingests ([`SourceEvent`]) and carries whole inside the
//! envelope's payload, so the run stays reconstructible from the stream
//! alone (BUILD-BRIEF step 4).
//!
//! Provenance is an OTel-style span context ([`SpanContext`]): a
//! `trace_id` constant along a causal chain, a `span_id` unique per
//! record, and the parent record's span. Ids are derived
//! deterministically from content and stream position (see
//! [`SpanContext::ingest`] / [`SpanContext::outcome`]) — never from wall
//! clock, randomness, or process ids — so two runs over the same input
//! produce byte-identical streams and playback can diff them position by
//! position (Temporal's position-matched discipline, FOUNDATIONS-APPLICATION).

use serde::{Deserialize, Serialize};

/// Well-known sender for events ingested from the chrono-tensor source.
pub const INGEST: &str = "ingest";
/// Receiver recorded for a transmission no enrolled patch triggers on.
/// An unrouted record is booked on the stream but wakes nobody.
pub const UNROUTED: &str = "unrouted";

/// A node identity: a patch id, or one of [`INGEST`] / [`UNROUTED`].
pub type NodeId = String;

/// OTel-style trace id: 16 hex chars (FNV-1a 64, slice-1 width).
pub type TraceId = String;
/// OTel-style span id: 16 hex chars.
pub type SpanId = String;

/// Well-known intents (open set; manifests may declare others).
pub mod intent {
    /// Source event asking a patch to start a relay chain.
    pub const PING: &str = "ping";
    /// A patch relaying an envelope onward (the chain continues).
    pub const RELAY: &str = "relay";
    /// Terminal outcome: inside-band hold (coasting), chain ends — a
    /// patch holds when it has already emitted on this trace (its own
    /// active-log reading) or has nothing to relay.
    pub const HOLD: &str = "hold";
    /// Receiver-side refusal: the patch's rules reject this sender.
    pub const REFUSE: &str = "refuse";
    /// A boundary driver amplifying a pattern (same seam, no privilege).
    pub const AMPLIFY: &str = "amplify";
    /// Source event a boundary driver triggers on.
    pub const TEMPO: &str = "tempo";
}

/// Span-context provenance carried on every transmission.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct SpanContext {
    pub trace_id: TraceId,
    pub span_id: SpanId,
    /// Span of the record that caused this one; `None` only for
    /// records minted directly from a source event.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub parent_span_id: Option<SpanId>,
}

/// A chrono-tensor source event in the yard's own 4-tuple form:
/// `[time/tick, channel/agent, intent, velocity/pressure]`.
/// One JSON array per line in the input file.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct SourceEvent {
    pub tick: u64,
    pub channel: String,
    pub intent: String,
    pub velocity: f64,
}

/// The payload a transmission carries. Slice-1 shape: an opaque body
/// string (echoed along a relay chain); ingest records additionally
/// carry the source 4-tuple's identifying fields so the stream alone can
/// prove input coverage (`source_index`, `source_tick`,
/// `source_velocity`).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
pub struct Payload {
    pub body: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_index: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_tick: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_velocity: Option<f64>,
}

/// A transmission record on the append-only stream. Field order here is
/// the wire order; serde keeps it stable, which is what playback's
/// event-for-event diff relies on.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct Transmission {
    /// Sequence time (the negotiated clock): assigned by `stream::append`
    /// and identical to the record's position in the stream. Positions
    /// are the ordering; there is no wall clock anywhere in a record.
    pub pos: u64,
    pub sender: NodeId,
    /// The patch whose manifest triggers on (sender, intent), or
    /// [`UNROUTED`] when nothing matches. Routing is re-derivable from
    /// the enrolled manifests, so it stays positionally checkable.
    pub receiver: NodeId,
    /// Typed intent (open set; see [`intent`]).
    pub intent: String,
    pub payload: Payload,
    /// Content hash of the payload (FNV-1a 64, hex) — the "payload
    /// reference": records are compared and verified by reference, and
    /// `verify` recomputes it from the payload.
    pub payload_ref: String,
    pub provenance: SpanContext,
    /// Stream position of the record this one answers; `None` only for
    /// records minted directly from a source event.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub causal_pos: Option<u64>,
    /// Declared cost. Source-minted records carry 0; patch outcomes
    /// carry the patch's declared cost (refusals book 0).
    pub cost: u64,
}

impl Transmission {
    /// True when some patch was routed to receive this record.
    pub fn is_routed(&self) -> bool {
        self.receiver != UNROUTED
    }

    /// Build the ingest record for a source event. `pos` is the stream
    /// position it will occupy; the span is minted from content only.
    pub fn from_source(pos: u64, index: u64, event: &SourceEvent) -> Transmission {
        let payload = Payload {
            body: event.channel.clone(),
            source_index: Some(index),
            source_tick: Some(event.tick),
            source_velocity: Some(event.velocity),
        };
        Transmission {
            pos,
            sender: event.channel.clone(),
            receiver: UNROUTED.to_string(), // engine::loop re-routes on append
            intent: event.intent.clone(),
            payload_ref: payload_ref(&payload),
            payload,
            provenance: SpanContext::ingest(index, event),
            causal_pos: None,
            cost: 0,
        }
    }
}

impl SpanContext {
    /// Mint the root span of a causal chain from the source event's
    /// content and index alone. Deterministic across runs.
    pub fn ingest(index: u64, event: &SourceEvent) -> SpanContext {
        let trace_id = fnv_hex(&[
            b"trace\0",
            &index.to_le_bytes(),
            event.tick.to_le_bytes().as_slice(),
            event.channel.as_bytes(),
            b"\0",
            event.intent.as_bytes(),
            b"\0",
            &event.velocity.to_bits().to_le_bytes(),
        ]);
        let span_id = fnv_hex(&[b"span\0", trace_id.as_bytes(), b"\0ingest"]);
        SpanContext { trace_id, span_id, parent_span_id: None }
    }

    /// Mint the span of an outcome record: same trace as the record it
    /// answers, new span derived from (trace, position, sender).
    pub fn outcome(trace_id: &str, pos: u64, sender: &str) -> SpanContext {
        let span_id = fnv_hex(&[
            b"span\0",
            trace_id.as_bytes(),
            b"\0",
            &pos.to_le_bytes(),
            b"\0",
            sender.as_bytes(),
        ]);
        SpanContext { trace_id: trace_id.to_string(), span_id, parent_span_id: None }
    }
}

/// Payload reference: FNV-1a 64 over a canonical field layout, hex.
/// Not a security hash — a stable content reference (std's DefaultHasher
/// is not stability-guaranteed across releases, hence our own).
pub fn payload_ref(payload: &Payload) -> String {
    fnv_hex(&[
        b"payload\0",
        payload.body.as_bytes(),
        b"\0",
        &payload.source_index.map(|v| v.to_le_bytes()).unwrap_or([0xff; 8]),
        b"\0",
        &payload.source_tick.map(|v| v.to_le_bytes()).unwrap_or([0xff; 8]),
        b"\0",
        &payload
            .source_velocity
            .map(|v| v.to_bits().to_le_bytes())
            .unwrap_or([0xff; 8]),
    ])
}

/// FNV-1a 64-bit, hex-encoded (16 chars). Deterministic, dependency-free.
pub fn fnv_hex(parts: &[&[u8]]) -> String {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for part in parts {
        for &byte in *part {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
        }
    }
    format!("{hash:016x}")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fnv_matches_reference_vectors() {
        // Known FNV-1a 64 vectors (offset basis, "a", "foobar").
        assert_eq!(fnv_hex(&[b""]), "cbf29ce484222325");
        assert_eq!(fnv_hex(&[b"a"]), "af63dc4c8601ec8c");
        assert_eq!(fnv_hex(&[b"foobar"]), "85944171f73967e8");
    }

    #[test]
    fn payload_ref_is_stable_and_content_sensitive() {
        let p = Payload { body: "hello".into(), ..Default::default() };
        assert_eq!(payload_ref(&p), payload_ref(&p));
        let q = Payload { body: "hello!".into(), ..Default::default() };
        assert_ne!(payload_ref(&p), payload_ref(&q));
    }

    #[test]
    fn ingest_spans_are_deterministic_and_chain_outcomes_share_trace() {
        let ev = SourceEvent {
            tick: 7,
            channel: "ingest".into(),
            intent: "ping".into(),
            velocity: 0.5,
        };
        let a = SpanContext::ingest(0, &ev);
        let b = SpanContext::ingest(0, &ev);
        assert_eq!(a, b, "same content, same span");
        let c = SpanContext::ingest(1, &ev);
        assert_ne!(a.trace_id, c.trace_id, "different source index, different trace");

        let out = SpanContext::outcome(&a.trace_id, 1, "alpha");
        assert_eq!(out.trace_id, a.trace_id, "outcome stays on the causal trace");
        assert_ne!(out.span_id, a.span_id);
    }
}
