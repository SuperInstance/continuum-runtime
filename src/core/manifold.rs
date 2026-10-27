//! `core/manifold` — the runtime's bookkeeping view of "where things
//! stand": which patches are enrolled and which (sender, intent) pairs
//! wake whom (ARCHITECTURE.md). This is an index the engine loop routes
//! by; it is **not** a shared state store patches read from — patches
//! hold their own active logs (P4). Foundation mapping: log-side
//! (derived) — routing here is re-derivable from the enrolled manifests,
//! which is what keeps stream records positionally checkable under
//! playback.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::seams::envelope::{NodeId, INGEST, UNROUTED};

/// A declared trigger condition: "wake me when a record with this
/// sender and this intent is on the stream."
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Trigger {
    pub sender: NodeId,
    pub intent: String,
}

/// Receiver-side policy: which senders this patch's rules accept. A
/// patch wakes for anything it triggers on, but its own judgement may
/// still refuse the envelope — the refusal is identical for every
/// rejected sender, boundary drivers included (BUILD-BRIEF criterion 4).
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Policy {
    /// Accepted sender ids, or `["*"]` for any.
    pub accept_senders: Vec<NodeId>,
}

/// A patch's enrolment manifest, validated by `engine::compiler` before
/// it joins the run.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Manifest {
    pub id: NodeId,
    /// `"patch"` or `"boundary"` (a boundary driver enrols like any
    /// patch and gets no extra delivery path — role is bookkeeping).
    pub role: String,
    pub triggers: Vec<Trigger>,
    pub policy: Policy,
    /// Intent this patch emits when it relays (default vocabulary:
    /// `relay`; a boundary driver's amplification is just its emit
    /// intent, e.g. `amplify`).
    pub emit_intent: String,
    /// Declared cost booked on every non-refusal outcome.
    pub cost: u64,
}

pub const ROLE_PATCH: &str = "patch";
pub const ROLE_BOUNDARY: &str = "boundary";

/// The enrolment index: enrolled manifests by patch id, plus the
/// routing table built from their triggers. Deterministic ordering
/// everywhere (BTreeMap; ties broken by patch id) so routing is a pure
/// function of the manifest set.
pub struct Manifold {
    patches: BTreeMap<NodeId, Manifest>,
}

impl Manifold {
    pub fn new() -> Manifold {
        Manifold { patches: BTreeMap::new() }
    }

    /// Enrol a validated manifest. Ids are unique by construction (the
    /// compiler checks); double enrolment of the same id replaces, which
    /// the enrolment path never does.
    pub fn enrol(&mut self, manifest: Manifest) {
        self.patches.insert(manifest.id.clone(), manifest);
    }

    pub fn get(&self, id: &str) -> Option<&Manifest> {
        self.patches.get(id)
    }

    pub fn ids(&self) -> Vec<NodeId> {
        self.patches.keys().cloned().collect()
    }

    pub fn manifests(&self) -> impl Iterator<Item = &Manifest> {
        self.patches.values()
    }

    /// Route a stream record's (sender, intent) to the patch that
    /// triggers on it, or [`UNROUTED`]. First match in patch-id order —
    /// deterministic and re-derivable, so `verify` can replay routing
    /// against a run's enrolled manifests.
    pub fn route(&self, sender: &str, intent: &str) -> NodeId {
        for manifest in self.patches.values() {
            if manifest.triggers.iter().any(|t| t.sender == sender && t.intent == intent) {
                return manifest.id.clone();
            }
        }
        UNROUTED.to_string()
    }
}

impl Default for Manifold {
    fn default() -> Self {
        Self::new()
    }
}

/// A source event's channel is its sender-ish identity; events arriving
/// from the ingest seam carry the well-known sender.
pub fn source_sender(channel: &str) -> NodeId {
    if channel.is_empty() {
        INGEST.to_string()
    } else {
        channel.to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest(id: &str, triggers: Vec<Trigger>) -> Manifest {
        Manifest {
            id: id.into(),
            role: ROLE_PATCH.into(),
            triggers,
            policy: Policy { accept_senders: vec!["*".into()] },
            emit_intent: "relay".into(),
            cost: 1,
        }
    }

    #[test]
    fn routing_is_deterministic_and_unrouted_when_nothing_matches() {
        let mut m = Manifold::new();
        m.enrol(manifest(
            "beta",
            vec![Trigger { sender: "alpha".into(), intent: "relay".into() }],
        ));
        m.enrol(manifest(
            "alpha",
            vec![Trigger { sender: "ingest".into(), intent: "ping".into() }],
        ));

        assert_eq!(m.route("ingest", "ping"), "alpha");
        assert_eq!(m.route("alpha", "relay"), "beta");
        assert_eq!(m.route("nobody", "cares"), UNROUTED);
        assert_eq!(m.route("ingest", "noise"), UNROUTED, "no-match is unrouted, not an error");
    }
}
