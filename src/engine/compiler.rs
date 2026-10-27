//! `engine/compiler` — admission-time checking (ARCHITECTURE.md,
//! plausible as validation; the "constraint graph = truth" claim is
//! rejected, not implemented). In plain words: when a patch is
//! enrolled, verify its declared envelope types, trigger conditions and
//! constraint set are complete and satisfiable before it joins the run.

use crate::core::manifold::{Manifest, ROLE_BOUNDARY, ROLE_PATCH};
use crate::seams::envelope::INGEST;

/// Validate a manifest for enrolment. Returns every problem found (a
/// manifest is admitted only when the list is empty), so enrolment
/// refuses with the full diagnosis rather than the first.
pub fn validate(manifest: &Manifest, enrolled: &[String]) -> Result<(), Vec<String>> {
    let mut problems = Vec::new();

    if manifest.id.trim().is_empty() {
        problems.push("id must be non-empty".into());
    }
    if enrolled.iter().any(|id| id == &manifest.id) {
        problems.push(format!("id '{}' already enrolled", manifest.id));
    }
    if manifest.role != ROLE_PATCH && manifest.role != ROLE_BOUNDARY {
        problems.push(format!(
            "role must be '{}' or '{}' (found '{}')",
            ROLE_PATCH, ROLE_BOUNDARY, manifest.role
        ));
    }
    if manifest.triggers.is_empty() {
        problems.push("triggers must declare at least one (sender, intent) pair".into());
    }
    for (n, trigger) in manifest.triggers.iter().enumerate() {
        if trigger.sender.trim().is_empty() {
            problems.push(format!("trigger {n}: sender must be non-empty"));
        }
        if trigger.intent.trim().is_empty() {
            problems.push(format!("trigger {n}: intent must be non-empty"));
        }
    }
    if manifest.policy.accept_senders.is_empty() {
        problems.push("policy.accept_senders must be non-empty (use [\"*\"] for any)".into());
    }
    if manifest.emit_intent.trim().is_empty() {
        problems.push("emit_intent must be non-empty".into());
    }
    // Satisfiability-lite: a trigger sender must be a plausible seam —
    // the ingest seam, an enrolled patch, or a patch enrolled alongside
    // this one. Anything else is almost certainly a typo; we warn (to
    // stderr) rather than refuse, because an unmatched trigger merely
    // never fires — which the stream records as inert, not as an error.
    let known: Vec<String> = enrolled
        .iter()
        .cloned()
        .chain(std::iter::once(manifest.id.clone()))
        .collect();
    for trigger in &manifest.triggers {
        if trigger.sender != INGEST && !known.contains(&trigger.sender) {
            eprintln!(
                "compiler: warning: patch '{}' triggers on sender '{}' which is neither '{}' nor an enrolled patch id",
                manifest.id, trigger.sender, INGEST
            );
        }
    }
    match problems.is_empty() {
        true => Ok(()),
        false => Err(problems),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::manifold::{Policy, Trigger};

    fn ok_manifest() -> Manifest {
        Manifest {
            id: "alpha".into(),
            role: ROLE_PATCH.into(),
            triggers: vec![Trigger { sender: INGEST.into(), intent: "ping".into() }],
            policy: Policy { accept_senders: vec!["*".into()] },
            emit_intent: "relay".into(),
            cost: 2,
        }
    }

    #[test]
    fn valid_manifest_passes() {
        assert!(validate(&ok_manifest(), &[]).is_ok());
    }

    #[test]
    fn incomplete_manifests_are_refused_with_all_problems() {
        let bad = Manifest {
            id: "  ".into(),
            role: "dj".into(),
            triggers: vec![],
            policy: Policy { accept_senders: vec![] },
            emit_intent: "".into(),
            cost: 0,
        };
        let problems = validate(&bad, &[]).unwrap_err();
        assert!(problems.iter().any(|p| p.contains("id must be non-empty")));
        assert!(problems.iter().any(|p| p.contains("role must be")));
        assert!(problems.iter().any(|p| p.contains("triggers must declare")));
        assert!(problems.iter().any(|p| p.contains("accept_senders")));
        assert!(problems.iter().any(|p| p.contains("emit_intent")));
    }

    #[test]
    fn duplicate_ids_are_refused() {
        let m = ok_manifest();
        let enrolled = vec!["alpha".to_string()];
        assert!(validate(&m, &enrolled).is_err());
    }
}
