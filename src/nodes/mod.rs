//! Patch process types.
//!
//! Slice 1 implements `sovereign` (the isolated patch wrapper with
//! BEAM/OTP supervision semantics) and `patch` (the patch process
//! itself: its own active log and ledger side, receiver-side refusal).
//! `jepa` and `boundary` remain README-stubbed — the boundary *role* is
//! enrolment bookkeeping (`core::manifold::ROLE_BOUNDARY`), needing no
//! node type of its own.

pub mod patch;
pub mod sovereign;
