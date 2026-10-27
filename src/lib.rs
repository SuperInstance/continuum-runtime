//! continuum-runtime — slice 1: the stream core.
//!
//! An append-only transmission stream (`stream`), the beam-quilt
//! transmission envelope (`seams::envelope`), sovereign patch processes
//! supervised BEAM-style (`nodes::sovereign`, `nodes::patch`), and the
//! trigger-driven engine loop (`engine::r#loop`) that routes transmissions
//! by matching stream records against enrolled patch manifests
//! (`core::manifold`, `engine::compiler`).
//!
//! Design docs: README.md, ARCHITECTURE.md, BUILD-BRIEF.md,
//! FOUNDATIONS-APPLICATION.md, SLICE1-REPORT.md at the repo root.
//!
//! Module layout: core/ (manifold; trajectory and weigh remain
//! README-stubbed for a later slice), nodes/ (sovereign, patch; jepa and
//! boundary remain stubbed), seams/ (envelope; deadband and entrainment
//! remain stubbed), engine/ (loop, compiler), plus stream/ — the one
//! module the scaffold did not name; see SLICE1-REPORT.md for the choice.

pub mod core;
pub mod engine;
pub mod nodes;
pub mod seams;
pub mod stream;
