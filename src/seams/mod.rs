//! Everything that crosses between processes.
//!
//! Slice 1 implements `envelope` (the chrono-tensor event / beam-quilt
//! transmission envelope). `deadband` and `entrainment` remain
//! README-stubbed: deadband pending Casey's middle-behaviour ruling
//! (BUILD-BRIEF "Open items"), entrainment a non-goal for this slice.

pub mod envelope;
