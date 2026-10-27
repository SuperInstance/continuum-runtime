//! The running machinery: the trigger-driven wake loop, the
//! admission-time compiler, and the playback/verify position-matched
//! checks (the Temporal discipline; unnamed in the scaffold — see
//! SLICE1-REPORT.md).

pub mod compiler;
pub mod playback;
pub mod r#loop;
