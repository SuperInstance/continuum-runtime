//! Runtime bookkeeping and derived views.
//!
//! Slice 1 implements `manifold` (the enrolment index the engine loop
//! routes by). `trajectory` and `weigh` remain README-stubbed: they are
//! views over patch logs and seam outcomes that later slices derive —
//! nothing here pre-empts them.

pub mod manifold;
