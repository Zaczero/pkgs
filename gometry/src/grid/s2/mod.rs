//! Gometry's own S2 cell-geometry core (no external S2 dependency): the
//! canonical cell-id layer, cell geometry, set algebra, and an
//! exact-geometry coverer that classifies candidate cells directly against
//! gometry's predicate engine.
//!
//! Pure Rust, no `PyO3` — `src/py/cells/s2/` owns the Python surface and
//! error mapping. Ids, tokens, and hierarchy semantics are bit-compatible
//! with Google S2 (verified differentially against the reference crate and
//! s2sphere).

pub(crate) mod bounding;
pub(crate) mod cell;
pub(crate) mod cell_set;
pub(crate) mod cellid;
pub(crate) mod coverer;
pub(crate) mod projection;
pub(crate) mod seam;
