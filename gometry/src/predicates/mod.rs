//! Free-function spatial predicates and scalar distance measures.
//!
//! Relational predicates (`contains`/`within`/`intersects`/…), boolean shape
//! properties (`is_empty`/`is_simple`/…), and pairwise distance/measure
//! functions (`distance`/`hausdorff_distance`/`nearest_points`/`dwithin`).
//! These broadcast over scalars and arrays via the shared dispatch core in the
//! crate root, reached through `use super::*`.

mod de9im;
mod pairwise;
mod prepared;
mod topological;
pub(crate) mod unary;

pub(crate) use de9im::*;
pub(crate) use pairwise::*;
pub(crate) use prepared::*;
pub(crate) use topological::*;
