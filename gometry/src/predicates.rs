//! Free-function spatial predicates and scalar distance measures.
//!
//! Relational predicates (`contains`/`within`/`intersects`/…), boolean shape
//! properties (`is_empty`/`is_simple`/…), and pairwise distance/measure
//! functions (`distance`/`hausdorff_distance`/`nearest_points`/`dwithin`).
//! These broadcast over scalars and arrays via the shared dispatch core in the
//! crate root, reached through `use super::*`.

mod de9im;
pub(crate) mod engine;
mod pairwise;
mod topological;
pub(crate) mod unary;

pub(crate) use de9im::{equals, equals_exact, equals_identical, relate, relate_pattern};
pub(crate) use engine::{IndexEnvelope, Predicate, PredicateSpec, scalar_vs_shapes};
pub(crate) use pairwise::{
    distance, distance_3d, dwithin, frechet_distance, hausdorff_distance, nearest_points,
    shortest_line,
};
pub(crate) use topological::{
    contains, contains_properly, covered_by, covers, crosses, disjoint, intersects, overlaps,
    touches, within,
};
