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
mod prepared;
mod topological;
pub(crate) mod unary;

pub(crate) use de9im::{equals, equals_exact, equals_identical, relate, relate_pattern};
pub(crate) use engine::{
    IndexEnvelope, PREPARED_PREDICATE_MIN, Predicate, PredicateSpec, point_batch, scalar_vs_shapes,
    topology_scalar_pair,
};
pub(crate) use pairwise::{
    distance, distance_3d, dwithin, frechet_distance, hausdorff_distance, nearest_points,
    shortest_line,
};
pub(crate) use prepared::{_unpickle_prepared, PyPreparedGeometry};
pub(crate) use topological::{
    contains, contains_properly, contains_xy, covered_by, covers, crosses, disjoint, intersects,
    intersects_xy, overlaps, touches, within,
};
