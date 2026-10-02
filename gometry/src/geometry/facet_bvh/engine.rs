//! Facet partition, linework preparation, and BVH traversal.

mod bvh;
mod prepare;

#[cfg(test)]
pub(crate) use bvh::NEAREST_WORK;
pub(crate) use bvh::{FacetBvh, NearestCandidate, NearestTarget};
pub(crate) use prepare::{Facet, PreparedLinework, facet_point_distance_columns};
