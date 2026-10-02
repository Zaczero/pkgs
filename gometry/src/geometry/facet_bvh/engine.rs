//! Facet partition, linework preparation, and BVH traversal.

mod bvh;
mod prepare;

pub(crate) use bvh::{FacetBvh, NearestCandidate, NearestTarget};
pub(crate) use prepare::{Facet, PreparedLinework, facet_point_distance_columns};
