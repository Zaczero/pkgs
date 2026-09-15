//! Facet partition, linework preparation, and BVH traversal.

mod bvh;
mod prepare;

pub(crate) use bvh::{FacetBvh, NearestCandidate};
pub(crate) use prepare::{Facet, PreparedLinework};
