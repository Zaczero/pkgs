//! Envelope-indexed linework and point sets: candidate generation for
//! overlay noding/simplicity (`intersecting_candidates`), minimum clearance,
//! Hausdorff linework queries, and the prepared point-membership raycasters.
//! (Distance/`dwithin` traversal moved to the packed facet BVH in
//! `facet_bvh.rs`.)
//!
//! Exact kernels stay the final authority everywhere; the index only skips
//! pairs whose envelopes prove they cannot matter, so results are identical
//! to the brute-force scans (`min` over distances is order-independent).

mod index;
mod sweep;

pub(crate) use index::*;
pub(crate) use sweep::*;

#[cfg(test)]
mod tests;
