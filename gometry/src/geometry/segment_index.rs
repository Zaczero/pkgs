//! Envelope-indexed linework and point sets: candidate generation for
//! overlay noding/simplicity (`intersecting_candidates`), minimum clearance,
//! Hausdorff linework queries, and the private Y-stabbing substrate used by
//! prepared point membership (`point_location`).
//! (Distance/`dwithin` traversal moved to the packed facet BVH in
//! `facet_bvh.rs`.)
//!
//! Exact kernels stay the final authority everywhere; the index only skips
//! pairs whose envelopes prove they cannot matter, so results are identical
//! to the brute-force scans (`min` over distances is order-independent).

mod index;
mod stabbing;
mod sweep;

pub(crate) use index::{PointSetIndex, SegmentIndex};
pub(crate) use stabbing::{EdgeYIndex, YStabbingIndex};
pub(crate) use sweep::SEGMENT_INDEX_MIN_PAIRS;
pub(in crate::geometry) use sweep::{
    CHAIN_MIN_SEGMENTS, MonotoneRun, RUN_NODING_MIN, candidate_pairs_over_runs, flat_segment_sweep,
    for_each_bipartite_index_pair, for_each_candidate_pair, for_each_overlapping_bounds_pair,
    linework_contact, segments_cross, sign_of, single_chain,
};

#[cfg(test)]
mod tests;
