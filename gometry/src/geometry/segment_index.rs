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

use rstar::{RTreeNode, RTreeObject};

use super::BulkRTree;

mod index;
mod order_tree;
mod stabbing;
mod sweep;

/// Used child-slot bytes of a bulk-loaded R-tree.
///
/// Walks `root().children()`, a slice. Each slot is `size_of::<RTreeNode<T>>()`.
/// Spare `Vec` capacity is not visible through that API, so this is not RSS
/// and not an exact allocator footprint: an empty bulk-load still reserves
/// `MAX_SIZE + 1` root slots and reports 0. Leaves here (`SegmentEntry`,
/// `[f64; 2]`, `EnvelopeEntry`) are heapless payloads already inside the
/// slot; nested leaf heap is not charged.
fn bulk_rtree_heap_bytes<T: RTreeObject>(tree: &BulkRTree<T>) -> usize {
    fn node_bytes<T: RTreeObject>(node: &RTreeNode<T>) -> usize {
        let children_bytes = match node {
            RTreeNode::Leaf(_) => 0,
            RTreeNode::Parent(parent) => parent
                .children()
                .iter()
                .map(node_bytes)
                .fold(0_usize, usize::saturating_add),
        };
        std::mem::size_of::<RTreeNode<T>>().saturating_add(children_bytes)
    }
    tree.root()
        .children()
        .iter()
        .map(node_bytes)
        .fold(0_usize, usize::saturating_add)
}

pub(crate) use index::{PointSetIndex, SegmentIndex};
pub(in crate::geometry) use order_tree::OrderTree;
pub(crate) use stabbing::{
    EdgeId, EdgeYIndex, YStabbingIndex, checked_index_count, uses_linear_plan_for_len,
};
pub(crate) use sweep::SEGMENT_INDEX_MIN_PAIRS;
pub(in crate::geometry) use sweep::{
    MonotoneRun, candidate_pairs_over_staged_runs, for_each_candidate_pair,
    for_each_indexed_overlapping_bounds_pair, for_each_overlapping_bounds_pair, sign_of,
    single_chain,
};

#[cfg(test)]
mod tests;
