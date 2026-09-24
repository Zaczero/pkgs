//! Test-only counters for the exact partition.
//!
//! Tests assert on work done rather than on wall time: that a fast path was
//! entered, that a certificate declined, that a sweep did not grow quadratic.
//! The counters live here so the partition itself reads as the algorithm.

use std::cell::Cell;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::geometry) struct PartitionMetrics {
    pub partition_builds: usize,
    pub source_segments: usize,
    pub unique_source_segments: usize,
    pub candidate_pairs_attempted: usize,
    pub candidate_pairs_distinct: usize,
    pub candidate_pool_items: usize,
    pub overlap_contacts: usize,
    pub overlap_cuts: usize,
    pub source_cut_entries: usize,
    pub canonical_spans: usize,
    pub canonical_nodes: usize,
    pub global_faces: usize,
    pub topology_halfedges: usize,
    pub publication_nodes: usize,
    pub published_rows: usize,
    /// Crossings whose double-double certificate declined at construction.
    pub declined_crossings: usize,
    /// Crossings whose exact point a comparison had to build afterwards.
    pub exact_crossings: usize,
    /// Orientation tests made by leftward ray sweeps.
    pub ray_orientations: usize,
    /// Emissions whose items all end at stored nodes, so rounding moved
    /// nothing and the emission checks had nothing to find.
    pub unmoved_emissions: usize,
}

impl PartitionMetrics {
    const ZERO: Self = Self {
        partition_builds: 0,
        source_segments: 0,
        unique_source_segments: 0,
        candidate_pairs_attempted: 0,
        candidate_pairs_distinct: 0,
        candidate_pool_items: 0,
        overlap_contacts: 0,
        overlap_cuts: 0,
        source_cut_entries: 0,
        canonical_spans: 0,
        canonical_nodes: 0,
        global_faces: 0,
        topology_halfedges: 0,
        publication_nodes: 0,
        published_rows: 0,
        declined_crossings: 0,
        exact_crossings: 0,
        ray_orientations: 0,
        unmoved_emissions: 0,
    };
}

thread_local! {
    static PARTITION_CALLS: Cell<usize> = const { Cell::new(0) };
    static PARTITION_METRICS: Cell<PartitionMetrics> = const { Cell::new(PartitionMetrics::ZERO) };
}

pub(in crate::geometry) fn partition_builds() -> usize {
    PARTITION_METRICS.with(|metrics| metrics.get().partition_builds)
}

pub(crate) fn partition_calls() -> usize {
    PARTITION_CALLS.with(Cell::get)
}

pub(in crate::geometry) fn partition_metrics() -> PartitionMetrics {
    PARTITION_METRICS.with(Cell::get)
}

pub(super) fn record_candidate_pair() {
    update(|metrics| {
        metrics.candidate_pairs_attempted += 1;
        metrics.candidate_pairs_distinct += 1;
    });
}

pub(super) fn record_overlap(cuts: usize) {
    update(|metrics| {
        metrics.overlap_contacts += 1;
        metrics.overlap_cuts += cuts;
    });
}

pub(super) fn record_partition_call() {
    PARTITION_CALLS.with(|calls| calls.set(calls.get().saturating_add(1)));
}

pub(in crate::geometry) fn reset_partition_builds() {
    PARTITION_METRICS.with(|metrics| metrics.set(PartitionMetrics::ZERO));
}

pub(crate) fn reset_partition_calls() {
    PARTITION_CALLS.with(|calls| calls.set(0));
}

pub(super) fn update(update: impl FnOnce(&mut PartitionMetrics)) {
    PARTITION_METRICS.with(|metrics| {
        let mut current = metrics.get();
        update(&mut current);
        metrics.set(current);
    });
}
