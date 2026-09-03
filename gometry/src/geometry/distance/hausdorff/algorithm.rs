#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use crate::geometry::distance::hausdorff::{
    HausdorffProbeResult, HausdorffTargetLike, hausdorff_segment_tight_upper_bound_squared,
    segment_radius_coverage_certified, stats,
};
use crate::geometry::{Segment, XY};

/// Guarded `sqrt` for a squared planar distance (measurement lane — no
/// `hypot`/`mul_add` on the fast path).
///
/// A squared residual is a faithful magnitude only when **normal** (same rule
/// as [`crate::geometry::squared_norm_is_trustworthy`] / finishers). Zero and
/// negative map to 0; positive subnormals are **not** trusted as final
/// distances — intermediate Hausdorff upper bounds that only hold the square
/// still take `sqrt` (slightly loose is pruning-safe); public finishers must
/// recompute via [`crate::geometry::finish_planar_squared_min`].
pub(crate) fn sqrt_distance_squared(squared: f64) -> f64 {
    if squared.is_normal() {
        squared.sqrt()
    } else if squared <= 0.0 {
        0.0
    } else if squared.is_finite() {
        // Positive subnormal: not a faithful final magnitude. Intermediate
        // 1-Lipschitz bounds remain conservative if slightly large.
        squared.sqrt()
    } else {
        squared
    }
}

/// The 1-Lipschitz envelope maximum over a source segment, for the shape-level
/// directed sweep. Distance to the target is 1-Lipschitz along the segment, so
/// it is bounded by `min(d_start + t·L, d_end + (1 − t)·L)`, whose maximum over
/// `t` is `(d_start + d_end + L) / 2` — the exact envelope peak, reached where
/// the two ramps cross.
///
/// This is never worse than the `min(d_start, d_end) + L` form it replaces
/// (which is the envelope's value at an endpoint of the wrong ramp), because
/// `|d_start − d_end| ≤ L` is exactly what 1-Lipschitz means. Column kernels use
/// [`hausdorff_segment_tight_upper_bound_squared`], which refines this further
/// with a midpoint probe and a single-witness exact case; both need per-segment
/// probe results the shape-level sweep does not have.
pub(crate) fn hausdorff_segment_upper_bound_squared(
    dist_start_sq: f64,
    dist_end_sq: f64,
    length_sq: f64,
) -> f64 {
    let start = sqrt_distance_squared(dist_start_sq);
    let end = sqrt_distance_squared(dist_end_sq);
    let length = sqrt_distance_squared(length_sq);
    let bound = 0.5 * (start + end + length);
    bound * bound
}

#[derive(Clone, Copy)]
struct SegmentBoundEntry {
    bound_sq: f64,
    index: usize,
}

/// Max-heap order: higher bound first, then lower index (matches the prior
/// descending-bound / ascending-index stable processing order).
impl PartialEq for SegmentBoundEntry {
    fn eq(&self, other: &Self) -> bool {
        self.bound_sq.to_bits() == other.bound_sq.to_bits() && self.index == other.index
    }
}
impl Eq for SegmentBoundEntry {}
impl PartialOrd for SegmentBoundEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for SegmentBoundEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.bound_sq
            .total_cmp(&other.bound_sq)
            .then_with(|| other.index.cmp(&self.index))
    }
}

/// Reusable work buffers for one directed Hausdorff pass — avoids SoA→AoS
/// probe reallocations and per-call midpoint/result vectors.
pub(crate) struct HausdorffScratch {
    probes: Vec<(f64, f64)>,
    probe_results: Vec<HausdorffProbeResult>,
    vertex_dist_sq: Vec<f64>,
    vertex_witness: Vec<u32>,
    midpoint_dist_sq: Vec<f64>,
    midpoint_witness: Vec<u32>,
    midpoint_probes: Vec<(f64, f64)>,
    midpoint_indices: Vec<usize>,
    mid_results: Vec<HausdorffProbeResult>,
    stack: Vec<u32>,
    /// Staging vector for O(n) heapify via `BinaryHeap::from`.
    order_entries: Vec<SegmentBoundEntry>,
}

impl Default for HausdorffScratch {
    fn default() -> Self {
        Self::new()
    }
}

impl crate::scratch_pool::RetainScratch for HausdorffScratch {
    fn clear_for_reuse(&mut self) {
        self.probes.clear();
        self.probe_results.clear();
        self.vertex_dist_sq.clear();
        self.vertex_witness.clear();
        self.midpoint_dist_sq.clear();
        self.midpoint_witness.clear();
        self.midpoint_probes.clear();
        self.midpoint_indices.clear();
        self.mid_results.clear();
        self.stack.clear();
        self.order_entries.clear();
    }

    fn retained_bytes(&self) -> usize {
        use crate::scratch_pool::flat_vec_retained_bytes;
        flat_vec_retained_bytes(&self.probes)
            .saturating_add(flat_vec_retained_bytes(&self.probe_results))
            .saturating_add(flat_vec_retained_bytes(&self.vertex_dist_sq))
            .saturating_add(flat_vec_retained_bytes(&self.vertex_witness))
            .saturating_add(flat_vec_retained_bytes(&self.midpoint_dist_sq))
            .saturating_add(flat_vec_retained_bytes(&self.midpoint_witness))
            .saturating_add(flat_vec_retained_bytes(&self.midpoint_probes))
            .saturating_add(flat_vec_retained_bytes(&self.midpoint_indices))
            .saturating_add(flat_vec_retained_bytes(&self.mid_results))
            .saturating_add(flat_vec_retained_bytes(&self.stack))
            .saturating_add(flat_vec_retained_bytes(&self.order_entries))
    }
}

impl HausdorffScratch {
    const fn new() -> Self {
        Self {
            probes: Vec::new(),
            probe_results: Vec::new(),
            vertex_dist_sq: Vec::new(),
            vertex_witness: Vec::new(),
            midpoint_dist_sq: Vec::new(),
            midpoint_witness: Vec::new(),
            midpoint_probes: Vec::new(),
            midpoint_indices: Vec::new(),
            mid_results: Vec::new(),
            stack: Vec::new(),
            order_entries: Vec::new(),
        }
    }

    fn prepare_vertices(&mut self, left_xs: &[f64], left_ys: &[f64]) {
        let n = left_xs.len();
        self.probes.clear();
        self.probes.reserve(n);
        self.probes
            .extend(std::iter::zip(left_xs, left_ys).map(|(&x, &y)| (x, y)));
        self.probe_results.clear();
        self.probe_results
            .resize(n, HausdorffProbeResult::default());
        self.vertex_dist_sq.clear();
        self.vertex_dist_sq.reserve(n);
        self.vertex_witness.clear();
        self.vertex_witness.reserve(n);
        self.stack.clear();
    }

    /// Run `target.batch_probe` against the staged probe buffer without
    /// overlapping field borrows (probes/results/stack are temporarily taken).
    fn run_batch_probe<T: HausdorffTargetLike>(&mut self, target: &T) {
        let probes = std::mem::take(&mut self.probes);
        let mut results = std::mem::take(&mut self.probe_results);
        let mut stack = std::mem::take(&mut self.stack);
        target.batch_probe(&probes, &mut results, &mut stack);
        self.probes = probes;
        self.probe_results = results;
        self.stack = stack;
    }

    /// Midpoint probe batch against staged midpoint buffers.
    fn run_midpoint_batch_probe<T: HausdorffTargetLike>(&mut self, target: &T) {
        let probes = std::mem::take(&mut self.midpoint_probes);
        let mut results = std::mem::take(&mut self.mid_results);
        let mut stack = std::mem::take(&mut self.stack);
        results.clear();
        results.resize(probes.len(), HausdorffProbeResult::default());
        target.batch_probe(&probes, &mut results, &mut stack);
        self.midpoint_probes = probes;
        self.mid_results = results;
        self.stack = stack;
    }
}

thread_local! {
    static HAUSDORFF_SCRATCH: crate::scratch_pool::ScratchPool<HausdorffScratch> =
        crate::scratch_pool::ScratchPool::new(HausdorffScratch::new());
}

pub(crate) fn hausdorff_scratch() -> crate::scratch_pool::ScratchLease<HausdorffScratch> {
    crate::scratch_pool::ScratchPool::checkout(&HAUSDORFF_SCRATCH)
}

/// Branch-and-bound directed Hausdorff²: batch facet/SIMD vertex seeding,
/// tight Lipschitz bounds, radius-coverage certificates, and Taha–Hanbury
/// ordering on the tight bound.
pub(crate) fn directed_hausdorff_distance_squared_with_target_pruned<T: HausdorffTargetLike>(
    left_xs: &[f64],
    left_ys: &[f64],
    target: &T,
) -> f64 {
    let mut scratch = hausdorff_scratch();
    directed_hausdorff_distance_squared_with_target_pruned_with_scratch(
        left_xs,
        left_ys,
        target,
        0.0,
        &mut scratch,
    )
}

pub(crate) fn directed_hausdorff_distance_squared_with_target_pruned_with_scratch<
    T: HausdorffTargetLike,
>(
    left_xs: &[f64],
    left_ys: &[f64],
    target: &T,
    initial_cmax_sq: f64,
    scratch: &mut HausdorffScratch,
) -> f64 {
    directed_hausdorff_distance_squared_with_target_pruned_initial_with_scratch(
        left_xs,
        left_ys,
        target,
        initial_cmax_sq,
        scratch,
    )
}

#[expect(
    clippy::too_many_lines,
    reason = "batch seeding, tight bounds, coverage cert, ordering"
)]
pub(crate) fn directed_hausdorff_distance_squared_with_target_pruned_initial_with_scratch<
    T: HausdorffTargetLike,
>(
    left_xs: &[f64],
    left_ys: &[f64],
    target: &T,
    initial_cmax_sq: f64,
    scratch: &mut HausdorffScratch,
) -> f64 {
    let vertex_count = left_xs.len();
    debug_assert_eq!(vertex_count, left_ys.len());
    if vertex_count == 0 {
        return 0.0;
    }
    // Length-exact reborrows: vertex/segment loops index slices whose lengths
    // ARE the loop bounds (bounds-check elision).
    let left_ys = &left_ys[..vertex_count];
    let left_xs = &left_xs[..vertex_count];

    {
        scratch.prepare_vertices(left_xs, left_ys);
        scratch.run_batch_probe(target);
        stats::inc_vertex_probes(vertex_count);
        let mut cmax = initial_cmax_sq;
        let probe_count = scratch.probe_results.len();
        scratch.vertex_dist_sq.clear();
        scratch.vertex_witness.clear();
        scratch.vertex_dist_sq.reserve(probe_count);
        scratch.vertex_witness.reserve(probe_count);
        for index in 0..probe_count {
            let distance_squared = scratch.probe_results[index].distance_squared;
            let feature_id = scratch.probe_results[index].feature_id;
            scratch.vertex_dist_sq.push(distance_squared);
            scratch.vertex_witness.push(feature_id);
            cmax = cmax.max(distance_squared);
        }
        if vertex_count < 2 {
            return cmax;
        }

        let segment_count = vertex_count - 1;
        scratch.midpoint_dist_sq.clear();
        scratch.midpoint_dist_sq.resize(segment_count, 0.0);
        scratch.midpoint_witness.clear();
        scratch.midpoint_witness.resize(segment_count, u32::MAX);
        scratch.midpoint_probes.clear();
        scratch.midpoint_indices.clear();
        for (segment_index, (x_pair, y_pair)) in
            left_xs.windows(2).zip(left_ys.windows(2)).enumerate()
        {
            let (ax, bx) = (x_pair[0], x_pair[1]);
            let (ay, by) = (y_pair[0], y_pair[1]);
            let dx = bx - ax;
            let dy = by - ay;
            if dx == 0.0 && dy == 0.0 {
                continue;
            }
            scratch.midpoint_indices.push(segment_index);
            scratch
                .midpoint_probes
                .push((f64::midpoint(ax, bx), f64::midpoint(ay, by)));
        }
        if !scratch.midpoint_probes.is_empty() {
            scratch.run_midpoint_batch_probe(target);
            // Collect mid results first so we can write into midpoint columns
            // without overlapping borrows on `scratch`.
            let mid_pairs: Vec<(usize, f64, u32)> = scratch
                .mid_results
                .iter()
                .zip(scratch.midpoint_indices.iter())
                .map(|(slot, &segment_index)| {
                    (segment_index, slot.distance_squared, slot.feature_id)
                })
                .collect();
            // Length-exact midpoint columns for the segment_count domain.
            debug_assert!(scratch.midpoint_dist_sq.len() >= segment_count);
            for (segment_index, distance_squared, feature_id) in mid_pairs {
                scratch.midpoint_dist_sq[segment_index] = distance_squared;
                scratch.midpoint_witness[segment_index] = feature_id;
                cmax = cmax.max(distance_squared);
            }
        }

        let features = target.features_slice();
        // Take the distance columns out so order-building and process_segment
        // can share them without overlapping `scratch` field borrows.
        let vertex_dist_sq_buf = std::mem::take(&mut scratch.vertex_dist_sq);
        let vertex_witness_buf = std::mem::take(&mut scratch.vertex_witness);
        let midpoint_dist_sq_buf = std::mem::take(&mut scratch.midpoint_dist_sq);
        let midpoint_witness_buf = std::mem::take(&mut scratch.midpoint_witness);
        // Length-exact: vertex_* span vertices; midpoint_* span segments.
        let vertex_dist_sq = &vertex_dist_sq_buf[..vertex_count];
        let vertex_witness = &vertex_witness_buf[..vertex_count];
        let midpoint_dist_sq = &midpoint_dist_sq_buf[..segment_count];
        let midpoint_witness = &midpoint_witness_buf[..segment_count];

        let process_segment = |segment_index: usize, cmax: &mut f64| {
            let dist_start_sq = vertex_dist_sq[segment_index];
            let dist_end_sq = vertex_dist_sq[segment_index + 1];
            let ax = left_xs[segment_index];
            let ay = left_ys[segment_index];
            let bx = left_xs[segment_index + 1];
            let by = left_ys[segment_index + 1];
            let dx = bx - ax;
            let dy = by - ay;
            if dx == 0.0 && dy == 0.0 {
                return;
            }
            let length = sqrt_distance_squared(dx * dx + dy * dy);
            let bound_sq = hausdorff_segment_tight_upper_bound_squared(
                sqrt_distance_squared(dist_start_sq),
                sqrt_distance_squared(dist_end_sq),
                sqrt_distance_squared(midpoint_dist_sq[segment_index]),
                length,
                vertex_witness[segment_index],
                midpoint_witness[segment_index],
                vertex_witness[segment_index + 1],
                features,
                Some(XY::new(ax, ay)),
                Some(XY::new(bx, by)),
            );
            if bound_sq <= *cmax {
                stats::inc_segment_bound_skips();
                return;
            }
            if segment_radius_coverage_certified(ax, ay, dx, dy, target, *cmax) {
                stats::inc_coverage_certificate_skips();
                return;
            }
            stats::inc_exact_segment_evals();
            let exact = target.max_distance_squared_on_segment_culled(
                Segment {
                    start: XY::new(ax, ay),
                    end: XY::new(bx, by),
                },
                *cmax,
            );
            *cmax = (*cmax).max(exact);
        };

        if segment_count <= 8 {
            for segment_index in 0..segment_count {
                process_segment(segment_index, &mut cmax);
            }
            scratch.vertex_dist_sq = vertex_dist_sq_buf;
            scratch.vertex_witness = vertex_witness_buf;
            scratch.midpoint_dist_sq = midpoint_dist_sq_buf;
            scratch.midpoint_witness = midpoint_witness_buf;
            return cmax;
        }

        // Heapify bound entries in O(n) and pop descending until the top
        // bound cannot beat cmax — refinement normally stops after a few
        // high-bound candidates, so a full sort is wasted work.
        let mut order_entries = std::mem::take(&mut scratch.order_entries);
        order_entries.clear();
        order_entries.reserve(segment_count);
        for segment_index in 0..segment_count {
            let ax = left_xs[segment_index];
            let ay = left_ys[segment_index];
            let bx = left_xs[segment_index + 1];
            let by = left_ys[segment_index + 1];
            let dx = bx - ax;
            let dy = by - ay;
            if dx == 0.0 && dy == 0.0 {
                continue;
            }
            let length = sqrt_distance_squared(dx * dx + dy * dy);
            order_entries.push(SegmentBoundEntry {
                bound_sq: hausdorff_segment_tight_upper_bound_squared(
                    sqrt_distance_squared(vertex_dist_sq[segment_index]),
                    sqrt_distance_squared(vertex_dist_sq[segment_index + 1]),
                    sqrt_distance_squared(midpoint_dist_sq[segment_index]),
                    length,
                    vertex_witness[segment_index],
                    midpoint_witness[segment_index],
                    vertex_witness[segment_index + 1],
                    features,
                    Some(XY::new(ax, ay)),
                    Some(XY::new(bx, by)),
                ),
                index: segment_index,
            });
        }
        let mut order = std::collections::BinaryHeap::from(order_entries);
        while let Some(entry) = order.pop() {
            if entry.bound_sq <= cmax {
                break;
            }
            process_segment(entry.index, &mut cmax);
        }
        // Return capacity to the scratch for the next call.
        scratch.order_entries = order.into_vec();
        scratch.order_entries.clear();
        scratch.vertex_dist_sq = vertex_dist_sq_buf;
        scratch.vertex_witness = vertex_witness_buf;
        scratch.midpoint_dist_sq = midpoint_dist_sq_buf;
        scratch.midpoint_witness = midpoint_witness_buf;
        cmax
    }
}

/// Cheap discrete lower bound: max vertex distance (valid LB for directed H).
pub(crate) fn directed_hausdorff_vertex_lower_bound_squared_with_scratch<T: HausdorffTargetLike>(
    left_xs: &[f64],
    left_ys: &[f64],
    target: &T,
    scratch: &mut HausdorffScratch,
) -> f64 {
    scratch.prepare_vertices(left_xs, left_ys);
    scratch.run_batch_probe(target);
    scratch
        .probe_results
        .iter()
        .map(|result| result.distance_squared)
        .fold(0.0_f64, f64::max)
}

#[cfg(test)]
pub(crate) fn directed_hausdorff_distance_squared_with_target_columns<T: HausdorffTargetLike>(
    left_xs: &[f64],
    left_ys: &[f64],
    target: &T,
) -> f64 {
    directed_hausdorff_distance_squared_with_target_pruned(left_xs, left_ys, target)
}

#[cfg(test)]
mod scratch_tests {
    use super::*;

    fn reserve(len: usize) {
        let mut scratch = crate::scratch_pool::ScratchPool::checkout(&HAUSDORFF_SCRATCH);
        scratch.probes.reserve(len);
        scratch.probe_results.reserve(len);
        scratch.vertex_dist_sq.reserve(len);
        scratch.vertex_witness.reserve(len);
        scratch.midpoint_dist_sq.reserve(len);
        scratch.midpoint_witness.reserve(len);
        scratch.midpoint_probes.reserve(len);
        scratch.midpoint_indices.reserve(len);
        scratch.mid_results.reserve(len);
        scratch.stack.reserve(len);
        scratch.order_entries.reserve(len);
    }

    #[test]
    fn algorithm_columns_are_aggregate_bounded_and_reused() {
        reserve(256);
        let (_, allocations) = crate::test_support::allocation_count(|| reserve(256));
        assert_eq!(allocations, 0);

        reserve(crate::scratch_pool::SCRATCH_RETAIN_BYTES / std::mem::size_of::<(f64, f64)>() + 1);
        let empty = crate::scratch_pool::ScratchPool::checkout(&HAUSDORFF_SCRATCH);
        assert_eq!(empty.probes.capacity(), 0);
        assert_eq!(empty.probe_results.capacity(), 0);
        assert_eq!(empty.order_entries.capacity(), 0);
        drop(empty);

        let (_, first_allocations) = crate::test_support::allocation_count(|| reserve(256));
        assert!(first_allocations > 0);
        let (_, repeat_allocations) = crate::test_support::allocation_count(|| reserve(256));
        assert_eq!(repeat_allocations, 0);
    }
}
