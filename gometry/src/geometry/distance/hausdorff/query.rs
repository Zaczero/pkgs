#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use super::*;
use crate::geometry::facet_bvh::{BVH_MIN_INDEXED_SEGMENTS, FacetBvh, PreparedLinework};

/// Per-probe exact squared distance and nearest target-feature index.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct HausdorffProbeResult {
    pub(crate) distance_squared: f64,
    /// Index into [`HausdorffTargetLike::features_slice`]; `u32::MAX` when
    /// the linework is empty and only isolated points may answer.
    pub(crate) feature_id: u32,
}

#[cfg(not(test))]
pub(crate) mod stats {
    pub(crate) const fn inc_vertex_probes(_: usize) {}
    pub(crate) const fn inc_segment_bound_skips() {}
    pub(crate) const fn inc_coverage_certificate_skips() {}
    pub(crate) const fn inc_exact_segment_evals() {}
}

pub(crate) fn linework_squared_safe(linework: &PreparedLinework) -> bool {
    let safe_column = |column: &[f64]| {
        column
            .iter()
            .all(|value| value.abs() <= super::intersects::SQUARED_SPACE_MAX_MAGNITUDE)
    };
    let mut safe = true;
    linework.for_each_chain_xy_columns(|xs, ys| {
        safe &= safe_column(xs) && safe_column(ys);
    });
    safe
}

pub(crate) struct HausdorffLineworkQuery {
    pub(crate) linework: PreparedLinework,
    pub(crate) facet_bvh: Option<FacetBvh>,
    pub(crate) squared_safe: bool,
}

impl HausdorffLineworkQuery {
    pub(crate) fn from_linework(linework: PreparedLinework, force_bvh: bool) -> Self {
        let squared_safe = linework_squared_safe(&linework);
        let facet_bvh = if force_bvh || linework.segment_count() >= BVH_MIN_INDEXED_SEGMENTS {
            FacetBvh::build(&linework)
        } else {
            None
        };
        Self {
            linework,
            facet_bvh,
            squared_safe,
        }
    }

    pub(crate) fn from_open_xy_columns(xs: &[f64], ys: &[f64], force_bvh: bool) -> Self {
        Self::from_linework(PreparedLinework::from_open_xy_columns(xs, ys), force_bvh)
    }

    pub(crate) fn from_shape(shape: &Shape, force_bvh: bool) -> Self {
        Self::from_linework(PreparedLinework::build(shape), force_bvh)
    }

    pub(crate) fn batch_probe(
        &self,
        probes: &[(f64, f64)],
        out: &mut [HausdorffProbeResult],
        stack: &mut Vec<u32>,
    ) {
        debug_assert_eq!(probes.len(), out.len());
        if probes.is_empty() {
            return;
        }
        // Thread-local dist/witness columns so nested midpoint + vertex
        // batches reuse capacity instead of allocating per batch_probe call.
        thread_local! {
            static DIST: std::cell::RefCell<Vec<f64>> = const { std::cell::RefCell::new(Vec::new()) };
            static WITNESS: std::cell::RefCell<Vec<u32>> =
                const { std::cell::RefCell::new(Vec::new()) };
        }
        DIST.with(|dist_cell| {
            WITNESS.with(|witness_cell| {
                let mut dist = dist_cell.borrow_mut();
                let mut witness = witness_cell.borrow_mut();
                dist.clear();
                dist.resize(probes.len(), 0.0);
                witness.clear();
                witness.resize(probes.len(), u32::MAX);
                if self.squared_safe {
                    self.batch_probe_inner::<true>(probes, out, stack, &mut dist, &mut witness);
                } else {
                    self.batch_probe_inner::<false>(probes, out, stack, &mut dist, &mut witness);
                }
            });
        });
    }

    fn batch_probe_inner<const SQUARED: bool>(
        &self,
        probes: &[(f64, f64)],
        out: &mut [HausdorffProbeResult],
        stack: &mut Vec<u32>,
        dist: &mut [f64],
        witness: &mut [u32],
    ) {
        if let Some(bvh) = &self.facet_bvh {
            bvh.batch_min_point_distance_with_witness::<SQUARED>(
                &self.linework,
                probes,
                dist,
                witness,
                stack,
            );
        } else {
            self.linework
                .batch_min_point_distance_with_witness::<SQUARED>(probes, dist, witness);
        }
        for (slot, (&distance_squared, &feature_id)) in
            std::iter::zip(out.iter_mut(), std::iter::zip(dist.iter(), witness.iter()))
        {
            *slot = HausdorffProbeResult {
                distance_squared,
                feature_id,
            };
        }
    }

    pub(crate) fn distance_squared(&self, point: XY) -> f64 {
        self.probe_one(point).distance_squared
    }

    pub(crate) fn probe_one(&self, point: XY) -> HausdorffProbeResult {
        let mut stack = Vec::new();
        let mut out = [HausdorffProbeResult::default()];
        self.batch_probe(&[(point.x, point.y)], &mut out, &mut stack);
        out[0]
    }
}

pub(crate) fn point_feature_distance(point: XY, feature: HausdorffFeature) -> f64 {
    match feature {
        HausdorffFeature::Point(target) => {
            sqrt_distance_squared(point_distance_squared(point, target))
        },
        HausdorffFeature::Segment(segment) => {
            sqrt_distance_squared(point_segment_distance_squared(point, segment))
        },
    }
}

/// Outward-rounded tight upper bound on max point-to-target distance along a
/// source segment (distance space, then squared for comparison).
pub(crate) fn hausdorff_segment_tight_upper_bound_squared(
    dist_start: f64,
    dist_end: f64,
    dist_mid: f64,
    length: f64,
    witness_start: u32,
    witness_mid: u32,
    witness_end: u32,
    features: &[HausdorffFeature],
    start_xy: Option<XY>,
    end_xy: Option<XY>,
) -> f64 {
    let half = 0.5 * length;
    let lip = 0.5 * (dist_start + dist_end + length);
    let piece = (0.5 * (dist_start + dist_mid + half)).max(0.5 * (dist_mid + dist_end + half));
    let mut single = f64::INFINITY;
    if let (Some(start), Some(end)) = (start_xy, end_xy) {
        for &witness in &[witness_start, witness_mid, witness_end] {
            if witness == u32::MAX || witness as usize >= features.len() {
                continue;
            }
            let feature = features[witness as usize];
            let du = point_feature_distance(start, feature);
            let dv = point_feature_distance(end, feature);
            single = single.min(du.max(dv));
        }
    }
    let ub = lip.min(piece).min(single);
    // Outward round in distance space (next representable) before squaring.
    let rounded = if ub > 0.0 && ub.is_finite() {
        f64::from_bits(ub.to_bits() + 1)
    } else {
        ub
    };
    rounded * rounded
}

#[cfg(test)]
pub(crate) mod stats {
    use std::cell::Cell;

    #[derive(Debug)]
    pub(crate) struct HausdorffStats {
        pub vertex_probes: usize,
        pub segment_bound_skips: usize,
        pub coverage_certificate_skips: usize,
        pub exact_segment_evals: usize,
    }

    thread_local! {
        static COUNTERS: [Cell<usize>; 4] = Default::default();
    }

    pub(crate) fn reset() {
        COUNTERS.with(|counters| counters.iter().for_each(|counter| counter.set(0)));
    }

    pub(crate) fn snapshot() -> HausdorffStats {
        COUNTERS.with(|c| HausdorffStats {
            vertex_probes: c[0].get(),
            segment_bound_skips: c[1].get(),
            coverage_certificate_skips: c[2].get(),
            exact_segment_evals: c[3].get(),
        })
    }

    fn add(index: usize, count: usize) {
        COUNTERS.with(|counters| counters[index].set(counters[index].get() + count));
    }

    pub(crate) fn inc_vertex_probes(count: usize) {
        add(0, count);
    }
    pub(crate) fn inc_segment_bound_skips() {
        add(1, 1);
    }
    pub(crate) fn inc_coverage_certificate_skips() {
        add(2, 1);
    }
    pub(crate) fn inc_exact_segment_evals() {
        add(3, 1);
    }
}
