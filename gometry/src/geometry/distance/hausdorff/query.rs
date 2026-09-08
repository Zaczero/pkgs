use crate::geometry::distance::SQUARED_SPACE_MAX_MAGNITUDE;
use crate::geometry::distance::hausdorff::{HausdorffFeature, sqrt_distance_squared};
#[cfg(test)]
use crate::geometry::facet_bvh;
use crate::geometry::facet_bvh::{BVH_MIN_INDEXED_SEGMENTS, FacetBvh, PreparedLinework};
#[cfg(test)]
use crate::geometry::{LineSeq, Point};
use crate::geometry::{Shape, XY, point_distance_squared, point_segment_distance_squared};
use crate::scratch_pool::{RetainScratch, ScratchPool, flat_vec_retained_bytes};

#[derive(Default)]
struct QueryScratch {
    dist: Vec<f64>,
    witness: Vec<u32>,
}

impl RetainScratch for QueryScratch {
    fn clear_for_reuse(&mut self) {
        self.dist.clear();
        self.witness.clear();
    }

    fn retained_bytes(&self) -> usize {
        flat_vec_retained_bytes(&self.dist).saturating_add(flat_vec_retained_bytes(&self.witness))
    }
}

thread_local! {
    static QUERY_SCRATCH: ScratchPool<QueryScratch> = const { ScratchPool::new(QueryScratch {
        dist: Vec::new(),
        witness: Vec::new(),
    }) };
}

#[cfg(test)]
mod scratch_tests {
    use super::*;

    fn reserve(len: usize) {
        let mut scratch = ScratchPool::checkout(&QUERY_SCRATCH);
        scratch.dist.reserve(len);
        scratch.witness.reserve(len);
    }

    #[test]
    fn query_columns_are_aggregate_bounded_and_reentrant() {
        reserve(1024);
        let ((), allocations) = crate::test_support::allocation_count(|| reserve(1024));
        assert_eq!(allocations, 0);

        let held = ScratchPool::checkout(&QUERY_SCRATCH);
        let query =
            HausdorffLineworkQuery::from_open_xy_columns(&[0.0, 1.0], &[0.0, 0.0], false).unwrap();
        assert_eq!(query.distance_squared(XY::new(0.5, 1.0)).unwrap(), 1.0);
        drop(held);

        reserve(crate::scratch_pool::SCRATCH_RETAIN_BYTES / std::mem::size_of::<f64>() + 1);
        let empty = ScratchPool::checkout(&QUERY_SCRATCH);
        assert_eq!(empty.dist.capacity(), 0);
        assert_eq!(empty.witness.capacity(), 0);
        drop(empty);

        let ((), first_allocations) = crate::test_support::allocation_count(|| reserve(1024));
        assert!(first_allocations > 0);
        let ((), repeat_allocations) = crate::test_support::allocation_count(|| reserve(1024));
        assert_eq!(repeat_allocations, 0);
    }

    #[test]
    fn witness_indices_span_every_facet_in_flat_and_bvh_queries() {
        let segment_count = facet_bvh::FACET_SEGMENTS * 3 + 1;
        let xs: Vec<f64> = (0..=segment_count).map(|index| index as f64).collect();
        let ys = vec![0.0; xs.len()];
        let probe = [(segment_count as f64 - 0.5, 1.0)];

        for force_bvh in [false, true] {
            let query = HausdorffLineworkQuery::from_open_xy_columns(&xs, &ys, force_bvh).unwrap();
            let mut result = [HausdorffProbeResult::default()];
            query
                .batch_probe(&probe, &mut result, &mut Vec::new())
                .unwrap();
            assert_eq!(result[0].distance_squared, 1.0, "force_bvh={force_bvh}");
            assert_eq!(
                result[0].feature_id,
                (segment_count - 1) as u32,
                "force_bvh={force_bvh}"
            );
        }
    }

    fn probe(
        xs: &[f64],
        ys: &[f64],
        force_bvh: bool,
        px: f64,
        py: f64,
    ) -> (HausdorffLineworkQuery, HausdorffProbeResult) {
        let query = HausdorffLineworkQuery::from_open_xy_columns(xs, ys, force_bvh).unwrap();
        let mut result = [HausdorffProbeResult::default()];
        query
            .batch_probe(&[(px, py)], &mut result, &mut Vec::new())
            .unwrap();
        (query, result[0])
    }

    #[test]
    fn hypot_query_squares_once_at_the_probe_boundary() {
        let xs = [2e150, 3e150];
        let ys = [0.0, 0.0];
        for force_bvh in [false, true] {
            let (query, result) = probe(&xs, &ys, force_bvh, 2e150, 2.0);
            assert!(
                !query.squared_safe,
                "force_bvh={force_bvh}: magnitude > {SQUARED_SPACE_MAX_MAGNITUDE}"
            );
            assert_eq!(result.distance_squared, 4.0, "force_bvh={force_bvh}");
            assert_eq!(
                query
                    .linework
                    .min_points_distance::<false>(std::iter::once((2e150, 2.0)), f64::INFINITY)
                    .unwrap(),
                2.0,
                "force_bvh={force_bvh}: non-witness hypot stays hypot"
            );
        }
        let source = Shape::LineString(
            LineSeq::segment(Point::new_raw_xy(2e150, 2.0), Point::new_raw_xy(3e150, 2.0)).unwrap(),
        );
        let target = Shape::LineString(
            LineSeq::segment(Point::new_raw_xy(2e150, 0.0), Point::new_raw_xy(3e150, 0.0)).unwrap(),
        );
        assert_eq!(source.hausdorff_distance(&target).unwrap(), 2.0);
    }

    #[test]
    fn hypot_witness_picks_the_nearer_overflowing_segment() {
        let xs = [2e155, 2.1e155, 1e155];
        let ys = [0.0, 0.0, 0.0];
        for force_bvh in [false, true] {
            let (query, result) = probe(&xs, &ys, force_bvh, 0.0, 0.0);
            assert_eq!(query.linework.segment_count(), 2, "force_bvh={force_bvh}");
            assert_eq!(query.linework.facets.len(), 1, "force_bvh={force_bvh}");
            assert!(
                !query.squared_safe,
                "force_bvh={force_bvh}: magnitude > {SQUARED_SPACE_MAX_MAGNITUDE}"
            );
            assert_eq!(result.feature_id, 1, "force_bvh={force_bvh}");
        }
    }

    #[test]
    fn hypot_witness_picks_the_nearer_underflowing_second_segment() {
        let xs = [2e150, 3e150, 3e150];
        let ys = [8e-200, 8e-200, 0.0];
        for force_bvh in [false, true] {
            let (query, result) = probe(&xs, &ys, force_bvh, 3e150, 1e-200);
            assert_eq!(query.linework.segment_count(), 2, "force_bvh={force_bvh}");
            assert_eq!(query.linework.facets.len(), 1, "force_bvh={force_bvh}");
            assert!(
                !query.squared_safe,
                "force_bvh={force_bvh}: magnitude > {SQUARED_SPACE_MAX_MAGNITUDE}"
            );
            assert_eq!(result.feature_id, 1, "force_bvh={force_bvh}");
        }
    }
}

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
    pub(crate) const fn inc_point_envelope_candidates(_: usize) {}
    pub(crate) const fn inc_point_envelope_breakpoints(_: usize) {}
    pub(crate) const fn inc_point_envelope_samples(_: usize) {}
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
        pub point_envelope_candidates: usize,
        pub point_envelope_breakpoints: usize,
        pub point_envelope_samples: usize,
    }

    thread_local! {
        static COUNTERS: [Cell<usize>; 7] = Default::default();
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
            point_envelope_candidates: c[4].get(),
            point_envelope_breakpoints: c[5].get(),
            point_envelope_samples: c[6].get(),
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
    pub(crate) fn inc_point_envelope_candidates(count: usize) {
        add(4, count);
    }
    pub(crate) fn inc_point_envelope_breakpoints(count: usize) {
        add(5, count);
    }
    pub(crate) fn inc_point_envelope_samples(count: usize) {
        add(6, count);
    }
}

pub(crate) fn linework_squared_safe(linework: &PreparedLinework) -> bool {
    let safe_column = |column: &[f64]| {
        column
            .iter()
            .all(|value| value.abs() <= SQUARED_SPACE_MAX_MAGNITUDE)
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

    pub(crate) fn from_open_xy_columns(
        xs: &[f64],
        ys: &[f64],
        force_bvh: bool,
    ) -> crate::error::Result<Self> {
        Ok(Self::from_linework(
            PreparedLinework::from_open_xy_columns(xs, ys)?,
            force_bvh,
        ))
    }

    pub(crate) fn from_shape(shape: &Shape, force_bvh: bool) -> crate::error::Result<Self> {
        Ok(Self::from_linework(
            PreparedLinework::build(shape)?,
            force_bvh,
        ))
    }

    pub(crate) fn batch_probe(
        &self,
        probes: &[(f64, f64)],
        out: &mut [HausdorffProbeResult],
        stack: &mut Vec<u32>,
    ) -> crate::error::Result<()> {
        debug_assert_eq!(probes.len(), out.len());
        if probes.is_empty() {
            return Ok(());
        }
        let mut scratch = ScratchPool::checkout(&QUERY_SCRATCH);
        // An empty linework target contributes no zero-distance
        // feature.  Point-only targets are folded by
        // `HausdorffTarget::distance_squared` afterwards; starting
        // this lane at zero silently made every probe coincide with
        // a Point target and sent the extreme fallback a false zero.
        scratch.dist.resize(probes.len(), f64::INFINITY);
        scratch.witness.resize(probes.len(), u32::MAX);
        if self.squared_safe {
            let QueryScratch { dist, witness } = &mut *scratch;
            self.batch_probe_inner::<true>(probes, out, stack, dist, witness)?;
        } else {
            let QueryScratch { dist, witness } = &mut *scratch;
            self.batch_probe_inner::<false>(probes, out, stack, dist, witness)?;
        }
        Ok(())
    }

    fn batch_probe_inner<const SQUARED: bool>(
        &self,
        probes: &[(f64, f64)],
        out: &mut [HausdorffProbeResult],
        stack: &mut Vec<u32>,
        dist: &mut [f64],
        witness: &mut [u32],
    ) -> crate::error::Result<()> {
        if let Some(bvh) = &self.facet_bvh {
            bvh.batch_min_point_distance_with_witness::<SQUARED>(
                &self.linework,
                probes,
                dist,
                witness,
                stack,
            )?;
        } else {
            self.linework
                .batch_min_point_distance_with_witness::<SQUARED>(probes, dist, witness)?;
        }
        for (slot, (&distance, &feature_id)) in
            std::iter::zip(out.iter_mut(), std::iter::zip(dist.iter(), witness.iter()))
        {
            *slot = HausdorffProbeResult {
                distance_squared: if SQUARED {
                    distance
                } else {
                    distance * distance
                },
                feature_id,
            };
        }
        Ok(())
    }

    pub(crate) fn distance_squared(&self, point: XY) -> crate::error::Result<f64> {
        let mut stack = Vec::new();
        let mut out = [HausdorffProbeResult::default()];
        self.batch_probe(&[(point.x, point.y)], &mut out, &mut stack)?;
        Ok(out[0].distance_squared)
    }
}

pub(crate) fn point_feature_distance(
    point: XY,
    feature: HausdorffFeature,
) -> crate::error::Result<f64> {
    Ok(match feature {
        HausdorffFeature::Point(target) => {
            sqrt_distance_squared(point_distance_squared(point, target))
        },
        HausdorffFeature::Segment(segment) => {
            sqrt_distance_squared(point_segment_distance_squared(point, segment)?)
        },
    })
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
) -> crate::error::Result<f64> {
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
            single = single.min(du?.max(dv?));
        }
    }
    let ub = lip.min(piece).min(single);
    // Outward round in distance space (next representable) before squaring.
    let rounded = if ub > 0.0 && ub.is_finite() {
        f64::from_bits(ub.to_bits() + 1)
    } else {
        ub
    };
    Ok(rounded * rounded)
}
