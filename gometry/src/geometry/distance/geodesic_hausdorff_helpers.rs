use std::cmp::Ordering;

use super::geodesic_scratch::GeodesicScratch;
use super::geodesic_sweep::geodesic_target_witness;
use crate::error::Result;
use crate::geometry::{
    GeodesicMetric, GeodesicParts, GeodesicPreparedSegment as _, GeodesicSegment,
    GeodesicSegmentQuery, GeodesicSegmentQueryResult, GeodesicSegmentScratch,
    GeodesicTargetFeature, GeodesicWitnessNeed, Point,
};

/// Maximum branch-and-bound gap for the geodesic Hausdorff extremum.
#[cfg(test)]
pub(crate) const GEODESIC_HAUSDORFF_TOLERANCE: f64 = 0.01;

#[cfg(test)]
pub(crate) mod geodesic_hausdorff_counters {
    use std::cell::Cell;

    #[derive(Clone, Copy, Debug, Default)]
    pub(crate) struct Snapshot {
        pub outer_source_segments: u64,
        pub outer_interval_pops: u64,
        pub outer_interval_splits: u64,
        pub outer_complete_target_queries: u64,
        pub outer_bvh_target_queries: u64,
        pub outer_support_closures: u64,
        pub outer_regular_supports: u64,
        pub outer_regular_attempts: u64,
        pub outer_regular_foot_endpoint_declines: u64,
        pub outer_regular_near_zero_declines: u64,
        pub outer_regular_domain_declines: u64,
        pub outer_regular_derivative_declines: u64,
        pub outer_regular_branch_endpoint_declines: u64,
        pub outer_moving_supports: u64,
        pub outer_family_supports: u64,
    }

    thread_local! {
        static OUTER_SOURCE_SEGMENTS: Cell<u64> = const { Cell::new(0) };
        static OUTER_INTERVAL_POPS: Cell<u64> = const { Cell::new(0) };
        static OUTER_INTERVAL_SPLITS: Cell<u64> = const { Cell::new(0) };
        static OUTER_COMPLETE_TARGET_QUERIES: Cell<u64> = const { Cell::new(0) };
        static OUTER_BVH_TARGET_QUERIES: Cell<u64> = const { Cell::new(0) };
        static OUTER_SUPPORT_CLOSURES: Cell<u64> = const { Cell::new(0) };
        static OUTER_REGULAR_SUPPORTS: Cell<u64> = const { Cell::new(0) };
        static OUTER_REGULAR_ATTEMPTS: Cell<u64> = const { Cell::new(0) };
        static OUTER_REGULAR_FOOT_ENDPOINT_DECLINES: Cell<u64> = const { Cell::new(0) };
        static OUTER_REGULAR_NEAR_ZERO_DECLINES: Cell<u64> = const { Cell::new(0) };
        static OUTER_REGULAR_DOMAIN_DECLINES: Cell<u64> = const { Cell::new(0) };
        static OUTER_REGULAR_DERIVATIVE_DECLINES: Cell<u64> = const { Cell::new(0) };
        static OUTER_REGULAR_BRANCH_ENDPOINT_DECLINES: Cell<u64> = const { Cell::new(0) };
        static OUTER_MOVING_SUPPORTS: Cell<u64> = const { Cell::new(0) };
        static OUTER_FAMILY_SUPPORTS: Cell<u64> = const { Cell::new(0) };
    }

    macro_rules! define_counter {
        ($fn_name:ident, $cell:ident) => {
            pub(crate) fn $fn_name() {
                $cell.set($cell.get() + 1);
            }
        };
    }

    define_counter!(source_segment, OUTER_SOURCE_SEGMENTS);
    define_counter!(interval_pop, OUTER_INTERVAL_POPS);
    define_counter!(interval_split, OUTER_INTERVAL_SPLITS);
    define_counter!(complete_target_query, OUTER_COMPLETE_TARGET_QUERIES);
    define_counter!(bvh_target_query, OUTER_BVH_TARGET_QUERIES);
    define_counter!(support_closure, OUTER_SUPPORT_CLOSURES);
    define_counter!(regular_support, OUTER_REGULAR_SUPPORTS);
    define_counter!(regular_attempt, OUTER_REGULAR_ATTEMPTS);
    define_counter!(
        regular_foot_endpoint_decline,
        OUTER_REGULAR_FOOT_ENDPOINT_DECLINES
    );
    define_counter!(regular_near_zero_decline, OUTER_REGULAR_NEAR_ZERO_DECLINES);
    define_counter!(regular_domain_decline, OUTER_REGULAR_DOMAIN_DECLINES);
    define_counter!(
        regular_derivative_decline,
        OUTER_REGULAR_DERIVATIVE_DECLINES
    );
    define_counter!(
        regular_branch_endpoint_decline,
        OUTER_REGULAR_BRANCH_ENDPOINT_DECLINES
    );
    define_counter!(moving_support, OUTER_MOVING_SUPPORTS);
    define_counter!(family_support, OUTER_FAMILY_SUPPORTS);

    pub(crate) fn reset() {
        OUTER_SOURCE_SEGMENTS.set(0);
        OUTER_INTERVAL_POPS.set(0);
        OUTER_INTERVAL_SPLITS.set(0);
        OUTER_COMPLETE_TARGET_QUERIES.set(0);
        OUTER_BVH_TARGET_QUERIES.set(0);
        OUTER_SUPPORT_CLOSURES.set(0);
        OUTER_REGULAR_SUPPORTS.set(0);
        OUTER_REGULAR_ATTEMPTS.set(0);
        OUTER_REGULAR_FOOT_ENDPOINT_DECLINES.set(0);
        OUTER_REGULAR_NEAR_ZERO_DECLINES.set(0);
        OUTER_REGULAR_DOMAIN_DECLINES.set(0);
        OUTER_REGULAR_DERIVATIVE_DECLINES.set(0);
        OUTER_REGULAR_BRANCH_ENDPOINT_DECLINES.set(0);
        OUTER_MOVING_SUPPORTS.set(0);
        OUTER_FAMILY_SUPPORTS.set(0);
    }

    pub(crate) fn snapshot() -> Snapshot {
        Snapshot {
            outer_source_segments: OUTER_SOURCE_SEGMENTS.get(),
            outer_interval_pops: OUTER_INTERVAL_POPS.get(),
            outer_interval_splits: OUTER_INTERVAL_SPLITS.get(),
            outer_complete_target_queries: OUTER_COMPLETE_TARGET_QUERIES.get(),
            outer_bvh_target_queries: OUTER_BVH_TARGET_QUERIES.get(),
            outer_support_closures: OUTER_SUPPORT_CLOSURES.get(),
            outer_regular_supports: OUTER_REGULAR_SUPPORTS.get(),
            outer_regular_attempts: OUTER_REGULAR_ATTEMPTS.get(),
            outer_regular_foot_endpoint_declines: OUTER_REGULAR_FOOT_ENDPOINT_DECLINES.get(),
            outer_regular_near_zero_declines: OUTER_REGULAR_NEAR_ZERO_DECLINES.get(),
            outer_regular_domain_declines: OUTER_REGULAR_DOMAIN_DECLINES.get(),
            outer_regular_derivative_declines: OUTER_REGULAR_DERIVATIVE_DECLINES.get(),
            outer_regular_branch_endpoint_declines: OUTER_REGULAR_BRANCH_ENDPOINT_DECLINES.get(),
            outer_moving_supports: OUTER_MOVING_SUPPORTS.get(),
            outer_family_supports: OUTER_FAMILY_SUPPORTS.get(),
        }
    }
}

#[derive(Clone, Copy, Debug)]
struct TargetEvaluation {
    source: Point,
    distance: f64,
    lower: f64,
    evaluation_error: f64,
    target: Point,
    feature: GeodesicTargetFeature,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct GeodesicHausdorffWitness {
    pub(crate) source: Point,
    pub(crate) target: Point,
    pub(crate) distance: f64,
    pub(crate) lower: f64,
    pub(crate) upper: f64,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct HausdorffInterval {
    upper: f64,
    lower: f64,
    left: f64,
    right: f64,
    left_value: TargetEvaluation,
    right_value: TargetEvaluation,
}

impl PartialEq for HausdorffInterval {
    fn eq(&self, other: &Self) -> bool {
        self.upper.to_bits() == other.upper.to_bits()
            && self.left.to_bits() == other.left.to_bits()
            && self.right.to_bits() == other.right.to_bits()
    }
}

impl Eq for HausdorffInterval {}

impl PartialOrd for HausdorffInterval {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HausdorffInterval {
    fn cmp(&self, other: &Self) -> Ordering {
        self.upper
            .total_cmp(&other.upper)
            .then_with(|| other.left.total_cmp(&self.left))
            .then_with(|| other.right.total_cmp(&self.right))
    }
}

pub(crate) fn geodesic_directed_hausdorff_distance<M: GeodesicMetric>(
    source_parts: &GeodesicParts,
    target_parts: &GeodesicParts,
    metric: &M,
    scratch: &mut GeodesicScratch,
) -> Result<GeodesicHausdorffWitness> {
    let mut target_prepared = Vec::new();
    crate::boundary::try_reserve_vec(&mut target_prepared, target_parts.segments.len())?;
    for &segment in &target_parts.segments {
        crate::try_push_vec(&mut target_prepared, metric.prepare_segment(&segment)?)?;
    }

    let mut best = None;
    for &point in &source_parts.points {
        let evaluation = source_target_evaluation(point, target_parts, metric, scratch)?;
        let candidate = target_evaluation_witness(&evaluation);
        update_maximum(&mut best, &candidate);
    }
    for &segment in &source_parts.segments {
        let cmax = best.map_or(0.0, |candidate: GeodesicHausdorffWitness| candidate.lower);
        let candidate = geodesic_max_min_on_source_segment(
            &segment,
            target_parts,
            &target_prepared,
            metric,
            cmax,
            scratch,
        )?;
        update_maximum(&mut best, &candidate);
    }
    best.ok_or_else(|| {
        crate::crs::CrsError::message("geodesic Hausdorff source has no finite feature")
    })
}

pub(crate) fn geodesic_max_min_on_source_segment<M: GeodesicMetric>(
    segment: &GeodesicSegment,
    target_parts: &GeodesicParts,
    target_prepared: &[M::PreparedSegment],
    metric: &M,
    cmax: f64,
    scratch: &mut GeodesicScratch,
) -> Result<GeodesicHausdorffWitness> {
    #[cfg(test)]
    geodesic_hausdorff_counters::source_segment();
    if target_parts.segments.is_empty() && target_parts.point_only.is_empty() {
        return Err(crate::crs::CrsError::message(
            "geodesic Hausdorff target has no finite feature",
        ));
    }
    if !segment.length.is_finite() {
        return Err(crate::crs::CrsError::message(
            "geodesic Hausdorff source segment length is not finite",
        ));
    }
    let source = metric.prepare_segment(segment)?;
    let left_point = source.position(0.0)?;
    let left = source_target_evaluation(left_point, target_parts, metric, scratch)?;
    let right_point = source.position(segment.length)?;
    let right = source_target_evaluation(right_point, target_parts, metric, scratch)?;
    let mut best = if left.distance >= right.distance {
        target_evaluation_witness(&left)
    } else {
        target_evaluation_witness(&right)
    };
    if segment.length == 0.0 {
        return Ok(best);
    }

    scratch.outer.clear();
    let initial = source_interval(0.0, segment.length, &left, &right, metric);
    best.upper = initial.upper;
    if initial.upper <= completion_limit(metric, cmax.max(best.lower)) {
        return Ok(best);
    }
    crate::boundary::try_push_heap(&mut scratch.outer, initial)?;
    while let Some(interval) = scratch.outer.pop() {
        #[cfg(test)]
        geodesic_hausdorff_counters::interval_pop();
        best.lower = best.lower.max(interval.lower);
        if interval.upper <= completion_limit(metric, cmax.max(best.lower)) {
            break;
        }
        let middle = f64::midpoint(interval.left, interval.right);
        if !(middle > interval.left && middle < interval.right) {
            best.upper = best.upper.max(interval.upper);
            continue;
        }
        let middle_point = source.position(middle)?;
        let middle_value = source_target_evaluation(middle_point, target_parts, metric, scratch)?;
        let middle_witness = target_evaluation_witness(&middle_value);
        update_witness_maximum(&mut best, &middle_witness);

        let support = source_cell_support(
            &source,
            &interval,
            middle,
            &middle_value,
            target_parts,
            target_prepared,
            metric,
            scratch,
        )?;
        if let Some(upper) = support
            && upper <= completion_limit(metric, cmax.max(best.lower))
        {
            #[cfg(test)]
            geodesic_hausdorff_counters::support_closure();
            best.upper = best.upper.max(upper);
            continue;
        }

        let left = source_interval(
            interval.left,
            middle,
            &interval.left_value,
            &middle_value,
            metric,
        );
        let right = source_interval(
            middle,
            interval.right,
            &middle_value,
            &interval.right_value,
            metric,
        );
        #[cfg(test)]
        geodesic_hausdorff_counters::interval_split();
        if left.upper > completion_limit(metric, cmax.max(best.lower)) {
            crate::boundary::try_push_heap(&mut scratch.outer, left)?;
        }
        if right.upper > completion_limit(metric, cmax.max(best.lower)) {
            crate::boundary::try_push_heap(&mut scratch.outer, right)?;
        }
    }
    Ok(best)
}

fn source_target_evaluation<M: GeodesicMetric>(
    point: Point,
    target_parts: &GeodesicParts,
    metric: &M,
    scratch: &mut GeodesicScratch,
) -> Result<TargetEvaluation> {
    #[cfg(test)]
    geodesic_hausdorff_counters::complete_target_query();
    let Some(candidate) = geodesic_target_witness(
        point,
        target_parts,
        metric,
        &mut scratch.stack,
        &mut scratch.segment,
    )?
    else {
        return Err(crate::crs::CrsError::message(
            "geodesic Hausdorff target has no finite feature",
        ));
    };
    Ok(TargetEvaluation {
        source: point,
        distance: candidate.distance,
        lower: candidate.lower,
        evaluation_error: candidate.evaluation_error,
        target: candidate.target,
        feature: candidate.feature,
    })
}

const fn target_evaluation_witness(evaluation: &TargetEvaluation) -> GeodesicHausdorffWitness {
    GeodesicHausdorffWitness {
        source: evaluation.source,
        target: evaluation.target,
        distance: evaluation.distance,
        lower: evaluation.lower,
        upper: evaluation.distance,
    }
}

fn update_maximum(
    best: &mut Option<GeodesicHausdorffWitness>,
    candidate: &GeodesicHausdorffWitness,
) {
    if let Some(current) = best {
        if candidate.distance > current.distance {
            current.source = candidate.source;
            current.target = candidate.target;
            current.distance = candidate.distance;
        }
        current.lower = current.lower.max(candidate.lower);
        current.upper = current.upper.max(candidate.upper);
    } else {
        *best = Some(*candidate);
    }
}

fn update_witness_maximum(
    best: &mut GeodesicHausdorffWitness,
    candidate: &GeodesicHausdorffWitness,
) {
    if candidate.distance > best.distance {
        best.source = candidate.source;
        best.target = candidate.target;
        best.distance = candidate.distance;
    }
    best.lower = best.lower.max(candidate.lower);
    best.upper = best.upper.max(candidate.upper);
}

fn source_interval<M: GeodesicMetric>(
    left: f64,
    right: f64,
    left_value: &TargetEvaluation,
    right_value: &TargetEvaluation,
    metric: &M,
) -> HausdorffInterval {
    HausdorffInterval {
        upper: lipschitz_upper(
            left_value.distance,
            right_value.distance,
            right - left,
            metric,
        ),
        lower: (left_value.lower - left_value.evaluation_error)
            .max(right_value.lower - right_value.evaluation_error)
            .max(0.0),
        left,
        right,
        left_value: *left_value,
        right_value: *right_value,
    }
}

fn lipschitz_upper<M: GeodesicMetric>(left: f64, right: f64, width: f64, metric: &M) -> f64 {
    let raw = left
        .max(right)
        .max(f64::midpoint(left, right) + width / 2.0);
    outward_upper(raw, metric, raw + width)
}

fn completion_limit<M: GeodesicMetric>(metric: &M, best: f64) -> f64 {
    best + metric.geodesic_numerical_policy().hausdorff_gap()
}

fn outward_upper<M: GeodesicMetric>(value: f64, metric: &M, scale: f64) -> f64 {
    if !value.is_finite() {
        return f64::INFINITY;
    }
    let rounding = 128.0 * f64::EPSILON * scale.abs().max(1.0);
    let allowance = metric.geodesic_numerical_policy().evaluation_allowance(4.0) + rounding;
    (value + allowance).next_up()
}

fn source_cell_support<M: GeodesicMetric>(
    source: &M::PreparedSegment,
    interval: &HausdorffInterval,
    center: f64,
    center_value: &TargetEvaluation,
    target_parts: &GeodesicParts,
    target_prepared: &[M::PreparedSegment],
    metric: &M,
    scratch: &mut GeodesicScratch,
) -> Result<Option<f64>> {
    let mut support = exact_family_support(source, interval, center_value, metric)?;
    if support.is_some() {
        #[cfg(test)]
        geodesic_hausdorff_counters::family_support();
    }
    if let Some(value) = moving_witness_support(
        source,
        interval,
        center,
        center_value,
        target_prepared,
        metric,
    )? {
        #[cfg(test)]
        geodesic_hausdorff_counters::moving_support();
        support = Some(support.map_or(value, |current| current.min(value)));
    }
    if let Some(value) = regular_branch_support(
        source,
        interval,
        center,
        center_value,
        target_parts,
        target_prepared,
        metric,
        scratch,
    )? {
        #[cfg(test)]
        geodesic_hausdorff_counters::regular_support();
        support = Some(support.map_or(value, |current| current.min(value)));
    }
    Ok(support)
}

fn exact_family_support<M: GeodesicMetric>(
    source: &M::PreparedSegment,
    interval: &HausdorffInterval,
    center_value: &TargetEvaluation,
    metric: &M,
) -> Result<Option<f64>> {
    let mut support = None;
    if matches!(center_value.feature, GeodesicTargetFeature::Point)
        && metric.is_equatorial_segment(&source.segment())
    {
        if metric.is_polar_point(center_value.target) {
            support = Some(outward_upper(
                center_value.distance,
                metric,
                center_value.distance,
            ));
        } else if let Some(along) =
            metric.equatorial_opposite_meridian_parameter(&source.segment(), center_value.target)
        {
            let left_point = source.position(interval.left)?;
            let right_point = source.position(interval.right)?;
            let mut maximum = metric.segment_length(left_point, center_value.target);
            maximum = maximum.max(metric.segment_length(right_point, center_value.target));
            if along > interval.left && along < interval.right {
                let opposite = source.position(along)?;
                maximum = maximum.max(metric.segment_length(opposite, center_value.target));
            }
            if maximum.is_finite() {
                support = Some(outward_upper(maximum, metric, maximum));
            }
        }
    }
    if metric.is_spherical()
        && let Some(value) =
            sphere_fixed_target_upper(source, interval, center_value.target, metric)?
    {
        let value = outward_upper(value, metric, value);
        support = Some(support.map_or(value, |current| current.min(value)));
    }
    Ok(support)
}

fn sphere_fixed_target_upper<M: GeodesicMetric>(
    source: &M::PreparedSegment,
    interval: &HausdorffInterval,
    target: Point,
    metric: &M,
) -> Result<Option<f64>> {
    let radius = metric.geodesic_radius();
    if !(radius.is_finite() && radius > 0.0) {
        return Ok(None);
    }
    let left_point = source.position(interval.left)?;
    let left_unit = metric.surface_unit(left_point);
    let tangent = source.tangent(interval.left)?;
    let target_unit = metric.surface_unit(target);
    let (Some(left_unit), Some(tangent), Some(target_unit)) = (left_unit, tangent, target_unit)
    else {
        return Ok(None);
    };
    let a = dot3(left_unit, target_unit);
    let b = dot3(tangent, target_unit);
    let width_angle = (interval.right - interval.left) / radius;
    let phase = b.atan2(a) + std::f64::consts::PI;
    let mut maximum = metric.segment_length(left_point, target);
    let right_point = source.position(interval.right)?;
    maximum = maximum.max(metric.segment_length(right_point, target));
    for shift in -1..=1 {
        let relative_angle = phase + f64::from(shift) * std::f64::consts::TAU;
        if relative_angle > 0.0 && relative_angle < width_angle {
            let point = source.position(interval.left + relative_angle * radius)?;
            maximum = maximum.max(metric.segment_length(point, target));
        }
    }
    Ok(maximum.is_finite().then_some(maximum))
}

fn moving_witness_support<M: GeodesicMetric>(
    source: &M::PreparedSegment,
    interval: &HausdorffInterval,
    center: f64,
    center_value: &TargetEvaluation,
    target_prepared: &[M::PreparedSegment],
    metric: &M,
) -> Result<Option<f64>> {
    let GeodesicTargetFeature::Segment { index, along } = center_value.feature else {
        return Ok(None);
    };
    let Some(target) = target_prepared.get(index) else {
        return Ok(None);
    };
    let target_length = target.segment().length;
    let radius = metric.geodesic_radius();
    let half_width = (interval.right - interval.left) / 2.0;
    let half_width_normalized = half_width / radius;
    let along_normalized = along / radius;
    let target_length_normalized = target_length / radius;
    let center_distance_normalized = center_value.distance / radius;
    if !(radius.is_finite()
        && radius > 0.0
        && half_width_normalized > 0.0
        && along_normalized >= half_width_normalized
        && target_length_normalized - along_normalized >= half_width_normalized)
    {
        return Ok(None);
    }
    let (Some(source_tangent), Some(target_tangent)) =
        (source.tangent(center)?, target.tangent(along)?)
    else {
        return Ok(None);
    };
    let direction = if dot3(source_tangent, target_tangent) >= 0.0 {
        1.0
    } else {
        -1.0
    };
    let Some((omega, kappa, _)) = metric.geodesic_curvature_domain() else {
        return Ok(None);
    };
    let rho = (dot_difference(source_tangent, target_tangent, direction)
        + kappa * center_distance_normalized
        + tangent_allowance(metric))
    .min(2.0);
    if !(omega.is_finite() && omega > 0.0 && rho.is_finite()) {
        return Ok(None);
    }
    let omega_h = omega * half_width_normalized;
    let upper_normalized =
        center_distance_normalized * omega_h.cosh() + (rho / omega) * omega_h.sinh();
    let upper = upper_normalized * radius;
    if !upper.is_finite() {
        return Ok(None);
    }
    Ok(Some(outward_upper(upper, metric, upper)))
}

fn regular_branch_support<M: GeodesicMetric>(
    source: &M::PreparedSegment,
    interval: &HausdorffInterval,
    center: f64,
    center_value: &TargetEvaluation,
    target_parts: &GeodesicParts,
    target_prepared: &[M::PreparedSegment],
    metric: &M,
    scratch: &mut GeodesicScratch,
) -> Result<Option<f64>> {
    let GeodesicTargetFeature::Segment { index, along } = center_value.feature else {
        return Ok(None);
    };
    let Some(target) = target_prepared.get(index) else {
        return Ok(None);
    };
    let Some(target_segment) = target_parts.segments.get(index).copied() else {
        return Ok(None);
    };
    #[cfg(test)]
    geodesic_hausdorff_counters::regular_attempt();
    let target_length = target_segment.length;
    let radius = metric.geodesic_radius();
    let half_width_meters = (interval.right - interval.left) / 2.0;
    let half_width = half_width_meters / radius;
    let along_normalized = along / radius;
    let target_length_normalized = target_length / radius;
    if !(radius.is_finite()
        && radius > 0.0
        && half_width.partial_cmp(&0.0) == Some(Ordering::Greater))
    {
        return Ok(None);
    }
    if center_value
        .distance
        .partial_cmp(&metric.geodesic_backend_floor())
        != Some(Ordering::Greater)
    {
        #[cfg(test)]
        geodesic_hausdorff_counters::regular_near_zero_decline();
        return Ok(None);
    }
    if !(along_normalized > 0.0 && along_normalized < target_length_normalized) {
        #[cfg(test)]
        geodesic_hausdorff_counters::regular_foot_endpoint_decline();
        return Ok(None);
    }
    let Some((omega, _, d_star)) = metric.geodesic_curvature_domain() else {
        return Ok(None);
    };
    let patch_half =
        (2.0 * half_width).min(along_normalized.min(target_length_normalized - along_normalized));
    let patch_half_meters = patch_half * radius;
    if patch_half.partial_cmp(&0.0) != Some(Ordering::Greater) {
        return Ok(None);
    }
    let patch_left = along - patch_half_meters;
    let patch_right = along + patch_half_meters;
    let center_point = source.position(center)?;
    let left_sample = target.sample(center_point, patch_left, true)?;
    let right_sample = target.sample(center_point, patch_right, true)?;
    let (Some(left_derivative), Some(right_derivative)) =
        (left_sample.derivative, right_sample.derivative)
    else {
        #[cfg(test)]
        geodesic_hausdorff_counters::regular_derivative_decline();
        return Ok(None);
    };
    let d = center_value.distance / radius + half_width + patch_half;
    if !(d.is_finite() && d < d_star) {
        #[cfg(test)]
        geodesic_hausdorff_counters::regular_domain_decline();
        return Ok(None);
    }
    let omega_d = omega * d;
    let sine = omega_d.sin();
    let cotangent = if omega_d == 0.0 {
        1.0
    } else {
        omega_d / omega_d.tan()
    };
    if !(sine.is_finite() && sine > 0.0 && cotangent.is_finite() && cotangent > 0.0) {
        #[cfg(test)]
        geodesic_hausdorff_counters::regular_domain_decline();
        return Ok(None);
    }
    let mixed = omega_d / sine;
    let derivative_allowance = derivative_allowance(metric, center_value.distance) / radius;
    let left_derivative_value = left_derivative.value / radius;
    let left_derivative_error = left_derivative.error / radius;
    let right_derivative_value = right_derivative.value / radius;
    let right_derivative_error = right_derivative.error / radius;
    if !(left_derivative_value + left_derivative_error + mixed * half_width < -derivative_allowance
        && right_derivative_value - right_derivative_error - mixed * half_width
            > derivative_allowance)
    {
        #[cfg(test)]
        geodesic_hausdorff_counters::regular_derivative_decline();
        return Ok(None);
    }
    let p = 1.0 + derivative_allowance / half_width;
    let radius_bound = center_value.distance / radius
        + p * half_width
        + metric
            .geodesic_numerical_policy()
            .normalized_backend_floor();
    let max_radius = std::f64::consts::PI / (2.0 * omega);
    if !(radius_bound.is_finite() && radius_bound < max_radius) {
        return Ok(None);
    }
    let curvature = omega * (omega * radius_bound).tan();
    let same_side_margin =
        center_value.distance / radius - p * half_width - 0.5 * curvature * half_width * half_width;
    if !(curvature.is_finite() && curvature >= 0.0 && same_side_margin > derivative_allowance) {
        return Ok(None);
    }

    let source_left = source.position(interval.left)?;
    let source_right = source.position(interval.right)?;
    let left_branch = branch_endpoint_value(
        source_left,
        &target_segment,
        patch_left,
        patch_right,
        metric,
        &mut scratch.segment,
    )?;
    let right_branch = branch_endpoint_value(
        source_right,
        &target_segment,
        patch_left,
        patch_right,
        metric,
        &mut scratch.segment,
    )?;
    let (Some(left_branch), Some(right_branch)) = (left_branch, right_branch) else {
        return Ok(None);
    };
    let upper = concave_branch_upper(
        left_branch / radius,
        right_branch / radius,
        (interval.right - interval.left) / radius,
        curvature,
    ) * radius;
    Ok(Some(outward_upper(upper, metric, upper)))
}

fn branch_endpoint_value<M: GeodesicMetric>(
    point: Point,
    segment: &GeodesicSegment,
    patch_left: f64,
    patch_right: f64,
    metric: &M,
    scratch: &mut GeodesicSegmentScratch,
) -> Result<Option<f64>> {
    let result = metric.point_segment_query(
        point,
        segment,
        GeodesicSegmentQuery::Minimum {
            cutoff: f64::INFINITY,
            witness: GeodesicWitnessNeed::Yes,
        },
        scratch,
    )?;
    let GeodesicSegmentQueryResult::Attained {
        minimum,
        foot: Some(_),
    } = result
    else {
        return Ok(None);
    };
    if minimum.along > patch_left && minimum.along < patch_right {
        Ok(Some(minimum.distance))
    } else {
        #[cfg(test)]
        geodesic_hausdorff_counters::regular_branch_endpoint_decline();
        Ok(None)
    }
}

fn concave_branch_upper(left: f64, right: f64, width: f64, curvature: f64) -> f64 {
    let mut maximum = left.max(right);
    if curvature > 0.0 {
        let x =
            (f64::midpoint(0.0, width) + (right - left) / (curvature * width)).clamp(0.0, width);
        maximum =
            maximum.max(left + (right - left) * x / width + 0.5 * curvature * x * (width - x));
    }
    maximum
}

fn dot3(left: [f64; 3], right: [f64; 3]) -> f64 {
    left[0] * right[0] + left[1] * right[1] + left[2] * right[2]
}

fn dot_difference(source: [f64; 3], target: [f64; 3], direction: f64) -> f64 {
    [
        source[0] - direction * target[0],
        source[1] - direction * target[1],
        source[2] - direction * target[2],
    ]
    .into_iter()
    .map(f64::abs)
    .sum::<f64>()
    .min(2.0)
}

fn tangent_allowance<M: GeodesicMetric>(metric: &M) -> f64 {
    16.0 * f64::EPSILON + metric.geodesic_backend_floor() / metric.geodesic_radius().max(1.0)
}

fn derivative_allowance<M: GeodesicMetric>(metric: &M, distance: f64) -> f64 {
    8.0 * f64::EPSILON * distance.max(1.0) + metric.geodesic_backend_floor()
}

#[cfg(test)]
mod tests {
    use geographiclib_rs::Geodesic;

    use super::*;
    use crate::crs::EllipsoidMetric;

    #[test]
    fn regular_branch_endpoint_boundary_is_not_admitted() {
        let geodesic = Geodesic::wgs84();
        let metric = EllipsoidMetric::for_crs84_geometry_test(&geodesic);
        let point = Point::new_raw_xy(5.0, 1.0);
        let segment = metric
            .make_segment(Point::new_raw_xy(0.0, 0.0), Point::new_raw_xy(10.0, 0.0))
            .expect("equatorial segment is unique");
        let mut scratch = GeodesicSegmentScratch::default();
        let minimum = match metric
            .point_segment_query(
                point,
                &segment,
                GeodesicSegmentQuery::Minimum {
                    cutoff: f64::INFINITY,
                    witness: GeodesicWitnessNeed::Yes,
                },
                &mut scratch,
            )
            .expect("point-segment minimum should succeed")
        {
            GeodesicSegmentQueryResult::Attained { minimum, .. } => minimum,
            result => panic!("unexpected point-segment result: {result:?}"),
        };

        geodesic_hausdorff_counters::reset();
        let branch = branch_endpoint_value(
            point,
            &segment,
            minimum.along,
            minimum.along + 1.0,
            &metric,
            &mut scratch,
        );
        assert!(branch.expect("boundary probe should not fail").is_none());
        assert_eq!(
            geodesic_hausdorff_counters::snapshot().outer_regular_branch_endpoint_declines,
            1
        );
    }
}
