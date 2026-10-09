use std::cmp::Ordering;

use super::chart_parts::{ChartParts, point_chart_minimum};
use super::chart_scratch::ChartScratch;
use crate::crs::{ChartEdge, PointEdgeScratch};
use crate::error::Result;
use crate::geometry::interval::Interval as CertifiedInterval;
use crate::geometry::{
    ChartEdgeDifferential, ChartEdgeDifferentialBounds, ChartMetric, ChartPointDistanceSample,
    GeodesicMetric, GeodesicNumericalPolicy, Point, Shape, XY,
};

#[derive(Clone, Copy, Debug)]
pub(crate) struct ChartHausdorffWitness {
    #[cfg(test)]
    pub(crate) source: Point,
    #[cfg(test)]
    pub(crate) target: Point,
    pub(crate) distance: f64,
}

#[derive(Clone, Copy, Debug)]
struct TargetEvaluation {
    #[cfg(test)]
    source: Point,
    target: Point,
    distance: f64,
    lower_bound: f64,
    derivative: f64,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct ChartHausdorffInterval {
    upper: f64,
    left: f64,
    right: f64,
    left_value: TargetEvaluation,
    right_value: TargetEvaluation,
}

pub(crate) fn ensure_geographic_hausdorff_linework(shape: &Shape) -> Result<()> {
    if shape.has_area_parts() {
        return Err(crate::crs::CrsError::message(
            "geodesic distance for polygon interiors is unsupported",
        ));
    }
    Ok(())
}

impl PartialEq for ChartHausdorffInterval {
    fn eq(&self, other: &Self) -> bool {
        self.upper.to_bits() == other.upper.to_bits()
            && self.left.to_bits() == other.left.to_bits()
            && self.right.to_bits() == other.right.to_bits()
    }
}

impl Eq for ChartHausdorffInterval {}

impl PartialOrd for ChartHausdorffInterval {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ChartHausdorffInterval {
    fn cmp(&self, other: &Self) -> Ordering {
        self.upper
            .total_cmp(&other.upper)
            .then_with(|| other.left.total_cmp(&self.left))
            .then_with(|| other.right.total_cmp(&self.right))
    }
}

pub(crate) fn chart_directed_hausdorff_distance<M>(
    source: &ChartParts,
    target: &ChartParts,
    metric: &M,
    scratch: &mut ChartScratch,
) -> Result<ChartHausdorffWitness>
where
    M: GeodesicMetric + ChartMetric<PointEdgeScratch = PointEdgeScratch>,
{
    if target.edges.is_empty() && target.point_only.is_empty() {
        return Err(crate::crs::CrsError::message(
            "geographic Hausdorff target has no finite feature",
        ));
    }

    let mut best = None;
    for &point in &source.point_only {
        let value = source_target_evaluation(point, None, target, metric, scratch)?;
        update_maximum(&mut best, &value);
    }

    let mut previous_endpoint: Option<(XY, ChartEdgeDifferential, TargetEvaluation)> = None;
    for source_edge in &source.edges {
        let edge = source_edge.edge;
        let derivative_bounds = metric.chart_edge_differential_bounds(&edge);
        let left_differential = metric.chart_edge_differential(&edge, 0.0);
        let left = match previous_endpoint {
            Some((point, differential, value))
                if point.x.to_bits() == source_edge.start.x.to_bits()
                    && point.y.to_bits() == source_edge.start.y.to_bits()
                    && differential.speed.to_bits() == left_differential.speed.to_bits()
                    && differential.azimuth.to_bits() == left_differential.azimuth.to_bits() =>
            {
                #[cfg(test)]
                let value = TargetEvaluation {
                    source: source_edge.start,
                    ..value
                };
                value
            },
            _ => source_target_evaluation(
                source_edge.start,
                Some(left_differential),
                target,
                metric,
                scratch,
            )?,
        };
        let right_differential = if edge.is_parallel() {
            left_differential
        } else {
            metric.chart_edge_differential(&edge, 1.0)
        };
        let right = source_target_evaluation(
            source_edge.end,
            Some(right_differential),
            target,
            metric,
            scratch,
        )?;
        previous_endpoint = Some((source_edge.end.xy(), right_differential, right));
        update_maximum(&mut best, &left);
        update_maximum(&mut best, &right);

        let initial = source_interval(0.0, 1.0, &left, &right, &edge, derivative_bounds, metric);
        if initial.upper
            <= completion_limit(
                metric,
                best.as_ref().map_or(0.0, |witness| witness.distance),
            )
        {
            continue;
        }
        scratch.outer.clear();
        scratch.outer.push(initial);

        while let Some(interval) = scratch.outer.pop() {
            if interval.upper
                <= completion_limit(
                    metric,
                    best.as_ref().map_or(0.0, |witness| witness.distance),
                )
            {
                break;
            }

            let middle = f64::midpoint(interval.left, interval.right);
            if !(middle > interval.left && middle < interval.right) {
                // No interior binary64 parameter remains; the evaluated endpoints resolve this interval at source precision.
                continue;
            }
            let source = edge.chart_position(middle)?;
            let value = source_target_evaluation(
                source,
                Some(if edge.is_parallel() {
                    left_differential
                } else {
                    metric.chart_edge_differential(&edge, middle)
                }),
                target,
                metric,
                scratch,
            )?;
            update_maximum(&mut best, &value);

            let left = source_interval(
                interval.left,
                middle,
                &interval.left_value,
                &value,
                &edge,
                derivative_bounds,
                metric,
            );
            let right = source_interval(
                middle,
                interval.right,
                &value,
                &interval.right_value,
                &edge,
                derivative_bounds,
                metric,
            );
            let limit = completion_limit(
                metric,
                best.as_ref().map_or(0.0, |witness| witness.distance),
            );
            if left.upper > limit {
                scratch.outer.push(left);
            }
            if right.upper > limit {
                scratch.outer.push(right);
            }
        }
    }

    best.ok_or_else(|| {
        crate::crs::CrsError::message("geographic Hausdorff source has no finite feature")
    })
}

fn source_target_evaluation<M>(
    source: Point,
    source_differential: Option<ChartEdgeDifferential>,
    target: &ChartParts,
    metric: &M,
    scratch: &mut ChartScratch,
) -> Result<TargetEvaluation>
where
    M: GeodesicMetric + ChartMetric<PointEdgeScratch = PointEdgeScratch>,
{
    #[cfg(test)]
    counters::source_evaluation();
    let (target_point, lower_bound) = if target.edges.is_empty()
        && let [point] = target.point_only.as_slice()
    {
        // A singleton target has no minimum to search; the gradient sample
        // also supplies its attained distance.
        (*point, metric.point_distance_lower_bound(source, *point)?)
    } else {
        let Some(candidate) = point_chart_minimum(
            source,
            target,
            metric,
            &mut scratch.point_edge,
            &mut scratch.stack,
        )?
        else {
            return Err(crate::crs::CrsError::message(
                "geographic Hausdorff target has no finite feature",
            ));
        };
        (candidate.second, candidate.lower_bound)
    };

    let differential = source_differential.unwrap_or(ChartEdgeDifferential {
        speed: 0.0,
        azimuth: 0.0,
    });
    let ChartPointDistanceSample {
        distance,
        derivative,
    } = metric.chart_point_distance_sample(
        source,
        target_point,
        differential.speed,
        differential.azimuth,
    )?;
    Ok(TargetEvaluation {
        #[cfg(test)]
        source,
        target: target_point,
        distance,
        lower_bound: lower_bound.min(distance),
        derivative,
    })
}

fn source_interval<M: GeodesicMetric + ChartMetric>(
    left: f64,
    right: f64,
    left_value: &TargetEvaluation,
    right_value: &TargetEvaluation,
    edge: &ChartEdge,
    derivative_bounds: ChartEdgeDifferentialBounds,
    metric: &M,
) -> ChartHausdorffInterval {
    let mut upper = source_interval_upper(
        CertifiedInterval::exact(right)
            .sub(CertifiedInterval::exact(left))
            .upper(),
        left_value,
        right_value,
        derivative_bounds,
        metric,
    );
    if let Some(pole_upper) = metric
        .chart_edge_pole_distance_upper_bound(edge, left_value.target)
        .or_else(|| metric.chart_edge_pole_distance_upper_bound(edge, right_value.target))
    {
        upper = upper.min(outward_upper(metric, pole_upper));
    }
    ChartHausdorffInterval {
        upper,
        left,
        right,
        left_value: *left_value,
        right_value: *right_value,
    }
}

fn source_interval_upper<M: GeodesicMetric + ChartMetric>(
    width: f64,
    left: &TargetEvaluation,
    right: &TargetEvaluation,
    derivative_bounds: ChartEdgeDifferentialBounds,
    metric: &M,
) -> f64 {
    let policy = metric.geodesic_numerical_policy();
    let allowance = policy.evaluation_allowance(4.0).next_up();
    let width = CertifiedInterval::exact(width);
    let speed = CertifiedInterval::exact(derivative_bounds.speed);
    let source_span = speed.mul(width);
    let left_upper = CertifiedInterval::exact(left.distance)
        .add(CertifiedInterval::exact(allowance))
        .upper();
    let right_upper = CertifiedInterval::exact(right.distance)
        .add(CertifiedInterval::exact(allowance))
        .upper();
    let lipschitz = lipschitz_upper(left_upper, right_upper, source_span);
    let set_distance_lower = CertifiedInterval::exact(left.lower_bound)
        .add(CertifiedInterval::exact(right.lower_bound))
        .sub(source_span)
        .div(CertifiedInterval::exact(2.0))
        .lower()
        .max(0.0);
    let distance_lower = CertifiedInterval::exact(set_distance_lower)
        .sub(CertifiedInterval::exact(allowance))
        .lower()
        .max(0.0);
    let distance_upper = CertifiedInterval::exact(left_upper.max(right_upper))
        .add(source_span)
        .upper();
    let distance_hessian =
        metric.geodesic_distance_hessian_upper_bound(distance_lower, distance_upper);
    let second_derivative = if distance_hessian.is_finite() {
        speed
            .mul(speed)
            .mul(CertifiedInterval::exact(distance_hessian))
            .add(CertifiedInterval::exact(
                derivative_bounds.covariant_acceleration,
            ))
            .upper()
    } else {
        f64::INFINITY
    };
    if !second_derivative.is_finite() {
        return outward_upper(metric, lipschitz);
    }

    let taylor = taylor_upper(
        width,
        left_upper,
        left.derivative,
        right_upper,
        right.derivative,
        second_derivative,
        derivative_bounds.speed,
    );
    let upper = lipschitz.min(taylor);
    outward_upper(metric, upper)
}

fn lipschitz_upper(left: f64, right: f64, source_span: CertifiedInterval) -> f64 {
    CertifiedInterval::exact(left)
        .add(CertifiedInterval::exact(right))
        .add(source_span)
        .div(CertifiedInterval::exact(2.0))
        .upper()
        .max(left)
        .max(right)
}

fn taylor_upper(
    width: CertifiedInterval,
    left_distance: f64,
    left_derivative: f64,
    right_distance: f64,
    right_derivative: f64,
    second_derivative: f64,
    speed: f64,
) -> f64 {
    let second_derivative = CertifiedInterval::exact(second_derivative);
    let relative_slope_allowance = CertifiedInterval::exact(4.0).mul(CertifiedInterval::exact(
        GeodesicNumericalPolicy::BACKEND_RELATIVE,
    ));
    let slope_allowance = CertifiedInterval::exact(speed)
        .mul(relative_slope_allowance)
        .upper();
    let left_slope =
        CertifiedInterval::exact(left_derivative).add(CertifiedInterval::exact(slope_allowance));
    let right_slope =
        CertifiedInterval::exact(right_derivative).sub(CertifiedInterval::exact(slope_allowance));
    let left_distance = CertifiedInterval::exact(left_distance);
    let right_distance = CertifiedInterval::exact(right_distance);
    let q_left = |offset: CertifiedInterval| {
        left_distance.add(left_slope.mul(offset)).add(
            second_derivative
                .mul(offset.mul(offset))
                .div(CertifiedInterval::exact(2.0)),
        )
    };
    let q_right = |offset: CertifiedInterval| {
        right_distance.add(right_slope.mul(offset)).add(
            second_derivative
                .mul(offset.mul(offset))
                .div(CertifiedInterval::exact(2.0)),
        )
    };
    let zero = CertifiedInterval::exact(0.0);
    let negative_width = width.neg();
    let start = q_left(zero).upper().min(q_right(negative_width).upper());
    let end = q_left(width).upper().min(q_right(zero).upper());
    let mut upper = start.max(end);

    // The two upper Taylor quadratics have a linear difference. Enclose the
    // crossing parameter before evaluating either polynomial there.
    let slope = left_slope
        .sub(right_slope)
        .add(second_derivative.mul(width));
    let intercept = left_distance
        .sub(right_distance)
        .add(right_slope.mul(width))
        .sub(
            second_derivative
                .mul(width.mul(width))
                .div(CertifiedInterval::exact(2.0)),
        );
    if !slope.contains_zero() {
        let crossing = intercept.neg().div(slope);
        let domain = CertifiedInterval::from_bounds(0.0, width.upper());
        if crossing.intersects(domain) {
            let crossing = CertifiedInterval::from_bounds(
                crossing.lower().max(0.0),
                crossing.upper().min(width.upper()),
            );
            upper = upper.max(
                q_left(crossing)
                    .upper()
                    .min(q_right(crossing.sub(width)).upper()),
            );
        }
    }
    upper
}

fn outward_upper<M: GeodesicMetric>(metric: &M, value: f64) -> f64 {
    if !value.is_finite() {
        return f64::INFINITY;
    }
    let allowance = metric
        .geodesic_numerical_policy()
        .evaluation_allowance(4.0)
        .next_up();
    CertifiedInterval::exact(value)
        .add(CertifiedInterval::exact(allowance))
        .upper()
}

fn completion_limit<M: GeodesicMetric>(metric: &M, best: f64) -> f64 {
    CertifiedInterval::exact(best)
        .add(CertifiedInterval::exact(
            metric.geodesic_numerical_policy().hausdorff_gap(),
        ))
        .upper()
}

fn update_maximum(best: &mut Option<ChartHausdorffWitness>, candidate: &TargetEvaluation) {
    if best.is_none_or(|current| candidate.distance > current.distance) {
        *best = Some(ChartHausdorffWitness {
            #[cfg(test)]
            source: candidate.source,
            #[cfg(test)]
            target: candidate.target,
            distance: candidate.distance,
        });
    }
}

#[cfg(test)]
pub(crate) mod counters {
    use std::cell::Cell;

    thread_local! {
        static SOURCE_EVALUATIONS: Cell<u64> = const { Cell::new(0) };
    }

    pub(crate) fn reset() {
        SOURCE_EVALUATIONS.set(0);
    }

    pub(crate) fn source_evaluation() {
        SOURCE_EVALUATIONS.set(SOURCE_EVALUATIONS.get().saturating_add(1));
    }

    pub(crate) fn source_evaluations() -> u64 {
        SOURCE_EVALUATIONS.get()
    }
}

#[cfg(test)]
mod tests {
    use geographiclib_rs::{Geodesic, InverseGeodesic as _};

    use super::*;
    use crate::crs::EllipsoidMetric;
    use crate::geographic::GeographicFrame;
    fn target_evaluation(
        source: Point,
        target: Point,
        differential: ChartEdgeDifferential,
        metric: &(impl GeodesicMetric + ChartMetric<PointEdgeScratch = PointEdgeScratch>),
    ) -> TargetEvaluation {
        let ChartPointDistanceSample {
            distance,
            derivative,
        } = metric
            .chart_point_distance_sample(source, target, differential.speed, differential.azimuth)
            .expect("endpoint distance and derivative");
        let lower_bound = metric
            .point_distance_lower_bound(source, target)
            .expect("endpoint distance lower bound");
        TargetEvaluation {
            #[cfg(test)]
            source,
            target,
            distance,
            lower_bound: lower_bound.min(distance),
            derivative,
        }
    }

    fn chart_source(parameter: f64) -> Point {
        Point::new_raw_xy(10.0 * parameter, 0.0)
    }

    fn interval_upper(
        left: &TargetEvaluation,
        right: &TargetEvaluation,
        edge: &ChartEdge,
        metric: &(impl GeodesicMetric + ChartMetric<PointEdgeScratch = PointEdgeScratch>),
    ) -> f64 {
        let width = CertifiedInterval::exact(0.55)
            .sub(CertifiedInterval::exact(0.45))
            .upper();
        source_interval_upper(
            width,
            left,
            right,
            metric.chart_edge_differential_bounds(edge),
            metric,
        )
    }

    fn hessian_distance_region(
        left: &TargetEvaluation,
        right: &TargetEvaluation,
        speed: f64,
        metric: &impl GeodesicMetric,
    ) -> (f64, f64) {
        let width = CertifiedInterval::exact(0.55).sub(CertifiedInterval::exact(0.45));
        let source_span = CertifiedInterval::exact(speed).mul(width);
        let allowance = metric
            .geodesic_numerical_policy()
            .evaluation_allowance(4.0)
            .next_up();
        let lower = CertifiedInterval::exact(left.lower_bound)
            .add(CertifiedInterval::exact(right.lower_bound))
            .sub(source_span)
            .div(CertifiedInterval::exact(2.0))
            .lower()
            .max(0.0);
        let lower = CertifiedInterval::exact(lower)
            .sub(CertifiedInterval::exact(allowance))
            .lower()
            .max(0.0);
        let upper = CertifiedInterval::exact(left.distance.max(right.distance))
            .add(source_span)
            .add(CertifiedInterval::exact(allowance))
            .upper();
        (lower, upper)
    }

    #[test]
    fn singleton_target_taylor_upper_contains_attained_interior_distances() {
        let geodesic = Geodesic::wgs84();
        let frame = GeographicFrame::crs84();
        let metric = EllipsoidMetric::for_geodesic_at_frame(&geodesic, 0, frame);
        let edge = metric
            .chart_edge(Point::new_raw_xy(0.0, 0.0), Point::new_raw_xy(10.0, 0.0))
            .unwrap();
        let target = Point::new_raw_xy(5.0, 1.0);
        let left_source = chart_source(0.45);
        let right_source = chart_source(0.55);
        let left = target_evaluation(
            left_source,
            target,
            metric.chart_edge_differential(&edge, 0.45),
            &metric,
        );
        let right = target_evaluation(
            right_source,
            target,
            metric.chart_edge_differential(&edge, 0.55),
            &metric,
        );
        let bounds = metric.chart_edge_differential_bounds(&edge);
        let upper = source_interval_upper(
            CertifiedInterval::exact(0.55)
                .sub(CertifiedInterval::exact(0.45))
                .upper(),
            &left,
            &right,
            bounds,
            &metric,
        );
        let (distance_lower, distance_upper) =
            hessian_distance_region(&left, &right, bounds.speed, &metric);
        assert!(
            metric
                .geodesic_distance_hessian_upper_bound(distance_lower, distance_upper)
                .is_finite()
        );
        for parameter in [0.46, 0.5, 0.54] {
            let source = chart_source(parameter);
            let attained: f64 = geodesic.inverse(source.y, source.x, target.y, target.x);
            assert!(
                upper >= attained,
                "source t={parameter}: interval upper {upper} is below attained {attained}"
            );
        }
    }

    #[test]
    fn hausdorff_upper_contains_intervals_where_nearest_target_changes() {
        let geodesic = Geodesic::wgs84();
        let frame = GeographicFrame::crs84();
        let metric = EllipsoidMetric::for_geodesic_at_frame(&geodesic, 0, frame);
        let edge = metric
            .chart_edge(Point::new_raw_xy(0.0, 0.0), Point::new_raw_xy(10.0, 0.0))
            .unwrap();
        let left_target = Point::new_raw_xy(0.0, 0.5);
        let right_target = Point::new_raw_xy(10.0, 0.5);
        let left_source = chart_source(0.45);
        let right_source = chart_source(0.55);
        let left = target_evaluation(
            left_source,
            left_target,
            metric.chart_edge_differential(&edge, 0.45),
            &metric,
        );
        let right = target_evaluation(
            right_source,
            right_target,
            metric.chart_edge_differential(&edge, 0.55),
            &metric,
        );
        let upper = interval_upper(&left, &right, &edge, &metric);
        for parameter in [0.46, 0.5, 0.54] {
            let source = chart_source(parameter);
            let left_distance: f64 =
                geodesic.inverse(source.y, source.x, left_target.y, left_target.x);
            let right_distance: f64 =
                geodesic.inverse(source.y, source.x, right_target.y, right_target.x);
            let attained = left_distance.min(right_distance);
            assert!(
                upper >= attained,
                "source t={parameter}: interval upper {upper} is below target-set distance {attained}"
            );
        }
    }
}
