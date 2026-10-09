use std::cmp::Ordering;

use super::chart_parts::{ChartParts, point_chart_minimum};
use super::chart_scratch::ChartScratch;
use crate::crs::{ChartEdge, PointEdgeScratch};
use crate::error::Result;
use crate::geometry::{
    ChartEdgeDifferential, ChartEdgeDifferentialBounds, ChartMetric, ChartPointDistanceSample,
    GeodesicMetric, Point, Shape, XY,
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
        right - left,
        left_value,
        right_value,
        derivative_bounds,
        metric,
    );
    if let Some(pole_upper) = metric
        .chart_edge_pole_distance_upper_bound(edge, left_value.target)
        .or_else(|| metric.chart_edge_pole_distance_upper_bound(edge, right_value.target))
    {
        upper = upper.min(outward_upper(metric, pole_upper, pole_upper));
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
    let lipschitz = lipschitz_upper(
        left.distance,
        right.distance,
        width,
        derivative_bounds.speed,
    );
    let lower_distance =
        ((left.lower_bound + right.lower_bound - derivative_bounds.speed * width) / 2.0).max(0.0);
    let distance_hessian = metric.geodesic_distance_hessian_upper_bound(lower_distance);
    let second_derivative = derivative_bounds.speed * derivative_bounds.speed * distance_hessian
        + derivative_bounds.covariant_acceleration;
    if !second_derivative.is_finite() {
        return outward_upper(
            metric,
            lipschitz,
            lipschitz + derivative_bounds.speed * width,
        );
    }

    let taylor = taylor_upper(
        width,
        left.distance,
        left.derivative,
        right.distance,
        right.derivative,
        second_derivative,
    );
    let upper = lipschitz.min(taylor);
    outward_upper(
        metric,
        upper,
        upper.abs() + derivative_bounds.speed * width + second_derivative * width * width,
    )
}

fn lipschitz_upper(left: f64, right: f64, width: f64, speed: f64) -> f64 {
    left.max(right).max((left + right + speed * width) / 2.0)
}

fn taylor_upper(
    width: f64,
    left_distance: f64,
    left_derivative: f64,
    right_distance: f64,
    right_derivative: f64,
    second_derivative: f64,
) -> f64 {
    let left_at_start = left_distance;
    let right_at_start =
        right_distance - right_derivative * width + second_derivative * width * width / 2.0;
    let mut upper = left_at_start.min(right_at_start);

    let left_at_end =
        left_distance + left_derivative * width + second_derivative * width * width / 2.0;
    let right_at_end = right_distance;
    upper = upper.max(left_at_end.min(right_at_end));

    // The two Taylor quadratics share their second-order term, so their
    // crossing is linear. Between crossings, the smaller convex bound reaches
    // its maximum at an interval endpoint.
    let slope = left_derivative - right_derivative + second_derivative * width;
    let intercept = left_distance - right_distance + right_derivative * width
        - second_derivative * width * width / 2.0;
    if slope != 0.0 {
        let crossing = -intercept / slope;
        if (0.0..=width).contains(&crossing) {
            let left_value = left_distance
                + left_derivative * crossing
                + second_derivative * crossing * crossing / 2.0;
            let remaining = crossing - width;
            let right_value = right_distance
                + right_derivative * remaining
                + second_derivative * remaining * remaining / 2.0;
            upper = upper.max(left_value.min(right_value));
        }
    }
    upper
}

fn outward_upper<M: GeodesicMetric>(metric: &M, value: f64, scale: f64) -> f64 {
    if !value.is_finite() {
        return f64::INFINITY;
    }
    let rounding = 128.0 * f64::EPSILON * scale.abs().max(1.0);
    let allowance = metric.geodesic_numerical_policy().evaluation_allowance(4.0) + rounding;
    (value + allowance).next_up()
}

fn completion_limit<M: GeodesicMetric>(metric: &M, best: f64) -> f64 {
    best + metric.geodesic_numerical_policy().hausdorff_gap()
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
