use std::cmp::Ordering;

use super::chart_parts::{ChartEdgeParameter, ChartEdgePart, ChartParts, point_chart_minimum};
use super::chart_scratch::ChartScratch;
#[cfg(test)]
use crate::crs::ChartEdge;
use crate::crs::PointEdgeScratch;
use crate::error::Result;
use crate::geometry::interval::Interval as CertifiedInterval;
use crate::geometry::{
    ChartEdgeDifferential, ChartEdgeDifferentialBounds, ChartMetric, ChartPointDistanceSample,
    GeodesicMetric, GeodesicNumericalPolicy, Point, PointEdgeQuery, PointEdgeQueryResult, Shape,
    XY,
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
    target_edge: Option<ChartEdgeParameter>,
    distance: f64,
    lower_bound: f64,
    derivative: f64,
}

#[derive(Clone, Copy)]
struct SourceIntervalSeed<'a> {
    source_edge_index: usize,
    parameters: [f64; 2],
    left_value: &'a TargetEvaluation,
    right_value: &'a TargetEvaluation,
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
    for (source_index, source_edge) in source.edges.iter().enumerate() {
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

        let limit = completion_limit(
            metric,
            best.as_ref().map_or(0.0, |witness| witness.distance),
        );
        let initial = source_interval(
            SourceIntervalSeed {
                source_edge_index: source_index,
                parameters: [0.0, 1.0],
                left_value: &left,
                right_value: &right,
            },
            source_edge,
            target,
            derivative_bounds,
            metric,
            limit,
            scratch,
        )?;
        if initial.upper <= limit {
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
                return Err(crate::crs::CrsError::message(
                    "geographic Hausdorff did not converge at binary64 source parameter precision",
                ));
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

            let limit = completion_limit(
                metric,
                best.as_ref().map_or(0.0, |witness| witness.distance),
            );
            let left = source_interval(
                SourceIntervalSeed {
                    source_edge_index: source_index,
                    parameters: [interval.left, middle],
                    left_value: &interval.left_value,
                    right_value: &value,
                },
                source_edge,
                target,
                derivative_bounds,
                metric,
                limit,
                scratch,
            )?;
            let right = source_interval(
                SourceIntervalSeed {
                    source_edge_index: source_index,
                    parameters: [middle, interval.right],
                    left_value: &value,
                    right_value: &interval.right_value,
                },
                source_edge,
                target,
                derivative_bounds,
                metric,
                limit,
                scratch,
            )?;
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
    if !counters::source_evaluation() {
        return Err(crate::crs::CrsError::message(
            "geographic Hausdorff test source-evaluation limit exceeded",
        ));
    }
    let (target_point, lower_bound, target_edge) = if target.edges.is_empty()
        && let [point] = target.point_only.as_slice()
    {
        // A singleton target has no minimum to search; the gradient sample
        // also supplies its attained distance.
        (
            *point,
            metric.point_distance_lower_bound(source, *point)?,
            None,
        )
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
        (
            candidate.second,
            candidate.lower_bound,
            candidate.target_edge,
        )
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
        target_edge,
        distance,
        lower_bound: lower_bound.min(distance),
        derivative,
    })
}

fn source_interval<M: GeodesicMetric + ChartMetric<PointEdgeScratch = PointEdgeScratch>>(
    seed: SourceIntervalSeed<'_>,
    source_edge: &ChartEdgePart,
    target: &ChartParts,
    derivative_bounds: ChartEdgeDifferentialBounds,
    metric: &M,
    limit: f64,
    scratch: &mut ChartScratch,
) -> Result<ChartHausdorffInterval> {
    let SourceIntervalSeed {
        source_edge_index,
        parameters: [left, right],
        left_value,
        right_value,
    } = seed;
    let mut upper = source_interval_upper(
        CertifiedInterval::exact(right)
            .sub(CertifiedInterval::exact(left))
            .upper(),
        left_value,
        right_value,
        derivative_bounds,
        metric,
    );

    let left_edge_index = left_value
        .target_edge
        .map(|edge| edge.edge_index)
        .filter(|&edge| edge != source_edge_index && edge < target.edges.len());
    let right_edge_index = right_value
        .target_edge
        .map(|edge| edge.edge_index)
        .filter(|&edge| {
            edge != source_edge_index && Some(edge) != left_edge_index && edge < target.edges.len()
        });

    let source_edge_candidate =
        (source_edge_index < target.edges.len()).then_some(source_edge_index);
    let candidate_edges = [source_edge_candidate, left_edge_index, right_edge_index];
    if candidate_edges.iter().any(Option::is_some) {
        let source_points = [
            source_edge.edge.chart_position(left)?,
            source_edge.edge.chart_position(right)?,
        ];
        for edge_index in candidate_edges.into_iter().flatten() {
            let Some(target_edge) = target.edges.get(edge_index) else {
                continue;
            };
            let (target_left, left_is_nearest) = moving_endpoint_parameter_seed(
                source_points[0],
                edge_index,
                target_edge,
                left_value.target_edge,
            )?;
            let (target_right, right_is_nearest) = moving_endpoint_parameter_seed(
                source_points[1],
                edge_index,
                target_edge,
                right_value.target_edge,
            )?;
            let target_parameters = [target_left, target_right];
            let Some(mut correspondence_upper) = moving_correspondence_upper(
                source_edge,
                target_edge,
                [left, right],
                target_parameters,
                &source_points,
                metric,
            )?
            else {
                continue;
            };
            if correspondence_upper > limit && correspondence_upper < upper {
                let mut refined_parameters = target_parameters;
                let refined = !left_is_nearest || !right_is_nearest;
                if !left_is_nearest {
                    refined_parameters[0] =
                        moving_endpoint_minimum(source_points[0], target_edge, metric, scratch)?;
                }
                if !right_is_nearest {
                    refined_parameters[1] =
                        moving_endpoint_minimum(source_points[1], target_edge, metric, scratch)?;
                }
                if refined
                    && let Some(refined_upper) = moving_correspondence_upper(
                        source_edge,
                        target_edge,
                        [left, right],
                        refined_parameters,
                        &source_points,
                        metric,
                    )?
                {
                    correspondence_upper = correspondence_upper.min(refined_upper);
                }
            }
            upper = upper.min(correspondence_upper);
        }
    }

    for witness in [left_value.target, right_value.target] {
        if let Some(pole_upper) =
            metric.chart_edge_pole_distance_upper_bound(&source_edge.edge, [left, right], witness)
        {
            upper = upper.min(outward_upper(metric, pole_upper));
        }
    }
    Ok(ChartHausdorffInterval {
        upper,
        left,
        right,
        left_value: *left_value,
        right_value: *right_value,
    })
}

fn moving_endpoint_parameter_seed(
    source: Point,
    target_edge_index: usize,
    target_edge: &ChartEdgePart,
    endpoint_witness: Option<ChartEdgeParameter>,
) -> Result<(f64, bool)> {
    if let Some(witness) = endpoint_witness
        && witness.edge_index == target_edge_index
    {
        return Ok((witness.parameter, true));
    }

    Ok((target_edge.edge.projection_parameter(source)?, false))
}

fn moving_endpoint_minimum<M>(
    source: Point,
    target_edge: &ChartEdgePart,
    metric: &M,
    scratch: &mut ChartScratch,
) -> Result<f64>
where
    M: GeodesicMetric + ChartMetric<PointEdgeScratch = PointEdgeScratch>,
{
    #[cfg(test)]
    counters::moving_endpoint_minimum();
    match metric.point_edge_query(
        source,
        &target_edge.edge,
        0.0,
        PointEdgeQuery::Minimum {
            cutoff: f64::INFINITY,
        },
        &mut scratch.point_edge,
    )? {
        PointEdgeQueryResult::Minimum(minimum) => Ok(minimum.parameter),
        PointEdgeQueryResult::Excluded => {
            unreachable!("an infinite-cutoff candidate search admits the edge")
        },
        PointEdgeQueryResult::Within { .. }
        | PointEdgeQueryResult::Outside { .. }
        | PointEdgeQueryResult::Unresolved { .. } => {
            unreachable!("candidate endpoint searches request a point-edge minimum")
        },
    }
}

fn moving_correspondence_upper<M>(
    source_edge: &ChartEdgePart,
    target_edge: &ChartEdgePart,
    source_interval: [f64; 2],
    target_interval: [f64; 2],
    source_points: &[Point; 2],
    metric: &M,
) -> Result<Option<f64>>
where
    M: GeodesicMetric + ChartMetric<PointEdgeScratch = PointEdgeScratch>,
{
    let target_points = [
        target_edge.edge.chart_position(target_interval[0])?,
        target_edge.edge.chart_position(target_interval[1])?,
    ];
    let endpoint_distances = [
        metric.segment_length(source_points[0], target_points[0]),
        metric.segment_length(source_points[1], target_points[1]),
    ];
    #[cfg(test)]
    counters::moving_correspondence_evaluation();
    Ok(metric.chart_edge_moving_correspondence_upper(
        &source_edge.edge,
        &target_edge.edge,
        source_interval,
        target_interval,
        source_points,
        &target_points,
        endpoint_distances,
    ))
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
        static SOURCE_EVALUATION_LIMIT: Cell<Option<u64>> = const { Cell::new(None) };
        static MOVING_CORRESPONDENCE_EVALUATIONS: Cell<u64> = const { Cell::new(0) };
        static MOVING_ENDPOINT_MINIMA: Cell<u64> = const { Cell::new(0) };
    }

    pub(crate) fn reset() {
        SOURCE_EVALUATIONS.set(0);
        SOURCE_EVALUATION_LIMIT.set(None);
        MOVING_CORRESPONDENCE_EVALUATIONS.set(0);
        MOVING_ENDPOINT_MINIMA.set(0);
    }

    pub(crate) fn set_source_evaluation_limit(limit: u64) {
        SOURCE_EVALUATION_LIMIT.set(Some(limit));
    }

    pub(crate) fn source_evaluation() -> bool {
        let evaluations = SOURCE_EVALUATIONS.get().saturating_add(1);
        SOURCE_EVALUATIONS.set(evaluations);
        SOURCE_EVALUATION_LIMIT
            .get()
            .is_none_or(|limit| evaluations <= limit)
    }

    pub(crate) fn source_evaluations() -> u64 {
        SOURCE_EVALUATIONS.get()
    }

    pub(crate) fn moving_correspondence_evaluation() {
        MOVING_CORRESPONDENCE_EVALUATIONS
            .set(MOVING_CORRESPONDENCE_EVALUATIONS.get().saturating_add(1));
    }

    pub(crate) fn moving_correspondence_evaluations() -> u64 {
        MOVING_CORRESPONDENCE_EVALUATIONS.get()
    }

    pub(crate) fn moving_endpoint_minimum() {
        MOVING_ENDPOINT_MINIMA.set(MOVING_ENDPOINT_MINIMA.get().saturating_add(1));
    }

    pub(crate) fn moving_endpoint_minima() -> u64 {
        MOVING_ENDPOINT_MINIMA.get()
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
            target_edge: None,
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

    fn moving_edge_upper(
        source: &ChartEdge,
        target: &ChartEdge,
        metric: &EllipsoidMetric<'_>,
        source_interval: [f64; 2],
        target_interval: [f64; 2],
    ) -> f64 {
        let source_points = [
            source.chart_position(source_interval[0]).unwrap(),
            source.chart_position(source_interval[1]).unwrap(),
        ];
        let target_points = [
            target.chart_position(target_interval[0]).unwrap(),
            target.chart_position(target_interval[1]).unwrap(),
        ];
        let endpoint_distances = [
            metric.segment_length(source_points[0], target_points[0]),
            metric.segment_length(source_points[1], target_points[1]),
        ];
        metric
            .chart_edge_moving_correspondence_upper(
                source,
                target,
                source_interval,
                target_interval,
                &source_points,
                &target_points,
                endpoint_distances,
            )
            .expect("the paired chart interval has a pole-free transport strip")
    }

    fn assert_moving_upper_contains_samples(
        geodesic: &Geodesic,
        frame: GeographicFrame,
        source_coordinates: &[Point; 2],
        target_coordinates: &[Point; 2],
        source_interval: [f64; 2],
        target_interval: [f64; 2],
        name: &str,
    ) -> (f64, f64) {
        let metric = EllipsoidMetric::for_geodesic_at_frame(geodesic, 0, frame);
        let source = metric
            .chart_edge(source_coordinates[0], source_coordinates[1])
            .unwrap();
        let target = metric
            .chart_edge(target_coordinates[0], target_coordinates[1])
            .unwrap();
        let upper = moving_edge_upper(&source, &target, &metric, source_interval, target_interval);
        let mut sampled_max = 0.0_f64;
        for index in 0..=1024 {
            let fraction = f64::from(index) / 1024.0;
            let source_parameter =
                source_interval[0] + (source_interval[1] - source_interval[0]) * fraction;
            let target_parameter =
                target_interval[0] + (target_interval[1] - target_interval[0]) * fraction;
            let source_point = frame
                .to_geodesic_point(source.chart_position(source_parameter).unwrap())
                .unwrap();
            let target_point = frame
                .to_geodesic_point(target.chart_position(target_parameter).unwrap())
                .unwrap();
            let distance: f64 = geodesic.inverse(
                source_point.y,
                source_point.x,
                target_point.y,
                target_point.x,
            );
            sampled_max = sampled_max.max(distance);
            assert!(
                upper >= distance,
                "{name}: t={fraction}, moving upper {upper} < paired distance {distance}"
            );
        }
        (upper, sampled_max)
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

    #[test]
    fn moving_correspondence_bounds_cover_offsets_and_metric_deformation() {
        let wgs84 = Geodesic::wgs84();
        let frame = GeographicFrame::crs84();
        let source = [Point::new_raw_xy(10.0, 60.0), Point::new_raw_xy(12.5, 60.0)];
        for (name, target_longitudes) in [
            ("exact parallel", [10.0, 12.5]),
            ("one-ULP parallel", [10.0_f64.next_up(), 12.5_f64.next_up()]),
            ("1e-9 parallel", [10.0 + 1.0e-9, 12.5 + 1.0e-9]),
            ("1e-6 parallel", [10.0 + 1.0e-6, 12.5 + 1.0e-6]),
        ] {
            let target = [
                Point::new_raw_xy(target_longitudes[0], 60.01),
                Point::new_raw_xy(target_longitudes[1], 60.01),
            ];
            let (upper, sampled_max) = assert_moving_upper_contains_samples(
                &wgs84,
                frame,
                &source,
                &target,
                [0.0, 1.0],
                [0.0, 1.0],
                name,
            );
            let endpoint_distances: [f64; 2] = [
                wgs84.inverse(60.0, 10.0, 60.01, target_longitudes[0]),
                wgs84.inverse(60.0, 12.5, 60.01, target_longitudes[1]),
            ];
            assert!(
                upper <= endpoint_distances[0].max(endpoint_distances[1]) + 0.001,
                "{name}: upper {upper} materially exceeds its endpoint witness"
            );
            assert!(sampled_max <= upper, "{name}");
        }

        let oblique_source = [
            Point::new_raw_xy(10.0, 60.0),
            Point::new_raw_xy(12.5, 60.05),
        ];
        let oblique_target = [
            Point::new_raw_xy(10.000_001, 60.01),
            Point::new_raw_xy(12.500_001, 60.06),
        ];
        assert_moving_upper_contains_samples(
            &wgs84,
            frame,
            &oblique_source,
            &oblique_target,
            [0.1, 0.9],
            [0.15, 0.85],
            "oblique partial correspondence",
        );

        let prolate = Geodesic::new(6_378_137.0, -0.02);
        assert_moving_upper_contains_samples(
            &prolate,
            frame,
            &oblique_source,
            &oblique_target,
            [0.0, 1.0],
            [0.0, 1.0],
            "prolate metric deformation",
        );

        let meridian_pair_source = [Point::new_raw_xy(10.0, 60.0), Point::new_raw_xy(10.0, 62.0)];
        let meridian_pair_target = [
            Point::new_raw_xy(10.0, 60.01),
            Point::new_raw_xy(10.0, 62.01),
        ];
        let (upper, sampled_max) = assert_moving_upper_contains_samples(
            &wgs84,
            frame,
            &meridian_pair_source,
            &meridian_pair_target,
            [0.0, 1.0],
            [0.0, 1.0],
            "WGS84 common-meridian motion",
        );
        let meridian_at_60: f64 = wgs84.inverse(60.0, 10.0, 60.01, 10.0);
        let meridian_at_62: f64 = wgs84.inverse(62.0, 10.0, 62.01, 10.0);
        assert!((meridian_at_60 - 1_114.123_724_286_158_6).abs() < 1.0e-6);
        assert!((meridian_at_62 - 1_114.456_553_643_796_5).abs() < 1.0e-6);
        assert!(sampled_max + 1.0e-6 >= meridian_at_62);
        assert!(upper + 1.0e-6 >= meridian_at_62);

        let meridian_source = [Point::new_raw_xy(10.0, -1.0), Point::new_raw_xy(10.0, 1.0)];
        let meridian_target = [
            Point::new_raw_xy(10.01, -1.0),
            Point::new_raw_xy(10.01, 1.0),
        ];
        let sphere = Geodesic::new(6_378_137.0, 0.0);
        let (upper, sampled_max) = assert_moving_upper_contains_samples(
            &sphere,
            frame,
            &meridian_source,
            &meridian_target,
            [0.0, 1.0],
            [0.0, 1.0],
            "spherical meridian deformation control",
        );
        let endpoint_distances: [f64; 2] = [
            sphere.inverse(-1.0, 10.0, -1.0, 10.01),
            sphere.inverse(1.0, 10.0, 1.0, 10.01),
        ];
        let endpoint_max = endpoint_distances[0].max(endpoint_distances[1]);
        let equatorial_distance = 6_378_137.0 * 0.01_f64.to_radians();
        assert!(endpoint_max < equatorial_distance);
        assert!(sampled_max + 1.0e-6 >= equatorial_distance);
        assert!(
            upper + 1.0e-6 >= equatorial_distance,
            "spherical meridian upper {upper} missed the equatorial maximum {equatorial_distance}"
        );
    }
}
