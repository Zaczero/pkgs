//! Point-to-chart-edge geodesic minimum and threshold queries.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use geographiclib_rs::Geodesic;

use super::chart_edge::ChartEdge;
use super::ellipsoid_metric::inverse_distance_azimuths;
use crate::error::Result;
use crate::geographic::GeographicFrame;
use crate::geometry::{
    ChartEdgeDifferentialBounds, GeodesicNumericalPolicy, Point, PointEdgeMinimum, PointEdgeQuery,
    PointEdgeQueryResult,
};

const MAX_EVALUATIONS: usize = 128;
const CURVATURE_CUT_LOCUS_FRACTION: f64 = 0.999;

#[derive(Default)]
pub(crate) struct PointEdgeScratch {
    frontier: BinaryHeap<EdgeInterval>,
    sequence: u64,
}

impl PointEdgeScratch {
    pub(crate) const fn new() -> Self {
        Self {
            frontier: BinaryHeap::new(),
            sequence: 0,
        }
    }

    pub(crate) fn clear(&mut self) {
        self.frontier.clear();
        self.sequence = 0;
    }

    pub(crate) fn retained_bytes(&self) -> usize {
        self.frontier
            .capacity()
            .saturating_mul(std::mem::size_of::<EdgeInterval>())
    }
}

#[derive(Clone, Copy, Debug)]
struct EdgeInterval {
    lower_bound: f64,
    sequence: u64,
    t0: f64,
    d0: f64,
    g0: f64,
    t1: f64,
    d1: f64,
    g1: f64,
}

impl PartialEq for EdgeInterval {
    fn eq(&self, other: &Self) -> bool {
        self.lower_bound.total_cmp(&other.lower_bound) == Ordering::Equal
            && self.sequence == other.sequence
    }
}

impl Eq for EdgeInterval {}

impl PartialOrd for EdgeInterval {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for EdgeInterval {
    fn cmp(&self, other: &Self) -> Ordering {
        other
            .lower_bound
            .total_cmp(&self.lower_bound)
            .then_with(|| other.sequence.cmp(&self.sequence))
    }
}

#[expect(
    clippy::float_cmp,
    reason = "an exact pole coordinate selects a distinct analytic distance family"
)]
pub(crate) fn point_edge_query(
    geodesic: &Geodesic,
    frame: GeographicFrame,
    point: Point,
    edge: &ChartEdge,
    envelope_lower: f64,
    query: PointEdgeQuery,
    policy: GeodesicNumericalPolicy,
    scratch: &mut PointEdgeScratch,
) -> Result<PointEdgeQueryResult> {
    scratch.clear();
    let source_point = point;
    let point = frame
        .to_geodesic_point(point)
        .expect("geographic point was admitted before chart-edge distance");

    let (cutoff, threshold, locate) = match query {
        PointEdgeQuery::Minimum { cutoff } | PointEdgeQuery::Nearest { cutoff } => {
            (cutoff, None, false)
        },
        PointEdgeQuery::Locate { cutoff } => (cutoff, None, true),
        PointEdgeQuery::Dwithin { limit } => {
            if limit.is_nan() {
                return Err(crate::crs::CrsError::message(
                    "geodesic dwithin limit is not finite",
                ));
            }
            if limit < 0.0 || limit.is_infinite() && limit.is_sign_negative() {
                return Ok(PointEdgeQueryResult::Outside {
                    lower_bound: 0.0,
                    evaluations: 0,
                    budget_exhausted: false,
                });
            }
            if limit.is_infinite() {
                return Ok(PointEdgeQueryResult::Within { evaluations: 0 });
            }
            (f64::INFINITY, Some(limit), false)
        },
    };
    let ranks_witness = matches!(
        query,
        PointEdgeQuery::Nearest { .. } | PointEdgeQuery::Locate { .. }
    );

    if envelope_lower >= cutoff {
        return Ok(PointEdgeQueryResult::Excluded);
    }
    if let Some(limit) = threshold
        && envelope_lower > limit
    {
        return Ok(PointEdgeQueryResult::Outside {
            lower_bound: envelope_lower,
            evaluations: 0,
            budget_exhausted: false,
        });
    }

    let midpoint_derivative = edge.derivatives(0.5, geodesic);
    if midpoint_derivative.speed == 0.0 {
        let distance = inverse_distance(point, edge.geodetic_position(0.0)?, geodesic);
        return Ok(direct_result(distance, 0.0, 1, cutoff, threshold));
    }

    if point.y.abs() == 90.0 {
        let start = edge.geodetic_position(0.0)?;
        let end = edge.geodetic_position(1.0)?;
        let parameter = if point.y > 0.0 {
            if end.y > start.y { 1.0 } else { 0.0 }
        } else {
            if end.y < start.y { 1.0 } else { 0.0 }
        };
        let target = if parameter == 0.0 { start } else { end };
        let distance = inverse_distance(point, target, geodesic);
        return Ok(direct_result(distance, parameter, 1, cutoff, threshold));
    }

    let (d0, azimuth0) = sample(geodesic, point, edge, 0.0)?;
    if let Some(limit) = threshold
        && d0 <= limit
    {
        return Ok(PointEdgeQueryResult::Within { evaluations: 1 });
    }
    let (d1, azimuth1) = sample(geodesic, point, edge, 1.0)?;
    if let Some(limit) = threshold
        && d1 <= limit
    {
        return Ok(PointEdgeQueryResult::Within { evaluations: 2 });
    }
    let mut best = if d0 <= d1 {
        PointEdgeMinimum {
            distance: d0,
            parameter: 0.0,
            lower_bound: 0.0,
            evaluations: 2,
            budget_exhausted: false,
        }
    } else {
        PointEdgeMinimum {
            distance: d1,
            parameter: 1.0,
            lower_bound: 0.0,
            evaluations: 2,
            budget_exhausted: false,
        }
    };
    // Witness ties must not change the numerical anchor used by correction.
    let mut witness_parameter = best.parameter;
    if best.distance == 0.0 {
        best.lower_bound = 0.0;
        return Ok(direct_result(
            best.distance,
            witness_parameter,
            2,
            cutoff,
            threshold,
        ));
    }

    let start_derivative = edge.derivatives(0.0, geodesic);
    let end_derivative = edge.derivatives(1.0, geodesic);
    let derivative_bounds = edge.differential_bounds(geodesic);
    let g0 = distance_derivative(start_derivative, d0, azimuth0);
    let g1 = distance_derivative(end_derivative, d1, azimuth1);
    let mut best_correction = 0.0;
    // Minimum certifies an objective gap. Locate also certifies a station, so
    // its evaluated chart projection splits the frontier before refinement.
    let proposal = if locate {
        let parameter = edge.projection_parameter(source_point)?;
        if parameter > 0.0 && parameter < 1.0 {
            let (distance, azimuth) = sample(geodesic, point, edge, parameter)?;
            best.evaluations = best.evaluations.wrapping_add(1);
            if ranks_witness {
                update_witness_parameter(&best, &mut witness_parameter, distance, parameter, edge);
            }
            update_minimum(&mut best, distance, parameter);
            if best.distance == 0.0 {
                best.lower_bound = 0.0;
                return Ok(direct_result(
                    best.distance,
                    witness_parameter,
                    best.evaluations,
                    cutoff,
                    threshold,
                ));
            }
            Some(IntervalEndpoints {
                t0: 0.0,
                d0,
                g0,
                t1: parameter,
                d1: distance,
                g1: distance_derivative(edge.derivatives(parameter, geodesic), distance, azimuth),
            })
        } else {
            None
        }
    } else {
        None
    };
    if let Some(left) = proposal {
        let parameter = left.t1;
        let right = IntervalEndpoints {
            t0: parameter,
            d0: left.d1,
            g0: left.g1,
            t1: 1.0,
            d1,
            g1,
        };
        push_interval(scratch, left, derivative_bounds, geodesic, policy);
        push_interval(scratch, right, derivative_bounds, geodesic, policy);
    } else {
        push_interval(
            scratch,
            IntervalEndpoints {
                t0: 0.0,
                d0,
                g0,
                t1: 1.0,
                d1,
                g1,
            },
            derivative_bounds,
            geodesic,
            policy,
        );
    }

    loop {
        let Some(interval) = scratch.frontier.pop() else {
            best.lower_bound = best.distance;
            break;
        };
        if let Some(limit) = threshold
            && interval.lower_bound > limit
        {
            best.lower_bound = interval.lower_bound.min(best.distance);
            return Ok(PointEdgeQueryResult::Outside {
                lower_bound: best.lower_bound,
                evaluations: best.evaluations,
                budget_exhausted: false,
            });
        }
        if threshold.is_none() && interval.lower_bound >= best.distance - policy.minimum_gap() {
            best.lower_bound = interval.lower_bound.min(best.distance);
            break;
        }
        if best.evaluations >= MAX_EVALUATIONS {
            best.lower_bound = interval.lower_bound.min(best.distance);
            best.budget_exhausted = true;
            break;
        }

        let width = interval.t1 - interval.t0;
        let parameter = if interval.g0 < 0.0 && interval.g1 > 0.0 {
            interval.t0 + width * (-interval.g0 / (interval.g1 - interval.g0)).clamp(0.1, 0.9)
        } else {
            f64::midpoint(interval.t0, interval.t1)
        };
        let (distance, azimuth) = sample(geodesic, point, edge, parameter)?;
        best.evaluations = best.evaluations.wrapping_add(1);
        if let Some(limit) = threshold
            && distance <= limit
        {
            return Ok(PointEdgeQueryResult::Within {
                evaluations: best.evaluations,
            });
        }
        if ranks_witness {
            update_witness_parameter(&best, &mut witness_parameter, distance, parameter, edge);
        }
        let improved = update_minimum(&mut best, distance, parameter);
        if best.distance == 0.0 {
            best.lower_bound = 0.0;
            break;
        }

        let geometry = edge.derivatives(parameter, geodesic);
        let derivative = distance_derivative(geometry, distance, azimuth);
        #[expect(
            clippy::suspicious_operation_groupings,
            reason = "the distance gradient divided by squared chart speed is the tangent projection correction"
        )]
        if improved {
            best_correction = distance * derivative / (geometry.speed * geometry.speed);
        }
        push_interval(
            scratch,
            IntervalEndpoints {
                t0: interval.t0,
                d0: interval.d0,
                g0: interval.g0,
                t1: parameter,
                d1: distance,
                g1: derivative,
            },
            derivative_bounds,
            geodesic,
            policy,
        );
        push_interval(
            scratch,
            IntervalEndpoints {
                t0: parameter,
                d0: distance,
                g0: derivative,
                t1: interval.t1,
                d1: interval.d1,
                g1: interval.g1,
            },
            derivative_bounds,
            geodesic,
            policy,
        );
    }

    if threshold.is_none()
        && best.evaluations < MAX_EVALUATIONS
        && best.parameter > 0.0
        && best.parameter < 1.0
        && best.distance > 0.0
    {
        let parameter = best.parameter - best_correction;
        if parameter > 0.0 && parameter < 1.0 && parameter != best.parameter {
            let (distance, _) = sample(geodesic, point, edge, parameter)?;
            best.evaluations = best.evaluations.wrapping_add(1);
            if ranks_witness {
                update_witness_parameter(&best, &mut witness_parameter, distance, parameter, edge);
            }
            update_minimum(&mut best, distance, parameter);
            best.lower_bound = best.lower_bound.min(best.distance);
        }
    }
    if ranks_witness {
        best.parameter = witness_parameter;
    }
    Ok(finalize_query(best, cutoff, threshold))
}

#[expect(
    clippy::float_cmp,
    reason = "witness roles apply only to equal backend measurements"
)]
fn update_witness_parameter(
    best: &PointEdgeMinimum,
    witness_parameter: &mut f64,
    distance: f64,
    parameter: f64,
    edge: &ChartEdge,
) {
    if distance < best.distance
        || distance == best.distance
            && edge
                .witness_role(parameter)
                .cmp_tie(edge.witness_role(*witness_parameter), || {
                    parameter.total_cmp(witness_parameter)
                })
                .is_lt()
    {
        *witness_parameter = parameter;
    }
}

#[expect(
    clippy::float_cmp,
    reason = "exact attained-distance ties retain the earliest parameter on this source edge"
)]
fn update_minimum(best: &mut PointEdgeMinimum, distance: f64, parameter: f64) -> bool {
    if distance < best.distance || (distance == best.distance && parameter < best.parameter) {
        best.distance = distance;
        best.parameter = parameter;
        true
    } else {
        false
    }
}

fn finalize_query(
    best: PointEdgeMinimum,
    cutoff: f64,
    threshold: Option<f64>,
) -> PointEdgeQueryResult {
    match direct_result(
        best.distance,
        best.parameter,
        best.evaluations,
        cutoff,
        threshold,
    ) {
        PointEdgeQueryResult::Minimum(mut minimum) => {
            minimum.lower_bound = best.lower_bound;
            minimum.budget_exhausted = best.budget_exhausted;
            PointEdgeQueryResult::Minimum(minimum)
        },
        PointEdgeQueryResult::Outside { .. } => PointEdgeQueryResult::Outside {
            lower_bound: best.lower_bound,
            evaluations: best.evaluations,
            budget_exhausted: best.budget_exhausted,
        },
        PointEdgeQueryResult::Excluded => PointEdgeQueryResult::Excluded,
        PointEdgeQueryResult::Within { evaluations } => {
            PointEdgeQueryResult::Within { evaluations }
        },
    }
}

fn inverse_distance(point: Point, target: Point, geodesic: &Geodesic) -> f64 {
    super::ellipsoid_metric::inverse_distance(geodesic, point.x, point.y, target.x, target.y)
}

fn direct_result(
    distance: f64,
    parameter: f64,
    evaluations: usize,
    cutoff: f64,
    threshold: Option<f64>,
) -> PointEdgeQueryResult {
    if let Some(limit) = threshold {
        return if distance <= limit {
            PointEdgeQueryResult::Within { evaluations }
        } else {
            PointEdgeQueryResult::Outside {
                lower_bound: distance,
                evaluations,
                budget_exhausted: false,
            }
        };
    }
    if distance >= cutoff {
        return PointEdgeQueryResult::Excluded;
    }
    PointEdgeQueryResult::Minimum(PointEdgeMinimum {
        distance,
        parameter,
        lower_bound: distance,
        evaluations,
        budget_exhausted: false,
    })
}

fn sample(
    geodesic: &Geodesic,
    point: Point,
    edge: &ChartEdge,
    parameter: f64,
) -> crate::error::Result<(f64, f64)> {
    let target = edge.geodetic_position(parameter)?;
    let (distance, _azimuth1, azimuth2) =
        inverse_distance_azimuths(geodesic, point.x, point.y, target.x, target.y);
    Ok((distance, azimuth2))
}

fn distance_derivative(
    derivative: super::chart_edge::ChartEdgeDerivatives,
    distance: f64,
    azimuth: f64,
) -> f64 {
    if distance == 0.0 {
        return 0.0;
    }
    derivative.speed * (derivative.azimuth - azimuth.to_radians()).cos()
}

#[derive(Clone, Copy)]
struct IntervalEndpoints {
    t0: f64,
    d0: f64,
    g0: f64,
    t1: f64,
    d1: f64,
    g1: f64,
}

fn push_interval(
    scratch: &mut PointEdgeScratch,
    endpoints: IntervalEndpoints,
    derivative_bounds: ChartEdgeDifferentialBounds,
    geodesic: &Geodesic,
    policy: GeodesicNumericalPolicy,
) {
    let lower_bound = interval_lower_bound(endpoints, derivative_bounds, geodesic, policy);
    scratch.sequence = scratch.sequence.wrapping_add(1);
    scratch.frontier.push(EdgeInterval {
        lower_bound,
        sequence: scratch.sequence,
        t0: endpoints.t0,
        d0: endpoints.d0,
        g0: endpoints.g0,
        t1: endpoints.t1,
        d1: endpoints.d1,
        g1: endpoints.g1,
    });
}

fn interval_lower_bound(
    interval: IntervalEndpoints,
    derivative_bounds: ChartEdgeDifferentialBounds,
    geodesic: &Geodesic,
    policy: GeodesicNumericalPolicy,
) -> f64 {
    let width = interval.t1 - interval.t0;
    let lipschitz = ((interval.d0 + interval.d1 - derivative_bounds.speed * width) / 2.0).max(0.0);
    let d_lower = lipschitz;
    let curvature = curvature_hessian_bound(geodesic, d_lower);
    let second_derivative = derivative_bounds.speed * derivative_bounds.speed * curvature
        + derivative_bounds.covariant_acceleration;
    let mut lower = lipschitz;
    if second_derivative.is_finite() {
        let l0 = |t: f64| {
            interval.d0 + interval.g0 * (t - interval.t0)
                - second_derivative * (t - interval.t0) * (t - interval.t0) / 2.0
        };
        let l1 = |t: f64| {
            interval.d1 + interval.g1 * (t - interval.t1)
                - second_derivative * (t - interval.t1) * (t - interval.t1) / 2.0
        };
        let mut parabola = l0(interval.t0).max(l1(interval.t0));
        parabola = parabola.min(l0(interval.t1).max(l1(interval.t1)));

        // The two supporting parabolas share their quadratic term, so their
        // crossing is the root of a linear equation.
        let a = interval.g0 - interval.g1 + second_derivative * (interval.t0 - interval.t1);
        let c = interval.d0 - interval.d1 - interval.g0 * interval.t0
            + interval.g1 * interval.t1
            + second_derivative / 2.0 * (interval.t0 - interval.t1) * (-interval.t0 - interval.t1);
        if a != 0.0 {
            let crossing = -c / a;
            if (interval.t0..=interval.t1).contains(&crossing) {
                parabola = parabola.min(l0(crossing).max(l1(crossing)));
            }
        }
        lower = lower.max(parabola);
    }
    (lower - policy.backend_floor()).max(0.0)
}

pub(super) fn curvature_hessian_bound(geodesic: &Geodesic, distance: f64) -> f64 {
    if distance <= 0.0 {
        return f64::INFINITY;
    }
    let e2 = geodesic.f * (2.0 - geodesic.f);
    let denominator = geodesic.a * geodesic.a * (1.0 - e2);
    let equatorial = 1.0 / denominator;
    let polar = (1.0 - e2) * (1.0 - e2) / denominator;
    let curvature_high = equatorial.max(polar);
    let root = curvature_high.sqrt();
    let angle = root * distance;
    if angle < std::f64::consts::FRAC_PI_2 {
        return 1.0 / distance;
    }
    if angle >= CURVATURE_CUT_LOCUS_FRACTION * std::f64::consts::PI {
        return f64::INFINITY;
    }
    root * angle.tan().recip().abs()
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use geographiclib_rs::InverseGeodesic as _;

    use super::*;
    use crate::crs::GeodesicMetric as EllipsoidChartMetric;
    use crate::crs::geodesic::AuthalicSeries;
    use crate::geometry::ChartMetric as _;

    fn metric(geodesic: &Geodesic, frame: GeographicFrame) -> EllipsoidChartMetric {
        let authalic_series = AuthalicSeries::new(geodesic);
        EllipsoidChartMetric {
            crs: Arc::new("test geographic CRS".to_owned()),
            geodesic: Box::new(*geodesic),
            authalic_series,
            frame,
            state: crate::crs::runtime_state_snapshot().expect("runtime state"),
        }
    }

    fn point_edge_minimum(
        metric: &EllipsoidChartMetric,
        point: Point,
        start: Point,
        end: Point,
    ) -> PointEdgeMinimum {
        let edge = metric.chart_edge(start, end).expect("admitted chart edge");
        let mut scratch = PointEdgeScratch::default();
        match metric
            .point_edge_query(
                point,
                &edge,
                metric.point_envelope_bound(point)(edge.bounds()),
                PointEdgeQuery::Minimum {
                    cutoff: f64::INFINITY,
                },
                &mut scratch,
            )
            .expect("point-edge query")
        {
            PointEdgeQueryResult::Minimum(minimum) => minimum,
            result @ (PointEdgeQueryResult::Excluded
            | PointEdgeQueryResult::Within { .. }
            | PointEdgeQueryResult::Outside { .. }) => {
                panic!("unbounded point-edge query did not return a minimum: {result:?}")
            },
        }
    }

    fn oracle_distance(
        metric: &EllipsoidChartMetric,
        point: Point,
        edge: &ChartEdge,
        parameter: f64,
    ) -> f64 {
        let point = metric
            .frame
            .to_geodesic_point(point)
            .expect("oracle point is admitted");
        let target = edge.geodetic_position(parameter).unwrap();
        let distance: f64 = metric
            .geodesic
            .inverse(point.y, point.x, target.y, target.x);
        distance
    }

    fn dense_oracle(metric: &EllipsoidChartMetric, point: Point, edge: &ChartEdge) -> f64 {
        const LAST_SAMPLE: u32 = 4_000;
        let mut samples = Vec::with_capacity(LAST_SAMPLE as usize + 1);
        let mut best_distance = f64::INFINITY;
        for index in 0..=LAST_SAMPLE {
            let parameter = f64::from(index) / f64::from(LAST_SAMPLE);
            let distance = oracle_distance(metric, point, edge, parameter);
            best_distance = best_distance.min(distance);
            samples.push(distance);
        }
        if let Some(first) = samples.first() {
            best_distance = best_distance.min(*first);
        }
        if let Some(last) = samples.last() {
            best_distance = best_distance.min(*last);
        }
        let golden = 0.5 * (5.0_f64.sqrt() - 1.0);
        let parameter_step = 1.0 / f64::from(LAST_SAMPLE);
        for (index, window) in (1..LAST_SAMPLE).zip(samples.windows(3)) {
            let [before, current, after] = window else {
                unreachable!("a three-sample window has three values")
            };
            if current <= before && current <= after && (current < before || current < after) {
                let center = f64::from(index) * parameter_step;
                let mut left = center - parameter_step;
                let mut right = center + parameter_step;
                let mut x1 = right - golden * (right - left);
                let mut x2 = left + golden * (right - left);
                let mut f1 = oracle_distance(metric, point, edge, x1);
                let mut f2 = oracle_distance(metric, point, edge, x2);
                for _ in 0..80 {
                    if f1 <= f2 {
                        right = x2;
                        x2 = x1;
                        f2 = f1;
                        x1 = right - golden * (right - left);
                        f1 = oracle_distance(metric, point, edge, x1);
                    } else {
                        left = x1;
                        x1 = x2;
                        f1 = f2;
                        x2 = left + golden * (right - left);
                        f2 = oracle_distance(metric, point, edge, x2);
                    }
                }
                best_distance = best_distance.min(f1).min(f2);
            }
        }
        best_distance
    }

    #[test]
    fn point_edge_fixed_minima_agree_with_dense_karney_and_golden_oracle() {
        let frame = GeographicFrame::crs84();
        let metric = metric(&Geodesic::wgs84(), frame);
        let cases = [
            (
                "short equatorial interior",
                Point::new_raw_xy(0.0, 0.001),
                Point::new_raw_xy(-0.005, 0.0),
                Point::new_raw_xy(0.005, 0.0),
                0.4..=0.6,
            ),
            (
                "start endpoint",
                Point::new_raw_xy(-3.0, 1.0),
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(2.0, 0.0),
                0.0..=0.0,
            ),
            (
                "end endpoint",
                Point::new_raw_xy(3.0, 1.0),
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(2.0, 0.0),
                1.0..=1.0,
            ),
            (
                "long oblique interior",
                Point::new_raw_xy(30.0, 22.0),
                Point::new_raw_xy(-70.0, 10.0),
                Point::new_raw_xy(80.0, 55.0),
                0.01..=0.99,
            ),
            (
                "seam interior",
                Point::new_raw_xy(180.0, 20.0),
                Point::new_raw_xy(175.0, 10.0),
                Point::new_raw_xy(-175.0, 10.0),
                0.4..=0.6,
            ),
        ];
        for (name, point, start, end, parameter_range) in cases {
            let edge = metric.chart_edge(start, end).expect("admitted fixed edge");
            let actual = point_edge_minimum(&metric, point, start, end);
            let expected = dense_oracle(&metric, point, &edge);
            let gap = 4.0e-11 * metric.geodesic.a;
            assert!(
                actual.distance >= expected - 1.0e-9,
                "{name}: attained {} below oracle {expected}",
                actual.distance
            );
            assert!(
                actual.distance <= expected + gap,
                "{name}: attained {} exceeds oracle {expected} by more than {gap}",
                actual.distance
            );
            assert!(
                parameter_range.contains(&actual.parameter),
                "{name}: t={}",
                actual.parameter
            );
            assert!(
                !actual.budget_exhausted,
                "{name}: exhausted the 128-evaluation budget"
            );
        }
    }

    #[test]
    fn near_antipodal_short_edge_uses_the_lipschitz_fallback() {
        let frame = GeographicFrame::crs84();
        let metric = metric(&Geodesic::wgs84(), frame);
        let point = Point::new_raw_xy(179.999, 0.001);
        let start = Point::new_raw_xy(0.0, 0.0);
        let end = Point::new_raw_xy(1.0e-10, 0.0);
        let edge = metric.chart_edge(start, end).unwrap();
        let minimum = point_edge_minimum(&metric, point, start, end);
        assert!(curvature_hessian_bound(&metric.geodesic, minimum.distance).is_infinite());
        let oracle = dense_oracle(&metric, point, &edge);
        assert!((minimum.distance - oracle).abs() <= 4.0e-11 * metric.geodesic.a);
        assert!(!minimum.budget_exhausted);
        assert!(minimum.evaluations <= 128);
    }

    #[test]
    fn point_edge_cutoff_is_strict_and_threshold_is_inclusive() {
        let frame = GeographicFrame::crs84();
        let metric = metric(&Geodesic::wgs84(), frame);
        let point = Point::new_raw_xy(1.0, 0.0);
        let target = Point::new_raw_xy(0.0, 0.0);
        let edge = metric.chart_edge(target, target).unwrap();
        let minimum = point_edge_minimum(&metric, point, target, target);
        assert!((minimum.distance - 111_319.490_793_273_57).abs() <= 1.0e-9);
        let distance = minimum.distance;
        for (name, cutoff, expected_attainment) in [
            ("below", distance.next_down(), false),
            ("equal", distance, false),
            ("above", distance.next_up(), true),
        ] {
            let result = point_edge_query(
                &metric.geodesic,
                frame,
                point,
                &edge,
                0.0,
                PointEdgeQuery::Minimum { cutoff },
                GeodesicNumericalPolicy::new(metric.geodesic.a),
                &mut PointEdgeScratch::default(),
            )
            .unwrap();
            match result {
                PointEdgeQueryResult::Minimum(actual) => {
                    assert!(expected_attainment, "{name}");
                    assert_eq!(actual.distance, distance);
                },
                PointEdgeQueryResult::Excluded => assert!(!expected_attainment, "{name}"),
                other @ (PointEdgeQueryResult::Within { .. }
                | PointEdgeQueryResult::Outside { .. }) => {
                    panic!("{name}: unexpected cutoff result {other:?}")
                },
            }
        }
        for (name, limit, expected_within) in [
            ("negative infinity", f64::NEG_INFINITY, false),
            ("negative", -1.0, false),
            ("below", distance.next_down(), false),
            ("equal", distance, true),
            ("above", distance.next_up(), true),
            ("positive infinity", f64::INFINITY, true),
        ] {
            let result = point_edge_query(
                &metric.geodesic,
                frame,
                point,
                &edge,
                0.0,
                PointEdgeQuery::Dwithin { limit },
                GeodesicNumericalPolicy::new(metric.geodesic.a),
                &mut PointEdgeScratch::default(),
            )
            .unwrap();
            assert_eq!(
                matches!(result, PointEdgeQueryResult::Within { .. }),
                expected_within,
                "{name}"
            );
        }
        let error = point_edge_query(
            &metric.geodesic,
            frame,
            point,
            &edge,
            0.0,
            PointEdgeQuery::Dwithin { limit: f64::NAN },
            GeodesicNumericalPolicy::new(metric.geodesic.a),
            &mut PointEdgeScratch::default(),
        )
        .expect_err("NaN is not an admitted threshold");
        assert!(
            error
                .to_string()
                .contains("geodesic dwithin limit is not finite")
        );
    }

    #[test]
    fn point_edge_pole_plateaus_close_within_the_budget() {
        let frame = GeographicFrame::crs84();
        let metric = metric(&Geodesic::wgs84(), frame);
        for (name, point, start, end) in [
            (
                "exact north pole",
                Point::new_raw_xy(0.0, 90.0),
                Point::new_raw_xy(-45.0, 60.0),
                Point::new_raw_xy(45.0, 60.0),
            ),
            (
                "near north pole plateau",
                Point::new_raw_xy(0.0, 89.999_999_999),
                Point::new_raw_xy(-45.0, 80.0),
                Point::new_raw_xy(45.0, 80.0),
            ),
            (
                "north pole plateau transition",
                Point::new_raw_xy(0.0, 89.9),
                Point::new_raw_xy(-45.0, 89.0),
                Point::new_raw_xy(45.0, 89.0),
            ),
            (
                "near south pole plateau",
                Point::new_raw_xy(0.0, -89.999_999),
                Point::new_raw_xy(-45.0, -70.0),
                Point::new_raw_xy(45.0, -70.0),
            ),
        ] {
            let edge = metric.chart_edge(start, end).unwrap();
            let minimum = point_edge_minimum(&metric, point, start, end);
            let oracle = dense_oracle(&metric, point, &edge);
            assert!(
                (minimum.distance - oracle).abs() <= 1.0e-4,
                "{name}: {} vs {oracle}",
                minimum.distance
            );
            assert!(minimum.evaluations <= 128, "{name}");
        }
    }

    #[test]
    fn chart_parameter_trial_closes_the_near_edge_station_counterexample() {
        let frame = GeographicFrame::crs84();
        let metric = metric(&Geodesic::wgs84(), frame);
        let start = Point::new_raw_xy(0.0, 10.0);
        let end = Point::new_raw_xy(120.0, 60.0);
        // Independent 60-digit mpmath arclength inversion of the WGS 84 chart
        // edge gives station 6,004,860.750429569 m at this represented query.
        let point = Point::new_raw_xy(53.074_630_350_026_74, 32.114_429_312_511_14);
        let target_station = 6_004_860.750_429_569;

        let edge = metric.chart_edge(start, end).expect("admitted chart edge");
        let PointEdgeQueryResult::Minimum(minimum) = metric
            .point_edge_query(
                point,
                &edge,
                0.0,
                PointEdgeQuery::Locate {
                    cutoff: f64::INFINITY,
                },
                &mut PointEdgeScratch::default(),
            )
            .expect("station minimum")
        else {
            panic!("unbounded station query must return a minimum");
        };
        let with_station = metric
            .chart_edge_length_to(&edge, minimum.parameter)
            .expect("trial station");

        // The precomputed allowance is 30.704 ns: two 4-epsilon length
        // inversions, a four-ULP physical coordinate envelope, four ULPs of
        // parameter propagated by the sampled edge speed, and two station ULPs.
        assert!((with_station - target_station).abs() <= 30.71e-9);
        assert!(minimum.distance < 2.0e-9);
        assert!(minimum.evaluations <= 3);
        assert!(!minimum.budget_exhausted);
    }

    #[test]
    fn chart_witness_role_nearest_preserves_minimum_measurements() {
        let metric = metric(&Geodesic::wgs84(), GeographicFrame::crs84());
        for (name, start, end, point) in [
            ("interior", [-1.0, 1.0], [1.0, 1.0], [0.0, 0.0]),
            ("endpoint", [0.0, 1.0], [1.0, 2.0], [0.0, 0.0]),
            ("flat", [-1e-10, 1.0], [1e-10, 1.0], [0.0, 0.0]),
            ("seam", [175.0, 10.0], [-175.0, 10.0], [180.0, 20.0]),
            ("polar", [-45.0, 80.0], [45.0, 80.0], [0.0, 89.999_999_999]),
            ("pole", [-45.0, 60.0], [45.0, 60.0], [0.0, 90.0]),
            ("degenerate", [0.0, 1.0], [0.0, 1.0], [0.0, 0.0]),
            ("contact", [-1.0, 0.0], [1.0, 0.0], [0.0, 0.0]),
        ] {
            let point = Point::new_raw_xy(point[0], point[1]);
            let edge = metric
                .chart_edge(
                    Point::new_raw_xy(start[0], start[1]),
                    Point::new_raw_xy(end[0], end[1]),
                )
                .unwrap();
            let query = |query| {
                metric
                    .point_edge_query(point, &edge, 0.0, query, &mut PointEdgeScratch::default())
                    .unwrap()
            };
            let PointEdgeQueryResult::Minimum(reference) = query(PointEdgeQuery::Minimum {
                cutoff: f64::INFINITY,
            }) else {
                panic!("{name}: unbounded minimum");
            };
            for cutoff in [f64::INFINITY, reference.distance.next_up()] {
                let PointEdgeQueryResult::Minimum(minimum) =
                    query(PointEdgeQuery::Minimum { cutoff })
                else {
                    panic!("{name}: admitted minimum");
                };
                let PointEdgeQueryResult::Minimum(nearest) =
                    query(PointEdgeQuery::Nearest { cutoff })
                else {
                    panic!("{name}: admitted nearest");
                };
                assert_eq!(
                    minimum.distance.to_bits(),
                    nearest.distance.to_bits(),
                    "{name}"
                );
                assert_eq!(
                    minimum.lower_bound.to_bits(),
                    nearest.lower_bound.to_bits(),
                    "{name}"
                );
                assert_eq!(minimum.evaluations, nearest.evaluations, "{name}");
                assert_eq!(minimum.budget_exhausted, nearest.budget_exhausted, "{name}");
                if name == "flat" {
                    assert_eq!(minimum.parameter, 0.0);
                    assert_eq!(
                        edge.witness_role(nearest.parameter),
                        crate::geometry::WitnessRole::Point
                    );
                    let PointEdgeQueryResult::Minimum(locate) =
                        query(PointEdgeQuery::Locate { cutoff })
                    else {
                        panic!("flat: admitted locate");
                    };
                    assert_eq!(locate.distance.to_bits(), minimum.distance.to_bits());
                    assert_eq!(locate.parameter, 0.5);
                    assert_eq!(locate.evaluations, 3);
                }
            }
        }
    }

    #[test]
    fn chart_edge_minimum_matches_independent_ellipsoid_quadrature() {
        let frame = GeographicFrame::crs84();
        let metric = metric(&Geodesic::wgs84(), frame);
        let start = Point::new_raw_xy(-70.0, 30.3);
        let end = Point::new_raw_xy(-68.0, 31.3);
        let point = Point::new_raw_xy(-68.0, 31.0);
        // Independent 70-digit auxiliary-sphere geodesic quadrature and
        // stationary-point shooting: no chart-edge search or length code.
        let expected_distance = 28_757.993_325_000_867;
        let minimum = point_edge_minimum(&metric, point, start, end);
        // Four-epsilon ellipsoid-scale arithmetic and four coordinate ULPs
        // propagated through WGS 84's local longitude/latitude scales.
        assert!(
            (minimum.distance - expected_distance).abs() <= 13.0e-9,
            "distance {} differs from the independent minimum",
            minimum.distance
        );
    }

    #[test]
    fn chart_box_lower_bound_is_sound_for_seams_and_poles() {
        let frame = GeographicFrame::crs84();
        let metric = metric(&Geodesic::wgs84(), frame);
        let cases = [
            (
                Point::new_raw_xy(-180.0, 0.0),
                Point::new_raw_xy(179.0, 10.0),
                Point::new_raw_xy(-179.0, 20.0),
            ),
            (
                Point::new_raw_xy(30.0, 89.9),
                Point::new_raw_xy(-45.0, 80.0),
                Point::new_raw_xy(45.0, 80.0),
            ),
            (
                Point::new_raw_xy(100.0, -89.8),
                Point::new_raw_xy(170.0, 88.0),
                Point::new_raw_xy(-100.0, 87.0),
            ),
            (
                Point::new_raw_xy(0.0, 90.0),
                Point::new_raw_xy(-120.0, 89.0),
                Point::new_raw_xy(120.0, 89.5),
            ),
        ];
        for (point, start, end) in cases {
            let edge = metric.chart_edge(start, end).expect("admitted chart edge");
            let bounds = edge.bounds();
            let lower = metric.point_envelope_bound(point)(bounds);
            for sample in 0..=256_u32 {
                let parameter = f64::from(sample) / 256.0;
                let distance = oracle_distance(&metric, point, &edge, parameter);
                assert!(
                    lower <= distance + 1.0e-8,
                    "bounds={bounds:?}, point={point:?}, t={parameter}: {lower} > {distance}"
                );
            }
        }
    }

    #[test]
    fn point_edge_exact_degenerate_pole_sphere_and_prolate_families() {
        let frame = GeographicFrame::crs84();
        let wgs84 = metric(&Geodesic::wgs84(), frame);
        let point = Point::new_raw_xy(0.0, 30.0);
        let degenerate = point_edge_minimum(&wgs84, Point::new_raw_xy(2.0, 31.0), point, point);
        assert_eq!(degenerate.evaluations, 1);
        assert_eq!(degenerate.parameter, 0.0);

        let parallel_start = Point::new_raw_xy(-45.0, 80.0);
        let parallel_end = Point::new_raw_xy(45.0, 80.0);
        let pole = Point::new_raw_xy(0.0, 90.0);
        let pole_minimum = point_edge_minimum(&wgs84, pole, parallel_start, parallel_end);
        assert_eq!(pole_minimum.evaluations, 1);
        assert!(
            (pole_minimum.distance
                - oracle_distance(
                    &wgs84,
                    pole,
                    &wgs84.chart_edge(parallel_start, parallel_end).unwrap(),
                    0.0,
                ))
            .abs()
                <= 1.0e-9
        );

        for (name, flattening, point, start, end) in [
            (
                "sphere long oblique edge",
                0.0,
                Point::new_raw_xy(30.0, 22.0),
                Point::new_raw_xy(-70.0, 10.0),
                Point::new_raw_xy(80.0, 55.0),
            ),
            (
                "sphere polar seam",
                0.0,
                Point::new_raw_xy(-179.0, 89.0),
                Point::new_raw_xy(170.0, 88.0),
                Point::new_raw_xy(-100.0, 87.0),
            ),
            (
                "prolate long oblique edge",
                -0.01,
                Point::new_raw_xy(30.0, 22.0),
                Point::new_raw_xy(-70.0, 10.0),
                Point::new_raw_xy(80.0, 55.0),
            ),
            (
                "prolate polar seam",
                -0.01,
                Point::new_raw_xy(-179.0, 89.0),
                Point::new_raw_xy(170.0, 88.0),
                Point::new_raw_xy(-100.0, 87.0),
            ),
        ] {
            let metric = metric(&Geodesic::new(6_371_000.0, flattening), frame);
            let edge = metric.chart_edge(start, end).unwrap();
            let minimum = point_edge_minimum(&metric, point, start, end);
            let oracle = dense_oracle(&metric, point, &edge);
            let gap = 4.0e-11 * metric.geodesic.a;
            assert!(
                (minimum.distance - oracle).abs() <= gap,
                "{name}: attained={} at t={} oracle={oracle} gap={gap}",
                minimum.distance,
                minimum.parameter
            );
        }
    }
}
