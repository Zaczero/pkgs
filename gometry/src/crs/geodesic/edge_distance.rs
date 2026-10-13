//! Point-to-chart-edge geodesic minimum and threshold queries.

use std::cmp::Ordering;
use std::collections::BinaryHeap;

use geographiclib_rs::Geodesic;

use super::chart_edge::ChartEdge;
use super::ellipsoid_metric::inverse_distance_azimuths;
use crate::error::Result;
use crate::geographic::GeographicFrame;
use crate::geometry::interval::{Interval as CertifiedInterval, PI_LOWER, PI_UPPER};
use crate::geometry::{
    ChartEdgeDifferentialBounds, GeodesicNumericalPolicy, Point, PointEdgeMinimum, PointEdgeQuery,
    PointEdgeQueryResult,
};

const MAX_POINT_EDGE_MINIMUM_EVALUATIONS: usize = 128;
pub(crate) const EVALUATION_SAFETY_CEILING: usize = 0x4000;
// Midpoint depth supplies a threshold work scale; safeguarded secant steps
// and previously resolved branches can exhaust the allowance before parameter
// resolution.
const BINARY64_MIDPOINT_DEPTH_SCALE: usize =
    f64::MIN_EXP.unsigned_abs() as usize + f64::MANTISSA_DIGITS as usize + 1;

#[derive(Default)]
pub(crate) struct PointEdgeScratch {
    frontier: BinaryHeap<EdgeInterval>,
    sequence: u64,
}

#[cfg(test)]
pub(crate) mod test_counters {
    use std::cell::Cell;

    thread_local! {
        static SAMPLES: Cell<u64> = const { Cell::new(0) };
        static SAMPLE_LIMIT: Cell<Option<u64>> = const { Cell::new(None) };
    }

    pub(crate) fn reset() {
        SAMPLES.set(0);
        SAMPLE_LIMIT.set(None);
    }

    pub(crate) fn set_sample_limit(limit: u64) {
        SAMPLE_LIMIT.set(Some(limit));
    }

    pub(crate) fn sampled() -> bool {
        let samples = SAMPLES.get().saturating_add(1);
        SAMPLES.set(samples);
        SAMPLE_LIMIT.get().is_none_or(|limit| samples <= limit)
    }

    pub(crate) fn samples() -> u64 {
        SAMPLES.get()
    }
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
    let initial = IntervalEndpoints {
        t0: 0.0,
        d0,
        g0,
        t1: 1.0,
        d1,
        g1,
    };
    if threshold.is_some()
        && let Some((distance, parameter)) =
            monotone_endpoint_minimum(initial, derivative_bounds, geodesic, policy)
    {
        return Ok(direct_result(
            distance,
            parameter,
            best.evaluations,
            cutoff,
            threshold,
        ));
    }
    let mut best_correction = 0.0;
    let mut resolved_lower_bound = f64::INFINITY;
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
        push_interval(scratch, initial, derivative_bounds, geodesic, policy);
    }

    loop {
        let Some(interval) = scratch.frontier.pop() else {
            best.lower_bound = resolved_lower_bound.min(best.distance);
            break;
        };
        if let Some(limit) = threshold
            && interval.lower_bound > limit
        {
            best.lower_bound = interval
                .lower_bound
                .min(best.distance)
                .min(resolved_lower_bound);
            return Ok(PointEdgeQueryResult::Outside {
                lower_bound: best.lower_bound,
                evaluations: best.evaluations,
            });
        }
        if let Some(limit) = threshold
            && let Some((distance, _parameter)) = monotone_endpoint_minimum(
                IntervalEndpoints {
                    t0: interval.t0,
                    d0: interval.d0,
                    g0: interval.g0,
                    t1: interval.t1,
                    d1: interval.d1,
                    g1: interval.g1,
                },
                derivative_bounds,
                geodesic,
                policy,
            )
        {
            if distance <= limit {
                return Ok(PointEdgeQueryResult::Within {
                    evaluations: best.evaluations,
                });
            }
            resolved_lower_bound = resolved_lower_bound.min(distance);
            continue;
        }
        if threshold.is_none() && interval.lower_bound >= best.distance - policy.minimum_gap() {
            best.lower_bound = interval.lower_bound.min(best.distance);
            break;
        }
        let evaluation_limit = if threshold.is_some() {
            evaluation_limit(scratch.frontier.len().saturating_add(1))
        } else {
            MAX_POINT_EDGE_MINIMUM_EVALUATIONS
        };
        if best.evaluations >= evaluation_limit {
            best.lower_bound = interval
                .lower_bound
                .min(best.distance)
                .min(resolved_lower_bound);
            best.budget_exhausted = true;
            if threshold.is_some() {
                return Ok(PointEdgeQueryResult::Unresolved {
                    lower_bound: best.lower_bound,
                    evaluations: best.evaluations,
                });
            }
            break;
        }

        let width = interval.t1 - interval.t0;
        let parameter = if interval.g0 < 0.0 && interval.g1 > 0.0 {
            interval.t0 + width * (-interval.g0 / (interval.g1 - interval.g0)).clamp(0.1, 0.9)
        } else {
            f64::midpoint(interval.t0, interval.t1)
        };
        if !(parameter > interval.t0 && parameter < interval.t1) {
            best.lower_bound = interval
                .lower_bound
                .min(best.distance)
                .min(resolved_lower_bound);
            if threshold.is_some() {
                return Ok(PointEdgeQueryResult::Unresolved {
                    lower_bound: best.lower_bound,
                    evaluations: best.evaluations,
                });
            }
            best.budget_exhausted = true;
            break;
        }
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
        && best.evaluations < MAX_POINT_EDGE_MINIMUM_EVALUATIONS
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
    let lower_bound = best.lower_bound.min(best.distance);
    if let Some(limit) = threshold {
        if best.distance <= limit {
            return PointEdgeQueryResult::Within {
                evaluations: best.evaluations,
            };
        }
        if lower_bound > limit {
            return PointEdgeQueryResult::Outside {
                lower_bound,
                evaluations: best.evaluations,
            };
        }
        return PointEdgeQueryResult::Unresolved {
            lower_bound,
            evaluations: best.evaluations,
        };
    }
    if lower_bound >= cutoff {
        PointEdgeQueryResult::Excluded
    } else {
        PointEdgeQueryResult::Minimum(PointEdgeMinimum {
            lower_bound,
            ..best
        })
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
    #[cfg(test)]
    if !test_counters::sampled() {
        return Err(crate::crs::CrsError::message(
            "point-edge test work limit exceeded",
        ));
    }
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
    let width = CertifiedInterval::exact(interval.t1).sub(CertifiedInterval::exact(interval.t0));
    let speed_span = CertifiedInterval::exact(derivative_bounds.speed).mul(width);
    let lipschitz_interval = CertifiedInterval::exact(interval.d0)
        .add(CertifiedInterval::exact(interval.d1))
        .sub(speed_span)
        .div(CertifiedInterval::exact(2.0));
    let lipschitz = lipschitz_interval.lower().max(0.0);
    let hessian_allowance = policy.evaluation_allowance(4.0).next_up();
    let distance_lower = lipschitz_interval
        .sub(CertifiedInterval::exact(hessian_allowance))
        .lower()
        .max(0.0);
    let distance_upper = CertifiedInterval::exact(interval.d0.max(interval.d1))
        .add(speed_span)
        .add(CertifiedInterval::exact(hessian_allowance))
        .upper();
    let distance_hessian = curvature_hessian_bound(geodesic, distance_lower, distance_upper);
    let second_derivative = if distance_hessian.is_finite() {
        CertifiedInterval::exact(derivative_bounds.speed)
            .mul(CertifiedInterval::exact(derivative_bounds.speed))
            .mul(CertifiedInterval::exact(distance_hessian))
            .add(CertifiedInterval::exact(
                derivative_bounds.covariant_acceleration,
            ))
            .upper()
    } else {
        f64::INFINITY
    };
    let mut lower = lipschitz;
    if second_derivative.is_finite() && interval.g0.is_finite() && interval.g1.is_finite() {
        let relative_slope_allowance = CertifiedInterval::exact(4.0).mul(CertifiedInterval::exact(
            GeodesicNumericalPolicy::BACKEND_RELATIVE,
        ));
        let slope_allowance = CertifiedInterval::exact(derivative_bounds.speed)
            .mul(relative_slope_allowance)
            .upper();
        let g0 =
            CertifiedInterval::exact(interval.g0).sub(CertifiedInterval::exact(slope_allowance));
        let g1 =
            CertifiedInterval::exact(interval.g1).sub(CertifiedInterval::exact(slope_allowance));
        let endpoint_allowance = policy.backend_floor().next_up();
        let d0 = CertifiedInterval::exact(interval.d0)
            .sub(CertifiedInterval::exact(endpoint_allowance))
            .lower();
        let d1 = CertifiedInterval::exact(interval.d1)
            .sub(CertifiedInterval::exact(endpoint_allowance))
            .lower();
        let b = CertifiedInterval::exact(second_derivative);
        let zero = CertifiedInterval::exact(0.0);
        let half = CertifiedInterval::exact(2.0);

        let left_start = lower_parabola(d0, g0, b, zero);
        let right_start = lower_parabola(d1, g1, b, width.neg());
        let start = left_start.max(right_start).lower();
        let left_end = lower_parabola(d0, g0, b, width);
        let right_end = lower_parabola(d1, g1, b, zero);
        let end = left_end.max(right_end).lower();
        let mut parabola = start.min(end);

        // Both supporting parabolas share their quadratic term, so their
        // crossing is linear in the offset from the interval start.
        let slope = g0.sub(g1).sub(b.mul(width));
        let intercept = CertifiedInterval::exact(d0)
            .sub(CertifiedInterval::exact(d1))
            .add(g1.mul(width))
            .add(b.mul(width.mul(width)).div(half));
        if !slope.contains_zero() {
            let crossing = intercept.neg().div(slope);
            let domain = CertifiedInterval::from_bounds(0.0, width.upper());
            if crossing.intersects(domain) {
                let crossing = CertifiedInterval::from_bounds(
                    crossing.lower().max(0.0),
                    crossing.upper().min(width.upper()),
                );
                let left_at_crossing = lower_parabola(d0, g0, b, crossing);
                let right_offset = crossing.sub(width);
                let right_at_crossing = lower_parabola(d1, g1, b, right_offset);
                parabola = parabola.min(left_at_crossing.max(right_at_crossing).lower());
            }
        }
        lower = lower.max(parabola);
    }
    CertifiedInterval::exact(lower)
        .sub(CertifiedInterval::exact(policy.backend_floor().next_up()))
        .lower()
        .max(0.0)
}

fn evaluation_limit(live_branches: usize) -> usize {
    BINARY64_MIDPOINT_DEPTH_SCALE
        .saturating_mul(live_branches.max(1))
        .min(EVALUATION_SAFETY_CEILING)
}

fn monotone_endpoint_minimum(
    interval: IntervalEndpoints,
    derivative_bounds: ChartEdgeDifferentialBounds,
    geodesic: &Geodesic,
    policy: GeodesicNumericalPolicy,
) -> Option<(f64, f64)> {
    if !(interval.g0.is_finite()
        && interval.g1.is_finite()
        && interval.d0.is_finite()
        && interval.d1.is_finite()
        && derivative_bounds.speed.is_finite()
        && derivative_bounds.covariant_acceleration.is_finite())
    {
        return None;
    }
    let width = CertifiedInterval::exact(interval.t1).sub(CertifiedInterval::exact(interval.t0));
    let distance_upper = CertifiedInterval::exact(interval.d0.max(interval.d1))
        .add(CertifiedInterval::exact(derivative_bounds.speed).mul(width))
        .add(CertifiedInterval::exact(policy.evaluation_allowance(4.0)))
        .upper();
    if distance_upper >= normal_radius_lower_bound(geodesic)? {
        return None;
    }

    // In this normal ball, Hessian(distance) is nonnegative. The chart curve's
    // covariant acceleration can therefore change its distance derivative by
    // at most acceleration × parameter width, without a 1/distance bound.
    let acceleration_span =
        CertifiedInterval::exact(derivative_bounds.covariant_acceleration).mul(width);
    let slope_allowance = CertifiedInterval::exact(derivative_bounds.speed)
        .mul(CertifiedInterval::exact(
            4.0 * GeodesicNumericalPolicy::BACKEND_RELATIVE,
        ))
        .upper();
    let allowance = CertifiedInterval::exact(slope_allowance);
    let start_slope = CertifiedInterval::exact(interval.g0)
        .sub(acceleration_span)
        .sub(allowance);
    if start_slope.lower() > 0.0 {
        return Some((interval.d0, interval.t0));
    }
    let end_slope = CertifiedInterval::exact(interval.g1)
        .add(acceleration_span)
        .add(allowance);
    if end_slope.upper() < 0.0 {
        return Some((interval.d1, interval.t1));
    }
    None
}

fn lower_parabola(
    distance: f64,
    slope: CertifiedInterval,
    second_derivative: CertifiedInterval,
    offset: CertifiedInterval,
) -> CertifiedInterval {
    CertifiedInterval::exact(distance)
        .add(slope.mul(offset))
        .sub(
            second_derivative
                .mul(offset.mul(offset))
                .div(CertifiedInterval::exact(2.0)),
        )
}

/// Bound the distance Hessian only inside a certified normal ball.
///
/// For an ellipsoid with semiaxes `a` and `b`, its principal curvatures are
/// bounded by `κ = max(a/b², b/a²)`. Rauch comparison places the first
/// conjugate point at least `π/κ` away. Klingenberg's alternative is half the
/// length of a geodesic loop; Fenchel's total-curvature theorem, allowing a
/// `π` corner at the loop base, bounds that loop length below by `π/κ`.
/// Therefore the injectivity radius is at least `π/(2κ)`. Before that radius,
/// Hessian comparison with Euclidean space and the nonnegative Gaussian
/// curvature gives `0 <= Hess(distance) <= 1/r`.
///
/// `distance_lower` and `distance_upper` must already enclose every distance
/// in the interval, including the backend allowance.
pub(super) fn curvature_hessian_bound(
    geodesic: &Geodesic,
    distance_lower: f64,
    distance_upper: f64,
) -> f64 {
    if !(distance_lower.is_finite() && distance_lower > 0.0 && distance_upper.is_finite())
        || normal_radius_lower_bound(geodesic).is_none_or(|radius| distance_upper >= radius)
    {
        return f64::INFINITY;
    }
    CertifiedInterval::exact(1.0)
        .div(CertifiedInterval::exact(distance_lower))
        .upper()
}

fn normal_radius_lower_bound(geodesic: &Geodesic) -> Option<f64> {
    let one = CertifiedInterval::exact(1.0);
    let flattening = CertifiedInterval::exact(geodesic.f);
    let c = one.sub(flattening.mul(CertifiedInterval::exact(2.0).sub(flattening)));
    if c.lower() <= 0.0 {
        return None;
    }
    let semi_major = CertifiedInterval::exact(geodesic.a);
    let d_max = 1.0_f64.max(c.upper());
    let d_max_root = CertifiedInterval::exact(d_max).sqrt();
    let minimum_meridian_radius = semi_major
        .mul(CertifiedInterval::exact(c.lower()))
        .div(CertifiedInterval::exact(d_max).mul(d_max_root))
        .lower();
    let meridian_curvature =
        CertifiedInterval::exact(1.0).div(CertifiedInterval::exact(minimum_meridian_radius));
    let semi_minor = semi_major.mul(c.sqrt());
    let semi_major_squared = semi_major.mul(semi_major);
    let semi_minor_squared = semi_minor.mul(semi_minor);
    let equatorial = semi_major.div(semi_minor_squared);
    let polar = semi_minor.div(semi_major_squared);
    // Mmin supplies the same meridional principal-curvature extremum as one of
    // the semiaxis terms, with its own outward scale enclosure.
    let maximum_principal_curvature = equatorial.max(polar).max(meridian_curvature);
    let injectivity_radius_lower = CertifiedInterval::from_bounds(PI_LOWER, PI_UPPER)
        .div(CertifiedInterval::exact(2.0).mul(maximum_principal_curvature))
        .lower();
    (injectivity_radius_lower.is_finite() && injectivity_radius_lower > 0.0)
        .then_some(injectivity_radius_lower)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use geographiclib_rs::InverseGeodesic as _;

    use super::*;
    use crate::crs::GeodesicMetric as EllipsoidChartMetric;
    use crate::crs::geodesic::AuthalicSeries;
    use crate::geometry::{ChartMetric as _, GeodesicMetric as _};

    mod bounds;
    mod budget;
    mod minima;
    mod support;
    mod threshold;

    use support::*;
}
