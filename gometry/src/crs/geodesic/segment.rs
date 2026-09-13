#![allow(
    clippy::similar_names,
    reason = "latitude, longitude and azimuth triples are named by axis and endpoint"
)]
use geographiclib_rs::{Geodesic, GeodesicLine, InverseGeodesic as _, geodesic_capability as caps};

use crate::crs::CrsError;
#[cfg(test)]
use crate::crs::geodesic::geodesic_counters;
use crate::crs::geodesic::{
    GEODESIC_BACKEND_RELATIVE_FLOOR, GEODESIC_LINE_CACHE, GEODESIC_LINE_CACHE_CAPACITY,
    GEODESIC_LINE_CAPS, GEODESIC_LINE_OUTMASK, GeodesicLineKey, inverse_distance,
};
use crate::error::Result;
use crate::geometry::{
    GeodesicInnerInterval, GeodesicSegment, GeodesicSegmentMinimum, GeodesicSegmentQuery,
    GeodesicSegmentQueryResult, GeodesicSegmentScratch, GeodesicThresholdCompletion,
    GeodesicWitnessNeed, MOrdinate, Point, ZOrdinate,
};

const POINT_SEGMENT_BOUND_ROUNDING_ULPS: f64 = 128.0;
const POINT_SEGMENT_DERIVATIVE_ROUNDING_ULPS: f64 = 256.0;
// Endpoint lower bounds and the attained sample each carry the backend floor;
// keep two further terms for the conservative bound/square-root path.
const POINT_SEGMENT_BACKEND_SLACK_MULTIPLIER: f64 = 4.0;
const POINT_SEGMENT_PROBE_INTERVALS: usize = 8;
const POINT_SEGMENT_LINE_CAPS: u64 = caps::LATITUDE
    | caps::LONGITUDE
    | caps::AZIMUTH
    | caps::REDUCEDLENGTH
    | caps::GEODESICSCALE
    | caps::DISTANCE_IN;
const INTERSECT_LINE_OUTMASK: u64 =
    caps::LATITUDE | caps::LONGITUDE | caps::AZIMUTH | caps::REDUCEDLENGTH | caps::GEODESICSCALE;

/// Construct a segment only when the selected shortest geodesic is unambiguous.
pub(crate) fn checked_geodesic_segment(
    geodesic: &Geodesic,
    start: Point,
    end: Point,
) -> Result<GeodesicSegment> {
    crate::crs::ensure_geographic_lonlat(start.x, start.y)?;
    crate::crs::ensure_geographic_lonlat(end.x, end.y)?;
    let (length, azimuth0, azimuth1, _m12, _m12_scale, _m21, _a12): (
        f64,
        f64,
        f64,
        f64,
        f64,
        f64,
        f64,
    ) = geodesic.inverse(start.y, start.x, end.y, end.x);
    ensure_unique_shortest(geodesic, start, end, length, azimuth0, azimuth1)?;
    Ok(GeodesicSegment {
        start,
        end,
        length,
        azimuth0,
    })
}

#[expect(
    clippy::float_cmp,
    reason = "a pole or antimeridian is one specific representable coordinate, and the question is whether a vertex IS that coordinate; a tolerance would admit vertices merely near it"
)]
fn ensure_unique_shortest(
    geodesic: &Geodesic,
    start: Point,
    end: Point,
    length: f64,
    azimuth0: f64,
    azimuth1: f64,
) -> Result<()> {
    let coordinate_tolerance = 32.0 * f64::EPSILON;
    if length == 0.0 {
        return Err(CrsError::message(
            "geodesic segment endpoints coincide, so the shortest geodesic is nonunique",
        ));
    }
    let opposite_poles = start.y.abs() == 90.0 && end.y.abs() == 90.0 && start.y != end.y;
    let delta_lon = longitude_difference(start.x, end.x);
    let antipodal = (delta_lon.abs() - 180.0).abs() <= coordinate_tolerance
        && (start.y + end.y).abs() <= coordinate_tolerance;
    if !(length.is_finite() && azimuth0.is_finite() && azimuth1.is_finite()) {
        return Err(CrsError::message(
            "geodesic segment construction produced a non-finite inverse",
        ));
    }
    let ambiguous = opposite_poles
        || (geodesic.f == 0.0 && antipodal)
        || (geodesic.f > 0.0
            && (start.y + end.y).abs() <= coordinate_tolerance
            && start.y.abs() < 90.0
            && end.y.abs() < 90.0
            && angle_difference_radians(azimuth0, azimuth1).abs() > coordinate_tolerance)
        || (geodesic.f < 0.0
            && (delta_lon.abs() - 180.0).abs() <= coordinate_tolerance
            && azimuth0.to_radians().sin().abs() > coordinate_tolerance);
    if ambiguous {
        return Err(CrsError::message(
            "geodesic segment endpoints have multiple shortest geodesics",
        ));
    }
    Ok(())
}

fn longitude_difference(start: f64, end: f64) -> f64 {
    let mut difference = (end - start) % 360.0;
    if difference > 180.0 {
        difference -= 360.0;
    } else if difference < -180.0 {
        difference += 360.0;
    }
    difference
}

#[expect(
    clippy::large_types_passed_by_value,
    reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
)]
pub(crate) fn geodesic_segment_intersection_with_solver(
    geodesic: &Geodesic,
    solver: &IntersectSolver,
    first: GeodesicSegment,
    second: GeodesicSegment,
) -> Result<Option<(Point, Point)>> {
    let line_x = cached_geodesic_line(geodesic, first);
    let line_y = cached_geodesic_line(geodesic, second);
    let mut mode = 0;
    let q = solver.segment(&line_x, first.length, &line_y, second.length, &mut mode)?;
    if mode != 0 {
        return Ok(None);
    }
    let first_point = materialize_segment_point(&line_x, first, q.x)?;
    let second_point = materialize_segment_point(&line_y, second, q.y)?;
    Ok(Some((first_point, second_point)))
}

/// Run the one point-to-segment owner for a value, threshold, or witness query.
///
/// The interval queue owns only parameter cells. It never returns a pruning
/// bound as a distance: an attained result carries its value and optional foot,
/// while exclusion and unresolved probe state have distinct variants.
#[expect(
    clippy::large_types_passed_by_value,
    reason = "the segment is an owned Copy snapshot consumed by the geodesic query"
)]
pub(crate) fn geodesic_point_segment_query(
    geodesic: &Geodesic,
    point: Point,
    segment: GeodesicSegment,
    query: GeodesicSegmentQuery,
    scratch: &mut GeodesicSegmentScratch,
) -> Result<GeodesicSegmentQueryResult> {
    #[cfg(test)]
    geodesic_counters::segment_query();
    if !segment.length.is_finite() || segment.length < 0.0 {
        return Err(CrsError::message("geodesic segment length is not finite"));
    }
    let prepared = PreparedSegment::new(geodesic, &segment);
    let (left_value, right_value) = prepared.endpoint_values(geodesic, point)?;
    let endpoint_minimum = prepared.endpoint_minimum(left_value, right_value);

    match query {
        GeodesicSegmentQuery::Minimum { cutoff, witness } => {
            if cutoff.is_nan() {
                return Err(CrsError::message(
                    "geodesic point-to-segment cutoff is not finite",
                ));
            }
            if let Some(along) = exact_segment_contact(geodesic, point, &prepared) {
                let minimum = GeodesicSegmentMinimum {
                    distance: 0.0,
                    along,
                };
                return if 0.0 < cutoff {
                    attained_result(&prepared, minimum, witness)
                } else {
                    Ok(GeodesicSegmentQueryResult::Excluded { lower_bound: 0.0 })
                };
            }
            if segment.length == 0.0 {
                return if endpoint_minimum.distance < cutoff {
                    attained_result(&prepared, endpoint_minimum, witness)
                } else {
                    Ok(GeodesicSegmentQueryResult::Excluded {
                        lower_bound: endpoint_minimum.distance,
                    })
                };
            }
            if let Some(minimum) =
                exact_family_minimum(geodesic, point, &prepared, endpoint_minimum)?
            {
                if minimum.distance < cutoff {
                    return attained_result(&prepared, minimum, witness);
                }
                return Ok(GeodesicSegmentQueryResult::Excluded {
                    lower_bound: minimum.distance,
                });
            }
            search_point_segment(
                &prepared,
                geodesic,
                point,
                query,
                endpoint_minimum,
                left_value,
                right_value,
                scratch,
            )
        },
        GeodesicSegmentQuery::Threshold {
            limit,
            completion,
            witness,
        } => {
            if limit.is_nan() {
                return Err(CrsError::message("geodesic dwithin limit is not finite"));
            }
            if limit < 0.0 || limit.is_infinite() && limit.is_sign_negative() {
                return Ok(GeodesicSegmentQueryResult::ThresholdOutside { lower_bound: 0.0 });
            }
            if let Some(along) = exact_segment_contact(geodesic, point, &prepared) {
                return attained_result(
                    &prepared,
                    GeodesicSegmentMinimum {
                        distance: 0.0,
                        along,
                    },
                    witness,
                );
            }
            if endpoint_minimum.distance <= limit {
                return attained_result(&prepared, endpoint_minimum, witness);
            }
            if limit.is_infinite() {
                return Err(CrsError::message(
                    "geodesic threshold query produced no finite endpoint",
                ));
            }
            if segment.length == 0.0 {
                return Ok(GeodesicSegmentQueryResult::ThresholdOutside {
                    lower_bound: endpoint_minimum.distance,
                });
            }
            if let Some(minimum) =
                exact_family_minimum(geodesic, point, &prepared, endpoint_minimum)?
            {
                return if minimum.distance <= limit {
                    attained_result(&prepared, minimum, witness)
                } else {
                    Ok(GeodesicSegmentQueryResult::ThresholdOutside {
                        lower_bound: minimum.distance,
                    })
                };
            }
            if matches!(completion, GeodesicThresholdCompletion::Probe) {
                return probe_point_segment(&prepared, geodesic, point, limit, witness);
            }
            search_point_segment(
                &prepared,
                geodesic,
                point,
                query,
                endpoint_minimum,
                left_value,
                right_value,
                scratch,
            )
        },
    }
}

#[derive(Clone, Copy)]
struct PreparedSegment {
    segment: GeodesicSegment,
    line: GeodesicLine,
}

#[derive(Clone, Copy)]
struct SegmentSample {
    distance: f64,
    derivative: Option<Derivative>,
}

#[derive(Clone, Copy)]
struct Derivative {
    value: f64,
    error: f64,
}

impl PreparedSegment {
    fn new(geodesic: &Geodesic, segment: &GeodesicSegment) -> Self {
        Self {
            segment: *segment,
            line: cached_point_segment_line(geodesic, *segment),
        }
    }

    fn endpoint_values(&self, geodesic: &Geodesic, point: Point) -> Result<(f64, f64)> {
        let left = inverse_distance(
            geodesic,
            point.x,
            point.y,
            self.segment.start.x,
            self.segment.start.y,
        );
        let right = if self.segment.length == 0.0 {
            left
        } else {
            inverse_distance(
                geodesic,
                point.x,
                point.y,
                self.segment.end.x,
                self.segment.end.y,
            )
        };
        ensure_finite_distance(left)?;
        ensure_finite_distance(right)?;
        Ok((left, right))
    }

    fn endpoint_minimum(&self, left: f64, right: f64) -> GeodesicSegmentMinimum {
        if left <= right {
            GeodesicSegmentMinimum {
                distance: left,
                along: 0.0,
            }
        } else {
            GeodesicSegmentMinimum {
                distance: right,
                along: self.segment.length,
            }
        }
    }

    fn sample(
        &self,
        geodesic: &Geodesic,
        point: Point,
        along: f64,
        derivative: bool,
    ) -> Result<SegmentSample> {
        #[cfg(test)]
        {
            geodesic_counters::segment_value_evaluation();
            if derivative {
                geodesic_counters::segment_derivative_evaluation();
            }
        }
        let (_, lat, lon, azimuth, ..) =
            self.line._gen_position(false, along, GEODESIC_LINE_OUTMASK);
        if !(lat.is_finite() && lon.is_finite() && (!derivative || azimuth.is_finite())) {
            return Err(CrsError::message(
                "geodesic line evaluation produced a non-finite position",
            ));
        }
        let (distance, query_azimuth, ..): (f64, f64, f64, f64) =
            geodesic.inverse(lat, lon, point.y, point.x);
        ensure_finite_distance(distance)?;
        let derivative = if derivative {
            let value = if distance > 0.0 && query_azimuth.is_finite() {
                let (angle, correction) = angle_difference(azimuth, query_azimuth);
                let slope = -(angle.to_radians() + correction.to_radians()).cos();
                let angular_error = 128.0 * f64::EPSILON
                    + backend_floor(geodesic) / distance.max(f64::MIN_POSITIVE);
                let error = distance
                    * (angular_error + POINT_SEGMENT_DERIVATIVE_ROUNDING_ULPS * f64::EPSILON);
                (angular_error < 0.25 && error.is_finite()).then_some(Derivative {
                    value: distance * slope,
                    error,
                })
            } else {
                None
            };
            if value.is_none() {
                #[cfg(test)]
                geodesic_counters::segment_derivative_decline();
            }
            value
        } else {
            None
        };
        Ok(SegmentSample {
            distance,
            derivative,
        })
    }

    #[expect(
        clippy::float_cmp,
        reason = "the segment endpoints are exact parameter identities; a tolerance would change the returned witness"
    )]
    fn witness(&self, minimum: GeodesicSegmentMinimum) -> Result<Point> {
        if !(minimum.distance.is_finite()
            && minimum.along.is_finite()
            && minimum.along >= 0.0
            && minimum.along <= self.segment.length)
        {
            return Err(CrsError::message("geodesic segment witness is not finite"));
        }
        let foot = if minimum.along == 0.0 {
            self.segment.start
        } else if minimum.along == self.segment.length {
            self.segment.end
        } else {
            let (_, lat, lon, ..) =
                self.line
                    ._gen_position(false, minimum.along, GEODESIC_LINE_OUTMASK);
            if !(lat.is_finite() && lon.is_finite()) {
                return Err(CrsError::message(
                    "geodesic segment witness position is not finite",
                ));
            }
            let fraction = minimum.along / self.segment.length;
            Point::new_axes(
                lon,
                lat,
                ZOrdinate(interpolate_optional_ordinate(
                    self.segment.start.z(),
                    self.segment.end.z(),
                    fraction,
                )),
                MOrdinate(interpolate_optional_ordinate(
                    self.segment.start.m(),
                    self.segment.end.m(),
                    fraction,
                )),
            )?
        };
        Ok(foot)
    }
}

#[expect(
    clippy::large_types_passed_by_value,
    reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
)]
pub(crate) fn cached_geodesic_line(geodesic: &Geodesic, segment: GeodesicSegment) -> GeodesicLine {
    cached_geodesic_line_with_caps(geodesic, segment, GEODESIC_LINE_CAPS)
}

#[expect(
    clippy::large_types_passed_by_value,
    reason = "the segment is a hot Copy snapshot used to key and build the cached line"
)]
fn cached_point_segment_line(geodesic: &Geodesic, segment: GeodesicSegment) -> GeodesicLine {
    cached_geodesic_line_with_caps(geodesic, segment, POINT_SEGMENT_LINE_CAPS)
}

#[expect(
    clippy::large_types_passed_by_value,
    reason = "the segment is a hot Copy snapshot used to key and build the cached line"
)]
fn cached_geodesic_line_with_caps(
    geodesic: &Geodesic,
    segment: GeodesicSegment,
    caps: u64,
) -> GeodesicLine {
    let key = GeodesicLineKey {
        semi_major_bits: geodesic.a.to_bits(),
        flattening_bits: geodesic.f.to_bits(),
        start_lon_bits: segment.start.x.to_bits(),
        start_lat_bits: segment.start.y.to_bits(),
        azimuth_bits: segment.azimuth0.to_bits(),
        caps,
    };
    GEODESIC_LINE_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        if let Some(line) = cache.get(&key) {
            count_line_cache_hit!();
            return *line;
        }
        count_line_cache_miss!();
        if cache.len() == GEODESIC_LINE_CACHE_CAPACITY
            && let Some(evict) = cache.keys().next().copied()
        {
            cache.remove(&evict);
        }
        let line = GeodesicLine::new(
            geodesic,
            segment.start.y,
            segment.start.x,
            segment.azimuth0,
            Some(caps),
            None,
            None,
        );
        cache.insert(key, line);
        line
    })
}

fn search_point_segment(
    prepared: &PreparedSegment,
    geodesic: &Geodesic,
    point: Point,
    query: GeodesicSegmentQuery,
    endpoint_minimum: GeodesicSegmentMinimum,
    left_value: f64,
    right_value: f64,
    scratch: &mut GeodesicSegmentScratch,
) -> Result<GeodesicSegmentQueryResult> {
    let initial = GeodesicInnerInterval {
        lower: interval_lower_bound(geodesic, left_value, right_value, prepared.segment.length),
        left: 0.0,
        right: prepared.segment.length,
        left_value,
        right_value,
    };

    match query {
        GeodesicSegmentQuery::Minimum { cutoff, .. } if initial.lower >= cutoff => {
            Ok(GeodesicSegmentQueryResult::Excluded {
                lower_bound: initial.lower,
            })
        },
        GeodesicSegmentQuery::Threshold { limit, .. } if initial.lower > limit => {
            Ok(GeodesicSegmentQueryResult::ThresholdOutside {
                lower_bound: initial.lower,
            })
        },
        _ => {
            scratch.clear();
            crate::boundary::try_push_heap(&mut scratch.inner, initial)?;
            let mut minimum = endpoint_minimum;
            let mut backend_limited = false;
            while let Some(interval) = scratch.inner.pop() {
                #[cfg(test)]
                geodesic_counters::segment_interval_pop();
                if interval_is_excluded(interval.lower, query, minimum.distance, geodesic) {
                    if matches!(query, GeodesicSegmentQuery::Minimum { .. }) {
                        break;
                    }
                    continue;
                }
                if let Some(closed) =
                    convex_cell_minimum(prepared, geodesic, point, interval, &mut minimum)?
                {
                    #[cfg(test)]
                    geodesic_counters::segment_convex_closure();
                    update_minimum(&mut minimum, closed.distance, closed.along);
                    if let GeodesicSegmentQuery::Threshold { limit, .. } = query
                        && minimum.distance <= limit
                    {
                        return attained_result(prepared, minimum, query_witness(query));
                    }
                    continue;
                }
                if matches!(query, GeodesicSegmentQuery::Minimum { .. })
                    && minimum.distance == 0.0
                    && query_accepts_attained(query, minimum.distance)
                {
                    return attained_result(prepared, minimum, query_witness(query));
                }
                if !can_subdivide(interval.left, interval.right) {
                    if matches!(query, GeodesicSegmentQuery::Threshold {
                        completion: GeodesicThresholdCompletion::Complete,
                        ..
                    }) {
                        backend_limited = true;
                    }
                    continue;
                }
                #[cfg(test)]
                geodesic_counters::segment_interval_split();
                let middle = f64::midpoint(interval.left, interval.right);
                let middle_value = prepared.sample(geodesic, point, middle, false)?;
                update_minimum(&mut minimum, middle_value.distance, middle);
                if let GeodesicSegmentQuery::Threshold { limit, .. } = query
                    && minimum.distance <= limit
                {
                    return attained_result(prepared, minimum, query_witness(query));
                }
                if matches!(query, GeodesicSegmentQuery::Minimum { .. })
                    && minimum.distance == 0.0
                    && query_accepts_attained(query, minimum.distance)
                {
                    return attained_result(prepared, minimum, query_witness(query));
                }
                let left = GeodesicInnerInterval {
                    lower: interval_lower_bound(
                        geodesic,
                        interval.left_value,
                        middle_value.distance,
                        middle - interval.left,
                    ),
                    left: interval.left,
                    right: middle,
                    left_value: interval.left_value,
                    right_value: middle_value.distance,
                };
                let right = GeodesicInnerInterval {
                    lower: interval_lower_bound(
                        geodesic,
                        middle_value.distance,
                        interval.right_value,
                        interval.right - middle,
                    ),
                    left: middle,
                    right: interval.right,
                    left_value: middle_value.distance,
                    right_value: interval.right_value,
                };
                if interval_may_contain_answer(left.lower, query, minimum.distance) {
                    crate::boundary::try_push_heap(&mut scratch.inner, left)?;
                }
                if interval_may_contain_answer(right.lower, query, minimum.distance) {
                    crate::boundary::try_push_heap(&mut scratch.inner, right)?;
                }
            }
            finish_query(prepared, query, minimum, backend_limited)
        },
    }
}

fn query_accepts_attained(query: GeodesicSegmentQuery, distance: f64) -> bool {
    match query {
        GeodesicSegmentQuery::Minimum { cutoff, .. } => distance < cutoff,
        GeodesicSegmentQuery::Threshold { limit, .. } => distance <= limit,
    }
}

fn finish_query(
    prepared: &PreparedSegment,
    query: GeodesicSegmentQuery,
    minimum: GeodesicSegmentMinimum,
    backend_limited: bool,
) -> Result<GeodesicSegmentQueryResult> {
    match query {
        GeodesicSegmentQuery::Minimum { cutoff, witness } => {
            if minimum.distance < cutoff {
                attained_result(prepared, minimum, witness)
            } else {
                Ok(GeodesicSegmentQueryResult::Excluded {
                    lower_bound: cutoff,
                })
            }
        },
        GeodesicSegmentQuery::Threshold {
            limit,
            completion: GeodesicThresholdCompletion::Complete,
            witness,
        } => {
            if minimum.distance <= limit {
                attained_result(prepared, minimum, witness)
            } else if backend_limited {
                Ok(GeodesicSegmentQueryResult::ThresholdResolvedOutside {
                    minimum: minimum.distance,
                })
            } else {
                Ok(GeodesicSegmentQueryResult::ThresholdOutside {
                    lower_bound: limit.next_up(),
                })
            }
        },
        GeodesicSegmentQuery::Threshold {
            completion: GeodesicThresholdCompletion::Probe,
            ..
        } => unreachable!("probe threshold queries do not enter global search"),
    }
}

fn probe_point_segment(
    prepared: &PreparedSegment,
    geodesic: &Geodesic,
    point: Point,
    limit: f64,
    witness: GeodesicWitnessNeed,
) -> Result<GeodesicSegmentQueryResult> {
    for index in 1..POINT_SEGMENT_PROBE_INTERVALS {
        let along = prepared.segment.length * index as f64 / POINT_SEGMENT_PROBE_INTERVALS as f64;
        let sample = prepared.sample(geodesic, point, along, false)?;
        if sample.distance <= limit {
            return attained_result(
                prepared,
                GeodesicSegmentMinimum {
                    distance: sample.distance,
                    along,
                },
                witness,
            );
        }
    }
    Ok(GeodesicSegmentQueryResult::ThresholdUnresolved)
}

fn attained_result(
    prepared: &PreparedSegment,
    minimum: GeodesicSegmentMinimum,
    witness: GeodesicWitnessNeed,
) -> Result<GeodesicSegmentQueryResult> {
    let foot = if matches!(witness, GeodesicWitnessNeed::Yes) {
        Some(prepared.witness(minimum)?)
    } else {
        None
    };
    Ok(GeodesicSegmentQueryResult::Attained { minimum, foot })
}

const fn query_witness(query: GeodesicSegmentQuery) -> GeodesicWitnessNeed {
    match query {
        GeodesicSegmentQuery::Minimum { witness, .. }
        | GeodesicSegmentQuery::Threshold { witness, .. } => witness,
    }
}

fn interval_is_excluded(
    lower: f64,
    query: GeodesicSegmentQuery,
    minimum: f64,
    geodesic: &Geodesic,
) -> bool {
    match query {
        GeodesicSegmentQuery::Minimum { cutoff, .. } => {
            lower >= cutoff
                || lower >= minimum
                || (minimum <= cutoff
                    && lower.is_finite()
                    && minimum - lower <= backend_bound_slack(geodesic))
        },
        GeodesicSegmentQuery::Threshold { limit, .. } => lower > limit,
    }
}

fn interval_may_contain_answer(lower: f64, query: GeodesicSegmentQuery, minimum: f64) -> bool {
    match query {
        GeodesicSegmentQuery::Minimum { cutoff, .. } => lower < cutoff && lower < minimum,
        GeodesicSegmentQuery::Threshold { limit, .. } => lower <= limit,
    }
}

fn convex_cell_minimum(
    prepared: &PreparedSegment,
    geodesic: &Geodesic,
    point: Point,
    interval: GeodesicInnerInterval,
    minimum: &mut GeodesicSegmentMinimum,
) -> Result<Option<GeodesicSegmentMinimum>> {
    let width = interval.right - interval.left;
    if !(width > 0.0 && width.is_finite()) {
        return Ok(None);
    }
    let center = f64::midpoint(interval.left, interval.right);
    let center_sample = prepared.sample(geodesic, point, center, false)?;
    update_minimum(minimum, center_sample.distance, center);
    let half_width = width / 2.0;
    let Some((omega, _, d_star)) = geodesic_curvature_domain(geodesic) else {
        return Ok(None);
    };
    let d = center_sample.distance + half_width + backend_floor(geodesic);
    if !(d.is_finite() && d < d_star) {
        return Ok(None);
    }
    let omega_d = omega * d;
    let c_d = if omega_d == 0.0 {
        1.0
    } else {
        omega_d / omega_d.tan()
    };
    if !(c_d.is_finite() && c_d > 0.0) {
        return Ok(None);
    }
    let left = prepared.sample(geodesic, point, interval.left, true)?;
    let right = prepared.sample(geodesic, point, interval.right, true)?;
    let (Some(left_derivative), Some(right_derivative)) = (left.derivative, right.derivative)
    else {
        return Ok(None);
    };
    if left_derivative.value >= left_derivative.error {
        return Ok(Some(GeodesicSegmentMinimum {
            distance: left.distance,
            along: interval.left,
        }));
    }
    if right_derivative.value <= -right_derivative.error {
        return Ok(Some(GeodesicSegmentMinimum {
            distance: right.distance,
            along: interval.right,
        }));
    }
    if !(left_derivative.value < -left_derivative.error
        && right_derivative.value > right_derivative.error)
    {
        return Ok(None);
    }
    solve_convex_stationary(
        prepared,
        geodesic,
        point,
        interval.left,
        left,
        interval.right,
        right,
    )
}

fn solve_convex_stationary(
    prepared: &PreparedSegment,
    geodesic: &Geodesic,
    point: Point,
    mut left: f64,
    mut left_sample: SegmentSample,
    mut right: f64,
    mut right_sample: SegmentSample,
) -> Result<Option<GeodesicSegmentMinimum>> {
    let mut minimum = if left_sample.distance <= right_sample.distance {
        GeodesicSegmentMinimum {
            distance: left_sample.distance,
            along: left,
        }
    } else {
        GeodesicSegmentMinimum {
            distance: right_sample.distance,
            along: right,
        }
    };
    while can_subdivide(left, right) {
        let left_derivative = left_sample.derivative.expect("convex endpoint derivative");
        let right_derivative = right_sample.derivative.expect("convex endpoint derivative");
        let middle = stationary_trial(left, left_derivative.value, right, right_derivative.value);
        let middle_sample = prepared.sample(geodesic, point, middle, true)?;
        update_minimum(&mut minimum, middle_sample.distance, middle);
        let Some(derivative) = middle_sample.derivative else {
            return Ok(None);
        };
        if derivative.value < -derivative.error {
            left = middle;
            left_sample = middle_sample;
        } else if derivative.value > derivative.error {
            right = middle;
            right_sample = middle_sample;
        } else {
            break;
        }
    }
    Ok(Some(minimum))
}

fn geodesic_tangent_ecef(
    geodesic: &Geodesic,
    latitude: f64,
    longitude: f64,
    azimuth: f64,
) -> Option<[f64; 3]> {
    let phi = latitude.to_radians();
    let lambda = longitude.to_radians();
    let (sin_phi, cos_phi) = phi.sin_cos();
    let (sin_lambda, cos_lambda) = lambda.sin_cos();
    let e2 = geodesic.f * (2.0 - geodesic.f);
    let denominator = (1.0 - e2 * sin_phi * sin_phi).sqrt();
    if !(denominator.is_finite() && denominator > 0.0) {
        return None;
    }
    let radius = geodesic.a / denominator;
    let radius_derivative =
        geodesic.a * e2 * sin_phi * cos_phi / (denominator * denominator * denominator);
    let north = [
        (radius_derivative * cos_phi - radius * sin_phi) * cos_lambda,
        (radius_derivative * cos_phi - radius * sin_phi) * sin_lambda,
        radius_derivative * (1.0 - e2) * sin_phi + radius * (1.0 - e2) * cos_phi,
    ];
    let north_norm = north[0].hypot(north[1]).hypot(north[2]);
    if !(north_norm.is_finite() && north_norm > 0.0) {
        return None;
    }
    let north = [
        north[0] / north_norm,
        north[1] / north_norm,
        north[2] / north_norm,
    ];
    let east = [-sin_lambda, cos_lambda, 0.0];
    let (sin_azimuth, cos_azimuth) = azimuth.to_radians().sin_cos();
    let tangent = [
        cos_azimuth * north[0] + sin_azimuth * east[0],
        cos_azimuth * north[1] + sin_azimuth * east[1],
        cos_azimuth * north[2] + sin_azimuth * east[2],
    ];
    let norm = tangent[0].hypot(tangent[1]).hypot(tangent[2]);
    (norm.is_finite() && norm > 0.0)
        .then(|| [tangent[0] / norm, tangent[1] / norm, tangent[2] / norm])
}

fn dot3(left: [f64; 3], right: [f64; 3]) -> f64 {
    left[0] * right[0] + left[1] * right[1] + left[2] * right[2]
}

fn stationary_trial(left: f64, left_slope: f64, right: f64, right_slope: f64) -> f64 {
    let middle = f64::midpoint(left, right);
    let denominator = right_slope - left_slope;
    if denominator == 0.0 || !denominator.is_finite() {
        return middle;
    }
    let secant = left - left_slope * (right - left) / denominator;
    if secant > left && secant < right {
        secant
    } else {
        middle
    }
}

pub(crate) fn geodesic_curvature_domain(geodesic: &Geodesic) -> Option<(f64, f64, f64)> {
    let a = geodesic.a;
    let b = geodesic._b;
    if !(a.is_finite() && a > 0.0 && b.is_finite() && b > 0.0) {
        return None;
    }
    let omega = (1.0 / b).max(b / (a * a));
    let minimum = a.min(b);
    let maximum = a.max(b);
    let kappa = maximum / (minimum * minimum);
    let d_star = (std::f64::consts::PI / kappa).min(std::f64::consts::PI / (2.0 * omega));
    (omega.is_finite()
        && omega > 0.0
        && kappa.is_finite()
        && kappa > 0.0
        && d_star.is_finite()
        && d_star > 0.0)
        .then_some((omega, kappa, d_star))
}

fn backend_floor(geodesic: &Geodesic) -> f64 {
    geodesic.a.abs() * GEODESIC_BACKEND_RELATIVE_FLOOR
}

fn backend_bound_slack(geodesic: &Geodesic) -> f64 {
    (POINT_SEGMENT_BACKEND_SLACK_MULTIPLIER * backend_floor(geodesic)).max(f64::MIN_POSITIVE)
}

fn interval_lower_bound(geodesic: &Geodesic, left: f64, right: f64, width: f64) -> f64 {
    if !(left.is_finite()
        && left >= 0.0
        && right.is_finite()
        && right >= 0.0
        && width.is_finite()
        && width > 0.0)
    {
        return 0.0;
    }
    let floor = backend_floor(geodesic);
    let left_lower = if floor.is_finite() {
        (left - floor).max(0.0)
    } else {
        0.0
    };
    let right_lower = if floor.is_finite() {
        (right - floor).max(0.0)
    } else {
        0.0
    };
    let scale = left_lower.max(right_lower).max(width);
    if !(scale.is_finite() && scale > 0.0) {
        return 0.0;
    }
    let left_scaled = left_lower / scale;
    let right_scaled = right_lower / scale;
    let width_scaled = width / scale;
    if !(left_scaled.is_finite() && right_scaled.is_finite() && width_scaled.is_finite()) {
        return 0.0;
    }

    // The minimum of the two endpoint cones is a valid global lower bound.
    let cone_x = f64::midpoint(left_scaled - right_scaled, width_scaled).clamp(0.0, width_scaled);
    let cone = (left_scaled - cone_x)
        .max(right_scaled - (width_scaled - cone_x))
        .max(0.0)
        * scale;

    // On the admitted positively curved ellipsoid, squared distance is
    // semiconcave along a geodesic. This convex quadratic is safe only when
    // its cancellation is resolved beyond the explicit arithmetic allowance;
    // otherwise the cone alone remains the bound.
    let squared = if width_scaled > 0.0 {
        let numerator = width_scaled.mul_add(
            width_scaled,
            left_scaled.mul_add(left_scaled, -(right_scaled * right_scaled)),
        );
        let x = (numerator / (2.0 * width_scaled)).clamp(0.0, width_scaled);
        let fraction = x / width_scaled;
        let q = (1.0 - fraction) * (left_scaled * left_scaled)
            + fraction * (right_scaled * right_scaled)
            - x * (width_scaled - x);
        let rounding = POINT_SEGMENT_BOUND_ROUNDING_ULPS
            * f64::EPSILON
            * (left_scaled * left_scaled
                + right_scaled * right_scaled
                + width_scaled * width_scaled
                + 1.0);
        if q.is_finite() && q > rounding {
            (q - rounding).sqrt() * scale
        } else {
            0.0
        }
    } else {
        0.0
    };

    let bound = cone.max(squared) - POINT_SEGMENT_BOUND_ROUNDING_ULPS * f64::EPSILON * scale;
    if bound.is_finite() {
        bound.max(0.0).next_down()
    } else {
        0.0
    }
}

fn exact_segment_contact(
    geodesic: &Geodesic,
    point: Point,
    prepared: &PreparedSegment,
) -> Option<f64> {
    let segment = prepared.segment;
    if same_geodesic_vertex(point, segment.start) {
        return Some(0.0);
    }
    if same_geodesic_vertex(point, segment.end) {
        return Some(segment.length);
    }
    if segment.length == 0.0 {
        return None;
    }
    let (from_start, ..): (f64, f64, f64, f64) =
        geodesic.inverse(segment.start.y, segment.start.x, point.y, point.x);
    if from_start.is_finite() && from_start > 0.0 && from_start < segment.length {
        let (_, lat, lon, ..) =
            prepared
                .line
                ._gen_position(false, from_start, GEODESIC_LINE_OUTMASK);
        if lat.is_finite()
            && lon.is_finite()
            && same_geodesic_vertex(point, Point::new_raw_xy(lon, lat))
        {
            return Some(from_start);
        }
    }
    if let Some(along) = equatorial_arc_parameter(point, &segment) {
        return Some(along);
    }
    if same_geodesic_longitude(segment.start.x, segment.end.x)
        && same_geodesic_longitude(point.x, segment.start.x)
        && point.y >= segment.start.y.min(segment.end.y)
        && point.y <= segment.start.y.max(segment.end.y)
    {
        let (along, ..): (f64, f64, f64, f64) =
            geodesic.inverse(segment.start.y, segment.start.x, point.y, point.x);
        if along.is_finite() && along >= 0.0 && along <= segment.length {
            return Some(along);
        }
    }
    None
}

fn equatorial_arc_parameter(point: Point, segment: &GeodesicSegment) -> Option<f64> {
    if segment.start.y != 0.0 || segment.end.y != 0.0 || point.y != 0.0 {
        return None;
    }
    let delta = longitude_difference(segment.start.x, segment.end.x);
    if delta == 0.0 || delta.abs() >= 180.0 {
        return None;
    }
    let point_delta = longitude_difference(segment.start.x, point.x);
    ((delta > 0.0 && (0.0..=delta).contains(&point_delta))
        || (delta < 0.0 && (delta..=0.0).contains(&point_delta)))
    .then_some(segment.length * point_delta.abs() / delta.abs())
}

#[expect(
    clippy::float_cmp,
    reason = "the equatorial endpoints are exact parameter identities; a tolerance would change the chosen longitude"
)]
fn exact_equatorial_minimum(
    geodesic: &Geodesic,
    point: Point,
    segment: &GeodesicSegment,
    endpoint_minimum: GeodesicSegmentMinimum,
) -> Option<GeodesicSegmentMinimum> {
    if segment.start.y != 0.0 || segment.end.y != 0.0 || segment.length == 0.0 {
        return None;
    }
    let delta = longitude_difference(segment.start.x, segment.end.x);
    if delta == 0.0 || delta.abs() >= 180.0 {
        return None;
    }
    let point_delta = longitude_difference(segment.start.x, point.x);
    let along = if (delta > 0.0 && point_delta < 0.0)
        || (delta < 0.0 && point_delta > 0.0)
        || point_delta.abs() > delta.abs()
    {
        endpoint_minimum.along
    } else {
        segment.length * point_delta.abs() / delta.abs()
    };
    let longitude = if along == 0.0 {
        segment.start.x
    } else if along == segment.length {
        segment.end.x
    } else {
        point.x
    };
    let distance = inverse_distance(geodesic, point.x, point.y, longitude, 0.0);
    distance
        .is_finite()
        .then_some(GeodesicSegmentMinimum { distance, along })
}

fn exact_family_minimum(
    geodesic: &Geodesic,
    point: Point,
    prepared: &PreparedSegment,
    endpoint_minimum: GeodesicSegmentMinimum,
) -> Result<Option<GeodesicSegmentMinimum>> {
    if geodesic.f == 0.0
        && let Some(minimum) =
            exact_sphere_segment_minimum(geodesic, point, prepared, endpoint_minimum)?
    {
        return Ok(Some(minimum));
    }
    Ok(exact_equatorial_minimum(
        geodesic,
        point,
        &prepared.segment,
        endpoint_minimum,
    ))
}

fn exact_sphere_segment_minimum(
    geodesic: &Geodesic,
    point: Point,
    prepared: &PreparedSegment,
    endpoint_minimum: GeodesicSegmentMinimum,
) -> Result<Option<GeodesicSegmentMinimum>> {
    let radius = geodesic.a;
    let theta = prepared.segment.length / radius;
    if !(radius.is_finite() && radius > 0.0 && theta.is_finite() && theta < std::f64::consts::PI) {
        return Ok(None);
    }
    let (_, lat, lon, azimuth, ..) = prepared
        .line
        ._gen_position(false, 0.0, GEODESIC_LINE_OUTMASK);
    let Some(tangent) = geodesic_tangent_ecef(geodesic, lat, lon, azimuth) else {
        return Ok(None);
    };
    let start = unit_vector(lon, lat);
    let query = unit_vector(point.x, point.y);
    let coefficient_a = dot3(query, start);
    let coefficient_b = dot3(query, tangent);
    // The dot product is maximized, not minimized, at atan2(B, A). Adding
    // π selects the antipodal stationary direction and can choose a farther
    // endpoint when the closest stationary point lies inside the arc.
    let phase = coefficient_b.atan2(coefficient_a);
    let mut minimum = endpoint_minimum;
    for shift in -1..=1 {
        let angle = phase + f64::from(shift) * std::f64::consts::TAU;
        if angle <= 0.0 || angle >= theta {
            continue;
        }
        let along = angle * radius;
        let sample = prepared.sample(geodesic, point, along, false)?;
        update_minimum(&mut minimum, sample.distance, along);
    }
    Ok(Some(minimum))
}

fn unit_vector(longitude: f64, latitude: f64) -> [f64; 3] {
    let (sin_lat, cos_lat) = latitude.to_radians().sin_cos();
    let (sin_lon, cos_lon) = longitude.to_radians().sin_cos();
    [cos_lat * cos_lon, cos_lat * sin_lon, sin_lat]
}

fn update_minimum(minimum: &mut GeodesicSegmentMinimum, distance: f64, along: f64) {
    if distance < minimum.distance
        || (distance.to_bits() == minimum.distance.to_bits() && along < minimum.along)
    {
        minimum.distance = distance;
        minimum.along = along;
    }
}

#[expect(
    clippy::large_types_passed_by_value,
    reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
)]
fn materialize_segment_point(
    line: &GeodesicLine,
    segment: GeodesicSegment,
    along: f64,
) -> Result<Point> {
    if along <= 0.0 {
        return Ok(segment.start);
    }
    if along >= segment.length {
        return Ok(segment.end);
    }
    let (_, lat, lon, ..) = line._gen_position(false, along, GEODESIC_LINE_OUTMASK);
    if !(lat.is_finite() && lon.is_finite()) {
        return Err(CrsError::message(
            "geodesic segment witness position is not finite",
        ));
    }
    let fraction = along / segment.length;
    Point::new_axes(
        lon,
        lat,
        ZOrdinate(interpolate_optional_ordinate(
            segment.start.z(),
            segment.end.z(),
            fraction,
        )),
        MOrdinate(interpolate_optional_ordinate(
            segment.start.m(),
            segment.end.m(),
            fraction,
        )),
    )
}

fn ensure_finite_distance(distance: f64) -> Result<()> {
    if distance.is_finite() && distance >= 0.0 {
        Ok(())
    } else {
        Err(CrsError::message(
            "geodesic point-to-segment evaluation produced a non-finite distance",
        ))
    }
}

#[expect(
    clippy::float_cmp,
    reason = "geographic endpoint aliases are exact pole or period identities, never near-coordinate contacts"
)]
fn same_geodesic_vertex(left: Point, right: Point) -> bool {
    left.y == right.y
        && (left.x == right.x || left.y.abs() == 90.0 || (left.x - right.x).abs() == 360.0)
}

#[expect(
    clippy::float_cmp,
    reason = "longitude aliases are exact period identities, never near-coordinate contacts"
)]
fn same_geodesic_longitude(left: f64, right: f64) -> bool {
    left == right || (left - right).abs() == 360.0
}

fn can_subdivide(left: f64, right: f64) -> bool {
    let middle = f64::midpoint(left, right);
    middle > left && middle < right
}

fn interpolate_optional_ordinate(
    start: Option<f64>,
    end: Option<f64>,
    fraction: f64,
) -> Option<f64> {
    match (start, end) {
        (Some(start), Some(end)) => Some(start + (end - start) * fraction),
        _ => None,
    }
}

fn angle_difference_radians(alpha_degrees: f64, beta_degrees: f64) -> f64 {
    angle_difference(beta_degrees, alpha_degrees).0.to_radians()
}

pub(crate) struct IntersectSolver {
    geodesic: Geodesic,
    radius: f64,
    d: f64,
    eps: f64,
    tol: f64,
    delta: f64,
    t1: f64,
    d1: f64,
}

#[derive(Clone, Copy, Debug)]
struct XPoint {
    x: f64,
    y: f64,
    coincidence: i32,
}

impl XPoint {
    const fn new(x: f64, y: f64) -> Self {
        Self {
            x,
            y,
            coincidence: 0,
        }
    }

    const fn with_coincidence(x: f64, y: f64, coincidence: i32) -> Self {
        Self { x, y, coincidence }
    }

    fn add(self, other: Self) -> Self {
        Self::with_coincidence(
            self.x + other.x,
            self.y + other.y,
            if other.coincidence != 0 {
                other.coincidence
            } else {
                self.coincidence
            },
        )
    }

    fn l1(self) -> f64 {
        self.x.abs() + self.y.abs()
    }

    fn l1_from(self, other: Self) -> f64 {
        (self.x - other.x).abs() + (self.y - other.y).abs()
    }
}

impl IntersectSolver {
    pub(crate) fn new(geodesic: &Geodesic) -> Result<Self> {
        let radius = geodesic._c2.sqrt();
        let d = radius * std::f64::consts::PI;
        let eps = 3.0 * f64::EPSILON;
        let tol = d * f64::EPSILON.powf(0.75);
        let delta = d * f64::EPSILON.powf(0.2);
        let mut t1 = geodesic.a * (1.0 - geodesic.f) * std::f64::consts::PI;
        let t4;
        let mut t2 = 2.0 * Self::distpolar_static(geodesic, tol, 90.0);
        let t5 = 2.0 * inverse_distance(geodesic, 0.0, 0.0, 0.0, 90.0);
        let t3;
        if geodesic.f > 0.0 {
            t3 = Self::distoblique(geodesic, radius, d, tol)?;
            t4 = t1;
        } else {
            t3 = t5;
            t4 = Self::polarb(geodesic, d, tol);
            std::mem::swap(&mut t1, &mut t2);
        }
        let d1 = t2 / 2.0;
        let d2 = 2.0 * t3 / 3.0;
        let d3 = t4 - delta;
        if !(d1 < d3 && d2 < d3 && d2 < 2.0 * t1) {
            return Err(CrsError::message(
                "geodesic segment intersection is unsupported for this ellipsoid",
            ));
        }
        Ok(Self {
            geodesic: *geodesic,
            radius,
            d,
            eps,
            tol,
            delta,
            t1,
            d1,
        })
    }

    fn position(&self, line: &GeodesicLine, along: f64) -> (f64, f64, f64) {
        let (_, lat, lon, azimuth, ..) = line._gen_position(false, along, INTERSECT_LINE_OUTMASK);
        (lat, lon, azimuth)
    }

    fn spherical(&self, line_x: &GeodesicLine, line_y: &GeodesicLine, p: XPoint) -> XPoint {
        let (lat_x, lon_x, azi_x) = self.position(line_x, p.x);
        let (lat_y, lon_y, azi_y) = self.position(line_y, p.y);
        let (z, azi_xa, azi_ya, _a12): (f64, f64, f64, f64) =
            self.geodesic.inverse(lat_x, lon_x, lat_y, lon_y);
        let sin_z = (z / self.radius).sin();
        let cos_z = (z / self.radius).cos();
        let x = angle_difference(azi_x, azi_xa);
        let y = angle_difference(azi_y, azi_ya);
        let xy = angle_difference(x.0, y.0);
        let sign = 1.0_f64.copysign(xy.0 + (xy.1 + y.1 - x.1));
        let (sin_x, cos_x) = sincos_degrees(sign * x.0, sign * x.1);
        let (sin_y, cos_y) = sincos_degrees(sign * y.0, sign * y.1);
        if z <= self.eps * self.radius {
            let coincidence =
                if (sin_x - sin_y).abs() <= self.eps && (cos_x - cos_y).abs() <= self.eps {
                    1
                } else if (sin_x + sin_y).abs() <= self.eps && (cos_x + cos_y).abs() <= self.eps {
                    -1
                } else {
                    0
                };
            return XPoint::with_coincidence(0.0, 0.0, coincidence);
        }
        if sin_x.abs() <= self.eps && sin_y.abs() <= self.eps {
            return XPoint::with_coincidence(
                cos_x * z / 2.0,
                -cos_y * z / 2.0,
                if cos_x * cos_y > 0.0 { 1 } else { -1 },
            );
        }
        XPoint::new(
            self.radius * (sin_y * sin_z).atan2(sin_y * cos_x * cos_z - cos_y * sin_x),
            self.radius * (sin_x * sin_z).atan2(-sin_x * cos_y * cos_z + cos_x * sin_y),
        )
    }

    fn basic(&self, line_x: &GeodesicLine, line_y: &GeodesicLine, p0: XPoint) -> Result<XPoint> {
        let mut q = p0;
        for _ in 0..100 {
            let dq = self.spherical(line_x, line_y, q);
            q = q.add(dq);
            if !(q.x.is_finite() && q.y.is_finite() && dq.x.is_finite() && dq.y.is_finite()) {
                return Err(CrsError::message(
                    "geodesic segment intersection failed to converge",
                ));
            }
            if q.coincidence != 0 || dq.l1() <= self.tol {
                return Ok(q);
            }
        }
        Err(CrsError::message(
            "geodesic segment intersection failed to converge",
        ))
    }

    fn closest(&self, line_x: &GeodesicLine, line_y: &GeodesicLine, p0: XPoint) -> Result<XPoint> {
        let offsets = [(0.0, 0.0), (1.0, 0.0), (-1.0, 0.0), (0.0, 1.0), (0.0, -1.0)];
        let mut skip = [false; 5];
        let mut q = XPoint::new(f64::NAN, 0.0);
        for (n, (ix, iy)) in offsets.into_iter().enumerate() {
            if skip[n] {
                continue;
            }
            let mut qx = self.basic(
                line_x,
                line_y,
                XPoint::new(p0.x + ix * self.d1, p0.y + iy * self.d1),
            )?;
            qx = fixcoincident(p0, qx);
            if qx.l1_from(q) <= self.delta {
                continue;
            }
            if qx.l1_from(p0) < self.t1 {
                q = qx;
                break;
            }
            if n == 0 || qx.l1_from(p0) < q.l1_from(p0) {
                q = qx;
            }
            for m in n + 1..5 {
                skip[m] = skip[m]
                    || qx.l1_from(XPoint::new(
                        p0.x + offsets[m].0 * self.d1,
                        p0.y + offsets[m].1 * self.d1,
                    )) < 2.0 * self.t1 - self.d1 - self.delta;
            }
        }
        if q.x.is_finite() && q.y.is_finite() {
            Ok(q)
        } else {
            Err(CrsError::message(
                "geodesic segment intersection failed to converge",
            ))
        }
    }

    fn segment(
        &self,
        line_x: &GeodesicLine,
        sx: f64,
        line_y: &GeodesicLine,
        sy: f64,
        mode: &mut i32,
    ) -> Result<XPoint> {
        let p0 = XPoint::new(sx / 2.0, sy / 2.0);
        let mut q = fixsegment(sx, sy, self.closest(line_x, line_y, p0)?);
        *mode = segmentmode(sx, sy, q);
        if *mode != 0 && p0.l1() >= p0.l1_from(q) {
            let mut segment_mode_x = 1;
            let mut corner_result = XPoint::new(f64::NAN, f64::NAN);
            for ix in 0..2 {
                for iy in 0..2 {
                    if segment_mode_x == 0 {
                        break;
                    }
                    let corner = XPoint::new(f64::from(ix) * sx, f64::from(iy) * sy);
                    if q.l1_from(corner) >= 2.0 * self.t1 {
                        corner_result = fixcoincident(corner, self.basic(line_x, line_y, corner)?);
                        segment_mode_x = segmentmode(sx, sy, corner_result);
                    }
                }
            }
            if segment_mode_x == 0 {
                *mode = 0;
                q = corner_result;
            }
        }
        Ok(q)
    }

    fn conjugate_distance(&self, line: &GeodesicLine, target: f64, semi: bool) -> f64 {
        self.conjugate_distance_from(line, target, semi, 0.0, 1.0, 1.0)
    }

    fn conjugate_distance_from(
        &self,
        line: &GeodesicLine,
        target: f64,
        semi: bool,
        m12: f64,
        m12_scale: f64,
        m21_scale: f64,
    ) -> f64 {
        let mut s = target;
        for _ in 0..100 {
            let (_, _, _, _, _, m13, m13_scale, m31_scale, _) =
                line._gen_position(false, s, INTERSECT_LINE_OUTMASK);
            let m23 = m13 * m12_scale - m12 * m13_scale;
            let m23_scale = m13_scale * m21_scale
                + if m12 == 0.0 {
                    0.0
                } else {
                    (1.0 - m12_scale * m21_scale) * m13 / m12
                };
            let m32_scale = m31_scale * m12_scale
                + if m13 == 0.0 {
                    0.0
                } else {
                    (1.0 - m13_scale * m31_scale) * m12 / m13
                };
            let ds = if semi {
                m23 * m23_scale / (1.0 - m23_scale * m32_scale)
            } else {
                -m23 / m32_scale
            };
            s += ds;
            if ds.abs() <= self.tol || !ds.is_finite() {
                break;
            }
        }
        s
    }

    fn distpolar(&self, latitude: f64) -> f64 {
        let line = GeodesicLine::new(
            &self.geodesic,
            latitude,
            0.0,
            0.0,
            Some(GEODESIC_LINE_CAPS),
            None,
            None,
        );
        self.conjugate_distance(
            &line,
            (1.0 + self.geodesic.f / 2.0) * self.geodesic.a * std::f64::consts::PI / 2.0,
            true,
        )
    }

    fn polarb(geodesic: &Geodesic, d: f64, tol: f64) -> f64 {
        if geodesic.f == 0.0 {
            return d;
        }
        let mut lat0 = 63.0;
        let mut s0 = Self::distpolar_static(geodesic, tol, lat0);
        let mut lat1 = 65.0;
        let mut s1 = Self::distpolar_static(geodesic, tol, lat1);
        let mut lat2 = 64.0;
        let mut s2 = Self::distpolar_static(geodesic, tol, lat2);
        let mut sx = s2;
        for _ in 0..10 {
            let denominator = (lat1 - lat0) * s2 + (lat0 - lat2) * s1 + (lat2 - lat1) * s0;
            if denominator == 0.0 || !denominator.is_finite() {
                break;
            }
            let latn = ((lat1 - lat0) * (lat1 + lat0) * s2
                + (lat0 - lat2) * (lat1 + lat2) * s1
                + (lat2 - lat1) * (lat2 + lat1) * s0)
                / (2.0 * denominator);
            lat0 = lat1;
            s0 = s1;
            lat1 = lat2;
            s1 = s2;
            lat2 = latn;
            s2 = Self::distpolar_static(geodesic, tol, lat2);
            if geodesic.f < 0.0 {
                sx = sx.min(s2);
            } else {
                sx = sx.max(s2);
            }
        }
        2.0 * sx
    }

    fn distpolar_static(geodesic: &Geodesic, tol: f64, latitude: f64) -> f64 {
        let solver = Self {
            geodesic: *geodesic,
            radius: geodesic._c2.sqrt(),
            d: geodesic._c2.sqrt() * std::f64::consts::PI,
            eps: 3.0 * f64::EPSILON,
            tol,
            delta: 0.0,
            t1: 0.0,
            d1: 0.0,
        };
        solver.distpolar(latitude)
    }

    #[expect(
        clippy::float_cmp,
        reason = "a pole or antimeridian is one specific representable coordinate, and the question is whether a vertex IS that coordinate; a tolerance would admit vertices merely near it"
    )]
    fn distoblique(geodesic: &Geodesic, radius: f64, d: f64, tol: f64) -> Result<f64> {
        if geodesic.f == 0.0 {
            return Ok(d);
        }
        let mut azi0 = 46.0;
        let mut ds0 = 0.0;
        Self::conjdist_static(geodesic, radius, tol, azi0, &mut ds0)?;
        let mut azi1 = 44.0;
        let mut ds1 = 0.0;
        let mut s1 = Self::conjdist_static(geodesic, radius, tol, azi1, &mut ds1)?;
        let mut sx = s1;
        let mut dsx = ds1.abs();
        for _ in 0..10 {
            if ds1 == ds0 {
                break;
            }
            let azimuth = (azi0 * ds1 - azi1 * ds0) / (ds1 - ds0);
            azi0 = azi1;
            ds0 = ds1;
            azi1 = azimuth;
            s1 = Self::conjdist_static(geodesic, radius, tol, azi1, &mut ds1)?;
            if ds1.abs() < dsx {
                sx = s1;
                dsx = ds1.abs();
            }
            if ds1 == 0.0 {
                break;
            }
        }
        Ok(sx)
    }

    fn conjdist_static(
        geodesic: &Geodesic,
        radius: f64,
        tol: f64,
        azimuth: f64,
        difference: &mut f64,
    ) -> Result<f64> {
        let solver = Self {
            geodesic: *geodesic,
            radius,
            d: radius * std::f64::consts::PI,
            eps: 3.0 * f64::EPSILON,
            tol,
            delta: 0.0,
            t1: 0.0,
            d1: 0.0,
        };
        let line = GeodesicLine::new(
            geodesic,
            0.0,
            0.0,
            azimuth,
            Some(GEODESIC_LINE_CAPS),
            None,
            None,
        );
        let s = solver.conjugate_distance(&line, solver.d, false);
        let p = solver.basic(&line, &line, XPoint::new(s / 2.0, -3.0 * s / 2.0))?;
        *difference = p.l1() - 2.0 * s;
        Ok(s)
    }
}

#[expect(
    clippy::float_cmp,
    reason = "a pole or antimeridian is one specific representable coordinate, and the question is whether a vertex IS that coordinate; a tolerance would admit vertices merely near it"
)]
fn angle_difference(first: f64, second: f64) -> (f64, f64) {
    let (sum, error) = exact_sum(remainder(-first, 360.0), remainder(second, 360.0));
    let (difference, error) = exact_sum(remainder(sum, 360.0), error);
    if difference == 180.0 && error > 0.0 {
        exact_sum(-180.0, error)
    } else {
        let sign = if difference == 0.0 || difference.abs() == 180.0 {
            if error == 0.0 { second - first } else { -error }
        } else {
            difference
        };
        (difference.copysign(sign), error)
    }
}

fn exact_sum(first: f64, second: f64) -> (f64, f64) {
    let sum = first + second;
    let first_part = sum - second;
    let second_part = sum - first_part;
    let error = -((first_part - first) + (second_part - second));
    (sum, error)
}

fn remainder(value: f64, period: f64) -> f64 {
    let reduced = value % period;
    if reduced < -period / 2.0 {
        reduced + period
    } else if reduced < period / 2.0 {
        reduced
    } else {
        reduced - period
    }
}

fn angle_round(value: f64) -> f64 {
    let threshold = 1.0 / 16.0;
    let mut magnitude = value.abs();
    if magnitude < threshold {
        magnitude = threshold - (threshold - magnitude);
    }
    magnitude.copysign(value)
}

#[expect(
    clippy::float_cmp,
    reason = "a pole or antimeridian is one specific representable coordinate, and the question is whether a vertex IS that coordinate; a tolerance would admit vertices merely near it"
)]
fn sincos_degrees(value: f64, correction: f64) -> (f64, f64) {
    let reduced_turn = value.rem_euclid(360.0);
    let quadrant = ((reduced_turn / 90.0) + 0.5).floor() as i32;
    let reduced = angle_round(reduced_turn - f64::from(quadrant) * 90.0 + correction);
    let (mut sine, mut cosine) = reduced.to_radians().sin_cos();
    if 2.0 * reduced.abs() == 90.0 {
        cosine = std::f64::consts::FRAC_1_SQRT_2;
        sine = cosine.copysign(reduced);
    } else if 3.0 * reduced.abs() == 90.0 {
        cosine = 3.0_f64.sqrt() / 2.0;
        sine = 0.5_f64.copysign(reduced);
    }
    match quadrant.rem_euclid(4) {
        0 => {},
        1 => (sine, cosine) = (cosine, -sine),
        2 => (sine, cosine) = (-sine, -cosine),
        _ => (sine, cosine) = (-cosine, sine),
    }
    cosine += 0.0;
    if sine == 0.0 {
        sine = sine.copysign(value);
    }
    (sine, cosine)
}

fn fixcoincident(p0: XPoint, p: XPoint) -> XPoint {
    if p.coincidence == 0 {
        return p;
    }
    let s =
        ((p0.x + f64::from(p.coincidence) * p0.y) - (p.x + f64::from(p.coincidence) * p.y)) / 2.0;
    XPoint::with_coincidence(p.x + s, p.y + f64::from(p.coincidence) * s, p.coincidence)
}

fn fixsegment(sx: f64, sy: f64, p: XPoint) -> XPoint {
    if p.coincidence == 0 {
        return p;
    }
    let coincidence = f64::from(p.coincidence);
    let pya = p.y - coincidence * p.x;
    let sa = -p.x;
    let pyb = p.y - coincidence * (p.x - sx);
    let sb = sx - p.x;
    let pxc = p.x - coincidence * p.y;
    let sc = -coincidence * p.y;
    let pxd = p.x - coincidence * (p.y - sy);
    let sd = coincidence * (sy - p.y);
    let ga = (0.0..=sy).contains(&pya);
    let gb = (0.0..=sy).contains(&pyb);
    let gc = (0.0..=sx).contains(&pxc);
    let gd = (0.0..=sx).contains(&pxd);
    let s = if ga && gb {
        f64::midpoint(sa, sb)
    } else if gc && gd {
        f64::midpoint(sc, sd)
    } else if ga && gc {
        f64::midpoint(sa, sc)
    } else if ga && gd {
        f64::midpoint(sa, sd)
    } else if gb && gc {
        f64::midpoint(sb, sc)
    } else if gb && gd {
        f64::midpoint(sb, sd)
    } else if p.coincidence > 0 {
        if ((p.x - p.y) + sy).abs() < ((p.x - p.y) - sx).abs() {
            (sy - (p.x + p.y)) / 2.0
        } else {
            (sx - (p.x + p.y)) / 2.0
        }
    } else if (p.x + p.y).abs() < ((p.x + p.y) - (sx + sy)).abs() {
        -(p.x - p.y) / 2.0
    } else {
        ((sx - sy) - (p.x - p.y)) / 2.0
    };
    XPoint::with_coincidence(p.x + s, p.y + coincidence * s, p.coincidence)
}

fn segmentmode(sx: f64, sy: f64, p: XPoint) -> i32 {
    side_code(p.x, sx) * 3 + side_code(p.y, sy)
}

fn side_code(value: f64, limit: f64) -> i32 {
    if value < 0.0 {
        -1
    } else {
        i32::from(value > limit)
    }
}
