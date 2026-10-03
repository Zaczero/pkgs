use geographiclib_rs::{Geodesic, GeodesicLine, InverseGeodesic as _, geodesic_capability as caps};

#[cfg(test)]
use crate::crs::geodesic::geodesic_counters;
use crate::crs::geodesic::{
    GEODESIC_BACKEND_RELATIVE_FLOOR, GEODESIC_LINE_OUTMASK, inverse_distance,
};
use crate::crs::{CrsError, GEODESIC_LINE_CACHE, GeodesicLineKey};
use crate::error::Result;
use crate::geographic::GeographicFrame;
use crate::geometry::{
    GeodesicInnerInterval, GeodesicMinimum, GeodesicNumericalPolicy, GeodesicSegment,
    GeodesicSegmentMinimum, GeodesicSegmentQuery, GeodesicSegmentQueryResult,
    GeodesicSegmentScratch, GeodesicWitnessNeed, MOrdinate, Point, ZOrdinate,
    interpolate_optional_ordinate,
};

const POINT_SEGMENT_BOUND_ROUNDING_ULPS: f64 = 128.0;
const POINT_SEGMENT_DERIVATIVE_ROUNDING_ULPS: f64 = 256.0;
// Endpoint lower bounds and the attained sample each carry the backend floor;
// keep two further terms for the conservative bound/square-root path.
const POINT_SEGMENT_BACKEND_SLACK_MULTIPLIER: f64 = 4.0;
const POINT_SEGMENT_LINE_CAPS: u64 = caps::LATITUDE
    | caps::LONGITUDE
    | caps::AZIMUTH
    | caps::REDUCEDLENGTH
    | caps::GEODESICSCALE
    | caps::DISTANCE_IN;

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
    let (length, azimuth0) =
        spherical_meridional_branch(geodesic, start, end).unwrap_or((length, azimuth0));
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
    if length == 0.0 {
        return Ok(());
    }
    let opposite_poles = start.y.abs() == 90.0 && end.y.abs() == 90.0 && start.y != end.y;
    let (delta_lon, delta_correction) = angle_difference(start.x, end.x);
    let exact_half_turn = delta_lon.abs() == 180.0 && delta_correction == 0.0;
    let opposite_latitudes = start.y == -end.y;
    if !(length.is_finite() && azimuth0.is_finite() && azimuth1.is_finite()) {
        return Err(CrsError::message(
            "geodesic segment construction produced a non-finite inverse",
        ));
    }
    let ambiguous = opposite_poles
        || (geodesic.f == 0.0 && exact_half_turn && opposite_latitudes)
        || (geodesic.f > 0.0
            && opposite_latitudes
            && start.y.abs() < 90.0
            && end.y.abs() < 90.0
            && azimuths_show_distinct_branches(azimuth0, azimuth1))
        || (geodesic.f < 0.0 && exact_half_turn && azimuth0 != 0.0 && azimuth0.abs() != 180.0);
    if ambiguous {
        return Err(CrsError::message(
            "geodesic segment endpoints have multiple shortest geodesics",
        ));
    }
    Ok(())
}

fn azimuths_show_distinct_branches(first: f64, second: f64) -> bool {
    let (difference, correction) = angle_difference(first, second);
    difference != 0.0 || correction != 0.0
}

fn spherical_meridional_branch(
    geodesic: &Geodesic,
    start: Point,
    end: Point,
) -> Option<(f64, f64)> {
    if geodesic.f != 0.0 {
        return None;
    }
    let (angular_length, azimuth) = spherical_meridional_branch_parameters(start, end)?;
    let length = geodesic.a * angular_length;
    if !(length.is_finite() && length > 0.0) {
        return None;
    }
    Some((length, azimuth))
}

#[expect(
    clippy::float_cmp,
    reason = "exact represented antipodal branch conditions are structural, not tolerances"
)]
pub(crate) fn spherical_meridional_branch_parameters(
    start: Point,
    end: Point,
) -> Option<(f64, f64)> {
    if start.y + end.y == 0.0 {
        return None;
    }
    let (longitude, correction) = angle_difference(start.x, end.x);
    if longitude.abs() != 180.0 || correction != 0.0 {
        return None;
    }
    let latitude_sum = start.y + end.y;
    let angular_length = std::f64::consts::PI - latitude_sum.abs().to_radians();
    if !(angular_length > 0.0 && angular_length.is_finite()) {
        return None;
    }
    Some((angular_length, if latitude_sum > 0.0 { 0.0 } else { 180.0 }))
}

pub(crate) fn longitude_difference(start: f64, end: f64) -> f64 {
    let axis = GeographicFrame::crs84().longitude();
    axis.shortest_alias_step(start, end)
        .map_or(f64::NAN, |step| step.delta(axis))
}

/// Run the point-to-segment minimum owner used by Hausdorff witnesses.
///
/// The interval queue owns only parameter cells. It never returns a pruning
/// bound as a distance: an attained result carries its value and optional foot.
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

    let GeodesicSegmentQuery::Minimum { cutoff, witness } = query;
    if cutoff.is_nan() {
        return Err(CrsError::message(
            "geodesic point-to-segment cutoff is not finite",
        ));
    }
    if let Some(along) = exact_segment_contact(geodesic, point, &prepared) {
        let minimum = GeodesicSegmentMinimum::new(0.0, along);
        return if 0.0 < cutoff {
            attained_contact_result(&prepared, minimum, witness, point)
        } else {
            Ok(GeodesicSegmentQueryResult::Excluded)
        };
    }
    if segment.length == 0.0 {
        return if endpoint_minimum.distance < cutoff {
            attained_result(&prepared, endpoint_minimum, witness)
        } else {
            Ok(GeodesicSegmentQueryResult::Excluded)
        };
    }
    if let Some(minimum) = exact_family_minimum(geodesic, point, &prepared, endpoint_minimum)? {
        if minimum.distance < cutoff {
            return attained_result(&prepared, minimum, witness);
        }
        return Ok(GeodesicSegmentQueryResult::Excluded);
    }
    search_point_segment(
        &prepared,
        geodesic,
        point,
        cutoff,
        witness,
        endpoint_minimum,
        left_value,
        right_value,
        scratch,
    )
}

#[derive(Clone, Copy)]
pub(crate) struct PreparedSegment {
    geodesic: Geodesic,
    segment: GeodesicSegment,
    line: GeodesicLine,
}

#[derive(Clone, Copy)]
pub(crate) struct SegmentSample {
    pub(crate) distance: f64,
    pub(crate) derivative: Option<Derivative>,
}

#[derive(Clone, Copy)]
pub(crate) struct Derivative {
    pub(crate) value: f64,
    pub(crate) error: f64,
}

impl PreparedSegment {
    pub(crate) fn new(geodesic: &Geodesic, segment: &GeodesicSegment) -> Self {
        Self {
            geodesic: *geodesic,
            segment: *segment,
            line: cached_geodesic_line_with_caps(geodesic, segment, POINT_SEGMENT_LINE_CAPS),
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
            GeodesicSegmentMinimum::new(left, 0.0)
        } else {
            GeodesicSegmentMinimum::new(right, self.segment.length)
        }
    }

    pub(crate) fn sample(
        &self,
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
            self.geodesic.inverse(lat, lon, point.y, point.x);
        ensure_finite_distance(distance)?;
        let derivative = if derivative {
            let value = if distance > 0.0 && query_azimuth.is_finite() {
                let (angle, correction) = angle_difference(azimuth, query_azimuth);
                let slope = -(angle.to_radians() + correction.to_radians()).cos();
                let angular_error = 128.0 * f64::EPSILON
                    + backend_floor(&self.geodesic) / distance.max(f64::MIN_POSITIVE);
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

    fn witness(&self, minimum: GeodesicSegmentMinimum) -> Result<Point> {
        if !(minimum.distance.is_finite()
            && minimum.along.is_finite()
            && minimum.along >= 0.0
            && minimum.along <= self.segment.length)
        {
            return Err(CrsError::message("geodesic segment witness is not finite"));
        }
        self.position(minimum.along)
    }

    #[expect(
        clippy::float_cmp,
        reason = "the segment endpoints are exact parameter identities; a tolerance would change the returned point"
    )]
    pub(crate) fn position(&self, along: f64) -> Result<Point> {
        if !(along.is_finite() && along >= 0.0 && along <= self.segment.length) {
            return Err(CrsError::message("geodesic segment position is not finite"));
        }
        if along == 0.0 {
            return Ok(self.segment.start);
        }
        if along == self.segment.length {
            return Ok(self.segment.end);
        }
        let (_, lat, lon, ..) = self.line._gen_position(false, along, GEODESIC_LINE_OUTMASK);
        if !(lat.is_finite() && lon.is_finite()) {
            return Err(CrsError::message("geodesic segment position is not finite"));
        }
        let fraction = along / self.segment.length;
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
        )
    }

    pub(crate) fn tangent(&self, along: f64) -> Result<Option<[f64; 3]>> {
        if !(along.is_finite() && along >= 0.0 && along <= self.segment.length) {
            return Err(CrsError::message("geodesic segment tangent is not finite"));
        }
        let (_, lat, lon, azimuth, ..) =
            self.line._gen_position(false, along, GEODESIC_LINE_OUTMASK);
        if !(lat.is_finite() && lon.is_finite() && azimuth.is_finite()) {
            return Err(CrsError::message(
                "geodesic segment tangent position is not finite",
            ));
        }
        Ok(geodesic_tangent_ecef(&self.geodesic, lat, lon, azimuth))
    }

    #[expect(
        clippy::float_cmp,
        reason = "the segment endpoints are exact parameter identities; a tolerance would change the returned point"
    )]
    pub(crate) fn contact_witness(
        &self,
        minimum: GeodesicSegmentMinimum,
        point: Point,
    ) -> Result<Point> {
        let foot = self.witness(minimum)?;
        if minimum.along == 0.0 || minimum.along == self.segment.length {
            return Ok(foot);
        }
        Point::new_axes(point.x, point.y, ZOrdinate(foot.z()), MOrdinate(foot.m()))
    }
}

fn cached_geodesic_line_with_caps(
    geodesic: &Geodesic,
    segment: &GeodesicSegment,
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
        let line = GeodesicLine::new(
            geodesic,
            segment.start.y,
            segment.start.x,
            segment.azimuth0,
            Some(caps),
            None,
            None,
        );
        cache.insert_new(key, line);
        line
    })
}

fn search_point_segment(
    prepared: &PreparedSegment,
    geodesic: &Geodesic,
    point: Point,
    cutoff: f64,
    witness: GeodesicWitnessNeed,
    endpoint_minimum: GeodesicSegmentMinimum,
    left_value: f64,
    right_value: f64,
    scratch: &mut GeodesicSegmentScratch,
) -> Result<GeodesicSegmentQueryResult> {
    let query = GeodesicSegmentQuery::Minimum { cutoff, witness };
    let initial = GeodesicInnerInterval {
        lower: interval_lower_bound(geodesic, left_value, right_value, prepared.segment.length),
        left: 0.0,
        right: prepared.segment.length,
        left_value,
        right_value,
    };

    match query {
        GeodesicSegmentQuery::Minimum { cutoff, .. } if initial.lower >= cutoff => {
            Ok(GeodesicSegmentQueryResult::Excluded)
        },
        GeodesicSegmentQuery::Minimum { .. } => {
            scratch.clear();
            scratch.inner.push(initial);
            let mut minimum = endpoint_minimum;
            let mut lower = 0.0;
            while let Some(interval) = scratch.inner.pop() {
                #[cfg(test)]
                geodesic_counters::segment_interval_pop();
                if minimum.distance >= interval.lower
                    && minimum.distance - interval.lower
                        <= GeodesicNumericalPolicy::new(geodesic.a).minimum_gap()
                {
                    lower = interval.lower;
                    return finish_query(prepared, query, minimum, lower);
                }
                if interval_is_excluded(interval.lower, query, minimum.distance) {
                    break;
                }
                if let Some((closed, closed_lower)) =
                    convex_cell_minimum(prepared, geodesic, point, interval, &mut minimum)?
                {
                    #[cfg(test)]
                    geodesic_counters::segment_convex_closure();
                    update_minimum(&mut minimum, closed.distance, closed.along);
                    lower = lower.max(interval.lower).max(closed_lower);
                    if minimum.distance - lower
                        <= GeodesicNumericalPolicy::new(geodesic.a).minimum_gap()
                    {
                        return finish_query(prepared, query, minimum, lower);
                    }
                    continue;
                }
                if !can_subdivide(interval.left, interval.right) {
                    lower = lower.max(interval.lower);
                    return finish_query(prepared, query, minimum, lower);
                }
                #[cfg(test)]
                geodesic_counters::segment_interval_split();
                let middle = f64::midpoint(interval.left, interval.right);
                let middle_value = prepared.sample(point, middle, false)?;
                update_minimum(&mut minimum, middle_value.distance, middle);
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
                    scratch.inner.push(left);
                }
                if interval_may_contain_answer(right.lower, query, minimum.distance) {
                    scratch.inner.push(right);
                }
                if let Some(next) = scratch.inner.peek()
                    && minimum.distance - next.lower
                        <= GeodesicNumericalPolicy::new(geodesic.a).minimum_gap()
                {
                    lower = next.lower;
                    return finish_query(prepared, query, minimum, lower);
                }
            }
            if minimum.distance.is_finite() && scratch.inner.is_empty() {
                lower = lower.max(minimum.distance);
            }
            finish_query(prepared, query, minimum, lower)
        },
    }
}

fn finish_query(
    prepared: &PreparedSegment,
    query: GeodesicSegmentQuery,
    minimum: GeodesicSegmentMinimum,
    lower: f64,
) -> Result<GeodesicSegmentQueryResult> {
    let minimum = complete_minimum_bound(prepared, minimum, lower);
    match query {
        GeodesicSegmentQuery::Minimum { cutoff, witness } => {
            if minimum.witness.distance < cutoff {
                attained_result_resolved(prepared, minimum, witness)
            } else {
                Ok(GeodesicSegmentQueryResult::Excluded)
            }
        },
    }
}

fn attained_result(
    prepared: &PreparedSegment,
    minimum_witness: GeodesicSegmentMinimum,
    witness: GeodesicWitnessNeed,
) -> Result<GeodesicSegmentQueryResult> {
    attained_result_resolved(
        prepared,
        complete_minimum_bound(prepared, minimum_witness, 0.0),
        witness,
    )
}

fn attained_result_resolved(
    prepared: &PreparedSegment,
    minimum: GeodesicMinimum,
    witness: GeodesicWitnessNeed,
) -> Result<GeodesicSegmentQueryResult> {
    let foot = if matches!(witness, GeodesicWitnessNeed::Yes) {
        Some(prepared.witness(minimum.witness)?)
    } else {
        None
    };
    Ok(GeodesicSegmentQueryResult::Attained { minimum, foot })
}

fn complete_minimum_bound(
    prepared: &PreparedSegment,
    witness: GeodesicSegmentMinimum,
    lower: f64,
) -> GeodesicMinimum {
    let policy = GeodesicNumericalPolicy::new(prepared.geodesic.a);
    let evaluation_error = policy.evaluation_allowance(POINT_SEGMENT_BACKEND_SLACK_MULTIPLIER);
    let numerical_gap = policy.minimum_gap();
    GeodesicMinimum {
        witness,
        lower: lower.max((witness.distance - evaluation_error - numerical_gap).max(0.0)),
        evaluation_error,
    }
}

fn attained_contact_result(
    prepared: &PreparedSegment,
    minimum: GeodesicSegmentMinimum,
    witness: GeodesicWitnessNeed,
    point: Point,
) -> Result<GeodesicSegmentQueryResult> {
    let foot = if matches!(witness, GeodesicWitnessNeed::Yes) {
        Some(prepared.contact_witness(minimum, point)?)
    } else {
        None
    };
    Ok(GeodesicSegmentQueryResult::Attained {
        minimum: GeodesicMinimum::from_witness(minimum),
        foot,
    })
}

fn interval_is_excluded(lower: f64, query: GeodesicSegmentQuery, minimum: f64) -> bool {
    match query {
        GeodesicSegmentQuery::Minimum { cutoff, .. } => lower >= cutoff || lower >= minimum,
    }
}

fn interval_may_contain_answer(lower: f64, query: GeodesicSegmentQuery, minimum: f64) -> bool {
    match query {
        GeodesicSegmentQuery::Minimum { cutoff, .. } => lower < cutoff && lower < minimum,
    }
}

fn convex_cell_minimum(
    prepared: &PreparedSegment,
    geodesic: &Geodesic,
    point: Point,
    interval: GeodesicInnerInterval,
    minimum: &mut GeodesicSegmentMinimum,
) -> Result<Option<(GeodesicSegmentMinimum, f64)>> {
    let width = interval.right - interval.left;
    if !(width > 0.0 && width.is_finite()) {
        return Ok(None);
    }
    let center = f64::midpoint(interval.left, interval.right);
    let center_sample = prepared.sample(point, center, false)?;
    update_minimum(minimum, center_sample.distance, center);
    let half_width = width / 2.0;
    let Some((omega, _, d_star)) = geodesic_curvature_domain(geodesic.a, geodesic._b) else {
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
    let left = prepared.sample(point, interval.left, true)?;
    let right = prepared.sample(point, interval.right, true)?;
    let (Some(left_derivative), Some(right_derivative)) = (left.derivative, right.derivative)
    else {
        return Ok(None);
    };
    if left_derivative.value >= left_derivative.error {
        return Ok(Some((
            GeodesicSegmentMinimum::new(left.distance, interval.left),
            interval.lower,
        )));
    }
    if right_derivative.value <= -right_derivative.error {
        return Ok(Some((
            GeodesicSegmentMinimum::new(right.distance, interval.right),
            interval.lower,
        )));
    }
    if !(left_derivative.value < -left_derivative.error
        && right_derivative.value > right_derivative.error)
    {
        return Ok(None);
    }
    solve_convex_stationary(
        prepared,
        point,
        interval.left,
        left,
        interval.right,
        right,
        c_d,
    )
}

fn solve_convex_stationary(
    prepared: &PreparedSegment,
    point: Point,
    mut left: f64,
    mut left_sample: SegmentSample,
    mut right: f64,
    mut right_sample: SegmentSample,
    curvature: f64,
) -> Result<Option<(GeodesicSegmentMinimum, f64)>> {
    let mut minimum = if left_sample.distance <= right_sample.distance {
        GeodesicSegmentMinimum::new(left_sample.distance, left)
    } else {
        GeodesicSegmentMinimum::new(right_sample.distance, right)
    };
    let mut residual_lower = 0.0;
    while can_subdivide(left, right) {
        let left_derivative = left_sample.derivative.expect("convex endpoint derivative");
        let right_derivative = right_sample.derivative.expect("convex endpoint derivative");
        let middle = stationary_trial(left, left_derivative.value, right, right_derivative.value);
        let middle_sample = prepared.sample(point, middle, true)?;
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
            let derivative_bound = derivative.value.abs() + derivative.error;
            let downward_distance = (minimum.distance
                - POINT_SEGMENT_BACKEND_SLACK_MULTIPLIER * backend_floor(&prepared.geodesic))
            .max(0.0);
            if curvature > 0.0 && derivative_bound.is_finite() {
                residual_lower = (downward_distance * downward_distance
                    - derivative_bound * derivative_bound / curvature)
                    .max(0.0)
                    .sqrt();
            }
            break;
        }
    }
    Ok(Some((minimum, residual_lower)))
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

pub(crate) fn geodesic_curvature_domain(a: f64, b: f64) -> Option<(f64, f64, f64)> {
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
        .then_some(GeodesicSegmentMinimum::new(distance, along))
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
    let start = geodesic_unit_vector(lon, lat);
    let query = geodesic_unit_vector(point.x, point.y);
    let coefficient_a = dot3(query, start);
    let coefficient_b = dot3(query, tangent);
    // The dot product is maximized, and therefore the distance minimized, at
    // atan2(B, A). The antipodal stationary direction belongs to the maximum
    // distance support, not this point-to-segment minimum.
    let phase = coefficient_b.atan2(coefficient_a);
    let mut minimum = endpoint_minimum;
    for shift in -1..=1 {
        let angle = phase + f64::from(shift) * std::f64::consts::TAU;
        if angle <= 0.0 || angle >= theta {
            continue;
        }
        let along = angle * radius;
        let sample = prepared.sample(point, along, false)?;
        update_minimum(&mut minimum, sample.distance, along);
    }
    Ok(Some(minimum))
}

pub(crate) fn geodesic_unit_vector(longitude: f64, latitude: f64) -> [f64; 3] {
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

fn ensure_finite_distance(distance: f64) -> Result<()> {
    if distance.is_finite() && distance >= 0.0 {
        Ok(())
    } else {
        Err(CrsError::message(
            "geodesic point-to-segment evaluation produced a non-finite distance",
        ))
    }
}

fn same_geodesic_vertex(left: Point, right: Point) -> bool {
    let frame = GeographicFrame::crs84();
    frame.quotient_vertex(left.x, left.y) == frame.quotient_vertex(right.x, right.y)
}

fn same_geodesic_longitude(left: f64, right: f64) -> bool {
    let axis = GeographicFrame::crs84().longitude();
    matches!(
        (axis.canonicalize(left), axis.canonicalize(right)),
        (Ok(left), Ok(right)) if left == right
    )
}

fn can_subdivide(left: f64, right: f64) -> bool {
    let middle = f64::midpoint(left, right);
    middle > left && middle < right
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
pub(crate) fn sincos_degrees(value: f64, correction: f64) -> (f64, f64) {
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

#[cfg(test)]
mod longitude_identity_tests {
    use super::same_geodesic_longitude;

    #[test]
    fn geodesic_longitude_identity_is_the_exact_quotient_alias() {
        assert!(same_geodesic_longitude(10.0, 370.0));
        assert!(same_geodesic_longitude(-180.0, 180.0));
        assert!(same_geodesic_longitude(0.0, -0.0));
        // The stored values differ by about 2.3e-14 degrees after one period,
        // although `360.1 - 0.1` rounds to exactly 360 in binary64.
        assert!(!same_geodesic_longitude(0.1, 360.1));
    }
}
