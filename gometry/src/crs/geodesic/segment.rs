#![expect(
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
    GeodesicInnerInterval, GeodesicMinimum, GeodesicNumericalPolicy, GeodesicSegment,
    GeodesicSegmentMinimum, GeodesicSegmentQuery, GeodesicSegmentQueryResult,
    GeodesicSegmentScratch, GeodesicWitnessNeed, MOrdinate, Point, SegmentContact,
    SegmentPairWitness, ZOrdinate, interpolate_optional_ordinate,
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
const INTERSECT_LINE_OUTMASK: u64 =
    caps::LATITUDE | caps::LONGITUDE | caps::AZIMUTH | caps::REDUCEDLENGTH | caps::GEODESICSCALE;

/// Construct a segment only when the selected shortest geodesic is unambiguous.
pub(crate) fn checked_geodesic_segment(
    geodesic: &Geodesic,
    start: Point,
    end: Point,
) -> Result<GeodesicSegment> {
    #[cfg(test)]
    geodesic_counters::segment_admission();
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
    let mut difference = (end - start) % 360.0;
    if difference > 180.0 {
        difference -= 360.0;
    } else if difference < -180.0 {
        difference += 360.0;
    }
    difference
}

/// Resolve an intersection candidate without allowing the candidate
/// generator to own contact semantics.
pub(crate) fn geodesic_segment_contact(
    geodesic: &Geodesic,
    solver: &IntersectSolver,
    first: &GeodesicSegment,
    second: &GeodesicSegment,
) -> Result<SegmentContact> {
    if let Some((first_along, second_along)) =
        exact_structural_segment_contact(geodesic, first, second)
    {
        return Ok(SegmentContact::Contact {
            first_along,
            second_along,
        });
    }

    let Some(candidate) =
        geodesic_segment_intersection_with_solver(geodesic, solver, first, second)?
    else {
        return Ok(SegmentContact::Disjoint);
    };
    if regular_root_inclusion(geodesic, first, second, &candidate)? {
        return Ok(SegmentContact::Contact {
            first_along: candidate.first_along,
            second_along: candidate.second_along,
        });
    }
    if candidate.distance <= GeodesicNumericalPolicy::new(geodesic.a).normalized_minimum_gap() {
        return Ok(SegmentContact::Unresolved { candidate });
    }
    Ok(SegmentContact::Disjoint)
}

fn exact_structural_segment_contact(
    geodesic: &Geodesic,
    first: &GeodesicSegment,
    second: &GeodesicSegment,
) -> Option<(f64, f64)> {
    for (first_along, first_point) in [(0.0, first.start), (first.length, first.end)] {
        for (second_along, second_point) in [(0.0, second.start), (second.length, second.end)] {
            if same_geodesic_vertex(first_point, second_point) {
                return Some((first_along, second_along));
            }
        }
    }

    if is_equatorial_segment(first) && is_equatorial_segment(second) {
        for point in [first.start, first.end, second.start, second.end] {
            if let (Some(first_along), Some(second_along)) = (
                equatorial_parameter(point, first),
                equatorial_parameter(point, second),
            ) {
                return Some((first_along, second_along));
            }
        }
    }

    if is_equatorial_segment(first)
        && is_meridian_segment(second)
        && let Some((first_along, second_along)) = equator_meridian_contact(geodesic, first, second)
    {
        return Some((first_along, second_along));
    }
    if is_meridian_segment(first) && is_equatorial_segment(second) {
        return equator_meridian_contact(geodesic, second, first)
            .map(|(second_along, first_along)| (first_along, second_along));
    }
    if is_meridian_segment(first)
        && is_meridian_segment(second)
        && same_geodesic_longitude(first.start.x, second.start.x)
    {
        let lower = first
            .start
            .y
            .min(first.end.y)
            .max(second.start.y.min(second.end.y));
        let upper = first
            .start
            .y
            .max(first.end.y)
            .min(second.start.y.max(second.end.y));
        if lower <= upper {
            let first_along = meridian_parameter(geodesic, first, lower)?;
            let second_along = meridian_parameter(geodesic, second, lower)?;
            return Some((first_along, second_along));
        }
    }
    None
}

fn is_equatorial_segment(segment: &GeodesicSegment) -> bool {
    segment.start.y == 0.0
        && segment.end.y == 0.0
        && longitude_difference(segment.start.x, segment.end.x).abs() < 180.0
}

#[expect(
    clippy::float_cmp,
    reason = "exact represented meridian membership is structural, not a proximity test"
)]
fn is_meridian_segment(segment: &GeodesicSegment) -> bool {
    same_geodesic_longitude(segment.start.x, segment.end.x) && segment.start.y != segment.end.y
}

fn equatorial_parameter(point: Point, segment: &GeodesicSegment) -> Option<f64> {
    if point.y != 0.0 || !is_equatorial_segment(segment) {
        return None;
    }
    let delta = longitude_difference(segment.start.x, segment.end.x);
    let point_delta = longitude_difference(segment.start.x, point.x);
    ((delta > 0.0 && (0.0..=delta).contains(&point_delta))
        || (delta < 0.0 && (delta..=0.0).contains(&point_delta)))
    .then_some(segment.length * point_delta.abs() / delta.abs())
}

fn equator_meridian_contact(
    geodesic: &Geodesic,
    equator: &GeodesicSegment,
    meridian: &GeodesicSegment,
) -> Option<(f64, f64)> {
    if !(same_geodesic_longitude(meridian.start.x, meridian.end.x)
        && meridian.start.y.min(meridian.end.y) <= 0.0
        && 0.0 <= meridian.start.y.max(meridian.end.y))
    {
        return None;
    }
    let point = Point::new_raw_xy(meridian.start.x, 0.0);
    let equator_along = equatorial_parameter(point, equator)?;
    let meridian_along = meridian_parameter(geodesic, meridian, 0.0)?;
    Some((equator_along, meridian_along))
}

#[expect(
    clippy::float_cmp,
    reason = "exact endpoint parameters preserve represented coordinate identity"
)]
fn meridian_parameter(
    geodesic: &Geodesic,
    segment: &GeodesicSegment,
    latitude: f64,
) -> Option<f64> {
    if !is_meridian_segment(segment)
        || latitude < segment.start.y.min(segment.end.y)
        || latitude > segment.start.y.max(segment.end.y)
    {
        return None;
    }
    if latitude == segment.start.y {
        return Some(0.0);
    }
    if latitude == segment.end.y {
        return Some(segment.length);
    }
    let along: f64 = geodesic.inverse(segment.start.y, segment.start.x, latitude, segment.start.x);
    (along.is_finite() && along >= 0.0 && along <= segment.length).then_some(along)
}

/// A small front-sheet ECEF chart used only to admit a regular transverse
/// numerical root. Its strict inclusion checks are intentionally silent on
/// singular, endpoint, or poorly conditioned candidates.
#[expect(
    clippy::neg_cmp_op_on_partial_ord,
    reason = "the preceding finite checks make this strict interval inclusion explicit"
)]
fn regular_root_inclusion(
    geodesic: &Geodesic,
    first: &GeodesicSegment,
    second: &GeodesicSegment,
    candidate: &SegmentPairWitness,
) -> Result<bool> {
    if !(candidate.first_along > 0.0
        && candidate.first_along < first.length
        && candidate.second_along > 0.0
        && candidate.second_along < second.length)
    {
        return Ok(false);
    }
    let first_line = cached_geodesic_line(geodesic, first)?;
    let second_line = cached_geodesic_line(geodesic, second)?;
    let (_, first_lat, first_lon, first_azimuth, ..) =
        first_line._gen_position(false, candidate.first_along, INTERSECT_LINE_OUTMASK);
    let (_, second_lat, second_lon, second_azimuth, ..) =
        second_line._gen_position(false, candidate.second_along, INTERSECT_LINE_OUTMASK);
    let (Some(first_position), Some(second_position)) = (
        surface_ecef(geodesic, first_lat, first_lon),
        surface_ecef(geodesic, second_lat, second_lon),
    ) else {
        return Ok(false);
    };
    let (Some(first_tangent), Some(second_tangent)) = (
        geodesic_tangent_ecef(geodesic, first_lat, first_lon, first_azimuth),
        geodesic_tangent_ecef(geodesic, second_lat, second_lon, second_azimuth),
    ) else {
        return Ok(false);
    };
    let cross_norm = norm3(cross3(first_tangent, second_tangent));
    if !(cross_norm.is_finite()
        && cross_norm > 1024.0 * f64::EPSILON
        && dot3(first_position, second_position) > 0.0)
    {
        return Ok(false);
    }

    let axis0 = first_tangent;
    let projection = dot3(second_tangent, axis0);
    let axis1_raw = [
        second_tangent[0] - projection * axis0[0],
        second_tangent[1] - projection * axis0[1],
        second_tangent[2] - projection * axis0[2],
    ];
    let axis1_norm = norm3(axis1_raw);
    if !(axis1_norm.is_finite() && axis1_norm > 0.0) {
        return Ok(false);
    }
    let axis1 = [
        axis1_raw[0] / axis1_norm,
        axis1_raw[1] / axis1_norm,
        axis1_raw[2] / axis1_norm,
    ];
    let delta = [
        first_position[0] - second_position[0],
        first_position[1] - second_position[1],
        first_position[2] - second_position[2],
    ];
    let f0 = [dot3(delta, axis0), dot3(delta, axis1)];
    if f0.iter().any(|value| !value.is_finite()) {
        return Ok(false);
    }

    let jacobian = [[1.0, -dot3(second_tangent, axis0)], [
        dot3(axis1, axis0),
        -dot3(second_tangent, axis1),
    ]];
    let determinant = jacobian[0][0] * jacobian[1][1] - jacobian[0][1] * jacobian[1][0];
    if !(determinant.is_finite() && determinant.abs() > 1024.0 * f64::EPSILON) {
        return Ok(false);
    }
    let inverse = [
        [jacobian[1][1] / determinant, -jacobian[0][1] / determinant],
        [-jacobian[1][0] / determinant, jacobian[0][0] / determinant],
    ];

    let scale = first.length.max(second.length).max(1.0);
    let half = 256.0 * f64::EPSILON * scale + 8.0 * GeodesicNumericalPolicy::BACKEND_RELATIVE;
    if !(half.is_finite()
        && candidate.first_along - half > 0.0
        && candidate.first_along + half < first.length
        && candidate.second_along - half > 0.0
        && candidate.second_along + half < second.length)
    {
        return Ok(false);
    }
    let curvature = (1.0 / (1.0 - geodesic.f).abs().max(f64::MIN_POSITIVE)).max(1.0);
    let tangent_radius = curvature * half + 64.0 * f64::EPSILON;
    let position_radius = 64.0 * f64::EPSILON + GeodesicNumericalPolicy::BACKEND_RELATIVE;
    let correction = [
        inverse[0][0] * f0[0] + inverse[0][1] * f0[1],
        inverse[1][0] * f0[0] + inverse[1][1] * f0[1],
    ];
    let mut remainder = [[0.0; 2]; 2];
    for row in 0..2 {
        for column in 0..2 {
            let mut value = if row == column { 1.0 } else { 0.0 };
            value -= inverse[row][0] * jacobian[0][column] + inverse[row][1] * jacobian[1][column];
            remainder[row][column] = value.abs();
        }
    }
    for row in 0..2 {
        let interval_radius = inverse[row][0].abs() * position_radius
            + inverse[row][1].abs() * position_radius
            + (inverse[row][0].abs() + inverse[row][1].abs()) * tangent_radius * half;
        let image_radius = remainder[row][0] * half + remainder[row][1] * half + interval_radius;
        if !(correction[row].abs() + image_radius < half) {
            return Ok(false);
        }
    }
    Ok(true)
}

fn surface_ecef(geodesic: &Geodesic, latitude: f64, longitude: f64) -> Option<[f64; 3]> {
    let phi = latitude.to_radians();
    let lambda = longitude.to_radians();
    let (sin_phi, cos_phi) = phi.sin_cos();
    let (sin_lambda, cos_lambda) = lambda.sin_cos();
    let e2 = geodesic.f * (2.0 - geodesic.f);
    let denominator = (1.0 - e2 * sin_phi * sin_phi).sqrt();
    if !(denominator.is_finite() && denominator > 0.0) {
        return None;
    }
    let radius = 1.0 / denominator;
    Some([
        radius * cos_phi * cos_lambda,
        radius * cos_phi * sin_lambda,
        radius * (1.0 - e2) * sin_phi,
    ])
}

fn cross3(left: [f64; 3], right: [f64; 3]) -> [f64; 3] {
    [
        left[1] * right[2] - left[2] * right[1],
        left[2] * right[0] - left[0] * right[2],
        left[0] * right[1] - left[1] * right[0],
    ]
}

fn norm3(value: [f64; 3]) -> f64 {
    value[0].hypot(value[1]).hypot(value[2])
}

pub(crate) fn geodesic_segment_intersection_with_solver(
    geodesic: &Geodesic,
    solver: &IntersectSolver,
    first: &GeodesicSegment,
    second: &GeodesicSegment,
) -> Result<Option<SegmentPairWitness>> {
    let line_x = cached_geodesic_line(geodesic, first)?;
    let line_y = cached_geodesic_line(geodesic, second)?;
    let mut mode = 0;
    let q = solver.segment(&line_x, first.length, &line_y, second.length, &mut mode)?;
    let first_along = q.x.clamp(0.0, first.length);
    let second_along = q.y.clamp(0.0, second.length);
    let first_point = materialize_segment_point(&line_x, first, first_along)?;
    let second_point = materialize_segment_point(&line_y, second, second_along)?;
    let distance: f64 =
        geodesic.inverse(first_point.y, first_point.x, second_point.y, second_point.x);
    if !(first_along.is_finite()
        && second_along.is_finite()
        && first_point.x.is_finite()
        && first_point.y.is_finite()
        && second_point.x.is_finite()
        && second_point.y.is_finite()
        && distance.is_finite())
    {
        return Ok(None);
    }
    let _ = mode;
    Ok(Some(SegmentPairWitness {
        first: first_point,
        second: second_point,
        first_along,
        second_along,
        distance,
    }))
}

/// Run the one point-to-segment owner for a value, threshold, or witness query.
///
/// The interval queue owns only parameter cells. It never returns a pruning
/// bound as a distance: an attained result carries its value and optional foot,
/// while exclusion and threshold-outside state have distinct variants.
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
    let prepared = PreparedSegment::new(geodesic, &segment)?;
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
            if let Some(minimum) =
                exact_family_minimum(geodesic, point, &prepared, endpoint_minimum)?
            {
                if minimum.distance < cutoff {
                    return attained_result(&prepared, minimum, witness);
                }
                return Ok(GeodesicSegmentQueryResult::Excluded);
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
        GeodesicSegmentQuery::Threshold { limit, witness } => {
            if limit.is_nan() {
                return Err(CrsError::message("geodesic dwithin limit is not finite"));
            }
            if limit < 0.0 || limit.is_infinite() && limit.is_sign_negative() {
                return Ok(GeodesicSegmentQueryResult::ThresholdOutside);
            }
            if let Some(along) = exact_segment_contact(geodesic, point, &prepared) {
                return attained_contact_result(
                    &prepared,
                    GeodesicSegmentMinimum::new(0.0, along),
                    witness,
                    point,
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
                return Ok(GeodesicSegmentQueryResult::ThresholdOutside);
            }
            if let Some(minimum) =
                exact_family_minimum(geodesic, point, &prepared, endpoint_minimum)?
            {
                return if minimum.distance <= limit {
                    attained_result(&prepared, minimum, witness)
                } else {
                    Ok(GeodesicSegmentQueryResult::ThresholdOutside)
                };
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
    pub(crate) fn new(geodesic: &Geodesic, segment: &GeodesicSegment) -> Result<Self> {
        Ok(Self {
            geodesic: *geodesic,
            segment: *segment,
            line: cached_point_segment_line(geodesic, segment)?,
        })
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

pub(crate) fn cached_geodesic_line(
    geodesic: &Geodesic,
    segment: &GeodesicSegment,
) -> Result<GeodesicLine> {
    cached_geodesic_line_with_caps(geodesic, segment, GEODESIC_LINE_CAPS)
}

fn cached_point_segment_line(
    geodesic: &Geodesic,
    segment: &GeodesicSegment,
) -> Result<GeodesicLine> {
    cached_geodesic_line_with_caps(geodesic, segment, POINT_SEGMENT_LINE_CAPS)
}

fn cached_geodesic_line_with_caps(
    geodesic: &Geodesic,
    segment: &GeodesicSegment,
    caps: u64,
) -> Result<GeodesicLine> {
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
            return Ok(*line);
        }
        count_line_cache_miss!();
        if cache.len() == GEODESIC_LINE_CACHE_CAPACITY {
            if let Some(evict) = cache.keys().next().copied() {
                cache.remove(&evict);
            }
        } else {
            cache.try_reserve(1).map_err(crate::error::Error::from)?;
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
        // The miss path reserved one map slot above, or removed an entry from
        // a full cache, so insertion cannot grow the map.
        cache.insert(key, line);
        Ok(line)
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
            Ok(GeodesicSegmentQueryResult::Excluded)
        },
        GeodesicSegmentQuery::Threshold { limit, .. } if initial.lower > limit => {
            Ok(GeodesicSegmentQueryResult::ThresholdOutside)
        },
        _ => {
            scratch.clear();
            crate::boundary::try_push_heap(&mut scratch.inner, initial)?;
            let mut minimum = endpoint_minimum;
            let mut lower = 0.0;
            while let Some(interval) = scratch.inner.pop() {
                #[cfg(test)]
                geodesic_counters::segment_interval_pop();
                if minimum.distance >= interval.lower
                    && minimum.distance - interval.lower
                        <= GeodesicNumericalPolicy::new(geodesic.a).normalized_minimum_gap()
                {
                    lower = interval.lower;
                    return finish_query(prepared, query, minimum, lower);
                }
                if interval_is_excluded(interval.lower, query, minimum.distance) {
                    if matches!(query, GeodesicSegmentQuery::Minimum { .. }) {
                        break;
                    }
                    continue;
                }
                if let Some((closed, closed_lower)) =
                    convex_cell_minimum(prepared, geodesic, point, interval, &mut minimum)?
                {
                    #[cfg(test)]
                    geodesic_counters::segment_convex_closure();
                    update_minimum(&mut minimum, closed.distance, closed.along);
                    lower = lower.max(interval.lower).max(closed_lower);
                    if let GeodesicSegmentQuery::Threshold { limit, .. } = query
                        && minimum.distance <= limit
                    {
                        return attained_result(prepared, minimum, query_witness(query));
                    }
                    if minimum.distance - lower
                        <= GeodesicNumericalPolicy::new(geodesic.a).normalized_minimum_gap()
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
                if let GeodesicSegmentQuery::Threshold { limit, .. } = query
                    && minimum.distance <= limit
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
                if let Some(next) = scratch.inner.peek()
                    && minimum.distance - next.lower
                        <= GeodesicNumericalPolicy::new(geodesic.a).normalized_minimum_gap()
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
        GeodesicSegmentQuery::Threshold { limit, witness } => {
            if minimum.witness.distance <= limit {
                attained_result_resolved(prepared, minimum, witness)
            } else {
                Ok(GeodesicSegmentQueryResult::ThresholdResolvedOutside {
                    minimum: minimum.witness.distance,
                })
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

const fn query_witness(query: GeodesicSegmentQuery) -> GeodesicWitnessNeed {
    match query {
        GeodesicSegmentQuery::Minimum { witness, .. }
        | GeodesicSegmentQuery::Threshold { witness, .. } => witness,
    }
}

fn complete_minimum_bound(
    prepared: &PreparedSegment,
    witness: GeodesicSegmentMinimum,
    lower: f64,
) -> GeodesicMinimum {
    let policy = GeodesicNumericalPolicy::new(prepared.geodesic.a);
    let evaluation_error = policy.evaluation_allowance(POINT_SEGMENT_BACKEND_SLACK_MULTIPLIER);
    let numerical_gap = policy.normalized_minimum_gap();
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
) -> Result<Option<(GeodesicSegmentMinimum, f64)>> {
    let width = interval.right - interval.left;
    if !(width > 0.0 && width.is_finite()) {
        return Ok(None);
    }
    let center = f64::midpoint(interval.left, interval.right);
    let center_sample = prepared.sample(point, center, false)?;
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

fn materialize_segment_point(
    line: &GeodesicLine,
    segment: &GeodesicSegment,
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
    if left == right || (left - right).abs() == 360.0 {
        return true;
    }
    let (difference, correction) = angle_difference(left, right);
    difference == 0.0 && correction == 0.0
}

fn can_subdivide(left: f64, right: f64) -> bool {
    let middle = f64::midpoint(left, right);
    middle > left && middle < right
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
