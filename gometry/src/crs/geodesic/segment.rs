#![allow(clippy::similar_names, reason = "latitude, longitude and azimuth triples are named by axis and endpoint")]
use std::cmp::Ordering;
use std::collections::BinaryHeap;

use geographiclib_rs::{Geodesic, GeodesicLine, InverseGeodesic as _, geodesic_capability as caps};

use crate::crs::CrsError;
#[cfg(test)]
use crate::crs::geodesic::geodesic_counters;
use crate::crs::geodesic::{
    GEODESIC_LINE_CACHE, GEODESIC_LINE_CACHE_CAPACITY, GEODESIC_LINE_CAPS, GEODESIC_LINE_OUTMASK,
    GeodesicLineKey, inverse_distance,
};
use crate::error::Result;
use crate::geometry::{GeodesicSegment, GeodesicSegmentWitness, MOrdinate, Point, ZOrdinate};

const POINT_SEGMENT_TOLERANCE_METRES: f64 = 1e-3;
const POINT_SEGMENT_BOUND_ROUNDING_SLACK_METRES: f64 = 1e-6;
const POINT_SEGMENT_CONTACT_TOLERANCE_METRES: f64 = 1e-6;
const POINT_SEGMENT_CONTACT_ANGLE_TOLERANCE_RADIANS: f64 = 1e-12;
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

#[expect(clippy::float_cmp, reason = "a pole or antimeridian is one specific representable coordinate, and the question is whether a vertex IS that coordinate; a tolerance would admit vertices merely near it")]
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

#[expect(clippy::large_types_passed_by_value, reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow")]
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
    let first_point = geodesic_segment_witness_at(geodesic, first, 0.0, q.x)?.foot;
    let second_point = geodesic_segment_witness_at(geodesic, second, 0.0, q.y)?.foot;
    Ok(Some((first_point, second_point)))
}

/// Geodesic distance from `point` to the selected shortest geodesic segment.
/// The interval lower bound is the one-dimensional 1-Lipschitz certificate;
/// no derivative or unimodality assumption is used.
#[expect(
    clippy::large_types_passed_by_value,
    reason = "the segment is handed on by value to the kernel that consumes it"
)]
pub(crate) fn geodesic_point_to_segment(
    geodesic: &Geodesic,
    point: Point,
    segment: GeodesicSegment,
    best: f64,
) -> Result<f64> {
    Ok(geodesic_segment_minimum(geodesic, point, segment, best)?.distance)
}

#[expect(clippy::large_types_passed_by_value, reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow")]
/// Certify whether `point` is within `limit` metres of `segment`.
///
/// The distance-valued kernel may stop within its reporting tolerance once it
/// cannot improve `best`; that is appropriate for minimization, but not for a
/// threshold predicate. This path refines every live 1-Lipschitz interval and
/// returns `false` only when the interval lower bounds all exceed `limit`.
pub(crate) fn geodesic_point_within_segment(
    geodesic: &Geodesic,
    point: Point,
    segment: GeodesicSegment,
    limit: f64,
) -> Result<bool> {
    if limit.is_nan() {
        return Err(CrsError::message("geodesic dwithin limit is not finite"));
    }
    if limit < 0.0 {
        return Ok(false);
    }
    if limit.is_infinite() {
        return Ok(limit.is_sign_positive());
    }
    if !segment.length.is_finite() || segment.length < 0.0 {
        return Err(CrsError::message("geodesic segment length is not finite"));
    }
    if point_on_geodesic_segment(geodesic, point, segment).is_some() {
        return Ok(true);
    }
    let line = cached_geodesic_line(geodesic, segment);
    let left_value = inverse_distance(geodesic, point.x, point.y, segment.start.x, segment.start.y);
    let right_value = if segment.length == 0.0 {
        left_value
    } else {
        inverse_distance(geodesic, point.x, point.y, segment.end.x, segment.end.y)
    };
    ensure_finite_distance(left_value)?;
    ensure_finite_distance(right_value)?;
    if left_value <= limit || right_value <= limit {
        return Ok(true);
    }
    if segment.length == 0.0 {
        return Ok(false);
    }
    let initial = Interval {
        lower: interval_lower_bound(left_value, right_value, segment.length),
        left: 0.0,
        right: segment.length,
        left_value,
        right_value,
    };
    if initial.lower > limit {
        return Ok(false);
    }
    let mut live = BinaryHeap::from([initial]);
    while let Some(interval) = live.pop() {
        if interval.lower > limit {
            return Ok(false);
        }
        let middle = f64::midpoint(interval.left, interval.right);
        if !(middle > interval.left && middle < interval.right) {
            return Err(CrsError::message(
                "geodesic dwithin proof interval cannot be subdivided",
            ));
        }
        let middle_value = line_distance(geodesic, &line, point, middle)?;
        ensure_finite_distance(middle_value)?;
        if middle_value <= limit {
            return Ok(true);
        }
        let left = Interval {
            lower: interval_lower_bound(interval.left_value, middle_value, middle - interval.left),
            left: interval.left,
            right: middle,
            left_value: interval.left_value,
            right_value: middle_value,
        };
        let right = Interval {
            lower: interval_lower_bound(
                middle_value,
                interval.right_value,
                interval.right - middle,
            ),
            left: middle,
            right: interval.right,
            left_value: middle_value,
            right_value: interval.right_value,
        };
        if left.lower <= limit {
            live.push(left);
        }
        if right.lower <= limit {
            live.push(right);
        }
    }
    Ok(false)
}

#[expect(
    clippy::large_types_passed_by_value,
    reason = "the segment is handed on by value to the kernel that consumes it"
)]
pub(crate) fn geodesic_locate_on_segment(
    geodesic: &Geodesic,
    point: Point,
    segment: GeodesicSegment,
    best: f64,
) -> Result<(f64, f64)> {
    let minimum = geodesic_segment_minimum(geodesic, point, segment, best)?;
    Ok((minimum.distance, minimum.along))
}

#[expect(clippy::large_types_passed_by_value, reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow")]
pub(crate) fn geodesic_foot_on_segment(
    geodesic: &Geodesic,
    point: Point,
    segment: GeodesicSegment,
    best: f64,
) -> Result<GeodesicSegmentWitness> {
    if let Some(along) = point_on_geodesic_segment(geodesic, point, segment) {
        return Ok(GeodesicSegmentWitness {
            distance: 0.0,
            foot: point,
            along,
        });
    }
    let minimum = geodesic_segment_minimum(geodesic, point, segment, best)?;
    geodesic_segment_witness_at(geodesic, segment, minimum.distance, minimum.along)
}

#[expect(clippy::large_types_passed_by_value, reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow")]
pub(crate) fn cached_geodesic_line(geodesic: &Geodesic, segment: GeodesicSegment) -> GeodesicLine {
    let key = GeodesicLineKey {
        semi_major_bits: geodesic.a.to_bits(),
        flattening_bits: geodesic.f.to_bits(),
        start_lon_bits: segment.start.x.to_bits(),
        start_lat_bits: segment.start.y.to_bits(),
        azimuth_bits: segment.azimuth0.to_bits(),
        caps: GEODESIC_LINE_CAPS,
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
            Some(GEODESIC_LINE_CAPS),
            None,
            None,
        );
        cache.insert(key, line);
        line
    })
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct GeodesicSegmentMinimum {
    pub(crate) distance: f64,
    pub(crate) along: f64,
}

#[derive(Clone, Copy, Debug)]
struct Interval {
    lower: f64,
    left: f64,
    right: f64,
    left_value: f64,
    right_value: f64,
}

impl PartialEq for Interval {
    fn eq(&self, other: &Self) -> bool {
        self.lower.to_bits() == other.lower.to_bits()
            && self.left.to_bits() == other.left.to_bits()
            && self.right.to_bits() == other.right.to_bits()
    }
}

impl Eq for Interval {}

impl PartialOrd for Interval {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Interval {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap is a max-heap; invert the bound so the live interval with
        // the smallest lower bound is always processed first.
        other
            .lower
            .total_cmp(&self.lower)
            .then_with(|| other.left.total_cmp(&self.left))
            .then_with(|| other.right.total_cmp(&self.right))
    }
}

#[expect(
    clippy::large_types_passed_by_value,
    reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
)]
pub(crate) fn geodesic_segment_minimum(
    geodesic: &Geodesic,
    point: Point,
    segment: GeodesicSegment,
    best: f64,
) -> Result<GeodesicSegmentMinimum> {
    if !segment.length.is_finite() || segment.length < 0.0 {
        return Err(CrsError::message("geodesic segment length is not finite"));
    }
    if let Some(along) = point_on_geodesic_segment(geodesic, point, segment) {
        return Ok(GeodesicSegmentMinimum {
            distance: 0.0,
            along,
        });
    }
    let line = cached_geodesic_line(geodesic, segment);
    let left_value = inverse_distance(geodesic, point.x, point.y, segment.start.x, segment.start.y);
    let right_value = if segment.length == 0.0 {
        left_value
    } else {
        inverse_distance(geodesic, point.x, point.y, segment.end.x, segment.end.y)
    };
    ensure_finite_distance(left_value)?;
    ensure_finite_distance(right_value)?;
    let mut minimum = if left_value <= right_value {
        GeodesicSegmentMinimum {
            distance: left_value,
            along: 0.0,
        }
    } else {
        GeodesicSegmentMinimum {
            distance: right_value,
            along: segment.length,
        }
    };
    if segment.length == 0.0 {
        return Ok(minimum);
    }

    let initial = Interval {
        lower: interval_lower_bound(left_value, right_value, segment.length),
        left: 0.0,
        right: segment.length,
        left_value,
        right_value,
    };
    if initial.lower >= best {
        return Ok(minimum);
    }
    let mut live = BinaryHeap::from([initial]);
    while let Some(interval) = live.pop() {
        if minimum.distance - interval.lower <= POINT_SEGMENT_TOLERANCE_METRES {
            break;
        }
        // The heap is ordered by lower bound, so this interval's bound is the
        // smallest still live: no unexplored arc can reach below `best`, and
        // `best` is the caller's standing minimum. Any value at or above it is
        // discarded by the caller, and one below it has already been proved
        // within tolerance by the discard rule. Bounding entry alone left the
        // search refining a minimum nobody would read.
        //
        // Like the tolerance break above, this leaves arcs unexplored, so the
        // reported `along` is the smallest among sampled positions attaining
        // the minimum rather than the smallest attaining it anywhere. That is
        // the witness contract both exits have always had.
        if interval.lower >= best {
            break;
        }
        let middle = f64::midpoint(interval.left, interval.right);
        if !(middle > interval.left && middle < interval.right) {
            return Err(CrsError::message(
                "geodesic point-to-segment proof interval cannot be subdivided",
            ));
        }
        let middle_value = line_distance(geodesic, &line, point, middle)?;
        ensure_finite_distance(middle_value)?;
        update_minimum(
            &mut minimum,
            middle_value,
            middle,
            POINT_SEGMENT_TOLERANCE_METRES,
        );

        let left = Interval {
            lower: interval_lower_bound(interval.left_value, middle_value, middle - interval.left),
            left: interval.left,
            right: middle,
            left_value: interval.left_value,
            right_value: middle_value,
        };
        let right = Interval {
            lower: interval_lower_bound(
                middle_value,
                interval.right_value,
                interval.right - middle,
            ),
            left: middle,
            right: interval.right,
            left_value: middle_value,
            right_value: interval.right_value,
        };
        if left.lower < minimum.distance - POINT_SEGMENT_TOLERANCE_METRES {
            live.push(left);
        }
        if right.lower < minimum.distance - POINT_SEGMENT_TOLERANCE_METRES {
            live.push(right);
        }
    }
    Ok(minimum)
}

fn line_distance(
    geodesic: &Geodesic,
    line: &GeodesicLine,
    point: Point,
    along: f64,
) -> Result<f64> {
    let (_, lat, lon, ..) = line._gen_position(false, along, GEODESIC_LINE_OUTMASK);
    if !(lat.is_finite() && lon.is_finite()) {
        return Err(CrsError::message(
            "geodesic line evaluation produced a non-finite position",
        ));
    }
    Ok(inverse_distance(geodesic, point.x, point.y, lon, lat))
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

#[expect(clippy::large_types_passed_by_value, reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow")]
fn point_on_geodesic_segment(
    geodesic: &Geodesic,
    point: Point,
    segment: GeodesicSegment,
) -> Option<f64> {
    let (from_start, start_azimuth, ..): (f64, f64, f64, f64) =
        geodesic.inverse(segment.start.y, segment.start.x, point.y, point.x);
    let (to_end, ..): (f64, f64, f64, f64) =
        geodesic.inverse(point.y, point.x, segment.end.y, segment.end.x);
    (from_start.is_finite()
        && to_end.is_finite()
        && from_start >= 0.0
        && from_start <= segment.length
        && (from_start + to_end - segment.length).abs() <= POINT_SEGMENT_CONTACT_TOLERANCE_METRES
        && angle_difference_radians(segment.azimuth0, start_azimuth).abs()
            <= POINT_SEGMENT_CONTACT_ANGLE_TOLERANCE_RADIANS)
        .then_some(from_start)
}

fn interval_lower_bound(left: f64, right: f64, width: f64) -> f64 {
    (0.5 * (left + right - width) - POINT_SEGMENT_BOUND_ROUNDING_SLACK_METRES)
        .next_down()
        .max(0.0)
}

fn update_minimum(minimum: &mut GeodesicSegmentMinimum, distance: f64, along: f64, tolerance: f64) {
    if distance < minimum.distance - tolerance
        || ((distance - minimum.distance).abs() <= tolerance && along < minimum.along)
    {
        minimum.distance = distance;
        minimum.along = along;
    }
}

#[expect(
    clippy::large_types_passed_by_value,
    reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
)]
pub(crate) fn geodesic_segment_witness_at(
    geodesic: &Geodesic,
    segment: GeodesicSegment,
    distance: f64,
    along: f64,
) -> Result<GeodesicSegmentWitness> {
    if !(distance.is_finite() && along.is_finite()) {
        return Err(CrsError::message("geodesic segment witness is not finite"));
    }
    let foot = if along <= 0.0 {
        segment.start
    } else if along >= segment.length {
        segment.end
    } else {
        let fraction = along / segment.length;
        let line = cached_geodesic_line(geodesic, segment);
        let (_, lat, lon, ..) = line._gen_position(false, along, GEODESIC_LINE_OUTMASK);
        if !(lat.is_finite() && lon.is_finite()) {
            return Err(CrsError::message(
                "geodesic segment witness position is not finite",
            ));
        }
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
        )?
    };
    Ok(GeodesicSegmentWitness {
        distance,
        foot,
        along,
    })
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
            t4 = Self::polarb(geodesic, radius, d, tol);
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

    fn polarb(geodesic: &Geodesic, radius: f64, d: f64, tol: f64) -> f64 {
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
        let _ = radius;
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

    #[expect(clippy::float_cmp, reason = "a pole or antimeridian is one specific representable coordinate, and the question is whether a vertex IS that coordinate; a tolerance would admit vertices merely near it")]
    fn distoblique(geodesic: &Geodesic, radius: f64, d: f64, tol: f64) -> Result<f64> {
        if geodesic.f == 0.0 {
            return Ok(d);
        }
        let mut azi0 = 46.0;
        let mut ds0 = 0.0;
        let mut s0 = Self::conjdist_static(geodesic, radius, tol, azi0, &mut ds0)?;
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
            s0 = s1;
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
        let _ = s0;
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

#[expect(clippy::float_cmp, reason = "a pole or antimeridian is one specific representable coordinate, and the question is whether a vertex IS that coordinate; a tolerance would admit vertices merely near it")]
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

#[expect(clippy::float_cmp, reason = "a pole or antimeridian is one specific representable coordinate, and the question is whether a vertex IS that coordinate; a tolerance would admit vertices merely near it")]
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
    } else { i32::from(value > limit) }
}
