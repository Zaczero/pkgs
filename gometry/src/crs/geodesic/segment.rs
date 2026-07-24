#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use geographiclib_rs::Geodesic;

use super::*;
use crate::geometry::{GeodesicSegment, GeodesicSegmentWitness, MOrdinate, Point, ZOrdinate};

/// Whether geodesic segments `a`–`b` and `c`–`d` properly cross, via the
/// orientation test: `c` and `d` must lie on opposite sides of geodesic `a`–`b`
/// and `a`, `b` on opposite sides of `c`–`d`. "Side" is the sign of the signed
/// azimuth difference at the segment start, which `geographiclib` resolves
/// correctly across the antimeridian and poles.
pub(crate) fn geodesic_segments_cross(
    geodesic: &Geodesic,
    a: Point,
    b: Point,
    c: Point,
    d: Point,
) -> bool {
    let side = |from: Point, to: Point, query: Point| -> f64 {
        let (azimuth_to, _) = geo_azimuths(geodesic, from, to);
        let (azimuth_query, _) = geo_azimuths(geodesic, from, query);
        // Signed angle in [-180, 180): positive left of `from->to`, negative
        // right. The azimuths are in [-180, 180], so the difference sits in
        // [-360, 360] and one conditional re-center replaces the float
        // `rem_euclid` (a soft-fmod libm call at the x86-64-v2 baseline).
        let delta = azimuth_query - azimuth_to;
        if delta >= 180.0 {
            delta - 360.0
        } else if delta < -180.0 {
            delta + 360.0
        } else {
            delta
        }
    };
    let opposite = |x: f64, y: f64| x != 0.0 && y != 0.0 && (x < 0.0) != (y < 0.0);
    opposite(side(a, b, c), side(a, b, d)) && opposite(side(c, d, a), side(c, d, b))
}

/// Geodesic distance (meters) from `point` to the geodesic segment `a`–`b`.
///
/// The distance along the segment is a smooth, unimodal function of the
/// along-track position for any point off the segment's antipode. Endpoint
/// probes bracket the interior minimum; a cross-track-seeded Newton iteration
/// (using `geographiclib`'s reduced length and geodesic scale) finds the foot
/// in a handful of inverses, with golden-section as the unconditional fallback
/// for hard brackets (near-conjugate, non-finite derivatives). All longitude
/// wrapping — including the antimeridian — and the poles are handled by
/// `geographiclib`'s own `inverse`/`direct`.
///
/// `best` is the smallest distance found across all pairs so far: the segment
/// distance is bounded below by `max(d_a, d_b) − segment_length` (triangle
/// inequality), so when that bound is already `>= best` the costly along-track
/// search is skipped and the cheaper `min(d_a, d_b)` endpoint distance
/// returned.
pub(crate) fn geodesic_point_to_segment(
    geodesic: &Geodesic,
    point: Point,
    segment: GeodesicSegment,
    best: f64,
) -> f64 {
    geodesic_segment_minimum(geodesic, point, segment, best).distance
}

pub(crate) fn geodesic_locate_on_segment(
    geodesic: &Geodesic,
    point: Point,
    segment: GeodesicSegment,
    best: f64,
) -> (f64, f64) {
    let minimum = geodesic_segment_minimum(geodesic, point, segment, best);
    (minimum.distance, minimum.along)
}

/// Minimum geodesic distance from `point` to segment `a`–`b` **and** the
/// along-track distance (meters from `a`) at which it occurs — the latter is
/// the linear-referencing "locate" position of the perpendicular foot.
///
/// Solved by cross-track-seeded Newton on the along-track parameter (unimodal
/// for realistic edges), bracketed by endpoint derivative signs, with
/// branch-and-bound pruning. Golden-section is the fallback when Newton cannot
/// maintain a valid bracket (near-conjugate segments, non-finite derivatives).
pub(crate) fn geodesic_foot_on_segment(
    geodesic: &Geodesic,
    point: Point,
    segment: GeodesicSegment,
    best: f64,
) -> GeodesicSegmentWitness {
    let minimum = geodesic_segment_minimum(geodesic, point, segment, best);
    geodesic_segment_witness_at(geodesic, segment, minimum.distance, minimum.along)
}

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
    pub(crate) used_golden_fallback: bool,
}

#[derive(Clone, Copy)]
pub(crate) struct EndpointProbe {
    pub(crate) distance: f64,
    /// `-cos` of the angle between the segment direction and the geodesic
    /// toward the probe point — the cross-track component that seeds
    /// Newton.
    derivative: f64,
    /// Reduced length (`m12`) from the endpoint inverse, meters.
    reduced_length: f64,
}

#[derive(Clone, Copy)]
pub(crate) struct NewtonProbe {
    distance: f64,
    derivative: f64,
    m12: f64,
    m21: f64,
}

pub(crate) fn geodesic_segment_witness_at(
    geodesic: &Geodesic,
    segment: GeodesicSegment,
    distance: f64,
    along: f64,
) -> GeodesicSegmentWitness {
    // Materialize the foot on the ellipsoid: endpoints return the endpoint
    // vertex exactly (carrying its Z/M); an interior winner is placed by
    // `direct` from the segment start with Z/M interpolated by arc fraction —
    // never by linear lon/lat, so the antimeridian and poles stay correct.
    let foot = if along <= 0.0 {
        segment.start
    } else if along >= segment.length {
        segment.end
    } else {
        let fraction = along / segment.length;
        let line = cached_geodesic_line(geodesic, segment);
        let (_, lat, lon, ..) = line._gen_position(false, along, GEODESIC_LINE_OUTMASK);
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
        .unwrap_or(if fraction < 0.5 {
            segment.start
        } else {
            segment.end
        })
    };
    GeodesicSegmentWitness {
        distance,
        foot,
        along,
    }
}

pub(crate) fn geodesic_segment_minimum(
    geodesic: &Geodesic,
    point: Point,
    segment: GeodesicSegment,
    best: f64,
) -> GeodesicSegmentMinimum {
    let endpoint_a = endpoint_probe(geodesic, point, segment.start, segment.azimuth0);
    let endpoint_b = endpoint_probe(geodesic, point, segment.end, segment.azimuth1);
    count_endpoint_inverses!(2);
    let endpoint_minimum =
        nearer_endpoint(endpoint_a.distance, endpoint_b.distance, segment.length);
    if !segment.length.is_finite() || segment.length == 0.0 {
        return endpoint_minimum;
    }

    if endpoint_a.distance.max(endpoint_b.distance) - segment.length >= best {
        return endpoint_minimum;
    }
    if !endpoint_a.derivative.is_finite() || !endpoint_b.derivative.is_finite() {
        return geodesic_segment_minimum_golden(geodesic, point, segment, endpoint_minimum);
    }
    if endpoint_a.distance.min(endpoint_b.distance) > std::f64::consts::PI * geodesic.a * 0.49 {
        return geodesic_segment_minimum_golden(geodesic, point, segment, endpoint_minimum);
    }
    if endpoint_a.derivative >= 0.0 {
        return GeodesicSegmentMinimum {
            distance: endpoint_a.distance,
            along: 0.0,
            used_golden_fallback: false,
        };
    }
    if endpoint_b.derivative <= 0.0 {
        return GeodesicSegmentMinimum {
            distance: endpoint_b.distance,
            along: segment.length,
            used_golden_fallback: false,
        };
    }
    if endpoint_a.derivative < 0.0
        && endpoint_b.derivative > 0.0
        && let Some(minimum) = geodesic_segment_minimum_newton(
            geodesic,
            point,
            segment,
            endpoint_a,
            endpoint_b,
            endpoint_minimum,
        )
    {
        return minimum;
    }
    geodesic_segment_minimum_golden(geodesic, point, segment, endpoint_minimum)
}

pub(crate) fn nearer_endpoint(
    distance_a: f64,
    distance_b: f64,
    segment_length: f64,
) -> GeodesicSegmentMinimum {
    if distance_a <= distance_b {
        GeodesicSegmentMinimum {
            distance: distance_a,
            along: 0.0,
            used_golden_fallback: false,
        }
    } else {
        GeodesicSegmentMinimum {
            distance: distance_b,
            along: segment_length,
            used_golden_fallback: false,
        }
    }
}

pub(crate) fn endpoint_probe(
    geodesic: &Geodesic,
    point: Point,
    target: Point,
    segment_azimuth: f64,
) -> EndpointProbe {
    let (distance, _azi_p, azi2_pq, m12, _m12_scale, _m21, _a12): (
        f64,
        f64,
        f64,
        f64,
        f64,
        f64,
        f64,
    ) = geo_inverse(geodesic, point, target);
    let delta = angle_difference_radians(segment_azimuth, away_from_probe_azimuth(azi2_pq));
    EndpointProbe {
        distance,
        derivative: -delta.cos(),
        reduced_length: m12,
    }
}

/// Initial along-track position (meters from `a`) from endpoint cross-track
/// projections; averaged and clamped into `[0, segment.length]`.
pub(crate) fn cross_track_seed_along(
    segment_length: f64,
    endpoint_a: EndpointProbe,
    endpoint_b: EndpointProbe,
) -> f64 {
    let from_a = endpoint_a.reduced_length * endpoint_a.derivative;
    let from_b = segment_length - endpoint_b.reduced_length * endpoint_b.derivative;
    f64::midpoint(from_a, from_b).clamp(0.0, segment_length)
}

pub(crate) fn geodesic_segment_minimum_newton(
    geodesic: &Geodesic,
    point: Point,
    segment: GeodesicSegment,
    endpoint_a: EndpointProbe,
    endpoint_b: EndpointProbe,
    mut best_minimum: GeodesicSegmentMinimum,
) -> Option<GeodesicSegmentMinimum> {
    let line = cached_geodesic_line(geodesic, segment);
    let (mut lo, mut hi) = (0.0, segment.length);
    let mut along = cross_track_seed_along(segment.length, endpoint_a, endpoint_b);
    let mut previous_abs_derivative = f64::INFINITY;
    let mut previous_distance = f64::INFINITY;
    for _ in 0..NEWTON_MAX_ITERATIONS {
        if hi - lo <= GOLDEN_SECTION_TOLERANCE_METRES {
            return Some(best_minimum);
        }
        let probe = newton_probe(geodesic, &line, point, along)?;
        if probe.m12.abs() <= MIN_REDUCED_LENGTH_METRES {
            return None;
        }
        if probe.distance < best_minimum.distance {
            best_minimum = GeodesicSegmentMinimum {
                distance: probe.distance,
                along,
                used_golden_fallback: false,
            };
        }
        let abs_derivative = probe.derivative.abs();
        if abs_derivative * segment.length <= GOLDEN_SECTION_TOLERANCE_METRES {
            return Some(best_minimum);
        }
        if probe.derivative < 0.0 {
            lo = along;
        } else if probe.derivative > 0.0 {
            hi = along;
        } else {
            return Some(best_minimum);
        }
        if !(lo.is_finite() && hi.is_finite() && lo < hi) {
            return None;
        }
        let sin2 = (1.0 - probe.derivative * probe.derivative).max(0.0);
        let denominator =
            probe.derivative * probe.derivative + probe.distance * (probe.m21 / probe.m12) * sin2;
        let delta_s = -probe.distance * probe.derivative / denominator;
        let candidate = along + delta_s;
        let improves =
            abs_derivative < previous_abs_derivative || probe.distance < previous_distance;
        let accept_newton = delta_s.is_finite()
            && denominator.is_finite()
            && denominator > 0.0
            && candidate > lo
            && candidate < hi
            && improves;
        if accept_newton && delta_s.abs() <= GOLDEN_SECTION_TOLERANCE_METRES {
            if let Some(candidate_probe) = newton_probe(geodesic, &line, point, candidate) {
                if candidate_probe.distance < best_minimum.distance {
                    best_minimum = GeodesicSegmentMinimum {
                        distance: candidate_probe.distance,
                        along: candidate,
                        used_golden_fallback: false,
                    };
                }
                return Some(best_minimum);
            }
            return None;
        }
        along = if accept_newton {
            candidate
        } else {
            f64::midpoint(lo, hi)
        };
        previous_abs_derivative = abs_derivative;
        previous_distance = probe.distance;
    }
    (hi - lo <= GOLDEN_SECTION_TOLERANCE_METRES).then_some(best_minimum)
}

pub(crate) fn newton_probe(
    geodesic: &Geodesic,
    line: &GeodesicLine,
    point: Point,
    along: f64,
) -> Option<NewtonProbe> {
    let (_, lat, lon, alpha, ..) = line._gen_position(false, along, GEODESIC_LINE_OUTMASK);
    if !(lat.is_finite() && lon.is_finite() && alpha.is_finite()) {
        return None;
    }
    let (rho, _azi_p, azi2_pq, m12, _m12_scale, m21, _a12): (f64, f64, f64, f64, f64, f64, f64) =
        geodesic.inverse(point.y, point.x, lat, lon);
    count_newton_inverse!();
    if rho > std::f64::consts::PI * geodesic.a * 0.49 {
        return None;
    }
    let delta = angle_difference_radians(alpha, away_from_probe_azimuth(azi2_pq));
    let derivative = -delta.cos();
    (rho.is_finite() && derivative.is_finite() && m12.is_finite() && m21.is_finite()).then_some(
        NewtonProbe {
            distance: rho,
            derivative,
            m12,
            m21,
        },
    )
}

pub(crate) fn angle_difference_radians(alpha_degrees: f64, beta_degrees: f64) -> f64 {
    let mut delta = (alpha_degrees - beta_degrees).to_radians();
    if delta > std::f64::consts::PI {
        delta -= std::f64::consts::TAU;
    } else if delta < -std::f64::consts::PI {
        delta += std::f64::consts::TAU;
    }
    delta
}

pub(crate) fn away_from_probe_azimuth(azi2_pq: f64) -> f64 {
    azi2_pq + 180.0
}

pub(crate) fn geodesic_segment_minimum_golden(
    geodesic: &Geodesic,
    point: Point,
    segment: GeodesicSegment,
    mut best_minimum: GeodesicSegmentMinimum,
) -> GeodesicSegmentMinimum {
    let line = cached_geodesic_line(geodesic, segment);
    let at = |along: f64| -> f64 {
        let (_, lat, lon, ..) = line._gen_position(false, along, GEODESIC_LINE_OUTMASK);
        count_fallback_golden_probe!();
        inverse_distance(geodesic, point.x, point.y, lon, lat)
    };
    let (mut low, mut high) = (0.0, segment.length);
    let mut c = (high - low) * (-GOLDEN_RATIO) + high;
    let mut d = (high - low) * GOLDEN_RATIO + low;
    let (mut fc, mut fd) = (at(c), at(d));
    for _ in 0..GOLDEN_FALLBACK_ITERATIONS {
        if high - low <= GOLDEN_FALLBACK_TOLERANCE_METRES {
            break;
        }
        if fc < fd {
            high = d;
            d = c;
            fd = fc;
            c = (high - low) * (-GOLDEN_RATIO) + high;
            fc = at(c);
        } else {
            low = c;
            c = d;
            fc = fd;
            d = (high - low) * GOLDEN_RATIO + low;
            fd = at(d);
        }
    }
    if fc < best_minimum.distance {
        best_minimum.distance = fc;
        best_minimum.along = c;
    }
    if fd < best_minimum.distance {
        best_minimum.distance = fd;
        best_minimum.along = d;
    }
    let midpoint = f64::midpoint(low, high);
    let fm = at(midpoint);
    if fm < best_minimum.distance {
        best_minimum.distance = fm;
        best_minimum.along = midpoint;
    }
    if let Some(along) =
        refine_golden_bracket_by_derivative(geodesic, &line, point, 0.0, segment.length)
    {
        let distance = at(along);
        if distance <= best_minimum.distance + GOLDEN_SECTION_TOLERANCE_METRES {
            best_minimum.distance = distance;
            best_minimum.along = along;
        }
    }
    best_minimum.used_golden_fallback = true;
    best_minimum
}

pub(crate) fn refine_golden_bracket_by_derivative(
    geodesic: &Geodesic,
    line: &GeodesicLine,
    point: Point,
    mut low: f64,
    mut high: f64,
) -> Option<f64> {
    let mut low_derivative = derivative_at(geodesic, line, point, low)?;
    let high_derivative = derivative_at(geodesic, line, point, high)?;
    if !(low_derivative <= 0.0 && high_derivative >= 0.0) {
        return None;
    }
    for _ in 0..64 {
        if high - low <= GOLDEN_FALLBACK_TOLERANCE_METRES {
            break;
        }
        let mid = f64::midpoint(low, high);
        let derivative = derivative_at(geodesic, line, point, mid)?;
        if derivative < 0.0 {
            low = mid;
            low_derivative = derivative;
        } else if derivative > 0.0 {
            high = mid;
        } else {
            return Some(mid);
        }
    }
    let _ = low_derivative;
    Some(f64::midpoint(low, high))
}

pub(crate) fn derivative_at(
    geodesic: &Geodesic,
    line: &GeodesicLine,
    point: Point,
    along: f64,
) -> Option<f64> {
    let (_, lat, lon, alpha, ..) = line._gen_position(false, along, GEODESIC_LINE_OUTMASK);
    if !(lat.is_finite() && lon.is_finite() && alpha.is_finite()) {
        return None;
    }
    let (_, azi2_pq) = inverse_azimuths(geodesic, point.x, point.y, lon, lat);
    Some(-angle_difference_radians(alpha, away_from_probe_azimuth(azi2_pq)).cos())
}
