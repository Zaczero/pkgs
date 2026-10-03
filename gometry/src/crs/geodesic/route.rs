use geographiclib_rs::{Geodesic, InverseGeodesic as _};

use crate::crs::CrsError;
use crate::error::Result;
use crate::geometry::Point;

#[derive(Clone, Copy, Debug)]
pub(crate) struct GeodesicRoute {
    pub(crate) length: f64,
    pub(crate) azimuth: f64,
}

pub(crate) fn checked_geodesic_route(
    geodesic: &Geodesic,
    start: Point,
    end: Point,
) -> Result<GeodesicRoute> {
    crate::crs::ensure_geographic_lonlat(start.x, start.y)?;
    crate::crs::ensure_geographic_lonlat(end.x, end.y)?;
    let (length, azimuth, reverse_azimuth, ..): (f64, f64, f64, f64, f64, f64, f64) =
        geodesic.inverse(start.y, start.x, end.y, end.x);
    ensure_unique_shortest(geodesic, start, end, length, azimuth, reverse_azimuth)?;
    let (length, azimuth) =
        spherical_meridional_route(geodesic, start, end).unwrap_or((length, azimuth));
    Ok(GeodesicRoute { length, azimuth })
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
    azimuth: f64,
    reverse_azimuth: f64,
) -> Result<()> {
    if length == 0.0 {
        return Ok(());
    }
    let opposite_poles = start.y.abs() == 90.0 && end.y.abs() == 90.0 && start.y != end.y;
    let (delta_lon, delta_correction) = angle_difference(start.x, end.x);
    let exact_half_turn = delta_lon.abs() == 180.0 && delta_correction == 0.0;
    let opposite_latitudes = start.y == -end.y;
    if !(length.is_finite() && azimuth.is_finite() && reverse_azimuth.is_finite()) {
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
            && azimuths_show_distinct_branches(azimuth, reverse_azimuth))
        || (geodesic.f < 0.0 && exact_half_turn && azimuth != 0.0 && azimuth.abs() != 180.0);
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

fn spherical_meridional_route(geodesic: &Geodesic, start: Point, end: Point) -> Option<(f64, f64)> {
    if geodesic.f != 0.0 {
        return None;
    }
    let (angular_length, azimuth) = spherical_meridional_route_parameters(start, end)?;
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
fn spherical_meridional_route_parameters(start: Point, end: Point) -> Option<(f64, f64)> {
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

#[expect(
    clippy::float_cmp,
    reason = "exact represented antipodal branch conditions are structural, not tolerances"
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
    reason = "a pole or antimeridian is one specific representable coordinate; a tolerance would admit vertices merely near it"
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
mod tests {
    use geographiclib_rs::{DirectGeodesic as _, Geodesic};

    use super::checked_geodesic_route;
    use crate::geometry::Point;

    #[test]
    fn geodesic_route_rejects_nonunique_shortest_endpoints() {
        for (geodesic, start, end) in [
            (Geodesic::wgs84(), (0.0, 90.0), (0.0, -90.0)),
            (Geodesic::wgs84(), (0.0, 0.0), (180.0, 0.0)),
            (Geodesic::new(6_378_137.0, 0.0), (0.0, 10.0), (180.0, -10.0)),
            (
                Geodesic::new(6_378_137.0, -0.01),
                (0.0, 10.0),
                (180.0, -10.0),
            ),
        ] {
            checked_geodesic_route(
                &geodesic,
                Point::new_raw_xy(start.0, start.1),
                Point::new_raw_xy(end.0, end.1),
            )
            .unwrap_err();
        }
    }

    #[test]
    fn spherical_antipodal_latitude_sign_selects_the_original_meridian_branch() {
        let sphere = Geodesic::new(6_378_137.0, 0.0);
        for (latitude, expected_azimuth) in [(-1e-18, 180.0_f64), (1e-18, 0.0_f64)] {
            let route = checked_geodesic_route(
                &sphere,
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(180.0, latitude),
            )
            .expect("signed antipodal perturbation is unique");
            assert_eq!(route.azimuth.to_bits(), expected_azimuth.to_bits());
            let (middle_latitude, _): (f64, f64) =
                sphere.direct(0.0, 0.0, route.azimuth, route.length / 2.0);
            assert_eq!(
                middle_latitude.is_sign_negative(),
                latitude.is_sign_negative()
            );
        }
    }
}
