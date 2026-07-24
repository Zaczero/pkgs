#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! Ellipsoidal rhumb-line (loxodrome) solutions on the CRS ellipsoid.
//!
//! A rhumb line is straight in Mercator space, so the inverse problem reads
//! the isometric-latitude/longitude slope and rescales the Mercator length
//! by the divided difference of rectifying latitude with respect to
//! isometric latitude — GeographicLib's `Rhumb` formulation with the
//! order-6 third-flattening rectifying series (`AuxLatitude.cpp`), which is
//! good to ~10 nm on WGS84. The port keeps the exact closed-form isometric
//! latitude and the series rectifying latitude, and answers the east-west
//! limit through midpoint divided differences instead of the collapsing
//! `Δμ/Δψ` quotient. Differential-tested against GeographicLib
//! `RhumbSolve -p 10` vectors in `tests/test_linear_extras.py`.
//!
//! The order-6 sine series (`series`) and the divided-difference slope are
//! evaluated with the Clenshaw / three-term recurrences GeographicLib uses
//! (one `sin_cos` per base angle, then algebraic advancement of the
//! harmonics) instead of a fresh libm `sin`/`cos` per harmonic. The
//! recurrence is a few ULP from a naïve independent-`sin` sum — well below
//! the ~10 nm series-truncation floor and *closer* to GeographicLib, which
//! evaluates the same series the same way (pinned by
//! `tests/test_linear_extras.py` and the Rust unit tests below).
//!
//! Each public operation resolves a prepared per-CRS [`RhumbEllipsoid`] once
//! through the thread-local cache (`with_rhumb`) so a bulk call reuses the
//! order-6 coefficient tables and eccentricity across every row, and the
//! bearing/distance/destination kernels are split so none pays the other's
//! series/slope/`atan2` work.

use std::f64::consts::FRAC_PI_2;

use super::cache::{
    CRS_RHUMB_CACHE, CRS_RHUMB_CACHE_CAPACITY, CachedRhumbEllipsoid, ensure_thread_caches_current,
    lru_resolve,
};
use super::error::CrsError;
use super::geodesic::{ellipsoid_shape, ensure_geographic_lonlat};
use super::local::normalize_longitude;
use crate::error::Result;

/// The rectifying series is GeographicLib's order-6 expansion, documented
/// as full `f64` accuracy for `|f| <= 1/150` — far beyond every Earth
/// ellipsoid. Steeper ellipsoids would need the exact elliptic-integral
/// path, which no EPSG geographic CRS requires.
const MAX_FLATTENING: f64 = 1.0 / 150.0;

/// Rhumb working constants for one ellipsoid: eccentricity, rectifying
/// radius, and the order-6 rectifying-latitude series in both directions.
pub(in crate::crs) struct RhumbEllipsoid {
    e2: f64,
    /// Eccentricity `sqrt(e2)`, precomputed once so the per-inverse
    /// `isometric`/`slope` calls never repeat the square root.
    e: f64,
    /// Rectifying radius (meters): meridian length is `pi * radius`.
    radius: f64,
    /// `phi -> mu` series coefficients (geodetic to rectifying).
    to_mu: [f64; 6],
    /// `mu -> phi` series coefficients (rectifying to geodetic).
    to_phi: [f64; 6],
}

impl RhumbEllipsoid {
    fn for_crs(crs: &str) -> Result<Self> {
        let shape = ellipsoid_shape(crs)?;
        let f = shape.flattening;
        if !(0.0..=MAX_FLATTENING).contains(&f) {
            return Err(CrsError::message(format!(
                "rhumb operations require an oblate ellipsoid with flattening at most 1/150, \
                 got {f}"
            )));
        }
        // Third flattening; every coefficient below is a polynomial in it
        // (GeographicLib `AuxLatitude.cpp`, order 6).
        let n = f / (2.0 - f);
        let n2 = n * n;
        let to_mu = [
            n * (-1.5 + n2 * (9.0 / 16.0 + n2 * (-3.0 / 32.0))),
            n2 * (15.0 / 16.0 + n2 * (-15.0 / 32.0 + n2 * (135.0 / 2048.0))),
            n2 * n * (-35.0 / 48.0 + n2 * (105.0 / 256.0)),
            n2 * n2 * (315.0 / 512.0 + n2 * (-189.0 / 512.0)),
            n2 * n2 * n * (-693.0 / 1280.0),
            n2 * n2 * n2 * (1001.0 / 2048.0),
        ];
        let to_phi = [
            n * (1.5 + n2 * (-27.0 / 32.0 + n2 * (269.0 / 512.0))),
            n2 * (21.0 / 16.0 + n2 * (-55.0 / 32.0 + n2 * (6759.0 / 4096.0))),
            n2 * n * (151.0 / 96.0 + n2 * (-417.0 / 128.0)),
            n2 * n2 * (1097.0 / 512.0 + n2 * (-15543.0 / 2560.0)),
            n2 * n2 * n * (8011.0 / 2560.0),
            n2 * n2 * n2 * (293_393.0 / 61440.0),
        ];
        let e2 = f * (2.0 - f);
        Ok(Self {
            e2,
            e: e2.sqrt(),
            radius: shape.semi_major.get() / (1.0 + n)
                * (1.0 + n2 * (0.25 + n2 * (1.0 / 64.0 + n2 / 256.0))),
            to_mu,
            to_phi,
        })
    }

    /// Isometric latitude from latitude in degrees (exact closed form);
    /// signed infinity at the exact poles — `tan(pi/2)` is finite in `f64`,
    /// so the pole must branch on the degree value.
    #[expect(
        clippy::float_cmp,
        reason = "only the literal ±90 inputs are the poles; anything else is a real latitude"
    )]
    fn isometric(&self, latitude: f64) -> f64 {
        if latitude.abs() == 90.0 {
            return f64::INFINITY.copysign(latitude);
        }
        let phi = latitude.to_radians();
        phi.tan().asinh() - self.e * (self.e * phi.sin()).atanh()
    }

    /// Rectifying latitude `mu` (radians) from geodetic `phi`.
    fn rectifying(&self, phi: f64) -> f64 {
        series(phi, &self.to_mu)
    }

    /// Geodetic latitude `phi` from rectifying `mu`.
    fn geodetic(&self, mu: f64) -> f64 {
        series(mu, &self.to_phi)
    }

    /// The slope `Dmu/Dpsi` between two latitudes — the factor that turns a
    /// Mercator length into a rectifying (true meter) length. Both
    /// differences are computed in cancellation-free divided-difference
    /// forms (sine-series products for `mu`; the `asinh`/`atanh`
    /// subtraction identities for `psi`), so one formula covers every
    /// separation — no threshold, no midpoint approximation. Equal
    /// latitudes take the analytic derivative ratio, whose east-west limit
    /// is the parallel radius over the rectifying radius.
    fn slope(&self, phi1: f64, phi2: f64) -> f64 {
        let dphi = phi2 - phi1;
        if dphi == 0.0 {
            let mut dmu_dphi = 1.0;
            for (index, coefficient) in self.to_mu.iter().enumerate() {
                let m = 2.0 * (index + 1) as f64;
                dmu_dphi += coefficient * m * (m * phi1).cos();
            }
            let sin_phi = phi1.sin();
            let dpsi_dphi = (1.0 - self.e2) / (phi1.cos() * (1.0 - self.e2 * sin_phi * sin_phi));
            return dmu_dphi / dpsi_dphi;
        }
        let mid = f64::midpoint(phi1, phi2);
        // mu2 - mu1, term-wise stable: sum c_k [sin(2k phi2) - sin(2k phi1)]
        // rewritten as sum c_k 2 cos(2k mid) sin(k dphi) so a tiny dphi never
        // cancels two nearly-equal sines. The two harmonic sequences advance
        // by the three-term recurrences cos(k a) / sin(k b) instead of a fresh
        // libm call per harmonic (a = 2 mid, b = dphi).
        let cos_a = (2.0 * mid).cos();
        let (sin_b, cos_b) = dphi.sin_cos();
        let two_cos_a = 2.0 * cos_a;
        let two_cos_b = 2.0 * cos_b;
        // cos(k a): C_0 = 1, C_1 = cos a ; sin(k b): S_0 = 0, S_1 = sin b.
        let (mut cos_prev, mut cos_k) = (1.0, cos_a);
        let (mut sin_prev, mut sin_k) = (0.0, sin_b);
        let mut mu_diff = dphi;
        for coefficient in self.to_mu {
            mu_diff += coefficient * 2.0 * cos_k * sin_k;
            let cos_next = two_cos_a * cos_k - cos_prev;
            cos_prev = cos_k;
            cos_k = cos_next;
            let sin_next = two_cos_b * sin_k - sin_prev;
            sin_prev = sin_k;
            sin_k = sin_next;
        }
        // psi2 - psi1 via the subtraction identities:
        // asinh a - asinh b = asinh(a sqrt(1+b^2) - b sqrt(1+a^2)), with the
        // same-sign argument rewritten as (a-b)(a+b)/(a sqrt(1+b^2) +
        // b sqrt(1+a^2)) and tan b - tan a = sin(dphi)/(cos a cos b);
        // atanh u2 - atanh u1 = atanh((u2-u1)/(1 - u1 u2)) with
        // sin b - sin a in product form.
        let (t1, t2) = (phi1.tan(), phi2.tan());
        let (h1, h2) = ((1.0 + t1 * t1).sqrt(), (1.0 + t2 * t2).sqrt());
        let dt = dphi.sin() / (phi1.cos() * phi2.cos());
        let asinh_diff = if t1 * t2 > 0.0 {
            (dt * (t2 + t1) / (t2 * h1 + t1 * h2)).asinh()
        } else {
            (t2 * h1 - t1 * h2).asinh()
        };
        let (u1, u2) = (self.e * phi1.sin(), self.e * phi2.sin());
        let du = self.e * 2.0 * mid.cos() * (dphi / 2.0).sin();
        let atanh_diff = (du / (1.0 - u1 * u2)).atanh();
        mu_diff / (asinh_diff - self.e * atanh_diff)
    }
}

/// Clenshaw evaluation of the sine series `sum_{k=1}^{6} c[k-1] sin(k theta)`.
///
/// One `sin_cos` of the base angle, then the three-term recurrence
/// GeographicLib and the transverse-Mercator kernel (`in_core::tmerc::clens`)
/// use — never a fresh libm `sin` per harmonic.
fn clenshaw_sine(theta: f64, coefficients: &[f64; 6]) -> f64 {
    let (sin_theta, cos_theta) = theta.sin_cos();
    let two_cos = 2.0 * cos_theta;
    let mut h2 = 0.0_f64;
    let mut h1 = coefficients[5];
    let mut h = h1;
    for &coefficient in coefficients[..5].iter().rev() {
        h = -h2 + two_cos * h1 + coefficient;
        h2 = h1;
        h1 = h;
    }
    sin_theta * h
}

/// `theta + sum c[k] sin(2 (k + 1) theta)` — the auxiliary-latitude series,
/// evaluated by Clenshaw over the doubled angle (`sin(k (2 theta))`).
fn series(theta: f64, coefficients: &[f64; 6]) -> f64 {
    theta + clenshaw_sine(2.0 * theta, coefficients)
}

/// Shortest longitude delta `lon1 -> lon2` in degrees, in `(-180, 180]`
/// (an exact 180-degree tie takes the east-going rhumb, like
/// GeographicLib).
fn shortest_lon_delta(lon1: f64, lon2: f64) -> f64 {
    let delta = (lon2 - lon1).rem_euclid(360.0);
    if delta > 180.0 { delta - 360.0 } else { delta }
}

/// Resolve the prepared [`RhumbEllipsoid`] for `crs` from the thread-local
/// cache and run `f` against it — the rhumb sibling of `with_geodesic`, so a
/// bulk rhumb call rebuilds the order-6 coefficient tables at most once per
/// distinct CRS.
pub(in crate::crs) fn with_rhumb<T>(
    crs: &str,
    f: impl FnOnce(&RhumbEllipsoid) -> Result<T>,
) -> Result<T> {
    ensure_thread_caches_current();
    CRS_RHUMB_CACHE.with(|cache| {
        let mut cache = cache.borrow_mut();
        let index = lru_resolve(
            &mut cache,
            CRS_RHUMB_CACHE_CAPACITY,
            |item| item.crs == crs,
            || {
                Ok(CachedRhumbEllipsoid {
                    crs: crs.to_owned(),
                    ellipsoid: RhumbEllipsoid::for_crs(crs)?,
                })
            },
        )?;
        f(&cache[index].ellipsoid)
    })
}

/// Rhumb-line distance (meters) between two lon/lat points.
///
/// Distance-only: no `atan2` azimuth is computed.
pub(crate) fn rhumb_distance_crs(
    crs: &str,
    lon1: f64,
    lat1: f64,
    lon2: f64,
    lat2: f64,
) -> Result<f64> {
    ensure_geographic_lonlat(lon1, lat1)?;
    ensure_geographic_lonlat(lon2, lat2)?;
    with_rhumb(crs, |ellipsoid| {
        let psi1 = ellipsoid.isometric(lat1);
        let psi2 = ellipsoid.isometric(lat2);
        let lambda12 = shortest_lon_delta(lon1, lon2).to_radians();
        let dpsi = psi2 - psi1;
        let phi1 = lat1.to_radians();
        let phi2 = lat2.to_radians();
        let distance = if psi1.is_infinite() || psi2.is_infinite() {
            // A pole endpoint: every longitude is the same physical point, so
            // the rhumb runs the meridian — Mercator coordinates blow up but
            // the rectifying arc is exact.
            (ellipsoid.rectifying(phi2) - ellipsoid.rectifying(phi1)).abs() * ellipsoid.radius
        } else {
            let mercator = (lambda12 * lambda12 + dpsi * dpsi).sqrt();
            mercator * ellipsoid.slope(phi1, phi2) * ellipsoid.radius
        };
        finite(distance, "rhumb distance")
    })
}

/// Constant rhumb bearing (degrees clockwise from north, `0..360`) from one
/// lon/lat point to another.
///
/// Bearing-only: no rectifying series, slope, or distance is computed.
pub(crate) fn rhumb_bearing_crs(
    crs: &str,
    lon1: f64,
    lat1: f64,
    lon2: f64,
    lat2: f64,
) -> Result<f64> {
    ensure_geographic_lonlat(lon1, lat1)?;
    ensure_geographic_lonlat(lon2, lat2)?;
    let azimuth = with_rhumb(crs, |ellipsoid| {
        let psi1 = ellipsoid.isometric(lat1);
        let psi2 = ellipsoid.isometric(lat2);
        let lambda12 = shortest_lon_delta(lon1, lon2).to_radians();
        let azimuth = lambda12.atan2(psi2 - psi1).to_degrees();
        finite(azimuth, "rhumb bearing")
    })?;
    // atan2 degrees land in (-180, 180]; the branch normalize is
    // bit-identical to `rem_euclid(360)` there without the `fmod`.
    Ok(if azimuth < 0.0 {
        azimuth + 360.0
    } else {
        azimuth
    })
}

/// Destination lon/lat along a constant `bearing` for `meters` (direct
/// problem). `None` when the path would cross a pole — the destination
/// longitude is indeterminate there (GeographicLib reports `NaN`).
#[expect(
    clippy::float_cmp,
    reason = "exact-pole arrival (mu2 == pi/2) is a deliberate boundary case"
)]
pub(crate) fn rhumb_destination_crs(
    crs: &str,
    lon: f64,
    lat: f64,
    bearing: f64,
    meters: f64,
) -> Result<Option<(f64, f64)>> {
    ensure_geographic_lonlat(lon, lat)?;
    with_rhumb(crs, |ellipsoid| {
        let phi1 = lat.to_radians();
        let alpha = bearing.to_radians();
        let arc = meters / ellipsoid.radius;
        // Exactly meridional courses have a zero east component by definition;
        // `sin(pi)` noise would otherwise leak into the longitude.
        let east = if bearing.rem_euclid(180.0) == 0.0 {
            0.0
        } else {
            arc * alpha.sin()
        };
        let mu2 = ellipsoid.rectifying(phi1) + arc * alpha.cos();
        // Past a pole the longitude is indeterminate — and so is ARRIVING at a
        // pole with any east component still to spend.
        if mu2.abs() > FRAC_PI_2 || (mu2.abs() == FRAC_PI_2 && east != 0.0) {
            return Ok(None);
        }
        let phi2 = ellipsoid.geodetic(mu2);
        // Zero east component (meridional or zero-distance runs) keeps the
        // longitude exactly — and dodges the 0/0 when the run ends on a pole.
        let dlon = if east == 0.0 {
            0.0
        } else {
            (east / ellipsoid.slope(phi1, phi2)).to_degrees()
        };
        // Series round-off can land an exact-pole arrival a few ULPs past 90.
        let lat2 = finite(phi2.to_degrees(), "rhumb destination")?.clamp(-90.0, 90.0);
        let lon2 = finite(normalize_longitude(lon + dlon), "rhumb destination")?;
        Ok(Some((lon2, lat2)))
    })
}

fn finite(value: f64, operation: &str) -> Result<f64> {
    if value.is_finite() {
        Ok(value)
    } else {
        Err(CrsError::invalid(format!("{operation} calculation failed")))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Naïve reference: `theta + sum c_k sin(2k theta)` with an independent
    /// libm `sin` per harmonic (the pre-Clenshaw form).
    fn series_naive(theta: f64, coefficients: &[f64; 6]) -> f64 {
        let mut value = theta;
        for (index, coefficient) in coefficients.iter().enumerate() {
            value += coefficient * (2.0 * (index + 1) as f64 * theta).sin();
        }
        value
    }

    #[test]
    fn clenshaw_series_matches_the_naive_sum_within_a_few_ulp() {
        let ellipsoid = RhumbEllipsoid::for_crs("EPSG:4326").unwrap();
        let mut max_ulp = 0_i64;
        for step in -155..=155 {
            let theta = f64::from(step) * 0.01;
            for coefficients in [&ellipsoid.to_mu, &ellipsoid.to_phi] {
                let clenshaw = series(theta, coefficients);
                let naive = series_naive(theta, coefficients);
                let ulps = (clenshaw.to_bits() as i64 - naive.to_bits() as i64).abs();
                max_ulp = max_ulp.max(ulps);
                assert!(
                    (clenshaw - naive).abs() <= 8.0 * f64::EPSILON,
                    "theta={theta} clenshaw={clenshaw} naive={naive}"
                );
            }
        }
        // A handful of ULP at most: the recurrence is cancellation-safe here.
        assert!(
            max_ulp <= 4,
            "series drifted {max_ulp} ULP from the naive sum"
        );
    }

    /// Divided-difference slope with an independent libm `sin`/`cos` per
    /// harmonic (the pre-recurrence form of `RhumbEllipsoid::slope`).
    fn slope_naive(ellipsoid: &RhumbEllipsoid, phi1: f64, phi2: f64) -> f64 {
        let dphi = phi2 - phi1;
        let mid = f64::midpoint(phi1, phi2);
        let mut mu_diff = dphi;
        for (index, coefficient) in ellipsoid.to_mu.iter().enumerate() {
            let m = 2.0 * (index + 1) as f64;
            mu_diff += coefficient * 2.0 * (m * mid).cos() * (m * dphi / 2.0).sin();
        }
        let (t1, t2) = (phi1.tan(), phi2.tan());
        let (h1, h2) = ((1.0 + t1 * t1).sqrt(), (1.0 + t2 * t2).sqrt());
        let dt = dphi.sin() / (phi1.cos() * phi2.cos());
        let asinh_diff = if t1 * t2 > 0.0 {
            (dt * (t2 + t1) / (t2 * h1 + t1 * h2)).asinh()
        } else {
            (t2 * h1 - t1 * h2).asinh()
        };
        let (u1, u2) = (ellipsoid.e * phi1.sin(), ellipsoid.e * phi2.sin());
        let du = ellipsoid.e * 2.0 * mid.cos() * (dphi / 2.0).sin();
        let atanh_diff = (du / (1.0 - u1 * u2)).atanh();
        mu_diff / (asinh_diff - ellipsoid.e * atanh_diff)
    }

    #[test]
    fn recurrence_slope_matches_the_naive_trig_form() {
        let ellipsoid = RhumbEllipsoid::for_crs("EPSG:4326").unwrap();
        for a in -80..=80 {
            for delta in [1, 5, 25, 60] {
                let phi1 = f64::from(a).to_radians();
                let phi2 = f64::from(a + delta).to_radians();
                let refactored = ellipsoid.slope(phi1, phi2);
                let naive = slope_naive(&ellipsoid, phi1, phi2);
                assert!(
                    (refactored - naive).abs() <= 1e-12 * naive.abs().max(1.0),
                    "phi1={phi1} phi2={phi2} refactored={refactored} naive={naive}"
                );
            }
        }
    }

    #[test]
    fn split_distance_matches_a_naive_full_inverse() {
        // The distance-only kernel must equal a naïve inverse built from the
        // faithful `isometric` plus the pre-refactor series/slope — proof that
        // the Clenshaw/recurrence rewrite did not shift the answer.
        let ellipsoid = RhumbEllipsoid::for_crs("EPSG:4326").unwrap();
        let cases: [(f64, f64, f64, f64); 4] = [
            (-73.7781, 40.6413, -0.4543, 51.4700),
            (2.3522, 48.8566, 139.6917, 35.6895),
            (-58.0, -34.6, 18.4, -33.9),
            (10.0, 0.0, 10.0, 45.0),
        ];
        for &(lon1, lat1, lon2, lat2) in &cases {
            let phi1 = lat1.to_radians();
            let phi2 = lat2.to_radians();
            let psi1 = ellipsoid.isometric(lat1);
            let psi2 = ellipsoid.isometric(lat2);
            let lambda12 = shortest_lon_delta(lon1, lon2).to_radians();
            let dpsi = psi2 - psi1;
            let naive = if psi1.is_infinite() || psi2.is_infinite() {
                (series_naive(phi2, &ellipsoid.to_mu) - series_naive(phi1, &ellipsoid.to_mu)).abs()
                    * ellipsoid.radius
            } else {
                (lambda12 * lambda12 + dpsi * dpsi).sqrt()
                    * slope_naive(&ellipsoid, phi1, phi2)
                    * ellipsoid.radius
            };
            let refactored = rhumb_distance_crs("EPSG:4326", lon1, lat1, lon2, lat2).unwrap();
            assert!(
                (refactored - naive).abs() <= 1e-6 * naive.abs().max(1.0),
                "({lon1},{lat1})->({lon2},{lat2}) refactored={refactored} naive={naive}"
            );
        }
    }

    #[test]
    fn rhumb_destination_round_trips_the_inverse() {
        let (lon0, lat0) = (-73.7781_f64, 40.6413_f64);
        let (lon1, lat1) = (2.3522_f64, 48.8566_f64);
        let distance = rhumb_distance_crs("EPSG:4326", lon0, lat0, lon1, lat1).unwrap();
        let bearing = rhumb_bearing_crs("EPSG:4326", lon0, lat0, lon1, lat1).unwrap();
        let (lon2, lat2) = rhumb_destination_crs("EPSG:4326", lon0, lat0, bearing, distance)
            .unwrap()
            .expect("non-polar rhumb has a destination");
        assert!((lon2 - lon1).abs() < 1e-6, "lon {lon2} vs {lon1}");
        assert!((lat2 - lat1).abs() < 1e-6, "lat {lat2} vs {lat1}");
    }
}
