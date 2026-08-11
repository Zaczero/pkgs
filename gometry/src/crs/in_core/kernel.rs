#![expect(
    clippy::unreadable_literal,
    reason = "authalic series coefficients retain PROJ's published numerator and denominator spellings"
)]
//! Shared projection-kernel types for in-core fast paths.

use crate::crs::in_core::params::OperationSpec;
use crate::error::Result;

pub(super) const EPS10: f64 = 1.0e-10;
pub(super) const DEG_TO_RAD: f64 = std::f64::consts::PI / 180.0;
pub(super) const RAD_TO_DEG: f64 = 180.0 / std::f64::consts::PI;
const PHI2_TOL: f64 = 1.0e-12;
const PHI2_MAX_ITER: usize = 15;

pub(super) fn eccentricity_squared(ellipsoid: Ellipsoid) -> f64 {
    let d = 1.0 + ellipsoid.n;
    4.0 * ellipsoid.n / (d * d)
}

pub(super) fn eccentricity(ellipsoid: Ellipsoid) -> f64 {
    eccentricity_squared(ellipsoid).sqrt()
}

pub(super) fn msfn(sin_phi: f64, cos_phi: f64, es: f64) -> f64 {
    cos_phi / (1.0 - es * sin_phi * sin_phi).sqrt()
}

pub(super) fn tsfn(sin_phi: f64, cos_phi: f64, e: f64) -> f64 {
    // Callers already hold `phi.sin_cos()`, so `cos_phi` is threaded in rather
    // than recomputed with a second libm `cos` per point on the hot forward
    // LCC / polar-stereographic paths.
    let spherical = if sin_phi > 0.0 {
        cos_phi / (1.0 + sin_phi)
    } else {
        (1.0 - sin_phi) / cos_phi
    };
    (e * (e * sin_phi).atanh()).exp() * spherical
}

pub(super) fn phi2(ts: f64, e: f64) -> Option<f64> {
    if ts == 0.0 {
        return Some(std::f64::consts::FRAC_PI_2);
    }
    if ts <= 0.0 || !ts.is_finite() {
        return None;
    }
    let mut phi = std::f64::consts::FRAC_PI_2 - 2.0 * ts.atan();
    for _ in 0..PHI2_MAX_ITER {
        let con = e * phi.sin();
        let ratio = (1.0 - con) / (1.0 + con);
        if ratio <= 0.0 || !ratio.is_finite() {
            return None;
        }
        let next = std::f64::consts::FRAC_PI_2 - 2.0 * (ts * ratio.powf(0.5 * e)).atan();
        if (next - phi).abs() <= PHI2_TOL {
            return next.is_finite().then_some(next);
        }
        phi = next;
    }
    None
}

#[derive(Clone, Copy, Debug)]
pub(super) struct Authalic {
    es: f64,
    e: f64,
    pub(super) qp: f64,
    apa: [f64; 6],
}

impl Authalic {
    pub(super) fn new(ellipsoid: Ellipsoid) -> Option<Self> {
        let es = eccentricity_squared(ellipsoid);
        let e = es.sqrt();
        if !(0.0..1.0).contains(&es) || e == 0.0 {
            return None;
        }
        let one_es = 1.0 - es;
        let qp = one_es * (1.0 / one_es + e.atanh() / e);
        let n = ellipsoid.n;
        let poly = |coeffs: &[f64]| coeffs.iter().rev().fold(0.0, |v, c| v * n + c);
        let apa = [
            n * poly(&[
                4.0 / 3.0,
                4.0 / 45.0,
                -16.0 / 35.0,
                -2582.0 / 14175.0,
                60136.0 / 467775.0,
                28112932.0 / 212837625.0,
            ]),
            n.powi(2)
                * poly(&[
                    46.0 / 45.0,
                    152.0 / 945.0,
                    -11966.0 / 14175.0,
                    -21016.0 / 51975.0,
                    251310128.0 / 638512875.0,
                ]),
            n.powi(3)
                * poly(&[
                    3044.0 / 2835.0,
                    3802.0 / 14175.0,
                    -94388.0 / 66825.0,
                    -8797648.0 / 10945935.0,
                ]),
            n.powi(4)
                * poly(&[
                    6059.0 / 4725.0,
                    41072.0 / 93555.0,
                    -1472637812.0 / 638512875.0,
                ]),
            n.powi(5) * poly(&[768272.0 / 467775.0, 455935736.0 / 638512875.0]),
            n.powi(6) * 4210684958.0 / 1915538625.0,
        ];
        qp.is_finite().then_some(Self { es, e, qp, apa })
    }

    pub(super) fn q(self, sin_phi: f64) -> f64 {
        let esin = self.e * sin_phi;
        (1.0 - self.es) * (sin_phi / (1.0 - esin * esin) + esin.atanh() / self.e)
    }

    pub(super) fn beta(self, phi: f64) -> f64 {
        (self.q(phi.sin()) / self.qp).clamp(-1.0, 1.0).asin()
    }

    pub(super) fn inverse_beta(self, beta: f64) -> f64 {
        beta + self
            .apa
            .iter()
            .enumerate()
            .map(|(i, c)| c * (2.0 * (i + 1) as f64 * beta).sin())
            .sum::<f64>()
    }
}

pub(in crate::crs) trait ProjectionKernel: Sized + Copy {
    type Setup: Copy;
    type MethodParams: Copy + 'static;

    const SPECS: &'static [OperationSpec<Self::MethodParams>];

    fn setup(ellipsoid: Ellipsoid, params: Self::MethodParams) -> Result<Self::Setup>;

    /// Forward with longitude already relative to central meridian (radians) and
    /// output in un-offset meters.
    fn forward_unframed(setup: &Self::Setup, lam_rad: f64, lat_rad: f64) -> Result<(f64, f64)>;

    /// Inverse returning relative longitude (radians) and latitude (radians),
    /// with easting/northing already stripped of false origin.
    fn inverse_unframed(setup: &Self::Setup, x: f64, y: f64) -> Result<(f64, f64)>;
}

#[derive(Clone, Copy, Debug)]
pub(in crate::crs) struct Ellipsoid {
    pub(super) a: f64,
    pub(super) n: f64,
}

impl Ellipsoid {
    pub(super) const fn from_a_f(a: f64, f: f64) -> Self {
        let n = f / (2.0 - f);
        Self { a, n }
    }
}
