#![expect(
    clippy::suspicious_operation_groupings,
    reason = "EPSG Albers equations use conventional m, q, n, c, and rho notation"
)]
//! Ellipsoidal Albers Equal Area (EPSG method 9822).

use crate::crs::in_core::adjlon;
use crate::crs::in_core::kernel::{
    Authalic, EPS10, Ellipsoid, ProjectionKernel, eccentricity_squared, msfn,
};
use crate::crs::in_core::params::{
    FrameSpec, MethodSpec, OperationSpec, ParamUnit, Projected, Requirement, admit_projection,
    epsg, param_field, set_finite,
};
use crate::error::Result;
use crate::geometry::GeometryErrorKind;

#[derive(Clone, Copy, Debug, Default)]
pub(in crate::crs) struct AeaParams {
    lat_0: f64,
    lat_1: f64,
    lat_2: f64,
}
#[derive(Clone, Copy, Debug, Default)]
pub(in crate::crs) struct Aea;
#[derive(Clone, Copy, Debug)]
pub(in crate::crs) struct Setup {
    auth: Authalic,
    n: f64,
    c: f64,
    dd: f64,
    rho0: f64,
    a: f64,
}

// `qs` is formed from a subtraction, a square, and several divisions. The
// EPSG AEA catalogue reaches eight binary64 epsilons of endpoint overshoot;
// twice that leaves headroom for the final ratio division. This is an
// overshoot allowance only; an interior ratio, however close to one, is still
// evaluated rather than classified as a pole.
const DOMAIN_ROUNDING: f64 = 16.0 * f64::EPSILON;

const fn set_lat_0(p: &mut AeaParams, v: f64) -> bool {
    set_finite(&mut p.lat_0, v)
}
const fn set_lat_1(p: &mut AeaParams, v: f64) -> bool {
    set_finite(&mut p.lat_1, v)
}
const fn set_lat_2(p: &mut AeaParams, v: f64) -> bool {
    set_finite(&mut p.lat_2, v)
}
fn finalize(p: AeaParams) -> Option<AeaParams> {
    ((p.lat_1 + p.lat_2).to_radians().abs() >= EPS10).then_some(p)
}

impl ProjectionKernel for Aea {
    type Setup = Setup;
    type MethodParams = AeaParams;
    const SPECS: &'static [OperationSpec<AeaParams>] = &[OperationSpec {
        method: MethodSpec { code: "9822" },
        frame: FrameSpec {
            lon_0: epsg("8822"),
            x_0: epsg("8826"),
            y_0: epsg("8827"),
        },
        defaults: AeaParams {
            lat_0: 0.0,
            lat_1: 0.0,
            lat_2: 0.0,
        },
        params: &[
            param_field(
                epsg("8821"),
                ParamUnit::Degrees,
                set_lat_0,
                Requirement::Required,
            ),
            param_field(
                epsg("8823"),
                ParamUnit::Degrees,
                set_lat_1,
                Requirement::Required,
            ),
            param_field(
                epsg("8824"),
                ParamUnit::Degrees,
                set_lat_2,
                Requirement::Required,
            ),
        ],
        finalize,
    }];
    fn setup(e: Ellipsoid, p: AeaParams) -> Result<Setup> {
        let auth = Authalic::new(e)
            .ok_or_else(|| GeometryErrorKind::projection("invalid Albers Equal Area ellipsoid"))?;
        let (p1, p2, p0) = (
            p.lat_1.to_radians(),
            p.lat_2.to_radians(),
            p.lat_0.to_radians(),
        );
        if p1.abs() > std::f64::consts::FRAC_PI_2
            || p2.abs() > std::f64::consts::FRAC_PI_2
            || (p1 + p2).abs() < EPS10
        {
            return Err(GeometryErrorKind::projection(
                "invalid Albers Equal Area parameters",
            ));
        }
        let es = eccentricity_squared(e);
        let (s1, c1) = p1.sin_cos();
        let m1 = msfn(s1, c1, es);
        let q1 = auth.q(s1);
        let n = if (p1 - p2).abs() >= EPS10 {
            let (s2, c2) = p2.sin_cos();
            let m2 = msfn(s2, c2, es);
            let q2 = auth.q(s2);
            (m1 * m1 - m2 * m2) / (q2 - q1)
        } else {
            s1
        };
        if !n.is_finite() || n == 0.0 {
            return Err(GeometryErrorKind::projection(
                "invalid Albers Equal Area parameters",
            ));
        }
        let c = m1 * m1 + n * q1;
        let dd = 1.0 / n;
        let r = c - n * auth.q(p0.sin());
        if r < 0.0 {
            return Err(GeometryErrorKind::projection(
                "invalid Albers Equal Area parameters",
            ));
        }
        Ok(Setup {
            auth,
            n,
            c,
            dd,
            rho0: dd * r.sqrt(),
            a: e.a,
        })
    }
    fn forward_unframed(s: &Setup, lam: f64, phi: f64) -> Result<(f64, f64)> {
        // At the exact +/-180 degree cone seam, equivalent longitude spellings
        // can round onto opposite signed sides before `adjlon`. The point is far
        // outside every admitted CRS area of use; preserve the raw arithmetic
        // instead of imposing an arbitrary canonical seam side.
        let r = s.c - s.n * s.auth.q(phi.sin());
        if r < 0.0 {
            return Err(GeometryErrorKind::projection_domain(
                "coordinate is outside the Albers Equal Area domain",
            ));
        }
        let rho = s.dd * r.sqrt();
        let theta = s.n * adjlon(lam);
        let (st, ct) = theta.sin_cos();
        let (x, y) = (s.a * rho * st, s.a * (s.rho0 - rho * ct));
        if x.is_finite() && y.is_finite() {
            Ok((x, y))
        } else {
            Err(GeometryErrorKind::NonFiniteCoordinate.into())
        }
    }
    #[expect(
        clippy::float_cmp,
        reason = "only exact clamped endpoints denote the projection poles"
    )]
    fn inverse_unframed(s: &Setup, x: f64, y: f64) -> Result<(f64, f64)> {
        let mut xx = x / s.a;
        let mut yy = s.rho0 - y / s.a;
        let mut rho = xx.hypot(yy);
        let zero_radius = rho == 0.0;
        if s.n < 0.0 {
            rho = -rho;
            xx = -xx;
            yy = -yy;
        }
        let qs = (s.c - (rho / s.dd).powi(2)) / s.n;
        let ratio = qs / s.auth.qp;
        if !ratio.is_finite() || ratio.abs() > 1.0 + DOMAIN_ROUNDING {
            return Err(GeometryErrorKind::projection_domain(
                "coordinate is outside the Albers Equal Area domain",
            ));
        }
        let ratio = ratio.clamp(-1.0, 1.0);
        // The authalic series leaves a tiny endpoint residual because
        // sin(k*pi) is not zero in binary64. A clamped endpoint is the pole,
        // so preserve that exact geographic coordinate.
        let phi = if ratio == 1.0 {
            std::f64::consts::FRAC_PI_2
        } else if ratio == -1.0 {
            -std::f64::consts::FRAC_PI_2
        } else {
            s.auth.inverse_beta(ratio.asin())
        };
        let longitude = if zero_radius {
            0.0
        } else {
            adjlon(xx.atan2(yy) / s.n)
        };
        Ok((longitude, phi))
    }
}
pub(super) fn admit(
    op: &super::super::CrsCoordinateOperationInfo,
    e: Ellipsoid,
) -> Option<Projected<Aea>> {
    admit_projection::<Aea>(op, e)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inverse_validates_the_cone_apex_before_accepting_zero_radius() {
        let ellipsoid = Ellipsoid::from_a_f(6_378_137.0, 1.0 / 298.257_222_101);
        let setup = Aea::setup(ellipsoid, AeaParams {
            lat_0: 23.0,
            lat_1: 29.5,
            lat_2: 45.5,
        })
        .unwrap();
        let apex_y = setup.a * setup.rho0;
        Aea::inverse_unframed(&setup, 0.0, apex_y).unwrap_err();
    }

    #[test]
    fn inverse_accepts_a_valid_polar_cone_apex() {
        let ellipsoid = Ellipsoid::from_a_f(6_378_137.0, 1.0 / 298.257_222_101);
        for latitude in [90.0, -90.0] {
            let setup = Aea::setup(ellipsoid, AeaParams {
                lat_0: latitude,
                lat_1: latitude,
                lat_2: latitude,
            })
            .unwrap();
            let actual = Aea::inverse_unframed(&setup, 0.0, setup.a * setup.rho0).unwrap();
            assert_eq!(actual.0, 0.0);
            assert_eq!(actual.1, latitude.to_radians());
        }
    }
}
