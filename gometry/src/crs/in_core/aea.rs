#![expect(
    clippy::many_single_char_names,
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
    epsg, param_field,
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

const fn set_lat_0(p: &mut AeaParams, v: f64) -> bool {
    if v.is_finite() {
        p.lat_0 = v;
        true
    } else {
        false
    }
}
const fn set_lat_1(p: &mut AeaParams, v: f64) -> bool {
    if v.is_finite() {
        p.lat_1 = v;
        true
    } else {
        false
    }
}
const fn set_lat_2(p: &mut AeaParams, v: f64) -> bool {
    if v.is_finite() {
        p.lat_2 = v;
        true
    } else {
        false
    }
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
            return Err(GeometryErrorKind::projection(
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
    fn inverse_unframed(s: &Setup, x: f64, y: f64) -> Result<(f64, f64)> {
        let mut xx = x / s.a;
        let mut yy = s.rho0 - y / s.a;
        let mut rho = xx.hypot(yy);
        if rho == 0.0 {
            return Ok((
                0.0,
                if s.n > 0.0 {
                    std::f64::consts::FRAC_PI_2
                } else {
                    -std::f64::consts::FRAC_PI_2
                },
            ));
        }
        if s.n < 0.0 {
            rho = -rho;
            xx = -xx;
            yy = -yy;
        }
        let qs = (s.c - (rho / s.dd).powi(2)) / s.n;
        let phi = if (s.auth.qp - qs.abs()).abs() > 1e-7 {
            if qs.abs() > s.auth.qp {
                return Err(GeometryErrorKind::projection(
                    "coordinate is outside the Albers Equal Area domain",
                ));
            }
            s.auth.inverse_beta((qs / s.auth.qp).asin())
        } else if qs < 0.0 {
            -std::f64::consts::FRAC_PI_2
        } else {
            std::f64::consts::FRAC_PI_2
        };
        Ok((adjlon(xx.atan2(yy) / s.n), phi))
    }
}
pub(super) fn admit(
    op: &super::super::CrsCoordinateOperationInfo,
    e: Ellipsoid,
) -> Option<Projected<Aea>> {
    admit_projection::<Aea>(op, e)
}
