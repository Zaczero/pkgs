//! Ellipsoidal Lambert Azimuthal Equal Area (EPSG method 9820).

use crate::crs::in_core::adjlon;
use crate::crs::in_core::kernel::{
    Authalic, EPS10, Ellipsoid, ProjectionKernel, eccentricity_squared,
};
use crate::crs::in_core::params::{
    FrameSpec, MethodSpec, OperationSpec, ParamUnit, Projected, Requirement, admit_projection,
    epsg, param_field,
};
use crate::error::Result;
use crate::geometry::GeometryErrorKind;

#[derive(Clone, Copy, Debug, Default)]
pub(in crate::crs) struct LaeaParams {
    lat_0: f64,
}
#[derive(Clone, Copy, Debug, Default)]
pub(in crate::crs) struct Laea;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Mode {
    North,
    South,
    Equatorial,
    Oblique,
}
#[derive(Clone, Copy, Debug)]
pub(in crate::crs) struct Setup {
    auth: Authalic,
    mode: Mode,
    sin_b1: f64,
    cos_b1: f64,
    xmf: f64,
    ymf: f64,
    dd: f64,
    rq: f64,
    a: f64,
    lat_0: f64,
}
fn set_lat_0(p: &mut LaeaParams, v: f64) -> bool {
    if v.is_finite() && v.abs() <= 90.0 {
        p.lat_0 = v;
        true
    } else {
        false
    }
}
#[expect(
    clippy::unnecessary_wraps,
    reason = "OperationSpec finalize has a fallible common signature"
)]
const fn finalize(p: LaeaParams) -> Option<LaeaParams> {
    Some(p)
}
impl ProjectionKernel for Laea {
    type Setup = Setup;
    type MethodParams = LaeaParams;
    const SPECS: &'static [OperationSpec<LaeaParams>] = &[OperationSpec {
        method: MethodSpec { code: "9820" },
        frame: FrameSpec {
            lon_0: epsg("8802"),
            x_0: epsg("8806"),
            y_0: epsg("8807"),
        },
        defaults: LaeaParams { lat_0: 0.0 },
        params: &[param_field(
            epsg("8801"),
            ParamUnit::Degrees,
            set_lat_0,
            Requirement::Required,
        )],
        finalize,
    }];
    fn setup(el: Ellipsoid, p: LaeaParams) -> Result<Setup> {
        let auth = Authalic::new(el).ok_or_else(|| {
            GeometryErrorKind::projection("invalid Lambert Azimuthal Equal Area ellipsoid")
        })?;
        let phi0 = p.lat_0.to_radians();
        let t = phi0.abs();
        let mode = if (t - std::f64::consts::FRAC_PI_2).abs() < EPS10 {
            if phi0 < 0.0 { Mode::South } else { Mode::North }
        } else if t < EPS10 {
            Mode::Equatorial
        } else {
            Mode::Oblique
        };
        let rq = (0.5 * auth.qp).sqrt();
        let (mut sin_b1, mut cos_b1, mut xmf, mut ymf, mut dd) = (0.0, 0.0, 1.0, 1.0, 1.0);
        match mode {
            Mode::North | Mode::South => {},
            Mode::Equatorial => {
                dd = 1.0 / rq;
                ymf = 0.5 * auth.qp;
            },
            Mode::Oblique => {
                let b1 = auth.beta(phi0);
                (sin_b1, cos_b1) = b1.sin_cos();
                let (sp, cp) = phi0.sin_cos();
                dd = cp / ((1.0 - eccentricity_squared(el) * sp * sp).sqrt() * rq * cos_b1);
                xmf = rq * dd;
                ymf = rq / dd;
            },
        }
        Ok(Setup {
            auth,
            mode,
            sin_b1,
            cos_b1,
            xmf,
            ymf,
            dd,
            rq,
            a: el.a,
            lat_0: phi0,
        })
    }
    #[expect(
        clippy::float_cmp,
        reason = "only the exact direction-ambiguous antipode is excluded"
    )]
    fn forward_unframed(s: &Setup, lam: f64, phi: f64) -> Result<(f64, f64)> {
        let l = adjlon(lam);
        // LAEA's antipodal radius is finite, but its boundary point depends on
        // approach direction: the geographic antipode has no unique projected
        // coordinate. Reject that single point consistently in every aspect;
        // adjacent representable coordinates remain valid.
        if phi == -s.lat_0 && l.abs() == std::f64::consts::PI {
            return Err(GeometryErrorKind::projection(
                "coordinate is outside the Lambert Azimuthal Equal Area domain",
            ));
        }
        let (sl, cl) = l.sin_cos();
        let beta = s.auth.beta(phi);
        let (sb, cb) = beta.sin_cos();
        let (x, y) = match s.mode {
            Mode::Oblique => {
                // `1 + u dot v` as `|u + v|^2 / 2` avoids cancellation at
                // representable points immediately beside the antipode.
                let d = 0.5
                    * ((s.cos_b1 + cb * cl).powi(2) + (cb * sl).powi(2) + (s.sin_b1 + sb).powi(2));
                if d == 0.0 {
                    return Err(GeometryErrorKind::projection(
                        "coordinate is outside the Lambert Azimuthal Equal Area domain",
                    ));
                }
                let b = (2.0 / d).sqrt();
                (
                    s.xmf * b * cb * sl,
                    s.ymf * b * (s.cos_b1 * sb - s.sin_b1 * cb * cl),
                )
            },
            Mode::Equatorial => {
                let d = 0.5 * ((1.0 + cb * cl).powi(2) + (cb * sl).powi(2) + sb.powi(2));
                if d == 0.0 {
                    return Err(GeometryErrorKind::projection(
                        "coordinate is outside the Lambert Azimuthal Equal Area domain",
                    ));
                }
                let b = (2.0 / d).sqrt();
                (b * cb * sl, b * sb * s.ymf)
            },
            Mode::North | Mode::South => {
                let at_antipode = if s.mode == Mode::North {
                    phi + std::f64::consts::FRAC_PI_2
                } else {
                    phi - std::f64::consts::FRAC_PI_2
                };
                if at_antipode == 0.0 {
                    return Err(GeometryErrorKind::projection(
                        "coordinate is outside the Lambert Azimuthal Equal Area domain",
                    ));
                }
                let q = if s.mode == Mode::North {
                    s.auth.qp - s.auth.q(phi.sin())
                } else {
                    s.auth.qp + s.auth.q(phi.sin())
                };
                let b = q.max(0.0).sqrt();
                (b * sl, cl * if s.mode == Mode::South { b } else { -b })
            },
        };
        Ok((s.a * x, s.a * y))
    }
    fn inverse_unframed(s: &Setup, x: f64, y: f64) -> Result<(f64, f64)> {
        let (mut xx, mut yy) = (x / s.a, y / s.a);
        let ab = match s.mode {
            Mode::Equatorial | Mode::Oblique => {
                xx /= s.dd;
                yy *= s.dd;
                let rho = xx.hypot(yy);
                if rho < EPS10 {
                    return Ok((0.0, s.lat_0));
                }
                let arg = 0.5 * rho / s.rq;
                if arg > 1.0 {
                    return Err(GeometryErrorKind::projection(
                        "coordinate is outside the Lambert Azimuthal Equal Area domain",
                    ));
                }
                let ce = 2.0 * arg.asin();
                let (sc, cc) = ce.sin_cos();
                xx *= sc;
                if s.mode == Mode::Oblique {
                    let ab = cc * s.sin_b1 + yy * sc * s.cos_b1 / rho;
                    yy = rho * s.cos_b1 * cc - yy * s.sin_b1 * sc;
                    ab
                } else {
                    let ab = yy * sc / rho;
                    yy = rho * cc;
                    ab
                }
            },
            Mode::North => {
                yy = -yy;
                let q = xx * xx + yy * yy;
                if q == 0.0 {
                    return Ok((0.0, s.lat_0));
                }
                1.0 - q / s.auth.qp
            },
            Mode::South => {
                let q = xx * xx + yy * yy;
                if q == 0.0 {
                    return Ok((0.0, s.lat_0));
                }
                -(1.0 - q / s.auth.qp)
            },
        };
        if ab.abs() > 1.0 + 1e-12 {
            return Err(GeometryErrorKind::projection(
                "coordinate is outside the Lambert Azimuthal Equal Area domain",
            ));
        }
        Ok((
            adjlon(xx.atan2(yy)),
            s.auth.inverse_beta(ab.clamp(-1.0, 1.0).asin()),
        ))
    }
}
pub(super) fn admit(
    op: &super::super::CrsCoordinateOperationInfo,
    e: Ellipsoid,
) -> Option<Projected<Laea>> {
    admit_projection::<Laea>(op, e)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn equatorial_mode_round_trips_without_an_epsg_admission() {
        let ellipsoid = Ellipsoid::from_a_f(6_378_137.0, 1.0 / 298.257_223_563);
        let setup = Laea::setup(ellipsoid, LaeaParams { lat_0: 0.0 }).unwrap();
        assert_eq!(setup.mode, Mode::Equatorial);
        let expected = (12.0_f64.to_radians(), 35.0_f64.to_radians());
        let xy = Laea::forward_unframed(&setup, expected.0, expected.1).unwrap();
        let actual = Laea::inverse_unframed(&setup, xy.0, xy.1).unwrap();
        assert!((actual.0 - expected.0).abs() < 1e-13);
        assert!((actual.1 - expected.1).abs() < 1e-13);
    }

    #[test]
    fn polar_and_oblique_exact_antipodes_are_rejected_only_at_the_point() {
        let ellipsoid = Ellipsoid::from_a_f(6_378_137.0, 1.0 / 298.257_223_563);
        for lat_0 in [90.0, -90.0] {
            let setup = Laea::setup(ellipsoid, LaeaParams { lat_0 }).unwrap();
            let antipode = -lat_0;
            Laea::forward_unframed(&setup, 0.0, antipode.to_radians()).unwrap_err();
            let interior = f64::midpoint(antipode, 0.0);
            Laea::forward_unframed(&setup, 0.0, interior.to_radians()).unwrap();
        }

        let lat_0 = 52.0_f64;
        let setup = Laea::setup(ellipsoid, LaeaParams { lat_0 }).unwrap();
        Laea::forward_unframed(&setup, std::f64::consts::PI, -lat_0.to_radians()).unwrap_err();
        for latitude in [
            f64::next_up(-lat_0).to_radians(),
            f64::next_down(-lat_0).to_radians(),
        ] {
            Laea::forward_unframed(&setup, std::f64::consts::PI, latitude).unwrap();
        }
    }
}
