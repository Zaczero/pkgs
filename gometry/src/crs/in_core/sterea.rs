#![expect(
    clippy::many_single_char_names,
    reason = "EPSG Gauss-sphere equations use conventional C, K, R, and e notation"
)]
//! Ellipsoidal double oblique stereographic (EPSG method 9809).

use crate::crs::in_core::adjlon;
use crate::crs::in_core::kernel::{Ellipsoid, ProjectionKernel, eccentricity};
use crate::crs::in_core::params::{
    FrameSpec, MethodSpec, OperationSpec, ParamUnit, Projected, Requirement, admit_projection,
    epsg, param_field,
};
use crate::error::Result;
use crate::geometry::GeometryErrorKind;

#[derive(Clone, Copy, Debug, Default)]
pub(in crate::crs) struct StereaParams {
    lat_0: f64,
    k0: f64,
}
#[derive(Clone, Copy, Debug, Default)]
pub(in crate::crs) struct Sterea;
#[derive(Clone, Copy, Debug)]
pub(in crate::crs) struct Setup {
    c: f64,
    k: f64,
    e: f64,
    ratexp: f64,
    chi0: f64,
    sin_chi0: f64,
    cos_chi0: f64,
    r2: f64,
    k0: f64,
    a: f64,
}
fn set_lat_0(p: &mut StereaParams, v: f64) -> bool {
    if v.is_finite() && v.abs() < 90.0 {
        p.lat_0 = v;
        true
    } else {
        false
    }
}
fn set_k0(p: &mut StereaParams, v: f64) -> bool {
    if v.is_finite() && v > 0.0 {
        p.k0 = v;
        true
    } else {
        false
    }
}
fn finalize(p: StereaParams) -> Option<StereaParams> {
    (p.k0 > 0.0).then_some(p)
}
fn srat(v: f64, r: f64) -> f64 {
    ((1.0 - v) / (1.0 + v)).powf(r)
}
impl ProjectionKernel for Sterea {
    type Setup = Setup;
    type MethodParams = StereaParams;
    const SPECS: &'static [OperationSpec<StereaParams>] = &[OperationSpec {
        method: MethodSpec { code: "9809" },
        frame: FrameSpec {
            lon_0: epsg("8802"),
            x_0: epsg("8806"),
            y_0: epsg("8807"),
        },
        defaults: StereaParams {
            lat_0: 0.0,
            k0: 0.0,
        },
        params: &[
            param_field(
                epsg("8801"),
                ParamUnit::Degrees,
                set_lat_0,
                Requirement::Required,
            ),
            param_field(
                epsg("8805"),
                ParamUnit::Unity,
                set_k0,
                Requirement::Required,
            ),
        ],
        finalize,
    }];
    fn setup(el: Ellipsoid, p: StereaParams) -> Result<Setup> {
        let e = eccentricity(el);
        let es = e * e;
        let phi0 = p.lat_0.to_radians();
        let (sp, cp) = phi0.sin_cos();
        // Match PROJ's Gauss-sphere arrangement: its setup squares cos(phi0)
        // before applying this second product, making this term cos(phi0)^4.
        let c = (1.0 + es * cp.powi(4) / (1.0 - es)).sqrt();
        let chi0 = (sp / c).asin();
        let ratexp = 0.5 * c * e;
        let sr = srat(e * sp, ratexp);
        let k = (0.5 * chi0 + std::f64::consts::FRAC_PI_4).tan()
            / ((0.5 * phi0 + std::f64::consts::FRAC_PI_4).tan().powf(c) * sr);
        let r = (1.0 - es).sqrt() / (1.0 - es * sp * sp);
        if [c, k, r].iter().all(|v| v.is_finite()) {
            Ok(Setup {
                c,
                k,
                e,
                ratexp,
                chi0,
                sin_chi0: chi0.sin(),
                cos_chi0: chi0.cos(),
                r2: 2.0 * r,
                k0: p.k0,
                a: el.a,
            })
        } else {
            Err(GeometryErrorKind::projection(
                "invalid Oblique Stereographic parameters",
            ))
        }
    }
    fn forward_unframed(s: &Setup, lam: f64, phi: f64) -> Result<(f64, f64)> {
        let chi = 2.0
            * (s.k
                * (0.5 * phi + std::f64::consts::FRAC_PI_4).tan().powf(s.c)
                * srat(s.e * phi.sin(), s.ratexp))
            .atan()
            - std::f64::consts::FRAC_PI_2;
        let l = s.c * adjlon(lam);
        let (si, co) = chi.sin_cos();
        let denom = 1.0 + s.sin_chi0 * si + s.cos_chi0 * co * l.cos();
        if denom == 0.0 {
            return Err(GeometryErrorKind::projection(
                "coordinate is outside the Oblique Stereographic domain",
            ));
        }
        let scale = s.a * s.k0 * s.r2 / denom;
        let result = (
            scale * co * l.sin(),
            scale * (s.cos_chi0 * si - s.sin_chi0 * co * l.cos()),
        );
        if result.0.is_finite() && result.1.is_finite() {
            Ok(result)
        } else {
            Err(GeometryErrorKind::projection(
                "coordinate is outside the Oblique Stereographic domain",
            ))
        }
    }
    fn inverse_unframed(s: &Setup, x: f64, y: f64) -> Result<(f64, f64)> {
        let xx = x / (s.a * s.k0);
        let yy = y / (s.a * s.k0);
        let rho = xx.hypot(yy);
        let (chi, l) = if rho == 0.0 {
            (s.chi0, 0.0)
        } else {
            let c = 2.0 * rho.atan2(s.r2);
            let (sc, cc) = c.sin_cos();
            (
                (cc * s.sin_chi0 + yy * sc * s.cos_chi0 / rho).asin(),
                (xx * sc).atan2(rho * s.cos_chi0 * cc - yy * s.sin_chi0 * sc),
            )
        };
        let num = ((0.5 * chi + std::f64::consts::FRAC_PI_4).tan() / s.k).powf(1.0 / s.c);
        let mut phi = chi;
        // This cap is a defensive totality bound. No admitted finite-domain
        // input is known to exhaust it (including 1e9 m probes); the fixed-point
        // map converges before the cap on the reachable projection domain.
        for _ in 0..20 {
            let next = 2.0 * (num * srat(s.e * phi.sin(), -0.5 * s.e)).atan()
                - std::f64::consts::FRAC_PI_2;
            if (next - phi).abs() < 1e-14 {
                return Ok((adjlon(l / s.c), next));
            }
            phi = next;
        }
        Err(GeometryErrorKind::projection(
            "coordinate is outside the Oblique Stereographic domain",
        ))
    }
}
/// Grid-bearing modern ETRS89 -> RD New operations are rejected before this
/// admission site; this kernel accelerates only grid-free, same-datum paths.
pub(super) fn admit(
    op: &super::super::CrsCoordinateOperationInfo,
    e: Ellipsoid,
) -> Option<Projected<Sterea>> {
    admit_projection::<Sterea>(op, e)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn polar_origins_are_refused() {
        let mut params = StereaParams::default();
        assert!(!set_lat_0(&mut params, -90.0));
        assert!(!set_lat_0(&mut params, 90.0));
    }

    #[test]
    fn non_finite_forward_result_is_an_error() {
        let ellipsoid = Ellipsoid::from_a_f(6_377_397.155, 1.0 / 299.152_812_8);
        let setup = Sterea::setup(ellipsoid, StereaParams {
            lat_0: 52.156_160_555_555_55,
            k0: 0.999_907_9,
        })
        .unwrap();
        Sterea::forward_unframed(&setup, 0.0, 100.0_f64.to_radians()).unwrap_err();
    }
}
