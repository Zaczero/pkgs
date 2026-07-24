#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! Ellipsoidal polar stereographic (EPSG methods 9810/9829).

use super::adjlon;
use super::kernel::{
    EPS10, Ellipsoid, ProjectionKernel, eccentricity, eccentricity_squared, phi2, tsfn,
};
use super::params::{
    FrameSpec, MethodSpec, OperationSpec, Projected, Requirement, admit_projection, epsg,
    param_field,
};
use crate::error::Result;
use crate::geometry::GeometryErrorKind;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::crs) enum Pole {
    North,
    South,
}

#[derive(Clone, Copy, Debug, Default)]
pub(in crate::crs) struct PolarStereoParams {
    lat_0_deg: f64,
    lat_ts_deg: f64,
    k0: f64,
}

#[derive(Clone, Copy, Debug, Default)]
pub(in crate::crs) struct PolarStereo;

#[derive(Clone, Copy, Debug)]
pub(in crate::crs) struct Setup {
    pub(super) e: f64,
    pub(super) akm1: f64,
    pub(super) a: f64,
    pub(super) pole: Pole,
}

pub(super) fn polar_stereo_setup(ellipsoid: Ellipsoid, params: PolarStereoParams) -> Option<Setup> {
    if (params.lat_0_deg.abs() - 90.0).abs() >= EPS10 {
        return None;
    }

    let es = eccentricity_squared(ellipsoid);
    let e = eccentricity(ellipsoid);
    let pole = if params.lat_0_deg < 0.0 {
        Pole::South
    } else {
        Pole::North
    };

    let phits = params.lat_ts_deg.abs().to_radians();
    let akm1 = if (phits - std::f64::consts::FRAC_PI_2).abs() < EPS10 {
        if params.k0 <= 0.0 {
            return None;
        }
        2.0 * params.k0 / ((1.0 + e).powf(1.0 + e) * (1.0 - e).powf(1.0 - e)).sqrt()
    } else {
        if phits <= 0.0 || phits >= std::f64::consts::FRAC_PI_2 {
            return None;
        }
        let (s, c) = phits.sin_cos();
        c / tsfn(s, c, e) / (1.0 - es * s * s).sqrt()
    };

    if !akm1.is_finite() || akm1 <= 0.0 {
        return None;
    }

    Some(Setup {
        e,
        akm1,
        a: ellipsoid.a,
        pole,
    })
}

pub(super) fn geographic_to_polar_stereo_unframed(
    lam_rad: f64,
    lat_rad: f64,
    setup: &Setup,
    ellipsoid_a: f64,
) -> Result<(f64, f64)> {
    let lam = adjlon(lam_rad);
    let phi = match setup.pole {
        Pole::North => lat_rad,
        Pole::South => -lat_rad,
    };

    if phi <= -std::f64::consts::FRAC_PI_2 {
        return Err(GeometryErrorKind::projection(
            "coordinate is outside the polar stereographic domain",
        ));
    }

    let rho = if (phi - std::f64::consts::FRAC_PI_2).abs() < 1e-15 {
        0.0
    } else {
        let (s, c) = phi.sin_cos();
        setup.akm1 * tsfn(s, c, setup.e)
    };

    let (sin_lam, cos_lam) = lam.sin_cos();
    let easting = ellipsoid_a * rho * sin_lam;
    let northing = ellipsoid_a
        * match setup.pole {
            Pole::North => -rho * cos_lam,
            Pole::South => rho * cos_lam,
        };

    if !easting.is_finite() || !northing.is_finite() {
        return Err(GeometryErrorKind::NonFiniteCoordinate.into());
    }
    Ok((easting, northing))
}

pub(super) fn polar_stereo_to_geographic_unframed(
    easting: f64,
    northing: f64,
    setup: &Setup,
    ellipsoid_a: f64,
) -> Result<(f64, f64)> {
    let x = easting / ellipsoid_a;
    let y = northing / ellipsoid_a;
    let yy = match setup.pole {
        Pole::North => -y,
        Pole::South => y,
    };
    let rho = (x * x + yy * yy).sqrt();

    let phi_abs = if rho <= EPS10 {
        std::f64::consts::FRAC_PI_2
    } else {
        phi2(rho / setup.akm1, setup.e).ok_or_else(|| {
            GeometryErrorKind::projection("coordinate is outside the polar stereographic domain")
        })?
    };

    let lat = match setup.pole {
        Pole::North => phi_abs,
        Pole::South => -phi_abs,
    };
    let lam = if x == 0.0 && yy == 0.0 {
        0.0
    } else {
        x.atan2(yy)
    };

    let lam = adjlon(lam);
    if !lam.is_finite() || !lat.is_finite() {
        return Err(GeometryErrorKind::NonFiniteCoordinate.into());
    }
    Ok((lam, lat))
}

fn set_lat_0(params: &mut PolarStereoParams, value: f64) -> bool {
    if !value.is_finite() || (value.abs() - 90.0).abs() >= EPS10 {
        return false;
    }
    params.lat_0_deg = value;
    params.lat_ts_deg = value;
    true
}

fn set_k0(params: &mut PolarStereoParams, value: f64) -> bool {
    if value.is_finite() && value > 0.0 {
        params.k0 = value;
        true
    } else {
        false
    }
}

fn set_lat_ts(params: &mut PolarStereoParams, value: f64) -> bool {
    if !value.is_finite() || value == 0.0 || value.abs() > 90.0 {
        return false;
    }
    params.lat_ts_deg = value;
    true
}

fn finalize_polar_a(params: PolarStereoParams) -> Option<PolarStereoParams> {
    (params.k0 > 0.0).then_some(params)
}

#[expect(
    clippy::unnecessary_wraps,
    reason = "OperationSpec::finalize returns Option for admission failure"
)]
fn finalize_polar_b(mut params: PolarStereoParams) -> Option<PolarStereoParams> {
    params.lat_0_deg = if params.lat_ts_deg > 0.0 { 90.0 } else { -90.0 };
    Some(params)
}

impl ProjectionKernel for PolarStereo {
    type Setup = Setup;
    type MethodParams = PolarStereoParams;

    const SPECS: &'static [OperationSpec<PolarStereoParams>] = &[
        OperationSpec {
            method: MethodSpec { code: "9810" },
            frame: FrameSpec {
                lon_0: epsg("8802"),
                x_0: epsg("8806"),
                y_0: epsg("8807"),
            },
            defaults: PolarStereoParams {
                lat_0_deg: 90.0,
                lat_ts_deg: 90.0,
                k0: 1.0,
            },
            params: &[
                param_field(
                    epsg("8801"),
                    super::params::ParamUnit::Degrees,
                    set_lat_0,
                    Requirement::Required,
                ),
                param_field(
                    epsg("8805"),
                    super::params::ParamUnit::Unity,
                    set_k0,
                    Requirement::Required,
                ),
            ],
            finalize: finalize_polar_a,
        },
        OperationSpec {
            method: MethodSpec { code: "9829" },
            frame: FrameSpec {
                lon_0: epsg("8833"),
                x_0: epsg("8806"),
                y_0: epsg("8807"),
            },
            defaults: PolarStereoParams {
                lat_0_deg: 90.0,
                lat_ts_deg: 70.0,
                k0: 1.0,
            },
            params: &[param_field(
                epsg("8832"),
                super::params::ParamUnit::Degrees,
                set_lat_ts,
                Requirement::Required,
            )],
            finalize: finalize_polar_b,
        },
    ];

    fn setup(ellipsoid: Ellipsoid, params: PolarStereoParams) -> Result<Self::Setup> {
        polar_stereo_setup(ellipsoid, params)
            .ok_or_else(|| GeometryErrorKind::projection("invalid polar stereographic parameters"))
    }

    fn forward_unframed(setup: &Self::Setup, lam_rad: f64, lat_rad: f64) -> Result<(f64, f64)> {
        geographic_to_polar_stereo_unframed(lam_rad, lat_rad, setup, setup.a)
    }

    fn inverse_unframed(setup: &Self::Setup, x: f64, y: f64) -> Result<(f64, f64)> {
        polar_stereo_to_geographic_unframed(x, y, setup, setup.a)
    }
}

pub(super) fn admit(
    operation: &super::super::CrsCoordinateOperationInfo,
    ellipsoid: Ellipsoid,
) -> Option<Projected<PolarStereo>> {
    admit_projection::<PolarStereo>(operation, ellipsoid)
}
