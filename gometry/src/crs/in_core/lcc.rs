#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! Ellipsoidal Lambert Conformal Conic (EPSG methods 9801/9802).

use crate::crs::in_core::adjlon;
use crate::crs::in_core::kernel::{
    EPS10, Ellipsoid, ProjectionKernel, eccentricity, eccentricity_squared, msfn, phi2, tsfn,
};
use crate::crs::in_core::params::{
    FrameSpec, MethodSpec, OperationSpec, Projected, Requirement, admit_projection, epsg,
    param_field,
};
use crate::error::Result;
use crate::geometry::GeometryErrorKind;

#[derive(Clone, Copy, Debug, Default)]
pub(in crate::crs) struct LccParams {
    lat_0_deg: f64,
    lat_1_deg: f64,
    lat_2_deg: f64,
    k0: f64,
}

#[derive(Clone, Copy, Debug, Default)]
pub(in crate::crs) struct Lcc;

#[derive(Clone, Copy, Debug)]
pub(in crate::crs) struct Setup {
    pub(super) e: f64,
    pub(super) n: f64,
    pub(super) inv_n: f64,
    pub(super) f: f64,
    pub(super) rho0: f64,
    pub(super) ak: f64,
    pub(super) inv_ak: f64,
}

pub(super) fn lcc_setup(ellipsoid: Ellipsoid, params: LccParams) -> Option<Setup> {
    if params.k0 <= 0.0 {
        return None;
    }

    let es = eccentricity_squared(ellipsoid);
    let e = eccentricity(ellipsoid);

    let phi0 = params.lat_0_deg.to_radians();
    let phi1 = params.lat_1_deg.to_radians();
    let phi2 = params.lat_2_deg.to_radians();

    if (phi1 + phi2).abs() < EPS10 {
        return None;
    }
    if phi1.abs() >= std::f64::consts::FRAC_PI_2 || phi2.abs() >= std::f64::consts::FRAC_PI_2 {
        return None;
    }

    let (s1, c1) = phi1.sin_cos();
    let m1 = msfn(s1, c1, es);
    let t1 = tsfn(s1, c1, e);

    let n = if (phi1 - phi2).abs() >= EPS10 {
        let (s2, c2) = phi2.sin_cos();
        let m2 = msfn(s2, c2, es);
        let t2 = tsfn(s2, c2, e);
        if t1 <= 0.0 || t2 <= 0.0 || !t1.is_finite() || !t2.is_finite() {
            return None;
        }
        (m1 / m2).ln() / (t1 / t2).ln()
    } else {
        s1
    };

    if !n.is_finite() {
        return None;
    }

    let f = m1 / (n * t1.powf(n));
    if !f.is_finite() || f <= 0.0 {
        return None;
    }

    let rho0 = if (phi0.abs() - std::f64::consts::FRAC_PI_2).abs() < EPS10 {
        0.0
    } else {
        let (s0, c0) = phi0.sin_cos();
        f * tsfn(s0, c0, e).powf(n)
    };

    let ak = ellipsoid.a * params.k0;
    Some(Setup {
        e,
        n,
        inv_n: 1.0 / n,
        f,
        rho0,
        ak,
        inv_ak: 1.0 / ak,
    })
}

pub(super) fn geographic_to_lcc_unframed(
    lam_rad: f64,
    lat_rad: f64,
    setup: &Setup,
) -> Result<(f64, f64)> {
    let lam = adjlon(lam_rad);
    let phi = lat_rad;

    let rho = if (phi.abs() - std::f64::consts::FRAC_PI_2).abs() < EPS10 {
        if phi * setup.n <= 0.0 {
            return Err(GeometryErrorKind::projection(
                "coordinate is outside the Lambert Conformal Conic domain",
            ));
        }
        0.0
    } else {
        let (s, c) = phi.sin_cos();
        setup.f * tsfn(s, c, setup.e).powf(setup.n)
    };

    let theta = setup.n * lam;
    let (sin_theta, cos_theta) = theta.sin_cos();
    let easting = setup.ak * rho * sin_theta;
    let northing = setup.ak * (setup.rho0 - rho * cos_theta);

    if !easting.is_finite() || !northing.is_finite() {
        return Err(GeometryErrorKind::NonFiniteCoordinate.into());
    }
    Ok((easting, northing))
}

pub(super) fn lcc_to_geographic_unframed(
    easting: f64,
    northing: f64,
    setup: &Setup,
) -> Result<(f64, f64)> {
    let mut x = easting * setup.inv_ak;
    let mut y = setup.rho0 - northing * setup.inv_ak;
    let mut rho = (x * x + y * y).sqrt();

    let (phi, lam) = if rho != 0.0 {
        if setup.n < 0.0 {
            rho = -rho;
            x = -x;
            y = -y;
        }
        let phi = phi2((rho / setup.f).powf(setup.inv_n), setup.e).ok_or_else(|| {
            GeometryErrorKind::projection(
                "coordinate is outside the Lambert Conformal Conic domain",
            )
        })?;
        let lam = x.atan2(y) / setup.n;
        (phi, lam)
    } else if setup.n > 0.0 {
        (std::f64::consts::FRAC_PI_2, 0.0)
    } else {
        (-std::f64::consts::FRAC_PI_2, 0.0)
    };

    let lam = adjlon(lam);
    if !lam.is_finite() || !phi.is_finite() {
        return Err(GeometryErrorKind::NonFiniteCoordinate.into());
    }
    Ok((lam, phi))
}

fn set_lat_0(params: &mut LccParams, value: f64) -> bool {
    value.is_finite().then(|| {
        params.lat_0_deg = value;
        true
    }) == Some(true)
}

fn set_lat_1(params: &mut LccParams, value: f64) -> bool {
    value.is_finite().then(|| {
        params.lat_1_deg = value;
        true
    }) == Some(true)
}

fn set_lat_2(params: &mut LccParams, value: f64) -> bool {
    value.is_finite().then(|| {
        params.lat_2_deg = value;
        true
    }) == Some(true)
}

fn set_k0(params: &mut LccParams, value: f64) -> bool {
    (value.is_finite() && value > 0.0).then(|| {
        params.k0 = value;
        true
    }) == Some(true)
}

fn finalize_lcc_1sp(mut params: LccParams) -> Option<LccParams> {
    params.lat_1_deg = params.lat_0_deg;
    params.lat_2_deg = params.lat_0_deg;
    (params.k0 > 0.0).then_some(params)
}

fn finalize_lcc_2sp(params: LccParams) -> Option<LccParams> {
    (params.k0 > 0.0).then_some(params)
}

impl ProjectionKernel for Lcc {
    type Setup = Setup;
    type MethodParams = LccParams;

    const SPECS: &'static [OperationSpec<LccParams>] = &[
        OperationSpec {
            method: MethodSpec { code: "9801" },
            frame: FrameSpec {
                lon_0: epsg("8802"),
                x_0: epsg("8806"),
                y_0: epsg("8807"),
            },
            defaults: LccParams {
                lat_0_deg: 0.0,
                lat_1_deg: 0.0,
                lat_2_deg: 0.0,
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
            finalize: finalize_lcc_1sp,
        },
        OperationSpec {
            method: MethodSpec { code: "9802" },
            frame: FrameSpec {
                lon_0: epsg("8822"),
                x_0: epsg("8826"),
                y_0: epsg("8827"),
            },
            defaults: LccParams {
                lat_0_deg: 0.0,
                lat_1_deg: 0.0,
                lat_2_deg: 0.0,
                k0: 1.0,
            },
            params: &[
                param_field(
                    epsg("8821"),
                    super::params::ParamUnit::Degrees,
                    set_lat_0,
                    Requirement::Required,
                ),
                param_field(
                    epsg("8823"),
                    super::params::ParamUnit::Degrees,
                    set_lat_1,
                    Requirement::Required,
                ),
                param_field(
                    epsg("8824"),
                    super::params::ParamUnit::Degrees,
                    set_lat_2,
                    Requirement::Required,
                ),
            ],
            finalize: finalize_lcc_2sp,
        },
    ];

    fn setup(ellipsoid: Ellipsoid, params: LccParams) -> Result<Self::Setup> {
        lcc_setup(ellipsoid, params).ok_or_else(|| {
            GeometryErrorKind::projection("invalid Lambert Conformal Conic parameters")
        })
    }

    fn forward_unframed(setup: &Self::Setup, lam_rad: f64, lat_rad: f64) -> Result<(f64, f64)> {
        geographic_to_lcc_unframed(lam_rad, lat_rad, setup)
    }

    fn inverse_unframed(setup: &Self::Setup, x: f64, y: f64) -> Result<(f64, f64)> {
        lcc_to_geographic_unframed(x, y, setup)
    }
}

pub(super) fn admit(
    operation: &super::super::CrsCoordinateOperationInfo,
    ellipsoid: Ellipsoid,
) -> Option<Projected<Lcc>> {
    admit_projection::<Lcc>(operation, ellipsoid)
}
