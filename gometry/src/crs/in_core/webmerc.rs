#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! WGS84 Web Mercator (EPSG:3857) — spherical closed-form formulas.

use crate::boundary::geographic::WEB_MERCATOR_MAX_LATITUDE;
use crate::crs::WEB_MERCATOR_RADIUS;
use crate::crs::in_core::kernel::{Ellipsoid, ProjectionKernel, RAD_TO_DEG};
use crate::crs::in_core::params::{
    FrameSpec, MethodSpec, OperationSpec, Projected, ProjectionFrame, Requirement,
    admit_projection, epsg, param_assume_degrees, param_assume_unity,
};
use crate::error::Result;
use crate::geometry::GeometryErrorKind;

#[derive(Clone, Copy, Debug, Default)]
pub(in crate::crs) struct WebMercParams;

#[derive(Clone, Copy, Debug, Default)]
pub(in crate::crs) struct WebMerc;

#[derive(Clone, Copy, Debug)]
pub(in crate::crs) struct Setup {
    radius: f64,
    inv_radius: f64,
}

pub(in crate::crs) const WGS84_WEB_MERCATOR: Projected<WebMerc> = Projected {
    frame: ProjectionFrame::ZERO,
    setup: Setup {
        radius: WEB_MERCATOR_RADIUS,
        inv_radius: 1.0 / WEB_MERCATOR_RADIUS,
    },
};

pub(super) fn lonlat_to_web_mercator_xy(lon: f64, lat: f64) -> Result<(f64, f64)> {
    WGS84_WEB_MERCATOR.forward_xy(lon, lat)
}

pub(super) fn web_mercator_to_lonlat_xy(x: f64, y: f64) -> (f64, f64) {
    WGS84_WEB_MERCATOR
        .inverse_xy(x, y)
        .unwrap_or((f64::NAN, f64::NAN))
}

fn forward_unframed_setup(setup: Setup, lam_rad: f64, lat_rad: f64) -> Result<(f64, f64)> {
    let lat_deg = lat_rad * RAD_TO_DEG;
    if !(-WEB_MERCATOR_MAX_LATITUDE..=WEB_MERCATOR_MAX_LATITUDE).contains(&lat_deg) {
        return Err(GeometryErrorKind::WebMercatorLatitude(lat_deg).into());
    }
    let x = setup.radius * lam_rad;
    let y = if lat_rad == 0.0 {
        lat_rad
    } else {
        setup.radius * ((std::f64::consts::FRAC_PI_4 + lat_rad / 2.0).tan()).ln()
    };
    if !x.is_finite() || !y.is_finite() {
        return Err(GeometryErrorKind::NonFiniteCoordinate.into());
    }
    Ok((x, y))
}

fn inverse_unframed_setup(setup: Setup, x: f64, y: f64) -> (f64, f64) {
    let lam_rad = x / setup.radius;
    let lat_rad = if y == 0.0 {
        y
    } else {
        2.0 * (y * setup.inv_radius).exp().atan() - std::f64::consts::FRAC_PI_2
    };
    (lam_rad, lat_rad)
}

#[expect(
    clippy::unnecessary_wraps,
    reason = "OperationSpec::finalize returns Option for admission failure"
)]
const fn finalize_web_merc(params: WebMercParams) -> Option<WebMercParams> {
    Some(params)
}

impl ProjectionKernel for WebMerc {
    type Setup = Setup;
    type MethodParams = WebMercParams;

    const SPECS: &'static [OperationSpec<WebMercParams>] = &[OperationSpec {
        method: MethodSpec { code: "1024" },
        frame: FrameSpec {
            lon_0: epsg("8802"),
            x_0: epsg("8806"),
            y_0: epsg("8807"),
        },
        defaults: WebMercParams,
        params: &[
            param_assume_degrees(
                epsg("8801"),
                0.0,
                super::params::EPS10,
                Requirement::Optional,
            ),
            param_assume_unity(epsg("8805"), 1.0, 1e-12, Requirement::Optional),
        ],
        finalize: finalize_web_merc,
    }];

    fn setup(ellipsoid: Ellipsoid, _params: WebMercParams) -> Result<Self::Setup> {
        if (ellipsoid.a - WEB_MERCATOR_RADIUS).abs() > 1e-3 {
            return Err(GeometryErrorKind::projection(
                "ellipsoid semi-major axis must match WGS84 for pseudo-Mercator",
            ));
        }
        Ok(Setup {
            radius: ellipsoid.a,
            inv_radius: 1.0 / ellipsoid.a,
        })
    }

    fn forward_unframed(setup: &Self::Setup, lam_rad: f64, lat_rad: f64) -> Result<(f64, f64)> {
        forward_unframed_setup(*setup, lam_rad, lat_rad)
    }

    fn inverse_unframed(setup: &Self::Setup, x: f64, y: f64) -> Result<(f64, f64)> {
        Ok(inverse_unframed_setup(*setup, x, y))
    }
}

pub(super) fn admit(
    operation: &super::super::CrsCoordinateOperationInfo,
    ellipsoid: Ellipsoid,
) -> Option<Projected<WebMerc>> {
    admit_projection::<WebMerc>(operation, ellipsoid)
}
