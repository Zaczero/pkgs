#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! WGS84 Web Mercator (EPSG:3857) — spherical closed-form formulas.

use crate::crs::in_core::kernel::{DEG_TO_RAD, Ellipsoid, ProjectionKernel, RAD_TO_DEG};
use crate::crs::in_core::params::{
    FrameSpec, MethodSpec, OperationSpec, Projected, ProjectionFrame, Requirement,
    admit_projection, epsg, param_assume_degrees, param_assume_unity,
};
use crate::crs::{TransformFailure, WEB_MERCATOR_RADIUS};
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

pub(super) fn raw_forward_batch_indexed(
    x: &mut [f64],
    y: &mut [f64],
) -> std::result::Result<(), TransformFailure> {
    for (index, (lon, lat)) in std::iter::zip(x, y).enumerate() {
        let (out_x, out_y) = forward_unframed_setup(
            WGS84_WEB_MERCATOR.setup,
            *lon * DEG_TO_RAD,
            *lat * DEG_TO_RAD,
        )
        .map_err(|error| TransformFailure::at(error, index))?;
        if !out_x.is_finite() || !out_y.is_finite() {
            return Err(TransformFailure::at(
                GeometryErrorKind::NonFiniteCoordinate.into(),
                index,
            ));
        }
        (*lon, *lat) = (out_x, out_y);
    }
    Ok(())
}

pub(super) fn raw_inverse_batch_indexed(
    x: &mut [f64],
    y: &mut [f64],
) -> std::result::Result<(), TransformFailure> {
    for (index, (lon, lat)) in std::iter::zip(x, y).enumerate() {
        let (out_lon_rad, lat_rad) = inverse_unframed_setup(WGS84_WEB_MERCATOR.setup, *lon, *lat);
        let out_lon = out_lon_rad * RAD_TO_DEG;
        let out_lat = lat_rad * RAD_TO_DEG;
        if !out_lon.is_finite() || !out_lat.is_finite() {
            return Err(TransformFailure::at(
                GeometryErrorKind::NonFiniteCoordinate.into(),
                index,
            ));
        }
        (*lon, *lat) = (out_lon, out_lat);
    }
    Ok(())
}

fn forward_unframed_setup(setup: Setup, lam_rad: f64, lat_rad: f64) -> Result<(f64, f64)> {
    if !lat_rad.is_finite() {
        return Err(GeometryErrorKind::NonFiniteCoordinate.into());
    }
    if lat_rad.abs() >= std::f64::consts::FRAC_PI_2 {
        let latitude_deg = lat_rad * RAD_TO_DEG;
        return Err(GeometryErrorKind::projection_domain(format!(
            "latitude {latitude_deg:e} is outside the finite Web Mercator projection domain"
        )));
    }
    let x = setup.radius * lam_rad;
    let y = if lat_rad == 0.0 {
        lat_rad
    } else {
        setup.radius * lat_rad.tan().asinh()
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
        (y * setup.inv_radius).sinh().atan()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::ErrorKind;

    #[test]
    fn projection_poles_are_typed_domain_errors() {
        for latitude in [std::f64::consts::FRAC_PI_2, -std::f64::consts::FRAC_PI_2] {
            let error = WGS84_WEB_MERCATOR
                .forward_xy(0.0, latitude * RAD_TO_DEG)
                .expect_err("projection poles must not produce finite coordinates");
            assert!(matches!(
                error.kind(),
                ErrorKind::Geometry(GeometryErrorKind::ProjectionDomain(_))
            ));
        }
    }
}
