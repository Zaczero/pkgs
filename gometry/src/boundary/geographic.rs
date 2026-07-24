//! Geographic (lon/lat) domain validation and small geo-rs conversions.
//!
//! `validate_lonlat_*` enforce the WGS84 angular domain before grid/S2 ops;
//! `point_xy` is the shared `Shape` -> coordinate adapter.
//! Re-exported at the crate root for `use super::*` and `crate::` consumers.

use pyo3::prelude::*;

use crate::py::errors::geometry_type_err;
use crate::*;

pub(crate) fn invalid_lonlat_error(lon: f64, lat: f64) -> crate::error::Error {
    GeometryErrorKind::message(format!(
        "invalid longitude/latitude ({lon}, {lat}); coordinates are (x, y) = (lon, lat) — use swap_xy() for latitude-first data"
    ))
}

/// Minimum WGS84 longitude (degrees west).
pub(crate) const MIN_LONGITUDE: f64 = -180.0;
/// Maximum WGS84 longitude (degrees east).
pub(crate) const MAX_LONGITUDE: f64 = 180.0;
/// Minimum WGS84 latitude (degrees south).
pub(crate) const MIN_LATITUDE: f64 = -90.0;
/// Maximum WGS84 latitude (degrees north).
pub(crate) const MAX_LATITUDE: f64 = 90.0;

/// Web Mercator latitude domain edge (EPSG:3857 and slippy-map tiles).
pub(crate) const WEB_MERCATOR_MAX_LATITUDE: f64 = 85.051_128_779_806_6;

/// Earth's authalic radius in meters — the sphere model the H3 backend uses
/// for cell areas, shared by the S2 cell area so the two grids compare.
pub(crate) const EARTH_AUTHALIC_RADIUS_M: f64 = 6_371_007.180_918_475;

/// Spherical area (m²) of a lon/lat rectangle on the authalic Earth:
/// `R² * Δλ * (sin φ2 - sin φ1)` — exact for the sphere, the same frame
/// the H3/S2 cell areas use, so systems compare.
pub(crate) fn lonlat_rect_area_m2(bounds: crate::geometry::Bounds) -> f64 {
    let width = (bounds.maxx() - bounds.minx()).to_radians();
    let band = bounds.maxy().to_radians().sin() - bounds.miny().to_radians().sin();
    EARTH_AUTHALIC_RADIUS_M * EARTH_AUTHALIC_RADIUS_M * width * band
}

pub(crate) fn validate_lonlat_shape(shape: &Shape) -> PyResult<()> {
    match shape {
        Shape::Point(point) => validate_lonlat_point(*point),
        Shape::MultiPoint(points) => {
            for point in points {
                validate_lonlat_point(point)?;
            }
            Ok(())
        },
        Shape::LineString(points) => {
            for point in points {
                validate_lonlat_point(point)?;
            }
            Ok(())
        },
        Shape::MultiLineString(lines) => {
            for line in lines {
                for point in line {
                    validate_lonlat_point(point)?;
                }
            }
            Ok(())
        },
        Shape::Polygon(polygon) => validate_lonlat_polygon(polygon),
        Shape::MultiPolygon(polygons) => {
            for polygon in polygons {
                validate_lonlat_polygon(polygon)?;
            }
            Ok(())
        },
        Shape::GeometryCollection(geometries) => {
            for geometry in geometries {
                validate_lonlat_shape(geometry)?;
            }
            Ok(())
        },
        Shape::Empty(..) => Ok(()),
    }
}

pub(crate) fn validate_lonlat_polygon(polygon: &Polygon) -> PyResult<()> {
    for point in &polygon.shell {
        validate_lonlat_point(point)?;
    }
    for hole in polygon.holes.iter() {
        for point in hole {
            validate_lonlat_point(point)?;
        }
    }
    Ok(())
}

pub(crate) fn validate_lonlat_point(point: Point) -> PyResult<()> {
    validate_lonlat_xy(point.x, point.y)
}

pub(crate) fn validate_lonlat_xy(lon: f64, lat: f64) -> PyResult<()> {
    const EPSILON: f64 = 1.0e-12;
    let lon_range = MIN_LONGITUDE - EPSILON..=MAX_LONGITUDE + EPSILON;
    let lat_range = MIN_LATITUDE - EPSILON..=MAX_LATITUDE + EPSILON;
    if !lon_range.contains(&lon) || !lat_range.contains(&lat) {
        return Err(invalid_lonlat_error(lon, lat).into());
    }
    Ok(())
}

pub(crate) fn point_xy(shape: &Shape) -> PyResult<(f64, f64)> {
    let Shape::Point(point) = shape else {
        return Err(geometry_type_err("expected Point geometry"));
    };
    Ok((point.x, point.y))
}
