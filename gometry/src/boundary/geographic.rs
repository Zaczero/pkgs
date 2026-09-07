//! Geographic (lon/lat) domain validation and small geo-rs conversions.
//!
//! `validate_lonlat_*` enforce the WGS84 angular domain before grid/S2 ops;
//! `point_xy` is the shared `Shape` -> coordinate adapter.
//! Re-exported at the crate root for `use super::*` and `crate::` consumers.

use pyo3::prelude::*;

use crate::py::errors::geometry_type_err;
use crate::{GeometryErrorKind, Shape};

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

/// Public geographic admission accepts a tiny floating-point overshoot at the
/// two latitude limits.  Canonicalize only that admitted exterior sliver: an
/// inward neighbour remains its stored latitude, while an accepted outward
/// neighbour denotes the physical pole rather than a distinct point beyond it.
pub(crate) fn normalize_accepted_latitude(latitude: f64) -> f64 {
    const EPSILON: f64 = 1.0e-12;
    if (MAX_LATITUDE..=MAX_LATITUDE + EPSILON).contains(&latitude) {
        MAX_LATITUDE
    } else if (MIN_LATITUDE - EPSILON..=MIN_LATITUDE).contains(&latitude) {
        MIN_LATITUDE
    } else {
        latitude
    }
}

/// Physical-pole identity after the geographic admission normalization.
/// Longitude is intentionally absent: every longitude spelling at either
/// normalized pole is the same spherical point.
pub(crate) fn normalized_geographic_pole(latitude: f64) -> Option<bool> {
    match normalize_accepted_latitude(latitude) {
        MAX_LATITUDE => Some(true),
        MIN_LATITUDE => Some(false),
        _ => None,
    }
}

/// Return the grid-topology image of a geographically admitted shape.
///
/// The public latitude boundary deliberately accepts a tiny exterior rounding
/// sliver.  At either pole that sliver has no distinct physical location, so
/// every grid carrier must see the same canonical coordinate before it derives
/// bounds, chains, or point witnesses.  The map is otherwise bit-preserving,
/// including longitude and Z/M ordinates.
pub(crate) fn normalize_accepted_geographic_latitudes(shape: &Shape) -> Shape {
    shape
        .map_points(&|point| {
            Ok::<_, crate::error::Error>(
                point.with_xy_raw(point.x, normalize_accepted_latitude(point.y)),
            )
        })
        .expect("geographic latitude normalization is an infallible finite-coordinate map")
}

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

pub(crate) fn validate_lonlat_shape(shape: &Shape) -> crate::error::Result<()> {
    shape.try_for_each_point(|point| validate_lonlat_xy(point.x, point.y))
}

pub(crate) fn validate_lonlat_xy(lon: f64, lat: f64) -> crate::error::Result<()> {
    const EPSILON: f64 = 1.0e-12;
    let lon_range = MIN_LONGITUDE - EPSILON..=MAX_LONGITUDE + EPSILON;
    let lat_range = MIN_LATITUDE - EPSILON..=MAX_LATITUDE + EPSILON;
    if !lon_range.contains(&lon) || !lat_range.contains(&lat) {
        return Err(invalid_lonlat_error(lon, lat));
    }
    Ok(())
}

pub(crate) fn point_xy(shape: &Shape) -> PyResult<(f64, f64)> {
    let Shape::Point(point) = shape else {
        return Err(geometry_type_err("expected Point geometry"));
    };
    Ok((point.x, point.y))
}

#[cfg(test)]
mod validate_lonlat_shape_tests {
    use super::validate_lonlat_shape;
    use crate::geometry::{
        CoordSeq, CoordinateAxes, GeometryErrorKind, GeometryKind, LineSeq, Point, Polygon, Ring,
        Shape,
    };

    fn xy_seq(xs: Vec<f64>, ys: Vec<f64>) -> CoordSeq {
        CoordSeq::from_vecs(xs, ys, None, None)
    }

    fn domain_error(shape: &Shape) -> crate::error::Error {
        validate_lonlat_shape(shape).expect_err("expected a domain error")
    }

    fn assert_first_xy(error: crate::error::Error, lon: f64, lat: f64) {
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
        ));
        let message = error.to_string();
        let expected = format!("invalid longitude/latitude ({lon}, {lat})");
        assert!(message.contains(&expected), "{message}");
        assert!(
            !message.contains("(10, 91)"),
            "later vertex must not replace the first: {message}"
        );
    }

    #[test]
    fn first_vertex_is_the_diagnostic() {
        let shape =
            Shape::LineString(LineSeq::try_new(xy_seq(vec![0.0, 10.0], vec![95.0, 91.0])).unwrap());
        assert_first_xy(domain_error(&shape), 0.0, 95.0);
    }

    #[test]
    fn polygon_reports_hole_after_in_domain_shell() {
        let shell = Ring::closed_coordseq(xy_seq(vec![0.0, 1.0, 1.0, 0.0, 0.0], vec![
            0.0, 0.0, 1.0, 1.0, 0.0,
        ]))
        .unwrap();
        let hole =
            Ring::closed_coordseq(xy_seq(vec![0.2, 0.3, 0.3, 0.2], vec![95.0, 0.2, 0.3, 95.0]))
                .unwrap();
        let shape = Shape::Polygon(Polygon::new(shell, vec![hole]));
        assert_first_xy(domain_error(&shape), 0.2, 95.0);
    }

    #[test]
    fn collection_reports_second_member() {
        let shape = Shape::geometry_collection(vec![
            Shape::Point(Point::new_raw_xy(0.0, 0.0)),
            Shape::Point(Point::new_raw_xy(0.0, 95.0)),
        ])
        .unwrap();
        assert_first_xy(domain_error(&shape), 0.0, 95.0);
    }

    #[test]
    fn empty_shape_is_ok() {
        validate_lonlat_shape(&Shape::Empty(GeometryKind::Point, CoordinateAxes::XY))
            .expect("empty has no vertices");
    }
}
