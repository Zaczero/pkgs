//! Degree-based longitude/latitude admission and physical-pole normalization.

use std::collections::HashMap;
use std::ops::ControlFlow;

use super::{GeodeticPole, GeographicDiagnostic, GeographicError};
use crate::geometry::ShapeOwnerKey;
use crate::{GeometryErrorKind, Shape};

pub(crate) fn invalid_lonlat_error(lon: f64, lat: f64) -> crate::error::Error {
    GeometryErrorKind::message(format!(
        "invalid longitude/latitude ({lon}, {lat}); coordinates are (x, y) = (lon, lat) — use swap_xy() for latitude-first data"
    ))
}

pub(crate) fn geographic_admission_error(diagnostic: &GeographicDiagnostic) -> crate::error::Error {
    match diagnostic.source {
        GeographicError::NonFinite { .. } | GeographicError::LatitudeOutOfRange { .. } => {
            let coordinates = invalid_lonlat_error(diagnostic.native.x, diagnostic.native.y);
            let location = &diagnostic.location;
            if location.row.is_some()
                || !location.members.as_slice().is_empty()
                || location.ring.is_some()
                || location.coordinate != 0
            {
                GeometryErrorKind::message(format!("{coordinates} ({diagnostic})"))
            } else {
                coordinates
            }
        },
        GeographicError::InvalidAngularFactor { .. }
        | GeographicError::InvalidSweep { .. }
        | GeographicError::ChartTurnOverflow
        | GeographicError::AmbiguousFullTurnRing { .. } => {
            GeometryErrorKind::message(diagnostic.to_string())
        },
    }
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
pub(crate) fn normalized_geographic_pole(latitude: f64) -> Option<GeodeticPole> {
    match normalize_accepted_latitude(latitude) {
        MAX_LATITUDE => Some(GeodeticPole::North),
        MIN_LATITUDE => Some(GeodeticPole::South),
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
    let changes = matches!(
        shape.try_for_each_first_xy_point_run(|point| {
            if normalize_accepted_latitude(point.y).to_bits() == point.y.to_bits() {
                ControlFlow::Continue(())
            } else {
                ControlFlow::Break(())
            }
        }),
        ControlFlow::Break(())
    );
    if !changes {
        return shape.clone();
    }
    normalize_grid_shape(shape, &mut HashMap::new())
}

fn normalize_grid_shape(shape: &Shape, collections: &mut HashMap<ShapeOwnerKey, Shape>) -> Shape {
    match shape {
        Shape::GeometryCollection(collection) => {
            let owner = collection
                .members_are_shared()
                .then(|| ShapeOwnerKey::new(collection.as_slice()));
            if let Some(mapped) = owner.as_ref().and_then(|owner| collections.get(owner)) {
                return mapped.clone();
            }
            let mapped = Shape::GeometryCollection(
                collection
                    .try_map_members(|member| {
                        Ok::<_, crate::error::Error>(normalize_grid_shape(member, collections))
                    })
                    .expect("geographic latitude normalization preserves admitted collections"),
            );
            if let Some(owner) = owner {
                collections.insert(owner, mapped.clone());
            }
            mapped
        },
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Polygon(_)
        | Shape::MultiPolygon(_)
        | Shape::Empty(..) => shape
            .map_points(&|point| {
                Ok::<_, crate::error::Error>(
                    point.with_xy_raw(point.x, normalize_accepted_latitude(point.y)),
                )
            })
            .expect("geographic latitude normalization is an infallible finite-coordinate map"),
    }
}

/// Earth's authalic radius in meters, used by H3 and S2 cell-area calculations.
pub(crate) const EARTH_AUTHALIC_RADIUS_M: f64 = 6_371_007.180_918_475;

/// WGS84 ellipsoidal area (m²) of a longitude/latitude rectangle.
pub(crate) fn lonlat_rect_area_m2(bounds: crate::geometry::Bounds) -> f64 {
    let south_degrees = bounds.miny();
    let north_degrees = bounds.maxy();
    let latitude_delta_degrees = north_degrees - south_degrees;
    let latitude_delta = latitude_delta_degrees.to_radians();

    // Avoid subtracting endpoint q values, and derive the midpoint cosine
    // from colatitude in one-sided bands so narrow polar cells stay resolved.
    let cosine_midpoint = if south_degrees >= 0.0 {
        ((90.0 - north_degrees) + latitude_delta_degrees / 2.0)
            .to_radians()
            .sin()
    } else if north_degrees <= 0.0 {
        ((90.0 + south_degrees) + latitude_delta_degrees / 2.0)
            .to_radians()
            .sin()
    } else {
        f64::midpoint(south_degrees, north_degrees)
            .to_radians()
            .cos()
    };
    let delta_sine = 2.0 * cosine_midpoint * (latitude_delta / 2.0).sin();
    let sine_south = south_degrees.to_radians().sin();
    let sine_north = north_degrees.to_radians().sin();

    let flattening = crate::crs::WGS84_F;
    let eccentricity_squared = flattening * (2.0 - flattening);
    let eccentricity = eccentricity_squared.sqrt();
    let endpoint_product = sine_south * sine_north;
    let atanh_denominator = 1.0 - eccentricity_squared * endpoint_product;
    // This is q(north) - q(south) with the rational and atanh difference
    // identities applied before rounding.
    let rational_difference = delta_sine * (1.0 + eccentricity_squared * endpoint_product)
        / ((1.0 - eccentricity_squared * sine_south * sine_south)
            * (1.0 - eccentricity_squared * sine_north * sine_north));
    let atanh_difference = (eccentricity * delta_sine / atanh_denominator).atanh() / eccentricity;
    let authalic_q_difference =
        (1.0 - eccentricity_squared) * (rational_difference + atanh_difference);
    let longitude_width = (bounds.maxx() - bounds.minx()).to_radians();

    crate::crs::WGS84_A * crate::crs::WGS84_A * longitude_width * authalic_q_difference / 2.0
}

pub(crate) fn validate_lonlat_shape(shape: &Shape) -> crate::error::Result<()> {
    match shape.try_for_each_first_xy_point_run(|point| {
        match validate_lonlat_xy(point.x, point.y) {
            Ok(()) => ControlFlow::Continue(()),
            Err(error) => ControlFlow::Break(error),
        }
    }) {
        ControlFlow::Continue(()) => Ok(()),
        ControlFlow::Break(error) => Err(error),
    }
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

#[cfg(test)]
mod validate_lonlat_shape_tests {
    use super::validate_lonlat_shape;
    use crate::geometry::{
        CoordSeq, CoordinateAxes, GeometryErrorKind, GeometryKind, LineSeq, Point, Polygon, Ring,
        Shape,
    };

    fn xy_seq(xs: Vec<f64>, ys: Vec<f64>) -> CoordSeq {
        CoordSeq::try_from_vecs(xs, ys, None, None).unwrap()
    }

    fn domain_error(shape: &Shape) -> crate::error::Error {
        validate_lonlat_shape(shape).expect_err("expected a domain error")
    }

    fn assert_first_xy(error: &crate::error::Error, lon: f64, lat: f64) {
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
        assert_first_xy(&domain_error(&shape), 0.0, 95.0);
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
        assert_first_xy(&domain_error(&shape), 0.2, 95.0);
    }

    #[test]
    fn collection_reports_second_member() {
        let shape = Shape::geometry_collection(vec![
            Shape::Point(Point::new_raw_xy(0.0, 0.0)),
            Shape::Point(Point::new_raw_xy(0.0, 95.0)),
        ])
        .unwrap();
        assert_first_xy(&domain_error(&shape), 0.0, 95.0);
    }

    #[test]
    fn empty_shape_is_ok() {
        validate_lonlat_shape(&Shape::Empty(GeometryKind::Point, CoordinateAxes::XY))
            .expect("empty has no vertices");
    }
}
