use geographiclib_rs::Geodesic;

use crate::crs::geodesic::{
    CrsError, DirectGeodesic as _, Result, ensure_geographic_lonlat, inverse_azimuths,
    inverse_distance_azimuths, with_geodesic,
};

/// Copy the cached ellipsoid for a CRS once (batch nav kernels borrow it).
pub(crate) fn geodesic_for_crs(crs: &str) -> Result<Geodesic> {
    with_geodesic(crs, |geodesic| Ok(*geodesic))
}

/// Initial geodesic bearing (degrees clockwise from north, `0..360`) on a
/// resolved ellipsoid — azimuth-only inverse grade.
pub(crate) fn geodesic_bearing(
    geodesic: &Geodesic,
    lon1: f64,
    lat1: f64,
    lon2: f64,
    lat2: f64,
) -> Result<f64> {
    ensure_geographic_lonlat(lon1, lat1)?;
    ensure_geographic_lonlat(lon2, lat2)?;
    let (azimuth, _) = inverse_azimuths(geodesic, lon1, lat1, lon2, lat2);
    // geographiclib azimuths land in (-180, 180]; the branch normalize
    // is bit-identical to `rem_euclid(360)` there without the `fmod`.
    finite(azimuth, "geodesic bearing").map(|azimuth| {
        if azimuth < 0.0 {
            azimuth + 360.0
        } else {
            azimuth
        }
    })
}

/// Initial geodesic bearing (degrees clockwise from north, `0..360`).
pub(crate) fn geodesic_bearing_crs(
    crs: &str,
    lon1: f64,
    lat1: f64,
    lon2: f64,
    lat2: f64,
) -> Result<f64> {
    with_geodesic(crs, |geodesic| {
        geodesic_bearing(geodesic, lon1, lat1, lon2, lat2)
    })
}

/// Destination lon/lat from a point along `azimuth` for `meters` (direct
/// problem). Lat/lon-only direct grade — final azimuth is not exposed.
pub(crate) fn geodesic_destination(
    geodesic: &Geodesic,
    lon: f64,
    lat: f64,
    azimuth: f64,
    meters: f64,
) -> Result<(f64, f64)> {
    ensure_geographic_lonlat(lon, lat)?;
    if crate::geometry::same_topological_coordinate(meters, 0.0) {
        return Ok((lon, lat));
    }
    let (lat2, lon2): (f64, f64) = geodesic.direct(lat, lon, azimuth, meters);
    finite(lon2, "geodesic destination")?;
    finite(lat2, "geodesic destination")?;
    Ok((lon2, lat2))
}

/// Destination lon/lat from a point along `azimuth` for `meters` (direct
/// problem).
pub(crate) fn geodesic_destination_crs(
    crs: &str,
    lon: f64,
    lat: f64,
    azimuth: f64,
    meters: f64,
) -> Result<(f64, f64)> {
    with_geodesic(crs, |geodesic| {
        geodesic_destination(geodesic, lon, lat, azimuth, meters)
    })
}

/// Point-between kernel: one inverse (distance+azimuth) + optional direct.
///
/// Returns `(lon, lat, ratio)` where `ratio` is the clamped fraction used
/// for the step (and for Z/M lerp at the call site). Endpoint short-circuits
/// return the input coordinates with ratio 0 or 1 — callers that need the
/// original `Point` (Z/M) should prefer those endpoints over the bare lon/lat.
pub(crate) fn geodesic_point_between(
    geodesic: &Geodesic,
    lon1: f64,
    lat1: f64,
    lon2: f64,
    lat2: f64,
    distance: f64,
    normalized: bool,
) -> Result<(f64, f64, f64)> {
    ensure_geographic_lonlat(lon1, lat1)?;
    ensure_geographic_lonlat(lon2, lat2)?;
    // One inverse yields both the total length (absolute-distance ratio) and
    // the forward azimuth for the direct step — never a separate distance-only
    // inverse first.
    let (total, azimuth, _) = inverse_distance_azimuths(geodesic, lon1, lat1, lon2, lat2);
    let total = finite(total, "geodesic distance")?;
    let ratio = if normalized {
        distance
    } else if total == 0.0 {
        0.0
    } else {
        distance / total
    }
    .clamp(0.0, 1.0);
    if crate::geometry::same_topological_coordinate(ratio, 0.0) {
        return Ok((lon1, lat1, ratio));
    }
    if crate::geometry::same_topological_coordinate(ratio, 1.0) {
        return Ok((lon2, lat2, ratio));
    }
    let (lat, lon): (f64, f64) = geodesic.direct(lat1, lon1, azimuth, total * ratio);
    finite(lon, "geodesic interpolate")?;
    finite(lat, "geodesic interpolate")?;
    Ok((lon, lat, ratio))
}

pub(crate) fn finite(value: f64, operation: &str) -> Result<f64> {
    if value.is_finite() {
        Ok(value)
    } else {
        Err(CrsError::invalid(format!("{operation} calculation failed")))
    }
}

/// Signed spherical cross-track distance (meters) from `point` to the
/// great circle through `start -> end`.
///
/// Positive left of the directed path, negative right, zero on it.
/// Spherical on the CRS ellipsoid's mean radius `(2a + b) / 3` — the
/// classic navigation formula (ellipsoidal cross-track has no closed
/// form).
pub(crate) fn geodesic_cross_track_crs(
    crs: &str,
    point: (f64, f64),
    start: (f64, f64),
    end: (f64, f64),
) -> Result<f64> {
    let radius = geodesic_cross_track_radius_crs(crs)?;
    geodesic_cross_track_with_radius(radius, point, start, end)
}

pub(crate) fn geodesic_cross_track_radius_crs(crs: &str) -> Result<f64> {
    let shape = ellipsoid_shape(crs)?;
    Ok(shape.semi_major.get() * (1.0 - shape.flattening / 3.0))
}

pub(crate) fn geodesic_cross_track_with_radius(
    radius: f64,
    point: (f64, f64),
    start: (f64, f64),
    end: (f64, f64),
) -> Result<f64> {
    for (lon, lat) in [point, start, end] {
        ensure_geographic_lonlat(lon, lat)?;
    }
    let angle_to_point = sphere_central_angle(start, point);
    let bearing_to_point = sphere_initial_bearing(start, point);
    let bearing_of_path = sphere_initial_bearing(start, end);
    Ok((angle_to_point.sin() * (bearing_of_path - bearing_to_point).sin()).asin() * radius)
}

/// Haversine central angle between two lon/lat points (radians).
pub(crate) fn sphere_central_angle((lon1, lat1): (f64, f64), (lon2, lat2): (f64, f64)) -> f64 {
    let (lat1, lat2) = (lat1.to_radians(), lat2.to_radians());
    let half_dlat = (lat2 - lat1) / 2.0;
    let half_dlon = (lon2 - lon1).to_radians() / 2.0;
    let h = half_dlat.sin() * half_dlat.sin()
        + lat1.cos() * lat2.cos() * half_dlon.sin() * half_dlon.sin();
    2.0 * h.sqrt().clamp(0.0, 1.0).asin()
}

/// Spherical initial bearing from one lon/lat point toward another
/// (radians clockwise from north).
pub(crate) fn sphere_initial_bearing((lon1, lat1): (f64, f64), (lon2, lat2): (f64, f64)) -> f64 {
    let (lat1, lat2) = (lat1.to_radians(), lat2.to_radians());
    let dlon = (lon2 - lon1).to_radians();
    let y = dlon.sin() * lat2.cos();
    let x = lat1.cos() * lat2.sin() - lat1.sin() * lat2.cos() * dlon.cos();
    y.atan2(x)
}

/// Validated ellipsoid semi-major axis (meters) and flattening.
#[derive(Debug, Clone, Copy)]
pub(crate) struct EllipsoidShape {
    pub semi_major: crate::Positive,
    pub flattening: f64,
}

impl EllipsoidShape {
    pub(crate) fn new(major: f64, flattening: f64) -> Result<Self> {
        let semi_major = crate::Positive::try_new("semi_major", major)?;
        if flattening.is_finite() && (0.0..1.0).contains(&flattening) {
            Ok(Self {
                semi_major,
                flattening,
            })
        } else {
            Err(CrsError::invalid(format!(
                "flattening must be in [0, 1), got {flattening}"
            )))
        }
    }
}

/// The CRS ellipsoid's semi-major axis (meters) and flattening — the
/// parameters the spatial index's geodesic lower bound needs.
pub(crate) fn ellipsoid_shape(crs: &str) -> Result<EllipsoidShape> {
    with_geodesic(crs, |geodesic| EllipsoidShape::new(geodesic.a, geodesic.f))
}
