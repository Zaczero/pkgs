use std::simd::cmp::SimdPartialEq as _;
use std::simd::num::SimdFloat as _;

use geographiclib_rs::{Geodesic, PolygonArea, Winding};

use crate::crs::geodesic::{
    CrsError, DirectGeodesic as _, EllipsoidMetric, GeodesicDirectInfo, GeodesicInterpolateInfo,
    GeodesicInverseInfo, ensure_geographic_lonlat, inverse_distance, inverse_distance_azimuths,
    with_geodesic_erased,
};
use crate::error::Result;
use crate::geometry::{
    Coordinates, Polygon, REDUCE_LANES, ReduceSimd, Shape, canonicalize_zero,
    same_topological_coordinate, simd_mask_all,
};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum AngleUnit {
    Degrees,
    Radians,
}

impl AngleUnit {
    pub(crate) const fn of(radians: bool) -> Self {
        if radians {
            Self::Radians
        } else {
            Self::Degrees
        }
    }

    pub(crate) fn is_radians(self) -> bool {
        self == Self::Radians
    }
}

fn angle_to_degrees<const RADIANS: bool>(v: f64) -> f64 {
    if RADIANS { v.to_degrees() } else { v }
}

fn degrees_into<const RADIANS: bool>(v: f64) -> f64 {
    if RADIANS { v.to_radians() } else { v }
}

fn distance_along<const NORMALIZED: bool>(distance: f64, total: f64) -> f64 {
    if NORMALIZED {
        distance * total
    } else if distance < 0.0 {
        total + distance
    } else {
        distance
    }
}

impl From<bool> for AngleUnit {
    fn from(radians: bool) -> Self {
        Self::of(radians)
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum DistanceMode {
    Absolute,
    Normalized,
}

impl DistanceMode {
    pub(crate) const fn of(normalized: bool) -> Self {
        if normalized {
            Self::Normalized
        } else {
            Self::Absolute
        }
    }

    pub(crate) fn is_normalized(self) -> bool {
        self == Self::Normalized
    }

    pub(crate) fn planar_along(self, distance: f64, to_metre: f64) -> f64 {
        match self {
            Self::Normalized => distance,
            Self::Absolute => distance / to_metre,
        }
    }
}

impl From<bool> for DistanceMode {
    fn from(normalized: bool) -> Self {
        Self::of(normalized)
    }
}

/// Run `f` against the ellipsoid metric for `crs`.
///
/// Centralizes the geographic-CRS check, the lon/lat domain validation of every
/// input `shape`, the thread-cache refresh, CRS normalization, and the cached
/// [`Geodesic`] lookup — the single entry point the geodesic metric ops
/// (`geodesic_hausdorff`/`_nearest`/…) share, so none re-implements the
/// validation and caching boilerplate.
pub(crate) fn with_ellipsoid_metric<T>(
    crs: &str,
    shapes: &[&Shape],
    f: impl FnOnce(&EllipsoidMetric<'_>) -> Result<T>,
) -> Result<T> {
    with_resolved_ellipsoid_metric(crs, shapes, |_, metric| f(metric))
}

/// Run `f` against the normalized CRS string and resolved ellipsoid metric.
///
/// This is the broadcast-friendly sibling of [`with_ellipsoid_metric`]: the
/// CRS kind/unit checks, input lon/lat validation, PROJ cache refresh,
/// normalization, and geodesic lookup happen once outside the row loop.
pub(crate) fn with_resolved_ellipsoid_metric<T>(
    crs: &str,
    shapes: &[&Shape],
    f: impl FnOnce(&str, &EllipsoidMetric<'_>) -> Result<T>,
) -> Result<T> {
    with_geodesic_erased(crs, shapes, |error| error, f)
}

pub(crate) fn geodesic_shape_area(geodesic: &Geodesic, shape: &Shape) -> f64 {
    canonicalize_zero(match shape {
        Shape::Polygon(polygon) => geodesic_polygon_area(geodesic, polygon),
        Shape::MultiPolygon(polygons) => compensated_sum(
            polygons
                .iter()
                .map(|polygon| geodesic_polygon_area(geodesic, polygon)),
        ),
        Shape::GeometryCollection(geometries) => compensated_sum(
            geometries
                .iter()
                .map(|geometry| geodesic_shape_area(geodesic, geometry)),
        ),
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Empty(..) => 0.0,
    })
}

pub(crate) fn geodesic_shape_length(geodesic: &Geodesic, shape: &Shape) -> f64 {
    canonicalize_zero(match shape {
        Shape::Point(_) | Shape::MultiPoint(_) | Shape::Empty(..) => 0.0,
        Shape::LineString(points) => geodesic_line_length(geodesic, points),
        Shape::MultiLineString(lines) => compensated_sum(
            lines
                .iter()
                .map(|line| geodesic_line_length(geodesic, line)),
        ),
        Shape::Polygon(polygon) => geodesic_polygon_perimeter(geodesic, polygon),
        Shape::MultiPolygon(polygons) => compensated_sum(
            polygons
                .iter()
                .map(|polygon| geodesic_polygon_perimeter(geodesic, polygon)),
        ),
        Shape::GeometryCollection(geometries) => compensated_sum(
            geometries
                .iter()
                .map(|geometry| geodesic_shape_length(geodesic, geometry)),
        ),
    })
}

pub(crate) fn geodesic_polygon_area(geodesic: &Geodesic, polygon: &Polygon) -> f64 {
    // Orientation-independent, matching planar `Polygon::area`: the unsigned
    // shell area less the unsigned hole areas. `PolygonArea` returns a signed
    // value (negative for clockwise rings), so take magnitudes.
    compensated_sum(
        std::iter::once(geodesic_ring_measure(geodesic, &polygon.shell).1.abs()).chain(
            polygon
                .holes
                .iter()
                .map(|hole| -geodesic_ring_measure(geodesic, hole).1.abs()),
        ),
    )
}

pub(crate) fn geodesic_polygon_perimeter(geodesic: &Geodesic, polygon: &Polygon) -> f64 {
    compensated_sum(
        std::iter::once(geodesic_ring_measure(geodesic, &polygon.shell).0).chain(
            polygon
                .holes
                .iter()
                .map(|hole| geodesic_ring_measure(geodesic, hole).0),
        ),
    )
}

pub(crate) fn geodesic_ring_measure<C: Coordinates + ?Sized>(
    geodesic: &Geodesic,
    ring: &C,
) -> (f64, f64) {
    geodesic_ring_measure_lonlat(
        geodesic,
        ring.coord_count(),
        ring.iter_coords().map(|point| (point.x, point.y)),
    )
}

pub(crate) fn geodesic_ring_measure_columns(
    geodesic: &Geodesic,
    xs: &[f64],
    ys: &[f64],
) -> (f64, f64) {
    debug_assert_eq!(xs.len(), ys.len());
    geodesic_ring_measure_lonlat(
        geodesic,
        xs.len(),
        std::iter::zip(xs, ys).map(|(&lon, &lat)| (lon, lat)),
    )
}

fn geodesic_ring_measure_lonlat(
    geodesic: &Geodesic,
    coord_count: usize,
    points: impl IntoIterator<Item = (f64, f64)>,
) -> (f64, f64) {
    if coord_count < 3 {
        return (0.0, 0.0);
    }
    let mut area = PolygonArea::new(geodesic, Winding::CounterClockwise);
    for (lon, lat) in points {
        area.add_point(lat, lon);
    }
    // `sign = true` bounds the result to (-half-planet, half-planet], so a
    // clockwise ring yields the small enclosed area negated rather than the
    // huge complement. Callers take the magnitude for orientation independence.
    let (perimeter, area, _) = area.compute(true);
    (perimeter, area)
}

pub(crate) fn geodesic_line_length<C: Coordinates + ?Sized>(
    geodesic: &Geodesic,
    points: &C,
) -> f64 {
    compensated_sum(
        points
            .segment_pairs()
            .map(|[start, end]| geodesic_segment_length(geodesic, start.x, start.y, end.x, end.y)),
    )
}

pub(crate) fn geodesic_line_length_columns(geodesic: &Geodesic, xs: &[f64], ys: &[f64]) -> f64 {
    debug_assert_eq!(xs.len(), ys.len());
    compensated_sum(
        xs.array_windows::<2>()
            .zip(ys.array_windows::<2>())
            .map(|([x0, x1], [y0, y1])| geodesic_segment_length(geodesic, *x0, *y0, *x1, *y1)),
    )
}

fn compensated_sum(values: impl IntoIterator<Item = f64>) -> f64 {
    let mut sum = 0.0;
    let mut correction = 0.0;
    for value in values {
        let adjusted = value - correction;
        let next = sum + adjusted;
        correction = (next - sum) - adjusted;
        sum = next;
    }
    sum
}

pub(crate) fn ensure_geographic_columns(xs: &[f64], ys: &[f64]) -> Result<()> {
    debug_assert_eq!(xs.len(), ys.len());
    for (&x, &y) in std::iter::zip(xs, ys) {
        ensure_geographic_lonlat(x, y)?;
    }
    Ok(())
}

fn geodesic_segment_length(
    geodesic: &Geodesic,
    start_lon: f64,
    start_lat: f64,
    end_lon: f64,
    end_lat: f64,
) -> f64 {
    inverse_distance(geodesic, start_lon, start_lat, end_lon, end_lat)
}

pub(crate) fn geodesic_direct_on_ellipsoid(
    geodesic: &Geodesic,
    longitude: f64,
    latitude: f64,
    azimuth: f64,
    distance: f64,
    angle_unit: AngleUnit,
) -> Result<GeodesicDirectInfo> {
    match angle_unit {
        AngleUnit::Degrees => geodesic_direct_on_ellipsoid_const::<false>(
            geodesic, longitude, latitude, azimuth, distance,
        ),
        AngleUnit::Radians => geodesic_direct_on_ellipsoid_const::<true>(
            geodesic, longitude, latitude, azimuth, distance,
        ),
    }
}

pub(crate) fn geodesic_direct_on_ellipsoid_const<const RADIANS: bool>(
    geodesic: &Geodesic,
    longitude: f64,
    latitude: f64,
    azimuth: f64,
    distance: f64,
) -> Result<GeodesicDirectInfo> {
    let (input_longitude, input_latitude) = (longitude, latitude);
    let longitude = angle_to_degrees::<RADIANS>(longitude);
    let latitude = angle_to_degrees::<RADIANS>(latitude);
    let azimuth = angle_to_degrees::<RADIANS>(azimuth);
    let (latitude, longitude, final_azimuth) =
        geodesic.direct(latitude, longitude, azimuth, distance);
    let mut info = GeodesicDirectInfo {
        longitude: degrees_into::<RADIANS>(longitude),
        latitude: degrees_into::<RADIANS>(latitude),
        final_azimuth: degrees_into::<RADIANS>(final_azimuth),
    };
    if same_topological_coordinate(distance, 0.0) {
        info.longitude = input_longitude;
        info.latitude = input_latitude;
    }
    if !info.longitude.is_finite() || !info.latitude.is_finite() || !info.final_azimuth.is_finite()
    {
        return Err(CrsError::invalid(
            "geodesic direct calculation failed".to_owned(),
        ));
    }
    Ok(info)
}

pub(crate) fn endpoints_are_repeated(
    lon1: &[f64],
    lat1: &[f64],
    lon2: &[f64],
    lat2: &[f64],
) -> bool {
    let column_bits_eq_value = |column: &[f64], value: u64| {
        simd_mask_all(
            column.len(),
            |index| column[index].to_bits() == value,
            |start| {
                let chunk = start / REDUCE_LANES;
                let (chunks, _) = column.as_chunks::<REDUCE_LANES>();
                ReduceSimd::from_array(chunks[chunk])
                    .to_bits()
                    .simd_eq(std::simd::Simd::<u64, REDUCE_LANES>::splat(value))
            },
        )
    };
    column_bits_eq_value(lon1, lon1[0].to_bits())
        && column_bits_eq_value(lat1, lat1[0].to_bits())
        && column_bits_eq_value(lon2, lon2[0].to_bits())
        && column_bits_eq_value(lat2, lat2[0].to_bits())
}

pub(crate) fn geodesic_line_solution_const<const RADIANS: bool>(
    geodesic: &Geodesic,
    lon1: f64,
    lat1: f64,
    lon2: f64,
    lat2: f64,
) -> Result<(f64, f64, f64, f64)> {
    let lon1 = angle_to_degrees::<RADIANS>(lon1);
    let lat1 = angle_to_degrees::<RADIANS>(lat1);
    let lon2 = angle_to_degrees::<RADIANS>(lon2);
    let lat2 = angle_to_degrees::<RADIANS>(lat2);
    let (total, forward_azimuth, _) = inverse_distance_azimuths(geodesic, lon1, lat1, lon2, lat2);
    if !total.is_finite() || !forward_azimuth.is_finite() {
        return Err(CrsError::invalid(
            "geodesic interpolate calculation failed".to_owned(),
        ));
    }
    Ok((lon1, lat1, forward_azimuth, total))
}

pub(crate) fn geodesic_interpolate_on_line_const<const RADIANS: bool, const NORMALIZED: bool>(
    geodesic: &Geodesic,
    (longitude, latitude, forward_azimuth, total): (f64, f64, f64, f64),
    distance: f64,
) -> Result<GeodesicInterpolateInfo> {
    let target = distance_along::<NORMALIZED>(distance, total).clamp(0.0, total);
    let direct = geodesic_direct_on_ellipsoid_const::<false>(
        geodesic,
        longitude,
        latitude,
        forward_azimuth,
        target,
    )?;
    let info = GeodesicInterpolateInfo {
        longitude: degrees_into::<RADIANS>(direct.longitude),
        latitude: degrees_into::<RADIANS>(direct.latitude),
        final_azimuth: degrees_into::<RADIANS>(direct.final_azimuth),
        distance: target,
    };
    if !info.longitude.is_finite() || !info.latitude.is_finite() || !info.final_azimuth.is_finite()
    {
        return Err(CrsError::invalid(
            "geodesic interpolate calculation failed".to_owned(),
        ));
    }
    Ok(info)
}

pub(crate) fn geodesic_inverse_on_ellipsoid(
    geodesic: &Geodesic,
    lon1: f64,
    lat1: f64,
    lon2: f64,
    lat2: f64,
    z1: Option<f64>,
    z2: Option<f64>,
    angle_unit: AngleUnit,
) -> Result<GeodesicInverseInfo> {
    match (angle_unit, z1, z2) {
        (AngleUnit::Degrees, Some(z1), Some(z2)) => {
            geodesic_inverse_on_ellipsoid_const::<false, true>(
                geodesic, lon1, lat1, lon2, lat2, z1, z2,
            )
        },
        (AngleUnit::Degrees, ..) => geodesic_inverse_on_ellipsoid_const::<false, false>(
            geodesic, lon1, lat1, lon2, lat2, 0.0, 0.0,
        ),
        (AngleUnit::Radians, Some(z1), Some(z2)) => {
            geodesic_inverse_on_ellipsoid_const::<true, true>(
                geodesic, lon1, lat1, lon2, lat2, z1, z2,
            )
        },
        (AngleUnit::Radians, ..) => geodesic_inverse_on_ellipsoid_const::<true, false>(
            geodesic, lon1, lat1, lon2, lat2, 0.0, 0.0,
        ),
    }
}

pub(crate) fn geodesic_inverse_on_ellipsoid_const<const RADIANS: bool, const HEIGHT: bool>(
    geodesic: &Geodesic,
    lon1: f64,
    lat1: f64,
    lon2: f64,
    lat2: f64,
    z1: f64,
    z2: f64,
) -> Result<GeodesicInverseInfo> {
    let lon1 = angle_to_degrees::<RADIANS>(lon1);
    let lat1 = angle_to_degrees::<RADIANS>(lat1);
    let lon2 = angle_to_degrees::<RADIANS>(lon2);
    let lat2 = angle_to_degrees::<RADIANS>(lat2);
    let (distance, forward_azimuth, reverse_azimuth) =
        inverse_distance_azimuths(geodesic, lon1, lat1, lon2, lat2);
    // Guarded sqrt (the `line_length_3d` discipline): `hypot` is a per-row libm
    // call, so combine the geodesic distance and the height delta in squared
    // space and reserve `hypot` for the overflow/underflow rescue.
    let distance_3d = HEIGHT.then(|| {
        let dz = z2 - z1;
        let squared = distance * distance + dz * dz;
        if crate::geometry::squared_norm_is_trustworthy(squared, distance == 0.0 && dz == 0.0) {
            squared.sqrt()
        } else {
            distance.hypot(dz)
        }
    });
    let info = GeodesicInverseInfo {
        distance,
        distance_3d,
        forward_azimuth: degrees_into::<RADIANS>(forward_azimuth),
        reverse_azimuth: degrees_into::<RADIANS>(reverse_azimuth),
    };
    if !info.distance.is_finite()
        || info
            .distance_3d
            .is_some_and(|distance| !distance.is_finite())
        || !info.forward_azimuth.is_finite()
        || !info.reverse_azimuth.is_finite()
    {
        return Err(CrsError::invalid("geodesic calculation failed".to_owned()));
    }
    Ok(info)
}
