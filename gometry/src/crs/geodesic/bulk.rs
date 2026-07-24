use std::simd::cmp::SimdPartialOrd;

use super::*;
use crate::error::Result;
use crate::geometry::{REDUCE_LANES, ReduceSimd, Shape, column_all_finite, simd_mask_any};

pub(crate) fn geodesic_inverse(
    crs: &str,
    lon1: f64,
    lat1: f64,
    lon2: f64,
    lat2: f64,
    z1: Option<f64>,
    z2: Option<f64>,
    angle_unit: impl Into<AngleUnit>,
) -> Result<GeodesicInverseInfo> {
    let angle_unit = angle_unit.into();
    if !lon1.is_finite()
        || !lat1.is_finite()
        || !lon2.is_finite()
        || !lat2.is_finite()
        || z1.is_some_and(|value| !value.is_finite())
        || z2.is_some_and(|value| !value.is_finite())
    {
        return Err(GeometryErrorKind::message(
            "geodesic coordinates must be finite",
        ));
    }
    if z1.is_some() != z2.is_some() {
        return Err(GeometryErrorKind::message(
            "geodesic height distance requires both z1 and z2",
        ));
    }
    ensure_latitudes_in_domain(&[lat1], angle_unit.is_radians())?;
    ensure_latitudes_in_domain(&[lat2], angle_unit.is_radians())?;
    let crs = normalize(crs)?;
    with_geodesic(&crs, |geodesic| {
        geodesic_inverse_on_ellipsoid(geodesic, lon1, lat1, lon2, lat2, z1, z2, angle_unit)
    })
}

pub(crate) fn geodesic_inverses(
    crs: &str,
    lon1: &[f64],
    lat1: &[f64],
    lon2: &[f64],
    lat2: &[f64],
    z1: Option<&[f64]>,
    z2: Option<&[f64]>,
    angle_unit: impl Into<AngleUnit>,
) -> Result<Vec<GeodesicInverseInfo>> {
    let angle_unit = angle_unit.into();
    ensure_same_geodesic_len(
        [
            (lon1.len(), "lon1"),
            (lat1.len(), "lat1"),
            (lon2.len(), "lon2"),
            (lat2.len(), "lat2"),
        ],
        "lon1, lat1, lon2, and lat2",
    )?;
    if let Some(z1) = z1 {
        ensure_same_geodesic_len([(lon1.len(), "lon1"), (z1.len(), "z1")], "lon1 and z1")?;
    }
    if let Some(z2) = z2 {
        ensure_same_geodesic_len([(lon1.len(), "lon1"), (z2.len(), "z2")], "lon1 and z2")?;
    }
    if !column_all_finite(lon1)
        || !column_all_finite(lat1)
        || !column_all_finite(lon2)
        || !column_all_finite(lat2)
        || z1.is_some_and(|values| !column_all_finite(values))
        || z2.is_some_and(|values| !column_all_finite(values))
    {
        return Err(GeometryErrorKind::message(
            "geodesic coordinates must be finite",
        ));
    }
    ensure_latitudes_in_domain(lat1, angle_unit.is_radians())?;
    ensure_latitudes_in_domain(lat2, angle_unit.is_radians())?;
    if z1.is_some() != z2.is_some() {
        return Err(GeometryErrorKind::message(
            "geodesic height distance requires both z1 and z2",
        ));
    }
    let crs = normalize(crs)?;
    match (angle_unit, z1, z2) {
        (AngleUnit::Degrees, Some(z1), Some(z2)) => with_geodesic(&crs, |geodesic| {
            geodesic_inverses_impl::<false, true>(geodesic, lon1, lat1, lon2, lat2, z1, z2)
        }),
        (AngleUnit::Degrees, ..) => with_geodesic(&crs, |geodesic| {
            geodesic_inverses_impl::<false, false>(geodesic, lon1, lat1, lon2, lat2, &[], &[])
        }),
        (AngleUnit::Radians, Some(z1), Some(z2)) => with_geodesic(&crs, |geodesic| {
            geodesic_inverses_impl::<true, true>(geodesic, lon1, lat1, lon2, lat2, z1, z2)
        }),
        (AngleUnit::Radians, ..) => with_geodesic(&crs, |geodesic| {
            geodesic_inverses_impl::<true, false>(geodesic, lon1, lat1, lon2, lat2, &[], &[])
        }),
    }
}

fn geodesic_inverses_impl<const RADIANS: bool, const HEIGHT: bool>(
    geodesic: &geographiclib_rs::Geodesic,
    lon1: &[f64],
    lat1: &[f64],
    lon2: &[f64],
    lat2: &[f64],
    z1: &[f64],
    z2: &[f64],
) -> Result<Vec<GeodesicInverseInfo>> {
    let mut result = Vec::with_capacity(lon1.len());
    for row in 0..lon1.len() {
        let (za, zb) = if HEIGHT {
            (z1[row], z2[row])
        } else {
            (0.0, 0.0)
        };
        result.push(geodesic_inverse_on_ellipsoid_const::<RADIANS, HEIGHT>(
            geodesic, lon1[row], lat1[row], lon2[row], lat2[row], za, zb,
        )?);
    }
    Ok(result)
}

pub(crate) fn geodesic_direct(
    crs: &str,
    longitude: f64,
    latitude: f64,
    azimuth: f64,
    distance: f64,
    angle_unit: impl Into<AngleUnit>,
) -> Result<GeodesicDirectInfo> {
    let angle_unit = angle_unit.into();
    if !longitude.is_finite()
        || !latitude.is_finite()
        || !azimuth.is_finite()
        || !distance.is_finite()
    {
        return Err(GeometryErrorKind::message(
            "geodesic direct inputs must be finite",
        ));
    }
    ensure_latitudes_in_domain(&[latitude], angle_unit.is_radians())?;
    if distance < 0.0 {
        return Err(GeometryErrorKind::message(
            "geodesic direct distance must be non-negative",
        ));
    }
    let crs = normalize(crs)?;
    with_geodesic(&crs, |geodesic| {
        geodesic_direct_on_ellipsoid(geodesic, longitude, latitude, azimuth, distance, angle_unit)
    })
}

pub(crate) fn geodesic_direct_columns(
    crs: &str,
    longitude: &[f64],
    latitude: &[f64],
    azimuth: &[f64],
    distance: &[f64],
    angle_unit: impl Into<AngleUnit>,
) -> Result<GeodesicDirectColumns> {
    let angle_unit = angle_unit.into();
    ensure_same_geodesic_len(
        [
            (longitude.len(), "longitude"),
            (latitude.len(), "latitude"),
            (azimuth.len(), "azimuth"),
            (distance.len(), "distance"),
        ],
        "longitude, latitude, azimuth, and distance",
    )?;
    if !column_all_finite(longitude)
        || !column_all_finite(latitude)
        || !column_all_finite(azimuth)
        || !column_all_finite(distance)
    {
        return Err(GeometryErrorKind::message(
            "geodesic direct inputs must be finite",
        ));
    }
    ensure_latitudes_in_domain(latitude, angle_unit.is_radians())?;
    let (distance_chunks, _) = distance.as_chunks::<REDUCE_LANES>();
    if simd_mask_any(
        distance.len(),
        |index| distance[index] < 0.0,
        |start| {
            let chunk = start / REDUCE_LANES;
            ReduceSimd::from_array(distance_chunks[chunk]).simd_lt(ReduceSimd::splat(0.0))
        },
    ) {
        return Err(GeometryErrorKind::message(
            "geodesic direct distance must be non-negative",
        ));
    }
    let crs = normalize(crs)?;
    match angle_unit {
        AngleUnit::Degrees => with_geodesic(&crs, |geodesic| {
            geodesic_direct_columns_impl::<false>(geodesic, longitude, latitude, azimuth, distance)
        }),
        AngleUnit::Radians => with_geodesic(&crs, |geodesic| {
            geodesic_direct_columns_impl::<true>(geodesic, longitude, latitude, azimuth, distance)
        }),
    }
}

fn geodesic_direct_columns_impl<const RADIANS: bool>(
    geodesic: &geographiclib_rs::Geodesic,
    longitude: &[f64],
    latitude: &[f64],
    azimuth: &[f64],
    distance: &[f64],
) -> Result<GeodesicDirectColumns> {
    let mut columns = GeodesicDirectColumns {
        longitude: Vec::with_capacity(longitude.len()),
        latitude: Vec::with_capacity(longitude.len()),
        final_azimuth: Vec::with_capacity(longitude.len()),
    };
    for row in 0..longitude.len() {
        let info = geodesic_direct_on_ellipsoid_const::<RADIANS>(
            geodesic,
            longitude[row],
            latitude[row],
            azimuth[row],
            distance[row],
        )?;
        columns.longitude.push(info.longitude);
        columns.latitude.push(info.latitude);
        columns.final_azimuth.push(info.final_azimuth);
    }
    Ok(columns)
}

pub(crate) fn geodesic_interpolates(
    crs: &str,
    lon1: &[f64],
    lat1: &[f64],
    lon2: &[f64],
    lat2: &[f64],
    distance: &[f64],
    distance_mode: impl Into<DistanceMode>,
    angle_unit: impl Into<AngleUnit>,
) -> Result<Vec<GeodesicInterpolateInfo>> {
    let distance_mode = distance_mode.into();
    let angle_unit = angle_unit.into();
    ensure_same_geodesic_len(
        [
            (lon1.len(), "lon1"),
            (lat1.len(), "lat1"),
            (lon2.len(), "lon2"),
            (lat2.len(), "lat2"),
            (distance.len(), "distance"),
        ],
        "lon1, lat1, lon2, lat2, and distance",
    )?;
    if !column_all_finite(lon1)
        || !column_all_finite(lat1)
        || !column_all_finite(lon2)
        || !column_all_finite(lat2)
        || !column_all_finite(distance)
    {
        return Err(GeometryErrorKind::message(
            "geodesic interpolate inputs must be finite",
        ));
    }
    ensure_latitudes_in_domain(lat1, angle_unit.is_radians())?;
    ensure_latitudes_in_domain(lat2, angle_unit.is_radians())?;
    let crs = normalize(crs)?;
    match (angle_unit, distance_mode) {
        (AngleUnit::Degrees, DistanceMode::Absolute) => with_geodesic(&crs, |geodesic| {
            geodesic_interpolates_impl::<false, false>(geodesic, lon1, lat1, lon2, lat2, distance)
        }),
        (AngleUnit::Degrees, DistanceMode::Normalized) => with_geodesic(&crs, |geodesic| {
            geodesic_interpolates_impl::<false, true>(geodesic, lon1, lat1, lon2, lat2, distance)
        }),
        (AngleUnit::Radians, DistanceMode::Absolute) => with_geodesic(&crs, |geodesic| {
            geodesic_interpolates_impl::<true, false>(geodesic, lon1, lat1, lon2, lat2, distance)
        }),
        (AngleUnit::Radians, DistanceMode::Normalized) => with_geodesic(&crs, |geodesic| {
            geodesic_interpolates_impl::<true, true>(geodesic, lon1, lat1, lon2, lat2, distance)
        }),
    }
}

fn geodesic_interpolates_impl<const RADIANS: bool, const NORMALIZED: bool>(
    geodesic: &geographiclib_rs::Geodesic,
    lon1: &[f64],
    lat1: &[f64],
    lon2: &[f64],
    lat2: &[f64],
    distance: &[f64],
) -> Result<Vec<GeodesicInterpolateInfo>> {
    fn target_along<const NORMALIZED: bool>(distance: f64, total: f64) -> f64 {
        if NORMALIZED {
            distance * total
        } else if distance < 0.0 {
            total + distance
        } else {
            distance
        }
        .clamp(0.0, total)
    }

    let carry_endpoint = |mut info: GeodesicInterpolateInfo,
                          target: f64,
                          total: f64,
                          start: (f64, f64),
                          end: (f64, f64)| {
        if crate::geometry::same_topological_coordinate(target, 0.0) {
            info.longitude = start.0;
            info.latitude = start.1;
        } else if crate::geometry::same_topological_coordinate(target, total) {
            info.longitude = end.0;
            info.latitude = end.1;
        }
        info
    };

    let mut result = Vec::with_capacity(lon1.len());
    if lon1.len() > 1 && endpoints_are_repeated(lon1, lat1, lon2, lat2) {
        let line =
            geodesic_line_solution_const::<RADIANS>(geodesic, lon1[0], lat1[0], lon2[0], lat2[0])?;
        for &value in distance {
            let info =
                geodesic_interpolate_on_line_const::<RADIANS, NORMALIZED>(geodesic, line, value)?;
            let target = target_along::<NORMALIZED>(value, line.3);
            result.push(carry_endpoint(
                info,
                target,
                line.3,
                (lon1[0], lat1[0]),
                (lon2[0], lat2[0]),
            ));
        }
        return Ok(result);
    }
    for row in 0..lon1.len() {
        let line = geodesic_line_solution_const::<RADIANS>(
            geodesic, lon1[row], lat1[row], lon2[row], lat2[row],
        )?;
        let info = geodesic_interpolate_on_line_const::<RADIANS, NORMALIZED>(
            geodesic,
            line,
            distance[row],
        )?;
        let target = target_along::<NORMALIZED>(distance[row], line.3);
        result.push(carry_endpoint(
            info,
            target,
            line.3,
            (lon1[row], lat1[row]),
            (lon2[row], lat2[row]),
        ));
    }
    Ok(result)
}

pub(crate) fn geodesic_length(crs: &str, shape: &Shape) -> Result<f64> {
    ensure_geodesic_lonlat_crs(crs)?;
    ensure_geographic_domain(shape)?;
    let crs = normalize(crs)?;
    with_geodesic(&crs, |geodesic| Ok(geodesic_shape_length(geodesic, shape)))
}
