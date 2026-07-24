use std::simd::cmp::SimdPartialOrd;
use std::simd::num::SimdFloat;

use super::*;
use crate::boundary::geographic::invalid_lonlat_error;
use crate::error::Result;
use crate::geometry::{CoordSeq, REDUCE_LANES, ReduceSimd, Shape, simd_mask_any};

/// Validate that every coordinate of `shape` is usable for geodesic
/// measurement: finite longitude and a latitude within `[-90, 90]`. Without
/// this an out-of-domain latitude flows into `geographiclib` and surfaces as a
/// silent `NaN` metric instead of a clear error.
pub(crate) fn ensure_geographic_domain(shape: &Shape) -> Result<()> {
    shape.try_for_each_point(|point| ensure_geographic_lonlat(point.x, point.y))
}

pub(crate) fn ensure_geographic_coordseq(line: &CoordSeq) -> Result<()> {
    for (&x, &y) in std::iter::zip(line.xs(), line.ys()) {
        ensure_geographic_lonlat(x, y)?;
    }
    Ok(())
}

/// Validate one lon/lat coordinate for geodesic use: finite longitude and
/// a latitude within `[-90, 90]`.
///
/// Shared by the shape- and point-level geodesic entry points (and the
/// spherical clustering lane) so an out-of-domain input never reaches
/// `geographiclib` — and always fails with one class and one wording.
pub(crate) fn ensure_geographic_lonlat(lon: f64, lat: f64) -> Result<()> {
    if lon.is_finite()
        && lat.is_finite()
        && (-180.0..=180.0).contains(&lon)
        && (-90.0..=90.0).contains(&lat)
    {
        Ok(())
    } else {
        Err(invalid_lonlat_error(lon, lat))
    }
}

/// Validate a batch of latitudes against the pole bound, honoring `radians`
/// (the batch geodesic calculators already finite-check, but not the range).
pub(crate) fn ensure_latitudes_in_domain(latitudes: &[f64], radians: bool) -> Result<()> {
    let bound = if radians {
        std::f64::consts::FRAC_PI_2
    } else {
        90.0
    };
    let bound_simd = ReduceSimd::splat(bound);
    let (chunks, _) = latitudes.as_chunks::<REDUCE_LANES>();
    if simd_mask_any(
        latitudes.len(),
        |index| latitudes[index].abs() > bound,
        |start| {
            let chunk = start / REDUCE_LANES;
            ReduceSimd::from_array(chunks[chunk])
                .abs()
                .simd_gt(bound_simd)
        },
    ) {
        let message = if radians {
            "geographic latitude is outside the valid domain".to_owned()
        } else {
            "geographic latitude is outside the valid [-90, 90] degree domain".to_owned()
        };
        return Err(CrsError::message(message));
    }
    Ok(())
}

pub(crate) fn ensure_geodesic_lonlat_crs(crs: &str) -> Result<()> {
    let info = info(crs)?;
    if info.kind.starts_with("geographic") {
        ensure_geographic_degree_units(crs, &info)?;
        return Ok(());
    }
    Err(CrsError::invalid(format!(
        "{} geodesic geometry measurements require a geographic longitude-latitude CRS",
        info.crs
    )))
}
