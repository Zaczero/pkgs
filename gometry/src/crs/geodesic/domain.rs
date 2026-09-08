use std::simd::cmp::SimdPartialOrd as _;
use std::simd::num::SimdFloat as _;

use crate::boundary::geographic;
use crate::boundary::geographic::invalid_lonlat_error;
use crate::crs::RuntimeState;
use crate::crs::geodesic::{CrsError, geographic_metric_frame_at};
use crate::error::{ResourceError, Result};
use crate::geographic::{GeographicFrame, GeographicViewError, ScalarGeographicView};
use crate::geometry::{CoordSeq, REDUCE_LANES, ReduceSimd, Shape, simd_mask_any};

/// Validate geometry coordinates in their CRS-native geographic frame. This
/// is the metric admission boundary; the geodesic kernels receive converted
/// coordinates only after this check succeeds.
pub(crate) fn ensure_geographic_domain_in_frame(
    frame: GeographicFrame,
    shape: &Shape,
) -> Result<()> {
    match ScalarGeographicView::try_new(frame, shape) {
        Ok(_) => Ok(()),
        Err(GeographicViewError::Resource) => Err(ResourceError::Allocation.into()),
        Err(GeographicViewError::Diagnostic(diagnostic)) => Err(geographic::invalid_lonlat_error(
            diagnostic.native.x,
            diagnostic.native.y,
        )),
    }
}

pub(crate) fn ensure_geographic_lonlat_in_frame(
    frame: GeographicFrame,
    lon: f64,
    lat: f64,
) -> Result<()> {
    frame
        .longitude()
        .canonicalize(lon)
        .and_then(|_| crate::geographic::Latitude::try_new(frame, lat))
        .map(|_| ())
        .map_err(|_| geographic::invalid_lonlat_error(lon, lat))
}

pub(crate) fn ensure_geographic_columns_in_frame(
    frame: GeographicFrame,
    xs: &[f64],
    ys: &[f64],
) -> Result<()> {
    for (&x, &y) in std::iter::zip(xs, ys) {
        ensure_geographic_lonlat_in_frame(frame, x, y)?;
    }
    Ok(())
}

pub(crate) fn ensure_geographic_coordseq_in_frame(
    frame: GeographicFrame,
    line: &CoordSeq,
) -> Result<()> {
    ensure_geographic_columns_in_frame(frame, line.xs(), line.ys())
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

pub(crate) fn ensure_geodesic_lonlat_crs_at(crs: &str, state: &RuntimeState) -> Result<()> {
    geographic_metric_frame_at(crs, state).map(|_| ())
}
