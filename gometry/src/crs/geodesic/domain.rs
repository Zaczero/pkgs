use crate::boundary::geographic;
use crate::boundary::geographic::invalid_lonlat_error;
use crate::crs::RuntimeState;
use crate::crs::geodesic::geographic_metric_frame_at;
use crate::error::Result;
use crate::geographic::{GeographicFrame, ScalarGeographicView};
use crate::geometry::{CoordSeq, Shape};

/// Validate geometry coordinates in their CRS-native geographic frame. This
/// is the metric admission boundary; the geodesic kernels receive converted
/// coordinates only after this check succeeds.
pub(crate) fn ensure_geographic_domain_in_frame(
    frame: GeographicFrame,
    shape: &Shape,
) -> Result<()> {
    ScalarGeographicView::try_new(frame, shape)
        .map(|_| ())
        .map_err(|diagnostic| {
            geographic::invalid_lonlat_error(diagnostic.native.x, diagnostic.native.y)
        })
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

pub(crate) fn ensure_geodesic_lonlat_crs_at(crs: &str, state: &RuntimeState) -> Result<()> {
    geographic_metric_frame_at(crs, state).map(|_| ())
}
