use crate::crs::RuntimeState;
use crate::crs::geodesic::geographic_metric_frame_at;
use crate::error::Result;
use crate::geographic::lonlat::invalid_lonlat_error;
use crate::geographic::{GeographicFrame, ScalarGeographicView, lonlat as geographic};
use crate::geometry::{CoordSeq, Shape};

/// Expand one longitude/latitude domain check inside a coordinate loop. The
/// caller can reuse its frame's longitude axis; the returned error is
/// propagated with `?` in source-coordinate order.
macro_rules! geographic_lonlat_result {
    ($frame:expr, $longitude:expr, $lon:expr, $lat:expr $(,)?) => {{
        let __lon = $lon;
        let __lat = $lat;
        $longitude
            .canonicalize(__lon)
            .and_then(|_| $crate::geographic::Latitude::try_new($frame, __lat))
            .map(|_| ())
            .map_err(|_| $crate::geographic::lonlat::invalid_lonlat_error(__lon, __lat))
    }};
}
pub(crate) use geographic_lonlat_result;

/// Validate geometry coordinates in their CRS-native geographic frame. This
/// is the metric admission boundary; the geodesic kernels receive converted
/// coordinates only after this check succeeds.
pub(crate) fn ensure_geographic_domain_in_frame(
    frame: GeographicFrame,
    shape: &Shape,
) -> Result<()> {
    ScalarGeographicView::try_new(frame, shape)
        .map(|_| ())
        .map_err(|diagnostic| geographic::geographic_admission_error(&diagnostic))
}

pub(crate) fn ensure_geographic_lonlat_in_frame(
    frame: GeographicFrame,
    lon: f64,
    lat: f64,
) -> Result<()> {
    let longitude = frame.longitude();
    geographic_lonlat_result!(frame, longitude, lon, lat)
}

pub(crate) fn ensure_geographic_columns_in_frame(
    frame: GeographicFrame,
    xs: &[f64],
    ys: &[f64],
) -> Result<()> {
    let longitude = frame.longitude();
    for (&x, &y) in std::iter::zip(xs, ys) {
        geographic_lonlat_result!(frame, longitude, x, y)?;
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
