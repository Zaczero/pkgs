use crate::boundary::metadata::Frame;
use crate::geometry::{Shape, ShapeData};

/// Whether ``frame`` carries a geographic CRS (EPSG:4326/4979 fast path, then
/// cached PROJ lookup).
pub(crate) fn is_geographic_frame(frame: &Frame) -> bool {
    let Some(crs) = frame.crs_str() else {
        return false;
    };
    crate::crs::is_wgs84_lonlat(crs) || crate::crs::is_geographic_crs(crs).unwrap_or(false)
}

pub(crate) fn geographic_frame(
    frame: &Frame,
) -> crate::error::Result<Option<crate::geographic::GeographicFrame>> {
    let Some(crs) = frame.crs_str() else {
        return Ok(None);
    };
    Ok(crate::crs::info(crs)?.geographic_frame())
}

/// Whether a shape edge crosses the canonical seam in its native periodic
/// longitude frame. The public `Shape::crosses_antimeridian` remains the
/// legacy predicate; geodesic kernels must use this frame-aware form.
pub(crate) fn crosses_antimeridian_in_frame(
    shape: &Shape,
    frame: crate::geographic::GeographicFrame,
) -> bool {
    let axis = frame.longitude();
    let mut crosses = false;
    shape.for_each_segment_chain(|chain| {
        if crosses {
            return;
        }
        for pair in chain.xs().windows(2) {
            let (Ok(start), Ok(end)) = (axis.canonicalize(pair[0]), axis.canonicalize(pair[1]))
            else {
                continue;
            };
            let raw_delta = (pair[1] - pair[0]).abs();
            // A seam endpoint usually needs no split, but a stored edge that
            // runs more than a half turn to or from the seam carries polar-ring
            // winding. Canonicalizing that endpoint first would erase the
            // signal and make bounds reject points inside a polar cap.
            if start.identity(axis) == crate::geographic::SeamIdentity::Seam
                || end.identity(axis) == crate::geographic::SeamIdentity::Seam
            {
                if raw_delta > axis.half_period() && raw_delta < axis.period().next_up() {
                    crosses = true;
                    break;
                }
                continue;
            }
            if (end.get() - start.get()).abs() > axis.half_period() {
                crosses = true;
                break;
            }
        }
    });
    crosses
}

/// Whether a planar topology op must split-normalize ``shape`` at ±180 before
/// evaluating. The geographic verdict is checked FIRST: it is a cheap cached
/// CRS lookup (and short-circuits on a CRS-free frame), so the lazy ring-vertex
/// walk of ``crosses_antimeridian`` runs only for actually-geographic geometry
/// and only once per immutable handle. Every projected or CRS-free op skips the
/// coordinate scan entirely.
pub(crate) fn geographic_crossing(frame: &Frame, shape: &ShapeData) -> bool {
    let Some(geographic) = frame
        .crs_str()
        .and_then(|crs| crate::crs::info(crs).ok())
        .and_then(|info| info.geographic_frame())
    else {
        return false;
    };
    crosses_antimeridian_in_frame(shape.shape(), geographic)
}
