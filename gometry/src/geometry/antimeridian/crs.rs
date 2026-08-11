use crate::boundary::metadata::Frame;
use crate::geometry::ShapeData;

/// Whether ``frame`` carries a geographic CRS (EPSG:4326/4979 fast path, then
/// cached PROJ lookup).
pub(crate) fn is_geographic_frame(frame: &Frame) -> bool {
    let Some(crs) = frame.crs_str() else {
        return false;
    };
    crate::crs::is_wgs84_lonlat(crs) || crate::crs::is_geographic_crs(crs).unwrap_or(false)
}

/// Whether a planar topology op must split-normalize ``shape`` at ±180 before
/// evaluating. The geographic verdict is checked FIRST: it is a cheap cached
/// CRS lookup (and short-circuits on a CRS-free frame), so the lazy ring-vertex
/// walk of ``crosses_antimeridian`` runs only for actually-geographic geometry
/// and only once per immutable handle. Every projected or CRS-free op skips the
/// coordinate scan entirely.
pub(crate) fn geographic_crossing(frame: &Frame, shape: &ShapeData) -> bool {
    is_geographic_frame(frame) && shape.crosses_antimeridian()
}
