use crate::boundary::metadata::Frame;
use crate::geometry::{Coordinates, Shape, ShapeData, TopologyDomain};

/// Resolve the complete topology domain from CRS metadata. WGS84 uses the
/// constant native frame; other geographic CRSs use their cached native frame.
pub(crate) fn topology_domain(frame: &Frame) -> crate::error::Result<TopologyDomain> {
    Ok(match frame.crs_str() {
        Some(crs) if crate::crs::is_wgs84_lonlat(crs) => {
            TopologyDomain::Geographic(crate::geographic::GeographicFrame::crs84())
        },
        Some(crs) => crate::crs::info(crs)?
            .geographic_frame()
            .map_or(TopologyDomain::Cartesian, TopologyDomain::Geographic),
        None => TopologyDomain::Cartesian,
    })
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

pub(crate) fn crosses_antimeridian_coords_in_frame<C: Coordinates + ?Sized>(
    coords: &C,
    frame: crate::geographic::GeographicFrame,
) -> bool {
    let axis = frame.longitude();
    coords.segment_pairs().any(|[start, end]| {
        let raw_delta = (end.x - start.x).abs();
        let (Ok(start), Ok(end)) = (axis.canonicalize(start.x), axis.canonicalize(end.x)) else {
            return false;
        };
        if start.identity(axis) == crate::geographic::SeamIdentity::Seam
            || end.identity(axis) == crate::geographic::SeamIdentity::Seam
        {
            return raw_delta > axis.half_period() && raw_delta < axis.period().next_up();
        }
        (end.get() - start.get()).abs() > axis.half_period()
    })
}

/// Whether a planar topology op must split-normalize ``shape`` at ±180 before
/// evaluating. Cartesian domains skip the coordinate scan entirely.
pub(crate) fn geographic_crossing(domain: TopologyDomain, shape: &ShapeData) -> bool {
    match domain {
        TopologyDomain::Cartesian => false,
        TopologyDomain::Geographic(frame) => crosses_antimeridian_in_frame(shape.shape(), frame),
    }
}
