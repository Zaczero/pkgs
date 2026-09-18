use std::simd::cmp::SimdPartialOrd as _;
use std::simd::num::SimdFloat as _;

use crate::boundary::metadata::Frame;
use crate::crs::CrsError;
use crate::geographic::{GeographicFrame, SeamIdentity};
use crate::geometry::{
    Coordinates, REDUCE_LANES, ReduceSimd, Shape, ShapeData, TopologyDomain, simd_mask_any,
};

/// Resolve the complete topology domain from CRS metadata. WGS84 uses the
/// constant native frame; other geographic CRSs use their cached native frame.
pub(crate) fn topology_domain(frame: &Frame) -> crate::error::Result<TopologyDomain> {
    Ok(match frame.crs_str() {
        Some(crs) if crate::crs::is_wgs84_lonlat(crs) => {
            TopologyDomain::Geographic(GeographicFrame::crs84())
        },
        Some(crs) => crate::crs::info(crs)?
            .geographic_frame()
            .map_or(TopologyDomain::Cartesian, TopologyDomain::Geographic),
        None => TopologyDomain::Cartesian,
    })
}

/// The longitude frame of an antimeridian operation: a geographic CRS's own,
/// or CRS84 for CRS-free lon/lat. A projected CRS has no antimeridian.
pub(crate) fn antimeridian_frame(
    frame: &Frame,
    domain: TopologyDomain,
    op: &str,
) -> crate::error::Result<GeographicFrame> {
    match domain {
        TopologyDomain::Geographic(frame) => Ok(frame),
        TopologyDomain::Cartesian if frame.crs_ref().is_none() => Ok(GeographicFrame::crs84()),
        TopologyDomain::Cartesian => Err(CrsError::invalid(format!(
            "{op} requires a geographic CRS; use set_crs(...) or to_crs(...) to attach one"
        ))),
    }
}

/// Whether any chain of `shape` crosses the seam of `frame`
/// ([`chain_crosses_antimeridian`]).
pub(crate) fn crosses_antimeridian_in_frame(shape: &Shape, frame: GeographicFrame) -> bool {
    shape
        .try_for_each_segment_chain(|chain| {
            if chain_crosses_antimeridian(chain, frame) {
                Err(())
            } else {
                Ok(())
            }
        })
        .is_err()
}

/// Whether a stored chain leaves the planar chart of `frame`: an edge takes
/// the long way between canonical longitudes, or runs more than a half turn
/// to or from the seam.
///
/// This is a property of the stored spelling, and deliberately so: it decides
/// whether planar evaluation of the stored coordinates is faithful. A ring
/// that touches the seam from the east but spells that vertex `-180` stores a
/// 350-degree edge and needs seam-aware handling, as does a stored edge that
/// winds past the seam around a pole.
pub(crate) fn chain_crosses_antimeridian<C: Coordinates + ?Sized>(
    chain: &C,
    frame: GeographicFrame,
) -> bool {
    let axis = frame.longitude();
    let half = axis.half_period();
    if let Some((xs, _)) = chain.xy_columns()
        && xs.iter().all(|x| x.abs() < half)
    {
        // Canonical longitudes off the seam: only a long step crosses.
        return steps_exceed(xs, half);
    }
    chain.segment_pairs().any(|[start, end]| {
        let raw_delta = (end.x - start.x).abs();
        let (Ok(start), Ok(end)) = (axis.canonicalize(start.x), axis.canonicalize(end.x)) else {
            return false;
        };
        if start.identity(axis) == SeamIdentity::Seam || end.identity(axis) == SeamIdentity::Seam {
            return raw_delta > half && raw_delta < axis.period().next_up();
        }
        (end.get() - start.get()).abs() > half
    })
}

/// Whether any consecutive pair of `xs` differs by more than `limit`.
fn steps_exceed(xs: &[f64], limit: f64) -> bool {
    let segments = xs.len().saturating_sub(1);
    if segments == 0 {
        return false;
    }
    let (x0, _) = xs[..segments].as_chunks::<REDUCE_LANES>();
    let (x1, _) = xs[1..=segments].as_chunks::<REDUCE_LANES>();
    simd_mask_any(
        segments,
        |index| (xs[index + 1] - xs[index]).abs() > limit,
        |start| {
            let chunk = start / REDUCE_LANES;
            (ReduceSimd::from_array(x1[chunk]) - ReduceSimd::from_array(x0[chunk]))
                .abs()
                .simd_gt(ReduceSimd::splat(limit))
        },
    )
}

/// Whether a planar topology op must split-normalize ``shape`` at ±180 before
/// evaluating. Cartesian domains skip the coordinate scan entirely.
pub(crate) fn geographic_crossing(domain: TopologyDomain, shape: &ShapeData) -> bool {
    match domain {
        TopologyDomain::Cartesian => false,
        TopologyDomain::Geographic(frame) => crosses_antimeridian_in_frame(shape.shape(), frame),
    }
}
