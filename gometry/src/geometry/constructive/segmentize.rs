//! Route-aware subdivision of coordinate sequences.

use std::num::NonZeroUsize;
use std::ops::Range;
use std::sync::Arc;

use crate::geographic::GeographicFrame;
use crate::geometry::constructive::Result;
use crate::geometry::generated_budget::{ExpansionBudget, GENERATED_ITEM_LIMIT};
use crate::geometry::types::ensure_coordseq_vertex_capacity;
use crate::geometry::{
    CoordSeq, Coordinates as _, GeometryErrorKind, Point, Segment, XY, axis_pow2_scale,
    interpolate_subdivision_ordinate, line_segments, point_distance, same_point, scaled_residual,
};

mod chart;
mod output;
mod plan;
mod routes;

pub(crate) use chart::ChartSourceParameters;
use chart::ChartWithSourceParameters;
pub(crate) use output::{SegmentPlacement, SegmentPlacementOwned};
use plan::{Target, subdivide};
use routes::subdivide_placed;

pub(crate) fn segmentize_points_budgeted(
    points: &CoordSeq,
    max_segment_length: f64,
    placement: &SegmentPlacement<'_>,
    budget: &mut ExpansionBudget,
) -> Result<CoordSeq> {
    subdivide_placed(
        points,
        placement,
        Target::MaxLength(max_segment_length),
        budget,
    )
}

/// Place chart-edge vertices using counts certified by the geographic local
/// projection certificate. The ordinary subdivision path provides route
/// evaluation, ordinate interpolation, output columns, and the caller-owned
/// generated-work budget.
pub(crate) fn segmentize_points_with_piece_counts_budgeted(
    points: &CoordSeq,
    piece_counts: &[NonZeroUsize],
    placement: &SegmentPlacement<'_>,
    budget: &mut ExpansionBudget,
) -> Result<CoordSeq> {
    if piece_counts.len() != points.len().saturating_sub(1) {
        return Err(GeometryErrorKind::message(
            "chart-edge piece counts do not match the coordinate sequence",
        ));
    }
    if piece_counts.iter().all(|pieces| pieces.get() == 1) {
        return Ok(points.clone());
    }
    subdivide_placed(points, placement, Target::Pieces(piece_counts), budget)
}

/// Segmentize geographic chart edges while retaining each emitted source
/// parameter for the clearance-support consumer.
pub(crate) fn segmentize_chart_points_with_piece_counts_budgeted(
    points: &CoordSeq,
    piece_counts: &[NonZeroUsize],
    geodesic: &geographiclib_rs::Geodesic,
    frame: GeographicFrame,
    budget: &mut ExpansionBudget,
) -> Result<(CoordSeq, ChartSourceParameters)> {
    if piece_counts.len() != points.len().saturating_sub(1) {
        return Err(GeometryErrorKind::message(
            "chart-edge piece counts do not match the coordinate sequence",
        ));
    }
    let mut route = ChartWithSourceParameters::new(geodesic, frame, piece_counts.len());
    let refined = subdivide(&mut route, points, Target::Pieces(piece_counts), budget)?;
    let source_parameters = route.finish()?;
    Ok((refined, source_parameters))
}

/// Insert `ceil(1 / fraction) - 1` evenly spaced vertices into every segment
/// (the GEOS densify semantics): each segment is split into equal pieces no
/// longer than `fraction` of its own length, re-segmenting continuous
/// Hausdorff and refining the vertex sequence for discrete Fréchet. `fraction` is in
/// `(0, 1]`; `1` keeps the vertices unchanged.
#[cfg(test)]
pub(crate) fn densify_points_budgeted(
    points: &CoordSeq,
    fraction: f64,
    budget: &mut ExpansionBudget,
) -> Result<CoordSeq> {
    densify_points_budgeted_with_placement(points, fraction, &SegmentPlacement::Planar, budget)
}

pub(crate) fn densify_points_budgeted_with_placement(
    points: &CoordSeq,
    fraction: f64,
    placement: &SegmentPlacement<'_>,
    budget: &mut ExpansionBudget,
) -> Result<CoordSeq> {
    let steps = (1.0 / fraction).ceil() as usize;
    subdivide_placed(points, placement, Target::Fraction(steps), budget)
}

#[cfg(test)]
mod tests;
