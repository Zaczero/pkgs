//! Set-overlay (intersection/union/difference/symmetric-difference and
//! all-input variants) and linework set-ops (`line_merge`/`shared_paths`) on
//! `Shape`, plus the shared noding/assembly kernel behind them.

use crate::geometry::{CoordinateAxes, Shape, TopologyDomain};

/// Whether synthesized overlay output must source Z/M from inputs (`Strict`)
/// or may degrade to plain XY (`Lenient`).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Strictness {
    Strict,
    Lenient,
}

impl Strictness {
    pub(crate) const fn is_strict(self) -> bool {
        matches!(self, Self::Strict)
    }
}

impl From<bool> for Strictness {
    fn from(strict: bool) -> Self {
        if strict { Self::Strict } else { Self::Lenient }
    }
}

/// Set operation evaluated by the mixed-dimension overlay engine.
///
/// Semantics are planar Simple Features over X/Y; Z/M are restored after
/// topology and never participate in the covered-set computation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum OverlayOp {
    Intersection,
    Union,
    Difference,
    SymmetricDifference,
}

impl OverlayOp {
    pub(crate) const fn name(self) -> &'static str {
        match self {
            Self::Intersection => "intersection",
            Self::Union => "union",
            Self::Difference => "difference",
            Self::SymmetricDifference => "symmetric_difference",
        }
    }

    pub(crate) fn apply(
        self,
        left: &super::Shape,
        right: &super::Shape,
        strictness: Strictness,
    ) -> crate::error::Result<super::Shape> {
        match self {
            Self::Intersection => left.intersection(right, strictness),
            Self::Union => left.union(right, strictness),
            Self::Difference => left.difference(right, strictness),
            Self::SymmetricDifference => left.symmetric_difference(right, strictness),
        }
    }

    pub(crate) fn apply_in_frame(
        self,
        left: &super::Shape,
        right: &super::Shape,
        domain: TopologyDomain,
        strictness: Strictness,
    ) -> crate::error::Result<super::Shape> {
        match domain {
            TopologyDomain::Cartesian => self.apply(left, right, strictness),
            TopologyDomain::Geographic(_) => {
                super::overlay_all(&[left, right], domain, self, strictness)
            },
        }
    }
}

/// Build one occupied-support shape for a metric or moment consumer.
///
/// Cartesian inputs use the exact incidence partition directly. Geographic
/// inputs retain the geographic graph's seam and pole quotient; this wrapper
/// changes only the selected region, leaving the metric conversion to its
/// owner.
pub(crate) fn occupied_support_shape(
    shape: &Shape,
    domain: TopologyDomain,
) -> crate::error::Result<Shape> {
    match domain {
        TopologyDomain::Cartesian => occupied_cartesian_support(shape),
        TopologyDomain::Geographic(frame) => {
            if matches!(shape, Shape::Polygon(_) | Shape::MultiPolygon(_))
                && crate::geometry::validate_shape_in_frame(shape, domain)?.is_none()
            {
                // A frame-valid polygon already carries exactly one
                // shell/hole support per part. In particular, polar caps
                // cannot be reconstructed by a chart arrangement without
                // losing the quotient's implicit pole closure.
                return Ok(shape.clone());
            }
            if let Some(bounds) = shape.bounds()
                && crate::geometry::raw_coordinate_bounds_are_sound_in_frame(
                    Some(shape),
                    bounds,
                    frame,
                )
            {
                // A canonical, non-seam, non-polar geographic shape has one
                // chart with the same incidences as Cartesian longitude and
                // latitude. The exact partition is therefore equivalent to
                // the geographic quotient on this certified chart.
                occupied_cartesian_support(shape)
            } else {
                nary::occupied_geographic_support(shape, frame)
            }
        },
    }
}

/// Coordinate axes an empty overlay declares. Non-empty results resolve each
/// axis from contributing source values; an empty has no vertices to resolve,
/// so it retains the operation's full declared source layout instead.
pub(crate) fn overlay_declared_axes(inputs: &[&Shape], op: OverlayOp) -> CoordinateAxes {
    match op {
        OverlayOp::Difference => inputs
            .first()
            .map_or(CoordinateAxes::XY, |shape| shape.axes()),
        OverlayOp::Intersection | OverlayOp::Union | OverlayOp::SymmetricDifference => inputs
            .iter()
            .fold(CoordinateAxes::XY, |axes, shape| axes.union(shape.axes())),
    }
}

mod areal;
mod assembly;
mod clip;
mod exact_partition;
mod nary;
mod noding;
mod rect;
mod shape;

pub(in crate::geometry) use areal::{binary_areal_overlay, build_areal_arrangement};
pub(crate) use assembly::{
    build_certified_overlay_shape, build_overlay_shape, dedup_points, merge_chains,
};
pub(super) use clip::polygon_parts_to_shape;
pub(crate) use clip::{
    clip_multipoint_columns, clip_polygonal, clipped_line_parts, clipped_linestring,
    line_parts_to_shape,
};
#[cfg(test)]
pub(crate) use exact_partition::cartesian_support_nonempty;
pub(in crate::geometry) use exact_partition::{
    ExactChainView, ExactLineEmission, ExactMetricSpan, ExactMetricSupport, ExactPartition,
    ExactSourceInventory, NodeId, RayAxis, RayPoints, certify_emitted_incidence, shoot_left,
};
pub(crate) use exact_partition::{
    exact_intersects_bounds, occupied_cartesian_area, occupied_cartesian_support,
};
#[cfg(test)]
pub(in crate::geometry) use exact_partition::{
    partition_builds, partition_metrics, reset_partition_builds,
};
#[cfg(test)]
pub(crate) use exact_partition::{partition_calls, reset_partition_calls};
pub(in crate::geometry) use nary::{
    VoronoiIntermediateOverlay, binary_cartesian_overlay_without_partition_budget,
    overlay_all_geographic_from, overlay_view_error,
};
pub(crate) use nary::{empty_shape_for_dimension_with_axes, overlay_all};
#[cfg(test)]
pub(crate) use noding::self_node_segments;
pub(crate) use noding::self_node_segments_sourced;
pub(in crate::geometry) use noding::{NodingScan, full_noding_events};
pub(crate) use rect::rect_polygon;
#[cfg(test)]
mod tests;
