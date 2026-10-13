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
}

/// Build one occupied-support shape for a metric or moment consumer.
///
/// Ordinary geographic charts share the exact partition route with Cartesian
/// overlays. Seam and pole inputs retain Q's quotient; this wrapper changes
/// only selected support and leaves metric conversion to its owner.
pub(crate) fn occupied_support_shape(
    shape: &Shape,
    domain: TopologyDomain,
) -> crate::error::Result<Shape> {
    match domain {
        TopologyDomain::Cartesian => occupied_cartesian_support(shape),
        TopologyDomain::Geographic(frame) => {
            let support = shape.force_2d()?;
            if matches!(&support, Shape::Polygon(_) | Shape::MultiPolygon(_))
                && crate::geometry::validate_shape_in_frame(&support, domain)?.is_none()
            {
                // A frame-valid polygon already carries exactly one
                // shell/hole support per part. In particular, polar caps
                // cannot be reconstructed by a chart arrangement without
                // losing the quotient's implicit pole closure.
                return Ok(support);
            }
            nary::occupied_geographic_support(&support, frame)
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
pub(in crate::geometry) use exact_partition::EmbeddingDomain;
mod nary;
mod noding;
mod rect;
mod shape;

#[cfg(test)]
pub(in crate::geometry) use areal::binary_areal_overlay;
pub(in crate::geometry) use areal::ray_crossing;
pub(crate) use assembly::{
    CertifiedOverlayOrder, build_certified_overlay_shape, build_certified_overlay_shape_with_axes,
    build_certified_overlay_shape_with_axes_and_order, merge_chains,
};
pub(crate) use clip::intersect_rectangular_polygonal;
pub(super) use clip::polygon_parts_to_shape;
#[cfg(test)]
pub(crate) use exact_partition::cartesian_support_nonempty;
pub(in crate::geometry) use exact_partition::subdivision::{
    SubdivisionPart, SubdivisionRootCatalog, SubdivisionTraceRequest,
};
pub(in crate::geometry) use exact_partition::{
    AreaLocationStrategy, ExactChainView, ExactLineEmission, ExactMetricSpan, ExactMetricSupport,
    ExactPartition, ExactPointForest, ExactSourceInventory, GlobalFaces, NodeId,
    OverlayNodeReceipt, OverlayPolygonPartWithSources, OverlaySourceEdgeUse, RayAxis, RayPoints,
    RayPointsWithWork, StagedExactSources, StagedRelationItem, StagedSourceComponent,
    clip_polygonal_rectangle, compare_departures_with_work, emitted_pair_preserves_incidence,
    graph_global_faces_with_work, homogeneous_midpoint, segments_within_chebyshev, shoot_left,
    shoot_left_with_work, visit_component_faces, walk_is_shell_with_work,
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
#[cfg(test)]
pub(crate) use exact_partition::{
    reset_source_span_view_order_counts, reset_subdivision_resolution_batches,
    reset_subdivision_root_xy_position_calls, source_span_view_order_counts,
    subdivision_root_xy_position_calls, take_subdivision_resolution_batches,
};
pub(in crate::geometry) use nary::{
    binary_cartesian_difference_without_partition_budget,
    binary_cartesian_intersection_with_edge_roles, overlay_all_geographic_from_with_metric,
    overlay_all_geographic_from_with_output_frame, overlay_view_error,
};
pub(crate) use nary::{
    overlay_all, overlay_all_geographic_chart, overlay_all_geographic_output_with_metric_and_frame,
    overlay_all_geographic_with_metric_and_frame, overlay_all_with_metric,
    overlay_intersection_with_rectangle,
};
pub(crate) use rect::{rect_polygon, rectangle_bounds};
#[cfg(test)]
#[path = "overlay/clip/extracted_tests.rs"]
mod rectangle_clip_tests;
#[cfg(test)]
#[path = "overlay/tests/support.rs"]
mod test_support;
