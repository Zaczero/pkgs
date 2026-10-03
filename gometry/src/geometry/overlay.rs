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
            if matches!(shape, Shape::Polygon(_) | Shape::MultiPolygon(_))
                && crate::geometry::validate_shape_in_frame(shape, domain)?.is_none()
            {
                // A frame-valid polygon already carries exactly one
                // shell/hole support per part. In particular, polar caps
                // cannot be reconstructed by a chart arrangement without
                // losing the quotient's implicit pole closure.
                return Ok(shape.clone());
            }
            nary::occupied_geographic_support(shape, frame)
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

pub(in crate::geometry) use areal::ray_crossing;
#[cfg(test)]
pub(in crate::geometry) use areal::{binary_areal_overlay, build_areal_arrangement};
pub(crate) use assembly::{
    build_certified_overlay_shape, build_certified_overlay_shape_with_axes, merge_chains,
};
pub(crate) use clip::intersect_rectangular_polygonal;
pub(super) use clip::polygon_parts_to_shape;
#[cfg(test)]
pub(crate) use exact_partition::cartesian_support_nonempty;
pub(in crate::geometry) use exact_partition::{
    ExactChainView, ExactLineEmission, ExactMetricSpan, ExactMetricSupport, ExactPartition,
    ExactSourceInventory, NodeId, RayAxis, RayPoints, clip_polygonal_rectangle,
    emitted_pair_preserves_incidence, segments_within_chebyshev, shoot_left, visit_component_faces,
};
#[cfg(test)]
pub(in crate::geometry) use exact_partition::{
    ExactPointForest, StagedExactSources, StagedRelationItem, StagedSourceComponent,
    homogeneous_midpoint,
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
pub(crate) use nary::{
    overlay_all, overlay_all_geographic_chart, overlay_intersection_with_rectangle,
};
#[cfg(test)]
pub(crate) use noding::self_node_segments_sourced;
#[cfg(test)]
pub(in crate::geometry) use noding::{NodingScan, full_noding_events};
pub(crate) use rect::{rect_polygon, rectangle_bounds};
#[cfg(test)]
mod tests;
