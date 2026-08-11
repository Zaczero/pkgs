//! Binary set-overlay (intersection/union/difference/symmetric-difference,
//! `union_all`) and linework set-ops (`line_merge`/`shared_paths`) on `Shape`,
//! plus the noding/assembly kernel behind them.

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

mod areal;
mod assembly;
mod clip;
mod mixed;
mod nary;
mod noding;
mod parts;
mod rect;
mod shape;

pub(crate) use areal::bbox_overlap_clusters_for_bounds;
pub(in crate::geometry) use areal::{
    binary_areal_overlay, build_areal_arrangement, dissolve_polygons, outside_winding_seeds,
    polygon_winding_loops,
};
pub(crate) use assembly::{
    build_overlay_shape, disjoint_overlay_combine, empty_overlay_shape, line_line_cross_points,
    merge_chains,
};
pub(super) use clip::polygon_parts_to_shape;
pub(crate) use clip::{
    clip_multipoint_columns, clip_polygonal, clipped_line_parts, clipped_linestring,
    line_parts_to_shape,
};
pub(in crate::geometry) use clip::{clip_polygonal_parts, window_clip_shape};
pub(crate) use mixed::{
    collect_line_segments, dedup_points, for_each_overlapping_pair, overlay_lines_general,
    overlay_points, polygon_line_touch_points, rect_boundary_contact, union_lines,
};
pub(crate) use nary::{
    OverlayWorkItem, empty_nary_overlay_shape, empty_shape_for_dimension,
    has_mixed_non_empty_topological_dimensions, overlay_work_clusters,
    symmetric_difference_cluster_balanced, symmetric_difference_cluster_ordered,
};
pub(in crate::geometry) use noding::full_noding_events;
pub(crate) use noding::{
    dedup_segments_to_coordseqs, for_each_segment_overlap_point, node_segments, self_node_segments,
    self_node_segments_sourced,
};
pub(crate) use parts::{
    DimensionalParts, contained_shortcut_cached, contained_shortcut_impl,
    window_clip_large_operands,
};
pub(crate) use rect::{RectSide, axis_rectangle, proper_rect_intersection, rect_polygon};
impl OverlayOp {
    /// The pure shape kernel shared by the array and free broadcast paths
    /// (frame resolution lives in the broadcast engine, once per pair-set).
    /// The CONTAINED gate runs on the prepared handles first: a
    /// broadcast's fixed operand builds its cached engines once for every
    /// pair.
    pub(crate) fn kernel(
        self,
        strictness: Strictness,
    ) -> impl Fn(&super::ShapeData, &super::ShapeData) -> crate::error::Result<super::Shape>
    + Copy
    + Send
    + Sync {
        move |left, right| {
            if let Some(shape) = contained_shortcut_cached(left, right, self) {
                // Faithful re-presentation of an input operand — Z/M is correct
                // as-is under both policies (see `Shape::overlay_inner`).
                return Ok(shape);
            }
            self.apply(left.shape(), right.shape(), strictness)
        }
    }
}

#[cfg(test)]
pub(crate) mod test_counters {
    use std::cell::Cell;

    thread_local! {
        static ENABLED: Cell<bool> = const { Cell::new(false) };
        static COUNT: Cell<u64> = const { Cell::new(0) };
    }

    pub(crate) fn enable() {
        ENABLED.with(|enabled| enabled.set(true));
    }

    pub(crate) fn disable() {
        ENABLED.with(|enabled| enabled.set(false));
    }

    pub(crate) fn reset() {
        COUNT.with(|count| count.set(0));
    }

    pub(crate) fn count() -> u64 {
        COUNT.with(std::cell::Cell::get)
    }

    pub(crate) fn bump_candidate_pair() {
        ENABLED.with(|enabled| {
            if enabled.get() {
                COUNT.with(|count| count.set(count.get().saturating_add(1)));
            }
        });
    }
}

#[cfg(test)]
mod tests;
