//! Constructive/editing transforms on `Shape`: buffer, offset, simplify,
//! snap, segmentize, reverse, orient, normalize, clip — and (below) quantize,
//! the `set_z`/`set_m` ordinate setters, `force_2d`, and the affine family.

use crate::error::Result;
mod buffer;
mod edit;
mod offset;
mod ring;
mod simplify;
mod smooth;
mod snap;
mod walk;
mod winding;

pub(crate) use edit::{affine_about_origin_ordinate, stable_affine_xy};
pub(crate) use offset::{DEFAULT_MITER_LIMIT, reversed_points, sided_strip_parts};
pub(in crate::geometry) use ring::{canonical_ring_seq, oriented_ring};
pub(crate) use ring::{
    decimal_scale, normalized_line, orient_ring, quantize_column_simd, quantize_to_scale,
    snap_column_simd, stable_snap_ordinate, vw_keep,
};
pub(crate) use simplify::{raw_offset_loop, rdp_keep, split, strict_cycle, validate_buffer_style};
#[cfg(test)]
pub(crate) use snap::densify_points_budgeted;
pub(crate) use snap::{
    SegmentPlacement, SnapReference, densify_points_budgeted_with_placement,
    remove_repeated_line_points, remove_repeated_points, segmentize_points_budgeted,
    snap_ring_to_reference, verify_planar_segmentize_output, verify_planar_segmentize_shape,
};
pub(crate) use walk::{
    WalkColumns, WalkCount, WalkJoin, WalkJoinRule, WalkPlan, WalkSink, close_xy_loop, emit_cap,
    extend_cleaned, materialize_walk,
};
pub(in crate::geometry) use walk::{assemble_region_polygons, winding_region};
#[cfg(test)]
pub(crate) use walk::{reset_walk_plan_construction_count, walk_plan_construction_count};
#[cfg(test)]
pub(crate) use winding::{convex_buffer, winding_buffer, winding_erosion, winding_stroke};
pub(crate) use winding::{
    convex_buffer_budgeted, degenerate_polygonal_as_linework, emit_arc, multipoint_loops,
    point_buffer, unit_right_normal,
};
pub(in crate::geometry) use winding::{
    winding_buffer_budgeted, winding_collection_budgeted, winding_erosion_budgeted,
    winding_stroke_budgeted,
};
#[cfg(test)]
mod tests;
