#![expect(
    clippy::similar_names,
    reason = "axis and formula symbols name paired quantities here (minx/miny, xs/ys, lat_rad/lam_rad, step/stop), which this lint reads as typos"
)]
use std::cmp::Ordering;

use ahash::HashSetExt as _;

use crate::geometry::constructive::{
    Result, WalkColumns, WalkCount, WalkJoin, WalkJoinRule, WalkPlan, WalkSink, close_xy_loop,
    emit_cap, extend_cleaned, materialize_walk, middle_outside_segment_bounds, raw_offset_loop,
    sided_buffer_loop, strict_cycle, winding_region,
};
use crate::geometry::numerics::product::{axis_pow2_scale, scaled_residual};
use crate::geometry::overlay::ExactSourceInventory;
use crate::geometry::{
    BufferCapStyle, CoordSeq, CoordWindow, Coordinates as _, ExpansionBudget, GeometryErrorKind,
    GeometryKind, HashSet, LineSeq, MultiPoint, Orientation, Point, Polygon, Ring, Shape, XY,
    canonical_f64_bits, column_all_finite, dedup_consecutive_points, interpolate_f64,
    line_is_simple, orientation, polygon_parts_to_shape, power_of_two_exponent, ring_winding,
    same_point, same_topological_coordinate, scale_by_power_of_two, shell_is_convex,
};

mod arcs;
mod degenerate;
mod loops;
mod polygon;
pub(in crate::geometry) mod stroke;

pub(crate) use arcs::{emit_arc, source_vector, unit_right_normal};
pub(crate) use degenerate::degenerate_polygonal_as_linework;
pub(crate) use loops::{multipoint_loops, point_buffer};
pub(crate) use polygon::convex_buffer_budgeted;
#[cfg(test)]
pub(crate) use polygon::{convex_buffer, winding_buffer, winding_erosion};
pub(in crate::geometry) use polygon::{
    winding_buffer_budgeted, winding_collection_budgeted, winding_erosion_budgeted,
};
#[cfg(test)]
pub(crate) use stroke::winding_stroke;
pub(in crate::geometry) use stroke::{winding_sided_stroke_budgeted, winding_stroke_budgeted};
