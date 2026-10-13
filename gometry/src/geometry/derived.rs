//! Derived single-geometry outputs from `Shape`: centroid, representative
//! point on surface, envelope, convex/concave hull, and pole of
//! inaccessibility.

mod centroid;
mod concave;
mod hull;
mod minimum_circle;
mod minimum_rectangle;
mod polylabel;
mod shape;
mod surface;

pub(crate) use centroid::{
    PointSetCenter, areal_centroid, areal_centroid_with_bounds, centroid_is_empty,
    centroid_line_row_columns, centroid_polygon_row_columns,
    centroid_polygon_row_columns_with_bounds, exact_point_chart_centroid, lineal_centroid,
    point_centroid, point_set_center_summary,
};
pub(crate) use concave::{canonicalize_concave_hull_points, native_concave_hull};
pub(crate) use hull::{
    geometric_xy_cmp, monotone_chain_chart_hull, monotone_chain_hull, shape_from_open_hull,
    shape_from_selected_hull,
};
pub(crate) use minimum_circle::{emit_minimum_bounding_circle, smallest_enclosing_circle};
pub(crate) use minimum_rectangle::{minimum_area_rectangle, minimum_area_rectangle_chart};
pub(crate) use polylabel::{
    build_inscribed_circle_budgeted, degenerate_polygon_center, emit_zero_radius_point,
    polylabel_point, scale_aware_default_tolerance,
};
pub(crate) use surface::{
    collection_surface_point, lineal_surface_point, multipoint_surface_point,
    point_on_surface_line_columns, point_on_surface_polygon_row_columns,
    point_set_surface_point_at, polygonal_surface_point,
};

#[cfg(test)]
mod tests;
