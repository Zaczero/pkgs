//! Derived single-geometry outputs from `Shape`: centroid, representative
//! point on surface, envelope, convex/concave hull, and pole of
//! inaccessibility.

mod centroid;
mod concave;
mod hull;
mod minimum_rectangle;
mod polylabel;
mod shape;
mod surface;

pub(crate) use centroid::{
    areal_centroid, centroid_is_empty, centroid_line_row_columns, centroid_polygon_row_columns,
    lineal_centroid, point_centroid,
};
pub(crate) use concave::{canonicalize_concave_hull_points, native_concave_hull};
pub(crate) use hull::{
    geometric_xy_cmp, monotone_chain_hull, shape_from_open_hull, shape_from_selected_hull,
};
pub(crate) use minimum_rectangle::minimum_area_rectangle;
pub(crate) use polylabel::{
    build_inscribed_circle_budgeted, degenerate_polygon_center, emit_minimum_bounding_circle,
    emit_zero_radius_point, polylabel_point, scale_aware_default_tolerance,
    smallest_enclosing_circle,
};
pub(crate) use surface::{
    collection_surface_point, lineal_surface_point, multipoint_surface_point,
    point_on_surface_line_columns, point_on_surface_polygon_row_columns, polygonal_surface_point,
};

#[cfg(test)]
mod tests;
