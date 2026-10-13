#![expect(
    clippy::similar_names,
    reason = "axis and formula symbols name paired quantities here (minx/miny, xs/ys, lat_rad/lam_rad, step/stop), which this lint reads as typos"
)]
use std::collections::HashMap;

use crate::NonNegative;
use crate::geometry::constructive::{
    Result, SegmentPlacement, decimal_scale, normalized_line, quantize_column_simd,
    remove_repeated_line_points, remove_repeated_points, remove_repeated_ring_points,
    segmentize_points_budgeted, snap_column_simd, split,
};
use crate::geometry::numerics::product::{
    DekkerParts, binary_zero, checked_normal_sum, floor_binary_exponent, normal_or_zero,
    two_product_dekker_admitted, two_product_dekker_checked_parts, two_sum,
};
use crate::geometry::numerics::product_sum::ExactSum;
use crate::geometry::overlay::{
    OverlayOp, overlay_all_geographic_from_with_output_frame, overlay_intersection_with_rectangle,
    overlay_view_error,
};
use crate::geometry::{
    Bounds, CoordSeq, CoordinateAxes, ExpansionBudget, GeometryErrorKind, GeometryKind, HasM, HasZ,
    LineSeq, MOrdinate, MultiPoint, MultiPolygon, OrdinateAxis, Point, Polygon, ReduceSimd, Shape,
    ShapeOwnerKey, Strictness, ZOrdinate, axis_pow2_scale, column_all_finite, compare_point_slices,
    compare_points, compare_polygons, compare_shapes, same_topological_coordinate,
    set_point_ordinate, simd_xy_map_f64, simd_xy_map_one_f64,
};

/// A parsed `origin` argument for the ergonomic affine helpers: parsed
/// once per call, resolved per shape — which also lets array bindings
/// notice when the origin is per-shape (`Centroid`/`Center`, an identity
/// for packed point rows) versus one fixed pivot.
#[derive(Clone, Copy)]
pub enum OriginSpec {
    Centroid,
    Center,
    Fixed(f64, f64),
}

impl OriginSpec {
    pub(crate) fn resolve_shape(self, shape: &Shape) -> crate::error::Result<(f64, f64)> {
        Ok(match self {
            Self::Centroid => shape.centroid_xy()?,
            Self::Center => shape.bounds_center_xy(),
            Self::Fixed(x, y) => (x, y),
        })
    }
}

mod affine;
mod chains;
mod constructive;
mod coordinates;
mod order;
mod shape;

#[cfg(test)]
pub(crate) use affine::affine_about_origin_ordinate;
pub(super) use affine::affine_ordinate_simd;
#[cfg(test)]
pub(super) use affine::fast_affine_about_origin_ordinate;
pub(crate) use affine::{
    AffineLinear, OriginAffine, affine_x_row_identity, affine_y_row_identity, stable_affine_xy,
};
