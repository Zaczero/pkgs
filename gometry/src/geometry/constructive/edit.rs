use std::collections::HashMap;

use crate::NonNegative;
use crate::geometry::constructive::{
    Result, SegmentPlacement, normalized_line, quantize_column, remove_repeated_line_points,
    remove_repeated_points, remove_repeated_ring_points, segmentize_points_budgeted,
    snap_column_simd, split,
};
use crate::geometry::numerics::product::{
    DekkerParts, binary_zero, checked_normal_sum, floor_binary_exponent, normal_or_zero,
    two_product_dekker_admitted, two_product_dekker_checked_parts, two_sum,
};
use crate::geometry::numerics::product_sum::ExactSum;
use crate::geometry::{
    CoordSeq, CoordinateAxes, ExpansionBudget, GeometryErrorKind, GeometryKind, HasM, HasZ,
    LineSeq, MOrdinate, MultiPoint, MultiPolygon, OrdinateAxis, Point, Polygon, ReduceSimd, Shape,
    ShapeOwnerKey, ZOrdinate, axis_pow2_scale, column_all_finite, compare_point_slices,
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

use affine::affine_ordinate_simd;
pub(crate) use affine::{
    AffineLinear, OriginAffine, affine_x_row_identity, affine_y_row_identity, stable_affine_xy,
};
