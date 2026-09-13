//! Unified dispatch spine — scalar/array/grid routing for geometry ops.
//!
//! Routes the unary, binary, and packed fast-path lanes. [`BulkElement`] lives
//! in `broadcast`; [`Operation`] and [`MetricResolver`] centralize op names and
//! CRS metric policy.

mod binary;
mod binary_fast;
pub(crate) mod kernels;
mod linref;
mod metric;
mod operation;
mod packed;
mod unary;

pub(crate) use binary::{
    NoBinaryFastPath, admit_binary, admit_binary_inputs, dispatch_binary,
    dispatch_binary_array_left, dispatch_binary_geometry, dispatch_binary_geometry_admitted,
    dispatch_typed_metric_binary, geometry_kernel_over_array,
};
pub(crate) use binary_fast::{
    OverlayFastPath, dispatch_broadcast2, dispatch_distance, dispatch_distance_3d,
    dispatch_dwithin_admitted, dispatch_frechet_distance, dispatch_hausdorff_distance,
    dispatch_overlay, dispatch_predicate,
};
pub(crate) use linref::{
    line_interpolate_points_scalar, line_locate_point_input, line_locate_point_m_input,
};
pub(crate) use metric::{CachelessRowContext, MetricResolver, MetricScratch, UnaryMeasureKernel};
pub(crate) use operation::{MetricOp, Operation};
pub(crate) use packed::{PackedLinearReference, PackedUnary};
pub(crate) use unary::{
    dispatch_unary, unary_array, unary_array_linref_shapes, unary_array_shapes,
    unary_array_shapes_budgeted, unary_scalar, unary_scalar_linref_shape, unary_scalar_shape,
    unary_scalar_shape_admitted,
};
