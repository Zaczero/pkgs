//! Unified dispatch spine — scalar/array/grid routing for geometry ops.
//!
//! Routes the unary, binary, and packed execution lanes. [`BulkElement`] lives
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
    admit_binary, admit_binary_inputs, dispatch_binary_array_left, dispatch_binary_geometry,
    dispatch_binary_geometry_admitted_with_resolver, dispatch_typed_metric_binary,
};
pub(crate) use binary_fast::{
    dispatch_broadcast2, dispatch_distance, dispatch_distance_3d, dispatch_dwithin_admitted,
    dispatch_frechet_distance, dispatch_hausdorff_distance, dispatch_overlay, dispatch_predicate,
};
pub(crate) use linref::{line_interpolate_points_scalar, line_locate_input};
#[cfg(test)]
pub(crate) use metric::Lane;
pub(crate) use metric::{ LinearReferenceRow, MetricCtx, MetricResolver, MetricRowContext, MetricScratch,
    UnaryMeasureKernel,
};
pub(crate) use operation::{MetricOp, Operation};
pub(crate) use packed::{PackedLinearReference, PackedUnary};
pub(crate) use unary::{
    dispatch_unary, unary_array, unary_array_linref_shapes, unary_array_linref_with_model,
    unary_array_shapes, unary_array_shapes_budgeted, unary_scalar, unary_scalar_linref_column,
    unary_scalar_linref_shape, unary_scalar_shape,
};
