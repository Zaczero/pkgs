//! Unified dispatch spine — scalar/array/grid routing for geometry ops.
//!
//! Routes the unary, binary, and packed fast-path lanes. [`BulkElement`] lives
//! in [`element`]; [`Operation`] and [`MetricResolver`] centralize op names and
//! CRS metric policy.

mod binary;
mod binary_fast;
mod element;
pub(crate) mod kernels;
mod linref;
mod metric;
mod operation;
mod packed;
mod simple;
mod unary;

pub(crate) use binary::{
    NoBinaryFastPath, dispatch_binary, dispatch_binary_array_left, dispatch_binary_geometry,
    geometry_kernel_over_array,
};
pub(crate) use binary_fast::{
    OverlayFastPath, dispatch_broadcast2, dispatch_distance, dispatch_distance_3d,
    dispatch_dwithin, dispatch_frechet_distance, dispatch_hausdorff_distance, dispatch_overlay,
    dispatch_predicate,
};
pub(crate) use element::BulkElement;
pub(crate) use linref::{
    line_interpolate_points_scalar, line_locate_point_input, line_locate_point_m_input,
};
pub(crate) use metric::{MetricResolver, MetricScratch};
pub(crate) use operation::Operation;
pub(crate) use packed::PackedUnary;
pub(crate) use simple::dispatch_unary_simple_same;
pub(crate) use unary::{
    dispatch_unary, unary_array, unary_array_shapes, unary_scalar, unary_scalar_shape,
};
