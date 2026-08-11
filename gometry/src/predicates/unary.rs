#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use pyo3::prelude::*;
use pyo3::types::PyAny;

use crate::dispatch::Operation;
use crate::geometry::is_geographic_frame;
use crate::py::numpy::bool_array;

macro_rules! unary_predicate {
    ($scalar:ident, $array:ident, $op:ident, $kernel:ident) => {
        pub(crate) fn $scalar(py: Python<'_>, geom: &crate::PyGeometry) -> PyResult<bool> {
            crate::dispatch::unary_scalar(
                py,
                geom,
                crate::dispatch::Operation::$op,
                None,
                crate::dispatch::kernels::$kernel,
            )
        }

        pub(crate) fn $array(
            py: Python<'_>,
            values: &crate::PyGeometryArray,
        ) -> PyResult<Py<PyAny>> {
            crate::dispatch::unary_array(
                py,
                values,
                crate::dispatch::Operation::$op,
                None,
                None,
                crate::dispatch::kernels::$kernel,
            )
        }
    };
}

unary_predicate!(is_empty_scalar, is_empty_array, IsEmpty, unary_is_empty);
unary_predicate!(is_closed_scalar, is_closed_array, IsClosed, unary_is_closed);
unary_predicate!(is_ring_scalar, is_ring_array, IsRing, unary_is_ring);
unary_predicate!(is_ccw_scalar, is_ccw_array, IsCcw, unary_is_ccw);
unary_predicate!(is_convex_scalar, is_convex_array, IsConvex, unary_is_convex);
unary_predicate!(is_valid_scalar, is_valid_array, IsValid, unary_is_valid);
unary_predicate!(
    crosses_antimeridian_scalar,
    crosses_antimeridian_array,
    CrossesAntimeridian,
    unary_crosses_antimeridian
);

/// Scalar simplicity — shared kernel with the array lane.
pub(crate) fn is_simple_scalar(py: Python<'_>, geom: &crate::PyGeometry) -> PyResult<bool> {
    crate::dispatch::unary_scalar(
        py,
        geom,
        Operation::IsSimple,
        None,
        crate::dispatch::kernels::unary_is_simple,
    )
}

/// Array simplicity: packed dense/nullable path first in a SMALL function,
/// then the shared per-row unary fallback. Splitting keeps the common dense
/// packed getter off the monomorphized UnaryRowMode/frame-cache body that
/// grew the getter ~19% and added frontend stalls (round-6 audit).
pub(crate) fn is_simple_array(
    py: Python<'_>,
    values: &crate::PyGeometryArray,
) -> PyResult<Py<PyAny>> {
    if let Some(result) = try_is_simple_packed(py, values) {
        return result;
    }
    is_simple_array_row_fallback(py, values)
}

/// Dense + nullable packed is_simple only. Geographic antimeridian crossings
/// fall through so the frame-aware per-row lane owns them.
fn try_is_simple_packed(
    py: Python<'_>,
    array: &crate::PyGeometryArray,
) -> Option<PyResult<Py<PyAny>>> {
    if is_geographic_frame(&array.frame)
        && array
            .storage()
            .iter_shapes()
            .any(|shape| shape.crosses_antimeridian())
    {
        return None;
    }
    array
        .is_simple_unary_packed()
        .map(|values| bool_array(py, values))
}

fn is_simple_array_row_fallback(
    py: Python<'_>,
    values: &crate::PyGeometryArray,
) -> PyResult<Py<PyAny>> {
    crate::dispatch::unary_array(
        py,
        values,
        Operation::IsSimple,
        None,
        None,
        crate::dispatch::kernels::unary_is_simple,
    )
}
