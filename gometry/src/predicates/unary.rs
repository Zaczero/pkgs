#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use pyo3::prelude::*;
use pyo3::types::PyAny;

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
unary_predicate!(is_simple_scalar, is_simple_array, IsSimple, unary_is_simple);
unary_predicate!(is_valid_scalar, is_valid_array, IsValid, unary_is_valid);
unary_predicate!(
    crosses_antimeridian_scalar,
    crosses_antimeridian_array,
    CrossesAntimeridian,
    unary_crosses_antimeridian
);
