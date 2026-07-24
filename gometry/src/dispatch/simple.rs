//! Unary dispatch for ops whose scalar/array lanes delegate to existing
//! ``PyGeometry`` / ``PyGeometryArray`` methods (packed fast paths, cross-row
//! assembly, frame transforms, …) instead of a per-row spine kernel.

use pyo3::prelude::*;
use pyo3::types::PyAny;

use crate::broadcast::{GeometryInput, classify_input, expected_geometry_or_array_for};

/// Classify once, then run scalar or array closures that delegate to the
/// internal geometry helpers (preserves packed-column and cross-row semantics).
pub(crate) fn dispatch_unary_simple_same<T>(
    value: &Bound<'_, PyAny>,
    scalar: impl FnOnce(&crate::PyGeometry) -> PyResult<T>,
    array: impl FnOnce(&crate::PyGeometryArray) -> PyResult<T>,
) -> PyResult<T> {
    match classify_input(value) {
        Some(GeometryInput::One(geometry)) => scalar(geometry),
        Some(GeometryInput::Many(values)) => array(values),
        None => Err(expected_geometry_or_array_for(value)),
    }
}
