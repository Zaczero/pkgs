//! Optional Python length admission for iterator capacity planning.

use pyo3::ffi;
use pyo3::prelude::*;

fn type_has_slot(value: &Bound<'_, PyAny>, slot: std::ffi::c_int) -> bool {
    let ty = value.get_type();
    // SAFETY: `ty` is a live Python type and `slot` is one of the public,
    // supported length-slot identifiers used below.
    unsafe { !ffi::PyType_GetSlot(ty.as_type_ptr(), slot).is_null() }
}

/// Read Python's size only when the type supplies a length slot.
///
/// Missing length is normal for a one-shot iterator. Calling `len()` after
/// classification preserves the provider's descriptor behavior and exception.
/// Acquire the iterator first so its provider's exception keeps precedence.
pub fn optional_len(value: &Bound<'_, PyAny>) -> PyResult<Option<usize>> {
    if !type_has_slot(value, ffi::Py_mp_length) && !type_has_slot(value, ffi::Py_sq_length) {
        return Ok(None);
    }
    value.len().map(Some)
}
