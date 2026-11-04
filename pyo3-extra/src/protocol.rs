//! Numeric protocol calls preserving provider exceptions and ownership.
use pyo3::ffi;
use pyo3::prelude::*;
/// Whether Python's numeric index slot is present on the value's type.
///
/// This is deliberately a type-slot query rather than an attribute lookup:
/// an instance-only ``__index__`` attribute is not part of Python's index
/// protocol and must not be invoked or used for classification.
pub fn has_index(value: &Bound<'_, PyAny>) -> bool {
    // SAFETY: `value` is a live Python object while its `Bound` is held.
    unsafe { ffi::PyIndex_Check(value.as_ptr()) != 0 }
}
pub fn index<'py>(value: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
    // SAFETY: `PyNumber_Index` borrows `value`, returns a new reference on
    // success, and sets the active Python exception on failure.
    unsafe { Bound::from_owned_ptr_or_err(value.py(), ffi::PyNumber_Index(value.as_ptr())) }
}
