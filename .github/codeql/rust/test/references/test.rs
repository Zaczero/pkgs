//! Reference conversions use the real PyO3 and CPython API declarations.

use pyo3::types::PyTuple;
use pyo3::{Bound, PyAny, PyResult, Python, ffi};

pub fn bad_index<'py>(value: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
    unsafe { Bound::from_borrowed_ptr_or_err(value.py(), ffi::PyNumber_Index(value.as_ptr())) }
}

pub fn bad_alias<'py>(value: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
    use pyo3::Bound as Handle;
    use pyo3::ffi::PyNumber_Index as index;
    unsafe { Handle::from_borrowed_ptr_or_err(value.py(), index(value.as_ptr())) }
}

pub fn bad_new_ref<'py>(value: &Bound<'py, PyAny>) -> Bound<'py, PyAny> {
    unsafe { Bound::from_borrowed_ptr(value.py(), ffi::Py_NewRef(value.as_ptr())) }
}

pub fn bad_list(py: Python<'_>) -> Option<Bound<'_, PyAny>> {
    unsafe { Bound::from_borrowed_ptr_or_opt(py, ffi::PyList_New(0)) }
}

pub fn bad_tuple(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    unsafe { Bound::from_borrowed_ptr_or_err(py, ffi::PyTuple_New(0)) }
}

pub fn bad_bytes(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    unsafe {
        Bound::from_borrowed_ptr_or_err(py, ffi::PyBytes_FromStringAndSize(b"x".as_ptr().cast(), 1))
    }
}

pub fn bad_capsule(py: Python<'_>) -> PyResult<Bound<'_, PyAny>> {
    static TOKEN: u8 = 0;
    unsafe {
        Bound::from_borrowed_ptr_or_err(
            py,
            ffi::PyCapsule_New((&raw const TOKEN).cast_mut().cast(), c"test".as_ptr(), None),
        )
    }
}

pub fn good_index<'py>(value: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyAny>> {
    unsafe { Bound::from_owned_ptr_or_err(value.py(), ffi::PyNumber_Index(value.as_ptr())) }
}

pub fn good_new_ref<'py>(value: &Bound<'py, PyAny>) -> Bound<'py, PyAny> {
    unsafe { Bound::from_owned_ptr(value.py(), ffi::Py_NewRef(value.as_ptr())) }
}

pub fn good_borrowed_item<'py>(value: &Bound<'py, PyTuple>) -> PyResult<Bound<'py, PyAny>> {
    unsafe { Bound::from_borrowed_ptr_or_err(value.py(), ffi::PyTuple_GetItem(value.as_ptr(), 0)) }
}

pub fn good_unrelated<'py>(value: &Bound<'py, PyAny>) -> Bound<'py, PyAny> {
    #[allow(non_snake_case)]
    fn PyNumber_Index(value: &Bound<'_, PyAny>) -> *mut ffi::PyObject {
        value.as_ptr()
    }
    unsafe { Bound::from_borrowed_ptr(value.py(), PyNumber_Index(value)) }
}
