#[cfg(not(Py_LIMITED_API))]
use core::ffi::c_char;
use core::ffi::c_int;
#[cfg(Py_3_15)]
use core::ffi::c_void;

use crate::Py_ssize_t;
use crate::object::*;

#[cfg(not(any(PyPy, GraalPy, Py_LIMITED_API)))]
#[repr(C)]
pub struct PyBytesObject {
    pub ob_base: PyVarObject,
    #[cfg_attr(
        Py_3_11,
        deprecated(note = "Deprecated in Python 3.11 and will be removed in a future version.")
    )]
    pub ob_shash: crate::Py_hash_t,
    pub ob_sval: [c_char; 1],
}

#[cfg(any(PyPy, GraalPy, Py_LIMITED_API))]
opaque_struct!(pub PyBytesObject);

extern_libpython! {
    #[cfg_attr(PyPy, link_name = "_PyPyBytes_Resize")]
    pub(crate) fn _PyBytes_Resize(bytes: *mut *mut PyObject, newsize: Py_ssize_t) -> c_int;
}

#[cfg(not(Py_LIMITED_API))]
#[inline]
pub unsafe fn PyBytes_AS_STRING(op: *mut PyObject) -> *const c_char {
    #[cfg(not(any(PyPy, GraalPy)))]
    // A newly allocated bytes object's payload may still be uninitialized and
    // writable. Do not introduce a shared reference into its pointer provenance.
    return (&raw const (*op.cast::<PyBytesObject>()).ob_sval).cast();
    #[cfg(any(PyPy, GraalPy))]
    return crate::PyBytes_AsString(op);
}

#[cfg(Py_3_15)]
opaque_struct!(pub PyBytesWriter);

#[cfg(Py_3_15)]
extern_libpython! {

    pub fn PyBytesWriter_Create(size: Py_ssize_t) -> *mut PyBytesWriter;

    pub fn PyBytesWriter_Discard(writer: *mut PyBytesWriter);

    pub fn PyBytesWriter_Finish(writer: *mut PyBytesWriter) -> *mut PyObject;

    pub fn PyBytesWriter_FinishWithSize(
        writer: *mut PyBytesWriter,
        size: Py_ssize_t,
    ) -> *mut PyObject;

    pub fn PyBytesWriter_GetData(writer: *mut PyBytesWriter) -> *mut c_void;

    pub fn PyBytesWriter_GetSize(writer: *mut PyBytesWriter) -> Py_ssize_t;

    pub fn PyBytesWriter_Resize(writer: *mut PyBytesWriter, size: Py_ssize_t) -> c_int;

    pub fn PyBytesWriter_Grow(writer: *mut PyBytesWriter, size: Py_ssize_t) -> c_int;

    pub fn PyBytesWriter_WriteBytes(
        writer: *mut PyBytesWriter,
        bytes: *const c_void,
        size: Py_ssize_t,
    ) -> c_int;
}

#[cfg(all(test, not(any(PyPy, GraalPy, Py_LIMITED_API))))]
mod tests {
    use core::mem::MaybeUninit;

    use super::{PyBytes_AS_STRING, PyBytesObject};

    #[test]
    fn bytes_data_pointer_preserves_write_access_to_private_storage() {
        let mut bytes = MaybeUninit::<PyBytesObject>::uninit();
        // SAFETY: this test owns the allocation. The helper only computes the
        // payload address; no header or uninitialized payload value is read.
        unsafe {
            let data = PyBytes_AS_STRING(bytes.as_mut_ptr().cast()).cast_mut();
            data.write(65);
            assert_eq!(data.read(), 65);
        }
    }
}
