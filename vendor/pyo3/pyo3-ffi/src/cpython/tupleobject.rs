use crate::object::*;
#[cfg(Py_3_14)]
use crate::pyport::Py_hash_t;
#[cfg(not(PyPy))]
use crate::pyport::Py_ssize_t;

#[repr(C)]
pub struct PyTupleObject {
    pub ob_base: PyVarObject,
    #[cfg(Py_3_14)]
    pub ob_hash: Py_hash_t,
    pub ob_item: [*mut PyObject; 1],
}

// skipped _PyTuple_Resize
// skipped _PyTuple_MaybeUntrack

// skipped _PyTuple_CAST

/// Macro, trading safety for speed
#[inline]
#[cfg(not(PyPy))]
pub unsafe fn PyTuple_GET_SIZE(op: *mut PyObject) -> Py_ssize_t {
    Py_SIZE(op)
}

#[inline]
#[cfg(not(any(PyPy, GraalPy)))]
pub unsafe fn PyTuple_GET_ITEM(op: *mut PyObject, i: Py_ssize_t) -> *mut PyObject {
    // The declared array is only the first slot of a flexible allocation.
    // A reference to it would narrow access to that one slot.
    *(&raw const (*op.cast::<PyTupleObject>()).ob_item)
        .cast::<*mut PyObject>()
        .offset(i)
}

/// Macro, *only* to be used to fill in brand new tuples
#[inline]
#[cfg(not(any(PyPy, GraalPy)))]
pub unsafe fn PyTuple_SET_ITEM(op: *mut PyObject, i: Py_ssize_t, v: *mut PyObject) {
    *(&raw mut (*op.cast::<PyTupleObject>()).ob_item)
        .cast::<*mut PyObject>()
        .offset(i) = v;
}

// skipped _PyTuple_DebugMallocStats

#[cfg(all(test, not(any(PyPy, GraalPy))))]
mod tests {
    use core::mem::MaybeUninit;

    use super::{PyObject, PyTuple_GET_ITEM, PyTuple_SET_ITEM, PyTupleObject};

    #[test]
    fn tuple_access_preserves_provenance_of_flexible_slots() {
        let mut storage = [const { MaybeUninit::<PyTupleObject>::uninit() }; 2];
        let object = storage.as_mut_ptr().cast::<PyObject>();
        let mut first = MaybeUninit::<PyObject>::uninit();
        let mut second = MaybeUninit::<PyObject>::uninit();
        // SAFETY: the allocation is aligned and large enough for the header and
        // two slots. These macros do not inspect the header or slot pointees.
        unsafe {
            PyTuple_SET_ITEM(object, 0, first.as_mut_ptr());
            PyTuple_SET_ITEM(object, 1, second.as_mut_ptr());
            assert_eq!(PyTuple_GET_ITEM(object, 0), first.as_mut_ptr());
            assert_eq!(PyTuple_GET_ITEM(object, 1), second.as_mut_ptr());
        }
    }
}
