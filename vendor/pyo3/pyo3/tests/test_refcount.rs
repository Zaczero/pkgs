//! Full-API reference-count field and immortal-transition parity with CPython.

#![cfg(all(
    Py_3_14,
    target_pointer_width = "64",
    not(any(Py_LIMITED_API, Py_GIL_DISABLED, PyPy, GraalPy)),
    not(py_sys_config = "Py_REF_DEBUG")
))]

use pyo3::ffi;
use pyo3::prelude::*;
use pyo3::types::PyList;

#[test]
fn immortal_refcount_excludes_adjacent_object_flags() {
    Python::initialize();
    Python::attach(|py| {
        let none = py.None();
        let native_count = py
            .import("sys")
            .unwrap()
            .call_method1("getrefcount", (&none,))
            .unwrap()
            .extract::<isize>()
            .unwrap();
        // SAFETY: None is a live immortal object and this thread is attached.
        let count = unsafe { ffi::Py_REFCNT(none.as_ptr()) };
        assert_eq!(count, native_count);
    });
}

#[test]
fn incref_matches_native_immortal_transition_thresholds() {
    Python::initialize();
    Python::attach(|py| {
        let object = PyList::empty(py);
        let pointer = object.as_ptr();
        // CPython refcount.h uses different INCREF/DECREF thresholds: 2^31
        // marks immortality, but increfs continue until 3*2^30.
        for count in [1, 1 << 31, (3 << 30) - 1, 3 << 30, u32::MAX] {
            // SAFETY: retain this valid heap object under the GIL throughout.
            // Only its count field changes; no allocation/callback or owner
            // destruction occurs before restoration, which precedes assertions.
            let (rust_count, native_count) = unsafe {
                let original = (*pointer).ob_refcnt.refcnt_and_flags.ob_refcnt;
                (*pointer).ob_refcnt.refcnt_and_flags.ob_refcnt = count;
                ffi::Py_INCREF(pointer);
                let rust_count = (*pointer).ob_refcnt.refcnt_and_flags.ob_refcnt;
                (*pointer).ob_refcnt.refcnt_and_flags.ob_refcnt = count;
                ffi::Py_IncRef(pointer);
                let native_count = (*pointer).ob_refcnt.refcnt_and_flags.ob_refcnt;
                (*pointer).ob_refcnt.refcnt_and_flags.ob_refcnt = original;
                (rust_count, native_count)
            };
            assert_eq!(rust_count, native_count, "initial count {count}");
        }
    });
}
