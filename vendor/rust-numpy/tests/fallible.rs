//! This binary has one test because it temporarily replaces process-global C API slots.

use std::{
    ffi::{c_int, c_void},
    ptr,
    sync::{
        Arc,
        atomic::{AtomicPtr, AtomicUsize, Ordering},
    },
};

use numpy::{
    IntoPyArray, PyArray1, PyArrayDescr, PyArrayMethods,
    npyffi::{PyArray_Descr, PyArrayObject, npy_intp},
};
#[cfg(not(Py_LIMITED_API))]
use pyo3::types::PyTypeMethods;
use pyo3::{
    Bound, Py, PyAny, PyErr, Python, ffi, pyclass,
    types::{PyAnyMethods, PyCapsule, PyCapsuleMethods},
};

static ERROR: AtomicPtr<ffi::PyObject> = AtomicPtr::new(ptr::null_mut());
static SET_BASE: AtomicPtr<c_void> = AtomicPtr::new(ptr::null_mut());
static FAILED_ARRAY: AtomicPtr<ffi::PyObject> = AtomicPtr::new(ptr::null_mut());
static BASE_CALLS: AtomicUsize = AtomicUsize::new(0);

#[cfg(not(Py_LIMITED_API))]
unsafe extern "C" fn fail_alloc(
    _ty: *mut ffi::PyTypeObject,
    _items: ffi::Py_ssize_t,
) -> *mut ffi::PyObject {
    unsafe { ffi::PyErr_SetObject(ffi::PyExc_MemoryError, ERROR.load(Ordering::Relaxed)) };
    ptr::null_mut()
}

unsafe extern "C" fn fail_new(
    _ty: *mut ffi::PyTypeObject,
    dtype: *mut PyArray_Descr,
    _ndim: c_int,
    _dims: *mut npy_intp,
    _strides: *mut npy_intp,
    _data: *mut c_void,
    _flags: c_int,
    _obj: *mut ffi::PyObject,
) -> *mut ffi::PyObject {
    unsafe {
        ffi::Py_DECREF(dtype.cast());
        ffi::PyErr_SetObject(ffi::PyExc_MemoryError, ERROR.load(Ordering::Relaxed));
    }
    ptr::null_mut()
}

unsafe extern "C" fn fail_zeros(
    _ndim: c_int,
    _dims: *mut npy_intp,
    dtype: *mut PyArray_Descr,
    _fortran: c_int,
) -> *mut ffi::PyObject {
    unsafe {
        ffi::Py_DECREF(dtype.cast());
        ffi::PyErr_SetObject(ffi::PyExc_MemoryError, ERROR.load(Ordering::Relaxed));
    }
    ptr::null_mut()
}

type SetBase = unsafe extern "C" fn(*mut PyArrayObject, *mut ffi::PyObject) -> c_int;

unsafe extern "C" fn count_set_base(array: *mut PyArrayObject, owner: *mut ffi::PyObject) -> c_int {
    BASE_CALLS.fetch_add(1, Ordering::Relaxed);
    let original: SetBase = unsafe { std::mem::transmute(SET_BASE.load(Ordering::Relaxed)) };
    unsafe { original(array, owner) }
}

unsafe extern "C" fn fail_set_base(array: *mut PyArrayObject, owner: *mut ffi::PyObject) -> c_int {
    BASE_CALLS.fetch_add(1, Ordering::Relaxed);
    let original: SetBase = unsafe { std::mem::transmute(SET_BASE.load(Ordering::Relaxed)) };
    unsafe {
        // Exercise NumPy's real "base already set" failure and reference stealing.
        ffi::Py_INCREF(ffi::Py_None());
        original(array, ffi::Py_None());
        FAILED_ARRAY.store(
            ffi::PyWeakref_NewRef(array.cast(), ptr::null_mut()),
            Ordering::Relaxed,
        );
        let result = original(array, owner);
        let py = Python::assume_attached();
        let error = PyErr::fetch(py);
        ERROR.store(error.value(py).as_ptr(), Ordering::Relaxed);
        error.restore(py);
        result
    }
}

struct SlotGuard {
    slot: *mut *const c_void,
    original: *const c_void,
}

impl SlotGuard {
    unsafe fn replace(slot: *mut *const c_void, replacement: *const c_void) -> Self {
        let original = unsafe { slot.replace(replacement) };
        Self { slot, original }
    }
}

impl Drop for SlotGuard {
    fn drop(&mut self) {
        unsafe { self.slot.write(self.original) };
    }
}

#[cfg(not(Py_LIMITED_API))]
struct AllocGuard {
    ty: *mut ffi::PyTypeObject,
    original: Option<ffi::allocfunc>,
}

#[cfg(not(Py_LIMITED_API))]
impl AllocGuard {
    unsafe fn replace(ty: *mut ffi::PyTypeObject) -> Self {
        let original = unsafe { (*ty).tp_alloc.replace(fail_alloc) };
        Self { ty, original }
    }
}

#[cfg(not(Py_LIMITED_API))]
impl Drop for AllocGuard {
    fn drop(&mut self) {
        unsafe { (*self.ty).tp_alloc = self.original };
    }
}

#[pyclass]
struct DropWitness(Arc<AtomicUsize>);

impl Drop for DropWitness {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::Relaxed);
    }
}

fn values(py: Python<'_>, drops: &Arc<AtomicUsize>) -> Vec<Py<PyAny>> {
    vec![Py::new(py, DropWitness(drops.clone())).unwrap().into_any()]
}

fn assert_error(py: Python<'_>, error: PyErr) {
    assert_eq!(error.value(py).as_ptr(), ERROR.load(Ordering::Relaxed));
    assert!(!PyErr::occurred(py));
}

fn refcount(object: &Bound<'_, PyAny>) -> ffi::Py_ssize_t {
    unsafe { ffi::Py_REFCNT(object.as_ptr()) }
}

#[test]
fn construction_failures_preserve_errors_and_release_owners() {
    Python::attach(|py| {
        let warm = PyArray1::from_vec(py, vec![1_u64]);
        #[cfg(not(Py_LIMITED_API))]
        let container_type = warm.getattr("base").unwrap().get_type();
        let module = if numpy::npyffi::is_numpy_2(py) {
            "numpy._core._multiarray_umath"
        } else {
            "numpy.core._multiarray_umath"
        };
        let capsule = py
            .import(module)
            .unwrap()
            .getattr("_ARRAY_API")
            .unwrap()
            .cast_into::<PyCapsule>()
            .unwrap();
        let api = capsule
            .pointer_checked(None)
            .unwrap()
            .cast::<*const c_void>()
            .as_ptr();
        // NumPy API slot numbers are part of its published C ABI.
        let new_slot = unsafe { api.add(94) };
        let zeros_slot = unsafe { api.add(183) };
        let base_slot = unsafe { api.add(282) };
        SET_BASE.store(unsafe { base_slot.read() }.cast_mut(), Ordering::Relaxed);
        let sentinel = pyo3::exceptions::PyMemoryError::new_err("array construction sentinel");
        ERROR.store(sentinel.value(py).as_ptr(), Ordering::Relaxed);

        // Replacing tp_alloc requires the full C API; the remaining failures
        // exercise NumPy's capsule API on both full- and limited-API builds.
        #[cfg(not(Py_LIMITED_API))]
        {
            let drops = Arc::new(AtomicUsize::new(0));
            let input = values(py, &drops);
            let _alloc = unsafe { AllocGuard::replace(container_type.as_type_ptr()) };
            assert_error(py, input.try_into_pyarray(py).unwrap_err());
            assert_eq!(
                drops.load(Ordering::Relaxed),
                1,
                "container allocation failure"
            );
        }

        let drops = Arc::new(AtomicUsize::new(0));
        let input = values(py, &drops);
        let dtype = PyArrayDescr::object(py);
        let dtype_refs = refcount(dtype.as_any());
        BASE_CALLS.store(0, Ordering::Relaxed);
        {
            let _new = unsafe { SlotGuard::replace(new_slot, fail_new as *const c_void) };
            let _base = unsafe { SlotGuard::replace(base_slot, count_set_base as *const c_void) };
            assert_error(py, input.try_into_pyarray(py).unwrap_err());
        }
        assert_eq!(
            BASE_CALLS.load(Ordering::Relaxed),
            0,
            "null array must not reach SetBaseObject"
        );
        assert_eq!(drops.load(Ordering::Relaxed), 1, "array allocation failure");
        assert_eq!(
            refcount(dtype.as_any()),
            dtype_refs,
            "failed constructor consumes dtype"
        );

        let drops = Arc::new(AtomicUsize::new(0));
        let input = values(py, &drops);
        {
            let _base = unsafe { SlotGuard::replace(base_slot, fail_set_base as *const c_void) };
            assert_error(py, input.try_into_pyarray(py).unwrap_err());
        }
        assert_eq!(drops.load(Ordering::Relaxed), 1, "base assignment failure");
        let weakref = unsafe {
            Bound::<PyAny>::from_owned_ptr(
                py,
                FAILED_ARRAY.swap(ptr::null_mut(), Ordering::Relaxed),
            )
        };
        assert!(
            weakref.call0().unwrap().is_none(),
            "failed array must be destroyed"
        );

        ERROR.store(sentinel.value(py).as_ptr(), Ordering::Relaxed);
        let dtype = numpy::dtype::<bool>(py);
        let dtype_refs = refcount(dtype.as_any());
        {
            let _zeros = unsafe { SlotGuard::replace(zeros_slot, fail_zeros as *const c_void) };
            assert_error(py, PyArray1::<bool>::try_zeros(py, 3, false).unwrap_err());
        }
        assert_eq!(
            refcount(dtype.as_any()),
            dtype_refs,
            "failed zeros consumes dtype"
        );

        let owner = warm.clone().into_any();
        let readonly = warm.readonly();
        let view = readonly.as_array();
        let owner_refs = refcount(&owner);
        {
            let _new = unsafe { SlotGuard::replace(new_slot, fail_new as *const c_void) };
            let result = unsafe { PyArray1::try_borrow_from_array(&view, owner.clone()) };
            assert_error(py, result.unwrap_err());
        }
        assert_eq!(
            refcount(&owner),
            owner_refs,
            "failed borrowed view releases base"
        );
        {
            let _base = unsafe { SlotGuard::replace(base_slot, fail_set_base as *const c_void) };
            let result = unsafe { PyArray1::try_borrow_from_array(&view, owner.clone()) };
            assert_error(py, result.unwrap_err());
        }
        assert_eq!(
            refcount(&owner),
            owner_refs,
            "failed base assignment steals borrowed owner"
        );
        let weakref = unsafe {
            Bound::<PyAny>::from_owned_ptr(
                py,
                FAILED_ARRAY.swap(ptr::null_mut(), Ordering::Relaxed),
            )
        };
        assert!(weakref.call0().unwrap().is_none());
        ERROR.store(ptr::null_mut(), Ordering::Relaxed);
    });
}
