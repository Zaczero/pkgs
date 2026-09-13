#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! NumPy-native return helpers.
//!
//! Public bulk numeric lanes return fixed-width, read-only `numpy.ndarray`
//! objects. Keep the conversions here so call sites state their domain result
//! shape rather than each spelling out rust-numpy mechanics.

use numpy::ndarray::{Array2, ArrayView1};
use numpy::{
    IntoPyArray as _, PyArray1, PyArrayDescrMethods as _, PyArrayMethods as _, ToPyArray as _,
};
use pyo3::PyClass;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::{PyAny, PyModule, PyType};

pub(crate) fn numpy_module(py: Python<'_>) -> PyResult<&Bound<'_, PyModule>> {
    static NUMPY: PyOnceLock<Py<PyModule>> = PyOnceLock::new();
    NUMPY
        .get_or_try_init(py, || py.import("numpy").map(Bound::unbind))
        .map(|module| module.bind(py))
}

/// Reject NumPy masked arrays before any buffer or iterator can expose their
/// hidden data values. The type comparison deliberately does not inspect a
/// user-provided `mask` attribute, and ordinary ndarray subclasses remain
/// valid inputs.
pub(crate) fn reject_masked_array(value: &Bound<'_, PyAny>) -> PyResult<()> {
    static MASKED_ARRAY: PyOnceLock<Py<PyType>> = PyOnceLock::new();

    if value.cast::<numpy::PyUntypedArray>().is_err() {
        return Ok(());
    }
    let masked_array = MASKED_ARRAY.import(value.py(), "numpy.ma", "MaskedArray")?;
    if value.get_type().is_subclass(masked_array.as_any())? {
        return Err(pyo3::exceptions::PyTypeError::new_err(
            "masked NumPy arrays are not supported; fill or disable the mask before passing data to gometry",
        ));
    }
    Ok(())
}

pub(crate) fn freeze<T, D>(array: &Bound<'_, numpy::PyArray<T, D>>) -> PyResult<()>
where
    T: numpy::Element,
    D: numpy::ndarray::Dimension,
{
    array.try_readwrite()?.make_nonwriteable();
    Ok(())
}

pub(crate) fn float64_array(py: Python<'_>, values: Vec<f64>) -> PyResult<Py<PyAny>> {
    try_array_from_vec(py, values)
}

pub(crate) fn bool_array(py: Python<'_>, values: Vec<bool>) -> PyResult<Py<PyAny>> {
    try_array_from_vec(py, values)
}

trait TryNumpyElement: numpy::Element + Copy {
    const NPY_TYPE: numpy::npyffi::NPY_TYPES;
}

impl TryNumpyElement for bool {
    const NPY_TYPE: numpy::npyffi::NPY_TYPES = numpy::npyffi::NPY_TYPES::NPY_BOOL;
}

impl TryNumpyElement for f64 {
    const NPY_TYPE: numpy::npyffi::NPY_TYPES = numpy::npyffi::NPY_TYPES::NPY_DOUBLE;
}

/// Build an owned NumPy array without rust-numpy's `IntoPyArray` adapter.
///
/// That adapter stores the Rust allocation behind a `PySliceContainer` and
/// constructs the container with `Bound::new(...).expect(...)`. Use NumPy's
/// own allocation here instead: its C API reports an allocation failure as a
/// null result, which can be converted to the pending Python exception before
/// the Rust values are copied into the array.
fn try_array_from_vec<T>(py: Python<'_>, values: Vec<T>) -> PyResult<Py<PyAny>>
where
    T: TryNumpyElement,
{
    let Ok(dimension) = numpy::npyffi::npy_intp::try_from(values.len()) else {
        drop(values);
        return Err(pyo3::exceptions::PyOverflowError::new_err(
            "NumPy array is too large",
        ));
    };
    let mut dimensions = [dimension];
    // SAFETY: the NumPy API is initialized by this call, and the built-in type
    // descriptor is returned as an owned pointer or null with a Python error.
    let dtype_ptr = unsafe { numpy::PY_ARRAY_API.PyArray_DescrFromType(py, T::NPY_TYPE as _) };
    // SAFETY: `dtype_ptr` is an owned descriptor pointer returned by NumPy or
    // null with the Python exception describing the failed allocation.
    let Some(dtype) = (unsafe { Bound::from_owned_ptr_or_opt(py, dtype_ptr.cast()) }) else {
        drop(values);
        return Err(pyo3::PyErr::fetch(py));
    };
    let dtype = match dtype.cast_into::<numpy::PyArrayDescr>() {
        Ok(dtype) => dtype,
        Err(error) => {
            drop(values);
            return Err(error.into());
        },
    };
    // SAFETY: `dtype` is an owned descriptor whose ownership is transferred to
    // `PyArray_NewFromDescr`;
    // `dimensions` remains live for the duration of this call, and the null
    // data pointer asks NumPy to own its newly allocated storage.
    let ptr = unsafe {
        numpy::PY_ARRAY_API.PyArray_NewFromDescr(
            py,
            numpy::npyffi::get_type_object(py, numpy::npyffi::NpyTypes::PyArray_Type),
            dtype.into_dtype_ptr(),
            1,
            dimensions.as_mut_ptr(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            0,
            std::ptr::null_mut(),
        )
    };
    // SAFETY: NumPy returns either a new owned Python reference or null with
    // the Python exception describing the failed allocation.
    // SAFETY: `ptr` is an owned Python reference returned by NumPy or null
    // with the Python exception describing the failed allocation.
    let Some(array) = (unsafe { Bound::from_owned_ptr_or_opt(py, ptr) }) else {
        drop(values);
        return Err(pyo3::PyErr::fetch(py));
    };
    let array = match array.cast_into::<PyArray1<T>>() {
        Ok(array) => array,
        Err(error) => {
            drop(values);
            return Err(error.into());
        },
    };
    let mut write = match array.try_readwrite() {
        Ok(write) => write,
        Err(error) => {
            drop(values);
            return Err(error.into());
        },
    };
    match write.as_slice_mut() {
        Ok(slice) => slice.copy_from_slice(&values),
        Err(error) => {
            drop(values);
            return Err(error.into());
        },
    }
    drop(values);
    drop(write);
    freeze(&array)?;
    Ok(array.into_any().unbind())
}

pub(crate) fn bool_slice_array(py: Python<'_>, values: &[bool]) -> PyResult<Py<PyAny>> {
    let array = ArrayView1::from(values).to_pyarray(py);
    freeze(&array)?;
    Ok(array.into_any().unbind())
}

pub(crate) fn false_bool_array(py: Python<'_>, len: usize) -> PyResult<Py<PyAny>> {
    let array = PyArray1::<bool>::zeros(py, len, false);
    freeze(&array)?;
    Ok(array.into_any().unbind())
}

pub(crate) fn int64_array(py: Python<'_>, values: Vec<i64>) -> PyResult<Py<PyAny>> {
    let array = values.into_pyarray(py);
    freeze(&array)?;
    Ok(array.into_any().unbind())
}

pub(crate) fn uint64_array(py: Python<'_>, values: Vec<u64>) -> PyResult<Py<PyAny>> {
    let array = values.into_pyarray(py);
    freeze(&array)?;
    Ok(array.into_any().unbind())
}

pub(crate) fn astype_readonly(
    array: &Bound<'_, PyAny>,
    dtype: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let cast = array.call_method1("astype", (dtype,))?;
    cast.call_method1("setflags", (false,))?;
    Ok(cast.unbind())
}

/// Read-only `NumPy` view over a `u64` slice owned by `owner`.
///
/// # Safety
///
/// The returned array stores `owner` as its `NumPy` base object. Callers must
/// pass a slice whose allocation stays valid and unreallocated for as long as
/// the owner lives.
pub(crate) unsafe fn uint64_slice_array(
    owner: Bound<'_, PyAny>,
    values: &[u64],
) -> PyResult<Py<PyAny>> {
    let view = ArrayView1::from(values);
    // SAFETY: forwarded from this helper's caller.
    let array = unsafe { PyArray1::borrow_from_array(&view, owner) };
    freeze(&array)?;
    Ok(array.into_any().unbind())
}

pub(crate) fn optional_float64_array(
    py: Python<'_>,
    values: impl IntoIterator<Item = Option<f64>>,
) -> PyResult<Py<PyAny>> {
    float64_array(
        py,
        crate::try_collect_results(
            values
                .into_iter()
                .map(|value| Ok::<_, pyo3::PyErr>(value.unwrap_or(f64::NAN))),
        )?,
    )
}

pub(crate) fn float64_matrix(
    py: Python<'_>,
    values: Vec<f64>,
    rows: usize,
    columns: usize,
) -> PyResult<Py<PyAny>> {
    let array = Array2::from_shape_vec((rows, columns), values)
        .map_err(|_| PyValueError::new_err("matrix values do not match shape"))?
        .into_pyarray(py);
    freeze(&array)?;
    Ok(array.into_any().unbind())
}

pub(crate) fn bounds_array(py: Python<'_>, values: Vec<f64>) -> PyResult<Py<PyAny>> {
    debug_assert_eq!(values.len() % 4, 0);
    let rows = values.len() / 4;
    float64_matrix(py, values, rows, 4)
}

pub(crate) fn bounds3d_array(
    py: Python<'_>,
    values: impl IntoIterator<Item = Option<crate::geometry::Bounds3D>>,
) -> PyResult<Py<PyAny>> {
    let values = values.into_iter();
    let mut out = crate::try_vec_with_capacity_hint(values.size_hint().0.saturating_mul(6))?;
    for row in values {
        match row {
            Some(bounds) => {
                crate::try_push(&mut out, bounds.minx)?;
                crate::try_push(&mut out, bounds.miny)?;
                crate::try_push(&mut out, bounds.minz)?;
                crate::try_push(&mut out, bounds.maxx)?;
                crate::try_push(&mut out, bounds.maxy)?;
                crate::try_push(&mut out, bounds.maxz)?;
            },
            None => {
                for _ in 0..6 {
                    crate::try_push(&mut out, f64::NAN)?;
                }
            },
        }
    }
    let rows = out.len() / 6;
    float64_matrix(py, out, rows, 6)
}

// Production seal lives in a std-only module so compile-fail fixtures can
// `#[path]`-include it (see `py/arrow_c/compile_fail/item_c_mutable_pyclass.rs`).
#[path = "frozen_i64_bound.rs"]
mod frozen_i64_bound;

pub(crate) use frozen_i64_bound::{ImmutableI64Owner, frozen_i64_owner};
use frozen_i64_bound::{owner_derived_i64_slice, require_immutable_i64_owner};

/// Safe read-only `NumPy` view over immutable Arc i64 storage.
///
/// The closure ties `slf` and the returned slice lifetimes so the backing
/// cannot outlive its owner. Two independent bounds enforce the safety claim:
///
/// 1. **`PyClass<Frozen = True>`** — the language-level PyO3 frozen marker; a
///    mutable `#[pyclass]` cannot satisfy this bound at all.
/// 2. **`ImmutableI64Owner`** — sealed opt-in for immutable i64 backing
///    (only `Groups` implements it today).
///
/// The [`require_immutable_i64_owner`] call couples this function to the seal
/// module so the where-clause cannot silently drop that half of the bound.
pub(crate) fn frozen_i64_view<T>(
    slf: Bound<'_, T>,
    pick: impl for<'a> FnOnce(&'a T) -> &'a [i64],
) -> PyResult<Py<PyAny>>
where
    T: ImmutableI64Owner + PyClass<Frozen = pyo3::pyclass::boolean_struct::True>,
{
    let owner = slf.clone().into_any();
    let borrowed = slf.borrow();
    // Production seal gate — same trait the compile-fail fixture path-includes.
    require_immutable_i64_owner(&*borrowed);
    let values = owner_derived_i64_slice(&*borrowed, pick);
    // SAFETY: `ImmutableI64Owner` + frozen `PyClass` prove immutable
    // backing; `pick` ties the slice lifetime to `slf`; `owner` is that same
    // object pinned as the NumPy base.
    unsafe { int64_slice_array(owner, values) }
}

/// Read-only `NumPy` view over an `i64` slice owned by `owner`.
///
/// # Safety
///
/// The returned array stores `owner` as its `NumPy` base object. Callers must
/// pass a slice whose allocation stays valid and unreallocated for as long as
/// the owner lives.
pub(crate) unsafe fn int64_slice_array(
    owner: Bound<'_, PyAny>,
    values: &[i64],
) -> PyResult<Py<PyAny>> {
    let view = ArrayView1::from(values);
    // SAFETY: forwarded from this helper's caller.
    let array = unsafe { PyArray1::borrow_from_array(&view, owner) };
    freeze(&array)?;
    Ok(array.into_any().unbind())
}

/// Read-only `NumPy` view over an `f64` slice owned by `owner`.
///
/// # Safety
///
/// The returned array stores `owner` as its `NumPy` base object. Callers must
/// pass a slice whose allocation stays valid and unreallocated for as long as
/// the owner lives.
pub(crate) unsafe fn float64_slice_array(
    owner: Bound<'_, PyAny>,
    values: &[f64],
) -> PyResult<Py<PyAny>> {
    let view = ArrayView1::from(values);
    // SAFETY: forwarded from this helper's caller.
    let array = unsafe { PyArray1::borrow_from_array(&view, owner) };
    freeze(&array)?;
    Ok(array.into_any().unbind())
}

pub(crate) fn usize_array(
    py: Python<'_>,
    values: impl IntoIterator<Item = usize>,
) -> PyResult<Py<PyAny>> {
    // Row ids come from CPython-visible sequence lengths (`isize`-bounded by
    // the interpreter); converting to NumPy's fixed `int64` index lane is
    // therefore lossless on supported targets.
    int64_array(
        py,
        crate::try_collect_results(
            values
                .into_iter()
                .map(|value| Ok::<_, pyo3::PyErr>(value as i64)),
        )?,
    )
}
