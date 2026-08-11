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
use numpy::{IntoPyArray as _, PyArray1, PyArrayMethods as _, ToPyArray as _};
use pyo3::PyClass;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::sync::PyOnceLock;
use pyo3::types::PyModule;

pub(crate) fn numpy_module(py: Python<'_>) -> PyResult<&Bound<'_, PyModule>> {
    static NUMPY: PyOnceLock<Py<PyModule>> = PyOnceLock::new();
    NUMPY
        .get_or_try_init(py, || py.import("numpy").map(Bound::unbind))
        .map(|module| module.bind(py))
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
    let array = values.into_pyarray(py);
    freeze(&array)?;
    Ok(array.into_any().unbind())
}

pub(crate) fn bool_array(py: Python<'_>, values: Vec<bool>) -> PyResult<Py<PyAny>> {
    let array = values.into_pyarray(py);
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
        values
            .into_iter()
            .map(|value| value.unwrap_or(f64::NAN))
            .collect(),
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
    let mut out = Vec::with_capacity(values.size_hint().0.saturating_mul(6));
    for row in values {
        match row {
            Some(bounds) => {
                out.push(bounds.minx);
                out.push(bounds.miny);
                out.push(bounds.minz);
                out.push(bounds.maxx);
                out.push(bounds.maxy);
                out.push(bounds.maxz);
            },
            None => out.extend_from_slice(&[f64::NAN; 6]),
        }
    }
    let rows = out.len() / 6;
    float64_matrix(py, out, rows, 6)
}

// Production seal lives in a std-only module so compile-fail fixtures can
// `#[path]`-include it (see `compile_fail/item_c_mutable_pyclass.rs`).
#[path = "frozen_i64_bound.rs"]
mod frozen_i64_bound;

use frozen_i64_bound::require_immutable_i64_owner;
pub(crate) use frozen_i64_bound::{ImmutableI64Owner, frozen_i64_owner};

/// Safe read-only `NumPy` view over immutable Arc i64 storage.
///
/// The closure ties `slf` and the returned slice lifetimes so the backing
/// cannot outlive its owner. Two independent bounds enforce the safety claim:
///
/// 1. **`PyClass<Frozen = True>`** — the language-level PyO3 frozen marker; a
///    mutable `#[pyclass]` cannot satisfy this bound at all.
/// 2. **`ImmutableI64Owner`** — sealed opt-in for immutable Arc i64 backing
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
    let values = pick(&borrowed);
    // SAFETY: `ImmutableI64Owner` + frozen `PyClass` prove immutable Arc
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
    int64_array(py, values.into_iter().map(|value| value as i64).collect())
}
