#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
#![allow(
    clippy::needless_pass_by_value,
    reason = "PyO3 special-method receivers must retain their binding-compatible ownership shape"
)]
#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]

use std::borrow::Cow;
use std::fmt::Write as _;
use std::mem::size_of;

use numpy::{Element, PyReadonlyArrayDyn, PyUntypedArray, PyUntypedArrayMethods as _};
use pyo3::exceptions::{PyIndexError, PyTypeError, PyValueError};
use pyo3::types::{PyBool, PyBytes, PyInt, PyList, PySequence, PyTuple};

use crate::array::{
    Arc, Bound, CoordSeq, GeometryArrayStorage, GeometryError, HeapSize, IntoPyObject as _,
    MissingMask, OverlayOp, PointColumnBuilder, Py, PyAny, PyAnyMethods as _, PyBytesMethods as _,
    PyCoordinates, PyErr, PyGeometryArray, PyGeometryArrayIter, PyListMethods as _, PyResult,
    PyTupleMethods as _, PyTypeMethods as _, Python, Shape, Typed, exact_geometry,
    exact_geometry_array, overlay_operator, physical_row,
};
use crate::py::row::{RowContainer, RowGetItemContainer, array_getitem, collect_slice_rows};

pub(crate) enum FancyIndex {
    Empty,
    Mask,
    Indices,
    Invalid,
}

pub(crate) enum NumpyFancyIndex {
    Scalar(isize),
    Mask(Vec<bool>),
    Indices(Vec<isize>),
}

pub(crate) fn numpy_fancy_index(
    index: &Bound<'_, PyAny>,
    label: &str,
    type_label: &str,
) -> PyResult<Option<NumpyFancyIndex>> {
    let Ok(array) = index.cast::<PyUntypedArray>() else {
        return Ok(None);
    };
    let ndim = array.ndim();
    if ndim > 1 {
        return Err(PyTypeError::new_err(format!(
            "{label} NumPy indices must be zero- or one-dimensional"
        )));
    }
    if index.extract::<PyReadonlyArrayDyn<'_, bool>>().is_ok() {
        if ndim == 0 {
            return Err(PyTypeError::new_err(format!(
                "boolean scalar is not a {type_label} index; use an integer index or a boolean mask"
            )));
        }
        // `ndarray.tobytes()` materializes immutable Python bytes — owned by
        // construction, never an ArrayView over writable provider memory.
        let owned = owned_bool_mask_from_tobytes(index)?;
        return Ok(Some(NumpyFancyIndex::Mask(owned)));
    }
    macro_rules! try_integer_array {
        ($($ty:ty),* $(,)?) => {
            $(
                if index.extract::<PyReadonlyArrayDyn<'_, $ty>>().is_ok() {
                    return numpy_integer_fancy_index::<$ty>(index, ndim, label).map(Some);
                }
            )*
        };
    }
    try_integer_array!(i8, i16, i32, i64, isize, u8, u16, u32, u64, usize);
    Err(PyTypeError::new_err(format!(
        "{label} NumPy indices must have an integer or boolean dtype"
    )))
}

/// Copy a NumPy bool mask via `tobytes()` into owned `Vec<bool>`.
fn owned_bool_mask_from_tobytes(index: &Bound<'_, PyAny>) -> PyResult<Vec<bool>> {
    let py_bytes = index.call_method0("tobytes")?;
    let bytes = py_bytes.cast::<PyBytes>()?.as_bytes();
    Ok(bytes.iter().map(|&b| b != 0).collect())
}

pub(crate) fn numpy_integer_fancy_index<T>(
    index: &Bound<'_, PyAny>,
    ndim: usize,
    label: &str,
) -> PyResult<NumpyFancyIndex>
where
    T: Copy + Element + TryInto<isize>,
{
    // Owned capture via `tobytes()` — immutable bytes by construction.
    let values = owned_integer_indices_from_tobytes::<T>(index, label)?;
    if ndim == 0 {
        return Ok(NumpyFancyIndex::Scalar(
            values
                .into_iter()
                .next()
                .expect("zero-dimensional NumPy arrays have one element"),
        ));
    }
    Ok(NumpyFancyIndex::Indices(values))
}

fn owned_integer_indices_from_tobytes<T>(
    index: &Bound<'_, PyAny>,
    label: &str,
) -> PyResult<Vec<isize>>
where
    T: Copy + Element + TryInto<isize>,
{
    let py_bytes = index.call_method0("tobytes")?;
    let bytes = py_bytes.cast::<PyBytes>()?.as_bytes();
    let width = size_of::<T>();
    if width == 0 || !bytes.len().is_multiple_of(width) {
        return Err(PyTypeError::new_err(format!(
            "{label} NumPy integer index buffer length is invalid"
        )));
    }
    let mut out = Vec::with_capacity(bytes.len() / width);
    for chunk in bytes.chunks_exact(width) {
        // SAFETY: `T: Element` for integer dtypes used here; bytes come from
        // NumPy's native-endian `tobytes()` for that dtype.
        let value = unsafe { std::ptr::read_unaligned(chunk.as_ptr().cast::<T>()) };
        out.push(
            value
                .try_into()
                .map_err(|_| PyIndexError::new_err(format!("{label} index is too large")))?,
        );
    }
    Ok(out)
}

pub(crate) fn is_bool_scalar(index: &Bound<'_, PyAny>) -> bool {
    index.cast_exact::<PyBool>().is_ok() || index.extract::<bool>().is_ok()
}

pub(crate) enum CollectedFancyIndex {
    Empty,
    Indices(Vec<isize>),
    Mask(Vec<bool>),
    Invalid,
}

/// Classify and materialize a fancy-index sequence in **one pass**.
///
/// Exact `list` / `tuple` of bool or int extract directly into the mask /
/// index vector (no intermediate `Vec<PyObject>`). Generic sequences and
/// other executable providers still collect items once with fallible
/// reservation so a lying `__len__` cannot allocator-abort.
pub(crate) fn classify_and_collect_fancy_index(
    py: Python<'_>,
    sequence: &Bound<'_, PySequence>,
) -> PyResult<CollectedFancyIndex> {
    // Trusted-shape fast lane: exact builtins are ABI-honest length sources.
    let any = sequence.as_any();
    if let Ok(list) = any.cast::<PyList>() {
        return classify_and_collect_exact_list(list);
    }
    if let Ok(tuple) = any.cast::<PyTuple>() {
        return classify_and_collect_exact_tuple(tuple);
    }

    let items = crate::collect_sequence_items(sequence)?;
    if items.is_empty() {
        return Ok(CollectedFancyIndex::Empty);
    }
    let mut kind = FancyIndex::Empty;
    let mut indices = Vec::new();
    let mut mask = Vec::new();
    for item in &items {
        let bound = item.bind(py);
        if bound.cast_exact::<PyBool>().is_ok() {
            kind = match kind {
                FancyIndex::Empty | FancyIndex::Mask => FancyIndex::Mask,
                _ => FancyIndex::Invalid,
            };
            if matches!(kind, FancyIndex::Invalid) {
                return Ok(CollectedFancyIndex::Invalid);
            }
            mask.push(bound.extract::<bool>()?);
        } else if bound.cast::<PyInt>().is_ok() {
            kind = match kind {
                FancyIndex::Empty | FancyIndex::Indices => FancyIndex::Indices,
                _ => FancyIndex::Invalid,
            };
            if matches!(kind, FancyIndex::Invalid) {
                return Ok(CollectedFancyIndex::Invalid);
            }
            indices.push(bound.extract::<isize>()?);
        } else {
            return Ok(CollectedFancyIndex::Invalid);
        }
    }
    Ok(match kind {
        FancyIndex::Empty => CollectedFancyIndex::Empty,
        FancyIndex::Indices => CollectedFancyIndex::Indices(indices),
        FancyIndex::Mask => CollectedFancyIndex::Mask(mask),
        FancyIndex::Invalid => CollectedFancyIndex::Invalid,
    })
}

/// Exact `list` fancy-index ingest.
///
/// Free-threading soundness: never snapshot `len` then call
/// [`PyListMethods::get_item_unchecked`] — another thread can shrink the list
/// between those steps (UB). Nested `extract` / type checks can also suspend a
/// critical section, so one outer critical section around the whole classify
/// loop is not enough.
///
/// Shape: take an **owned immutable snapshot** via [`PyListMethods::to_tuple`]
/// (synchronized list→tuple copy under the list's free-threaded lock), then
/// reuse the exact-tuple lane. Classification and nested Python work run only
/// on the frozen tuple, which cannot shrink under concurrent mutation.
fn classify_and_collect_exact_list(list: &Bound<'_, PyList>) -> PyResult<CollectedFancyIndex> {
    let snapshot = list.to_tuple();
    classify_and_collect_exact_tuple(&snapshot)
}

/// Exact `tuple`: same single-pass shape as the list lane (no PyObject staging).
fn classify_and_collect_exact_tuple(tuple: &Bound<'_, PyTuple>) -> PyResult<CollectedFancyIndex> {
    let len = tuple.len();
    if len == 0 {
        return Ok(CollectedFancyIndex::Empty);
    }
    // SAFETY: index 0 is in range for a non-empty exact tuple.
    let first = unsafe { tuple.get_item_unchecked(0) };
    if first.cast_exact::<PyBool>().is_ok() {
        let py = tuple.py();
        let true_ptr = PyBool::new(py, true).as_ptr();
        let false_ptr = PyBool::new(py, false).as_ptr();
        let mut mask = Vec::with_capacity(len);
        for i in 0..len {
            // SAFETY: `i` is in `0..len` for this exact tuple.
            let item = unsafe { tuple.get_item_unchecked(i) };
            let ptr = item.as_ptr();
            if ptr == true_ptr {
                mask.push(true);
            } else if ptr == false_ptr {
                mask.push(false);
            } else {
                return Ok(CollectedFancyIndex::Invalid);
            }
        }
        return Ok(CollectedFancyIndex::Mask(mask));
    }
    if first.cast::<PyInt>().is_ok() {
        let mut indices = Vec::with_capacity(len);
        for i in 0..len {
            // SAFETY: `i` is in `0..len` for this exact tuple.
            let item = unsafe { tuple.get_item_unchecked(i) };
            if item.cast_exact::<PyBool>().is_ok() || item.cast::<PyInt>().is_err() {
                return Ok(CollectedFancyIndex::Invalid);
            }
            indices.push(item.extract::<isize>()?);
        }
        return Ok(CollectedFancyIndex::Indices(indices));
    }
    Ok(CollectedFancyIndex::Invalid)
}

impl HeapSize for PyGeometryArray {
    fn heap_bytes(&self) -> usize {
        self.storage().logical_heap_bytes()
            + self.missing().map_or(0, MissingMask::len)
            + self.frame_caches.heap_bytes()
            + self.prepared_cache_heap_bytes()
    }
}

impl RowContainer for PyGeometryArray {
    const LABEL: &'static str = "GeometryArray";
    const INDEX_LABEL: &'static str = "geometry array";

    fn row_count(&self) -> usize {
        self.storage().len()
    }

    fn scalar_row(&self, py: Python<'_>, row: usize) -> PyResult<Py<PyAny>> {
        if self.is_row_missing(row) {
            return Ok(py.None());
        }
        Ok(Typed(self.geometry_at(row)).into_pyobject(py)?.unbind())
    }
}

impl RowGetItemContainer for PyGeometryArray {
    fn gather_rows(&self, rows: &[usize]) -> Self {
        self.gather_logical_rows(rows)
    }

    fn slice_rows(&self, start: isize, stop: isize, step: isize) -> Self {
        if matches!(self.storage(), GeometryArrayStorage::Points { .. }) {
            let rows = collect_slice_rows(start, stop, step);
            let mut out = self.slice_packed_points(start, stop, step);
            out.frame_caches = self.selected_frame_caches(rows);
            return out;
        }
        self.gather_logical_rows(&collect_slice_rows(start, stop, step))
    }

    fn empty(&self) -> Self {
        self.gather_logical_rows(&[])
    }

    fn container_to_py(py: Python<'_>, value: Self) -> PyResult<Py<PyAny>> {
        Ok(value.into_pyobject(py)?.into_any().unbind())
    }

    fn mask_length_error(mask_len: usize, len: usize, numpy: bool) -> PyErr {
        if numpy {
            return PyIndexError::new_err(format!(
                "boolean mask length {mask_len} does not match GeometryArray length {len}",
            ));
        }
        GeometryError::new_err(format!(
            "mask length {mask_len} does not match array length {len}"
        ))
    }
}

frozen_pymethods! {
impl PyGeometryArray {
    /// Returns
    /// -------
    /// Coordinates
    ///     Flattened coordinates from present rows; missing rows contribute
    ///     no coordinates. This is a vertex stream, not a row-aligned view;
    ///     use `get_coordinates(..., return_index=True)` to recover source-row
    ///     alignment, or call `drop_missing()` first for an explicit dense path.
    #[getter]
    pub fn coords(&self) -> PyCoordinates {
        PyCoordinates::new(self.coordinate_view())
    }

    /// Select rows by integer, slice, or fancy index.
    ///
    /// An ``int`` returns one typed geometry (or raises ``IndexError``).
    /// A ``slice`` or integer sequence / boolean mask returns a gathered
    /// ``GeometryArray`` (missing rows stay missing).
    ///
    /// Returns
    /// -------
    /// Geometry or GeometryArray
    ///     Scalar geometry for an int index; array for slice/fancy selection.
    pub fn __getitem__(&self, py: Python<'_>, index: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        array_getitem(self, py, index)
    }

    /// Iterate geometries in row order (including missing rows as ``None``).
    ///
    /// Returns
    /// -------
    /// iterator of Geometry or None
    pub fn __iter__(&self) -> PyGeometryArrayIter {
        PyGeometryArrayIter::new(self.clone(), false)
    }

    /// Iterate geometries in reverse row order.
    ///
    /// Returns
    /// -------
    /// iterator of Geometry or None
    pub fn __reversed__(&self) -> PyGeometryArrayIter {
        PyGeometryArrayIter::new(self.clone(), true)
    }

    /// First index of a structurally equal element in `[start, stop)`
    /// (list.index semantics, using Geometry.__eq__:
    /// same
    /// CRS, epoch,
    /// and exact geometry; negative bounds count from the end).
    ///
    /// Parameters
    /// ----------
    /// value : Geometry
    ///     The element to locate.
    ///
    /// start : int, default 0
    ///     First position searched.
    ///
    /// stop : int, optional
    ///     One past the last position searched (the array length when omitted).
    ///
    /// Returns
    /// -------
    /// int
    ///     The first matching position.
    ///
    /// Raises
    /// ------
    /// ValueError
    ///     If no element in the window equals ``value``.
    #[pyo3(signature = (value, start = 0, stop = None), text_signature = "($self, value, start=0, stop=None)")]
    pub fn index(
        &self,
        value: &Bound<'_, PyAny>,
        start: i64,
        stop: Option<i64>,
    ) -> PyResult<usize> {
        let len = self.storage().len();
        let clamp = |bound: i64| -> usize {
            let resolved = if bound < 0 {
                bound + i64::try_from(len).unwrap_or(i64::MAX)
            } else {
                bound
            };
            usize::try_from(resolved.max(0)).unwrap_or(0).min(len)
        };
        let start = clamp(start);
        let stop = stop.map_or(len, clamp);
        if value.is_none() {
            if start < stop
                && let Some(position) = (start..stop).find(|&row| self.is_row_missing(row))
            {
                return Ok(position);
            }
            return Err(PyValueError::new_err("None is not in array"));
        }
        let Some(geometry) = exact_geometry(value) else {
            return Err(PyValueError::new_err("value is not in array"));
        };
        if self.frame_matches(geometry)
            && start < stop
            && let Some(position) = self
                .storage()
                .iter_shapes()
                .enumerate()
                .take(stop)
                .skip(start)
                .position(|(row, shape)| {
                    !self.is_row_missing(row) && shape.as_ref() == geometry.shape.shape()
                })
        {
            return Ok(start + position);
        }
        Err(PyValueError::new_err("geometry is not in array"))
    }
    /// Number of structurally equal elements (`list.count` semantics, using
    /// `Geometry.__eq__`).
    ///
    /// Parameters
    /// ----------
    /// value : Geometry
    ///     The element to count.
    ///
    /// Returns
    /// -------
    /// int
    ///     How many elements equal ``value``.
    pub fn count(&self, value: &Bound<'_, PyAny>) -> usize {
        if value.is_none() {
            return self
                .missing()
                .as_ref()
                .map_or(0, |mask| mask.iter().filter(|missing| **missing).count());
        }
        let Some(geometry) = exact_geometry(value) else {
            return 0;
        };
        if !self.frame_matches(geometry) {
            return 0;
        }
        self.storage()
            .iter_shapes()
            .enumerate()
            .filter(|(row, shape)| {
                !self.is_row_missing(*row) && shape.as_ref() == geometry.shape.shape()
            })
            .count()
    }

    pub fn __and__(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        other: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        overlay_operator(py, slf.as_any(), other, OverlayOp::Intersection)
    }

    pub fn __or__(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        other: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        overlay_operator(py, slf.as_any(), other, OverlayOp::Union)
    }

    pub fn __sub__(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        other: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        overlay_operator(py, slf.as_any(), other, OverlayOp::Difference)
    }

    pub fn __xor__(
        slf: &Bound<'_, Self>,
        py: Python<'_>,
        other: &Bound<'_, PyAny>,
    ) -> PyResult<Py<PyAny>> {
        overlay_operator(py, slf.as_any(), other, OverlayOp::SymmetricDifference)
    }

    pub fn __eq__(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> Py<PyAny> {
        crate::broadcast::py_bool_or_not_implemented(py, exact_geometry_array(other), |other| {
            self.frame == other.frame
                && self.missing() == other.missing()
                && (Arc::ptr_eq(self.storage_arc(), other.storage_arc())
                    || (self.storage().len() == other.storage().len()
                        && self
                            .storage()
                            .iter_shapes()
                            .zip(other.storage().iter_shapes())
                            .enumerate()
                            .all(|(row, (left, right))| {
                                // Mutually-missing rows are equal by mask;
                                // placeholders are NaN points.
                                self.is_row_missing(row) || left.as_ref() == right.as_ref()
                            })))
        })
    }

    /// Which rows are missing, as a boolean `numpy.ndarray`.
    ///
    /// True marks a missing row (None on access); a dense array
    /// returns all-False. The pandas/pyarrow validity convention, with
    /// gometry's full-word spelling.
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    ///     One ``bool`` per row.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> gm.GeometryArray([gm.Point(0, 0), None]).is_missing.tolist()
    /// [False, True]
    #[getter]
    pub fn is_missing(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.missing().map_or_else(
            || crate::py::numpy::false_bool_array(py, self.storage().len()),
            |mask| crate::py::numpy::bool_slice_array(py, mask),
        )
    }

    /// Attach a missing mask to this array's rows (internal; the pandas
    /// bridge builds masked arrays without a rebuild). Length-checked;
    /// new bits are OR-merged with any existing mask so clearing a bit
    /// cannot expose NaN placeholders in packed storage as trusted geometry.
    /// All-present results normalize back to dense. Use ``fill_missing`` to
    /// replace missing rows with real geometry.
    ///
    /// The mask is collected with a fixed expected length and fallible
    /// reservation — never generic ``Vec<bool>`` extraction from a lying
    /// ``__len__`` (which can allocator-abort before the length check).
    pub fn _with_missing(&self, py: Python<'_>, mask: &Bound<'_, PyAny>) -> PyResult<Self> {
        let expected = self.storage().len();
        let mask = crate::collect_bool_mask(py, mask, expected)?;
        let incoming = MissingMask::from_vec(expected, mask);
        // OR with existing bits (pandas GeometryExtensionArray semantics):
        // never unmask a row that already carries a missing placeholder.
        let merged = crate::array::missing::union_pair(self.missing(), incoming.as_ref());
        Ok(self.clone().with_missing_mask(merged))
    }

    /// Return a new array without the missing rows.
    ///
    /// Returns
    /// -------
    /// GeometryArray
    ///     The present rows, in order; the input unchanged (and returned
    ///     as-is when dense).
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> len(gm.GeometryArray([gm.Point(0, 0), None]).drop_missing())
    /// 1
    pub fn drop_missing(&self) -> Self {
        self.missing().map_or_else(
            || self.clone(),
            |mask| {
                let rows: Vec<usize> = mask
                    .iter()
                    .enumerate()
                    .filter(|(_, missing)| !**missing)
                    .map(|(row, _)| row)
                    .collect();
                // Carry frame-only caches for the retained rows — the dense
                // gather rebuilds storage but frame-dependent sidecars remain
                // valid for present rows and must not be discarded.
                self.gather_logical_rows_dense(&rows)
                    .with_selected_caches_from(self, rows.iter().copied())
            },
        )
    }

    /// Return a new array with every missing row replaced by ``value``.
    ///
    /// Parameters
    /// ----------
    /// value : Geometry or GeometryArray
    ///     A scalar fill geometry, or a row-aligned array whose values fill
    ///     the matching missing rows. Only masked rows are consumed from an
    ///     array fill value; every consumed row must be present.
    ///
    /// Returns
    /// -------
    /// GeometryArray
    ///     A dense array (no missing rows); the input unchanged (and
    ///     returned as-is when already dense).
    ///
    /// Raises
    /// ------
    /// CRSMismatchError
    ///     If ``value``'s CRS or coordinate-epoch metadata conflicts with the
    ///     array's.
    /// ValueError
    ///     If an array fill value has a different length.
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> arr = gm.GeometryArray([gm.Point(0, 0), None])
    /// >>> arr.fill_missing(gm.Point(9, 9)).to_wkt()[1]
    /// 'POINT (9 9)'
    pub fn fill_missing(&self, value: &Bound<'_, PyAny>) -> PyResult<Self> {
        let scalar = exact_geometry(value);
        let fill_array = exact_geometry_array(value);
        if scalar.is_none() && fill_array.is_none() {
            return Err(PyTypeError::new_err("expected Geometry or GeometryArray"));
        }
        if let Some(fill) = fill_array
            && fill.storage().len() != self.storage().len()
        {
            return Err(PyValueError::new_err(format!(
                "fill array length {} does not match GeometryArray length {}",
                fill.storage().len(),
                self.storage().len()
            )));
        }
        let Some(mask) = self.missing() else {
            return Ok(self.clone());
        };
        if let Some(geometry) = scalar {
            // The column keeps its own stored label (`compatible` is
            // left-biased); the fill only has to name the same frame.
            self.frame.compatible(&geometry.frame, "fill_missing")?;
            return Ok(self.fill_missing_with_shape(mask, geometry.shape.shape()));
        }
        if let Some(fill) = fill_array {
            self.frame.compatible(&fill.frame, "fill_missing")?;
            // Reject missing fill rows that land on a missing target row before
            // allocating the output column — only consumed fill rows must be present.
            for (row, &missing) in mask.iter().enumerate() {
                if missing && fill.is_row_missing(row) {
                    return Err(GeometryError::new_err(
                        "fill array contains missing geometries at rows this array needs",
                    ));
                }
            }
            return Ok(self.fill_missing_with_array(mask, fill));
        }
        unreachable!("fill value type checked above")
    }

    /// Private batch scatter used by the pandas adapter: replace selected
    /// positions in one native call (never rebuilds the column through Python
    /// per row). ``positions`` and ``values`` are equal-length sequences;
    /// each value is a ``Geometry`` or missing (``None``).
    ///
    /// Parameters
    /// ----------
    /// positions : sequence of int
    ///     Non-negative logical row indices (already bounds-normalized).
    /// values : sequence of Geometry or None
    ///     Replacement values aligned with ``positions``.
    ///
    /// Returns
    /// -------
    /// GeometryArray
    #[pyo3(name = "_replace_at")]
    pub fn replace_at(
        &self,
        positions: &Bound<'_, PyAny>,
        values: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        let positions = crate::collect_py_iter(positions, |item| item.extract::<isize>())?;
        let values = crate::collect_py_iter(values, Ok)?;
        if positions.len() != values.len() {
            return Err(PyValueError::new_err(format!(
                "positions length {} does not match values length {}",
                positions.len(),
                values.len()
            )));
        }
        let n = self.storage().len();
        let mut rows: Vec<(usize, Option<Shape>)> = Vec::with_capacity(positions.len());
        for (position, value) in positions.into_iter().zip(values) {
            if position < 0 || position as usize >= n {
                return Err(PyIndexError::new_err(format!(
                    "index {position} is out of bounds for axis 0 with size {n}"
                )));
            }
            let row = position as usize;
            if value.is_none() {
                rows.push((row, None));
                continue;
            }
            let Some(geometry) = exact_geometry(&value) else {
                return Err(PyTypeError::new_err(format!(
                    "expected Geometry or missing, got {}",
                    value.get_type().name()?
                )));
            };
            // The column keeps its own stored label; the scattered row only
            // has to name the same frame.
            self.frame.compatible(&geometry.frame, "_replace_at")?;
            rows.push((row, Some(geometry.shape.shape().clone())));
        }
        // Packed identity XY points: copy ordinate columns once and patch
        // selected rows (cost ~O(n) memcpy of f64 columns, not per-row Shape).
        if let GeometryArrayStorage::Points { coords, row_map } = self.storage()
            && row_map.is_identity()
            && coords.axes() == crate::geometry::CoordinateAxes::XY
            && rows.iter().all(|(_, shape)| {
                shape.as_ref().is_none_or(|s| {
                    matches!(s, Shape::Point(p) if p.axes == crate::geometry::CoordinateAxes::XY)
                })
            })
        {
            let mut xs = coords.xs().to_vec();
            let mut ys = coords.ys().to_vec();
            let mut missing_flags: Vec<bool> =
                (0..n).map(|row| self.is_row_missing(row)).collect();
            for (row, shape) in rows {
                match shape {
                    None => {
                        xs[row] = f64::NAN;
                        ys[row] = f64::NAN;
                        missing_flags[row] = true;
                    },
                    Some(Shape::Point(point)) => {
                        xs[row] = point.x;
                        ys[row] = point.y;
                        missing_flags[row] = false;
                    },
                    Some(_) => unreachable!("axes gate above"),
                }
            }
            let seq = CoordSeq::from_columns_unchecked(xs.into(), ys.into(), None, None);
            return Ok(Self::packed_points(seq, self.frame.clone())
                .with_missing_mask(MissingMask::from_vec(n, missing_flags)));
        }

        let mut shapes: Vec<Shape> = self.storage().iter_shapes().map(Cow::into_owned).collect();
        let mut missing_flags: Vec<bool> = (0..n).map(|row| self.is_row_missing(row)).collect();
        for (row, shape) in rows {
            match shape {
                None => {
                    missing_flags[row] = true;
                    shapes[row] = Self::missing_placeholder();
                },
                Some(shape) => {
                    missing_flags[row] = false;
                    shapes[row] = shape;
                },
            }
        }
        Ok(Self::from_shapes(shapes, self.frame.clone())
            .with_missing_mask(MissingMask::from_vec(n, missing_flags)))
    }

    /// Whether a geometry (or missing row via ``None``) is present.
    ///
    /// Equality is structural (``Geometry.__eq__``): CRS, epoch, and exact
    /// geometry. ``None in arr`` is true when any row is missing.
    ///
    /// Returns
    /// -------
    /// bool
    pub fn __contains__(&self, item: &Bound<'_, PyAny>) -> bool {
        if item.is_none() {
            return self.has_missing();
        }
        exact_geometry(item).is_some_and(|geom| {
            self.frame_matches(geom)
                && self
                    .storage()
                    .iter_shapes()
                    .enumerate()
                    .any(|(row, shape)| {
                        !self.is_row_missing(row) && shape.as_ref() == geom.shape.shape()
                    })
        })
    }

    pub fn __hash__(&self) -> u64 {
        use std::hash::{Hash as _, Hasher as _};
        let mut hasher = crate::collections::python_hasher();
        self.crs_ref().hash(&mut hasher);
        self.epoch().map(f64::to_bits).hash(&mut hasher);
        self.storage().len().hash(&mut hasher);
        for (row, shape) in self.storage().iter_shapes().enumerate() {
            let missing = self.is_row_missing(row);
            missing.hash(&mut hasher);
            if !missing {
                shape.hash(&mut hasher);
            }
        }
        hasher.finish()
    }
    /// Logical coordinate payload in bytes (numpy's ``nbytes`` convention):
    /// the stored ``f64`` ordinate columns for this array's selected rows.
    /// Slices and fancy-indexed arrays report only their logical rows. Object
    /// headers, CSR offset columns, row maps, prepared-geometry / frame
    /// sidecars, gather memos, and CRS metadata are excluded (``nbytes`` is
    /// payload-only, matching NumPy).
    ///
    /// Returns
    /// -------
    /// int
    #[getter]
    pub fn nbytes(&self) -> usize {
        self.storage().logical_coordinate_bytes()
    }

    /// `sys.getsizeof` support: the wrapper plus this array's logical
    /// Rust-side heap — coordinate payload, CSR offsets, row maps, missing
    /// mask, and any already-materialized prepared-geometry / frame sidecars.
    /// Lazy cache slots that have not been populated do not inflate the total.
    /// Shared backing buffers are accounted like NumPy views: the logical view
    /// is reported, not the whole shared parent allocation.
    pub fn __sizeof__(&self) -> usize {
        std::mem::size_of::<Self>() + self.heap_bytes()
    }

    pub fn __repr__(&self) -> String {
        let mut out = String::from("<GeometryArray");
        let mut kinds = self
            .present_shape_rows()
            .map(|(_, shape)| shape.geometry_type());
        if let Some(first) = kinds.next()
            && kinds.all(|kind| kind == first)
        {
            out.push('[');
            out.push_str(first);
            out.push(']');
        }
        let _ = write!(out, " len={}", self.storage().len());
        if let Some(mask) = self.missing() {
            let missing = mask.missing_count();
            if missing != 0 {
                let _ = write!(out, " missing={missing}");
            }
        }
        if let Some(crs) = self.crs_str() {
            out.push(' ');
            out.push_str(crs);
        }
        if let Some(epoch) = self.epoch() {
            let _ = write!(out, " @{epoch}");
        }
        out.push('>');
        out
    }
}
}

impl PyGeometryArray {
    /// Dense fill from a scalar shape: packed columns stay column-native;
    /// mixed storage patches only missing rows. Never stages `PyGeometry`.
    fn fill_missing_with_shape(&self, mask: &MissingMask, fill: &Shape) -> Self {
        let frame = self.frame.clone();
        match (self.storage(), fill) {
            (GeometryArrayStorage::Points { coords, row_map }, Shape::Point(point))
                if point.axes == coords.axes() =>
            {
                let mut builder = PointColumnBuilder::like_coords(coords, mask.len());
                for (row, &missing) in mask.iter().enumerate() {
                    if missing {
                        builder.push(*point);
                    } else {
                        builder.push_at(coords, physical_row(row_map.as_deref(), row));
                    }
                }
                Self::packed_points(builder.finish_infallible(), frame)
            },
            (GeometryArrayStorage::Mixed(shapes), _) => {
                let mut out = shapes.clone();
                for (row, &missing) in mask.iter().enumerate() {
                    if missing {
                        out[row] = fill.clone();
                    }
                }
                Self::from_shapes(out, frame)
            },
            _ => {
                let shapes: Vec<Shape> = self
                    .storage()
                    .iter_shapes()
                    .enumerate()
                    .map(|(row, shape)| {
                        if mask[row] {
                            fill.clone()
                        } else {
                            shape.into_owned()
                        }
                    })
                    .collect();
                Self::from_shapes(shapes, frame)
            },
        }
    }

    /// Dense fill from a row-aligned array: packed point×point stays columnar;
    /// otherwise collect shapes only (no `PyGeometry` staging).
    fn fill_missing_with_array(&self, mask: &MissingMask, fill: &Self) -> Self {
        let frame = self.frame.clone();
        match (self.storage(), fill.storage()) {
            (
                GeometryArrayStorage::Points {
                    coords: left_coords,
                    row_map: left_map,
                },
                GeometryArrayStorage::Points {
                    coords: right_coords,
                    row_map: right_map,
                },
            ) if left_coords.axes() == right_coords.axes() => {
                let mut builder = PointColumnBuilder::like_coords(left_coords, mask.len());
                for (row, &missing) in mask.iter().enumerate() {
                    if missing {
                        builder.push_at(right_coords, physical_row(right_map.as_deref(), row));
                    } else {
                        builder.push_at(left_coords, physical_row(left_map.as_deref(), row));
                    }
                }
                Self::packed_points(builder.finish_infallible(), frame)
            },
            (GeometryArrayStorage::Mixed(shapes), _) => {
                let mut out = shapes.clone();
                for (row, fill_shape) in fill.storage().iter_shapes().enumerate() {
                    if mask[row] {
                        out[row] = fill_shape.into_owned();
                    }
                }
                Self::from_shapes(out, frame)
            },
            _ => {
                let shapes: Vec<Shape> = self
                    .storage()
                    .iter_shapes()
                    .zip(fill.storage().iter_shapes())
                    .enumerate()
                    .map(|(row, (shape, fill_shape))| {
                        if mask[row] {
                            fill_shape.into_owned()
                        } else {
                            shape.into_owned()
                        }
                    })
                    .collect();
                Self::from_shapes(shapes, frame)
            },
        }
    }
}
