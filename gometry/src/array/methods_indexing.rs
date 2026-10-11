#![expect(
    clippy::similar_names,
    reason = "axis and formula symbols name paired quantities here (minx/miny, xs/ys, lat_rad/lam_rad, step/stop), which this lint reads as typos"
)]
#![expect(
    clippy::used_underscore_items,
    reason = "`_with_missing` is private by Python convention, not unused"
)]
use std::fmt::Write as _;

use numpy::{
    Element, PyArrayDescrMethods as _, PyArrayDyn, PyArrayMethods as _, PyUntypedArray,
    PyUntypedArrayMethods as _,
};
use pyo3::exceptions::{PyIndexError, PyTypeError, PyValueError};
use pyo3::types::{PyBool, PyInt, PyList, PySequence, PyTuple};

use crate::array::{
    Arc, ArrayRow, Bound, GeometryError, HeapSize, IntoPyObject as _, OverlayOp, Py, PyAny,
    PyAnyMethods as _, PyCoordinates, PyErr, PyGeometryArray, PyGeometryArrayIter,
    PyListMethods as _, PyResult, PyTupleMethods as _, PyTypeMethods as _, Python, Shape, Typed,
    exact_geometry, exact_geometry_array, overlay_operator,
};
use crate::boundary::coordinates::measure_storage_payload_bytes;
use crate::geometry::storage::{ValidityMask, validity as missing};
use crate::py::numpy::{false_bool_array, reject_masked_array};
use crate::py::row::{RowContainer, RowGetItemContainer, array_getitem, slice_row_indices};
use crate::py::sizeof::python_sizeof;

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
    reject_masked_array(index)?;
    let ndim = array.ndim();
    if ndim > 1 {
        return Err(PyTypeError::new_err(format!(
            "{label} NumPy indices must be zero- or one-dimensional"
        )));
    }
    let dtype = array.dtype();
    if dtype.kind() == b'b' && dtype.itemsize() == 1 {
        if ndim == 0 {
            return Err(PyTypeError::new_err(format!(
                "boolean scalar is not a {type_label} index; use an integer index or a boolean mask"
            )));
        }
        let owned = owned_numpy_values::<bool>(index)?;
        return Ok(Some(NumpyFancyIndex::Mask(owned)));
    }
    match (dtype.kind(), dtype.itemsize()) {
        (b'i', 1) => return numpy_integer_fancy_index::<i8>(index, ndim, label).map(Some),
        (b'i', 2) => return numpy_integer_fancy_index::<i16>(index, ndim, label).map(Some),
        (b'i', 4) => return numpy_integer_fancy_index::<i32>(index, ndim, label).map(Some),
        (b'i', 8) => return numpy_integer_fancy_index::<i64>(index, ndim, label).map(Some),
        (b'u', 1) => return numpy_integer_fancy_index::<u8>(index, ndim, label).map(Some),
        (b'u', 2) => return numpy_integer_fancy_index::<u16>(index, ndim, label).map(Some),
        (b'u', 4) => return numpy_integer_fancy_index::<u32>(index, ndim, label).map(Some),
        (b'u', 8) => return numpy_integer_fancy_index::<u64>(index, ndim, label).map(Some),
        _ => {},
    }
    Err(PyTypeError::new_err(format!(
        "{label} NumPy indices must have an integer or boolean dtype"
    )))
}

fn native_numpy_array<'py, T>(index: &Bound<'py, PyAny>) -> PyResult<Bound<'py, PyArrayDyn<T>>>
where
    T: Element,
{
    let py = index.py();
    let array = index.cast::<PyUntypedArray>()?;
    // SAFETY: `array` is a verified NumPy array and the target is a built-in dtype.
    let ptr = unsafe {
        numpy::PY_ARRAY_API.PyArray_CastToType(
            py,
            array.as_array_ptr(),
            T::get_dtype(py).into_dtype_ptr(),
            0,
        )
    };
    // SAFETY: the C API returns a new reference or null; the helper handles both cases.
    unsafe { Bound::from_owned_ptr_or_err(py, ptr).map(|array| array.cast_into_unchecked()) }
}

/// Copy logical NumPy values through a C-level native-dtype cast.
fn owned_numpy_values<T>(index: &Bound<'_, PyAny>) -> PyResult<Vec<T>>
where
    T: Copy + Element,
{
    let array = native_numpy_array::<T>(index)?;
    let readonly = array.try_readonly()?;
    let values = readonly.as_array();
    Ok(values.iter().copied().collect())
}

pub(crate) fn numpy_integer_fancy_index<T>(
    index: &Bound<'_, PyAny>,
    ndim: usize,
    label: &str,
) -> PyResult<NumpyFancyIndex>
where
    T: Copy + Element + TryInto<isize>,
{
    let array = native_numpy_array::<T>(index)?;
    let readonly = array.try_readonly()?;
    let values = readonly.as_array();
    if ndim == 0 {
        let value = values
            .iter()
            .copied()
            .next()
            .expect("zero-dimensional NumPy arrays have one element");
        return Ok(NumpyFancyIndex::Scalar(numpy_index_value(value, label)?));
    }
    let mut out = Vec::with_capacity(values.len());
    for value in values.iter().copied() {
        out.push(numpy_index_value(value, label)?);
    }
    Ok(NumpyFancyIndex::Indices(out))
}

fn numpy_index_value<T>(value: T, label: &str) -> PyResult<isize>
where
    T: TryInto<isize>,
{
    value
        .try_into()
        .map_err(|_| PyIndexError::new_err(format!("{label} index is too large")))
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

enum FancyIndexCollector {
    Empty,
    Indices(Vec<isize>),
    Mask(Vec<bool>),
}

impl FancyIndexCollector {
    const fn new() -> Self {
        Self::Empty
    }

    fn push<'py>(
        &mut self,
        py: Python<'py>,
        bound: &Bound<'py, PyAny>,
        item_count: usize,
    ) -> PyResult<bool> {
        if bound.cast_exact::<PyBool>().is_ok() {
            if matches!(self, Self::Indices(_)) {
                return Ok(false);
            }
            let value = bound.extract::<bool>()?;
            return match self {
                Self::Empty => {
                    let mut mask = Vec::with_capacity(item_count);
                    mask.push(value);
                    *self = Self::Mask(mask);
                    Ok(true)
                },
                Self::Mask(mask) => {
                    mask.push(value);
                    Ok(true)
                },
                Self::Indices(_) => unreachable!("mixed indices decline before boolean extraction"),
            };
        }

        let integer = match crate::py_index_object("index", bound) {
            Ok(integer) => integer,
            Err(error)
                if error.is_instance_of::<PyTypeError>(py) && !crate::py_index_check(bound) =>
            {
                return Ok(false);
            },
            Err(error) => return Err(error),
        };
        if matches!(self, Self::Mask(_)) {
            return Ok(false);
        }
        let value = integer
            .extract::<isize>()
            .map_err(|_| PyIndexError::new_err("index is too large"))?;
        match self {
            Self::Empty => {
                let mut indices = Vec::with_capacity(item_count);
                indices.push(value);
                *self = Self::Indices(indices);
                Ok(true)
            },
            Self::Indices(indices) => {
                indices.push(value);
                Ok(true)
            },
            Self::Mask(_) => unreachable!("mixed masks decline before integer extraction"),
        }
    }

    fn finish(self) -> CollectedFancyIndex {
        match self {
            Self::Empty => CollectedFancyIndex::Empty,
            Self::Indices(indices) => CollectedFancyIndex::Indices(indices),
            Self::Mask(mask) => CollectedFancyIndex::Mask(mask),
        }
    }
}

/// Classify and materialize a fancy-index sequence in one pass.
///
/// A list is first snapshotted into an immutable tuple, then its slots are
/// scanned by the generic classifier. Other providers collect their items once.
pub(crate) fn classify_and_collect_fancy_index(
    py: Python<'_>,
    sequence: &Bound<'_, PySequence>,
) -> PyResult<CollectedFancyIndex> {
    let any = sequence.as_any();
    if let Ok(list) = any.cast::<PyList>() {
        let snapshot = list.to_tuple();
        return classify_and_collect_stable_tuple(py, &snapshot);
    }
    if let Ok(tuple) = any.cast::<PyTuple>() {
        return classify_and_collect_exact_tuple(tuple);
    }

    classify_and_collect_generic_sequence(py, sequence)
}

fn classify_and_collect_generic_sequence(
    py: Python<'_>,
    sequence: &Bound<'_, PySequence>,
) -> PyResult<CollectedFancyIndex> {
    let items = crate::collect_sequence_items(sequence)?;
    let item_count = items.len();
    let mut collector = FancyIndexCollector::new();
    for item in &items {
        let bound = item.bind(py);
        if !collector.push(py, bound, item_count)? {
            return Ok(CollectedFancyIndex::Invalid);
        }
    }
    Ok(collector.finish())
}

fn classify_and_collect_stable_tuple<'py>(
    py: Python<'py>,
    tuple: &Bound<'py, PyTuple>,
) -> PyResult<CollectedFancyIndex> {
    let item_count = tuple.len();
    for (position, _) in tuple.as_slice().iter().enumerate() {
        crate::check_python_signals(py, position)?;
    }
    let mut collector = FancyIndexCollector::new();
    for item in tuple.as_slice() {
        if !collector.push(py, item, item_count)? {
            return Ok(CollectedFancyIndex::Invalid);
        }
    }
    Ok(collector.finish())
}

/// Exact tuple fast path; its bool/int classification is intentionally distinct
/// from the generic list-snapshot rules above.
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
            if item.cast_exact::<PyBool>().is_ok() {
                return Ok(CollectedFancyIndex::Invalid);
            }
            if item.cast::<PyInt>().is_err() {
                let sequence = tuple.cast::<PySequence>()?.clone();
                return classify_and_collect_generic_sequence(tuple.py(), &sequence);
            }
            indices.push(crate::py_isize_required("index", &item)?);
        }
        return Ok(CollectedFancyIndex::Indices(indices));
    }
    let sequence = tuple.cast::<PySequence>()?.clone();
    classify_and_collect_generic_sequence(tuple.py(), &sequence)
}

impl HeapSize for PyGeometryArray {
    fn heap_bytes(&self) -> usize {
        self.storage()
            .logical_heap_bytes()
            .saturating_add(self.missing().map_or(0, HeapSize::heap_bytes))
            .saturating_add(self.row_products.heap_bytes())
            .saturating_add(self.bounds_cache.heap_bytes())
            // This cache's value is an inline four-f64 tuple, so its Arc and
            // OnceLock owner are the only heap bytes in either state.
            .saturating_add(crate::heap_size::ARC_CONTROL_BYTES)
            .saturating_add(std::mem::size_of::<
                std::sync::OnceLock<Option<(f64, f64, f64, f64)>>,
            >())
            .saturating_add(self.frame.crs_ref().map_or(0, HeapSize::heap_bytes))
    }
}

impl RowContainer for PyGeometryArray {
    const LABEL: &'static str = "GeometryArray";
    const INDEX_LABEL: &'static str = "geometry array";

    fn row_count(&self) -> usize {
        self.len()
    }

    fn scalar_row(&self, py: Python<'_>, row: usize) -> PyResult<Py<PyAny>> {
        if self.is_row_missing(row) {
            return Ok(py.None());
        }
        Ok(Typed(self.geometry_at(row).map_err(PyErr::from)?)
            .into_pyobject(py)?
            .unbind())
    }
}

impl RowGetItemContainer for PyGeometryArray {
    fn gather_rows(&self, rows: &[usize]) -> PyResult<Self> {
        self.gather_logical_rows(rows).map_err(PyErr::from)
    }

    fn slice_rows(&self, start: isize, stop: isize, step: isize, count: usize) -> PyResult<Self> {
        if step == 1 {
            return self
                .gather_logical_row_range(start as usize..stop.max(start) as usize)
                .map_err(PyErr::from);
        }
        self.gather_logical_rows_iter(slice_row_indices(start, step, count))
            .map_err(PyErr::from)
    }

    fn empty(&self) -> PyResult<Self> {
        self.gather_logical_rows(&[]).map_err(PyErr::from)
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

impl PyGeometryArray {
    pub(crate) fn try_drop_missing(&self) -> crate::error::Result<Self> {
        if self.missing().is_none() {
            return Ok(self.clone());
        }
        let rows = self.rows.drop_missing()?;
        let source_rows = self.rows.present_logical_indices();
        Ok(Self::from_selected_rows(
            self,
            rows,
            self.frame.clone(),
            source_rows,
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
    ///     use `.row_index` to recover source-row alignment, or call
    ///     `drop_missing()` first for an explicit dense path.
    #[getter]
    pub fn coords(slf: &Bound<'_, Self>) -> PyCoordinates {
        PyCoordinates::new(
            slf.borrow().coordinate_view(),
            slf.clone().unbind().into_any(),
        )
    }

    /// Select rows by integer-like scalar, slice, or fancy index.
    ///
    /// An ``int``, object implementing ``__index__``, or zero-dimensional
    /// integer ndarray returns one typed geometry. A ``slice``, integer
    /// sequence, one-dimensional integer ndarray, or boolean mask returns a
    /// gathered ``GeometryArray``; missing rows stay missing. Boolean scalars
    /// and NumPy index arrays with rank above one raise ``TypeError``.
    ///
    /// Returns
    /// -------
    /// Geometry, None, or GeometryArray
    ///     Scalar geometry for integer-like scalar selection; array for
    ///     slice or one-dimensional fancy selection.
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
        #[pyo3(from_py_with = crate::strict_i64)] start: i64,
        #[pyo3(from_py_with = crate::strict_optional_i64)] stop: Option<i64>,
    ) -> PyResult<usize> {
        let len = self.len();
        let clamp = |bound: i64| -> usize {
            let resolved = if bound < 0 {
                bound.saturating_add(i64::try_from(len).unwrap_or(i64::MAX))
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
        if self.frame_matches(geometry) && start < stop {
            for (row, position) in self
                .row_operation()
                .rows_with_missing(|| ())
                .take(stop)
                .skip(start)
                .zip(start..stop)
            {
                let equal = match row {
                    ArrayRow::Present(row) => row
                        .row()
                        .with_shape(|shape| shape == geometry.shape.shape())?,
                    ArrayRow::Missing { .. } => false,
                };
                if equal {
                    return Ok(position);
                }
            }
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
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "the match count cannot exceed the admitted array row count"
    )]
    pub fn count(&self, value: &Bound<'_, PyAny>) -> PyResult<usize> {
        if value.is_none() {
            return Ok(self
                .missing()
                .as_ref()
                .map_or(0, |mask| mask.iter().filter(|missing| *missing).count()));
        }
        let Some(geometry) = exact_geometry(value) else {
            return Ok(0);
        };
        if !self.frame_matches(geometry) {
            return Ok(0);
        }
        let mut count = 0;
        for row in self.row_operation().rows_with_missing(|| ()) {
            if let ArrayRow::Present(row) = row
                && row
                    .row()
                    .with_shape(|shape| shape == geometry.shape.shape())?
            {
                count += 1;
            }
        }
        Ok(count)
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

    pub fn __eq__(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        let Some(other) = exact_geometry_array(other) else {
            return Ok(py.NotImplemented());
        };
        let mut equal = self.frame == other.frame && self.missing() == other.missing();
        if equal && !Arc::ptr_eq(self.storage_arc(), other.storage_arc()) {
            equal = self.len() == other.len();
            if equal {
                for (left, right) in self
                    .row_operation()
                    .rows_with_missing(|| ())
                    .zip(other.row_operation().rows_with_missing(|| ()))
                {
                    let row_equal = match (left, right) {
                        (ArrayRow::Missing { .. }, ArrayRow::Missing { .. }) => true,
                        (ArrayRow::Present(left), ArrayRow::Present(right)) => left
                            .row()
                            .try_with_shape(|left| {
                                right.row().with_shape(|right| left == right)
                            })?,
                        _ => false,
                    };
                    if !row_equal {
                        equal = false;
                        break;
                    }
                }
            }
        }
        Ok(crate::broadcast::py_bool(py, equal))
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
            || false_bool_array(py, self.len()),
            |mask| {
                let array = numpy::PyArray1::<bool>::zeros(py, self.len(), false);
                let mut write = array.try_readwrite()?;
                for (output, missing) in write.as_slice_mut()?.iter_mut().zip(mask.iter()) {
                    *output = missing;
                }
                write.make_nonwriteable();
                Ok(array.into_any().unbind())
            },
        )
    }

    /// Attach a missing mask to this array's rows (internal; the pandas
    /// bridge builds masked arrays without a rebuild). Length-checked;
    /// new bits are OR-merged with any existing mask so clearing a bit
    /// cannot expose NaN placeholders in packed storage as trusted geometry.
    /// All-present results normalize back to dense. Use ``fill_missing`` to
    /// replace missing rows with real geometry.
    ///
    /// The array's row count determines the expected mask length. The mask's
    /// length hint is ignored; its iterator supplies the values.
    pub fn _with_missing(&self, mask: &Bound<'_, PyAny>) -> PyResult<Self> {
        let expected = self.len();
        let mask = crate::collect_bool_mask(mask, expected)?;
        let incoming = ValidityMask::from_vec(expected, mask);
        // OR with existing bits (pandas GeometryExtensionArray semantics):
        // never unmask a row that already carries a missing placeholder.
        let merged = missing::union_many([self.missing(), incoming.as_ref()]);
        self.clone().with_missing_mask(merged).map_err(PyErr::from)
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
    pub fn drop_missing(&self) -> PyResult<Self> {
        self.try_drop_missing().map_err(PyErr::from)
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
            && fill.len() != self.len()
        {
            return Err(PyValueError::new_err(format!(
                "fill array length {} does not match GeometryArray length {}",
                fill.len(),
                self.len()
            )));
        }
        let Some(mask) = self.missing() else {
            return Ok(self.clone());
        };
        if let Some(geometry) = scalar {
            // The column keeps its own stored label (`compatible` is
            // left-biased); the fill only has to name the same frame.
            self.frame.compatible(&geometry.frame, "fill_missing")?;
            return self.fill_missing_with_shape(mask, geometry.shape.shape());
        }
        if let Some(fill) = fill_array {
            self.frame.compatible(&fill.frame, "fill_missing")?;
            // Reject missing fill rows that land on a missing target row before
            // allocating the output column — only consumed fill rows must be present.
            for (row, missing) in mask.iter().enumerate() {
                if missing && fill.is_row_missing(row) {
                    return Err(GeometryError::new_err(
                        "fill array contains missing geometries at rows this array needs",
                    ));
                }
            }
            return self.fill_missing_with_array(mask, fill);
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
    #[expect(
        clippy::indexing_slicing,
        reason = "positions are bounds-checked before they index the row map, and selected replacements come from that map"
    )]
    pub fn replace_at(
        &self,
        positions: &Bound<'_, PyAny>,
        values: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        let positions = crate::collect_py_iter(positions, |item| {
            crate::py_isize_required("position", &item)
        })?;
        let values = crate::collect_py_iter(values, Ok)?;
        if positions.len() != values.len() {
            return Err(PyValueError::new_err(format!(
                "positions length {} does not match values length {}",
                positions.len(),
                values.len()
            )));
        }
        if positions.is_empty() {
            return Ok(self.clone());
        }
        let n = self.len();
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
        let mut selected = vec![None; n];
        for (index, (row, _)) in rows.iter().enumerate() {
            selected[*row] = Some(index);
        }
        let mut builder = crate::array::StreamingShapes::with_capacity(0, n);
        for (row, source) in self
            .row_operation()
            .rows_with_missing(|| ())
            .enumerate()
        {
            if let Some(replacement) = selected[row] {
                match rows[replacement].1.as_ref() {
                    None => builder.try_push_missing().map_err(PyErr::from)?,
                    Some(shape) => {
                        builder
                            .try_push_shape_ref(shape)
                            .map_err(PyErr::from)?;
                    },
                }
                continue;
            }
            match source {
                ArrayRow::Missing { .. } => {
                    builder.try_push_missing().map_err(PyErr::from)?;
                },
                ArrayRow::Present(row) => {
                    builder
                        .try_push_row(Some(row.row()))
                        .map_err(PyErr::from)?;
                },
            }
        }
        let (storage, missing, layout) = builder.finish_parts().map_err(PyErr::from)?;
        Self::from_result_storage(
            Arc::new(storage),
            self.frame.clone(),
            missing,
            layout,
            crate::array::ComputedCaches::fresh(),
        )
        .map_err(PyErr::from)
    }

    /// Whether a geometry (or missing row via ``None``) is present.
    ///
    /// Equality is structural (``Geometry.__eq__``): CRS, epoch, and exact
    /// geometry. ``None in arr`` is true when any row is missing.
    ///
    /// Returns
    /// -------
    /// bool
    pub fn __contains__(&self, py: Python<'_>, item: &Bound<'_, PyAny>) -> PyResult<bool> {
        if item.is_none() {
            return Ok(self.has_missing());
        }
        let Some(geom) = exact_geometry(item) else {
            return Ok(false);
        };
        let same_frame = self.frame_matches(geom);
        let expected = Arc::clone(&geom.shape);
        let array = self.clone();
        py.detach(move || {
            if !same_frame {
                return Ok(false);
            }
            for row in array.row_operation().present_rows() {
                if row
                    .row()
                    .with_shape(|shape| shape == expected.shape())
                    .map_err(PyErr::from)?
                {
                    return Ok(true);
                }
            }
            Ok(false)
        })
    }

    pub fn __hash__(&self) -> PyResult<u64> {
        use std::hash::{Hash as _, Hasher as _};
        let mut hasher = crate::collections::python_hasher();
        self.crs_ref().hash(&mut hasher);
        self.epoch().map(f64::to_bits).hash(&mut hasher);
        self.len().hash(&mut hasher);
        for row in self.row_operation().rows_with_missing(|| ()) {
            match row {
                ArrayRow::Missing { .. } => true.hash(&mut hasher),
                ArrayRow::Present(row) => {
                    false.hash(&mut hasher);
                    row.row().with_shape(|shape| shape.hash(&mut hasher))?;
                },
            }
        }
        Ok(hasher.finish())
    }
    /// Logical coordinate payload in bytes (numpy's ``nbytes`` convention):
    /// the stored ``f64`` ordinate columns for this array's selected rows.
    /// Slices and fancy-indexed arrays report only their logical rows. Object
    /// headers, CSR offset columns, row maps, prepared-geometry / frame
    /// sidecars and CRS metadata are excluded (``nbytes`` is
    /// payload-only, matching NumPy).
    ///
    /// Returns
    /// -------
    /// int
    #[getter]
    pub fn nbytes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyInt>> {
        let bytes = measure_storage_payload_bytes(
            py,
            self.storage(),
            self.missing(),
            0..self.len(),
        )?;
        Ok(bytes.into_pyint(py))
    }

    /// `sys.getsizeof` support: Python's allocation base plus this array's logical
    /// Rust-side heap — coordinate payload, CSR offsets, row maps, missing
    /// mask, cache owners and their already-materialized contents, spilled CRS
    /// frame text, and any already-materialized prepared-geometry / frame
    /// sidecars.
    /// Lazy cache slots that have not been populated do not inflate the total.
    /// Shared backing buffers use the logical view estimate, not the whole
    /// shared parent allocation.
    ///
    /// Raises
    /// ------
    /// OverflowError
    ///     If the reported size exceeds Python's ``Py_ssize_t`` range.
    pub fn __sizeof__(slf: &Bound<'_, Self>) -> PyResult<isize> {
        python_sizeof(slf.py(), slf.as_any(), slf.borrow().heap_bytes())
    }

    pub fn __repr__(&self) -> PyResult<String> {
        let mut out = String::from("<GeometryArray");
        let mut first = None;
        let mut uniform = true;
        for row in self.row_operation().present_rows() {
            let kind = row
                .row()
                .with_shape(Shape::geometry_type)
                .map_err(PyErr::from)?;
            if let Some(previous) = first {
                uniform &= previous == kind;
            } else {
                first = Some(kind);
            }
        }
        if uniform && let Some(first) = first {
            out.push('[');
            out.push_str(first);
            out.push(']');
        }
        write!(out, " len={}", self.len()).expect("writing a count to String");
        if let Some(mask) = self.missing() {
            let missing = mask.missing_count();
            if missing != 0 {
                write!(out, " missing={missing}").expect("writing a count to String");
            }
        }
        if let Some(crs) = self.crs_str() {
            out.push(' ');
            out.push_str(crs);
        }
        if let Some(epoch) = self.epoch() {
            write!(out, " @{epoch}").expect("writing an epoch to String");
        }
        out.push('>');
        Ok(out)
    }
}
}

impl PyGeometryArray {
    /// Dense fill from a scalar shape: packed columns stay column-native;
    /// mixed storage patches only missing rows. Never stages `PyGeometry`.
    fn fill_missing_with_shape(&self, _mask: &ValidityMask, fill: &Shape) -> PyResult<Self> {
        let mut builder = crate::array::StreamingShapes::with_capacity(0, self.len());
        for row in self.row_operation().rows_with_missing(|| ()) {
            match row {
                ArrayRow::Missing { .. } => {
                    builder.try_push_shape_ref(fill).map_err(PyErr::from)?;
                },
                ArrayRow::Present(row) => {
                    builder.try_push_row(Some(row.row())).map_err(PyErr::from)?;
                },
            }
        }
        builder.finish(self.frame.clone()).map_err(PyErr::from)
    }

    /// Dense fill from a row-aligned array through the native row builder;
    /// incompatible rows take its single explicit mixed fallback.
    fn fill_missing_with_array(&self, _mask: &ValidityMask, fill: &Self) -> PyResult<Self> {
        let mut builder = crate::array::StreamingShapes::with_capacity(0, self.len());
        for (left, right) in self
            .row_operation()
            .rows_with_missing(|| ())
            .zip(fill.row_operation().rows_with_missing(|| ()))
        {
            match left {
                ArrayRow::Missing { .. } => match right {
                    ArrayRow::Present(row) => {
                        builder.try_push_row(Some(row.row())).map_err(PyErr::from)?;
                    },
                    ArrayRow::Missing { .. } => {
                        unreachable!("required fill rows were checked present");
                    },
                },
                ArrayRow::Present(row) => {
                    builder.try_push_row(Some(row.row())).map_err(PyErr::from)?;
                },
            }
        }
        builder.finish(self.frame.clone()).map_err(PyErr::from)
    }
}

#[cfg(test)]
mod fancy_index_tests {
    use super::*;
    use crate::test_support::allocation_count;

    #[test]
    fn known_length_integer_indices_reserve_once() {
        Python::initialize();
        Python::attach(|py| {
            let allocations = [2_usize, 128].map(|rows| {
                let tuple = PyTuple::new(py, 0..rows).unwrap();
                let (result, allocations) =
                    allocation_count(|| classify_and_collect_stable_tuple(py, &tuple));
                let CollectedFancyIndex::Indices(indices) = result.unwrap() else {
                    panic!("integer tuple snapshot must produce integer indices");
                };
                assert!(
                    indices
                        .iter()
                        .copied()
                        .eq(0..isize::try_from(rows).unwrap())
                );
                allocations
            });
            assert!(allocations[1] <= allocations[0]);
        });
    }
}

#[cfg(test)]
mod replacement_tests {
    use super::*;
    use crate::PyGeometry;
    use crate::crs::frame::Frame;
    use crate::geometry::{Point, ShapeData};

    #[test]
    fn replacement_starts_with_fresh_products_and_releases_the_source_owner() {
        use pyo3::IntoPyObjectExt as _;

        let source = PyGeometryArray::mixed_shapes(
            vec![
                Shape::Point(Point::new_raw_xy(0.0, 0.0)),
                Shape::Point(Point::new_raw_xy(1.0, 1.0)),
            ],
            Frame::None,
        )
        .unwrap();
        let source_plan = source
            .row_products
            .prepared_plan::<std::convert::Infallible>(0, || {
                Ok(Arc::new(ShapeData::new(Shape::Point(Point::new_raw_xy(
                    0.0, 0.0,
                )))))
            })
            .unwrap();
        let source_frame = source.row_products.frame_cache(0);
        let source_unchanged_plan = source
            .row_products
            .prepared_plan::<std::convert::Infallible>(1, || {
                Ok(Arc::new(ShapeData::new(Shape::Point(Point::new_raw_xy(
                    1.0, 1.0,
                )))))
            })
            .unwrap();
        let source_unchanged_frame = source.row_products.frame_cache(1);
        let source_owner = source.row_products.owner_weak();
        assert_eq!(source.row_products.owner_strong_count(), 1);
        Python::initialize();
        let result = Python::attach(|py| {
            let replacement = Typed(PyGeometry::with_frame(
                Shape::Point(Point::new_raw_xy(9.0, 9.0)),
                Frame::None,
            ))
            .into_py_any(py)?;
            let positions = pyo3::types::PyList::new(py, [0_isize])?;
            let values = pyo3::types::PyList::new(py, [replacement])?;
            source.replace_at(positions.as_any(), values.as_any())
        })
        .unwrap();

        assert_eq!(result.row_products.owner_strong_count(), 1);
        let source_counts = source.row_products.initialized_counts();
        let result_counts = result.row_products.initialized_counts();
        let source_heap = source.heap_bytes();
        let result_heap = result.heap_bytes();
        println!(
            "replacement_diagnostic source_counts={source_counts:?} \
             result_counts={result_counts:?} source_heap={source_heap} \
             result_heap={result_heap} source_owner_strong={} \
             result_owner_strong={} source_alive_before_drop={}",
            source.row_products.owner_strong_count(),
            result.row_products.owner_strong_count(),
            source_owner.upgrade().is_some(),
        );
        assert_eq!(source_counts, (1, 2, 2));
        assert_eq!(result_counts, (0, 0, 0));
        assert!(result.row_products.cached_prepared_plan(0).is_none());
        assert!(result.row_products.cached_prepared_plan(1).is_none());
        drop(source_plan);
        drop(source_frame);
        drop(source_unchanged_plan);
        drop(source_unchanged_frame);
        drop(source);
        assert!(
            source_owner.upgrade().is_none(),
            "a replacement must not retain the source product owner"
        );

        let result_plan = result
            .row_products
            .prepared_plan::<std::convert::Infallible>(0, || {
                Ok(Arc::new(ShapeData::new(Shape::Point(Point::new_raw_xy(
                    9.0, 9.0,
                )))))
            })
            .unwrap();
        assert_eq!(
            result_plan.shape(),
            &Shape::Point(Point::new_raw_xy(9.0, 9.0))
        );
        result.row_products.frame_cache(0);
        let result_unchanged_plan = result
            .row_products
            .prepared_plan::<std::convert::Infallible>(1, || {
                Ok(Arc::new(ShapeData::new(Shape::Point(Point::new_raw_xy(
                    1.0, 1.0,
                )))))
            })
            .unwrap();
        result.row_products.frame_cache(1);
        assert_eq!(result.row_products.initialized_counts(), (1, 2, 2));
        assert_eq!(
            result_unchanged_plan.shape(),
            &Shape::Point(Point::new_raw_xy(1.0, 1.0))
        );
        println!(
            "replacement_diagnostic source_alive_after_drop={}",
            source_owner.upgrade().is_some()
        );
    }
}
