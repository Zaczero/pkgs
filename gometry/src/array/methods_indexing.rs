#![allow(
    clippy::used_underscore_items,
    reason = "`_with_missing` is private by Python convention, not unused"
)]
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

use crate::array::PackedStorage;
use std::fmt::Write as _;

use numpy::{
    Element, PyArrayDescrMethods as _, PyArrayDyn, PyArrayMethods as _, PyUntypedArray,
    PyUntypedArrayMethods as _,
};
use pyo3::exceptions::{PyIndexError, PyTypeError, PyValueError};
use pyo3::types::{PyBool, PyInt, PyList, PySequence, PyTuple};

use crate::array::{
    Arc, ArrayRow, Bound, CoordSeq, GeometryArrayStorage, GeometryError, HeapSize,
    IntoPyObject as _, OverlayOp, Py, PyAny, PyAnyMethods as _, PyCoordinates, PyErr,
    PyGeometryArray, PyGeometryArrayIter, PyListMethods as _, PyResult, PyTupleMethods as _,
    PyTypeMethods as _, Python, Shape, Typed, ValidityMask, exact_geometry, exact_geometry_array,
    overlay_operator,
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
    crate::py::numpy::reject_masked_array(index)?;
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
    let mut out = crate::try_vec_with_capacity(values.len())?;
    for value in values.iter().copied() {
        out.push(value);
    }
    Ok(out)
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
    let mut out = crate::try_vec_with_capacity(values.len())?;
    for value in values.iter().copied() {
        crate::try_push(&mut out, numpy_index_value(value, label)?)?;
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

struct FancyIndexCollector {
    kind: FancyIndex,
    indices: Vec<isize>,
    mask: Vec<bool>,
}

impl FancyIndexCollector {
    const fn new() -> Self {
        Self {
            kind: FancyIndex::Empty,
            indices: Vec::new(),
            mask: Vec::new(),
        }
    }

    fn push<'py>(
        &mut self,
        py: Python<'py>,
        bound: &Bound<'py, PyAny>,
        item_count: usize,
    ) -> PyResult<bool> {
        if bound.cast_exact::<PyBool>().is_ok() {
            self.kind = match self.kind {
                FancyIndex::Empty | FancyIndex::Mask => FancyIndex::Mask,
                _ => FancyIndex::Invalid,
            };
            if matches!(self.kind, FancyIndex::Invalid) {
                return Ok(false);
            }
            if self.mask.is_empty() {
                crate::try_reserve_vec(&mut self.mask, item_count)?;
            }
            self.mask.push(bound.extract::<bool>()?);
            return Ok(true);
        }

        let integer = match crate::py_index_object("index", bound) {
            Ok(integer) => integer,
            Err(error)
                if error.is_instance_of::<PyTypeError>(py)
                    && bound.getattr_opt(pyo3::intern!(py, "__index__"))?.is_none() =>
            {
                return Ok(false);
            },
            Err(error) => return Err(error),
        };
        self.kind = match self.kind {
            FancyIndex::Empty | FancyIndex::Indices => FancyIndex::Indices,
            _ => FancyIndex::Invalid,
        };
        if matches!(self.kind, FancyIndex::Invalid) {
            return Ok(false);
        }
        crate::try_push(
            &mut self.indices,
            integer
                .extract::<isize>()
                .map_err(|_| PyIndexError::new_err("index is too large"))?,
        )?;
        Ok(true)
    }

    fn finish(self) -> CollectedFancyIndex {
        match self.kind {
            FancyIndex::Empty => CollectedFancyIndex::Empty,
            FancyIndex::Indices => CollectedFancyIndex::Indices(self.indices),
            FancyIndex::Mask => CollectedFancyIndex::Mask(self.mask),
            FancyIndex::Invalid => CollectedFancyIndex::Invalid,
        }
    }
}

/// Classify and materialize a fancy-index sequence without trusting its length.
///
/// A list is first snapshotted into an immutable tuple, then its slots are
/// scanned by the generic classifier. Other executable providers still collect
/// items once with fallible reservation so a lying `__len__` cannot
/// allocator-abort.
pub(crate) fn classify_and_collect_fancy_index(
    py: Python<'_>,
    sequence: &Bound<'_, PySequence>,
) -> PyResult<CollectedFancyIndex> {
    // Trusted-shape fast lane: exact builtins are ABI-honest length sources.
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
        let mut mask = crate::try_vec_with_capacity(len)?;
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
        let mut indices = crate::try_vec_with_capacity(len)?;
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
            crate::try_push(&mut indices, crate::py_isize_required("index", &item)?)?;
        }
        return Ok(CollectedFancyIndex::Indices(indices));
    }
    let sequence = tuple.cast::<PySequence>()?.clone();
    classify_and_collect_generic_sequence(tuple.py(), &sequence)
}

impl HeapSize for PyGeometryArray {
    fn heap_bytes(&self) -> usize {
        self.storage().logical_heap_bytes()
            + self.missing().map_or(0, HeapSize::heap_bytes)
            + self.row_products.heap_bytes()
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
        Ok(Typed(self.geometry_at(row)).into_pyobject(py)?.unbind())
    }
}

impl RowGetItemContainer for PyGeometryArray {
    fn gather_rows(&self, rows: &[usize]) -> Self {
        self.gather_logical_rows(rows)
    }

    fn slice_rows(&self, start: isize, stop: isize, step: isize) -> Self {
        if step == 1 {
            return self.gather_logical_row_range(start as usize..stop.max(start) as usize);
        }
        if matches!(self.storage(), GeometryArrayStorage::Packed(PackedStorage::Points { .. }))
            && !self.is_compact_nullable()
        {
            let rows = collect_slice_rows(start, stop, step);
            return self.slice_packed_points(start, stop, step)
                .with_missing_mask(self.gather_missing(&rows))
                .with_selected_caches_from(self, rows);
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
    ///     use `.row_index` to recover source-row alignment, or call
    ///     `drop_missing()` first for an explicit dense path.
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
        #[pyo3(from_py_with = crate::strict_i64)] start: i64,
        #[pyo3(from_py_with = crate::strict_optional_i64)] stop: Option<i64>,
    ) -> PyResult<usize> {
        let len = self.len();
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
                .row_operation()
                .rows_with_missing(|| ())
                .take(stop)
                .skip(start)
                .position(|row| {
                    matches!(row, ArrayRow::Present(row)
                        if row.row().with_shape(|shape| shape == geometry.shape.shape()))
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
                .map_or(0, |mask| mask.iter().filter(|missing| *missing).count());
        }
        let Some(geometry) = exact_geometry(value) else {
            return 0;
        };
        if !self.frame_matches(geometry) {
            return 0;
        }
        self.row_operation()
            .rows_with_missing(|| ())
            .filter(|row| {
                matches!(row, ArrayRow::Present(row)
                    if row.row().with_shape(|shape| shape == geometry.shape.shape()))
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
                    || (self.len() == other.len()
                        && self
                            .row_operation()
                            .rows_with_missing(|| ())
                            .zip(other.row_operation().rows_with_missing(|| ()))
                            .all(|(left, right)| match (left, right) {
                                (ArrayRow::Missing { .. }, ArrayRow::Missing { .. }) => true,
                                (ArrayRow::Present(left), ArrayRow::Present(right)) => left
                                    .row()
                                    .with_shape(|left| {
                                        right.row().with_shape(|right| left == right)
                                    }),
                                _ => false,
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
            || crate::py::numpy::false_bool_array(py, self.len()),
            |mask| {
                let values: Vec<bool> = mask.iter().collect();
                crate::py::numpy::bool_slice_array(py, &values)
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
    /// The mask is collected with a fixed expected length and fallible
    /// reservation — never generic ``Vec<bool>`` extraction from a lying
    /// ``__len__`` (which can allocator-abort before the length check).
    pub fn _with_missing(&self, py: Python<'_>, mask: &Bound<'_, PyAny>) -> PyResult<Self> {
        let expected = self.len();
        let mask = crate::collect_bool_mask(py, mask, expected)?;
        let incoming = ValidityMask::from_vec(expected, mask);
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
                    .filter(|(_, missing)| !*missing)
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
            return Ok(self.fill_missing_with_shape(mask, geometry.shape.shape()));
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
        let mut rows: Vec<(usize, Option<Shape>)> = crate::try_vec_with_capacity(positions.len())?;
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
        if let GeometryArrayStorage::Packed(PackedStorage::Points { coords, row_map }) = self.storage()
            && !self.is_compact_nullable()
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
            let mut missing_flags = if rows.iter().any(|(_, shape)| shape.is_none()) {
                let mut flags = crate::try_vec_with_capacity(n)?;
                flags.resize(n, false);
                Some(flags)
            } else {
                None
            };
            for (row, shape) in rows {
                match shape {
                    None => {
                        xs[row] = f64::NAN;
                        ys[row] = f64::NAN;
                        if let Some(flags) = &mut missing_flags {
                            flags[row] = true;
                        }
                    },
                    Some(Shape::Point(point)) => {
                        xs[row] = point.x;
                        ys[row] = point.y;
                        if let Some(flags) = &mut missing_flags {
                            flags[row] = false;
                        }
                    },
                    Some(_) => unreachable!("axes gate above"),
                }
            }
            // SAFETY: point replacement updates X and Y vectors in lockstep;
            // both retain the original checked row count.
            let seq = unsafe { CoordSeq::from_columns_unchecked(xs.into(), ys.into(), None, None) };
            return Ok(Self::packed_points(seq, self.frame.clone())
                .with_missing_mask(
                    missing_flags.and_then(|flags| ValidityMask::from_vec(n, flags)),
                ));
        }

        let mut shapes: Vec<Option<Shape>> = self
            .row_operation()
            .rows_with_missing(|| None::<Shape>)
            .map(|row| match row {
                ArrayRow::Missing { value, .. } => value,
                ArrayRow::Present(row) => Some(row.row().with_shape(Clone::clone)),
            })
            .collect();
        for (row, shape) in rows {
            match shape {
                None => {
                    shapes[row] = None;
                },
                Some(shape) => {
                    shapes[row] = Some(shape);
                },
            }
        }
        let missing_flags = shapes.iter().map(Option::is_none).collect();
        let present = Self::from_shapes(shapes.into_iter().flatten().collect(), self.frame.clone());
        Ok(ValidityMask::from_vec(n, missing_flags).map_or_else(
            || present.clone(),
            |mask| Self::scatter_present_rows(&present, mask),
        ))
    }

    /// Whether a geometry (or missing row via ``None``) is present.
    ///
    /// Equality is structural (``Geometry.__eq__``): CRS, epoch, and exact
    /// geometry. ``None in arr`` is true when any row is missing.
    ///
    /// Returns
    /// -------
    /// bool
    pub fn __contains__(&self, py: Python<'_>, item: &Bound<'_, PyAny>) -> bool {
        if item.is_none() {
            let array = self.clone();
            return py.detach(move || array.has_missing());
        }
        let Some(geom) = exact_geometry(item) else {
            return false;
        };
        let same_frame = self.frame_matches(geom);
        let expected = Arc::clone(&geom.shape);
        let array = self.clone();
        py.detach(move || {
            same_frame
                && array
                    .present_shape_rows()
                    .any(|(_, shape)| shape.as_ref() == expected.shape())
        })
    }

    pub fn __hash__(&self) -> u64 {
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
                    row.row().with_shape(|shape| shape.hash(&mut hasher));
                },
            }
        }
        hasher.finish()
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
        let _ = write!(out, " len={}", self.len());
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
    fn fill_missing_with_shape(&self, _mask: &ValidityMask, fill: &Shape) -> Self {
        let shapes = self
            .row_operation()
            .rows_with_missing(|| ())
            .map(|row| match row {
                ArrayRow::Missing { .. } => fill.clone(),
                ArrayRow::Present(row) => row.row().with_shape(Clone::clone),
            })
            .collect();
        Self::from_shapes(shapes, self.frame.clone())
    }

    /// Dense fill from a row-aligned array: packed point×point stays columnar;
    /// otherwise collect shapes only (no `PyGeometry` staging).
    fn fill_missing_with_array(&self, _mask: &ValidityMask, fill: &Self) -> Self {
        let shapes = self
            .row_operation()
            .rows_with_missing(|| ())
            .zip(fill.row_operation().rows_with_missing(|| ()))
            .map(|(left, right)| match left {
                ArrayRow::Missing { .. } => match right {
                    ArrayRow::Present(shape) => shape.row().with_shape(Clone::clone),
                    ArrayRow::Missing { .. } => {
                        unreachable!("required fill rows were checked present")
                    },
                },
                ArrayRow::Present(shape) => shape.row().with_shape(Clone::clone),
            })
            .collect();
        Self::from_shapes(shapes, self.frame.clone())
    }
}
