#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::collections::hash_map::Entry;
use std::sync::Arc;

use numpy::PyArrayMethods as _;
use pyo3::exceptions::{PyMemoryError, PyOverflowError, PyTypeError, PyValueError};
use pyo3::prelude::PyAnyMethods as _;
use pyo3::types::{PyAny, PyBool, PyInt, PyTuple};
use pyo3::{
    Bound, IntoPyObject as _, Py, PyErr, PyRef, PyResult, Python, pyclass, pyfunction, pymethods,
};

use crate::HeapSize;
use crate::array::{RowSelection, RowSelectionRef, physical_row, row_selection_from_logical_rows};
use crate::broadcast::py_bool_or_not_implemented;
use crate::collections::{HashMap, HashMapExt as _};
use crate::geometry::{CoordSeq, CoordSeqBuilder, CoordinateAxes, LineSeq};
use crate::py::cells::grid_kind::is_custom_index_provider;
use crate::py::cells::h3::{
    DirectedEdgeIndex, LatLng, PyH3Edge, PyH3Vertex, VertexIndex, collect_h3_index_ids,
    h3_edge_index, h3_vertex_index, validate_h3_index_ids,
};
use crate::py::cells::{GridKind, Point, PyCellArray, Shape};
use crate::py::row::{RowContainer, RowGetItemContainer, array_getitem};

fn try_hex_token(id: u64) -> PyResult<String> {
    use std::fmt::Write as _;
    let mut token = String::new();
    token
        .try_reserve(16)
        .map_err(|_| PyMemoryError::new_err("failed to allocate H3 token"))?;
    write!(&mut token, "{id:x}").expect("writing into a reserved string cannot fail");
    Ok(token)
}

/// An immutable array of H3 topological vertex ids.
///
/// Index with an integer for an `H3Vertex`; slice or mask for another
/// `H3VertexArray`. The id column is exposed as read-only ``uint64`` data.
#[pyclass(
    name = "H3VertexArray",
    module = "gometry",
    frozen,
    immutable_type,
    sequence,
    weakref,
    skip_from_py_object
)]
#[derive(Clone, Debug)]
pub(crate) struct PyH3VertexArray {
    ids: Arc<[u64]>,
    selection: RowSelection,
}

/// Lazy iterator over an `H3VertexArray`'s vertices.
#[pyclass(
    name = "H3VertexArrayIterator",
    module = "gometry",
    frozen,
    immutable_type
)]
pub(super) struct PyH3VertexArrayIter {
    source: PyH3VertexArray,
    state: crate::py::row::RowIterState,
}

/// An immutable array of H3 directed-edge ids.
///
/// Index with an integer for an `H3Edge`; slice or mask for another
/// `H3EdgeArray`. The id column is exposed as read-only ``uint64`` data.
#[pyclass(
    name = "H3EdgeArray",
    module = "gometry",
    frozen,
    immutable_type,
    sequence,
    weakref,
    skip_from_py_object
)]
#[derive(Clone, Debug)]
pub(crate) struct PyH3EdgeArray {
    ids: Arc<[u64]>,
    selection: RowSelection,
}

/// Lazy iterator over an `H3EdgeArray`'s edges.
#[pyclass(
    name = "H3EdgeArrayIterator",
    module = "gometry",
    frozen,
    immutable_type
)]
pub(super) struct PyH3EdgeArrayIter {
    source: PyH3EdgeArray,
    state: crate::py::row::RowIterState,
}

impl PyH3VertexArray {
    pub(super) fn from_ids(ids: Vec<u64>) -> Self {
        Self {
            ids: Arc::from(ids),
            selection: RowSelection::Identity,
        }
    }
}

impl PyH3EdgeArray {
    pub(super) fn from_ids(ids: Vec<u64>) -> Self {
        Self {
            ids: Arc::from(ids),
            selection: RowSelection::Identity,
        }
    }
}

impl PyH3VertexArrayIter {
    const fn new(source: PyH3VertexArray, reverse: bool) -> Self {
        Self {
            source,
            state: crate::py::row::RowIterState::new(reverse),
        }
    }
}

impl PyH3EdgeArrayIter {
    const fn new(source: PyH3EdgeArray, reverse: bool) -> Self {
        Self {
            source,
            state: crate::py::row::RowIterState::new(reverse),
        }
    }
}

row_iter_pymethods! {
    impl PyH3VertexArrayIter {
        source: PyH3VertexArray,
    }
}

row_iter_pymethods! {
    impl PyH3EdgeArrayIter {
        source: PyH3EdgeArray,
    }
}

fn collect_h3_vertex_ids(ids: &Bound<'_, PyAny>) -> PyResult<Vec<u64>> {
    collect_h3_index_ids::<VertexIndex>(ids, h3_vertex_index)
}

fn collect_h3_edge_ids(ids: &Bound<'_, PyAny>) -> PyResult<Vec<u64>> {
    collect_h3_index_ids::<DirectedEdgeIndex>(ids, h3_edge_index)
}

macro_rules! h3_index_array_common {
    (
        impl $array:ident {
            iter: $iter:ident,
            scalar: $scalar:ident,
            label: $label:literal,
            class_name: $class_name:literal,
            collect: $collect:path,
            parse: $parse:path,
            unpickle: $unpickle:literal,
            build_scalar: |$id:ident| $build_scalar:expr $(,)?
        }
    ) => {
        impl $array {
            fn selection_ref(&self) -> RowSelectionRef<'_> {
                self.selection.as_deref()
            }

            pub(super) fn len(&self) -> usize {
                self.selection_ref().len(self.ids.len())
            }

            fn id_at(&self, logical: usize) -> u64 {
                self.ids[physical_row(self.selection_ref(), logical)]
            }

            fn scalar_at(&self, logical: usize) -> $scalar {
                let $id = self.id_at(logical);
                $build_scalar
            }

            fn with_selection(&self, selection: RowSelection) -> Self {
                Self {
                    ids: Arc::clone(&self.ids),
                    selection,
                }
            }

            fn select_logical_rows(&self, rows: impl IntoIterator<Item = usize>) -> Self {
                self.with_selection(row_selection_from_logical_rows(
                    self.selection_ref(),
                    self.ids.len(),
                    rows,
                ))
            }

            fn slice(&self, start: isize, stop: isize, step: isize) -> Self {
                if let Some(logical) = CoordSeq::contiguous_positive_slice(start, stop, step) {
                    let len = logical.end - logical.start;
                    let selection = match self.selection_ref() {
                        RowSelectionRef::Identity => RowSelection::window_checked(
                            logical.start,
                            len,
                            self.ids.len(),
                            "cell array",
                        )
                        .expect("normalized cell slice is in bounds"),
                        RowSelectionRef::Window {
                            start: base,
                            len: base_len,
                        } if logical.start <= base_len && logical.end <= base_len => {
                            RowSelection::window_checked(
                                base + logical.start,
                                len,
                                self.ids.len(),
                                "cell array",
                            )
                            .expect("normalized cell slice is in bounds")
                        },
                        map => row_selection_from_logical_rows(map, self.ids.len(), logical),
                    };
                    return self.with_selection(selection);
                }
                let mut rows = Vec::new();
                let mut i = start;
                while (step > 0 && i < stop) || (step < 0 && i > stop) {
                    rows.push(i as usize);
                    i += step;
                }
                self.select_logical_rows(rows)
            }

            fn logical_contiguous_ids(&self) -> Option<&[u64]> {
                match self.selection_ref() {
                    RowSelectionRef::Identity => Some(&self.ids),
                    RowSelectionRef::Window { start, len } => self.ids.get(start..start + len),
                    RowSelectionRef::Gather(_) => None,
                }
            }

            fn logical_ids_vec(&self) -> PyResult<Vec<u64>> {
                let mut ids = crate::try_vec_with_capacity(self.len())?;
                for logical in 0..self.len() {
                    crate::try_push(&mut ids, self.id_at(logical))?;
                }
                Ok(ids)
            }

            fn factorized_entries(&self) -> PyResult<(Vec<u64>, Vec<i64>, Vec<i64>)> {
                let mut slot_of: HashMap<u64, usize> = HashMap::new();
                slot_of
                    .try_reserve(self.len())
                    .map_err(|_| PyMemoryError::new_err("failed to allocate H3 factorization"))?;
                let mut uniques = Vec::new();
                let mut counts = Vec::new();
                let mut codes = crate::try_vec_with_capacity(self.len())?;
                for row in 0..self.len() {
                    let id = self.id_at(row);
                    let slot = match slot_of.entry(id) {
                        Entry::Occupied(entry) => *entry.get(),
                        Entry::Vacant(entry) => {
                            let slot = uniques.len();
                            entry.insert(slot);
                            crate::try_push(&mut uniques, id)?;
                            crate::try_push(&mut counts, 0_i64)?;
                            slot
                        },
                    };
                    counts[slot] = counts[slot]
                        .checked_add(1)
                        .ok_or_else(|| PyOverflowError::new_err("H3 count overflows int64"))?;
                    crate::try_push(
                        &mut codes,
                        i64::try_from(slot).map_err(|_| {
                            PyOverflowError::new_err("H3 factor code overflows int64")
                        })?,
                    )?;
                }
                Ok((uniques, counts, codes))
            }
        }

        impl RowContainer for $array {
            const LABEL: &'static str = $class_name;
            const INDEX_LABEL: &'static str = $label;

            fn row_count(&self) -> usize {
                self.len()
            }

            fn scalar_row(&self, py: Python<'_>, row: usize) -> PyResult<Py<PyAny>> {
                Ok(self.scalar_at(row).into_pyobject(py)?.into_any().unbind())
            }
        }

        impl HeapSize for $array {
            fn heap_bytes(&self) -> usize {
                self.len() * size_of::<u64>() + self.selection.heap_bytes()
            }
        }

        impl RowGetItemContainer for $array {
            fn gather_rows(&self, rows: &[usize]) -> Self {
                self.select_logical_rows(rows.iter().copied())
            }

            fn slice_rows(&self, start: isize, stop: isize, step: isize) -> Self {
                self.slice(start, stop, step)
            }

            fn empty(&self) -> Self {
                Self {
                    ids: Arc::from([]),
                    selection: RowSelection::Identity,
                }
            }

            fn container_to_py(py: Python<'_>, value: Self) -> PyResult<Py<PyAny>> {
                Ok(value.into_pyobject(py)?.into_any().unbind())
            }
        }

        #[pymethods]
        impl $array {
            #[classattr]
            #[expect(non_upper_case_globals, reason = "Python dunder name")]
            const __array_ufunc__: Option<Py<PyAny>> = None;

            #[new]
            fn new(values: &Bound<'_, PyAny>) -> PyResult<Self> {
                Ok(Self {
                    ids: Arc::from($collect(values)?),
                    selection: RowSelection::Identity,
                })
            }

            fn __sizeof__(&self) -> usize {
                std::mem::size_of::<Self>() + self.nbytes() + self.selection.heap_bytes()
            }

            /// Logical id payload in bytes (``len * 8``).
            ///
            /// Returns
            /// -------
            /// int
            #[getter]
            fn nbytes(&self) -> usize {
                self.len() * size_of::<u64>()
            }

            /// Number of edges or vertices.
            ///
            /// Returns
            /// -------
            /// int
            fn __len__(&self) -> usize {
                self.len()
            }

            /// ``False`` only when the array is empty.
            ///
            /// Returns
            /// -------
            /// bool
            fn __bool__(&self) -> bool {
                self.len() != 0
            }

            /// Select by integer, slice, or fancy index.
            ///
            /// An ``int`` returns one edge/vertex. A ``slice`` or fancy index
            /// returns an array of the same type.
            ///
            /// Returns
            /// -------
            /// H3Edge or H3Vertex or H3EdgeArray or H3VertexArray
            fn __getitem__(&self, py: Python<'_>, index: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
                array_getitem(self, py, index)
            }

            /// Iterate elements in row order.
            ///
            /// Returns
            /// -------
            /// iterator
            fn __iter__(&self) -> $iter {
                $iter::new(self.clone(), false)
            }

            /// Iterate elements in reverse row order.
            ///
            /// Returns
            /// -------
            /// iterator
            fn __reversed__(&self) -> $iter {
                $iter::new(self.clone(), true)
            }

            fn __eq__(&self, py: Python<'_>, other: &Bound<'_, PyAny>) -> Py<PyAny> {
                py_bool_or_not_implemented(py, other.extract::<PyRef<Self>>().ok(), |other| {
                    self.len() == other.len()
                        && (0..self.len()).all(|row| self.id_at(row) == other.id_at(row))
                })
            }

            fn __hash__(&self) -> u64 {
                use std::hash::{Hash, Hasher};
                let mut hasher = crate::collections::python_hasher();
                $class_name.hash(&mut hasher);
                self.len().hash(&mut hasher);
                for row in 0..self.len() {
                    self.id_at(row).hash(&mut hasher);
                }
                hasher.finish()
            }

            /// Whether an edge/vertex id appears in the array.
            ///
            /// Returns
            /// -------
            /// bool
            fn __contains__(
                &self,
                py: Python<'_>,
                item: &Bound<'_, PyAny>,
            ) -> PyResult<bool> {
                let custom_index = is_custom_index_provider(item)?;
                match $parse(item) {
                    Ok(needle) => {
                        let needle = u64::from(needle);
                        let array = self.clone();
                        Ok(py.detach(move || {
                            (0..array.len()).any(|row| array.id_at(row) == needle)
                        }))
                    },
                    Err(error)
                        if item.cast_exact::<PyBool>().is_ok()
                            || (error.is_instance_of::<crate::py::errors::GeometryError>(item.py())
                                && !(item.cast_exact::<PyBool>().is_err()
                                    && item.cast::<PyInt>().is_err()
                                    && item
                                        .getattr_opt(pyo3::intern!(item.py(), "__index__"))?
                                        .is_some()))
                            || (error.is_instance_of::<PyTypeError>(item.py())
                                && !custom_index) =>
                    {
                        Ok(false)
                    },
                    Err(error) => Err(error),
                }
            }

            /// First index of an equal id in ``[start, stop)``.
            ///
            /// Parameters
            /// ----------
            /// value : H3 value or int id
            ///     The element to locate.
            /// start : int, default 0
            ///     First position searched.
            /// stop : int, optional
            ///     One past the last position searched.
            ///
            /// Returns
            /// -------
            /// int
            #[pyo3(signature = (value, start = 0, stop = None), text_signature = "($self, value, start=0, stop=None)")]
            fn index(
                &self,
                value: &Bound<'_, PyAny>,
                #[pyo3(from_py_with = crate::strict_i64)] start: i64,
                #[pyo3(from_py_with = crate::strict_optional_i64)] stop: Option<i64>,
            ) -> PyResult<usize> {
                let custom_index = is_custom_index_provider(value)?;
                let needle = $parse(value).map(u64::from).map_err(|error| {
                    if error.is_instance_of::<crate::py::errors::GeometryError>(value.py())
                        || (error.is_instance_of::<PyTypeError>(value.py()) && !custom_index)
                    {
                        let value = value
                            .repr()
                            .and_then(|repr| repr.extract::<String>())
                            .unwrap_or_else(|_| "value".to_owned());
                        PyValueError::new_err(format!("{value} is not in array"))
                    } else {
                        error
                    }
                })?;
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
                if start < stop {
                    for row in start..stop {
                        if self.id_at(row) == needle {
                            return Ok(row);
                        }
                    }
                }
                let value = value
                    .repr()
                    .and_then(|repr| repr.extract::<String>())
                    .unwrap_or_else(|_| $label.to_owned());
                Err(PyValueError::new_err(format!("{value} is not in array")))
            }

            /// Number of elements with the same id.
            ///
            /// Parameters
            /// ----------
            /// value : H3 value or int id
            ///     The element to count.
            ///
            /// Returns
            /// -------
            /// int
            fn count(&self, value: &Bound<'_, PyAny>) -> PyResult<usize> {
                let custom_index = is_custom_index_provider(value)?;
                let needle = match $parse(value) {
                    Ok(needle) => u64::from(needle),
                    Err(error) if custom_index => return Err(error),
                    Err(_) => return Ok(0),
                };
                Ok((0..self.len())
                    .filter(|&row| self.id_at(row) == needle)
                    .count())
            }

            /// Unique values and counts, ordered by descending count.
            ///
            /// Returns
            /// -------
            /// tuple
            ///     ``(unique_values, counts)`` with read-only ``int64`` counts.
                ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7)[0]
    /// >>> assert cell is not None
    /// >>> edges = cell.edges
    /// >>> unique, counts = edges.value_counts()
    /// >>> counts.tolist()
    /// [1, 1, 1, 1, 1, 1]
fn value_counts(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
                let (uniques, counts, _) = self.factorized_entries()?;
                let mut order = crate::try_vec_with_capacity(uniques.len())?;
                for slot in 0..uniques.len() {
                    crate::try_push(&mut order, slot)?;
                }
                order.sort_by(|&left, &right| {
                    counts[right]
                        .cmp(&counts[left])
                        .then_with(|| left.cmp(&right))
                });
                let mut sorted_uniques = crate::try_vec_with_capacity(order.len())?;
                let mut sorted_counts = crate::try_vec_with_capacity(order.len())?;
                for &slot in &order {
                    crate::try_push(&mut sorted_uniques, uniques[slot])?;
                    crate::try_push(&mut sorted_counts, counts[slot])?;
                }
                let values = Self::from_ids(sorted_uniques)
                    .into_pyobject(py)?
                    .into_any()
                    .unbind();
                let counts = crate::py::numpy::int64_array(py, sorted_counts)?;
                Ok(PyTuple::new(py, [values, counts])?.into_any().unbind())
            }

            /// Factorize values into dense integer codes and first-seen uniques.
            ///
            /// Returns
            /// -------
            /// tuple
            ///     ``(codes, unique_values)``.
                ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7)[0]
    /// >>> assert cell is not None
    /// >>> edges = cell.edges
    /// >>> codes, unique = edges.factorize()
    /// >>> len(codes) == len(edges)
    /// True
fn factorize(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
                let (uniques, _, codes) = self.factorized_entries()?;
                let codes = crate::py::numpy::int64_array(py, codes)?;
                let values = Self::from_ids(uniques)
                    .into_pyobject(py)?
                    .into_any()
                    .unbind();
                Ok(PyTuple::new(py, [codes, values])?.into_any().unbind())
            }

            fn __reduce__(&self, py: Python<'_>) -> PyResult<(Py<PyAny>, (Vec<u64>,))> {
                let callable = crate::gometry_lib_module(py)?
                    .getattr(pyo3::intern!(py, $unpickle))?
                    .unbind();
                Ok((callable, (self.logical_ids_vec()?,)))
            }

            fn __repr__(&self) -> String {
                format!("<{} len={}>", $class_name, self.len())
            }

            /// Hexadecimal token of every row.
            ///
            /// Returns
            /// -------
            /// list of str
            #[getter]
            fn token(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
                use pyo3::IntoPyObjectExt;
                let mut tokens = crate::try_vec_with_capacity(self.len())?;
                for id in self.logical_ids_vec()? {
                    crate::try_push(&mut tokens, try_hex_token(id)?)?;
                }
                tokens.into_py_any(py)
            }

            /// Return a read-only ``uint64`` ndarray view of the id column.
            ///
            /// Contiguous selections are zero-copy; gathered selections
            /// materialize the logical id order.
            ///
            /// Returns
            /// -------
            /// numpy.ndarray
                ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7)[0]
    /// >>> assert cell is not None
    /// >>> edges = cell.edges
    /// >>> type(edges.to_numpy()).__name__
    /// 'ndarray'
fn to_numpy(slf: Bound<'_, Self>) -> PyResult<Py<PyAny>> {
                let owner = slf.clone().into_any();
                let borrowed = slf.borrow();
                if let Some(values) = borrowed.logical_contiguous_ids() {
                    // SAFETY: `values` is tied to `slf`, and `owner` pins that object as
                    // the array base.
                    return unsafe { crate::py::numpy::uint64_slice_array(owner, values) };
                }
                crate::py::numpy::uint64_array(owner.py(), borrowed.logical_ids_vec()?)
            }

            /// NumPy array protocol.
            ///
            /// ``dtype=None`` / ``uint64`` exports the raw validated ids;
            /// ``dtype=object`` exports typed H3 values, matching iteration.
            ///
            /// Parameters
            /// ----------
            /// dtype : uint64 or object, optional
            /// copy : bool, optional
            ///     ``False`` requires a zero-copy id export; gathered ids,
            ///     opposite-endian numeric ids, and ``dtype=object`` raise
            ///     because they must materialize.
            ///
            /// Returns
            /// -------
            /// numpy.ndarray
            #[pyo3(signature = (dtype=None, copy=None))]
            fn __array__(
                slf: Bound<'_, Self>,
                dtype: Option<&Bound<'_, PyAny>>,
                copy: Option<bool>,
            ) -> PyResult<Py<PyAny>> {
                let py = slf.py();
                let mut requested_dtype = None;
                let mut native = true;
                if let Some(dtype) = dtype
                    && !dtype.is_none()
                {
                    let numpy = crate::py::numpy::numpy_module(py)?;
                    let dtype = numpy.getattr("dtype")?.call1((dtype,))?;
                    let kind = dtype.getattr("kind")?.extract::<String>()?;
                    let itemsize = dtype.getattr("itemsize")?.extract::<usize>()?;
                    if kind == "O" {
                        if copy == Some(false) {
                            return Err(crate::GeometryError::new_err(
                                "gometry cannot return the requested array without copying",
                            ));
                        }
                        let values = {
                            let borrowed = slf.borrow();
                            let mut values = crate::try_vec_with_capacity(borrowed.len())?;
                            for row in 0..borrowed.len() {
                                crate::try_push(
                                    &mut values,
                                    borrowed
                                        .scalar_at(row)
                                        .into_pyobject(py)
                                        .map(|value| value.into_any().unbind())?,
                                )?;
                            }
                            values
                        };
                        let array = numpy::PyArray1::from_owned_object_array(
                            py,
                            numpy::ndarray::Array1::from_vec(values),
                        );
                        array.try_readwrite()?.make_nonwriteable();
                        return Ok(array.into_any().unbind());
                    }
                    if kind != "u" || itemsize != size_of::<u64>() {
                        return Err(crate::GeometryError::new_err(
                            "dtype must be uint64, object, or None",
                        ));
                    }
                    native = dtype.getattr("isnative")?.extract::<bool>()?;
                    requested_dtype = Some(dtype);
                }
                if !native && copy == Some(false) {
                    return Err(crate::GeometryError::new_err(
                        "gometry cannot return the requested array without copying",
                    ));
                }
                let array = if copy == Some(true) {
                    let ids = slf.borrow().logical_ids_vec()?;
                    crate::py::numpy::uint64_array(py, ids)?
                } else {
                    if copy == Some(false) {
                        let can_borrow = slf.borrow().logical_contiguous_ids().is_some();
                        if !can_borrow {
                            return Err(crate::GeometryError::new_err(
                                "gometry cannot return the requested array without copying",
                            ));
                        }
                    }
                    Self::to_numpy(slf)?
                };
                if !native {
                    let dtype = requested_dtype.expect("non-native ids require a requested dtype");
                    return crate::py::numpy::astype_readonly(array.bind(py), &dtype);
                }
                Ok(array)
            }

            fn __copy__(slf: &Bound<'_, Self>) -> Py<Self> {
                slf.clone().unbind()
            }

            #[pyo3(signature = (memo))]
            fn __deepcopy__(
                slf: &Bound<'_, Self>,
                memo: &Bound<'_, PyAny>,
            ) -> Py<Self> {
                let _ = memo;
                slf.clone().unbind()
            }
        }
    };
}

h3_index_array_common! {
    impl PyH3VertexArray {
        iter: PyH3VertexArrayIter,
        scalar: PyH3Vertex,
        label: "H3 vertex",
        class_name: "H3VertexArray",
        collect: collect_h3_vertex_ids,
        parse: h3_vertex_index,
        unpickle: "_unpickle_h3_vertex_array",
        build_scalar: |id| PyH3Vertex {
            vertex: VertexIndex::try_from(id).expect("ids validated at construction"),
        },
    }
}

h3_index_array_common! {
    impl PyH3EdgeArray {
        iter: PyH3EdgeArrayIter,
        scalar: PyH3Edge,
        label: "H3 edge",
        class_name: "H3EdgeArray",
        collect: collect_h3_edge_ids,
        parse: h3_edge_index,
        unpickle: "_unpickle_h3_edge_array",
        build_scalar: |id| PyH3Edge {
            edge: DirectedEdgeIndex::try_from(id).expect("ids validated at construction"),
        },
    }
}

#[pymethods]
impl PyH3VertexArray {
    /// The location of every vertex.
    ///
    /// Returns
    /// -------
    /// GeometryArray
    ///     One ``Point`` (lon/lat, ``OGC:CRS84``) per vertex.
    #[getter]
    fn point(&self, py: Python<'_>) -> PyResult<crate::PyGeometryArray> {
        let ids = self.logical_ids_vec()?;
        let points = py.detach(move || {
            let mut points = crate::try_vec_with_capacity(ids.len())?;
            for id in ids {
                let vertex = VertexIndex::try_from(id).expect("ids validated at construction");
                let latlng = LatLng::from(vertex);
                crate::try_push(&mut points, crate::geometry::XY {
                    x: latlng.lng(),
                    y: latlng.lat(),
                })?;
            }
            Ok::<_, PyErr>(points)
        })?;
        let mut builder = CoordSeqBuilder::try_with_capacity(CoordinateAxes::XY, points.len())?;
        for point in points {
            builder.push(crate::geometry::Point::new_raw_xy(point.x, point.y));
        }
        let coords = builder.finish()?;
        Ok(crate::PyGeometryArray::packed_points(
            coords,
            crate::Frame::Crs(crate::wgs84_crs()),
        ))
    }
}

/// Rebuild a pickled `H3VertexArray` from its 64-bit indexes (internal).
#[pyfunction]
pub(super) fn _unpickle_h3_vertex_array(ids: &Bound<'_, PyAny>) -> PyResult<PyH3VertexArray> {
    let ids = crate::collect_u64_sequence(ids, "H3 vertex array pickle ids")?;
    validate_h3_index_ids::<VertexIndex>(&ids)?;
    Ok(PyH3VertexArray::from_ids(ids))
}

#[pymethods]
impl PyH3EdgeArray {
    /// The cells these directed edges leave.
    ///
    /// Returns
    /// -------
    /// CellArray
    ///     One origin H3Cell per edge.
    #[getter]
    fn origin(&self) -> PyResult<PyCellArray> {
        let mut ids = crate::try_vec_with_capacity(self.len())?;
        for id in self.logical_ids_vec()? {
            let edge = DirectedEdgeIndex::try_from(id).expect("ids validated at construction");
            crate::try_push(&mut ids, u64::from(edge.origin()))?;
        }
        Ok(PyCellArray::from_ids(GridKind::H3Cell, ids))
    }

    /// The cells these directed edges enter.
    ///
    /// Returns
    /// -------
    /// CellArray
    ///     One destination H3Cell per edge.
    #[getter]
    fn destination(&self) -> PyResult<PyCellArray> {
        let mut ids = crate::try_vec_with_capacity(self.len())?;
        for id in self.logical_ids_vec()? {
            let edge = DirectedEdgeIndex::try_from(id).expect("ids validated at construction");
            crate::try_push(&mut ids, u64::from(edge.destination()))?;
        }
        Ok(PyCellArray::from_ids(GridKind::H3Cell, ids))
    }

    /// Reverse every directed edge from its destination back to its origin.
    ///
    /// Returns
    /// -------
    /// H3EdgeArray
    ///
    /// Examples
    /// --------
    /// >>> import gometry as gm
    /// >>> cell = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7)[0]
    /// >>> assert cell is not None
    /// >>> edges = cell.edges
    /// >>> edges.reverse()[0].token
    /// '1672830829ffffff'
    fn reverse(&self) -> PyResult<Self> {
        let mut ids = crate::try_vec_with_capacity(self.len())?;
        for id in self.logical_ids_vec()? {
            let edge = DirectedEdgeIndex::try_from(id).expect("ids validated at construction");
            crate::try_push(&mut ids, u64::from(edge.reverse()))?;
        }
        Ok(Self::from_ids(ids))
    }

    /// Edge linework along each shared cell boundary.
    ///
    /// Returns
    /// -------
    /// GeometryArray
    ///     One ``LineString`` (lon/lat, ``OGC:CRS84``) per edge.
    #[getter]
    fn line(&self, py: Python<'_>) -> PyResult<crate::PyGeometryArray> {
        let ids = self.logical_ids_vec()?;
        let shapes = py.detach(move || {
            let mut shapes = crate::try_vec_with_capacity(ids.len())?;
            for id in ids {
                let edge = DirectedEdgeIndex::try_from(id).expect("ids validated at construction");
                let boundary = edge.boundary();
                let mut points = crate::try_vec_with_capacity(boundary.len())?;
                for latlng in boundary.iter() {
                    crate::try_push(&mut points, Point::new_raw_xy(latlng.lng(), latlng.lat()))?;
                }
                crate::try_push(
                    &mut shapes,
                    Shape::LineString(
                        LineSeq::try_new(CoordSeq::try_from_points(&points)?)
                            .expect("H3 edge boundary is lineal"),
                    ),
                )?;
            }
            Ok::<_, PyErr>(shapes)
        })?;
        Ok(crate::PyGeometryArray::from_shapes(
            shapes,
            crate::Frame::Crs(crate::wgs84_crs()),
        ))
    }

    /// Length of every edge in meters (spherical, like `H3Cell.area`).
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    ///     One ``float64`` length per edge.
    #[getter]
    fn length(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let ids = self.logical_ids_vec()?;
        let values = py.detach(move || {
            let mut values = crate::try_vec_with_capacity(ids.len())?;
            for id in ids {
                let edge = DirectedEdgeIndex::try_from(id).expect("ids validated at construction");
                crate::try_push(&mut values, edge.length_m())?;
            }
            Ok::<_, PyErr>(values)
        })?;
        crate::py::numpy::float64_array(py, values)
    }
}

/// Rebuild a pickled `H3EdgeArray` from its 64-bit indexes (internal).
#[pyfunction]
pub(super) fn _unpickle_h3_edge_array(ids: &Bound<'_, PyAny>) -> PyResult<PyH3EdgeArray> {
    let ids = crate::collect_u64_sequence(ids, "H3 edge array pickle ids")?;
    validate_h3_index_ids::<DirectedEdgeIndex>(&ids)?;
    Ok(PyH3EdgeArray::from_ids(ids))
}
