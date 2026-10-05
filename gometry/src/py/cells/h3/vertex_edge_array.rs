use std::sync::Arc;

use numpy::PyArrayMethods as _;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::PyAnyMethods as _;
use pyo3::types::{PyAny, PyTuple};
use pyo3::{
    Bound, IntoPyObject as _, Py, PyErr, PyRef, PyResult, Python, pyclass, pyfunction, pymethods,
};

use crate::HeapSize;
use crate::broadcast::py_bool_or_not_implemented;
use crate::collections::{HashMap, HashMapExt as _};
use crate::geometry::storage::{
    RowSelection, RowSelectionRef, physical_row, row_selection_from_logical_rows,
};
use crate::geometry::{CoordSeq, CoordSeqBuilder, CoordinateAxes, LineSeq};
use crate::py::cells::grid_kind::membership_id;
use crate::py::cells::h3::{
    DirectedEdgeIndex, LatLng, PyH3Edge, PyH3Vertex, VertexIndex, collect_h3_index_ids,
    h3_edge_index, h3_vertex_index, validate_h3_index_ids,
};
use crate::py::cells::{
    FactorizationError, GridKind, Point, PyCellArray, Shape, sort_value_counts,
};
use crate::py::numpy::float64_array;
use crate::py::row::{RowContainer, RowGetItemContainer, RowIterState, array_getitem};

fn hex_token(id: u64) -> String {
    format!("{id:x}")
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
    state: RowIterState,
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
    state: RowIterState,
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
            state: RowIterState::new(reverse),
        }
    }
}

impl PyH3EdgeArrayIter {
    const fn new(source: PyH3EdgeArray, reverse: bool) -> Self {
        Self {
            source,
            state: RowIterState::new(reverse),
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
                *self
                    .ids
                    .get(physical_row(self.selection_ref(), logical))
                    .expect("logical H3 index row maps to a stored id")
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

            fn slice(&self, start: isize, stop: isize, step: isize, count: usize) -> Self {
                if let Some(logical) = CoordSeq::contiguous_positive_slice(start, stop, step) {
                    let selection = row_selection_from_logical_rows(
                        self.selection_ref(),
                        self.ids.len(),
                        logical,
                    );
                    return self.with_selection(selection);
                }
                self.select_logical_rows(crate::py::row::slice_row_indices(start, step, count))
            }

            fn logical_contiguous_ids(&self) -> Option<&[u64]> {
                match self.selection_ref() {
                    RowSelectionRef::Identity => Some(&self.ids),
                    RowSelectionRef::Window { start, len } => {
                        self.ids.get(start..).and_then(|ids| ids.get(..len))
                    },
                    RowSelectionRef::Gather(_) => None,
                }
            }

            fn logical_ids_vec(&self) -> Vec<u64> {
                (0..self.len()).map(|logical| self.id_at(logical)).collect()
            }

            fn value_count_entries(
                &self,
            ) -> std::result::Result<(Vec<u64>, Vec<i64>), FactorizationError> {
                let mut slot_of: HashMap<u64, usize> = HashMap::with_capacity(self.len());
                let mut uniques = Vec::with_capacity(self.len());
                let mut counts = Vec::<i64>::with_capacity(self.len());
                for row in 0..self.len() {
                    let id = self.id_at(row);
                    crate::py::cells::record_first_seen_count!(slot_of, uniques, counts, id);
                }
                Ok(sort_value_counts(uniques, counts))
            }

            fn factorize_entries(
                &self,
            ) -> std::result::Result<(Vec<u64>, Vec<i64>), FactorizationError> {
                let mut slot_of: HashMap<u64, usize> = HashMap::with_capacity(self.len());
                let mut uniques = Vec::with_capacity(self.len());
                let mut codes = Vec::with_capacity(self.len());
                for row in 0..self.len() {
                    let id = self.id_at(row);
                    let slot = crate::py::cells::first_seen_slot!(slot_of, uniques, id);
                    let code = i64::try_from(slot).map_err(|_| FactorizationError::CodeOverflow)?;
                    codes.push(code);
                }
                Ok((uniques, codes))
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
                crate::heap_size::ARC_CONTROL_BYTES
                    .saturating_add(self.len().saturating_mul(size_of::<u64>()))
                    .saturating_add(self.selection.heap_bytes())
            }
        }

        impl RowGetItemContainer for $array {
            fn gather_rows(&self, rows: &[usize]) -> PyResult<Self> {
                Ok(self.select_logical_rows(rows.iter().copied()))
            }

            fn slice_rows(
                &self,
                start: isize,
                stop: isize,
                step: isize,
                count: usize,
            ) -> PyResult<Self> {
                Ok(self.slice(start, stop, step, count))
            }

            fn empty(&self) -> PyResult<Self> {
                Ok(Self {
                    ids: Arc::from([]),
                    selection: RowSelection::Identity,
                })
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
                self.total_size()
            }

            /// Logical id payload in bytes (``len * 8``).
            ///
            /// Returns
            /// -------
            /// int
            #[getter]
            fn nbytes(&self) -> usize {
                // ID and gather vector allocations bound the logical length.
                self.len().wrapping_mul(size_of::<u64>())
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
                let Some(needle) = membership_id(item, $parse)? else {
                    return Ok(false);
                };
                let needle = u64::from(needle);
                let array = self.clone();
                Ok(py.detach(move || (0..array.len()).any(|row| array.id_at(row) == needle)))
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
                let Some(needle) = membership_id(value, $parse)?.map(u64::from) else {
                    let value = value
                        .repr()
                        .and_then(|repr| repr.extract::<String>())
                        .unwrap_or_else(|_| $label.to_owned());
                    return Err(PyValueError::new_err(format!("{value} is not in array")));
                };
                let len = self.len();
                let clamp = |bound: i64| -> usize {
                    let resolved = if bound < 0 {
                        bound
                            .checked_add(i64::try_from(len).unwrap_or(i64::MAX))
                            .expect("negative index adjustment fits i64")
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
                let Some(needle) = membership_id(value, $parse)?.map(u64::from) else {
                    return Ok(0);
                };
                Ok((0..self.len())
                    .filter(|&row| self.id_at(row) == needle)
                    .count())
            }

            /// Unique values and counts, ordered by descending count, with
            /// first appearance breaking ties.
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
                let (values, sorted_counts) = match self.value_count_entries() {
                    Ok(values) => values,
                    Err(error) => {
                        return Err(error.into_py_err(
                            "H3 count overflows int64",
                            "H3 factor code overflows int64",
                        ));
                    },
                };
                let values = match Self::from_ids(values).into_pyobject(py) {
                    Ok(values) => values.into_any().unbind(),
                    Err(error) => {
                        drop(sorted_counts);
                        return Err(error);
                    },
                };
                let counts = match crate::py::numpy::int64_array(py, sorted_counts) {
                    Ok(counts) => counts,
                    Err(error) => {
                        drop(values);
                        return Err(error);
                    },
                };
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
                let (uniques, codes) = match self.factorize_entries() {
                    Ok(values) => values,
                    Err(error) => {
                        return Err(error.into_py_err(
                            "H3 count overflows int64",
                            "H3 factor code overflows int64",
                        ));
                    },
                };
                let codes = match crate::py::numpy::int64_array(py, codes) {
                    Ok(codes) => codes,
                    Err(error) => {
                        drop(uniques);
                        return Err(error);
                    },
                };
                let values = match Self::from_ids(uniques).into_pyobject(py) {
                    Ok(values) => values.into_any().unbind(),
                    Err(error) => {
                        drop(codes);
                        return Err(error);
                    },
                };
                Ok(PyTuple::new(py, [codes, values])?.into_any().unbind())
            }

            fn __reduce__(&self, py: Python<'_>) -> PyResult<(Py<PyAny>, (Vec<u64>,))> {
                let callable = crate::gometry_lib_module(py)?
                    .getattr(pyo3::intern!(py, $unpickle))?
                    .unbind();
                Ok((callable, (self.logical_ids_vec(),)))
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
                let mut tokens = Vec::with_capacity(self.len());
                for id in self.logical_ids_vec() {
                    tokens.push(hex_token(id));
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
                crate::py::numpy::uint64_array(owner.py(), borrowed.logical_ids_vec())
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
                            let mut values = Vec::with_capacity(borrowed.len());
                            for row in 0..borrowed.len() {
                                values.push(
                                    borrowed
                                        .scalar_at(row)
                                        .into_pyobject(py)
                                        .map(|value| value.into_any().unbind())?,
                                );
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
                    let ids = slf.borrow().logical_ids_vec();
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
        unpickle: "_unpickle_h3_vertex_array_v1",
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
        unpickle: "_unpickle_h3_edge_array_v1",
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
        let ids = self.logical_ids_vec();
        let points = py.detach(move || {
            ids.into_iter()
                .map(|id| {
                    let vertex = VertexIndex::try_from(id).expect("ids validated at construction");
                    let latlng = LatLng::from(vertex);
                    crate::geometry::XY {
                        x: latlng.lng(),
                        y: latlng.lat(),
                    }
                })
                .collect::<Vec<_>>()
        });
        let mut builder = CoordSeqBuilder::with_capacity(CoordinateAxes::XY, points.len());
        for point in points {
            builder.push(crate::geometry::Point::new_raw_xy(point.x, point.y));
        }
        let coords = builder.finish()?;
        Ok(crate::PyGeometryArray::packed_points(
            coords,
            crate::Frame::Crs(crate::wgs84_crs()),
        )?)
    }
}

#[expect(
    clippy::used_underscore_items,
    reason = "the leading underscore is the Python convention for a module-private helper, and pickle stores these names in its stream, so the name is a wire contract rather than a Rust style choice"
)]
/// Rebuild a pickled `H3VertexArray` from its 64-bit indexes (internal).
#[pyfunction]
pub(super) fn _unpickle_h3_vertex_array_v1(ids: &Bound<'_, PyAny>) -> PyResult<PyH3VertexArray> {
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
    fn origin(&self) -> PyCellArray {
        let ids = self
            .logical_ids_vec()
            .into_iter()
            .map(|id| {
                let edge = DirectedEdgeIndex::try_from(id).expect("ids validated at construction");
                u64::from(edge.origin())
            })
            .collect();
        PyCellArray::from_ids(GridKind::H3Cell, ids)
    }

    /// The cells these directed edges enter.
    ///
    /// Returns
    /// -------
    /// CellArray
    ///     One destination H3Cell per edge.
    #[getter]
    fn destination(&self) -> PyCellArray {
        let ids = self
            .logical_ids_vec()
            .into_iter()
            .map(|id| {
                let edge = DirectedEdgeIndex::try_from(id).expect("ids validated at construction");
                u64::from(edge.destination())
            })
            .collect();
        PyCellArray::from_ids(GridKind::H3Cell, ids)
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
    fn reverse(&self) -> Self {
        let ids = self
            .logical_ids_vec()
            .into_iter()
            .map(|id| {
                let edge = DirectedEdgeIndex::try_from(id).expect("ids validated at construction");
                u64::from(edge.reverse())
            })
            .collect();
        Self::from_ids(ids)
    }

    /// Edge linework along each shared cell boundary.
    ///
    /// Returns
    /// -------
    /// GeometryArray
    ///     One ``LineString`` (lon/lat, ``OGC:CRS84``) per edge.
    #[getter]
    fn line(&self, py: Python<'_>) -> PyResult<crate::PyGeometryArray> {
        let ids = self.logical_ids_vec();
        let shapes = py.detach(move || {
            let mut shapes = Vec::with_capacity(ids.len());
            for id in ids {
                let edge = DirectedEdgeIndex::try_from(id).expect("ids validated at construction");
                let boundary = edge.boundary();
                let points: Vec<_> = boundary
                    .iter()
                    .map(|latlng| Point::new_raw_xy(latlng.lng(), latlng.lat()))
                    .collect();
                let shape =
                    Shape::LineString(LineSeq::from_proven(CoordSeq::try_from_points(&points)?));
                shapes.push(crate::geometry::publish_stored_computed_shape(
                    &shape,
                    crate::geographic::GeographicFrame::crs84(),
                    "H3 edge",
                )?);
            }
            Ok::<_, PyErr>(shapes)
        })?;
        Ok(crate::PyGeometryArray::from_shapes(
            shapes,
            crate::Frame::Crs(crate::wgs84_crs()),
        )?)
    }

    /// Length of every edge in meters (spherical, like `H3Cell.area`).
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    ///     One ``float64`` length per edge.
    #[getter]
    fn length(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let ids = self.logical_ids_vec();
        let values = py.detach(move || {
            ids.into_iter()
                .map(|id| {
                    DirectedEdgeIndex::try_from(id)
                        .expect("ids validated at construction")
                        .length_m()
                })
                .collect()
        });
        float64_array(py, values)
    }
}

#[expect(
    clippy::used_underscore_items,
    reason = "the leading underscore is the Python convention for a module-private helper, and pickle stores these names in its stream, so the name is a wire contract rather than a Rust style choice"
)]
/// Rebuild a pickled `H3EdgeArray` from its 64-bit indexes (internal).
#[pyfunction]
pub(super) fn _unpickle_h3_edge_array_v1(ids: &Bound<'_, PyAny>) -> PyResult<PyH3EdgeArray> {
    let ids = crate::collect_u64_sequence(ids, "H3 edge array pickle ids")?;
    validate_h3_index_ids::<DirectedEdgeIndex>(&ids)?;
    Ok(PyH3EdgeArray::from_ids(ids))
}
