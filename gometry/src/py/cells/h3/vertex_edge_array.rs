#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::collections::hash_map::Entry;
use std::sync::Arc;

use numpy::PyArrayMethods;
use pyo3::exceptions::PyValueError;
use pyo3::types::{PyAny, PyAnyMethods, PyTuple};
use pyo3::{Bound, IntoPyObject, Py, PyRef, PyResult, Python, pyclass, pyfunction, pymethods};

use super::*;
use crate::HeapSize;
use crate::array::{RowSelection, RowSelectionRef, physical_row, row_selection_from_logical_rows};
use crate::broadcast::py_bool_or_not_implemented;
use crate::collections::{HashMap, HashMapExt};
use crate::geometry::{CoordSeq, LineSeq};
use crate::py::cells::*;
use crate::py::row::{RowContainer, RowGetItemContainer, array_getitem};

/// An immutable array of H3 topological vertex ids.
///
/// Index with an integer for an `H3Vertex`; slice or mask for another
/// `H3VertexArray`. The id column is exposed as read-only ``uint64`` data.
#[pyclass(
    name = "H3VertexArray",
    module = "gometry",
    frozen,
    sequence,
    weakref,
    skip_from_py_object
)]
#[derive(Clone, Debug)]
pub(super) struct PyH3VertexArray {
    ids: Arc<[u64]>,
    selection: RowSelection,
}

/// Lazy iterator over an `H3VertexArray`'s vertices.
#[pyclass(name = "H3VertexArrayIterator", module = "gometry", frozen)]
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
    sequence,
    weakref,
    skip_from_py_object
)]
#[derive(Clone, Debug)]
pub(super) struct PyH3EdgeArray {
    ids: Arc<[u64]>,
    selection: RowSelection,
}

/// Lazy iterator over an `H3EdgeArray`'s edges.
#[pyclass(name = "H3EdgeArrayIterator", module = "gometry", frozen)]
pub(super) struct PyH3EdgeArrayIter {
    source: PyH3EdgeArray,
    state: crate::py::row::RowIterState,
}

impl PyH3VertexArray {
    pub(super) fn from_trusted_ids(ids: Vec<u64>) -> Self {
        Self {
            ids: Arc::from(ids),
            selection: RowSelection::Identity,
        }
    }
}

impl PyH3EdgeArray {
    pub(super) fn from_trusted_ids(ids: Vec<u64>) -> Self {
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
                        RowSelectionRef::Identity => RowSelection::window(logical.start, len),
                        RowSelectionRef::Window {
                            start: base,
                            len: base_len,
                        } if logical.start <= base_len && logical.end <= base_len => {
                            RowSelection::window(base + logical.start, len)
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

            fn logical_ids_vec(&self) -> Vec<u64> {
                (0..self.len()).map(|logical| self.id_at(logical)).collect()
            }

            fn factorized_entries(&self) -> (Vec<u64>, Vec<i64>, Vec<i64>) {
                let mut slot_of: HashMap<u64, usize> = HashMap::with_capacity(self.len());
                let mut uniques = Vec::new();
                let mut counts = Vec::new();
                let mut codes = Vec::with_capacity(self.len());
                for row in 0..self.len() {
                    let id = self.id_at(row);
                    let slot = match slot_of.entry(id) {
                        Entry::Occupied(entry) => *entry.get(),
                        Entry::Vacant(entry) => {
                            let slot = uniques.len();
                            entry.insert(slot);
                            uniques.push(id);
                            counts.push(0_i64);
                            slot
                        },
                    };
                    counts[slot] += 1;
                    codes.push(slot as i64);
                }
                (uniques, counts, codes)
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
            fn __contains__(&self, item: &Bound<'_, PyAny>) -> bool {
                $parse(item).is_ok_and(|needle| {
                    let needle = u64::from(needle);
                    (0..self.len()).any(|row| self.id_at(row) == needle)
                })
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
            fn index(&self, value: &Bound<'_, PyAny>, start: i64, stop: Option<i64>) -> PyResult<usize> {
                let needle = $parse(value).map(u64::from).map_err(|_| {
                    let value = value
                        .repr()
                        .and_then(|repr| repr.extract::<String>())
                        .unwrap_or_else(|_| "value".to_owned());
                    PyValueError::new_err(format!("{value} is not in array"))
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
            fn count(&self, value: &Bound<'_, PyAny>) -> usize {
                $parse(value).map(u64::from).map_or(0, |needle| {
                    (0..self.len())
                        .filter(|&row| self.id_at(row) == needle)
                        .count()
                })
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
    /// >>> edges = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7).cells[0].edges
    /// >>> unique, counts = edges.value_counts()
    /// >>> counts.tolist()
    /// [1, 1, 1, 1, 1, 1]
fn value_counts(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
                let (uniques, counts, _) = self.factorized_entries();
                let mut order: Vec<usize> = (0..uniques.len()).collect();
                order.sort_by(|&left, &right| {
                    counts[right]
                        .cmp(&counts[left])
                        .then_with(|| left.cmp(&right))
                });
                let sorted_uniques = order.iter().map(|&slot| uniques[slot]).collect();
                let sorted_counts = order.iter().map(|&slot| counts[slot]).collect();
                let values = Self::from_trusted_ids(sorted_uniques)
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
    /// >>> edges = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7).cells[0].edges
    /// >>> codes, unique = edges.factorize()
    /// >>> len(codes) == len(edges)
    /// True
fn factorize(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
                let (uniques, _, codes) = self.factorized_entries();
                let codes = crate::py::numpy::int64_array(py, codes)?;
                let values = Self::from_trusted_ids(uniques)
                    .into_pyobject(py)?
                    .into_any()
                    .unbind();
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

            /// Return a read-only ``uint64`` ndarray view of the id column.
            ///
            /// Returns
            /// -------
            /// numpy.ndarray
            #[getter]
            fn values(slf: Bound<'_, Self>) -> PyResult<Py<PyAny>> {
                Self::to_numpy(slf)
            }

            /// Hexadecimal token of every row.
            ///
            /// Returns
            /// -------
            /// list of str
            #[getter]
            fn token(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
                use pyo3::IntoPyObjectExt;
                let tokens: Vec<String> = self
                    .logical_ids_vec()
                    .into_iter()
                    .map(|id| format!("{id:x}"))
                    .collect();
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
    /// >>> edges = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7).cells[0].edges
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
            ///     ``False`` requires a zero-copy id export; gathered ids and
            ///     ``dtype=object`` raise because they must materialize.
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
                            (0..borrowed.len())
                                .map(|row| {
                                    borrowed
                                        .scalar_at(row)
                                        .into_pyobject(py)
                                        .map(|value| value.into_any().unbind())
                                })
                                .collect::<PyResult<Vec<_>>>()?
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
                }
                if copy == Some(true) {
                    let ids = slf.borrow().logical_ids_vec();
                    return crate::py::numpy::uint64_array(py, ids);
                }
                if copy == Some(false) {
                    let can_borrow = slf.borrow().logical_contiguous_ids().is_some();
                    if !can_borrow {
                        return Err(crate::GeometryError::new_err(
                            "gometry cannot return the requested array without copying",
                        ));
                    }
                }
                Self::to_numpy(slf)
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
    ///     One ``Point`` (lon/lat, ``EPSG:4326``) per vertex.
    #[getter]
    fn point(&self) -> crate::PyGeometryArray {
        let points: Vec<crate::geometry::XY> = self
            .logical_ids_vec()
            .into_iter()
            .map(|id| {
                let vertex = VertexIndex::try_from(id).expect("ids validated at construction");
                let latlng = LatLng::from(vertex);
                crate::geometry::XY {
                    x: latlng.lng(),
                    y: latlng.lat(),
                }
            })
            .collect();
        let coords = CoordSeq::from_xy(&points);
        crate::PyGeometryArray::packed_points(
            coords,
            crate::Frame::Crs(crate::crs_arc_static("EPSG:4326")),
        )
    }
}

/// Rebuild a pickled `H3VertexArray` from its 64-bit indexes (internal).
#[pyfunction]
pub(super) fn _unpickle_h3_vertex_array(ids: &Bound<'_, PyAny>) -> PyResult<PyH3VertexArray> {
    let ids = crate::collect_u64_sequence(ids, "H3 vertex array pickle ids")?;
    validate_h3_index_ids::<VertexIndex>(&ids)?;
    Ok(PyH3VertexArray::from_trusted_ids(ids))
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
        PyCellArray::from_trusted_ids(GridKind::H3Cell, ids)
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
        PyCellArray::from_trusted_ids(GridKind::H3Cell, ids)
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
    /// >>> edges = gm.h3_cover(gm.Point(-122.4194, 37.7749, crs=4326), resolution=7).cells[0].edges
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
        Self::from_trusted_ids(ids)
    }

    /// Edge linework along each shared cell boundary.
    ///
    /// Returns
    /// -------
    /// GeometryArray
    ///     One ``LineString`` (lon/lat, ``EPSG:4326``) per edge.
    #[getter]
    fn line(&self) -> crate::PyGeometryArray {
        let shapes = self
            .logical_ids_vec()
            .into_iter()
            .map(|id| {
                let edge = DirectedEdgeIndex::try_from(id).expect("ids validated at construction");
                let points: Vec<Point> = edge
                    .boundary()
                    .iter()
                    .map(|latlng| Point::new_unchecked_xy(latlng.lng(), latlng.lat()))
                    .collect();
                Shape::LineString(
                    LineSeq::try_new(CoordSeq::from(points)).expect("H3 edge boundary is lineal"),
                )
            })
            .collect();
        crate::PyGeometryArray::from_shapes(
            shapes,
            crate::Frame::Crs(crate::crs_arc_static("EPSG:4326")),
        )
    }

    /// Length of every edge in meters (spherical, like `H3Cell.area`).
    ///
    /// Returns
    /// -------
    /// numpy.ndarray
    ///     One ``float64`` length per edge.
    #[getter]
    fn length(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let values = self
            .logical_ids_vec()
            .into_iter()
            .map(|id| {
                let edge = DirectedEdgeIndex::try_from(id).expect("ids validated at construction");
                edge.length_m()
            })
            .collect();
        crate::py::numpy::float64_array(py, values)
    }
}

/// Rebuild a pickled `H3EdgeArray` from its 64-bit indexes (internal).
#[pyfunction]
pub(super) fn _unpickle_h3_edge_array(ids: &Bound<'_, PyAny>) -> PyResult<PyH3EdgeArray> {
    let ids = crate::collect_u64_sequence(ids, "H3 edge array pickle ids")?;
    validate_h3_index_ids::<DirectedEdgeIndex>(&ids)?;
    Ok(PyH3EdgeArray::from_trusted_ids(ids))
}
