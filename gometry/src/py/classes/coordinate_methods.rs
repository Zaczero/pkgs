//! Python method surfaces for the coordinate views (`Coordinates`).
//!
//! Extracted `#[pymethods]` blocks for `PyCoordinates`; reach the crate-root
//! coordinate storage and helpers via `use super::*`.
use pyo3::types::{PyAny, PyDict, PyList};

use crate::py::numpy::float64_matrix;
use crate::py::row::{RowIndexOrSlice, parse_row_index_or_slice};
use crate::{
    Arc, Bound, CoordSeq, CoordinateAxes, CoordinateAxis, GeometryArrayStorage, GeometryError,
    HeapSize, Point, Py, PyAnyMethods, PyDictMethods, PyErr, PyGeometryArray, PyRef, PyResult,
    PyTupleMethods, Python, Shape, coordinates, coordinates_object, point_tuple, py_bool, pyclass,
    pymethods,
};

/// Flat, indexable coordinate sequence behind `geom.coords`:
/// coordinates flattened depth-first across parts/rings, with per-axis columns.
///
/// Random access (`coords[i]`) is storage-shaped O(1)/O(log runs) on packed
/// columns and single-run shapes; iteration is a view-owning cursor (O(1)
/// construct, O(1) next) rather than an eager materialization of every vertex.
///
/// For `GeometryArray.coords`, missing rows contribute no vertices. The view is
/// a flattened vertex stream, not a row-aligned container; use `.row_index`
/// when you need source-row indexes,
/// or call `drop_missing()` first for an explicit dense-only path.
#[pyclass(
    name = "Coordinates",
    module = "gometry",
    frozen,
    immutable_type,
    sequence
)]
pub struct PyCoordinates {
    pub(crate) view: coordinates::CoordinateView,
    /// `None` renders each coordinate in its own axes (native); `Some` forces a
    /// fixed rectangular layout, padding absent Z/M with `None` (via `select`).
    pub(crate) layout: Option<CoordinateAxes>,
}

pub(crate) use crate::array::{CoordinateReplacement, ReplacementAxis};

impl PyCoordinates {
    pub(crate) fn to_numpy_internal(
        &self,
        py: Python<'_>,
        axes: Option<CoordinateAxes>,
        missing: f64,
    ) -> PyResult<Py<PyAny>> {
        let layout = match axes.or(self.layout) {
            Some(layout) => layout,
            None => self.view.axes_for_python(py)?,
        };
        let (_, dims) = coordinate_axis_order(layout);
        let (rows, data) = coordinate_matrix_data(py, &self.view, layout, missing)?;
        float64_matrix(py, data, rows, dims)
    }
}

/// View-owning cursor for lazy coordinate iteration. The point stream advances
/// packed rows and nested shapes directly without flattening the view first.
struct CoordinateCursor {
    points: std::sync::Mutex<coordinates::CoordinatePointCursor>,
    layout: Option<CoordinateAxes>,
    remaining_hint: std::sync::atomic::AtomicUsize,
}

impl HeapSize for CoordinateCursor {
    fn heap_bytes(&self) -> usize {
        self.points
            .lock()
            .expect("coordinate iterator cursor mutex is not poisoned")
            .logical_heap_bytes()
    }
}

/// Lazy iterator over a ``Coordinates`` view. A traversal stack retains the
/// current shape or packed row and yields tuples without materializing all
/// points up front.
#[pyclass(
    name = "CoordinatesIterator",
    module = "gometry",
    frozen,
    immutable_type
)]
pub struct PyCoordinatesIter {
    source: CoordinateCursor,
}

impl PyCoordinatesIter {
    fn new(
        py: Python<'_>,
        view: coordinates::CoordinateView,
        layout: Option<CoordinateAxes>,
        reverse: bool,
    ) -> PyResult<Self> {
        let hint = view.exact_measure(py)?.length_hint();
        Ok(Self {
            source: CoordinateCursor {
                points: std::sync::Mutex::new(view.point_cursor(reverse)),
                layout,
                remaining_hint: std::sync::atomic::AtomicUsize::new(hint),
            },
        })
    }
}

#[pymethods]
impl PyCoordinatesIter {
    const fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    /// Advisory remaining count, capped at Python's largest sequence length.
    fn __length_hint__(&self) -> usize {
        self.source
            .remaining_hint
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// ``sys.getsizeof`` support for the iterator and its retained view.
    fn __sizeof__(&self) -> usize {
        std::mem::size_of::<Self>().saturating_add(self.source.heap_bytes())
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        let point = self
            .source
            .points
            .lock()
            .expect("coordinate iterator cursor mutex is not poisoned")
            .next_point();
        let Some(point) = point else {
            return Ok(None);
        };
        self.source
            .remaining_hint
            .try_update(
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
                |hint| Some(hint.saturating_sub(1)),
            )
            .expect("coordinate iterator length hint update is infallible");
        coordinate_tuple(py, point, self.source.layout).map(Some)
    }
}

mod helpers;
mod pymethods;
mod set_coords;
mod types;

pub(crate) use helpers::{
    column_axis_to_py, coordinate_axis_order, coordinate_matrix_data, coordinate_tuple,
    get_coordinates,
};
pub(crate) use set_coords::{
    map_coordinates_callback, parse_coordinate_replacement, replace_shape_coordinates,
    slice_replacement_for_shape,
};
