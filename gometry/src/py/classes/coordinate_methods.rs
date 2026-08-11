//! Python method surfaces for the coordinate views (`Coordinates`).
//!
//! Extracted `#[pymethods]` blocks for `PyCoordinates`; reach the crate-root
//! coordinate storage and helpers via `use super::*`.
#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]

use pyo3::types::{PyAny, PyDict, PyList};

use crate::py::row::{RowContainer, RowIndexOrSlice, RowIterState, parse_row_index_or_slice};
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
/// a flattened vertex stream, not a row-aligned container; use
/// `get_coordinates(..., return_index=True)` when you need source-row indexes,
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
        let layout = axes.unwrap_or_else(|| self.layout.unwrap_or_else(|| self.view.axes()));
        let (order, n) = coordinate_axis_order(layout);
        let dims = n;
        let rows = self.view.len();
        let data = if let Some(seq) = self.view.single_seq()
            && seq.len() == rows
        {
            interleave_coordseq_ordered(seq, &order[..n], missing)
        } else {
            let mut data = Vec::with_capacity(rows * dims);
            self.view.for_each_point(|coord| {
                for &axis in &order[..n] {
                    data.push(coordinate_ordinate(coord.point, axis).unwrap_or(missing));
                }
            });
            data
        };
        crate::py::numpy::float64_matrix(py, data, rows, dims)
    }
}

/// View-owning cursor for lazy coordinate iteration: O(1) construction, O(1)
/// `next` via storage-shaped `point_at` when the owner supports it. Gathered
/// ragged / mixed / masked owners materialize once on first access so full
/// iteration stays linear (never walk-from-start per step).
struct CoordinateCursor {
    view: coordinates::CoordinateView,
    layout: Option<CoordinateAxes>,
    len: usize,
    /// `None` when storage-shaped random access is O(1)/O(log); otherwise a
    /// one-shot materialization shared by every `scalar_row` call.
    material: Option<std::sync::OnceLock<Vec<crate::geometry::Point>>>,
}

impl HeapSize for CoordinateCursor {
    fn heap_bytes(&self) -> usize {
        // The view shares owner Arcs; report the logical payload like the parent.
        let material_bytes = self
            .material
            .as_ref()
            .and_then(std::sync::OnceLock::get)
            .map_or(0, |points| {
                points.len() * std::mem::size_of::<crate::geometry::Point>()
            });
        self.view.logical_heap_bytes() + material_bytes
    }
}

impl RowContainer for CoordinateCursor {
    const LABEL: &'static str = "Coordinates";

    fn row_count(&self) -> usize {
        self.len
    }

    fn scalar_row(&self, py: Python<'_>, row: usize) -> PyResult<Py<PyAny>> {
        let point = self.material.as_ref().map_or_else(
            || {
                self.view
                    .point_at(row)
                    .expect("iterator index already range-checked")
                    .point
            },
            |lock| {
                let points = lock.get_or_init(|| {
                    let mut out = Vec::with_capacity(self.len);
                    self.view.for_each_point(|coord| out.push(coord.point));
                    out
                });
                points[row]
            },
        );
        coordinate_tuple(py, point, self.layout)
    }
}

/// Lazy iterator over a ``Coordinates`` view, yielding one coordinate tuple per
/// step. Holds a cursor into the view (O(1) construct, O(1) next on storage-
/// shaped owners) rather than materializing all points up front.
#[pyclass(
    name = "CoordinatesIterator",
    module = "gometry",
    frozen,
    immutable_type
)]
pub struct PyCoordinatesIter {
    source: CoordinateCursor,
    state: RowIterState,
}

impl PyCoordinatesIter {
    fn new(
        view: coordinates::CoordinateView,
        layout: Option<CoordinateAxes>,
        reverse: bool,
    ) -> Self {
        let len = view.len();
        // Reverse needs random access at the far end; materialize when the
        // storage path is not O(1) so reverse does not re-walk from the head.
        let material = if view.has_o1_random_access() {
            None
        } else {
            Some(std::sync::OnceLock::new())
        };
        let _ = reverse; // reverse still uses RowIterState over random indices
        Self {
            source: CoordinateCursor {
                view,
                layout,
                len,
                material,
            },
            state: RowIterState::new(reverse),
        }
    }
}

row_iter_pymethods! {
    impl PyCoordinatesIter {
        source: CoordinateCursor,
    }
}

mod helpers;
mod pymethods;
mod set_coords;
mod types;

pub(crate) use helpers::{
    column_axis_to_py, coordinate_axis_order, coordinate_ordinate, coordinate_tuple,
    get_coordinates, interleave_coordseq_ordered,
};
pub(crate) use set_coords::{
    map_coordinates_callback, parse_coordinate_replacement, replace_shape_coordinates,
    slice_replacement_for_shape,
};
