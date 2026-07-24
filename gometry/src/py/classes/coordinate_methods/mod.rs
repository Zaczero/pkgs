#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! Python method surfaces for the coordinate views (`Coordinates`).
//!
//! Extracted `#[pymethods]` blocks for `PyCoordinates`; reach the crate-root
//! coordinate storage and helpers via `use super::*`.

use std::sync::Arc;

use pyo3::types::{PyAny, PyDict, PyList};

use crate::py::row::{RowContainer, RowIndexOrSlice, RowIterState, parse_row_index_or_slice};
use crate::*;

/// Flat, indexable coordinate sequence behind `geom.coords`:
/// coordinates flattened depth-first across parts/rings, with per-axis columns.
///
/// For `GeometryArray.coords`, missing rows contribute no vertices. The view is
/// a flattened vertex stream, not a row-aligned container; use
/// `get_coordinates(..., return_index=True)` when you need source-row indexes,
/// or call `drop_missing()` first for an explicit dense-only path.
#[pyclass(name = "Coordinates", module = "gometry", frozen, sequence)]
pub struct PyCoordinates {
    pub(crate) view: coordinates::CoordinateView,
    /// `None` renders each coordinate in its own axes (native); `Some` forces a
    /// fixed rectangular layout, padding absent Z/M with `None` (via `select`).
    pub(crate) layout: Option<CoordinateAxes>,
}

#[derive(Clone)]
pub(crate) enum ReplacementAxis {
    Replace(Arc<[f64]>),
    Carry,
}

#[derive(Clone)]
pub(crate) struct CoordinateReplacement {
    pub(crate) xs: Arc<[f64]>,
    pub(crate) ys: Arc<[f64]>,
    pub(crate) zs: ReplacementAxis,
    pub(crate) ms: ReplacementAxis,
    pub(crate) len: usize,
    pub(crate) axes: CoordinateAxes,
    pub(crate) positional: bool,
}

impl CoordinateReplacement {
    /// Positional matrices use the geometry's *union* axes (NaN-padded for
    /// members that lack Z/M). Each independent sequence keeps its own axes:
    /// ignore padded columns rather than fabricating or rejecting them.
    const fn ignore_union_padding_for_missing_axis(&self, member_has_axis: bool) -> bool {
        self.positional && !member_has_axis
    }

    fn axis_column_for_seq(
        &self,
        axis: &ReplacementAxis,
        member_has_axis: bool,
        carried: Option<Arc<[f64]>>,
        range: std::ops::Range<usize>,
    ) -> PyResult<Option<Arc<[f64]>>> {
        match axis {
            ReplacementAxis::Replace(values) => {
                if self.ignore_union_padding_for_missing_axis(member_has_axis) {
                    return Ok(None);
                }
                if !member_has_axis {
                    return Err(crate::py::errors::InvalidGeometryError::new_err(
                        "coordinates must preserve each coordinate sequence axes",
                    ));
                }
                Ok(Some(Arc::from(&values[range])))
            },
            ReplacementAxis::Carry => Ok(carried),
        }
    }

    pub(crate) fn z_column_for_seq(
        &self,
        seq: &CoordSeq,
        range: std::ops::Range<usize>,
    ) -> PyResult<Option<Arc<[f64]>>> {
        self.axis_column_for_seq(&self.zs, seq.axes().has_z(), seq.carried_zs(), range)
    }

    pub(crate) fn m_column_for_seq(
        &self,
        seq: &CoordSeq,
        range: std::ops::Range<usize>,
    ) -> PyResult<Option<Arc<[f64]>>> {
        self.axis_column_for_seq(&self.ms, seq.axes().has_m(), seq.carried_ms(), range)
    }

    fn axis_at(
        &self,
        axis: &ReplacementAxis,
        member_has_axis: bool,
        carried: Option<f64>,
        index: usize,
    ) -> PyResult<Option<f64>> {
        match axis {
            ReplacementAxis::Replace(values) => {
                if self.ignore_union_padding_for_missing_axis(member_has_axis) {
                    return Ok(None);
                }
                if !member_has_axis {
                    return Err(crate::py::errors::InvalidGeometryError::new_err(
                        "coordinates must preserve each coordinate sequence axes",
                    ));
                }
                Ok(Some(values[index]))
            },
            ReplacementAxis::Carry => Ok(carried),
        }
    }

    pub(crate) fn z_at(&self, old: Point, index: usize) -> PyResult<Option<f64>> {
        self.axis_at(&self.zs, old.z().is_some(), old.z(), index)
    }

    pub(crate) fn m_at(&self, old: Point, index: usize) -> PyResult<Option<f64>> {
        self.axis_at(&self.ms, old.m().is_some(), old.m(), index)
    }
}

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

struct CoordinateRows {
    points: Vec<Point>,
    layout: Option<CoordinateAxes>,
}

impl HeapSize for CoordinateRows {
    fn heap_bytes(&self) -> usize {
        self.points.heap_bytes()
    }
}

impl RowContainer for CoordinateRows {
    const LABEL: &'static str = "Coordinates";

    fn row_count(&self) -> usize {
        self.points.len()
    }

    fn scalar_row(&self, py: Python<'_>, row: usize) -> PyResult<Py<PyAny>> {
        coordinate_tuple(py, self.points[row], self.layout)
    }
}

/// Lazy iterator over a ``Coordinates`` view, yielding one coordinate tuple per
/// step.
#[pyclass(name = "CoordinatesIterator", module = "gometry", frozen)]
pub struct PyCoordinatesIter {
    /// Flattened points, collected in one view traversal at construction —
    /// `point_at` walks the whole view per call, which would make iteration
    /// quadratic.
    source: CoordinateRows,
    state: RowIterState,
}

impl PyCoordinatesIter {
    const fn new(points: Vec<Point>, layout: Option<CoordinateAxes>, reverse: bool) -> Self {
        Self {
            source: CoordinateRows { points, layout },
            state: RowIterState::new(reverse),
        }
    }
}

row_iter_pymethods! {
    impl PyCoordinatesIter {
        source: CoordinateRows,
    }
}

mod helpers;
mod pymethods;
mod set_coords;
mod types;

pub(crate) use helpers::*;
pub(crate) use set_coords::*;
