//! Retained circle solutions and their scalar/column projections.

use std::ops::Range;
use std::sync::Arc;

use pyo3::IntoPyObjectExt as _;
use pyo3::prelude::*;

use crate::broadcast::BulkElement;
use crate::crs::frame::Frame;
use crate::crs::local::FramedCircle;
use crate::geometry::Shape;
use crate::geometry::storage::ValidityMask;
use crate::py::numpy::{bool_array, float64_array};
use crate::py::row::{RowIndexOrSlice, RowIterState, parse_row_index_or_slice, slice_row_indices};
use crate::{PyGeometry, Typed};

#[derive(Clone)]
pub(crate) struct CircleOutput(pub(crate) Option<Arc<FramedCircle>>);

impl BulkElement for CircleOutput {
    fn into_py(self, py: Python<'_>, frame: &Frame) -> PyResult<Py<PyAny>> {
        match self.0 {
            None => Ok(py.None()),
            Some(disk) => PyCircle {
                disk,
                frame: frame.clone(),
            }
            .into_py_any(py),
        }
    }

    fn bulk_into_py(values: Vec<Self>, py: Python<'_>, frame: &Frame) -> PyResult<Py<PyAny>> {
        PyCircleArray {
            rows: 0..values.len(),
            values: values.into(),
            frame: frame.clone(),
        }
        .into_py_any(py)
    }

    fn missing_value() -> Self {
        Self(None)
    }
}

/// A solved circle with a source-CRS center and a radius in ``unit``.
///
/// ``working_crs`` identifies the Cartesian frame in which the disk is solved.
/// Geographic metric results with one or two distinct point sites retain an
/// ellipsoidal solution; other geographic circles use a local projection.
/// ``to_polygon`` realizes the retained disk without repeating its optimization;
/// polygon resolution does not change its center or radius.
#[pyclass(name = "Circle", module = "gometry", frozen, immutable_type)]
#[derive(Clone)]
pub(crate) struct PyCircle {
    disk: Arc<FramedCircle>,
    frame: Frame,
}

#[pymethods]
impl PyCircle {
    fn __eq__(&self, other: &Bound<'_, PyAny>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let Ok(other) = other.cast::<Self>() else {
            return Ok(py.NotImplemented());
        };
        let other = other.get();
        let equal = self.frame == other.frame
            && (Arc::ptr_eq(&self.disk, &other.disk)
                || py.detach(|| self.disk.same_disk(&other.disk))?);
        equal.into_py_any(py)
    }

    /// Circle center in the input CRS, retaining its coordinate epoch.
    #[getter]
    fn center(&self, py: Python<'_>) -> PyResult<Typed> {
        Ok(Typed(PyGeometry::with_frame(
            py.detach(|| self.disk.center())?,
            self.frame.clone(),
        )))
    }

    /// Radius in the selected metric units; NaN for an empty circle.
    #[getter]
    fn radius(&self, py: Python<'_>) -> PyResult<f64> {
        Ok(py.detach(|| self.disk.radius())?)
    }

    /// ``meters`` for SI radii, or ``planar`` for native coordinate units.
    #[getter]
    fn unit(&self) -> &'static str {
        self.disk.unit()
    }

    /// Cartesian working CRS, or None for ellipsoidal, CRS-free, or empty disks.
    #[getter]
    fn working_crs(&self) -> Option<&str> {
        self.disk.working_crs()
    }

    /// Realize the retained disk as a polygon, or a Point for a degenerate disk.
    ///
    /// In a Cartesian working frame, inscribed polygons lie inside their
    /// certified disk and bounding polygons enclose their exact disk.
    /// Ellipsoidal one-/two-point disks use the same local-projection
    /// approximation as geographic Point.buffer. Geographic output is in the source CRS.
    /// Empty circles return POLYGON EMPTY. Finite numeric solutions can exist
    /// even when polygon coordinates cannot be represented, in which case this
    /// method raises OverflowError without invalidating the numerical solution.
    ///
    /// Parameters
    /// ----------
    /// quadrant_segments : int, default 16
    ///     Positive number of straight segments per quarter-circle.
    ///
    /// Returns
    /// -------
    /// Polygon or Point
    ///     Polygonal realization in the source CRS and epoch; a Point for a
    ///     degenerate disk and POLYGON EMPTY for an empty disk.
    ///
    /// Raises
    /// ------
    /// GeometryError
    ///     If the segment count is not positive or cannot be represented.
    /// OverflowError
    ///     If the certified polygon cannot be represented with finite coordinates.
    /// CRSError
    ///     If a geographic polygon cannot fit the local projection's accuracy domain.
    ///     Ellipsoidal center and radius remain available for widely separated sites.
    #[pyo3(signature = (*, quadrant_segments=16))]
    fn to_polygon(&self, py: Python<'_>, quadrant_segments: i64) -> PyResult<Typed> {
        let segments = crate::validate_quadrant_segments(quadrant_segments)?;
        let shape = py.detach(|| self.disk.to_polygon(segments))?;
        Ok(Typed(PyGeometry::with_frame(shape, self.frame.clone())))
    }

    fn __repr__(&self, py: Python<'_>) -> PyResult<String> {
        let center = self
            .center(py)?
            .into_pyobject(py)?
            .repr()?
            .to_str()?
            .to_owned();
        Ok(format!(
            "Circle(center={center}, radius={:?}, unit={:?})",
            self.radius(py)?,
            self.unit()
        ))
    }

    fn __copy__<'py>(slf: &Bound<'py, Self>) -> Bound<'py, Self> {
        slf.clone()
    }

    fn __deepcopy__<'py>(slf: &Bound<'py, Self>, memo: &Bound<'py, PyAny>) -> Bound<'py, Self> {
        let _ = memo;
        slf.clone()
    }
}

/// An immutable sequence of solved circles with null-aware vector projections.
///
/// ``center`` returns a point GeometryArray; ``radius`` returns a read-only
/// NumPy array with NaN for empty or missing rows. ``is_missing`` distinguishes
/// missing rows from empty circles. Indexing returns Circle or None; slicing
/// retains the circle solutions without repeating their optimization.
#[pyclass(name = "CircleArray", module = "gometry", frozen, sequence)]
#[cfg_attr(not(Py_3_14), pyo3(immutable_type))]
#[derive(Clone)]
pub(crate) struct PyCircleArray {
    values: Arc<[CircleOutput]>,
    rows: Range<usize>,
    frame: Frame,
}

impl PyCircleArray {
    fn row_matches(
        &self,
        value: &CircleOutput,
        needle: Option<&PyCircle>,
    ) -> crate::error::Result<bool> {
        match (&value.0, needle) {
            (None, None) => Ok(true),
            (Some(disk), Some(needle)) => Ok(self.frame == needle.frame
                && (Arc::ptr_eq(disk, &needle.disk) || disk.same_disk(&needle.disk)?)),
            (None, Some(_)) | (Some(_), None) => Ok(false),
        }
    }

    fn find_row(
        &self,
        needle: Option<&PyCircle>,
        rows: Range<usize>,
    ) -> crate::error::Result<Option<usize>> {
        for row in rows {
            if self.row_matches(
                self.values().get(row).expect("search rows are bounded"),
                needle,
            )? {
                return Ok(Some(row));
            }
        }
        Ok(None)
    }

    fn values(&self) -> &[CircleOutput] {
        self.values
            .get(self.rows.clone())
            .expect("circle rows belong to their owner")
    }

    fn shapes(
        &self,
        py: Python<'_>,
        kernel: impl Fn(&FramedCircle) -> crate::error::Result<Shape> + pyo3::marker::Ungil,
    ) -> PyResult<Py<PyAny>> {
        let shapes = py.detach(|| {
            self.values()
                .iter()
                .enumerate()
                .map(|(row, value)| {
                    value
                        .0
                        .as_ref()
                        .map_or(Ok(Shape::empty_point()), |disk| kernel(disk))
                        .map_err(|error| crate::rows_err((row, error)))
                })
                .collect::<PyResult<Vec<_>>>()
        })?;
        let missing = ValidityMask::from_missing_iter(
            self.rows.len(),
            self.values().iter().map(|row| row.0.is_none()),
        );
        Shape::bulk_into_py_masked(shapes, py, &self.frame, missing.as_ref())
    }
}

#[pymethods]
impl PyCircleArray {
    fn __contains__(&self, py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<bool> {
        let needle = if value.is_none() {
            None
        } else if let Ok(circle) = value.cast::<PyCircle>() {
            Some(circle.get())
        } else {
            return Ok(false);
        };
        Ok(py
            .detach(|| self.find_row(needle, 0..self.rows.len()))?
            .is_some())
    }

    /// Number of rows equal to a Circle, or missing rows when value is None.
    fn count(&self, py: Python<'_>, value: &Bound<'_, PyAny>) -> PyResult<usize> {
        let needle = if value.is_none() {
            None
        } else if let Ok(circle) = value.cast::<PyCircle>() {
            Some(circle.get())
        } else {
            return Ok(0);
        };
        Ok(py.detach(|| {
            let mut count = 0_usize;
            for value in self.values() {
                if self.row_matches(value, needle)? {
                    count = count.wrapping_add(1);
                }
            }
            Ok::<_, crate::error::Error>(count)
        })?)
    }

    /// First matching row within start:stop, with normal negative-index bounds.
    ///
    /// Raises
    /// ------
    /// ValueError
    ///     If the selected window contains no equal Circle or missing row.
    #[pyo3(signature = (value, start=0, stop=None))]
    fn index(
        &self,
        py: Python<'_>,
        value: &Bound<'_, PyAny>,
        #[pyo3(from_py_with = crate::strict_i64)] start: i64,
        #[pyo3(from_py_with = crate::strict_optional_i64)] stop: Option<i64>,
    ) -> PyResult<usize> {
        let not_found = || pyo3::exceptions::PyValueError::new_err("value is not in CircleArray");
        let needle = if value.is_none() {
            None
        } else if let Ok(circle) = value.cast::<PyCircle>() {
            Some(circle.get())
        } else {
            return Err(not_found());
        };
        let len = i64::try_from(self.rows.len()).expect("circle length fits in i64");
        let clamp = |bound: i64| {
            let bound = if bound < 0 {
                bound.saturating_add(len)
            } else {
                bound
            };
            usize::try_from(bound.max(0))
                .expect("nonnegative index bound")
                .min(self.rows.len())
        };
        let start = clamp(start);
        let stop = stop.map_or(self.rows.len(), clamp);
        py.detach(|| self.find_row(needle, start..stop))?
            .ok_or_else(not_found)
    }

    fn __eq__(&self, other: &Bound<'_, PyAny>, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let Ok(other) = other.cast::<Self>() else {
            return Ok(py.NotImplemented());
        };
        let other = other.get();
        let equal = self.frame == other.frame
            && self.rows.len() == other.rows.len()
            && py.detach(|| {
                for (left, right) in self.values().iter().zip(other.values()) {
                    match (&left.0, &right.0) {
                        (None, None) => {},
                        (Some(left), Some(right)) => {
                            if !Arc::ptr_eq(left, right) && !left.same_disk(right)? {
                                return Ok::<_, crate::error::Error>(false);
                            }
                        },
                        (None, Some(_)) | (Some(_), None) => return Ok(false),
                    }
                }
                Ok(true)
            })?;
        equal.into_py_any(py)
    }

    /// Circle centers as a point GeometryArray in the source CRS and epoch.
    /// Empty circles have empty Points; missing rows remain missing.
    #[getter]
    fn center(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        self.shapes(py, FramedCircle::center)
    }

    /// Read-only float64 radii in each circle's selected units.
    /// Empty and missing rows are NaN.
    #[getter]
    fn radius(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let values = py.detach(|| {
            self.values()
                .iter()
                .enumerate()
                .map(|(row, value)| {
                    value
                        .0
                        .as_ref()
                        .map_or(Ok(f64::NAN), |disk| disk.radius())
                        .map_err(|error| crate::rows_err((row, error)))
                })
                .collect::<PyResult<Vec<_>>>()
        })?;
        float64_array(py, values)
    }

    /// Read-only boolean mask distinguishing missing rows from empty circles.
    #[getter]
    fn is_missing(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        bool_array(
            py,
            self.values()
                .iter()
                .map(|value| value.0.is_none())
                .collect(),
        )
    }

    /// Effective radius units per row: meters or planar native coordinate units.
    /// Missing rows are None.
    #[getter]
    fn unit(&self) -> Vec<Option<&'static str>> {
        self.values()
            .iter()
            .map(|value| value.0.as_ref().map(|disk| disk.unit()))
            .collect()
    }

    /// Working CRS definitions per row, or None where absent.
    #[getter]
    fn working_crs(&self) -> Vec<Option<&str>> {
        self.values()
            .iter()
            .map(|value| value.0.as_ref().and_then(|disk| disk.working_crs()))
            .collect()
    }

    /// Realize each retained disk without repeating its optimization.
    ///
    /// Parameters
    /// ----------
    /// quadrant_segments : int, default 16
    ///     Positive number of straight segments per quarter-circle, shared by
    ///     every row. Resolution does not change the solved centers or radii.
    ///
    /// Returns
    /// -------
    /// GeometryArray
    ///     Polygon or degenerate Point per row, in the source CRS and epoch.
    ///     Empty disks yield empty polygons; missing rows remain missing.
    ///
    /// Raises
    /// ------
    /// GeometryError
    ///     If the segment count is not positive or cannot be represented.
    /// OverflowError
    ///     If a certified polygon cannot be represented with finite coordinates.
    /// CRSError
    ///     If a geographic realization exceeds the local-projection accuracy domain.
    #[pyo3(signature = (*, quadrant_segments=16))]
    fn to_polygon(&self, py: Python<'_>, quadrant_segments: i64) -> PyResult<Py<PyAny>> {
        let segments = crate::validate_quadrant_segments(quadrant_segments)?;
        self.shapes(py, |disk| disk.to_polygon(segments))
    }

    fn __len__(&self) -> usize {
        self.rows.len()
    }

    fn __getitem__(&self, py: Python<'_>, index: &Bound<'_, PyAny>) -> PyResult<Py<PyAny>> {
        match parse_row_index_or_slice(index, self.rows.len(), "CircleArray")? {
            RowIndexOrSlice::Index(row) => self
                .values()
                .get(row)
                .expect("normalized circle index")
                .clone()
                .into_py(py, &self.frame),
            RowIndexOrSlice::Slice {
                start,
                stop,
                step: 1,
                ..
            } => Self {
                values: Arc::clone(&self.values),
                frame: self.frame.clone(),
                rows: self.rows.start.wrapping_add(start as usize)
                    ..self.rows.start.wrapping_add(stop.max(start) as usize),
            }
            .into_py_any(py),
            RowIndexOrSlice::Slice {
                start, step, count, ..
            } => {
                let values = slice_row_indices(start, step, count)
                    .map(|row| {
                        self.values()
                            .get(row)
                            .expect("normalized circle slice")
                            .clone()
                    })
                    .collect();
                CircleOutput::bulk_into_py(values, py, &self.frame)
            },
        }
    }

    fn __iter__(&self) -> PyCircleArrayIter {
        PyCircleArrayIter {
            array: self.clone(),
            cursor: RowIterState::new(false),
        }
    }

    fn __reversed__(&self) -> PyCircleArrayIter {
        PyCircleArrayIter {
            array: self.clone(),
            cursor: RowIterState::new(true),
        }
    }

    fn __repr__(&self) -> String {
        format!("CircleArray(len={})", self.rows.len())
    }

    fn __copy__<'py>(slf: &Bound<'py, Self>) -> Bound<'py, Self> {
        slf.clone()
    }

    fn __deepcopy__<'py>(slf: &Bound<'py, Self>, memo: &Bound<'py, PyAny>) -> Bound<'py, Self> {
        let _ = memo;
        slf.clone()
    }
}

#[pyclass(
    name = "CircleArrayIterator",
    module = "gometry",
    frozen,
    immutable_type
)]
pub(crate) struct PyCircleArrayIter {
    array: PyCircleArray,
    cursor: RowIterState,
}

#[pymethods]
impl PyCircleArrayIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(&self, py: Python<'_>) -> PyResult<Option<Py<PyAny>>> {
        self.cursor
            .next_row(self.array.rows.len())
            .map(|row| {
                self.array
                    .values()
                    .get(row)
                    .expect("iterator row")
                    .clone()
                    .into_py(py, &self.array.frame)
            })
            .transpose()
    }

    fn __length_hint__(&self) -> usize {
        self.cursor.length_hint(self.array.rows.len())
    }
}
