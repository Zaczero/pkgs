#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use pyo3::IntoPyObjectExt as _;
use pyo3::prelude::*;

use crate::array::MissingMask;
use crate::boundary::metadata::Frame;
use crate::geometry::{Bounds, Shape};
use crate::py::numpy::{bool_array, float64_array};
use crate::{PyGeometry, PyGeometryArray, Typed};

/// The bulk return form of a dispatch element. Numeric and boolean lanes land
/// in fixed-width NumPy arrays; text stays a Python list; geometries become a
/// `GeometryArray`; grid cells become a `CellArray`.
pub(crate) trait BulkElement: Sized + Send {
    /// Scalar lane: one element to its natural Python container.
    fn into_py(self, py: Python<'_>, frame: &Frame) -> PyResult<Py<PyAny>>;

    /// Array lane: row results to the natural Python container.
    fn bulk_into_py(values: Vec<Self>, py: Python<'_>, frame: &Frame) -> PyResult<Py<PyAny>>;

    /// The value a missing input row yields WITHOUT running the kernel:
    /// predicates ``false``, measures ``NaN``, geometry results the missing
    /// placeholder (masked out by `bulk_into_py_masked`).
    fn missing_value() -> Self;

    /// `bulk_into_py` with the rows' missing mask: text/bytes lanes emit
    /// ``None`` entries, geometry lanes carry the mask onto the result array;
    /// numeric/bool lanes need nothing beyond `missing_value` placeholders.
    fn bulk_into_py_masked(
        values: Vec<Self>,
        py: Python<'_>,
        frame: &Frame,
        missing: Option<&MissingMask>,
    ) -> PyResult<Py<PyAny>> {
        let _ = missing;
        Self::bulk_into_py(values, py, frame)
    }
}

impl BulkElement for Option<Bounds> {
    fn missing_value() -> Self {
        None
    }

    fn into_py(self, py: Python<'_>, _frame: &Frame) -> PyResult<Py<PyAny>> {
        match self {
            Some(bounds) => Ok(bounds.into_tuple().into_pyobject(py)?.unbind().into()),
            None => Ok(py.None()),
        }
    }

    fn bulk_into_py(values: Vec<Self>, py: Python<'_>, _frame: &Frame) -> PyResult<Py<PyAny>> {
        let mut flat = Vec::with_capacity(values.len() * 4);
        for bounds in values {
            match bounds {
                Some(b) => flat.extend_from_slice(&[b.minx(), b.miny(), b.maxx(), b.maxy()]),
                None => flat.extend_from_slice(&[f64::NAN; 4]),
            }
        }
        crate::py::numpy::bounds_array(py, flat)
    }
}

impl BulkElement for f64 {
    fn into_py(self, py: Python<'_>, _frame: &Frame) -> PyResult<Py<PyAny>> {
        self.into_py_any(py)
    }

    fn bulk_into_py(values: Vec<Self>, py: Python<'_>, _frame: &Frame) -> PyResult<Py<PyAny>> {
        float64_array(py, values)
    }

    fn missing_value() -> Self {
        Self::NAN
    }
}

impl BulkElement for bool {
    fn into_py(self, py: Python<'_>, _frame: &Frame) -> PyResult<Py<PyAny>> {
        self.into_py_any(py)
    }

    fn bulk_into_py(values: Vec<Self>, py: Python<'_>, _frame: &Frame) -> PyResult<Py<PyAny>> {
        bool_array(py, values)
    }

    fn missing_value() -> Self {
        false
    }
}

impl BulkElement for String {
    fn into_py(self, py: Python<'_>, _frame: &Frame) -> PyResult<Py<PyAny>> {
        self.into_py_any(py)
    }

    fn bulk_into_py(values: Vec<Self>, py: Python<'_>, _frame: &Frame) -> PyResult<Py<PyAny>> {
        values.into_py_any(py)
    }

    fn missing_value() -> Self {
        Self::new()
    }

    fn bulk_into_py_masked(
        values: Vec<Self>,
        py: Python<'_>,
        _frame: &Frame,
        missing: Option<&MissingMask>,
    ) -> PyResult<Py<PyAny>> {
        let Some(mask) = missing else {
            return values.into_py_any(py);
        };
        let rows: Vec<Py<PyAny>> = values
            .into_iter()
            .enumerate()
            .map(|(row, value)| {
                if mask[row] {
                    Ok(py.None())
                } else {
                    value.into_py_any(py)
                }
            })
            .collect::<PyResult<_>>()?;
        rows.into_py_any(py)
    }
}

impl BulkElement for Shape {
    fn into_py(self, py: Python<'_>, frame: &Frame) -> PyResult<Py<PyAny>> {
        Ok(Typed(PyGeometry::with_frame(self, frame.clone()))
            .into_pyobject(py)?
            .unbind())
    }

    fn bulk_into_py(values: Vec<Self>, py: Python<'_>, frame: &Frame) -> PyResult<Py<PyAny>> {
        Ok(PyGeometryArray::from_shapes(values, frame.clone())
            .into_pyobject(py)?
            .unbind()
            .into())
    }

    fn missing_value() -> Self {
        PyGeometryArray::missing_placeholder()
    }

    fn bulk_into_py_masked(
        values: Vec<Self>,
        py: Python<'_>,
        frame: &Frame,
        missing: Option<&MissingMask>,
    ) -> PyResult<Py<PyAny>> {
        Ok(PyGeometryArray::from_shapes(values, frame.clone())
            .with_missing_mask(missing.cloned())
            .into_pyobject(py)?
            .unbind()
            .into())
    }
}
