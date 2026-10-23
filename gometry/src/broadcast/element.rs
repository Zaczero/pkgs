use pyo3::IntoPyObjectExt as _;
use pyo3::prelude::*;
use pyo3::types::PyList;

use crate::array::StreamingShapes;
use crate::crs::frame::Frame;
use crate::geometry::storage::{ArrayRows, ValidityMask};
use crate::geometry::{Bounds, Shape};
use crate::py::numpy::{bool_array, bounds_array, float64_array};
use crate::{PyGeometry, PyGeometryArray, Typed};

/// The bulk return form of a dispatch element. Numeric and boolean lanes land
/// in fixed-width NumPy arrays; text stays a Python list; geometries become a
/// `GeometryArray`; grid cells become a `CellArray`.
pub(crate) trait BulkElement: Sized + pyo3::marker::Ungil {
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
        missing: Option<&ValidityMask>,
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
        let mut flat = Vec::with_capacity(values.len().wrapping_mul(4));
        for bounds in values {
            match bounds {
                Some(b) => {
                    flat.extend_from_slice(&[b.minx(), b.miny(), b.maxx(), b.maxy()]);
                },
                None => {
                    flat.extend_from_slice(&[f64::NAN; 4]);
                },
            }
        }
        bounds_array(py, flat)
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
        missing: Option<&ValidityMask>,
    ) -> PyResult<Py<PyAny>> {
        let Some(mask) = missing else {
            return values.into_py_any(py);
        };
        Ok(PyList::new(
            py,
            values
                .into_iter()
                .enumerate()
                .map(|(row, value)| (!mask.is_missing(row)).then_some(value)),
        )?
        .unbind()
        .into())
    }
}

impl BulkElement for Shape {
    fn into_py(self, py: Python<'_>, frame: &Frame) -> PyResult<Py<PyAny>> {
        Ok(Typed(PyGeometry::with_frame(self, frame.clone()))
            .into_pyobject(py)?
            .unbind())
    }

    fn bulk_into_py(values: Vec<Self>, py: Python<'_>, frame: &Frame) -> PyResult<Py<PyAny>> {
        Ok(shape_results_array(values, frame, None)?
            .into_pyobject(py)?
            .unbind()
            .into())
    }

    fn missing_value() -> Self {
        ArrayRows::missing_placeholder()
    }

    fn bulk_into_py_masked(
        values: Vec<Self>,
        py: Python<'_>,
        frame: &Frame,
        missing: Option<&ValidityMask>,
    ) -> PyResult<Py<PyAny>> {
        Ok(shape_results_array(values, frame, missing)?
            .into_pyobject(py)?
            .unbind()
            .into())
    }
}

fn shape_results_array(
    values: Vec<Shape>,
    frame: &Frame,
    missing: Option<&ValidityMask>,
) -> crate::error::Result<PyGeometryArray> {
    let mut rows = StreamingShapes::with_capacity(0, values.len());
    rows.try_extend_shapes(values, missing)?;
    rows.finish(frame.clone())
}

#[cfg(test)]
mod shape_streaming_tests {
    use super::{BulkElement, shape_results_array};
    use crate::array::LogicalRow;
    use crate::crs::frame::Frame;
    use crate::geometry::storage::{GeometryArrayStorage, PackedStorage, ValidityMask};
    use crate::geometry::{CoordinateAxes, GeometryKind, Shape};

    #[test]
    fn measured_nullable_lines_stay_packed_after_bulk_publication() {
        let first = crate::io::parse_wkt("LINESTRING M (0 0 10, 1 0 20, 2 0 30)")
            .expect("first measured line");
        let second = crate::io::parse_wkt("LINESTRING M (3 0 40, 4 0 50, 5 0 60)")
            .expect("second measured line");
        let missing = ValidityMask::from_vec(3, vec![false, true, false]).expect("mask");
        let array = shape_results_array(
            vec![first, <Shape as BulkElement>::missing_value(), second],
            &Frame::None,
            Some(&missing),
        )
        .expect("shape rows publish");

        let GeometryArrayStorage::Packed(PackedStorage::Lines {
            coords, offsets, ..
        }) = array.storage()
        else {
            panic!("homogeneous measured lines stay packed");
        };
        assert_eq!(coords.ms(), Some(&[10.0, 20.0, 30.0, 40.0, 50.0, 60.0][..]));
        assert_eq!(offsets.as_slice(), &[0, 3, 3, 6]);
        assert!(array.is_row_missing(1));
    }

    #[test]
    fn bulk_publication_keeps_present_typed_empty_when_mixed() {
        let empty = Shape::typed_empty(GeometryKind::Polygon, CoordinateAxes::XY);
        let missing = ValidityMask::from_vec(2, vec![false, true]).expect("mask");
        let array = shape_results_array(
            vec![empty.clone(), <Shape as BulkElement>::missing_value()],
            &Frame::None,
            Some(&missing),
        )
        .expect("empty polygon remains present");

        assert!(matches!(array.storage(), GeometryArrayStorage::Mixed(_)));
        let Some(LogicalRow::Present { row, .. }) = array.logical_row(0) else {
            panic!("typed empty polygon remains present");
        };
        assert_eq!(
            row.with_shape(Clone::clone)
                .expect("typed empty polygon row"),
            empty
        );
        assert!(matches!(
            array.logical_row(1),
            Some(LogicalRow::Missing { .. })
        ));
    }
}
