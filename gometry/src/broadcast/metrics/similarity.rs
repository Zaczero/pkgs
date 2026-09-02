//! Similarity metric parameter broadcasts and scalar CRS-aware thresholds.

use pyo3::types::PyAny;

use crate::broadcast::metrics::{
    Arc, Bound, CollectRows as _, DistanceUnit, Frame, GeometryInput, Py, PyGeometryArray,
    PyResult, Python, Shape, classify_required, crs, float64_array, paired_arrays, resolve_metric,
    rows_err,
};

/// Per-element ``densify=`` lane for array Hausdorff/Fréchet.
pub(crate) fn array_crs_similarity_metric_per_densify(
    py: Python<'_>,
    array: &PyGeometryArray,
    other: &Bound<'_, PyAny>,
    operation: &str,
    unit: Option<DistanceUnit>,
    densify: &crate::OptionalDensifyParam,
    kernel: impl Fn(&crs::MetricModel, &Shape, &Shape, Option<f64>) -> crate::error::Result<f64>
    + Send
    + Sync,
) -> PyResult<Py<PyAny>> {
    use GeometryInput::{Many, One};
    match classify_required(other)? {
        One(right) => {
            Frame::compatible_parts(
                array.crs_ref(),
                array.epoch(),
                right.crs_ref(),
                right.epoch(),
                operation,
            )?;
            let model = resolve_metric(array.crs_str(), unit, operation)?;
            let right = Arc::clone(&right.shape);
            float64_array(
                py,
                array
                    .logical_shape_rows()
                    .map(|left| match left {
                        crate::array::LogicalShapeRow::Missing { .. } => Ok(f64::NAN),
                        crate::array::LogicalShapeRow::Present { logical, shape } => {
                            kernel(&model, &shape, right.shape(), densify.at(logical))
                        },
                    })
                    .collect_rows()
                    .map_err(rows_err)?,
            )
        },
        Many(right) => {
            paired_arrays(array, right, operation)?;
            let model = resolve_metric(array.crs_str(), unit, operation)?;
            float64_array(
                py,
                array
                    .logical_shape_rows()
                    .zip(right.logical_shape_rows())
                    .map(|(left, right)| match (left, right) {
                        (
                            crate::array::LogicalShapeRow::Present {
                                logical,
                                shape: left,
                            },
                            crate::array::LogicalShapeRow::Present { shape: right, .. },
                        ) => kernel(&model, &left, &right, densify.at(logical)),
                        _ => Ok(f64::NAN),
                    })
                    .collect_rows()
                    .map_err(rows_err)?,
            )
        },
    }
}
