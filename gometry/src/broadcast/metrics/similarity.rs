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
            let missing = array.missing().cloned();
            float64_array(
                py,
                array
                    .storage()
                    .iter_rows()
                    .enumerate()
                    .map(|(row, left)| {
                        if missing.as_ref().is_some_and(|mask| mask[row]) {
                            return Ok(f64::NAN);
                        }
                        left.with_shape(|left| kernel(&model, left, right.shape(), densify.at(row)))
                    })
                    .collect_rows()
                    .map_err(rows_err)?,
            )
        },
        Many(right) => {
            let (lefts, rights) = paired_arrays(array, right, operation)?;
            let missing = crate::array::missing::union_pair(array.missing(), right.missing());
            let model = resolve_metric(array.crs_str(), unit, operation)?;
            float64_array(
                py,
                lefts
                    .iter_shapes()
                    .zip(rights.iter_shapes())
                    .enumerate()
                    .map(|(row, (left, right))| {
                        if missing.as_ref().is_some_and(|mask| mask[row]) {
                            return Ok(f64::NAN);
                        }
                        kernel(&model, &left, &right, densify.at(row))
                    })
                    .collect_rows()
                    .map_err(rows_err)?,
            )
        },
    }
}
