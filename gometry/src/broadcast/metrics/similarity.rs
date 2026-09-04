//! Similarity metric parameter broadcasts and scalar CRS-aware thresholds.

use crate::array::PackedStorage;
use pyo3::types::PyAny;

use crate::array::{ArrayRow, GeometryArrayStorage, LineRows, ValidityMask};
use crate::broadcast::metrics::{
    Arc, Bound, CollectRows as _, DistanceUnit, Frame, GeometryInput, Py, PyGeometryArray,
    PyResult, Python, Shape, classify_required, crs, float64_array, paired_arrays, resolve_metric,
    rows_err, same_storage_similarity_metric_zeros,
};
use crate::dispatch::MetricOp;

#[derive(Clone, Copy, Debug)]
pub(crate) enum SimilarityOp {
    Hausdorff { densify: Option<f64> },
    Frechet { densify: Option<f64> },
}

impl SimilarityOp {
    pub(crate) const fn with_densify(self, densify: Option<f64>) -> Self {
        match self {
            Self::Hausdorff { .. } => Self::Hausdorff { densify },
            Self::Frechet { .. } => Self::Frechet { densify },
        }
    }

    pub(crate) const fn metric_op(self) -> MetricOp {
        match self {
            Self::Hausdorff { .. } => MetricOp::HausdorffDistance,
            Self::Frechet { .. } => MetricOp::FrechetDistance,
        }
    }

    pub(crate) const fn densify(self) -> Option<f64> {
        match self {
            Self::Hausdorff { densify } | Self::Frechet { densify } => densify,
        }
    }
}

pub(crate) trait SimilarityMode {
    const OP: MetricOp;

    fn eval<K>(
        model: &crs::MetricModel,
        left: &Shape,
        right: &Shape,
        densify: Option<f64>,
        scratch: &mut crate::geometry::FrechetDpScratch,
        kernel: &K,
    ) -> crate::error::Result<f64>
    where
        K: Fn(&crs::MetricModel, &Shape, &Shape) -> crate::error::Result<f64>;

    fn try_same_storage_identity(
        model: &crs::MetricModel,
        storage: &GeometryArrayStorage,
        missing: Option<&ValidityMask>,
    ) -> crate::error::Result<Option<Vec<f64>>>;

    fn has_empty_line(left: &LineRows<'_>, right: &LineRows<'_>) -> bool;

    fn packed_batch(
        left: crate::geometry::PackedLineColumnView<'_>,
        right: crate::geometry::PackedLineColumnView<'_>,
        len: usize,
        scale: f64,
    ) -> crate::error::Result<Vec<f64>>;

    fn identical_empty<P>(
        kernel: &P,
        left_xs: &[f64],
        left_ys: &[f64],
        right_xs: &[f64],
        right_ys: &[f64],
        scale: f64,
    ) -> Option<crate::error::Result<f64>>
    where
        P: Fn(&[f64], &[f64], &[f64], &[f64]) -> crate::error::Result<f64>;
}

pub(crate) struct HausdorffMode;

impl SimilarityMode for HausdorffMode {
    const OP: MetricOp = MetricOp::HausdorffDistance;

    fn eval<K>(
        model: &crs::MetricModel,
        left: &Shape,
        right: &Shape,
        _densify: Option<f64>,
        _scratch: &mut crate::geometry::FrechetDpScratch,
        kernel: &K,
    ) -> crate::error::Result<f64>
    where
        K: Fn(&crs::MetricModel, &Shape, &Shape) -> crate::error::Result<f64>,
    {
        kernel(model, left, right)
    }

    fn try_same_storage_identity(
        model: &crs::MetricModel,
        storage: &GeometryArrayStorage,
        missing: Option<&ValidityMask>,
    ) -> crate::error::Result<Option<Vec<f64>>> {
        let identity = match storage {
            GeometryArrayStorage::Packed(PackedStorage::Lines { row_map, .. }) => row_map.is_identity(),
            GeometryArrayStorage::Packed(PackedStorage::Points { .. })
            | GeometryArrayStorage::Packed(PackedStorage::Polygons { .. })
            | GeometryArrayStorage::Mixed(_) => true,
        };
        if !identity {
            return Ok(None);
        }
        for (row_index, row) in storage.iter_rows().enumerate() {
            if missing.is_some_and(|mask| mask.is_missing(row_index)) {
                continue;
            }
            row.with_shape(crate::broadcast::metrics::require_nonempty_hausdorff)?;
            if let crs::MetricModel::Geodesic { frame, .. } = model {
                row.with_shape(|shape| crs::ensure_geographic_domain_in_frame(*frame, shape))?;
            }
        }
        Ok(Some(same_storage_similarity_metric_zeros(storage)))
    }

    fn has_empty_line(_left: &LineRows<'_>, _right: &LineRows<'_>) -> bool {
        false
    }

    fn packed_batch(
        left: crate::geometry::PackedLineColumnView<'_>,
        right: crate::geometry::PackedLineColumnView<'_>,
        len: usize,
        scale: f64,
    ) -> crate::error::Result<Vec<f64>> {
        let mut values = vec![0.0; len];
        crate::geometry::hausdorff_distance_line_columns_batch(left, right, &mut values)?;
        crate::geometry::scale_metric_values(&mut values, scale);
        Ok(values)
    }

    fn identical_empty<P>(
        _kernel: &P,
        _left_xs: &[f64],
        _left_ys: &[f64],
        _right_xs: &[f64],
        _right_ys: &[f64],
        _scale: f64,
    ) -> Option<crate::error::Result<f64>>
    where
        P: Fn(&[f64], &[f64], &[f64], &[f64]) -> crate::error::Result<f64>,
    {
        None
    }
}

pub(crate) struct FrechetMode;

impl SimilarityMode for FrechetMode {
    const OP: MetricOp = MetricOp::FrechetDistance;

    fn eval<K>(
        model: &crs::MetricModel,
        left: &Shape,
        right: &Shape,
        densify: Option<f64>,
        scratch: &mut crate::geometry::FrechetDpScratch,
        _kernel: &K,
    ) -> crate::error::Result<f64>
    where
        K: Fn(&crs::MetricModel, &Shape, &Shape) -> crate::error::Result<f64>,
    {
        crate::broadcast::metrics::metric_frechet_densified_with_scratch(
            model, left, right, densify, scratch,
        )
    }

    fn try_same_storage_identity(
        _model: &crs::MetricModel,
        _storage: &GeometryArrayStorage,
        _missing: Option<&ValidityMask>,
    ) -> crate::error::Result<Option<Vec<f64>>> {
        Ok(None)
    }

    fn has_empty_line(left: &LineRows<'_>, right: &LineRows<'_>) -> bool {
        (0..left.len()).any(|row| left.row_xy(row).0.is_empty() || right.row_xy(row).0.is_empty())
    }

    fn packed_batch(
        left: crate::geometry::PackedLineColumnView<'_>,
        right: crate::geometry::PackedLineColumnView<'_>,
        len: usize,
        scale: f64,
    ) -> crate::error::Result<Vec<f64>> {
        let mut values = vec![0.0; len];
        crate::geometry::frechet_distance_line_columns_batch(left, right, &mut values);
        crate::geometry::scale_metric_values(&mut values, scale);
        Ok(values)
    }

    fn identical_empty<P>(
        kernel: &P,
        left_xs: &[f64],
        left_ys: &[f64],
        right_xs: &[f64],
        right_ys: &[f64],
        scale: f64,
    ) -> Option<crate::error::Result<f64>>
    where
        P: Fn(&[f64], &[f64], &[f64], &[f64]) -> crate::error::Result<f64>,
    {
        Some(kernel(left_xs, left_ys, right_xs, right_ys).map(|value| value * scale))
    }
}

/// Per-element ``densify=`` lane for array Hausdorff/Fréchet.
pub(crate) fn array_crs_similarity_metric_per_densify(
    py: Python<'_>,
    array: &PyGeometryArray,
    other: &Bound<'_, PyAny>,
    operation: MetricOp,
    unit: Option<DistanceUnit>,
    densify: &crate::OptionalDensifyParam,
    kernel: impl Fn(&crs::MetricModel, &Shape, &Shape, Option<f64>) -> crate::error::Result<f64>
    + Send
    + Sync,
) -> PyResult<Py<PyAny>> {
    let operation_name = operation.name();
    match classify_required(other)? {
        GeometryInput::One(right) => {
            Frame::compatible_parts(
                array.crs_ref(),
                array.epoch(),
                right.crs_ref(),
                right.epoch(),
                operation_name,
            )?;
            let model = resolve_metric(array.crs_str(), unit, operation)?;
            let right = Arc::clone(&right.shape);
            float64_array(
                py,
                array
                    .row_operation()
                    .rows_with_missing(|| f64::NAN)
                    .map(|left| match left {
                        ArrayRow::Missing { value, .. } => Ok(value),
                        ArrayRow::Present(left) => {
                            let logical = left.logical();
                            left.row().with_shape(|left| {
                                kernel(&model, left, right.shape(), densify.at(logical))
                            })
                        },
                    })
                    .collect_rows()
                    .map_err(rows_err)?,
            )
        },
        GeometryInput::Many(right) => {
            paired_arrays(array, right, operation_name)?;
            let model = resolve_metric(array.crs_str(), unit, operation)?;
            float64_array(
                py,
                array
                    .row_operation()
                    .rows_with_missing(|| f64::NAN)
                    .zip(right.row_operation().rows_with_missing(|| f64::NAN))
                    .map(|(left, right)| match (left, right) {
                        (ArrayRow::Present(left), ArrayRow::Present(right)) => {
                            let logical = left.logical();
                            left.row().with_shape(|left| {
                                right.row().with_shape(|right| {
                                    kernel(&model, left, right, densify.at(logical))
                                })
                            })
                        },
                        _ => Ok(f64::NAN),
                    })
                    .collect_rows()
                    .map_err(rows_err)?,
            )
        },
    }
}
