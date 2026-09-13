//! Similarity metric parameter broadcasts and scalar CRS-aware thresholds.

#![allow(
    clippy::similar_names,
    reason = "coordinate lanes are named by side and axis"
)]
use pyo3::types::{PyAny, PyAnyMethods as _};

use crate::array::{ArrayRow, GeometryArrayStorage, LineRows, PackedStorage, ValidityMask};
use crate::broadcast::metrics::pair::metric_hausdorff;
use crate::broadcast::metrics::{
    Arc, Bound, CollectRows as _, DistanceUnit, F64Param, Frame, GeometryInput, Py,
    PyGeometryArray, PyResult, Python, Shape, crs, float64_array, resolve_metric, rows_err,
    same_storage_similarity_metric_zeros,
};
use crate::dispatch::{CachelessRowContext, MetricOp};
use crate::geometry::{FrechetDpScratch, PackedLineColumnView, PreparedPlan};

#[derive(Clone, Debug)]
pub(crate) enum SimilarityOp {
    Hausdorff,
    Frechet { densify: SimilarityDensify },
}

#[derive(Clone, Debug)]
pub(crate) enum SimilarityDensify {
    None,
    Scalar(f64),
    PerElement(Box<[f64]>),
}

impl SimilarityDensify {
    fn at(&self, row: usize) -> Option<f64> {
        match self {
            Self::None => None,
            Self::Scalar(fraction) => Some(*fraction),
            Self::PerElement(fractions) => Some(fractions[row]),
        }
    }
}

impl SimilarityOp {
    pub(crate) const fn hausdorff() -> Self {
        Self::Hausdorff
    }

    pub(crate) fn frechet_with_densify(
        value: Option<&Bound<'_, PyAny>>,
        len: usize,
    ) -> PyResult<Self> {
        let densify = parse_densify(value, len)?;
        Ok(Self::Frechet { densify })
    }

    pub(crate) const fn metric_op(&self) -> MetricOp {
        match self {
            Self::Hausdorff => MetricOp::HausdorffDistance,
            Self::Frechet { .. } => MetricOp::FrechetDistance,
        }
    }

    pub(crate) const fn is_undensified(&self) -> bool {
        match self {
            Self::Hausdorff => true,
            Self::Frechet { densify } => matches!(densify, SimilarityDensify::None),
        }
    }

    pub(crate) const fn is_per_element(&self) -> bool {
        match self {
            Self::Hausdorff => false,
            Self::Frechet { densify } => {
                matches!(densify, SimilarityDensify::PerElement(_))
            },
        }
    }

    pub(crate) fn eval_shapes_at(
        &self,
        model: &crs::MetricModel,
        left: &Shape,
        right: &Shape,
        row: usize,
        scratch: &mut FrechetDpScratch,
    ) -> crate::error::Result<f64> {
        match self {
            Self::Hausdorff => metric_hausdorff(model, left, right),
            Self::Frechet { densify } => {
                crate::broadcast::metrics::metric_frechet_densified_with_scratch(
                    model,
                    left,
                    right,
                    densify.at(row),
                    scratch,
                )
            },
        }
    }

    pub(crate) fn eval_scalar(
        &self,
        left: &PreparedPlan,
        right: &PreparedPlan,
        context: &CachelessRowContext<'_>,
    ) -> crate::error::Result<f64> {
        let model = context.metric.require_model(self.metric_op())?;
        let mut scratch = FrechetDpScratch::default();
        self.eval_shapes_at(model, left.shape(), right.shape(), 0, &mut scratch)
    }

    pub(crate) fn try_same_storage_identity(
        &self,
        model: &crs::MetricModel,
        storage: &GeometryArrayStorage,
        missing: Option<&ValidityMask>,
    ) -> crate::error::Result<Option<Vec<f64>>> {
        if !matches!(self, Self::Hausdorff) {
            return Ok(None);
        }
        let identity = match storage {
            GeometryArrayStorage::Packed(PackedStorage::Lines { row_map, .. }) => {
                row_map.is_identity()
            },
            GeometryArrayStorage::Packed(
                PackedStorage::Points { .. } | PackedStorage::Polygons { .. },
            )
            | GeometryArrayStorage::Mixed(_) => true,
        };
        if !identity {
            return Ok(None);
        }
        for (row_index, row) in storage.iter_rows().enumerate() {
            if missing.is_some_and(|mask| mask.is_missing(row_index)) {
                continue;
            }
            row.try_with_shape(crate::broadcast::metrics::require_nonempty_hausdorff)?;
            if let crs::MetricModel::Geodesic { frame, .. } = model {
                row.try_with_shape(|shape| crs::ensure_geographic_domain_in_frame(*frame, shape))?;
            }
        }
        Ok(Some(same_storage_similarity_metric_zeros(storage)))
    }

    pub(crate) fn has_empty_line(&self, left: &LineRows<'_>, right: &LineRows<'_>) -> bool {
        matches!(self, Self::Frechet { .. })
            && (0..left.len())
                .any(|row| left.row_xy(row).0.is_empty() || right.row_xy(row).0.is_empty())
    }

    pub(crate) fn packed_batch(
        &self,
        left: PackedLineColumnView<'_>,
        right: PackedLineColumnView<'_>,
        len: usize,
        scale: f64,
    ) -> crate::error::Result<Vec<f64>> {
        let mut values = vec![0.0; len];
        match self {
            Self::Hausdorff => {
                crate::geometry::hausdorff_distance_line_columns_batch(left, right, &mut values)?;
            },
            Self::Frechet { .. } => {
                crate::geometry::frechet_distance_line_columns_batch(left, right, &mut values);
            },
        }
        crate::geometry::scale_metric_values(&mut values, scale);
        Ok(values)
    }

    pub(crate) fn packed_value(
        &self,
        left_xs: &[f64],
        left_ys: &[f64],
        right_xs: &[f64],
        right_ys: &[f64],
        scale: f64,
    ) -> crate::error::Result<f64> {
        let value = match self {
            Self::Hausdorff => crate::geometry::hausdorff_distance_line_columns(
                left_xs, left_ys, right_xs, right_ys,
            )?,
            Self::Frechet { .. } => {
                crate::geometry::frechet_distance_line_columns(left_xs, left_ys, right_xs, right_ys)
            },
        };
        Ok(value * scale)
    }

    #[expect(
        clippy::unnecessary_wraps,
        reason = "None distinguishes an ordinary window from the empty-window result, whose Result carries the operation-specific value or error"
    )]
    pub(crate) fn identical_window_value(
        &self,
        left_xs: &[f64],
        _left_ys: &[f64],
        right_xs: &[f64],
        _right_ys: &[f64],
        scale: f64,
    ) -> Option<crate::error::Result<f64>> {
        if !left_xs.is_empty() || !right_xs.is_empty() {
            return Some(Ok(0.0));
        }
        match self {
            Self::Hausdorff => Some(Err(crate::geometry::GeometryErrorKind::message(
                "hausdorff_distance requires non-empty geometries",
            ))),
            Self::Frechet { .. } => Some(Ok(f64::INFINITY * scale)),
        }
    }
}

fn parse_densify(value: Option<&Bound<'_, PyAny>>, len: usize) -> PyResult<SimilarityDensify> {
    let Some(value) = value else {
        return Ok(SimilarityDensify::None);
    };
    if value.is_none() {
        return Ok(SimilarityDensify::None);
    }
    let param = F64Param::parse(value, "densify", len)?;
    param.try_validate(|fraction| validate_densify(Some(fraction)).map(|_| ()))?;
    Ok(match param {
        F64Param::Scalar(fraction) => SimilarityDensify::Scalar(fraction),
        F64Param::PerElement(fractions) => SimilarityDensify::PerElement(fractions),
    })
}

fn validate_densify(densify: Option<f64>) -> PyResult<Option<f64>> {
    if let Some(fraction) = densify
        && !(fraction.is_finite() && fraction > 0.0 && fraction <= 1.0)
    {
        return Err(crate::py::errors::GeometryError::new_err(format!(
            "densify must be in (0, 1], got {fraction}"
        )));
    }
    Ok(densify)
}

/// Per-element ``densify=`` lane for array Fréchet.
pub(crate) fn array_crs_frechet_metric_per_densify(
    py: Python<'_>,
    array: &PyGeometryArray,
    other: GeometryInput<'_>,
    _frame: &Frame,
    operation: SimilarityOp,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    match other {
        GeometryInput::One(right) => {
            let model = resolve_metric(array.crs_str(), unit, operation.metric_op())?;
            let right = Arc::clone(&right.shape);
            float64_array(
                py,
                py.detach(move || {
                    let mut scratch = FrechetDpScratch::default();
                    array
                        .row_operation()
                        .rows_with_missing(|| f64::NAN)
                        .map(|left| match left {
                            ArrayRow::Missing { value, .. } => Ok(value),
                            ArrayRow::Present(left) => {
                                let logical = left.logical();
                                left.row().try_with_shape(|left| {
                                    operation.eval_shapes_at(
                                        &model,
                                        left,
                                        right.shape(),
                                        logical,
                                        &mut scratch,
                                    )
                                })
                            },
                        })
                        .collect_rows()
                })
                .map_err(rows_err)?,
            )
        },
        GeometryInput::Many(right) => {
            let model = resolve_metric(array.crs_str(), unit, operation.metric_op())?;
            let left = array.clone();
            let right = right.clone();
            float64_array(
                py,
                py.detach(move || {
                    let mut scratch = FrechetDpScratch::default();
                    left.row_operation()
                        .rows_with_missing(|| f64::NAN)
                        .zip(right.row_operation().rows_with_missing(|| f64::NAN))
                        .map(|(left, right)| match (left, right) {
                            (ArrayRow::Present(left), ArrayRow::Present(right)) => {
                                let logical = left.logical();
                                left.row().try_with_shape(|left| {
                                    right.row().try_with_shape(|right| {
                                        operation.eval_shapes_at(
                                            &model,
                                            left,
                                            right,
                                            logical,
                                            &mut scratch,
                                        )
                                    })
                                })
                            },
                            _ => Ok(f64::NAN),
                        })
                        .collect_rows()
                })
                .map_err(rows_err)?,
            )
        },
    }
}
