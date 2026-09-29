//! Similarity metric parameter broadcasts and scalar CRS-aware thresholds.

#![expect(
    clippy::similar_names,
    reason = "coordinate lanes are named by side and axis"
)]
use pyo3::types::{PyAny, PyAnyMethods as _};

use crate::array::{ArrayRow, GeometryArrayStorage, LineRows, PackedStorage, ValidityMask};
use crate::broadcast::metrics;
use crate::broadcast::metrics::pair::{ensure_hausdorff_geodesic_shape, metric_hausdorff};
use crate::broadcast::metrics::{
    Arc, Bound, CollectRows as _, DistanceUnit, F64Param, Frame, GeometryInput, Py,
    PyGeometryArray, PyResult, Python, Shape, crs, float64_array, resolve_metric, rows_err,
    same_storage_similarity_metric_zeros,
};
use crate::dispatch::{CachelessRowContext, MetricOp};
use crate::geometry::{FrechetDpScratch, PackedLineColumnView, PreparedPlan};
use crate::py::errors;
use crate::py::errors::GeometryError;

#[derive(Clone, Debug)]
pub(crate) enum SimilarityOp {
    Hausdorff { max_error: Option<f64> },
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
            Self::PerElement(fractions) => Some(
                *fractions
                    .get(row)
                    .expect("densify values match the admitted array rows"),
            ),
        }
    }
}

impl SimilarityOp {
    pub(crate) fn hausdorff(value: Option<&Bound<'_, PyAny>>) -> PyResult<Self> {
        Ok(Self::Hausdorff {
            max_error: parse_max_error(value)?,
        })
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
            Self::Hausdorff { .. } => MetricOp::HausdorffDistance,
            Self::Frechet { .. } => MetricOp::FrechetDistance,
        }
    }

    pub(crate) const fn is_undensified(&self) -> bool {
        match self {
            Self::Hausdorff { .. } => true,
            Self::Frechet { densify } => matches!(densify, SimilarityDensify::None),
        }
    }

    pub(crate) const fn is_per_element(&self) -> bool {
        match self {
            Self::Hausdorff { .. } => false,
            Self::Frechet { densify } => {
                matches!(densify, SimilarityDensify::PerElement(_))
            },
        }
    }

    /// Bounded certification is planar-only. The geodesic sibling retains its
    /// numerical contract and therefore must not receive a bound.
    pub(crate) fn validate_model(&self, model: &crs::MetricModel) -> crate::error::Result<()> {
        match self {
            Self::Hausdorff { max_error: Some(_) }
                if matches!(model, crs::MetricModel::Geodesic { .. }) =>
            {
                Err(crate::geometry::GeometryErrorKind::message(
                    "max_error is only supported for planar Hausdorff distance; \
                     omit it for geodesic metrics",
                ))
            },
            Self::Hausdorff { .. } | Self::Frechet { .. } => Ok(()),
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
            Self::Hausdorff { max_error } => metric_hausdorff(model, left, right, *max_error),
            Self::Frechet { densify } => metrics::metric_frechet_densified_with_scratch(
                model,
                left,
                right,
                densify.at(row),
                scratch,
            ),
        }
    }

    pub(crate) fn eval_scalar(
        &self,
        left: &PreparedPlan,
        right: &PreparedPlan,
        context: &CachelessRowContext<'_>,
    ) -> crate::error::Result<f64> {
        let model = context.metric.require_model(self.metric_op())?;
        self.validate_model(model)?;
        let mut scratch = FrechetDpScratch::default();
        self.eval_shapes_at(model, left.shape(), right.shape(), 0, &mut scratch)
    }

    pub(crate) fn try_same_storage_identity(
        &self,
        model: &crs::MetricModel,
        storage: &GeometryArrayStorage,
        missing: Option<&ValidityMask>,
    ) -> crate::error::Result<Option<Vec<f64>>> {
        if !matches!(self, Self::Hausdorff { .. }) {
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
        let mut has_areal_support = false;
        for (row_index, row) in storage.iter_rows().enumerate() {
            if missing.is_some_and(|mask| mask.is_missing(row_index)) {
                continue;
            }
            let areal = row.try_with_shape(|shape| -> crate::error::Result<bool> {
                if let crs::MetricModel::Geodesic { frame, .. } = model {
                    ensure_hausdorff_geodesic_shape(*frame, shape)?;
                }
                Ok(shape.has_area_parts())
            })?;
            has_areal_support |= areal;
        }
        if has_areal_support && matches!(model, crs::MetricModel::Planar { .. }) {
            return Ok(None);
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
            Self::Hausdorff { max_error } => {
                crate::geometry::hausdorff_distance_line_columns_batch(
                    left,
                    right,
                    &mut values,
                    *max_error,
                    scale,
                )?;
            },
            Self::Frechet { .. } => {
                crate::geometry::frechet_distance_line_columns_batch(left, right, &mut values);
                crate::geometry::scale_metric_values(&mut values, scale);
            },
        }
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
        match self {
            Self::Hausdorff { max_error } => crate::geometry::hausdorff_distance_line_columns(
                left_xs, left_ys, right_xs, right_ys, *max_error, scale,
            ),
            Self::Frechet { .. } => Ok(crate::geometry::frechet_distance_line_columns(
                left_xs, left_ys, right_xs, right_ys,
            ) * scale),
        }
    }
}

fn parse_max_error(value: Option<&Bound<'_, PyAny>>) -> PyResult<Option<f64>> {
    let Some(value) = value else {
        return Ok(None);
    };
    if value.is_none() {
        return Ok(None);
    }
    let Some(value) = crate::f64_scalar_named("max_error", value)? else {
        return Err(errors::parameter_error(
            "max_error must be a scalar positive finite float",
            "max_error",
        ));
    };
    if !(value.is_finite() && value > 0.0) {
        return Err(errors::float_parameter_error(
            format!("max_error must be a positive finite scalar, got {value}"),
            "max_error",
            value,
        ));
    }
    Ok(Some(value))
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
        return Err(GeometryError::new_err(format!(
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
