use std::simd::StdFloat as _;
use std::simd::cmp::{SimdPartialEq as _, SimdPartialOrd as _};

use pyo3::types::PyAny;

use crate::array::{ArrayRow, ArrayRowOperation};
use crate::broadcast::bool_array_mask_missing;
use crate::broadcast::metrics::{
    Arc, Bounds, CollectRows as _, DistanceUnit, F64Param, Frame, GeometryInput, Point, Py,
    PyGeometry, PyGeometryArray, PyResult, Python, Shape, SimilarityOp, bool_array, crs,
    ensure_geodesic_shape, float64_array, geodesic_point_columns_dwithin_shape_values,
    geodesic_point_columns_to_shape_values, mask_missing, pair_distance_geodesic,
    pair_distance_planar, pair_dwithin_geodesic, pair_dwithin_planar, point_distance,
    resolve_metric, rows_err,
};
use crate::dispatch::MetricOp;
use crate::error::BatchError;
use crate::geometry::packed_line_metrics::scale_metric_values;
use crate::geometry::storage::{ValidityMask, validity as missing};
use crate::geometry::{
    PreparedPlan, ReduceSimd, pair_map4_guarded_f64, pair_select_mask, points_dwithin, storage,
};

fn float64_array_mask_missing(
    py: Python<'_>,
    mut values: Vec<f64>,
    missing: Option<&ValidityMask>,
) -> PyResult<Py<PyAny>> {
    mask_missing(&mut values, missing, f64::NAN);
    float64_array(py, values)
}
/// CRS-aware array float metric used by the spine-routed similarity lanes.
/// Fréchet owns its optional densification; Hausdorff evaluates its original
/// continuous supports.
pub(crate) fn array_crs_metric_float(
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
            operation.validate_model(&model)?;
            let left = array.clone();
            let right = Arc::clone(&right.shape);
            float64_array(
                py,
                py.detach(move || {
                    let mut scratch = crate::geometry::FrechetDpScratch::default();
                    left.row_operation()
                        .rows_with_missing(|| f64::NAN)
                        .map(|row| match row {
                            ArrayRow::Missing { value, .. } => Ok(value),
                            ArrayRow::Present(row) => {
                                let logical = row.logical();
                                operation.eval_rows_at(
                                    &model,
                                    row.row(),
                                    right.shape().into(),
                                    logical,
                                    &mut scratch,
                                )
                            },
                        })
                        .collect_rows()
                })
                .map_err(rows_err)?,
            )
        },
        GeometryInput::Many(right) => {
            let lefts = Arc::clone(array.storage_arc());
            let rights = Arc::clone(right.storage_arc());
            let model = resolve_metric(array.crs_str(), unit, operation.metric_op())?;
            operation.validate_model(&model)?;
            let missing = missing::union_many([array.missing(), right.missing()]);
            if Arc::ptr_eq(&lefts, &rights)
                && let Some(values) =
                    operation.try_same_storage_identity(&model, &lefts, missing.as_ref())?
            {
                return float64_array_mask_missing(py, values, missing.as_ref());
            }
            if let crs::MetricModel::Planar { to_metre } = &model
                && operation.is_undensified()
                && let (Some(left_lines), Some(right_lines)) =
                    (lefts.line_rows(), rights.line_rows())
                && left_lines.is_packed_pair(&right_lines)
            {
                let has_empty_line = operation.has_empty_line(&left_lines, &right_lines);
                if missing.is_none() && !has_empty_line {
                    let len = left_lines.len();
                    let scale = to_metre.get();
                    return float64_array(
                        py,
                        py.detach(move || {
                            let left_view = left_lines.packed_column_view().expect("checked above");
                            let right_view =
                                right_lines.packed_column_view().expect("checked above");
                            operation.packed_batch(left_view, right_view, len, scale)
                        })?,
                    );
                }
                return float64_array(
                    py,
                    py.detach(move || {
                        (0..left_lines.len())
                            .map(|index| {
                                if missing.as_ref().is_some_and(|mask| mask.is_missing(index)) {
                                    return Ok(f64::NAN);
                                }
                                let (left_xs, left_ys) = left_lines.row_xy(index);
                                let (right_xs, right_ys) = right_lines.row_xy(index);
                                if std::ptr::eq(left_xs.as_ptr(), right_xs.as_ptr())
                                    && std::ptr::eq(left_ys.as_ptr(), right_ys.as_ptr())
                                    && left_xs.len() == right_xs.len()
                                {
                                    return Ok(if left_xs.is_empty() {
                                        f64::INFINITY
                                    } else {
                                        0.0
                                    });
                                }
                                operation.packed_value(
                                    left_xs,
                                    left_ys,
                                    right_xs,
                                    right_ys,
                                    to_metre.get(),
                                )
                            })
                            .collect_rows()
                    })
                    .map_err(rows_err)?,
                );
            }
            let left = array.clone();
            let right = right.clone();
            float64_array(
                py,
                py.detach(move || {
                    let mut scratch = crate::geometry::FrechetDpScratch::default();
                    left.row_operation()
                        .rows_with_missing(|| f64::NAN)
                        .zip(right.row_operation().rows_with_missing(|| f64::NAN))
                        .map(|(left, right)| match (left, right) {
                            (ArrayRow::Present(left), ArrayRow::Present(right)) => {
                                let logical = left.logical();
                                operation.eval_rows_at(
                                    &model,
                                    left.row(),
                                    right.row(),
                                    logical,
                                    &mut scratch,
                                )
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

/// Element-wise lane ladder for array × array CRS-aware metrics, shared by
/// `distance` and `dwithin`: packed points stream shared columns through
/// `packed_points` with the GIL released (one kernel call per pair, no `Shape`
/// wrapping); general rows run `pair` over prepared plans.
fn array_pair_metric<'a, T, P, C, F, Q>(
    py: Python<'_>,
    array: &'a PyGeometryArray,
    right: &'a PyGeometryArray,
    packed_points: Option<P>,
    packed_columns: Option<C>,
    mut row_kernel: Option<Q>,
    missing_value: T,
    pair: F,
) -> PyResult<Vec<T>>
where
    T: Copy + pyo3::marker::Ungil + 'static,
    P: Fn(Point, Point) -> crate::error::Result<T> + pyo3::marker::Ungil,
    C: Fn(&[f64], &[f64], &[f64], &[f64]) -> crate::error::Result<Vec<T>> + pyo3::marker::Ungil,
    Q: FnMut(
            &mut crate::geometry::RowMetricPair<'a>,
            storage::ShapeRow<'a>,
            storage::ShapeRow<'a>,
            Option<Bounds>,
            Option<Bounds>,
        ) -> crate::error::Result<Option<T>>
        + pyo3::marker::Ungil,
    F: Fn(
            usize,
            &PreparedPlan,
            &PreparedPlan,
            &mut ArrayRowOperation<'_>,
            &mut ArrayRowOperation<'_>,
        ) -> crate::error::Result<T>
        + pyo3::marker::Ungil,
{
    let missing = missing::union_many([array.missing(), right.missing()]);
    if !array.is_compact_nullable()
        && !right.is_compact_nullable()
        && let (Some(lefts), Some(rights)) =
            (array.storage().point_rows(), right.storage().point_rows())
    {
        if let Some(columns) = packed_columns
            && missing.is_none()
            && let Some(values) = array.pair_packed_points_detached(
                py,
                right,
                move |left_xs, left_ys, right_xs, right_ys| {
                    columns(left_xs, left_ys, right_xs, right_ys)
                },
            )?
        {
            return Ok(values);
        }
        if let Some(kernel) = packed_points {
            return py
                .detach(move || {
                    lefts
                        .iter()
                        .zip(rights.iter())
                        .enumerate()
                        .map(|(row, (left, right))| {
                            if missing.as_ref().is_some_and(|mask| mask.is_missing(row)) {
                                Ok(missing_value)
                            } else {
                                kernel(left, right)
                            }
                        })
                        .collect_rows()
                })
                .map_err(rows_err);
        }
        unreachable!("all-point pair metrics provide a direct point kernel")
    }
    let left_bounds = array.existing_element_bounds();
    let right_bounds = right.existing_element_bounds();
    py.detach(move || {
        let mut left_rows = array.row_operation();
        let mut right_rows = right.row_operation();
        let mut scratch = crate::geometry::RowMetricPair::new();
        left_rows
            .rows_with_missing(move || missing_value)
            .zip(right_rows.rows_with_missing(move || missing_value))
            .map(|(left, right)| {
                let (ArrayRow::Present(left_row), ArrayRow::Present(right_row)) = (left, right)
                else {
                    return Ok(match left {
                        ArrayRow::Missing { value, .. } => value,
                        ArrayRow::Present(_) => match right {
                            ArrayRow::Missing { value, .. } => value,
                            ArrayRow::Present(_) => unreachable!(),
                        },
                    });
                };
                let index = left_row.logical();
                let (left, right) = if let Some(kernel) = row_kernel.as_mut()
                    && left_row
                        .row()
                        .segment_count_lower_bound()
                        .saturating_add(right_row.row().segment_count_lower_bound())
                        <= crate::geometry::RowMetricPair::MAX_SEGMENTS
                {
                    let a = left_row.row();
                    let b = right_row.row();
                    let a_bounds = left_bounds
                        .as_ref()
                        .and_then(|bounds| bounds.get(index).copied().flatten())
                        .or_else(|| left_rows.quick_bounds_at(index, a));
                    let b_bounds = right_bounds
                        .as_ref()
                        .and_then(|bounds| bounds.get(index).copied().flatten())
                        .or_else(|| right_rows.quick_bounds_at(index, b));
                    if let Some(value) = kernel(&mut scratch, a, b, a_bounds, b_bounds)? {
                        return Ok(value);
                    }
                    (
                        left_rows.prepared_at_with_bounds(index, a, a_bounds)?,
                        right_rows.prepared_at_with_bounds(index, b, b_bounds)?,
                    )
                } else {
                    (
                        left_rows.prepared(left_row)?,
                        right_rows.prepared(right_row)?,
                    )
                };
                pair(index, &left, &right, &mut left_rows, &mut right_rows)
            })
            .collect_rows()
    })
    .map_err(rows_err)
}

/// Per-element CRS-aware distances from `array` to `other` (scalar or array).
pub(crate) fn array_crs_distances<'a>(
    py: Python<'_>,
    array: &'a PyGeometryArray,
    other: GeometryInput<'a>,
    _frame: &Frame,
    operation: MetricOp,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    // Operand classification and structural checks (length, frame) come
    // BEFORE metric-model resolution, so an invalid pairing reports as such
    // even when the array's CRS cannot back a metric.
    match other {
        GeometryInput::One(right) => {
            let model = resolve_metric(array.crs_str(), unit, operation)?;
            // Planar scalar-vs-array: build the fixed operand's distance parts ONCE
            // and reuse them across every element (a large fixed geometry otherwise
            // rebuilds its vertex/segment sets per pair).
            if let crs::MetricModel::Planar { to_metre } = model {
                let right_shape = right.prepared_plan();
                if !array.is_compact_nullable()
                    && let Some(values) = array.reduce_packed_points_detached(py, {
                        let right_shape = Arc::clone(&right_shape);
                        move |points| {
                            let mut values = right_shape.distance_points(
                                points.present_points().map(|point| (point.x, point.y)),
                            )?;
                            for value in &mut values {
                                *value *= to_metre.get();
                            }
                            Ok(values)
                        }
                    })?
                {
                    return float64_array(py, values);
                }
                // Gathered mixed points run the same batch kernel from by-value points.
                if !array.is_compact_nullable()
                    && let Some(points) = array.storage().point_rows()
                {
                    let right_shape = right.prepared_plan();
                    return float64_array_mask_missing(
                        py,
                        py.detach(move || {
                            let mut values = right_shape
                                .distance_points(points.iter().map(|point| (point.x, point.y)))?;
                            for value in &mut values {
                                *value *= to_metre.get();
                            }
                            Ok::<_, crate::error::Error>(values)
                        })?,
                        array.missing(),
                    );
                }
                // The fixed operand's prepared state (parts + facet trees) builds
                // once on its handle and serves every element; each element's
                // state persists on ITS handle for later operations. Pure Rust
                // work — run it detached.
                let array = array.clone();
                let right_shape = right.prepared_plan();
                return float64_array(
                    py,
                    py.detach(move || {
                        let mut rows = array.row_operation();
                        let mut scratch = crate::geometry::RowMetricPair::new();
                        let fixed = storage::ShapeRow::Shape(right_shape.shape());
                        let fixed_bounds = right_shape.bounds();
                        let element_bounds = array.existing_element_bounds();
                        rows.rows_with_missing(|| f64::NAN)
                            .map(|logical_row| {
                                let ArrayRow::Present(row) = logical_row else {
                                    if let ArrayRow::Missing { value, .. } = logical_row {
                                        return Ok(value);
                                    }
                                    unreachable!();
                                };
                                let logical = row.logical();
                                let shape = row.row();
                                let bounds = element_bounds
                                    .as_ref()
                                    .and_then(|bounds| bounds.get(logical).copied().flatten())
                                    .or_else(|| rows.quick_bounds_at(logical, shape));
                                if let Some(value) =
                                    scratch.distance(shape, fixed, bounds, fixed_bounds)?
                                {
                                    return Ok(value * to_metre.get());
                                }
                                let left = rows.prepared_at_with_bounds(logical, shape, bounds)?;
                                Ok(left.distance(&right_shape)? * to_metre.get())
                            })
                            .collect_rows()
                    })
                    .map_err(rows_err)?,
                );
            }
            // Geodesic scalar-fixed packed points: the geodesic cache resolves
            // ONCE and raw columns stream through the same point kernel (the
            // per-row lane re-resolves the CRS cache for every pair).
            if let (crs::MetricModel::Geodesic { crs, state, frame }, Shape::Point(target)) =
                (&model, right.shape.shape())
                && !array.has_missing()
                && let Some(values) = array.reduce_packed_points_detached(py, {
                    let crs = Arc::clone(crs);
                    let state = Arc::clone(state);
                    let target = *target;
                    move |points| {
                        let (xs, ys) = points.geodesic_xy_lanes();
                        crs::geodesic_point_distances(&crs, &xs, &ys, target, *frame, &state)
                            .map_err(BatchError::Batch)
                    }
                })?
            {
                return float64_array(py, values);
            }
            // Geodesic point-array -> fixed complex shape: one prepared
            // geodesic BVH over the fixed operand, one stack for all probes.
            if let crs::MetricModel::Geodesic { crs, state, frame } = &model
                && !array.has_missing()
                && let Some(values) = array.reduce_packed_points_detached(py, {
                    let crs = Arc::clone(crs);
                    let state = Arc::clone(state);
                    let right_shape = right.prepared_plan();
                    let right_cache = right.frame_cache();
                    move |points| {
                        let (xs, ys) = points.geodesic_xy_lanes();
                        geodesic_point_columns_to_shape_values(
                            &crs,
                            &xs,
                            &ys,
                            &right_shape,
                            &right_cache,
                            *frame,
                            &state,
                        )
                        .map_err(BatchError::Batch)
                    }
                })?
            {
                return float64_array(py, values);
            }
            // Geodesic scalar-fixed general rows: resolve the CRS/ellipsoid
            // once; chart parts for the fixed operand are cached on its handle
            // when the first live row reaches the shared chart-distance kernel.
            if let crs::MetricModel::Geodesic { crs, state, .. } = model {
                let array = array.clone();
                let right_shape = right.prepared_plan();
                let right_cache = right.frame_cache();
                let rows = py.detach(move || {
                    let mut rows = array.row_operation();
                    crs::with_resolved_ellipsoid_metric_at(
                        &crs,
                        &[right_shape.shape()],
                        &state,
                        |crs, metric| {
                            let (semi_major, flattening) = metric.ellipsoid_parameters();
                            Ok(rows
                                .rows_with_missing(|| f64::NAN)
                                .map(|logical_row| {
                                    let ArrayRow::Present(row) = logical_row else {
                                        if let ArrayRow::Missing { value, .. } = logical_row {
                                            return Ok(value);
                                        }
                                        unreachable!();
                                    };
                                    let row_index = row.logical();
                                    let left = rows.prepared(row)?;
                                    let left_cache = rows.frame_cache(row_index);
                                    crs::finite(
                                        left.geodesic_distance_cached(
                                            &left_cache,
                                            &right_shape,
                                            &right_cache,
                                            crs,
                                            semi_major,
                                            flattening,
                                            metric,
                                        )?,
                                        crs::GeodesicCalculation::Distance,
                                    )
                                })
                                .collect_rows())
                        },
                    )
                })?;
                return float64_array(py, rows.map_err(rows_err)?);
            }
            unreachable!("metric model is Planar or Geodesic")
        },
        GeometryInput::Many(right) => {
            let model = resolve_metric(array.crs_str(), unit, operation)?;
            // Planar packed × packed: pure column zip — one guarded-sqrt distance
            // per pair (`hypot` is a libm call at every x86-64 level), no
            // per-pair `Shape` wrapping, and the GIL is released.
            // Resolve the geodesic ellipsoid ONCE for the whole array pair (the
            // per-pair kernel re-resolved the CRS for every row).
            let resolved = crs::ResolvedMetric::from_model(&model)?;
            match &resolved {
                crs::ResolvedMetric::Planar { to_metre } => {
                    let packed_planar = Some(move |a, b| Ok(point_distance(a, b) * to_metre.get()));
                    let packed_columns =
                        Some(move |lx: &[f64], ly: &[f64], rx: &[f64], ry: &[f64]| {
                            let mut values = vec![0.0; lx.len()];
                            pair_map4_guarded_f64(
                                lx,
                                ly,
                                rx,
                                ry,
                                &mut values,
                                |lx, ly, rx, ry| {
                                    point_distance(
                                        Point::new_raw_xy(lx, ly),
                                        Point::new_raw_xy(rx, ry),
                                    )
                                },
                                |lx, ly, rx, ry| {
                                    let dx = std::ops::Sub::sub(lx, rx);
                                    let dy = std::ops::Sub::sub(ly, ry);
                                    let squared = std::ops::Add::add(
                                        std::ops::Mul::mul(dx, dx),
                                        std::ops::Mul::mul(dy, dy),
                                    );
                                    let distance = squared.sqrt();
                                    let zero = ReduceSimd::splat(0.0);
                                    let zero_delta = dx.simd_eq(zero) & dy.simd_eq(zero);
                                    // Full trust rule (not exact-zero-only): positive
                                    // subnormal squares must cold-rescue via scalar
                                    // `point_distance` / hypot — otherwise multi-scale
                                    // packed batches disagree with the scalar path.
                                    let bad = crate::geometry::squared_norm_untrusted_mask(
                                        squared, zero_delta,
                                    );
                                    (distance, bad)
                                },
                            );
                            scale_metric_values(&mut values, to_metre.get());
                            Ok(values)
                        });
                    float64_array(
                        py,
                        array_pair_metric(
                            py,
                            array,
                            right,
                            packed_planar,
                            packed_columns,
                            Some(
                                |scratch: &mut crate::geometry::RowMetricPair<'a>,
                                 left,
                                 right,
                                 left_bounds,
                                 right_bounds| {
                                    scratch
                                        .distance(left, right, left_bounds, right_bounds)
                                        .map(|value| value.map(|value| value * to_metre.get()))
                                },
                            ),
                            f64::NAN,
                            |_row, left, right_data, _left_products, _right_products| {
                                pair_distance_planar(to_metre.get(), left, right_data)
                            },
                        )?,
                    )
                },
                crs::ResolvedMetric::Geodesic(metric) => {
                    ensure_geodesic_array_rows(array, metric.frame)?;
                    ensure_geodesic_array_rows(right, metric.frame)?;
                    let geodesic = *metric.geodesic;
                    let packed_columns =
                        Some(move |lx: &[f64], ly: &[f64], rx: &[f64], ry: &[f64]| {
                            crs::geodesic_point_pair_distances(
                                &geodesic,
                                lx,
                                ly,
                                rx,
                                ry,
                                metric.frame,
                            )
                        });
                    let packed_planar = Some(move |left, right| {
                        crs::geodesic_point_pair_distance(
                            &geodesic,
                            left,
                            right,
                            metric.frame,
                            metric.state.generation,
                        )
                    });
                    float64_array(
                        py,
                        array_pair_metric(
                            py,
                            array,
                            right,
                            packed_planar,
                            packed_columns,
                            None::<
                                fn(
                                    &mut crate::geometry::RowMetricPair<'a>,
                                    storage::ShapeRow<'a>,
                                    storage::ShapeRow<'a>,
                                    Option<Bounds>,
                                    Option<Bounds>,
                                )
                                    -> crate::error::Result<Option<f64>>,
                            >,
                            f64::NAN,
                            |row, left, right_data, left_products, right_products| {
                                pair_distance_geodesic(
                                    metric,
                                    left,
                                    &left_products.frame_cache(row),
                                    right_data,
                                    &right_products.frame_cache(row),
                                )
                            },
                        )?,
                    )
                },
            }
        },
    }
}

/// CRS-aware `dwithin` for an array left operand.
///
/// Resolves the metric model
/// once and, on the planar path, uses the short-circuiting [`Shape::dwithin`]
/// (bounds-prune + intersects fast-path) rather than computing every exact
/// distance; geodesic rows use their threshold owner and retain its early
/// exclusion/hit behavior.
pub(crate) fn array_crs_dwithin<'a>(
    py: Python<'_>,
    array: &'a PyGeometryArray,
    other: GeometryInput<'a>,
    _frame: &Frame,
    distance: f64,
    operation: MetricOp,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    match other {
        GeometryInput::One(right) => {
            array_crs_dwithin_scalar(py, array, right, distance, operation, unit)
        },
        GeometryInput::Many(right) => {
            let model = resolve_metric(array.crs_str(), unit, operation)?;
            // Resolve the geodesic ellipsoid ONCE for the whole array pair.
            let resolved = crs::ResolvedMetric::from_model(&model)?;
            match &resolved {
                crs::ResolvedMetric::Planar { to_metre } => {
                    let limit = distance / to_metre.get();
                    let limit_sq = limit * limit;
                    // Underflow-safe dwithin (squared only when both sides honest).
                    let packed_planar = Some(move |a, b| Ok(points_dwithin(a, b, limit)));
                    let packed_columns =
                        Some(move |lx: &[f64], ly: &[f64], rx: &[f64], ry: &[f64]| {
                            let mut out = vec![false; lx.len()];
                            // A positive-subnormal limit square quantizes the
                            // boundary; keep the packed lane in distance space
                            // until the square is a normal f64.
                            let use_sq = limit_sq.is_normal() && limit > 0.0;
                            if use_sq {
                                pair_select_mask(
                                    lx,
                                    ly,
                                    rx,
                                    ry,
                                    &mut out,
                                    |lx, ly, rx, ry| {
                                        points_dwithin(
                                            Point::new_raw_xy(lx, ly),
                                            Point::new_raw_xy(rx, ry),
                                            limit,
                                        )
                                    },
                                    |lx, ly, rx, ry| {
                                        let dx = std::ops::Sub::sub(lx, rx);
                                        let dy = std::ops::Sub::sub(ly, ry);
                                        std::ops::Add::add(
                                            std::ops::Mul::mul(dx, dx),
                                            std::ops::Mul::mul(dy, dy),
                                        )
                                        .simd_le(ReduceSimd::splat(limit_sq))
                                    },
                                );
                            } else {
                                let left_y = ly
                                    .get(..out.len())
                                    .expect("latitude columns cover every logical row");
                                let right_x = rx
                                    .get(..out.len())
                                    .expect("right longitude covers every logical row");
                                let right_y = ry
                                    .get(..out.len())
                                    .expect("right latitude covers every logical row");
                                for (((left_x, left_y), (right_x, right_y)), slot) in lx
                                    .iter()
                                    .zip(left_y)
                                    .zip(right_x.iter().zip(right_y))
                                    .zip(out.iter_mut())
                                {
                                    *slot = points_dwithin(
                                        Point::new_raw_xy(*left_x, *left_y),
                                        Point::new_raw_xy(*right_x, *right_y),
                                        limit,
                                    );
                                }
                            }
                            Ok(out)
                        });
                    bool_array(
                        py,
                        array_pair_metric(
                            py,
                            array,
                            right,
                            packed_planar,
                            packed_columns,
                            Some(
                                |scratch: &mut crate::geometry::RowMetricPair<'a>,
                                 left,
                                 right,
                                 left_bounds,
                                 right_bounds| {
                                    scratch.dwithin(left, right, left_bounds, right_bounds, limit)
                                },
                            ),
                            false,
                            |_row, left, right_data, _left_products, _right_products| {
                                pair_dwithin_planar(to_metre.get(), left, right_data, distance)
                            },
                        )?,
                    )
                },
                crs::ResolvedMetric::Geodesic(metric) => {
                    ensure_geodesic_array_rows(array, metric.frame)?;
                    ensure_geodesic_array_rows(right, metric.frame)?;
                    let geodesic = *metric.geodesic;
                    let packed_columns =
                        Some(move |lx: &[f64], ly: &[f64], rx: &[f64], ry: &[f64]| {
                            crs::geodesic_point_pair_dwithin(
                                &geodesic,
                                lx,
                                ly,
                                rx,
                                ry,
                                distance,
                                metric.frame,
                            )
                        });
                    let packed_planar = Some(move |left, right| {
                        crs::geodesic_point_pair_dwithin_one(
                            &geodesic,
                            left,
                            right,
                            distance,
                            metric.frame,
                            metric.state.generation,
                        )
                    });
                    bool_array(
                        py,
                        array_pair_metric(
                            py,
                            array,
                            right,
                            packed_planar,
                            packed_columns,
                            None::<
                                fn(
                                    &mut crate::geometry::RowMetricPair<'a>,
                                    storage::ShapeRow<'a>,
                                    storage::ShapeRow<'a>,
                                    Option<Bounds>,
                                    Option<Bounds>,
                                )
                                    -> crate::error::Result<Option<bool>>,
                            >,
                            false,
                            |row, left, right_data, left_products, right_products| {
                                pair_dwithin_geodesic(
                                    metric,
                                    left,
                                    &left_products.frame_cache(row),
                                    right_data,
                                    &right_products.frame_cache(row),
                                    distance,
                                )
                            },
                        )?,
                    )
                },
            }
        },
    }
}

/// Admit every present row in a geographic array before a dwithin result
/// buffer is reserved. Domain errors and unsupported filled interiors must
/// therefore win over a later injected output-allocation failure.
fn ensure_geodesic_array_rows(
    array: &PyGeometryArray,
    frame: crate::geographic::GeographicFrame,
) -> crate::error::Result<()> {
    let rows = array.row_operation();
    for row in rows.rows_with_missing(|| ()) {
        let ArrayRow::Present(row) = row else {
            continue;
        };
        row.row()
            .try_with_shape(|shape| ensure_geodesic_shape(frame, shape))?;
    }
    Ok(())
}

fn try_collect_dwithin_rows(
    row_count: usize,
    values: impl IntoIterator<Item = crate::error::Result<bool>>,
) -> crate::error::Result<Vec<bool>, (usize, crate::error::Error)> {
    let mut output = Vec::with_capacity(row_count);
    for (row, value) in values.into_iter().enumerate() {
        output.push(value.map_err(|error| (row, error))?);
    }
    Ok(output)
}

/// Per-element `dwithin`: one distance threshold PER geometry (the cold
/// scalar-or-array lane; a scalar keeps the optimized [`array_crs_dwithin`]
/// packed/refuter path). Runs under the GIL because this rare row lane retains
/// native error context until the final Python boundary. Mirrors the scalar
/// dispatch's scalar-operand / array-operand split.
pub(crate) fn array_crs_dwithin_per_element(
    py: Python<'_>,
    array: &PyGeometryArray,
    other: GeometryInput<'_>,
    _frame: &Frame,
    distance: &F64Param,
    operation: MetricOp,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    let values = array_crs_dwithin_per_element_values(array, other, distance, operation, unit)?;
    bool_array(py, values)
}

fn array_crs_dwithin_per_element_values(
    array: &PyGeometryArray,
    other: GeometryInput<'_>,
    distance: &F64Param,
    operation: MetricOp,
    unit: Option<DistanceUnit>,
) -> PyResult<Vec<bool>> {
    match other {
        GeometryInput::One(right) => {
            let model = resolve_metric(array.crs_str(), unit, operation)?;
            let resolved = crs::ResolvedMetric::from_model(&model)?;
            let right_plan = right.prepared_plan();
            if let crs::ResolvedMetric::Geodesic(metric) = &resolved {
                ensure_geodesic_array_rows(array, metric.frame)?;
                ensure_geodesic_shape(metric.frame, right_plan.shape())?;
            }
            let results: Vec<bool> = match &resolved {
                crs::ResolvedMetric::Planar { to_metre } => {
                    let right_bounds = right_plan.bounds();
                    let left_bounds = array.existing_element_bounds();
                    let mut rows = array.row_operation();
                    let mut scratch = crate::geometry::RowMetricPair::new();
                    let fixed = storage::ShapeRow::Shape(right_plan.shape());
                    try_collect_dwithin_rows(
                        array.len(),
                        rows.rows_with_missing(|| false).map(|left| {
                            let ArrayRow::Present(left) = left else {
                                return Ok(false);
                            };
                            let row = left.logical();
                            let left = left.row();
                            let bounds = left_bounds
                                .as_ref()
                                .and_then(|bounds| bounds.get(row).copied().flatten())
                                .or_else(|| rows.quick_bounds_at(row, left));
                            if let Some(value) = scratch.dwithin(
                                left,
                                fixed,
                                bounds,
                                right_bounds,
                                distance.get(row) / to_metre.get(),
                            )? {
                                return Ok(value);
                            }
                            let left = rows.prepared_at_with_bounds(row, left, bounds)?;
                            pair_dwithin_planar(
                                to_metre.get(),
                                &left,
                                &right_plan,
                                distance.get(row),
                            )
                        }),
                    )
                    .map_err(rows_err)?
                },
                crs::ResolvedMetric::Geodesic(metric) => {
                    let right_cache = right.frame_cache();
                    let mut rows = array.row_operation();
                    try_collect_dwithin_rows(
                        array.len(),
                        rows.rows_with_missing(|| false).map(|left| {
                            let ArrayRow::Present(left) = left else {
                                return Ok(false);
                            };
                            let row = left.logical();
                            let left = rows.prepared(left)?;
                            let left_cache = rows.frame_cache(row);
                            pair_dwithin_geodesic(
                                metric,
                                &left,
                                &left_cache,
                                &right_plan,
                                &right_cache,
                                distance.get(row),
                            )
                        }),
                    )
                    .map_err(rows_err)?
                },
            };
            Ok(results)
        },
        GeometryInput::Many(right) => {
            let model = resolve_metric(array.crs_str(), unit, operation)?;
            let resolved = crs::ResolvedMetric::from_model(&model)?;
            let results: Vec<bool> = match &resolved {
                crs::ResolvedMetric::Planar { to_metre } => {
                    let left_bounds = array.existing_element_bounds();
                    let right_bounds = right.existing_element_bounds();
                    let mut left_rows = array.row_operation();
                    let mut right_rows = right.row_operation();
                    let mut scratch = crate::geometry::RowMetricPair::new();
                    try_collect_dwithin_rows(
                        array.len(),
                        left_rows
                            .rows_with_missing(|| false)
                            .zip(right_rows.rows_with_missing(|| false))
                            .map(|(left, right_row)| {
                                let (ArrayRow::Present(left), ArrayRow::Present(right_row)) =
                                    (left, right_row)
                                else {
                                    return Ok(false);
                                };
                                let row = left.logical();
                                let left = left.row();
                                let right_row = right_row.row();
                                let a_bounds = left_bounds
                                    .as_ref()
                                    .and_then(|bounds| bounds.get(row).copied().flatten())
                                    .or_else(|| left_rows.quick_bounds_at(row, left));
                                let b_bounds = right_bounds
                                    .as_ref()
                                    .and_then(|bounds| bounds.get(row).copied().flatten())
                                    .or_else(|| right_rows.quick_bounds_at(row, right_row));
                                if let Some(value) = scratch.dwithin(
                                    left,
                                    right_row,
                                    a_bounds,
                                    b_bounds,
                                    distance.get(row) / to_metre.get(),
                                )? {
                                    return Ok(value);
                                }
                                let left =
                                    left_rows.prepared_at_with_bounds(row, left, a_bounds)?;
                                let right_data =
                                    right_rows.prepared_at_with_bounds(row, right_row, b_bounds)?;
                                pair_dwithin_planar(
                                    to_metre.get(),
                                    &left,
                                    &right_data,
                                    distance.get(row),
                                )
                            }),
                    )
                    .map_err(rows_err)?
                },
                crs::ResolvedMetric::Geodesic(metric) => {
                    let mut left_rows = array.row_operation();
                    let mut right_rows = right.row_operation();
                    ensure_geodesic_array_rows(array, metric.frame)?;
                    ensure_geodesic_array_rows(right, metric.frame)?;
                    try_collect_dwithin_rows(
                        array.len(),
                        left_rows
                            .rows_with_missing(|| false)
                            .zip(right_rows.rows_with_missing(|| false))
                            .map(|(left, right_row)| {
                                let (ArrayRow::Present(left), ArrayRow::Present(right_row)) =
                                    (left, right_row)
                                else {
                                    return Ok(false);
                                };
                                let row = left.logical();
                                let left = left.row();
                                let right_row = right_row.row();
                                let left = left_rows.prepared_at(row, left)?;
                                let right_data = right_rows.prepared_at(row, right_row)?;
                                let left_cache = left_rows.frame_cache(row);
                                let right_cache = right_rows.frame_cache(row);
                                pair_dwithin_geodesic(
                                    metric,
                                    &left,
                                    &left_cache,
                                    &right_data,
                                    &right_cache,
                                    distance.get(row),
                                )
                            }),
                    )
                    .map_err(rows_err)?
                },
            };
            Ok(results)
        },
    }
}

/// `array_crs_dwithin` against one fixed scalar operand: the frame is checked
/// once at the array level, the fixed operand's distance parts and geo tree
/// are built once, and the planar path runs with the GIL released.
pub(crate) fn array_crs_dwithin_scalar(
    py: Python<'_>,
    array: &PyGeometryArray,
    other: &PyGeometry,
    distance: f64,
    operation: MetricOp,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    let model = resolve_metric(array.crs_str(), unit, operation)?;
    // The physical storage uses a finite NaN placeholder for missing rows.
    // Once a mask exists, enter the row lane before any packed reduction,
    // point materialization, bounds read, or kernel call; masking the result
    // afterwards is too late for kernels that assert finite coordinates.
    if array.has_missing() {
        let resolved = crs::ResolvedMetric::from_model(&model)?;
        let array_owned = array.clone();
        let other_shape = other.prepared_plan();
        if let crs::ResolvedMetric::Geodesic(metric) = &resolved {
            ensure_geodesic_array_rows(array, metric.frame)?;
            ensure_geodesic_shape(metric.frame, other_shape.shape())?;
        }
        let rows = match &resolved {
            crs::ResolvedMetric::Planar { to_metre } => {
                let mut rows = array_owned.row_operation();
                let mut scratch = crate::geometry::RowMetricPair::new();
                let fixed = storage::ShapeRow::Shape(other_shape.shape());
                let fixed_bounds = other_shape.bounds();
                let element_bounds = array.existing_element_bounds();
                let limit = distance / to_metre.get();
                try_collect_dwithin_rows(
                    array.len(),
                    rows.rows_with_missing(|| false).map(|row| {
                        let ArrayRow::Present(row) = row else {
                            return Ok(false);
                        };
                        let logical = row.logical();
                        let shape = row.row();
                        let bounds = element_bounds
                            .as_ref()
                            .and_then(|bounds| bounds.get(logical).copied().flatten())
                            .or_else(|| rows.quick_bounds_at(logical, shape));
                        if let Some(value) =
                            scratch.dwithin(shape, fixed, bounds, fixed_bounds, limit)?
                        {
                            return Ok(value);
                        }
                        let left = rows.prepared_at_with_bounds(logical, shape, bounds)?;
                        pair_dwithin_planar(to_metre.get(), &left, &other_shape, distance)
                    }),
                )
                .map_err(rows_err)?
            },
            crs::ResolvedMetric::Geodesic(metric) => {
                let other_cache = other.frame_cache();
                let mut rows = array_owned.row_operation();
                try_collect_dwithin_rows(
                    array.len(),
                    rows.rows_with_missing(|| false).map(|row| {
                        let ArrayRow::Present(row) = row else {
                            return Ok(false);
                        };
                        let row_index = row.logical();
                        let left = rows.prepared(row)?;
                        let left_cache = rows.frame_cache(row_index);
                        pair_dwithin_geodesic(
                            metric,
                            &left,
                            &left_cache,
                            &other_shape,
                            &other_cache,
                            distance,
                        )
                    }),
                )
                .map_err(rows_err)?
            },
        };
        return bool_array(py, rows);
    }
    if let crs::MetricModel::Planar { to_metre } = &model {
        let limit = distance / to_metre.get();
        if !array.is_compact_nullable()
            && let Some(values) = array.reduce_packed_points_detached(py, {
                let other_shape = other.prepared_plan();
                move |points| {
                    Ok(other_shape.dwithin_points(
                        points.present_points().map(|point| (point.x, point.y)),
                        limit,
                    )?)
                }
            })?
        {
            return bool_array_mask_missing(py, values, array.missing());
        }
        // Gathered mixed points run the same batch kernel from by-value points.
        if !array.is_compact_nullable()
            && let Some(points) = array.storage().point_rows()
        {
            let other_shape = other.prepared_plan();
            return bool_array_mask_missing(
                py,
                py.detach(move || {
                    other_shape.dwithin_points(points.iter().map(|point| (point.x, point.y)), limit)
                })?,
                array.missing(),
            );
        }
    }
    match model {
        crs::MetricModel::Planar { to_metre } => {
            let array = array.clone();
            let missing = array.missing().cloned();
            let row_count = array.len();
            let limit = distance / to_metre.get();
            let other_shape = other.prepared_plan();
            let other_bounds = other_shape.bounds();
            let element_bounds = array.existing_element_bounds();
            bool_array_mask_missing(
                py,
                py.detach(move || {
                    let mut rows = array.row_operation();
                    let mut scratch = crate::geometry::RowMetricPair::new();
                    let fixed = storage::ShapeRow::Shape(other_shape.shape());
                    try_collect_dwithin_rows(
                        row_count,
                        rows.rows_with_missing(|| false).map(|row| {
                            let ArrayRow::Present(row) = row else {
                                return Ok(false);
                            };
                            let index = row.logical();
                            let row = row.row();
                            let bounds = element_bounds
                                .as_ref()
                                .and_then(|bounds| bounds.get(index).copied().flatten())
                                .or_else(|| rows.quick_bounds_at(index, row));
                            if let Some(value) =
                                scratch.dwithin(row, fixed, bounds, other_bounds, limit)?
                            {
                                return Ok(value);
                            }
                            let left = rows.prepared_at_with_bounds(index, row, bounds)?;
                            left.dwithin(&other_shape, limit)
                        }),
                    )
                })
                .map_err(rows_err)?,
                missing.as_ref(),
            )
        },
        crs::MetricModel::Geodesic { crs, state, frame } => {
            // Packed point rows use the same threshold owner as scalar
            // geodesic point/line queries.
            if !array.has_missing()
                && let Some(values) = array.reduce_packed_points_detached(py, {
                    let crs = Arc::clone(&crs);
                    let state = Arc::clone(&state);
                    let other_shape = other.prepared_plan();
                    let other_cache = other.frame_cache();
                    move |points| {
                        let (xs, ys) = points.geodesic_xy_lanes();
                        geodesic_point_columns_dwithin_shape_values(
                            &crs,
                            &xs,
                            &ys,
                            &other_shape,
                            &other_cache,
                            distance,
                            frame,
                            &state,
                        )
                        .map_err(BatchError::Batch)
                    }
                })?
            {
                return bool_array_mask_missing(py, values, array.missing());
            }
            let array_owned = array.clone();
            let other_shape = other.prepared_plan();
            let other_cache = other.frame_cache();
            let row_count = array_owned.len();
            let rows = py.detach(move || {
                crs::with_resolved_ellipsoid_metric_at(
                    &crs,
                    &[other_shape.shape()],
                    &state,
                    |crs, metric| {
                        ensure_geodesic_array_rows(&array_owned, metric.frame())?;
                        ensure_geodesic_shape(metric.frame(), other_shape.shape())?;
                        if distance == f64::INFINITY {
                            let other_present = !other_shape.shape().is_empty();
                            let rows = array_owned.row_operation();
                            return Ok(try_collect_dwithin_rows(
                                row_count,
                                rows.rows_with_missing(|| false).map(|row| match row {
                                    ArrayRow::Present(row) => row
                                        .row()
                                        .with_shape(|shape| other_present && !shape.is_empty()),
                                    ArrayRow::Missing { value, .. } => Ok(value),
                                }),
                            ));
                        }
                        let (semi_major, flattening) = metric.ellipsoid_parameters();
                        let mut rows = array_owned.row_operation();
                        Ok(try_collect_dwithin_rows(
                            row_count,
                            rows.rows_with_missing(|| false).map(|row| {
                                let ArrayRow::Present(row) = row else {
                                    return Ok(false);
                                };
                                let row_index = row.logical();
                                let left = rows.prepared(row)?;
                                let left_cache = rows.frame_cache(row_index);
                                left.geodesic_dwithin_cached(
                                    &left_cache,
                                    &other_shape,
                                    &other_cache,
                                    crs,
                                    semi_major,
                                    flattening,
                                    metric,
                                    distance,
                                )
                            }),
                        ))
                    },
                )
            })?;
            bool_array_mask_missing(py, rows.map_err(rows_err)?, array.missing())
        },
    }
}
