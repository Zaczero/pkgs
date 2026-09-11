#![allow(
    clippy::similar_names,
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::simd::StdFloat as _;
use std::simd::cmp::{SimdPartialEq as _, SimdPartialOrd as _};

use pyo3::types::PyAny;

use crate::PackedColumnError;
use crate::array::{ArrayRow, ArrayRowOperation, ValidityMask};
use crate::broadcast::bool_array_mask_missing;
use crate::broadcast::metrics::{
    Arc, Bound, Bounds, CollectRows as _, DistanceUnit, F64Param, Frame, GeometryInput, Point, Py,
    PyGeometry, PyGeometryArray, PyResult, Python, Shape, SimilarityOp, binary_frame_crs,
    bool_array, broadcast2_geometry, classify_required, crs, float64_array,
    geodesic_point_columns_dwithin_shape_values, geodesic_point_columns_to_shape_values,
    mask_missing, pair_distance_geodesic, pair_distance_planar, pair_dwithin_geodesic,
    pair_dwithin_planar, paired_arrays, point_distance, resolve_metric, rows_err,
};
use crate::dispatch::MetricOp;
use crate::geometry::packed_line_metrics::scale_metric_values;
use crate::geometry::{
    PreparedPlan, ReduceSimd, bounds_distance_squared, pair_map4_guarded_f64, pair_select_mask,
    points_dwithin,
};

fn float64_array_mask_missing(
    py: Python<'_>,
    mut values: Vec<f64>,
    missing: Option<&ValidityMask>,
) -> PyResult<Py<PyAny>> {
    mask_missing(&mut values, missing, f64::NAN);
    float64_array(py, values)
}
/// CRS-aware array float metric used by the spine-routed Hausdorff/Fréchet
/// lanes. The operation owns both the metric identity and its densify shape;
/// row traversal only supplies the logical row's value.
#[expect(
    clippy::too_many_lines,
    reason = "scalar/array operand dispatch plus packed-line batch and per-pair lanes"
)]
pub(crate) fn array_crs_metric_float(
    py: Python<'_>,
    array: &PyGeometryArray,
    other: &Bound<'_, PyAny>,
    operation: SimilarityOp,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    let operation_name = operation.metric_op().name();
    match classify_required(other)? {
        GeometryInput::One(right) => {
            Frame::compatible_parts(
                array.crs_ref(),
                array.epoch(),
                right.crs_ref(),
                right.epoch(),
                operation_name,
            )?;
            let model = resolve_metric(array.crs_str(), unit, operation.metric_op())?;
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
                                row.row().try_with_shape(|left| {
                                    operation.eval_shapes(
                                        &model,
                                        left,
                                        right.shape(),
                                        operation.densify_at(logical),
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
            let (lefts, rights) = paired_arrays(array, right, operation_name)?;
            let model = resolve_metric(array.crs_str(), unit, operation.metric_op())?;
            let missing = crate::array::missing::union_pair(array.missing(), right.missing())?;
            if Arc::ptr_eq(&lefts, &rights)
                && let Some(values) =
                    operation.try_same_storage_identity(&model, &lefts, missing.as_ref())?
            {
                return float64_array_mask_missing(py, values, missing.as_ref());
            }
            if let crs::MetricModel::Planar { to_metre } = &model
                && operation.scalar_densify().is_none()
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
                            let left_lines = lefts.line_rows().expect("checked above");
                            let right_lines = rights.line_rows().expect("checked above");
                            let left_view = left_lines.packed_column_view().expect("checked above");
                            let right_view =
                                right_lines.packed_column_view().expect("checked above");
                            operation.packed_batch(left_view, right_view, len, scale)
                        })?,
                    );
                }
                let operation = operation;
                return float64_array(
                    py,
                    py.detach(move || {
                        let left_lines = lefts.line_rows().expect("checked above");
                        let right_lines = rights.line_rows().expect("checked above");
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
                                    && let Some(value) = operation.identical_window_value(
                                        left_xs,
                                        left_ys,
                                        right_xs,
                                        right_ys,
                                        to_metre.get(),
                                    )
                                {
                                    return value;
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
                                left.row().try_with_shape(|left| {
                                    right.row().try_with_shape(|right| {
                                        operation.eval_shapes(
                                            &model,
                                            left,
                                            right,
                                            operation.densify_at(logical),
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

/// CRS-aware binary geometry metric broadcast (behind `shortest_line`).
pub(crate) fn crs_metric_binary_geometry_broadcast(
    py: Python<'_>,
    left: &Bound<'_, PyAny>,
    right: &Bound<'_, PyAny>,
    operation: MetricOp,
    unit: Option<DistanceUnit>,
    kernel: impl Fn(&crs::MetricModel, &PreparedPlan, &PreparedPlan) -> crate::error::Result<Shape>
    + Send
    + Sync,
) -> PyResult<Py<PyAny>> {
    let model = resolve_metric(binary_frame_crs(left, right).as_deref(), unit, operation)?;
    broadcast2_geometry(py, left, right, operation.name(), move |a, b| {
        kernel(&model, a, b)
    })
}

/// Element-wise lane ladder for array × array CRS-aware metrics, shared by
/// `distance` and `dwithin`: packed points stream shared columns through
/// `packed_points` with the GIL released (one kernel call per pair, no `Shape`
/// wrapping); general rows run `pair` over prepared plans.
fn array_pair_metric<T, P, C, B, F>(
    py: Python<'_>,
    array: &PyGeometryArray,
    right: &PyGeometryArray,
    packed_points: Option<P>,
    packed_columns: Option<C>,
    bounds_refute: Option<B>,
    missing_value: T,
    pair: F,
) -> PyResult<Vec<T>>
where
    T: Copy + Send + 'static,
    P: Fn(Point, Point) -> crate::error::Result<T> + Send,
    C: Fn(&[f64], &[f64], &[f64], &[f64]) -> crate::error::Result<Vec<T>> + Send,
    B: Fn(Bounds, Bounds) -> Option<T> + Send + Sync,
    F: Fn(
            usize,
            &PreparedPlan,
            &PreparedPlan,
            &mut ArrayRowOperation<'_>,
            &mut ArrayRowOperation<'_>,
        ) -> crate::error::Result<T>
        + Send
        + Sync,
{
    let missing = crate::array::missing::union_pair(array.missing(), right.missing())?;
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
    // Row pairs ride prepared handles; dwithin can settle far-apart boxes
    // before materializing both operands' prepared plans + distance parts.
    if let Some(refute) = bounds_refute
        && let (Some(left_bounds), Some(right_bounds)) = (
            array.existing_element_bounds(),
            right.existing_element_bounds(),
        )
    {
        let left_array = array.clone();
        let right_array = right.clone();
        return py
            .detach(move || {
                let mut left_rows = left_array.row_operation();
                let mut right_rows = right_array.row_operation();
                left_rows
                    .rows_with_missing(|| missing_value)
                    .zip(right_rows.rows_with_missing(|| missing_value))
                    .map(|(left, right)| {
                        let (ArrayRow::Present(left_row), ArrayRow::Present(right_row)) =
                            (left, right)
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
                        let left_row = left_row.row();
                        let right_row = right_row.row();
                        if let (Some(lb), Some(rb)) = (left_bounds[index], right_bounds[index])
                            && let Some(verdict) = refute(lb, rb)
                        {
                            return Ok(verdict);
                        }
                        let left = left_rows.prepared_at(index, left_row)?;
                        let right = right_rows.prepared_at(index, right_row)?;
                        pair(index, &left, &right, &mut left_rows, &mut right_rows)
                    })
                    .collect_rows()
            })
            .map_err(rows_err);
    }
    let left_array = array.clone();
    let right_array = right.clone();
    py.detach(move || {
        let mut left_rows = left_array.row_operation();
        let mut right_rows = right_array.row_operation();
        left_rows
            .rows_with_missing(|| missing_value)
            .zip(right_rows.rows_with_missing(|| missing_value))
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
                let left = left_rows.prepared(left_row)?;
                let right = right_rows.prepared(right_row)?;
                pair(index, &left, &right, &mut left_rows, &mut right_rows)
            })
            .collect_rows()
    })
    .map_err(rows_err)
}

/// Per-element CRS-aware distances from `array` to `other` (scalar or array).
#[expect(
    clippy::too_many_lines,
    reason = "one exhaustive owner keeps operand classification, metric resolution, and packed fast paths consistent"
)]
pub(crate) fn array_crs_distances(
    py: Python<'_>,
    array: &PyGeometryArray,
    other: &Bound<'_, PyAny>,
    operation: MetricOp,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    // Operand classification and structural checks (length, frame) come
    // BEFORE metric-model resolution, so an invalid pairing reports as such
    // even when the array's CRS cannot back a metric.
    match classify_required(other)? {
        GeometryInput::One(right) => {
            Frame::compatible_parts(
                array.crs_ref(),
                array.epoch(),
                right.crs_ref(),
                right.epoch(),
                operation.name(),
            )?;
            let model = resolve_metric(array.crs_str(), unit, operation)?;
            // Planar scalar-vs-array: build the fixed operand's distance parts ONCE
            // and reuse them across every element (a large fixed geometry otherwise
            // rebuilds its vertex/segment sets per pair).
            if let crs::MetricModel::Planar { to_metre } = model {
                let right_shape = right.plan_or_transient()?;
                if !array.is_compact_nullable()
                    && let Some(values) = array.reduce_packed_points_detached(py, {
                        let right_shape = Arc::clone(&right_shape);
                        move |points| {
                            let mut values = right_shape
                                .distance_points(points.iter().map(|point| (point.x, point.y)))?;
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
                    let right_shape = right.plan_or_transient()?;
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
                let right_shape = right.plan_or_transient()?;
                return float64_array(
                    py,
                    py.detach(move || {
                        let mut rows = array.row_operation();
                        rows.rows_with_missing(|| f64::NAN)
                            .map(|logical_row| {
                                let ArrayRow::Present(row) = logical_row else {
                                    if let ArrayRow::Missing { value, .. } = logical_row {
                                        return Ok(value);
                                    }
                                    unreachable!();
                                };
                                let left = rows.prepared(row)?;
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
                    let crs = crs.clone();
                    let state = Arc::clone(state);
                    let target = *target;
                    move |points| {
                        let (xs, ys) = points
                            .geodesic_xy_lanes()
                            .map_err(PackedColumnError::Batch)?;
                        crs::geodesic_point_distances(&crs, &xs, &ys, target, *frame, &state)
                            .map_err(PackedColumnError::Batch)
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
                    let crs = crs.clone();
                    let state = Arc::clone(state);
                    let right_shape = right.plan_or_transient()?;
                    let right_cache = right.frame_cache()?;
                    move |points| {
                        let (xs, ys) = points
                            .geodesic_xy_lanes()
                            .map_err(PackedColumnError::Batch)?;
                        geodesic_point_columns_to_shape_values(
                            &crs,
                            &xs,
                            &ys,
                            &right_shape,
                            &right_cache,
                            *frame,
                            &state,
                        )
                        .map_err(PackedColumnError::Batch)
                    }
                })?
            {
                return float64_array(py, values);
            }
            // Geodesic scalar-fixed general rows: resolve the CRS/ellipsoid once,
            // build the fixed operand's geodesic parts once, then let every array
            // row use its own persistent handle cache.
            if let crs::MetricModel::Geodesic { crs, state, .. } = model {
                let array = array.clone();
                let right_shape = right.plan_or_transient()?;
                let right_cache = right.frame_cache()?;
                let rows = py.detach(move || {
                    let mut rows = array.row_operation();
                    crs::with_resolved_ellipsoid_metric_at(
                        &crs,
                        &[right_shape.shape()],
                        &state,
                        |crs, metric| {
                            let (semi_major, flattening) = metric.ellipsoid_parameters();
                            right_shape.prepare_geodesic_parts(
                                &right_cache,
                                crs,
                                semi_major,
                                flattening,
                                metric,
                            )?;
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
                                    let left_cache = rows.frame_cache(row_index)?;
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
            paired_arrays(array, right, operation.name())?;
            let model = resolve_metric(array.crs_str(), unit, operation)?;
            // Planar packed × packed: pure column zip — one guarded-sqrt distance
            // per pair (`hypot` is a libm call at every x86-64 level), no
            // per-pair `Shape` wrapping, GIL released (shapely's vectorized C
            // loop is the bar here).
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
                                    let dx = lx - rx;
                                    let dy = ly - ry;
                                    let squared = dx * dx + dy * dy;
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
                            None::<fn(Bounds, Bounds) -> Option<f64>>,
                            f64::NAN,
                            |_row, left, right_data, _left_products, _right_products| {
                                pair_distance_planar(to_metre.get(), left, right_data)
                            },
                        )?,
                    )
                },
                crs::ResolvedMetric::Geodesic(metric) => {
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
                            None::<fn(Bounds, Bounds) -> Option<f64>>,
                            f64::NAN,
                            |row, left, right_data, left_products, right_products| {
                                pair_distance_geodesic(
                                    metric,
                                    left,
                                    &*left_products.frame_cache(row)?,
                                    right_data,
                                    &*right_products.frame_cache(row)?,
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
/// distance — the geodesic path still needs the full distance.
#[expect(
    clippy::too_many_lines,
    reason = "planar vs geodesic dwithin branches share one metric resolution; splitting would re-resolve CRS"
)]
pub(crate) fn array_crs_dwithin(
    py: Python<'_>,
    array: &PyGeometryArray,
    other: &Bound<'_, PyAny>,
    distance: f64,
    operation: MetricOp,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    match classify_required(other)? {
        GeometryInput::One(right) => {
            array_crs_dwithin_scalar(py, array, right, distance, operation, unit)
        },
        GeometryInput::Many(right) => {
            // Structural checks (length, frame) precede metric-model resolution
            // — see `array_crs_distances`.
            paired_arrays(array, right, operation.name())?;
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
                                        let dx = lx - rx;
                                        let dy = ly - ry;
                                        (dx * dx + dy * dy).simd_le(ReduceSimd::splat(limit_sq))
                                    },
                                );
                            } else {
                                for (index, slot) in out.iter_mut().enumerate() {
                                    *slot = points_dwithin(
                                        Point::new_raw_xy(lx[index], ly[index]),
                                        Point::new_raw_xy(rx[index], ry[index]),
                                        limit,
                                    );
                                }
                            }
                            Ok(out)
                        });
                    // Planar box-separation refuter: boxes farther apart (squared)
                    // than the planar limit can never be within it.
                    let bounds_refute =
                        limit_sq
                            .is_finite()
                            .then_some(move |left: Bounds, right: Bounds| {
                                (bounds_distance_squared(left, right) > limit_sq).then_some(false)
                            });
                    bool_array(
                        py,
                        array_pair_metric(
                            py,
                            array,
                            right,
                            packed_planar,
                            packed_columns,
                            bounds_refute,
                            false,
                            |_row, left, right_data, _left_products, _right_products| {
                                pair_dwithin_planar(to_metre.get(), left, right_data, distance)
                            },
                        )?,
                    )
                },
                crs::ResolvedMetric::Geodesic(metric) => {
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
                            None::<fn(Bounds, Bounds) -> Option<bool>>,
                            false,
                            |row, left, right_data, left_products, right_products| {
                                pair_dwithin_geodesic(
                                    metric,
                                    left,
                                    &*left_products.frame_cache(row)?,
                                    right_data,
                                    &*right_products.frame_cache(row)?,
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

/// Boxes farther apart than the threshold can never be within it.
///
/// The constant-threshold lane installs this same refutation as a closure over
/// one precomputed square. Here the threshold varies per row, so the square is
/// formed per row as well.
fn dwithin_box_refutes(left: Option<Bounds>, right: Option<Bounds>, limit: f64) -> bool {
    let (Some(left), Some(right)) = (left, right) else {
        return false;
    };
    let limit_sq = limit * limit;
    limit_sq.is_finite() && bounds_distance_squared(left, right) > limit_sq
}

/// Per-element `dwithin`: one distance threshold PER geometry (the cold
/// scalar-or-array lane; a scalar keeps the optimized [`array_crs_dwithin`]
/// packed/refuter path). Runs under the GIL — the geodesic pair kernel builds
/// `PyErr` — which is fine for this rare per-row form. Mirrors the scalar
/// dispatch's scalar-operand / array-operand split.
pub(crate) fn array_crs_dwithin_per_element(
    py: Python<'_>,
    array: &PyGeometryArray,
    other: &Bound<'_, PyAny>,
    distance: &F64Param,
    operation: MetricOp,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    match classify_required(other)? {
        GeometryInput::One(right) => {
            Frame::compatible_parts(
                array.crs_ref(),
                array.epoch(),
                right.crs_ref(),
                right.epoch(),
                operation.name(),
            )?;
            let model = resolve_metric(array.crs_str(), unit, operation)?;
            let resolved = crs::ResolvedMetric::from_model(&model)?;
            let right_plan = right.plan_or_transient()?;
            let right_cache = right.frame_cache()?;
            let results: Vec<bool> = match &resolved {
                crs::ResolvedMetric::Planar { to_metre } => {
                    let right_bounds = right_plan.bounds();
                    let left_bounds = array.existing_element_bounds();
                    let mut rows = array.row_operation();
                    rows.rows_with_missing(|| false)
                        .map(|left| {
                            let ArrayRow::Present(left) = left else {
                                return Ok(false);
                            };
                            let row = left.logical();
                            let left = left.row();
                            if dwithin_box_refutes(
                                left_bounds
                                    .as_ref()
                                    .and_then(|bounds| bounds.get(row).copied().flatten())
                                    .or_else(|| left.quick_bounds()),
                                right_bounds,
                                distance.get(row) / to_metre.get(),
                            ) {
                                return Ok(false);
                            }
                            let left = rows.prepared_at(row, left)?;
                            pair_dwithin_planar(
                                to_metre.get(),
                                &left,
                                &right_plan,
                                distance.get(row),
                            )
                            .map_err(Into::into)
                        })
                        .collect::<PyResult<_>>()?
                },
                crs::ResolvedMetric::Geodesic(metric) => {
                    let mut rows = array.row_operation();
                    rows.rows_with_missing(|| false)
                        .map(|left| {
                            let ArrayRow::Present(left) = left else {
                                return Ok(false);
                            };
                            let row = left.logical();
                            let left = rows.prepared(left)?;
                            let left_cache = rows.frame_cache(row)?;
                            pair_dwithin_geodesic(
                                metric,
                                &left,
                                &left_cache,
                                &right_plan,
                                &right_cache,
                                distance.get(row),
                            )
                            .map_err(Into::into)
                        })
                        .collect::<PyResult<_>>()?
                },
            };
            bool_array(py, results)
        },
        GeometryInput::Many(right) => {
            paired_arrays(array, right, operation.name())?;
            let model = resolve_metric(array.crs_str(), unit, operation)?;
            let resolved = crs::ResolvedMetric::from_model(&model)?;
            let results: Vec<bool> = match &resolved {
                crs::ResolvedMetric::Planar { to_metre } => {
                    let left_bounds = array.existing_element_bounds();
                    let right_bounds = right.existing_element_bounds();
                    let mut left_rows = array.row_operation();
                    let mut right_rows = right.row_operation();
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
                            if dwithin_box_refutes(
                                left_bounds
                                    .as_ref()
                                    .and_then(|bounds| bounds.get(row).copied().flatten())
                                    .or_else(|| left.quick_bounds()),
                                right_bounds
                                    .as_ref()
                                    .and_then(|bounds| bounds.get(row).copied().flatten())
                                    .or_else(|| right_row.quick_bounds()),
                                distance.get(row) / to_metre.get(),
                            ) {
                                return Ok(false);
                            }
                            let left = left_rows.prepared_at(row, left)?;
                            let right_data = right_rows.prepared_at(row, right_row)?;
                            pair_dwithin_planar(
                                to_metre.get(),
                                &left,
                                &right_data,
                                distance.get(row),
                            )
                            .map_err(Into::into)
                        })
                        .collect::<PyResult<_>>()?
                },
                crs::ResolvedMetric::Geodesic(metric) => {
                    let mut left_rows = array.row_operation();
                    let mut right_rows = right.row_operation();
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
                            let left_cache = left_rows.frame_cache(row)?;
                            let right_cache = right_rows.frame_cache(row)?;
                            pair_dwithin_geodesic(
                                metric,
                                &left,
                                &left_cache,
                                &right_data,
                                &right_cache,
                                distance.get(row),
                            )
                            .map_err(Into::into)
                        })
                        .collect::<PyResult<_>>()?
                },
            };
            bool_array(py, results)
        },
    }
}

/// `array_crs_dwithin` against one fixed scalar operand: the frame is checked
/// once at the array level, the fixed operand's distance parts and geo tree
/// are built once, and the planar path runs with the GIL released.
#[expect(
    clippy::too_many_lines,
    reason = "one exhaustive owner keeps operand classification, metric resolution, and packed fast paths consistent"
)]
pub(crate) fn array_crs_dwithin_scalar(
    py: Python<'_>,
    array: &PyGeometryArray,
    other: &PyGeometry,
    distance: f64,
    operation: MetricOp,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    let operation_name = operation.name();
    Frame::compatible_parts(
        array.crs_ref(),
        array.epoch(),
        other.crs_ref(),
        other.epoch(),
        operation_name,
    )?;
    let model = resolve_metric(array.crs_str(), unit, operation)?;
    // The physical storage uses a finite NaN placeholder for missing rows.
    // Once a mask exists, enter the row lane before any packed reduction,
    // point materialization, bounds read, or kernel call; masking the result
    // afterwards is too late for kernels that assert finite coordinates.
    if array.has_missing() {
        let resolved = crs::ResolvedMetric::from_model(&model)?;
        let array_owned = array.clone();
        let other_shape = other.plan_or_transient()?;
        let other_cache = other.frame_cache()?;
        let rows = match &resolved {
            crs::ResolvedMetric::Planar { to_metre } => {
                let mut rows = array_owned.row_operation();
                rows.rows_with_missing(|| false)
                    .map(|row| {
                        let ArrayRow::Present(row) = row else {
                            return Ok(false);
                        };
                        let left = rows.prepared(row)?;
                        pair_dwithin_planar(to_metre.get(), &left, &other_shape, distance)
                            .map_err(Into::into)
                    })
                    .collect::<PyResult<Vec<_>>>()?
            },
            crs::ResolvedMetric::Geodesic(metric) => {
                let mut rows = array_owned.row_operation();
                rows.rows_with_missing(|| false)
                    .map(|row| {
                        let ArrayRow::Present(row) = row else {
                            return Ok(false);
                        };
                        let row_index = row.logical();
                        let left = rows.prepared(row)?;
                        let left_cache = rows.frame_cache(row_index)?;
                        pair_dwithin_geodesic(
                            metric,
                            &left,
                            &left_cache,
                            &other_shape,
                            &other_cache,
                            distance,
                        )
                        .map_err(Into::into)
                    })
                    .collect::<PyResult<Vec<_>>>()?
            },
        };
        return bool_array(py, rows);
    }
    if let crs::MetricModel::Planar { to_metre } = &model {
        let limit = distance / to_metre.get();
        if !array.is_compact_nullable()
            && let Some(values) = array.reduce_packed_points_detached(py, {
                let other_shape = other.plan_or_transient()?;
                move |points| {
                    Ok(other_shape
                        .dwithin_points(points.iter().map(|point| (point.x, point.y)), limit)?)
                }
            })?
        {
            return bool_array_mask_missing(py, values, array.missing());
        }
        // Gathered mixed points run the same batch kernel from by-value points.
        if !array.is_compact_nullable()
            && let Some(points) = array.storage().point_rows()
        {
            let other_shape = other.plan_or_transient()?;
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
            let limit = distance / to_metre.get();
            let other_shape = other.plan_or_transient()?;
            // Box-separation reject against the FIXED operand's box, from the
            // array's batch per-element boxes: an element whose box sits farther
            // (squared) than the limit is settled false before materialization.
            let sq_limit = (limit * limit).is_finite().then_some(limit * limit);
            let other_bounds = other_shape.bounds();
            let element_bounds = array.existing_element_bounds();
            bool_array_mask_missing(
                py,
                py.detach(move || {
                    let mut rows = array.row_operation();
                    rows.rows_with_missing(|| false)
                        .map(|row| {
                            let ArrayRow::Present(row) = row else {
                                return Ok(false);
                            };
                            let index = row.logical();
                            let row = row.row();
                            if let (Some(sq_limit), Some(other_bounds), Some(element_bounds)) =
                                (sq_limit, other_bounds, element_bounds.as_ref())
                                && let Some(element) = element_bounds[index]
                                && bounds_distance_squared(element, other_bounds) > sq_limit
                            {
                                return Ok(false);
                            }
                            let left = rows.prepared_at(index, row)?;
                            left.dwithin(&other_shape, limit)
                        })
                        .collect::<crate::error::Result<Vec<bool>>>()
                })?,
                missing.as_ref(),
            )
        },
        crs::MetricModel::Geodesic { crs, state, frame } => {
            // Packed point lane: one geodesic cache resolve, raw columns
            // through the same point kernel (mirrors the distances lane).
            if let Shape::Point(target) = other.shape.shape()
                && !array.has_missing()
                && let Some(values) = array.reduce_packed_points_detached(py, {
                    let crs = crs.clone();
                    let state = Arc::clone(&state);
                    let target = *target;
                    move |points| {
                        let (xs, ys) = points
                            .geodesic_xy_lanes()
                            .map_err(PackedColumnError::Batch)?;
                        crs::geodesic_point_distances(&crs, &xs, &ys, target, frame, &state)
                            .map_err(PackedColumnError::Batch)
                    }
                })?
            {
                return bool_array_mask_missing(
                    py,
                    values
                        .into_iter()
                        .map(|value| value <= distance)
                        .collect::<Vec<_>>(),
                    array.missing(),
                );
            }
            if !array.has_missing()
                && let Some(values) = array.reduce_packed_points_detached(py, {
                    let crs = crs.clone();
                    let state = Arc::clone(&state);
                    let other_shape = other.plan_or_transient()?;
                    let other_cache = other.frame_cache()?;
                    move |points| {
                        let (xs, ys) = points
                            .geodesic_xy_lanes()
                            .map_err(PackedColumnError::Batch)?;
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
                        .map_err(PackedColumnError::Batch)
                    }
                })?
            {
                return bool_array_mask_missing(py, values, array.missing());
            }
            let array_owned = array.clone();
            let other_shape = other.plan_or_transient()?;
            let other_cache = other.frame_cache()?;
            let rows = py.detach(move || {
                let mut rows = array_owned.row_operation();
                crs::with_resolved_ellipsoid_metric_at(
                    &crs,
                    &[other_shape.shape()],
                    &state,
                    |crs, metric| {
                        let (semi_major, flattening) = metric.ellipsoid_parameters();
                        other_shape.prepare_geodesic_parts(
                            &other_cache,
                            crs,
                            semi_major,
                            flattening,
                            metric,
                        )?;
                        Ok(rows
                            .rows_with_missing(|| false)
                            .map(|row| {
                                let ArrayRow::Present(row) = row else {
                                    return Ok(false);
                                };
                                let row_index = row.logical();
                                let left = rows.prepared(row)?;
                                let left_cache = rows.frame_cache(row_index)?;
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
                            })
                            .collect_rows())
                    },
                )
            })?;
            bool_array_mask_missing(py, rows.map_err(rows_err)?, array.missing())
        },
    }
}
