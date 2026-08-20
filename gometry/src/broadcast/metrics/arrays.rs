#![allow(
    clippy::similar_names,
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::simd::StdFloat as _;
use std::simd::cmp::{SimdPartialEq as _, SimdPartialOrd as _};

use pyo3::types::PyAny;

use crate::PackedColumnError;
use crate::array::MissingMask;
use crate::broadcast::bool_array_mask_missing;
use crate::broadcast::metrics::{
    Arc, Bound, Bounds, CollectRows as _, DistanceUnit, F64Param, Frame, GeometryArrayStorage,
    GeometryInput, Point, Py, PyGeometry, PyGeometryArray, PyResult, Python, Shape, ShapeData,
    binary_frame_crs, bool_array, broadcast2_geometry, classify_required, crs,
    finite_geodesic_value, float64_array, geodesic_point_columns_dwithin_shape_values,
    geodesic_point_columns_to_shape_values, mask_missing, pair_distance_resolved_result,
    pair_dwithin_resolved, pair_dwithin_resolved_result, paired_arrays, point_distance,
    resolve_metric, rows_err, same_storage_similarity_metric_zeros,
};
use crate::geometry::packed_line_metrics::scale_metric_values;
use crate::geometry::{
    ReduceSimd, bounds_distance_squared, pair_map4_guarded_f64, pair_select_mask, points_dwithin,
};

fn float64_array_mask_missing(
    py: Python<'_>,
    mut values: Vec<f64>,
    missing: Option<&MissingMask>,
) -> PyResult<Py<PyAny>> {
    mask_missing(&mut values, missing, f64::NAN);
    float64_array(py, values)
}
/// CRS-aware array float metric used by the spine-routed Hausdorff/Fréchet lanes.
#[expect(
    clippy::too_many_lines,
    reason = "scalar/array operand dispatch plus packed-line batch and per-pair lanes"
)]
pub(crate) fn array_crs_metric_float(
    py: Python<'_>,
    array: &PyGeometryArray,
    other: &Bound<'_, PyAny>,
    operation: &str,
    unit: Option<DistanceUnit>,
    packed_planar_lines: Option<
        impl Fn(&[f64], &[f64], &[f64], &[f64]) -> crate::error::Result<f64> + Send + Sync,
    >,
    kernel: impl Fn(&crs::MetricModel, &Shape, &Shape) -> crate::error::Result<f64> + Send + Sync,
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
            let lefts = Arc::clone(array.storage_arc());
            let right = Arc::clone(&right.shape);
            let missing = array.missing().cloned();
            float64_array(
                py,
                py.detach(move || {
                    lefts
                        .iter_rows()
                        .enumerate()
                        .map(|(row_index, row)| {
                            if missing.as_ref().is_some_and(|mask| mask[row_index]) {
                                return Ok(f64::NAN);
                            }
                            row.with_shape(|left| kernel(&model, left, right.shape()))
                        })
                        .collect_rows()
                })
                .map_err(rows_err)?,
            )
        },
        Many(right) => {
            let (lefts, rights) = paired_arrays(array, right, operation)?;
            let missing = crate::array::missing::union_pair(array.missing(), right.missing());
            let model = resolve_metric(array.crs_str(), unit, operation)?;
            if Arc::ptr_eq(&lefts, &rights)
                && operation == "hausdorff_distance"
                && crate::array::row_map_is_identity(match &*lefts {
                    GeometryArrayStorage::Lines { row_map, .. } => row_map.as_deref(),
                    _ => crate::array::RowSelectionRef::Identity,
                })
            {
                // Domain validation before the identity zeros path: out-of-
                // domain latitudes must still raise under a geodesic model.
                if matches!(model, crs::MetricModel::Geodesic(_)) {
                    for (row_index, row) in lefts.iter_rows().enumerate() {
                        if missing.as_ref().is_some_and(|mask| mask[row_index]) {
                            continue;
                        }
                        row.with_shape(crs::ensure_geographic_domain)?;
                    }
                }
                return float64_array_mask_missing(
                    py,
                    same_storage_similarity_metric_zeros(&lefts),
                    missing.as_ref(),
                );
            }
            if let crs::MetricModel::Planar { to_metre } = &model
                && let (Some(left_lines), Some(right_lines)) =
                    (lefts.line_rows(), rights.line_rows())
                && left_lines.is_packed_pair(&right_lines)
            {
                let frechet_has_empty_line = operation == "frechet_distance"
                    && (0..left_lines.len()).any(|row| {
                        left_lines.row_xy(row).0.is_empty() || right_lines.row_xy(row).0.is_empty()
                    });
                if operation == "hausdorff_distance" && missing.is_none() {
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
                            let mut values = vec![0.0; len];
                            crate::geometry::hausdorff_distance_line_columns_batch(
                                left_view,
                                right_view,
                                &mut values,
                            );
                            crate::geometry::scale_metric_values(&mut values, scale);
                            values
                        }),
                    );
                }
                if operation == "frechet_distance" && missing.is_none() && !frechet_has_empty_line {
                    let len = left_lines.len();
                    let scale = to_metre.get();
                    let values = py.detach(move || {
                        let left_lines = lefts.line_rows().expect("checked above");
                        let right_lines = rights.line_rows().expect("checked above");
                        let left_view = left_lines.packed_column_view().expect("checked above");
                        let right_view = right_lines.packed_column_view().expect("checked above");
                        let mut values = vec![0.0; len];
                        crate::geometry::frechet_distance_line_columns_batch(
                            left_view,
                            right_view,
                            &mut values,
                        );
                        crate::geometry::scale_metric_values(&mut values, scale);
                        values
                    });
                    return float64_array(py, values);
                }
                if let Some(line_kernel) = packed_planar_lines {
                    let is_frechet = operation == "frechet_distance";
                    return float64_array(
                        py,
                        py.detach(move || {
                            let left_lines = lefts.line_rows().expect("checked above");
                            let right_lines = rights.line_rows().expect("checked above");
                            (0..left_lines.len())
                                .map(|index| {
                                    if missing.as_ref().is_some_and(|mask| mask[index]) {
                                        return Ok(f64::NAN);
                                    }
                                    let (left_xs, left_ys) = left_lines.row_xy(index);
                                    let (right_xs, right_ys) = right_lines.row_xy(index);
                                    if std::ptr::eq(left_xs.as_ptr(), right_xs.as_ptr())
                                        && std::ptr::eq(left_ys.as_ptr(), right_ys.as_ptr())
                                        && left_xs.len() == right_xs.len()
                                    {
                                        if left_xs.is_empty() && is_frechet {
                                            return line_kernel(
                                                left_xs, left_ys, right_xs, right_ys,
                                            )
                                            .map(|value| value * to_metre.get());
                                        }
                                        return Ok(if left_xs.is_empty() {
                                            f64::INFINITY
                                        } else {
                                            0.0
                                        });
                                    }
                                    line_kernel(left_xs, left_ys, right_xs, right_ys)
                                        .map(|value| value * to_metre.get())
                                })
                                .collect_rows()
                        })
                        .map_err(rows_err)?,
                    );
                }
            }
            float64_array(
                py,
                py.detach(move || {
                    lefts
                        .iter_shapes()
                        .zip(rights.iter_shapes())
                        .enumerate()
                        .map(|(row, (left, right))| {
                            if missing.as_ref().is_some_and(|mask| mask[row]) {
                                return Ok(f64::NAN);
                            }
                            kernel(&model, &left, &right)
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
    operation: &str,
    unit: Option<DistanceUnit>,
    kernel: impl Fn(&crs::MetricModel, &ShapeData, &ShapeData) -> crate::error::Result<Shape>
    + Send
    + Sync,
) -> PyResult<Py<PyAny>> {
    let model = resolve_metric(binary_frame_crs(left, right).as_deref(), unit, operation)?;
    broadcast2_geometry(py, left, right, operation, move |a, b| kernel(&model, a, b))
}

/// Element-wise lane ladder for array × array CRS-aware metrics, shared by
/// `distance` and `dwithin`: planar packed × packed streams both shared
/// columns through `packed_planar` with the GIL released (one kernel call per
/// pair, no `Shape` wrapping); remaining all-`Point` pairings and general
/// rows run `pair` over per-row shapes.
#[expect(
    clippy::too_many_lines,
    reason = "the generic pair loop keeps bounds, parallel execution, and missing-value scattering together"
)]
fn array_pair_metric<T, P, C, B, F>(
    py: Python<'_>,
    array: &PyGeometryArray,
    right: &PyGeometryArray,
    packed_planar: Option<P>,
    packed_columns: Option<C>,
    bounds_refute: Option<B>,
    missing_value: T,
    pair: F,
) -> PyResult<Vec<T>>
where
    T: Copy + Send + 'static,
    P: Fn(Point, Point) -> T + Send,
    C: Fn(&[f64], &[f64], &[f64], &[f64]) -> crate::error::Result<Vec<T>> + Send,
    B: Fn(Bounds, Bounds) -> Option<T> + Send + Sync,
    F: Fn(usize, &ShapeData, &ShapeData) -> crate::error::Result<T> + Send + Sync,
{
    let missing = crate::array::missing::union_pair(array.missing(), right.missing());
    if let (Some(lefts), Some(rights)) =
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
        if let Some(kernel) = packed_planar {
            return Ok(py.detach(move || {
                lefts
                    .iter()
                    .zip(rights.iter())
                    .enumerate()
                    .map(|(row, (left, right))| {
                        if missing.as_ref().is_some_and(|mask| mask[row]) {
                            missing_value
                        } else {
                            kernel(left, right)
                        }
                    })
                    .collect()
            }));
        }
        // Remaining all-`Point` pairings (mixed storage, geodesic model).
        return py
            .detach(move || {
                lefts
                    .iter()
                    .zip(rights.iter())
                    .enumerate()
                    .map(|(row, (left, right))| {
                        if missing.as_ref().is_some_and(|mask| mask[row]) {
                            return Ok(missing_value);
                        }
                        pair(
                            row,
                            &ShapeData::new(Shape::Point(left)),
                            &ShapeData::new(Shape::Point(right)),
                        )
                    })
                    .collect_rows()
            })
            .map_err(rows_err);
    }
    // Row pairs ride prepared handles; dwithin can settle far-apart boxes
    // before materializing both operands' `ShapeData` + distance parts.
    if let Some(refute) = bounds_refute
        && let (Some(left_bounds), Some(right_bounds)) =
            (array.cached_element_bounds(), right.cached_element_bounds())
    {
        let left_array = array.clone();
        let right_array = right.clone();
        return py
            .detach(move || {
                left_array
                    .storage()
                    .iter_rows()
                    .zip(right_array.storage().iter_rows())
                    .enumerate()
                    .map(|(index, (left_row, right_row))| {
                        if missing.as_ref().is_some_and(|mask| mask[index]) {
                            return Ok(missing_value);
                        }
                        if let (Some(lb), Some(rb)) = (left_bounds[index], right_bounds[index])
                            && let Some(verdict) = refute(lb, rb)
                        {
                            return Ok(verdict);
                        }
                        let left = left_array.prepared_row(index, left_row);
                        let right = right_array.prepared_row(index, right_row);
                        pair(index, &left, &right)
                    })
                    .collect_rows()
            })
            .map_err(rows_err);
    }
    let left_array = array.clone();
    let right_array = right.clone();
    py.detach(move || {
        left_array
            .storage()
            .iter_rows()
            .zip(right_array.storage().iter_rows())
            .enumerate()
            .map(|(index, (left_row, right_row))| {
                if missing.as_ref().is_some_and(|mask| mask[index]) {
                    return Ok(missing_value);
                }
                let left = left_array.prepared_row(index, left_row);
                let right = right_array.prepared_row(index, right_row);
                pair(index, &left, &right)
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
    operation: &str,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    // Operand classification and structural checks (length, frame) come
    // BEFORE metric-model resolution, so an invalid pairing reports as such
    // even when the array's CRS cannot back a metric.
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
            // Planar scalar-vs-array: build the fixed operand's distance parts ONCE
            // and reuse them across every element (a large fixed geometry otherwise
            // rebuilds its vertex/segment sets per pair).
            if let crs::MetricModel::Planar { to_metre } = model {
                let right_shape = Arc::clone(&right.shape);
                if let Some(values) = array.reduce_packed_points_detached(py, {
                    let right_shape = Arc::clone(&right_shape);
                    move |xs, ys| {
                        let mut values = right_shape.distance_points(std::iter::zip(
                            xs.iter().copied(),
                            ys.iter().copied(),
                        ));
                        for value in &mut values {
                            *value *= to_metre.get();
                        }
                        Ok(values)
                    }
                })? {
                    return float64_array(py, values);
                }
                // Gathered mixed points run the same batch kernel from by-value points.
                if let Some(points) = array.storage().point_rows() {
                    let right_shape = Arc::clone(&right.shape);
                    return float64_array_mask_missing(
                        py,
                        py.detach(move || {
                            let mut values = right_shape
                                .distance_points(points.iter().map(|point| (point.x, point.y)));
                            for value in &mut values {
                                *value *= to_metre.get();
                            }
                            values
                        }),
                        array.missing(),
                    );
                }
                // The fixed operand's prepared state (parts + facet trees) builds
                // once on its handle and serves every element; each element's
                // state persists on ITS handle for later operations. Pure Rust
                // work — run it detached.
                let array = array.clone();
                let missing = array.missing().cloned();
                let right_shape = Arc::clone(&right.shape);
                return float64_array_mask_missing(
                    py,
                    py.detach(move || {
                        array
                            .storage()
                            .iter_rows()
                            .enumerate()
                            .map(|(row, row_shape)| {
                                let left = array.prepared_row(row, row_shape);
                                left.distance(&right_shape) * to_metre.get()
                            })
                            .collect::<Vec<f64>>()
                    }),
                    missing.as_ref(),
                );
            }
            // Geodesic scalar-fixed packed points: the geodesic cache resolves
            // ONCE and raw columns stream through the same point kernel (the
            // per-row lane re-resolves the CRS cache for every pair).
            if let (crs::MetricModel::Geodesic(crs), Shape::Point(target)) =
                (&model, right.shape.shape())
                && !array.has_missing()
                && let Some(values) = array.reduce_packed_points_detached(py, {
                    let crs = crs.clone();
                    let target = *target;
                    move |xs, ys| {
                        crs::geodesic_point_distances(&crs, xs, ys, target)
                            .map_err(PackedColumnError::Batch)
                    }
                })?
            {
                return float64_array(py, values);
            }
            // Geodesic point-array -> fixed complex shape: one prepared
            // geodesic BVH over the fixed operand, one stack for all probes.
            if let crs::MetricModel::Geodesic(crs) = &model
                && !array.has_missing()
                && let Some(values) = array.reduce_packed_points_detached(py, {
                    let crs = crs.clone();
                    let right_shape = Arc::clone(&right.shape);
                    let right_cache = Arc::clone(&right.frame_cache);
                    move |xs, ys| {
                        geodesic_point_columns_to_shape_values(
                            &crs,
                            xs,
                            ys,
                            &right_shape,
                            &right_cache,
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
            if let crs::MetricModel::Geodesic(crs) = model {
                let array = array.clone();
                let right_shape = Arc::clone(&right.shape);
                let right_cache = Arc::clone(&right.frame_cache);
                let missing = array.missing().cloned();
                let rows = py.detach(move || {
                    crs::with_resolved_ellipsoid_metric(
                        &crs,
                        &[right_shape.shape()],
                        |crs, metric| {
                            let (semi_major, flattening) = metric.ellipsoid_parameters();
                            right_shape.prepare_geodesic_parts(
                                &right_cache,
                                crs,
                                semi_major,
                                flattening,
                                metric,
                            )?;
                            Ok(array
                                .storage()
                                .iter_rows()
                                .enumerate()
                                .map(|(row_index, row)| {
                                    if missing.as_ref().is_some_and(|mask| mask[row_index]) {
                                        return Ok(f64::NAN);
                                    }
                                    let left = array.prepared_row(row_index, row);
                                    let left_cache = array.row_frame_cache(row_index);
                                    finite_geodesic_value(
                                        left.geodesic_distance_cached(
                                            &left_cache,
                                            &right_shape,
                                            &right_cache,
                                            crs,
                                            semi_major,
                                            flattening,
                                            metric,
                                        )?,
                                        "geodesic distance",
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
        Many(right) => {
            paired_arrays(array, right, operation)?;
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
                    let packed_planar = Some(move |a, b| point_distance(a, b) * to_metre.get());
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
                                        Point::new_unchecked_xy(lx, ly),
                                        Point::new_unchecked_xy(rx, ry),
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
                            |row, left, right_data| {
                                pair_distance_resolved_result(
                                    &resolved,
                                    left,
                                    &array.row_frame_cache(row),
                                    right_data,
                                    &right.row_frame_cache(row),
                                )
                            },
                        )?,
                    )
                },
                crs::ResolvedMetric::Geodesic { geodesic, .. } => {
                    let geodesic = **geodesic;
                    let packed_columns =
                        Some(move |lx: &[f64], ly: &[f64], rx: &[f64], ry: &[f64]| {
                            crs::geodesic_point_pair_distances(&geodesic, lx, ly, rx, ry)
                        });
                    let packed_planar: Option<fn(Point, Point) -> f64> = None;
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
                            |row, left, right_data| {
                                pair_distance_resolved_result(
                                    &resolved,
                                    left,
                                    &array.row_frame_cache(row),
                                    right_data,
                                    &right.row_frame_cache(row),
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
    operation: &str,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    use GeometryInput::{Many, One};
    match classify_required(other)? {
        One(right) => array_crs_dwithin_scalar(py, array, right, distance, operation, unit),
        Many(right) => {
            // Structural checks (length, frame) precede metric-model resolution
            // — see `array_crs_distances`.
            paired_arrays(array, right, operation)?;
            let model = resolve_metric(array.crs_str(), unit, operation)?;
            // Resolve the geodesic ellipsoid ONCE for the whole array pair.
            let resolved = crs::ResolvedMetric::from_model(&model)?;
            match &resolved {
                crs::ResolvedMetric::Planar { to_metre } => {
                    let limit = distance / to_metre.get();
                    let limit_sq = limit * limit;
                    // Underflow-safe dwithin (squared only when both sides honest).
                    let packed_planar = Some(move |a, b| points_dwithin(a, b, limit));
                    let packed_columns =
                        Some(move |lx: &[f64], ly: &[f64], rx: &[f64], ry: &[f64]| {
                            let mut out = vec![false; lx.len()];
                            let use_sq = limit_sq.is_finite() && limit_sq != 0.0 && limit > 0.0;
                            if use_sq {
                                pair_select_mask(
                                    lx,
                                    ly,
                                    rx,
                                    ry,
                                    &mut out,
                                    |lx, ly, rx, ry| {
                                        points_dwithin(
                                            Point::new_unchecked_xy(lx, ly),
                                            Point::new_unchecked_xy(rx, ry),
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
                                        Point::new_unchecked_xy(lx[index], ly[index]),
                                        Point::new_unchecked_xy(rx[index], ry[index]),
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
                            |row, left, right_data| {
                                pair_dwithin_resolved_result(
                                    &resolved,
                                    left,
                                    &array.row_frame_cache(row),
                                    right_data,
                                    &right.row_frame_cache(row),
                                    distance,
                                )
                            },
                        )?,
                    )
                },
                crs::ResolvedMetric::Geodesic { geodesic, .. } => {
                    let geodesic = **geodesic;
                    let packed_columns =
                        Some(move |lx: &[f64], ly: &[f64], rx: &[f64], ry: &[f64]| {
                            crs::geodesic_point_pair_dwithin(&geodesic, lx, ly, rx, ry, distance)
                        });
                    let packed_planar: Option<fn(Point, Point) -> bool> = None;
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
                            |row, left, right_data| {
                                pair_dwithin_resolved_result(
                                    &resolved,
                                    left,
                                    &array.row_frame_cache(row),
                                    right_data,
                                    &right.row_frame_cache(row),
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
    operation: &str,
    unit: Option<DistanceUnit>,
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
            let resolved = crs::ResolvedMetric::from_model(&model)?;
            let missing = array.missing().cloned();
            let results: Vec<bool> = array
                .storage()
                .iter_rows()
                .enumerate()
                .map(|(row, left)| {
                    if missing.as_ref().is_some_and(|mask| mask[row]) {
                        return Ok(false);
                    }
                    let left = array.prepared_row(row, left);
                    pair_dwithin_resolved(
                        &resolved,
                        &left,
                        &array.row_frame_cache(row),
                        &right.shape,
                        &right.frame_cache,
                        distance.get(row),
                    )
                })
                .collect::<PyResult<_>>()?;
            bool_array(py, results)
        },
        Many(right) => {
            paired_arrays(array, right, operation)?;
            let model = resolve_metric(array.crs_str(), unit, operation)?;
            let resolved = crs::ResolvedMetric::from_model(&model)?;
            let missing = crate::array::missing::union_pair(array.missing(), right.missing());
            let results: Vec<bool> = array
                .storage()
                .iter_rows()
                .zip(right.storage().iter_rows())
                .enumerate()
                .map(|(row, (left, right_row))| {
                    if missing.as_ref().is_some_and(|mask| mask[row]) {
                        return Ok(false);
                    }
                    let left = array.prepared_row(row, left);
                    let right_data = right.prepared_row(row, right_row);
                    pair_dwithin_resolved(
                        &resolved,
                        &left,
                        &array.row_frame_cache(row),
                        &right_data,
                        &right.row_frame_cache(row),
                        distance.get(row),
                    )
                })
                .collect::<PyResult<_>>()?;
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
    operation: &str,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    Frame::compatible_parts(
        array.crs_ref(),
        array.epoch(),
        other.crs_ref(),
        other.epoch(),
        operation,
    )?;
    let model = resolve_metric(array.crs_str(), unit, operation)?;
    // The physical storage uses a finite NaN placeholder for missing rows.
    // Once a mask exists, enter the row lane before any packed reduction,
    // point materialization, bounds read, or kernel call; masking the result
    // afterwards is too late for kernels that assert finite coordinates.
    if array.has_missing() {
        let resolved = crs::ResolvedMetric::from_model(&model)?;
        let array_owned = array.clone();
        let other_shape = Arc::clone(&other.shape);
        let other_cache = Arc::clone(&other.frame_cache);
        let missing = array.missing().cloned();
        let rows = array_owned
            .storage()
            .iter_rows()
            .enumerate()
            .map(|(row_index, row)| {
                if missing.as_ref().is_some_and(|mask| mask[row_index]) {
                    return Ok(false);
                }
                let left = array_owned.prepared_row(row_index, row);
                pair_dwithin_resolved(
                    &resolved,
                    &left,
                    &array_owned.row_frame_cache(row_index),
                    &other_shape,
                    &other_cache,
                    distance,
                )
            })
            .collect::<PyResult<Vec<_>>>()?;
        return bool_array(py, rows);
    }
    if let crs::MetricModel::Planar { to_metre } = &model {
        let limit = distance / to_metre.get();
        if let Some(values) = array.reduce_packed_points_detached(py, {
            let other_shape = Arc::clone(&other.shape);
            move |xs, ys| {
                Ok(other_shape.dwithin_points(
                    std::iter::zip(xs.iter().copied(), ys.iter().copied()),
                    limit,
                ))
            }
        })? {
            return bool_array_mask_missing(py, values, array.missing());
        }
        // Gathered mixed points run the same batch kernel from by-value points.
        if let Some(points) = array.storage().point_rows() {
            let other_shape = Arc::clone(&other.shape);
            return bool_array_mask_missing(
                py,
                py.detach(move || {
                    other_shape.dwithin_points(points.iter().map(|point| (point.x, point.y)), limit)
                }),
                array.missing(),
            );
        }
    }
    match model {
        crs::MetricModel::Planar { to_metre } => {
            let array = array.clone();
            let missing = array.missing().cloned();
            let limit = distance / to_metre.get();
            let other_shape = Arc::clone(&other.shape);
            // Box-separation reject against the FIXED operand's box, from the
            // array's batch per-element boxes: an element whose box sits farther
            // (squared) than the limit is settled false before materialization.
            let sq_limit = (limit * limit).is_finite().then_some(limit * limit);
            let other_bounds = other_shape.bounds();
            let element_bounds = array.cached_element_bounds();
            bool_array_mask_missing(
                py,
                py.detach(move || {
                    array
                        .storage()
                        .iter_rows()
                        .enumerate()
                        .map(|(index, row)| {
                            if let (Some(sq_limit), Some(other_bounds), Some(element_bounds)) =
                                (sq_limit, other_bounds, element_bounds.as_ref())
                                && let Some(element) = element_bounds[index]
                                && bounds_distance_squared(element, other_bounds) > sq_limit
                            {
                                return false;
                            }
                            let left = array.prepared_row(index, row);
                            left.dwithin(&other_shape, limit)
                        })
                        .collect::<Vec<bool>>()
                }),
                missing.as_ref(),
            )
        },
        crs::MetricModel::Geodesic(crs) => {
            // Packed point lane: one geodesic cache resolve, raw columns
            // through the same point kernel (mirrors the distances lane).
            if let Shape::Point(target) = other.shape.shape()
                && !array.has_missing()
                && let Some(values) = array.reduce_packed_points_detached(py, {
                    let crs = crs.clone();
                    let target = *target;
                    move |xs, ys| {
                        crs::geodesic_point_distances(&crs, xs, ys, target)
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
                    let other_shape = Arc::clone(&other.shape);
                    let other_cache = Arc::clone(&other.frame_cache);
                    move |xs, ys| {
                        geodesic_point_columns_dwithin_shape_values(
                            &crs,
                            xs,
                            ys,
                            &other_shape,
                            &other_cache,
                            distance,
                        )
                        .map_err(PackedColumnError::Batch)
                    }
                })?
            {
                return bool_array_mask_missing(py, values, array.missing());
            }
            let array_owned = array.clone();
            let other_shape = Arc::clone(&other.shape);
            let other_cache = Arc::clone(&other.frame_cache);
            let missing = array.missing().cloned();
            let rows = py.detach(move || {
                crs::with_resolved_ellipsoid_metric(&crs, &[other_shape.shape()], |crs, metric| {
                    let (semi_major, flattening) = metric.ellipsoid_parameters();
                    other_shape.prepare_geodesic_parts(
                        &other_cache,
                        crs,
                        semi_major,
                        flattening,
                        metric,
                    )?;
                    Ok(array_owned
                        .storage()
                        .iter_rows()
                        .enumerate()
                        .map(|(row_index, row)| {
                            if missing.as_ref().is_some_and(|mask| mask[row_index]) {
                                return Ok(false);
                            }
                            let left = array_owned.prepared_row(row_index, row);
                            let left_cache = array_owned.row_frame_cache(row_index);
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
                })
            })?;
            bool_array_mask_missing(py, rows.map_err(rows_err)?, array.missing())
        },
    }
}
