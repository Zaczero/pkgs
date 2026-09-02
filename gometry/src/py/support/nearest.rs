#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use crate::array::ValidityMask;
use crate::py::support::{
    CollectRows as _, CoordinateAxes, DistanceUnit, Frame, Point, PointRows, Py, PyAny, PyGeometry,
    PyGeometryArray, PyResult, PyTypeError, Python, Shape, Typed, geometry_type_err,
    line_locate_shape, metric_nearest_points, resolve_line_metric, resolve_metric, rows_err,
};

pub(crate) fn py_geometry_array(shapes: Vec<Shape>, frame: &Frame) -> PyGeometryArray {
    PyGeometryArray::from_shapes(shapes, frame.clone())
}

/// Build the `(left, right)` witness point columns from per-row nearest
/// pairs. A `None` row is an empty operand → `(POINT EMPTY, POINT EMPTY)`
/// (the output-type sentinel), so an empty row never aborts the batch; a
/// present row is dropped to its common axes — both via
/// [`crate::geometry::nearest_point_pair`].
pub(crate) fn nearest_point_columns_masked(
    pairs: Vec<(Option<(Point, Point)>, CoordinateAxes)>,
    frame: Frame,
    missing: Option<ValidityMask>,
) -> PyResult<(PyGeometryArray, PyGeometryArray)> {
    let pair_count = pairs.len();
    let mut left = Vec::new();
    let mut right = Vec::new();
    if let Err(error) = crate::try_reserve_pair(&mut left, &mut right, pair_count) {
        drop(pairs);
        return Err(error.into());
    }
    for (row, (pair, common)) in pairs.into_iter().enumerate() {
        if missing.as_ref().is_some_and(|mask| mask.is_missing(row)) {
            left.push(PyGeometryArray::missing_placeholder());
            right.push(PyGeometryArray::missing_placeholder());
            continue;
        }
        let (left_shape, right_shape) = crate::geometry::nearest_point_pair(pair, common);
        left.push(left_shape);
        right.push(right_shape);
    }
    Ok((
        PyGeometryArray::from_shapes(left, frame.clone()).with_missing_mask(missing.clone()),
        PyGeometryArray::from_shapes(right, frame).with_missing_mask(missing),
    ))
}

pub(crate) fn py_nearest_points(
    py: Python<'_>,
    left: &PyGeometry,
    right: &PyGeometry,
    unit: Option<DistanceUnit>,
) -> PyResult<(Typed, Typed)> {
    left.frame.compatible(&right.frame, "nearest_points")?;
    let model = resolve_metric(left.crs_str(), unit, "nearest_points")?;
    let common = crate::geometry::common_axes(left.shape.shape(), right.shape.shape());
    let left_plan = left.plan_or_transient();
    let right_plan = right.plan_or_transient();
    let pair = py.detach(move || metric_nearest_points(&model, &left_plan, &right_plan))?;
    let (left_shape, right_shape) = crate::geometry::nearest_point_pair(pair, common);
    Ok((
        Typed(PyGeometry::with_frame(left_shape, left.frame.clone())),
        Typed(PyGeometry::with_frame(right_shape, right.frame.clone())),
    ))
}

/// One fixed point against every array line: the frame is checked once, the
/// metric model resolved once, and the locate loop runs detached on the rows'
/// cached line indexes.
pub(crate) fn geometry_array_line_locate_point_geometry(
    py: Python<'_>,
    left: &PyGeometryArray,
    point: &PyGeometry,
    normalized: bool,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    Frame::compatible_parts(
        left.crs_ref(),
        left.epoch(),
        point.crs_ref(),
        point.epoch(),
        "line_locate_point",
    )?;
    let query = require_locate_point(point, "line_locate_point")?;
    let model = resolve_line_metric(left.crs_str(), unit, normalized, "line_locate_point")?;
    let left = left.clone();
    let result = py
        .detach(move || {
            let mut products = left.row_products.operation();
            left.logical_storage_rows()
                .map(|logical_row| {
                    let crate::array::LogicalRow::Present {
                        logical: index,
                        row,
                    } = logical_row
                    else {
                        return Ok(f64::NAN);
                    };
                    let line = left.prepared_row_with(&mut products, index, row);
                    crate::broadcast::degrade_linref_float(line_locate_shape(
                        &model,
                        &line,
                        &products.frame_cache(index),
                        query,
                        normalized,
                    ))
                })
                .collect_rows()
        })
        .map_err(crate::broadcast::rows_err)?;
    crate::py::numpy::float64_array(py, result)
}

/// One fixed geometry against every array element, preserving public
/// left/right witness orientation through `fixed_is_left`.
///
/// Frame/model resolution, antimeridian-aware geodesic preparation, missing
/// rows, and output materialization live here once for both scalar-left and
/// array-left entry points.
pub(crate) fn fixed_geometry_array_nearest_points(
    py: Python<'_>,
    fixed: &PyGeometry,
    array: &PyGeometryArray,
    fixed_is_left: bool,
    unit: Option<DistanceUnit>,
) -> PyResult<(PyGeometryArray, PyGeometryArray)> {
    Frame::compatible_parts(
        fixed.crs_ref(),
        fixed.epoch(),
        array.crs_ref(),
        array.epoch(),
        "nearest_points",
    )?;
    let model = resolve_metric(fixed.crs_str(), unit, "nearest_points")?;
    let fixed_plan = fixed.plan_or_transient();
    let fixed_cache = fixed.frame_cache();
    let array_owned = array.clone();
    if let crate::crs::MetricModel::Geodesic { crs, frame, state } = model {
        let rows = py.detach(move || {
            let mut products = array_owned.row_products.operation();
            crate::crs::with_resolved_ellipsoid_metric_at(
                &crs,
                &[fixed_plan.shape()],
                &state,
                |crs, metric| {
                    let (semi_major, flattening) = metric.ellipsoid_parameters();
                    if !crate::geometry::crosses_antimeridian_in_frame(fixed_plan.shape(), frame) {
                        fixed_plan.prepare_geodesic_parts(
                            &fixed_cache,
                            crs,
                            semi_major,
                            flattening,
                            metric,
                        )?;
                    }
                    Ok(array_owned
                        .logical_storage_rows()
                        .map(|logical_row| {
                            let crate::array::LogicalRow::Present {
                                logical: row_index,
                                row,
                            } = logical_row
                            else {
                                return Ok((None, CoordinateAxes::XY));
                            };
                            let element =
                                array_owned.prepared_row_with(&mut products, row_index, row);
                            let element_cache = products.frame_cache(row_index);
                            let common =
                                crate::geometry::common_axes(fixed_plan.shape(), element.shape());
                            if fixed_is_left {
                                fixed_plan.geodesic_nearest_points_cached_split(
                                    &fixed_cache,
                                    &element,
                                    &element_cache,
                                    crs,
                                    semi_major,
                                    flattening,
                                    metric,
                                    frame,
                                )
                            } else {
                                element.geodesic_nearest_points_cached_split(
                                    &element_cache,
                                    &fixed_plan,
                                    &fixed_cache,
                                    crs,
                                    semi_major,
                                    flattening,
                                    metric,
                                    frame,
                                )
                            }
                            .map(|pair| (pair, common))
                        })
                        .collect_rows())
                },
            )
        })?;
        let pairs = rows.map_err(rows_err)?;
        return nearest_point_columns_masked(pairs, array.frame.clone(), array.missing().cloned());
    }
    let pairs = py
        .detach(move || {
            let mut products = array_owned.row_products.operation();
            array_owned
                .logical_storage_rows()
                .map(|logical_row| {
                    let crate::array::LogicalRow::Present {
                        logical: row_index,
                        row,
                    } = logical_row
                    else {
                        return Ok((None, CoordinateAxes::XY));
                    };
                    let element = array_owned.prepared_row_with(&mut products, row_index, row);
                    let common = crate::geometry::common_axes(fixed_plan.shape(), element.shape());
                    if fixed_is_left {
                        metric_nearest_points(&model, &fixed_plan, &element)
                    } else {
                        metric_nearest_points(&model, &element, &fixed_plan)
                    }
                    .map(|pair| (pair, common))
                })
                .collect_rows()
        })
        .map_err(crate::broadcast::rows_err)?;
    nearest_point_columns_masked(pairs, array.frame.clone(), array.missing().cloned())
}

/// Extract the query `Point` for a linear-referencing locate, with the
/// operation-tagged `TypeError` the scalar path raises.
pub(crate) fn require_locate_point(point: &PyGeometry, operation: &str) -> PyResult<Point> {
    match point.shape.shape() {
        Shape::Point(query) => Ok(*query),
        _ => Err(PyTypeError::new_err(format!(
            "{operation} requires a Point"
        ))),
    }
}

/// Extract one query `Point` per array row (the array form of
/// [`require_locate_point`], same error).
pub(crate) fn require_locate_points<'a>(
    points: &'a PyGeometryArray,
    operation: &str,
) -> PyResult<PointRows<'a>> {
    points
        .storage()
        .point_rows()
        .ok_or_else(|| geometry_type_err(format!("{operation} requires a Point")))
}
