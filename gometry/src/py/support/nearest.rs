#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::sync::Arc;

use crate::array::MissingMask;
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
    missing: Option<MissingMask>,
) -> (PyGeometryArray, PyGeometryArray) {
    let mut left = Vec::with_capacity(pairs.len());
    let mut right = Vec::with_capacity(pairs.len());
    for (row, (pair, common)) in pairs.into_iter().enumerate() {
        if missing.as_ref().is_some_and(|mask| mask[row]) {
            left.push(PyGeometryArray::missing_placeholder());
            right.push(PyGeometryArray::missing_placeholder());
            continue;
        }
        let (left_shape, right_shape) = crate::geometry::nearest_point_pair(pair, common);
        left.push(left_shape);
        right.push(right_shape);
    }
    (
        PyGeometryArray::from_shapes(left, frame.clone()).with_missing_mask(missing.clone()),
        PyGeometryArray::from_shapes(right, frame).with_missing_mask(missing),
    )
}

pub(crate) fn py_nearest_points(
    left: &PyGeometry,
    right: &PyGeometry,
    unit: Option<DistanceUnit>,
) -> PyResult<(Typed, Typed)> {
    left.frame.compatible(&right.frame, "nearest_points")?;
    let model = resolve_metric(left.crs_str(), unit, "nearest_points")?;
    let common = crate::geometry::common_axes(left.shape.shape(), right.shape.shape());
    let (left_shape, right_shape) = crate::geometry::nearest_point_pair(
        metric_nearest_points(&model, &left.shape, &right.shape)?,
        common,
    );
    Ok((
        PyGeometry::typed_with_epoch(left_shape, left.crs_ref().cloned(), left.epoch()),
        PyGeometry::typed_with_epoch(right_shape, right.crs_ref().cloned(), right.epoch()),
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
    let missing = left.missing().cloned();
    let result = py
        .detach(move || {
            left.storage()
                .iter_rows()
                .enumerate()
                .map(|(index, row)| {
                    if missing.as_ref().is_some_and(|mask| mask[index]) {
                        return Ok(f64::NAN);
                    }
                    row.with_data(|line| {
                        crate::broadcast::degrade_linref_float(line_locate_shape(
                            &model,
                            line,
                            &left.row_frame_cache(index),
                            query,
                            normalized,
                        ))
                    })
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
#[expect(
    clippy::too_many_lines,
    reason = "planar and geodesic nearest lanes share one output assembly path"
)]
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
    let fixed_shape = Arc::clone(&fixed.shape);
    let fixed_cache = Arc::clone(&fixed.frame_cache);
    let storage = Arc::clone(array.storage_arc());
    let array_owned = array.clone();
    let missing = array.missing().cloned();
    if let crate::crs::MetricModel::Geodesic(crs) = model {
        let rows = py.detach(move || {
            crate::crs::with_resolved_ellipsoid_metric(
                &crs,
                &[fixed_shape.shape()],
                |crs, metric| {
                    let (semi_major, flattening) = metric.ellipsoid_parameters();
                    if !fixed_shape.shape().crosses_antimeridian() {
                        fixed_shape.prepare_geodesic_parts(
                            &fixed_cache,
                            crs,
                            semi_major,
                            flattening,
                            metric,
                        )?;
                    }
                    Ok(array_owned
                        .storage()
                        .iter_rows()
                        .enumerate()
                        .map(|(row_index, row)| {
                            if missing.as_ref().is_some_and(|mask| mask[row_index]) {
                                return Ok((None, CoordinateAxes::XY));
                            }
                            row.with_data(|element| {
                                let element_cache = array_owned.row_frame_cache(row_index);
                                let common = crate::geometry::common_axes(
                                    fixed_shape.shape(),
                                    element.shape(),
                                );
                                if fixed_is_left {
                                    fixed_shape.geodesic_nearest_points_cached_split(
                                        &fixed_cache,
                                        element,
                                        &element_cache,
                                        crs,
                                        semi_major,
                                        flattening,
                                        metric,
                                    )
                                } else {
                                    element.geodesic_nearest_points_cached_split(
                                        &element_cache,
                                        &fixed_shape,
                                        &fixed_cache,
                                        crs,
                                        semi_major,
                                        flattening,
                                        metric,
                                    )
                                }
                                .map(|pair| (pair, common))
                            })
                        })
                        .collect_rows())
                },
            )
        })?;
        let pairs = rows.map_err(rows_err)?;
        return Ok(nearest_point_columns_masked(
            pairs,
            array.frame.clone(),
            array.missing().cloned(),
        ));
    }
    let missing = array.missing().cloned();
    let pairs = py
        .detach(move || {
            storage
                .iter_rows()
                .enumerate()
                .map(|(row_index, row)| {
                    if missing.as_ref().is_some_and(|mask| mask[row_index]) {
                        return Ok((None, CoordinateAxes::XY));
                    }
                    row.with_data(|element| {
                        let common =
                            crate::geometry::common_axes(fixed_shape.shape(), element.shape());
                        if fixed_is_left {
                            metric_nearest_points(&model, &fixed_shape, element)
                        } else {
                            metric_nearest_points(&model, element, &fixed_shape)
                        }
                        .map(|pair| (pair, common))
                    })
                })
                .collect_rows()
        })
        .map_err(crate::broadcast::rows_err)?;
    Ok(nearest_point_columns_masked(
        pairs,
        array.frame.clone(),
        array.missing().cloned(),
    ))
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
