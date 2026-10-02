use crate::array::ArrayRow;
use crate::geometry::storage::ValidityMask;
use crate::py::support::{
    CollectRows as _, CoordinateAxes, Frame, Point, PyGeometry, PyGeometryArray, PyResult, Python,
    Shape, metric_nearest_points_resolved, rows_err,
};

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
    let mut left = Vec::with_capacity(pair_count);
    let mut right = Vec::with_capacity(pair_count);
    for (row, (pair, common)) in pairs.into_iter().enumerate() {
        if missing.as_ref().is_some_and(|mask| mask.is_missing(row)) {
            left.push(PyGeometryArray::missing_placeholder());
            right.push(PyGeometryArray::missing_placeholder());
            continue;
        }
        let (left_shape, right_shape) = crate::geometry::nearest_point_pair(pair.as_ref(), common);
        left.push(left_shape);
        right.push(right_shape);
    }
    Ok((
        PyGeometryArray::from_shapes(left, frame.clone())?.with_missing_mask(missing.clone())?,
        PyGeometryArray::from_shapes(right, frame)?.with_missing_mask(missing)?,
    ))
}

pub(crate) fn fixed_geometry_array_nearest_points_admitted(
    py: Python<'_>,
    fixed: &PyGeometry,
    array: &PyGeometryArray,
    fixed_is_left: bool,
    model: crate::crs::MetricModel,
) -> PyResult<(PyGeometryArray, PyGeometryArray)> {
    let fixed_plan = fixed.prepared_plan();
    let fixed_cache = fixed.frame_cache();
    let array_owned = array.clone();
    if let crate::crs::MetricModel::Geodesic { crs, state, .. } = model {
        let rows = py.detach(move || {
            let mut rows = array_owned.row_operation();
            crate::crs::with_resolved_ellipsoid_metric_at(
                &crs,
                &[fixed_plan.shape()],
                &state,
                |crs, metric| {
                    let (semi_major, flattening) = metric.ellipsoid_parameters();
                    Ok(rows
                        .rows_with_missing(|| (None::<(Point, Point)>, CoordinateAxes::XY))
                        .map(|logical_row| {
                            let ArrayRow::Present(row) = logical_row else {
                                return Ok((None, CoordinateAxes::XY));
                            };
                            let row_index = row.logical();
                            let element = rows.prepared(row)?;
                            let element_cache = rows.frame_cache(row_index);
                            let common =
                                crate::geometry::common_axes(fixed_plan.shape(), element.shape());
                            if fixed_is_left {
                                fixed_plan.geodesic_nearest_points_cached(
                                    &fixed_cache,
                                    &element,
                                    &element_cache,
                                    crs,
                                    semi_major,
                                    flattening,
                                    metric,
                                )
                            } else {
                                element.geodesic_nearest_points_cached(
                                    &element_cache,
                                    &fixed_plan,
                                    &fixed_cache,
                                    crs,
                                    semi_major,
                                    flattening,
                                    metric,
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
    let resolved = crate::crs::ResolvedMetric::from_model(&model)?;
    let pairs = py
        .detach(move || {
            let mut rows = array_owned.row_operation();
            rows.rows_with_missing(|| (None::<(Point, Point)>, CoordinateAxes::XY))
                .map(|logical_row| {
                    let ArrayRow::Present(row) = logical_row else {
                        return Ok((None, CoordinateAxes::XY));
                    };
                    let element = rows.prepared(row)?;
                    let common = crate::geometry::common_axes(fixed_plan.shape(), element.shape());
                    let fixed_cache = crate::geometry::FrameDependentCaches::default();
                    let element_cache = crate::geometry::FrameDependentCaches::default();
                    if fixed_is_left {
                        metric_nearest_points_resolved(
                            &resolved,
                            &fixed_plan,
                            &fixed_cache,
                            &element,
                            &element_cache,
                        )
                    } else {
                        metric_nearest_points_resolved(
                            &resolved,
                            &element,
                            &element_cache,
                            &fixed_plan,
                            &fixed_cache,
                        )
                    }
                    .map(|pair| (pair, common))
                })
                .collect_rows()
        })
        .map_err(crate::broadcast::rows_err)?;
    nearest_point_columns_masked(pairs, array.frame.clone(), array.missing().cloned())
}

/// One fixed geometry against every array element, emitting geodesic shortest
/// lines. `fixed_is_left` preserves public endpoint order. Callers admit the
/// geographic model first so each public entry keeps its own frame/metric
/// order.
pub(crate) fn fixed_geometry_array_shortest_line_geodesic(
    py: Python<'_>,
    fixed: &PyGeometry,
    array: &PyGeometryArray,
    fixed_is_left: bool,
    crs: &str,
    state: &crate::crs::RuntimeState,
) -> PyResult<PyGeometryArray> {
    let output_frame = if fixed_is_left {
        fixed.frame.clone()
    } else {
        array.frame.clone()
    };
    let fixed_plan = fixed.prepared_plan();
    let fixed_cache = fixed.frame_cache();
    let array_owned = array.clone();
    let present_count = array
        .missing()
        .map_or(array.len(), ValidityMask::present_count);
    let rows = py.detach(move || {
        let mut array_rows = array_owned.row_operation();
        crate::crs::with_resolved_ellipsoid_metric_at(
            crs,
            &[fixed_plan.shape()],
            state,
            |crs, metric| {
                let (semi_major, flattening) = metric.ellipsoid_parameters();
                let present = (|| -> Result<Vec<Shape>, (usize, crate::error::Error)> {
                    let mut shapes = Vec::with_capacity(present_count);
                    for row in array_rows.present_rows() {
                        let logical = row.logical();
                        let element = array_rows.prepared(row).map_err(|error| (logical, error))?;
                        let element_cache = array_rows.frame_cache(logical);
                        let pair = if fixed_is_left {
                            fixed_plan
                                .geodesic_nearest_points_cached(
                                    &fixed_cache,
                                    &element,
                                    &element_cache,
                                    crs,
                                    semi_major,
                                    flattening,
                                    metric,
                                )
                                .map_err(|error| (logical, error))?
                        } else {
                            element
                                .geodesic_nearest_points_cached(
                                    &element_cache,
                                    &fixed_plan,
                                    &fixed_cache,
                                    crs,
                                    semi_major,
                                    flattening,
                                    metric,
                                )
                                .map_err(|error| (logical, error))?
                        };
                        let line = crate::geometry::nearest_line(
                            pair.as_ref(),
                            crate::geometry::common_axes(fixed_plan.shape(), element.shape()),
                        )
                        .map_err(|error| (logical, error))?;
                        shapes.push(line);
                    }
                    Ok(shapes)
                })();
                Ok(present)
            },
        )
    })?;
    let present = PyGeometryArray::from_shapes(rows.map_err(rows_err)?, output_frame)?;
    Ok(array.scatter_present_result(present)?)
}

/// The query point of a linear-referencing locate. POINT EMPTY becomes the
/// NaN point, the form an empty point takes in a point column, which locates
/// nowhere.
pub(crate) fn locate_query(shape: &Shape) -> crate::error::Result<Point> {
    Ok(shape
        .point_operand("line_locate")?
        .unwrap_or(Point::new_raw_xy(f64::NAN, f64::NAN)))
}
