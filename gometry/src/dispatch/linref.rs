use std::sync::Arc;

use pyo3::prelude::*;
use pyo3::types::PyAny;

use crate::array::missing::{is_missing_row, union_pair};
use crate::array::{ArrayRow, line_locate_point_on_lines};
use crate::boundary::metadata::Frame;
use crate::broadcast::{
    CollectRows as _, GeometryInput, classify_required, degrade_linref_float, detach_collect_f64,
    paired_arrays,
};
use crate::dispatch::operation::Operation;
use crate::{
    DistanceUnit, GeometryArrayStorage, InterpolatePlan, PyGeometry, PyGeometryArray,
    crs_line_locate_point, geometry_array_line_locate_point_geometry,
    line_interpolate_points_shape, line_locate_shape, line_locate_shape_m,
    line_locate_shape_m_value, require_locate_point, require_locate_points,
};

fn line_locate_geometry_points(
    py: Python<'_>,
    geometry: &PyGeometry,
    points: &PyGeometryArray,
    normalized: bool,
    unit: Option<DistanceUnit>,
    op_name: &str,
) -> PyResult<Py<PyAny>> {
    Frame::compatible_parts(
        geometry.crs_ref(),
        geometry.epoch(),
        points.crs_ref(),
        points.epoch(),
        op_name,
    )?;
    let model = Operation::LineLocate
        .resolver_with_line_unit(unit, normalized)
        .resolve_ctx(
            &geometry.frame,
            op_name,
            &mut crate::dispatch::MetricScratch::default(),
        )?
        .require_model(op_name)?
        .clone();
    let line = geometry.plan_or_transient();
    if points.is_compact_nullable()
        && matches!(points.storage(), GeometryArrayStorage::Points { .. })
    {
        let points = points.clone();
        return detach_collect_f64(py, move || {
            points
                .row_operation()
                .rows_with_missing(|| f64::NAN)
                .map(|logical_row| {
                    let ArrayRow::Present(row) = logical_row else {
                        return Ok(f64::NAN);
                    };
                    let crate::array::ShapeRow::Point(query) = row.row() else {
                        return Ok(f64::NAN);
                    };
                    line_locate_shape(&model, &line, &geometry.frame_cache(), query, normalized)
                })
                .collect_rows()
        });
    }
    let queries = require_locate_points(points, op_name)?;
    let missing = points.missing().cloned();
    detach_collect_f64(py, move || {
        queries
            .iter()
            .enumerate()
            .map(|(row, query)| {
                if is_missing_row(missing.as_ref(), row) {
                    return Ok(f64::NAN);
                }
                line_locate_shape(&model, &line, &geometry.frame_cache(), query, normalized)
            })
            .collect_rows()
    })
}

pub(crate) fn line_locate_point_input(
    py: Python<'_>,
    geom: GeometryInput<'_>,
    point: &Bound<'_, PyAny>,
    normalized: bool,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    let op_name = Operation::LineLocate.name();
    let point = classify_required(point)?;
    match (geom, point) {
        (GeometryInput::One(geometry), GeometryInput::One(point_geometry)) => {
            return Ok(
                crs_line_locate_point(geometry, point_geometry, normalized, unit)?
                    .into_pyobject(py)?
                    .unbind()
                    .into(),
            );
        },
        (GeometryInput::One(geometry), GeometryInput::Many(points)) => {
            line_locate_geometry_points(py, geometry, points, normalized, unit, op_name)
        },
        (GeometryInput::Many(array), GeometryInput::One(point_geometry)) => {
            geometry_array_line_locate_point_geometry(py, array, point_geometry, normalized, unit)
        },
        (GeometryInput::Many(array), GeometryInput::Many(points)) => {
            paired_arrays(array, points, op_name)?;
            let queries = require_locate_points(points, op_name)?;
            let model = Operation::LineLocate
                .resolver_with_line_unit(unit, normalized)
                .resolve_ctx(
                    &array.frame,
                    op_name,
                    &mut crate::dispatch::MetricScratch::default(),
                )?
                .require_model(op_name)?
                .clone();
            let missing = union_pair(array.missing(), points.missing());
            if array.is_compact_nullable() || points.is_compact_nullable() {
                let array = array.clone();
                let points = points.clone();
                return detach_collect_f64(py, move || {
                    let mut rows = array.row_operation();
                    let point_rows = points.row_operation();
                    rows.rows_with_missing(|| f64::NAN)
                        .zip(point_rows.rows_with_missing(|| f64::NAN))
                        .map(|(line, point)| {
                            let (ArrayRow::Present(line), ArrayRow::Present(point)) = (line, point)
                            else {
                                return Ok(f64::NAN);
                            };
                            let row = line.logical();
                            let crate::array::ShapeRow::Point(query) = point.row() else {
                                return Ok(f64::NAN);
                            };
                            let line = rows.prepared(line);
                            degrade_linref_float(line_locate_shape(
                                &model,
                                &line,
                                &rows.frame_cache(row),
                                query,
                                normalized,
                            ))
                        })
                        .collect_rows()
                });
            }
            if let GeometryArrayStorage::Lines {
                coords,
                offsets,
                row_map,
            } = array.storage()
            {
                return line_locate_point_on_lines(
                    py,
                    Arc::clone(coords),
                    offsets.clone(),
                    row_map.clone(),
                    array.len(),
                    queries,
                    missing,
                    model,
                    normalized,
                );
            }
            let array = array.clone();
            let points = points.clone();
            detach_collect_f64(py, move || {
                let mut rows = array.row_operation();
                let point_rows = points.row_operation();
                rows.rows_with_missing(|| f64::NAN)
                    .zip(point_rows.rows_with_missing(|| f64::NAN))
                    .map(|(line_row, point_row)| {
                        let (ArrayRow::Present(line_row), ArrayRow::Present(point_row)) =
                            (line_row, point_row)
                        else {
                            return Ok(f64::NAN);
                        };
                        let row = line_row.logical();
                        let crate::array::ShapeRow::Point(query) = point_row.row() else {
                            return Ok(f64::NAN);
                        };
                        let line = rows.prepared(line_row);
                        degrade_linref_float(line_locate_shape(
                            &model,
                            &line,
                            &rows.frame_cache(row),
                            query,
                            normalized,
                        ))
                    })
                    .collect_rows()
            })
        },
    }
}

pub(crate) fn line_locate_point_m_input(
    py: Python<'_>,
    geom: GeometryInput<'_>,
    point: &Bound<'_, PyAny>,
) -> PyResult<Py<PyAny>> {
    let op_name = Operation::LineLocate.name();
    let point = classify_required(point)?;
    match (geom, point) {
        (GeometryInput::One(geometry), GeometryInput::One(point_geometry)) => {
            Frame::compatible_parts(
                geometry.crs_ref(),
                geometry.epoch(),
                point_geometry.crs_ref(),
                point_geometry.epoch(),
                op_name,
            )?;
            let query = require_locate_point(point_geometry, op_name)?;
            let model = crate::resolve_line_metric(geometry.crs_str(), None, false, op_name)?;
            let line = geometry.plan_or_transient();
            return Ok(line_locate_shape_m(&model, &line, query)?
                .into_pyobject(py)?
                .unbind()
                .into());
        },
        (GeometryInput::One(geometry), GeometryInput::Many(points)) => {
            Frame::compatible_parts(
                geometry.crs_ref(),
                geometry.epoch(),
                points.crs_ref(),
                points.epoch(),
                op_name,
            )?;
            let model = crate::resolve_line_metric(geometry.crs_str(), None, false, op_name)?;
            let line = geometry.plan_or_transient();
            if points.is_compact_nullable()
                && matches!(points.storage(), GeometryArrayStorage::Points { .. })
            {
                let points = points.clone();
                return detach_collect_f64(py, move || {
                    points
                        .row_operation()
                        .rows_with_missing(|| f64::NAN)
                        .map(|logical_row| {
                            let ArrayRow::Present(row) = logical_row else {
                                return Ok(f64::NAN);
                            };
                            let crate::array::ShapeRow::Point(query) = row.row() else {
                                return Ok(f64::NAN);
                            };
                            line_locate_shape_m(&model, &line, query)
                        })
                        .collect_rows()
                });
            }
            let queries = require_locate_points(points, op_name)?;
            let missing = points.missing().cloned();
            detach_collect_f64(py, move || {
                queries
                    .iter()
                    .enumerate()
                    .map(|(row, query)| {
                        if is_missing_row(missing.as_ref(), row) {
                            return Ok(f64::NAN);
                        }
                        line_locate_shape_m(&model, &line, query)
                    })
                    .collect_rows()
            })
        },
        (GeometryInput::Many(array), GeometryInput::One(point_geometry)) => {
            Frame::compatible_parts(
                array.crs_ref(),
                array.epoch(),
                point_geometry.crs_ref(),
                point_geometry.epoch(),
                op_name,
            )?;
            let query = require_locate_point(point_geometry, op_name)?;
            let model = crate::resolve_line_metric(array.crs_str(), None, false, op_name)?;
            let array = array.clone();
            detach_collect_f64(py, move || {
                array
                    .row_operation()
                    .rows_with_missing(|| f64::NAN)
                    .map(|row| match row {
                        ArrayRow::Missing { value, .. } => Ok(value),
                        ArrayRow::Present(row) => row.row().with_shape(|shape| {
                            degrade_linref_float(line_locate_shape_m_value(&model, shape, query))
                        }),
                    })
                    .collect_rows()
            })
        },
        (GeometryInput::Many(array), GeometryInput::Many(points)) => {
            paired_arrays(array, points, op_name)?;
            require_locate_points(points, op_name)?;
            let model = crate::resolve_line_metric(array.crs_str(), None, false, op_name)?;
            let array = array.clone();
            let points = points.clone();
            detach_collect_f64(py, move || {
                array
                    .row_operation()
                    .rows_with_missing(|| f64::NAN)
                    .zip(points.row_operation().rows_with_missing(|| f64::NAN))
                    .map(|(line, point)| match (line, point) {
                        (ArrayRow::Present(line), ArrayRow::Present(point)) => {
                            let crate::array::ShapeRow::Point(query) = point.row() else {
                                return Ok(f64::NAN);
                            };
                            line.row().with_shape(|line| {
                                degrade_linref_float(line_locate_shape_m_value(&model, line, query))
                            })
                        },
                        _ => Ok(f64::NAN),
                    })
                    .collect_rows()
            })
        },
    }
}

pub(crate) fn line_interpolate_points_scalar(
    geometry: &PyGeometry,
    plan: &InterpolatePlan,
    unit: Option<DistanceUnit>,
) -> PyResult<PyGeometryArray> {
    let op_name = Operation::LineInterpolate.name();
    let model = Operation::LineInterpolate
        .resolver_with_line_unit(unit, plan.is_fractional())
        .resolve_ctx(
            &geometry.frame,
            op_name,
            &mut crate::dispatch::MetricScratch::default(),
        )?
        .require_model(op_name)?
        .clone();
    let line = geometry.plan_or_transient();
    let shapes = line_interpolate_points_shape(&model, &line, &geometry.frame_cache(), plan)?;
    Ok(PyGeometryArray::from_shapes(shapes, geometry.frame.clone()))
}
