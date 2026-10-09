use std::sync::Arc;

use pyo3::prelude::*;

use crate::array::{ArrayRow, line_locate_point_on_lines};
use crate::broadcast::{
    CollectRows as _, GeometryInput, classify_required, degrade_linref_float, detach_collect_f64,
    validate_paired_arrays,
};
use crate::crs::frame::Frame;
use crate::dispatch::Lane;
use crate::dispatch::operation::{MetricOp, Operation};
use crate::geometry::storage::PackedStorage;
use crate::geometry::storage::validity::union_many;
use crate::geometry::{FrameDependentCaches, Point, PreparedPlan};
use crate::py::support::LineReferenceBasis;
use crate::{
    DistanceUnit, GeometryArrayStorage, InterpolatePlan, PyGeometry, PyGeometryArray,
    line_interpolate_points_shape, line_locate_shape, line_locate_shape_m, locate_query,
};

/// One located position: a CRS-aware distance, or the stored M value.
#[derive(Clone, Copy)]
struct Locate<'a> {
    model: &'a crate::crs::MetricModel,
    basis: LineReferenceBasis,
    normalized: bool,
}

impl Locate<'_> {
    fn at(
        self,
        plan: &PreparedPlan,
        cache: &FrameDependentCaches,
        query: Point,
        lane: Lane,
    ) -> crate::error::Result<f64> {
        degrade_linref_float(
            match self.basis {
                LineReferenceBasis::Distance => {
                    line_locate_shape(self.model, plan, cache, query, self.normalized)
                },
                LineReferenceBasis::M => line_locate_shape_m(self.model, plan, cache, query),
            },
            lane,
        )
    }
}

/// `line_locate` over every scalar and array pairing of lines and query
/// points. A missing row on either side answers NaN.
pub(crate) fn line_locate_input(
    py: Python<'_>,
    lines: GeometryInput<'_>,
    points: &Bound<'_, PyAny>,
    basis: LineReferenceBasis,
    normalized: bool,
    unit: Option<DistanceUnit>,
) -> PyResult<Py<PyAny>> {
    let op_name = MetricOp::LineLocate.name();
    let points = classify_required(points)?;
    let (crs, frame) = match lines {
        GeometryInput::One(line) => (line.crs_str(), &line.frame),
        GeometryInput::Many(array) => (array.crs_str(), &array.frame),
    };
    let (point_crs, point_epoch) = match &points {
        GeometryInput::One(point) => (point.crs_ref(), point.epoch()),
        GeometryInput::Many(points) => (points.crs_ref(), points.epoch()),
    };
    if let (GeometryInput::Many(array), GeometryInput::Many(points)) = (&lines, &points) {
        validate_paired_arrays(array, points, op_name)?;
    }
    Frame::compatible_parts(
        frame.crs_ref(),
        frame.epoch(),
        point_crs,
        point_epoch,
        op_name,
    )?;
    let model = crate::broadcast::resolve_metric(crs, unit, MetricOp::LineLocate)?;
    let locate = Locate {
        model: &model,
        basis,
        normalized,
    };
    match (lines, points) {
        (GeometryInput::One(line), GeometryInput::One(point)) => {
            let query = locate_query(point.shape.shape())?;
            let plan = line.prepared_plan();
            let located = locate.at(&plan, &line.frame_cache(), query, Lane::Scalar)?;
            Ok(located.into_pyobject(py)?.unbind().into())
        },
        (GeometryInput::One(line), GeometryInput::Many(points)) => {
            let plan = line.prepared_plan();
            let cache = line.frame_cache();
            let points = points.clone();
            detach_collect_f64(py, move || {
                points
                    .row_operation()
                    .rows_with_missing(|| f64::NAN)
                    .map(|row| match row {
                        ArrayRow::Missing { value, .. } => Ok(value),
                        ArrayRow::Present(point) => {
                            let query = point.row().with_shape(locate_query)??;
                            locate.at(&plan, &cache, query, Lane::Scalar)
                        },
                    })
                    .collect_rows()
            })
        },
        (GeometryInput::Many(array), GeometryInput::One(point)) => {
            let query = locate_query(point.shape.shape())?;
            let array = array.clone();
            detach_collect_f64(py, move || {
                let mut rows = array.row_operation();
                rows.rows_with_missing(|| f64::NAN)
                    .map(|row| match row {
                        ArrayRow::Missing { value, .. } => Ok(value),
                        ArrayRow::Present(line) => {
                            let row = line.logical();
                            let line = rows.prepared(line)?;
                            locate.at(&line, &rows.frame_cache(row), query, Lane::Array(row))
                        },
                    })
                    .collect_rows()
            })
        },
        (GeometryInput::Many(array), GeometryInput::Many(points)) => {
            if basis == LineReferenceBasis::Distance
                && !array.is_compact_nullable()
                && !points.is_compact_nullable()
                && let GeometryArrayStorage::Packed(PackedStorage::Lines {
                    coords,
                    offsets,
                    row_map,
                }) = array.storage()
                && let Some(queries) = points.storage().point_rows()
            {
                return line_locate_point_on_lines(
                    py,
                    Arc::clone(coords),
                    offsets.clone(),
                    row_map.clone(),
                    array.len(),
                    queries,
                    union_many([array.missing(), points.missing()]),
                    model.clone(),
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
                    .map(|pair| {
                        let (ArrayRow::Present(line), ArrayRow::Present(point)) = pair else {
                            return Ok(f64::NAN);
                        };
                        let query = point.row().with_shape(locate_query)??;
                        let row = line.logical();
                        let line = rows.prepared(line)?;
                        locate.at(&line, &rows.frame_cache(row), query, Lane::Array(row))
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
    let model = Operation::Metric(MetricOp::LineInterpolate)
        .resolver_with_unit(unit)
        .resolve_ctx(
            &geometry.frame,
            &mut crate::dispatch::MetricScratch::default(),
        )?
        .require_model(MetricOp::LineInterpolate)?
        .clone();
    let line = geometry.prepared_plan();
    let shapes =
        line_interpolate_points_shape(&model, &line, &geometry.frame_cache(), plan, Lane::Scalar)?;
    Ok(PyGeometryArray::from_shapes(
        shapes,
        geometry.frame.clone(),
    )?)
}
