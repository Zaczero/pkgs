use std::sync::Arc;

use pyo3::prelude::*;

use crate::array::{ArrayRow, line_locate_point_on_lines};
use crate::broadcast::{
    CollectRows as _, GeometryInput, classify_required, degrade_linref_float, detach_collect_f64,
    validate_paired_arrays,
};
use crate::crs::frame::Frame;
use crate::dispatch::operation::{MetricOp, Operation};
use crate::geometry::storage::PackedStorage;
use crate::geometry::storage::validity::union_many;
use crate::geometry::{FrameDependentCaches, Point, PreparedPlan};
use crate::py::support::{LineReferenceBasis, PreparedLineLocate, prepare_line_locate};
use crate::{
    DistanceUnit, GeometryArrayStorage, InterpolatePlan, PyGeometry, PyGeometryArray,
    line_interpolate_points_shape, locate_query,
};

/// One located position: a CRS-aware distance, or the stored M value.
#[derive(Clone, Copy)]
struct Locate<'a> {
    model: &'a crate::crs::MetricModel,
    basis: LineReferenceBasis,
    normalized: bool,
}

impl Locate<'_> {
    fn admit_query(
        self,
        plan: &PreparedPlan,
        cache: &FrameDependentCaches,
        query: Point,
    ) -> crate::error::Result<()> {
        if let crate::crs::MetricModel::Geodesic { frame, .. } = self.model {
            crate::geometry::GeographicPrepared::try_new(plan, cache, *frame)?;
            if !query.x.is_nan() {
                crate::crs::ensure_geographic_lonlat_in_frame(*frame, query.x, query.y)?;
            }
        }
        Ok(())
    }

    fn prepare<'a>(
        self,
        metric: &'a crate::crs::ResolvedMetric,
        plan: &'a PreparedPlan,
        cache: &FrameDependentCaches,
        query: Point,
    ) -> crate::error::Result<(PreparedLineLocate<'a>, f64)> {
        prepare_line_locate(
            metric,
            plan,
            cache,
            query,
            self.basis == LineReferenceBasis::M,
            self.normalized,
        )
    }
}

fn append_scalar_query_rows<'a>(
    queries: impl Iterator<Item = ArrayRow<'a, f64>>,
    values: &mut Vec<f64>,
    mut locate: impl FnMut(Point) -> crate::error::Result<f64>,
) -> std::result::Result<(), (usize, crate::error::Error)> {
    for row in queries {
        let row_index = values.len();
        match row {
            ArrayRow::Missing { value, .. } => values.push(value),
            ArrayRow::Present(point) => {
                let query = point
                    .row()
                    .with_shape(locate_query)
                    .map_err(|error| (row_index, error))?
                    .map_err(|error| (row_index, error))?;
                let value =
                    degrade_linref_float(locate(query)).map_err(|error| (row_index, error))?;
                values.push(value);
            },
        }
    }
    Ok(())
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
            let cache = line.frame_cache();
            let result = py.detach(move || {
                locate.admit_query(&plan, &cache, query)?;
                let metric = crate::crs::ResolvedMetric::from_model(locate.model)?;
                locate
                    .prepare(&metric, &plan, &cache, query)
                    .map(|(_, located)| located)
            });
            let located = degrade_linref_float(result)?;
            Ok(located.into_pyobject(py)?.unbind().into())
        },
        (GeometryInput::One(line), GeometryInput::Many(points)) => {
            let plan = line.prepared_plan();
            let cache = line.frame_cache();
            let points = points.clone();
            detach_collect_f64(py, move || {
                let rows = points.row_operation();
                let mut queries = rows.rows_with_missing(|| f64::NAN);
                let mut values = Vec::with_capacity(points.len());
                while let Some(row) = queries.next() {
                    let row_index = values.len();
                    match row {
                        ArrayRow::Missing { value, .. } => values.push(value),
                        ArrayRow::Present(point) => {
                            let query = point
                                .row()
                                .with_shape(locate_query)
                                .map_err(|error| (row_index, error))?
                                .map_err(|error| (row_index, error))?;
                            if let Err(error) = locate.admit_query(&plan, &cache, query) {
                                let value = degrade_linref_float(Err(error))
                                    .map_err(|error| (row_index, error))?;
                                values.push(value);
                                append_scalar_query_rows(queries, &mut values, |query| {
                                    locate.admit_query(&plan, &cache, query).map(|()| f64::NAN)
                                })?;
                                return Ok(values);
                            }
                            let metric = crate::crs::ResolvedMetric::from_model(locate.model)
                                .map_err(|error| (row_index, error))?;
                            match locate.prepare(&metric, &plan, &cache, query) {
                                Ok((route, located)) => {
                                    values.push(located);
                                    append_scalar_query_rows(queries, &mut values, |query| {
                                        route.locate(query)
                                    })?;
                                },
                                Err(error) => {
                                    let value = degrade_linref_float(Err(error))
                                        .map_err(|error| (row_index, error))?;
                                    values.push(value);
                                    append_scalar_query_rows(queries, &mut values, |query| {
                                        locate.admit_query(&plan, &cache, query).map(|()| f64::NAN)
                                    })?;
                                },
                            }
                            return Ok(values);
                        },
                    }
                }
                Ok(values)
            })
        },
        (GeometryInput::Many(array), GeometryInput::One(point)) => {
            let query = locate_query(point.shape.shape())?;
            let array = array.clone();
            detach_collect_f64(py, move || {
                let mut rows = array.row_operation();
                let mut metric = None;
                rows.rows_with_missing(|| f64::NAN)
                    .map(|row| match row {
                        ArrayRow::Missing { value, .. } => Ok(value),
                        ArrayRow::Present(line) => {
                            let row = line.logical();
                            let line = rows.prepared(line)?;
                            let cache = rows.frame_cache(row);
                            let result = (|| {
                                locate.admit_query(&line, &cache, query)?;
                                if metric.is_none() {
                                    metric =
                                        Some(crate::crs::ResolvedMetric::from_model(locate.model)?);
                                }
                                let metric = metric
                                    .as_ref()
                                    .expect("a present line resolves the operation metric");
                                locate
                                    .prepare(metric, &line, &cache, query)
                                    .map(|(_, located)| located)
                            })();
                            degrade_linref_float(result)
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
                let mut metric = None;
                rows.rows_with_missing(|| f64::NAN)
                    .zip(point_rows.rows_with_missing(|| f64::NAN))
                    .map(|pair| {
                        let (ArrayRow::Present(line), ArrayRow::Present(point)) = pair else {
                            return Ok(f64::NAN);
                        };
                        let query = point.row().with_shape(locate_query)??;
                        let row = line.logical();
                        let line = rows.prepared(line)?;
                        let cache = rows.frame_cache(row);
                        let result = (|| {
                            locate.admit_query(&line, &cache, query)?;
                            if metric.is_none() {
                                metric =
                                    Some(crate::crs::ResolvedMetric::from_model(locate.model)?);
                            }
                            let metric = metric
                                .as_ref()
                                .expect("a present pair resolves the operation metric");
                            locate
                                .prepare(metric, &line, &cache, query)
                                .map(|(_, located)| located)
                        })();
                        degrade_linref_float(result)
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
    let mut scratch = crate::dispatch::MetricScratch::default();
    let context = Operation::Metric(MetricOp::LineInterpolate)
        .resolver_with_unit(unit)
        .resolve_ctx(&geometry.frame, &mut scratch)?;
    let metric = context.require_resolved(MetricOp::LineInterpolate)?;
    let line = geometry.prepared_plan();
    let shapes = line_interpolate_points_shape(metric, &line, &geometry.frame_cache(), plan)?;
    Ok(PyGeometryArray::from_shapes(
        shapes,
        geometry.frame.clone(),
    )?)
}
