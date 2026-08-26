#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::sync::Arc;

use crate::array::{
    Bound, CollectRows as _, DistanceUnit, GeometryArrayStorage, GeometryError, I64Param,
    InterpolatePlan, MissingMask, PointRows, Py, PyAny, PyGeometryArray, PyResult, Python,
    RowSelection, Shape, crs, line_interpolate_points_coordseq, line_interpolate_points_shape,
    line_locate_coordseq, note_array_row, parse_interpolate_plan, positive_int,
    resolve_line_metric, rows_err,
};
use crate::broadcast::degrade_linref_float;
use crate::geometry::{CoordSeq, CsrOffsetColumn};
use crate::py::numpy::float64_array;

enum RowInterpolatePlan {
    Shared(InterpolatePlan),
    Counts(I64Param),
}

impl RowInterpolatePlan {
    fn is_fractional(&self) -> bool {
        match self {
            Self::Shared(plan) => plan.is_fractional(),
            Self::Counts(_) => true,
        }
    }

    fn with_row<T>(&self, row: usize, use_plan: impl FnOnce(&InterpolatePlan) -> T) -> T {
        match self {
            Self::Shared(plan) => use_plan(plan),
            Self::Counts(counts) => {
                let count = usize::try_from(counts.get(row)).expect("validated positive count");
                use_plan(&InterpolatePlan::Count(count))
            },
        }
    }
}

pub(crate) fn line_locate_point_on_lines(
    py: Python<'_>,
    coords: Arc<CoordSeq>,
    offsets: CsrOffsetColumn,
    row_map: RowSelection,
    logical_len: usize,
    queries: PointRows<'_>,
    missing: Option<MissingMask>,
    model: crs::MetricModel,
    normalized: bool,
) -> PyResult<Py<PyAny>> {
    let result = py
        .detach(move || match &model {
            crs::MetricModel::Geodesic { crs, state } => {
                crs::with_geodesic_coordseq_collect_rows_at(crs, state, |metric| {
                    let map = row_map.as_deref();
                    (0..logical_len)
                        .map(|row| {
                            if missing.as_ref().is_some_and(|mask| mask[row]) {
                                return Ok(f64::NAN);
                            }
                            let line = GeometryArrayStorage::line_view(&coords, &offsets, map, row);
                            degrade_linref_float(crs::geodesic_line_locate_coordseq(
                                &line,
                                queries.get(row),
                                normalized,
                                metric,
                            ))
                        })
                        .collect_rows()
                })
            },
            crs::MetricModel::Planar { .. } => {
                let map = row_map.as_deref();
                (0..logical_len)
                    .map(|row| {
                        if missing.as_ref().is_some_and(|mask| mask[row]) {
                            return Ok(f64::NAN);
                        }
                        let line = GeometryArrayStorage::line_view(&coords, &offsets, map, row);
                        degrade_linref_float(line_locate_coordseq(
                            &model,
                            &line,
                            queries.get(row),
                            normalized,
                        ))
                    })
                    .collect_rows()
            },
        })
        .map_err(rows_err)?;
    float64_array(py, result)
}

pub(crate) fn line_interpolate_points_rows(
    py: Python<'_>,
    array: &PyGeometryArray,
    count: Option<&Bound<'_, PyAny>>,
    distances: Option<&Bound<'_, PyAny>>,
    normalized: bool,
    unit: Option<DistanceUnit>,
) -> PyResult<crate::py::vectors::Groups> {
    let plan = match (count, distances) {
        (Some(count), None) => {
            if normalized {
                return Err(GeometryError::new_err(
                    "normalized applies to at; count samples fractions already",
                ));
            }
            let counts = I64Param::parse(count, "count", array.storage().len())?;
            counts.try_validate(|value| {
                positive_int("line_interpolate_points", "count", value).map(|_| ())
            })?;
            let mut budget =
                crate::geometry::ExpansionBudget::new("line_interpolate_points", "count");
            for row in 0..array.storage().len() {
                if !array.is_row_missing(row) {
                    budget
                        .add(usize::try_from(counts.get(row)).expect("validated positive count"))?;
                }
            }
            RowInterpolatePlan::Counts(counts)
        },
        (None, Some(distances)) => {
            RowInterpolatePlan::Shared(parse_interpolate_plan(None, Some(distances), normalized)?)
        },
        _ => {
            return Err(GeometryError::new_err(
                "line_interpolate requires exactly one of at or count",
            ));
        },
    };
    let model = resolve_line_metric(
        array.crs_str(),
        unit,
        plan.is_fractional(),
        "line_interpolate_points",
    )?;
    if let GeometryArrayStorage::Lines {
        coords,
        offsets,
        row_map,
    } = array.storage()
        && let crs::MetricModel::Planar { .. } = model
    {
        let coords = Arc::clone(coords);
        let offsets = offsets.clone();
        let row_map = row_map.clone();
        let missing = array.missing().cloned();
        let logical_len = array.storage().len();
        let frame = array.frame.clone();
        let (shapes, group_offsets) = py.detach(move || -> PyResult<_> {
            let map = row_map.as_deref();
            let mut builder = crate::CsrBuilder::<Shape>::new();
            for row in 0..logical_len {
                let values = if missing.as_ref().is_some_and(|mask| mask[row]) {
                    Vec::new()
                } else {
                    let line = GeometryArrayStorage::line_view(&coords, &offsets, map, row);
                    plan.with_row(row, |plan| {
                        line_interpolate_points_coordseq(&model, &line, plan)
                    })
                    .map_err(|error| note_array_row(error.into(), row))?
                    .into_iter()
                    .map(Shape::Point)
                    .collect()
                };
                builder
                    .push_row(values)
                    .map_err(|error| note_array_row(error.into(), row))?;
            }
            Ok(builder.finish())
        })?;
        return crate::py::vectors::Groups::from_geometry_flat(
            PyGeometryArray::from_shapes(shapes, frame),
            group_offsets,
        );
    }
    let mut builder = crate::CsrBuilder::<Shape>::new();
    for (row, handle) in array.storage().iter_rows().enumerate() {
        let values = if array.missing().is_some_and(|mask| mask[row]) {
            Vec::new()
        } else {
            plan.with_row(row, |plan| {
                let line = array.prepared_row(row, handle);
                line_interpolate_points_shape(&model, &line, &array.row_frame_cache(row), plan)
            })
            .map_err(|error| note_array_row(error.into(), row))?
        };
        builder
            .push_row(values)
            .map_err(|error| note_array_row(error.into(), row))?;
    }
    let (shapes, offsets) = builder.finish();
    crate::py::vectors::Groups::from_geometry_flat(
        PyGeometryArray::from_shapes(shapes, array.frame.clone()),
        offsets,
    )
}
