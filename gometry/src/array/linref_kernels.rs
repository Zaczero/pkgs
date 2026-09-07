#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::sync::Arc;

use crate::array::{
    Bound, CollectRows as _, DistanceUnit, GeometryArrayStorage, GeometryError, I64Param,
    InterpolatePlan, PackedStorage, PointRows, Py, PyAny, PyGeometryArray, PyResult, Python,
    RowSelection, Shape, ValidityMask, crs, line_interpolate_points_coordseq,
    line_interpolate_points_shape, line_locate_coordseq, note_array_row, parse_interpolate_plan,
    positive_int, resolve_line_metric, rows_err,
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
    missing: Option<ValidityMask>,
    model: crs::MetricModel,
    normalized: bool,
) -> PyResult<Py<PyAny>> {
    let result = py
        .detach(move || match &model {
            crs::MetricModel::Geodesic { crs, state, .. } => {
                crs::with_geodesic_coordseq_collect_rows_at(crs, state, |metric| {
                    let map = row_map.as_deref();
                    (0..logical_len)
                        .map(|row| {
                            if missing.as_ref().is_some_and(|mask| mask.is_missing(row)) {
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
                        if missing.as_ref().is_some_and(|mask| mask.is_missing(row)) {
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
            let counts = I64Param::parse(count, "count", array.len())?;
            counts.try_validate(|value| {
                positive_int("line_interpolate_points", "count", value).map(|_| ())
            })?;
            let mut budget =
                crate::geometry::ExpansionBudget::new("line_interpolate_points", "count");
            for row in 0..array.len() {
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
        crate::dispatch::MetricOp::LineInterpolatePoints,
    )?;
    if let GeometryArrayStorage::Packed(PackedStorage::Lines {
        coords,
        offsets,
        row_map,
    }) = array.storage()
        && let crs::MetricModel::Planar { .. } = model
    {
        let coords = Arc::clone(coords);
        let offsets = offsets.clone();
        let row_map = row_map.clone();
        let missing = array.missing().cloned();
        let logical_len = array.len();
        let frame = array.frame.clone();
        let result = py.detach(
            move || -> crate::error::Result<_, (usize, crate::error::Error)> {
                let map = row_map.as_deref();
                let mut builder = crate::CsrBuilder::<Shape>::new();
                for row in 0..logical_len {
                    if missing.as_ref().is_some_and(|mask| mask.is_missing(row)) {
                        builder
                            .push_row(std::iter::empty::<Shape>())
                            .map_err(|error| (row, error))?;
                        continue;
                    }
                    let line = GeometryArrayStorage::line_view(&coords, &offsets, map, row);
                    let points = plan
                        .with_row(row, |plan| {
                            line_interpolate_points_coordseq(&model, &line, plan)
                        })
                        .map_err(|error| (row, error))?;
                    builder
                        .push_row(points.into_iter().map(Shape::Point))
                        .map_err(|error| (row, error))?;
                }
                Ok(builder)
            },
        );
        let builder = match result {
            Ok(builder) => builder,
            Err((row, error)) => return Err(note_array_row(error.into(), row)),
        };
        return crate::py::vectors::Groups::from_geometry_builder(frame, builder);
    }
    let array = array.clone();
    let frame = array.frame.clone();
    let result = py.detach(move || interpolate_mixed_row_groups(&array, &plan, &model));
    let builder = match result {
        Ok(builder) => builder,
        Err((row, error)) => return Err(note_array_row(error.into(), row)),
    };
    crate::py::vectors::Groups::from_geometry_builder(frame, builder)
}

fn interpolate_mixed_row_groups(
    array: &PyGeometryArray,
    plan: &RowInterpolatePlan,
    model: &crs::MetricModel,
) -> std::result::Result<crate::CsrBuilder<Shape>, (usize, crate::error::Error)> {
    let mut builder = crate::CsrBuilder::<Shape>::new();
    let mut rows = array.row_operation();
    for logical_row in rows.rows_with_missing(|| ()) {
        match logical_row {
            crate::array::ArrayRow::Missing { logical, .. } => {
                builder
                    .push_row(std::iter::empty::<Shape>())
                    .map_err(|error| (logical, error))?;
            },
            crate::array::ArrayRow::Present(row) => {
                let logical = row.logical();
                let values = plan
                    .with_row(logical, |plan| {
                        let line = rows.prepared(row);
                        line_interpolate_points_shape(
                            &model,
                            &line,
                            &rows.frame_cache(logical),
                            plan,
                        )
                    })
                    .map_err(|error| (logical, error))?;
                builder.push_row(values).map_err(|error| (logical, error))?;
            },
        }
    }
    Ok(builder)
}

#[cfg(test)]
mod tests {
    use super::{RowInterpolatePlan, interpolate_mixed_row_groups};
    use crate::InterpolatePlan;
    use crate::array::{GeometryArrayStorage, PyGeometryArray, ValidityMask};
    use crate::boundary::Frame;
    use crate::crs::{DistanceMode, MetricModel};
    use crate::error::{ErrorKind, ResourceError};
    use crate::geometry::{CoordSeq, LineSeq, Shape};
    use crate::test_support::fail_allocations_after;

    fn line_seq(xs: Vec<f64>, ys: Vec<f64>, zs: Option<Vec<f64>>, ms: Option<Vec<f64>>) -> LineSeq {
        LineSeq::try_new(CoordSeq::from_vecs(xs, ys, zs, ms)).expect("test line")
    }

    fn mixed_zm_and_xy_lines() -> PyGeometryArray {
        let zm = Shape::LineString(line_seq(
            vec![0.0, 10.0],
            vec![0.0, 0.0],
            Some(vec![0.0, 10.0]),
            Some(vec![10.0, 20.0]),
        ));
        let xy = Shape::LineString(line_seq(vec![0.0, 4.0], vec![0.0, 0.0], None, None));
        let array = PyGeometryArray::from_shapes(vec![zm, xy], Frame::None);
        assert!(
            matches!(array.storage(), GeometryArrayStorage::Mixed(_)),
            "unequal axes must take the mixed interpolate walk"
        );
        array
    }

    fn warm_row_products(array: &PyGeometryArray) {
        let mut rows = array.row_operation();
        for logical_row in rows.rows_with_missing(|| ()) {
            if let crate::array::ArrayRow::Present(row) = logical_row {
                let logical = row.logical();
                let _prepared = rows.prepared(row);
                let _cache = rows.frame_cache(logical);
            }
        }
    }

    fn point_xyzm(shape: &Shape) -> (f64, f64, Option<f64>, Option<f64>) {
        match shape {
            Shape::Point(point) => (point.x, point.y, point.z(), point.m()),
            other => panic!("expected point, got {other:?}"),
        }
    }

    #[test]
    fn mixed_count_keeps_source_ordinates() {
        let array = mixed_zm_and_xy_lines();
        let builder = interpolate_mixed_row_groups(
            &array,
            &RowInterpolatePlan::Counts(crate::I64Param::Scalar(2)),
            &MetricModel::COORDINATE,
        )
        .expect("count samples");
        let (shapes, offsets) = builder.finish().expect("csr");
        assert_eq!(offsets, vec![0, 2, 4]);
        assert_eq!(point_xyzm(&shapes[0]), (0.0, 0.0, Some(0.0), Some(10.0)));
        assert_eq!(point_xyzm(&shapes[1]), (10.0, 0.0, Some(10.0), Some(20.0)));
        assert_eq!(point_xyzm(&shapes[2]), (0.0, 0.0, None, None));
        assert_eq!(point_xyzm(&shapes[3]), (4.0, 0.0, None, None));
    }

    #[test]
    fn mixed_at_distances_keep_source_ordinates() {
        let array = mixed_zm_and_xy_lines();
        let builder = interpolate_mixed_row_groups(
            &array,
            &RowInterpolatePlan::Shared(InterpolatePlan::Distances(
                vec![2.5],
                DistanceMode::Absolute,
            )),
            &MetricModel::COORDINATE,
        )
        .expect("at samples");
        let (shapes, offsets) = builder.finish().expect("csr");
        assert_eq!(offsets, vec![0, 1, 2]);
        assert_eq!(point_xyzm(&shapes[0]), (2.5, 0.0, Some(2.5), Some(12.5)));
        assert_eq!(point_xyzm(&shapes[1]), (2.5, 0.0, None, None));
    }

    #[test]
    fn mixed_missing_row_is_empty_group() {
        let array = mixed_zm_and_xy_lines().with_missing_mask(ValidityMask::from_sparse(2, &[1]));
        let builder = interpolate_mixed_row_groups(
            &array,
            &RowInterpolatePlan::Counts(crate::I64Param::Scalar(2)),
            &MetricModel::COORDINATE,
        )
        .expect("missing empty group");
        let (shapes, offsets) = builder.finish().expect("csr");
        assert_eq!(offsets, vec![0, 2, 2]);
        assert_eq!(shapes.len(), 2);
        assert_eq!(point_xyzm(&shapes[0]), (0.0, 0.0, Some(0.0), Some(10.0)));
        assert_eq!(point_xyzm(&shapes[1]), (10.0, 0.0, Some(10.0), Some(20.0)));
    }

    #[test]
    fn mixed_builder_allocation_fails_after_warm_cache() {
        let array = mixed_zm_and_xy_lines().with_missing_mask(ValidityMask::from_sparse(2, &[0]));
        warm_row_products(&array);
        let error = fail_allocations_after(0, || {
            match interpolate_mixed_row_groups(
                &array,
                &RowInterpolatePlan::Counts(crate::I64Param::Scalar(2)),
                &MetricModel::COORDINATE,
            ) {
                Err(error) => error,
                Ok(_) => panic!("builder alloc"),
            }
        });
        assert_eq!(error.0, 0);
        assert!(matches!(
            error.1.kind(),
            ErrorKind::Resource(ResourceError::Allocation)
        ));
    }
}
