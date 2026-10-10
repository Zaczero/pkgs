use std::sync::Arc;

use crate::array::{
    Bound, CollectRows as _, DistanceUnit, GeometryError, I64Param, InterpolatePlan, Py, PyAny,
    PyGeometryArray, PyResult, Python, Shape, crs, line_interpolate_points_coordseq,
    line_interpolate_points_shape, line_locate_coordseq, note_array_row, parse_interpolate_plan,
    positive_int, resolve_metric, rows_err,
};
use crate::broadcast::degrade_linref_float;
use crate::geometry::storage::{
    GeometryArrayStorage, PackedStorage, PointRows, RowSelection, ValidityMask,
};
use crate::geometry::{CoordSeq, CsrOffsetColumn};
use crate::py::numpy::float64_array;
use crate::py::vectors::Groups;

enum RowInterpolatePlan {
    Shared(InterpolatePlan),
    Counts(I64Param),
}

impl RowInterpolatePlan {
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
            crs::MetricModel::Geodesic {
                crs: geodesic_crs,
                frame,
                state,
            } => {
                let map = row_map.as_deref();
                let first_present = (0..logical_len)
                    .find(|&row| missing.as_ref().is_none_or(|mask| !mask.is_missing(row)));
                if let Some(row) = first_present {
                    let line = GeometryArrayStorage::line_view(&coords, &offsets, map, row);
                    crate::crs::ensure_geographic_coordseq_in_frame(*frame, &line)
                        .map_err(|error| (row, error))?;
                    let query = queries.get(row);
                    if !query.x.is_nan() {
                        crate::crs::ensure_geographic_lonlat_in_frame(*frame, query.x, query.y)
                            .map_err(|error| (row, error))?;
                    }
                }
                if let Some(first_present) = first_present {
                    crate::crs::with_geographic_coordseq_metric_at(geodesic_crs, state, |metric| {
                        let rows = (0..logical_len)
                            .map(|row| {
                                if missing.as_ref().is_some_and(|mask| mask.is_missing(row)) {
                                    return Ok(f64::NAN);
                                }
                                let line =
                                    GeometryArrayStorage::line_view(&coords, &offsets, map, row);
                                if row != first_present {
                                    crate::crs::ensure_geographic_coordseq_in_frame(*frame, &line)?;
                                }
                                let located = crate::crs::chart_line_locate_coordseq_admitted(
                                    &line,
                                    queries.get(row),
                                    normalized,
                                    metric,
                                );
                                degrade_linref_float(located, crate::dispatch::Lane::Array(row))
                            })
                            .collect_rows();
                        Ok(rows)
                    })
                    .map_err(|error| (first_present, error))?
                } else {
                    crate::crs::with_geographic_coordseq_metric_at(geodesic_crs, state, |_| {
                        Ok(Ok(vec![f64::NAN; logical_len]))
                    })
                    .map_err(|error| (0, error))?
                }
            },
            crs::MetricModel::Planar { .. } => {
                let map = row_map.as_deref();
                (0..logical_len)
                    .map(|row| {
                        if missing.as_ref().is_some_and(|mask| mask.is_missing(row)) {
                            return Ok(f64::NAN);
                        }
                        let line = GeometryArrayStorage::line_view(&coords, &offsets, map, row);
                        degrade_linref_float(
                            line_locate_coordseq(&model, &line, queries.get(row), normalized),
                            crate::dispatch::Lane::Array(row),
                        )
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
) -> PyResult<Groups> {
    let plan = match (count, distances) {
        (Some(count), None) => {
            if normalized {
                return Err(GeometryError::new_err(
                    "normalized applies to at; count samples fractions already",
                ));
            }
            let counts = I64Param::parse(count, "count", array.len())?;
            counts.try_validate(|value| {
                positive_int("line_interpolate", "count", value).map(|_| ())
            })?;
            for row in 0..array.len() {
                if !array.is_row_missing(row) {
                    crate::geometry::ExpansionBudget::check(
                        "line_interpolate",
                        "count",
                        usize::try_from(counts.get(row)).expect("validated positive count"),
                    )?;
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
    let model = resolve_metric(
        array.crs_str(),
        unit,
        crate::dispatch::MetricOp::LineInterpolate,
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
                let mut builder = crate::CsrBuilder::<Shape>::with_capacity(0, logical_len);
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
                    builder.push_row(points).map_err(|error| (row, error))?;
                }
                Ok(builder)
            },
        );
        let builder = match result {
            Ok(builder) => builder,
            Err((row, error)) => return Err(note_array_row(error.into(), row)),
        };
        return Groups::from_geometry_builder(frame, builder, array.missing().cloned());
    }
    let array = array.clone();
    let frame = array.frame.clone();
    let missing = array.missing().cloned();
    let result = py.detach(move || interpolate_mixed_row_groups(&array, &plan, &model));
    let builder = match result {
        Ok(builder) => builder,
        Err((row, error)) => return Err(note_array_row(error.into(), row)),
    };
    Groups::from_geometry_builder(frame, builder, missing)
}

fn interpolate_mixed_row_groups(
    array: &PyGeometryArray,
    plan: &RowInterpolatePlan,
    model: &crs::MetricModel,
) -> std::result::Result<crate::CsrBuilder<Shape>, (usize, crate::error::Error)> {
    let mut builder = crate::CsrBuilder::<Shape>::with_capacity(0, array.len());
    let mut rows = array.row_operation();
    let mut metric = None;
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
                        if plan.sample_count() == 0 {
                            return Ok(Vec::new());
                        }
                        let line = rows.prepared(row)?;
                        let cache = rows.frame_cache(logical);
                        if let crs::MetricModel::Geodesic { frame, .. } = model {
                            crate::geometry::GeographicPrepared::try_new(&line, &cache, *frame)?;
                        }
                        if metric.is_none() {
                            metric = Some(crs::ResolvedMetric::from_model(model)?);
                        }
                        line_interpolate_points_shape(
                            metric
                                .as_ref()
                                .expect("a present row resolves the operation metric"),
                            &line,
                            &cache,
                            plan,
                            crate::dispatch::Lane::Array(logical),
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
    use crate::array::PyGeometryArray;
    use crate::crs::frame::{Frame, crs_arc};
    use crate::crs::{DistanceMode, MetricModel};
    use crate::geometry::storage::{GeometryArrayStorage, ValidityMask};
    use crate::geometry::{CoordSeq, LineSeq, Shape};
    use crate::test_support::linear_reference_counters;

    fn line_seq(xs: Vec<f64>, ys: Vec<f64>, zs: Option<Vec<f64>>, ms: Option<Vec<f64>>) -> LineSeq {
        LineSeq::try_new(CoordSeq::try_from_vecs(xs, ys, zs, ms).unwrap()).expect("test line")
    }

    fn mixed_zm_and_xy_lines() -> PyGeometryArray {
        let zm = Shape::LineString(line_seq(
            vec![0.0, 10.0],
            vec![0.0, 0.0],
            Some(vec![0.0, 10.0]),
            Some(vec![10.0, 20.0]),
        ));
        let xy = Shape::LineString(line_seq(vec![0.0, 4.0], vec![0.0, 0.0], None, None));
        let array = PyGeometryArray::from_shapes(vec![zm, xy], Frame::None).unwrap();
        assert!(
            matches!(array.storage(), GeometryArrayStorage::Mixed(_)),
            "unequal axes must take the mixed interpolate walk"
        );
        array
    }

    fn geographic_lines(lines: Vec<Shape>, crs: &str) -> PyGeometryArray {
        PyGeometryArray::from_shapes(lines, Frame::Crs(crs_arc(crs))).unwrap()
    }

    fn geographic_line(end_latitude: f64) -> Shape {
        Shape::LineString(line_seq(
            vec![0.0, 0.0],
            vec![0.0, end_latitude],
            None,
            None,
        ))
    }

    fn point_xyzm(shape: &Shape) -> (f64, f64, Option<f64>, Option<f64>) {
        let Shape::Point(point) = shape else {
            panic!("expected point, got {shape:?}");
        };
        (point.x, point.y, point.z(), point.m())
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
        let (shapes, offsets) = builder.finish();
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
        let (shapes, offsets) = builder.finish();
        assert_eq!(offsets, vec![0, 1, 2]);
        assert_eq!(point_xyzm(&shapes[0]), (2.5, 0.0, Some(2.5), Some(12.5)));
        assert_eq!(point_xyzm(&shapes[1]), (2.5, 0.0, None, None));
    }

    #[test]
    fn mixed_missing_row_is_empty_group() {
        let array = mixed_zm_and_xy_lines()
            .with_missing_mask(ValidityMask::from_sparse(2, &[1]))
            .unwrap();
        let builder = interpolate_mixed_row_groups(
            &array,
            &RowInterpolatePlan::Counts(crate::I64Param::Scalar(2)),
            &MetricModel::COORDINATE,
        )
        .expect("missing empty group");
        let (shapes, offsets) = builder.finish();
        assert_eq!(offsets, vec![0, 2, 2]);
        assert_eq!(shapes.len(), 2);
        assert_eq!(point_xyzm(&shapes[0]), (0.0, 0.0, Some(0.0), Some(10.0)));
        assert_eq!(point_xyzm(&shapes[1]), (10.0, 0.0, Some(10.0), Some(20.0)));
    }

    #[test]
    fn array_count_sampling_admits_source_rows_before_ellipsoid_resolution() {
        let bad_crs = "+proj=longlat +a=6378137 +f=0.1 +type=crs";
        let plan = RowInterpolatePlan::Counts(crate::I64Param::Scalar(1));

        let two_faults = geographic_lines(vec![geographic_line(95.0)], bad_crs);
        let model = crate::broadcast::resolve_metric(
            two_faults.crs_str(),
            None,
            crate::dispatch::MetricOp::LineInterpolate,
        )
        .unwrap();
        linear_reference_counters::reset();
        let (row, error) = interpolate_mixed_row_groups(&two_faults, &plan, &model)
            .err()
            .expect("source admission rejects the invalid line");
        assert_eq!(row, 0);
        assert!(
            error
                .to_string()
                .contains("invalid longitude/latitude (0, 95)"),
            "{error}",
        );
        assert_eq!(linear_reference_counters::snapshot().resolved_metrics, 0,);

        let valid_source = geographic_lines(vec![geographic_line(1.0)], bad_crs);
        let model = crate::broadcast::resolve_metric(
            valid_source.crs_str(),
            None,
            crate::dispatch::MetricOp::LineInterpolate,
        )
        .unwrap();
        linear_reference_counters::reset();
        let (_, error) = interpolate_mixed_row_groups(&valid_source, &plan, &model)
            .err()
            .expect("ellipsoid resolution rejects the unsupported flattening");
        assert!(
            error
                .to_string()
                .contains("order-six binary64 series domain")
        );
        assert_eq!(linear_reference_counters::snapshot().resolved_metrics, 1,);

        let invalid_source = geographic_lines(vec![geographic_line(95.0)], "EPSG:4326");
        let model = crate::broadcast::resolve_metric(
            invalid_source.crs_str(),
            None,
            crate::dispatch::MetricOp::LineInterpolate,
        )
        .unwrap();
        linear_reference_counters::reset();
        let (_, error) = interpolate_mixed_row_groups(&invalid_source, &plan, &model)
            .err()
            .expect("source admission rejects the invalid latitude");
        assert!(
            error
                .to_string()
                .contains("invalid longitude/latitude (0, 95)")
        );
        assert_eq!(linear_reference_counters::snapshot().resolved_metrics, 0,);

        let nullable = geographic_lines(vec![geographic_line(1.0), geographic_line(95.0)], bad_crs)
            .with_missing_mask(ValidityMask::from_sparse(2, &[0]))
            .unwrap();
        let model = crate::broadcast::resolve_metric(
            nullable.crs_str(),
            None,
            crate::dispatch::MetricOp::LineInterpolate,
        )
        .unwrap();
        linear_reference_counters::reset();
        let (row, error) = interpolate_mixed_row_groups(&nullable, &plan, &model)
            .err()
            .expect("source admission rejects the first present invalid line");
        assert_eq!(row, 1);
        assert!(
            error
                .to_string()
                .contains("invalid longitude/latitude (0, 95)")
        );
        assert_eq!(linear_reference_counters::snapshot().resolved_metrics, 0,);
    }

    #[test]
    fn mixed_geographic_count_sampling_resolves_one_metric_for_all_rows() {
        let array = geographic_lines(
            vec![
                Shape::LineString(line_seq(vec![0.0, 2.0], vec![0.0, 0.0], None, None)),
                Shape::LineString(line_seq(
                    vec![10.0, 12.0],
                    vec![1.0, 1.0],
                    Some(vec![3.0, 7.0]),
                    None,
                )),
            ],
            "OGC:CRS84",
        );
        assert!(
            matches!(array.storage(), GeometryArrayStorage::Mixed(_)),
            "different coordinate axes use mixed rows"
        );
        let model = crate::broadcast::resolve_metric(
            array.crs_str(),
            None,
            crate::dispatch::MetricOp::LineInterpolate,
        )
        .unwrap();
        linear_reference_counters::reset();

        let builder = interpolate_mixed_row_groups(
            &array,
            &RowInterpolatePlan::Counts(crate::I64Param::Scalar(2)),
            &model,
        )
        .expect("geographic count samples");
        let (shapes, offsets) = builder.finish();

        assert_eq!(offsets, [0, 2, 4]);
        assert_eq!(point_xyzm(&shapes[0]), (0.0, 0.0, None, None));
        assert_eq!(point_xyzm(&shapes[1]), (2.0, 0.0, None, None));
        assert_eq!(point_xyzm(&shapes[2]), (10.0, 1.0, Some(3.0), None));
        assert_eq!(point_xyzm(&shapes[3]), (12.0, 1.0, Some(7.0), None));
        assert_eq!(linear_reference_counters::snapshot().resolved_metrics, 1,);
    }
}
