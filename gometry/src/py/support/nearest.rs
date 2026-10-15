use crate::array::ArrayRow;
use crate::geometry::publication::{GeometryOutput, PublicationRows, geographic_line_pending_row};
use crate::geometry::storage::ValidityMask;
use crate::py::support::{
    CoordinateAxes, Point, PyGeometry, PyGeometryArray, PyResult, Python, Shape,
    metric_nearest_points_resolved, rows_err,
};

/// Build the `(left, right)` witness point columns from per-row nearest
/// pairs. A `None` row is an empty operand → `(POINT EMPTY, POINT EMPTY)`
/// (the output-type sentinel), so an empty row never aborts the batch; a
/// present row is dropped to its common axes — both via
/// [`crate::geometry::nearest_point_pair`].
pub(crate) fn nearest_point_columns_masked(
    pairs: impl ExactSizeIterator<Item = crate::error::Result<(Option<(Point, Point)>, CoordinateAxes)>>,
    missing: Option<&ValidityMask>,
    domain: crate::geometry::TopologyDomain,
) -> Result<(PublicationRows, PublicationRows), (usize, crate::error::Error)> {
    let pair_count = pairs.len();
    if missing.is_some_and(|mask| mask.len() != pair_count) {
        return Err((
            0,
            crate::geometry::GeometryErrorKind::Invalid(
                "nearest-point validity does not match its output rows".into(),
            )
            .into(),
        ));
    }
    let mut left = PublicationRows::with_capacity(pair_count);
    let mut right = PublicationRows::with_capacity(pair_count);
    for (row, pair) in pairs.enumerate() {
        let (pair, common) = pair.map_err(|error| (row, error))?;
        if missing.is_some_and(|mask| mask.is_missing(row)) {
            left.try_push_missing().map_err(|error| (row, error))?;
            right.try_push_missing().map_err(|error| (row, error))?;
            continue;
        }
        let (left_shape, right_shape) = crate::geometry::nearest_point_pair(pair.as_ref(), common);
        if domain.is_geographic() {
            left.try_push_drawing(left_shape, row, "nearest_points")
                .map_err(|error| (row, error))?;
            right
                .try_push_drawing(right_shape, row, "nearest_points")
                .map_err(|error| (row, error))?;
        } else {
            left.try_push(GeometryOutput::Shape(left_shape))
                .map_err(|error| (row, error))?;
            right
                .try_push(GeometryOutput::Shape(right_shape))
                .map_err(|error| (row, error))?;
        }
    }
    Ok((left, right))
}

pub(crate) fn fixed_geometry_array_nearest_points_admitted(
    py: Python<'_>,
    fixed: &PyGeometry,
    array: &PyGeometryArray,
    fixed_is_left: bool,
    model: crate::crs::MetricModel,
    domain: crate::geometry::TopologyDomain,
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
                    let pairs = rows
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
                        });
                    Ok(nearest_point_columns_masked(
                        pairs,
                        array_owned.missing(),
                        domain,
                    ))
                },
            )
        })?;
        let (left, right) = rows.map_err(rows_err)?;
        return Ok((
            left.finish(array.frame.clone())?,
            right.finish(array.frame.clone())?,
        ));
    }
    let resolved = crate::crs::ResolvedMetric::from_model(&model)?;
    let source_frame = fixed.frame.clone();
    let pairs = py
        .detach(move || {
            crate::broadcast::with_source_arc_context(
                &resolved,
                &source_frame,
                domain,
                |source_context| {
                    let mut rows = array_owned.row_operation();
                    let pairs = rows
                        .rows_with_missing(|| (None::<(Point, Point)>, CoordinateAxes::XY))
                        .map(|logical_row| {
                            let ArrayRow::Present(row) = logical_row else {
                                return Ok((None, CoordinateAxes::XY));
                            };
                            let element = rows.prepared(row)?;
                            let common =
                                crate::geometry::common_axes(fixed_plan.shape(), element.shape());
                            let fixed_cache = crate::geometry::FrameDependentCaches::default();
                            let element_cache = crate::geometry::FrameDependentCaches::default();
                            if fixed_is_left {
                                metric_nearest_points_resolved(
                                    &resolved,
                                    &fixed_plan,
                                    &fixed_cache,
                                    &element,
                                    &element_cache,
                                    (common.has_z() || common.has_m()).then_some(source_context),
                                )
                            } else {
                                metric_nearest_points_resolved(
                                    &resolved,
                                    &element,
                                    &element_cache,
                                    &fixed_plan,
                                    &fixed_cache,
                                    (common.has_z() || common.has_m()).then_some(source_context),
                                )
                            }
                            .map(|pair| (pair, common))
                        });
                    nearest_point_columns_masked(pairs, array_owned.missing(), domain)
                },
            )
        })
        .map_err(crate::broadcast::rows_err)?;
    Ok((
        pairs.0.finish(array.frame.clone())?,
        pairs.1.finish(array.frame.clone())?,
    ))
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
    let rows = py.detach(move || {
        let mut array_rows = array_owned.row_operation();
        crate::crs::with_resolved_ellipsoid_metric_at(
            crs,
            &[fixed_plan.shape()],
            state,
            |crs, metric| {
                let (semi_major, flattening) = metric.ellipsoid_parameters();
                let frame = crate::geometry::ChartMetric::chart_frame(metric);
                let metric_access = || Ok(metric);
                let source_context = crate::geometry::SourceArcContext::new(
                    crate::geometry::TopologyDomain::Geographic(frame),
                    Some(&metric_access),
                );
                let present = (|| -> std::result::Result<
                    PyGeometryArray,
                    (usize, crate::error::Error),
                > {

                    let mut publication = PublicationRows::with_capacity(array_owned.len());
                    for row in array_rows.rows_with_missing(Shape::empty_point) {
                        let (logical, result) = match row {
                            ArrayRow::Missing { logical, .. } => {
                                (logical, publication.try_push_missing())
                            },
                            ArrayRow::Present(row) => {
                                let logical = row.logical();
                                let result = (|| {
                                    let element = array_rows.prepared(row)?;
                                    let element_cache = array_rows.frame_cache(logical);
                                    let witness = if fixed_is_left {
                                        fixed_plan.geodesic_nearest_line_cached(
                                            &fixed_cache,
                                            &element,
                                            &element_cache,
                                            crs,
                                            semi_major,
                                            flattening,
                                            metric,
                                        )?
                                    } else {
                                        element.geodesic_nearest_line_cached(
                                            &element_cache,
                                            &fixed_plan,
                                            &fixed_cache,
                                            crs,
                                            semi_major,
                                            flattening,
                                            metric,
                                        )?
                                    };
                                    let common = crate::geometry::common_axes(
                                        fixed_plan.shape(),
                                        element.shape(),
                                    );
                                    let source_capture =
                                        crate::geometry::SourceArcCapture::for_output_axes(
                                            common,
                                            &source_context,
                                        );
                                    let line = match witness {
                                        Some((start, end, step)) => {
                                            crate::geometry::nearest_line_with_intent(
                                                Some(&(start, end)),
                                                common,
                                                frame,
                                                crate::geographic::ComputedEdgeIntent::retained_chart_witness(
                                                    frame,
                                                    [start.x, end.x],
                                                    step,
                                                )?,
                                                &source_capture,
                                            )?
                                        },
                                        None => crate::geometry::nearest_line(None, common)?,
                                    };
                                    let pending =
                                        geographic_line_pending_row(
                                            line,
                                            logical,
                                            "shortest_line",
                                        )?;
                                    publication.try_push(GeometryOutput::Pending(pending))
                                })();
                                (logical, result)
                            },
                        };
                        result.map_err(|error| (logical, error))?;
                    }
                    publication
                        .finish(output_frame)
                        .map_err(|error| (array_owned.len().saturating_sub(1), error))
                })();
                Ok(present)
            },
        )
    })?;
    rows.map_err(rows_err)
}

/// The query point of a linear-referencing locate. POINT EMPTY becomes the
/// NaN point, the form an empty point takes in a point column, which locates
/// nowhere.
pub(crate) fn locate_query(shape: &Shape) -> crate::error::Result<Point> {
    Ok(shape
        .point_operand("line_locate")?
        .unwrap_or(Point::new_raw_xy(f64::NAN, f64::NAN)))
}
