use super::frame::LOCAL_SCALE_ERROR_LIMIT;
use super::input::{LocalInputPurpose, PreparedLocalInput};
use crate::error::Result;
use crate::geometry::{
    Point, PointKey, Shape, ShapeCountQuery, VertexProvenance, emit_from_original,
    geometric_xy_cmp, shape_from_selected_hull,
};

/// CRS-aware constructive operation with one generated-work budget threaded
/// through projection-input refinement and the projected kernel.
pub(crate) fn metric_constructive_shape_budgeted(
    model: &crate::crs::MetricModel,
    crs: Option<&str>,
    shape: &Shape,
    distance: f64,
    budget: &mut crate::geometry::ExpansionBudget,
    mut kernel: impl FnMut(&Shape, f64, &mut crate::geometry::ExpansionBudget) -> Result<Shape>,
) -> Result<Shape> {
    match model {
        crate::crs::MetricModel::Planar { to_metre } => {
            kernel(shape, distance / to_metre.get(), budget)
        },
        crate::crs::MetricModel::Geodesic { state, .. } => geodesic_local_shape(
            shape,
            crs,
            true,
            Some(distance),
            state,
            budget,
            |shape, budget| kernel(shape, distance, budget),
        ),
    }
}

/// CRS-aware constructive whose precision is optional. An omitted tolerance
/// stays omitted until the kernel sees the working geometry, allowing its
/// scale-aware default to be computed in the actual execution frame.
pub(crate) fn metric_optional_constructive_shape(
    model: &crate::crs::MetricModel,
    crs: Option<&str>,
    shape: &Shape,
    tolerance: Option<f64>,
    kernel: impl Fn(&Shape, Option<f64>) -> Result<Shape>,
) -> Result<Shape> {
    match model {
        crate::crs::MetricModel::Planar { to_metre } => {
            kernel(shape, tolerance.map(|value| value / to_metre.get()))
        },
        crate::crs::MetricModel::Geodesic { state, .. } => {
            let mut budget = crate::geometry::ExpansionBudget::new("polylabel", "input_refinement");
            geodesic_local_shape(shape, crs, true, None, state, &mut budget, |shape, _| {
                kernel(shape, tolerance)
            })
        },
    }
}

/// CRS-aware concave hull. Projected/CRS-free inputs run the native kernel in
/// the receiver coordinate frame (scaling `length_threshold` for projected SI
/// metrics). Geographic inputs use a local projection only to decide which
/// input vertices survive. Selected witnesses supply their original ordinates;
/// the new boundary edges receive canonical computed longitude spelling.
pub(crate) fn metric_concave_hull(
    model: &crate::crs::MetricModel,
    crs: Option<&str>,
    shape: &Shape,
    concavity: f64,
    length_threshold: f64,
) -> Result<Shape> {
    match model {
        crate::crs::MetricModel::Planar { to_metre } => {
            shape.concave_hull(concavity, length_threshold / to_metre.get())
        },
        crate::crs::MetricModel::Geodesic { state, .. } => {
            let mut budget =
                crate::geometry::ExpansionBudget::new("concave_hull", "input_refinement");
            geodesic_by_identity(
                shape,
                crs,
                state,
                &mut budget,
                LocalInputPurpose::OriginalSites,
                |projected, _, _| {
                    let capacity = ShapeCountQuery::default().coordinates(projected).ok();
                    let mut seen = crate::collections::try_hash_set()?;
                    let mut pairs = Vec::new();
                    if let Some(capacity) = capacity {
                        #[expect(
                            clippy::disallowed_methods,
                            reason = "XY deduplication can make these outputs much smaller than the coordinate count."
                        )]
                        let _seen_reservation = seen.try_reserve(capacity);
                        #[expect(
                            clippy::disallowed_methods,
                            reason = "XY deduplication can make these outputs much smaller than the coordinate count."
                        )]
                        let _pairs_reservation = pairs.try_reserve(capacity);
                    }
                    let mut point_index = 0_usize;
                    projected.for_each_point(|point| {
                        // Projected vertices are 1:1 with the source; the original
                        // index is the enumeration order (no identity-index slice).
                        let original_index = point_index;
                        point_index = point_index.wrapping_add(1);
                        if seen.insert(PointKey::new(point)) {
                            // A count-derived reservation, when representable,
                            // covers every source vertex.
                            pairs.push((point, original_index));
                        }
                    });
                    if pairs.is_empty() {
                        return Ok((Vec::new(), Shape::empty_polygon()));
                    }
                    pairs.sort_unstable_by(|(left, _), (right, _)| geometric_xy_cmp(*left, *right));
                    let mut projected_points = Vec::with_capacity(pairs.len());
                    for &(point, _) in &pairs {
                        projected_points.push(point.to_xy());
                    }
                    let mut provenance = Vec::with_capacity(projected_points.len());
                    let mut selected = Vec::with_capacity(projected_points.len());
                    for index in crate::geometry::native_concave_hull(
                        &projected_points,
                        concavity,
                        length_threshold,
                    )? {
                        let (_, original_index) = pairs
                            .get(index)
                            .expect("hull vertex index is in the projected input");
                        provenance.push(VertexProvenance::Input(*original_index));
                        selected.push(
                            pairs
                                .get(index)
                                .expect("selected hull index belongs to sites")
                                .0,
                        );
                    }
                    Ok((
                        provenance,
                        shape_from_selected_hull(&selected, Shape::empty_polygon)?,
                    ))
                },
                |points, _, plan, _, _| emit_from_original(points, plan),
                |points| shape_from_selected_hull(&points, Shape::empty_polygon),
            )
        },
    }
}

pub(crate) fn geodesic_by_identity(
    shape: &Shape,
    crs: Option<&str>,
    state: &crate::crs::RuntimeState,
    budget: &mut crate::geometry::ExpansionBudget,
    purpose: LocalInputPurpose,
    plan_fn: impl FnOnce(
        &Shape,
        &Shape,
        &[crate::geometry::RefinedChainProvenance],
    ) -> Result<(Vec<VertexProvenance>, Shape)>,
    emit_fn: impl FnOnce(
        &[Point],
        &[Point],
        &[VertexProvenance],
        crate::geometry::CoordinateAxes,
        crate::geographic::GeographicFrame,
    ) -> Result<Vec<Point>>,
    rebuild: impl FnOnce(Vec<Point>) -> Result<Shape>,
) -> Result<Shape> {
    if shape.is_empty() {
        return rebuild(Vec::new());
    }
    let source = crs.unwrap_or("EPSG:4326");
    let prepared =
        PreparedLocalInput::prepare(shape, &[shape], source, None, purpose, state, budget)?
            .expect("nonempty identity input prepares a local frame");
    let original = prepared.source_original(shape).points_vec()?;
    let projected_original = prepared.projected_original();
    if ShapeCountQuery::default().coordinates(projected_original)? != original.len() {
        return Err(crate::crs::CrsError::invalid(
            "internal identity projection changed point count",
        ));
    }
    let (plan, projected_output) = plan_fn(
        projected_original,
        prepared.projected_working(),
        prepared.refined_chains(),
    )?;
    let preserve_input_vertices = plan
        .iter()
        .all(|vertex| matches!(vertex, VertexProvenance::Input(_)));
    let has_segment_witness = plan.iter().any(|vertex| {
        matches!(
            vertex,
            VertexProvenance::OnSegment { .. } | VertexProvenance::OnSource { .. }
        )
    });
    let source_axes = prepared.source_original(shape).axes();
    let restored = if has_segment_witness {
        let support_points = prepared.source_working(shape).points_vec()?;
        let total = original.len().wrapping_add(support_points.len());
        let mut source_points = Vec::with_capacity(total);
        source_points.extend(original.iter().copied());
        source_points.extend(support_points);
        rebuild(emit_fn(
            &source_points,
            &original,
            &plan,
            source_axes,
            prepared.geographic_frame(),
        )?)?
    } else {
        rebuild(emit_fn(
            &original,
            &original,
            &plan,
            source_axes,
            prepared.geographic_frame(),
        )?)?
    };
    if matches!(restored, Shape::Point(_)) {
        return Ok(restored);
    }
    super::output::publish(
        &projected_output,
        &restored,
        prepared.local_crs(),
        prepared.geographic_frame(),
        budget.operation(),
        preserve_input_vertices,
        budget,
    )
}

#[cfg(test)]
fn metric_minimum_bounding_circle(
    model: &crate::crs::MetricModel,
    crs: Option<&str>,
    shape: &Shape,
) -> Result<Shape> {
    super::FramedCircle::solve(
        model,
        crs,
        shape,
        None,
        false,
        None,
        crate::geometry::TopologyDomain::Cartesian,
    )?
    .to_polygon(std::num::NonZeroU32::new(16).unwrap())
}

/// Run a meter-based planar operation in the shape's best local projection,
/// returning the result in the source CRS after a linear scale-error fit check.
/// Used by the geodesic constructive paths; pure shape work, so array lanes
/// run it detached.
///
/// The operation is a **local projection approximation** of geodesic
/// construction: one extent-validated local frame is chosen, the shape is
/// transformed there, the planar kernel runs at meter scale, and the result is
/// transformed back. The frame must hold the input and the result together. Distortion accumulates when the geometry spans many UTM
/// zones, sits far from the anchor, or uses a large distance relative to the
/// feature size. The returned geometry is tagged with the input CRS; it is a
/// CRS-aware local projection result, not an exact ellipsoidal buffer.
fn geodesic_local_shape(
    shape: &Shape,
    crs: Option<&str>,
    checked: bool,
    operation_distance_m: Option<f64>,
    state: &crate::crs::RuntimeState,
    budget: &mut crate::geometry::ExpansionBudget,
    mut op: impl FnMut(&Shape, &mut crate::geometry::ExpansionBudget) -> Result<Shape>,
) -> Result<Shape> {
    if shape.is_empty() {
        return op(shape, budget);
    }
    let source = crs.unwrap_or("EPSG:4326");
    let starting_budget = budget.clone();
    *budget = starting_budget.clone();
    let initial = PreparedLocalInput::prepare(
        shape,
        &[shape],
        source,
        operation_distance_m,
        LocalInputPurpose::BoundarySupport,
        state,
        budget,
    )?
    .expect("nonempty geometry prepares a local frame");
    let initial_definition = initial.local_crs().definition.clone();
    let restored = run_local_projected_operation(&initial, source, state, budget, &mut op)?;
    let initial_fits = !checked || initial.restored_result_fits(&restored, source, state)?;
    drop(initial);
    // Distance-driven ops (buffer/offset/densify) demand metric accuracy, so
    // they reject a result that exceeds the local projection's linear
    // scale-error budget. The minimum bounding *circle* is an intentionally
    // approximate bounding primitive (like bounds/centroid) that must remain
    // usable at city-to-continental scale, so it skips the fit check.
    if initial_fits {
        return Ok(restored);
    }
    // The frame was fitted to the input, and an operation's reach (a buffer
    // distance) carries the result past it: a point beside a UTM zone edge
    // fits that zone while its buffer does not. Fit one frame to the input
    // and the result together, since the operation measures between them,
    // and run once more there.
    let exceeded = || {
        crate::crs::CrsError::invalid(format!(
            "constructive result exceeds the {:.1}% linear scale-error limit in one local projection; use a smaller distance, split the geometry, or choose a projected CRS explicitly",
            LOCAL_SCALE_ERROR_LIMIT * 100.0
        ))
    };
    *budget = starting_budget;
    let fitting_shapes = [shape, &restored];
    let refit_local = PreparedLocalInput::select_local_frame(&fitting_shapes, source, state)
        .map_err(|error| crate::crs::preserve_resource_error(error, |_| exceeded()))?;
    let refit = PreparedLocalInput::prepare_selected(
        shape,
        refit_local,
        source,
        operation_distance_m,
        LocalInputPurpose::BoundarySupport,
        state,
        budget,
    )?;
    let refit = refit.expect("nonempty geometry prepares a refitted local frame");
    if refit.local_crs().definition != initial_definition {
        let refitted_result =
            run_local_projected_operation(&refit, source, state, budget, &mut op)?;
        if refit.restored_result_fits(&refitted_result, source, state)? {
            return Ok(refitted_result);
        }
    }
    Err(exceeded())
}

fn run_local_projected_operation(
    prepared: &PreparedLocalInput,
    source: &str,
    state: &crate::crs::RuntimeState,
    budget: &mut crate::geometry::ExpansionBudget,
    op: &mut impl FnMut(&Shape, &mut crate::geometry::ExpansionBudget) -> Result<Shape>,
) -> Result<Shape> {
    let local = prepared.local_crs();
    let result = op(prepared.projected_working(), budget)?;
    let restored = crate::crs::transform_at(&result, &local.definition, source, state)
        .map_err(|error| local.annotate_error(error))?;
    super::output::publish(
        &result,
        &restored,
        local,
        prepared.geographic_frame(),
        budget.operation(),
        false,
        budget,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::crs::runtime_state_snapshot;
    use crate::geometry::{
        BufferCapStyle, BufferJoinStyle, CoordSeq, GeodesicMetric as _, LineSeq, XY,
    };

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn minimum_bounding_circle_of_one_point_keeps_the_source_record() {
        let source = Shape::Point(Point::new_raw_xy(370.125, -0.0));
        let model = crate::crs::metric_model(Some("EPSG:4326")).unwrap();
        assert_eq!(
            metric_minimum_bounding_circle(&model, Some("EPSG:4326"), &source).unwrap(),
            source
        );
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn minimum_bounding_circle_across_the_seam_publishes_minor_edges() {
        let source = crate::io::parse_wkt("MULTIPOINT ((179.8 0), (-179.8 0))").unwrap();
        let model = crate::crs::metric_model(Some("EPSG:4326")).unwrap();
        let output = metric_minimum_bounding_circle(&model, Some("EPSG:4326"), &source).unwrap();
        let Shape::Polygon(output) = output else {
            unreachable!()
        };
        assert!(
            output
                .shell
                .iter()
                .all(|point| (-180.0..180.0).contains(&point.x))
        );
        let axis = crate::geographic::GeographicFrame::crs84().longitude();
        for [start, end] in crate::geometry::Coordinates::segment_pairs(output.shell.coords()) {
            assert!(
                axis.chart_step(start.x, end.x).unwrap().delta(axis).abs() < axis.half_period()
            );
        }
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn buffer_near_the_seam_keeps_canonical_vertices_without_densifying() {
        let state = runtime_state_snapshot().unwrap();
        let input = Shape::Point(Point::new_raw_xy(179.9, 0.0));
        let local = PreparedLocalInput::select_local_frame(&[&input], "EPSG:4326", &state).unwrap();
        let projected =
            crate::crs::transform_at(&input, "EPSG:4326", &local.definition, &state).unwrap();
        let projected_output = projected.buffer(20_000.0).unwrap();
        let before =
            crate::crs::transform_at(&projected_output, &local.definition, "EPSG:4326", &state)
                .unwrap();
        let mut budget = crate::geometry::ExpansionBudget::new("buffer", "quadrant_segments");
        let after = geodesic_local_shape(
            &input,
            Some("EPSG:4326"),
            true,
            Some(20_000.0),
            &state,
            &mut budget,
            |working, _| working.buffer(20_000.0),
        )
        .unwrap();
        let (Shape::Polygon(before), Shape::Polygon(after)) = (before, after) else {
            unreachable!()
        };
        assert_eq!(after.shell.len(), before.shell.len());
        let axis = crate::geographic::GeographicFrame::crs84().longitude();
        for [start, end] in crate::geometry::Coordinates::segment_pairs(after.shell.coords()) {
            assert!(
                axis.chart_step(start.x, end.x).unwrap().delta(axis).abs() < axis.half_period()
            );
        }
        assert!(
            after
                .shell
                .iter()
                .all(|point| (-180.0..180.0).contains(&point.x))
        );
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn polar_buffer_keeps_its_certified_minor_edges_and_complete_turn() {
        let state = runtime_state_snapshot().unwrap();
        let input = Shape::Point(Point::new_raw_xy(0.0, 89.9));
        let local = PreparedLocalInput::select_local_frame(&[&input], "EPSG:4326", &state).unwrap();
        let projected_input =
            crate::crs::transform_at(&input, "EPSG:4326", &local.definition, &state).unwrap();
        let projected_output = projected_input.buffer(20_000.0).unwrap();
        let before =
            crate::crs::transform_at(&projected_output, &local.definition, "EPSG:4326", &state)
                .unwrap();
        let pole = local
            .projected_pole()
            .expect("a polar frame retains its projected pole");
        let pole = Point::new_raw_xy(pole.0, pole.1);
        let Shape::Polygon(projected) = &projected_output else {
            unreachable!()
        };
        for ring in projected.rings() {
            for [start, end] in crate::geometry::Coordinates::segment_pairs(ring.coords()) {
                assert!(!crate::geometry::point_on_segment(
                    pole.xy(),
                    start.xy(),
                    end.xy()
                ));
            }
        }
        let mut budget = crate::geometry::ExpansionBudget::new("buffer", "quadrant_segments");
        let after = geodesic_local_shape(
            &input,
            Some("EPSG:4326"),
            true,
            Some(20_000.0),
            &state,
            &mut budget,
            |working, _| working.buffer(20_000.0),
        )
        .unwrap();
        let (Shape::Polygon(before), Shape::Polygon(after)) = (&before, &after) else {
            unreachable!()
        };
        assert_eq!(before.shell.len(), 33);
        assert_eq!(after.shell.len(), 33);
        for (before, after) in before.shell.iter().zip(after.shell.iter()) {
            if before.x.to_bits() == 180.0_f64.to_bits() {
                assert_eq!(after.x.to_bits(), (-180.0_f64).to_bits());
                assert_eq!(after.y.to_bits(), before.y.to_bits());
            } else {
                assert_eq!(after, before);
            }
        }
        let axis = crate::geographic::GeographicFrame::crs84().longitude();
        let mut turn = 0;
        for [start, end] in crate::geometry::Coordinates::segment_pairs(after.shell.coords()) {
            turn = axis.add_chart_transit(turn, start.x, end.x).unwrap();
        }
        assert_eq!(turn, 1);
        assert_eq!(after.shell.first(), after.shell.last());
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn bounding_circle_points_keep_the_ellipsoidal_two_point_solution() {
        let coordinates = CoordSeq::try_from_xy(&[XY::new(0.0, 0.0), XY::new(0.0, 1.0)]).unwrap();
        let point_set = Shape::MultiPoint(
            crate::geometry::MultiPoint::from_coordseq(coordinates.clone()).unwrap(),
        );
        let line = Shape::LineString(LineSeq::try_new(coordinates).unwrap());
        let state = runtime_state_snapshot().unwrap();

        let model = crate::crs::metric_model(Some("EPSG:4326")).unwrap();
        let point_radius = super::super::FramedCircle::solve(
            &model,
            Some("EPSG:4326"),
            &point_set,
            None,
            false,
            None,
            crate::geometry::TopologyDomain::Cartesian,
        )
        .unwrap()
        .radius()
        .unwrap();
        let edge_radius = super::super::FramedCircle::solve(
            &model,
            Some("EPSG:4326"),
            &line,
            None,
            false,
            None,
            crate::geometry::TopologyDomain::Cartesian,
        )
        .unwrap()
        .radius()
        .unwrap();
        let local = PreparedLocalInput::select_local_frame(&[&line], "EPSG:4326", &state).unwrap();
        let projected =
            crate::crs::transform_at(&line, "EPSG:4326", &local.definition, &state).unwrap();
        let expected_edge_radius = projected.minimum_bounding_radius().unwrap();

        let expected_point_radius =
            crate::crs::with_ellipsoid_metric_at("EPSG:4326", &[&point_set], &state, |metric| {
                Ok(
                    metric.segment_length(Point::new_raw_xy(0.0, 0.0), Point::new_raw_xy(0.0, 1.0))
                        / 2.0,
                )
            })
            .unwrap();
        assert_eq!(point_radius, expected_point_radius);
        assert_eq!(edge_radius, expected_edge_radius);
        assert!((point_radius - edge_radius).abs() > 1.0);
    }

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
    fn projected_buffer_path_charges_refinement_before_kernel_work() {
        let half_width = 6.926_294_673_429_263 / 60.0;
        let west = 3.0 - half_width;
        let east = 3.0 + half_width;
        let source = Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_xy(&[XY::new(west, 39.0), XY::new(east, 39.0)]).unwrap(),
            )
            .unwrap(),
        );
        let state = runtime_state_snapshot().unwrap();
        let local =
            PreparedLocalInput::select_local_frame(&[&source], "EPSG:4326", &state).unwrap();
        assert_eq!(local.definition, "EPSG:32631");
        let distance = 100.0;
        let quadrant_segments = std::num::NonZeroU32::new(8).unwrap();
        let miter_limit = crate::Positive::try_new("miter_limit", 5.0).unwrap();
        let mut actual_budget =
            crate::geometry::ExpansionBudget::new("buffer", "quadrant_segments");
        let actual = super::geodesic_local_shape(
            &source,
            Some("EPSG:4326"),
            true,
            Some(distance),
            &state,
            &mut actual_budget,
            |projected, budget| {
                projected.buffer_with_style_budgeted(
                    distance,
                    BufferCapStyle::Round,
                    BufferJoinStyle::Round,
                    quadrant_segments,
                    miter_limit,
                    budget,
                )
            },
        )
        .unwrap();
        assert!(!actual.is_empty());
        assert!(actual_budget.used() > 0);

        let mut expected_budget =
            crate::geometry::ExpansionBudget::new("buffer", "quadrant_segments");
        let prepared = PreparedLocalInput::prepare(
            &source,
            &[&source],
            "EPSG:4326",
            Some(distance),
            LocalInputPurpose::BoundarySupport,
            &state,
            &mut expected_budget,
        )
        .unwrap()
        .unwrap();
        let expected_input_charge = expected_budget.used();
        assert!(expected_input_charge > 0);
        let expected = prepared
            .projected_working()
            .buffer_with_style_budgeted(
                distance,
                BufferCapStyle::Round,
                BufferJoinStyle::Round,
                quadrant_segments,
                miter_limit,
                &mut expected_budget,
            )
            .unwrap();
        let restored_expected =
            crate::crs::transform_at(&expected, &local.definition, "EPSG:4326", &state).unwrap();
        assert_eq!(actual, restored_expected);
        assert_eq!(actual_budget.used(), expected_budget.used());

        let unrefined_input =
            crate::crs::transform_at(&source, "EPSG:4326", &local.definition, &state).unwrap();
        let mut unrefined_budget =
            crate::geometry::ExpansionBudget::new("buffer", "quadrant_segments");
        let _unrefined = unrefined_input
            .buffer_with_style_budgeted(
                distance,
                BufferCapStyle::Round,
                BufferJoinStyle::Round,
                quadrant_segments,
                miter_limit,
                &mut unrefined_budget,
            )
            .unwrap();
        assert!(
            expected_budget.used() > unrefined_budget.used(),
            "unrefined buffer did not record the planned input subdivision work",
        );
    }
}
