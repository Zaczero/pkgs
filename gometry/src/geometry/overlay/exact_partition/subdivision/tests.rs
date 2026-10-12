use super::*;
use crate::geometry::ordinates::take_source_arc_observations;
use crate::geometry::overlay::rect::rect_polygon;

#[test]
fn traced_line_ranges_reject_clip_rectangle_segments_as_line_provenance() {
    let source = crate::io::parse_wkt("LINESTRING (0 0, 1 1)").unwrap();
    let left = Shape::Polygon(rect_polygon(Bounds::new_raw(-1.0, -1.0, 0.5, 2.0)).unwrap());
    let right = Shape::Polygon(rect_polygon(Bounds::new_raw(0.5, -1.0, 2.0, 2.0)).unwrap());
    let inventory = ExactSourceInventory::from_shapes(&[&source, &left, &right]).unwrap();
    let mut budget = ExpansionBudget::new("subdivide", "traced line range control");
    let partition = inventory.partition(&mut budget).unwrap();
    let ranges =
        source_line_ranges(LineSource::from_shape(&source).expect("source is lineal")).unwrap();
    let mut work = Unmetered;
    let source_ids = (0..inventory.source_segments.len()).collect::<Vec<_>>();
    let views = partition
        .source_span_trace_views(&inventory.source_segments, &source_ids, &mut work)
        .unwrap();
    let views = SourceLineTraceViews { source_ids, views };
    let selected = vec![false; partition.spans.len()];
    let traces = [SourceTrace::from_edge_rows([Vec::new()], &mut work).unwrap()];

    assert!(
        ranges.iter().map(Range::len).sum::<usize>() < views.views.len(),
        "the partition inventory includes clip-rectangle segments"
    );
    reset_source_line_runs_calls();
    let Err(error) =
        inventory.source_line_runs(&partition, &ranges, &views, &selected, &traces, &mut work)
    else {
        panic!("clip-rectangle segments have no source-line trace");
    };
    assert_eq!(source_line_runs_calls(), 1);
    assert!(
        error
            .to_string()
            .contains("line member source ranges omit partition source segments"),
        "{error}"
    );

    let source_views = inventory
        .source_line_trace_views(&partition, &mut work)
        .unwrap();
    assert_eq!(
        source_views.source_ids,
        vec![0],
        "the line view remaps only operand-0 source rows"
    );
    let mut omitted_source = source_views;
    omitted_source.source_ids.pop();
    omitted_source.views.pop();
    reset_source_line_runs_calls();
    let Err(error) = inventory.source_line_runs(
        &partition,
        &ranges,
        &omitted_source,
        &selected,
        &traces,
        &mut work,
    ) else {
        panic!("an omitted operand-0 source row cannot be admitted");
    };
    assert_eq!(source_line_runs_calls(), 1);
    assert!(
        error
            .to_string()
            .contains("line member source ranges omit partition source segments"),
        "{error}"
    );
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "the fallible geometry setup propagates with `?`; assertions are the test oracle"
)]
#[expect(
    clippy::float_cmp,
    reason = "these exact coordinates identify original source roots; a tolerance could admit the wrong edge"
)]
fn ordinary_traced_subdivision_resolves_reversed_and_partial_root_coowners() -> Result<()> {
    let crs = concat!(
        "GEOGCS[\"Custom grad\",DATUM[\"Custom datum\",",
        "SPHEROID[\"Custom\",7000000,150]],PRIMEM[\"Greenwich\",0],",
        "UNIT[\"grad\",0.015707963267948967],",
        "AXIS[\"Longitude\",EAST],AXIS[\"Latitude\",NORTH]]"
    );
    let model = crate::crs::metric_model(Some(crs))?;
    let crate::crs::MetricModel::Geodesic { frame, .. } = &model else {
        return Err(crate::crs::CrsError::invalid(
            "custom test CRS is not geographic",
        ));
    };
    let frame = *frame;
    let axes = CoordinateAxes::new(crate::geometry::HasZ(true), crate::geometry::HasM(true));
    let scale = 10.0 / 9.0;
    let line = |coordinates: &[(f64, f64)], ordinates: &[(f64, f64)]| -> Result<LineSeq> {
        let points = coordinates
            .iter()
            .zip(ordinates)
            .map(|(&(x, y), &(z, m))| {
                Point::new_raw_axes(x * scale, y * scale, ZOrdinate(Some(z)), MOrdinate(Some(m)))
            })
            .collect::<Vec<_>>();
        Ok(LineSeq::from_proven(CoordSeq::try_from_points(&points)?))
    };

    for (partial_end_z, complete_ordinates) in [(7.0, true), (9.0, false)] {
        let original = Shape::multi_line_string(axes, vec![
            line(&[(0.0, 0.0), (12.0, 6.0)], &[(7.0, 70.0), (7.0, 70.0)])?,
            line(&[(12.0, 6.0), (0.0, 0.0)], &[(7.0, 70.0), (7.0, 70.0)])?,
            line(&[(4.0, 2.0), (10.0, 5.0)], &[
                (7.0, 70.0),
                (partial_end_z, 70.0),
            ])?,
        ]);
        let root_xy_positions_needed =
            SubdivisionRootCatalog::needs_root_xy_positions(&original, original.bounds(), frame);
        let (roots, initial_traces) =
            SubdivisionRootCatalog::from_line_shape(&original, frame, root_xy_positions_needed)?;
        drop(initial_traces);

        let zero = ExactRational::zero();
        let one = ExactRational::one();
        let root0_start = ExactRational::from_f64(5.0 / 12.0);
        let root1_start = ExactRational::from_f64(7.0 / 12.0);
        let partial_start = ExactRational::from_f64(1.0 / 6.0);
        let partial_end = ExactRational::one();
        let partial_producer_end = ExactRational::from_f64(5.0 / 7.0);
        let mut trace_work = Unmetered;
        let trace = SourceTrace::from_edge_rows(
            [vec![
                RootRange::from_mapped_with_work(
                    RootId::from_index(0),
                    root0_start,
                    one.clone(),
                    zero.clone(),
                    one.clone(),
                    &mut trace_work,
                )?,
                RootRange::from_mapped_with_work(
                    RootId::from_index(1),
                    root1_start,
                    zero.clone(),
                    zero.clone(),
                    one,
                    &mut trace_work,
                )?,
                RootRange::from_mapped_with_work(
                    RootId::from_index(2),
                    partial_start,
                    partial_end,
                    zero,
                    partial_producer_end,
                    &mut trace_work,
                )?,
            ]],
            &mut trace_work,
        )?;
        let traces = [trace];
        let child = Shape::LineString(line(&[(5.0, 2.5), (12.0, 6.0)], &[
            (7.0, 70.0),
            (7.0, 70.0),
        ])?);
        let left = Shape::Polygon(
            rect_polygon(Bounds::new_raw(
                5.0 * scale,
                2.5 * scale,
                7.0 * scale,
                6.0 * scale,
            ))
            .unwrap(),
        );
        let right = Shape::Polygon(
            rect_polygon(Bounds::new_raw(
                7.0 * scale,
                2.5 * scale,
                12.0 * scale,
                6.0 * scale,
            ))
            .unwrap(),
        );
        let inventory = ExactSourceInventory::from_shapes(&[&child, &left, &right])?;
        let mut budget = ExpansionBudget::new("subdivide", "coowner trace control");
        let mut partition = inventory.partition(&mut budget)?;
        partition.set_geographic_frame(frame);
        let metric = crate::crs::GeodesicMetric::from_model(&model)?;
        let metric_calls = std::cell::Cell::new(0_usize);
        let metric_access = || -> Result<&crate::crs::GeodesicMetric> {
            metric_calls.set(metric_calls.get().saturating_add(1));
            Ok(&metric)
        };
        let context = SourceArcContext::new(
            crate::geometry::TopologyDomain::Geographic(frame),
            Some(&metric_access),
        );

        drop(take_source_arc_observations());
        let parts = inventory.subdivision_parts_with_roots(
            &partition,
            &SubdivisionTraceRequest {
                source_shape: &child,
                source_traces: Some(&traces),
                roots: &roots,
                root_positions: root_xy_positions_needed.then_some(&roots),
                declared_axes: axes,
                strict: false,
                clip_operands: [1, 2],
                context: &context,
                fallback_inventory: Some(&inventory),
                reuse_existing_source_vertices: true,
            },
            &mut budget,
            &mut trace_work,
        )?;
        let observations = take_source_arc_observations();
        let cut_points = parts
            .iter()
            .flat_map(|part| {
                let mut points = Vec::new();
                part.shape.for_each_point(|point| {
                    if (point.x - 7.0 * scale).abs() <= 1e-12
                        && (point.y - 3.5 * scale).abs() <= 1e-12
                    {
                        points.push(point);
                    }
                });
                points
            })
            .collect::<Vec<_>>();
        assert!(!cut_points.is_empty());
        assert!(metric_calls.get() > 0);
        assert!(observations.iter().any(|observation| {
            observation.source[0].x == 0.0
                && observation.source[1].x == 12.0 * scale
                && (observation.parameter - 7.0 / 12.0).abs() <= 1e-15
        }));
        assert!(observations.iter().any(|observation| {
            observation.source[0].x == 12.0 * scale
                && observation.source[1].x == 0.0
                && (observation.parameter - 5.0 / 12.0).abs() <= 1e-15
        }));
        assert!(observations.iter().any(|observation| {
            observation.source[0].x == 4.0 * scale
                && observation.source[1].x == 10.0 * scale
                && (observation.parameter - 0.5).abs() <= 1e-15
        }));
        if complete_ordinates {
            assert!(cut_points.iter().all(|point| point.axes == axes));
            assert!(cut_points.iter().all(|point| point.z() == Some(7.0)));
            assert!(cut_points.iter().all(|point| point.m() == Some(70.0)));
        } else {
            let measured =
                CoordinateAxes::new(crate::geometry::HasZ(false), crate::geometry::HasM(true));
            assert!(cut_points.iter().all(|point| point.axes == measured));
            assert!(cut_points.iter().all(|point| point.z().is_none()));
            assert!(cut_points.iter().all(|point| point.m() == Some(70.0)));
        }
    }
    Ok(())
}

#[test]
fn public_geographic_seam_subdivision_uses_ordinary_source_line_runs() {
    let shape = crate::io::parse_wkt(
        "LINESTRING (179 0, -179 30, -178 31, -177 32, -176 33, -175 34, -174 35, -173 36, -172 37)",
    )
    .unwrap();
    let context = SourceArcContext::new(
        crate::geometry::TopologyDomain::Geographic(crate::geographic::GeographicFrame::crs84()),
        None,
    );

    reset_source_line_runs_calls();
    let result = shape.subdivide_with_context(8, false, &context);

    assert!(
        source_line_runs_calls() > 0,
        "ordinary geographic subdivision carries original line roots through exact cuts"
    );
    let parts = result.expect("ordinary geographic subdivision preserves seam support");
    let mut counts = crate::geometry::ShapeCountQuery::default();
    assert!(!parts.is_empty());
    assert!(parts.iter().all(|part| {
        counts
            .coordinate_count(part)
            .is_some_and(|count| count <= 8)
    }));
    let points = parts.iter().flat_map(|part| {
        let mut points = Vec::new();
        part.for_each_point(|point| points.push(point));
        points
    });
    let points = points.collect::<Vec<_>>();
    assert!(
        points
            .iter()
            .any(|point| (point.x.abs() - 180.0).abs() <= 1e-12 && (point.y - 15.0).abs() <= 1e-12),
        "ordinary geographic clipping retains the original edge's seam site"
    );
    for point in points.iter().filter(|point| point.y < 30.0) {
        let continuous_x = if point.x >= 179.0 - 1e-12 {
            point.x
        } else if point.x <= -179.0 + 1e-12 {
            point.x + 360.0
        } else {
            panic!("output point {point:?} is outside the original seam edge");
        };
        assert!((point.y - 15.0 * (continuous_x - 179.0)).abs() <= 2e-12);
    }
}

#[test]
fn line_source_ranges_reserve_only_stored_multiline_values() {
    let line = LineSeq::from_proven(
        CoordSeq::try_from_xy(&[XY::new(0.0, 0.0), XY::new(1.0, 0.0)]).unwrap(),
    );
    let mut members = Vec::with_capacity(65);
    for _ in 0..64 {
        members.push(crate::geometry::MultiMember::Empty);
    }
    members.push(crate::geometry::MultiMember::Value(line));
    let lines = MultiLineString::try_new(crate::geometry::CoordinateAxes::XY, members).unwrap();
    let member_count = lines.member_count();
    assert_eq!(lines.len(), 1);

    let shape = Shape::MultiLineString(lines);
    let source = LineSource::from_shape(&shape).expect("multiline source");
    let ranges = source_line_ranges(source).unwrap();
    assert_eq!(ranges.len(), 1);
    assert_eq!(ranges[0], 0..1);
    assert!(ranges.capacity() < member_count);
}
