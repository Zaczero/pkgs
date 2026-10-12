use super::*;
use crate::geometry::embedding_search::{
    ClassAssignment, GlobalCheck, Problem, Search, SearchAccount,
};
use crate::geometry::ordinates::take_source_arc_observations;
use crate::geometry::overlay::Strictness;
use crate::geometry::overlay::rect::rect_polygon;
use crate::geometry::segments::{SegmentEndpoint, SharedSpan, TouchEndpoint, TouchLocation};
use crate::geometry::topology::SourceTrace;

#[test]
fn planar_embedding_domain_admits_finite_coordinates_only() {
    let domain = embedding::EmbeddingDomain::Plane;
    let nearest = XY::new(0.0, 0.0);
    assert!(domain.admits(nearest, nearest));
    assert!(domain.admits(XY::new(0.0, 0.0), XY::new(f64::MAX, f64::MIN)));
    assert!(!domain.admits(XY::new(0.0, 0.0), XY::new(f64::INFINITY, 0.0)));
    assert!(!domain.admits(XY::new(0.0, 0.0), XY::new(0.0, f64::NAN)));
}

#[test]
fn rectangle_embedding_domain_keeps_candidates_in_the_closed_window() {
    let bounds = Bounds::new(-2.0, -3.0, 4.0, 5.0).unwrap();
    let domain = embedding::EmbeddingDomain::Rectangle(bounds);
    let nearest = XY::new(0.0, 0.0);
    assert!(domain.admits(nearest, nearest));
    assert!(domain.admits(nearest, XY::new(-2.0, 5.0)));
    assert!(domain.admits(nearest, XY::new(4.0, -3.0)));
    assert!(!domain.admits(nearest, XY::new((-2.0_f64).next_down(), 0.0)));
    assert!(!domain.admits(nearest, XY::new(4.0_f64.next_up(), 0.0)));
}

#[test]
fn geographic_embedding_domain_uses_frame_limits_and_existing_singular_lines() {
    use crate::geographic::{AngularFactor, GeographicFrame};

    let grad = AngularFactor::try_new("test angle", std::f64::consts::PI / 200.0).unwrap();
    let frame = GeographicFrame::try_new(grad, grad, 0.0).unwrap();
    let domain = embedding::EmbeddingDomain::Geographic(frame);
    let half_period = frame.longitude().half_period();
    let pole = frame.pole();
    let ordinary = XY::new(0.0, 0.0);

    assert!(domain.admits(ordinary, ordinary));
    assert!(domain.admits(ordinary, XY::new(25.0, 40.0)));
    assert!(!domain.admits(ordinary, XY::new(half_period, 0.0)));
    assert!(!domain.admits(ordinary, XY::new(0.0, pole)));
    assert!(!domain.admits(ordinary, XY::new(half_period.next_up(), 0.0)));
    assert!(!domain.admits(ordinary, XY::new(0.0, pole.next_up().next_up())));

    let seam = XY::new(-half_period, 0.0);
    assert!(domain.admits(seam, XY::new(-half_period, 1.0)));
    let pole_node = XY::new(0.0, pole);
    assert!(domain.admits(pole_node, XY::new(1.0, pole)));
    assert!(domain.admits(pole_node, XY::new(1.0, pole.next_up())));
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "the test propagates fallible setup with `?` and intentionally fails through assertions"
)]
#[expect(
    clippy::float_cmp,
    reason = "the certified rectangle must retain its exact supplied bounds"
)]
fn rectangle_partition_uses_its_clip_window_as_embedding_domain() {
    let Shape::Polygon(polygon) =
        crate::io::parse_wkt("POLYGON ((-1 -1,1 -1,1 1,-1 1,-1 -1))").unwrap()
    else {
        panic!("expected polygon");
    };
    let bounds = Bounds::new(-0.5, -0.5, 0.5, 0.5).unwrap();
    let rectangle = rect_polygon(bounds).unwrap();
    let (inventory, intervals) =
        ExactSourceInventory::from_polygon_rect_clip(&[&polygon], &rectangle, bounds, false)
            .unwrap()
            .expect("the rectangle clips the polygon");
    let mut budget = ExpansionBudget::new("clip_by_rect", "embedding domain test");
    let partition = inventory
        .partition_in_window(&intervals, bounds, &mut budget)
        .unwrap();

    assert!(matches!(
        partition.embedding_domain,
        embedding::EmbeddingDomain::Rectangle(actual)
            if actual.into_array() == bounds.into_array()
    ));
}

#[test]
fn geographic_frame_installs_domain_and_preserves_all_source_vertices() {
    let source = crate::io::parse_wkt("LINESTRING (0 0,2 0)").unwrap();
    let inventory = ExactSourceInventory::from_shapes(&[&source]).unwrap();
    let mut budget = ExpansionBudget::new("overlay", "geographic embedding domain test");
    let mut partition = inventory.partition(&mut budget).unwrap();
    let frame = crate::geographic::GeographicFrame::crs84();
    partition.set_geographic_frame(frame);

    assert!(matches!(
        partition.embedding_domain,
        embedding::EmbeddingDomain::Geographic(actual) if actual == frame
    ));
    assert!(matches!(partition.kept_nodes, KeptNodes::All));
}

fn subdivision_children(source: &Shape, split_x: bool, cut: f64) -> [Shape; 2] {
    let bounds = source.bounds().unwrap();
    let (minx, miny, maxx, maxy) = bounds.into_tuple();
    let halves = if split_x {
        [
            Bounds::new_raw(minx, miny, cut, maxy),
            Bounds::new_raw(cut, miny, maxx, maxy),
        ]
    } else {
        [
            Bounds::new_raw(minx, miny, maxx, cut),
            Bounds::new_raw(minx, cut, maxx, maxy),
        ]
    };
    let [left_bounds, right_bounds] = halves;
    let left = Shape::Polygon(rect_polygon(left_bounds).unwrap());
    let right = Shape::Polygon(rect_polygon(right_bounds).unwrap());
    let inventory = ExactSourceInventory::from_shapes(&[source, &left, &right]).unwrap();
    let mut budget = ExpansionBudget::new("subdivide", "shared seam");
    let partition = inventory.partition(&mut budget).unwrap();
    let (topology, edges) = partition.local_topology().unwrap();
    let global = partition.global_faces(&topology, &edges).unwrap();
    let rings = inventory.winding_source_rings();
    let facts = inventory
        .build_cell_facts(&partition, &topology, &edges, &global, &rings, false)
        .unwrap();
    let first = inventory.select_subdivision_cells(&partition, &topology, &global, &facts, 0, 1);
    let second = inventory.select_subdivision_cells(&partition, &topology, &global, &facts, 0, 2);
    for ((&left, &right), &source) in first
        .faces
        .iter()
        .zip(&second.faces)
        .zip(facts.face_membership.iter().step_by(3))
    {
        assert_eq!(left || right, source);
        assert!(!(left && right));
    }
    for ((&left, &right), &source) in first
        .spans
        .iter()
        .zip(&second.spans)
        .zip(facts.span_support.iter().step_by(3))
    {
        assert_eq!(left || right, source);
    }
    let parent = inventory.select_subdivision_cells(&partition, &topology, &global, &facts, 0, 0);
    for ((&left, &right), &source) in first.nodes.iter().zip(&second.nodes).zip(&parent.nodes) {
        assert_eq!(left || right, source);
    }
    let plans: Vec<_> = [first, second]
        .iter()
        .map(|cells| {
            cells
                .emission_plan(&partition, &topology, &edges, &global)
                .unwrap()
        })
        .collect();
    let emissions: Vec<_> = plans
        .iter()
        .map(|plan| plan.emission(&topology, &edges))
        .collect();
    let refs: Vec<_> = emissions.iter().collect();
    let embedding = partition.embed(&refs, "subdivide").unwrap();
    if emissions
        .iter()
        .any(|emission| embedding::rounding_moves(&partition, &emission.items))
    {
        let segments: Vec<Vec<_>> = emissions
            .iter()
            .map(|emission| {
                emission
                    .items
                    .iter()
                    .map(|&item| embedding::Embedding::segment_at(embedding.coordinates(), item))
                    .collect()
            })
            .collect();
        let views: Vec<_> = segments.iter().map(Vec::as_slice).collect();
        assert!(
            embedding::certify(
                &partition,
                &embedding,
                &refs,
                &views,
                &mut embedding::FirstFailure::default()
            )
            .unwrap()
            .is_continue()
        );
    }
    let children = inventory
        .subdivision_parts::<crate::crs::GeodesicMetric>(
            &partition,
            source,
            source.axes(),
            false,
            [1, 2],
            &mut budget,
            &crate::geometry::SourceArcContext::cartesian(),
        )
        .unwrap();
    let [first, second] = &children;
    assert!(first.relate(second).unwrap().starts_with('F'));
    assert!(
        Shape::union_all(&children, Strictness::Lenient)
            .unwrap()
            .validate()
            .unwrap()
            .is_none()
    );
    children
}

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
    let error = match inventory.source_line_runs(
        &partition,
        &ranges,
        &omitted_source,
        &selected,
        &traces,
        &mut work,
    ) {
        Ok(_) => panic!("an omitted operand-0 source row cannot be admitted"),
        Err(error) => error,
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
                    one.clone(),
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
            &child,
            Some(&traces),
            &roots,
            axes,
            false,
            [1, 2],
            &mut budget,
            &context,
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
fn subdivision_rounds_the_exterior_crossing_without_cracking_the_seam() {
    let source =
        crate::io::parse_wkt("POLYGON((0 0,3 1,3 2,2 2,1 2,0 2,-1 2,-1 1,-1 0,0 0))").unwrap();
    let [left, right] = subdivision_children(&source, true, 0.5);
    let seam = crate::io::parse_wkt("LINESTRING(0.5 0.16666666666666666,0.5 2)").unwrap();
    let shared = left.intersection(&right, Strictness::Lenient).unwrap();
    assert!(shared.equals(&seam).unwrap());
    let union = Shape::union_all(&[left, right], Strictness::Lenient).unwrap();
    // The exact exterior crossing is (1/2, 1/6), not a binary64 point.
    assert!(!union.equals(&source).unwrap());
    let Shape::Polygon(polygon) = union else {
        panic!("one shell");
    };
    assert!(polygon.holes.is_empty());
}

#[test]
fn subdivision_through_a_component_gap_has_no_shared_seam() {
    let source = crate::io::parse_wkt(
        "MULTIPOLYGON(((0 0,1 0,2 0,2 1,2 2,1 2,0 2,0 1,0 0)),\
        ((10 0,11 0,12 0,12 1,12 2,11 2,10 2,10 1,10 0)))",
    )
    .unwrap();
    let [left, right] = subdivision_children(&source, true, 6.0);
    assert!(
        left.intersection(&right, Strictness::Lenient)
            .unwrap()
            .is_empty()
    );
    let union = Shape::union_all(&[left, right], Strictness::Lenient).unwrap();
    assert!(union.equals(&source).unwrap());
    let Shape::MultiPolygon(polygons) = union else {
        panic!("two shells");
    };
    assert_eq!(polygons.len(), 2);
    assert!(polygons.iter().all(|polygon| polygon.holes.is_empty()));
}

#[test]
fn subdivision_keeps_a_zero_dimensional_shared_cut_support() {
    let source =
        crate::io::parse_wkt("MULTIPOLYGON(((-2 0,-1 -1,0 0,-1 1,-2 0)),((0 0,1 -1,2 0,1 1,0 0)))")
            .unwrap();
    let [left, right] = subdivision_children(&source, true, 0.0);
    let shared = left.intersection(&right, Strictness::Lenient).unwrap();
    assert!(
        shared
            .equals(&Shape::Point(Point::new_raw_xy(0.0, 0.0)))
            .unwrap()
    );
    assert!(
        Shape::union_all(&[left, right], Strictness::Lenient)
            .unwrap()
            .equals(&source)
            .unwrap()
    );
}

#[test]
fn subdivision_keeps_a_hole_vertex_on_the_other_childs_dissolved_seam() {
    let source =
        crate::io::parse_wkt("POLYGON((-4 -3,4 -3,4 3,-4 3,-4 -3),(-2 -1,0 0,-2 1,-2 -1))")
            .unwrap();
    let [left, right] = subdivision_children(&source, true, 0.0);
    let vertex = XY::new(0.0, 0.0);
    let mut left_keeps = false;
    let mut right_keeps = false;
    left.for_each_point(|point| left_keeps |= point.xy() == vertex);
    right.for_each_point(|point| right_keeps |= point.xy() == vertex);
    assert!(left_keeps);
    assert!(!right_keeps);
    assert!(
        crate::geometry::boundary_with_metric::<crate::crs::GeodesicMetric>(
            &crate::geometry::PreparedPlan::transient(&right),
            &crate::geometry::SourceArcContext::cartesian(),
        )
        .unwrap()
        .intersects(&Shape::Point(Point::new_raw_xy(0.0, 0.0)))
        .unwrap()
    );
    let seam = crate::io::parse_wkt("LINESTRING(0 -3,0 3)").unwrap();
    assert!(
        left.intersection(&right, Strictness::Lenient)
            .unwrap()
            .equals(&seam)
            .unwrap()
    );
    let union = Shape::union_all(&[left, right], Strictness::Lenient).unwrap();
    assert!(union.equals(&source).unwrap());
    let Shape::Polygon(polygon) = union else {
        panic!("one shell");
    };
    assert_eq!(polygon.holes.len(), 1);
}

fn certify_plan(
    plan: &BoundaryPlan,
    partition: &ExactPartition<'_>,
    topology: &IndexedTopology,
    edge_of_slot: &[u32],
) -> crate::error::Result<(Vec<[NodeId; 2]>, Vec<Segment>)> {
    let emission = plan.emission(topology, edge_of_slot);
    let embedding = partition.embed(&[&emission], "test")?;
    let segments = emission
        .items
        .iter()
        .map(|&item| embedding::Embedding::segment_at(embedding.coordinates(), item))
        .collect();
    Ok((emission.items, segments))
}

fn check_plan_fans(
    plan: &BoundaryPlan,
    partition: &ExactPartition<'_>,
    topology: &IndexedTopology,
    edge_of_slot: &[u32],
    segments: &[Segment],
) -> crate::error::Result<()> {
    let emission = plan.emission(topology, edge_of_slot);
    let mut coordinates = partition.points.rounded.clone();
    for (&[start, end], segment) in emission.items.iter().zip(segments) {
        coordinates[start.0 as usize] = segment.start;
        coordinates[end.0 as usize] = segment.end;
    }
    let embedding = embedding::Embedding::from_owned(coordinates);
    embedding::check_emitted_fans(
        partition,
        &embedding,
        &emission.items[..emission.chords],
        &emission.fan_spans,
        &segments[..emission.chords],
        emission.areal_chords,
    )
}

fn check_plan_faces(
    plan: &BoundaryPlan,
    partition: &ExactPartition<'_>,
    topology: &IndexedTopology,
    edge_of_slot: &[u32],
    items: &[[NodeId; 2]],
    segments: &[Segment],
) -> crate::error::Result<()> {
    let emission = plan.emission(topology, edge_of_slot);
    let mut coordinates = partition.points.rounded.clone();
    for (&[start, end], segment) in items.iter().zip(segments) {
        coordinates[start.0 as usize] = segment.start;
        coordinates[end.0 as usize] = segment.end;
    }
    let embedding = embedding::Embedding::from_owned(coordinates);
    embedding::check_global_faces(
        partition,
        &embedding,
        items,
        emission
            .faces
            .as_deref()
            .expect("boundary plans carry faces"),
    )
}

#[derive(Default)]
struct WitnessLog(Vec<embedding::Witness>);

impl embedding::Sink for WitnessLog {
    fn report(&mut self, witness: embedding::Witness) -> std::ops::ControlFlow<()> {
        self.0.push(witness);
        std::ops::ControlFlow::Continue(())
    }
}

fn boundary_of_polygonal_support(wkt: &str) -> String {
    let shape = crate::io::parse_wkt(wkt).expect("valid boundary fixture");
    let inventory = ExactSourceInventory::from_polygonal_union(&[&shape])
        .expect("polygonal union sources stage")
        .expect("fixture is polygonal");
    let mut budget = ExpansionBudget::new("boundary", "exact partition");
    let partition = inventory
        .partition(&mut budget)
        .expect("exact partition builds");
    let boundary = inventory
        .occupied_boundary_shape::<crate::crs::GeodesicMetric>(
            &partition,
            shape.axes(),
            &crate::geometry::SourceArcContext::cartesian(),
        )
        .expect("boundary cells emit");
    crate::io::to_wkt(&boundary).expect("boundary WKT writes")
}

#[test]
fn boundary_cells_publish_the_outline_of_occupied_area() {
    let cases = [
        (
            "outward retraced spur",
            "POLYGON ((0 0,4 0,4 4,2 4,2 6,2 4,0 4,0 0))",
            "LINESTRING (0 0, 4 0, 4 4, 0 4, 0 0)",
        ),
        (
            "inward retraced slit",
            "POLYGON ((0 0,4 0,4 4,2 4,2 2,2 4,0 4,0 0))",
            "LINESTRING (0 0, 4 0, 4 4, 0 4, 0 0)",
        ),
        (
            "adjacent multipolygon members",
            "MULTIPOLYGON (((0 0,2 0,2 2,0 2,0 0)),((2 0,4 0,4 2,2 2,2 0)))",
            "LINESTRING (0 0, 4 0, 4 2, 0 2, 0 0)",
        ),
        (
            "overlapping multipolygon members",
            "MULTIPOLYGON (((0 0,4 0,4 2,0 2,0 0)),((2 0,6 0,6 2,2 2,2 0)))",
            "LINESTRING (0 0, 6 0, 6 2, 0 2, 0 0)",
        ),
        (
            "external hole",
            "POLYGON ((0 0,4 0,4 4,0 4,0 0),(5 5,6 5,6 6,5 6,5 5))",
            "LINESTRING (0 0, 4 0, 4 4, 0 4, 0 0)",
        ),
    ];
    for (name, input, expected) in cases {
        assert_eq!(boundary_of_polygonal_support(input), expected, "{name}");
    }
}

#[test]
fn boundary_cells_publish_only_odd_residual_nodes() {
    assert_eq!(
        boundary_of_polygonal_support("POLYGON ((0 0,2 2,4 4,0 0))"),
        "MULTIPOINT ((0 0), (4 4))",
    );
    assert_eq!(
        boundary_of_polygonal_support("POLYGON ((1 2,1 2,1 2,1 2))"),
        "GEOMETRYCOLLECTION EMPTY",
    );
    assert_eq!(
        boundary_of_polygonal_support(
            "MULTIPOLYGON (((0 0,4 0,4 4,0 4,0 0)),((10 0,12 0,14 0,10 0)))"
        ),
        "GEOMETRYCOLLECTION (LINESTRING (0 0, 4 0, 4 4, 0 4, 0 0), MULTIPOINT ((10 0), (14 0)))",
    );
    assert_eq!(
        boundary_of_polygonal_support("MULTIPOLYGON (((0 0,4 0,4 4,0 4,0 0)),((1 1,2 2,3 3,1 1)))"),
        "LINESTRING (0 0, 4 0, 4 4, 0 4, 0 0)",
    );
}

#[test]
fn boundary_emission_preserves_source_zm_for_area_and_residual_points() {
    let input = "MULTIPOLYGON ZM (\
        ((0 0 1 11,4 0 2 22,4 4 3 33,2 4 4 44,2 6 5 55,2 4 4 44,0 4 6 66,0 0 1 11)),\
        ((10 0 5 55,12 0 6 66,14 0 7 77,10 0 5 55))\
    )";
    assert_eq!(
        boundary_of_polygonal_support(input),
        "GEOMETRYCOLLECTION (LINESTRING ZM (0 0 1 11, 4 0 2 22, 4 4 3 33, 2 4 4 44, 0 4 6 66, 0 0 1 11), MULTIPOINT ZM ((10 0 5 55), (14 0 7 77)))",
    );
}

#[test]
fn boundary_node_serializer_uses_accepted_embedding_for_line_and_residual_zm_point() {
    let source_point = |xy: XY| {
        Point::new_axes(xy.x, xy.y, ZOrdinate(Some(17.25)), MOrdinate(Some(73.5))).unwrap()
    };
    let mut sources: Vec<_> = lattice_crossing_group(1.0, 2.0_f64.powi(-52))
        .into_iter()
        .map(|segment| {
            Shape::LineString(
                LineSeq::try_new(
                    CoordSeq::try_from_points(&[
                        source_point(segment.start),
                        source_point(segment.end),
                    ])
                    .unwrap(),
                )
                .unwrap(),
            )
        })
        .collect();
    sources.push(Shape::Point(source_point(XY::new(1.0, 1.0))));
    let source_refs: Vec<_> = sources.iter().collect();
    let inventory = ExactSourceInventory::from_shapes(&source_refs).unwrap();
    let mut budget = ExpansionBudget::new("boundary", "accepted embedding serialization");
    let partition = inventory.partition(&mut budget).unwrap();
    let mut crossings = partition.crossings();
    assert_eq!(
        crossings.len(),
        2,
        "the fixed lattice fixture has two crossings"
    );
    crate::geometry::try_sort_by(&mut crossings, |left, right| {
        partition.compare_nodes(*left, *right)
    })
    .unwrap();
    let [residual_node, line_node] = crossings.as_slice() else {
        panic!("the fixed lattice fixture has two crossings");
    };
    let residual_node = *residual_node;
    let line_node = *line_node;

    let emission = partition.span_emission(0..u32::try_from(partition.spans.len()).unwrap());
    let accepted = partition.embed(&[&emission], "boundary").unwrap();
    let xy_bits = |point: XY| (point.x.to_bits(), point.y.to_bits());
    let nearest = partition.nearest_xy(line_node);
    assert_eq!(
        xy_bits(nearest),
        xy_bits(partition.nearest_xy(residual_node))
    );
    assert_ne!(xy_bits(accepted.xy(line_node)), xy_bits(nearest));
    assert_ne!(xy_bits(accepted.xy(residual_node)), xy_bits(nearest));
    assert_ne!(
        xy_bits(accepted.xy(line_node)),
        xy_bits(accepted.xy(residual_node))
    );

    let line_nodes = emission
        .items
        .iter()
        .copied()
        .find(|[start, end]| {
            (*start == line_node || *end == line_node)
                && *start != residual_node
                && *end != residual_node
        })
        .expect("the repaired line node has a segment away from the residual node");
    let [line_start, line_end] = line_nodes;
    let line_crossing = line_nodes
        .iter()
        .position(|&node| node == line_node)
        .expect("selected line contains the repaired node");
    let nodes = [residual_node, line_start, line_end];
    let axes = CoordinateAxes::XYZM;
    let accepted_nodes = inventory
        .resolve_node_points::<crate::crs::GeodesicMetric, _>(
            &partition,
            &accepted,
            nodes.iter().copied(),
            axes,
            &crate::geometry::SourceArcContext::cartesian(),
            None,
        )
        .unwrap();
    let accepted_shape = inventory
        .boundary_node_parts_to_shape_resolved(
            NodeParts {
                points: vec![residual_node],
                lines: vec![vec![line_start, line_end]],
                polygons: Vec::new(),
            },
            &accepted_nodes,
            axes,
            true,
            true,
            true,
        )
        .unwrap();
    let Shape::GeometryCollection(accepted_parts) = &accepted_shape else {
        panic!("area boundary with a residual point is a collection");
    };
    let Shape::LineString(accepted_line) = accepted_parts
        .first()
        .expect("the area boundary line is first")
    else {
        panic!("the area boundary component is one line");
    };
    let Shape::MultiPoint(accepted_points) = accepted_parts
        .get(1)
        .expect("the residual component follows the area boundary")
    else {
        panic!("the residual boundary point remains a MultiPoint");
    };
    let accepted_line_xy = XY::new(
        *accepted_line
            .xs()
            .get(line_crossing)
            .expect("line retains the repaired crossing"),
        *accepted_line
            .ys()
            .get(line_crossing)
            .expect("line retains the repaired crossing"),
    );
    let accepted_residual_xy = XY::new(
        *accepted_points
            .xs()
            .first()
            .expect("residual MultiPoint retains its node"),
        *accepted_points
            .ys()
            .first()
            .expect("residual MultiPoint retains its node"),
    );
    assert_eq!(xy_bits(accepted_line_xy), xy_bits(accepted.xy(line_node)));
    assert_eq!(
        xy_bits(accepted_residual_xy),
        xy_bits(accepted.xy(residual_node))
    );
    assert_ne!(xy_bits(accepted_line_xy), xy_bits(nearest));
    assert_ne!(xy_bits(accepted_residual_xy), xy_bits(nearest));
    let line_z = accepted_line.zs().expect("line carries source Z");
    let line_m = accepted_line.ms().expect("line carries source M");
    assert_eq!(line_z.len(), 2);
    assert_eq!(line_m.len(), 2);
    assert!(
        line_z
            .iter()
            .all(|value| value.to_bits() == 17.25_f64.to_bits())
    );
    assert!(
        line_m
            .iter()
            .all(|value| value.to_bits() == 73.5_f64.to_bits())
    );
    let accepted_residual = accepted_points.point_at(0);
    assert_eq!(
        (
            accepted_residual.z().map(f64::to_bits),
            accepted_residual.m().map(f64::to_bits)
        ),
        (Some(17.25_f64.to_bits()), Some(73.5_f64.to_bits()))
    );

    let nearest_embedding = embedding::Embedding::nearest(&partition);
    let nearest_nodes = inventory
        .resolve_node_points::<crate::crs::GeodesicMetric, _>(
            &partition,
            &nearest_embedding,
            nodes.iter().copied(),
            axes,
            &crate::geometry::SourceArcContext::cartesian(),
            None,
        )
        .unwrap();
    let nearest_shape = inventory
        .boundary_node_parts_to_shape_resolved(
            NodeParts {
                points: vec![residual_node],
                lines: vec![vec![line_start, line_end]],
                polygons: Vec::new(),
            },
            &nearest_nodes,
            axes,
            true,
            true,
            true,
        )
        .unwrap();
    let Shape::GeometryCollection(nearest_parts) = &nearest_shape else {
        panic!("the negative control keeps the same output structure");
    };
    let Shape::LineString(nearest_line) = nearest_parts
        .first()
        .expect("the negative-control line is first")
    else {
        panic!("the negative-control area boundary component is one line");
    };
    let Shape::MultiPoint(nearest_points) = nearest_parts
        .get(1)
        .expect("the negative-control point follows the line")
    else {
        panic!("the negative-control residual is a MultiPoint");
    };
    let nearest_line_xy = XY::new(
        *nearest_line
            .xs()
            .get(line_crossing)
            .expect("negative-control line retains its crossing"),
        *nearest_line
            .ys()
            .get(line_crossing)
            .expect("negative-control line retains its crossing"),
    );
    let nearest_residual_xy = XY::new(
        *nearest_points
            .xs()
            .first()
            .expect("negative-control MultiPoint retains its node"),
        *nearest_points
            .ys()
            .first()
            .expect("negative-control MultiPoint retains its node"),
    );
    assert_eq!(xy_bits(nearest_line_xy), xy_bits(nearest));
    assert_eq!(xy_bits(nearest_residual_xy), xy_bits(nearest));
    assert_eq!(xy_bits(nearest_line_xy), xy_bits(nearest_residual_xy));
    assert_ne!(xy_bits(nearest_line_xy), xy_bits(accepted_line_xy));
    assert_ne!(xy_bits(nearest_residual_xy), xy_bits(accepted_residual_xy));
}

#[test]
fn mixed_source_order_keeps_absent_and_interpolated_ordinates_distinct() {
    let xy = crate::io::parse_wkt("LINESTRING (0 0,4 0)").unwrap();
    let zm = crate::io::parse_wkt("LINESTRING ZM (2 -1 10 100,2 1 30 300)").unwrap();
    for shapes in [[&xy, &zm], [&zm, &xy]] {
        let inventory = ExactSourceInventory::from_shapes(&shapes).unwrap();
        let mut budget = ExpansionBudget::new("node", "source ordinates");
        let partition = inventory.partition(&mut budget).unwrap();
        let crossing = *partition.crossings().first().unwrap();
        let (z, m) = inventory
            .resolve_node_ordinates::<crate::crs::GeodesicMetric>(
                &partition,
                crossing,
                crate::geometry::CoordinateAxes::XYZM,
                &crate::geometry::SourceArcContext::cartesian(),
                None,
            )
            .unwrap();
        assert_eq!((z.0, m.0), (Some(20.0), Some(200.0)));
        let endpoint = partition.stored_node(XY::new(0.0, 0.0)).unwrap();
        let (z, m) = inventory
            .resolve_node_ordinates::<crate::crs::GeodesicMetric>(
                &partition,
                endpoint,
                crate::geometry::CoordinateAxes::XYZM,
                &crate::geometry::SourceArcContext::cartesian(),
                None,
            )
            .unwrap();
        assert_eq!((z.0, m.0), (None, None));
    }
}

#[test]
fn resolved_node_points_use_embedding_xy_and_exact_ordinates() {
    let xy = crate::io::parse_wkt("LINESTRING (0 0,4 0)").unwrap();
    let zm = crate::io::parse_wkt("LINESTRING ZM (2 -1 10 100,2 1 30 300)").unwrap();
    let inventory = ExactSourceInventory::from_shapes(&[&xy, &zm]).unwrap();
    let mut budget = ExpansionBudget::new("node", "embedding ordinates");
    let partition = inventory.partition(&mut budget).unwrap();
    let crossing = *partition.crossings().first().unwrap();
    let endpoint = partition.stored_node(XY::new(0.0, 0.0)).unwrap();
    let mut coordinates = partition.points.rounded.clone();
    *coordinates
        .get_mut(crossing.0 as usize)
        .expect("crossing belongs to the partition coordinate table") = XY::new(12.0, 13.0);
    let embedding = embedding::Embedding::from_owned(coordinates);
    let points = inventory
        .resolve_node_points::<crate::crs::GeodesicMetric, _>(
            &partition,
            &embedding,
            [crossing, endpoint],
            crate::geometry::CoordinateAxes::XYZM,
            &crate::geometry::SourceArcContext::cartesian(),
            None,
        )
        .unwrap();

    let crossing = points.point(crossing, embedding.xy(crossing));
    assert_eq!(crossing.xy(), XY::new(12.0, 13.0));
    assert_eq!((crossing.z(), crossing.m()), (Some(20.0), Some(200.0)));
    let endpoint = points.point(endpoint, embedding.xy(endpoint));
    assert_eq!(endpoint.xy(), XY::new(0.0, 0.0));
    assert_eq!((endpoint.z(), endpoint.m()), (None, None));
}

#[test]
fn xy_node_parts_serialize_the_certified_embedding() {
    let horizontal = crate::io::parse_wkt("LINESTRING (0 0,4 0)").unwrap();
    let vertical = crate::io::parse_wkt("LINESTRING (2 -1,2 1)").unwrap();
    let inventory = ExactSourceInventory::from_shapes(&[&horizontal, &vertical]).unwrap();
    let mut budget = ExpansionBudget::new("overlay", "certified XY serialization");
    let partition = inventory.partition(&mut budget).unwrap();
    let crossing = *partition.crossings().first().unwrap();
    let endpoint = partition.stored_node(XY::new(0.0, 0.0)).unwrap();
    let mut coordinates = partition.points.rounded;
    *coordinates
        .get_mut(crossing.0 as usize)
        .expect("crossing belongs to the partition coordinate table") = XY::new(12.0, 13.0);
    let embedding = embedding::Embedding::from_owned(coordinates);
    let points = ResolvedNodePoints::from_embedding(&embedding, None);
    let result = inventory
        .node_parts_to_shape_resolved(
            NodeParts {
                points: Vec::new(),
                lines: vec![vec![endpoint, crossing]],
                polygons: Vec::new(),
            },
            &embedding,
            &points,
            crate::geometry::CoordinateAxes::XY,
            false,
            "test",
            None,
        )
        .unwrap();

    match result {
        Shape::LineString(line) => {
            assert_eq!(line.xs(), [0.0, 12.0]);
            assert_eq!(line.ys(), [0.0, 13.0]);
        },
        Shape::MultiLineString(lines) => {
            assert_eq!(lines.len(), 1);
            assert_eq!(lines[0].xs(), [0.0, 12.0]);
            assert_eq!(lines[0].ys(), [0.0, 13.0]);
        },
        other @ (Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::Polygon(_)
        | Shape::MultiPolygon(_)
        | Shape::GeometryCollection(_)
        | Shape::Empty(..)) => panic!("expected one line, got {other:?}"),
    }
}

#[test]
fn coverage_xy_parts_serialize_the_certified_embedding() {
    let source = crate::io::parse_wkt("POLYGON ((0 0,4 0,4 4,0 4,0 0))").unwrap();
    let inventory = ExactSourceInventory::from_shapes(&[&source]).unwrap();
    let mut budget = ExpansionBudget::new("coverage_clean", "certified XY serialization");
    let partition = inventory.partition(&mut budget).unwrap();
    let node = |x, y| partition.stored_node(XY::new(x, y)).unwrap();
    let ring = vec![
        node(0.0, 0.0),
        node(4.0, 0.0),
        node(4.0, 4.0),
        node(0.0, 4.0),
        node(0.0, 0.0),
    ];
    let moved = node(4.0, 4.0);
    let mut coordinates = partition.points.rounded.clone();
    *coordinates
        .get_mut(moved.0 as usize)
        .expect("stored vertex belongs to the partition coordinate table") = XY::new(5.0, 5.0);
    let embedding = embedding::Embedding::from_owned(coordinates);
    let points = ResolvedNodePoints::from_embedding(&embedding, None);
    let result = ExactSourceInventory::coverage_shape_from_node_parts(
        NodeParts {
            points: Vec::new(),
            lines: Vec::new(),
            polygons: vec![vec![ring]],
        },
        &points,
        crate::geometry::CoordinateAxes::XY,
    )
    .unwrap();

    match result {
        Shape::Polygon(polygon) => {
            assert_eq!(polygon.shell.coords().xs(), [0.0, 4.0, 5.0, 0.0, 0.0]);
            assert_eq!(polygon.shell.coords().ys(), [0.0, 0.0, 5.0, 4.0, 0.0]);
        },
        Shape::MultiPolygon(polygons) => {
            assert_eq!(polygons.len(), 1);
            assert_eq!(polygons[0].shell.coords().xs(), [0.0, 4.0, 5.0, 0.0, 0.0]);
            assert_eq!(polygons[0].shell.coords().ys(), [0.0, 0.0, 5.0, 4.0, 0.0]);
        },
        other @ (Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::GeometryCollection(_)
        | Shape::Empty(..)) => panic!("expected one polygon, got {other:?}"),
    }
}

#[test]
fn csr_prefix_offsets_use_the_native_index_domain_past_u32_max() {
    let u32_max = usize::try_from(u32::MAX).expect("u32 count fits this supported target");
    let mut over_u32 = [0, u32_max, 1];
    construction::prefix_contact_offsets(&mut over_u32);
    assert_eq!(over_u32, [0, u32_max, u32_max.wrapping_add(1)]);

    let mut at_u32 = [0, u32_max.wrapping_sub(1), 1];
    construction::prefix_contact_offsets(&mut at_u32);
    assert_eq!(at_u32, [0, u32_max.wrapping_sub(1), u32_max]);
}

#[test]
fn source_provenance_placement_offsets_keep_checked_u32_totals() {
    let mut counts = [2, 0, 3];
    assert_eq!(placement_offsets(&mut counts).unwrap(), [0, 2, 2, 5]);
    assert_eq!(counts, [0, 2, 2]);

    let mut at_limit = [u32::MAX, 0];
    assert_eq!(placement_offsets(&mut at_limit).unwrap(), [
        0,
        u32::MAX,
        u32::MAX
    ]);
    assert_eq!(at_limit, [0, u32::MAX]);

    let mut over_limit = [u32::MAX, 1];
    assert!(matches!(
        placement_offsets(&mut over_limit),
        Err(error)
            if matches!(
                error.kind(),
                crate::error::ErrorKind::Resource(crate::error::ResourceError::SizeOverflow)
            )
    ));
}

#[test]
fn point_append_admission_rejects_when_count_is_u32_max() {
    let u32_max = usize::try_from(u32::MAX).expect("u32 count fits this supported target");
    let last_id = u32::MAX.wrapping_sub(1);
    assert_eq!(
        construction::point_id_for_append(u32_max.wrapping_sub(1))
            .expect("the maximum supported count can be reached"),
        last_id
    );
    assert!(matches!(
        construction::point_id_for_append(u32_max),
        Err(error)
            if matches!(
                error.kind(),
                crate::error::ErrorKind::Resource(crate::error::ResourceError::SizeOverflow)
            )
    ));
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

impl ExactPartition<'_> {
    fn stored_node(&self, point: XY) -> Option<NodeId> {
        let key = PointKey::new(point);
        (0..self.node_count())
            .find(|&node| self.stored_key(node) == Some(key))
            .map(|node| NodeId(node as u32))
    }

    /// The span a leftward ray from `query` meets first, by the sweep.
    fn left_contact(&self, query: NodeId) -> Option<usize> {
        shoot_left(
            &self.points,
            self.spans.len(),
            |index| [self.spans[index].start.0, self.spans[index].end.0],
            &[query.0],
        )
        .unwrap()[0]
    }

    fn crossings(&self) -> Vec<NodeId> {
        (0..self.node_count() as u32)
            .map(NodeId)
            .filter(|&node| !self.is_stored(node))
            .collect()
    }
}

use std::fmt::Write as _;

#[test]
fn line_contraction_preserves_bends_and_protected_points() {
    for protect in [false, true] {
        let mut sources = vec![
            seg(2.0, 0.0, 1.0, 0.0),
            seg(0.0, 0.0, 1.0, 0.0),
            seg(2.0, 0.0, 2.0, 1.0),
        ];
        if protect {
            sources.push(seg(1.0, 0.0, 1.0, 0.0));
        }
        let case = run(&sources);
        let partition = case.partition();
        let (topology, edges) = partition.local_topology().unwrap();
        let faces = partition.global_faces(&topology, &edges).unwrap();
        let plan = faces
            .boundary_plan(
                &(0..partition.spans.len() as u32).collect::<Vec<_>>(),
                &topology,
                &edges,
                &partition,
                &[false],
            )
            .unwrap();
        let (_, emitted) = certify_plan(&plan, &partition, &topology, &edges).unwrap();
        assert_eq!(emitted.len(), if protect { 3 } else { 2 });
        assert_eq!(plan.source_darts.len(), 3);
        assert!(emitted.iter().any(|segment| {
            segment.start == XY::new(2.0, 0.0) && segment.end == XY::new(2.0, 1.0)
        }));
        if !protect {
            assert!(emitted.iter().any(|segment| {
                segment.start == XY::new(2.0, 0.0) && segment.end == XY::new(0.0, 0.0)
            }));
        }
    }
}

#[test]
fn line_contraction_ignores_omitted_branch_at_the_join() {
    let case = run(&[
        seg(0.0, 0.0, 1.0, 0.0),
        seg(1.0, 0.0, 2.0, 0.0),
        seg(1.0, 0.0, 1.0, 1.0),
    ]);
    let partition = case.partition();
    let (topology, edges) = partition.local_topology().unwrap();
    let faces = partition.global_faces(&topology, &edges).unwrap();
    let lines: Vec<_> = partition
        .spans
        .iter()
        .enumerate()
        .filter_map(|(index, span)| {
            (partition.nearest_xy(span.start).y == 0.0 && partition.nearest_xy(span.end).y == 0.0)
                .then_some(index as u32)
        })
        .collect();
    let plan = faces
        .boundary_plan(&lines, &topology, &edges, &partition, &[false])
        .unwrap();
    let (_, emitted) = certify_plan(&plan, &partition, &topology, &edges).unwrap();
    assert_eq!(plan.source_darts.len(), 2);
    assert_eq!(emitted.len(), 1);
    let endpoints = [emitted[0].start, emitted[0].end];
    assert!(
        endpoints == [XY::new(0.0, 0.0), XY::new(2.0, 0.0)]
            || endpoints == [XY::new(2.0, 0.0), XY::new(0.0, 0.0)]
    );
}

#[test]
fn boundary_contraction_preserves_explicit_points_and_source_paths() {
    for (name, protect, attach_line) in [
        ("unprotected", false, false),
        ("explicit point", true, false),
        ("line junction", false, true),
    ] {
        let mut sources = vec![
            seg(1.0, 0.0, 2.0, 0.0),
            seg(2.0, 0.0, 2.0, 2.0),
            seg(2.0, 2.0, 0.0, 2.0),
            seg(0.0, 2.0, 0.0, 0.0),
            seg(0.0, 0.0, 1.0, 0.0),
        ];
        if protect {
            sources.push(seg(1.0, 0.0, 1.0, 0.0));
        }
        if attach_line {
            sources.push(seg(1.0, 0.0, 1.0, -1.0));
        }
        let case = run(&sources);
        let partition = case.partition();
        let (topology, edges) = partition.local_topology().unwrap();
        let faces = partition.global_faces(&topology, &edges).unwrap();
        assert_eq!(faces.count, 2);
        let lines = if attach_line {
            let endpoint = partition.stored_node(XY::new(1.0, -1.0)).unwrap();
            vec![
                partition
                    .spans
                    .iter()
                    .position(|span| span.start == endpoint || span.end == endpoint)
                    .unwrap() as u32,
            ]
        } else {
            Vec::new()
        };
        let plan = faces
            .boundary_plan(&lines, &topology, &edges, &partition, &[false, true])
            .unwrap();
        assert_eq!(plan.walks.len(), 1);
        assert_eq!(plan.source_darts.len(), if attach_line { 6 } else { 5 });
        let (_, emitted) = certify_plan(&plan, &partition, &topology, &edges).unwrap();
        assert_eq!(
            plan.walks[0].len(),
            if protect || attach_line { 5 } else { 4 },
            "{name}"
        );
        assert_eq!(
            emitted.len(),
            plan.walks[0].len() + usize::from(attach_line)
        );
        assert_eq!(
            emitted
                .iter()
                .any(|segment| segment.start == XY::new(1.0, 0.0)),
            protect || attach_line,
        );
        if !protect && !attach_line {
            assert!(emitted.iter().any(|segment| {
                segment.start == XY::new(0.0, 0.0) && segment.end == XY::new(2.0, 0.0)
            }));
        }
    }
}

#[test]
fn square_boundary_contraction_preserves_ordered_chain_and_point() {
    for (name, explicit_point, expected_sources, expected_groups, expected_xs, expected_ys) in [
        (
            "four-dart square bottom contracts once",
            None,
            vec![
                ((0.0, 0.0), (1.0, 0.0)),
                ((1.0, 0.0), (2.0, 0.0)),
                ((2.0, 0.0), (3.0, 0.0)),
                ((3.0, 0.0), (4.0, 0.0)),
                ((4.0, 0.0), (4.0, 2.0)),
                ((4.0, 2.0), (0.0, 2.0)),
                ((0.0, 2.0), (0.0, 0.0)),
            ],
            vec![
                (0..4, [1, 0]),
                (4..5, [1, 0]),
                (5..6, [1, 0]),
                (6..7, [1, 0]),
            ],
            vec![0.0, 4.0, 4.0, 0.0, 0.0],
            vec![0.0, 0.0, 2.0, 2.0, 0.0],
        ),
        (
            "explicit point at the square bottom midpoint splits the chord",
            Some(XY::new(2.0, 0.0)),
            vec![
                ((0.0, 0.0), (1.0, 0.0)),
                ((1.0, 0.0), (2.0, 0.0)),
                ((2.0, 0.0), (3.0, 0.0)),
                ((3.0, 0.0), (4.0, 0.0)),
                ((4.0, 0.0), (4.0, 2.0)),
                ((4.0, 2.0), (0.0, 2.0)),
                ((0.0, 2.0), (0.0, 0.0)),
            ],
            vec![
                (0..2, [1, 0]),
                (2..4, [1, 0]),
                (4..5, [1, 0]),
                (5..6, [1, 0]),
                (6..7, [1, 0]),
            ],
            vec![0.0, 2.0, 4.0, 4.0, 0.0, 0.0],
            vec![0.0, 0.0, 0.0, 2.0, 2.0, 0.0],
        ),
    ] {
        let case = square_boundary_case(explicit_point);
        let mut budget = ExpansionBudget::new("union", "square boundary regression");
        let partition = partition_unique_segments::<true>(
            &case.unique,
            &case.provenance,
            &case.isolated,
            &mut budget,
        )
        .unwrap();
        assert_eq!(budget.used(), 0, "{name}");
        assert!(
            matches!(&partition.kept_nodes, KeptNodes::Selected),
            "{name}"
        );
        if let Some(point) = explicit_point {
            let point = partition
                .stored_node(point)
                .expect("explicit point is stored");
            assert!(partition.point_nodes.contains(&point), "{name}");
        }
        let (topology, edges) = partition.local_topology().unwrap();
        let global = partition.global_faces(&topology, &edges).unwrap();
        assert_eq!(global.count, 2, "{name}");
        let inner_face = edges
            .iter()
            .enumerate()
            .find_map(|(slot, _)| {
                let origin = partition.nearest_xy(NodeId(topology.owners[slot]));
                let target = partition.nearest_xy(NodeId(topology.targets[slot]));
                (origin == XY::new(0.0, 0.0) && target == XY::new(1.0, 0.0))
                    .then_some(global.walk_faces[topology.face_of[slot] as usize] as usize)
            })
            .expect("square bottom has a directed source dart");
        assert_ne!(inner_face, 0, "{name}");
        let mut occupied = vec![false; global.count as usize];
        occupied[inner_face] = true;
        let plan = global
            .boundary_plan(&[], &topology, &edges, &partition, &occupied)
            .unwrap();

        assert_eq!(plan.face_quotient, [0, 1], "{name}");
        assert_eq!(plan.walks.len(), 1, "{name}");
        assert!(plan.lines.is_empty(), "{name}");
        assert!(plan.points.is_empty(), "{name}");
        assert_eq!(plan.source_darts.len(), 7, "{name}");
        assert_eq!(
            forward_source_darts(&plan, &partition, &topology),
            expected_sources,
            "{name}: ordered source darts"
        );
        assert_eq!(
            plan.walks[0]
                .iter()
                .map(|chord| (chord.sources.clone(), chord.sides))
                .collect::<Vec<_>>(),
            expected_groups,
            "{name}: chord source ranges and side labels"
        );

        let (points, lines, polygons) = plan
            .emit_xy_parts(&partition, &topology, &edges, "union")
            .unwrap();
        assert!(points.is_empty(), "{name}");
        assert!(lines.is_empty(), "{name}");
        assert_eq!(polygons.len(), 1, "{name}");
        assert!(polygons[0].holes.is_empty(), "{name}");
        assert_eq!(
            polygons[0].shell.coords().xs(),
            expected_xs,
            "{name}: emitted x-coordinate buffer"
        );
        assert_eq!(
            polygons[0].shell.coords().ys(),
            expected_ys,
            "{name}: emitted y-coordinate buffer"
        );
    }
}

#[test]
fn areal_boundary_contraction_merges_radial_multi_joins() {
    let radial =
        crate::io::parse_wkt("POLYGON ((-4 0,-3 -1,-2 -2,-1 -3,0 -4,3 -3,4 0,3 3,0 4,-3 3,-4 0))")
            .unwrap();
    let inventory = ExactSourceInventory::from_shapes(&[&radial]).unwrap();
    let context = union_boundary_context(&inventory);
    let plan = context
        .cells
        .emission_plan(
            &context.partition,
            &context.topology,
            &context.edges,
            &context.global,
        )
        .unwrap();

    assert_eq!(context.budget_used, 0);
    assert_eq!(context.global.count, 2);
    assert_eq!(plan.face_quotient, [0, 1]);
    assert_eq!(plan.walks.len(), 1);
    assert_eq!(plan.lines.len(), 0);
    assert!(plan.points.is_empty());
    assert_eq!(plan.source_darts.len(), 10);
    assert_eq!(
        forward_source_darts(&plan, &context.partition, &context.topology),
        [
            ((-4.0, 0.0), (-3.0, -1.0)),
            ((-3.0, -1.0), (-2.0, -2.0)),
            ((-2.0, -2.0), (-1.0, -3.0)),
            ((-1.0, -3.0), (0.0, -4.0)),
            ((0.0, -4.0), (3.0, -3.0)),
            ((3.0, -3.0), (4.0, 0.0)),
            ((4.0, 0.0), (3.0, 3.0)),
            ((3.0, 3.0), (0.0, 4.0)),
            ((0.0, 4.0), (-3.0, 3.0)),
            ((-3.0, 3.0), (-4.0, 0.0)),
        ]
    );
    assert_eq!(plan.walks[0].len(), 7);
    let mut source_counts: Vec<_> = plan.walks[0]
        .iter()
        .map(|chord| chord.sources.len())
        .collect();
    source_counts.sort_unstable();
    assert_eq!(source_counts, [1, 1, 1, 1, 1, 1, 4]);
    assert!(plan.walks[0].iter().all(|chord| chord.sides == [1, 0]));
    let (points, lines, polygons) = plan
        .emit_xy_parts(
            &context.partition,
            &context.topology,
            &context.edges,
            "union",
        )
        .unwrap();
    assert!(points.is_empty());
    assert!(lines.is_empty());
    assert_eq!(polygons.len(), 1);
    assert_eq!(
        crate::io::to_wkt(&Shape::Polygon(polygons.into_iter().next().unwrap())).unwrap(),
        "POLYGON ((-4 0, 0 -4, 3 -3, 4 0, 3 3, 0 4, -3 3, -4 0))"
    );
}

#[test]
fn explicit_point_and_residual_line_preserve_forward_join_paths() {
    let case = protected_mixed_boundary_case();
    let context = raw_protected_mixed_boundary_context(&case);
    let protected_node = context
        .partition
        .stored_node(XY::new(-2.0, -2.0))
        .expect("the explicit point is a stored radial boundary node");
    assert!(context.partition.point_nodes.contains(&protected_node));
    let plan = context
        .cells
        .emission_plan(
            &context.partition,
            &context.topology,
            &context.edges,
            &context.global,
        )
        .unwrap();

    assert_eq!(context.budget_used, 0);
    assert_eq!(plan.face_quotient, [0, 1]);
    assert_eq!(plan.walks.len(), 1);
    assert_eq!(plan.walks[0].len(), 8);
    assert_eq!(plan.source_darts.len(), 12);
    let mut source_counts: Vec<_> = plan.walks[0]
        .iter()
        .map(|chord| chord.sources.len())
        .collect();
    source_counts.sort_unstable();
    assert_eq!(source_counts, [1, 1, 1, 1, 1, 1, 2, 2]);
    assert_eq!(plan.lines.len(), 1);
    assert_eq!(plan.lines[0].sources.len(), 2);
    assert!(plan.points.is_empty());
    assert_eq!(
        forward_source_darts(&plan, &context.partition, &context.topology),
        [
            ((-4.0, 0.0), (-3.0, -1.0)),
            ((-3.0, -1.0), (-2.0, -2.0)),
            ((-2.0, -2.0), (-1.0, -3.0)),
            ((-1.0, -3.0), (0.0, -4.0)),
            ((0.0, -4.0), (3.0, -3.0)),
            ((3.0, -3.0), (4.0, 0.0)),
            ((4.0, 0.0), (3.0, 3.0)),
            ((3.0, 3.0), (0.0, 4.0)),
            ((0.0, 4.0), (-3.0, 3.0)),
            ((-3.0, 3.0), (-4.0, 0.0)),
            ((8.0, 0.0), (9.0, 0.0)),
            ((9.0, 0.0), (10.0, 0.0)),
        ]
    );
    let (points, lines, polygons) = plan
        .emit_xy_parts(
            &context.partition,
            &context.topology,
            &context.edges,
            "union",
        )
        .unwrap();
    assert!(points.is_empty());
    assert_eq!(lines.len(), 1);
    assert_eq!(lines[0].xs(), [8.0, 10.0]);
    assert_eq!(lines[0].ys(), [0.0, 0.0]);
    assert_eq!(polygons.len(), 1);
    assert_eq!(
        crate::io::to_wkt(&Shape::Polygon(polygons.into_iter().next().unwrap())).unwrap(),
        "POLYGON ((-4 0, -2 -2, 0 -4, 3 -3, 4 0, 3 3, 0 4, -3 3, -4 0))"
    );
}

#[test]
fn rounded_chord_contact_with_an_isolated_node_is_repaired() {
    let point = XY::new(0.5, 1.0 / 6.0);
    let case = run(&[
        seg(0.0, 0.0, 3.0, 1.0),
        seg(1.0, -1.0, 1.0, 1.0),
        seg(point.x, point.y, point.x, point.y),
    ]);
    let partition = case.partition();
    let start = partition.stored_node(XY::new(0.0, 0.0)).unwrap();
    let crossing = partition.crossings()[0];
    let isolated = partition.point_nodes[0];
    assert!(partition.isolated.contains(&isolated));
    assert!(partition.spans.iter().any(|span| {
        (span.start == start && span.end == crossing)
            || (span.end == start && span.start == crossing)
    }));
    let nodes = [start, crossing, isolated];
    let coordinates = partition.round_nodes(&nodes);
    embedding::check_node_injectivity(&nodes, &coordinates).unwrap();
    let (topology, edges) = partition.local_topology().unwrap();
    let faces = partition.global_faces(&topology, &edges).unwrap();
    let mut cells = ArealCells {
        faces: vec![false; faces.count as usize],
        spans: vec![false; partition.spans.len()],
        nodes: vec![false; partition.node_count()],
    };
    for (index, span) in partition.spans.iter().enumerate() {
        cells.spans[index] = (span.start == start && span.end == crossing)
            || (span.end == start && span.start == crossing);
    }
    cells.nodes[isolated.0 as usize] = true;
    let plan = cells
        .emission_plan(&partition, &topology, &edges, &faces)
        .unwrap();
    let emission = plan.emission(&topology, &edges);
    assert_eq!(emission.items.len(), 2);
    let embedding = partition.embed(&[&emission], "test").unwrap();
    assert_eq!(embedding.xy(start), partition.nearest_xy(start));
    assert_eq!(embedding.xy(isolated), partition.nearest_xy(isolated));
    assert_ne!(embedding.xy(crossing), partition.nearest_xy(crossing));
    let segments: Vec<_> = emission
        .items
        .iter()
        .map(|&item| embedding::Embedding::segment_at(embedding.coordinates(), item))
        .collect();
    let mut sink = embedding::FirstFailure::default();
    assert!(
        embedding::certify(
            &partition,
            &embedding,
            &[&emission],
            &[&segments],
            &mut sink,
        )
        .unwrap()
        .is_continue()
    );
    let (points, lines, polygons) = plan
        .emit_xy_parts(&partition, &topology, &edges, "test")
        .unwrap();
    assert_eq!(points.len(), 1);
    assert_eq!(lines.len(), 1);
    assert!(polygons.is_empty());
    assert!(!emitted_pair_preserves_incidence(
        [(start, coordinates[0]), (crossing, coordinates[1])],
        [(isolated, coordinates[2]); 2],
    ));
}

#[test]
fn certificate_ignores_unemitted_segment_suffix() {
    let case = run(&[seg(0.0, 0.0, 1.0, 0.0), seg(10.0, 0.0, 11.0, 0.0)]);
    let partition = case.partition();
    let emission = partition.span_emission(0..u32::try_from(partition.spans.len()).unwrap());
    assert_eq!(emission.items.len(), 2);

    let embedding = embedding::Embedding::nearest(&partition);
    let mut segments: Vec<_> = emission
        .items
        .iter()
        .map(|&item| embedding::Embedding::segment_at(embedding.coordinates(), item))
        .collect();
    segments.push(*segments.first().expect("emission has a segment"));

    let mut witnesses = WitnessLog::default();
    assert!(
        embedding::certify(
            &partition,
            &embedding,
            &[&emission],
            &[&segments],
            &mut witnesses,
        )
        .unwrap()
        .is_continue()
    );
    assert!(witnesses.0.is_empty());
}

#[test]
fn emitted_chords_allow_only_prescribed_endpoint_contacts() {
    let a = (NodeId(0), XY::new(0.0, 0.0));
    let b = (NodeId(1), XY::new(2.0, 0.0));
    let c = (NodeId(2), XY::new(2.0, 2.0));
    let d = (NodeId(3), XY::new(4.0, 0.0));
    let middle = (NodeId(4), XY::new(1.0, 0.0));
    for right in [[b, c], [b, d], [b, b], [c, c]] {
        assert!(emitted_pair_preserves_incidence([a, b], right));
        assert!(emitted_pair_preserves_incidence(right, [a, b]));
    }
    for right in [[middle, c], [middle, d], [middle, middle], [b, a]] {
        assert!(!emitted_pair_preserves_incidence([a, b], right));
        assert!(!emitted_pair_preserves_incidence(right, [a, b]));
    }
    let crossing = [
        (NodeId(5), XY::new(1.0, -1.0)),
        (NodeId(6), XY::new(1.0, 1.0)),
    ];
    assert!(!emitted_pair_preserves_incidence([a, b], crossing));
    assert!(!emitted_pair_preserves_incidence(crossing, [a, b]));
}

#[test]
fn external_emitted_incidence_rejects_unprescribed_contacts() {
    let left = ([0, 1], seg(0.0, 0.0, 2.0, 0.0));
    for right in [
        ([2, 3], seg(1.0, -1.0, 1.0, 1.0)),
        ([2, 3], seg(1.0, 0.0, 1.0, 1.0)),
        ([2, 3], seg(1.0, 0.0, 3.0, 0.0)),
        ([1, 0], seg(2.0, 0.0, 0.0, 0.0)),
    ] {
        for items in [[left, right], [right, left]] {
            let error = certify_emitted_incidence(&items).expect_err("unprescribed contact");
            assert!(
                matches!(
                    error.kind(),
                    crate::error::ErrorKind::Geometry(GeometryErrorKind::ExactUnexpectedContact {
                        first,
                        second,
                    })
                    if (*first, *second) == (0, 1) || (*first, *second) == (1, 0)
                ),
                "{error:?}"
            );
        }
    }
}

#[test]
fn external_emitted_incidence_accepts_disjoint_and_shared_endpoints() {
    let left = ([0, 1], seg(0.0, 0.0, 2.0, 0.0));
    for right in [
        ([2, 3], seg(0.0, 1.0, 2.0, 1.0)),
        ([1, 2], seg(2.0, 0.0, 2.0, 1.0)),
        ([1, 2], seg(2.0, 0.0, 3.0, 0.0)),
    ] {
        for items in [[left, right], [right, left]] {
            certify_emitted_incidence(&items).expect("prescribed incidence");
        }
    }
}

#[test]
fn live_node_injectivity_distinguishes_repetition_from_rounding_collision() {
    let case = run(&[seg(0.0, 0.0, 2.0, 0.0)]);
    let mut partition = case.partition();
    // A node at exact (x, 0), as a crossing whose exact point is known.
    let mut push = |x: ExactDyadic| {
        let node = NodeId(partition.node_count() as u32);
        let rounded = round_ratio(&x, &dyadic(1.0)).unwrap();
        let exact = std::cell::OnceCell::new();
        exact.set(Box::new([x, dyadic(0.0), dyadic(1.0)])).unwrap();
        partition
            .points
            .push(
                NodeRecipe::Crossing {
                    segments: [0, 0],
                    exact,
                },
                XY::new(rounded, 0.0),
            )
            .unwrap();
        node
    };
    let first = push(ExactDyadic::from_f64(1.0));
    let second = push(
        ExactDyadic::from_f64(1.0)
            .try_add(ExactDyadic::from_f64(2.0_f64.powi(-54)))
            .unwrap(),
    );
    assert_eq!(
        partition.compare_nodes(first, second).unwrap(),
        Ordering::Less
    );
    let nodes = [first, second];
    let coordinates = partition.round_nodes(&nodes);
    assert_eq!(coordinates, [XY::new(1.0, 0.0); 2]);
    assert!(embedding::check_node_injectivity(&nodes, &coordinates).is_err());
    embedding::check_node_injectivity(&[first, first], &coordinates).unwrap();
    let endpoint = partition.stored_node(XY::new(2.0, 0.0)).unwrap();
    let nodes = [first, endpoint];
    embedding::check_node_injectivity(&nodes, &partition.round_nodes(&nodes)).unwrap();
    embedding::check_node_injectivity(&[], &[]).unwrap();
}

#[test]
fn selected_nodes_round_in_requested_order() {
    let case = run(&[seg(0.0, 0.0, 3.0, 1.0), seg(1.0, -1.0, 1.0, 1.0)]);
    let partition = case.partition();
    let crossing = partition.crossings()[0];
    let endpoint = partition.stored_node(XY::new(3.0, 1.0)).unwrap();
    let rounded = partition.round_nodes(&[endpoint, crossing, endpoint]);
    assert_eq!(rounded[0], XY::new(3.0, 1.0));
    assert_eq!(rounded[1].x, 1.0);
    assert_eq!(rounded[1].y.to_bits(), 0x3FD5_5555_5555_5555);
    assert_eq!(rounded[2], XY::new(3.0, 1.0));
    assert!(partition.round_nodes(&[]).is_empty());
}

#[test]
fn collapsed_ring_sources_retain_support_without_winding_labels() {
    let polygons = [
        "POLYGON ((0 0,4 0,2 0,0 0),(1 1,3 1,3 3,1 3,1 1))",
        "POLYGON ((8 0,12 0,12 4,8 4,8 0),(9 2,11 2,10 2,9 2))",
    ]
    .map(|wkt| {
        let Shape::Polygon(polygon) = crate::io::parse_wkt(wkt).unwrap() else {
            unreachable!()
        };
        polygon
    });
    let sources = super::ArealSources::new(&polygons).unwrap();
    assert_eq!(sources.collapsed_rings, [(0, 0..3), (3, 11..14)]);
    assert_eq!(sources.polygon_rings, [0..2, 2..4]);
    assert_eq!(sources.source_rings, [
        None,
        None,
        None,
        Some(1),
        Some(1),
        Some(1),
        Some(1),
        Some(2),
        Some(2),
        Some(2),
        Some(2),
        None,
        None,
        None,
    ]);
    assert_eq!(sources.segments[0].start, XY::new(0.0, 0.0));
    assert_eq!(sources.segments[0].end, XY::new(4.0, 0.0));
    assert_eq!(sources.segments[11].start, XY::new(9.0, 2.0));
    assert_eq!(sources.segments[11].end, XY::new(11.0, 2.0));
    assert!(!sources.occupied(&[0, 1, 0, 0], false));
    assert!(sources.occupied(&[0, 0, 1, 0], false));
    let case = run(&sources.segments);
    let partition = case.partition();
    let (topology, edges) = partition.local_topology().unwrap();
    let faces = partition.global_faces(&topology, &edges).unwrap();
    let mut labels = Vec::new();
    partition
        .visit_face_windings(
            &topology,
            &edges,
            &faces,
            &sources.source_rings,
            sources.ring_count,
            |_, winding| {
                labels.push((
                    *winding.first_chunk::<4>().expect("four rings are labelled"),
                    sources.occupied(winding, false),
                ));
                Ok(())
            },
        )
        .unwrap();
    labels.sort_unstable();
    assert_eq!(labels, [
        ([0, 0, 0, 0], false),
        ([0, 0, 1, 0], true),
        ([0, 1, 0, 0], false),
    ]);
    let cells = sources
        .cells(&partition, &topology, &edges, &faces, false)
        .unwrap();
    assert_eq!(cells.faces.iter().filter(|&&filled| filled).count(), 1);
    for (index, span) in partition.spans.iter().enumerate() {
        assert!(
            partition.is_stored(span.start),
            "stored-only fixture constructed a node"
        );
        let start = partition.nearest_xy(span.start);
        let expected = start.x >= 8.0 || start.y == 0.0;
        assert_eq!(cells.spans[index], expected);
    }
    for (index, &filled) in cells.nodes.iter().enumerate() {
        assert!(
            partition.is_stored(NodeId(index as u32)),
            "stored-only fixture constructed a node"
        );
        let point = partition.nearest_xy(NodeId(index as u32));
        assert_eq!(filled, point.x >= 8.0 || point.y == 0.0);
    }
    let (lines, points) = cells.lower_dimensions(&partition, &topology, &edges, &faces);
    assert!(points.is_empty());
    let mut intervals = lines
        .iter()
        .map(|&index| {
            let span = partition.spans[index as usize];
            assert!(
                partition.is_stored(span.start),
                "stored-only fixture constructed a node"
            );
            let start = partition.nearest_xy(span.start);
            assert!(
                partition.is_stored(span.end),
                "stored-only fixture constructed a node"
            );
            let end = partition.nearest_xy(span.end);
            assert_eq!((start.y, end.y), (0.0, 0.0));
            (start.x.min(end.x), start.x.max(end.x))
        })
        .collect::<Vec<_>>();
    intervals.sort_unstable_by(|left, right| left.0.total_cmp(&right.0));
    assert_eq!(intervals, [(0.0, 2.0), (2.0, 4.0)]);
    let plan = cells
        .emission_plan(&partition, &topology, &edges, &faces)
        .unwrap();
    let (points, lines, polygons) = plan
        .emit_xy_parts(&partition, &topology, &edges, "test")
        .unwrap();
    assert!(points.is_empty());
    assert_eq!(lines.len(), 1);
    let mut endpoints = lines[0].xs().to_vec();
    endpoints.sort_unstable_by(f64::total_cmp);
    assert_eq!(endpoints, [0.0, 4.0]);
    assert_eq!(lines[0].ys(), [0.0, 0.0]);
    assert_eq!(polygons.len(), 1);
    assert!(polygons[0].holes.is_empty());
}

#[test]
fn areal_cells_exclude_cancelled_spurs_and_keep_collapsed_points() {
    let polygons = [
        "POLYGON ((0 0,4 0,4 4,0 4,0 0,-2 0,0 0))",
        "POLYGON ((6 6,6 6,6 6,6 6))",
    ]
    .map(|wkt| {
        let Shape::Polygon(polygon) = crate::io::parse_wkt(wkt).unwrap() else {
            unreachable!()
        };
        polygon
    });
    let sources = ArealSources::new(&polygons).unwrap();
    let (unique, provenance) =
        dedup_undirected_segments_sourced(sources.segments.iter().copied()).unwrap();
    let mut budget = ExpansionBudget::new("exact_partition", "items");
    let partition =
        partition_unique_segments::<true>(&unique, &provenance, &[], &mut budget).unwrap();
    let (topology, edges) = partition.local_topology().unwrap();
    let faces = partition.global_faces(&topology, &edges).unwrap();
    let cells = sources
        .cells(&partition, &topology, &edges, &faces, false)
        .unwrap();
    let mut selected = Vec::new();
    for (index, &filled) in cells.nodes.iter().enumerate() {
        if filled {
            assert!(
                partition.is_stored(NodeId(index as u32)),
                "stored-only fixture constructed a node"
            );
            let point = partition.nearest_xy(NodeId(index as u32));
            selected.push(point);
        }
    }
    selected.sort_unstable_by(|left, right| {
        left.x.total_cmp(&right.x).then(left.y.total_cmp(&right.y))
    });
    assert_eq!(selected, [
        XY::new(0.0, 0.0),
        XY::new(0.0, 4.0),
        XY::new(4.0, 0.0),
        XY::new(4.0, 4.0),
        XY::new(6.0, 6.0),
    ]);
    assert_eq!(cells.spans.iter().filter(|&&filled| filled).count(), 4);
    let (lines, points) = cells.lower_dimensions(&partition, &topology, &edges, &faces);
    assert!(lines.is_empty());
    assert_eq!(points, [partition.stored_node(XY::new(6.0, 6.0)).unwrap()]);
    let plan = cells
        .emission_plan(&partition, &topology, &edges, &faces)
        .unwrap();
    assert_eq!(plan.points, [(points[0], 0)]);
    assert!(plan.lines.is_empty());
    assert_eq!(plan.walks.len(), 1);
    let (_, rounded) = certify_plan(&plan, &partition, &topology, &edges).unwrap();
    let mut actual: Vec<_> = rounded.iter().map(|span| (span.start, span.end)).collect();
    assert_eq!(actual.pop(), Some((XY::new(6.0, 6.0), XY::new(6.0, 6.0))));
    let first = actual
        .iter()
        .position(|&(start, _)| start == XY::new(0.0, 0.0))
        .unwrap();
    actual.rotate_left(first);
    assert_eq!(actual, [
        (XY::new(0.0, 0.0), XY::new(4.0, 0.0)),
        (XY::new(4.0, 0.0), XY::new(4.0, 4.0)),
        (XY::new(4.0, 4.0), XY::new(0.0, 4.0)),
        (XY::new(0.0, 4.0), XY::new(0.0, 0.0)),
    ]);
}

#[test]
fn collapsed_point_shell_holes_are_applied_to_every_visited_face() {
    let cases = [
        (
            "collection-inner-first",
            "GEOMETRYCOLLECTION (POLYGON ((1 1,1 1,1 1,1 1),(0 0,2 0,2 2,0 2,0 0)), POLYGON ((3 3,3 3,3 3,3 3),(-2 -2,4 -2,4 4,-2 4,-2 -2)))",
        ),
        (
            "collection-outer-first",
            "GEOMETRYCOLLECTION (POLYGON ((3 3,3 3,3 3,3 3),(-2 -2,4 -2,4 4,-2 4,-2 -2)), POLYGON ((1 1,1 1,1 1,1 1),(0 0,2 0,2 2,0 2,0 0)))",
        ),
        (
            "multipolygon-inner-first",
            "MULTIPOLYGON (((1 1,1 1,1 1,1 1),(0 0,2 0,2 2,0 2,0 0)), ((3 3,3 3,3 3,3 3),(-2 -2,4 -2,4 4,-2 4,-2 -2)))",
        ),
        (
            "multipolygon-outer-first",
            "MULTIPOLYGON (((3 3,3 3,3 3,3 3),(-2 -2,4 -2,4 4,-2 4,-2 -2)), ((1 1,1 1,1 1,1 1),(0 0,2 0,2 2,0 2,0 0)))",
        ),
    ];
    let point = crate::io::parse_wkt("POINT (3 3)").unwrap();

    for (name, wkt) in cases {
        let shape = crate::io::parse_wkt(wkt).unwrap();
        let inventory =
            ExactSourceInventory::from_shapes_with_occurrences(&[&shape, &point]).unwrap();
        let mut budget = ExpansionBudget::new("relate", "collapsed point shells covered by holes");
        let partition = inventory.partition(&mut budget).unwrap();
        let (topology, edges) = partition.local_topology().unwrap();
        let global = partition.global_faces(&topology, &edges).unwrap();
        let winding_rings = inventory.winding_source_rings();
        let facts = inventory
            .build_cell_facts(
                &partition,
                &topology,
                &edges,
                &global,
                &winding_rings,
                false,
            )
            .unwrap();
        for point in [XY::new(1.0, 1.0), XY::new(3.0, 3.0)] {
            let node = partition
                .stored_node(point)
                .expect("collapsed shell point is staged");
            assert!(
                !facts.point_membership[node.0 as usize * 2],
                "{name}: a shell point covered by its own hole must not be occupied: {point:?}"
            );
        }
        assert_eq!(
            inventory.relate(&partition).unwrap().text(),
            "FFFFFF0F2",
            "{name}"
        );

        let result = Shape::union_all(&[&shape], Strictness::Lenient).unwrap();
        assert_eq!(
            crate::io::to_wkt(&result).unwrap(),
            "POLYGON EMPTY",
            "{name}"
        );
    }
}

#[test]
fn incident_collapsed_shell_points_keep_line_and_explicit_point_support() {
    let cases = [
        (
            "inner-first",
            "GEOMETRYCOLLECTION (POLYGON ((3 3,3 3,3 3,3 3),(2 2,4 2,4 4,2 4,2 2)), POLYGON ((3 3,3 3,3 3,3 3),(-2 -2,8 -2,8 8,-2 8,-2 -2)), LINESTRING (2 3,4 3), POINT (10 10))",
        ),
        (
            "outer-first",
            "GEOMETRYCOLLECTION (POLYGON ((3 3,3 3,3 3,3 3),(-2 -2,8 -2,8 8,-2 8,-2 -2)), POLYGON ((3 3,3 3,3 3,3 3),(2 2,4 2,4 4,2 4,2 2)), LINESTRING (2 3,4 3), POINT (10 10))",
        ),
    ];

    for (name, wkt) in cases {
        let shape = crate::io::parse_wkt(wkt).unwrap();
        let inventory = ExactSourceInventory::from_shapes_with_occurrences(&[&shape]).unwrap();
        let mut budget = ExpansionBudget::new("union", "incident collapsed shell points");
        let partition = inventory.partition(&mut budget).unwrap();
        let (topology, edges) = partition.local_topology().unwrap();
        let global = partition.global_faces(&topology, &edges).unwrap();
        let winding_rings = inventory.winding_source_rings();
        let facts = inventory
            .build_cell_facts(
                &partition,
                &topology,
                &edges,
                &global,
                &winding_rings,
                false,
            )
            .unwrap();

        let incident = partition
            .stored_node(XY::new(3.0, 3.0))
            .expect("line and both collapsed shells share this node");
        let incident_row = topology.starts[incident.0 as usize] as usize
            ..topology.starts[incident.0 as usize + 1] as usize;
        assert!(incident_row.len() >= 2, "{name}: shell point is incident");
        assert!(
            !facts.point_membership[incident.0 as usize],
            "{name}: the covered shell point must not be published"
        );
        assert!(
            facts.span_curve.iter().any(|&is_curve| is_curve),
            "{name}: the incident line remains curve support"
        );

        let explicit = partition
            .stored_node(XY::new(10.0, 10.0))
            .expect("explicit point source is staged");
        assert!(
            facts.point_membership[explicit.0 as usize],
            "{name}: unrelated explicit point support remains occupied"
        );

        let result = Shape::union_all(&[&shape], Strictness::Lenient).unwrap();
        assert_eq!(
            crate::io::to_wkt(&result).unwrap(),
            "GEOMETRYCOLLECTION (POINT (10 10), LINESTRING (2 3, 3 3, 4 3))",
            "{name}"
        );
    }
}

#[test]
fn zero_length_sources_keep_owners_at_incident_and_isolated_nodes() {
    let (unique, provenance) = dedup_undirected_segments_sourced([
        seg(1.0, 0.0, 1.0, 0.0),
        seg(0.0, 0.0, 2.0, 0.0),
        seg(1.0, 0.0, 1.0, 0.0),
        seg(3.0, 2.0, 3.0, 2.0),
    ])
    .unwrap();
    let mut budget = ExpansionBudget::new("exact_partition", "items");
    let partition =
        partition_unique_segments::<true>(&unique, &provenance, &[], &mut budget).unwrap();
    let incident = partition.stored_node(XY::new(1.0, 0.0)).unwrap();
    let isolated = partition.stored_node(XY::new(3.0, 2.0)).unwrap();
    assert_eq!(partition.point_sources, [
        (incident, 0),
        (incident, 2),
        (isolated, 3)
    ]);
    assert_eq!(partition.isolated, [isolated]);
    assert_eq!(partition.spans.len(), 2);
    assert!(partition.point_nodes.is_empty());
}

#[test]
fn stored_ring_sources_keep_direction_and_shell_hole_composition() {
    let polygon = |wkt| {
        let Shape::Polygon(polygon) = crate::io::parse_wkt(wkt).unwrap() else {
            unreachable!()
        };
        polygon
    };
    let polygons = [
        polygon("POLYGON ((0 0,0 4,4 4,4 0,0 0),(1 1,1 3,3 3,3 1,1 1))"),
        polygon("POLYGON ((8 0,10 0,10 2,8 2,8 0))"),
    ];
    let sources = super::ArealSources::new(&polygons).unwrap();
    assert_eq!(sources.ring_count, 3);
    assert_eq!(sources.polygon_rings, [0..2, 2..3]);
    assert_eq!(sources.segments[0].start, super::XY::new(0.0, 0.0));
    assert_eq!(sources.segments[0].end, super::XY::new(0.0, 4.0));
    assert_eq!(sources.segments[4].start, super::XY::new(1.0, 1.0));
    assert_eq!(sources.segments[4].end, super::XY::new(1.0, 3.0));
    assert_eq!(sources.source_rings, [
        Some(0),
        Some(0),
        Some(0),
        Some(0),
        Some(1),
        Some(1),
        Some(1),
        Some(1),
        Some(2),
        Some(2),
        Some(2),
        Some(2)
    ]);
    let case = run(&sources.segments);
    let partition = case.partition();
    let (topology, edges) = partition.local_topology().unwrap();
    let faces = partition.global_faces(&topology, &edges).unwrap();
    let mut labels = Vec::new();
    partition
        .visit_face_windings(
            &topology,
            &edges,
            &faces,
            &sources.source_rings,
            sources.ring_count,
            |_, winding| {
                labels.push((
                    *winding
                        .first_chunk::<3>()
                        .expect("three rings are labelled"),
                    sources.occupied(winding, false),
                ));
                Ok(())
            },
        )
        .unwrap();
    labels.sort_unstable();
    assert_eq!(labels, [
        ([-1, -1, 0], false),
        ([-1, 0, 0], true),
        ([0, 0, 0], false),
        ([0, 0, 1], true)
    ]);
    for (windings, nonzero, parity) in [
        ([0, 0, 0], false, false),
        ([-2, 0, 0], true, false),
        ([-1, -2, 0], false, true),
        ([-1, 1, 0], false, false),
        ([0, 1, 0], false, false),
        ([-1, -1, 1], true, true),
    ] {
        assert_eq!(sources.occupied(&windings, false), nonzero, "{windings:?}");
        assert_eq!(sources.occupied(&windings, true), parity, "{windings:?}");
    }
}

#[test]
fn occupied_support_owner_composes_stored_rings_and_parts() {
    let cases = [
        ("POLYGON ((0 0,1 0,1 1,0 1,0 0,1 0,1 1,0 1,0 0))", 1.0),
        ("POLYGON ((0 0,1 1,1 0,0 1,0 0))", 0.5),
        (
            "MULTIPOLYGON (((0 0,1 0,1 1,0 1,0 0)),((0.5 0,1.5 0,1.5 1,0.5 1,0.5 0)))",
            1.5,
        ),
        ("POLYGON ((0 0,1 0,1 1,0 1,0 0),(3 0,4 0,4 1,3 1,3 0))", 1.0),
        (
            "POLYGON ((0 0,1 0,1 1,0 1,0 0),(-0.5 -0.5,1.5 -0.5,1.5 1.5,-0.5 1.5,-0.5 -0.5))",
            0.0,
        ),
    ];
    for (wkt, expected) in cases {
        let shape = crate::io::parse_wkt(wkt).expect("valid test WKT");
        let support = occupied_cartesian_support(&shape).expect("exact occupied support");
        assert_eq!(support.area(), expected, "{wkt}");
    }
}

#[test]
fn occupied_area_owner_rounds_exact_selected_faces() {
    let cases = [
        ("POLYGON ((0 0,1 0,1 1,0 1,0 0,1 0,1 1,0 1,0 0))", 1.0),
        ("POLYGON ((0 0,1 1,1 0,0 1,0 0))", 0.5),
        (
            "MULTIPOLYGON (((0 0,1 0,1 1,0 1,0 0)),((0.5 0,1.5 0,1.5 1,0.5 1,0.5 0)))",
            1.5,
        ),
        ("POLYGON ((0 0,1 0,1 1,0 1,0 0),(3 0,4 0,4 1,3 1,3 0))", 1.0),
        (
            "POLYGON ((0 0,1 0,1 1,0 1,0 0),(-0.5 -0.5,1.5 -0.5,1.5 1.5,-0.5 1.5,-0.5 -0.5))",
            0.0,
        ),
    ];
    for (wkt, expected) in cases {
        let shape = crate::io::parse_wkt(wkt).expect("valid test WKT");
        assert_eq!(
            occupied_cartesian_area(&shape).expect("exact occupied area"),
            expected,
            "{wkt}"
        );
    }
}

#[test]
fn occupied_area_inventory_folds_repeated_shared_collection_owners() {
    let polygon =
        crate::io::parse_wkt("POLYGON ((0 0,1 0,1 1,0 1,0 0))").expect("valid shared-DAG polygon");
    let mut shape =
        Shape::geometry_collection(vec![polygon]).expect("one-polygon collection is admitted");
    for _ in 0..16 {
        shape = Shape::geometry_collection(vec![shape.clone(), shape])
            .expect("collection depth is admitted");
    }

    let inventory = ExactSourceInventory::from_occupied_area_shape(&shape)
        .expect("area sources stage")
        .expect("the repeated DAG contains a polygon");
    assert_eq!(inventory.source_segments.len(), 4);
    assert_eq!(
        occupied_cartesian_area(&shape).expect("exact occupied area"),
        1.0
    );
}

#[test]
fn set_inventory_deduplicates_shared_owners_and_occurrence_inventory_does_not() {
    let line = crate::io::parse_wkt("LINESTRING (0 0,1 0)").unwrap();
    let polygon = crate::io::parse_wkt("POLYGON ((0 0,1 0,1 1,0 1,0 0))").unwrap();
    let point = crate::io::parse_wkt("POINT (0 0)").unwrap();

    for (shape, segments, isolated) in [(line, 1, 0), (polygon, 4, 0), (point, 0, 1)] {
        let owner = Shape::geometry_collection(vec![shape]).unwrap();
        let shared = Shape::geometry_collection(vec![owner.clone(), owner]).unwrap();

        let set = ExactSourceInventory::from_shapes(&[&shared]).unwrap();
        assert_eq!(set.source_segments.len(), segments);
        assert_eq!(set.isolated.len(), isolated);

        let occurrences = ExactSourceInventory::from_shapes_with_occurrences(&[&shared]).unwrap();
        assert_eq!(occurrences.source_segments.len(), segments * 2);
        assert_eq!(occurrences.isolated.len(), isolated * 2);
    }
}

#[test]
fn staged_source_occurrences_are_readable_and_can_omit_zm_carriers() {
    let line = crate::io::parse_wkt("LINESTRING ZM (0 0 1 2,1 0 3 4)").unwrap();
    let polygon =
        crate::io::parse_wkt("POLYGON ZM ((0 0 1 2,2 0 1 2,2 2 1 2,0 2 1 2,0 0 1 2))").unwrap();
    let point = crate::io::parse_wkt("POINT ZM (5 5 1 2)").unwrap();
    let owner = Shape::geometry_collection(vec![line, polygon, point]).unwrap();
    let shared = Shape::geometry_collection(vec![owner.clone(), owner]).unwrap();
    let rows = || [storage::ShapeRow::Shape(&shared)].into_iter();

    let staged = StagedExactSources::from_rows_with_occurrences(rows(), false).unwrap();
    assert!(staged.relation_components().is_none());
    assert_eq!(staged.segments().len(), 10);
    assert_eq!(staged.source_operands(), &[0; 10]);
    assert_eq!(staged.source_rings(), &[
        None,
        Some(0),
        Some(0),
        Some(0),
        Some(0),
        None,
        Some(1),
        Some(1),
        Some(1),
        Some(1),
    ]);
    assert_eq!(staged.ring_count, 2);
    assert_eq!(staged.ring_kinds(), &[RingKind::Winding, RingKind::Winding]);
    assert_eq!(staged.ring_polygons(), &[0, 1]);
    assert_eq!(staged.ring_holes(), &[false, false]);
    assert_eq!(staged.polygon_operands(), &[0, 0]);
    assert_eq!(staged.polygon_ring_ranges(), &[0..1, 1..2]);
    assert_eq!(
        staged
            .polygon_rings()
            .first()
            .expect("one operand has two polygon ring ranges")
            .as_slice(),
        &[0..1, 1..2]
    );
    assert_eq!(staged.isolated().len(), 2);
    assert_eq!(staged.isolated_operands(), &[0, 0]);
    assert_eq!(staged.line_boundaries().len(), 4);
    assert_eq!(
        staged
            .source_points
            .as_ref()
            .expect("ZM carriers retained")
            .len(),
        10
    );

    let staged_xy = StagedExactSources::from_rows_with_occurrences(rows(), true).unwrap();
    assert_eq!(staged_xy.segments().len(), staged.segments().len());
    assert_eq!(staged_xy.source_operands(), staged.source_operands());
    assert_eq!(staged_xy.source_rings(), staged.source_rings());
    assert!(staged_xy.source_points.is_none());
}

#[test]
fn source_ring_kind_projection_uses_each_full_ring_id() {
    let source = crate::io::parse_wkt(
        "GEOMETRYCOLLECTION (\
            LINESTRING (30 0,31 0),\
            LINESTRING (40 0,41 0,41 1,40 0),\
            LINESTRING (50 0,50 0),\
            POLYGON ((20 20,22 20,22 22,20 22,20 20)),\
            POLYGON ((60 0,61 0,62 0,60 0)),\
            POLYGON ((70 0,70 0,70 0,70 0)),\
            POINT (80 0)\
        )",
    )
    .unwrap();
    let staged = StagedExactSources::from_rows_with_occurrences(
        [storage::ShapeRow::Shape(&source)].into_iter(),
        true,
    )
    .unwrap();

    assert_eq!(staged.segments().len(), 15);
    assert_eq!(staged.ring_count, 3);
    assert_eq!(staged.isolated().len(), 3);
    assert_eq!(staged.source_rings(), &[
        None,
        None,
        None,
        None,
        None,
        Some(0),
        Some(0),
        Some(0),
        Some(0),
        Some(1),
        Some(1),
        Some(1),
        Some(2),
        Some(2),
        Some(2),
    ]);
    assert_eq!(staged.ring_kinds(), &[
        RingKind::Winding,
        RingKind::Collapsed,
        RingKind::Collapsed
    ]);

    let inventory = ExactSourceInventory::from_staged(staged).unwrap();
    assert_eq!(inventory.winding_source_rings(), vec![
        None,
        None,
        None,
        None,
        None,
        Some(0),
        Some(0),
        Some(0),
        Some(0),
        None,
        None,
        None,
        None,
        None,
        None,
    ]);
    assert_eq!(
        inventory.source_winding_at_stored(XY::new(21.0, 21.0)),
        (vec![1, 0, 0], false)
    );
    let interior = [dyadic(21.0), dyadic(21.0), dyadic(1.0)];
    assert_eq!(
        inventory
            .source_winding_at_symbolic(&interior, [1, 0])
            .unwrap(),
        vec![1, 0, 0]
    );
}

#[test]
fn relation_staging_keeps_closed_and_collapsed_line_components() {
    let closed = crate::io::parse_wkt("LINESTRING (0 0,4 0,4 4,0 0)").unwrap();
    let reversed = crate::io::parse_wkt("LINESTRING (0 0,4 4,4 0,0 0)").unwrap();
    let collapsed = crate::io::parse_wkt("LINESTRING (7 7,7 7)").unwrap();
    let polygon = crate::io::parse_wkt("POLYGON ((20 20,24 20,24 24,20 24,20 20))").unwrap();
    let shapes = [closed, reversed, collapsed, polygon];
    let staged =
        StagedExactSources::from_rows_for_relation(shapes.iter().map(storage::ShapeRow::Shape))
            .unwrap();

    assert_eq!(staged.segments().len(), 11);
    assert!(staged.line_boundaries().is_empty());
    assert_eq!(staged.isolated(), &[XY::new(7.0, 7.0), XY::new(7.0, 7.0)]);
    assert_eq!(staged.isolated_operands(), &[2, 2]);

    let components = staged
        .relation_components()
        .expect("relation staging retains component ranges");
    assert_eq!(components.len(), 4);
    assert_eq!(components[0].operand, 0);
    assert_eq!(components[0].segments, 0..3);
    assert_eq!(components[0].ring, None);
    assert!(components[0].has_nonzero_segment);
    assert_eq!(components[1].operand, 1);
    assert_eq!(components[1].segments, 3..6);
    assert_eq!(components[1].ring, None);
    assert!(components[1].has_nonzero_segment);
    assert_eq!(components[2].operand, 2);
    assert_eq!(components[2].segments, 6..7);
    assert_eq!(components[2].ring, None);
    assert!(!components[2].has_nonzero_segment);
    assert_eq!(components[3].operand, 3);
    assert_eq!(components[3].segments, 7..11);
    assert_eq!(components[3].ring, Some(0));
    assert_eq!(components[3].ring_kind, Some(RingKind::Winding));
    assert!(components[3].has_nonzero_segment);
    assert_eq!(
        staged.relation_source_order(),
        Some(
            [
                StagedRelationItem::Component(0),
                StagedRelationItem::Component(1),
                StagedRelationItem::Isolated(0),
                StagedRelationItem::Isolated(1),
                StagedRelationItem::Component(3),
            ]
            .as_slice()
        )
    );
}

#[test]
fn segment_contact_classifies_pairs_and_defers_collinear_spans() {
    let segment = |start: (f64, f64), end: (f64, f64)| Segment {
        start: XY::new(start.0, start.1),
        end: XY::new(end.0, end.1),
    };
    let cases = [
        (
            segment((0.0, 0.0), (1.0, 0.0)),
            segment((3.0, 0.0), (4.0, 1.0)),
            Contact::Disjoint,
            None,
            None,
        ),
        (
            segment((0.0, 0.0), (4.0, 4.0)),
            segment((0.0, 4.0), (4.0, 0.0)),
            Contact::Cross,
            None,
            None,
        ),
        (
            segment((0.0, 0.0), (2.0, 0.0)),
            segment((2.0, 0.0), (2.0, 2.0)),
            Contact::Touch(TouchLocation::Shared(TouchEndpoint::RightStart)),
            None,
            Some(XY::new(2.0, 0.0)),
        ),
        (
            segment((0.0, 0.0), (4.0, 0.0)),
            segment((2.0, -2.0), (2.0, 0.0)),
            Contact::Touch(TouchLocation::LeftInterior(SegmentEndpoint::End)),
            None,
            Some(XY::new(2.0, 0.0)),
        ),
        (
            segment((0.0, 0.0), (4.0, 0.0)),
            segment((2.0, 0.0), (2.0, 2.0)),
            Contact::Touch(TouchLocation::LeftInterior(SegmentEndpoint::Start)),
            None,
            Some(XY::new(2.0, 0.0)),
        ),
        (
            segment((2.0, -2.0), (2.0, 0.0)),
            segment((0.0, 0.0), (4.0, 0.0)),
            Contact::Touch(TouchLocation::RightInterior(SegmentEndpoint::End)),
            None,
            Some(XY::new(2.0, 0.0)),
        ),
        (
            segment((0.0, 0.0), (4.0, 0.0)),
            segment((2.0, 0.0), (6.0, 0.0)),
            Contact::Collinear,
            Some(SharedSpan {
                same_direction: true,
                start: XY::new(2.0, 0.0),
                end: XY::new(4.0, 0.0),
            }),
            None,
        ),
        (
            segment((0.0, 0.0), (2.0, 0.0)),
            segment((2.0, 0.0), (4.0, 0.0)),
            Contact::Collinear,
            Some(SharedSpan {
                same_direction: true,
                start: XY::new(2.0, 0.0),
                end: XY::new(2.0, 0.0),
            }),
            None,
        ),
        (
            segment((2.0, 0.0), (2.0, 0.0)),
            segment((0.0, 0.0), (4.0, 0.0)),
            Contact::Touch(TouchLocation::RightInterior(SegmentEndpoint::Start)),
            None,
            Some(XY::new(2.0, 0.0)),
        ),
        (
            segment((0.0, 0.0), (0.0, 0.0)),
            segment((0.0, 0.0), (4.0, 0.0)),
            Contact::Touch(TouchLocation::Shared(TouchEndpoint::LeftStart)),
            None,
            Some(XY::new(0.0, 0.0)),
        ),
        (
            segment((0.0, 0.0), (4.0, 4.0)),
            segment((4.0, 4.0), (0.0, 0.0)),
            Contact::Collinear,
            Some(SharedSpan {
                same_direction: false,
                start: XY::new(0.0, 0.0),
                end: XY::new(4.0, 4.0),
            }),
            None,
        ),
    ];
    for (index, (left, right, expected, expected_span, expected_touch)) in
        cases.into_iter().enumerate()
    {
        let contact = segment_contact_exact(left, right);
        assert_eq!(contact, expected, "pair {index}");
        let actual_touch = match contact {
            Contact::Touch(endpoint) => Some(endpoint.point(left, right)),
            Contact::Disjoint | Contact::Cross | Contact::Collinear => None,
        };
        assert_eq!(actual_touch, expected_touch, "touch point for pair {index}");
        assert_eq!(
            contact.shared_span(left, right),
            expected_span,
            "shared span for pair {index}",
        );
    }
}

#[test]
fn partition_projection_matches_contact_for_seeded_degenerate_pairs() {
    fn next(state: &mut u64) -> u64 {
        *state = (*state).wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut value = *state;
        value = (value ^ (value >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        value = (value ^ (value >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        value ^ (value >> 31)
    }

    fn point(state: &mut u64) -> (i32, i32) {
        let x = (next(state) % 17) as i32 - 8;
        let y = (next(state) % 17) as i32 - 8;
        (x, y)
    }

    fn offset(origin: (i32, i32), delta: (i32, i32), scale: i32) -> (i32, i32) {
        (origin.0 + delta.0 * scale, origin.1 + delta.1 * scale)
    }

    let segment = |start: (i32, i32), end: (i32, i32)| Segment {
        start: XY::new(f64::from(start.0), f64::from(start.1)),
        end: XY::new(f64::from(end.0), f64::from(end.1)),
    };
    let mut pairs = vec![
        (segment((0, 0), (2, 0)), segment((4, 0), (4, 1))),
        (segment((0, 0), (4, 4)), segment((0, 4), (4, 0))),
        (segment((0, 0), (2, 0)), segment((2, 0), (2, 2))),
        (segment((0, 0), (4, 0)), segment((2, -2), (2, 0))),
        (segment((2, -2), (2, 0)), segment((0, 0), (4, 0))),
        (segment((0, 0), (4, 0)), segment((2, 0), (6, 0))),
        (segment((0, 0), (2, 0)), segment((2, 0), (4, 0))),
        (segment((0, 0), (0, 0)), segment((0, 0), (4, 0))),
        (segment((2, 0), (2, 0)), segment((0, 0), (4, 0))),
        (segment((0, 1), (0, 1)), segment((0, 0), (4, 0))),
    ];

    let mut seed = 0x7A31_09D5_C462_EF81;
    for index in 0..512 {
        let left_start = point(&mut seed);
        let left_end = if index % 17 == 0 {
            left_start
        } else {
            point(&mut seed)
        };
        let right_start = match index % 5 {
            0 => left_start,
            1 => left_end,
            _ => point(&mut seed),
        };
        let right_end = if index % 19 == 0 {
            right_start
        } else if index % 11 == 0 {
            left_start
        } else {
            point(&mut seed)
        };
        pairs.push((
            segment(left_start, left_end),
            segment(right_start, right_end),
        ));
    }
    for _ in 0..128 {
        let origin = point(&mut seed);
        let mut delta = (
            (next(&mut seed) % 7) as i32 - 3,
            (next(&mut seed) % 7) as i32 - 3,
        );
        if delta == (0, 0) {
            delta.0 = 1;
        }
        let left = segment(origin, offset(origin, delta, 4));
        let right = segment(
            offset(origin, delta, (next(&mut seed) % 8) as i32 - 2),
            offset(origin, delta, (next(&mut seed) % 8) as i32 - 2),
        );
        pairs.push((left, right));
    }
    for _ in 0..64 {
        let origin = point(&mut seed);
        let mut delta = (
            (next(&mut seed) % 5) as i32 - 2,
            (next(&mut seed) % 5) as i32 - 2,
        );
        if delta == (0, 0) {
            delta.0 = 1;
        }
        let middle = offset(origin, delta, 2);
        let perpendicular = (-delta.1, delta.0);
        pairs.push((
            segment(origin, offset(origin, delta, 4)),
            segment(middle, offset(middle, perpendicular, 2)),
        ));
    }

    let mut saw_disjoint = false;
    let mut saw_shared = false;
    let mut saw_left_interior = false;
    let mut saw_right_interior = false;
    let mut saw_collinear = false;
    let mut saw_cross = false;

    for (pair_index, (left, right)) in pairs.into_iter().enumerate() {
        let expected = segment_contact_exact(left, right);
        let unique = [left, right];
        let mut forest = construction::Forest::with_capacity(&unique, 4);
        let mut start_handles = Vec::with_capacity(unique.len());
        let mut end_handles = Vec::with_capacity(unique.len());
        for segment in &unique {
            start_handles.push(
                forest
                    .push_stored(segment.start)
                    .expect("stored start handle admits"),
            );
            end_handles.push(
                forest
                    .push_stored(segment.end)
                    .expect("stored end handle admits"),
            );
        }
        let handles =
            construction::EndpointHandles::from_columns(&start_handles, &end_handles, &[]);
        let mut contacts = Vec::new();
        let _ = construction::take_partition_projection_effect();
        let has_overlap = construction::classify_pair_into(
            &unique,
            0,
            1,
            &handles,
            &mut contacts,
            &mut forest,
            None,
        )
        .expect("small finite integer pairs stay representable");
        let effect = construction::take_partition_projection_effect()
            .expect("partition projection records its selected effect");
        let projected = match effect {
            construction::PartitionProjectionEffect::Disjoint => {
                saw_disjoint = true;
                assert!(
                    contacts.is_empty(),
                    "disjoint pair {pair_index} emitted a cut"
                );
                Contact::Disjoint
            },
            construction::PartitionProjectionEffect::Shared(endpoint) => {
                saw_shared = true;
                assert!(
                    contacts.is_empty(),
                    "shared endpoint pair {pair_index} emitted a cut"
                );
                Contact::Touch(TouchLocation::Shared(endpoint))
            },
            construction::PartitionProjectionEffect::LeftInterior(endpoint) => {
                saw_left_interior = true;
                assert_eq!(contacts.len(), 1, "left-interior pair {pair_index}");
                let Some(&(source, handle)) = contacts.first() else {
                    unreachable!("the asserted single contact is present")
                };
                assert_eq!(source, 0, "left-interior cut source for pair {pair_index}");
                let expected_handle = match endpoint {
                    SegmentEndpoint::Start => *start_handles.get(1).expect("right start handle"),
                    SegmentEndpoint::End => *end_handles.get(1).expect("right end handle"),
                };
                assert_eq!(
                    handle, expected_handle,
                    "left-interior cut for pair {pair_index}"
                );
                Contact::Touch(TouchLocation::LeftInterior(endpoint))
            },
            construction::PartitionProjectionEffect::RightInterior(endpoint) => {
                saw_right_interior = true;
                assert_eq!(contacts.len(), 1, "right-interior pair {pair_index}");
                let Some(&(source, handle)) = contacts.first() else {
                    unreachable!("the asserted single contact is present")
                };
                assert_eq!(source, 1, "right-interior cut source for pair {pair_index}");
                let expected_handle = match endpoint {
                    SegmentEndpoint::Start => *start_handles.first().expect("left start handle"),
                    SegmentEndpoint::End => *end_handles.first().expect("left end handle"),
                };
                assert_eq!(
                    handle, expected_handle,
                    "right-interior cut for pair {pair_index}"
                );
                Contact::Touch(TouchLocation::RightInterior(endpoint))
            },
            construction::PartitionProjectionEffect::Collinear => {
                saw_collinear = true;
                let expected_cuts = [
                    (
                        left,
                        right.start,
                        0,
                        *start_handles.get(1).expect("right start handle"),
                    ),
                    (
                        left,
                        right.end,
                        0,
                        *end_handles.get(1).expect("right end handle"),
                    ),
                    (
                        right,
                        left.start,
                        1,
                        *start_handles.first().expect("left start handle"),
                    ),
                    (
                        right,
                        left.end,
                        1,
                        *end_handles.first().expect("left end handle"),
                    ),
                ]
                .into_iter()
                .filter(|(segment, point, ..)| {
                    *point != segment.start
                        && *point != segment.end
                        && point.x >= segment.start.x.min(segment.end.x)
                        && point.x <= segment.start.x.max(segment.end.x)
                        && point.y >= segment.start.y.min(segment.end.y)
                        && point.y <= segment.start.y.max(segment.end.y)
                })
                .map(|(_, _, source, handle)| (source, handle))
                .collect::<Vec<_>>();
                assert_eq!(
                    contacts, expected_cuts,
                    "collinear interior cuts for pair {pair_index}"
                );
                Contact::Collinear
            },
            construction::PartitionProjectionEffect::Cross => {
                saw_cross = true;
                assert_eq!(contacts.len(), 2, "crossing pair {pair_index}");
                let [(left_source, left_handle), (right_source, right_handle)] =
                    contacts.as_slice()
                else {
                    unreachable!("the asserted two crossing contacts are present")
                };
                assert_eq!((*left_source, *right_source), (0, 1));
                assert_eq!(left_handle, right_handle);
                Contact::Cross
            },
        };
        assert_eq!(
            projected, expected,
            "projection mismatch for pair {pair_index}"
        );
        assert_eq!(
            has_overlap,
            matches!(expected, Contact::Collinear),
            "overlap result for pair {pair_index}"
        );
    }

    assert!(
        saw_disjoint
            && saw_shared
            && saw_left_interior
            && saw_right_interior
            && saw_collinear
            && saw_cross,
        "the corpus must exercise every classifier projection arm"
    );
}

#[test]
fn cartesian_set_support_consumers_match_unshared_expansion() {
    let base = crate::io::parse_wkt("POLYGON ((0 0,1 0,1 1,0 1,0 0))").unwrap();
    let mut shared = base;
    for _ in 0..10 {
        shared = Shape::geometry_collection(vec![shared.clone(), shared]).unwrap();
    }
    let expanded = crate::io::parse_wkb(
        &crate::io::to_wkb(&shared, None, false).expect("shared support serializes"),
    )
    .expect("unshared support expansion parses")
    .shape;

    let shared_support = occupied_cartesian_support(&shared).unwrap();
    let expanded_support = occupied_cartesian_support(&expanded).unwrap();
    assert_eq!(
        crate::io::to_wkb(&shared_support, None, false).unwrap(),
        crate::io::to_wkb(&expanded_support, None, false).unwrap()
    );
    assert_eq!(
        occupied_cartesian_area(&shared).unwrap().to_bits(),
        occupied_cartesian_area(&expanded).unwrap().to_bits()
    );
    assert_eq!(
        cartesian_support_nonempty(&shared).unwrap(),
        cartesian_support_nonempty(&expanded).unwrap()
    );
    let bounds = Bounds::new_raw(0.25, 0.25, 0.75, 0.75);
    assert_eq!(
        exact_intersects_bounds(&shared, bounds).unwrap(),
        exact_intersects_bounds(&expanded, bounds).unwrap()
    );
}

#[test]
fn occupied_area_ingress_declines_collection_without_areal_members() {
    let shape = crate::io::parse_wkt("GEOMETRYCOLLECTION (POINT (1 2), LINESTRING (0 0, 3 4))")
        .expect("valid nonareal collection");
    assert!(
        ExactSourceInventory::from_occupied_area_shape(&shape)
            .expect("nonareal collection is admitted")
            .is_none()
    );
    assert_eq!(occupied_cartesian_area(&shape).unwrap(), 0.0);
}

use crate::geometry::generated_budget::PAIRWISE_WORK_LIMIT;

/// One shared operand must select exactly the cells separate operands do.
#[test]
fn polygonal_union_as_one_operand_matches_separate_operands() {
    let shapes: Vec<Shape> = [
        "POLYGON ((0 0,4 0,4 4,0 4,0 0),(1 1,3 1,3 3,1 3,1 1))",
        "POLYGON ((0 0,4 0,4 4,0 4,0 0),(1 1,3 1,3 3,1 3,1 1))",
        "POLYGON ((2 2,6 2,6 6,2 6,2 2))",
        "POLYGON ((4 0,8 0,8 2,4 2,4 0))",
        "POLYGON ((1.5 1.5,2.5 1.5,2.5 2.5,1.5 2.5,1.5 1.5))",
        "MULTIPOLYGON (((10 10,11 10,11 11,10 11,10 10)),((11 11,12 11,12 12,11 12,11 11)))",
        "POLYGON ((0 0,1 -1,2 0,1 1,0 0))",
    ]
    .iter()
    .map(|wkt| crate::io::parse_wkt(wkt).unwrap())
    .collect();
    let union = |inventory: ExactSourceInventory, inputs: &[&Shape]| {
        let mut budget = ExpansionBudget::new("union", "exact partition");
        let partition = inventory.partition(&mut budget).unwrap();
        let axes = super::super::overlay_declared_axes(inputs, crate::geometry::OverlayOp::Union);
        inventory
            .overlay_shape::<crate::crs::GeodesicMetric>(
                &partition,
                crate::geometry::OverlayOp::Union,
                axes,
                true,
                &crate::geometry::SourceArcContext::cartesian(),
            )
            .unwrap()
    };
    for count in 2..=shapes.len() {
        let inputs: Vec<&Shape> = shapes[..count].iter().collect();
        let shared = ExactSourceInventory::from_polygonal_union(&inputs)
            .unwrap()
            .expect("polygonal inputs share one operand");
        let separate = ExactSourceInventory::from_shapes(&inputs).unwrap();
        assert_eq!(
            union(shared, &inputs),
            union(separate, &inputs),
            "{count} inputs"
        );
    }
    let line = crate::io::parse_wkt("LINESTRING (0 0,1 1)").unwrap();
    assert!(
        ExactSourceInventory::from_polygonal_union(&[&shapes[0], &line])
            .unwrap()
            .is_none()
    );
}

fn seg(x0: f64, y0: f64, x1: f64, y1: f64) -> Segment {
    Segment {
        start: XY::new(x0, y0),
        end: XY::new(x1, y1),
    }
}

struct Case {
    unique: Vec<Segment>,
    provenance: SegmentProvenance,
    isolated: Vec<XY>,
}

impl Case {
    fn from_segments(segments: &[Segment]) -> Self {
        let mut edges = Vec::new();
        let mut isolated = Vec::new();
        for &segment in segments {
            if same_point(segment.start, segment.end) {
                isolated.push(segment.start);
            } else {
                edges.push(segment);
            }
        }
        let (unique, provenance) = dedup_undirected_segments_sourced(edges).unwrap();
        Self {
            unique,
            provenance,
            isolated,
        }
    }

    fn partition(&self) -> ExactPartition<'_> {
        let mut budget = ExpansionBudget::new("exact_partition", "items");
        partition_unique_segments::<true>(
            &self.unique,
            &self.provenance,
            &self.isolated,
            &mut budget,
        )
        .unwrap()
    }
}

fn run(segments: &[Segment]) -> Case {
    Case::from_segments(segments)
}

struct BoundaryContext<'a> {
    partition: ExactPartition<'a>,
    topology: IndexedTopology,
    edges: Vec<u32>,
    global: GlobalFaces,
    cells: ArealCells,
    budget_used: usize,
}

fn union_boundary_context(inventory: &ExactSourceInventory) -> BoundaryContext<'_> {
    let mut budget = ExpansionBudget::new("union", "boundary regression");
    let partition = inventory.partition(&mut budget).unwrap();
    let budget_used = budget.used();
    let (topology, edges) = partition.local_topology().unwrap();
    let global = partition.global_faces(&topology, &edges).unwrap();
    let cells = inventory
        .select_cells(
            &partition,
            &topology,
            &edges,
            &global,
            crate::geometry::OverlayOp::Union,
            false,
        )
        .unwrap();
    BoundaryContext {
        partition,
        topology,
        edges,
        global,
        cells,
        budget_used,
    }
}

fn protected_mixed_boundary_case() -> Case {
    let mut segments = vec![
        seg(-4.0, 0.0, -3.0, -1.0),
        seg(-3.0, -1.0, -2.0, -2.0),
        seg(-2.0, -2.0, -1.0, -3.0),
        seg(-1.0, -3.0, 0.0, -4.0),
        seg(0.0, -4.0, 3.0, -3.0),
        seg(3.0, -3.0, 4.0, 0.0),
        seg(4.0, 0.0, 3.0, 3.0),
        seg(3.0, 3.0, 0.0, 4.0),
        seg(0.0, 4.0, -3.0, 3.0),
        seg(-3.0, 3.0, -4.0, 0.0),
    ];
    segments.push(seg(-2.0, -2.0, -2.0, -2.0));
    segments.extend([seg(8.0, 0.0, 9.0, 0.0), seg(9.0, 0.0, 10.0, 0.0)]);
    Case::from_segments(&segments)
}

fn square_boundary_case(explicit_point: Option<XY>) -> Case {
    let mut segments = vec![
        seg(0.0, 0.0, 1.0, 0.0),
        seg(1.0, 0.0, 2.0, 0.0),
        seg(2.0, 0.0, 3.0, 0.0),
        seg(3.0, 0.0, 4.0, 0.0),
        seg(4.0, 0.0, 4.0, 2.0),
        seg(4.0, 2.0, 0.0, 2.0),
        seg(0.0, 2.0, 0.0, 0.0),
    ];
    if let Some(point) = explicit_point {
        segments.push(seg(point.x, point.y, point.x, point.y));
    }
    Case::from_segments(&segments)
}

fn raw_protected_mixed_boundary_context(case: &Case) -> BoundaryContext<'_> {
    let mut budget = ExpansionBudget::new("union", "protected mixed boundary regression");
    let partition = partition_unique_segments::<true>(
        &case.unique,
        &case.provenance,
        &case.isolated,
        &mut budget,
    )
    .unwrap();
    assert!(matches!(&partition.kept_nodes, KeptNodes::Selected));
    let budget_used = budget.used();
    let (topology, edges) = partition.local_topology().unwrap();
    let global = partition.global_faces(&topology, &edges).unwrap();
    assert_eq!(global.count, 2);
    let inner_face = edges
        .iter()
        .enumerate()
        .find_map(|(slot, _)| {
            let origin = partition.nearest_xy(NodeId(topology.owners[slot]));
            let target = partition.nearest_xy(NodeId(topology.targets[slot]));
            ((origin.x, origin.y) == (-4.0, 0.0) && (target.x, target.y) == (-3.0, -1.0))
                .then_some(global.walk_faces[topology.face_of[slot] as usize] as usize)
        })
        .expect("the radial boundary has its first directed source dart");
    assert_ne!(inner_face, 0);
    let mut faces = vec![false; global.count as usize];
    faces[inner_face] = true;
    let mut spans = vec![false; partition.spans.len()];
    assert_eq!(partition.point_nodes.len(), 1);
    for (index, span) in partition.spans.iter().enumerate() {
        let start = partition.nearest_xy(span.start);
        let end = partition.nearest_xy(span.end);
        if start.y == 0.0 && end.y == 0.0 && start.x.min(end.x) >= 8.0 {
            spans[index] = true;
        }
    }
    assert_eq!(spans.iter().filter(|&&selected| selected).count(), 2);
    let mut nodes = vec![false; partition.node_count()];
    let point = partition.point_nodes[0];
    assert_eq!(partition.nearest_xy(point), XY::new(-2.0, -2.0));
    nodes[point.0 as usize] = true;
    let cells = ArealCells {
        faces,
        spans,
        nodes,
    };
    BoundaryContext {
        partition,
        topology,
        edges,
        global,
        cells,
        budget_used,
    }
}

fn forward_source_darts(
    plan: &BoundaryPlan,
    partition: &ExactPartition<'_>,
    topology: &IndexedTopology,
) -> Vec<((f64, f64), (f64, f64))> {
    plan.source_darts
        .iter()
        .map(|&slot| {
            let origin = partition.nearest_xy(NodeId(topology.owners[slot as usize]));
            let target = partition.nearest_xy(NodeId(topology.targets[slot as usize]));
            ((origin.x, origin.y), (target.x, target.y))
        })
        .collect()
}

fn metric_support_for(wkt: &str) -> ExactMetricSupport {
    let shape = crate::io::parse_wkt(wkt).expect("valid metric-support WKT");
    let inventory = ExactSourceInventory::from_shapes(&[&shape]).unwrap();
    let mut budget = ExpansionBudget::new("hausdorff", "metric support");
    let partition = inventory.partition(&mut budget).unwrap();
    inventory
        .metric_support(&partition, super::AreaLocationStrategy::PersistentSlabs)
        .unwrap()
}

fn metric_support_with_boundary_for(wkt: &str) -> ExactMetricSupport {
    let shape = crate::io::parse_wkt(wkt).expect("valid metric-support WKT");
    let inventory = ExactSourceInventory::from_shapes(&[&shape]).unwrap();
    let mut budget = ExpansionBudget::new("maximum_inscribed_circle", "source");
    let partition = inventory.partition(&mut budget).unwrap();
    inventory
        .metric_support_with_boundary(&partition, super::AreaLocationStrategy::PersistentSlabs)
        .unwrap()
}

fn dyadic(value: f64) -> ExactDyadic {
    ExactDyadic::from_f64(value)
}

fn ratio_matches(actual_num: &ExactDyadic, actual_den: &ExactDyadic, num: f64, den: f64) -> bool {
    compare_ratios(actual_num, actual_den, &dyadic(num), &dyadic(den)).unwrap() == Ordering::Equal
}

fn node_at(node: &ExactMetricNode, x: f64, y: f64) -> bool {
    node_at_ratios(node, x, 1.0, y, 1.0)
}

fn node_at_ratios(node: &ExactMetricNode, x_num: f64, x_den: f64, y_num: f64, y_den: f64) -> bool {
    let [actual_x, actual_y, actual_w] = node.homogeneous();
    ratio_matches(actual_x, actual_w, x_num, x_den)
        && ratio_matches(actual_y, actual_w, y_num, y_den)
}

fn span_between(span: &ExactMetricSpan, first: (f64, f64), second: (f64, f64)) -> bool {
    (node_at(&span.start, first.0, first.1) && node_at(&span.end, second.0, second.1))
        || (node_at(&span.start, second.0, second.1) && node_at(&span.end, first.0, first.1))
}

fn source_is(span: &ExactMetricSpan, source: Segment) -> bool {
    PointKey::new(span.source.start) == PointKey::new(source.start)
        && PointKey::new(span.source.end) == PointKey::new(source.end)
}

fn assert_ratio(actual: &[ExactDyadic; 2], num: f64, den: f64) {
    assert!(
        ratio_matches(&actual[0], &actual[1], num, den),
        "expected {num}/{den}, got {actual:?}"
    );
}

#[test]
fn metric_support_unions_retraced_linework_and_drops_covered_points() {
    let support = metric_support_for(
        "GEOMETRYCOLLECTION(\
         LINESTRING (0 0,2 0,4 0),\
         LINESTRING (4 0,1 0,4 0),\
         LINESTRING (4 0,0 0),\
         POINT (2 0),\
         POINT (8 1))",
    );
    assert!(support.faces.is_empty());
    assert_eq!(support.spans.len(), 3);
    assert!(support.boundary_spans.is_none());
    assert!(
        support
            .spans
            .iter()
            .any(|span| span_between(span, (0.0, 0.0), (1.0, 0.0)))
    );
    assert!(
        support
            .spans
            .iter()
            .any(|span| span_between(span, (1.0, 0.0), (2.0, 0.0)))
    );
    assert!(
        support
            .spans
            .iter()
            .any(|span| span_between(span, (2.0, 0.0), (4.0, 0.0)))
    );
    assert_eq!(support.points.len(), 1);
    assert!(support.boundary_points.is_none());
    assert!(node_at(&support.points[0], 8.0, 1.0));
}

#[test]
fn metric_support_keeps_crossing_nodes_and_original_source_parameters_exact() {
    let support = metric_support_for(
        "GEOMETRYCOLLECTION(\
         LINESTRING (0 0,3 1),\
         LINESTRING (1 -1,1 2))",
    );
    let source = seg(0.0, 0.0, 3.0, 1.0);
    let spans: Vec<_> = support
        .spans
        .iter()
        .filter(|span| source_is(span, source))
        .collect();
    assert_eq!(spans.len(), 2);
    let mut crossing_nodes = Vec::new();
    for span in spans {
        let endpoint = if node_at_ratios(&span.start, 1.0, 1.0, 1.0, 3.0) {
            assert_ratio(&span.start_parameter, 1.0, 3.0);
            &span.start
        } else {
            assert!(node_at_ratios(&span.end, 1.0, 1.0, 1.0, 3.0));
            assert_ratio(&span.end_parameter, 1.0, 3.0);
            &span.end
        };
        crossing_nodes.push(endpoint.homogeneous().clone());
    }
    assert_eq!(crossing_nodes.len(), 2);
    assert_eq!(crossing_nodes[0], crossing_nodes[1]);
    assert!(
        support.spans.iter().any(|span| {
            source_is(span, source)
                && ((ratio_matches(&span.start_parameter[0], &span.start_parameter[1], 0.0, 1.0)
                    && ratio_matches(&span.end_parameter[0], &span.end_parameter[1], 1.0, 3.0))
                    || (ratio_matches(
                        &span.start_parameter[0],
                        &span.start_parameter[1],
                        1.0,
                        3.0,
                    ) && ratio_matches(
                        &span.end_parameter[0],
                        &span.end_parameter[1],
                        1.0,
                        1.0,
                    )))
        }),
        "the oblique source must retain both exact parameter windows"
    );
}

#[test]
fn metric_support_groups_filled_hole_walks_and_ignores_external_holes() {
    let support = metric_support_for(
        "POLYGON ((0 0,6 0,6 6,0 6,0 0),\
                  (1 1,3 1,3 3,1 3,1 1),\
                  (10 10,12 10,12 12,10 12,10 10))",
    );
    assert_eq!(support.spans.len(), 8);
    assert_eq!(support.points.len(), 0);
    assert_eq!(support.faces.len(), 1);
    assert_eq!(support.faces[0].len(), 2);
    let mut cycle_lengths: Vec<_> = support.faces[0].iter().map(Vec::len).collect();
    cycle_lengths.sort_unstable();
    assert_eq!(cycle_lengths, [4, 4]);
    assert!(support.spans.iter().all(|span| {
        [&span.start, &span.end]
            .into_iter()
            .all(|node| !node_at(node, 10.0, 10.0) && !node_at(node, 12.0, 12.0))
    }));
    assert!(
        support.faces[0]
            .iter()
            .flatten()
            .all(|node| { !node_at(node, 10.0, 10.0) && !node_at(node, 12.0, 12.0) })
    );
}

#[test]
fn metric_support_handles_bowties_collapsed_rings_and_coincident_cancellation() {
    let bowtie = metric_support_for("POLYGON ((0 0,4 4,0 4,4 0,0 0))");
    assert_eq!(bowtie.spans.len(), 6);
    assert_eq!(bowtie.faces.len(), 2);
    assert!(bowtie.faces.iter().all(|face| face.len() == 1));
    assert!(bowtie.faces.iter().all(|face| face[0].len() == 3));

    let collapsed = metric_support_for("POLYGON ((0 0,4 0,2 0,0 0))");
    assert!(collapsed.points.is_empty());
    assert!(collapsed.faces.is_empty());
    assert_eq!(collapsed.spans.len(), 2);
    assert!(
        collapsed
            .spans
            .iter()
            .any(|span| span_between(span, (0.0, 0.0), (2.0, 0.0)))
    );
    assert!(
        collapsed
            .spans
            .iter()
            .any(|span| span_between(span, (2.0, 0.0), (4.0, 0.0)))
    );

    for wkt in [
        "POLYGON ((0 0,4 0,4 4,0 4,0 0),(0 0,4 0,4 4,0 4,0 0))",
        "POLYGON ((0 0,4 0,2 0,0 0),(0 0,4 0,2 0,0 0))",
    ] {
        let cancelled = metric_support_for(wkt);
        assert!(cancelled.spans.is_empty(), "{wkt}");
        assert!(cancelled.points.is_empty(), "{wkt}");
        assert!(cancelled.faces.is_empty(), "{wkt}");
    }
}

#[test]
fn metric_support_preserves_multipart_union_and_mixed_dimension_points() {
    let multipart = metric_support_for(
        "MULTIPOLYGON (\
         ((0 0,2 0,2 2,0 2,0 0)),\
         ((2 0,4 0,4 2,2 2,2 0)))",
    );
    assert_eq!(multipart.faces.len(), 2);
    assert_eq!(multipart.spans.len(), 7);
    for endpoint in [
        ((0.0, 0.0), (2.0, 0.0)),
        ((2.0, 0.0), (4.0, 0.0)),
        ((4.0, 0.0), (4.0, 2.0)),
        ((4.0, 2.0), (2.0, 2.0)),
        ((2.0, 2.0), (0.0, 2.0)),
        ((0.0, 2.0), (0.0, 0.0)),
    ] {
        assert!(
            multipart
                .spans
                .iter()
                .any(|span| span_between(span, endpoint.0, endpoint.1))
        );
    }

    let mixed = metric_support_for(
        "GEOMETRYCOLLECTION(\
         POLYGON ((0 0,4 0,4 4,0 4,0 0)),\
         POINT (10 1))",
    );
    assert_eq!(mixed.spans.len(), 4);
    assert_eq!(mixed.faces.len(), 1);
    assert_eq!(mixed.faces[0].len(), 1);
    assert_eq!(mixed.points.len(), 1);
    assert!(node_at(&mixed.points[0], 10.0, 1.0));
}

#[test]
fn metric_support_boundary_incidence_excludes_internal_and_residual_items() {
    let adjacent = metric_support_with_boundary_for(
        "MULTIPOLYGON (\
         ((0 0,2 0,2 2,0 2,0 0)),\
         ((2 0,4 0,4 2,2 2,2 0)))",
    );
    let adjacent_boundary = adjacent
        .boundary_spans
        .as_ref()
        .expect("boundary selection");
    assert_eq!(adjacent_boundary.len(), adjacent.spans.len());
    assert_eq!(
        adjacent_boundary
            .iter()
            .filter(|&&selected| selected)
            .count(),
        6
    );
    assert!(
        adjacent
            .spans
            .iter()
            .zip(adjacent_boundary)
            .any(|(span, &selected)| { !selected && span_between(span, (2.0, 0.0), (2.0, 2.0)) })
    );

    let overlapping = metric_support_with_boundary_for(
        "MULTIPOLYGON (\
         ((0 0,4 0,4 4,0 4,0 0)),\
         ((2 0,6 0,6 4,2 4,2 0)))",
    );
    let overlapping_boundary = overlapping
        .boundary_spans
        .as_ref()
        .expect("boundary selection");
    assert!(
        overlapping
            .spans
            .iter()
            .zip(overlapping_boundary)
            .any(|(span, &selected)| { !selected && span_between(span, (2.0, 0.0), (2.0, 4.0)) })
    );
    assert!(
        overlapping
            .spans
            .iter()
            .zip(overlapping_boundary)
            .any(|(span, &selected)| { !selected && span_between(span, (4.0, 0.0), (4.0, 4.0)) })
    );

    let holed = metric_support_with_boundary_for(
        "POLYGON ((0 0,10 0,10 10,0 10,0 0),\
                  (4 4,6 4,6 6,4 6,4 4))",
    );
    let holed_boundary = holed.boundary_spans.as_ref().expect("boundary selection");
    assert!(
        holed
            .spans
            .iter()
            .zip(holed_boundary)
            .all(|(_, &selected)| selected)
    );

    let tangent_hole =
        metric_support_with_boundary_for("POLYGON ((0 0,6 0,6 6,0 6,0 0),(0 3,2 2,2 4,0 3))");
    let tangent_hole_boundary = tangent_hole
        .boundary_spans
        .as_ref()
        .expect("boundary selection");
    assert_eq!(tangent_hole_boundary.len(), tangent_hole.spans.len());
    assert!(tangent_hole_boundary.iter().all(|&selected| selected));
    for edge in [
        ((0.0, 3.0), (2.0, 2.0)),
        ((2.0, 2.0), (2.0, 4.0)),
        ((2.0, 4.0), (0.0, 3.0)),
    ] {
        assert!(
            tangent_hole
                .spans
                .iter()
                .zip(tangent_hole_boundary)
                .any(|(span, &selected)| selected && span_between(span, edge.0, edge.1))
        );
    }

    let residual = metric_support_with_boundary_for(
        "GEOMETRYCOLLECTION(\
         POLYGON ((0 0,4 0,4 4,0 4,0 0)),\
         LINESTRING (1 1,3 1,1 1),\
         POINT (2 2))",
    );
    assert!(!residual.faces.is_empty());
    assert_eq!(residual.points.len(), 1);
    assert!(
        residual
            .boundary_points
            .as_ref()
            .expect("boundary selection")
            .iter()
            .all(|&selected| !selected)
    );
    let residual_boundary = residual
        .boundary_spans
        .as_ref()
        .expect("boundary selection");
    assert_eq!(
        residual_boundary
            .iter()
            .filter(|&&selected| selected)
            .count(),
        4
    );
    assert!(
        residual
            .spans
            .iter()
            .zip(residual_boundary)
            .any(|(span, &selected)| { !selected && span_between(span, (1.0, 1.0), (3.0, 1.0)) })
    );
    let external = metric_support_with_boundary_for(
        "GEOMETRYCOLLECTION(\
         POLYGON ((0 0,4 0,4 4,0 4,0 0)),\
         LINESTRING (10 0,12 0))",
    );
    let external_boundary = external
        .boundary_spans
        .as_ref()
        .expect("boundary selection");
    assert!(
        external
            .spans
            .iter()
            .zip(external_boundary)
            .any(|(span, &selected)| { !selected && span_between(span, (10.0, 0.0), (12.0, 0.0)) })
    );
    let collapsed_inside = metric_support_with_boundary_for(
        "GEOMETRYCOLLECTION(\
         POLYGON ((0 0,4 0,4 4,0 4,0 0)),\
         POLYGON ((1 1,3 1,1 1,1 1)))",
    );
    let collapsed_inside_boundary = collapsed_inside
        .boundary_spans
        .as_ref()
        .expect("boundary selection");
    assert_eq!(
        collapsed_inside_boundary
            .iter()
            .filter(|&&selected| selected)
            .count(),
        4
    );
    assert!(collapsed_inside_boundary.iter().any(|&selected| !selected));

    let collapsed = metric_support_with_boundary_for("POLYGON ((0 0,4 0,2 0,0 0))");
    assert!(collapsed.faces.is_empty());
    assert!(
        collapsed
            .boundary_spans
            .as_ref()
            .expect("boundary selection")
            .iter()
            .all(|&selected| selected)
    );

    let line_support = metric_support_with_boundary_for(
        "GEOMETRYCOLLECTION(LINESTRING (0 0,1 0,2 0), POINT (8 1))",
    );
    assert!(line_support.faces.is_empty());
    assert!(
        line_support
            .boundary_spans
            .as_ref()
            .expect("boundary selection")
            .iter()
            .all(|&selected| selected)
    );
    assert_eq!(line_support.boundary_points, Some(vec![true]));
}

#[test]
fn metric_support_boundary_incidence_scales_without_pairwise_matching() {
    let count = 128;
    let mut wkt = String::from("MULTIPOLYGON (");
    for index in 0..count {
        if index != 0 {
            wkt.push(',');
        }
        write!(
            wkt,
            "(({} 0,{} 0,{} 1,{} 1,{} 0))",
            index,
            index + 1,
            index + 1,
            index,
            index
        )
        .unwrap();
    }
    wkt.push(')');
    let support = metric_support_with_boundary_for(&wkt);
    let boundary_spans = support.boundary_spans.as_ref().expect("boundary selection");
    assert_eq!(boundary_spans.len(), support.spans.len());
    assert_eq!(
        boundary_spans.iter().filter(|&&selected| selected).count(),
        2 * count + 2
    );
    assert_eq!(
        boundary_spans.iter().filter(|&&selected| !selected).count(),
        count - 1
    );
}

#[test]
fn metric_support_returns_boundary_flags_only_when_requested() {
    let wkt = "POLYGON ((0 0,4 0,4 4,0 4,0 0))";
    let ordinary = metric_support_for(wkt);
    assert!(ordinary.boundary_spans.is_none());
    assert!(ordinary.boundary_points.is_none());

    let boundary = metric_support_with_boundary_for(wkt);
    assert_eq!(boundary.boundary_spans, Some(vec![true; 4]));
    assert_eq!(boundary.boundary_points, Some(Vec::new()));
}

fn assert_ring_xy(ring: &Ring, xs: &[f64], ys: &[f64]) {
    assert_eq!(ring.coords().xs(), xs);
    assert_eq!(ring.coords().ys(), ys);
}

fn polygon_with_shell_xy<'a>(polygons: &'a [Shape], xs: &[f64], ys: &[f64]) -> &'a Polygon {
    polygons
        .iter()
        .filter_map(polygon_output)
        .find(|polygon| polygon.shell.coords().xs() == xs && polygon.shell.coords().ys() == ys)
        .expect("polygon with the literal shell is emitted")
}

fn polygon_output(shape: &Shape) -> Option<&Polygon> {
    match shape {
        Shape::Polygon(polygon) => Some(polygon),
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::MultiPolygon(_)
        | Shape::GeometryCollection(_)
        | Shape::Empty(..) => None,
    }
}

fn line_string_output(shape: &Shape) -> Option<&LineSeq> {
    match shape {
        Shape::LineString(line) => Some(line),
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::MultiLineString(_)
        | Shape::Polygon(_)
        | Shape::MultiPolygon(_)
        | Shape::GeometryCollection(_)
        | Shape::Empty(..) => None,
    }
}

#[test]
fn linework_selection_keeps_minimal_faces_and_disconnected_support() {
    let crossing = [
        seg(0.0, 0.0, 2.0, 2.0),
        seg(0.0, 2.0, 2.0, 0.0),
        seg(0.0, 0.0, 2.0, 0.0),
        seg(2.0, 0.0, 2.0, 2.0),
        seg(2.0, 2.0, 0.0, 2.0),
        seg(0.0, 2.0, 0.0, 0.0),
    ];
    let inventory = ExactSourceInventory::from_segments(&crossing).unwrap();
    let mut budget = ExpansionBudget::new("polygonize", "partition");
    let mut partition = inventory.partition(&mut budget).unwrap();
    let faces = inventory
        .linework_polygons(
            &mut partition,
            false,
            "test",
            CoordinateAxes::XY,
            false,
            &crate::geometry::SourceArcContext::cartesian(),
        )
        .unwrap();
    assert_eq!(faces.len(), 4);
    for (xs, ys) in [
        (&[0.0, 2.0, 1.0, 0.0][..], &[0.0, 0.0, 1.0, 0.0][..]),
        (&[1.0, 2.0, 2.0, 1.0][..], &[1.0, 0.0, 2.0, 1.0][..]),
        (&[0.0, 1.0, 2.0, 0.0][..], &[2.0, 1.0, 2.0, 2.0][..]),
        (&[0.0, 1.0, 0.0, 0.0][..], &[0.0, 1.0, 2.0, 0.0][..]),
    ] {
        let polygon = polygon_with_shell_xy(&faces, xs, ys);
        assert!(
            polygon.holes.is_empty(),
            "each crossing-square cell is a simple triangle"
        );
    }

    let disconnected = [
        seg(0.0, 0.0, 2.0, 2.0),
        seg(2.0, 2.0, 0.0, 2.0),
        seg(0.0, 2.0, 2.0, 0.0),
        seg(2.0, 0.0, 0.0, 0.0),
        seg(5.0, 0.0, 6.0, 0.0),
        seg(6.0, 0.0, 6.0, 1.0),
        seg(6.0, 1.0, 5.0, 1.0),
        seg(5.0, 1.0, 5.0, 0.0),
    ];
    let inventory = ExactSourceInventory::from_segments(&disconnected).unwrap();
    let mut budget = ExpansionBudget::new("build_area", "partition");
    let mut partition = inventory.partition(&mut budget).unwrap();
    let parts = inventory
        .linework_polygons(
            &mut partition,
            true,
            "test",
            CoordinateAxes::XY,
            false,
            &crate::geometry::SourceArcContext::cartesian(),
        )
        .unwrap();
    assert_eq!(parts.len(), 3);
    assert_eq!(parts.iter().map(Shape::area).sum::<f64>(), 3.0);
}

#[test]
fn polygonize_emission_preserves_hosts_across_disconnected_cycles() {
    let segments = [
        seg(0.0, 0.0, 8.0, 0.0),
        seg(8.0, 0.0, 8.0, 8.0),
        seg(8.0, 8.0, 0.0, 8.0),
        seg(0.0, 8.0, 0.0, 0.0),
        seg(2.0, 2.0, 4.0, 2.0),
        seg(4.0, 2.0, 4.0, 4.0),
        seg(4.0, 4.0, 2.0, 4.0),
        seg(2.0, 4.0, 2.0, 2.0),
    ];
    let inventory = ExactSourceInventory::from_segments(&segments).unwrap();
    let mut budget = ExpansionBudget::new("polygonize", "partition");
    let mut partition = inventory.partition(&mut budget).unwrap();
    let polygons = inventory
        .linework_polygons(
            &mut partition,
            false,
            "polygonize",
            CoordinateAxes::XY,
            false,
            &crate::geometry::SourceArcContext::cartesian(),
        )
        .unwrap();
    assert_eq!(polygons.len(), 2);
    let annulus = polygons
        .first()
        .and_then(polygon_output)
        .expect("the annulus face is emitted");
    let inner_face = polygons
        .get(1)
        .and_then(polygon_output)
        .expect("the inner face is emitted");
    assert_ring_xy(&annulus.shell, &[0.0, 8.0, 8.0, 0.0, 0.0], &[
        0.0, 0.0, 8.0, 8.0, 0.0,
    ]);
    assert_eq!(annulus.holes.len(), 1);
    let hole = annulus.holes.first().expect("the annulus has its hole");
    assert_ring_xy(hole, &[2.0, 2.0, 4.0, 4.0, 2.0], &[2.0, 4.0, 4.0, 2.0, 2.0]);
    assert_ring_xy(&inner_face.shell, &[2.0, 4.0, 4.0, 2.0, 2.0], &[
        2.0, 2.0, 4.0, 4.0, 2.0,
    ]);
    assert!(inner_face.holes.is_empty());

    let (topology, edges) = partition.local_topology().unwrap();
    let global = partition.global_faces(&topology, &edges).unwrap();
    let emission = emission::polygonize_emission(&partition, &topology, &edges, &global).unwrap();
    let emissions = [&emission];

    let nearest = embedding::Embedding::nearest(&partition);
    let nearest_segments: Vec<_> = emission
        .items
        .iter()
        .map(|&item| embedding::Embedding::segment_at(nearest.coordinates(), item))
        .collect();
    let nearest_segment_views = [nearest_segments.as_slice()];
    let mut nearest_witnesses = WitnessLog::default();
    assert!(
        embedding::certify(
            &partition,
            &nearest,
            &emissions,
            &nearest_segment_views,
            &mut nearest_witnesses,
        )
        .unwrap()
        .is_continue()
    );
    assert!(nearest_witnesses.0.is_empty());

    let mut coordinates = partition.points.rounded.clone();
    for (from, to) in [
        (XY::new(2.0, 2.0), XY::new(10.0, 10.0)),
        (XY::new(4.0, 2.0), XY::new(12.0, 10.0)),
        (XY::new(4.0, 4.0), XY::new(12.0, 12.0)),
        (XY::new(2.0, 4.0), XY::new(10.0, 12.0)),
    ] {
        let node = partition.stored_node(from).unwrap();
        coordinates[node.0 as usize] = to;
    }
    let moved = embedding::Embedding::from_owned(coordinates);
    let moved_segments: Vec<_> = emission
        .items
        .iter()
        .map(|&item| embedding::Embedding::segment_at(moved.coordinates(), item))
        .collect();
    let moved_segment_views = [moved_segments.as_slice()];
    let mut moved_witnesses = WitnessLog::default();
    assert!(
        embedding::certify(
            &partition,
            &moved,
            &emissions,
            &moved_segment_views,
            &mut moved_witnesses,
        )
        .unwrap()
        .is_continue()
    );
    assert!(matches!(moved_witnesses.0.as_slice(), [
        embedding::Witness::Face { .. }
    ]));
}

#[test]
fn polygonize_full_certifies_diagnostic_lines_with_polygon_faces() {
    let segments = [
        seg(0.0, 0.0, 4.0, 0.0),
        seg(4.0, 0.0, 4.0, 4.0),
        seg(4.0, 4.0, 0.0, 4.0),
        seg(0.0, 4.0, 0.0, 0.0),
        seg(-1.0, 2.0, 5.0, 2.0),
    ];
    let inventory = ExactSourceInventory::from_segments(&segments).unwrap();
    let mut budget = ExpansionBudget::new("polygonize_full", "partition");
    let mut partition = inventory.partition(&mut budget).unwrap();
    partition.kept_nodes = KeptNodes::All;
    let (topology, edges) = partition.local_topology().unwrap();
    let global = partition.global_faces(&topology, &edges).unwrap();
    let diagnostic_spans: Vec<_> = (0..partition.spans.len())
        .map(|span| {
            partition
                .owners(span)
                .iter()
                .any(|&(source, _)| source == 4)
        })
        .collect();
    let emission = emission::polygonize_full_emission(
        &partition,
        &topology,
        &edges,
        &global,
        &diagnostic_spans,
    )
    .unwrap();
    assert!(emission.areal_chords > 0);
    assert!(emission.chords > emission.areal_chords);
    assert_eq!(emission.fan_spans.len(), emission.chords);
    assert_eq!(emission.faces.as_ref().map(Vec::len), Some(emission.chords));
    let area_nodes: Vec<_> = emission.items[..emission.areal_chords]
        .iter()
        .flatten()
        .copied()
        .collect();
    assert!(
        emission.items[emission.areal_chords..]
            .iter()
            .flatten()
            .any(|node| area_nodes.contains(node)),
        "diagnostic dangles meet the polygon boundary at emitted nodes"
    );

    let nearest = embedding::Embedding::nearest(&partition);
    let segments: Vec<_> = emission
        .items
        .iter()
        .map(|&item| embedding::Embedding::segment_at(nearest.coordinates(), item))
        .collect();
    let emissions = [&emission];
    let segment_views = [segments.as_slice()];
    let mut witnesses = WitnessLog::default();
    assert!(
        embedding::certify(
            &partition,
            &nearest,
            &emissions,
            &segment_views,
            &mut witnesses,
        )
        .unwrap()
        .is_continue()
    );
    assert!(witnesses.0.is_empty());

    let full = inventory
        .polygonize_full(
            &mut partition,
            CoordinateAxes::XY,
            &[CoordinateAxes::XY],
            false,
            &crate::geometry::SourceArcContext::cartesian(),
        )
        .unwrap();
    assert_eq!(full.polygons.len(), 2);
    let lower = polygon_output(full.polygons.first().expect("the lower face is emitted"))
        .expect("polygonize_full emits polygon faces");
    let upper = polygon_output(full.polygons.get(1).expect("the upper face is emitted"))
        .expect("polygonize_full emits polygon faces");
    assert_ring_xy(&lower.shell, &[0.0, 4.0, 4.0, 0.0, 0.0], &[
        0.0, 0.0, 2.0, 2.0, 0.0,
    ]);
    assert_ring_xy(&upper.shell, &[0.0, 4.0, 4.0, 0.0, 0.0], &[
        2.0, 2.0, 4.0, 4.0, 2.0,
    ]);
    assert_eq!(full.dangles.len(), 1);
    let row = full
        .dangles
        .first()
        .expect("the source has one diagnostic row");
    assert_eq!(row.len(), 2);
    let right = line_string_output(row.first().expect("the right dangle is emitted"))
        .expect("diagnostic dangles are line strings");
    let left = line_string_output(row.get(1).expect("the left dangle is emitted"))
        .expect("diagnostic dangles are line strings");
    assert_eq!(right.as_coords().xs(), &[4.0, 5.0]);
    assert_eq!(right.as_coords().ys(), &[2.0, 2.0]);
    assert_eq!(left.as_coords().xs(), &[-1.0, 0.0]);
    assert_eq!(left.as_coords().ys(), &[2.0, 2.0]);
}

#[test]
fn polygonize_faces_attach_closed_holes_to_their_literal_hosts() {
    let segments = [
        seg(0.0, 0.0, 8.0, 0.0),
        seg(8.0, 0.0, 8.0, 8.0),
        seg(8.0, 8.0, 0.0, 8.0),
        seg(0.0, 8.0, 0.0, 0.0),
        seg(2.0, 2.0, 4.0, 2.0),
        seg(4.0, 2.0, 4.0, 4.0),
        seg(4.0, 4.0, 2.0, 4.0),
        seg(2.0, 4.0, 2.0, 2.0),
        seg(12.0, 0.0, 20.0, 0.0),
        seg(20.0, 0.0, 20.0, 8.0),
        seg(20.0, 8.0, 12.0, 8.0),
        seg(12.0, 8.0, 12.0, 0.0),
        seg(14.0, 2.0, 17.0, 2.0),
        seg(17.0, 2.0, 17.0, 5.0),
        seg(17.0, 5.0, 14.0, 5.0),
        seg(14.0, 5.0, 14.0, 2.0),
    ];
    let inventory = ExactSourceInventory::from_segments(&segments).unwrap();
    let mut budget = ExpansionBudget::new("polygonize", "two closed hole hosts");
    let mut partition = inventory.partition(&mut budget).unwrap();
    let polygons = inventory
        .linework_polygons(
            &mut partition,
            false,
            "polygonize",
            CoordinateAxes::XY,
            false,
            &crate::geometry::SourceArcContext::cartesian(),
        )
        .unwrap();
    assert_eq!(polygons.len(), 4);

    let outer_a = polygon_with_shell_xy(&polygons, &[0.0, 8.0, 8.0, 0.0, 0.0], &[
        0.0, 0.0, 8.0, 8.0, 0.0,
    ]);
    assert_eq!(outer_a.holes.len(), 1);
    let hole_a = outer_a
        .holes
        .first()
        .expect("the first annulus has its hole");
    assert_ring_xy(hole_a, &[2.0, 2.0, 4.0, 4.0, 2.0], &[
        2.0, 4.0, 4.0, 2.0, 2.0,
    ]);

    let inner_a = polygon_with_shell_xy(&polygons, &[2.0, 4.0, 4.0, 2.0, 2.0], &[
        2.0, 2.0, 4.0, 4.0, 2.0,
    ]);
    assert!(inner_a.holes.is_empty());

    let outer_b = polygon_with_shell_xy(&polygons, &[12.0, 20.0, 20.0, 12.0, 12.0], &[
        0.0, 0.0, 8.0, 8.0, 0.0,
    ]);
    assert_eq!(outer_b.holes.len(), 1);
    let hole_b = outer_b
        .holes
        .first()
        .expect("the second annulus has its hole");
    assert_ring_xy(hole_b, &[14.0, 14.0, 17.0, 17.0, 14.0], &[
        2.0, 5.0, 5.0, 2.0, 2.0,
    ]);

    let inner_b = polygon_with_shell_xy(&polygons, &[14.0, 17.0, 17.0, 14.0, 14.0], &[
        2.0, 2.0, 5.0, 5.0, 2.0,
    ]);
    assert!(inner_b.holes.is_empty());
}

#[test]
fn polygonize_faces_keep_point_touching_and_tangent_hole_cycles_separate() {
    let tangent_hole = [
        seg(0.0, 0.0, 6.0, 0.0),
        seg(6.0, 0.0, 6.0, 6.0),
        seg(6.0, 6.0, 0.0, 6.0),
        seg(0.0, 6.0, 0.0, 0.0),
        seg(0.0, 3.0, 2.0, 2.0),
        seg(2.0, 2.0, 2.0, 4.0),
        seg(2.0, 4.0, 0.0, 3.0),
    ];
    let inventory = ExactSourceInventory::from_segments(&tangent_hole).unwrap();
    let mut budget = ExpansionBudget::new("polygonize", "tangent hole cycles");
    let mut partition = inventory.partition(&mut budget).unwrap();
    let polygons = inventory
        .linework_polygons(
            &mut partition,
            false,
            "polygonize",
            CoordinateAxes::XY,
            false,
            &crate::geometry::SourceArcContext::cartesian(),
        )
        .unwrap();
    assert_eq!(polygons.len(), 2);
    let outer = polygons
        .iter()
        .filter_map(polygon_output)
        .find(|polygon| polygon.shell.coords().xs().contains(&6.0))
        .expect("the outer bounded face is emitted");
    assert_ring_xy(&outer.shell, &[0.0, 6.0, 6.0, 0.0, 0.0, 0.0], &[
        0.0, 0.0, 6.0, 6.0, 3.0, 0.0,
    ]);
    assert_eq!(outer.holes.len(), 1);
    let hole = outer
        .holes
        .first()
        .expect("the outer face has its tangent hole");
    assert_ring_xy(hole, &[0.0, 2.0, 2.0, 0.0], &[3.0, 4.0, 2.0, 3.0]);
    let triangle = polygons
        .iter()
        .filter_map(polygon_output)
        .find(|polygon| polygon.shell.coords().xs().iter().all(|&x| x <= 2.0))
        .expect("the bounded tangent triangle is emitted independently");
    assert_ring_xy(&triangle.shell, &[0.0, 2.0, 2.0, 0.0], &[
        3.0, 2.0, 4.0, 3.0,
    ]);
    assert!(triangle.holes.is_empty());

    let point_touch = [
        seg(0.0, 0.0, 2.0, 0.0),
        seg(2.0, 0.0, 2.0, 2.0),
        seg(2.0, 2.0, 0.0, 2.0),
        seg(0.0, 2.0, 0.0, 0.0),
        seg(2.0, 2.0, 4.0, 2.0),
        seg(4.0, 2.0, 4.0, 4.0),
        seg(4.0, 4.0, 2.0, 4.0),
        seg(2.0, 4.0, 2.0, 2.0),
    ];
    let inventory = ExactSourceInventory::from_segments(&point_touch).unwrap();
    let mut budget = ExpansionBudget::new("polygonize", "point-touching faces");
    let mut partition = inventory.partition(&mut budget).unwrap();
    let polygons = inventory
        .linework_polygons(
            &mut partition,
            false,
            "polygonize",
            CoordinateAxes::XY,
            false,
            &crate::geometry::SourceArcContext::cartesian(),
        )
        .unwrap();
    assert_eq!(polygons.len(), 2);
    let first = polygons
        .iter()
        .filter_map(polygon_output)
        .find(|polygon| polygon.shell.coords().xs().contains(&0.0))
        .expect("the left point-touching face is emitted");
    assert_ring_xy(&first.shell, &[0.0, 2.0, 2.0, 0.0, 0.0], &[
        0.0, 0.0, 2.0, 2.0, 0.0,
    ]);
    assert!(first.holes.is_empty());
    let second = polygons
        .iter()
        .filter_map(polygon_output)
        .find(|polygon| polygon.shell.coords().xs().contains(&4.0))
        .expect("the right point-touching face is emitted separately");
    assert_ring_xy(&second.shell, &[2.0, 4.0, 4.0, 2.0, 2.0], &[
        2.0, 2.0, 4.0, 4.0, 2.0,
    ]);
    assert!(second.holes.is_empty());
}

#[test]
fn polygonize_keeps_collinear_partition_nodes_in_serialized_and_certified_boundaries() {
    let segments = [
        seg(0.0, 0.0, 1.0, 0.0),
        seg(1.0, 0.0, 2.0, 0.0),
        seg(2.0, 0.0, 2.0, 2.0),
        seg(2.0, 2.0, 0.0, 2.0),
        seg(0.0, 2.0, 0.0, 0.0),
    ];
    let inventory = ExactSourceInventory::from_segments(&segments).unwrap();
    let mut budget = ExpansionBudget::new("polygonize", "collinear partition node");
    let mut partition = inventory.partition(&mut budget).unwrap();
    let polygons = inventory
        .linework_polygons(
            &mut partition,
            false,
            "polygonize",
            CoordinateAxes::XY,
            false,
            &crate::geometry::SourceArcContext::cartesian(),
        )
        .unwrap();
    assert_eq!(polygons.len(), 1);
    let polygon = polygons
        .first()
        .and_then(polygon_output)
        .expect("the closed linework emits a face");
    assert_ring_xy(&polygon.shell, &[0.0, 1.0, 2.0, 2.0, 0.0, 0.0], &[
        0.0, 0.0, 0.0, 2.0, 2.0, 0.0,
    ]);
    let node = partition
        .stored_node(XY::new(1.0, 0.0))
        .expect("the collinear source endpoint is a partition node");
    let (topology, edges) = partition.local_topology().unwrap();
    let global = partition.global_faces(&topology, &edges).unwrap();
    let emission = emission::polygonize_emission(&partition, &topology, &edges, &global).unwrap();
    assert!(
        emission.items.iter().any(|item| item.contains(&node)),
        "the embedding certificate includes each serialized boundary node"
    );
}

#[test]
fn polygonize_faces_sort_global_faces_after_dangling_walks() {
    let segments = [
        seg(1.0, 1.0, 2.0, 1.0),
        seg(6.0, 0.0, 7.0, 0.0),
        seg(7.0, 0.0, 7.0, 1.0),
        seg(7.0, 1.0, 6.0, 1.0),
        seg(6.0, 1.0, 6.0, 0.0),
        seg(2.0, 1.0, 2.0, 0.0),
        seg(0.0, 0.0, 4.0, 0.0),
        seg(4.0, 0.0, 4.0, 4.0),
        seg(4.0, 4.0, 0.0, 4.0),
        seg(0.0, 4.0, 0.0, 0.0),
    ];
    let inventory = ExactSourceInventory::from_segments(&segments).unwrap();
    let mut budget = ExpansionBudget::new("polygonize", "source-order face labels");
    let mut partition = inventory.partition(&mut budget).unwrap();
    let (topology, edges) = partition.local_topology().unwrap();
    let global = partition.global_faces(&topology, &edges).unwrap();
    let walks = global.boundary_walks(&topology, |face, other| face != 0 && face != other);
    let mut shell_faces = Vec::new();
    for walk in &walks {
        let first = *walk.first().expect("a boundary walk is nonempty");
        let local_face = *topology
            .face_of
            .get(first as usize)
            .expect("boundary dart belongs to the topology");
        let face = *global
            .walk_faces
            .get(local_face as usize)
            .expect("local face belongs to the global face map");
        if emission::walk_is_shell(
            &partition,
            &topology,
            walk.len(),
            |step| {
                *walk
                    .get(step)
                    .expect("shell classifier step belongs to the boundary walk")
            },
            |step| {
                *walk
                    .get(step)
                    .expect("shell classifier step belongs to the boundary walk")
            },
        )
        .unwrap()
        {
            shell_faces.push(face);
        }
    }
    assert_eq!(shell_faces, [2, 1], "walk discovery precedes face sorting");

    let polygons = inventory
        .linework_polygons(
            &mut partition,
            false,
            "polygonize",
            CoordinateAxes::XY,
            false,
            &crate::geometry::SourceArcContext::cartesian(),
        )
        .unwrap();
    assert_eq!(polygons.len(), 2);
    let a = polygons
        .first()
        .and_then(polygon_output)
        .expect("the first square face is emitted");
    let b = polygons
        .get(1)
        .and_then(polygon_output)
        .expect("the second square face is emitted");
    assert_ring_xy(&a.shell, &[0.0, 2.0, 4.0, 4.0, 0.0, 0.0], &[
        0.0, 0.0, 0.0, 4.0, 4.0, 0.0,
    ]);
    assert!(a.holes.is_empty());
    assert_ring_xy(&b.shell, &[6.0, 7.0, 7.0, 6.0, 6.0], &[
        0.0, 0.0, 1.0, 1.0, 0.0,
    ]);
    assert!(b.holes.is_empty());
}

#[test]
fn polygonize_faces_serialize_the_supplied_certified_node_table() {
    let segments = [
        seg(-2.0, 0.0, 2.0, 0.0),
        seg(-1.0, -2.0, 1.0, 4.0),
        seg(-1.0, 2.0, 2.0, -1.0),
    ];
    let inventory = ExactSourceInventory::from_segments(&segments).unwrap();
    let mut budget = ExpansionBudget::new("polygonize", "certified constructed vertex");
    let partition = inventory.partition(&mut budget).unwrap();
    let (topology, edges) = partition.local_topology().unwrap();
    let global = partition.global_faces(&topology, &edges).unwrap();
    let emission = emission::polygonize_emission(&partition, &topology, &edges, &global).unwrap();
    let nearest = embedding::Embedding::nearest(&partition);
    let crossing = partition
        .crossings()
        .into_iter()
        .find(|&node| partition.nearest_xy(node).x.to_bits() == 0xBFD5_5555_5555_5555)
        .expect("the left crossing rounds to negative one third");
    let nearest_xy = partition.nearest_xy(crossing);
    assert_eq!(nearest_xy.y.to_bits(), 0_u64);
    let candidate_xy = XY::new(f64::from_bits(0xBFD5_5555_5555_5554), nearest_xy.y);
    let mut coordinates = nearest.coordinates().to_vec();
    *coordinates
        .get_mut(crossing.0 as usize)
        .expect("the constructed node belongs to the embedding table") = candidate_xy;
    let supplied = embedding::Embedding::from_owned(coordinates);
    for point in [
        XY::new(-2.0, 0.0),
        XY::new(2.0, 0.0),
        XY::new(-1.0, -2.0),
        XY::new(1.0, 4.0),
        XY::new(-1.0, 2.0),
        XY::new(2.0, -1.0),
    ] {
        let node = partition
            .stored_node(point)
            .expect("source vertex is stored");
        assert_eq!(
            supplied.coordinates().get(node.0 as usize),
            Some(&point),
            "source vertices stay pinned at {point:?}"
        );
    }
    let emitted_segments: Vec<_> = emission
        .items
        .iter()
        .map(|&item| embedding::Embedding::segment_at(supplied.coordinates(), item))
        .collect();
    let segment_views = [emitted_segments.as_slice()];
    let mut witnesses = WitnessLog::default();
    assert!(
        embedding::certify(
            &partition,
            &supplied,
            &[&emission],
            &segment_views,
            &mut witnesses,
        )
        .unwrap()
        .is_continue(),
        "the complete supplied coordinate table passes the production certificate"
    );
    assert!(witnesses.0.is_empty());

    let groups =
        emission::polygonize_node_groups(&partition, &topology, &global, &supplied).unwrap();
    let polygons = inventory
        .polygon_shapes_from_node_groups(
            &partition,
            &supplied,
            groups,
            CoordinateAxes::XY,
            false,
            "polygonize",
            &crate::geometry::SourceArcContext::cartesian(),
            None,
        )
        .unwrap();
    assert_eq!(polygons.len(), 1);
    let polygon = polygons
        .first()
        .and_then(polygon_output)
        .expect("the bounded triangle is emitted");
    assert_eq!(
        polygon
            .shell
            .coords()
            .xs()
            .iter()
            .map(|x| x.to_bits())
            .collect::<Vec<_>>(),
        [
            0xBFD5_5555_5555_5554,
            0x3FF0_0000_0000_0000,
            0,
            0xBFD5_5555_5555_5554
        ]
    );
    assert_eq!(polygon.shell.coords().ys(), &[0.0, 0.0, 1.0, 0.0]);
    assert!(polygon.holes.is_empty());
}

#[test]
fn polygonize_returns_no_polygons_without_a_bounded_face() {
    for segments in [
        &[][..],
        &[seg(0.0, 0.0, 0.0, 0.0)][..],
        &[seg(0.0, 0.0, 2.0, 0.0)][..],
    ] {
        let inventory = ExactSourceInventory::from_segments(segments).unwrap();
        let mut budget = ExpansionBudget::new("polygonize", "linework without a bounded face");
        let mut partition = inventory.partition(&mut budget).unwrap();
        assert!(
            inventory
                .linework_polygons(
                    &mut partition,
                    false,
                    "polygonize",
                    CoordinateAxes::XY,
                    false,
                    &crate::geometry::SourceArcContext::cartesian(),
                )
                .unwrap()
                .is_empty()
        );
    }
}

#[test]
fn generated_budget_charges_only_net_canonical_growth() {
    use crate::geometry::generated_budget::GENERATED_ITEM_LIMIT;

    let crossing = [seg(0.0, 0.0, 2.0, 2.0), seg(0.0, 2.0, 2.0, 0.0)];
    let cases = [
        (vec![seg(0.0, 0.0, 1.0, 0.0)], 0),
        (vec![seg(0.0, 0.0, 1.0, 0.0); 2], 0),
        (vec![seg(0.0, 0.0, 1.0, 0.0), seg(1.0, 0.0, 2.0, 0.0)], 0),
        (vec![seg(1.0, 1.0, 1.0, 1.0)], 0),
        (crossing.to_vec(), 3),
        (vec![seg(0.0, 0.0, 4.0, 0.0), seg(1.0, 0.0, 3.0, 0.0)], 1),
        (vec![seg(0.0, 0.0, 2.0, 0.0), seg(1.0, 0.0, 1.0, 0.0)], 1),
        (vec![crossing[0], crossing[1], seg(1.0, 1.0, 1.0, 1.0)], 2),
        (
            vec![
                crossing[0],
                crossing[1],
                seg(10.0, 0.0, 14.0, 0.0),
                seg(10.0, 0.0, 12.0, 0.0),
                seg(12.0, 0.0, 14.0, 0.0),
            ],
            2,
        ),
    ];
    for (segments, growth) in cases {
        let case = run(&segments);
        let mut budget = ExpansionBudget::new("exact_partition", "items");
        budget.add(GENERATED_ITEM_LIMIT - growth).unwrap();
        let partition = partition_unique_segments::<true>(
            &case.unique,
            &case.provenance,
            &case.isolated,
            &mut budget,
        )
        .unwrap();
        assert_eq!(budget.used(), GENERATED_ITEM_LIMIT);
        assert!(partition.node_count() > 0);
        if growth > 0 {
            let mut short = ExpansionBudget::new("exact_partition", "items");
            short.add(GENERATED_ITEM_LIMIT - growth + 1).unwrap();
            let Err(error) = partition_unique_segments::<true>(
                &case.unique,
                &case.provenance,
                &case.isolated,
                &mut short,
            ) else {
                panic!("one fewer generated item must reject the partition");
            };
            assert!(matches!(
                error.kind(),
                crate::error::ErrorKind::Geometry(
                    GeometryErrorKind::GeneratedOutputTooLarge { .. }
                )
            ));
        }
    }
}

#[test]
fn zero_length_unique_sources_do_not_credit_atom_growth() {
    use crate::geometry::generated_budget::GENERATED_ITEM_LIMIT;

    let (unique, provenance) = dedup_undirected_segments_sourced(vec![
        seg(0.0, 0.0, 2.0, 2.0),
        seg(0.0, 2.0, 2.0, 0.0),
        seg(10.0, 10.0, 10.0, 10.0),
    ])
    .unwrap();
    for ordinates in [false, true] {
        let mut budget = ExpansionBudget::new("exact_partition", "items");
        budget.add(GENERATED_ITEM_LIMIT - 3).unwrap();
        let partition = if ordinates {
            partition_unique_segments::<true>(&unique, &provenance, &[], &mut budget)
        } else {
            partition_unique_segments::<false>(&unique, &provenance, &[], &mut budget)
        }
        .unwrap();
        assert_eq!(partition.node_count(), 6);
        assert_eq!(partition.spans.len(), 4);
        assert_eq!(partition.isolated.len(), 1);
        assert_eq!(budget.used(), GENERATED_ITEM_LIMIT);
    }
}

#[test]
fn intermediate_partition_can_run_after_the_output_budget_is_exhausted() {
    use crate::geometry::generated_budget::GENERATED_ITEM_LIMIT;

    let crossing = [seg(0.0, 0.0, 2.0, 2.0), seg(0.0, 2.0, 2.0, 0.0)];
    let inventory = ExactSourceInventory::from_segments(&crossing).unwrap();
    let mut exhausted = ExpansionBudget::new("voronoi", "published output");
    exhausted.add(GENERATED_ITEM_LIMIT - 1).unwrap();
    let Err(error) = inventory.partition(&mut exhausted) else {
        panic!("partition growth exceeded the remaining output budget");
    };
    assert!(matches!(
        error.kind(),
        crate::error::ErrorKind::Geometry(GeometryErrorKind::GeneratedOutputTooLarge { .. })
    ));

    let partition = inventory.partition_without_budget().unwrap();
    assert_eq!(partition.node_count(), 5);
    assert_eq!(partition.spans.len(), 4);
}

fn crossing_node(partition: &ExactPartition<'_>) -> NodeId {
    let mut crossings =
        (0..partition.node_count() as u32).filter(|&node| !partition.is_stored(NodeId(node)));
    let node = crossings.next().expect("expected a crossing node");
    assert_eq!(crossings.next(), None, "expected one crossing node");
    NodeId(node)
}

/// The node's exact point is `(x[0] / x[1], y[0] / y[1])`.
fn assert_exact_at(
    partition: &ExactPartition<'_>,
    node: NodeId,
    x: [&ExactDyadic; 2],
    y: [&ExactDyadic; 2],
) {
    let point = partition.exact(node).unwrap();
    let [px, py, pw] = &*point;
    assert_eq!(compare_ratios(px, pw, x[0], x[1]).unwrap(), Ordering::Equal);
    assert_eq!(compare_ratios(py, pw, y[0], y[1]).unwrap(), Ordering::Equal);
}

fn constructed_count(partition: &ExactPartition) -> usize {
    partition.crossings().len()
}

fn component_count(partition: &ExactPartition) -> usize {
    let n = partition.node_count();
    if n == 0 {
        return 0;
    }
    let mut parent: Vec<u32> = (0..n as u32).collect();
    let find = |parent: &mut [u32], mut node: u32| -> u32 {
        while parent[node as usize] != node {
            parent[node as usize] = parent[parent[node as usize] as usize];
            node = parent[node as usize];
        }
        node
    };
    for span in &partition.spans {
        let a = find(&mut parent, span.start.0);
        let b = find(&mut parent, span.end.0);
        if a != b {
            parent[b as usize] = a;
        }
    }
    let mut roots = Vec::new();
    for node in 0..n as u32 {
        let root = find(&mut parent, node);
        if !roots.contains(&root) {
            roots.push(root);
        }
    }
    roots.len()
}

#[test]
fn public_entry_matches_inner_on_empty() {
    let mut budget = ExpansionBudget::new("exact_partition", "items");
    let (unique, provenance) = dedup_undirected_segments_sourced(Vec::<Segment>::new()).unwrap();
    let partition =
        partition_unique_segments::<true>(&unique, &provenance, &[], &mut budget).unwrap();
    assert_eq!(partition.node_count(), 0);
}

#[test]
fn generated_t_has_seven_nodes_five_spans_two_components() {
    let rn_sixth = 1.0_f64 / 6.0;
    let case = run(&[
        seg(0.0, 0.0, 3.0, 1.0),
        seg(1.0, -1.0, 1.0, 1.0),
        seg(0.5, 0.0, 0.5, rn_sixth),
    ]);
    let partition = case.partition();
    assert_eq!(partition.unique.len(), 3);
    assert_eq!(partition.node_count(), 7);
    assert_eq!(partition.spans.len(), 5);
    assert_eq!(constructed_count(&partition), 1);
    assert_eq!(component_count(&partition), 2);
    let c_end = partition.stored_node(XY::new(0.5, rn_sixth)).unwrap();
    let on_a = partition
        .spans
        .iter()
        .any(|span| span.unique == 0 && (span.start == c_end || span.end == c_end));
    assert!(!on_a, "C's endpoint must not node onto A");
    let (one, three) = (dyadic(1.0), dyadic(3.0));
    assert_exact_at(&partition, crossing_node(&partition), [&one, &one], [
        &one, &three,
    ]);
}

#[test]
fn endpoint_near_miss_five_nodes_four_spans() {
    let nd = 1.0_f64.next_down();
    let case = run(&[seg(0.0, 0.0, 1.0, 1.0), seg(1.0, nd, 0.0, 2.0)]);
    let partition = case.partition();
    assert_eq!(partition.node_count(), 5);
    assert_eq!(partition.spans.len(), 4);
    assert_eq!(constructed_count(&partition), 1);
    let q_num = ExactDyadic::from_f64(1.0);
    let two = ExactDyadic::from_f64(2.0);
    let p54 = {
        let mut p = ExactDyadic::from_f64(1.0);
        for _ in 0..54 {
            p = p.try_product(&two).unwrap();
        }
        p
    };
    let den = p54.clone().try_add(q_num).unwrap();
    // The crossing lies on y = x at 2^54 / (2^54 + 1).
    assert_exact_at(&partition, crossing_node(&partition), [&p54, &den], [
        &p54, &den,
    ]);
}

#[test]
fn concurrent_three_sources_one_constructed_node() {
    let case = run(&[
        seg(0.0, 0.0, 3.0, 1.0),
        seg(1.0, -1.0, 1.0, 1.0),
        seg(0.0, 1.0, 3.0, -1.0),
    ]);
    let partition = case.partition();
    assert_eq!(constructed_count(&partition), 1);
    let (one, three) = (dyadic(1.0), dyadic(3.0));
    assert_exact_at(&partition, crossing_node(&partition), [&one, &one], [
        &one, &three,
    ]);
}

#[test]
fn overlap_reversed_duplicate_keeps_reverse_owner() {
    let case = run(&[
        seg(0.0, 0.0, 4.0, 0.0),
        seg(1.0, 0.0, 3.0, 0.0),
        seg(2.0, -1.0, 2.0, 1.0),
        seg(4.0, 0.0, 0.0, 0.0),
    ]);
    let partition = case.partition();
    let reversed = partition.spans.iter().enumerate().any(|(index, _)| {
        partition
            .owners(index)
            .iter()
            .any(|&(ordinal, reversed)| ordinal == 3 && reversed)
    });
    assert!(reversed, "reversed duplicate must keep the reverse flag");
    let overlap = partition.spans.iter().enumerate().filter(|(index, _)| {
        let owners = partition.owners(*index);
        owners.iter().any(|(ordinal, _)| *ordinal == 0)
            && owners.iter().any(|(ordinal, _)| *ordinal == 1)
    });
    assert!(
        overlap.count() > 0,
        "overlap of A and B must be one aggregated atom"
    );
}

#[test]
fn overlap_aggregate_owners_are_one_atom() {
    let case = run(&[seg(0.0, 0.0, 4.0, 0.0), seg(1.0, 0.0, 3.0, 0.0)]);
    let partition = case.partition();
    let shared: Vec<_> = partition
        .spans
        .iter()
        .enumerate()
        .filter(|(index, _)| {
            let owners = partition.owners(*index);
            owners.iter().any(|(ordinal, _)| *ordinal == 0)
                && owners.iter().any(|(ordinal, _)| *ordinal == 1)
        })
        .collect();
    assert_eq!(shared.len(), 1);
    assert_eq!(partition.owners(shared[0].0).len(), 2);
}

#[test]
fn sourced_dedup_unique_segments_keep_identity_owner_rows() {
    let (unique, provenance) = dedup_undirected_segments_sourced([
        seg(0.0, 0.0, 1.0, 0.0),
        seg(0.0, 1.0, 1.0, 1.0),
        seg(0.0, 2.0, 1.0, 2.0),
    ])
    .unwrap();

    assert_eq!(
        unique
            .iter()
            .map(|segment| (segment.start, segment.end))
            .collect::<Vec<_>>(),
        vec![
            (XY::new(0.0, 0.0), XY::new(1.0, 0.0)),
            (XY::new(0.0, 1.0), XY::new(1.0, 1.0)),
            (XY::new(0.0, 2.0), XY::new(1.0, 2.0)),
        ]
    );
    assert_eq!(provenance.owners(0), &[(0, false)]);
    assert_eq!(provenance.owners(1), &[(1, false)]);
    assert_eq!(provenance.owners(2), &[(2, false)]);
}

#[test]
fn sourced_dedup_first_late_duplicate_keeps_every_ordered_owner() {
    let (unique, provenance) = dedup_undirected_segments_sourced([
        seg(0.0, 0.0, 4.0, 0.0),
        seg(0.0, 2.0, 4.0, 2.0),
        seg(0.0, 4.0, 4.0, 4.0),
        seg(4.0, 2.0, 0.0, 2.0),
        seg(0.0, 0.0, 4.0, 0.0),
        seg(0.0, 6.0, 4.0, 6.0),
        seg(0.0, 2.0, 4.0, 2.0),
        seg(4.0, 0.0, 0.0, 0.0),
    ])
    .unwrap();

    assert_eq!(
        unique
            .iter()
            .map(|segment| (segment.start, segment.end))
            .collect::<Vec<_>>(),
        vec![
            (XY::new(0.0, 0.0), XY::new(4.0, 0.0)),
            (XY::new(0.0, 2.0), XY::new(4.0, 2.0)),
            (XY::new(0.0, 4.0), XY::new(4.0, 4.0)),
            (XY::new(0.0, 6.0), XY::new(4.0, 6.0)),
        ]
    );
    assert_eq!(provenance.owners(0), &[(0, false), (4, false), (7, true)]);
    assert_eq!(provenance.owners(1), &[(1, false), (3, true), (6, false)]);
    assert_eq!(provenance.owners(2), &[(2, false)]);
    assert_eq!(provenance.owners(3), &[(5, false)]);
}

#[test]
fn sourced_dedup_preserves_signed_zero_and_degenerate_owner_direction() {
    let (unique, provenance) = dedup_undirected_segments_sourced([
        seg(-0.0, 0.0, 2.0, 0.0),
        seg(2.0, 0.0, 0.0, 0.0),
        seg(0.0, 1.0, -0.0, 1.0),
        seg(-0.0, 1.0, 0.0, 1.0),
    ])
    .unwrap();

    assert_eq!(unique.len(), 2);
    assert_eq!(unique[0].start.x.to_bits(), (-0.0_f64).to_bits());
    assert_eq!(unique[0].end.x.to_bits(), 2.0_f64.to_bits());
    assert_eq!(unique[1].start.x.to_bits(), 0.0_f64.to_bits());
    assert_eq!(unique[1].end.x.to_bits(), (-0.0_f64).to_bits());
    assert_eq!(provenance.owners(0), &[(0, false), (1, true)]);
    assert_eq!(provenance.owners(1), &[(2, false), (3, false)]);
}

#[test]
fn segment_provenance_preserves_empty_direct_csr_rows_and_iteration() {
    let default = SegmentProvenance::default();
    let (empty_unique, empty) = dedup_undirected_segments_sourced(Vec::<Segment>::new()).unwrap();
    assert!(empty_unique.is_empty());
    for provenance in [&default, &empty] {
        assert_eq!(provenance.len(), 0);
        assert_eq!(provenance.iter().count(), 0);
    }

    let provenance =
        SegmentProvenance::from_csr(vec![0, 2, 2, 3], vec![(7, false), (2, true), (9, false)]);
    assert_eq!(provenance.len(), 3);
    assert_eq!(provenance.owners(0), &[(7, false), (2, true)]);
    assert_eq!(&provenance[0], &[(7, false), (2, true)]);
    assert!(provenance[1].is_empty());
    assert_eq!(provenance.owners(2), &[(9, false)]);

    let rows = provenance.iter().collect::<Vec<_>>();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], &[(7, false), (2, true)]);
    assert!(rows[1].is_empty());
    assert_eq!(rows[2], &[(9, false)]);
}

#[test]
#[should_panic(expected = "index out of bounds")]
fn segment_provenance_index_out_of_range_keeps_slice_panic() {
    let provenance = SegmentProvenance::from_csr(vec![0, 0], Vec::new());
    let _ = provenance[1];
}

#[test]
fn degenerate_point_is_isolated_node() {
    let case = run(&[seg(0.0, 0.0, 2.0, 0.0), seg(1.0, 1.0, 1.0, 1.0)]);
    let partition = case.partition();
    assert_eq!(partition.isolated.len(), 1);
    assert_eq!(
        partition.stored_node(XY::new(1.0, 1.0)),
        Some(partition.isolated[0])
    );
}

#[test]
fn explicit_point_occurrences_retain_incident_and_isolated_ownership() {
    let case = run(&[
        seg(0.0, 0.0, 4.0, 0.0),
        seg(2.0, 0.0, 2.0, 0.0),
        seg(0.0, 0.0, 0.0, 0.0),
        seg(2.0, 0.0, 2.0, 0.0),
        seg(7.0, 3.0, 7.0, 3.0),
    ]);
    let partition = case.partition();
    let middle = partition.stored_node(XY::new(2.0, 0.0)).unwrap();
    let endpoint = partition.stored_node(XY::new(0.0, 0.0)).unwrap();
    let isolated = partition.stored_node(XY::new(7.0, 3.0)).unwrap();
    assert_eq!(partition.point_nodes, [middle, endpoint, middle, isolated]);
    assert_eq!(partition.isolated, [isolated]);
    assert_eq!(partition.spans.len(), 2);
}

#[test]
fn isolated_nodes_preserve_point_sources_and_occurrence_order_in_both_lanes() {
    let (unique, provenance) = dedup_undirected_segments_sourced([
        seg(0.0, 0.0, 4.0, 0.0),
        seg(8.0, 1.0, 10.0, 1.0),
        seg(20.0, 2.0, 20.0, 2.0),
    ])
    .unwrap();
    let isolated = [
        XY::new(4.0, 0.0),
        XY::new(30.0, 3.0),
        XY::new(40.0, 4.0),
        XY::new(30.0, 3.0),
    ];
    for ordinates in [false, true] {
        for budgeted in [false, true] {
            let mut budget = ExpansionBudget::new("exact_partition", "items");
            let partition = match (ordinates, budgeted) {
                (false, false) => {
                    partition_unique_segments_unbudgeted::<false>(&unique, &provenance, &isolated)
                },
                (true, false) => {
                    partition_unique_segments_unbudgeted::<true>(&unique, &provenance, &isolated)
                },
                (false, true) => {
                    partition_unique_segments::<false>(&unique, &provenance, &isolated, &mut budget)
                },
                (true, true) => {
                    partition_unique_segments::<true>(&unique, &provenance, &isolated, &mut budget)
                },
            }
            .unwrap();
            assert_eq!(partition.node_count(), 7);
            assert_eq!(partition.spans.len(), 2);
            assert_eq!(partition.isolated, [NodeId(4), NodeId(5), NodeId(6)]);
            assert_eq!(partition.point_nodes, [
                NodeId(1),
                NodeId(5),
                NodeId(6),
                NodeId(5)
            ]);
            assert_eq!(partition.point_sources, [(NodeId(4), 2)]);
            assert_eq!(budget.used(), 0);
        }
    }
}

#[test]
fn constructed_crossing_splits_every_overlapping_source_in_both_orders() {
    let mut sources = [
        seg(0.0, 0.0, 4.0, 0.0),
        seg(1.0, 0.0, 3.0, 0.0),
        seg(0.0, -1.0, 4.0, 2.0),
        seg(4.0, 0.0, 0.0, 0.0),
        seg(10.0, 10.0, 11.0, 11.0),
        seg(11.0, 11.0, 12.0, 12.0),
        seg(20.0, 10.0, 21.0, 11.0),
    ];
    for reverse in [false, true] {
        if reverse {
            sources.reverse();
        }
        let case = run(&sources);
        let partition = case.partition();
        let constructed: Vec<_> = partition.crossings().into_iter().collect();
        assert_eq!(constructed.len(), 1);
        let crossing = constructed[0];
        let departures: Vec<_> = [
            XY::new(3.0, 0.0),
            XY::new(4.0, 2.0),
            XY::new(1.0, 0.0),
            XY::new(0.0, -1.0),
        ]
        .into_iter()
        .map(|point| {
            let target = partition.stored_node(point).unwrap();
            partition
                .spans
                .iter()
                .position(|span| {
                    (span.start == crossing && span.end == target)
                        || (span.end == crossing && span.start == target)
                })
                .unwrap()
        })
        .collect();
        for (position, &left) in departures.iter().enumerate() {
            for &right in &departures[position + 1..] {
                assert_eq!(
                    partition.compare_departures(crossing, left, right),
                    Ordering::Less
                );
                assert_eq!(
                    partition.compare_departures(crossing, right, left),
                    Ordering::Greater
                );
            }
        }
        let exact = partition.exact(crossing).unwrap();
        let [x, y, w] = &*exact;
        let (topology, edges) = partition.local_topology().unwrap();
        let row = topology.starts[crossing.0 as usize] as usize
            ..topology.starts[crossing.0 as usize + 1] as usize;
        assert_eq!(row.len(), 4);
        for (&edge, &expected) in edges[row].iter().zip(&departures) {
            assert_eq!(edge as usize, expected);
        }
        let four = ExactDyadic::from_f64(4.0);
        let three = ExactDyadic::from_f64(3.0);
        let zero = ExactDyadic::from_f64(0.0);
        let one = ExactDyadic::from_f64(1.0);
        assert_eq!(
            compare_ratios(x, w, &four, &three).unwrap(),
            Ordering::Equal
        );
        assert_eq!(compare_ratios(y, w, &zero, &one).unwrap(), Ordering::Equal);
        let left = partition.stored_node(XY::new(1.0, 0.0)).unwrap();
        let right = partition.stored_node(XY::new(3.0, 0.0)).unwrap();
        assert_eq!(
            partition.compare_nodes(left, crossing).unwrap(),
            Ordering::Less
        );
        assert_eq!(
            partition.compare_nodes(right, crossing).unwrap(),
            Ordering::Greater
        );
        let mut owner_counts: Vec<_> = partition
            .spans
            .iter()
            .enumerate()
            .filter(|(_, span)| span.start == crossing || span.end == crossing)
            .map(|(index, _)| partition.owners(index).len())
            .collect();
        owner_counts.sort_unstable();
        assert_eq!(owner_counts, [1, 1, 3, 3]);
        assert_eq!(partition.spans.len(), 9);
    }
}

#[test]
fn local_walks_keep_nested_components_and_bridge_sides_separate() {
    let case = run(&[
        seg(8.0, 8.0, 9.0, 9.0),
        seg(4.0, 2.0, 3.0, 4.0),
        seg(0.0, 0.0, 8.0, 0.0),
        seg(8.0, 0.0, 8.0, 8.0),
        seg(8.0, 8.0, 0.0, 8.0),
        seg(0.0, 8.0, 0.0, 0.0),
        seg(2.0, 2.0, 4.0, 2.0),
        seg(3.0, 4.0, 2.0, 2.0),
        seg(6.0, 6.0, 6.0, 6.0),
    ]);
    let partition = case.partition();
    let (topology, edges) = partition.local_topology().unwrap();
    let outer = partition.stored_node(XY::new(0.0, 0.0)).unwrap().0 as usize;
    let inner = partition.stored_node(XY::new(2.0, 2.0)).unwrap().0 as usize;
    let isolated = partition.stored_node(XY::new(6.0, 6.0)).unwrap().0 as usize;
    let minima = partition.component_minima(&topology).unwrap();
    assert_eq!(minima.len(), 3);
    assert_eq!(
        minima[topology.component_of[outer] as usize],
        NodeId(outer as u32)
    );
    assert_eq!(
        minima[topology.component_of[inner] as usize],
        NodeId(inner as u32)
    );
    assert_eq!(
        minima[topology.component_of[isolated] as usize],
        NodeId(isolated as u32)
    );
    assert_ne!(topology.component_of[outer], topology.component_of[inner]);
    assert_ne!(
        topology.component_of[outer],
        topology.component_of[isolated]
    );
    assert_ne!(
        topology.component_of[inner],
        topology.component_of[isolated]
    );
    assert_eq!(topology.starts[isolated], topology.starts[isolated + 1]);
    let mut lengths: Vec<_> = topology
        .face_starts
        .array_windows::<2>()
        .map(|[start, end]| end - start)
        .collect();
    lengths.sort_unstable();
    assert_eq!(lengths, [3, 3, 4, 6]);
    let tip = partition.stored_node(XY::new(9.0, 9.0)).unwrap().0 as usize;
    let slot = topology.starts[tip] as usize;
    let twin = topology.twins[slot] as usize;
    assert_eq!(topology.face_of[slot], topology.face_of[twin]);
    assert_eq!(edges[slot], edges[twin]);
    let outer_walk = partition
        .exterior_walk(&topology, &edges, NodeId(outer as u32))
        .unwrap()
        .unwrap();
    assert_eq!(outer_walk, topology.face_of[slot]);
    let inner_walk = partition
        .exterior_walk(&topology, &edges, NodeId(inner as u32))
        .unwrap()
        .unwrap();
    assert_ne!(inner_walk, outer_walk);
    assert_eq!(
        partition
            .exterior_walk(&topology, &edges, NodeId(isolated as u32))
            .unwrap(),
        None
    );
    assert_eq!(partition.left_contact(NodeId(outer as u32)), None);
    let wall_start = partition.stored_node(XY::new(0.0, 8.0)).unwrap();
    let wall = partition
        .spans
        .iter()
        .position(|span| {
            (span.start == wall_start && span.end == NodeId(outer as u32))
                || (span.end == wall_start && span.start == NodeId(outer as u32))
        })
        .unwrap();
    for query in [inner, isolated] {
        assert_eq!(partition.left_contact(NodeId(query as u32)), Some(wall));
    }
    let host = partition.contact_walk(&topology, &edges, wall);
    assert_ne!(host, outer_walk);
    let wall_slot = (topology.starts[outer] as usize..topology.starts[outer + 1] as usize)
        .find(|&slot| edges[slot] == wall as u32)
        .unwrap();
    assert_eq!(host, topology.face_of[topology.twins[wall_slot] as usize]);
    let faces = partition.global_faces(&topology, &edges).unwrap();
    assert_eq!(faces.count, 3);
    assert_eq!(faces.walk_faces[outer_walk as usize], 0);
    assert_eq!(
        faces.walk_faces[inner_walk as usize],
        faces.walk_faces[host as usize]
    );
    assert_ne!(faces.walk_faces[host as usize], 0);
    assert_eq!(
        faces.component_hosts[topology.component_of[isolated] as usize],
        faces.walk_faces[host as usize]
    );
}

#[test]
fn exterior_walk_at_minimum_with_only_lower_departures() {
    let case = run(&[
        seg(0.0, 0.0, 2.0, -3.0),
        seg(2.0, -3.0, 3.0, -1.0),
        seg(3.0, -1.0, 0.0, 0.0),
    ]);
    let partition = case.partition();
    let (topology, edges) = partition.local_topology().unwrap();
    let root = partition.stored_node(XY::new(0.0, 0.0)).unwrap();
    let upper_target = partition.stored_node(XY::new(3.0, -1.0)).unwrap();
    let slot = (topology.starts[root.0 as usize] as usize
        ..topology.starts[root.0 as usize + 1] as usize)
        .find(|&slot| topology.targets[slot] == upper_target.0)
        .unwrap();
    let exterior = partition
        .exterior_walk(&topology, &edges, root)
        .unwrap()
        .unwrap();
    assert_eq!(exterior, topology.face_of[slot]);
    assert_ne!(exterior, topology.face_of[topology.twins[slot] as usize]);
}

/// Host faces for a shell with a grid of holes come from one sweep: four
/// times the holes costs about four times the orientation tests, not
/// sixteen.
#[test]
fn hole_hosts_scale_with_the_hole_count() {
    let orientations = |side: u32| {
        let extent = 10.0 * f64::from(side) + 10.0;
        let mut segments = vec![
            seg(0.0, 0.0, extent, 0.0),
            seg(extent, 0.0, extent, extent),
            seg(extent, extent, 0.0, extent),
            seg(0.0, extent, 0.0, 0.0),
        ];
        for row in 0..side {
            for column in 0..side {
                let (x, y) = (10.0 * f64::from(column) + 5.0, 10.0 * f64::from(row) + 5.0);
                segments.extend([
                    seg(x, y, x, y + 2.0),
                    seg(x, y + 2.0, x + 2.0, y + 2.0),
                    seg(x + 2.0, y + 2.0, x + 2.0, y),
                    seg(x + 2.0, y, x, y),
                ]);
            }
        }
        let case = run(&segments);
        let partition = case.partition();
        let (topology, edges) = partition.local_topology().unwrap();
        reset_partition_builds();
        let faces = partition.global_faces(&topology, &edges).unwrap();
        assert_eq!(faces.count as usize, 2 + (side * side) as usize);
        partition_metrics().ray_orientations
    };
    let (small, large) = (orientations(16), orientations(32));
    assert!(large < 6 * small, "{small} -> {large}");
}

/// A leftward ray includes a lower endpoint, excludes an upper endpoint, and
/// never crosses a horizontal span. The empty-query check uses nonempty edges.
#[test]
fn rays_handle_empty_queries_and_query_height_boundaries() {
    for (segments, query_point, contact) in [
        (
            vec![seg(0.0, 0.0, 2.0, 0.0), seg(4.0, 0.0, 4.0, 0.0)],
            XY::new(4.0, 0.0),
            None,
        ),
        (
            vec![
                seg(0.0, 2.0, 2.0, 0.0),
                seg(2.0, 0.0, 3.0, 2.0),
                seg(4.0, 0.0, 4.0, 0.0),
            ],
            XY::new(4.0, 0.0),
            Some((XY::new(2.0, 0.0), XY::new(3.0, 2.0))),
        ),
        (
            vec![seg(0.0, 0.0, 2.0, 2.0), seg(4.0, 2.0, 4.0, 2.0)],
            XY::new(4.0, 2.0),
            None,
        ),
    ] {
        let case = run(&segments);
        let partition = case.partition();
        let (topology, edges) = partition.local_topology().unwrap();
        let query = partition.stored_node(query_point).unwrap();
        let expected = contact.map(|(start, end)| {
            let (start, end) = (
                partition.stored_node(start).unwrap(),
                partition.stored_node(end).unwrap(),
            );
            partition
                .spans
                .iter()
                .position(|span| span.start == start && span.end == end)
                .unwrap()
        });
        let ray_edge = |index: usize| {
            let span = partition
                .spans
                .get(index)
                .expect("ray edge index comes from the partition spans");
            [span.start.0, span.end.0]
        };
        assert!(
            shoot_left(&partition.points, partition.spans.len(), ray_edge, &[])
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            shoot_left(&partition.points, partition.spans.len(), ray_edge, &[
                query.0
            ],)
            .unwrap(),
            [expected]
        );
        let faces = partition.global_faces(&topology, &edges).unwrap();
        assert_eq!(
            faces.component_hosts[topology.component_of[query.0 as usize] as usize],
            0
        );
    }
}

#[test]
fn ray_status_matches_vector_at_crossing_height_and_preserves_query_order() {
    let case = run(&[
        seg(0.0, 0.0, 4.0, 4.0),
        seg(4.0, 4.0, 0.0, 4.0),
        seg(0.0, 4.0, 4.0, 0.0),
        seg(4.0, 0.0, 0.0, 0.0),
        seg(6.0, 2.0, 6.0, 2.0),
    ]);
    let partition = case.partition();
    let origin = partition.stored_node(XY::new(0.0, 0.0)).unwrap();
    let isolated = partition.stored_node(XY::new(6.0, 2.0)).unwrap();
    let (topology, _) = partition.local_topology().unwrap();
    let minima = partition.component_minima(&topology).unwrap();
    assert_eq!(minima.len(), 2);
    assert!(minima.contains(&origin));
    assert!(minima.contains(&isolated));
    let crossing = partition
        .crossings()
        .into_iter()
        .next()
        .expect("crossing creates one exact node");
    let crossing_point = partition
        .points
        .rounded
        .get(crossing.0 as usize)
        .copied()
        .expect("crossing node is in the point arena");
    assert_eq!(crossing_point, XY::new(2.0, 2.0));
    let top_right = partition.stored_node(XY::new(4.0, 4.0)).unwrap();
    let expected = partition
        .spans
        .iter()
        .position(|span| {
            (span.start == crossing && span.end == top_right)
                || (span.start == top_right && span.end == crossing)
        })
        .unwrap();

    // Both queries are component minima. The first shares its y with the
    // crossing event; the second verifies results retain caller query order.
    let queries = [isolated.0, origin.0];
    let edge = |index: usize| {
        let span = partition
            .spans
            .get(index)
            .expect("edge index comes from the sweep input");
        [span.start.0, span.end.0]
    };
    let tree = shoot_left(&partition.points, partition.spans.len(), edge, &queries).unwrap();
    let vector = super::rays::shoot_left_vector_reference(
        &partition.points,
        partition.spans.len(),
        edge,
        &queries,
    )
    .unwrap();
    assert_eq!(tree, vector);
    assert_eq!(tree, [Some(expected), None]);
}

#[test]
fn ray_sweep_keeps_crossings_with_equal_rounded_heights() {
    let one = 1.0_f64;
    let segments = [
        seg(0.0, 0.0, 0.0, 2.0),
        seg(-1.0, one.next_down(), 1.0, one),
        seg(-1.0, one, 1.0, one.next_up()),
    ];
    let mut points = construction::PointArena::with_capacity(&segments, 3);
    let below = points.push_crossing([0, 1]).unwrap();
    let above = points.push_crossing([0, 2]).unwrap();
    let query = points.push_stored(XY::new(2.0, 1.0)).unwrap();

    assert_eq!(RayPoints::rounded(&points, below).y, 1.0);
    assert_eq!(RayPoints::rounded(&points, above).y, 1.0);
    assert_eq!(RayPoints::rounded(&points, query).y, 1.0);
    let (lower, upper) = if RayPoints::compare_axis(&points, below, above, Axis::Y)
        .unwrap()
        .is_lt()
    {
        (below, above)
    } else {
        (above, below)
    };
    assert_eq!(
        RayPoints::compare_axis(&points, lower, query, Axis::Y).unwrap(),
        Ordering::Less
    );
    assert_eq!(
        RayPoints::compare_axis(&points, query, upper, Axis::Y).unwrap(),
        Ordering::Less
    );

    assert_eq!(
        shoot_left(&points, 1, |_| [lower, upper], &[query]).unwrap(),
        [Some(0)]
    );
}

#[test]
fn vertex_visibility_links_disconnected_exteriors_to_infinity() {
    let case = run(&[
        seg(0.0, 2.0, 2.0, 0.0),
        seg(2.0, 0.0, 3.0, 2.0),
        seg(3.0, 2.0, 0.0, 2.0),
        seg(4.0, 0.0, 6.0, 0.0),
        seg(6.0, 0.0, 5.0, 1.0),
        seg(5.0, 1.0, 4.0, 0.0),
        seg(8.0, 0.0, 8.0, 0.0),
    ]);
    let partition = case.partition();
    let (topology, edges) = partition.local_topology().unwrap();
    let root = partition.stored_node(XY::new(4.0, 0.0)).unwrap();
    let (valley, rim) = (
        partition.stored_node(XY::new(2.0, 0.0)).unwrap(),
        partition.stored_node(XY::new(3.0, 2.0)).unwrap(),
    );
    let contact = partition
        .spans
        .iter()
        .position(|span| {
            (span.start == valley && span.end == rim) || (span.start == rim && span.end == valley)
        })
        .unwrap();
    assert_eq!(partition.left_contact(root), Some(contact));
    let faces = partition.global_faces(&topology, &edges).unwrap();
    assert_eq!(faces.count, 3);
    assert_eq!(faces.component_hosts, [0, 0, 0]);
    let exterior = partition
        .exterior_walk(&topology, &edges, root)
        .unwrap()
        .unwrap();
    assert_eq!(faces.walk_faces[exterior as usize], 0);
    let host = partition.contact_walk(&topology, &edges, contact);
    assert_eq!(faces.walk_faces[host as usize], 0);
    let mut windings = Vec::new();
    partition
        .visit_face_windings(
            &topology,
            &edges,
            &faces,
            &[Some(0), Some(0), Some(0), Some(1), Some(1), Some(1), None],
            2,
            |_, winding| {
                windings.push(*winding.first_chunk::<2>().expect("two rings are labelled"));
                Ok(())
            },
        )
        .unwrap();
    windings.sort_unstable();
    assert_eq!(windings, [[0, 0], [0, 1], [1, 0]]);
}

#[test]
fn face_windings_keep_nested_reversed_ring_multiplicity() {
    let case = run(&[
        seg(0.0, 0.0, 8.0, 0.0),
        seg(8.0, 0.0, 8.0, 8.0),
        seg(8.0, 8.0, 0.0, 8.0),
        seg(0.0, 8.0, 0.0, 0.0),
        seg(2.0, 2.0, 3.0, 4.0),
        seg(3.0, 4.0, 4.0, 2.0),
        seg(4.0, 2.0, 2.0, 2.0),
        seg(2.0, 2.0, 3.0, 4.0),
        seg(3.0, 4.0, 4.0, 2.0),
        seg(4.0, 2.0, 2.0, 2.0),
    ]);
    let partition = case.partition();
    let (topology, edges) = partition.local_topology().unwrap();
    let faces = partition.global_faces(&topology, &edges).unwrap();
    let mut windings = Vec::new();
    partition
        .visit_face_windings(
            &topology,
            &edges,
            &faces,
            &[
                Some(0),
                Some(0),
                Some(0),
                Some(0),
                Some(1),
                Some(1),
                Some(1),
                Some(1),
                Some(1),
                Some(1),
            ],
            2,
            |_, winding| {
                windings.push(*winding.first_chunk::<2>().expect("two rings are labelled"));
                Ok(())
            },
        )
        .unwrap();
    windings.sort_unstable();
    assert_eq!(windings, [[0, 0], [1, -2], [1, 0]]);
}

#[test]
fn selected_boundaries_keep_hole_direction_and_host_face() {
    let case = run(&[
        seg(0.0, 0.0, 6.0, 0.0),
        seg(6.0, 0.0, 6.0, 6.0),
        seg(6.0, 6.0, 0.0, 6.0),
        seg(0.0, 6.0, 0.0, 0.0),
        seg(2.0, 2.0, 4.0, 2.0),
        seg(4.0, 2.0, 4.0, 4.0),
        seg(4.0, 4.0, 2.0, 4.0),
        seg(2.0, 4.0, 2.0, 2.0),
    ]);
    let partition = case.partition();
    let (topology, edges) = partition.local_topology().unwrap();
    let faces = partition.global_faces(&topology, &edges).unwrap();
    assert_eq!(faces.count, 3);
    let inner = partition.stored_node(XY::new(2.0, 2.0)).unwrap();
    let host = faces.component_hosts[topology.component_of[inner.0 as usize] as usize];
    let mut occupied = vec![false; 3];
    occupied[host as usize] = true;
    let plan = faces
        .boundary_plan(&Vec::new(), &topology, &edges, &partition, &occupied)
        .unwrap();
    assert_eq!(plan.face_quotient, [0, 1, 2]);
    assert_eq!(
        plan.walks[0]
            .iter()
            .map(|chord| chord.sides)
            .collect::<Vec<_>>(),
        vec![[host, 0]; 4]
    );
    assert_eq!(plan.walks[1].len(), 4);
    assert_eq!(plan.polygon_walks(&partition, &topology).unwrap(), [vec![
        0, 1
    ]]);
    let (_, mut displaced) = certify_plan(&plan, &partition, &topology, &edges).unwrap();
    assert_eq!(displaced.len(), 8);
    for segment in &mut displaced[4..] {
        segment.start.x += 20.0;
        segment.end.x += 20.0;
    }
    let items = plan.emission(&topology, &edges).items;
    check_plan_fans(&plan, &partition, &topology, &edges, &displaced).unwrap();
    let error =
        check_plan_faces(&plan, &partition, &topology, &edges, &items, &displaced).unwrap_err();
    assert!(matches!(
        error.kind(),
        crate::error::ErrorKind::Geometry(GeometryErrorKind::ExactGlobalFace { .. })
    ));
    for sides in plan.walks[1].iter().map(|chord| chord.sides) {
        assert_eq!(sides[0], host);
        assert_ne!(sides[1], 0);
        assert_ne!(sides[1], host);
    }
    let walks = plan.walks;
    assert_eq!(walks.len(), 2);
    for (walk, expected) in walks
        .iter()
        .zip([[(0.0, 0.0), (6.0, 0.0), (6.0, 6.0), (0.0, 6.0)], [
            (2.0, 2.0),
            (2.0, 4.0),
            (4.0, 4.0),
            (4.0, 2.0),
        ]])
    {
        let actual: Vec<_> = walk
            .iter()
            .map(|chord| {
                let slot = plan.source_darts[chord.sources.start];
                assert_eq!(
                    faces.walk_faces[topology.face_of[slot as usize] as usize],
                    host
                );
                let node = NodeId(topology.owners[slot as usize]);
                assert!(partition.is_stored(node), "square corners are stored");
                let point = partition.nearest_xy(node);
                (point.x, point.y)
            })
            .collect();
        assert_eq!(actual, expected);
    }
    let empty = faces
        .boundary_plan(&Vec::new(), &topology, &edges, &partition, &[false; 3])
        .unwrap();
    assert!(empty.walks.is_empty());
    assert!(empty.source_darts.is_empty());
    assert!(
        certify_plan(&empty, &partition, &topology, &edges)
            .unwrap()
            .1
            .is_empty()
    );
    assert_eq!(empty.face_quotient, [0, 0, 0]);
}

#[test]
fn selected_boundaries_split_a_hole_touching_its_shell() {
    let case = run(&[
        seg(0.0, 0.0, 6.0, 0.0),
        seg(6.0, 0.0, 6.0, 6.0),
        seg(6.0, 6.0, 0.0, 6.0),
        seg(0.0, 6.0, 0.0, 0.0),
        seg(0.0, 3.0, 2.0, 2.0),
        seg(2.0, 2.0, 2.0, 4.0),
        seg(2.0, 4.0, 0.0, 3.0),
    ]);
    let partition = case.partition();
    let (topology, edges) = partition.local_topology().unwrap();
    let faces = partition.global_faces(&topology, &edges).unwrap();
    let origin = partition.stored_node(XY::new(0.0, 0.0)).unwrap();
    let east = partition.stored_node(XY::new(6.0, 0.0)).unwrap();
    let bottom = (topology.starts[origin.0 as usize]..topology.starts[origin.0 as usize + 1])
        .find(|&slot| topology.targets[slot as usize] == east.0)
        .unwrap();
    let host = faces.walk_faces[topology.face_of[bottom as usize] as usize];
    let mut occupied = vec![false; faces.count as usize];
    occupied[host as usize] = true;
    let plan = faces
        .boundary_plan(&Vec::new(), &topology, &edges, &partition, &occupied)
        .unwrap();
    let mut lengths: Vec<_> = plan.walks.iter().map(Vec::len).collect();
    lengths.sort_unstable();
    assert_eq!(lengths, [3, 5]);
    let polygons = plan.polygon_walks(&partition, &topology).unwrap();
    assert_eq!(polygons.len(), 1);
    assert_eq!(
        polygons[0]
            .iter()
            .map(|&ring| plan.walks[ring].len())
            .collect::<Vec<_>>(),
        [5, 3]
    );
    let contact = partition.stored_node(XY::new(0.0, 3.0)).unwrap();
    for walk in &plan.walks {
        assert_eq!(
            walk.iter()
                .filter(|chord| {
                    topology.owners[plan.source_darts[chord.sources.start] as usize] == contact.0
                })
                .count(),
            1
        );
    }
    assert_eq!(
        certify_plan(&plan, &partition, &topology, &edges)
            .unwrap()
            .1
            .len(),
        8
    );
}

#[test]
fn selected_polygon_walks_classify_constructed_extrema() {
    for sources in [
        [
            seg(-2.0, 0.0, 2.0, 0.0),
            seg(-1.0, -2.0, 1.0, 4.0),
            seg(-1.0, 2.0, 2.0, -1.0),
        ],
        [
            seg(-4.0, 4.0, 4.0, -4.0),
            seg(-4.0, 9.0, 4.0, -7.0),
            seg(-4.0, 11.0, 4.0, -13.0),
        ],
    ] {
        let case = run(&sources);
        let partition = case.partition();
        let (topology, edges) = partition.local_topology().unwrap();
        let faces = partition.global_faces(&topology, &edges).unwrap();
        let occupied: Vec<_> = (0..faces.count).map(|face| face != 0).collect();
        let plan = faces
            .boundary_plan(&Vec::new(), &topology, &edges, &partition, &occupied)
            .unwrap();
        let polygons = plan.polygon_walks(&partition, &topology).unwrap();
        assert_eq!(polygons.len(), 1);
        assert_eq!(polygons[0].len(), 1);
        assert_eq!(plan.walks[polygons[0][0]].len(), 3);
        for chord in &plan.walks[polygons[0][0]] {
            let node = NodeId(topology.owners[plan.source_darts[chord.sources.start] as usize]);
            assert!(!partition.is_stored(node));
        }
        assert_eq!(
            certify_plan(&plan, &partition, &topology, &edges)
                .unwrap()
                .1
                .len(),
            3
        );
    }
}

#[test]
fn certified_parts_serialize_polygon_line_and_isolated_point() {
    let case = run(&[
        seg(0.0, 0.0, 4.0, 0.0),
        seg(4.0, 0.0, 4.0, 4.0),
        seg(4.0, 4.0, 0.0, 4.0),
        seg(0.0, 4.0, 0.0, 0.0),
        seg(6.0, 1.0, 8.0, 1.0),
        seg(10.0, 3.0, 10.0, 3.0),
    ]);
    let partition = case.partition();
    let (topology, edges) = partition.local_topology().unwrap();
    let faces = partition.global_faces(&topology, &edges).unwrap();
    let cells = ArealCells {
        faces: (0..faces.count).map(|face| face != 0).collect(),
        spans: vec![true; partition.spans.len()],
        nodes: vec![true; partition.node_count()],
    };
    let plan = cells
        .emission_plan(&partition, &topology, &edges, &faces)
        .unwrap();
    let (points, lines, polygons) = plan
        .emit_xy_parts(&partition, &topology, &edges, "test")
        .unwrap();
    assert_eq!(points.iter().map(|point| point.xy()).collect::<Vec<_>>(), [
        XY::new(10.0, 3.0)
    ]);
    assert_eq!(lines.len(), 1);
    let mut endpoints = lines[0].xs().to_vec();
    endpoints.sort_unstable_by(f64::total_cmp);
    assert_eq!(endpoints, [6.0, 8.0]);
    assert_eq!(lines[0].ys(), [1.0, 1.0]);
    assert_eq!(polygons.len(), 1);
    assert!(polygons[0].holes.is_empty());
    let shell = &polygons[0].shell;
    assert_eq!(shell.coords().xs().len(), 5);
    let mut corners: Vec<_> = shell
        .iter()
        .take(4)
        .map(|point| (point.x, point.y))
        .collect();
    corners.sort_unstable_by(|left, right| left.partial_cmp(right).unwrap());
    assert_eq!(corners, [(0.0, 0.0), (0.0, 4.0), (4.0, 0.0), (4.0, 4.0)]);
}

#[test]
fn certified_line_serialization_keeps_bends_and_protected_vertices() {
    let case = run(&[
        seg(2.0, 0.0, 1.0, 0.0),
        seg(0.0, 0.0, 1.0, 0.0),
        seg(2.0, 0.0, 2.0, 3.0),
        seg(1.0, 0.0, 1.0, 0.0),
    ]);
    let partition = case.partition();
    let (topology, edges) = partition.local_topology().unwrap();
    let faces = partition.global_faces(&topology, &edges).unwrap();
    let cells = ArealCells {
        faces: vec![false; faces.count as usize],
        spans: vec![true; partition.spans.len()],
        nodes: vec![true; partition.node_count()],
    };
    let plan = cells
        .emission_plan(&partition, &topology, &edges, &faces)
        .unwrap();
    let (points, lines, polygons) = plan
        .emit_xy_parts(&partition, &topology, &edges, "test")
        .unwrap();
    assert!(points.is_empty());
    assert!(polygons.is_empty());
    assert_eq!(lines.len(), 1);
    let coordinates: Vec<_> = lines[0]
        .xs()
        .iter()
        .copied()
        .zip(lines[0].ys().iter().copied())
        .collect();
    assert_eq!(coordinates, [
        (0.0, 0.0),
        (1.0, 0.0),
        (2.0, 0.0),
        (2.0, 3.0)
    ]);
}

#[test]
fn certified_line_serialization_closes_cycles_and_stops_at_junctions() {
    for spur in [false, true] {
        let mut segments = vec![
            seg(0.0, 0.0, 4.0, 0.0),
            seg(4.0, 4.0, 4.0, 0.0),
            seg(4.0, 4.0, 0.0, 4.0),
            seg(0.0, 0.0, 0.0, 4.0),
        ];
        if spur {
            segments.push(seg(0.0, 0.0, -2.0, 0.0));
        }
        let case = run(&segments);
        let partition = case.partition();
        let (topology, edges) = partition.local_topology().unwrap();
        let faces = partition.global_faces(&topology, &edges).unwrap();
        let cells = ArealCells {
            faces: vec![false; faces.count as usize],
            spans: vec![true; partition.spans.len()],
            nodes: vec![true; partition.node_count()],
        };
        let plan = cells
            .emission_plan(&partition, &topology, &edges, &faces)
            .unwrap();
        let (points, lines, polygons) = plan
            .emit_xy_parts(&partition, &topology, &edges, "test")
            .unwrap();
        assert!(points.is_empty());
        assert!(polygons.is_empty());
        assert_eq!(lines.len(), if spur { 2 } else { 1 });
        let cycle = lines.iter().find(|line| line.xs().len() == 5).unwrap();
        let coordinates: Vec<_> = cycle
            .xs()
            .iter()
            .copied()
            .zip(cycle.ys().iter().copied())
            .collect();
        assert_eq!(coordinates.first(), coordinates.last());
        let mut corners = coordinates[..4].to_vec();
        let origin = corners
            .iter()
            .position(|&point| point == (0.0, 0.0))
            .unwrap();
        corners.rotate_left(origin);
        if corners[1] == (4.0, 0.0) {
            corners[1..].reverse();
        }
        assert_eq!(corners, [(0.0, 0.0), (0.0, 4.0), (4.0, 4.0), (4.0, 0.0)]);
        if spur {
            let line = lines.iter().find(|line| line.xs().len() == 2).unwrap();
            let mut xs = line.xs().to_vec();
            xs.sort_unstable_by(f64::total_cmp);
            assert_eq!(xs, [-2.0, 0.0]);
            assert_eq!(line.ys(), [0.0, 0.0]);
        }
    }
}

#[test]
#[expect(
    clippy::float_cmp,
    reason = "These assertions pin exact emitted ordinates, not approximate geometry"
)]
fn selected_polygon_walks_keep_an_island_inside_a_hole_separate() {
    let case = run(&[
        seg(0.0, 0.0, 10.0, 0.0),
        seg(10.0, 0.0, 10.0, 10.0),
        seg(10.0, 10.0, 0.0, 10.0),
        seg(0.0, 10.0, 0.0, 0.0),
        seg(2.0, 2.0, 8.0, 2.0),
        seg(8.0, 2.0, 8.0, 8.0),
        seg(8.0, 8.0, 2.0, 8.0),
        seg(2.0, 8.0, 2.0, 2.0),
        seg(4.0, 4.0, 6.0, 4.0),
        seg(6.0, 4.0, 6.0, 6.0),
        seg(6.0, 6.0, 4.0, 6.0),
        seg(4.0, 6.0, 4.0, 4.0),
    ]);
    let partition = case.partition();
    let (topology, edges) = partition.local_topology().unwrap();
    let faces = partition.global_faces(&topology, &edges).unwrap();
    let hole = partition.stored_node(XY::new(2.0, 2.0)).unwrap();
    let island = partition.stored_node(XY::new(4.0, 4.0)).unwrap();
    let island_host = faces.component_hosts[topology.component_of[island.0 as usize] as usize];
    let outer_interior = faces.component_hosts[topology.component_of[hole.0 as usize] as usize];
    let occupied: Vec<_> = (0..faces.count)
        .map(|face| face == outer_interior || (face != 0 && face != island_host))
        .collect();
    let plan = faces
        .boundary_plan(&Vec::new(), &topology, &edges, &partition, &occupied)
        .unwrap();
    let polygons = plan.polygon_walks(&partition, &topology).unwrap();
    let mut ring_counts: Vec<_> = polygons.iter().map(Vec::len).collect();
    ring_counts.sort_unstable();
    assert_eq!(ring_counts, [1, 2]);
    for polygon in polygons {
        let first = &plan.walks[polygon[0]][0];
        let node = NodeId(topology.owners[plan.source_darts[first.sources.start] as usize]);
        assert!(partition.is_stored(node), "stored square corner");
        let x = partition.nearest_xy(node).x;
        if polygon.len() == 1 {
            assert!((4.0..=6.0).contains(&x));
        } else {
            assert!(x == 0.0 || x == 10.0);
        }
    }
    assert_eq!(
        certify_plan(&plan, &partition, &topology, &edges)
            .unwrap()
            .1
            .len(),
        12
    );
    let (points, lines, polygons) = plan
        .emit_xy_parts(&partition, &topology, &edges, "test")
        .unwrap();
    assert!(points.is_empty());
    assert!(lines.is_empty());
    assert_eq!(polygons.len(), 2);
    let outer = polygons
        .iter()
        .find(|polygon| !polygon.holes.is_empty())
        .unwrap();
    assert_eq!(outer.holes.len(), 1);
    assert!(
        outer
            .shell
            .coords()
            .xs()
            .iter()
            .all(|&x| x == 0.0 || x == 10.0)
    );
    assert!(
        outer.holes[0]
            .coords()
            .xs()
            .iter()
            .all(|&x| x == 2.0 || x == 8.0)
    );
    let island = polygons
        .iter()
        .find(|polygon| polygon.holes.is_empty())
        .unwrap();
    assert!(
        island
            .shell
            .coords()
            .xs()
            .iter()
            .all(|&x| x == 4.0 || x == 6.0)
    );
}

#[test]
fn selected_boundaries_keep_point_touching_regions_separate() {
    let case = run(&[
        seg(0.0, 0.0, 2.0, 0.0),
        seg(2.0, 0.0, 2.0, 2.0),
        seg(2.0, 2.0, 0.0, 2.0),
        seg(0.0, 2.0, 0.0, 0.0),
        seg(2.0, 2.0, 4.0, 2.0),
        seg(4.0, 2.0, 4.0, 4.0),
        seg(4.0, 4.0, 2.0, 4.0),
        seg(2.0, 4.0, 2.0, 2.0),
    ]);
    let partition = case.partition();
    let (topology, edges) = partition.local_topology().unwrap();
    let faces = partition.global_faces(&topology, &edges).unwrap();
    assert_eq!(faces.count, 3);
    let plan = faces
        .boundary_plan(&Vec::new(), &topology, &edges, &partition, &[
            false, true, true,
        ])
        .unwrap();
    assert_eq!(plan.face_quotient, [0, 1, 2]);
    assert_eq!(
        plan.walks
            .iter()
            .map(|walk| walk.iter().map(|chord| chord.sides).collect::<Vec<_>>())
            .collect::<Vec<_>>(),
        [vec![[1, 0]; 4], vec![[2, 0]; 4]]
    );
    assert_eq!(
        certify_plan(&plan, &partition, &topology, &edges)
            .unwrap()
            .1
            .len(),
        8
    );
    assert_eq!(plan.walks.len(), 2);
    let walks = faces.boundary_walks(&topology, |face, other| {
        [false, true, true][face] && ![false, true, true][other]
    });
    assert_eq!(walks.len(), 2);
    let contact = partition.stored_node(XY::new(2.0, 2.0)).unwrap();
    for walk in walks {
        assert_eq!(walk.len(), 4);
        assert_eq!(
            walk.iter()
                .filter(|&&slot| topology.owners[slot as usize] == contact.0)
                .count(),
            1
        );
    }
    assert!(
        faces
            .boundary_walks(&topology, |face, other| [false, false, false][face]
                && ![false, false, false][other])
            .is_empty()
    );
}

#[test]
fn mixed_fan_checks_attached_chords_in_rotation_invariant_order() {
    let case = run(&[
        seg(0.0, 0.0, 4.0, 0.0),
        seg(4.0, 0.0, 4.0, 4.0),
        seg(4.0, 4.0, 0.0, 4.0),
        seg(0.0, 4.0, 0.0, 0.0),
        seg(0.0, 0.0, 0.0, -1.0),
        seg(0.0, -1.0, 0.0, -4.0),
        seg(0.0, 0.0, -1.0, 0.0),
        seg(-1.0, 0.0, -4.0, 0.0),
        seg(0.0, 0.0, -1.0, -1.0),
        seg(-1.0, -1.0, -4.0, -4.0),
    ]);
    let partition = case.partition();
    let (topology, edges) = partition.local_topology().unwrap();
    let origin = partition.stored_node(XY::new(0.0, 0.0)).unwrap();
    let faces = partition.global_faces(&topology, &edges).unwrap();
    let bounded = faces
        .walk_faces
        .iter()
        .copied()
        .find(|&face| face != 0)
        .expect("the square has a bounded face");
    let mut occupied = vec![false; faces.count as usize];
    occupied[bounded as usize] = true;
    let cells = ArealCells {
        faces: occupied,
        spans: vec![true; partition.spans.len()],
        nodes: vec![false; partition.node_count()],
    };
    let plan = cells
        .emission_plan(&partition, &topology, &edges, &faces)
        .unwrap();
    let emission = plan.emission(&topology, &edges);
    assert_eq!(plan.walks.len(), 1);
    assert!(emission.areal_chords > 0);
    let attached_lines: Vec<_> = emission
        .items
        .iter()
        .enumerate()
        .filter_map(|(index, &[start, end])| {
            (index >= emission.areal_chords && (start == origin || end == origin)).then_some(index)
        })
        .collect();
    assert_eq!(attached_lines.len(), 3);

    let (_, mut segments) = certify_plan(&plan, &partition, &topology, &edges).unwrap();
    let rotated: Vec<_> = segments
        .iter()
        .map(|segment| Segment {
            start: XY::new(-segment.start.y, segment.start.x),
            end: XY::new(-segment.end.y, segment.end.x),
        })
        .collect();
    check_plan_fans(&plan, &partition, &topology, &edges, &rotated).unwrap();
    let reflected: Vec<_> = segments
        .iter()
        .map(|segment| Segment {
            start: XY::new(segment.start.x, -segment.start.y),
            end: XY::new(segment.end.x, -segment.end.y),
        })
        .collect();
    let error = check_plan_fans(&plan, &partition, &topology, &edges, &reflected).unwrap_err();
    assert!(matches!(
        error.kind(),
        crate::error::ErrorKind::Geometry(GeometryErrorKind::ExactCyclicOrder { .. })
    ));
    segments.swap(attached_lines[0], attached_lines[1]);
    let error = check_plan_fans(&plan, &partition, &topology, &edges, &segments).unwrap_err();
    assert!(matches!(error.kind(), crate::error::ErrorKind::Geometry(
        GeometryErrorKind::ExactCyclicOrder { node }
    ) if *node == origin.0));
}

#[test]
fn isolated_point_in_a_hole_keeps_its_host_in_mixed_output() {
    let case = run(&[
        seg(0.0, 0.0, 6.0, 0.0),
        seg(6.0, 0.0, 6.0, 6.0),
        seg(6.0, 6.0, 0.0, 6.0),
        seg(0.0, 6.0, 0.0, 0.0),
        seg(2.0, 2.0, 4.0, 2.0),
        seg(4.0, 2.0, 4.0, 4.0),
        seg(4.0, 4.0, 2.0, 4.0),
        seg(2.0, 4.0, 2.0, 2.0),
        seg(3.0, 3.0, 3.0, 3.0),
    ]);
    let partition = case.partition();
    let (topology, edges) = partition.local_topology().unwrap();
    let faces = partition.global_faces(&topology, &edges).unwrap();
    let point = partition.stored_node(XY::new(3.0, 3.0)).unwrap();
    let inner = partition.stored_node(XY::new(2.0, 2.0)).unwrap();
    let area_face =
        faces.component_hosts[topology.component_of[inner.0 as usize] as usize] as usize;
    let mut occupied = vec![false; faces.count as usize];
    occupied[area_face] = true;
    let mut cells = ArealCells {
        faces: occupied,
        spans: vec![true; partition.spans.len()],
        nodes: vec![false; partition.node_count()],
    };
    cells.nodes[point.0 as usize] = true;
    let plan = cells
        .emission_plan(&partition, &topology, &edges, &faces)
        .unwrap();
    assert_eq!(plan.walks.len(), 2);
    assert!(plan.lines.is_empty());
    assert_eq!(plan.points.len(), 1);
    let emission = plan.emission(&topology, &edges);
    assert!(emission.areal_chords > 0);
    assert_eq!(emission.items[emission.chords], [point, point]);
    let mut displaced = certify_plan(&plan, &partition, &topology, &edges)
        .unwrap()
        .1;
    let point_item = emission
        .items
        .iter()
        .position(|&item| item == [point, point])
        .unwrap();
    displaced[point_item] = seg(5.0, 5.0, 5.0, 5.0);
    for (index, &[start, end]) in emission.items[..emission.chords].iter().enumerate() {
        assert!(emitted_pair_preserves_incidence(
            [(point, displaced[point_item].start); 2],
            [(start, displaced[index].start), (end, displaced[index].end),],
        ));
    }
    check_plan_fans(&plan, &partition, &topology, &edges, &displaced).unwrap();
    let error = check_plan_faces(
        &plan,
        &partition,
        &topology,
        &edges,
        &emission.items,
        &displaced,
    )
    .unwrap_err();
    assert!(matches!(error.kind(), crate::error::ErrorKind::Geometry(
        GeometryErrorKind::ExactGlobalFace { node }
    ) if *node == point.0));
}

#[test]
fn retained_line_cycle_preserves_its_bounded_face() {
    let case = run(&[
        seg(0.0, 0.0, 4.0, 0.0),
        seg(4.0, 0.0, 4.0, 4.0),
        seg(4.0, 4.0, 0.0, 4.0),
        seg(0.0, 4.0, 0.0, 0.0),
    ]);
    let partition = case.partition();
    let (topology, edges) = partition.local_topology().unwrap();
    let faces = partition.global_faces(&topology, &edges).unwrap();
    let plan = faces
        .boundary_plan(&[0, 1, 2, 3], &topology, &edges, &partition, &[
            false, false,
        ])
        .unwrap();
    assert!(plan.walks.is_empty());
    assert_eq!(
        plan.lines
            .iter()
            .flat_map(|chord| &plan.source_darts[chord.sources.clone()])
            .map(|&slot| edges[slot as usize])
            .collect::<Vec<_>>(),
        [0, 1, 2, 3]
    );
    assert_eq!(
        plan.lines
            .iter()
            .map(|chord| chord.sides)
            .collect::<Vec<_>>(),
        [[1, 0]; 4]
    );
    assert_eq!(plan.face_quotient, [0, 1]);
    let open = faces
        .boundary_plan(&[0, 1, 2], &topology, &edges, &partition, &[false, false])
        .unwrap();
    assert_eq!(open.face_quotient, [0, 0]);
    assert_eq!(
        open.lines
            .iter()
            .map(|chord| chord.sides)
            .collect::<Vec<_>>(),
        [[0, 0]; 3]
    );
}

#[test]
#[expect(
    clippy::float_cmp,
    reason = "This test distinguishes exact axis alignment from diagonal source spans"
)]
fn omitted_atoms_merge_faces_and_preserve_isolated_hosts() {
    let case = run(&[
        seg(0.0, 0.0, 4.0, 0.0),
        seg(4.0, 0.0, 4.0, 4.0),
        seg(4.0, 4.0, 0.0, 4.0),
        seg(0.0, 4.0, 0.0, 0.0),
        seg(0.0, 0.0, 4.0, 4.0),
        seg(1.0, 2.0, 1.0, 2.0),
    ]);
    let partition = case.partition();
    let (topology, edges) = partition.local_topology().unwrap();
    let faces = partition.global_faces(&topology, &edges).unwrap();
    assert_eq!(faces.count, 3);
    let plan = faces
        .boundary_plan(&Vec::new(), &topology, &edges, &partition, &[
            false, true, true,
        ])
        .unwrap();
    assert_eq!(plan.face_quotient, [0, 1, 1]);
    assert_eq!(
        plan.walks
            .iter()
            .map(|walk| walk.iter().map(|chord| chord.sides).collect::<Vec<_>>())
            .collect::<Vec<_>>(),
        [vec![[1, 0]; 4]]
    );
    assert_eq!(plan.walks.len(), 1);
    let walks = faces.boundary_walks(&topology, |face, other| {
        [false, true, true][face] && ![false, true, true][other]
    });
    assert_eq!(walks.len(), 1);
    let boundary: Vec<_> = walks[0]
        .iter()
        .map(|&slot| NodeId(topology.owners[slot as usize]))
        .collect();
    assert_eq!(boundary, [
        partition.stored_node(XY::new(0.0, 0.0)).unwrap(),
        partition.stored_node(XY::new(4.0, 0.0)).unwrap(),
        partition.stored_node(XY::new(4.0, 4.0)).unwrap(),
        partition.stored_node(XY::new(0.0, 4.0)).unwrap(),
    ]);
    let mut retained = vec![true; partition.spans.len()];
    let identity = faces.quotient(&topology, &edges, &retained);
    for (face, &mapped) in identity.iter().enumerate() {
        assert_eq!(mapped as usize, face);
    }
    for (edge, span) in partition.spans.iter().enumerate() {
        let source = partition.unique[span.unique as usize];
        if source.start.x != source.end.x && source.start.y != source.end.y {
            retained[edge] = false;
        }
    }
    let merged = faces.quotient(&topology, &edges, &retained);
    assert_eq!(merged[0], 0);
    assert_ne!(merged[1], 0);
    assert_eq!(merged[1], merged[2]);
    let point = partition.stored_node(XY::new(1.0, 2.0)).unwrap();
    let host = faces.component_hosts[topology.component_of[point.0 as usize] as usize];
    assert_eq!(merged[host as usize], merged[1]);
    retained.fill(false);
    let erased = faces.quotient(&topology, &edges, &retained);
    assert_eq!(erased, [0, 0, 0]);
}

#[test]
fn face_windings_cancel_opposite_sources_and_ignore_open_linework() {
    let case = run(&[
        seg(0.0, 0.0, 4.0, 0.0),
        seg(4.0, 0.0, 2.0, 3.0),
        seg(2.0, 3.0, 0.0, 0.0),
        seg(0.0, 0.0, 2.0, 3.0),
        seg(2.0, 3.0, 4.0, 0.0),
        seg(4.0, 0.0, 0.0, 0.0),
        seg(-1.0, 1.0, 5.0, 1.0),
    ]);
    let partition = case.partition();
    let (topology, edges) = partition.local_topology().unwrap();
    let faces = partition.global_faces(&topology, &edges).unwrap();
    let mut windings = Vec::new();
    partition
        .visit_face_windings(
            &topology,
            &edges,
            &faces,
            &[Some(0), Some(0), Some(0), Some(0), Some(0), Some(0), None],
            1,
            |_, winding| {
                windings.push(winding[0]);
                Ok(())
            },
        )
        .unwrap();
    assert_eq!(windings, [0, 0, 0]);
}

#[test]
fn degenerate_unique_without_isolated_slice_does_not_vanish() {
    let (unique, provenance) =
        dedup_undirected_segments_sourced(vec![seg(0.0, 0.0, 2.0, 0.0), seg(1.0, 1.0, 1.0, 1.0)])
            .unwrap();
    let mut budget = ExpansionBudget::new("exact_partition", "items");
    let partition =
        partition_unique_segments::<true>(&unique, &provenance, &[], &mut budget).unwrap();
    assert!(partition.stored_node(XY::new(1.0, 1.0)).is_some());
    assert_eq!(partition.isolated.len(), 1);
}

#[test]
fn signed_zero_endpoints_share_a_stored_node() {
    let case = run(&[seg(-0.0, 0.0, 1.0, 0.0), seg(0.0, 0.0, 0.0, 1.0)]);
    let partition = case.partition();
    let a = partition.stored_node(XY::new(-0.0, 0.0)).unwrap();
    let b = partition.stored_node(XY::new(0.0, 0.0)).unwrap();
    assert_eq!(a, b);
}

#[test]
fn full_finite_exponent_pair_is_five_nodes_four_spans() {
    let min_sub = f64::from_bits(1);
    let a = seg(0.0, 0.0, f64::MAX, min_sub);
    let b = seg(min_sub, -f64::MAX, f64::MAX, f64::MAX);
    let case = run(&[a, b]);
    let partition = case.partition();
    assert_eq!(partition.node_count(), 5);
    assert_eq!(partition.spans.len(), 4);
    assert_eq!(constructed_count(&partition), 1);
    for point in [a.start, a.end, b.start, b.end] {
        assert!(partition.stored_node(point).is_some());
    }
    let crossing = crossing_node(&partition);
    for endpoint in [a.start, a.end, b.start, b.end] {
        let endpoint = partition.stored_node(endpoint).unwrap();
        assert_ne!(
            partition.compare_nodes(crossing, endpoint).unwrap(),
            Ordering::Equal
        );
    }
}

#[test]
fn isolated_on_line_cuts_the_edge() {
    let mut budget = ExpansionBudget::new("exact_partition", "items");
    let (unique, provenance) =
        dedup_undirected_segments_sourced(vec![seg(0.0, 0.0, 2.0, 0.0)]).unwrap();
    let partition =
        partition_unique_segments::<true>(&unique, &provenance, &[XY::new(1.0, 0.0)], &mut budget)
            .unwrap();
    assert_eq!(partition.node_count(), 3);
    assert_eq!(partition.spans.len(), 2);
    assert!(partition.isolated.is_empty());
    let mid = partition.stored_node(XY::new(1.0, 0.0)).unwrap();
    assert!(
        partition
            .spans
            .iter()
            .any(|span| span.start == mid || span.end == mid)
    );
}

#[test]
fn isolated_at_crossing_is_stored_not_constructed() {
    let mut budget = ExpansionBudget::new("exact_partition", "items");
    let (unique, provenance) =
        dedup_undirected_segments_sourced(vec![seg(0.0, 0.0, 2.0, 2.0), seg(0.0, 2.0, 2.0, 0.0)])
            .unwrap();
    let partition =
        partition_unique_segments::<true>(&unique, &provenance, &[XY::new(1.0, 1.0)], &mut budget)
            .unwrap();
    assert_eq!(partition.node_count(), 5);
    assert_eq!(partition.spans.len(), 4);
    assert_eq!(constructed_count(&partition), 0);
    assert!(partition.isolated.is_empty());
    assert!(partition.is_stored(partition.stored_node(XY::new(1.0, 1.0)).unwrap()));
}

#[test]
fn stored_endpoint_concurrency_is_six_nodes_five_spans() {
    let case = run(&[
        seg(0.0, 0.0, 2.0, 0.0),
        seg(1.0, 0.0, 1.0, 1.0),
        seg(0.0, 1.0, 2.0, -1.0),
    ]);
    let partition = case.partition();
    assert_eq!(partition.node_count(), 6);
    assert_eq!(partition.spans.len(), 5);
    assert_eq!(constructed_count(&partition), 0);
    let junction = partition.stored_node(XY::new(1.0, 0.0)).unwrap();
    let incident = partition
        .spans
        .iter()
        .filter(|span| span.start == junction || span.end == junction)
        .count();
    assert_eq!(incident, 5);
}

#[test]
fn concurrent_three_sources_recipe_when_vertical_is_first() {
    let case = run(&[
        seg(1.0, -1.0, 1.0, 1.0),
        seg(0.0, 0.0, 3.0, 1.0),
        seg(0.0, 1.0, 3.0, -1.0),
    ]);
    let partition = case.partition();
    assert_eq!(constructed_count(&partition), 1);
    let (one, three) = (dyadic(1.0), dyadic(3.0));
    assert_exact_at(&partition, crossing_node(&partition), [&one, &one], [
        &one, &three,
    ]);
}

/// Ordinary crossings are certified in double-double and never build an
/// exact point: the lane is the one that actually runs.
#[test]
fn generic_crossings_round_without_exact_points() {
    let ring = |cx: f64, cy: f64, phase: f64| -> Vec<Segment> {
        let corners: Vec<XY> = (0..7)
            .map(|index| {
                let angle = phase + std::f64::consts::TAU * f64::from(index) / 7.0;
                XY::new(cx + 25.0 * angle.cos(), cy + 25.0 * angle.sin())
            })
            .collect();
        (0..7)
            .map(|index| Segment {
                start: corners[index],
                end: corners[(index + 1) % 7],
            })
            .collect()
    };
    let mut segments = ring(500_000.0, 5_200_000.0, 0.1);
    segments.extend(ring(500_020.0, 5_200_015.0, 0.2));
    reset_partition_builds();
    let case = run(&segments);
    let partition = case.partition();
    let (topology, edges) = partition.local_topology().unwrap();
    partition.global_faces(&topology, &edges).unwrap();
    let crossings = (0..partition.node_count() as u32)
        .filter(|&node| !partition.is_stored(NodeId(node)))
        .count();
    assert!(crossings >= 2);
    let metrics = partition_metrics();
    assert_eq!(metrics.declined_crossings, 0);
    assert_eq!(metrics.exact_crossings, 0);
}

/// The crossing's exact y is 1.5 + 2^-52 + 2^-53, the midpoint between two
/// doubles: the certificate declines and the exact rounding goes to even.
#[test]
fn rounding_midpoint_crossing_declines_and_rounds_to_even() {
    let x = 1.0 + f64::EPSILON;
    reset_partition_builds();
    let case = run(&[seg(0.0, 0.0, 2.0, 3.0), seg(x, 0.0, x, 3.0)]);
    let partition = case.partition();
    assert_eq!(partition_metrics().declined_crossings, 1);
    assert_eq!(
        partition.nearest_xy(crossing_node(&partition)),
        XY::new(x, 1.5 + 2.0 * f64::EPSILON)
    );
}

/// Three nearly concurrent segments cross at three points that differ
/// below the last bit, two of which round to one double; they stay three
/// nodes, ordered exactly.
#[test]
fn crossings_rounding_to_one_double_stay_distinct() {
    let case = run(&[
        seg(0.0, 0.0, 3.0, 1.0),
        seg(1.0, -1.0, 1.0, 1.0),
        seg(0.0, 1.0, 2.0, -1.0 / 3.0),
    ]);
    let partition = case.partition();
    let crossings: Vec<NodeId> = (0..partition.node_count() as u32)
        .map(NodeId)
        .filter(|&node| !partition.is_stored(node))
        .collect();
    assert_eq!(crossings.len(), 3);
    let mut shared_roundings = 0;
    for (index, &first) in crossings.iter().enumerate() {
        for &second in &crossings[index + 1..] {
            shared_roundings +=
                usize::from(partition.nearest_xy(first) == partition.nearest_xy(second));
            assert_ne!(
                partition.compare_nodes(first, second).unwrap(),
                Ordering::Equal
            );
        }
    }
    assert_eq!(shared_roundings, 1);
}

/// Coordinates outside the certificate window and nearly parallel carriers
/// decline to the exact construction, which still rounds nearest-even.
#[test]
fn out_of_window_and_near_parallel_crossings_decline() {
    for segments in [
        [
            seg(0.0, 0.0, 3e300, 1e300),
            seg(1e300, -1e300, 1e300, 1e300),
        ],
        [seg(0.0, 0.0, 1.0, 1.0), seg(0.0, 1e-17, 1.0, 1.0 - 1e-16)],
    ] {
        reset_partition_builds();
        let case = run(&segments);
        let partition = case.partition();
        assert_eq!(partition_metrics().declined_crossings, 1);
        let crossing = crossing_node(&partition);
        let [first_unique, second_unique] = partition.unique.first_chunk::<2>().unwrap();
        let [x, y, w] = crossing_homogeneous(*first_unique, *second_unique).unwrap();
        assert_eq!(
            partition.nearest_xy(crossing),
            XY::new(round_ratio(&x, &w).unwrap(), round_ratio(&y, &w).unwrap())
        );
    }
}

#[test]
fn star_crossings_keep_one_recipe_per_constructed_node() {
    let mut segments = vec![seg(0.0, 0.0, 5.0, 0.0)];
    for x in 1..=4 {
        segments.push(seg(f64::from(x), -1.0, f64::from(x), 1.0));
    }
    let case = run(&segments);
    let partition = case.partition();
    assert_eq!(constructed_count(&partition), 4);
    let (zero, one) = (dyadic(0.0), dyadic(1.0));
    let mut found = Vec::new();
    for node in 0..partition.node_count() as u32 {
        let node = NodeId(node);
        if partition.is_stored(node) {
            continue;
        }
        let point = partition.nearest_xy(node);
        assert_eq!(point.y, 0.0);
        assert_exact_at(&partition, node, [&dyadic(point.x), &one], [&zero, &one]);
        found.push(point.x);
    }
    found.sort_unstable_by(f64::total_cmp);
    assert_eq!(found, [1.0, 2.0, 3.0, 4.0]);
}

#[test]
fn disjoint_thirty_two_thousand_segments_are_not_capped_as_pairs() {
    let mut segments = Vec::with_capacity(32_000);
    for index in 0..32_000 {
        let x = f64::from(index);
        segments.push(seg(x, 0.0, x, 1.0));
    }
    let case = run(&segments);
    let partition = case.partition();
    assert_eq!(partition.node_count(), 64_000);
    assert_eq!(partition.spans.len(), 32_000);
    assert_eq!(constructed_count(&partition), 0);
}

#[test]
fn pairwise_limit_is_not_an_allocation_error() {
    let err =
        ExpansionBudget::pairwise("exact_partition", "pairs", PAIRWISE_WORK_LIMIT, 2).unwrap_err();
    assert!(matches!(
        err.kind(),
        crate::error::ErrorKind::Geometry(GeometryErrorKind::GeneratedOutputTooLarge { .. })
    ));
}

#[test]
fn rectangle_intersection_uses_exact_source_contacts() {
    let crossing = Shape::LineString(LineSeq::from_proven(
        CoordSeq::try_from_xy(&[XY::new(-1.0, 0.5), XY::new(2.0, 0.5)]).unwrap(),
    ));
    let bounds = Bounds::new_raw(0.0, 0.0, 1.0, 1.0);
    assert!(exact_intersects_bounds(&crossing, bounds).unwrap());

    let touching = Shape::LineString(LineSeq::from_proven(
        CoordSeq::try_from_xy(&[XY::new(-1.0, 0.0), XY::new(0.0, 0.0)]).unwrap(),
    ));
    assert!(exact_intersects_bounds(&touching, bounds).unwrap());

    let disjoint = Shape::LineString(LineSeq::from_proven(
        CoordSeq::try_from_xy(&[XY::new(2.0, 2.0), XY::new(3.0, 3.0)]).unwrap(),
    ));
    assert!(!exact_intersects_bounds(&disjoint, bounds).unwrap());
}

#[test]
fn rectangle_source_zip_preserves_full_ordinals_and_zm_in_both_orders() {
    let Shape::Polygon(subject) = crate::io::parse_wkt(
        "POLYGON ZM ((-2 -1 10 100,2 -1 20 200,2 1 30 300,-2 1 50 500,-2 -1 10 100))",
    )
    .unwrap() else {
        panic!("expected polygon");
    };
    let bounds = Bounds::new_raw(-1.0, -0.5, 1.0, 1.5);
    let rectangle = rect_polygon(bounds).unwrap();

    for rectangle_first in [false, true] {
        let (inventory, intervals) = ExactSourceInventory::from_polygon_rect_clip(
            &[&subject],
            &rectangle,
            bounds,
            rectangle_first,
        )
        .unwrap()
        .expect("the subject's top edge enters the rectangle interior");

        let expected_operands = if rectangle_first {
            [1, 1, 1, 1, 0, 0, 0, 0]
        } else {
            [0, 0, 0, 0, 1, 1, 1, 1]
        };
        assert_eq!(inventory.source_operands.as_slice(), expected_operands);

        let expected_unique_segments = if rectangle_first {
            [
                [XY::new(-1.0, -0.5), XY::new(1.0, -0.5)],
                [XY::new(1.0, -0.5), XY::new(1.0, 1.5)],
                [XY::new(1.0, 1.5), XY::new(-1.0, 1.5)],
                [XY::new(-1.0, 1.5), XY::new(-1.0, -0.5)],
                [XY::new(2.0, 1.0), XY::new(-2.0, 1.0)],
            ]
        } else {
            [
                [XY::new(2.0, 1.0), XY::new(-2.0, 1.0)],
                [XY::new(-1.0, -0.5), XY::new(1.0, -0.5)],
                [XY::new(1.0, -0.5), XY::new(1.0, 1.5)],
                [XY::new(1.0, 1.5), XY::new(-1.0, 1.5)],
                [XY::new(-1.0, 1.5), XY::new(-1.0, -0.5)],
            ]
        };
        let expected_source_ordinals = if rectangle_first {
            [0, 1, 2, 3, 6]
        } else {
            [2, 4, 5, 6, 7]
        };
        let actual_unique_segments: Vec<_> = inventory
            .unique
            .iter()
            .map(|segment| [segment.start, segment.end])
            .collect();
        assert_eq!(actual_unique_segments, expected_unique_segments);
        for (owners, source) in inventory.provenance.iter().zip(expected_source_ordinals) {
            assert_eq!(owners, &[(source, false)]);
        }

        let mut budget = ExpansionBudget::new("clip_by_rect", "rectangle source ordinals");
        let partition = inventory
            .partition_in_window(&intervals, bounds, &mut budget)
            .unwrap();
        let mut crossing_ordinates = Vec::new();
        for node in partition.crossings() {
            let (z, m) = inventory
                .resolve_node_ordinates::<crate::crs::GeodesicMetric>(
                    &partition,
                    node,
                    crate::geometry::CoordinateAxes::XYZM,
                    &SourceArcContext::cartesian(),
                    None,
                )
                .unwrap();
            crossing_ordinates.push((partition.nearest_xy(node), z.0, m.0));
        }
        let expected_crossing_ordinates = [
            (XY::new(-1.0, 1.0), Some(45.0), Some(450.0)),
            (XY::new(1.0, 1.0), Some(35.0), Some(350.0)),
        ];
        assert!(
            crossing_ordinates
                .iter()
                .all(
                    |&(actual_xy, actual_z, actual_m)| expected_crossing_ordinates.iter().any(
                        |&(expected_xy, expected_z, expected_m)| {
                            same_point(actual_xy, expected_xy)
                                && actual_z == expected_z
                                && actual_m == expected_m
                        }
                    )
                )
        );
        for expected in expected_crossing_ordinates {
            assert!(crossing_ordinates.contains(&expected));
        }
    }
}

#[test]
fn polygon_occupancy_uses_staged_ids_when_rectangle_is_first() {
    let Shape::Polygon(subject) =
        crate::io::parse_wkt("POLYGON ((10 10,14 10,14 14,10 14,10 10),(9 9,15 9,15 15,9 15,9 9))")
            .unwrap()
    else {
        unreachable!()
    };
    let bounds = Bounds::new_raw(9.5, 9.5, 13.0, 14.5);
    let rectangle = rect_polygon(bounds).unwrap();
    let (inventory, _) =
        ExactSourceInventory::from_polygon_rect_clip(&[&subject], &rectangle, bounds, true)
            .unwrap()
            .expect("the rectangle crosses the subject ring");

    assert_eq!(inventory.polygon_operands, [1, 0]);
    assert_eq!(inventory.ring_polygons, [0, 1, 1]);
    assert_eq!(inventory.ring_holes, [false, false, true]);

    let mut occupancy = PolygonOccupancy::new(&inventory, false);
    let mut winding = vec![0; inventory.ring_count];
    occupancy.cross(&mut winding, 1, 1);
    assert!(occupancy.operand_occupied(0));
    occupancy.cross(&mut winding, 2, 1);
    assert!(occupancy.polygon_holes_occupied(1));
    assert!(!occupancy.operand_occupied(0));
    assert!(!occupancy.operand_occupied(1));
    occupancy.cross(&mut winding, 2, -1);
    assert!(!occupancy.polygon_holes_occupied(1));
    assert!(occupancy.operand_occupied(0));
}

#[test]
fn rectangle_clip_provenance_keeps_full_source_ring_ordinals() {
    let Shape::Polygon(subject) =
        crate::io::parse_wkt("POLYGON ((-10 0,10 0,10 2,-10 2,-10 0))").unwrap()
    else {
        panic!("expected polygon");
    };
    let bounds = Bounds::new_raw(-1.0, -1.0, 1.0, 3.0);
    let rectangle = rect_polygon(bounds).unwrap();
    let (inventory, intervals) =
        ExactSourceInventory::from_polygon_rect_clip(&[&subject], &rectangle, bounds, true)
            .unwrap()
            .expect("the subject crosses the clip window");

    assert_eq!(intervals.len(), 6);
    assert_eq!(inventory.source_segments.len(), 8);
    assert_eq!(inventory.ring_count, 2);
    assert_eq!(inventory.source_rings, [
        Some(0),
        Some(0),
        Some(0),
        Some(0),
        Some(1),
        Some(1),
        Some(1),
        Some(1),
    ]);
    assert_eq!(inventory.winding_source_rings(), vec![
        Some(0),
        Some(0),
        Some(0),
        Some(0),
        Some(1),
        Some(1),
        Some(1),
        Some(1),
    ]);
    assert_eq!(
        inventory
            .provenance
            .iter()
            .map(<[_]>::to_vec)
            .collect::<Vec<_>>(),
        vec![
            vec![(0, false)],
            vec![(1, false)],
            vec![(2, false)],
            vec![(3, false)],
            vec![(4, false)],
            vec![(6, false)],
        ]
    );
}

#[test]
fn polygon_occupancy_keeps_raw_winding_rings_unowned() {
    let inventory = ExactSourceInventory::from_raw_loops(&[
        (vec![0.0, 1.0, 2.0], vec![0.0, 0.0, 0.0]),
        (vec![7.0, 8.0], vec![0.0, 0.0]),
        (Vec::new(), Vec::new()),
    ])
    .unwrap();
    assert_eq!(inventory.ring_count, 3);
    assert!(inventory.ring_polygons.is_empty());
    assert!(inventory.ring_holes.is_empty());
    assert_eq!(inventory.ring_kinds, [
        RingKind::Winding,
        RingKind::Winding,
        RingKind::Winding
    ]);
    assert_eq!(inventory.source_rings, [
        Some(0),
        Some(0),
        Some(0),
        None,
        None
    ]);
    assert_eq!(inventory.winding_source_rings(), vec![
        Some(0),
        Some(0),
        Some(0),
        None,
        None
    ]);

    let mut occupancy = PolygonOccupancy::new(&inventory, false);
    let error = occupancy.seed(&[]).unwrap_err();
    assert!(matches!(
        error.kind(),
        crate::error::ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
    ));

    let mut winding = vec![0; inventory.ring_count];
    occupancy.cross(&mut winding, 0, 1);
    assert_eq!(winding, [1, 0, 0]);
    assert!(!occupancy.operand_occupied(0));
}

fn append_constructed_point(
    partition: &mut ExactPartition<'_>,
    x: ExactDyadic,
    y: ExactDyadic,
) -> NodeId {
    let one = dyadic(1.0);
    let rounded = XY::new(
        round_ratio(&x, &one).unwrap(),
        round_ratio(&y, &one).unwrap(),
    );
    let exact = std::cell::OnceCell::new();
    exact
        .set(Box::new([x, y, one]))
        .expect("fresh constructed point stores one exact value");
    let node = NodeId(u32::try_from(partition.node_count()).unwrap());
    partition
        .points
        .push(NodeRecipe::Constructed { exact }, rounded)
        .unwrap();
    node
}

fn point_emission(nodes: &[NodeId]) -> embedding::Emission {
    embedding::Emission {
        items: nodes.iter().map(|&node| [node, node]).collect(),
        chords: 0,
        areal_chords: 0,
        fan_spans: Vec::new(),
        faces: None,
    }
}

fn append_lattice_collision_points(
    partition: &mut ExactPartition<'_>,
    count: usize,
) -> Vec<NodeId> {
    let order: Vec<_> = (0..count).collect();
    append_lattice_collision_points_in_order(partition, count, &order)
}

fn append_lattice_collision_points_in_order(
    partition: &mut ExactPartition<'_>,
    count: usize,
    order: &[usize],
) -> Vec<NodeId> {
    let offsets = [
        2.0_f64.powi(-54),
        2.0_f64.powi(-55),
        2.0_f64.powi(-56),
        2.0_f64.powi(-57),
        2.0_f64.powi(-58),
        2.0_f64.powi(-59),
        2.0_f64.powi(-60),
        2.0_f64.powi(-61),
        2.0_f64.powi(-62),
        2.0_f64.powi(-63),
    ];
    assert_eq!(order.len(), count);
    let one = dyadic(1.0);
    let mut nodes = vec![None; count];
    for &index in order {
        let x_offset = *offsets
            .get(index)
            .expect("fixture has one x offset per node");
        let y_index = count.wrapping_sub(index.wrapping_add(1));
        let y_offset = *offsets
            .get(y_index)
            .expect("fixture has one y offset per node");
        let node = append_constructed_point(
            partition,
            one.clone()
                .try_add(ExactDyadic::from_f64(x_offset))
                .unwrap(),
            one.clone()
                .try_add(ExactDyadic::from_f64(y_offset))
                .unwrap(),
        );
        *nodes
            .get_mut(index)
            .expect("input ordinal belongs to the lattice fixture") = Some(node);
    }
    nodes
        .into_iter()
        .map(|node| node.expect("every lattice point was appended once"))
        .collect()
}

fn lattice_crossing_group(origin: f64, ulp: f64) -> [Segment; 3] {
    [
        seg(origin - ulp, origin - ulp, origin + ulp, origin + ulp),
        seg(origin, origin + ulp, origin + ulp, origin - ulp),
        seg(origin, origin, origin + 2.0 * ulp, origin + ulp),
    ]
}

fn argentina_uruguay_minimal_segments() -> [Segment; 6] {
    [
        seg(
            -58.404_001_6,
            -33.178_818,
            -58.122_989_509_606_21,
            -33.022_490_639_567_15,
        ),
        seg(
            -58.122_989_509_606_21,
            -33.022_490_639_567_15,
            -58.127_583_9,
            -33.022_275,
        ),
        seg(
            -58.114_883_013_439_915,
            -33.017_980_986_099_005,
            -58.113_024_156_144_505,
            -33.016_946_901_572_59,
        ),
        seg(
            -58.113_024_156_144_505,
            -33.016_946_901_572_59,
            -58.100_100_6,
            -32.951_622_7,
        ),
        seg(
            -58.111_802_159_656_57,
            -33.011_057_831_530_63,
            -58.112_953_902_396_03,
            -33.016_907_819_328_67,
        ),
        seg(
            -58.112_953_902_396_03,
            -33.016_907_819_328_67,
            -58.404_001_6,
            -33.178_818,
        ),
    ]
}

fn lattice_group_emission(partition: &ExactPartition<'_>, origin: f64) -> embedding::Emission {
    let span_ids = partition
        .spans
        .iter()
        .enumerate()
        .filter_map(|(index, span)| {
            let start = partition.nearest_xy(span.start);
            let end = partition.nearest_xy(span.end);
            let nearby =
                |point: XY| (point.x - origin).abs() < 0.01 && (point.y - origin).abs() < 0.01;
            (nearby(start) && nearby(end)).then(|| u32::try_from(index).unwrap())
        });
    partition.span_emission(span_ids)
}

fn lattice_group_crossing(partition: &ExactPartition<'_>, origin: f64) -> NodeId {
    partition
        .crossings()
        .iter()
        .copied()
        .find(|&node| partition.nearest_xy(node) == XY::new(origin, origin))
        .expect("the lattice group has a constructed crossing in its center cell")
}

fn planar_candidate_values(
    exact_x: ExactDyadic,
    exact_y: ExactDyadic,
    domain: embedding::EmbeddingDomain,
) -> (XY, Vec<XY>, Vec<XY>) {
    let case = run(&[seg(0.0, 0.0, 2.0, 0.0)]);
    let mut partition = case.partition();
    partition.embedding_domain = domain;
    let node = append_constructed_point(&mut partition, exact_x, exact_y);
    let emission = point_emission(&[node]);
    let movable_cache = vec![None; partition.node_count()];
    let problem =
        embedding::PlanarProblem::new(&partition, &[&emission], &[node], movable_cache).unwrap();
    let nearest = partition.nearest_xy(node);
    let mut candidates = Vec::new();
    problem
        .fill_candidates(node, &mut candidates, &mut Unmetered)
        .unwrap();
    let mut ordered = candidates.clone();
    let mut work = Unmetered;
    crate::geometry::try_sort_by_with_work(&mut ordered, &mut work, |left, right, work| {
        problem.compare_candidates(node, *left, *right, work)
    })
    .unwrap();
    (nearest, candidates, ordered)
}

fn assert_exact_axis_displacement_bound(
    exact: &ExactDyadic,
    weight: &ExactDyadic,
    emitted: f64,
    nearest: f64,
) {
    let scaled_emitted = dyadic(emitted).try_product(weight).unwrap();
    let delta = scaled_emitted.try_subtract(exact.clone()).unwrap().abs();
    let step = (nearest.next_up() - nearest).max(nearest - nearest.next_down());
    let bound = dyadic(1.5 * step)
        .try_product(&weight.clone().abs())
        .unwrap();
    assert_ne!(
        delta.cmp(&bound),
        Ordering::Greater,
        "nearest={nearest:?}, emitted={emitted:?}, exact={exact:?}, weight={weight:?}, bound={bound:?}"
    );
}

struct TestVerifier<'a, P: Problem<Output = ()>> {
    inner: &'a mut P,
    local_calls: &'a std::cell::Cell<usize>,
    global_calls: &'a std::cell::Cell<usize>,
    node_order: Option<&'a dyn Fn(P::Node, P::Node) -> Ordering>,
    candidate_filter: Option<&'a dyn Fn(P::Node, P::Candidate) -> bool>,
    bypass_local: bool,
    bypass_global: bool,
}

impl<P: Problem<Output = ()>> Problem for TestVerifier<'_, P> {
    type Node = P::Node;
    type Candidate = P::Candidate;
    type Key = P::Key;
    type ClassState = P::ClassState;
    type Output = ();

    fn classes(&self) -> &[Vec<Self::Node>] {
        self.inner.classes()
    }

    fn fixed_keys<W: Work>(
        &self,
        class: usize,
        variables: &[Self::Node],
        work: &mut W,
    ) -> Result<Vec<Self::Key>> {
        self.inner.fixed_keys(class, variables, work)
    }

    fn compare_nodes<W: Work>(
        &self,
        left: Self::Node,
        right: Self::Node,
        work: &mut W,
    ) -> Result<Ordering> {
        self.node_order.map_or_else(
            || self.inner.compare_nodes(left, right, work),
            |compare| Ok(compare(left, right)),
        )
    }

    fn fill_candidates<W: Work>(
        &self,
        node: Self::Node,
        candidates: &mut Vec<Self::Candidate>,
        work: &mut W,
    ) -> Result<()> {
        self.inner.fill_candidates(node, candidates, work)?;
        if let Some(allowed) = self.candidate_filter {
            work.charge_product(candidates.len(), 1)?;
            candidates.retain(|&candidate| allowed(node, candidate));
        }
        Ok(())
    }

    fn compare_candidates<W: Work>(
        &self,
        node: Self::Node,
        left: Self::Candidate,
        right: Self::Candidate,
        work: &mut W,
    ) -> Result<Ordering> {
        self.inner.compare_candidates(node, left, right, work)
    }

    fn candidate_key(&self, candidate: Self::Candidate) -> Self::Key {
        self.inner.candidate_key(candidate)
    }

    fn prepare_class<A: SearchAccount>(
        &self,
        class: usize,
        variables: &[Self::Node],
        account: &mut A,
    ) -> Result<Self::ClassState> {
        self.inner.prepare_class(class, variables, account)
    }

    fn check_local<A: SearchAccount>(
        &self,
        class_state: &Self::ClassState,
        class: usize,
        variables: &[Self::Node],
        rank: usize,
        assigned: &[Option<Self::Candidate>],
        account: &mut A,
    ) -> Result<bool> {
        self.local_calls.set(self.local_calls.get().wrapping_add(1));
        if self.bypass_local {
            Ok(true)
        } else {
            self.inner
                .check_local(class_state, class, variables, rank, assigned, account)
        }
    }

    fn check_global<A: SearchAccount>(
        &mut self,
        assignments: &[ClassAssignment<'_, Self::Node, Self::Candidate>],
        account: &mut A,
    ) -> Result<GlobalCheck<Self::Output>> {
        self.global_calls
            .set(self.global_calls.get().wrapping_add(1));
        if self.bypass_global {
            Ok(GlobalCheck::Pass(()))
        } else {
            Problem::check_global(&mut *self.inner, assignments, account)
        }
    }

    fn initial_evaluations(&self) -> u64 {
        self.inner.initial_evaluations()
    }
}

struct FanBypassVerifier<'a, 'p, 's, 'e> {
    inner: &'a mut embedding::PlanarProblem<'p, 's, 'e>,
    global_calls: &'a std::cell::Cell<usize>,
}

impl Problem for FanBypassVerifier<'_, '_, '_, '_> {
    type Node = NodeId;
    type Candidate = XY;
    type Key = PointKey;
    type ClassState = embedding::PlanarClassState;
    type Output = ();

    fn classes(&self) -> &[Vec<Self::Node>] {
        Problem::classes(self.inner)
    }

    fn fixed_keys<W: Work>(
        &self,
        class: usize,
        variables: &[Self::Node],
        work: &mut W,
    ) -> Result<Vec<Self::Key>> {
        Problem::fixed_keys(self.inner, class, variables, work)
    }

    fn compare_nodes<W: Work>(
        &self,
        left: Self::Node,
        right: Self::Node,
        work: &mut W,
    ) -> Result<Ordering> {
        Problem::compare_nodes(self.inner, left, right, work)
    }

    fn fill_candidates<W: Work>(
        &self,
        node: Self::Node,
        candidates: &mut Vec<Self::Candidate>,
        work: &mut W,
    ) -> Result<()> {
        Problem::fill_candidates(self.inner, node, candidates, work)
    }

    fn compare_candidates<W: Work>(
        &self,
        node: Self::Node,
        left: Self::Candidate,
        right: Self::Candidate,
        work: &mut W,
    ) -> Result<Ordering> {
        Problem::compare_candidates(self.inner, node, left, right, work)
    }

    fn candidate_key(&self, candidate: Self::Candidate) -> Self::Key {
        Problem::candidate_key(self.inner, candidate)
    }

    fn prepare_class<A: SearchAccount>(
        &self,
        class: usize,
        variables: &[Self::Node],
        account: &mut A,
    ) -> Result<Self::ClassState> {
        Problem::prepare_class(self.inner, class, variables, account)
    }

    fn check_local<A: SearchAccount>(
        &self,
        class_state: &Self::ClassState,
        class: usize,
        variables: &[Self::Node],
        rank: usize,
        assigned: &[Option<Self::Candidate>],
        account: &mut A,
    ) -> Result<bool> {
        self.inner
            .check_local_without_fans(class_state, class, variables, rank, assigned, account)
    }

    fn check_global<A: SearchAccount>(
        &mut self,
        assignments: &[ClassAssignment<'_, Self::Node, Self::Candidate>],
        account: &mut A,
    ) -> Result<GlobalCheck<Self::Output>> {
        self.global_calls
            .set(self.global_calls.get().wrapping_add(1));
        Problem::check_global(&mut *self.inner, assignments, account)
    }

    fn initial_evaluations(&self) -> u64 {
        Problem::initial_evaluations(self.inner)
    }
}

fn first_certified_assignment(
    partition: &ExactPartition<'_>,
    emission: &embedding::Emission,
    variables: &[NodeId],
    candidates: &[Vec<XY>],
) -> Option<Vec<XY>> {
    fn visit(
        partition: &ExactPartition<'_>,
        emission: &embedding::Emission,
        variables: &[NodeId],
        candidates: &[Vec<XY>],
        rank: usize,
        selected: &mut Vec<XY>,
        answer: &mut Option<Vec<XY>>,
    ) {
        if answer.is_some() {
            return;
        }
        if rank == variables.len() {
            let mut coordinates = embedding::Embedding::nearest(partition)
                .coordinates()
                .to_vec();
            for (&node, &candidate) in variables.iter().zip(selected.iter()) {
                *coordinates
                    .get_mut(node.0 as usize)
                    .expect("oracle variable belongs to the partition") = candidate;
            }
            let embedding = embedding::Embedding::from_owned(coordinates);
            let segments: Vec<_> = emission
                .items
                .iter()
                .map(|&item| embedding::Embedding::segment_at(embedding.coordinates(), item))
                .collect();
            let mut sink = embedding::FirstFailure::default();
            if embedding::certify(
                partition,
                &embedding,
                &[emission],
                &[segments.as_slice()],
                &mut sink,
            )
            .expect("the exhaustive certificate runs on one fixture emission")
            .is_continue()
            {
                *answer = Some(selected.clone());
            }
            return;
        }
        for &candidate in candidates
            .get(rank)
            .expect("every class variable has one candidate row")
        {
            selected.push(candidate);
            visit(
                partition,
                emission,
                variables,
                candidates,
                rank.wrapping_add(1),
                selected,
                answer,
            );
            selected.pop();
            if answer.is_some() {
                return;
            }
        }
    }

    let mut answer = None;
    visit(
        partition,
        emission,
        variables,
        candidates,
        0,
        &mut Vec::with_capacity(variables.len()),
        &mut answer,
    );
    answer
}

fn assert_collision_class_matches_search(count: usize, compare_oracle: bool) {
    let case = run(&[seg(0.0, 0.0, 2.0, 0.0)]);
    let mut partition = case.partition();
    let nodes = append_lattice_collision_points(&mut partition, count);
    let emission = point_emission(&nodes);
    assert!(
        nodes
            .iter()
            .all(|&node| partition.nearest_xy(node) == XY::new(1.0, 1.0))
    );
    let seed = *nodes.first().expect("collision fixture has a seed");
    let problem = embedding::PlanarProblem::new(&partition, &[&emission], &[seed], vec![
        None;
        partition
            .node_count(
            )
    ])
    .unwrap();
    let classes = Problem::classes(&problem);
    assert_eq!(classes.len(), 1);
    let mut variables = classes
        .first()
        .expect("fixture has one proximity class")
        .clone();
    assert_eq!(variables.len(), count);
    let mut work = Unmetered;
    crate::geometry::try_sort_by_with_work(&mut variables, &mut work, |left, right, work| {
        problem.compare_nodes(*left, *right, work)
    })
    .unwrap();

    let oracle = compare_oracle.then(|| {
        let fixed = problem.fixed_keys(0, &variables, &mut Unmetered).unwrap();
        let mut candidates = Vec::with_capacity(count);
        for &node in &variables {
            let mut values = Vec::new();
            problem
                .fill_candidates(node, &mut values, &mut Unmetered)
                .unwrap();
            crate::geometry::try_sort_by_with_work(&mut values, &mut work, |left, right, work| {
                problem.compare_candidates(node, *left, *right, work)
            })
            .unwrap();
            let mut row = Vec::new();
            for value in values {
                let key = problem.candidate_key(value);
                if !fixed.contains(&key)
                    && !row
                        .iter()
                        .any(|prior: &XY| problem.candidate_key(*prior) == key)
                {
                    row.push(value);
                }
            }
            candidates.push(row);
        }
        first_certified_assignment(&partition, &emission, &variables, &candidates)
            .expect("the exhaustive oracle finds a certified assignment")
    });

    let assignment = Search::new(problem, "embedding_test", count)
        .unwrap()
        .run()
        .unwrap();
    assert_eq!(assignment.classes.len(), 1);
    let solution = assignment
        .classes
        .first()
        .expect("the collision class has one solution");
    assert_eq!(solution.variables, variables);
    if let Some(oracle) = oracle {
        assert_eq!(solution.values, oracle);
    }
    for (index, value) in solution.values.iter().enumerate() {
        assert!(
            !solution
                .values
                .get(..index)
                .expect("prior assignments are in range")
                .contains(value),
            "class members share {value:?}"
        );
    }
}

#[test]
fn small_collision_classes_match_the_exhaustive_production_certificate() {
    for count in [3, 4] {
        assert_collision_class_matches_search(count, true);
    }
}

#[test]
fn five_node_collision_class_has_a_matching_and_distinct_assignment() {
    assert_collision_class_matches_search(5, false);
}

#[test]
fn ten_node_collision_class_is_proved_unrepresentable_by_matching() {
    let case = run(&[seg(0.0, 0.0, 2.0, 0.0)]);
    let mut partition = case.partition();
    let nodes = append_lattice_collision_points(&mut partition, 10);
    let emission = point_emission(&nodes);
    let seed = *nodes.first().expect("collision fixture has a seed");
    let problem = embedding::PlanarProblem::new(&partition, &[&emission], &[seed], vec![
        None;
        partition
            .node_count(
            )
    ])
    .unwrap();
    let classes = Problem::classes(&problem);
    assert_eq!(classes.len(), 1);
    assert_eq!(classes.first().unwrap().len(), 10);
    let error = Search::new(problem, "embedding_test", 10)
        .unwrap()
        .run()
        .unwrap_err();
    assert!(matches!(
        error.kind(),
        crate::error::ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingUnrepresentable {
            operation: "embedding_test",
            constructed: 10
        })
    ));
}

#[test]
fn lattice_crossings_rounding_into_one_stored_vertex_are_repaired() {
    let ulp = 2.0_f64.powi(-52);
    let case = run(&lattice_crossing_group(1.0, ulp));
    let partition = case.partition();
    assert_eq!(constructed_count(&partition), 2);
    let mut crossings = partition.crossings();
    crate::geometry::try_sort_by(&mut crossings, |left, right| {
        partition.compare_nodes(*left, *right)
    })
    .unwrap();
    let third = dyadic(3.0);
    let five = dyadic(5.0);
    let lattice = dyadic(ulp);
    let x_third = third.clone().try_add(lattice.clone()).unwrap();
    let x_fifths = five
        .clone()
        .try_add(lattice.try_product(&dyadic(2.0)).unwrap())
        .unwrap();
    let y_fifth = five.clone().try_add(lattice).unwrap();
    let first = *crossings.first().expect("first exact crossing exists");
    let second = *crossings.get(1).expect("second exact crossing exists");
    assert_exact_at(&partition, first, [&x_third, &third], [&x_third, &third]);
    assert_exact_at(&partition, second, [&x_fifths, &five], [&y_fifth, &five]);
    assert_eq!(partition.nearest_xy(first), XY::new(1.0, 1.0));
    assert_eq!(partition.nearest_xy(second), XY::new(1.0, 1.0));
    let stored = partition.stored_node(XY::new(1.0, 1.0)).unwrap();
    let emission = partition.span_emission(0..u32::try_from(partition.spans.len()).unwrap());
    let problem = embedding::PlanarProblem::new(&partition, &[&emission], &[first], vec![
        None;
        partition
            .node_count(
            )
    ])
    .unwrap();
    let classes = Problem::classes(&problem);
    assert_eq!(classes.len(), 1);
    assert_eq!(classes.first().unwrap(), &vec![first, second]);

    let embedded = partition.embed(&[&emission], "overlay").unwrap();
    assert_eq!(
        embedded.xy(stored).x.to_bits(),
        partition.nearest_xy(stored).x.to_bits()
    );
    assert_eq!(
        embedded.xy(stored).y.to_bits(),
        partition.nearest_xy(stored).y.to_bits()
    );
    assert_ne!(embedded.xy(first), partition.nearest_xy(first));
    assert_ne!(embedded.xy(second), partition.nearest_xy(second));
    assert_ne!(embedded.xy(first), embedded.xy(second));
    for node in [first, second] {
        let exact = partition.exact(node).unwrap();
        let [x, y, weight] = &*exact;
        let emitted = embedded.xy(node);
        let nearest = partition.nearest_xy(node);
        assert_exact_axis_displacement_bound(x, weight, emitted.x, nearest.x);
        assert_exact_axis_displacement_bound(y, weight, emitted.y, nearest.y);
    }
    let segments: Vec<_> = emission
        .items
        .iter()
        .map(|&item| embedding::Embedding::segment_at(embedded.coordinates(), item))
        .collect();
    let mut sink = embedding::FirstFailure::default();
    assert!(
        embedding::certify(
            &partition,
            &embedded,
            &[&emission],
            &[segments.as_slice()],
            &mut sink,
        )
        .unwrap()
        .is_continue()
    );
}

#[test]
fn argentina_uruguay_minimal_crossings_match_the_exhaustive_oracle() {
    let case = run(&argentina_uruguay_minimal_segments());
    let partition = case.partition();
    let at_58 = partition
        .stored_node(XY::new(-58.122_989_509_606_21, -33.022_490_639_567_15))
        .unwrap();
    let at_62 = partition
        .stored_node(XY::new(-58.113_024_156_144_505, -33.016_946_901_572_59))
        .unwrap();
    let first = partition
        .crossings()
        .iter()
        .copied()
        .find(|&node| partition.nearest_xy(node) == partition.nearest_xy(at_58))
        .expect("the 58/367 crossing rounds onto stored node 58");
    let second = partition
        .crossings()
        .iter()
        .copied()
        .find(|&node| partition.nearest_xy(node) == partition.nearest_xy(at_62))
        .expect("the 62/367 crossing rounds onto stored node 62");
    let emission = partition.span_emission(0..u32::try_from(partition.spans.len()).unwrap());
    let problem = embedding::PlanarProblem::new(&partition, &[&emission], &[first, second], vec![
            None;
            partition
                .node_count(
                )
        ])
    .unwrap();
    let classes = Problem::classes(&problem);
    assert_eq!(classes.len(), 1);
    assert_eq!(classes.first().unwrap().len(), 3);
    let mut variables = classes.first().unwrap().clone();
    let mut work = Unmetered;
    crate::geometry::try_sort_by_with_work(&mut variables, &mut work, |left, right, work| {
        problem.compare_nodes(*left, *right, work)
    })
    .unwrap();
    let fixed = problem.fixed_keys(0, &variables, &mut Unmetered).unwrap();
    let mut candidates = Vec::with_capacity(variables.len());
    for &node in &variables {
        let mut values = Vec::new();
        problem
            .fill_candidates(node, &mut values, &mut Unmetered)
            .unwrap();
        crate::geometry::try_sort_by_with_work(&mut values, &mut work, |left, right, work| {
            problem.compare_candidates(node, *left, *right, work)
        })
        .unwrap();
        let mut row = Vec::new();
        for value in values {
            let key = problem.candidate_key(value);
            if !fixed.contains(&key)
                && !row
                    .iter()
                    .any(|prior: &XY| problem.candidate_key(*prior) == key)
            {
                row.push(value);
            }
        }
        candidates.push(row);
    }
    let oracle = first_certified_assignment(&partition, &emission, &variables, &candidates)
        .expect("the exact 58/62/367 component has a certified embedding");
    let assignment = Search::new(problem, "overlay", variables.len())
        .unwrap()
        .run()
        .unwrap();
    assert_eq!(assignment.classes.len(), 1);
    let solution = assignment.classes.first().unwrap();
    assert_eq!(solution.variables, variables);
    assert_eq!(solution.values, oracle);

    let embedding = partition.embed(&[&emission], "overlay").unwrap();
    assert_eq!(embedding.xy(at_58), partition.nearest_xy(at_58));
    assert_eq!(embedding.xy(at_62), partition.nearest_xy(at_62));
    for (&node, &expected) in solution.variables.iter().zip(&solution.values) {
        assert_eq!(embedding.xy(node), expected);
        let exact = partition.exact(node).unwrap();
        let [x, y, weight] = &*exact;
        let emitted = embedding.xy(node);
        let nearest = partition.nearest_xy(node);
        assert_exact_axis_displacement_bound(x, weight, emitted.x, nearest.x);
        assert_exact_axis_displacement_bound(y, weight, emitted.y, nearest.y);
    }
}

#[test]
fn full_ar_uy_symmetric_difference_repairs_nearest_embedding() {
    use crate::geometry::TopologyDomain;
    use crate::geometry::overlay::{Strictness, overlay_all};

    const COUNTRIES: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/fixtures/osm_countries_0_1.geojson"
    ));

    let countries: serde_json::Value = serde_json::from_str(COUNTRIES).unwrap();
    let features = countries["features"].as_array().unwrap();
    let country = |code: &str| {
        let feature = features
            .iter()
            .rev()
            .find(|feature| {
                feature["properties"]["tags"]["ISO3166-1:alpha2"].as_str() == Some(code)
            })
            .unwrap();
        crate::io::parse_geojson(&feature["geometry"]).unwrap()
    };
    let argentina = country("AR");
    let uruguay = country("UY");
    let mut expected_wkb = None;
    for (left, right) in [(&argentina, &uruguay), (&uruguay, &argentina)] {
        let mut operands = [left, right];
        operands.sort_unstable_by(|left, right| crate::geometry::compare_shapes(left, right));
        let inventory = ExactSourceInventory::from_shapes(&operands).unwrap();
        let mut budget = ExpansionBudget::new("symmetric_difference", "AR/UY embedding repair");
        let partition = inventory.partition(&mut budget).unwrap();
        let (topology, edges) = partition.local_topology().unwrap();
        let global = partition.global_faces(&topology, &edges).unwrap();
        let cells = inventory
            .select_cells(
                &partition,
                &topology,
                &edges,
                &global,
                crate::geometry::OverlayOp::SymmetricDifference,
                false,
            )
            .unwrap();
        let plan = cells
            .emission_plan(&partition, &topology, &edges, &global)
            .unwrap();
        let emission = plan.emission(&topology, &edges);
        let emissions = [&emission];
        let nearest = embedding::Embedding::nearest(&partition);
        let nearest_segments: Vec<_> = emission
            .items
            .iter()
            .map(|&item| embedding::Embedding::segment_at(nearest.coordinates(), item))
            .collect();
        let nearest_segment_views = [nearest_segments.as_slice()];
        let mut nearest_failure = embedding::FirstFailure::default();
        assert!(
            embedding::certify(
                &partition,
                &nearest,
                &emissions,
                &nearest_segment_views,
                &mut nearest_failure,
            )
            .unwrap()
            .is_break(),
            "the full country symmetric difference must require embedding repair"
        );

        let repaired = partition.embed(&emissions, "symmetric_difference").unwrap();
        assert!(
            emission.items.iter().flatten().any(|&node| {
                repaired.xy(node) != embedding::Embedding::xy_at(nearest.coordinates(), node)
            }),
            "the full country repair did not move any emitted node"
        );
        let repaired_segments: Vec<_> = emission
            .items
            .iter()
            .map(|&item| embedding::Embedding::segment_at(repaired.coordinates(), item))
            .collect();
        let repaired_segment_views = [repaired_segments.as_slice()];
        let mut repaired_failure = embedding::FirstFailure::default();
        assert!(
            embedding::certify(
                &partition,
                &repaired,
                &emissions,
                &repaired_segment_views,
                &mut repaired_failure,
            )
            .unwrap()
            .is_continue(),
            "the production embedding must satisfy the full output certificate"
        );

        let result = overlay_all(
            &[left, right],
            TopologyDomain::Cartesian,
            crate::geometry::OverlayOp::SymmetricDifference,
            Strictness::Lenient,
        )
        .unwrap();
        assert!(result.validate().unwrap().is_none());
        let wkb = crate::io::to_wkb(&result, None, false).unwrap();
        if let Some(expected) = &expected_wkb {
            assert_eq!(&wkb, expected, "public WKB changed by operand order");
        } else {
            expected_wkb = Some(wkb);
        }
    }
}

#[test]
fn only_free_neighbor_cells_occupied_by_stored_nodes_are_unrepresentable() {
    let ulp = 2.0_f64.powi(-52);
    let segments = [
        seg(
            1.0 - 100.0 * ulp,
            1.0 - 100.0 * ulp,
            1.0 + 100.0 * ulp,
            1.0 + 100.0 * ulp,
        ),
        seg(
            1.0 - 100.0 * ulp,
            1.0 + 201.0 * ulp,
            1.0 + 100.0 * ulp,
            1.0 - 199.0 * ulp,
        ),
    ];
    let mut case = run(&segments);
    case.isolated = [1.0_f64.next_down(), 1.0, 1.0_f64.next_up()]
        .into_iter()
        .flat_map(|x| [1.0_f64.next_down(), 1.0, 1.0_f64.next_up()].map(|y| XY::new(x, y)))
        .collect();
    let partition = case.partition();
    let mut emission = partition.span_emission(0..u32::try_from(partition.spans.len()).unwrap());
    for &point in &case.isolated {
        let node = partition
            .stored_node(point)
            .expect("every isolated neighbor is a stored node");
        emission.items.push([node, node]);
    }
    let error = partition.embed(&[&emission], "overlay").unwrap_err();
    assert!(matches!(
        error.kind(),
        crate::error::ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingUnrepresentable {
            operation: "overlay",
            ..
        })
    ));
}

#[test]
fn exhaustive_contact_failure_rejects_an_injective_certificate_bypass() {
    let down = 1.0_f64.next_down();
    let up = 1.0_f64.next_up();
    let case = run(&[
        seg(down, down, up, down),
        seg(down, 1.0, up, 1.0),
        seg(down, up, up, up),
    ]);
    let mut partition = case.partition();
    let ulp = 2.0_f64.powi(-52);
    let exact = dyadic(1.0).try_add(dyadic(ulp / 4.0)).unwrap();
    let point = append_constructed_point(&mut partition, exact.clone(), exact);
    let mut emission = partition.span_emission(0..u32::try_from(partition.spans.len()).unwrap());
    emission.items.push([point, point]);
    let mut problem = embedding::PlanarProblem::new(&partition, &[&emission], &[point], vec![
        None;
        partition
            .node_count(
            )
    ])
    .unwrap();
    let classes = Problem::classes(&problem);
    assert_eq!(classes.len(), 1);
    assert_eq!(classes.first().unwrap(), &vec![point]);
    let fixed = problem.fixed_keys(0, &[point], &mut Unmetered).unwrap();
    let mut raw_candidates = Vec::new();
    problem
        .fill_candidates(point, &mut raw_candidates, &mut Unmetered)
        .unwrap();
    assert_eq!(raw_candidates.len(), 9);
    let mut work = Unmetered;
    crate::geometry::try_sort_by_with_work(&mut raw_candidates, &mut work, |left, right, work| {
        problem.compare_candidates(point, *left, *right, work)
    })
    .unwrap();
    let mut available = Vec::new();
    for candidate in raw_candidates {
        let key = problem.candidate_key(candidate);
        if !fixed.contains(&key)
            && !available
                .iter()
                .any(|prior: &XY| problem.candidate_key(*prior) == key)
        {
            available.push(candidate);
        }
    }
    assert_eq!(available.len(), 3);
    assert!(
        first_certified_assignment(&partition, &emission, &[point], &[available.clone()],)
            .is_none(),
        "every available candidate touches one of the three unconnected lines"
    );

    let local_calls = std::cell::Cell::new(0);
    let normal_global_calls = std::cell::Cell::new(0);
    let error = Search::new(
        TestVerifier {
            inner: &mut problem,
            local_calls: &local_calls,
            global_calls: &normal_global_calls,
            node_order: None,
            candidate_filter: None,
            bypass_local: false,
            bypass_global: false,
        },
        "overlay",
        1,
    )
    .unwrap()
    .run()
    .unwrap_err();
    assert!(local_calls.get() >= available.len());
    assert!(matches!(
        error.kind(),
        crate::error::ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingUnrepresentable {
            operation: "overlay",
            constructed: 1
        })
    ));

    let bypass_calls = std::cell::Cell::new(0);
    let bypass_global_calls = std::cell::Cell::new(0);
    let bypass = Search::new(
        TestVerifier {
            inner: &mut problem,
            local_calls: &bypass_calls,
            global_calls: &bypass_global_calls,
            node_order: None,
            candidate_filter: None,
            bypass_local: true,
            bypass_global: false,
        },
        "overlay",
        1,
    )
    .unwrap()
    .run()
    .unwrap();
    assert_eq!(bypass_calls.get(), 1);
    let mut coordinates = embedding::Embedding::nearest(&partition)
        .coordinates()
        .to_vec();
    for class in bypass.classes {
        for (node, candidate) in class.variables.into_iter().zip(class.values) {
            *coordinates
                .get_mut(node.0 as usize)
                .expect("mutated assignment node belongs to the partition") = candidate;
        }
    }
    let embedding = embedding::Embedding::from_owned(coordinates);
    let segments: Vec<_> = emission
        .items
        .iter()
        .map(|&item| embedding::Embedding::segment_at(embedding.coordinates(), item))
        .collect();
    let mut witnesses = WitnessLog::default();
    assert!(
        embedding::certify(
            &partition,
            &embedding,
            &[&emission],
            &[segments.as_slice()],
            &mut witnesses,
        )
        .unwrap()
        .is_continue()
    );
    assert!(matches!(witnesses.0.as_slice(), [
        embedding::Witness::Contact { .. }
    ]));
}

#[test]
fn candidate_lattices_use_real_binade_neighbors_and_exact_order() {
    for center in [0.5_f64, 1.0, 2.0, 1024.0] {
        let upper_step = center.next_up() - center;
        let lower_step = center - center.next_down();
        assert_eq!(lower_step, upper_step * 0.5);
        let offset = dyadic(upper_step / 8.0);
        let exact = dyadic(center).try_add(offset).unwrap();
        let (nearest, candidates, ordered) = planar_candidate_values(
            exact.clone(),
            exact.clone(),
            embedding::EmbeddingDomain::Plane,
        );
        assert_eq!(nearest, XY::new(center, center));
        assert_eq!(candidates.len(), 9);
        for x in [center.next_down(), center, center.next_up()] {
            assert!(
                candidates
                    .iter()
                    .any(|candidate| candidate.x.to_bits() == x.to_bits())
            );
        }
        for y in [center.next_down(), center, center.next_up()] {
            assert!(
                candidates
                    .iter()
                    .any(|candidate| candidate.y.to_bits() == y.to_bits())
            );
        }
        let ordered_x: Vec<_> = ordered
            .iter()
            .filter_map(|candidate| {
                (candidate.y.to_bits() == center.to_bits()).then_some(candidate.x)
            })
            .collect();
        assert_eq!(
            ordered_x,
            [center, center.next_down(), center.next_up()],
            "exact displacement order around {center}"
        );
        for candidate in candidates {
            let bound = dyadic(1.5 * upper_step);
            let delta_x = dyadic(candidate.x)
                .try_subtract(exact.clone())
                .unwrap()
                .abs();
            let delta_y = dyadic(candidate.y)
                .try_subtract(exact.clone())
                .unwrap()
                .abs();
            assert_ne!(delta_x.cmp(&bound), Ordering::Greater);
            assert_ne!(delta_y.cmp(&bound), Ordering::Greater);
        }
    }
}

#[test]
fn zero_subnormal_and_finite_extreme_candidates_keep_their_real_keys() {
    let least = f64::from_bits(1);
    let exact_half = dyadic(least).try_product(&dyadic(0.5)).unwrap();
    let (zero, zero_candidates, _) = planar_candidate_values(
        exact_half.clone(),
        exact_half.clone(),
        embedding::EmbeddingDomain::Plane,
    );
    assert_eq!(zero.x, 0.0);
    assert!(
        zero_candidates
            .iter()
            .any(|candidate| candidate.x.to_bits() == (-least).to_bits())
    );
    assert!(
        zero_candidates
            .iter()
            .any(|candidate| candidate.x.to_bits() == 0.0_f64.to_bits())
    );
    assert!(
        zero_candidates
            .iter()
            .any(|candidate| candidate.x.to_bits() == least.to_bits())
    );
    let zero_keys: Vec<_> = zero_candidates
        .iter()
        .map(|candidate| PointKey::new(*candidate))
        .collect();
    for (index, key) in zero_keys.iter().enumerate() {
        assert!(
            !zero_keys
                .get(..index)
                .expect("earlier candidate keys are in range")
                .contains(key)
        );
    }
    assert_eq!(
        PointKey::new(XY::new(-0.0, -0.0)),
        PointKey::new(XY::new(0.0, 0.0))
    );

    for x in [least, -least] {
        let y = if x.is_sign_positive() {
            exact_half.clone()
        } else {
            dyadic(-least).try_product(&dyadic(0.5)).unwrap()
        };
        let (_, candidates, _) =
            planar_candidate_values(dyadic(x), y, embedding::EmbeddingDomain::Plane);
        assert!(
            candidates
                .iter()
                .all(|candidate| { candidate.x.is_finite() && candidate.y.is_finite() })
        );
        let keys: Vec<_> = candidates
            .iter()
            .map(|candidate| PointKey::new(*candidate))
            .collect();
        for (index, key) in keys.iter().enumerate() {
            assert!(
                !keys
                    .get(..index)
                    .expect("earlier candidate keys are in range")
                    .contains(key)
            );
        }
    }

    let max_step = f64::MAX - f64::MAX.next_down();
    let exact_max = dyadic(f64::MAX)
        .try_subtract(dyadic(max_step / 8.0))
        .unwrap();
    let subnormal = dyadic(least).try_product(&dyadic(0.5)).unwrap();
    let (nearest, candidates, _) =
        planar_candidate_values(exact_max, subnormal, embedding::EmbeddingDomain::Plane);
    assert_eq!(nearest.x, f64::MAX);
    assert!(candidates.iter().all(|candidate| candidate.x.is_finite()));
    assert!(
        candidates
            .iter()
            .any(|candidate| candidate.x.to_bits() == f64::MAX.to_bits())
    );
    assert!(
        candidates
            .iter()
            .any(|candidate| candidate.x.to_bits() == f64::MAX.next_down().to_bits())
    );
}

#[test]
fn exactly_represented_candidate_axes_keep_nearest_values() {
    let exact_x = dyadic(0.5);
    let y_step = 0.75_f64.next_up() - 0.75;
    let inexact_y = dyadic(0.75).try_add(dyadic(y_step / 8.0)).unwrap();
    let (nearest, candidates, _) =
        planar_candidate_values(exact_x, inexact_y, embedding::EmbeddingDomain::Plane);

    assert_eq!(nearest.x, 0.5);
    assert!(!candidates.is_empty());
    assert!(
        candidates
            .iter()
            .all(|candidate| candidate.x.to_bits() == nearest.x.to_bits()),
        "an exactly represented axis must retain its nearest coordinate: {candidates:?}"
    );

    let x_step = 0.5_f64.next_up() - 0.5;
    let inexact_x = dyadic(0.5).try_add(dyadic(x_step / 8.0)).unwrap();
    let exact_y = dyadic(0.75);
    let (nearest, candidates, _) =
        planar_candidate_values(inexact_x, exact_y, embedding::EmbeddingDomain::Plane);

    assert_eq!(nearest.y, 0.75);
    assert!(!candidates.is_empty());
    assert!(
        candidates
            .iter()
            .all(|candidate| candidate.y.to_bits() == nearest.y.to_bits()),
        "an exactly represented axis must retain its nearest coordinate: {candidates:?}"
    );
}

#[test]
fn rectangle_and_geographic_domains_filter_boundary_neighbors() {
    let half = 0.5_f64;
    let half_step = half.next_up() - half;
    let exact_x = dyadic(half).try_add(dyadic(half_step / 8.0)).unwrap();
    let three_quarters = 0.75_f64;
    let three_quarters_step = three_quarters.next_up() - three_quarters;
    let exact_y = dyadic(three_quarters)
        .try_add(dyadic(three_quarters_step / 8.0))
        .unwrap();
    let bounds = Bounds::new(half, 0.0, 1.0, 1.0).unwrap();
    let (nearest, candidates, _) = planar_candidate_values(
        exact_x,
        exact_y,
        embedding::EmbeddingDomain::Rectangle(bounds),
    );
    assert_eq!(nearest.x, half);
    assert_eq!(candidates.len(), 6);
    assert!(candidates.iter().all(|candidate| candidate.x >= half));

    let frame = crate::geographic::GeographicFrame::crs84();
    let longitude_step = 180.0 - 180.0_f64.next_down();
    let latitude_step = 90.0 - 90.0_f64.next_down();
    let exact_longitude = dyadic(180.0)
        .try_subtract(dyadic(longitude_step * 0.75))
        .unwrap();
    let exact_latitude = dyadic(90.0)
        .try_subtract(dyadic(latitude_step * 0.75))
        .unwrap();
    let (nearest, candidates, _) = planar_candidate_values(
        exact_longitude,
        exact_latitude,
        embedding::EmbeddingDomain::Geographic(frame),
    );
    assert_eq!(nearest.x, 180.0_f64.next_down());
    assert_eq!(nearest.y, 90.0_f64.next_down());
    assert!(candidates.iter().all(|candidate| {
        candidate.x.abs() <= 180.0
            && candidate.y.abs() <= 90.0
            && candidate.x.to_bits() != 180.0_f64.to_bits()
            && candidate.x.to_bits() != (-180.0_f64).to_bits()
            && candidate.y.to_bits() != 90.0_f64.to_bits()
            && candidate.y.to_bits() != (-90.0_f64).to_bits()
    }));
}

#[test]
fn distant_collision_classes_are_searched_independently() {
    let first_group = lattice_crossing_group(1.0, 2.0_f64.powi(-52));
    let second_group = lattice_crossing_group(8.0, 2.0_f64.powi(-49));
    let case = run(&[
        first_group[0],
        first_group[1],
        first_group[2],
        second_group[0],
        second_group[1],
        second_group[2],
    ]);
    let partition = case.partition();
    let first_emission = lattice_group_emission(&partition, 1.0);
    let second_emission = lattice_group_emission(&partition, 8.0);
    let pair_emission = partition.span_emission(0..u32::try_from(partition.spans.len()).unwrap());
    let first_crossing = lattice_group_crossing(&partition, 1.0);
    let second_crossing = lattice_group_crossing(&partition, 8.0);
    let cache = || vec![None; partition.node_count()];

    let first_problem =
        embedding::PlanarProblem::new(&partition, &[&first_emission], &[first_crossing], cache())
            .unwrap();
    assert_eq!(Problem::classes(&first_problem).len(), 1);
    let first = Search::new(first_problem, "overlay", 2)
        .unwrap()
        .run()
        .unwrap();

    let second_problem =
        embedding::PlanarProblem::new(&partition, &[&second_emission], &[second_crossing], cache())
            .unwrap();
    assert_eq!(Problem::classes(&second_problem).len(), 1);
    let second = Search::new(second_problem, "overlay", 2)
        .unwrap()
        .run()
        .unwrap();

    let pair_problem = embedding::PlanarProblem::new(
        &partition,
        &[&pair_emission],
        &[first_crossing, second_crossing],
        cache(),
    )
    .unwrap();
    let classes = Problem::classes(&pair_problem);
    assert_eq!(classes.len(), 2);
    assert!(classes.iter().all(|class| class.len() == 2));
    let pair = Search::new(pair_problem, "overlay", 4)
        .unwrap()
        .run()
        .unwrap();
    assert!(
        pair.evaluations <= first.evaluations + second.evaluations,
        "paired search used {} evaluations; independent classes used {} + {}",
        pair.evaluations,
        first.evaluations,
        second.evaluations
    );
}

#[test]
fn same_chord_endpoint_domains_retain_the_legal_diagonal_assignment() {
    let line = crate::io::parse_wkt("LINESTRING (0 0, 2 2)").unwrap();
    let triangle =
        crate::io::parse_wkt("POLYGON ((1 0, 1.0000000000000002 4, 1.0000000000000002 8, 1 0))")
            .unwrap();
    let inventory = ExactSourceInventory::from_shapes(&[&line, &triangle]).unwrap();
    let mut budget = ExpansionBudget::new("intersection", "same-chord embedding test");
    let partition = inventory.partition(&mut budget).unwrap();
    let (topology, edges) = partition.local_topology().unwrap();
    let global = partition.global_faces(&topology, &edges).unwrap();
    let cells = inventory
        .select_cells(
            &partition,
            &topology,
            &edges,
            &global,
            crate::geometry::OverlayOp::Intersection,
            false,
        )
        .unwrap();
    let plan = cells
        .emission_plan(&partition, &topology, &edges, &global)
        .unwrap();
    let emission = plan.emission(&topology, &edges);
    assert_eq!(emission.items.len(), 1);
    assert_eq!(emission.chords, 1);
    assert!(emission.faces.is_none());

    let nearest = XY::new(1.0, 1.0);
    let [source_start, source_end] = *emission
        .items
        .first()
        .expect("the selected line/area intersection has one chord");
    let mut endpoints = [source_start, source_end];
    crate::geometry::try_sort_by(&mut endpoints, |left, right| {
        partition.compare_nodes(*left, *right)
    })
    .unwrap();
    let [first, second] = endpoints;
    assert_eq!(partition.nearest_xy(first), nearest);
    assert_eq!(partition.nearest_xy(second), nearest);
    for node in [first, second] {
        let exact = partition.exact(node).unwrap();
        let [x, y, _] = exact.as_ref();
        assert_eq!(x, y, "both exact intersections lie on the source diagonal");
    }
    assert_eq!(
        partition.compare_axis(first, second, Axis::X).unwrap(),
        Ordering::Less
    );
    assert_eq!(
        partition.compare_axis(first, second, Axis::Y).unwrap(),
        Ordering::Less
    );
    let upper = XY::new(nearest.x.next_up(), nearest.y.next_up());
    assert_ne!(PointKey::new(nearest), PointKey::new(upper));

    let mut problem = embedding::PlanarProblem::new(&partition, &[&emission], &[first], vec![
        None;
        partition
            .node_count(
            )
    ])
    .unwrap();
    let allowed_assignment = |node: NodeId, candidate: XY| {
        (node == first && candidate == nearest) || (node == second && candidate == upper)
    };
    let local_calls = std::cell::Cell::new(0);
    let global_calls = std::cell::Cell::new(0);
    let assignment = Search::new(
        TestVerifier {
            inner: &mut problem,
            local_calls: &local_calls,
            global_calls: &global_calls,
            node_order: None,
            candidate_filter: Some(&allowed_assignment),
            bypass_local: false,
            bypass_global: false,
        },
        "intersection",
        2,
    )
    .unwrap()
    .run()
    .expect("the legal nearest/upper joint assignment survives embedding search");
    let solution = assignment
        .classes
        .first()
        .expect("the selected chord's movable endpoints share one prepared class");
    assert_eq!(solution.variables.as_slice(), &[first, second]);
    assert_eq!(solution.values.as_slice(), &[nearest, upper]);

    let mut coordinates = embedding::Embedding::nearest(&partition)
        .coordinates()
        .to_vec();
    for (&node, &candidate) in solution.variables.iter().zip(&solution.values) {
        *coordinates
            .get_mut(node.0 as usize)
            .expect("the selected chord endpoint belongs to the partition") = candidate;
    }
    let embedding = embedding::Embedding::from_owned(coordinates);
    let segments: Vec<_> = emission
        .items
        .iter()
        .map(|&item| embedding::Embedding::segment_at(embedding.coordinates(), item))
        .collect();
    assert_eq!(embedding.xy(first), nearest);
    assert_eq!(embedding.xy(second), upper);
    let mut sink = embedding::FirstFailure::default();
    assert!(
        embedding::certify(
            &partition,
            &embedding,
            &[&emission],
            &[segments.as_slice()],
            &mut sink,
        )
        .unwrap()
        .is_continue()
    );
}

#[test]
fn long_chord_with_disjoint_axiswise_candidate_domains_keeps_independent_classes() {
    let source = run(&[seg(0.0, 0.0, 0.0, 0.0)]);
    let minimum_subnormal = dyadic(f64::from_bits(1));
    let cases = [
        (
            "horizontal",
            dyadic(-1.0e100),
            minimum_subnormal.try_product(&dyadic(0.5)).unwrap(),
            dyadic(1.0e100),
            minimum_subnormal.try_product(&dyadic(0.5)).unwrap(),
        ),
        (
            "vertical",
            dyadic(1.0e100),
            dyadic(-1.0).try_add(dyadic(2.0_f64.powi(-55))).unwrap(),
            dyadic(1.0e100),
            dyadic(1.0).try_add(dyadic(2.0_f64.powi(-54))).unwrap(),
        ),
    ];

    for (name, start_x, start_y, end_x, end_y) in cases {
        let mut partition = source.partition();
        let start = append_constructed_point(&mut partition, start_x, start_y);
        let end = append_constructed_point(&mut partition, end_x, end_y);
        let emission = embedding::Emission {
            items: vec![[start, end]],
            chords: 1,
            areal_chords: 0,
            fan_spans: Vec::new(),
            faces: None,
        };
        let problem = embedding::PlanarProblem::new(&partition, &[&emission], &[start], vec![
            None;
            partition
                .node_count(
                )
        ])
        .unwrap();

        let mut start_candidates = Vec::new();
        problem
            .fill_candidates(start, &mut start_candidates, &mut Unmetered)
            .unwrap();
        let mut end_candidates = Vec::new();
        problem
            .fill_candidates(end, &mut end_candidates, &mut Unmetered)
            .unwrap();
        assert!(
            start_candidates.iter().all(|left| end_candidates
                .iter()
                .all(|right| PointKey::new(*left) != PointKey::new(*right))),
            "{name} endpoint candidate-key domains must be disjoint"
        );
        assert_eq!(
            Problem::classes(&problem),
            &[vec![start]],
            "{name} endpoint domains must remain independently repairable"
        );
    }
}

#[test]
fn local_pair_with_two_movable_left_endpoints_owns_both_in_one_class() {
    let case = run(&[seg(1.0, 0.0, 1.0, 0.0)]);
    let mut partition = case.partition();
    let minimum_subnormal = dyadic(f64::from_bits(1));
    let start = append_constructed_point(
        &mut partition,
        dyadic(0.0),
        minimum_subnormal.try_product(&dyadic(0.125)).unwrap(),
    );
    let end = append_constructed_point(
        &mut partition,
        dyadic(2.0),
        minimum_subnormal.try_product(&dyadic(0.375)).unwrap(),
    );
    let fixed = partition.stored_node(XY::new(1.0, 0.0)).unwrap();
    let emission = embedding::Emission {
        items: vec![[start, end], [fixed, fixed]],
        chords: 1,
        areal_chords: 0,
        fan_spans: Vec::new(),
        faces: None,
    };
    let problem = embedding::PlanarProblem::new(&partition, &[&emission], &[start], vec![
        None;
        partition
            .node_count(
            )
    ])
    .unwrap();
    let assignment = Search::new(problem, "difference", 2)
        .unwrap()
        .run()
        .unwrap();

    assert_eq!(assignment.classes.len(), 1);
    let solution = assignment.classes.first().unwrap();
    assert_eq!(solution.variables.as_slice(), &[start, end]);
    let [line_start, line_end] = solution.values.as_slice() else {
        panic!("the local pair assigns both movable line endpoints");
    };
    let line = Segment {
        start: *line_start,
        end: *line_end,
    };
    let fixed_point = Segment {
        start: XY::new(1.0, 0.0),
        end: XY::new(1.0, 0.0),
    };
    assert_eq!(
        segment_contact_exact(line, fixed_point),
        Contact::Disjoint,
        "the complete local pair must evaluate both movable line endpoints"
    );
}

fn degree_two_reversal_segments() -> [Segment; 3] {
    [
        seg(
            0.999_999_999_999_670_5,
            1.000_000_000_000_199_2,
            1.000_000_000_000_295_5,
            0.999_999_999_999_821_7,
        ),
        seg(
            0.999_999_999_999_786_4,
            1.000_000_000_000_594_4,
            1.000_000_000_000_156,
            0.999_999_999_999_566_3,
        ),
        seg(
            1.000_000_000_000_566_9,
            0.999_999_999_999_632_3,
            0.999_999_999_999_542_1,
            1.000_000_000_000_297_3,
        ),
    ]
}

fn fan_reversal_quad_segments() -> [Segment; 6] {
    let step = 2.0_f64.powi(-52);
    let point = |(x, y): (i32, i32)| XY::new(1.0 + f64::from(x) * step, 1.0 + f64::from(y) * step);
    let [a, b, c, d] = [(0, 9), (9, 5), (11, 5), (3, 8)].map(point);
    [
        seg(a.x, a.y, b.x, b.y),
        seg(b.x, b.y, c.x, c.y),
        seg(c.x, c.y, d.x, d.y),
        seg(d.x, d.y, a.x, a.y),
        seg(a.x, a.y, c.x, c.y),
        seg(b.x, b.y, d.x, d.y),
    ]
}

fn exact_orientation_order(
    partition: &ExactPartition<'_>,
    [first_node, second_node, third_node]: [NodeId; 3],
) -> crate::error::Result<Ordering> {
    let rational_coordinates = |node| -> crate::error::Result<(ExactRational, ExactRational)> {
        let exact = partition.exact(node)?;
        let [x_numerator, y_numerator, homogeneous_weight] = &*exact;
        Ok((
            ExactRational::new(x_numerator.clone(), homogeneous_weight.clone())?,
            ExactRational::new(y_numerator.clone(), homogeneous_weight.clone())?,
        ))
    };
    let (first_x, first_y) = rational_coordinates(first_node)?;
    let (second_x, second_y) = rational_coordinates(second_node)?;
    let (third_x, third_y) = rational_coordinates(third_node)?;
    let first_to_second_x = second_x.try_subtract(&first_x)?;
    let first_to_second_y = second_y.try_subtract(&first_y)?;
    let first_to_third_x = third_x.try_subtract(&first_x)?;
    let first_to_third_y = third_y.try_subtract(&first_y)?;
    first_to_second_x
        .try_product(&first_to_third_y)?
        .try_subtract(&first_to_second_y.try_product(&first_to_third_x)?)?
        .try_cmp(&ExactRational::zero())
}

fn rounded_orientation_order(
    embedding: &embedding::Embedding<'_>,
    [a, b, c]: [NodeId; 3],
) -> Ordering {
    let (a, b, c) = (embedding.xy(a), embedding.xy(b), embedding.xy(c));
    match stored_orientation_sign(a.x, a.y, b.x, b.y, c.x, c.y) {
        ExactSign::Negative => Ordering::Less,
        ExactSign::Zero => Ordering::Equal,
        ExactSign::Positive => Ordering::Greater,
    }
}

fn unique_emission_nodes(emission: &embedding::Emission) -> Vec<NodeId> {
    let mut nodes = Vec::new();
    for &[start, end] in emission
        .items
        .get(..emission.chords)
        .expect("chord count is within emitted items")
    {
        for node in [start, end] {
            if !nodes.contains(&node) {
                nodes.push(node);
            }
        }
    }
    nodes
}

fn emission_for_faces(
    partition: &ExactPartition<'_>,
    topology: &IndexedTopology,
    edges: &[u32],
    global: &GlobalFaces,
    selected_faces: &[u32],
) -> crate::error::Result<embedding::Emission> {
    let mut faces = vec![false; global.count as usize];
    for &face in selected_faces {
        *faces
            .get_mut(face as usize)
            .expect("selected face belongs to this global face table") = true;
    }
    let cells = ArealCells {
        faces,
        spans: vec![false; partition.spans.len()],
        nodes: vec![false; partition.node_count()],
    };
    let plan = cells.emission_plan(partition, topology, edges, global)?;
    Ok(plan.emission(topology, edges))
}

#[test]
fn degree_two_triangle_reversal_is_rejected_by_g_and_published_when_g_is_bypassed() {
    let case = run(&degree_two_reversal_segments());
    let partition = case.partition();
    assert_eq!(constructed_count(&partition), 3);
    let at_origin = partition
        .crossings()
        .iter()
        .copied()
        .find(|&node| partition.nearest_xy(node) == XY::new(1.0, 1.0))
        .expect("first crossing rounds to the origin cell");
    let at_east = partition
        .crossings()
        .iter()
        .copied()
        .find(|&node| partition.nearest_xy(node) == XY::new(1.0_f64.next_up(), 1.0))
        .expect("second crossing rounds east");
    let at_north = partition
        .crossings()
        .iter()
        .copied()
        .find(|&node| partition.nearest_xy(node) == XY::new(1.0, 1.0_f64.next_up()))
        .expect("third crossing rounds north");
    let triangle = [at_origin, at_east, at_north];
    assert_eq!(
        exact_orientation_order(&partition, triangle).unwrap(),
        Ordering::Less
    );
    let nearest = embedding::Embedding::nearest(&partition);
    assert_eq!(
        rounded_orientation_order(&nearest, triangle),
        Ordering::Greater
    );
    let emission = {
        let (topology, edges) = partition.local_topology().unwrap();
        let global = partition.global_faces(&topology, &edges).unwrap();
        emission::polygonize_emission(&partition, &topology, &edges, &global).unwrap()
    };
    assert_eq!(emission.chords, 3);
    assert_eq!(emission.areal_chords, 3);
    assert_eq!(unique_emission_nodes(&emission).len(), 3);
    assert!(unique_emission_nodes(&emission).contains(&at_origin));
    let nearest_segments: Vec<_> = emission
        .items
        .iter()
        .map(|&item| embedding::Embedding::segment_at(nearest.coordinates(), item))
        .collect();
    embedding::check_emitted_fans(
        &partition,
        &nearest,
        &emission.items[..emission.chords],
        &emission.fan_spans,
        &nearest_segments[..emission.chords],
        emission.areal_chords,
    )
    .unwrap();
    let mut nearest_witnesses = WitnessLog::default();
    assert!(
        embedding::certify(
            &partition,
            &nearest,
            &[&emission],
            &[nearest_segments.as_slice()],
            &mut nearest_witnesses,
        )
        .unwrap()
        .is_continue()
    );
    assert!(matches!(nearest_witnesses.0.as_slice(), [
        embedding::Witness::Face { .. }
    ]));

    let mut movable_cache = vec![None; partition.node_count()];
    let mut problem = embedding::PlanarProblem::new(
        &partition,
        &[&emission],
        &triangle,
        std::mem::take(&mut movable_cache),
    )
    .unwrap();
    let constructed = Problem::classes(&problem)
        .iter()
        .map(Vec::len)
        .sum::<usize>();
    assert_eq!(constructed, 3);
    let local_calls = std::cell::Cell::new(0);
    let global_calls = std::cell::Cell::new(0);
    let bypass = Search::new(
        TestVerifier {
            inner: &mut problem,
            local_calls: &local_calls,
            global_calls: &global_calls,
            node_order: None,
            candidate_filter: None,
            bypass_local: false,
            bypass_global: true,
        },
        "polygonize",
        constructed,
    )
    .unwrap()
    .run()
    .unwrap();
    assert_eq!(global_calls.get(), 1);

    let mut bypass_coordinates = nearest.coordinates().to_vec();
    for assignment in &bypass.classes {
        for (&node, &candidate) in assignment.variables.iter().zip(&assignment.values) {
            *bypass_coordinates
                .get_mut(node.0 as usize)
                .expect("bypass assignment node belongs to the partition") = candidate;
        }
    }
    let bypass_embedding = embedding::Embedding::from_owned(bypass_coordinates);
    for node in triangle {
        assert_eq!(bypass_embedding.xy(node), partition.nearest_xy(node));
    }
    let bypass_segments: Vec<_> = emission
        .items
        .iter()
        .map(|&item| embedding::Embedding::segment_at(bypass_embedding.coordinates(), item))
        .collect();
    let mut bypass_witnesses = WitnessLog::default();
    assert!(
        embedding::certify(
            &partition,
            &bypass_embedding,
            &[&emission],
            &[bypass_segments.as_slice()],
            &mut bypass_witnesses,
        )
        .unwrap()
        .is_continue()
    );
    assert!(matches!(bypass_witnesses.0.as_slice(), [
        embedding::Witness::Face { .. }
    ]));

    match partition.embed(&[&emission], "polygonize") {
        Ok(recovered) => {
            let recovered_segments: Vec<_> = emission
                .items
                .iter()
                .map(|&item| embedding::Embedding::segment_at(recovered.coordinates(), item))
                .collect();
            let mut witnesses = WitnessLog::default();
            assert!(
                embedding::certify(
                    &partition,
                    &recovered,
                    &[&emission],
                    &[recovered_segments.as_slice()],
                    &mut witnesses,
                )
                .unwrap()
                .is_continue()
            );
            assert!(witnesses.0.is_empty());
        },
        Err(error) => assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(
                GeometryErrorKind::ExactEmbeddingUnrepresentable { .. }
            )
        )),
    }
}

#[derive(Clone, Copy)]
struct FanCandidateStatus {
    p_clean: bool,
    f_clean: bool,
    g_clean: bool,
}

fn classify_fan_candidates(
    partition: &ExactPartition<'_>,
    emission: &embedding::Emission,
    center: NodeId,
) -> crate::error::Result<Vec<FanCandidateStatus>> {
    let problem = embedding::PlanarProblem::new(partition, &[emission], &[center], vec![
        None;
        partition
            .node_count(
            )
    ])?;
    let classes = Problem::classes(&problem);
    let class = classes
        .iter()
        .position(|members| members.contains(&center))
        .expect("fan origin belongs to one movable class");
    let fixed = problem.fixed_keys(class, &[center], &mut Unmetered)?;
    let mut candidates = Vec::new();
    problem.fill_candidates(center, &mut candidates, &mut Unmetered)?;
    let mut work = Unmetered;
    crate::geometry::try_sort_by_with_work(&mut candidates, &mut work, |left, right, work| {
        problem.compare_candidates(center, *left, *right, work)
    })?;
    let mut seen: Vec<PointKey> = Vec::new();
    candidates.retain(|candidate| {
        let key = problem.candidate_key(*candidate);
        !fixed.contains(&key) && !seen.contains(&key) && {
            seen.push(key);
            true
        }
    });

    let nearest = embedding::Embedding::nearest(partition);
    let mut classified = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        let mut coordinates = nearest.coordinates().to_vec();
        *coordinates
            .get_mut(center.0 as usize)
            .expect("fan origin belongs to the partition") = candidate;
        let embedding = embedding::Embedding::from_owned(coordinates);
        let segments: Vec<_> = emission
            .items
            .iter()
            .map(|&item| embedding::Embedding::segment_at(embedding.coordinates(), item))
            .collect();
        let mut witnesses = WitnessLog::default();
        let _certificate = embedding::certify(
            partition,
            &embedding,
            &[emission],
            &[segments.as_slice()],
            &mut witnesses,
        )?;
        let p_clean = witnesses.0.iter().all(|witness| {
            !matches!(
                witness,
                embedding::Witness::Collision(_, _) | embedding::Witness::Contact { .. }
            )
        });
        let f_clean = !witnesses.0.contains(&embedding::Witness::Fan(center));
        let g_clean = embedding::check_global_faces(
            partition,
            &embedding,
            &emission.items,
            emission
                .faces
                .as_deref()
                .expect("fan-pruning emissions retain global faces"),
        )
        .is_ok();
        classified.push(FanCandidateStatus {
            p_clean,
            f_clean,
            g_clean,
        });
    }
    Ok(classified)
}

#[test]
fn neighbor_float_fan_flip_is_classified_against_p_and_g() {
    let mut case = run(&fan_reversal_quad_segments());
    case.isolated.push(XY::new(7.0, 7.0));
    let partition = case.partition();
    assert_eq!(constructed_count(&partition), 1);
    let center = *partition.crossings().first().unwrap();
    let isolated = *partition
        .isolated
        .first()
        .expect("the unselected point supplies an empty fan row");
    assert_eq!(isolated.0.wrapping_add(1), center.0);
    assert_eq!(
        usize::try_from(center.0).unwrap().wrapping_add(1),
        partition.node_count(),
        "the four-departure fan is the final CSR row"
    );
    let step = 2.0_f64.powi(-52);
    let lattice =
        |(x, y): (i32, i32)| XY::new(1.0 + f64::from(x) * step, 1.0 + f64::from(y) * step);
    let [q0, q1, q2, q3] = [(0, 9), (9, 5), (11, 5), (3, 8)].map(lattice);
    let [q0, q1, q2, q3] = [
        partition.stored_node(q0).unwrap(),
        partition.stored_node(q1).unwrap(),
        partition.stored_node(q2).unwrap(),
        partition.stored_node(q3).unwrap(),
    ];
    let nearest = embedding::Embedding::nearest(&partition);
    assert_eq!(
        nearest.xy(center),
        XY::new(1.0 + 4.0 * 2.0_f64.powi(-52), 1.0 + 8.0 * 2.0_f64.powi(-52))
    );
    let (topology, edges) = partition.local_topology().unwrap();
    let global = partition.global_faces(&topology, &edges).unwrap();
    let face_count = global.count;
    let mut face_01 = None;
    let mut face_12 = None;
    let mut face_23 = None;
    let mut face_30 = None;
    for face in 1..face_count {
        let single = emission_for_faces(&partition, &topology, &edges, &global, &[face]).unwrap();
        let nodes = unique_emission_nodes(&single);
        if [center, q0, q1].iter().all(|node| nodes.contains(node)) {
            face_01 = Some(face);
        }
        if [center, q1, q2].iter().all(|node| nodes.contains(node)) {
            face_12 = Some(face);
        }
        if [center, q2, q3].iter().all(|node| nodes.contains(node)) {
            face_23 = Some(face);
        }
        if [center, q3, q0].iter().all(|node| nodes.contains(node)) {
            face_30 = Some(face);
        }
    }
    let opposite_faces = [
        [
            face_01.expect("the first bounded triangle exists"),
            face_23.expect("the third bounded triangle exists"),
        ],
        [
            face_12.expect("the second bounded triangle exists"),
            face_30.expect("the fourth bounded triangle exists"),
        ],
    ];
    let mut correctness_control = None;
    let mut work_control = None;
    for selected_faces in opposite_faces {
        let emission =
            emission_for_faces(&partition, &topology, &edges, &global, &selected_faces).unwrap();
        let center_degree = emission
            .items
            .get(..emission.areal_chords)
            .expect("areal chord prefix belongs to the emission")
            .iter()
            .filter(|item| item.contains(&center))
            .count();
        if center_degree != 4 {
            continue;
        }
        let statuses = classify_fan_candidates(&partition, &emission, center).unwrap();
        let Some(first_g_accepted) = statuses
            .iter()
            .position(|status| status.p_clean && status.g_clean)
        else {
            continue;
        };
        let accepted = *statuses
            .get(first_g_accepted)
            .expect("accepted candidate index belongs to its status row");
        if !accepted.f_clean {
            correctness_control = Some((selected_faces, accepted));
            break;
        }
        if work_control.is_none()
            && let Some(pruned) = statuses
                .get(..first_g_accepted)
                .expect("earlier candidates precede the accepted candidate")
                .iter()
                .find(|status| status.p_clean && !status.f_clean && !status.g_clean)
        {
            work_control = Some((selected_faces, *pruned));
        }
    }
    let (selected_faces, selected_fan_candidate) = correctness_control
        .or(work_control)
        .expect("the bounded face/candidate table contains an F-sensitive case");
    let emission =
        emission_for_faces(&partition, &topology, &edges, &global, &selected_faces).unwrap();
    let center_degree = emission
        .items
        .get(..emission.areal_chords)
        .expect("areal chord prefix belongs to the emission")
        .iter()
        .filter(|item| item.contains(&center))
        .count();
    assert_eq!(center_degree, 4);
    let mut problem = embedding::PlanarProblem::new(&partition, &[&emission], &[center], vec![
        None;
        partition
            .node_count(
            )
    ])
    .unwrap();
    let classes = Problem::classes(&problem);
    assert_eq!(classes.len(), 1);
    assert_eq!(classes.first().unwrap(), &vec![center]);
    let fixed = problem.fixed_keys(0, &[center], &mut Unmetered).unwrap();
    let mut candidates = Vec::new();
    problem
        .fill_candidates(center, &mut candidates, &mut Unmetered)
        .unwrap();
    let mut work = Unmetered;
    crate::geometry::try_sort_by_with_work(&mut candidates, &mut work, |left, right, work| {
        problem.compare_candidates(center, *left, *right, work)
    })
    .unwrap();
    candidates.retain(|candidate| !fixed.contains(&problem.candidate_key(*candidate)));
    assert!(candidates.contains(&nearest.xy(center)));

    let normal_local_calls = std::cell::Cell::new(0);
    let normal_global_calls = std::cell::Cell::new(0);
    let normal = Search::new(
        TestVerifier {
            inner: &mut problem,
            local_calls: &normal_local_calls,
            global_calls: &normal_global_calls,
            node_order: None,
            candidate_filter: None,
            bypass_local: false,
            bypass_global: false,
        },
        "polygonize",
        1,
    )
    .unwrap()
    .run()
    .unwrap();
    let bypass_global_calls = std::cell::Cell::new(0);
    let without_fan_pruning = Search::new(
        FanBypassVerifier {
            inner: &mut problem,
            global_calls: &bypass_global_calls,
        },
        "polygonize",
        1,
    )
    .unwrap()
    .run()
    .unwrap();
    let normal_assignment = normal.classes.first().unwrap();
    let bypass_assignment = without_fan_pruning.classes.first().unwrap();
    assert!(
        !selected_fan_candidate.g_clean,
        "the bounded quad fixture found an F-only publication rather than a G-pruned candidate"
    );
    assert_eq!(normal_assignment.values, bypass_assignment.values);
    assert!(
        bypass_global_calls.get() > normal_global_calls.get(),
        "same result but no added G work: baseline {}, fan-bypassed {}",
        normal_global_calls.get(),
        bypass_global_calls.get()
    );
}

#[test]
fn node_id_candidate_ordering_breaks_permutation_wkb_determinism() {
    fn serialize_for_order(input_order: &[usize], node_id_order: bool) -> Vec<u8> {
        let anchors = [
            XY::new(100.0, -10.0),
            XY::new(100.0, 0.0),
            XY::new(100.0, 10.0),
            XY::new(100.0, 20.0),
        ];
        let source = anchors
            .iter()
            .map(|point| seg(point.x, point.y, point.x, point.y))
            .collect::<Vec<_>>();
        let case = run(&source);
        let mut partition = case.partition();
        let nodes = append_lattice_collision_points_in_order(&mut partition, 4, input_order);
        let anchor_nodes: Vec<_> = anchors
            .iter()
            .map(|&point| {
                partition
                    .stored_node(point)
                    .expect("every labelled line has a stored anchor")
            })
            .collect();
        let emission = embedding::Emission {
            items: nodes
                .iter()
                .zip(&anchor_nodes)
                .map(|(&node, &anchor)| [node, anchor])
                .collect(),
            chords: nodes.len(),
            areal_chords: 0,
            fan_spans: Vec::new(),
            faces: None,
        };
        let mut problem = embedding::PlanarProblem::new(&partition, &[&emission], &nodes, vec![
                None;
                partition
                    .node_count()
            ])
        .unwrap();
        let local_calls = std::cell::Cell::new(0);
        let global_calls = std::cell::Cell::new(0);
        let by_node_id = |left: NodeId, right: NodeId| left.0.cmp(&right.0);
        let node_order =
            node_id_order.then_some(&by_node_id as &dyn Fn(NodeId, NodeId) -> Ordering);
        let assignment = Search::new(
            TestVerifier {
                inner: &mut problem,
                local_calls: &local_calls,
                global_calls: &global_calls,
                node_order,
                candidate_filter: None,
                bypass_local: true,
                bypass_global: true,
            },
            "embedding_test",
            nodes.len(),
        )
        .unwrap()
        .run()
        .unwrap();
        let mut coordinates = embedding::Embedding::nearest(&partition)
            .coordinates()
            .to_vec();
        for class in assignment.classes {
            for (node, candidate) in class.variables.into_iter().zip(class.values) {
                *coordinates
                    .get_mut(node.0 as usize)
                    .expect("assignment node belongs to this partition") = candidate;
            }
        }
        let embedding = embedding::Embedding::from_owned(coordinates);
        let lines = nodes
            .iter()
            .zip(anchor_nodes)
            .map(|(&node, anchor)| {
                let points = [embedding.xy(node), embedding.xy(anchor)];
                Shape::LineString(LineSeq::from_proven(
                    CoordSeq::try_from_xy(&points).expect("line points are finite"),
                ))
            })
            .collect();
        let output = Shape::geometry_collection(lines).unwrap();
        crate::io::to_wkb(&output, None, false).unwrap()
    }

    let forward_order = [0, 1, 2, 3];
    let reverse_order = [3, 2, 1, 0];
    assert_eq!(
        serialize_for_order(&forward_order, false),
        serialize_for_order(&reverse_order, false),
        "exact node order keeps the serialized embedding stable"
    );
    assert_ne!(
        serialize_for_order(&forward_order, true),
        serialize_for_order(&reverse_order, true),
        "node-ID order changes which exact vertex receives each candidate"
    );
}
