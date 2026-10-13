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
