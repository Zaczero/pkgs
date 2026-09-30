use super::*;

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
    assert_eq!(ranges, [0..1]);
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
        let (_, emitted) = plan.round_items(&partition, &topology).unwrap();
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
        let (_, emitted) = plan.round_items(&partition, &topology).unwrap();
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
fn rounded_chord_can_hit_an_isolated_node_without_node_collision() {
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
    ExactPartition::check_node_injectivity(&nodes, &coordinates).unwrap();
    let emitted = partition
        .round_incident_items(&[[start, crossing]])
        .unwrap();
    assert_eq!(emitted.len(), 1);
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
    let items = plan.incident_items(&topology);
    assert_eq!(items.len(), 2);
    let error = plan.emit_xy_parts(&partition, &topology).unwrap_err();
    assert!(matches!(
        error.kind(),
        crate::error::ErrorKind::Geometry(GeometryErrorKind::ExactUnexpectedContact { .. })
    ));
    assert!(!emitted_pair_preserves_incidence(
        [(start, coordinates[0]), (crossing, coordinates[1])],
        [(isolated, coordinates[2]); 2],
    ));
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
    assert!(ExactPartition::check_node_injectivity(&nodes, &coordinates).is_err());
    ExactPartition::check_node_injectivity(&[first, first], &coordinates).unwrap();
    let endpoint = partition.stored_node(XY::new(2.0, 0.0)).unwrap();
    let nodes = [first, endpoint];
    ExactPartition::check_node_injectivity(&nodes, &partition.round_nodes(&nodes)).unwrap();
    ExactPartition::check_node_injectivity(&[], &[]).unwrap();
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
    .map(|wkt| match crate::io::parse_wkt(wkt).unwrap() {
        crate::geometry::Shape::Polygon(polygon) => polygon,
        _ => unreachable!(),
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
        let start = partition.node_xy(span.start);
        let expected = start.x >= 8.0 || start.y == 0.0;
        assert_eq!(cells.spans[index], expected);
    }
    for (index, &filled) in cells.nodes.iter().enumerate() {
        assert!(
            partition.is_stored(NodeId(index as u32)),
            "stored-only fixture constructed a node"
        );
        let point = partition.node_xy(NodeId(index as u32));
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
            let start = partition.node_xy(span.start);
            assert!(
                partition.is_stored(span.end),
                "stored-only fixture constructed a node"
            );
            let end = partition.node_xy(span.end);
            assert_eq!((start.y, end.y), (0.0, 0.0));
            (start.x.min(end.x), start.x.max(end.x))
        })
        .collect::<Vec<_>>();
    intervals.sort_unstable_by(|left, right| left.0.total_cmp(&right.0));
    assert_eq!(intervals, [(0.0, 2.0), (2.0, 4.0)]);
    let plan = cells
        .emission_plan(&partition, &topology, &edges, &faces)
        .unwrap();
    let (points, lines, polygons) = plan.emit_xy_parts(&partition, &topology).unwrap();
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
    .map(|wkt| match crate::io::parse_wkt(wkt).unwrap() {
        crate::geometry::Shape::Polygon(polygon) => polygon,
        _ => unreachable!(),
    });
    let sources = ArealSources::new(&polygons).unwrap();
    let (unique, provenance) =
        dedup_undirected_segments_sourced(sources.segments.iter().copied()).unwrap();
    let mut budget = ExpansionBudget::new("exact_partition", "items");
    let partition = partition_unique_segments(&unique, &provenance, &[], &mut budget).unwrap();
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
            let point = partition.node_xy(NodeId(index as u32));
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
    let (_, rounded) = plan.round_items(&partition, &topology).unwrap();
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
fn zero_length_sources_keep_owners_at_incident_and_isolated_nodes() {
    let (unique, provenance) = dedup_undirected_segments_sourced([
        seg(1.0, 0.0, 1.0, 0.0),
        seg(0.0, 0.0, 2.0, 0.0),
        seg(1.0, 0.0, 1.0, 0.0),
        seg(3.0, 2.0, 3.0, 2.0),
    ])
    .unwrap();
    let mut budget = ExpansionBudget::new("exact_partition", "items");
    let partition = partition_unique_segments(&unique, &provenance, &[], &mut budget).unwrap();
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
    let polygon = |wkt| match crate::io::parse_wkt(wkt).unwrap() {
        crate::geometry::Shape::Polygon(polygon) => polygon,
        _ => unreachable!(),
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
            .overlay_shape(&partition, crate::geometry::OverlayOp::Union, axes, true)
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
        partition_unique_segments(&self.unique, &self.provenance, &self.isolated, &mut budget)
            .unwrap()
    }
}

fn run(segments: &[Segment]) -> Case {
    Case::from_segments(segments)
}

fn metric_support_for(wkt: &str) -> ExactMetricSupport {
    let shape = crate::io::parse_wkt(wkt).expect("valid metric-support WKT");
    let inventory = ExactSourceInventory::from_shapes(&[&shape]).unwrap();
    let mut budget = ExpansionBudget::new("hausdorff", "metric support");
    let partition = inventory.partition(&mut budget).unwrap();
    inventory.metric_support(&partition).unwrap()
}

fn metric_support_with_boundary_for(wkt: &str) -> ExactMetricSupport {
    let shape = crate::io::parse_wkt(wkt).expect("valid metric-support WKT");
    let inventory = ExactSourceInventory::from_shapes(&[&shape]).unwrap();
    let mut budget = ExpansionBudget::new("maximum_inscribed_circle", "source");
    let partition = inventory.partition(&mut budget).unwrap();
    inventory.metric_support_with_boundary(&partition).unwrap()
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
    let [actual_x, actual_y, actual_w] = &node.homogeneous;
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
        crossing_nodes.push(endpoint.homogeneous.clone());
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
    let faces = inventory.linework_polygons(&mut partition, false).unwrap();
    assert_eq!(faces.len(), 4);
    assert_eq!(faces.iter().map(Polygon::area).sum::<f64>(), 4.0);

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
    let parts = inventory.linework_polygons(&mut partition, true).unwrap();
    assert_eq!(parts.len(), 3);
    assert_eq!(parts.iter().map(Polygon::area).sum::<f64>(), 3.0);
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
        let partition =
            partition_unique_segments(&case.unique, &case.provenance, &case.isolated, &mut budget)
                .unwrap();
        assert_eq!(budget.used(), GENERATED_ITEM_LIMIT);
        assert!(partition.node_count() > 0);
        if growth > 0 {
            let mut short = ExpansionBudget::new("exact_partition", "items");
            short.add(GENERATED_ITEM_LIMIT - growth + 1).unwrap();
            let Err(error) = partition_unique_segments(
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
    let partition = partition_unique_segments(&unique, &provenance, &[], &mut budget).unwrap();
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

/// A ray just above a horizontal span or a vertex touching the query's
/// height passes over them: a point beside a horizontal span meets
/// nothing, and one beside a V meets the V's rising right side, whose
/// east face is the unbounded one either way.
#[test]
fn rays_pass_over_horizontal_spans_and_tangent_vertices() {
    for (segments, contact) in [
        (vec![seg(0.0, 0.0, 2.0, 0.0), seg(4.0, 0.0, 4.0, 0.0)], None),
        (
            vec![
                seg(0.0, 2.0, 2.0, 0.0),
                seg(2.0, 0.0, 3.0, 2.0),
                seg(4.0, 0.0, 4.0, 0.0),
            ],
            Some((XY::new(2.0, 0.0), XY::new(3.0, 2.0))),
        ),
    ] {
        let case = run(&segments);
        let partition = case.partition();
        let (topology, edges) = partition.local_topology().unwrap();
        let query = partition.stored_node(XY::new(4.0, 0.0)).unwrap();
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
        assert_eq!(partition.left_contact(query), expected);
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
    let (_, mut displaced) = plan.round_items(&partition, &topology).unwrap();
    assert_eq!(displaced.len(), 8);
    for segment in &mut displaced[4..] {
        segment.start.x += 20.0;
        segment.end.x += 20.0;
    }
    let items = plan.incident_items(&topology);
    plan.check_emitted_fans(&topology, &displaced).unwrap();
    let error = plan
        .check_global_faces(&partition, &items, &displaced)
        .unwrap_err();
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
                let point = partition.node_xy(node);
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
        empty
            .round_items(&partition, &topology)
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
    assert_eq!(plan.round_items(&partition, &topology).unwrap().1.len(), 8);
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
        assert_eq!(plan.round_items(&partition, &topology).unwrap().1.len(), 3);
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
    let (points, lines, polygons) = plan.emit_xy_parts(&partition, &topology).unwrap();
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
    let (points, lines, polygons) = plan.emit_xy_parts(&partition, &topology).unwrap();
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
        let (points, lines, polygons) = plan.emit_xy_parts(&partition, &topology).unwrap();
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
        let x = partition.node_xy(node).x;
        if polygon.len() == 1 {
            assert!((4.0..=6.0).contains(&x));
        } else {
            assert!(x == 0.0 || x == 10.0);
        }
    }
    assert_eq!(plan.round_items(&partition, &topology).unwrap().1.len(), 12);
    let (points, lines, polygons) = plan.emit_xy_parts(&partition, &topology).unwrap();
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
    assert_eq!(plan.round_items(&partition, &topology).unwrap().1.len(), 8);
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
fn emitted_fan_accepts_rotation_but_rejects_reflection() {
    let case = run(&[
        seg(0.0, 0.0, 2.0, 0.0),
        seg(0.0, 0.0, 0.0, 3.0),
        seg(0.0, 0.0, -4.0, 0.0),
        seg(0.0, 0.0, 0.0, -5.0),
    ]);
    let partition = case.partition();
    let (topology, edges) = partition.local_topology().unwrap();
    let faces = partition.global_faces(&topology, &edges).unwrap();
    let plan = faces
        .boundary_plan(&[0, 1, 2, 3], &topology, &edges, &partition, &[false])
        .unwrap();
    let (_, segments) = plan.round_items(&partition, &topology).unwrap();
    let rotated: Vec<_> = segments
        .iter()
        .map(|segment| Segment {
            start: XY::new(-segment.start.y, segment.start.x),
            end: XY::new(-segment.end.y, segment.end.x),
        })
        .collect();
    plan.check_emitted_fans(&topology, &rotated).unwrap();
    let reflected: Vec<_> = segments
        .iter()
        .map(|segment| Segment {
            start: XY::new(segment.start.x, -segment.start.y),
            end: XY::new(segment.end.x, -segment.end.y),
        })
        .collect();
    let error = plan.check_emitted_fans(&topology, &reflected).unwrap_err();
    assert!(matches!(
        error.kind(),
        crate::error::ErrorKind::Geometry(GeometryErrorKind::ExactCyclicOrder { .. })
    ));
}

#[test]
fn joined_chords_keep_endpoint_dart_order() {
    let case = run(&[
        seg(0.0, 0.0, 1.0, 0.0),
        seg(1.0, 0.0, 2.0, 0.0),
        seg(0.0, 0.0, 0.0, 1.0),
        seg(0.0, 1.0, 0.0, 3.0),
        seg(0.0, 0.0, -1.0, 0.0),
        seg(-1.0, 0.0, -4.0, 0.0),
        seg(0.0, 0.0, 0.0, -1.0),
        seg(0.0, -1.0, 0.0, -5.0),
    ]);
    let partition = case.partition();
    let (topology, _) = partition.local_topology().unwrap();
    let origin = partition.stored_node(XY::new(0.0, 0.0)).unwrap();
    let mut darts = Vec::new();
    let mut chords = Vec::new();
    for endpoint in [
        XY::new(2.0, 0.0),
        XY::new(0.0, 3.0),
        XY::new(-4.0, 0.0),
        XY::new(0.0, -5.0),
    ] {
        let leaf = partition.stored_node(endpoint).unwrap();
        let end_slot = topology.starts[leaf.0 as usize];
        let middle = topology.targets[end_slot as usize];
        let start_slot = (topology.starts[origin.0 as usize]
            ..topology.starts[origin.0 as usize + 1])
            .find(|&slot| topology.targets[slot as usize] == middle)
            .unwrap();
        darts.push([start_slot, end_slot]);
        chords.push(Segment {
            start: XY::new(0.0, 0.0),
            end: endpoint,
        });
    }
    ExactPartition::check_emitted_fans(&topology, &darts, &chords).unwrap();
    let plan = BoundaryPlan {
        source_darts: darts
            .iter()
            .flat_map(|&[start, end]| [start, topology.twins[end as usize]])
            .collect(),
        walks: Vec::new(),
        lines: (0..4)
            .map(|index| PlannedChord {
                sources: index * 2..index * 2 + 2,
                sides: [0, 0],
            })
            .collect(),
        points: Vec::new(),
        face_quotient: vec![0],
    };
    let (_, emitted) = plan.round_items(&partition, &topology).unwrap();
    assert_eq!(
        emitted
            .iter()
            .map(|segment| (segment.start, segment.end))
            .collect::<Vec<_>>(),
        [
            (XY::new(0.0, 0.0), XY::new(2.0, 0.0)),
            (XY::new(0.0, 0.0), XY::new(0.0, 3.0)),
            (XY::new(0.0, 0.0), XY::new(-4.0, 0.0)),
            (XY::new(0.0, 0.0), XY::new(0.0, -5.0)),
        ]
    );
    chords.swap(1, 2);
    let error = ExactPartition::check_emitted_fans(&topology, &darts, &chords).unwrap_err();
    assert!(matches!(error.kind(), crate::error::ErrorKind::Geometry(
        GeometryErrorKind::ExactCyclicOrder { node }
    ) if *node == origin.0));
}

#[test]
fn point_left_by_omitted_spoke_keeps_its_bounded_host() {
    let case = run(&[
        seg(0.0, 0.0, 4.0, 0.0),
        seg(4.0, 0.0, 4.0, 4.0),
        seg(4.0, 4.0, 0.0, 4.0),
        seg(0.0, 4.0, 0.0, 0.0),
        seg(0.0, 0.0, 2.0, 2.0),
    ]);
    let partition = case.partition();
    let (topology, edges) = partition.local_topology().unwrap();
    let faces = partition.global_faces(&topology, &edges).unwrap();
    let point = partition.stored_node(XY::new(2.0, 2.0)).unwrap();
    let mut cells = ArealCells {
        faces: vec![false; faces.count as usize],
        spans: vec![true; partition.spans.len()],
        nodes: vec![false; partition.node_count()],
    };
    for (index, span) in partition.spans.iter().enumerate() {
        if span.start == point || span.end == point {
            cells.spans[index] = false;
        }
    }
    cells.nodes[point.0 as usize] = true;
    let plan = cells
        .emission_plan(&partition, &topology, &edges, &faces)
        .unwrap();
    assert_eq!(plan.points, [(point, 1)]);
    assert_eq!(plan.lines.len(), 4);
    assert!(plan.walks.is_empty());
    let items = plan.incident_items(&topology);
    assert_eq!(items.len(), 5);
    assert_eq!(items[4], [point, point]);
    let (_, rounded) = plan.round_items(&partition, &topology).unwrap();
    assert_eq!(rounded[4].start, XY::new(2.0, 2.0));
    assert_eq!(rounded[4].end, XY::new(2.0, 2.0));
    let reflected: Vec<_> = rounded
        .iter()
        .map(|segment| Segment {
            start: XY::new(segment.start.x, -segment.start.y),
            end: XY::new(segment.end.x, -segment.end.y),
        })
        .collect();
    plan.check_emitted_fans(&topology, &reflected).unwrap();
    let error = plan
        .check_global_faces(&partition, &items, &reflected)
        .unwrap_err();
    assert!(matches!(
        error.kind(),
        crate::error::ErrorKind::Geometry(GeometryErrorKind::ExactGlobalFace { .. })
    ));
    let mut displaced = rounded;
    displaced[4] = seg(6.0, 6.0, 6.0, 6.0);
    let error = plan
        .check_global_faces(&partition, &items, &displaced)
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
    let partition = partition_unique_segments(&unique, &provenance, &[], &mut budget).unwrap();
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
        partition_unique_segments(&unique, &provenance, &[XY::new(1.0, 0.0)], &mut budget).unwrap();
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
        partition_unique_segments(&unique, &provenance, &[XY::new(1.0, 1.0)], &mut budget).unwrap();
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
        partition.node_xy(crossing_node(&partition)),
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
            shared_roundings += usize::from(partition.node_xy(first) == partition.node_xy(second));
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
            partition.node_xy(crossing),
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
        let point = partition.node_xy(node);
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
