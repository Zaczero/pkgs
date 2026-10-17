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
    let (small, large) = (orientations(2), orientations(4));
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
    assert_eq!(
        points
            .iter()
            .map(crate::geometry::Point::xy)
            .collect::<Vec<_>>(),
        [XY::new(10.0, 3.0)]
    );
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
