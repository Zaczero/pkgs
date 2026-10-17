#[test]
fn q_component_hosts_match_nested_shell_point_location_expectations() -> Result<()> {
    let outer = crate::io::parse_wkt("POLYGON ((0 0, 10 0, 10 10, 0 10, 0 0))")?;
    let inner = crate::io::parse_wkt("POLYGON ((3 3, 7 3, 7 7, 3 7, 3 3))")?;
    let outside = Shape::Point(Point::new_raw_xy(15.0, 5.0));
    let annulus = Shape::Point(Point::new_raw_xy(2.0, 5.0));
    let outer_location = test_topology(std::slice::from_ref(&outer));
    let inner_location = test_topology(std::slice::from_ref(&inner));
    let shapes = [
        Shape::geometry_collection(vec![outer, outside])?,
        Shape::geometry_collection(vec![inner, annulus])?,
    ];
    let topology = test_topology(&shapes);
    assert_eq!(
        outer_location
            .locate_point(Point::new_raw_xy(15.0, 5.0))?
            .area,
        crate::geometry::Loc::Exterior
    );
    assert_eq!(
        outer_location
            .locate_point(Point::new_raw_xy(2.0, 5.0))?
            .area,
        crate::geometry::Loc::Interior
    );
    assert_eq!(
        inner_location
            .locate_point(Point::new_raw_xy(2.0, 5.0))?
            .area,
        crate::geometry::Loc::Exterior
    );

    let source_point_index = |x: f64, y: f64| -> Result<usize> {
        topology
            .sources
            .points
            .iter()
            .position(|source| {
                source.point.x.to_bits() == x.to_bits() && source.point.y.to_bits() == y.to_bits()
            })
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("host witness point is missing".into()).into()
            })
    };
    let face_rows_for_point = |index: usize| -> Result<Vec<SourceRowId>> {
        let face = topology
            .q
            .source_point_faces
            .get(index)
            .copied()
            .flatten()
            .ok_or_else(|| GeometryErrorKind::Invalid("host witness has no face".into()))?;
        let mut rows = Vec::new();
        for row in 0..topology.sources.row_count {
            let row = SourceRowId(u32::try_from(row).map_err(|_| ResourceError::SizeOverflow)?);
            if topology.q.face_rows.contains(face, row) {
                rows.push(row);
            }
        }
        Ok(rows)
    };
    let outside_index = source_point_index(15.0, 5.0)?;
    let annulus_index = source_point_index(2.0, 5.0)?;
    assert!(face_rows_for_point(outside_index)?.is_empty());
    assert_eq!(
        face_rows_for_point(annulus_index)?,
        [SourceRowId(0)],
        "the point between the nested shells belongs to the outer row"
    );

    let source_polygon_node = |row: SourceRowId| -> Result<QNodeId> {
        let polygon = topology
            .sources
            .polygons
            .iter()
            .find(|polygon| polygon.row == row)
            .ok_or_else(|| GeometryErrorKind::Invalid("host witness polygon is missing".into()))?;
        let ring = topology
            .sources
            .rings
            .get(polygon.rings.start)
            .ok_or_else(|| GeometryErrorKind::Invalid("host witness ring is missing".into()))?;
        let edge = topology
            .sources
            .edges
            .get(ring.source_edges.start)
            .ok_or_else(|| GeometryErrorKind::Invalid("host witness edge is missing".into()))?;
        let port_id = *topology
            .q
            .source_edge_ports
            .get(edge.id.0 as usize)
            .and_then(|ports| ports.first())
            .ok_or_else(|| GeometryErrorKind::Invalid("host witness port is missing".into()))?;
        topology
            .q
            .ports
            .get(port_id.0 as usize)
            .map(|port| port.node)
            .ok_or_else(|| GeometryErrorKind::Invalid("host witness port is unknown".into()).into())
    };
    let plan = build_q_plan(&topology, OverlayOp::Difference)?;
    let source_points = source_point_coordinates(&topology)?;
    let nearest = QCoordinates::build(&topology)?;
    let publication = plan.fragment_recipes.publish(
        &topology,
        &nearest,
        OverlayOp::Difference.name(),
        &mut crate::work::Unmetered,
    )?;
    let emission = build_q_emission_unmetered(&plan, &topology, &source_points, &publication)?;
    let exact_chart = subgraph_chart(
        &topology,
        &emission,
        &nearest,
        None,
        &mut crate::work::Unmetered,
    )?;
    let exact_faces = QFaceStructure::build(&topology, &emission, exact_chart)?;

    let component_for_node = |node: QNodeId| -> Result<usize> {
        exact_faces
            .component_nodes
            .iter()
            .position(|nodes| nodes.contains(&node))
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("host witness component is missing".into()).into()
            })
    };
    let host_for_component = |component: usize| -> Result<GlobalFaceId> {
        exact_faces
            .global
            .component_hosts
            .get(component)
            .copied()
            .flatten()
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("host witness component has no host".into()).into()
            })
    };
    let host_for_point = |index: usize| -> Result<GlobalFaceId> {
        topology
            .q
            .source_point_faces
            .get(index)
            .copied()
            .flatten()
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("host witness point has no face".into()).into()
            })
    };
    let outer_node = source_polygon_node(SourceRowId(0))?;
    let inner_node = source_polygon_node(SourceRowId(1))?;
    assert_eq!(
        host_for_component(component_for_node(outer_node)?)?,
        host_for_point(outside_index)?,
        "the outer ring component is hosted in the exterior face"
    );
    assert_eq!(
        host_for_component(component_for_node(inner_node)?)?,
        host_for_point(annulus_index)?,
        "the inner ring component is hosted in the outer polygon's annulus"
    );
    Ok(())
}

#[test]
fn candidate_subgraph_rebuilds_cut_ports_on_a_full_longitude_band() -> Result<()> {
    let band = crate::io::parse_wkt(
        "POLYGON ((-180 20, -90 20, 0 20, 90 20, 180 20, 180 30, 90 30, 0 30, -90 30, -180 30, -180 20))",
    )?;
    let topology = test_topology(&[band]);
    let plan = build_q_plan(&topology, OverlayOp::Union)?;
    assert!(!plan.whole_sphere);
    let source_points = source_point_coordinates(&topology)?;
    let coordinates = QCoordinates::build(&topology)?;
    let publication = plan.fragment_recipes.publish(
        &topology,
        &coordinates,
        OverlayOp::Union.name(),
        &mut crate::work::Unmetered,
    )?;
    let emission = build_q_emission_unmetered(&plan, &topology, &source_points, &publication)?;
    let selected_cut_ports = emission
        .spans
        .iter()
        .filter_map(|&span| topology.q.spans.get(span))
        .flat_map(|span| span.ports)
        .filter(|port| {
            topology
                .q
                .ports
                .get(port.0 as usize)
                .is_some_and(|port| port.cut)
        })
        .count();
    assert!(
        selected_cut_ports > 0,
        "the full-longitude band must exercise paired chart-cut ports"
    );

    let exact_chart = subgraph_chart(
        &topology,
        &emission,
        &coordinates,
        None,
        &mut crate::work::Unmetered,
    )?;
    let exact_faces = QFaceStructure::build(&topology, &emission, exact_chart)?;
    let candidate_chart = subgraph_chart(
        &topology,
        &emission,
        &coordinates,
        Some(&coordinates),
        &mut crate::work::Unmetered,
    )?;
    let candidate_faces = QFaceStructure::build(&topology, &emission, candidate_chart)?;
    assert!(
        face_structure_mismatch_nodes(&exact_faces, &candidate_faces, &emission.live_nodes)
            .is_none(),
        "nearest cut reconstruction must preserve the quotient faces"
    );
    Ok(())
}

#[test]
fn b3_island_ar_uy_source_arcs_symdiff_wkb_is_identical_across_all_orders() -> Result<()> {
    let shapes = b3_border_repair_shapes();
    let orders = [[0, 1, 2], [0, 2, 1], [1, 0, 2], [1, 2, 0], [2, 0, 1], [
        2, 1, 0,
    ]];
    let mut expected = None;
    for order in orders {
        let operands = order.map(|index| shapes[index].clone());
        let topology = test_topology(&operands);
        let result = exact_geographic_overlay::<crate::crs::GeodesicMetric>(
            &topology,
            OverlayOp::SymmetricDifference,
            CoordinateAxes::XY,
            false,
            None,
        )?;
        let bytes = crate::io::to_wkb(&result, None, false)?;
        if let Some(first) = &expected {
            assert_eq!(
                &bytes, first,
                "island/AR/UY source-arc symmetric difference WKB differs in order {order:?}"
            );
        } else {
            expected = Some(bytes);
        }
    }
    Ok(())
}

#[test]
fn b3_island_ar_uy_source_arcs_symdiff_is_valid_and_identical_in_all_orders() -> Result<()> {
    let shapes = b3_border_repair_shapes();
    assert_eq!(shapes.len(), 3);
    let orders = [[0, 1, 2], [0, 2, 1], [1, 0, 2], [1, 2, 0], [2, 0, 1], [
        2, 1, 0,
    ]];
    let mut expected = None;
    for order in orders {
        let operands = order.map(|index| shapes[index].clone());
        let topology = test_topology(&operands);
        let plan = build_q_plan(&topology, OverlayOp::SymmetricDifference)?;
        let source_points = source_point_coordinates(&topology)?;
        let nearest = QCoordinates::build(&topology)?;
        let nearest_nodes = nearest.node.clone();
        let mut live_node_keys = plan.live_node_keys(&topology, &nearest, &source_points)?;
        let identity_conflicts = node_injectivity_collisions(&mut live_node_keys)?;
        let embedding = resolve_q_embedding_xy(
            &topology,
            &plan,
            &source_points,
            &nearest,
            OverlayOp::SymmetricDifference.name(),
        )?;
        let certificate_path = embedding.choice;
        let moved_nodes = nearest_nodes
            .iter()
            .zip(&embedding.coordinates.node)
            .enumerate()
            .filter_map(|(index, (before, after))| (before != after).then_some(index))
            .collect::<Vec<_>>();
        let result = publish_q_overlay(embedding)?;
        let issue = crate::geometry::validate_shape_in_frame(
            &result,
            crate::geometry::TopologyDomain::Geographic(GeographicFrame::crs84()),
        )?;
        if let Some(issue) = issue {
            return Err(GeometryErrorKind::Invalid(
                format!(
                    "island/AR/UY source-arc symmetric difference is invalid in operand order {order:?}: {issue:?}; certificate={certificate_path:?}, identity_conflicts={identity_conflicts:?}, moved_nodes={moved_nodes:?}"
                )
                .into(),
            )
            .into());
        }
        let bytes = crate::io::to_wkb(&result, None, false)?;
        if let Some(first) = &expected {
            assert_eq!(
                &bytes, first,
                "island/AR/UY source-arc symmetric difference must be byte-identical in every order"
            );
        } else {
            expected = Some(bytes);
        }
    }
    Ok(())
}

#[test]
fn b3_rebuild_and_compare_certifies_the_ar_uy_island_neighbor_assignment() {
    let shapes = b3_border_repair_shapes();
    assert_eq!(shapes.len(), 3);
    let topology = test_topology(&shapes);
    let plan = build_q_plan(&topology, OverlayOp::SymmetricDifference).expect("Q plan");
    assert!(!plan.whole_sphere);
    let source_points = source_point_coordinates(&topology).expect("source points");
    let coordinates = QCoordinates::build(&topology).expect("nearest Q coordinates");
    let (spans, point_nodes) = b3_selected_subgraph(&topology, &plan)
        .expect("selected H")
        .expect("area H");
    let selected_cut_nodes = spans
        .iter()
        .flat_map(|&span_index| {
            topology
                .q
                .spans
                .get(span_index)
                .into_iter()
                .flat_map(|span| span.ports)
        })
        .filter_map(|port_id| topology.q.ports.get(port_id.0 as usize))
        .filter(|port| port.cut)
        .map(|port| port.node)
        .collect::<BTreeSet<_>>();
    assert!(
        selected_cut_nodes.is_empty(),
        "the AR/UY/island witness must not rely on unchanged cut ports"
    );

    let exact = super::super::q::b3_subgraph_face_structure(
        topology.sources.frame,
        &topology.q,
        &spans,
        &point_nodes,
        None,
    )
    .expect("exact H rebuild");
    let exact_again = super::super::q::b3_subgraph_face_structure(
        topology.sources.frame,
        &topology.q,
        &spans,
        &point_nodes,
        None,
    )
    .expect("second exact H rebuild");
    assert!(b3_face_structures_match(&exact, &exact_again));

    let nearest_error = b3_compare_assignment(
        &topology,
        &plan,
        &source_points,
        &coordinates,
        &spans,
        &point_nodes,
        &exact,
    )
    .expect_err("nearest collision bypass must be rejected");
    assert!(
        nearest_error.to_string().contains("distinct nodes"),
        "nearest bypass was rejected for an unrelated reason: {nearest_error}"
    );

    let constructed =
        b3_constructed_collision_nodes(&plan, &topology, &coordinates, &source_points)
            .expect("nearest collision groups");
    assert_eq!(
        constructed.len(),
        2,
        "the source arcs have two constructed collisions"
    );
    let mut witness = QCoordinates {
        node: coordinates.node.clone(),
        longitude_periods: coordinates.longitude_periods.clone(),
        pole_port_longitude: coordinates.pole_port_longitude.clone(),
        pole_port_periods: coordinates.pole_port_periods.clone(),
    };
    for node in &constructed {
        let xy = witness
            .node
            .get_mut(node.0 as usize)
            .expect("constructed Q node coordinate");
        xy.x = xy.x.next_down();
    }
    for node in &constructed {
        let nearest = coordinates
            .node
            .get(node.0 as usize)
            .expect("nearest constructed Q node coordinate");
        let assigned = witness
            .node
            .get(node.0 as usize)
            .expect("assigned constructed Q node coordinate");
        assert_eq!(assigned.x.to_bits(), nearest.x.next_down().to_bits());
        assert_eq!(assigned.y.to_bits(), nearest.y.to_bits());
    }
    assert!(
        b3_compare_assignment(
            &topology,
            &plan,
            &source_points,
            &witness,
            &spans,
            &point_nodes,
            &exact,
        )
        .expect("the study's neighbor assignment is injective"),
        "the exact subgraph faces must match the study's certified assignment"
    );

    let mut swap_pairs = BTreeSet::new();
    for &span_index in &spans {
        let span = topology
            .q
            .spans
            .get(span_index)
            .expect("selected span for swap control");
        let [start_node, end_node] = span.nodes;
        if start_node == end_node
            || span.ports.iter().any(|port_id| {
                topology
                    .q
                    .ports
                    .get(port_id.0 as usize)
                    .is_some_and(|port| port.cut)
            })
        {
            continue;
        }
        let pair = if start_node < end_node {
            (start_node, end_node)
        } else {
            (end_node, start_node)
        };
        swap_pairs.insert(pair);
    }
    let mut swap_face_mismatch = None;
    let mut swap_rebuild_error = None;
    for (left, right) in swap_pairs {
        let left_xy = witness
            .node
            .get(left.0 as usize)
            .copied()
            .expect("left swap coordinate");
        let right_xy = witness
            .node
            .get(right.0 as usize)
            .copied()
            .expect("right swap coordinate");
        if (left_xy.x - right_xy.x).abs() >= 1.0
            || (left_xy.y - right_xy.y).abs() >= 1.0
            || topology
                .q
                .nodes
                .get(left.0 as usize)
                .and_then(|node| topology.q.node_ports.get(node.ports.clone()))
                .is_some_and(|ports| {
                    ports.iter().any(|port_id| {
                        witness
                            .pole_port_longitude
                            .get(port_id.0 as usize)
                            .is_some_and(Option::is_some)
                    })
                })
            || topology
                .q
                .nodes
                .get(right.0 as usize)
                .and_then(|node| topology.q.node_ports.get(node.ports.clone()))
                .is_some_and(|ports| {
                    ports.iter().any(|port_id| {
                        witness
                            .pole_port_longitude
                            .get(port_id.0 as usize)
                            .is_some_and(Option::is_some)
                    })
                })
        {
            continue;
        }
        let mut swapped = QCoordinates {
            node: witness.node.clone(),
            longitude_periods: witness.longitude_periods.clone(),
            pole_port_longitude: witness.pole_port_longitude.clone(),
            pole_port_periods: witness.pole_port_periods.clone(),
        };
        swapped.node.swap(left.0 as usize, right.0 as usize);
        let Ok(mut live) = plan.live_node_keys(&topology, &swapped, &source_points) else {
            continue;
        };
        if certify_node_injectivity(&mut live).is_err() {
            continue;
        }
        match super::super::q::b3_subgraph_face_structure(
            topology.sources.frame,
            &topology.q,
            &spans,
            &point_nodes,
            Some((&swapped.node, &swapped.pole_port_longitude)),
        ) {
            Ok(emitted) if !b3_face_structures_match(&exact, &emitted) => {
                let stage = if b3_same_rotation_system(
                    &exact.outgoing,
                    &emitted.outgoing,
                    &exact.outgoing_offsets,
                ) {
                    "global_face_structure"
                } else {
                    "rotation_system"
                };
                swap_face_mismatch = Some((left, right, stage));
                break;
            },
            Ok(_) => {},
            Err(error) => {
                swap_rebuild_error.get_or_insert_with(|| (left, right, error.to_string()));
            },
        }
    }
    let swap_rejected = swap_face_mismatch.is_some() || swap_rebuild_error.is_some();
    assert!(
        swap_rejected,
        "no collision-free swap across a selected chord reached and failed the emitted rebuild or face comparison"
    );
}
