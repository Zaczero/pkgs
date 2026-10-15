#[test]
fn mixed_geographic_overlay_routes_a_retained_source_point_through_q() {
    let frame = GeographicFrame::crs84();
    let polygon = Shape::Polygon(Polygon::new(
        Ring::try_from_closed(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(170.0, 2.0),
                Point::new_raw_xy(-170.0, 2.0),
                Point::new_raw_xy(-165.0, 12.0),
                Point::new_raw_xy(160.0, 10.0),
                Point::new_raw_xy(170.0, 2.0),
            ])
            .unwrap(),
        )
        .unwrap(),
        Vec::new(),
    ));
    let source_point = Point::new_raw_xy(0.0, -0.0);
    let shapes = [polygon, Shape::Point(source_point)];
    let inputs =
        GeographicInputs::try_from_shapes(frame, shapes.iter()).expect("sources admit");
    assert_eq!(
        crate::geometry::geographic_topology_route(inputs.views().iter().copied()),
        crate::geometry::GeographicTopologyRoute::Quotient
    );

    let output = overlay_all(
        &shapes,
        crate::geometry::TopologyDomain::Geographic(frame),
        OverlayOp::Union,
        Strictness::Lenient,
    )
    .expect("mixed geographic union emits");
    let mut points = Vec::new();
    collect_points(&output, &mut points);
    assert!(points.iter().any(|point| {
        point.x.to_bits() == source_point.x.to_bits()
            && point.y.to_bits() == source_point.y.to_bits()
    }));
}

#[test]
fn geographic_search_moves_a_constructed_vertex_off_a_rounded_source_vertex() {
    let left = crate::io::parse_wkt("POLYGON ((1 0, 2 0, 2 1, 1 1, 1 0))")
        .expect("parse left polygon");
    let right = crate::io::parse_wkt(
        "POLYGON ((0 0.022727272727272728, 45 -1, 45 -2, 0 -2, 0 0.022727272727272728))",
    )
    .expect("parse right polygon");
    let shapes = [left, right];
    let topology = test_topology(&shapes);
    let plan = build_q_plan(&topology, OverlayOp::Intersection).expect("build Q plan");
    let source_points = source_point_coordinates(&topology).expect("source point coordinates");
    let coordinates = QCoordinates::build(&topology).expect("nearest Q coordinates");
    let mut live = plan
        .live_node_keys(&topology, &coordinates, &source_points)
        .expect("nearest live nodes");
    assert_eq!(
        node_injectivity_collisions(&mut live)
            .expect("nearest node collisions")
            .len(),
        2
    );

    let result = exact_geographic_overlay::<crate::crs::GeodesicMetric>(
        &topology,
        OverlayOp::Intersection,
        CoordinateAxes::XY,
        false,
        None,
    )
    .expect("Q embedding search preserves the tiny intersection");
    let Shape::Polygon(polygon) = result else {
        panic!("the overlapping sliver remains one polygon");
    };
    let vertices = polygon.shell.coords().iter().collect::<Vec<_>>();
    assert_eq!(vertices.len(), 4);
    assert!(
        vertices.iter().any(|point| {
            point.x.to_bits() == 1.000_000_000_000_000_2_f64.to_bits()
                && point.y.to_bits() == 0.0_f64.to_bits()
        }),
        "the collision-free crossing uses the next longitude with its exact latitude: {vertices:?}"
    );
}

#[test]
fn geographic_face_certificate_rejects_a_collision_free_neighbor_with_changed_rotation()
-> Result<()> {
    let left = crate::io::parse_wkt("POLYGON ((1 0, 2 0, 2 1, 1 1, 1 0))")?;
    let right = crate::io::parse_wkt(
        "POLYGON ((0 0.022727272727272728, 45 -1, 45 -2, 0 -2, 0 0.022727272727272728))",
    )?;
    let topology = test_topology(&[left, right]);
    let plan = build_q_plan(&topology, OverlayOp::Intersection)?;
    let source_points = source_point_coordinates(&topology)?;
    let nearest = QCoordinates::build(&topology)?;
    let publication = plan.fragment_recipes.publish(
        &topology,
        &nearest,
        OverlayOp::Intersection.name(),
        &mut crate::work::Unmetered,
    )?;
    let emission = build_q_emission_unmetered(&plan, &topology, &source_points, &publication)?;
    let mut live = plan.live_node_keys(&topology, &nearest, &source_points)?;
    let collisions = node_injectivity_collisions(&mut live)?;
    assert_eq!(collisions.len(), 2);

    let one = ExactRational::from_f64(1.0);
    let mut moved = None;
    for &node_id in &collisions {
        let node =
            topology.q.nodes.get(node_id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("collision node is unknown".into())
            })?;
        let ports = topology
            .q
            .node_ports
            .get(node.ports.clone())
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("collision node ports are unknown".into())
            })?;
        for &port_id in ports {
            let port = topology.q.ports.get(port_id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("collision port is unknown".into())
            })?;
            if !port.cut && port.point.x.try_cmp(&one)?.is_gt() && port.point.y.is_zero() {
                moved = Some(node_id);
                break;
            }
        }
    }
    let moved = moved.ok_or_else(|| {
        GeometryErrorKind::Invalid("constructed crossing node is missing".into())
    })?;
    let mut assigned = nearest.clone();
    let candidate = assigned.xy(moved)?;
    *assigned.node.get_mut(moved.0 as usize).ok_or_else(|| {
        GeometryErrorKind::Invalid("constructed Q node coordinate is missing".into())
    })? = crate::geometry::XY::new(candidate.x.next_down(), candidate.y);
    let mut assigned_live = plan.live_node_keys(&topology, &assigned, &source_points)?;
    assert!(node_injectivity_collisions(&mut assigned_live)?.is_empty());

    let exact_chart = subgraph_chart(
        &topology,
        &emission,
        &nearest,
        None,
        &mut crate::work::Unmetered,
    )?;
    let exact_faces = QFaceStructure::build(&topology, &emission, exact_chart)?;
    let assigned_chart = subgraph_chart(
        &topology,
        &emission,
        &nearest,
        Some(&assigned),
        &mut crate::work::Unmetered,
    )?;
    let assigned_faces = QFaceStructure::build(&topology, &emission, assigned_chart)?;
    let witnesses =
        face_structure_mismatch_nodes(&exact_faces, &assigned_faces, &emission.live_nodes)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "collision-free next-down assignment preserved the exact Q faces".into(),
                )
            })?;
    assert!(
        witnesses.contains(&moved),
        "rotation-system witness should name the moved crossing node: {witnesses:?}"
    );
    Ok(())
}

#[test]
fn native_polygonize_emission_keeps_diagnostic_source_tags() -> Result<()> {
    let boundary = crate::io::parse_wkt("LINESTRING (0 0, 10 0, 10 10, 0 10, 0 0)")?;
    let dangle = crate::io::parse_wkt("LINESTRING ZM (5 0 1 10, 5 -3 2 20)")?;
    let shapes = [boundary, dangle];
    let topology = test_topology(&shapes);
    let mut work = crate::work::Unmetered;
    let mut plan = QEmissionPlan::build_polygonize(&topology, true, &mut work)?;
    plan.force_native = true;

    let source_points = source_point_coordinates(&topology)?;
    let nearest = QCoordinates::build(&topology)?;
    let operation = "polygonize";
    let declared_axes = shapes.iter().fold(CoordinateAxes::XY, |axes, shape| {
        axes.union(shape.axes())
    });
    let metric_access: Option<&SourceArcMetricAccess<'_, '_, crate::crs::GeodesicMetric>> = None;
    let mut budget = EvaluationBudget::new(operation, 0)?;
    let mut preparation_work = EvaluationOnly::new(&mut budget);
    let mut expansion_budget = ExpansionBudget::new(operation, "computed edges");
    let embedding = resolve_q_embedding(
        &topology,
        &plan,
        &source_points,
        &nearest,
        declared_axes,
        false,
        operation,
        metric_access,
        &mut expansion_budget,
        &mut preparation_work,
    )?;
    let super::super::emission::CompletedQDrawing::Drawing { diagnostic_lines, .. } = embedding.drawing else {
        panic!("the forced native polygonize plan produces a native drawing");
    };

    assert_eq!(diagnostic_lines.len(), 1);
    let diagnostic = diagnostic_lines
        .first()
        .ok_or_else(|| GeometryErrorKind::Invalid("native Q dangle tag was lost".into()))?;
    assert_eq!(
        diagnostic.tag.kind,
        super::super::emission::plan::DiagnosticKind::Dangle
    );
    assert_eq!(diagnostic.tag.source_row, SourceRowId(1));
    assert!(diagnostic.coordinates.axes().has_z());
    assert!(diagnostic.coordinates.axes().has_m());
    let mut coordinates = diagnostic
        .coordinates
        .iter()
        .map(|point| {
            (
                point.x.to_bits(),
                point.y.to_bits(),
                point.z().map(f64::to_bits),
                point.m().map(f64::to_bits),
            )
        })
        .collect::<Vec<_>>();
    coordinates.sort_unstable();
    assert_eq!(
        coordinates,
        [
            (
                5.0_f64.to_bits(),
                0.0_f64.to_bits(),
                Some(1.0_f64.to_bits()),
                Some(10.0_f64.to_bits()),
            ),
            (
                5.0_f64.to_bits(),
                (-3.0_f64).to_bits(),
                Some(2.0_f64.to_bits()),
                Some(20.0_f64.to_bits()),
            ),
        ]
    );
    Ok(())
}

#[test]
fn q_nearest_even_tie_is_published_byte_identically_when_certified() -> Result<()> {
    let shapes = [
        q_grid_parallelogram((30, -22), (-10, 27))?,
        q_grid_parallelogram((4, 13), (10, -18))?,
    ];
    let topology = test_topology(&shapes);
    let plan = build_q_plan(&topology, OverlayOp::Intersection)?;
    let source_points = source_point_coordinates(&topology)?;
    let nearest = QCoordinates::build(&topology)?;
    let publication = plan.fragment_recipes.publish(
        &topology,
        &nearest,
        OverlayOp::Intersection.name(),
        &mut crate::work::Unmetered,
    )?;
    let emission = build_q_emission_unmetered(&plan, &topology, &source_points, &publication)?;
    let mut live_node_keys = plan.live_node_keys(&topology, &nearest, &source_points)?;
    assert!(
        node_injectivity_collisions(&mut live_node_keys)?.is_empty(),
        "the nearest output nodes are injective"
    );
    assert!(
        nearest.has_inexact_nodes(&topology, live_node_keys.iter().map(|(_, node)| *node),)?,
        "the fixture has constructed intersections that need certification"
    );

    let midpoint = ExactRational::from_f64(100.0)
        .try_subtract(&ExactRational::from_f64(2.0_f64.powi(-47)))?;
    let node = QNodeId(5);
    let q_node = topology
        .q
        .nodes
        .get(node.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("tie witness node is missing".into()))?;
    let port_id = topology
        .q
        .node_ports
        .get(q_node.ports.clone())
        .and_then(|ports| {
            ports.iter().copied().find(|port| {
                topology
                    .q
                    .ports
                    .get(port.0 as usize)
                    .is_some_and(|port| !port.cut)
            })
        })
        .ok_or_else(|| GeometryErrorKind::Invalid("tie witness port is missing".into()))?;
    let exact_x = &topology
        .q
        .ports
        .get(port_id.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("tie witness port is unknown".into()))?
        .point
        .x;
    assert!(exact_x.try_cmp(&midpoint)?.is_eq());
    assert_eq!(nearest.xy(node)?.x, 100.0);
    let down =
        ExactRational::from_f64(nearest.xy(node)?.x.next_down()).try_subtract(&midpoint)?;
    let even = ExactRational::from_f64(nearest.xy(node)?.x).try_subtract(&midpoint)?;
    assert!(
        down.try_product(&down)?
            .try_cmp(&even.try_product(&even)?)?
            .is_eq()
    );

    let drawing = draw_q_plan_unmetered(&plan, &topology, &nearest, &source_points)?;
    let mut budget = EvaluationBudget::new(OverlayOp::Intersection.name(), 0)?;
    let output = match certify_q_embedding_nearest_xy(
        &topology,
        &emission,
        &nearest,
        &live_node_keys,
        drawing,
        OverlayOp::Intersection.name(),
        &mut budget,
    )? {
        NearestQCertificate::Pass(output) => *output,
        NearestQCertificate::SearchRequired => {
            return Err(GeometryErrorKind::Invalid(
                "the midpoint tie fixture should certify at nearest-even".into(),
            )
            .into());
        },
        NearestQCertificate::Unrepresentable => {
            return Err(GeometryErrorKind::Invalid(
                "the midpoint tie fixture should be representable at nearest-even".into(),
            )
            .into());
        },
    };
    let certified = match output.drawing {
        CompletedQDrawing::Source(source) => source.sealed.into_scalar()?.0,
        CompletedQDrawing::Native(native) => native.sealed.into_scalar()?.0,
        CompletedQDrawing::Drawing { shape, frame, .. } => {
            use crate::geometry::publication::{ProducerDrawingReceipt, StorageTarget, finish_producer_drawing};
            finish_producer_drawing(
                ProducerDrawingReceipt::drawing(shape)?,
                frame,
                StorageTarget::Scalar,
            )?.into_scalar()?.0
        },
    };
    let expected = crate::io::parse_wkt(
        "POLYGON ((99.9999999999998 80.00000000000007, 100 79.99999999999973, \
         100.00000000000011 79.99999999999964, 100.0000000000002 79.99999999999993, \
         100 80.00000000000027, 99.99999999999989 80.00000000000036, \
         99.9999999999998 80.00000000000007))",
    )?;
    let xy_record = |shape: &Shape| -> Result<Vec<(u64, u64)>> {
        let Shape::Polygon(polygon) = shape else {
            return Err(GeometryErrorKind::Invalid(
                "the midpoint tie output must be one polygon".into(),
            )
            .into());
        };
        let points = polygon.shell.coords().iter_coords().collect::<Vec<_>>();
        let (Some(first), Some(last)) = (points.first(), points.last()) else {
            return Err(GeometryErrorKind::Invalid(
                "the midpoint tie output ring is empty".into(),
            )
            .into());
        };
        if first.x.to_bits() != last.x.to_bits() || first.y.to_bits() != last.y.to_bits() {
            return Err(GeometryErrorKind::Invalid(
                "the midpoint tie output ring is not closed".into(),
            )
            .into());
        }
        let mut record = points
            .iter()
            .take(points.len().saturating_sub(1))
            .map(|point| (point.x.to_bits(), point.y.to_bits()))
            .collect::<Vec<_>>();
        record.sort_unstable();
        Ok(record)
    };
    let expected_record = xy_record(&expected)?;
    assert_eq!(
        xy_record(&certified)?,
        expected_record,
        "the certified nearest-even embedding must publish the independently enumerated XY record"
    );
    let actual = exact_geographic_overlay::<crate::crs::GeodesicMetric>(
        &topology,
        OverlayOp::Intersection,
        CoordinateAxes::XY,
        false,
        None,
    )?;
    assert!(actual.validate()?.is_none(), "nearest output must be valid");
    assert_eq!(
        xy_record(&actual)?,
        expected_record,
        "the independently enumerated nearest-even record must match the exact output"
    );
    assert_eq!(
        crate::io::to_wkb(&actual, None, false)?,
        crate::io::to_wkb(&certified, None, false)?,
        "the certified and exact outputs must keep identical WKB records"
    );
    Ok(())
}

#[test]
fn q_search_repairs_two_constructed_nodes_with_one_nearest_key() -> Result<()> {
    let shapes = [
        q_grid_parallelogram((-14, 4), (24, -18))?,
        q_grid_parallelogram((23, -29), (-17, -8))?,
    ];
    let topology = test_topology(&shapes);
    let plan = build_q_plan(&topology, OverlayOp::Intersection)?;
    let source_points = source_point_coordinates(&topology)?;
    let nearest = QCoordinates::build(&topology)?;
    let publication = plan.fragment_recipes.publish(
        &topology,
        &nearest,
        OverlayOp::Intersection.name(),
        &mut crate::work::Unmetered,
    )?;
    let emission = build_q_emission_unmetered(&plan, &topology, &source_points, &publication)?;
    let source_nodes = b3_source_endpoint_nodes(&topology);
    let mut nearest_keys = plan.live_node_keys(&topology, &nearest, &source_points)?;
    let conflicts = node_injectivity_collisions(&mut nearest_keys)?;
    let mut groups = BTreeMap::<EmittedNodeKey, BTreeSet<QNodeId>>::new();
    for (key, node) in plan.live_node_keys(&topology, &nearest, &source_points)? {
        groups.entry(key).or_default().insert(node);
    }
    let (collision_key, collision_group) = groups
        .iter()
        .find(|(_, nodes)| {
            nodes
                .iter()
                .filter(|node| !source_nodes.contains(node))
                .count()
                == 2
        })
        .ok_or_else(|| {
            GeometryErrorKind::Invalid(
                "fixture has no two-constructed-node rounding collision".into(),
            )
        })?;
    let constructed_nodes = collision_group
        .iter()
        .copied()
        .filter(|node| !source_nodes.contains(node))
        .collect::<Vec<_>>();
    assert_eq!(constructed_nodes, [QNodeId(2), QNodeId(3)]);
    assert!(
        conflicts
            .iter()
            .any(|node| constructed_nodes.contains(node)),
        "nearest live-key check must witness both constructed nodes"
    );
    assert_eq!(
        nearest.xy(constructed_nodes[0])?,
        nearest.xy(constructed_nodes[1])?
    );
    assert_eq!(
        *collision_key,
        emitted_node_key(topology.sources.frame, nearest.xy(constructed_nodes[0])?)?
    );

    let certified = resolve_q_embedding_xy(
        &topology,
        &plan,
        &source_points,
        &nearest,
        OverlayOp::Intersection.name(),
    )?;
    assert_eq!(certified.choice, QEmbeddingChoice::Searched);
    let certified_coordinates = certified.coordinates.clone();
    let mut certified_keys =
        plan.live_node_keys(&topology, &certified_coordinates, &source_points)?;
    assert!(node_injectivity_collisions(&mut certified_keys)?.is_empty());
    let emitted_nodes = emission.live_nodes.iter().copied().collect::<BTreeSet<_>>();
    for node in source_nodes.intersection(&emitted_nodes) {
        assert_eq!(certified_coordinates.xy(*node)?, nearest.xy(*node)?);
    }

    let written = write_certified_q_emission(certified)?;
    let expected = written;
    let actual = exact_geographic_overlay::<crate::crs::GeodesicMetric>(
        &topology,
        OverlayOp::Intersection,
        CoordinateAxes::XY,
        false,
        None,
    )?;
    assert_eq!(
        xy_bitstream(&actual),
        xy_bitstream(&expected),
        "publication must use the accepted drawing's node coordinates"
    );
    assert!(
        actual.validate()?.is_none(),
        "searched overlay must be valid"
    );
    assert_eq!(
        crate::io::to_wkb(&actual, None, false)?,
        crate::io::to_wkb(&expected, None, false)?,
        "the published output must use the certified node assignment"
    );
    Ok(())
}

#[test]
fn q_nearest_first_grid_sweep_64_keeps_certified_nearest_tables() -> Result<()> {
    let counts = q_grid_certificate_sweep(64)?;
    assert_eq!(counts.pairs, 64);
    assert!(
        counts.nearest_certified > 0,
        "the sample must include nearest-even certificate passes: {counts:?}"
    );
    assert_eq!(counts.nearest_certified + counts.search_paths, counts.pairs);
    eprintln!("Q nearest-first 64-pair sweep: {counts:?}");
    Ok(())
}

#[test]
#[ignore = "2,000-pair Q nearest-first acceptance audit; run out of suite with cargo test"]
fn q_nearest_first_grid_sweep_2000_out_of_suite() -> Result<()> {
    let counts = q_grid_certificate_sweep(2_000)?;
    assert_eq!(counts.pairs, 2_000);
    eprintln!("Q nearest-first 2,000-pair sweep: {counts:?}");
    assert_eq!(counts.nearest_certified + counts.search_paths, counts.pairs);
    Ok(())
}

#[test]
fn geographic_nearest_certificate_preserves_direct_q_au_pg_union_wkb() -> Result<()> {
    use sha2::{Digest as _, Sha256};

    let shapes = b3_read_countries(&["AU", "PG"]);
    let frame = GeographicFrame::crs84();
    let inputs = crate::geographic::GeographicInputs::try_from_shapes(frame, shapes.iter())
        .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
    assert_eq!(
        crate::geometry::geographic_topology_route(inputs.views().iter().copied()),
        crate::geometry::GeographicTopologyRoute::ExactPartition,
        "the public AU/PG binary union dispatches to exact partition"
    );
    let topology = test_topology(&shapes);
    let plan = build_q_plan(&topology, OverlayOp::Union)?;
    let source_points = source_point_coordinates(&topology)?;
    let nearest = QCoordinates::build(&topology)?;
    let embedding = resolve_q_embedding_xy(
        &topology,
        &plan,
        &source_points,
        &nearest,
        OverlayOp::Union.name(),
    )?;
    assert_eq!(embedding.choice, QEmbeddingChoice::Nearest);
    let result = publish_q_overlay(embedding)?;
    assert!(
        crate::geometry::validate_shape_in_frame(
            &result,
            crate::geometry::TopologyDomain::Geographic(GeographicFrame::crs84()),
        )?
        .is_none(),
        "the baseline union remains valid"
    );
    assert_eq!(
        format!(
            "{:x}",
            Sha256::digest(crate::io::to_wkb(&result, None, false)?)
        ),
        "3de292238e1cfcfa472ddd95548bb3ea28321182c2124f801d1fb63186fad206",
        "direct Q retains its producer-order parent WKB"
    );
    Ok(())
}
