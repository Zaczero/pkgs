#[test]
fn boundary_keeps_the_valid_seam_component_in_a_multirow_union() {
    let shape = crate::io::parse_wkt(
        "MULTIPOLYGON (\
         ((20 20,40 20,40 30,20 30,20 20)),\
         ((0 10,20 10,20 20,0 20,0 10)),\
         ((170 20,190 20,190 40,170 40,170 20))\
         )",
    )
    .expect("three-box multipolygon parses");
    let mut expected = vec![
        edge((0.0, 10.0), (20.0, 10.0)),
        edge((20.0, 10.0), (20.0, 20.0)),
        edge((20.0, 20.0), (0.0, 20.0)),
        edge((0.0, 20.0), (0.0, 10.0)),
        edge((20.0, 20.0), (40.0, 20.0)),
        edge((40.0, 20.0), (40.0, 30.0)),
        edge((40.0, 30.0), (20.0, 30.0)),
        edge((20.0, 30.0), (20.0, 20.0)),
        edge((170.0, 40.0), (170.0, 20.0)),
        edge((170.0, 20.0), (-180.0, 20.0)),
        edge((-180.0, 20.0), (-170.0, 20.0)),
        edge((-170.0, 20.0), (-170.0, 40.0)),
        edge((-170.0, 40.0), (-180.0, 40.0)),
        edge((-180.0, 40.0), (170.0, 40.0)),
    ];
    expected.sort_unstable();
    let actual = line_edges(&exact_boundary(&shape));
    assert_eq!(actual, expected);
    assert!(!actual.contains(&edge((-180.0, 20.0), (-180.0, 40.0))));
}

#[test]
fn boundary_keeps_a_pentagon_when_its_host_ray_hits_a_seam_box_node() {
    let shape = crate::io::parse_wkt(
        "MULTIPOLYGON (\
         ((0 10,20 10,20 20,40 20,0 30,0 10)),\
         ((170 20,190 20,190 40,170 40,170 20))\
         )",
    )
    .expect("pentagon and seam box parse");
    let mut expected = vec![
        edge((0.0, 30.0), (0.0, 10.0)),
        edge((0.0, 10.0), (20.0, 10.0)),
        edge((20.0, 10.0), (20.0, 20.0)),
        edge((20.0, 20.0), (40.0, 20.0)),
        edge((40.0, 20.0), (0.0, 30.0)),
        edge((170.0, 40.0), (170.0, 20.0)),
        edge((170.0, 20.0), (-180.0, 20.0)),
        edge((-180.0, 20.0), (-170.0, 20.0)),
        edge((-170.0, 20.0), (-170.0, 40.0)),
        edge((-170.0, 40.0), (-180.0, 40.0)),
        edge((-180.0, 40.0), (170.0, 40.0)),
    ];
    expected.sort_unstable();
    let actual = line_edges(&exact_boundary(&shape));
    assert_eq!(actual, expected);
    assert!(!actual.contains(&edge((-180.0, 20.0), (-180.0, 40.0))));
}

#[test]
fn boundary_certificate_rejects_a_reversed_constructed_hole_triangle() -> Result<()> {
    let unit = f64::from_bits(0x3D10_0000_0000_0000);
    let bar = |vertices: &[(i32, i32)]| -> Result<Polygon> {
        let coordinates = vertices
            .iter()
            .map(|&(x, y)| {
                format!(
                    "{} {}",
                    100.0 + f64::from(x) * unit,
                    80.0 + f64::from(y) * unit
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        let shape = crate::io::parse_wkt(&format!("POLYGON (({coordinates}))"))?;
        let Shape::Polygon(polygon) = shape else {
            return Err(GeometryErrorKind::Invalid(
                "a triangle bar must parse as a polygon".into(),
            )
            .into());
        };
        Ok(polygon)
    };
    let bars = vec![
        bar(&[(-30, -20), (30, -20), (30, 0), (-30, 0), (-30, -20)])?,
        bar(&[(-30, -10), (30, 11), (30, 31), (-30, 10), (-30, -10)])?,
        bar(&[(-30, 11), (30, -10), (30, 10), (-30, 31), (-30, 11)])?,
    ];
    let members = bars
        .into_iter()
        .map(crate::geometry::MultiMember::Value)
        .collect::<Vec<_>>();
    let source = Shape::MultiPolygon(crate::geometry::MultiPolygon::try_new(
        CoordinateAxes::XY,
        members,
    )?);
    let topology = test_topology(std::slice::from_ref(&source));
    let (plan, support) = build_q_boundary_plan(&topology)?;
    assert!(support.has_area);
    let source_points = boundary_source_points(&topology, SourceRowId(0))?;
    let nearest = QCoordinates::build(&topology)?;
    let publication = plan.fragment_recipes.publish(
        &topology,
        &nearest,
        "geographic boundary",
        &mut crate::geometry::Unmetered,
    )?;
    let mut witness_found = false;
    let mut work = crate::geometry::Unmetered;
    for recipe in plan.fragment_recipes.as_slice() {
        let references = recipe.reference_endpoints(&topology, &mut work)?;
        let start_x = references[0].x.round_with_work(&mut work)?;
        let end_x = references[1].x.round_with_work(&mut work)?;
        let anchored_end = ExactRational::from_f64(start_x)
            .try_add_with_work(&recipe.exact_displacement, &mut work)?
            .round_with_work(&mut work)?;
        if start_x.to_bits() != 100.000_000_000_000_01_f64.to_bits()
            || anchored_end.to_bits() != 99.999_999_999_999_97_f64.to_bits()
        {
            continue;
        }
        let assigned_end = nearest.point(&topology, recipe.endpoints[1])?;
        assert_eq!(
            assigned_end.x.to_bits(),
            99.999_999_999_999_99_f64.to_bits()
        );
        assert_eq!(end_x.to_bits(), assigned_end.x.to_bits());
        let fragment = publication
            .fragments
            .get(recipe.id.0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("near-ULP publication fragment is missing".into())
            })?;
        assert_eq!(fragment.exact_endpoints, references);
        assert!(fragment.source_records[1].is_none());
        let published_end = fragment.points.last().ok_or_else(|| {
            GeometryErrorKind::Invalid("near-ULP publication has no end point".into())
        })?;
        assert_eq!(published_end.x.to_bits(), end_x.to_bits());
        assert_eq!(
            published_end.y.to_bits(),
            references[1].y.round_with_work(&mut work)?.to_bits()
        );
        witness_found = true;
    }
    assert!(
        witness_found,
        "the captured near-ULP Q-reference interval must be published from its exact endpoints"
    );
    let emission = build_q_emission_unmetered(&plan, &topology, &source_points, &publication)?;

    let exact_apex_x = ExactRational::from_f64(100.0);
    let half_unit = ExactRational::from_f64(unit / 2.0);
    let exact_apex_y = ExactRational::from_f64(80.0).try_add(&half_unit)?;
    let mut apex = None;
    for &node_id in &emission.live_nodes {
        let node = topology.q.nodes.get(node_id.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("triangle-hole node is unknown".into())
        })?;
        let ports = topology
            .q
            .node_ports
            .get(node.ports.clone())
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("triangle-hole node ports are unknown".into())
            })?;
        for &port_id in ports {
            let port = topology.q.ports.get(port_id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("triangle-hole port is unknown".into())
            })?;
            if !port.cut
                && port.point.x.try_cmp(&exact_apex_x)?.is_eq()
                && port.point.y.try_cmp(&exact_apex_y)?.is_eq()
            {
                apex = Some(node_id);
                break;
            }
        }
    }
    let apex = apex.ok_or_else(|| {
        GeometryErrorKind::Invalid("constructed triangular-hole apex is missing".into())
    })?;
    assert_eq!(nearest.xy(apex)?.y.to_bits(), 80.0_f64.to_bits());

    let mut reversed = nearest.clone();
    let nearest_apex = reversed.xy(apex)?;
    *reversed.node.get_mut(apex.0 as usize).ok_or_else(|| {
        GeometryErrorKind::Invalid("triangle-hole coordinate is missing".into())
    })? = crate::geometry::XY::new(nearest_apex.x, 80.0_f64.next_down());
    let mut live = plan.live_node_keys(&topology, &reversed, &source_points)?;
    assert!(
        node_injectivity_collisions(&mut live)?.is_empty(),
        "reversing the degree-two hole triangle remains node-injective"
    );

    let exact_chart = subgraph_chart(
        &topology,
        &emission,
        &nearest,
        None,
        &mut crate::geometry::Unmetered,
    )?;
    let exact_faces = QFaceStructure::build(&topology, &emission, exact_chart)?;
    let reversed_chart = subgraph_chart(
        &topology,
        &emission,
        &nearest,
        Some(&reversed),
        &mut crate::geometry::Unmetered,
    )?;
    let reversed_faces = QFaceStructure::build(&topology, &emission, reversed_chart)?;
    let witnesses =
        face_structure_mismatch_nodes(&exact_faces, &reversed_faces, &emission.live_nodes)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "reversed hole triangle preserved the exact face/host structure".into(),
                )
            })?;
    assert!(
        witnesses.contains(&apex),
        "the face/host witness should name the moved apex: {witnesses:?}"
    );

    let drawing = draw_q_plan_unmetered(&plan, &topology, &reversed, &source_points)?;
    let mut budget = EvaluationBudget::new("geographic boundary", 0)?;
    let certificate = certify_q_embedding_nearest_xy(
        &topology,
        &emission,
        &reversed,
        &live,
        drawing,
        "geographic boundary",
        &mut budget,
    )?;
    let NearestQCertificate::SearchRequired = certificate else {
        return Err(GeometryErrorKind::Invalid(
            "the full boundary certificate accepted the reversed hole triangle".into(),
        )
        .into());
    };

    let accepted = resolve_q_embedding_xy(
        &topology,
        &plan,
        &source_points,
        &nearest,
        "geographic boundary",
    )?;
    let accepted_chart = subgraph_chart(
        &topology,
        &emission,
        &nearest,
        Some(&accepted.coordinates),
        &mut crate::geometry::Unmetered,
    )?;
    let accepted_faces = QFaceStructure::build(&topology, &emission, accepted_chart)?;
    assert!(
        face_structure_mismatch_nodes(&exact_faces, &accepted_faces, &emission.live_nodes)
            .is_none(),
        "the accepted table preserves the triangular-hole face"
    );
    Ok(())
}

#[test]
fn geographic_boundary_repairs_a_nearest_embedding_before_writing() -> Result<()> {
    let parts = [
        q_grid_parallelogram((-14, 4), (24, -18))?,
        q_grid_parallelogram((23, -29), (-17, -8))?,
    ];
    let source = Shape::geometry_collection(parts.into_iter().collect())?;
    let topology = test_topology(std::slice::from_ref(&source));
    let (plan, support) = build_q_boundary_plan(&topology)?;
    assert!(support.has_area);

    let source_points = boundary_source_points(&topology, SourceRowId(0))?;
    let nearest = QCoordinates::build(&topology)?;
    let publication = plan.fragment_recipes.publish(
        &topology,
        &nearest,
        "geographic boundary",
        &mut crate::geometry::Unmetered,
    )?;
    let emission = build_q_emission_unmetered(&plan, &topology, &source_points, &publication)?;
    let exact_chart = subgraph_chart(
        &topology,
        &emission,
        &nearest,
        None,
        &mut crate::geometry::Unmetered,
    )?;
    let exact_faces = QFaceStructure::build(&topology, &emission, exact_chart)?;
    assert!(
        subgraph_chart(
            &topology,
            &emission,
            &nearest,
            Some(&nearest),
            &mut crate::geometry::Unmetered,
        )
        .and_then(|chart| QFaceStructure::build(&topology, &emission, chart))
        .err()
        .is_some_and(|error| error.to_string().contains("coincident tangent rays")),
        "nearest boundary rounding must fail the exact area face/host control"
    );
    let embedding = resolve_q_embedding_xy(
        &topology,
        &plan,
        &source_points,
        &nearest,
        "geographic boundary",
    )?;
    assert_eq!(
        embedding.choice,
        QEmbeddingChoice::Searched,
        "the nearest boundary drawing must be repaired before publication"
    );
    let accepted_chart = subgraph_chart(
        &topology,
        &emission,
        &nearest,
        Some(&embedding.coordinates),
        &mut crate::geometry::Unmetered,
    )?;
    let accepted_faces = QFaceStructure::build(&topology, &emission, accepted_chart)?;
    assert!(
        face_structure_mismatch_nodes(&exact_faces, &accepted_faces, &emission.live_nodes)
            .is_none(),
        "the searched boundary drawing must restore the exact area face/host structure"
    );
    let written = write_certified_q_emission(embedding)?;
    let mut line_members = 0_usize;
    let mut polygon_members = 0_usize;
    written
        .shape
        .try_for_each_component(&mut |component| -> Result<()> {
            match component {
                crate::geometry::Component::LineString(_) => {
                    line_members = line_members.saturating_add(1);
                },
                crate::geometry::Component::Polygon(_) => {
                    polygon_members = polygon_members.saturating_add(1);
                },
                crate::geometry::Component::Point(_) => {},
            }
            Ok(())
        })?;
    assert!(
        line_members > 0,
        "the repaired areal boundary publishes linework"
    );
    assert_eq!(polygon_members, 0);
    Ok(())
}

#[test]
fn seam_only_collapsed_support_publishes_its_odd_endpoints() {
    let shape = crate::io::parse_wkt("POLYGON ((-180 -85,180 -85,180 85,-180 85,-180 -85))")
        .expect("seam-only polygon parses");
    let Shape::MultiPoint(points) = exact_boundary(&shape) else {
        panic!("seam-only boundary is multipoint");
    };
    let frame = GeographicFrame::crs84();
    let mut actual = points
        .iter_coords()
        .map(|point| {
            (
                frame
                    .longitude()
                    .canonicalize(point.x)
                    .expect("emitted longitude is in the frame")
                    .get()
                    .to_bits(),
                point.y.to_bits(),
            )
        })
        .collect::<Vec<_>>();
    actual.sort_unstable();
    let mut expected = [
        ((-180.0_f64).to_bits(), (-85.0_f64).to_bits()),
        ((-180.0_f64).to_bits(), 85.0_f64.to_bits()),
    ];
    expected.sort_unstable();
    assert_eq!(actual, expected);
}
