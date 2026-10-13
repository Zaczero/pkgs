#[test]
fn source_endpoint_record_uses_local_side_and_composed_root_side() -> Result<()> {
    let start = Point::new_raw_axes(-0.0, -0.0, ZOrdinate(Some(-0.0)), MOrdinate(Some(17.0)));
    let end = Point::new_raw_axes(10.0, 0.0, ZOrdinate(Some(23.0)), MOrdinate(Some(29.0)));
    let topology = endpoint_test_topology(&[endpoint_test_line(start, end)?])?;
    let edge =
        topology.sources.edges.first().ok_or_else(|| {
            GeometryErrorKind::Invalid("endpoint test edge is missing".into())
        })?;
    let ports = *topology
        .q
        .source_edge_ports
        .get(edge.id.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("endpoint test ports are missing".into()))?;
    let zero = ExactRational::zero();
    let one = ExactRational::one();
    let mut work = Unmetered;

    let forward = endpoint_test_recipe(ports, edge.id, zero.clone(), one.clone());
    assert_source_record_bits(
        forward.source_endpoint_record(&topology, EndpointSide::Start, &mut work)?,
        edge.source[0],
    );
    assert_source_record_bits(
        forward.source_endpoint_record(&topology, EndpointSide::End, &mut work)?,
        edge.source[1],
    );

    let reverse = endpoint_test_recipe([ports[1], ports[0]], edge.id, one, zero);
    assert_source_record_bits(
        reverse.source_endpoint_record(&topology, EndpointSide::Start, &mut work)?,
        edge.source[1],
    );
    assert_source_record_bits(
        reverse.source_endpoint_record(&topology, EndpointSide::End, &mut work)?,
        edge.source[0],
    );
    Ok(())
}

#[test]
fn partial_root_endpoint_does_not_claim_an_authentic_source_record() {
    let topology = endpoint_test_topology(&[endpoint_test_line(
        Point::new_raw_xy(0.0, 0.0),
        Point::new_raw_xy(10.0, 0.0),
    )
    .unwrap()])
    .unwrap();
    let edge = topology
        .sources
        .edges
        .first()
        .ok_or_else(|| GeometryErrorKind::Invalid("endpoint test edge is missing".into()))
        .unwrap();
    let ports = *topology
        .q
        .source_edge_ports
        .get(edge.id.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("endpoint test ports are missing".into()))
        .unwrap();
    let recipe = endpoint_test_recipe(
        ports,
        edge.id,
        ExactRational::from_f64(0.25),
        ExactRational::from_f64(0.75),
    );
    let mut work = Unmetered;

    assert!(
        recipe
            .source_endpoint_record(&topology, EndpointSide::Start, &mut work)
            .unwrap()
            .is_none()
    );
    assert!(
        recipe
            .source_endpoint_record(&topology, EndpointSide::End, &mut work)
            .unwrap()
            .is_none()
    );
}

#[test]
fn source_endpoint_record_accepts_each_matching_owner_orientation() {
    let first = crate::geometry::LineSeq::try_new(
        CoordSeq::try_from_points(&[
            Point::new_raw_axes(0.0, 0.0, ZOrdinate(Some(11.0)), MOrdinate(Some(101.0))),
            Point::new_raw_axes(10.0, 0.0, ZOrdinate(Some(12.0)), MOrdinate(Some(102.0))),
        ])
        .unwrap(),
    )
    .unwrap();
    let second = crate::geometry::LineSeq::try_new(
        CoordSeq::try_from_points(&[
            Point::new_raw_axes(10.0, 0.0, ZOrdinate(Some(21.0)), MOrdinate(Some(201.0))),
            Point::new_raw_axes(0.0, 0.0, ZOrdinate(Some(22.0)), MOrdinate(Some(202.0))),
        ])
        .unwrap(),
    )
    .unwrap();
    let shape = Shape::multi_line_string(
        crate::geometry::CoordinateAxes::new(
            crate::geometry::HasZ(true),
            crate::geometry::HasM(true),
        ),
        vec![first, second],
    );
    let topology = endpoint_test_topology(&[shape]).unwrap();
    assert_eq!(topology.sources.edges.len(), 2);
    let first_edge = topology
        .sources
        .edges
        .first()
        .ok_or_else(|| GeometryErrorKind::Invalid("first endpoint owner is missing".into()))
        .unwrap();
    let second_edge = topology
        .sources
        .edges
        .get(1)
        .ok_or_else(|| GeometryErrorKind::Invalid("second endpoint owner is missing".into()))
        .unwrap();
    let endpoints = *topology
        .q
        .source_edge_ports
        .get(first_edge.id.0 as usize)
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("first endpoint owner ports are missing".into())
        })
        .unwrap();
    let second_endpoints = *topology
        .q
        .source_edge_ports
        .get(second_edge.id.0 as usize)
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("second endpoint owner ports are missing".into())
        })
        .unwrap();
    assert_eq!(endpoints, [second_endpoints[1], second_endpoints[0]]);
    let zero = ExactRational::zero();
    let one = ExactRational::one();
    let mut recipe = endpoint_test_recipe(endpoints, first_edge.id, zero.clone(), one.clone());
    recipe.ranges.push(DirectedSourceRange {
        source_edge: second_edge.id,
        range: ExactSourceRange {
            start: one,
            end: zero,
        },
        emitted: true,
        producer_start: ExactRational::zero(),
        producer_end: ExactRational::one(),
        endpoint_only: false,
    });
    let mut work = Unmetered;

    assert_source_record_bits(
        recipe
            .source_endpoint_record(&topology, EndpointSide::Start, &mut work)
            .unwrap(),
        first_edge.source[0],
    );
    assert_source_record_bits(
        recipe
            .source_endpoint_record(&topology, EndpointSide::End, &mut work)
            .unwrap(),
        first_edge.source[1],
    );

    recipe.ranges.swap(0, 1);
    assert_source_record_bits(
        recipe
            .source_endpoint_record(&topology, EndpointSide::Start, &mut work)
            .unwrap(),
        second_edge.source[1],
    );
    assert_source_record_bits(
        recipe
            .source_endpoint_record(&topology, EndpointSide::End, &mut work)
            .unwrap(),
        second_edge.source[0],
    );
}

#[test]
fn publication_preserves_authentic_start_and_samples_constructed_end() {
    let horizontal = endpoint_test_line(
        Point::new_raw_axes(0.0, 0.0, ZOrdinate(Some(7.0)), MOrdinate(Some(11.0))),
        Point::new_raw_axes(10.0, 0.0, ZOrdinate(Some(13.0)), MOrdinate(Some(17.0))),
    )
    .unwrap();
    let vertical =
        endpoint_test_line(Point::new_raw_xy(5.0, -1.0), Point::new_raw_xy(5.0, 1.0)).unwrap();
    let source = Shape::geometry_collection(vec![horizontal, vertical]).unwrap();
    let topology = endpoint_test_topology(&[source]).unwrap();
    let edge = topology
        .sources
        .edges
        .iter()
        .find(|edge| {
            edge.source[0].x == 0.0
                && edge.source[0].y == 0.0
                && edge.source[1].x.to_bits() == 10.0_f64.to_bits()
                && edge.source[1].y == 0.0
        })
        .ok_or_else(|| GeometryErrorKind::Invalid("horizontal source edge is missing".into()))
        .unwrap();
    let [start_port, _] = *topology
        .q
        .source_edge_ports
        .get(edge.id.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("horizontal source ports are missing".into()))
        .unwrap();
    let constructed_end = topology
        .q
        .ports
        .iter()
        .find(|port| {
            port.point.x == ExactRational::from_f64(5.0)
                && port.point.y == ExactRational::zero()
        })
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("constructed crossing port is missing".into())
        })
        .unwrap();
    let mut work = Unmetered;
    let displacement = constructed_end
        .point
        .x
        .try_subtract_with_work(
            &topology
                .q
                .ports
                .get(start_port.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("horizontal start port is unknown".into())
                })
                .unwrap()
                .point
                .x,
            &mut work,
        )
        .unwrap();
    let recipe = FragmentRecipe {
        id: FragmentRecipeId(0),
        endpoints: [start_port, constructed_end.id],
        ranges: vec![DirectedSourceRange {
            source_edge: edge.id,
            range: ExactSourceRange {
                start: ExactRational::zero(),
                end: ExactRational::from_f64(0.5),
            },
            emitted: true,
            producer_start: ExactRational::zero(),
            producer_end: ExactRational::one(),
            endpoint_only: false,
        }],
        exact_displacement: displacement,
        use_kind: SelectedUse::Curve,
    };
    let coordinates = super::super::q_embedding::QCoordinates::build(&topology).unwrap();
    let recipes = FragmentRecipes {
        values: vec![recipe],
        walks: Vec::new(),
    };
    let publication = recipes
        .publish(&topology, &coordinates, "endpoint intent", &mut work)
        .unwrap();
    let fragment = publication
        .fragments
        .first()
        .ok_or_else(|| GeometryErrorKind::Invalid("endpoint intent fragment is missing".into()))
        .unwrap();
    assert_source_record_bits(fragment.source_records[0], edge.source[0]);
    assert!(fragment.source_records[1].is_none());
    assert_eq!(
        fragment.exact_endpoints[0],
        topology.q.ports[start_port.0 as usize].point
    );
    assert_eq!(fragment.exact_endpoints[1], constructed_end.point);
    let end = fragment
        .points
        .last()
        .ok_or_else(|| GeometryErrorKind::Invalid("endpoint intent has no end".into()))
        .unwrap();
    assert_eq!(
        end.x.to_bits(),
        fragment.exact_endpoints[1]
            .x
            .round_with_work(&mut work)
            .unwrap()
            .to_bits()
    );
    assert_eq!(
        end.y.to_bits(),
        fragment.exact_endpoints[1]
            .y
            .round_with_work(&mut work)
            .unwrap()
            .to_bits()
    );
}

#[test]
fn source_endpoint_record_admits_pole_latitude_but_keeps_its_spelling() {
    let frame = GeographicFrame::crs84();
    let pole = frame.pole();
    let first = crate::geometry::LineSeq::try_new(
        CoordSeq::try_from_points(&[
            Point::new_raw_axes(
                17.0,
                pole.next_up(),
                ZOrdinate(Some(31.0)),
                MOrdinate(Some(37.0)),
            ),
            Point::new_raw_axes(18.0, 80.0, ZOrdinate(Some(41.0)), MOrdinate(Some(43.0))),
        ])
        .unwrap(),
    )
    .unwrap();
    let second = crate::geometry::LineSeq::try_new(
        CoordSeq::try_from_points(&[
            Point::new_raw_axes(19.0, pole, ZOrdinate(Some(47.0)), MOrdinate(Some(53.0))),
            Point::new_raw_axes(20.0, 80.0, ZOrdinate(Some(59.0)), MOrdinate(Some(61.0))),
        ])
        .unwrap(),
    )
    .unwrap();
    let shape = Shape::multi_line_string(
        crate::geometry::CoordinateAxes::new(
            crate::geometry::HasZ(true),
            crate::geometry::HasM(true),
        ),
        vec![first, second],
    );
    let topology = endpoint_test_topology(&[shape]).unwrap();
    let first_edge = topology
        .sources
        .edges
        .first()
        .ok_or_else(|| GeometryErrorKind::Invalid("first pole edge is missing".into()))
        .unwrap();
    let second_edge = topology
        .sources
        .edges
        .get(1)
        .ok_or_else(|| GeometryErrorKind::Invalid("second pole edge is missing".into()))
        .unwrap();
    let first_ports = *topology
        .q
        .source_edge_ports
        .get(first_edge.id.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("first pole ports are missing".into()))
        .unwrap();
    let second_ports = *topology
        .q
        .source_edge_ports
        .get(second_edge.id.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("second pole ports are missing".into()))
        .unwrap();
    let first_pole = topology
        .q
        .ports
        .get(first_ports[0].0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("first pole port is missing".into()))
        .unwrap();
    let second_pole = topology
        .q
        .ports
        .get(second_ports[0].0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("second pole port is missing".into()))
        .unwrap();
    assert_eq!(first_pole.node, second_pole.node);
    assert_ne!(first_ports[0], second_ports[0]);
    let mut work = Unmetered;
    let matching = endpoint_test_recipe(
        first_ports,
        first_edge.id,
        ExactRational::zero(),
        ExactRational::one(),
    );
    assert_source_record_bits(
        matching
            .source_endpoint_record(&topology, EndpointSide::Start, &mut work)
            .unwrap(),
        first_edge.source[0],
    );
    let wrong_pole_spelling = endpoint_test_recipe(
        [second_ports[0], first_ports[1]],
        first_edge.id,
        ExactRational::zero(),
        ExactRational::one(),
    );
    assert!(
        wrong_pole_spelling
            .source_endpoint_record(&topology, EndpointSide::Start, &mut work)
            .unwrap()
            .is_none()
    );
}

#[test]
fn publisher_samples_resolve_through_the_original_source_edge() {
    let frame = GeographicFrame::crs84();
    let shape = crate::io::parse_wkt("LINESTRING ZM (0 0 1 100, 0 10 3 300)").unwrap();
    let inputs =
        crate::geographic::GeographicInputs::try_from_shapes(frame, std::iter::once(&shape))
            .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))
            .unwrap();
    let topology =
        ExactGeographicTopology::from_views(frame, inputs.views().iter().copied()).unwrap();
    let edge = topology
        .sources
        .edges
        .first()
        .ok_or_else(|| GeometryErrorKind::Invalid("source line edge is missing".into()))
        .unwrap();
    let endpoints = *topology
        .q
        .source_edge_ports
        .get(edge.id.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("source line ports are missing".into()))
        .unwrap();
    let zero = ExactRational::zero();
    let one = ExactRational::one();
    let recipe = FragmentRecipe {
        id: FragmentRecipeId(0),
        endpoints,
        ranges: vec![DirectedSourceRange {
            source_edge: edge.id,
            range: ExactSourceRange {
                start: zero.clone(),
                end: one.clone(),
            },
            emitted: true,
            producer_start: zero,
            producer_end: one,
            endpoint_only: false,
        }],
        exact_displacement: ExactRational::from_f64(frame.longitude().period()),
        use_kind: SelectedUse::Curve,
    };
    let recipes = FragmentRecipes {
        values: vec![recipe],
        walks: Vec::new(),
    };
    let coordinates = super::super::q_embedding::QCoordinates::build(&topology).unwrap();
    let mut work = Unmetered;
    let publication = recipes
        .publish(&topology, &coordinates, "source trace test", &mut work)
        .unwrap();
    let fragment = publication
        .fragments
        .first()
        .ok_or_else(|| GeometryErrorKind::Invalid("published fragment is missing".into()))
        .unwrap();

    assert_eq!(publication.samples.len(), 3);
    assert_eq!(fragment.points.len(), 5);
    assert_eq!(
        fragment.trace.rows.edge_count().checked_add(1),
        Some(fragment.points.len())
    );
    assert_eq!(
        fragment.points.first().and_then(|point| point.z()),
        Some(1.0)
    );
    assert_eq!(
        fragment.points.last().and_then(|point| point.m()),
        Some(300.0)
    );
    assert!(
        publication
            .samples
            .iter()
            .all(|sample| { sample.point.z().is_none() && sample.point.m().is_none() })
    );
    for (index, expected) in [
        ExactRational::from_f64(0.25),
        ExactRational::from_f64(0.5),
        ExactRational::from_f64(0.75),
    ]
    .into_iter()
    .enumerate()
    {
        let sample = publication
            .samples
            .get(index)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("accepted publisher sample is missing".into())
            })
            .unwrap();
        let source_trace = sample
            .source_trace
            .as_ref()
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("accepted publisher sample lost its source".into())
            })
            .unwrap();
        let TraceRequestLocation::Sample {
            recipe: trace_recipe,
            row,
        } = source_trace
        else {
            panic!("publisher sample does not reference its sample request row");
        };
        assert_eq!(
            *trace_recipe,
            sample
                .family
                .fragment_recipe()
                .expect("publication has a recipe parent")
        );
        let request = fragment
            .trace
            .rows
            .sample_requests_at(row.index(), &mut work)
            .unwrap()
            .into_iter()
            .next()
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("accepted publisher source row is empty".into())
            })
            .unwrap();
        assert_eq!(sample.parameter, expected);
        assert_eq!(request.parameter(), &expected);
        assert_eq!(request.root().index(), edge.id.0 as usize);
    }
}
