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
    try_sort_by(&mut crossings, |left, right| {
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
