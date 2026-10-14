#[test]
fn independent_overlapping_logical_rings_bind_across_wall_faces() {
    let coordinates = [
        (0.0, 0.0),
        (2.0, 0.0),
        (3.0, 0.0),
        (4.0, 0.0),
        (4.0, 4.0),
        (3.0, 4.0),
        (2.0, 4.0),
        (0.0, 4.0),
        (2.0, -1.0),
        (3.0, -1.0),
        (3.0, 5.0),
        (2.0, 5.0),
    ];
    let atoms = vec![
        [0, 1],
        [1, 2],
        [2, 3],
        [3, 4],
        [4, 5],
        [5, 6],
        [6, 7],
        [7, 0],
        [8, 9],
        [9, 2],
        [2, 5],
        [5, 10],
        [10, 11],
        [11, 6],
        [6, 1],
        [1, 8],
    ];
    let mut work = Unmetered;
    let graph =
        admit_known_graph(points(&coordinates), atoms, (0..12).collect(), &mut work).unwrap();
    let layout = admit_logical_layout(
        &graph,
        vec![
            LogicalArealWalk {
                output: 0,
                walk: 0,
                vertices: vec![0, 1, 2, 3, 4, 5, 6, 7, 0],
            },
            LogicalArealWalk {
                output: 1,
                walk: 0,
                vertices: vec![8, 9, 2, 5, 10, 11, 6, 1, 8],
            },
        ],
        None,
        &mut work,
    )
    .unwrap();
    assert_eq!(
        layout
            .areal
            .as_ref()
            .map(|proof| proof.signature.face_keys.len()),
        Some(6),
        "the wall arrangement has Exterior and five bounded faces"
    );
    assert!(
        admit_candidate(&graph, &layout, graph.points.clone(), &mut work)
            .unwrap()
            .is_some(),
        "the unchanged noded reference must admit both complete ring walks"
    );

    let affine = points(&coordinates.map(|(x, y)| (x + y, y)));
    assert!(
        admit_candidate(&graph, &layout, affine, &mut work)
            .unwrap()
            .is_some(),
        "an exact orientation-preserving affine image keeps the same wall correspondence"
    );

    let make_ring = |vertices: &[u32]| -> Result<Ring> {
        let records = vertices
            .iter()
            .map(|&vertex| {
                let xy = graph
                    .points
                    .rounded
                    .get(vertex as usize)
                    .copied()
                    .ok_or_else(|| {
                        crate::geometry::GeometryErrorKind::Invalid(
                            "overlapping collection ring vertex is unknown".into(),
                        )
                    })
                    .unwrap();
                Ok(Point::new_raw_xy(xy.x, xy.y))
            })
            .collect::<Result<Vec<_>>>()
            .unwrap();
        Ring::try_from_closed(CoordSeq::try_from_points(&records).unwrap())
    };
    let left_vertices = [0, 1, 2, 3, 4, 5, 6, 7, 0];
    let right_vertices = [8, 9, 2, 5, 10, 11, 6, 1, 8];
    let sink = Shape::geometry_collection(vec![
        Shape::Polygon(Polygon::new(make_ring(&left_vertices).unwrap(), Vec::new())),
        Shape::Polygon(Polygon::new(
            make_ring(&right_vertices).unwrap(),
            Vec::new(),
        )),
    ])
    .unwrap();
    let literal_layout = ShapeLiteralLayout {
        structure: Some(ShapeStructure::capture(&sink)),
        points: Vec::new(),
        lines: Vec::new(),
        polygons: vec![
            LiteralPolygon {
                output: 0,
                axes: CoordinateAxes::XY,
                shell: LiteralSequence {
                    output: 0,
                    axes: CoordinateAxes::XY,
                    images: vec![0, 1, 2, 3, 4, 5, 6, 7, 0],
                },
                holes: Vec::new(),
            },
            LiteralPolygon {
                output: 1,
                axes: CoordinateAxes::XY,
                shell: LiteralSequence {
                    output: 1,
                    axes: CoordinateAxes::XY,
                    images: vec![8, 9, 2, 5, 10, 11, 6, 1, 8],
                },
                holes: Vec::new(),
            },
        ],
    };
    let mut receiver = TestLiteralReceiver {
        images: ImageRows(graph.points.exact.clone()),
    };
    assert!(matches!(
        bind_literals(
            FrozenLiteralBuffers::shapes(sink, literal_layout),
            &mut receiver,
            &mut work,
        )
        .unwrap(),
        LiteralCheck::Pass(_)
    ));
}

#[test]
fn exterior_identity_tracks_global_face_zero_not_the_first_stable_key() {
    let mut work = Unmetered;
    let graph = admit_known_graph(
        points(&[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0)]),
        vec![[0, 1], [1, 2], [2, 3], [3, 0]],
        vec![0, 1, 2, 3],
        &mut work,
    )
    .unwrap();
    let layout = polygon_layout(&graph, &[0, 1, 2, 3, 0]);
    let proof = layout.areal.as_ref().unwrap();
    let [interior, exterior] = proof.signature.face_keys.as_slice() else {
        panic!("the square has distinct interior and exterior wall faces");
    };

    // On this CCW square the interior's stable boundary starts at dart 0,
    // so it sorts before the unbounded face's stable boundary at dart 1.
    assert_eq!(interior.first(), Some(&0));
    assert_eq!(exterior.first(), Some(&1));
    assert_eq!(proof.signature.exterior_face, 1);
}

#[test]
fn areal_fan_uses_cyclic_order_not_opposed_ray_identity() {
    let mut work = Unmetered;
    let graph_points = points(&[(0.0, 0.0), (2.0, 0.0), (0.0, 2.0), (-1.0, 0.0), (0.0, -1.0)]);
    let graph = admit_known_graph(
        graph_points,
        vec![[0, 1], [1, 2], [2, 0], [0, 3], [0, 4]],
        vec![0, 1, 2, 3, 4],
        &mut work,
    )
    .unwrap();
    let layout = polygon_layout(&graph, &[0, 1, 2, 0]);

    // A half-turn changes the angular sort cut, not the anchored stable-dart cycle.
    let rotated = points(&[(0.0, 0.0), (-2.0, 0.0), (0.0, -2.0), (1.0, 0.0), (0.0, 1.0)]);
    assert!(
        admit_candidate(&graph, &layout, rotated.clone(), &mut work)
            .unwrap()
            .is_some()
    );

    // The area triangle stays fixed; its attached line rays bend through
    // opposite directions while retaining their cyclic order.
    let opposition_changed = points(&[
        (0.0, 0.0),
        (2.0, 0.0),
        (0.0, 2.0),
        (-1.0, -1.0),
        (1.0, -1.0),
    ]);
    assert!(
        admit_candidate(&graph, &layout, opposition_changed, &mut work)
            .unwrap()
            .is_some()
    );

    // Swapping the two stable line darts changes the actual rotation.
    let reordered = points(&[(0.0, 0.0), (2.0, 0.0), (0.0, 2.0), (0.0, -1.0), (-1.0, 0.0)]);
    assert!(
        admit_candidate(&graph, &layout, reordered, &mut work)
            .unwrap()
            .is_none()
    );
    assert_eq!(rotated.rounded[0], XY::new(0.0, 0.0));
}

#[test]
fn known_graph_accepts_admitted_midpoint_repair_that_changes_opposition() {
    let two_to_53 = 1_i64 << 53;
    let ratio = |numerator, denominator| {
        ExactRational::new(
            ExactDyadic::from_i64(numerator),
            ExactDyadic::from_i64(denominator),
        )
        .unwrap()
    };
    let one = ExactRational::one();
    let zero = ExactRational::zero();
    let quarter = ratio(1, 4);
    let half = ratio(1, 2);
    let three_quarters = ratio(3, 4);
    let midpoint_x = ratio(two_to_53 + 1, two_to_53);
    let upper_x = ratio(two_to_53 + 2, two_to_53);
    let reference_points = exact_points(&[
        (one.clone(), zero),                // A
        (midpoint_x.clone(), half.clone()), // B
        (upper_x.clone(), one.clone()),     // C
        (midpoint_x, three_quarters),       // F
        (one.clone(), half),                // D
        (one, quarter),                     // E
    ]);
    let mut work = Unmetered;
    let graph = admit_known_graph(
        reference_points,
        vec![[0, 1], [1, 2], [2, 3], [3, 4], [4, 5], [5, 0]],
        vec![0, 1, 2, 3, 4, 5],
        &mut work,
    )
    .unwrap();
    let layout = polygon_layout(&graph, &[0, 1, 2, 3, 4, 5, 0]);

    let nearest = exact_points(&[
        (ExactRational::from_f64(1.0), ExactRational::zero()),
        (ExactRational::from_f64(1.0), ExactRational::from_f64(0.5)),
        (upper_x.clone(), ExactRational::one()),
        (ExactRational::from_f64(1.0), ExactRational::from_f64(0.75)),
        (ExactRational::from_f64(1.0), ExactRational::from_f64(0.5)),
        (ExactRational::from_f64(1.0), ExactRational::from_f64(0.25)),
    ]);
    assert!(
        admit_candidate(&graph, &layout, nearest, &mut work)
            .unwrap()
            .is_none()
    );

    let repaired = exact_points(&[
        (ExactRational::from_f64(1.0), ExactRational::zero()),
        (upper_x, ExactRational::from_f64(0.5)),
        (
            ExactRational::from_f64(1.0 + 2.0_f64.powi(-52)),
            ExactRational::one(),
        ),
        (ExactRational::from_f64(1.0), ExactRational::from_f64(0.75)),
        (ExactRational::from_f64(1.0), ExactRational::from_f64(0.5)),
        (ExactRational::from_f64(1.0), ExactRational::from_f64(0.25)),
    ]);
    let Some(assignment) = admit_candidate(&graph, &layout, repaired.clone(), &mut work).unwrap()
    else {
        panic!("the prescribed upper-neighbor assignment preserves this known graph");
    };
    let ring_points = [0_u32, 1, 2, 3, 4, 5, 0]
        .into_iter()
        .map(|index| {
            let point = repaired
                .rounded
                .get(index as usize)
                .copied()
                .ok_or_else(|| {
                    crate::geometry::GeometryErrorKind::Invalid(
                        "repaired point is outside its written ring".into(),
                    )
                })?;
            Ok(Point::new_raw_xy(point.x, point.y))
        })
        .collect::<Result<Vec<_>>>()
        .unwrap();
    let shape = Shape::Polygon(Polygon::new(
        Ring::try_from_closed(CoordSeq::try_from_points(&ring_points).unwrap()).unwrap(),
        Vec::new(),
    ));
    let layout = ShapeLiteralLayout {
        structure: Some(ShapeStructure::capture(&shape)),
        points: Vec::new(),
        lines: Vec::new(),
        polygons: vec![LiteralPolygon {
            output: 0,
            axes: CoordinateAxes::XY,
            shell: LiteralSequence {
                output: 0,
                axes: CoordinateAxes::XY,
                images: vec![0, 1, 2, 3, 4, 5, 0],
            },
            holes: Vec::new(),
        }],
    };
    let mut receiver = TestLiteralReceiver {
        images: ImageRows(assignment.points().exact.clone()),
    };
    assert!(matches!(
        bind_literals(
            FrozenLiteralBuffers::shapes(shape, layout),
            &mut receiver,
            &mut work,
        )
        .unwrap(),
        LiteralCheck::Pass(_)
    ));
}

#[test]
fn degree_three_areal_fan_accepts_opposition_change_without_reordering() {
    let two_to_53 = 1_i64 << 53;
    let epsilon = ExactRational::new(ExactDyadic::one(), ExactDyadic::from_i64(two_to_53)).unwrap();
    let constructed_y = ExactRational::one().try_add(&epsilon).unwrap();
    let reference = exact_points(&[
        (ExactRational::zero(), ExactRational::zero()), // O
        (ExactRational::one(), ExactRational::from_f64(-1.0)), // P
        (ExactRational::zero(), ExactRational::one()),  // R
        (ExactRational::from_f64(-1.0), constructed_y), // Q
        (ExactRational::from_f64(-1.0), ExactRational::one()), // fixed collision
    ]);
    let mut work = Unmetered;
    let graph = admit_known_graph(
        reference,
        vec![[0, 1], [1, 2], [2, 0], [0, 3]],
        vec![0, 1, 2, 3, 4],
        &mut work,
    )
    .unwrap();
    let layout = polygon_layout(&graph, &[0, 1, 2, 0]);
    let nearest = points(&[
        (0.0, 0.0),
        (1.0, -1.0),
        (0.0, 1.0),
        (-1.0, 1.0),
        (-1.0, 1.0),
    ]);
    assert!(
        admit_candidate(&graph, &layout, nearest, &mut work)
            .unwrap()
            .is_none()
    );
    let upper = points(&[
        (0.0, 0.0),
        (1.0, -1.0),
        (0.0, 1.0),
        (-1.0, 1.0 + 2.0_f64.powi(-52)),
        (-1.0, 1.0),
    ]);
    assert!(
        admit_candidate(&graph, &layout, upper, &mut work)
            .unwrap()
            .is_some()
    );
}

#[test]
fn reflected_closed_line_seals_beside_an_unrelated_polygon() {
    let half_ulp =
        ExactRational::new(ExactDyadic::one(), ExactDyadic::from_i64(1_i64 << 54)).unwrap();
    let line_apex_y = ExactRational::one().try_add(&half_ulp).unwrap();
    let reference = exact_points(&[
        (ExactRational::from_f64(-4.0), ExactRational::from_f64(-4.0)),
        (ExactRational::from_f64(-2.0), ExactRational::from_f64(-4.0)),
        (ExactRational::from_f64(-2.0), ExactRational::from_f64(-2.0)),
        (ExactRational::from_f64(-4.0), ExactRational::from_f64(-2.0)),
        (ExactRational::zero(), ExactRational::one()),
        (ExactRational::from_f64(2.0), ExactRational::one()),
        (ExactRational::one(), line_apex_y),
    ]);
    let mut work = Unmetered;
    let graph = admit_known_graph(
        reference,
        vec![
            [0, 1],
            [1, 2],
            [2, 3],
            [3, 0], // polygon wall
            [4, 5],
            [5, 6],
            [6, 4], // separate closed line
        ],
        (0..7).collect(),
        &mut work,
    )
    .unwrap();
    let layout = polygon_layout(&graph, &[0, 1, 2, 3, 0]);

    let nearest = points(&[
        (-4.0, -4.0),
        (-2.0, -4.0),
        (-2.0, -2.0),
        (-4.0, -2.0),
        (0.0, 1.0),
        (2.0, 1.0),
        (1.0, 1.0),
    ]);
    assert!(
        admit_candidate(&graph, &layout, nearest, &mut work)
            .unwrap()
            .is_none()
    );

    let lower = 1.0 - 2.0_f64.powi(-53);
    let repaired = points(&[
        (-4.0, -4.0),
        (-2.0, -4.0),
        (-2.0, -2.0),
        (-4.0, -2.0),
        (0.0, 1.0),
        (2.0, 1.0),
        (1.0, lower),
    ]);
    assert!(
        admit_candidate(&graph, &layout, repaired.clone(), &mut work)
            .unwrap()
            .is_some()
    );

    let polygon = Shape::Polygon(Polygon::new(
        Ring::try_from_closed(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(-4.0, -4.0),
                Point::new_raw_xy(-2.0, -4.0),
                Point::new_raw_xy(-2.0, -2.0),
                Point::new_raw_xy(-4.0, -2.0),
                Point::new_raw_xy(-4.0, -4.0),
            ])
            .unwrap(),
        )
        .unwrap(),
        Vec::new(),
    ));
    let line = Shape::LineString(LineSeq::from_proven(
        CoordSeq::try_from_points(&[
            Point::new_raw_xy(0.0, 1.0),
            Point::new_raw_xy(2.0, 1.0),
            Point::new_raw_xy(1.0, lower),
            Point::new_raw_xy(0.0, 1.0),
        ])
        .unwrap(),
    ));
    let sink = Shape::geometry_collection(vec![polygon, line]).unwrap();
    let literal_layout = ShapeLiteralLayout {
        structure: Some(ShapeStructure::capture(&sink)),
        points: Vec::new(),
        lines: vec![LiteralSequence {
            output: 0,
            axes: CoordinateAxes::XY,
            images: vec![4, 5, 6, 4],
        }],
        polygons: vec![LiteralPolygon {
            output: 0,
            axes: CoordinateAxes::XY,
            shell: LiteralSequence {
                output: 0,
                axes: CoordinateAxes::XY,
                images: vec![0, 1, 2, 3, 0],
            },
            holes: Vec::new(),
        }],
    };
    let mut receiver = TestLiteralReceiver {
        images: ImageRows(repaired.exact),
    };
    assert!(matches!(
        bind_literals(
            FrozenLiteralBuffers::shapes(sink, literal_layout),
            &mut receiver,
            &mut work,
        )
        .unwrap(),
        LiteralCheck::Pass(_)
    ));
}

#[test]
fn point_leaving_a_closed_line_face_keeps_its_areal_host() {
    let half_ulp =
        ExactRational::new(ExactDyadic::one(), ExactDyadic::from_i64(1_i64 << 54)).unwrap();
    let point_y = ExactRational::one().try_add(&half_ulp).unwrap();
    let reference = exact_points(&[
        (ExactRational::from_f64(-4.0), ExactRational::from_f64(-4.0)),
        (ExactRational::from_f64(-2.0), ExactRational::from_f64(-4.0)),
        (ExactRational::from_f64(-2.0), ExactRational::from_f64(-2.0)),
        (ExactRational::from_f64(-4.0), ExactRational::from_f64(-2.0)),
        (ExactRational::zero(), ExactRational::one()),
        (ExactRational::from_f64(2.0), ExactRational::one()),
        (ExactRational::one(), ExactRational::from_f64(2.0)),
        (ExactRational::one(), point_y),
    ]);
    let mut work = Unmetered;
    let graph = admit_known_graph(
        reference,
        vec![
            [0, 1],
            [1, 2],
            [2, 3],
            [3, 0], // polygon wall
            [4, 5],
            [5, 6],
            [6, 4], // closed line, not a wall
        ],
        (0..8).collect(),
        &mut work,
    )
    .unwrap();
    let layout = polygon_layout(&graph, &[0, 1, 2, 3, 0]);

    let nearest = points(&[
        (-4.0, -4.0),
        (-2.0, -4.0),
        (-2.0, -2.0),
        (-4.0, -2.0),
        (0.0, 1.0),
        (2.0, 1.0),
        (1.0, 2.0),
        (1.0, 1.0),
    ]);
    assert!(
        admit_candidate(&graph, &layout, nearest, &mut work)
            .unwrap()
            .is_none()
    );

    let lower = 1.0 - 2.0_f64.powi(-53);
    let repaired = points(&[
        (-4.0, -4.0),
        (-2.0, -4.0),
        (-2.0, -2.0),
        (-4.0, -2.0),
        (0.0, 1.0),
        (2.0, 1.0),
        (1.0, 2.0),
        (1.0, lower),
    ]);
    assert!(
        admit_candidate(&graph, &layout, repaired.clone(), &mut work)
            .unwrap()
            .is_some()
    );

    let polygon = Shape::Polygon(Polygon::new(
        Ring::try_from_closed(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(-4.0, -4.0),
                Point::new_raw_xy(-2.0, -4.0),
                Point::new_raw_xy(-2.0, -2.0),
                Point::new_raw_xy(-4.0, -2.0),
                Point::new_raw_xy(-4.0, -4.0),
            ])
            .unwrap(),
        )
        .unwrap(),
        Vec::new(),
    ));
    let line = Shape::LineString(LineSeq::from_proven(
        CoordSeq::try_from_points(&[
            Point::new_raw_xy(0.0, 1.0),
            Point::new_raw_xy(2.0, 1.0),
            Point::new_raw_xy(1.0, 2.0),
            Point::new_raw_xy(0.0, 1.0),
        ])
        .unwrap(),
    ));
    let point = Shape::Point(Point::new_raw_xy(1.0, lower));
    let sink = Shape::geometry_collection(vec![polygon, line, point]).unwrap();
    let literal_layout = ShapeLiteralLayout {
        structure: Some(ShapeStructure::capture(&sink)),
        points: vec![LiteralPoint {
            output: 0,
            axes: CoordinateAxes::XY,
            image: 7,
        }],
        lines: vec![LiteralSequence {
            output: 0,
            axes: CoordinateAxes::XY,
            images: vec![4, 5, 6, 4],
        }],
        polygons: vec![LiteralPolygon {
            output: 0,
            axes: CoordinateAxes::XY,
            shell: LiteralSequence {
                output: 0,
                axes: CoordinateAxes::XY,
                images: vec![0, 1, 2, 3, 0],
            },
            holes: Vec::new(),
        }],
    };
    let mut receiver = TestLiteralReceiver {
        images: ImageRows(repaired.exact),
    };
    assert!(matches!(
        bind_literals(
            FrozenLiteralBuffers::shapes(sink, literal_layout),
            &mut receiver,
            &mut work,
        )
        .unwrap(),
        LiteralCheck::Pass(_)
    ));
}

#[test]
fn moving_a_point_across_an_areal_wall_changes_its_host() {
    let half_ulp =
        ExactRational::new(ExactDyadic::one(), ExactDyadic::from_i64(1_i64 << 54)).unwrap();
    let point_x = ExactRational::one().try_add(&half_ulp).unwrap();
    let reference = exact_points(&[
        (ExactRational::zero(), ExactRational::zero()),
        (ExactRational::one(), ExactRational::zero()),
        (ExactRational::one(), ExactRational::from_f64(2.0)),
        (ExactRational::zero(), ExactRational::from_f64(2.0)),
        (point_x, ExactRational::one()),
    ]);
    let mut work = Unmetered;
    let graph = admit_known_graph(
        reference,
        vec![[0, 1], [1, 2], [2, 3], [3, 0]],
        vec![0, 1, 2, 3, 4],
        &mut work,
    )
    .unwrap();
    let layout = polygon_layout(&graph, &[0, 1, 2, 3, 0]);

    let nearest = points(&[(0.0, 0.0), (1.0, 0.0), (1.0, 2.0), (0.0, 2.0), (1.0, 1.0)]);
    assert!(
        admit_candidate(&graph, &layout, nearest, &mut work)
            .unwrap()
            .is_none()
    );

    let lower = points(&[
        (0.0, 0.0),
        (1.0, 0.0),
        (1.0, 2.0),
        (0.0, 2.0),
        (1.0 - 2.0_f64.powi(-53), 1.0),
    ]);
    assert!(
        admit_candidate(&graph, &layout, lower, &mut work)
            .unwrap()
            .is_none()
    );

    let upper = points(&[
        (0.0, 0.0),
        (1.0, 0.0),
        (1.0, 2.0),
        (0.0, 2.0),
        (1.0 + 2.0_f64.powi(-52), 1.0),
    ]);
    assert!(
        admit_candidate(&graph, &layout, upper, &mut work)
            .unwrap()
            .is_some()
    );
}

#[test]
fn attached_chord_between_wall_vertices_uses_the_areal_host_view() {
    let half_ulp =
        ExactRational::new(ExactDyadic::one(), ExactDyadic::from_i64(1_i64 << 54)).unwrap();
    let line_midpoint_y = ExactRational::one().try_add(&half_ulp).unwrap();
    let reference = exact_points(&[
        (ExactRational::zero(), ExactRational::zero()),
        (ExactRational::from_f64(2.0), ExactRational::zero()),
        (ExactRational::from_f64(2.0), ExactRational::from_f64(2.0)),
        (ExactRational::zero(), ExactRational::from_f64(2.0)),
        (ExactRational::one(), line_midpoint_y),
    ]);
    let mut work = Unmetered;
    let graph = admit_known_graph(
        reference,
        vec![[0, 1], [1, 2], [2, 3], [3, 0], [0, 4], [4, 2]],
        (0..5).collect(),
        &mut work,
    )
    .unwrap();
    let layout = polygon_layout(&graph, &[0, 1, 2, 3, 0]);

    let nearest = points(&[(0.0, 0.0), (2.0, 0.0), (2.0, 2.0), (0.0, 2.0), (1.0, 1.0)]);
    assert!(
        admit_candidate(&graph, &layout, nearest, &mut work)
            .unwrap()
            .is_some()
    );
    let lower = points(&[
        (0.0, 0.0),
        (2.0, 0.0),
        (2.0, 2.0),
        (0.0, 2.0),
        (1.0, 1.0 - 2.0_f64.powi(-53)),
    ]);
    assert!(
        admit_candidate(&graph, &layout, lower, &mut work)
            .unwrap()
            .is_some()
    );
}

#[test]
fn line_bridge_between_areal_components_keeps_each_wall_host() {
    let half_ulp =
        ExactRational::new(ExactDyadic::one(), ExactDyadic::from_i64(1_i64 << 54)).unwrap();
    let bridge_y = ExactRational::one().try_add(&half_ulp).unwrap();
    let reference = exact_points(&[
        (ExactRational::zero(), ExactRational::zero()), // left lower-left
        (ExactRational::one(), ExactRational::zero()),  // left lower-right
        (ExactRational::one(), ExactRational::one()),   // left upper-right
        (ExactRational::zero(), ExactRational::one()),  // left upper-left
        (ExactRational::from_f64(3.0), ExactRational::zero()), // right lower-left
        (ExactRational::from_f64(4.0), ExactRational::zero()), // right lower-right
        (ExactRational::from_f64(4.0), ExactRational::one()), // right upper-right
        (ExactRational::from_f64(3.0), ExactRational::one()), // right upper-left
        (ExactRational::from_f64(2.0), bridge_y),       // bridge vertex
    ]);
    let mut work = Unmetered;
    let graph = admit_known_graph(
        reference,
        vec![
            [0, 1],
            [1, 2],
            [2, 3],
            [3, 0],
            [4, 5],
            [5, 6],
            [6, 7],
            [7, 4],
            [2, 8],
            [8, 7],
        ],
        (0..9).collect(),
        &mut work,
    )
    .unwrap();
    let layout = admit_logical_layout(
        &graph,
        vec![
            LogicalArealWalk {
                output: 0,
                walk: 0,
                vertices: vec![0, 1, 2, 3, 0],
            },
            LogicalArealWalk {
                output: 1,
                walk: 0,
                vertices: vec![4, 5, 6, 7, 4],
            },
        ],
        None,
        &mut work,
    )
    .unwrap();

    let nearest = points(&[
        (0.0, 0.0),
        (1.0, 0.0),
        (1.0, 1.0),
        (0.0, 1.0),
        (3.0, 0.0),
        (4.0, 0.0),
        (4.0, 1.0),
        (3.0, 1.0),
        (2.0, 1.0),
    ]);
    assert!(
        admit_candidate(&graph, &layout, nearest, &mut work)
            .unwrap()
            .is_some()
    );
    let lower = points(&[
        (0.0, 0.0),
        (1.0, 0.0),
        (1.0, 1.0),
        (0.0, 1.0),
        (3.0, 0.0),
        (4.0, 0.0),
        (4.0, 1.0),
        (3.0, 1.0),
        (2.0, 1.0 - 2.0_f64.powi(-53)),
    ]);
    assert!(
        admit_candidate(&graph, &layout, lower, &mut work)
            .unwrap()
            .is_some()
    );
}

#[test]
fn line_only_cycle_does_not_acquire_areal_orientation_or_host_obligations() {
    let reference = points(&[(0.0, 0.0), (2.0, 0.0), (0.0, 2.0)]);
    let reversed = points(&[(0.0, 0.0), (2.0, 0.0), (0.0, -2.0)]);
    let edges = vec![[0, 1], [1, 2], [2, 0]];
    let identities = vec![0, 1, 2];
    let mut work = Unmetered;
    let line_graph = admit_known_graph(
        points(&[(0.0, 0.0), (2.0, 0.0), (0.0, 2.0)]),
        edges.clone(),
        identities.clone(),
        &mut work,
    )
    .unwrap();
    let line_layout = admit_logical_layout(&line_graph, Vec::new(), None, &mut work).unwrap();
    assert!(
        admit_candidate(&line_graph, &line_layout, reversed.clone(), &mut work)
            .unwrap()
            .is_some()
    );

    let polygon_graph = admit_known_graph(reference, edges, identities, &mut work).unwrap();
    let layout = polygon_layout(&polygon_graph, &[0, 1, 2, 0]);
    assert!(
        admit_candidate(&polygon_graph, &layout, reversed, &mut work)
            .unwrap()
            .is_none()
    );
}

#[test]
fn line_only_actual_shape_seals_while_reversed_polygon_face_is_rejected() {
    let reference = points(&[(0.0, 0.0), (2.0, 0.0), (0.0, 2.0)]);
    let reversed = points(&[(0.0, 0.0), (2.0, 0.0), (0.0, -2.0)]);
    let edges = vec![[0, 1], [1, 2], [2, 0]];
    let identities = vec![0, 1, 2];
    let mut work = Unmetered;
    let line_graph = admit_known_graph(
        points(&[(0.0, 0.0), (2.0, 0.0), (0.0, 2.0)]),
        edges.clone(),
        identities.clone(),
        &mut work,
    )
    .unwrap();
    let Some(_assignment) = admit_candidate(
        &line_graph,
        &admit_logical_layout(&line_graph, Vec::new(), None, &mut work).unwrap(),
        reversed.clone(),
        &mut work,
    )
    .unwrap() else {
        panic!("closed line accepts its preserved incidence after reflection");
    };

    let line = Shape::LineString(LineSeq::from_proven(
        CoordSeq::try_from_points(&[
            Point::new_raw_xy(0.0, 0.0),
            Point::new_raw_xy(2.0, 0.0),
            Point::new_raw_xy(0.0, -2.0),
            Point::new_raw_xy(0.0, 0.0),
        ])
        .unwrap(),
    ));
    let literal_layout = ShapeLiteralLayout {
        structure: Some(ShapeStructure::capture(&line)),
        points: Vec::<LiteralPoint<u32>>::new(),
        lines: vec![LiteralSequence {
            output: 0,
            axes: CoordinateAxes::XY,
            images: vec![0, 1, 2, 0],
        }],
        polygons: Vec::new(),
    };
    let mut receiver = TestLiteralReceiver {
        images: ImageRows(vec![
            ExactChartPoint::from_rationals(
                ExactRational::from_f64(0.0),
                ExactRational::from_f64(0.0),
            ),
            ExactChartPoint::from_rationals(
                ExactRational::from_f64(2.0),
                ExactRational::from_f64(0.0),
            ),
            ExactChartPoint::from_rationals(
                ExactRational::from_f64(0.0),
                ExactRational::from_f64(-2.0),
            ),
        ]),
    };
    let sealed = match bind_literals(
        FrozenLiteralBuffers::shapes(line, literal_layout),
        &mut receiver,
        &mut work,
    )
    .unwrap()
    {
        LiteralCheck::Pass(sealed) => sealed,
        LiteralCheck::Reject(_) => panic!("actual closed-line slots match their images"),
    };
    assert!(matches!(
        sealed.into_shape().expect("scalar sink owns a shape"),
        Shape::LineString(_)
    ));

    let polygon_graph = admit_known_graph(reference, edges, identities, &mut work).unwrap();
    let layout = polygon_layout(&polygon_graph, &[0, 1, 2, 0]);
    assert!(
        admit_candidate(&polygon_graph, &layout, reversed, &mut work)
            .unwrap()
            .is_none()
    );
}
