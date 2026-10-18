#[test]
fn stored_ring_scans_skip_edges_outside_the_probe_height() {
    let Shape::Polygon(polygon) = shape(
        "POLYGON ((0 0,1 0,2 0,3 0,4 0,4 1,4 2,4 3,4 4,\
             3 4,2 4,1 4,0 4,0 3,0 2,0 1,0 0))",
    ) else {
        panic!("fixture is a polygon");
    };
    let probes = Cell::new(0);
    let query = CountedStoredQuery {
        query: StoredQuery(XY::new(2.0, 1.5)),
        axis_probes: &probes,
    };
    let mut location = StreamingLocation::new(query, LocationMode::Full);
    location.visit_polygon(&polygon).unwrap();
    assert_eq!(location.finish().unwrap(), SourcePointLocation {
        location: Loc::Interior,
        area: true,
    });
    assert!(probes.get() <= 4, "{} endpoint probes", probes.get());
}

#[test]
fn boundary_tangent_probe_remains_unresolved() {
    let polygon = shape("POLYGON ((0 0,2 0,2 2,0 2,0 0))");
    let plan = crate::geometry::PreparedPlan::transient(&polygon);
    let point = [
        ExactDyadic::from_f64(1.0),
        ExactDyadic::from_f64(0.0),
        ExactDyadic::one(),
    ];
    let tangent = [ExactDyadic::one(), ExactDyadic::from_f64(0.0)];
    let location = plan.locate_exact_offset(&point, &tangent).unwrap();
    assert_eq!(location, SourcePointLocation {
        location: Loc::Boundary,
        area: true,
    });
    assert_eq!(occupied_toward(location), OccupiedToward::Tangent);
}

#[test]
fn edge_atom_label_drops_isolated_point_support_at_an_external_hole_vertex() {
    let source = shape(
        "GEOMETRYCOLLECTION (\
             POLYGON ((0 0,2 0,2 2,0 2,0 0),\
                      (10 10,12 10,12 12,10 12,10 10)),\
             POINT (10 10))",
    );
    let node = cartesian_point_location_and_area(&source, Point::new_raw_xy(10.0, 10.0));
    assert_eq!(node.location, Loc::Interior);
    assert!(!node.area);
    assert_eq!(node.edge_atom_location(), Loc::Exterior);

    let filled = shape(
        "GEOMETRYCOLLECTION (\
             POLYGON ((0 0,20 0,20 20,0 20,0 0)),\
             POINT (5 5))",
    );
    let node = cartesian_point_location_and_area(&filled, Point::new_raw_xy(5.0, 5.0));
    assert_eq!(node.location, Loc::Interior);
    assert!(node.area);
    assert_eq!(node.edge_atom_location(), Loc::Interior);
}

#[rstest::rstest]
#[case::polygon_0_0_10_0_10_10_0_10_0_0_2_2_2_8_8_8_8_2_2_2(
    "POLYGON ((0 0,10 0,10 10,0 10,0 0),(2 2,2 8,8 8,8 2,2 2))",
    vec![
        (0.0, 0.0),
        (5.0, 0.0),
        (10.0, 5.0),
        (1.0, 1.0),
        (2.0, 5.0),
        (5.0, 5.0),
        (11.0, 5.0),
    ]
)]
#[case::multipolygon_0_0_4_0_4_4_0_4_0_0_4_4_8_4_8_8_4_8_4_4("MULTIPOLYGON (((0 0,4 0,4 4,0 4,0 0)),((4 4,8 4,8 8,4 8,4 4)))", vec![(0.0, 0.0), (2.0, 0.0), (4.0, 4.0), (6.0, 6.0), (9.0, 9.0)])]
#[case::polygon_0_0_4_0_2_0_0_0("POLYGON ((0 0,4 0,2 0,0 0))", vec![
    (0.0, 0.0),
    (1.0, 0.0),
    (2.0, 0.0),
    (4.0, 0.0),
    (2.0, 1.0),
])]
#[case::polygon_1_1_1_1_1_1_1_1_1_1_3_1_3_3_1_3_1_1("POLYGON ((1 1,1 1,1 1,1 1),(1 1,3 1,3 3,1 3,1 1))", vec![
            (1.0, 1.0),
            (2.0, 2.0),
            (3.0, 1.0),
            (4.0, 4.0),
        ])]
#[case::geometrycollection_polygon_0_0_4_0_4_4_0_4_0_0_l_89d7e33e(
    "GEOMETRYCOLLECTION (POLYGON ((0 0,4 0,4 4,0 4,0 0)),LINESTRING (0 0,2 0),LINESTRING (2 0,4 0),POINT (7 7))",
    vec![
        (0.0, 0.0),
        (1.0, 0.0),
        (2.0, 0.0),
        (4.0, 4.0),
        (7.0, 7.0),
        (8.0, 8.0),
    ]
)]
#[case::polygon_0_0_1_0_1_2e_300_0_2e_300_0_0("POLYGON ((0 0,1 0,1 2e-300,0 2e-300,0 0))", vec![
    (0.0, 0.0),
    (0.5, 0.0),
    (1.0, 1e-300),
    (0.5, 1e-300),
    (2.0, 1e-300),
])]
#[case::polygon_0_0_5e_324_0_5e_324_5e_324_0_5e_324_0_0("POLYGON ((0 0,5e-324 0,5e-324 5e-324,0 5e-324,0 0))", vec![
            (0.0, 0.0),
            (5e-324, 0.0),
            (5e-324, 5e-324),
            (0.0, 5e-324),
        ])]
#[case::polygon_1e300_1e300_1_0000000000000002e300_1e300_13c100f3(
    "POLYGON ((1e300 1e300,1.0000000000000002e300 1e300,1.0000000000000002e300 1.0000000000000002e300,1e300 1.0000000000000002e300,1e300 1e300))",
    vec![
        (1e300, 1e300),
        (1.000_000_000_000_000_2e300, 1e300),
        (1.000_000_000_000_000_2e300, 1.000_000_000_000_000_2e300),
        (1.000_000_000_000_000_2e300, 1.000_000_000_000_000_2e300),
    ]
)]
fn streaming_location_matches_source_support_on_degenerate_inputs(
    #[case] wkt: &'static str,
    #[case] probes: Vec<(f64, f64)>,
) {
    let source = shape(wkt);
    for (x, y) in probes {
        assert_streaming_matches_reference(&source, Point::new_raw_xy(x, y));
    }
}

fn assert_streaming_ring_vertices_and_edges(wkt: &str) {
    let source = shape(wkt);
    let Shape::Polygon(polygon) = &source else {
        panic!("fixture is a polygon");
    };
    let vertices: Vec<_> = polygon.shell.iter_coords().collect();
    for &point in &vertices {
        assert_streaming_matches_reference(&source, point);
    }
    for pair in vertices.windows(2) {
        let [start, end] = pair else {
            unreachable!();
        };
        assert_streaming_matches_reference(
            &source,
            Point::new_raw_xy(f64::midpoint(start.x, end.x), f64::midpoint(start.y, end.y)),
        );
    }
    for (x, y) in [(1.0, 1.0), (2.0, 2.0), (1.0, 3.0), (2.0, 5.0), (-1.0, 2.0)] {
        assert_streaming_matches_reference(&source, Point::new_raw_xy(x, y));
    }
}

#[test]
fn streaming_spike_vertices_and_edges_match_reference() {
    assert_streaming_ring_vertices_and_edges("POLYGON ((0 0,4 0,4 4,2 4,2 7,2 4,0 4,0 0))");
}

#[test]
fn streaming_bow_tie_vertices_and_edges_match_reference() {
    assert_streaming_ring_vertices_and_edges("POLYGON ((0 0,4 4,0 4,4 0,0 0))");
}

#[test]
fn streaming_repeated_vertices_and_edges_match_reference() {
    assert_streaming_ring_vertices_and_edges("POLYGON ((0 0,4 0,4 0,4 4,0 4,0 0))");
}

#[test]
fn streaming_collinear_retrace_vertices_and_edges_match_reference() {
    assert_streaming_ring_vertices_and_edges("POLYGON ((0 0,4 0,2 0,6 0,0 0))");
}

#[test]
fn streaming_self_touch_vertices_and_edges_match_reference() {
    assert_streaming_ring_vertices_and_edges("POLYGON ((0 0,2 2,0 4,4 4,2 2,4 0,0 0))");
}

#[test]
fn nonincident_line_query_has_no_contact_scratch_allocation() {
    let shape = Shape::LineString(crate::geometry::LineSeq::from_proven(
        crate::geometry::CoordSeq::try_from_vecs(
            (0..=4096).map(f64::from).collect(),
            (0..=4096).map(|index| f64::from(index % 2)).collect(),
            None,
            None,
        )
        .unwrap(),
    ));
    let source = ReferenceSourceSupport::from_shape(&shape);
    let (location, allocations) =
        crate::test_support::allocation_count(|| source.location(Point::new_raw_xy(2048.25, 0.4)));
    assert_eq!(location, Loc::Exterior);
    assert_eq!(allocations, 0);
}

#[test]
fn one_shot_polygon_location_has_no_source_or_contact_allocations() {
    let source = shape("POLYGON ((0 0,10 0,10 10,0 10,0 0),(2 2,2 8,8 8,8 2,2 2))");
    let (location, allocations) = crate::test_support::allocation_count(|| {
        cartesian_point_location(&source, Point::new_raw_xy(1.0, 1.0))
    });
    assert_eq!(location, Loc::Interior);
    assert_eq!(allocations, 0);
}
