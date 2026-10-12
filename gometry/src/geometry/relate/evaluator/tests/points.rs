#[test]
fn same_geometry_containment_requires_actual_interior_support() {
    for wkt in [
        "POLYGON EMPTY",
        "POLYGON ((0 0,0 0,0 0,0 0))",
        "POLYGON ((0 0,1 0,2 0,0 0))",
        "POLYGON ((0 0,2 0,2 2,0 2,0 0),(0 0,2 0,2 2,0 2,0 0))",
        "POLYGON ((0 0,2 0,2 2,0 2,0 0,0 2,2 2,2 0,0 0))",
        "POLYGON ((0 0,4 4,0 4,4 0,0 0))",
        "POLYGON ((0 0,4 0,4 4,0 4,0 0),(1 1,2 1,2 2,1 2,1 1))",
        "POLYGON ((0 0,4 0,4 4,0 4,0 0),(5 5,6 5,6 6,5 6,5 5))",
        "GEOMETRYCOLLECTION (POLYGON ((0 0,1 0,2 0,0 0)), POINT (7 8))",
    ] {
        let shape = crate::io::parse_wkt(wkt).unwrap();
        let plan = PreparedPlan::transient(&shape);
        let oracle = partition_oracle(&shape, &shape).unwrap();
        for predicate in [
            Predicate::Contains,
            Predicate::Within,
            Predicate::Covers,
            Predicate::CoveredBy,
        ] {
            assert_eq!(
                self::predicate(&plan, &plan, predicate).unwrap(),
                matrix_predicates::decided(predicate, oracle),
                "{wkt}: {predicate:?}"
            );
        }
    }
}

#[test]
fn curated_locator_queries_match_partition_cell_facts() {
    run_curated_relation_corpus(true);
}

#[test]
fn large_polygon_locator_matches_partition_cell_facts() {
    let mut points = (0..256)
        .map(|index| {
            let angle = std::f64::consts::TAU * f64::from(index) / 256.0;
            Point::new_raw_xy(10.0 * angle.cos(), 10.0 * angle.sin())
        })
        .collect::<Vec<_>>();
    points.push(*points.first().expect("generated polygon has a vertex"));
    let shell = crate::geometry::Ring::from_closed(
        crate::geometry::CoordSeq::try_from_points(&points).unwrap(),
    )
    .unwrap();
    let polygon = Shape::Polygon(crate::geometry::Polygon::new(shell, Vec::new()));
    let point = Shape::Point(Point::new_raw_xy(0.0, 0.0));
    assert_locator_oracle_parity("256-vertex polygon", &polygon, &point);
}

#[test]
fn indexed_point_only_phase_matches_hole_boundary_oracle() {
    let polygon = crate::io::parse_wkt(
        "POLYGON ((0 0,10 0,10 10,0 10,0 0),\
             (2 2,4 2,4 4,2 4,2 2),(4 4,6 4,6 6,4 6,4 4))",
    )
    .unwrap();
    let polygon_plan = PreparedPlan::transient(&polygon);
    for (x, y) in [(3.0, 3.0), (2.0, 3.0), (4.0, 4.0), (0.0, 0.0), (3.0, 3.0)] {
        let point = Shape::Point(Point::new_raw_xy(x, y));
        let point_plan = PreparedPlan::transient(&point);
        let oracle = partition_oracle(&point, &polygon).unwrap();
        assert_eq!(
            relate(&point_plan, &polygon_plan).unwrap(),
            oracle,
            "POINT ({x} {y}): indexed P1 matrix"
        );
        assert_eq!(
            self::predicate(&point_plan, &polygon_plan, Predicate::Intersects).unwrap(),
            matrix_predicates::decided(Predicate::Intersects, oracle),
            "POINT ({x} {y}): indexed P1 intersects"
        );
    }
}
