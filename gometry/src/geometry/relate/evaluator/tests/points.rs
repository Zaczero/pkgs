#[rstest::rstest]
#[case::polygon_empty("POLYGON EMPTY")]
#[case::polygon_0_0_0_0_0_0_0_0("POLYGON ((0 0,0 0,0 0,0 0))")]
#[case::polygon_0_0_1_0_2_0_0_0("POLYGON ((0 0,1 0,2 0,0 0))")]
#[case::polygon_0_0_2_0_2_2_0_2_0_0_0_0_2_0_2_2_0_2_0_0(
    "POLYGON ((0 0,2 0,2 2,0 2,0 0),(0 0,2 0,2 2,0 2,0 0))"
)]
#[case::polygon_0_0_2_0_2_2_0_2_0_0_0_2_2_2_2_0_0_0(
    "POLYGON ((0 0,2 0,2 2,0 2,0 0,0 2,2 2,2 0,0 0))"
)]
#[case::polygon_0_0_4_4_0_4_4_0_0_0("POLYGON ((0 0,4 4,0 4,4 0,0 0))")]
#[case::polygon_0_0_4_0_4_4_0_4_0_0_1_1_2_1_2_2_1_2_1_1(
    "POLYGON ((0 0,4 0,4 4,0 4,0 0),(1 1,2 1,2 2,1 2,1 1))"
)]
#[case::polygon_0_0_4_0_4_4_0_4_0_0_5_5_6_5_6_6_5_6_5_5(
    "POLYGON ((0 0,4 0,4 4,0 4,0 0),(5 5,6 5,6 6,5 6,5 5))"
)]
#[case::geometrycollection_polygon_0_0_1_0_2_0_0_0_point_7_8(
    "GEOMETRYCOLLECTION (POLYGON ((0 0,1 0,2 0,0 0)), POINT (7 8))"
)]
fn same_geometry_containment_requires_actual_interior_support(#[case] wkt: &'static str) {
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

mod locator_corpus {
    use super::*;
    relation_corpus!(assert_locator_oracle_parity);
}

#[test]
fn indexed_polygon_locator_matches_partition_cell_facts() {
    let mut points = (0..32)
        .map(|index| {
            let angle = std::f64::consts::TAU * f64::from(index) / 32.0;
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
    assert_locator_oracle_parity("32-vertex polygon", &polygon, &point);
}

#[rstest::rstest]
#[case::x_3_0(3.0, 3.0)]
#[case::x_2_0(2.0, 3.0)]
#[case::x_4_0(4.0, 4.0)]
#[case::x_0_0(0.0, 0.0)]
#[case::x_3_0_be45754f(3.0, 3.0)]
fn indexed_point_only_phase_matches_hole_boundary_oracle(#[case] x: f64, #[case] y: f64) {
    let polygon = crate::io::parse_wkt(
        "POLYGON ((0 0,10 0,10 10,0 10,0 0),\
             (2 2,4 2,4 4,2 4,2 2),(4 4,6 4,6 6,4 6,4 4))",
    )
    .unwrap();
    let polygon_plan = PreparedPlan::transient(&polygon);

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
