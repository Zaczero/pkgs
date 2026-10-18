#[test]
fn prepared_exact_rectangle_queries_match_known_locations() {
    let frame = GeographicFrame::crs84();
    let outer = Shape::Polygon(rectangle(0.0, 0.0, 10.0, 10.0));
    let donut = Shape::Polygon(donut());
    let line = Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(10.0, 10.0),
            ])
            .unwrap(),
        )
        .unwrap(),
    );
    let seam = Shape::Polygon(polygon(&[
        [170.0, -10.0],
        [-170.0, -10.0],
        [-170.0, 10.0],
        [170.0, 10.0],
        [170.0, -10.0],
    ]));
    let polar = Shape::Polygon(polygon(&[
        [-10.0, 80.0],
        [10.0, 80.0],
        [10.0, 90.0],
        [-10.0, 90.0],
        [-10.0, 80.0],
    ]));
    let outer_q = prepared_q_topology(&outer, frame);
    let donut_q = prepared_q_topology(&donut, frame);
    let line_q = prepared_q_topology(&line, frame);
    let seam_q = prepared_q_topology(&seam, frame);
    let polar_q = prepared_q_topology(&polar, frame);
    for (name, topology, bounds, expected) in [
        (
            "interior",
            &outer_q,
            Bounds::new_raw(2.0, 2.0, 8.0, 8.0),
            ExactGeographicRectangleLocation::Interior,
        ),
        (
            "boundary aligned",
            &outer_q,
            Bounds::new_raw(0.0, 2.0, 8.0, 8.0),
            ExactGeographicRectangleLocation::Interior,
        ),
        (
            "crossing",
            &outer_q,
            Bounds::new_raw(8.0, 2.0, 12.0, 8.0),
            ExactGeographicRectangleLocation::Boundary,
        ),
        (
            "disjoint",
            &outer_q,
            Bounds::new_raw(12.0, 12.0, 14.0, 14.0),
            ExactGeographicRectangleLocation::Outside,
        ),
        (
            "line",
            &line_q,
            Bounds::new_raw(2.0, 2.0, 8.0, 8.0),
            ExactGeographicRectangleLocation::Boundary,
        ),
        (
            "hole",
            &donut_q,
            Bounds::new_raw(4.0, 4.0, 6.0, 6.0),
            ExactGeographicRectangleLocation::Outside,
        ),
        (
            "hole crossing",
            &donut_q,
            Bounds::new_raw(2.0, 2.0, 5.0, 5.0),
            ExactGeographicRectangleLocation::Boundary,
        ),
        (
            "seam interior",
            &seam_q,
            Bounds::new_raw(175.0, -5.0, 179.0, 5.0),
            ExactGeographicRectangleLocation::Interior,
        ),
        (
            "seam exterior",
            &seam_q,
            Bounds::new_raw(-5.0, -5.0, 5.0, 5.0),
            ExactGeographicRectangleLocation::Outside,
        ),
        (
            "polar interior",
            &polar_q,
            Bounds::new_raw(0.0, 81.0, 1.0, 82.0),
            ExactGeographicRectangleLocation::Interior,
        ),
    ] {
        assert_eq!(
            topology.classify_rectangle(bounds).unwrap(),
            Some(expected),
            "{name}",
        );
    }
}
#[rstest::rstest]
#[case::hole(Shape::Polygon(donut()))]
#[case::line_in_hole({
    let donut = Shape::Polygon(donut());
    let line_in_hole = Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(4.0, 5.0),
                Point::new_raw_xy(6.0, 5.0)
            ])
            .unwrap(),
        )
        .unwrap(),
    );
    Shape::geometry_collection(vec![donut, line_in_hole]).unwrap()
})]
fn prepared_rectangle_refinement_keeps_an_exact_hole_uncovered(#[case] source: Shape) {
    let frame = GeographicFrame::crs84();
    let bounds = Bounds::new_raw(3.0, 3.0, 7.0, 7.0);
    assert_eq!(
        prepared_q_topology(&source, frame)
            .classify_rectangle(bounds)
            .unwrap(),
        Some(ExactGeographicRectangleLocation::Boundary),
    );
}
#[test]
fn prepared_rectangle_refinement_declines_an_adjacent_float_midpoint() {
    let minx: f64 = 1.0;
    let maxx = minx.next_up();
    let bounds = Bounds::new_raw(minx, 0.0, maxx, 1.0);
    assert_eq!(f64::midpoint(minx, maxx), minx);
    let source = Shape::Polygon(rectangle(minx, 0.0, maxx, 1.0));
    assert_eq!(
        prepared_q_topology(&source, GeographicFrame::crs84())
            .classify_rectangle(bounds)
            .unwrap(),
        None,
    );
}
#[rstest::rstest]
#[case::bounds_new_raw_180_0_81_0_179_0_85_0(Bounds::new_raw(-180.0, 81.0, -179.0, 85.0))]
#[case::bounds_new_raw_0_0_85_0_10_0_90_0(Bounds::new_raw(0.0, 85.0, 10.0, 90.0))]
#[case::bounds_new_raw_180_0_1_0_180_0_1_0(Bounds::new_raw(-180.0, -1.0, 180.0, 1.0))]
fn prepared_rectangle_refinement_declines_geographic_singularities(#[case] bounds: Bounds) {
    let source = Shape::Polygon(polygon(&[
        [-10.0, 80.0],
        [10.0, 80.0],
        [10.0, 90.0],
        [-10.0, 90.0],
        [-10.0, 80.0],
    ]));
    let topology = prepared_q_topology(&source, GeographicFrame::crs84());

    assert_eq!(topology.classify_rectangle(bounds).unwrap(), None);
}
#[test]
fn geographic_exact_query_handle_reuses_the_prepared_q() {
    let source = Shape::Polygon(rectangle(0.0, 0.0, 4.0, 4.0));
    let plan = PreparedPlan::transient(&source);
    let cache = crate::geometry::FrameDependentCaches::default();
    let frame = crate::geographic::GeographicFrame::crs84();
    let prepared = GeographicPrepared::try_new(&plan, &cache, frame).expect("fixture admits");
    let first = prepared.exact_topology().unwrap();
    let inside_point = Point::new_raw_xy(2.0, 2.0);
    let inside_location = first.locate_point(inside_point).unwrap();
    assert_eq!(inside_location.geometry, Loc::Interior);
    let point_relation = graph_relate_shapes_geographic(&source, &Shape::Point(inside_point));
    assert!(point_relation.is_covers());
    assert_eq!(
        inside_location.geometry != Loc::Exterior,
        point_relation.is_intersects()
    );
    let outside_point = Point::new_raw_xy(8.0, 8.0);
    assert_eq!(
        first.locate_point(outside_point).unwrap().geometry,
        Loc::Exterior
    );
    assert!(!graph_relate_shapes_geographic(&source, &Shape::Point(outside_point)).is_intersects());

    let bounds = Bounds::new_raw(1.0, 1.0, 3.0, 3.0);
    let rectangle_relation = graph_relate_shapes_geographic(
        &source,
        &Shape::Polygon(rectangle(
            bounds.minx(),
            bounds.miny(),
            bounds.maxx(),
            bounds.maxy(),
        )),
    );
    assert_eq!(
        first.classify_rectangle(bounds).unwrap(),
        Some(crate::geometry::ExactGeographicRectangleLocation::Interior)
    );
    assert!(rectangle_relation.is_covers());
    assert!(rectangle_relation.is_intersects());
    let second = prepared.exact_topology().unwrap();
    assert!(std::sync::Arc::ptr_eq(&first, &second));
    assert_eq!(
        second
            .locate_point(Point::new_raw_xy(8.0, 8.0))
            .unwrap()
            .geometry,
        Loc::Exterior
    );
    assert_eq!(plan.geographic_exact_topology_builds(), 1);
}
#[rstest::rstest]
#[case::square_interior(
    "square interior",
    Shape::Polygon(rectangle(0.0, 0.0, 4.0, 4.0)),
    Point::new_raw_xy(2.0, 2.0),
    Loc::Interior,
    Loc::Interior
)]
#[case::square_boundary(
    "square boundary",
    Shape::Polygon(rectangle(0.0, 0.0, 4.0, 4.0)),
    Point::new_raw_xy(0.0, 2.0),
    Loc::Boundary,
    Loc::Boundary
)]
#[case::square_exterior(
    "square exterior",
    Shape::Polygon(rectangle(0.0, 0.0, 4.0, 4.0)),
    Point::new_raw_xy(8.0, 8.0),
    Loc::Exterior,
    Loc::Exterior
)]
#[case::hole_interior(
    "hole interior",
    Shape::Polygon(donut()),
    Point::new_raw_xy(5.0, 5.0),
    Loc::Exterior,
    Loc::Exterior
)]
#[case::line_interior("line interior", line(&[[0.0, 0.0], [10.0, 0.0]]), Point::new_raw_xy(5.0, 0.0), Loc::Interior, Loc::Exterior)]
#[case::line_boundary("line boundary", line(&[[0.0, 0.0], [10.0, 0.0]]), Point::new_raw_xy(0.0, 0.0), Loc::Boundary, Loc::Exterior)]
#[case::seam_line_interior("seam line interior", line(&[[170.0, 0.0], [-170.0, 0.0]]), Point::new_raw_xy(175.0, 0.0), Loc::Interior, Loc::Exterior)]
#[case::isolated_point(
    "isolated point",
    Shape::Point(Point::new_raw_xy(5.0, 5.0)),
    Point::new_raw_xy(5.0, 5.0),
    Loc::Interior,
    Loc::Exterior
)]
#[case::seam_alias(
    "seam alias",
    Shape::Polygon(polygon(&[
        [170.0, -10.0],
        [-170.0, -10.0],
        [-170.0, 10.0],
        [170.0, 10.0],
        [170.0, -10.0],
    ])),
    Point::new_raw_xy(-180.0, 0.0),
    Loc::Interior,
    Loc::Interior
)]
#[case::pole_cap(
    "pole cap",
    Shape::Polygon(polygon(&[
        [-10.0, 80.0],
        [10.0, 80.0],
        [10.0, 90.0],
        [-10.0, 90.0],
        [-10.0, 80.0],
    ])),
    Point::new_raw_xy(37.0, 85.0),
    Loc::Exterior,
    Loc::Exterior
)]
#[case::pole_boundary(
    "pole boundary",
    Shape::Polygon(polygon(&[
        [-10.0, 80.0],
        [10.0, 80.0],
        [10.0, 90.0],
        [-10.0, 90.0],
        [-10.0, 80.0],
    ])),
    Point::new_raw_xy(37.0, 90.0),
    Loc::Boundary,
    Loc::Boundary
)]
#[case::mod_2_multiline_node(
    "mod-2 multiline node",
    Shape::geometry_collection(vec![
        line(&[[0.0, 0.0], [1.0, 0.0]]),
        line(&[[1.0, 0.0], [2.0, 0.0]]),
    ])
    .unwrap(),
    Point::new_raw_xy(1.0, 0.0),
    Loc::Interior,
    Loc::Exterior
)]
fn cached_q_point_locations_match_pair_relations_across_geographic_support(
    #[case] name: &str,
    #[case] source: Shape,
    #[case] point: Point,
    #[case] expected_geometry: Loc,
    #[case] expected_area: Loc,
) {
    let source = &source;
    let plan = PreparedPlan::transient(source);
    let cache = crate::geometry::FrameDependentCaches::default();
    let prepared =
        GeographicPrepared::try_new(&plan, &cache, crate::geographic::GeographicFrame::crs84())
            .unwrap();
    let actual = prepared
        .exact_topology()
        .unwrap()
        .locate_point(point)
        .unwrap();
    assert_eq!(
        (actual.geometry, actual.area),
        (expected_geometry, expected_area),
        "{name} exact Q location",
    );
    let relation = graph_relate_shapes_geographic(source, &Shape::Point(point));
    assert_eq!(
        actual.geometry != Loc::Exterior,
        relation.is_covers(),
        "{name} exact Q location"
    );
    assert_eq!(
        relation.is_covers(),
        relation.is_intersects(),
        "{name} point relation is symmetric in support"
    );
}
