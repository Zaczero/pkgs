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
#[test]
fn prepared_rectangle_refinement_keeps_an_exact_hole_uncovered() {
    let frame = GeographicFrame::crs84();
    let donut = Shape::Polygon(donut());
    let line_in_hole = Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_points(&[Point::new_raw_xy(4.0, 5.0), Point::new_raw_xy(6.0, 5.0)])
                .unwrap(),
        )
        .unwrap(),
    );
    let bounds = Bounds::new_raw(3.0, 3.0, 7.0, 7.0);
    for source in [
        donut.clone(),
        Shape::geometry_collection(vec![donut, line_in_hole]).unwrap(),
    ] {
        assert_eq!(
            prepared_q_topology(&source, frame)
                .classify_rectangle(bounds)
                .unwrap(),
            Some(ExactGeographicRectangleLocation::Boundary),
        );
    }
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
#[test]
fn prepared_rectangle_refinement_declines_geographic_singularities() {
    let source = Shape::Polygon(polygon(&[
        [-10.0, 80.0],
        [10.0, 80.0],
        [10.0, 90.0],
        [-10.0, 90.0],
        [-10.0, 80.0],
    ]));
    let topology = prepared_q_topology(&source, GeographicFrame::crs84());
    for bounds in [
        Bounds::new_raw(-180.0, 81.0, -179.0, 85.0),
        Bounds::new_raw(0.0, 85.0, 10.0, 90.0),
        Bounds::new_raw(-180.0, -1.0, 180.0, 1.0),
    ] {
        assert_eq!(topology.classify_rectangle(bounds).unwrap(), None);
    }
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
#[test]
fn cached_q_point_locations_match_pair_relations_across_geographic_support() {
    let square = Shape::Polygon(rectangle(0.0, 0.0, 4.0, 4.0));
    let donut = Shape::Polygon(donut());
    let line_source = line(&[[0.0, 0.0], [10.0, 0.0]]);
    let seam_line = line(&[[170.0, 0.0], [-170.0, 0.0]]);
    let isolated = Shape::Point(Point::new_raw_xy(5.0, 5.0));
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
    let connected_lines = Shape::geometry_collection(vec![
        line(&[[0.0, 0.0], [1.0, 0.0]]),
        line(&[[1.0, 0.0], [2.0, 0.0]]),
    ])
    .unwrap();
    let cases = [
        (
            "square interior",
            &square,
            Point::new_raw_xy(2.0, 2.0),
            Loc::Interior,
            Loc::Interior,
        ),
        (
            "square boundary",
            &square,
            Point::new_raw_xy(0.0, 2.0),
            Loc::Boundary,
            Loc::Boundary,
        ),
        (
            "square exterior",
            &square,
            Point::new_raw_xy(8.0, 8.0),
            Loc::Exterior,
            Loc::Exterior,
        ),
        (
            "hole interior",
            &donut,
            Point::new_raw_xy(5.0, 5.0),
            Loc::Exterior,
            Loc::Exterior,
        ),
        (
            "line interior",
            &line_source,
            Point::new_raw_xy(5.0, 0.0),
            Loc::Interior,
            Loc::Exterior,
        ),
        (
            "line boundary",
            &line_source,
            Point::new_raw_xy(0.0, 0.0),
            Loc::Boundary,
            Loc::Exterior,
        ),
        (
            "seam line interior",
            &seam_line,
            Point::new_raw_xy(175.0, 0.0),
            Loc::Interior,
            Loc::Exterior,
        ),
        (
            "isolated point",
            &isolated,
            Point::new_raw_xy(5.0, 5.0),
            Loc::Interior,
            Loc::Exterior,
        ),
        (
            "seam alias",
            &seam,
            Point::new_raw_xy(-180.0, 0.0),
            Loc::Interior,
            Loc::Interior,
        ),
        (
            "pole cap",
            &polar,
            Point::new_raw_xy(37.0, 85.0),
            Loc::Exterior,
            Loc::Exterior,
        ),
        (
            "pole boundary",
            &polar,
            Point::new_raw_xy(37.0, 90.0),
            Loc::Boundary,
            Loc::Boundary,
        ),
        (
            "mod-2 multiline node",
            &connected_lines,
            Point::new_raw_xy(1.0, 0.0),
            Loc::Interior,
            Loc::Exterior,
        ),
    ];
    for (name, source, point, expected_geometry, expected_area) in cases {
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
}
