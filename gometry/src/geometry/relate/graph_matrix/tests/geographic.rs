#[test]
fn prepared_geographic_point_membership_reuses_seam_topology() {
    let shape = Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(175.0, 60.0),
                Point::new_raw_xy(-175.0, 62.0),
            ])
            .unwrap(),
        )
        .unwrap(),
    );
    let plan = PreparedPlan::new(shape);
    let cache = crate::geometry::FrameDependentCaches::default();
    let prepared = GeographicPrepared::try_new(&plan, &cache, GeographicFrame::crs84()).unwrap();
    assert!(
        !prepared
            .point_covers(Point::new_raw_xy(180.0, 30.0))
            .unwrap()
    );
    assert_eq!(plan.geographic_exact_topology_builds(), 0);
    let queries = prepared.prepare_point_covers(7).unwrap();
    for longitude in [180.0, -180.0, 540.0] {
        assert!(queries.covers(Point::new_raw_xy(longitude, 61.0)).unwrap());
        assert!(!queries.covers(Point::new_raw_xy(longitude, 61.25)).unwrap());
        assert!(
            prepared
                .point_covers(Point::new_raw_xy(longitude, 61.0))
                .unwrap()
        );
        assert!(
            !prepared
                .point_covers(Point::new_raw_xy(longitude, 61.25))
                .unwrap()
        );
    }
    assert_eq!(plan.geographic_exact_topology_builds(), 1);
}
#[test]
fn prepared_geographic_point_membership_uses_the_open_chart() {
    let plan = PreparedPlan::new(Shape::Polygon(donut()));
    let cache = crate::geometry::FrameDependentCaches::default();
    let prepared = GeographicPrepared::try_new(&plan, &cache, GeographicFrame::crs84()).unwrap();
    let queries = prepared.prepare_point_covers(7).unwrap();
    for (point, covered) in [
        (Point::new_raw_xy(1.0, 1.0), true),
        (Point::new_raw_xy(361.0, 1.0), true),
        (Point::new_raw_xy(5.0, 5.0), false),
        (Point::new_raw_xy(3.0, 5.0), true),
        (Point::new_raw_xy(20.0, 20.0), false),
    ] {
        assert_eq!(prepared.point_covers(point).unwrap(), covered);
        assert_eq!(queries.covers(point).unwrap(), covered);
    }
    assert_eq!(plan.geographic_exact_topology_builds(), 0);
}

#[test]
fn retained_geographic_queries_match_closed_point_and_line_support() {
    for (wkt, point, covered) in [
        ("POINT (1 2)", Point::new_raw_xy(361.0, 2.0), true),
        ("POINT (1 2)", Point::new_raw_xy(1.0, 2.5), false),
        ("LINESTRING (0 0,2 2)", Point::new_raw_xy(1.0, 1.0), true),
        ("LINESTRING (0 0,2 2)", Point::new_raw_xy(1.0, 1.5), false),
        ("POLYGON ((0 0,2 2,4 4,0 0))", Point::new_raw_xy(1.0, 1.0), true),
        ("POLYGON ((0 0,2 2,4 4,0 0))", Point::new_raw_xy(1.0, 1.5), false),
        ("LINESTRING (10 80,10 90)", Point::new_raw_xy(-100.0, 90.0), true),
        ("LINESTRING (10 80,10 90)", Point::new_raw_xy(-100.0, 89.0), false),
    ] {
        let plan = PreparedPlan::new(crate::io::parse_wkt(wkt).unwrap());
        let cache = crate::geometry::FrameDependentCaches::default();
        let prepared = GeographicPrepared::try_new(&plan, &cache, GeographicFrame::crs84()).unwrap();
        let queries = prepared.prepare_point_covers(7).unwrap();
        assert_eq!(queries.covers(point).unwrap(), covered, "{wkt}");
        assert_eq!(
            prepared.exact_topology().unwrap().locate_point(point).unwrap().geometry != Loc::Exterior,
            covered,
            "{wkt}: quotient authority"
        );
    }
}
#[test]
fn geographic_near_vertical_box_keeps_planar_boundary_dimension() {
    let hexagon = Shape::Polygon(polygon(&[
        [1.0, 0.0],
        [0.500_000_000_000_000_1, 0.866_025_403_784_438_6],
        [-0.499_999_999_999_999_8, 0.866_025_403_784_438_7],
        [-1.0, 0.000_000_000_000_000_122_464_679_914_735_32],
        [-0.500_000_000_000_000_4, -0.866_025_403_784_438_4],
        [0.500_000_000_000_000_1, -0.866_025_403_784_438_6],
        [1.0, 0.0],
    ]));
    for (maxx, expected) in [
        (0.49, b"212101212"),
        (0.5, b"212101212"),
        (0.500_000_000_000_000_1, b"212101212"),
        (0.51, b"212101212"),
    ] {
        let rectangle = crate::geometry::geographic_bounds_to_shape(
            Bounds::new(-10.0, -10.0, maxx, 10.0).expect("ordered bounds"),
            crate::crs::info("EPSG:4326")
                .expect("known geographic CRS")
                .geographic_frame()
                .expect("geographic CRS"),
        )
        .expect("geographic rectangle");
        let geographic = graph_relate_shapes_geographic(&hexagon, &rectangle);
        assert_eq!(geographic.0, *expected, "maxx={maxx}");
        assert_eq!(
            graph_relate_shapes_geographic(&rectangle, &hexagon).0,
            geographic.transpose().0,
            "maxx={maxx}: transpose"
        );
        assert_eq!(
            graph_relate_shapes(&hexagon, &rectangle).unwrap().0,
            *expected,
            "maxx={maxx}: planar"
        );
    }
}
#[test]
fn ordinary_geographic_relation_matches_known_matrices() {
    let frame = crate::crs::info("EPSG:4326")
        .unwrap()
        .geographic_frame()
        .unwrap();
    let with_hole = Shape::Polygon(donut());
    let multipolygon = Shape::MultiPolygon(
        vec![
            rectangle(-10.0, -10.0, -6.0, -6.0),
            rectangle(-4.0, -10.0, 0.0, -6.0),
        ]
        .into(),
    );
    for (name, left, right, expected) in [
        (
            "disjoint",
            Shape::Polygon(rectangle(-20.0, 0.0, -18.0, 2.0)),
            Shape::Polygon(rectangle(-16.0, 0.0, -14.0, 2.0)),
            "FF2FF1212",
        ),
        (
            "nested",
            Shape::Polygon(rectangle(-20.0, 0.0, -10.0, 10.0)),
            Shape::Polygon(rectangle(-18.0, 2.0, -16.0, 4.0)),
            "212FF1FF2",
        ),
        (
            "overlapping",
            Shape::Polygon(rectangle(-20.0, 0.0, -16.0, 4.0)),
            Shape::Polygon(rectangle(-18.0, 2.0, -14.0, 6.0)),
            "212101212",
        ),
        (
            "shared edge",
            Shape::Polygon(rectangle(-20.0, 0.0, -18.0, 2.0)),
            Shape::Polygon(rectangle(-18.0, 0.0, -16.0, 2.0)),
            "FF2F11212",
        ),
        (
            "shared corner",
            Shape::Polygon(rectangle(-20.0, 0.0, -18.0, 2.0)),
            Shape::Polygon(rectangle(-18.0, 2.0, -16.0, 4.0)),
            "FF2F01212",
        ),
        (
            "hole",
            with_hole,
            Shape::Polygon(rectangle(2.0, 2.0, 5.0, 5.0)),
            "212101212",
        ),
        (
            "multipolygon",
            multipolygon,
            Shape::Polygon(rectangle(-8.0, -8.0, -2.0, -4.0)),
            "212101212",
        ),
    ] {
        let left_plan = PreparedPlan::transient(&left);
        let right_plan = PreparedPlan::transient(&right);
        let left_cache = crate::geometry::FrameDependentCaches::default();
        let right_cache = crate::geometry::FrameDependentCaches::default();
        let left_prepared =
            GeographicPrepared::try_new(&left_plan, &left_cache, frame).expect("fixture admits");
        let right_prepared =
            GeographicPrepared::try_new(&right_plan, &right_cache, frame).expect("fixture admits");
        assert_eq!(
            left_prepared.chart_route(),
            GeographicTopologyRoute::ExactPartition,
            "{name}: left chart route"
        );
        assert_eq!(
            right_prepared.chart_route(),
            GeographicTopologyRoute::ExactPartition,
            "{name}: right chart route"
        );
        let q = graph_relate_data_geographic_q(&left_prepared, &right_prepared).unwrap();
        let routed = graph_relate_data_geographic(left_prepared, right_prepared).unwrap();
        assert_eq!(q.text(), expected, "{name}: exact Q matrix");
        assert_eq!(routed.text(), expected, "{name}: routed matrix");
        assert_eq!(
            graph_relate_shapes_geographic(&left, &right).text(),
            expected,
            "{name}: shape dispatch",
        );
    }
}
#[test]
fn ordinary_country_charts_match_q_relation() {
    const COUNTRIES: &str = include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/fixtures/osm_countries_0_1.geojson"
    ));

    let frame = crate::crs::info("EPSG:4326")
        .unwrap()
        .geographic_frame()
        .unwrap();
    let countries: serde_json::Value = serde_json::from_str(COUNTRIES).unwrap();
    let features = countries["features"].as_array().unwrap();
    let mut compared = 0;

    for feature in features {
        let country = feature["properties"]["tags"]["ISO3166-1:alpha2"]
            .as_str()
            .unwrap();
        let shape = crate::io::parse_geojson(&feature["geometry"]).unwrap();
        let plan = PreparedPlan::transient(&shape);
        let cache = crate::geometry::FrameDependentCaches::default();
        let prepared =
            GeographicPrepared::try_new(&plan, &cache, frame).expect("country fixture admits");
        if prepared.chart_route() == GeographicTopologyRoute::Quotient {
            continue;
        }

        let mut probe = None;
        prepared.view().for_each_coordinate(|coordinate| {
            if probe.is_none() {
                let point = coordinate.native_xy();
                probe = Some(Shape::Point(Point::new_raw_xy(point.x, point.y)));
            }
        });
        let probe = probe.unwrap_or_else(|| panic!("{country}: ordinary country has no vertex"));
        let probe_plan = PreparedPlan::transient(&probe);
        let probe_cache = crate::geometry::FrameDependentCaches::default();
        let prepared_probe = GeographicPrepared::try_new(&probe_plan, &probe_cache, frame).unwrap();
        assert_eq!(
            prepared_probe.chart_route(),
            GeographicTopologyRoute::ExactPartition,
            "{country}: source vertex chart route"
        );

        let q = graph_relate_data_geographic_q(&prepared, &prepared_probe).unwrap();
        let routed = graph_relate_data_geographic(prepared, prepared_probe).unwrap();
        assert_eq!(routed, q, "{country}: ExactPartition route versus Q");
        compared += 1;
    }

    assert!(
        compared > 0,
        "country fixture has no ordinary-chart geometries"
    );
}
#[test]
fn geographic_pair_q_matches_known_matrices_at_quotient_aliases() {
    let frame = crate::crs::info("EPSG:4326")
        .unwrap()
        .geographic_frame()
        .unwrap();
    let ordinary = Shape::Polygon(rectangle(10.0, 0.0, 12.0, 2.0));
    let seam = Shape::Polygon(polygon(&[
        [170.0, 0.0],
        [-170.0, 0.0],
        [-170.0, 10.0],
        [170.0, 10.0],
        [170.0, 0.0],
    ]));
    let pole = Shape::Polygon(rectangle(10.0, 80.0, 12.0, 90.0));
    let noncanonical = Shape::Polygon(rectangle(350.0, 0.0, 360.0, 2.0));
    for (name, left, expected) in [
        ("seam", seam, "FF2FF1212"),
        ("pole", pole, "FF2FF1212"),
        ("longitude 360", noncanonical, "FF2FF1212"),
    ] {
        let left_plan = PreparedPlan::transient(&left);
        let right_plan = PreparedPlan::transient(&ordinary);
        let left_cache = crate::geometry::FrameDependentCaches::default();
        let right_cache = crate::geometry::FrameDependentCaches::default();
        let left_prepared =
            GeographicPrepared::try_new(&left_plan, &left_cache, frame).expect("fixture admits");
        let right_prepared =
            GeographicPrepared::try_new(&right_plan, &right_cache, frame).expect("fixture admits");
        assert_eq!(
            left_prepared.chart_route(),
            GeographicTopologyRoute::Quotient,
            "{name}: quotient route",
        );
        assert_eq!(
            right_prepared.chart_route(),
            GeographicTopologyRoute::ExactPartition,
            "{name}: ordinary route",
        );
        assert_eq!(
            graph_relate_data_geographic(left_prepared, right_prepared,)
                .unwrap()
                .text(),
            expected,
            "{name}: exact quotient relation",
        );
        assert_eq!(
            graph_relate_shapes_geographic(&ordinary, &left).text(),
            expected,
            "{name}: reverse relation",
        );
    }
}
#[test]
fn geographic_cap_point_matrices_are_stable() {
    let polygon = Shape::Polygon(Polygon::new(
        Ring::from_closed(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(170.0, 80.0),
                Point::new_raw_xy(-170.0, 80.0),
                Point::new_raw_xy(-170.0, 90.0),
                Point::new_raw_xy(170.0, 90.0),
                Point::new_raw_xy(170.0, 80.0),
            ])
            .unwrap(),
        )
        .unwrap(),
        Vec::new(),
    ));
    for point in [
        Point::new_raw_xy(180.0, 85.0),
        Point::new_raw_xy(-180.0, 85.0),
        Point::new_raw_xy(179.999, 85.0),
        Point::new_raw_xy(-179.999, 85.0),
    ] {
        assert_eq!(
            graph_relate_shapes_geographic(&polygon, &Shape::Point(point)).text(),
            "0F2FF1FF2"
        );
    }
    for point in [
        Point::new_raw_xy(0.0, 90.0),
        Point::new_raw_xy(90.0, 90.0),
        Point::new_raw_xy(-90.0, 90.0),
    ] {
        assert_eq!(
            graph_relate_shapes_geographic(&polygon, &Shape::Point(point)).text(),
            "FF20F1FF2"
        );
    }
}
#[test]
fn geographic_pole_line_intersects_cap_in_both_orders() {
    let line = Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(160.0, 90.0),
                Point::new_raw_xy(180.0, 90.0),
            ])
            .unwrap(),
        )
        .expect("line coordinate sequence must contain at least two vertices"),
    );
    let cap = Shape::Polygon(polygon(&[
        [-10.0, 80.0],
        [10.0, 80.0],
        [10.0, 90.0],
        [-10.0, 90.0],
        [-10.0, 80.0],
    ]));
    assert!(graph_relate_shapes_geographic(&line, &cap).is_intersects());
    assert!(graph_relate_shapes_geographic(&cap, &line).is_intersects());
}
#[test]
fn geographic_pole_cap_vertex_is_boundary() {
    let cap = Shape::Polygon(polygon(&[
        [-10.0, 80.0],
        [10.0, 80.0],
        [10.0, 90.0],
        [-10.0, 90.0],
        [-10.0, 80.0],
    ]));
    let pole = Shape::Point(Point::new_raw_xy(0.0, 90.0));
    assert_eq!(
        graph_relate_shapes_geographic(&cap, &pole).text(),
        "FF20F1FF2"
    );
}
#[test]
fn geographic_split_crossing_keeps_boundary_point_relation() {
    let raw = Shape::Polygon(polygon(&[
        [170.0, 40.0],
        [-170.0, 40.0],
        [-170.0, 50.0],
        [170.0, 50.0],
        [170.0, 40.0],
    ]));
    let split = raw.split_antimeridian().expect("crossing polygon splits");
    let point = Shape::Point(Point::new_raw_xy(170.0, 40.0));
    let raw_point_relation = graph_relate_shapes_geographic(&raw, &point).text();
    let split_point_relation = graph_relate_shapes_geographic(&split, &point).text();
    assert_eq!(raw_point_relation, "FF20F1FF2");
    assert_eq!(split_point_relation, raw_point_relation);
    let probe = Shape::Polygon(rectangle(172.0, 42.0, 178.0, 48.0));
    let raw_probe_relation = graph_relate_shapes_geographic(&raw, &probe).text();
    let split_probe_relation = graph_relate_shapes_geographic(&split, &probe).text();
    assert_eq!(raw_probe_relation, "212FF1FF2");
    assert_eq!(split_probe_relation, raw_probe_relation);
}
#[test]
fn geographic_aggregate_contacts_use_quotient_face_locations() {
    let left = Shape::Polygon(rectangle(0.0, 0.0, 1.0, 1.0));
    let right = Shape::Polygon(rectangle(1.0, 0.0, 2.0, 1.0));
    let adjacent =
        Shape::geometry_collection(vec![left.clone(), right.clone()]).expect("valid collection");
    let duplicates = Shape::geometry_collection(vec![left.clone(), left.clone(), left, right])
        .expect("test collection is valid");
    let pinch = Shape::geometry_collection(vec![
        Shape::Polygon(Polygon::new(
            Ring::from_closed(
                CoordSeq::try_from_points(&[
                    Point::new_raw_xy(0.0, 0.0),
                    Point::new_raw_xy(-2.0, -1.0),
                    Point::new_raw_xy(-2.0, 1.0),
                    Point::new_raw_xy(0.0, 0.0),
                ])
                .unwrap(),
            )
            .unwrap(),
            Vec::new(),
        )),
        Shape::Polygon(Polygon::new(
            Ring::from_closed(
                CoordSeq::try_from_points(&[
                    Point::new_raw_xy(0.0, 0.0),
                    Point::new_raw_xy(2.0, 1.0),
                    Point::new_raw_xy(2.0, -1.0),
                    Point::new_raw_xy(0.0, 0.0),
                ])
                .unwrap(),
            )
            .unwrap(),
            Vec::new(),
        )),
    ])
    .expect("test collection is valid");
    let frame = crate::crs::info("EPSG:4326")
        .unwrap()
        .geographic_frame()
        .unwrap();
    for (geometry, point, expected) in [
        (&adjacent, Point::new_raw_xy(1.0, 0.5), "0F2FF1FF2"),
        (&adjacent, Point::new_raw_xy(1.0, 0.0), "FF20F1FF2"),
        (&duplicates, Point::new_raw_xy(1.0, 0.5), "0F2FF1FF2"),
        (&pinch, Point::new_raw_xy(0.0, 0.0), "FF20F1FF2"),
    ] {
        let point = Shape::Point(point);
        assert_eq!(
            graph_relate_shapes_geographic(geometry, &point).text(),
            expected
        );
        let left = PreparedPlan::new(geometry.clone());
        let right = PreparedPlan::new(point);
        let left_cache = crate::geometry::FrameDependentCaches::default();
        let right_cache = crate::geometry::FrameDependentCaches::default();
        assert_eq!(
            graph_relate_data_geographic(
                GeographicPrepared::try_new(&left, &left_cache, frame).expect("fixture admits"),
                GeographicPrepared::try_new(&right, &right_cache, frame).expect("fixture admits"),
            )
            .unwrap()
            .text(),
            expected
        );
    }
}
#[test]
fn geographic_grad_pole_aliases_match_after_native_latitude_admission() {
    let frame = crate::crs::info("EPSG:4807")
        .unwrap()
        .geographic_frame()
        .unwrap();
    let pole = 100.0;
    assert_eq!(frame.pole(), pole);
    for (latitude, exterior, rejected) in [
        (pole, pole.next_up(), pole.next_up().next_up()),
        (-pole, (-pole).next_down(), (-pole).next_down().next_down()),
    ] {
        let left = PreparedPlan::new(Shape::Point(Point::new_raw_xy(0.0, exterior)));
        let right = PreparedPlan::new(Shape::Point(Point::new_raw_xy(200.0, latitude)));
        let left_cache = crate::geometry::FrameDependentCaches::default();
        let right_cache = crate::geometry::FrameDependentCaches::default();
        assert!(
            graph_relate_data_geographic(
                GeographicPrepared::try_new(&left, &left_cache, frame)
                    .expect("one exterior native ULP admits"),
                GeographicPrepared::try_new(&right, &right_cache, frame)
                    .expect("native pole admits"),
            )
            .unwrap()
            .is_intersects()
        );
        let Err(crate::geographic::GeographicError::LatitudeOutOfRange { .. }) =
            crate::geographic::Latitude::try_new(frame, rejected)
        else {
            panic!("the second exterior native ULP must be rejected");
        };
    }
}
#[test]
fn mixed_geographic_relation_keeps_known_area_and_curve_cells() {
    let left = Shape::geometry_collection(vec![
        Shape::Polygon(rectangle(0.0, 0.0, 2.0, 2.0)),
        Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new_raw_xy(-1.0, 1.0),
                    Point::new_raw_xy(1.0, 1.0),
                    Point::new_raw_xy(3.0, 1.0),
                ])
                .unwrap(),
            )
            .expect("line coordinate sequence must contain at least two vertices"),
        ),
        Shape::Point(Point::new_raw_xy(1.0, 1.0)),
    ])
    .expect("test collection is valid");
    let right = Shape::geometry_collection(vec![
        Shape::Polygon(rectangle(1.0, -1.0, 3.0, 1.0)),
        Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new_raw_xy(1.0, 1.0),
                    Point::new_raw_xy(1.0, 3.0),
                ])
                .unwrap(),
            )
            .expect("line coordinate sequence must contain at least two vertices"),
        ),
    ])
    .expect("test collection is valid");
    let geographic_left = Shape::geometry_collection(vec![
        Shape::Polygon(polygon(&[
            [170.0, 0.0],
            [-170.0, 0.0],
            [-170.0, 10.0],
            [170.0, 10.0],
            [170.0, 0.0],
        ])),
        Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new_raw_xy(175.0, 5.0),
                    Point::new_raw_xy(180.0, 5.0),
                    Point::new_raw_xy(-175.0, 5.0),
                ])
                .unwrap(),
            )
            .expect("line coordinate sequence must contain at least two vertices"),
        ),
        Shape::Point(Point::new_raw_xy(180.0, 5.0)),
    ])
    .expect("test collection is valid");
    let geographic_right = Shape::geometry_collection(vec![
        Shape::Polygon(rectangle(175.0, -5.0, 179.0, 5.0)),
        Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new_raw_xy(180.0, 5.0),
                    Point::new_raw_xy(180.0, 15.0),
                ])
                .unwrap(),
            )
            .expect("line coordinate sequence must contain at least two vertices"),
        ),
    ])
    .expect("test collection is valid");
    let frame = crate::crs::info("EPSG:4326")
        .expect("EPSG:4326 must be available")
        .geographic_frame()
        .expect("EPSG:4326 must have a geographic frame");
    for (left, right, expected) in [
        (&left, &right, "212101212"),
        (&geographic_left, &geographic_right, "212101212"),
    ] {
        let direct = graph_relate_shapes_geographic(left, right);
        assert_eq!(direct.text(), expected);
        assert_eq!(
            graph_relate_shapes_geographic(right, left).text(),
            direct.transpose().text(),
        );

        let left_plan = PreparedPlan::transient(left);
        let right_plan = PreparedPlan::transient(right);
        let left_cache = crate::geometry::FrameDependentCaches::default();
        let right_cache = crate::geometry::FrameDependentCaches::default();
        let left_prepared =
            GeographicPrepared::try_new(&left_plan, &left_cache, frame).expect("fixture admits");
        let right_prepared =
            GeographicPrepared::try_new(&right_plan, &right_cache, frame).expect("fixture admits");
        assert_eq!(
            graph_relate_data_geographic_q(&left_prepared, &right_prepared)
                .unwrap()
                .text(),
            expected,
        );
        assert_eq!(
            graph_relate_data_geographic(left_prepared, right_prepared)
                .unwrap()
                .text(),
            expected,
        );
    }
}
#[test]
fn geographic_point_on_line_interior_is_an_exact_curve_contact() {
    for (name, coordinates, x) in [
        ("ordinary", [[0.0, 0.0], [10.0, 0.0]], 5.0),
        ("seam-crossing", [[170.0, 0.0], [-170.0, 0.0]], 175.0),
    ] {
        let line = line(&coordinates);
        let point = Shape::Point(Point::new_raw_xy(x, 0.0));
        assert_eq!(
            graph_relate_shapes_geographic(&line, &point).text(),
            "0F1FF0FF2",
            "{name} line interior contact"
        );
    }
}
#[test]
fn geographic_shared_multiline_endpoint_is_not_a_boundary() {
    let lines = Shape::geometry_collection(vec![
        line(&[[0.0, 0.0], [1.0, 0.0]]),
        line(&[[1.0, 0.0], [2.0, 0.0]]),
    ])
    .unwrap();
    let junction = Shape::Point(Point::new_raw_xy(1.0, 0.0));
    assert_eq!(
        graph_relate_shapes_geographic(&lines, &junction).text(),
        "0F1FF0FF2"
    );
}
