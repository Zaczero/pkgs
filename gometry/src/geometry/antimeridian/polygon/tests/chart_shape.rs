#[test]
fn degree_try_new_frame_equals_crs84() {
    let degrees = AngularFactor::try_new("angular unit", std::f64::consts::PI / 180.0).unwrap();
    let frame = GeographicFrame::try_new(degrees, degrees, 0.0).unwrap();
    assert_eq!(frame, GeographicFrame::crs84());
}

#[test]
fn antimeridian_pole_closure_maps_physical_pole_to_native_axis() {
    let unit = AngularFactor::try_new("degree", std::f64::consts::PI / 180.0).unwrap();
    let frame = GeographicFrame::try_new_with_orientation(
        unit,
        unit,
        0.0,
        GeographicAxisOrientation::from_axis_directions(true, false),
    )
    .unwrap();
    let source = polygon(&[(170.0, 80.0), (-70.0, 80.0), (50.0, 80.0)], &[]);
    let Shape::Polygon(source_polygon) = &source else {
        panic!("the pole-cap fixture is a polygon");
    };
    let summary = crate::geographic::RingSummary::try_new(
        frame,
        &crate::geographic::GeographicRing::from_ring(&source_polygon.shell),
    )
    .unwrap();
    assert_eq!(summary.bounded_pole(), Some(ChartPole::North));

    let split = source.split_antimeridian_in_frame(frame).unwrap();
    let physical_south = frame.native_latitude_pole(GeodeticPole::South);
    let physical_north = frame.native_latitude_pole(GeodeticPole::North);
    let mut saw_south_cap = false;
    let mut saw_north_cap = false;
    split.for_each_point(|point| {
        saw_south_cap |= point.y.to_bits() == physical_south.to_bits();
        saw_north_cap |= point.y.to_bits() == physical_north.to_bits();
    });
    assert!(saw_south_cap, "{split:?}");
    assert!(!saw_north_cap, "{split:?}");
}

#[test]
fn crs84_canonical_noncrossing_line_matches_source_and_skips_chart_alloc() {
    let shape = line(&[10.0, 20.0], &[1.0, 2.0]);
    let (cloned, clone_n) = allocation_count(|| shape.clone());
    let (split, split_n) = allocation_count(|| split_crs84(&shape));
    assert_eq!(split, cloned);
    assert_eq!(split_n, clone_n);
    let crossing = line(&[170.0, -170.0], &[0.0, 0.0]);
    let (_, crossing_n) = allocation_count(|| split_crs84(&crossing));
    assert!(clone_n < crossing_n);
}

#[test]
fn canonical_native_frame_line_skips_temporary_chart_allocation() {
    const CRS: &str = r#"GEOGCS["Custom grad",DATUM["Custom datum",SPHEROID["Custom",7000000,150]],PRIMEM["Greenwich",0],UNIT["grad",0.015707963267948967],AXIS["Longitude",EAST],AXIS["Latitude",NORTH]]"#;
    let state = crate::crs::runtime_state_snapshot().unwrap();
    let frame = crate::crs::info_with_state(CRS, &state)
        .unwrap()
        .geographic_frame()
        .unwrap();
    let shape = line(&[-20.0, 20.0], &[1.0, 2.0]);

    let (cloned, clone_allocations) = allocation_count(|| shape.clone());
    let (split, split_allocations) =
        allocation_count(|| shape.split_antimeridian_in_frame(frame).unwrap());

    assert_eq!(split, cloned);
    assert_eq!(split_allocations, clone_allocations);
}

#[test]
fn crs84_half_period_endpoint_is_identity() {
    let shape = line(&[180.0, 0.0], &[0.0, 0.0]);
    assert_eq!(split_crs84(&shape), shape);
}

#[test]
fn crs84_full_period_pair_is_identity() {
    let shape = line(&[-180.0, 180.0], &[0.0, 0.0]);
    assert_eq!(split_crs84(&shape), shape);
}

#[test]
fn crs84_world_rectangle_is_identity() {
    let shape = Shape::Polygon(Polygon::new(
        closed_ring(&[-180.0, 180.0, 180.0, -180.0, -180.0], &[
            -90.0, -90.0, 90.0, 90.0, -90.0,
        ]),
        Vec::new(),
    ));
    assert_eq!(split_crs84(&shape), shape);
}

#[test]
fn crs84_canonical_polygon_with_holes_is_identity() {
    let shape = Shape::Polygon(Polygon::new(
        closed_ring(&[0.0, 10.0, 10.0, 0.0, 0.0], &[0.0, 0.0, 10.0, 10.0, 0.0]),
        vec![closed_ring(&[2.0, 4.0, 4.0, 2.0, 2.0], &[
            2.0, 2.0, 4.0, 4.0, 2.0,
        ])],
    ));
    assert_eq!(split_crs84(&shape), shape);
}

#[test]
fn multiline_split_preserves_logical_empty_and_repeated_members() {
    let crossing_line = LineSeq::try_new(
        CoordSeq::try_from_vecs(vec![170.0, -170.0], vec![10.0, 10.0], None, None)
            .expect("finite XY line coordinates"),
    )
    .expect("two-point line");
    let multiline = MultiLineString::try_new(CoordinateAxes::XY, vec![
        MultiMember::Empty,
        MultiMember::Value(crossing_line.clone()),
        MultiMember::Empty,
        MultiMember::Value(crossing_line),
        MultiMember::Empty,
    ])
    .expect("multipart lines have homogeneous XY axes");
    let split = Shape::MultiLineString(multiline)
        .split_antimeridian_in_frame(GeographicFrame::crs84())
        .expect("valid multipart line splits");
    let split_lines = match split {
        Shape::MultiLineString(lines) => Some(lines),
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::Polygon(_)
        | Shape::MultiPolygon(_)
        | Shape::GeometryCollection(_)
        | Shape::Empty(..) => None,
    }
    .expect("multipart line remains a multipart line");
    assert_eq!(split_lines.axes(), CoordinateAxes::XY);
    let mut line_members = split_lines.members_with_values();
    assert!(matches!(line_members.next(), Some(MultiMember::Empty)));
    let first_part = line_members
        .next()
        .and_then(|member| match member {
            MultiMember::Empty => None,
            MultiMember::Value(line) => Some(line),
        })
        .expect("first crossing line has a first part");
    let second_part = line_members
        .next()
        .and_then(|member| match member {
            MultiMember::Empty => None,
            MultiMember::Value(line) => Some(line),
        })
        .expect("first crossing line has a second part");
    assert!(matches!(line_members.next(), Some(MultiMember::Empty)));
    let repeated_first_part = line_members
        .next()
        .and_then(|member| match member {
            MultiMember::Empty => None,
            MultiMember::Value(line) => Some(line),
        })
        .expect("repeated crossing line has a first part");
    let repeated_second_part = line_members
        .next()
        .and_then(|member| match member {
            MultiMember::Empty => None,
            MultiMember::Value(line) => Some(line),
        })
        .expect("repeated crossing line has a second part");
    assert!(matches!(line_members.next(), Some(MultiMember::Empty)));
    assert!(line_members.next().is_none());
    assert_eq!(first_part, repeated_first_part);
    assert_eq!(second_part, repeated_second_part);
}

#[test]
fn multipolygon_chart_split_preserves_logical_empty_and_repeated_members() {
    let crossing_polygon = Polygon::new(
        closed_ring(&[170.0, -170.0, -170.0, 170.0, 170.0], &[
            40.0, 40.0, 50.0, 50.0, 40.0,
        ]),
        Vec::new(),
    );
    let multipolygon = MultiPolygon::try_new(CoordinateAxes::XY, vec![
        MultiMember::Empty,
        MultiMember::Value(crossing_polygon.clone()),
        MultiMember::Empty,
        MultiMember::Value(crossing_polygon),
        MultiMember::Empty,
    ])
    .expect("multipart polygons have homogeneous XY axes");
    let split = Shape::MultiPolygon(multipolygon)
        .split_antimeridian_in_frame(GeographicFrame::crs84())
        .expect("valid multipart polygon splits");
    let split_polygons = match split {
        Shape::MultiPolygon(polygons) => Some(polygons),
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Polygon(_)
        | Shape::GeometryCollection(_)
        | Shape::Empty(..) => None,
    }
    .expect("multipart polygon remains a multipart polygon");
    assert_eq!(split_polygons.axes(), CoordinateAxes::XY);
    let mut polygon_members = split_polygons.members_with_values();
    assert!(matches!(polygon_members.next(), Some(MultiMember::Empty)));
    let first_part = polygon_members
        .next()
        .and_then(|member| match member {
            MultiMember::Empty => None,
            MultiMember::Value(polygon) => Some(polygon),
        })
        .expect("first crossing polygon has a first part");
    let second_part = polygon_members
        .next()
        .and_then(|member| match member {
            MultiMember::Empty => None,
            MultiMember::Value(polygon) => Some(polygon),
        })
        .expect("first crossing polygon has a second part");
    assert!(matches!(polygon_members.next(), Some(MultiMember::Empty)));
    let repeated_first_part = polygon_members
        .next()
        .and_then(|member| match member {
            MultiMember::Empty => None,
            MultiMember::Value(polygon) => Some(polygon),
        })
        .expect("repeated crossing polygon has a first part");
    let repeated_second_part = polygon_members
        .next()
        .and_then(|member| match member {
            MultiMember::Empty => None,
            MultiMember::Value(polygon) => Some(polygon),
        })
        .expect("repeated crossing polygon has a second part");
    assert!(matches!(polygon_members.next(), Some(MultiMember::Empty)));
    assert!(polygon_members.next().is_none());
    assert_eq!(first_part, repeated_first_part);
    assert_eq!(second_part, repeated_second_part);
}

#[test]
fn multipolygon_forced_pole_split_preserves_logical_empty_and_repeated_members() {
    let north_cap = Polygon::new(
        closed_ring(&[-45.0, 45.0, 135.0, -135.0, -45.0], &[
            40.0, 40.0, 40.0, 40.0, 40.0,
        ]),
        Vec::new(),
    );
    let multipolygon = MultiPolygon::try_new(CoordinateAxes::XY, vec![
        MultiMember::Empty,
        MultiMember::Value(north_cap.clone()),
        MultiMember::Empty,
        MultiMember::Value(north_cap),
        MultiMember::Empty,
    ])
    .expect("multipart polygons have homogeneous XY axes");
    let forced = Shape::MultiPolygon(multipolygon)
        .split_antimeridian_over_pole(true)
        .expect("valid multipart north-pole cap splits");
    let forced_caps = match forced {
        Shape::MultiPolygon(polygons) => Some(polygons),
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Polygon(_)
        | Shape::GeometryCollection(_)
        | Shape::Empty(..) => None,
    }
    .expect("forced pole split remains a multipart polygon");
    let mut groups = Vec::<Vec<Polygon>>::new();
    let mut current = Vec::new();
    let mut empty_rows = 0;
    for member in forced_caps.members_with_values() {
        match member {
            MultiMember::Empty => {
                groups.push(std::mem::take(&mut current));
                empty_rows += 1;
            },
            MultiMember::Value(polygon) => current.push(polygon.clone()),
        }
    }
    groups.push(current);
    assert_eq!(empty_rows, 3);
    assert_eq!(groups.len(), 4);
    let mut groups = groups.into_iter();
    assert!(groups.next().expect("leading empty row").is_empty());
    let first = groups.next().expect("first pole cap");
    assert!(!first.is_empty());
    let repeated = groups.next().expect("repeated pole cap");
    assert!(!repeated.is_empty());
    assert_eq!(repeated, first);
    assert!(groups.next().expect("trailing empty row").is_empty());
    assert!(groups.next().is_none());
}

#[test]
fn crs84_nested_xyzm_collection_is_identity() {
    let line = Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_vecs(
                vec![1.0, 2.0],
                vec![3.0, 4.0],
                Some(vec![10.0, 20.0]),
                Some(vec![7.0, 8.0]),
            )
            .unwrap(),
        )
        .unwrap(),
    );
    let point = Shape::Point(
        Point::new_axes(5.0, 6.0, ZOrdinate(Some(1.0)), MOrdinate(Some(2.0))).unwrap(),
    );
    let inner = Shape::geometry_collection(vec![point]).unwrap();
    let shape = Shape::geometry_collection(vec![line, inner]).unwrap();
    assert_eq!(split_crs84(&shape), shape);
}
