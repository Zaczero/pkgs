#[test]
fn zero_radius_boundary_point_uses_closed_membership() {
    let shape = degenerate_line_shape();
    let result = emit_zero_radius_point(&shape, Point::new_raw_xy(0.0, 0.0)).unwrap();
    let Shape::Point(point) = result else {
        panic!("degenerate maximum circle must emit its point center");
    };
    assert!(shape.covers_point(point));
    assert!(!shape.contains_point(point));
}

#[test]
fn zero_radius_outside_point_is_rejected_at_emission_boundary() {
    let shape = degenerate_line_shape();
    let result = emit_zero_radius_point(&shape, Point::new_raw_xy(1.0, 1.0));
    assert!(matches!(
        result,
        Err(error)
            if matches!(
                error.kind(),
                ErrorKind::Geometry(
                    crate::geometry::GeometryErrorKind::UnrepresentableOutput {
                        operation: "maximum_inscribed_circle",
                        ..
                    }
                )
            )
    ));
}

#[test]
fn source_tangent_does_not_enter_inscribed_circle() {
    let center = Point::new_raw_xy(1.0, 1.0);
    let points = circle_points(center, 1.0);
    let segment = Segment {
        start: XY::new(0.0, 0.0),
        end: XY::new(2.0, 0.0),
    };
    assert!(!segment_enters_candidate(segment, &points, true).unwrap());
    for segment in [
        Segment {
            start: XY::new(0.0, 0.0),
            end: XY::new(2.0, 0.0),
        },
        Segment {
            start: XY::new(2.0, 0.0),
            end: XY::new(2.0, 2.0),
        },
        Segment {
            start: XY::new(2.0, 2.0),
            end: XY::new(0.0, 2.0),
        },
        Segment {
            start: XY::new(0.0, 2.0),
            end: XY::new(0.0, 0.0),
        },
    ] {
        assert!(!segment_enters_candidate(segment, &points, true).unwrap());
    }
    let mut closed = points.clone();
    closed.push(points[0]);
    let seq = CoordSeq::try_from_points(&closed).unwrap();
    assert!(!crate::geometry::ring_winding(&seq).is_degenerate());
    assert!(crate::geometry::shell_is_convex(&seq));
    assert!(candidate_center_is_strictly_inside(&points, center, true));
    let source = Shape::Polygon(Polygon::new(
        Ring::from_closed(
            crate::geometry::CoordSeq::try_from_points(&[
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(2.0, 0.0),
                Point::new_raw_xy(2.0, 2.0),
                Point::new_raw_xy(0.0, 2.0),
                Point::new_raw_xy(0.0, 0.0),
            ])
            .unwrap(),
        )
        .unwrap(),
        Vec::new(),
    ));
    assert!(source.contains_point(center));
    assert!(!source_boundary_enters_candidate(&source, &points, true).unwrap());
}

#[test]
fn exact_boundary_radius_uses_the_smallest_stored_distance() {
    let shape = Shape::Polygon(Polygon::new(
        Ring::from_closed(
            crate::geometry::CoordSeq::try_from_points(&[
                Point::new_raw_xy(0.1, 0.1),
                Point::new_raw_xy(0.3, 0.1),
                Point::new_raw_xy(0.3, 0.3),
                Point::new_raw_xy(0.1, 0.3),
                Point::new_raw_xy(0.1, 0.1),
            ])
            .unwrap(),
        )
        .unwrap(),
        Vec::new(),
    ));
    assert_eq!(
        exact_boundary_radius(&shape, Point::new_raw_xy(0.2, 0.2))
            .unwrap()
            .to_bits(),
        0x3FB9_9999_9999_9998
    );
}

#[test]
fn exact_boundary_radius_handles_reciprocal_source_axes() {
    let extent = 1.0e150;
    let half_height = 1.0e-150;
    let shape = Shape::Polygon(Polygon::new(
        Ring::from_closed(
            crate::geometry::CoordSeq::try_from_points(&[
                Point::new_raw_xy(-extent, -half_height),
                Point::new_raw_xy(extent, -half_height),
                Point::new_raw_xy(extent, half_height),
                Point::new_raw_xy(-extent, half_height),
                Point::new_raw_xy(-extent, -half_height),
            ])
            .unwrap(),
        )
        .unwrap(),
        Vec::new(),
    ));
    assert_eq!(
        exact_boundary_radius(&shape, Point::new_raw_xy(0.0, 0.0))
            .unwrap()
            .to_bits(),
        half_height.to_bits()
    );
}

#[test]
fn source_hole_inside_candidate_is_rejected_without_boundary_contact() {
    let source = Shape::Polygon(Polygon::new(
        Ring::from_closed(
            crate::geometry::CoordSeq::try_from_points(&[
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(10.0, 0.0),
                Point::new_raw_xy(10.0, 10.0),
                Point::new_raw_xy(0.0, 10.0),
                Point::new_raw_xy(0.0, 0.0),
            ])
            .unwrap(),
        )
        .unwrap(),
        vec![
            Ring::from_closed(
                CoordSeq::try_from_points(&[
                    Point::new_raw_xy(4.5, 4.5),
                    Point::new_raw_xy(5.5, 4.5),
                    Point::new_raw_xy(5.5, 5.5),
                    Point::new_raw_xy(4.5, 5.5),
                    Point::new_raw_xy(4.5, 4.5),
                ])
                .unwrap(),
            )
            .unwrap(),
        ],
    ));
    let candidate = circle_points(Point::new_raw_xy(5.0, 3.0), 3.0);
    assert!(source.contains_point(Point::new_raw_xy(5.0, 3.0)));
    assert!(source_boundary_enters_candidate(&source, &candidate, true).unwrap());
}

#[test]
fn enclosed_hole_cycle_is_rejected_even_when_candidate_endpoints_are_covered() {
    let source = Shape::Polygon(Polygon::new(
        Ring::from_closed(
            crate::geometry::CoordSeq::try_from_points(&[
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(100.0, 0.0),
                Point::new_raw_xy(100.0, 100.0),
                Point::new_raw_xy(0.0, 100.0),
                Point::new_raw_xy(0.0, 0.0),
            ])
            .unwrap(),
        )
        .unwrap(),
        vec![
            Ring::from_closed(
                crate::geometry::CoordSeq::try_from_points(&[
                    Point::new_raw_xy(48.0, 48.0),
                    Point::new_raw_xy(52.0, 48.0),
                    Point::new_raw_xy(52.0, 52.0),
                    Point::new_raw_xy(48.0, 52.0),
                    Point::new_raw_xy(48.0, 48.0),
                ])
                .unwrap(),
            )
            .unwrap(),
        ],
    ));
    let candidate = [
        Point::new_raw_xy(55.0, 50.0),
        Point::new_raw_xy(40.0, 65.0),
        Point::new_raw_xy(25.0, 50.0),
        Point::new_raw_xy(40.0, 35.0),
    ];
    assert!(source.contains_point(Point::new_raw_xy(40.0, 50.0)));
    assert!(!source_vertex_outside_candidate(&source, &candidate));
    assert!(source_hole_is_enclosed(&source, &candidate, true));
}

#[test]
fn inward_chord_is_rejected_after_endpoint_only_checks_pass() {
    let source = Shape::Polygon(Polygon::new(
        Ring::from_closed(
            crate::geometry::CoordSeq::try_from_points(&[
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(10.0, 0.0),
                Point::new_raw_xy(10.0, 5.0),
                Point::new_raw_xy(5.0, 5.0),
                Point::new_raw_xy(5.0, 10.0),
                Point::new_raw_xy(0.0, 10.0),
                Point::new_raw_xy(0.0, 0.0),
            ])
            .unwrap(),
        )
        .unwrap(),
        Vec::new(),
    ));
    let candidate = [
        Point::new_raw_xy(6.5, 4.0),
        Point::new_raw_xy(4.0, 6.5),
        Point::new_raw_xy(1.5, 4.0),
        Point::new_raw_xy(4.0, 1.5),
    ];
    assert!(source.contains_point(Point::new_raw_xy(4.0, 4.0)));
    assert!(!source_vertex_outside_candidate(&source, &candidate));
    assert!(source_boundary_enters_candidate(&source, &candidate, true).unwrap());
}

#[test]
fn inscribed_circle_retries_after_a_rounded_radius_overrun() {
    let source = Shape::Polygon(Polygon::new(
        Ring::from_closed(
            crate::geometry::CoordSeq::try_from_points(&[
                Point::new_raw_xy(0.0, 1.0),
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(1.0, 0.0),
                Point::new_raw_xy(1.0, 1.0),
                Point::new_raw_xy(0.0, 1.0),
            ])
            .unwrap(),
        )
        .unwrap(),
        Vec::new(),
    ));
    let (prepared, boundary) = exact_inscribed_support(&source);
    let center = Point::new_raw_xy(0.5, 0.5);
    let clearance = exact_boundary_squared(&source, center).unwrap();
    let support = Some((&prepared, &boundary, &clearance));
    let quadrants = NonZeroU32::new(16).expect("non-zero");
    let steps = quadrants.get() as usize * 4;
    let radius = 0.5_f64.next_up();
    let first = build_inscribed_circle_once(center, radius, steps, steps + 1, quadrants, support);
    assert!(
        first.is_err(),
        "the rounded first radius must expose the boundary-rounding retry"
    );
    let emitted = build_inscribed_circle(center, radius, quadrants, support)
        .expect("the one-ULP retry must produce a contained ring");
    assert_eq!(emitted.geometry_type(), "Polygon");
}

#[test]
fn certified_inscribed_circle_reuses_the_directed_support_at_emission() {
    let source =
        crate::io::parse_wkt("MULTIPOLYGON (((0 0,2 0,2 4,0 4,0 0)),((2 0,4 0,4 4,2 4,2 0)))")
            .unwrap();
    overlay::reset_partition_calls();
    source.maximum_inscribed_circle(Some(0.1)).unwrap();
    assert_eq!(overlay::partition_calls(), 1);
}

#[test]
fn candidate_center_must_be_strictly_inside_every_edge() {
    let candidate = circle_points(Point::new_raw_xy(5.0, 3.0), 3.0);
    assert!(candidate_center_is_strictly_inside(
        &candidate,
        Point::new_raw_xy(5.0, 3.0),
        true,
    ));
    assert!(!candidate_center_is_strictly_inside(
        &candidate,
        Point::new_raw_xy(5.0, 6.0),
        true,
    ));
}
