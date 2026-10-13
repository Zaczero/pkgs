#[test]
fn reciprocal_ribbon_keeps_the_physical_nearest_edge() {
    let extent = 1.0e162;
    let half_height = 1.0 / extent;
    let shape = Shape::Polygon(Polygon::new(
        Ring::from_closed(
            CoordSeq::try_from_points(&[
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
    let center = polylabel_point(&shape, None).expect("finite ribbon");
    assert_eq!(center, Point::new_raw_xy(0.0, 0.0));
}

#[test]
fn dense_reciprocal_polygon_retains_a_physical_center() {
    let shape = dense_reciprocal_polygon(256);
    assert_eq!(
        shape.polylabel(None).unwrap(),
        Shape::Point(Point::new_raw_xy(0.0, 0.0)),
    );
}

#[test]
fn polylabel_refines_the_frontier_to_the_right_triangle_inradius() {
    let source = Shape::Polygon(Polygon::new(
        Ring::from_closed(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(10.0, 0.0),
                Point::new_raw_xy(0.0, 10.0),
                Point::new_raw_xy(0.0, 0.0),
            ])
            .unwrap(),
        )
        .unwrap(),
        Vec::new(),
    ));
    let tolerance = 0.01;
    let center = polylabel_point(&source, Some(tolerance)).expect("finite triangle pole");
    let expected_radius = 10.0 - 5.0 * 2.0_f64.sqrt();
    let clearance = center
        .x
        .min(center.y)
        .min((10.0 - center.x - center.y) / 2.0_f64.sqrt());

    assert!(center.x > 0.0 && center.y > 0.0 && center.x + center.y < 10.0);
    assert!(source.covers_point(center));
    assert!(clearance >= expected_radius - tolerance);
    assert!(clearance <= expected_radius + 8.0 * f64::EPSILON);
}
