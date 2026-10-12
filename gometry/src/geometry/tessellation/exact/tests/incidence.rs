#[test]
fn exact_segment_intersection_covers_extremes_endpoints_and_overlap() {
    let point = |x, y| ExactPoint::from_xy(XY::new(x, y));
    assert!(matches!(
        segment_intersection(
            &point(-f64::MAX, 0.0),
            &point(f64::MAX, 0.0),
            &point(0.0, -f64::from_bits(1)),
            &point(0.0, f64::from_bits(1)),
        ).unwrap(),
        SegmentIntersection::Point(hit) if hit.same_position(&point(0.0, 0.0)).unwrap()
    ));
    assert!(matches!(
        segment_intersection(&point(0.0, 0.0), &point(2.0, 0.0), &point(2.0, 0.0), &point(3.0, 1.0)).unwrap(),
        SegmentIntersection::Point(hit) if hit.same_position(&point(2.0, 0.0)).unwrap()
    ));
    assert!(matches!(
        segment_intersection(&point(0.0, 0.0), &point(3.0, 0.0), &point(1.0, 0.0), &point(2.0, 0.0)).unwrap(),
        SegmentIntersection::Overlap { start, end }
            if start.same_position(&point(1.0, 0.0)).unwrap() && end.same_position(&point(2.0, 0.0)).unwrap()
    ));
    for (b0, b1) in [((1.0, 0.0), (2.0, 0.0)), ((2.0, 0.0), (1.0, 0.0))] {
        assert!(matches!(
            segment_intersection(
                &point(0.0, 0.0),
                &point(3.0, 0.0),
                &point(b0.0, b0.1),
                &point(b1.0, b1.1),
            ).unwrap(),
            SegmentIntersection::Overlap { start, end }
                if start.same_position(&point(1.0, 0.0)).unwrap()
                    && end.same_position(&point(2.0, 0.0)).unwrap()
        ));
    }
}

#[test]
fn exact_cycle_and_point_classification_are_orientation_independent() {
    let ring = [
        ExactPoint::from_xy(XY::new(0.0, 0.0)),
        ExactPoint::from_xy(XY::new(2.0, 0.0)),
        ExactPoint::from_xy(XY::new(2.0, 2.0)),
        ExactPoint::from_xy(XY::new(0.0, 2.0)),
    ];
    assert_eq!(cycle_orientation(&ring).unwrap(), ExactSign::Positive);
    assert_eq!(
        point_in_cycle(&ring, &ExactPoint::from_xy(XY::new(1.0, 1.0))).unwrap(),
        PointInCycle::Inside
    );
    assert_eq!(
        point_in_cycle(&ring, &ExactPoint::from_xy(XY::new(2.0, 1.0))).unwrap(),
        PointInCycle::Boundary
    );
    assert_eq!(
        point_in_cycle(&ring, &ExactPoint::from_xy(XY::new(3.0, 1.0))).unwrap(),
        PointInCycle::Outside
    );
}

#[test]
fn exact_angle_orders_high_valence_star_without_scale() {
    let origin = ExactPoint::from_xy(XY::new(0.0, 0.0));
    let mut rays = [
        ExactPoint::from_xy(XY::new(0.0, -f64::MAX)),
        ExactPoint::from_xy(XY::new(-f64::from_bits(1), 0.0)),
        ExactPoint::from_xy(XY::new(0.0, f64::MAX)),
        ExactPoint::from_xy(XY::new(f64::from_bits(1), 0.0)),
    ];
    rays.sort_unstable_by(|left, right| angle_ccw_cmp(&origin, left, right).unwrap());
    assert_eq!(
        rays[0].round_nearest_even().unwrap(),
        XY::new(f64::from_bits(1), 0.0)
    );
    assert_eq!(
        rays[1].round_nearest_even().unwrap(),
        XY::new(0.0, f64::MAX)
    );
}
