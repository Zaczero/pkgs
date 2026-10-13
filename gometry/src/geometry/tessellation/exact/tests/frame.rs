#[test]
fn binary64_frame_strictly_encloses_ordinary_points_and_can_decline() {
    let points = [
        ExactPoint::from_xy(XY::new(-2.0, -1.0)),
        ExactPoint::from_xy(XY::new(3.0, 4.0)),
        ExactPoint::from_xy(XY::new(1.0, 2.0)),
    ];
    let frame = enclosing_frame_binary64(points.iter())
        .unwrap()
        .expect("ordinary frame");
    for point in &points {
        assert!(point.compare_x(&frame[0]).unwrap().is_gt());
        assert!(point.compare_y(&frame[0]).unwrap().is_gt());
        assert!(point.compare_x(&frame[2]).unwrap().is_lt());
        assert!(point.compare_y(&frame[2]).unwrap().is_lt());
    }

    let extremes = [
        ExactPoint::from_xy(XY::new(-f64::MAX, 0.0)),
        ExactPoint::from_xy(XY::new(f64::MAX, 1.0)),
    ];
    assert!(enclosing_frame_binary64(extremes.iter()).unwrap().is_none());
    enclosing_frame(&extremes).expect("exact fallback frame");
}
