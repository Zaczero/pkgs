#[test]
fn exact_segment_rejects_projective_point_at_infinity() {
    let horizontal = super::exact::ExactLine::through_points(
        &super::exact::ExactPoint::from_xy(XY::new(0.0, 0.0)),
        &super::exact::ExactPoint::from_xy(XY::new(1.0, 0.0)),
    )
    .expect("distinct segment");
    let parallel = super::exact::ExactLine::through_points(
        &super::exact::ExactPoint::from_xy(XY::new(0.0, 1.0)),
        &super::exact::ExactPoint::from_xy(XY::new(1.0, 1.0)),
    )
    .expect("distinct segment");
    let infinity = super::exact::incidence::line_intersection(&horizontal, &parallel).unwrap();
    let start = super::exact::ExactPoint::from_xy(XY::new(0.0, 0.0));
    let end = super::exact::ExactPoint::from_xy(XY::new(1.0, 0.0));

    assert!(!infinity.is_finite());
    assert!(matches!(
        super::exact::incidence::segment_intersection(&start, &end, &infinity, &infinity),
        Ok(super::exact::SegmentIntersection::None)
    ));
}
