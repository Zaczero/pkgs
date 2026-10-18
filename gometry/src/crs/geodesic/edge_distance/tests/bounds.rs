use super::*;

#[test]
fn hessian_bound_requires_the_whole_distance_interval_inside_its_normal_ball() {
    let sphere = Geodesic::new(1.0, 0.0);
    let hessian = curvature_hessian_bound(&sphere, 0.2, 1.4);
    assert!(hessian.is_finite());
    assert!(hessian >= 5.0);
    assert!(curvature_hessian_bound(&sphere, 0.2, 1.6).is_infinite());
    assert!(curvature_hessian_bound(&sphere, 0.0, 0.2).is_infinite());
}
#[test]
fn spherical_antipodal_interval_lower_never_exceeds_its_attained_endpoint() {
    let geodesic = Geodesic::new(1.0, 0.0);
    let frame = GeographicFrame::crs84();
    let latitude = 0.2_f64.to_degrees();
    let edge = ChartEdge::new(
        Point::new_raw_xy(0.0, -latitude),
        Point::new_raw_xy(0.0, latitude),
        frame,
    )
    .unwrap();
    let query = Point::new_raw_xy(180.0, 0.0);
    let (distance0, azimuth0) = sample(&geodesic, query, &edge, 0.0).unwrap();
    let (distance1, azimuth1) = sample(&geodesic, query, &edge, 1.0).unwrap();
    let derivative0 = edge.derivatives(0.0, &geodesic);
    let derivative1 = edge.derivatives(1.0, &geodesic);
    let lower = interval_lower_bound(
        IntervalEndpoints {
            t0: 0.0,
            d0: distance0,
            g0: distance_derivative(derivative0, distance0, azimuth0),
            t1: 1.0,
            d1: distance1,
            g1: distance_derivative(derivative1, distance1, azimuth1),
        },
        edge.differential_bounds(&geodesic),
        &geodesic,
        GeodesicNumericalPolicy::new(geodesic.a),
    );
    let radians_per_degree =
        CertifiedInterval::from_bounds(PI_LOWER, PI_UPPER).div(CertifiedInterval::exact(180.0));
    let attained_lower = CertifiedInterval::from_bounds(PI_LOWER, PI_UPPER)
        .sub(CertifiedInterval::exact(latitude).mul(radians_per_degree))
        .mul(CertifiedInterval::exact(geodesic.a))
        .lower();
    assert!(
        lower <= attained_lower,
        "certified lower {lower} exceeds the independent spherical endpoint distance lower {attained_lower}"
    );
}
#[rstest::rstest]
#[case::point_new_raw_xy_180_0_0_0(Point::new_raw_xy(-180.0, 0.0), Point::new_raw_xy(179.0, 10.0), Point::new_raw_xy(-179.0, 20.0))]
#[case::point_new_raw_xy_30_0_89_9(Point::new_raw_xy(30.0, 89.9), Point::new_raw_xy(-45.0, 80.0), Point::new_raw_xy(45.0, 80.0))]
#[case::point_new_raw_xy_100_0_89_8(Point::new_raw_xy(100.0, -89.8), Point::new_raw_xy(170.0, 88.0), Point::new_raw_xy(-100.0, 87.0))]
#[case::point_new_raw_xy_0_0_90_0(Point::new_raw_xy(0.0, 90.0), Point::new_raw_xy(-120.0, 89.0), Point::new_raw_xy(120.0, 89.5))]
fn chart_box_lower_bound_is_sound_for_seams_and_poles(
    #[case] point: Point,
    #[case] start: Point,
    #[case] end: Point,
) {
    let frame = GeographicFrame::crs84();
    let metric = metric(&Geodesic::wgs84(), frame);

    let edge = metric.chart_edge(start, end).expect("admitted chart edge");
    let bounds = edge.bounds();
    let lower = metric.point_envelope_bound(point)(bounds);
    for sample in 0..=256_u32 {
        let parameter = f64::from(sample) / 256.0;
        let distance = oracle_distance(&metric, point, &edge, parameter);
        assert!(
            lower <= distance + 1.0e-8,
            "bounds={bounds:?}, point={point:?}, t={parameter}: {lower} > {distance}"
        );
    }
}
