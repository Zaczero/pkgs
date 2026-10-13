#[test]
fn analytic_chart_differentials_contain_meridians_obliques_and_parallels() {
    let meridian_geodesic = wgs84();
    let meridian = ChartEdge::new(
        Point::new_raw_xy(0.0, -72.384_836_123_684_47),
        Point::new_raw_xy(0.0, 72.384_836_123_684_47),
        degrees(),
    )
    .unwrap();
    let meridian_bounds = meridian.differential_bounds(&meridian_geodesic);
    for parameter in [0.125, 0.1875, 0.5, 0.8125, 0.875] {
        let derivative = meridian.derivatives(parameter, &meridian_geodesic);
        assert!(meridian_bounds.speed >= derivative.speed);
        assert!(meridian_bounds.covariant_acceleration >= derivative.acceleration);
    }
    let peak = meridian
        .derivatives(0.1875, &meridian_geodesic)
        .acceleration
        / meridian_geodesic.a;
    assert!((peak - 0.064_216_997_824_935_52).abs() <= 2.0e-16);

    for (geodesic, frame, start, end) in [
        (
            wgs84(),
            degrees(),
            Point::new_raw_xy(-70.0, 10.0),
            Point::new_raw_xy(80.0, 55.0),
        ),
        (
            Geodesic::new(6_371_000.0, 0.0),
            degrees(),
            Point::new_raw_xy(-70.0, 10.0),
            Point::new_raw_xy(80.0, 55.0),
        ),
        (
            Geodesic::new(6_371_000.0, -0.01),
            degrees(),
            Point::new_raw_xy(170.0, 88.0),
            Point::new_raw_xy(-100.0, 87.0),
        ),
        (
            wgs84(),
            degrees(),
            Point::new_raw_xy(-45.0, 89.999_999_999),
            Point::new_raw_xy(45.0, 89.999_999_999),
        ),
        (
            wgs84(),
            radians(),
            Point::new_raw_xy(-0.7, 1.2),
            Point::new_raw_xy(0.7, 1.2),
        ),
        (
            wgs84(),
            south_axis_degrees(),
            Point::new_raw_xy(0.0, 72.384_836_123_684_47),
            Point::new_raw_xy(0.0, -72.384_836_123_684_47),
        ),
    ] {
        let edge = ChartEdge::new(start, end, frame).unwrap();
        let bounds = edge.differential_bounds(&geodesic);
        for parameter in [0.0, 0.125, 0.1875, 0.5, 0.8125, 0.875, 1.0] {
            let derivative = edge.derivatives(parameter, &geodesic);
            assert!(
                bounds.speed >= derivative.speed,
                "speed at t={parameter}: {} < {}",
                bounds.speed,
                derivative.speed
            );
            assert!(
                bounds.covariant_acceleration
                    >= derivative.acceleration + derivative.speed * edge.longitude_delta.abs(),
                "covariant acceleration at t={parameter}: {} < {}",
                bounds.covariant_acceleration,
                derivative.acceleration + derivative.speed * edge.longitude_delta.abs()
            );
        }
    }
}
