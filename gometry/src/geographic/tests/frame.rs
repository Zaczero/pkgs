use super::*;

#[test]
fn chart_and_geodetic_poles_remain_distinct_for_southward_latitude_axes() {
    let unit = factor(std::f64::consts::PI / 180.0);
    let frame = GeographicFrame::try_new_with_orientation(
        unit,
        unit,
        0.0,
        GeographicAxisOrientation::from_axis_directions(true, false),
    )
    .unwrap();
    assert_eq!(frame.chart_pole_latitude(ChartPole::North), 90.0);
    assert_eq!(frame.chart_pole_latitude(ChartPole::South), -90.0);
    assert_eq!(frame.native_latitude_pole(GeodeticPole::North), -90.0);
    assert_eq!(frame.native_latitude_pole(GeodeticPole::South), 90.0);
}
#[test]
fn conventional_degree_latitude_conversion_preserves_source_bits() {
    let unit = factor(std::f64::consts::PI / 180.0);
    for latitude_north in [true, false] {
        let frame = GeographicFrame::try_new_with_orientation(
            unit,
            unit,
            0.0,
            GeographicAxisOrientation::from_axis_directions(true, latitude_north),
        )
        .unwrap();
        for latitude in [
            -63.870_640_325_120_85,
            -61.836_984_191_501_61,
            43.625_172_661_832_906,
        ] {
            let native_latitude = latitude * frame.latitude_orientation_sign();
            assert_bits(
                frame.to_geodesic_latitude(native_latitude).unwrap(),
                latitude,
            );
            let native = frame
                .point_from_geodesic(crate::geometry::Point::new_raw_xy(0.0, latitude))
                .unwrap();
            assert_bits(native.y, native_latitude);
        }
    }
}
#[test]
fn frames_derive_degree_grad_and_radian_axes() {
    let degree = degrees();
    assert_eq!(degree.longitude().period(), 360.0);
    assert_eq!(degree.pole(), 90.0);
    let grad = frame(std::f64::consts::TAU / 400.0);
    assert_eq!(grad.longitude().period(), 400.0);
    assert_eq!(grad.pole(), 100.0);
    let radian = frame(1.0);
    assert_eq!(radian.longitude().period(), std::f64::consts::TAU);
    assert_eq!(radian.pole(), std::f64::consts::FRAC_PI_2);
    assert_eq!(
        degree.longitude().unit().radians_per_unit(),
        std::f64::consts::PI / 180.0
    );
    assert_eq!(
        grad.longitude().radians_per_unit(),
        std::f64::consts::TAU / 400.0
    );
    assert_eq!(
        grad.latitude_radians_per_unit(),
        std::f64::consts::TAU / 400.0
    );
    let adjacent_grad = frame((std::f64::consts::TAU / 400.0).next_up());
    assert_eq!(adjacent_grad.longitude().period(), 400.0);
    assert_eq!(adjacent_grad.pole(), 100.0);
    assert_eq!(grad, adjacent_grad);
    assert_eq!(
        degree.latitude_unit().radians_per_unit(),
        std::f64::consts::PI / 180.0
    );
    assert_eq!(degree.prime_meridian_radians(), 0.0);
    AngularFactor::try_new("unit", 0.0).unwrap_err();
    AngularFactor::try_new("unit", f64::INFINITY).unwrap_err();
    PeriodicAxis::try_new("subnormal axis", factor(f64::MAX)).unwrap_err();
    GeographicFrame::try_new(degree.longitude().unit(), factor(f64::from_bits(1)), 0.0)
        .unwrap_err();
}
#[test]
fn frame_conversion_uses_one_greenwich_seam_for_points_and_lanes() {
    let degree = degrees();
    let converted = degree
        .to_geodesic_point(crate::geometry::Point::new_raw_xy(180.0, 45.0))
        .unwrap();
    assert_bits(converted.x, -180.0);
    assert_bits(
        degree
            .point_from_geodesic(crate::geometry::Point::new_raw_xy(180.0, 45.0))
            .unwrap()
            .x,
        -180.0,
    );
    assert_bits(
        degree
            .to_geodesic_point(crate::geometry::Point::new_raw_xy(0.0, 90.0_f64.next_up()))
            .unwrap()
            .y,
        90.0,
    );

    let paris = GeographicFrame::try_new(
        factor(std::f64::consts::PI / 180.0),
        factor(std::f64::consts::PI / 180.0),
        2.0_f64.to_radians(),
    )
    .unwrap();
    let converted = paris
        .to_geodesic_point(crate::geometry::Point::new_raw_xy(0.0, 45.0))
        .unwrap();
    assert_eq!(converted.x, 2.0);
    assert_eq!(converted.y, 45.0);
    let restored = paris.point_from_geodesic(converted).unwrap();
    assert_eq!(restored.x, 0.0);
    assert_eq!(restored.y, 45.0);

    let mut xs = [0.0, 180.0];
    let mut ys = [45.0, -45.0];
    paris.to_geodesic_lanes(&mut xs, &mut ys).unwrap();
    assert_eq!(xs, [2.0, -178.0]);
    paris.lanes_from_geodesic(&mut xs, &mut ys).unwrap();
    assert_eq!(xs, [0.0, -180.0]);
}
#[test]
fn computed_native_points_canonicalize_longitude_and_preserve_other_axes() {
    for (frame, longitude, expected) in [
        (degrees(), 180.0, -180.0),
        (frame(std::f64::consts::TAU / 400.0), 200.0, -200.0),
        (frame(1.0), std::f64::consts::PI, -std::f64::consts::PI),
    ] {
        let point = crate::geometry::Point::new_axes(
            longitude,
            -0.0,
            crate::geometry::ZOrdinate(Some(-0.0)),
            crate::geometry::MOrdinate(Some(7.0)),
        )
        .unwrap();
        let emitted = frame.canonical_computed_point(point).unwrap();

        assert_bits(emitted.x, expected);
        assert_bits(emitted.y, -0.0);
        assert_bits(emitted.z().unwrap(), -0.0);
        assert_eq!(emitted.m(), Some(7.0));
    }

    let zero = degrees()
        .canonical_computed_point(crate::geometry::Point::new_raw_xy(-0.0, 0.0))
        .unwrap();
    assert_bits(zero.x, 0.0);
}
#[test]
#[expect(
    clippy::float_cmp,
    reason = "the error payload must retain the exact invalid input latitude"
)]
fn lane_conversion_leaves_the_failing_pair_and_later_pairs_unchanged() {
    let paris = GeographicFrame::try_new(
        factor(std::f64::consts::PI / 180.0),
        factor(std::f64::consts::PI / 180.0),
        2.0_f64.to_radians(),
    )
    .unwrap();

    let mut xs = [0.0, 1.0, 10.0, 99.0];
    let mut ys = [45.0, 100.0, 20.0];
    let error = paris
        .to_geodesic_lanes_indexed(&mut xs, &mut ys)
        .unwrap_err();
    assert_eq!(error.0, 1);
    assert!(matches!(
        error.1,
        GeographicError::LatitudeOutOfRange { value } if value == 100.0
    ));
    assert_eq!(xs, [2.0, 1.0, 10.0, 99.0]);
    assert_eq!(ys, [45.0, 100.0, 20.0]);

    let mut xs = [2.0, f64::NAN, 5.0, 99.0];
    let mut ys = [45.0, 10.0, 20.0];
    let error = paris
        .lanes_indexed_from_geodesic(&mut xs, &mut ys)
        .unwrap_err();
    assert_eq!(error.0, 1);
    assert!(matches!(error.1, GeographicError::NonFinite {
        name: "geodesic coordinate",
        ..
    }));
    assert_eq!(xs[0], 0.0);
    assert_eq!(xs[1].to_bits(), f64::NAN.to_bits());
    assert_eq!(xs[2], 5.0);
    assert_eq!(xs[3], 99.0);
    assert_eq!(ys, [45.0, 10.0, 20.0]);
}
#[test]
fn identity_and_expansion_carriers_are_explicit() {
    assert_ne!(SeamIdentity::Seam, SeamIdentity::Pole);
    assert_eq!(SeamIdentity::Ordinary, SeamIdentity::Ordinary);
    let frame = degrees();
    assert_eq!(
        frame
            .longitude()
            .canonicalize(180.0)
            .unwrap()
            .identity(frame.longitude()),
        SeamIdentity::Seam
    );
    assert_eq!(
        Latitude::try_new(frame, 90.0).unwrap().identity(frame),
        SeamIdentity::Pole
    );
    assert_eq!(frame.longitude().seam_transit(170.0, -170.0).unwrap(), 1);
}
