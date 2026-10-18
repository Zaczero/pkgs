#[test]
fn raw_ring_certificate_declines_unrecognized_noninteger_period() {
    let factor =
        crate::geographic::AngularFactor::try_new("unit", std::f64::consts::TAU / 399.5).unwrap();
    let frame = GeographicFrame::try_new(factor, factor, 0.0).unwrap();
    let xs = [200.0, 200.0, 100.0, 100.0, 200.0, 0.0, 0.0, 200.0];
    let ys = [0.0, 1.0, 1.0, 0.0, 0.0, 0.0, 1.0, 0.0];
    assert_eq!(frame.longitude().period(), 399.5);
    assert!(raw_ring_certificate(frame, &xs, &ys).is_none());
    assert_eq!(
        exact_ring_area_sign_columns(&xs, &ys, xs.len().saturating_sub(1)),
        AreaSign::Zero,
    );
    let area = lifted_ring_area_sign(frame, &xs, &ys, true).unwrap();
    assert_eq!(area.chart_turn, 0);
    assert_eq!(area.orientation, Some(1));
    assert_eq!(lifted_exact_ring_area_sign(frame, &xs, &ys).unwrap(), 1);
}

#[test]
fn raw_ring_certificate_keeps_exact_negative_seam_endpoints() {
    let frame = GeographicFrame::crs84();
    let xs = [-180.0, -170.0, -170.0, -180.0, -180.0];
    let ys = [-10.0, -10.0, 10.0, 10.0, -10.0];
    assert!(raw_ring_certificate(frame, &xs, &ys).is_some());
    let area = lifted_ring_area_sign(frame, &xs, &ys, true).unwrap();
    assert_eq!(area.chart_turn, 0);
    assert_eq!(area.orientation, Some(1));
    assert!(!area.full_extent);
}

#[rstest::rstest]
#[case::degrees(GeographicFrame::crs84())]
#[case::radians({
    let unit = crate::geographic::AngularFactor::try_new("unit", 1.0).unwrap();
    GeographicFrame::try_new(unit, unit, 0.0).unwrap()
})]
fn raw_ring_certificate_keeps_positive_seam_orientation_on_thin_cycles(
    #[case] frame: GeographicFrame,
) {
    let half = frame.longitude().half_period();
    let interior = half * 0.123_456_789;
    for height in [
        f64::from_bits(1),
        f64::MIN_POSITIVE,
        1.0_f64.next_up() - 1.0,
    ] {
        for (height, expected) in [(height, -1), (-height, 1), (0.0, 0)] {
            for (xs, ys) in [
                ([half, interior, interior, half, half], [
                    0.0, 0.0, height, height, 0.0,
                ]),
                ([interior, interior, half, half, interior], [
                    0.0, height, height, 0.0, 0.0,
                ]),
            ] {
                assert!(raw_ring_certificate(frame, &xs, &ys).is_some());
                let raw = exact_ring_area_sign_columns(&xs, &ys, 4);
                assert_eq!(raw, match expected {
                    -1 => AreaSign::Negative,
                    1 => AreaSign::Positive,
                    _ => AreaSign::Zero,
                },);
                assert_eq!(
                    lifted_exact_ring_area_sign(frame, &xs, &ys).unwrap(),
                    expected,
                );
                let area = lifted_ring_area_sign(frame, &xs, &ys, true).unwrap();
                assert_eq!(area.orientation, Some(expected));
                assert_eq!(area.chart_turn, 0);
                assert!(!area.full_extent);
            }
        }
    }
}

#[test]
fn raw_ring_certificate_declines_positive_seam_half_turn_ties() {
    for xs in [[180.0, 0.0, 90.0, 180.0], [90.0, 180.0, 0.0, 90.0]] {
        assert!(
            raw_ring_certificate(GeographicFrame::crs84(), &xs, &[0.0, 1.0, 1.0, 0.0],).is_none()
        );
    }
    assert!(
        raw_ring_certificate(
            GeographicFrame::crs84(),
            &[180.0, 170.0, -10.0, 170.0, 180.0],
            &[0.0, 1.0, 1.0, 0.0, 0.0],
        )
        .is_none()
    );
}

#[test]
fn raw_ring_certificate_preserves_full_extent_between_both_seam_spellings() {
    let frame = GeographicFrame::crs84();
    let xs = [-180.0, -90.0, 0.0, 90.0, 180.0, 90.0, 0.0, -90.0, -180.0];
    let ys = [-1.0, -1.0, -1.0, -1.0, -1.0, 1.0, 1.0, 1.0, -1.0];
    assert!(raw_ring_certificate(frame, &xs, &ys).is_some());
    let area = lifted_ring_area_sign(frame, &xs, &ys, true).unwrap();
    assert!(area.full_extent);
    assert_eq!(area.chart_turn, 0);
    assert_eq!(area.orientation, Some(1));
    assert_eq!(lifted_exact_ring_area_sign(frame, &xs, &ys).unwrap(), 1);
}

#[test]
fn opposite_seam_spellings_are_zero_steps_but_decline_the_raw_certificate() {
    let frame = GeographicFrame::crs84();
    let axis = frame.longitude();
    let xs = [180.0, -180.0, -180.0, 180.0, 180.0];
    let ys = [-10.0, -10.0, 10.0, 10.0, -10.0];
    assert!(raw_ring_certificate(frame, &xs, &ys).is_none());
    assert_eq!(
        axis.chart_step(180.0, -180.0)
            .unwrap()
            .delta(axis)
            .to_bits(),
        0.0_f64.to_bits()
    );
    let area = lifted_ring_area_sign(frame, &xs, &ys, true).unwrap();
    assert_eq!(area.chart_turn, 0);
    assert_eq!(area.orientation, Some(0));
    assert!(!area.full_extent);
}

#[rstest::rstest]
#[case::xs_180_0_180_0_190_0_190_0_180_0([-180.0, -180.0, -190.0, -190.0, -180.0], 1)]
#[case::xs_180_0_180_0_170_0_170_0_180_0([-180.0, -180.0, -170.0, -170.0, -180.0], -1)]
fn lifted_ring_orientation_distinguishes_equal_directed_seam_edges(
    #[case] xs: [f64; 5],
    #[case] expected: i8,
) {
    let frame = GeographicFrame::crs84();
    let ys = [-10.0, 10.0, 10.0, -10.0, -10.0];

    let area = lifted_ring_area_sign(frame, &xs, &ys, true).unwrap();
    assert_eq!(area.orientation, Some(expected));
    assert!(!area.full_extent);
    assert_eq!(area.chart_turn, 0);
    assert_eq!(
        lifted_exact_ring_area_sign(frame, &xs, &ys).unwrap(),
        expected
    );
    assert_eq!(
        lifted_ring_area_sign(frame, &xs, &ys, false)
            .unwrap()
            .orientation,
        None,
    );
}

#[test]
fn unrepresentable_lift_uses_the_exact_streaming_area_sign() {
    let frame = GeographicFrame::crs84();
    let xs = [170.0, -100.1, -100.1, 170.0, 170.0];
    let ys = [-10.0, -10.0, 10.0, 10.0, -10.0];
    assert_eq!(exactly_represented_lift(-100.1, 1, 360.0), None);
    let area = lifted_ring_area_sign(frame, &xs, &ys, true).unwrap();
    assert_eq!(area.orientation, Some(1));
    assert!(!area.full_extent);
    assert_eq!(lifted_exact_ring_area_sign(frame, &xs, &ys).unwrap(), 1);
}

#[test]
fn streaming_lifted_area_keeps_zero_orientation_undetermined() {
    let area = lifted_ring_area_sign(
        GeographicFrame::crs84(),
        &[-180.0, -180.0, -180.0],
        &[-10.0, 10.0, -10.0],
        true,
    )
    .unwrap();
    assert_eq!(area.orientation, Some(0));
}
