#[rstest::rstest]
#[case::degrees(degrees())]
#[case::grads(grads())]
#[case::radians(radians())]
#[case::south_axis_degrees(south_axis_degrees())]
fn chart_edge_colatitude_uses_native_subtraction_before_conversion(#[case] frame: GeographicFrame) {
    let latitude = frame.pole().next_down();
    let edge = ChartEdge::new(
        Point::new_raw_xy(0.0, latitude),
        Point::new_raw_xy(0.25, latitude),
        frame,
    )
    .unwrap();
    let expected = (frame.pole() - latitude.abs()) * frame.latitude_radians_per_unit();
    let actual = (edge.chart_pole - edge.start_latitude.abs()) * edge.latitude_unit.abs();
    assert_eq!(actual.to_bits(), expected.to_bits());
}

#[test]
fn chart_pole_degeneracy_uses_native_axis_magnitude_on_reversed_latitude_axis() {
    let frame = south_axis_degrees();
    let edge = ChartEdge::new(
        Point::new_raw_xy(0.0, frame.pole()),
        Point::new_raw_xy(20.0, frame.pole()),
        frame,
    )
    .unwrap();
    assert_eq!(edge.chart_pole, 90.0);
    assert!(edge.is_degenerate());
    assert_eq!(edge.geodetic_position(0.5).unwrap().y, -90.0);
}

#[test]
fn reversed_native_latitude_uses_the_same_physical_meridian_measure() {
    let geodesic = wgs84();
    let edge = ChartEdge::new(
        Point::new_raw_xy(15.0, 20.0),
        Point::new_raw_xy(15.0, 40.0),
        south_axis_degrees(),
    )
    .unwrap();
    let expected = super::super::inverse_distance(&geodesic, 15.0, -20.0, 15.0, -40.0);
    assert!((edge.length(&geodesic).unwrap() - expected).abs() <= 1.0e-8);
}

#[rstest::rstest]
#[case::degrees(degrees())]
#[case::grads(grads())]
#[case::radians(radians())]
fn chart_edge_rejects_exact_half_periods_with_an_actionable_geometry_error(
    #[case] frame: GeographicFrame,
) {
    let error = ChartEdge::new(
        Point::new_raw_xy(0.0, 10.0),
        Point::new_raw_xy(frame.longitude().half_period(), 10.0),
        frame,
    )
    .unwrap_err();
    assert!(
        matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(
                GeometryErrorKind::Parameter { message, .. }
            ) if message.as_ref() == "edge spans exactly half the longitude period; add a vertex"
        ),
        "{error}"
    );
}

#[rstest::rstest]
#[case::degrees(degrees())]
#[case::grads(grads())]
#[case::radians(radians())]
fn chart_edge_unwrapping_matches_topology_for_seams_ties_and_period_spellings(
    #[case] frame: GeographicFrame,
) {
    let axis = frame.longitude();
    let half = axis.half_period();
    let period = axis.period();
    for (from, to) in [
        (0.0, half.next_down()),
        (0.0, half),
        (0.0, half.next_up()),
        (0.0, -half.next_down()),
        (0.0, -half),
        (0.0, -half.next_up()),
        (-period, period),
        (2.0 * period, 3.0 * period),
        (-3.0 * period, -2.0 * period),
        (half.next_down(), -half.next_down()),
        (-half.next_down(), half.next_down()),
        (half.next_up(), -half.next_up()),
        (-half.next_up(), half.next_up()),
        (period * 5.25, period * -4.125),
        (period * -5.25, period * 4.125),
        (period * 0.125, period * 5.375),
        (period * -0.125, period * -5.375),
        (period * 0.375, period * -5.125),
        (period * -0.375, period * 5.125),
    ] {
        assert_topology_delta(frame, from, to);
    }
}

fn assert_topology_delta(frame: GeographicFrame, from: f64, to: f64) {
    let axis = frame.longitude();
    let step = axis.chart_step(from, to).unwrap();
    let delta = step.delta(axis);
    if matches!(step, ChartStep::Shortest(EdgeDirection::HalfTurn(_))) {
        assert!(
            ChartEdge::new(
                Point::new_raw_xy(from, 0.0),
                Point::new_raw_xy(to, 0.0),
                frame
            )
            .is_err(),
            "half-period edge was accepted: frame={frame:?}, from={from:?}, to={to:?}"
        );
        return;
    }
    let edge = ChartEdge::new(
        Point::new_raw_xy(from, 0.0),
        Point::new_raw_xy(to, 0.0),
        frame,
    )
    .unwrap();
    assert_eq!(
        edge.longitude_delta.to_bits(),
        (delta * axis.radians_per_unit() * frame.longitude_orientation_sign()).to_bits(),
        "frame={frame:?}, from={from:?}, to={to:?}"
    );

    let expected_transit = step.transit(axis, from, to).unwrap();
    assert_eq!(
        axis.chart_transit(from, to).unwrap(),
        expected_transit,
        "frame={frame:?}, from={from:?}, to={to:?}"
    );
}
