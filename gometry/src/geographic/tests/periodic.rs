use super::*;

#[test]
fn pole_identity_supersedes_longitude() {
    let paris = GeographicFrame::try_new(
        factor(std::f64::consts::PI / 180.0),
        factor(std::f64::consts::PI / 180.0),
        2.0_f64.to_radians(),
    )
    .unwrap();
    let shape = crate::geometry::Shape::Point(crate::geometry::Point::new(1234.0, 90.0).unwrap());
    let view = ScalarGeographicView::try_new(paris, &shape).unwrap();
    view.for_each_coordinate(|coordinate| {
        assert_eq!(coordinate.identity(paris), SeamIdentity::Pole);
    });
}
#[test]
fn native_periodic_seam_detection_uses_the_frame_half_period() {
    let grads = GeographicFrame::try_new(
        factor(std::f64::consts::TAU / 400.0),
        factor(std::f64::consts::TAU / 400.0),
        0.0,
    )
    .unwrap();
    let crossing = crate::geometry::Shape::LineString(
        crate::geometry::LineSeq::try_new(xy_seq(vec![150.0, 250.0], vec![0.0, 0.0])).unwrap(),
    );
    let non_crossing = crate::geometry::Shape::LineString(
        crate::geometry::LineSeq::try_new(xy_seq(vec![0.0, 300.0], vec![0.0, 0.0])).unwrap(),
    );
    let seam_endpoint = crate::geometry::Shape::LineString(
        crate::geometry::LineSeq::try_new(xy_seq(vec![200.0, 199.5], vec![0.0, 0.0])).unwrap(),
    );

    assert!(crate::geometry::crosses_antimeridian_in_frame(
        &crossing, grads
    ));
    assert!(!crate::geometry::crosses_antimeridian_in_frame(
        &non_crossing,
        grads
    ));
    assert!(!crate::geometry::crosses_antimeridian_in_frame(
        &seam_endpoint,
        grads
    ));
}
#[test]
fn ordinary_chart_certificate_matches_selected_period_and_edge_contracts() {
    let nearby_grads = nearby_grad_factor_frame();
    let frames = [
        degrees(),
        frame(1.0),
        frame(std::f64::consts::TAU / 400.0),
        nearby_grads,
    ];
    for frame in frames {
        let axis = frame.longitude();
        let half = axis.half_period();
        assert!(!axis.ordinary_chart_edge(-half / 2.0, half / 2.0));
        assert!(axis.ordinary_chart_edge(-half.next_down(), 0.0));
        assert!(!axis.ordinary_chart_edge(-half, half));
        assert!(!axis.ordinary_chart_edge(0.0, axis.period()));
        assert!(!axis.ordinary_chart_edge(-half.next_down(), half.next_down()));
        assert_eq!(axis.chart_transit(-half / 2.0, half / 2.0).unwrap(), 0);
    }

    let axis = degrees().longitude();
    let from = -90.0;
    let to = 90.0_f64.next_up();
    assert_eq!(to - from, 180.0);
    assert!(!axis.ordinary_chart_edge(from, to));
    let connection = axis.chart_connection(from, to).unwrap();
    assert_eq!(connection.source_lift, -1);
    assert_eq!(connection.seam_transit(), -1);

    for axis in [
        nearby_grads.longitude(),
        frame(std::f64::consts::TAU / 400.0).longitude(),
    ] {
        assert_bits(axis.period(), 400.0);
        assert_bits(axis.half_period(), 200.0);
        assert_eq!(axis.ordinary_longitude_bound(), 200.0);
        let nominal_half = axis.canonicalize(200.0).unwrap();
        assert_bits(nominal_half.get(), -200.0);
        assert_eq!(nominal_half.identity(axis), SeamIdentity::Seam);
    }
    assert_eq!(
        axis.canonicalize(-0.0).unwrap().get().to_bits(),
        0.0_f64.to_bits()
    );
}
#[test]
#[expect(
    clippy::float_cmp,
    reason = "these tests characterize exact binary64 spellings at a periodic seam"
)]
fn removed_period_workarounds_preserve_finite_longitude_bits() {
    use crate::geometry::numerics::product::exact_power_of_two;
    fn previous_wrap(axis: PeriodicAxis, value: f64) -> f64 {
        let mut reduced = value % axis.period();
        if value.fract() == 0.0 && axis.period().fract() == 0.0 {
            let nominal_remainder = value % axis.period();
            if nominal_remainder == 0.0 {
                reduced = 0.0;
            } else if nominal_remainder.abs() == axis.period() / 2.0 {
                reduced = -axis.half_period();
            }
        }
        if reduced < -axis.half_period() {
            reduced += axis.period();
        } else if reduced >= axis.half_period() {
            reduced -= axis.period();
        }
        if reduced == 0.0 {
            reduced = 0.0;
        }
        reduced
    }

    fn previous_ordinary_longitude(axis: PeriodicAxis, value: f64) -> Option<u64> {
        if value.abs().partial_cmp(&axis.half_period()) != Some(std::cmp::Ordering::Less) {
            return None;
        }
        if value.fract() == 0.0
            && axis.period().fract() == 0.0
            && value.abs() == axis.period() / 2.0
        {
            return None;
        }
        Some(if value == 0.0 { 0.0 } else { value }.to_bits())
    }

    fn previous_ordinary_longitude_bound(axis: PeriodicAxis) -> f64 {
        if axis.period().fract() == 0.0 && (axis.period() / 2.0).fract() == 0.0 {
            axis.half_period().min(axis.period() / 2.0)
        } else {
            axis.half_period()
        }
    }

    let mut axes = vec![degrees().longitude(), frame(1.0).longitude()];
    for turn in [400.0_f64.next_down(), 400.0, 400.0_f64.next_up()] {
        let unit = factor(std::f64::consts::TAU / turn);
        assert_eq!(unit.recognized_period(), Some(400.0));
        axes.push(
            GeographicFrame::try_new(unit, unit, 0.0)
                .unwrap()
                .longitude(),
        );
    }

    for axis in axes {
        let half = axis.half_period();
        let period = axis.period();
        let large_alias = period * (exact_power_of_two(40));
        let values = [
            -half,
            half,
            -half.next_down(),
            -half.next_up(),
            half.next_down(),
            half.next_up(),
            -0.0,
            0.0,
            -period,
            period,
            -large_alias,
            large_alias,
            -large_alias - half,
            large_alias + half,
            f64::MIN,
            f64::MAX,
        ];

        assert_eq!(
            axis.ordinary_longitude_bound().to_bits(),
            previous_ordinary_longitude_bound(axis).to_bits()
        );
        for value in values {
            assert!(value.is_finite());
            assert_eq!(
                axis.wrap_admitted(value).get().to_bits(),
                previous_wrap(axis, value).to_bits(),
                "wrapped bits for {value:?} with period {period:?}"
            );
            assert_eq!(
                axis.ordinary_longitude(value)
                    .map(|value| value.get().to_bits()),
                previous_ordinary_longitude(axis, value),
                "ordinary bits for {value:?} with period {period:?}"
            );
        }
    }
}
#[test]
fn longitude_canonicalization_pins_aliases_neighbors_and_nonfinite() {
    let axis = degrees().longitude();
    for (input, expected) in [
        (0.0, 0.0),
        (-0.0, 0.0),
        (360.0, 0.0),
        (720.0, 0.0),
        (-720.0, 0.0),
        (180.0, -180.0),
        (-180.0, -180.0),
        (540.0, -180.0),
    ] {
        assert_bits(axis.canonicalize(input).unwrap().get(), expected);
    }
    assert_eq!(
        axis.canonicalize(180.0_f64.next_down()).unwrap().get(),
        180.0_f64.next_down()
    );
    assert!(axis.canonicalize(180.0_f64.next_up()).unwrap().get() < -179.0);
    axis.canonicalize(f64::NAN).unwrap_err();
    axis.canonicalize(f64::INFINITY).unwrap_err();
}
#[test]
fn nearest_longitude_alias_rounds_the_selected_source_translation_once() {
    let axis = degrees().longitude();
    assert_bits(
        axis.nearest_longitude_alias(-190.0, 177.813_015_064_085_82)
            .unwrap(),
        -182.186_984_935_914_18,
    );
}
#[test]
fn nearest_longitude_alias_fuses_an_integer_shift_rounded_to_the_exactness_boundary() {
    use crate::geometry::numerics::product::exact_power_of_two;
    let axis = frame(std::f64::consts::TAU / 107.0).longitude();
    // 84_179_432_287_299 * 107 = 2^53 + 1, whose rounded product is 2^53.
    assert_bits(
        axis.nearest_longitude_alias(0.0, -(exact_power_of_two(53)))
            .unwrap(),
        1.0,
    );
}
#[test]
fn nearest_longitude_alias_keeps_period_counts_larger_than_a_significand_exact() {
    use crate::geometry::numerics::product::exact_power_of_two;
    let axis = degrees().longitude();
    // 27_777_777_777_777_778 * 360 - 10_000_000_000_000_000_000 = 80.
    assert_bits(axis.nearest_longitude_alias(1.0, -1.0e19).unwrap(), 80.0);
    assert_bits(axis.nearest_longitude_alias(-1.0, 1.0e19).unwrap(), -80.0);
    assert_bits(
        axis.nearest_longitude_alias(1.0, -f64::MAX).unwrap(),
        -128.0,
    );
    assert_bits(axis.nearest_longitude_alias(-1.0, f64::MAX).unwrap(), 128.0);

    let radians = frame(1.0).longitude();
    assert_bits(
        radians.nearest_longitude_alias(1.0, -1.0e19).unwrap(),
        exact_native_alias_oracle(-1.0e19, radians.period(), 1_591_549_430_918_953_420),
    );

    let grads = frame(std::f64::consts::TAU / 400.0).longitude();
    for (source, reference, periods) in [
        (-(1.0e19_f64.next_up()), 1.0, 25_000_000_000_000_005),
        (1.0e19_f64.next_up(), -1.0, -25_000_000_000_000_005),
    ] {
        assert_bits(
            grads.nearest_longitude_alias(reference, source).unwrap(),
            exact_native_alias_oracle(source, grads.period(), periods),
        );
    }

    let selected_period_tie = frame(std::f64::consts::TAU / 16.0_f64.next_down()).longitude();
    assert_bits(
        selected_period_tie
            .nearest_longitude_alias(0.0, -(exact_power_of_two(54) + 8.0))
            .unwrap(),
        -8.0,
    );
}
#[rstest::rstest]
#[case::degrees_longitude(degrees().longitude())]
#[case::frame_std_f64_consts_tau_400_0_longitude(frame(std::f64::consts::TAU / 400.0).longitude())]
#[case::frame_1_0_longitude(frame(1.0).longitude())]
fn nearest_longitude_alias_forms_whole_period_shifts_from_the_source(#[case] axis: PeriodicAxis) {
    // A nearby reference is not an alias of the source. Select k from
    // the exact operands, then round source+kP instead of copying reference.
    assert_bits(
        axis.nearest_longitude_alias(1.0e-16, -axis.period())
            .unwrap(),
        0.0,
    );
    assert_bits(
        axis.nearest_longitude_alias(-1.0e-16, axis.period())
            .unwrap(),
        0.0,
    );
}
#[rstest::rstest]
#[case::degrees_longitude(degrees().longitude(), [
    177.813_015_064_085_82,
    -122.427_194,
    0.1,
    -0.1,
])]
#[case::frame_std_f64_consts_tau_400_0_longitude(frame(std::f64::consts::TAU / 400.0).longitude(), [
            177.813_015_064_085_82,
            -122.427_194,
            0.1,
            -0.1,
        ])]
#[case::frame_1_0_longitude(frame(1.0).longitude(), [
    1.778_130_150_640_858_2,
    -1.224_271_94,
    0.1,
    -0.1,
])]
fn nearest_longitude_alias_matches_exact_native_period_shifts(
    #[case] axis: PeriodicAxis,
    #[case] longitudes: [f64; 4],
) {
    for longitude in longitudes {
        for periods in [-5_i32, -2, -1, 1, 2, 5] {
            let reference = longitude + f64::from(periods) * axis.period() + axis.period() / 16.0;
            assert_bits(
                axis.nearest_longitude_alias(reference, longitude).unwrap(),
                exact_native_alias_oracle(longitude, axis.period(), i128::from(periods)),
            );
        }
    }
}
#[test]
fn nearest_longitude_alias_reduces_before_rounding_and_keeps_tie_direction() {
    let axis = degrees().longitude();
    let source = 122.427_194_f64;
    let lifted = source + axis.period();
    let canonical = axis.canonicalize(lifted).unwrap().get();

    assert_bits(canonical, source.next_down());
    // Reducing the lifted displacement first recovers the chart spelling;
    // canonicalizing the rounded alias first loses one bit of the source.
    assert_bits(
        axis.nearest_longitude_alias(lifted, canonical).unwrap(),
        lifted,
    );
    assert_bits(axis.nearest_longitude_alias(530.0, 170.0).unwrap(), 530.0);

    // At a half-period tie, retain the route point's spelling in the
    // source-directed tie direction.
    assert_bits(axis.nearest_longitude_alias(180.0, 0.0).unwrap(), 0.0);
    assert_bits(axis.nearest_longitude_alias(0.0, 180.0).unwrap(), 180.0);
    assert_bits(axis.nearest_longitude_alias(540.0, 0.0).unwrap(), 360.0);
}
#[test]
fn nearest_longitude_alias_preserves_source_bits_for_no_shift() {
    let axis = degrees().longitude();
    let source = 0.2_f64;
    assert_eq!(
        axis.nearest_longitude_alias(78.0, source)
            .unwrap()
            .to_bits(),
        source.to_bits()
    );
    let negative_zero = -0.0;
    assert_eq!(
        axis.nearest_longitude_alias(negative_zero, negative_zero)
            .unwrap()
            .to_bits(),
        negative_zero.to_bits()
    );
}
#[rstest::rstest]
#[case::degrees_longitude(degrees().longitude())]
#[case::frame_std_f64_consts_tau_400_0_longitude(frame(std::f64::consts::TAU / 400.0).longitude())]
#[case::frame_1_0_longitude(frame(1.0).longitude())]
fn nearest_longitude_alias_pins_native_ties_and_finite_subtraction_overflow(
    #[case] axis: PeriodicAxis,
) {
    let half = axis.half_period();
    let period = axis.period();
    assert_bits(axis.nearest_longitude_alias(half, 0.0).unwrap(), 0.0);
    assert_bits(axis.nearest_longitude_alias(0.0, half).unwrap(), half);
    assert_bits(axis.nearest_longitude_alias(period, 0.0).unwrap(), period);
    assert_bits(axis.nearest_longitude_alias(-period, 0.0).unwrap(), -period);
    assert_bits(
        axis.nearest_longitude_alias(f64::MAX, -f64::MAX).unwrap(),
        f64::MAX,
    );
}
#[rstest::rstest]
#[case::degrees_longitude(degrees().longitude())]
#[case::frame_std_f64_consts_tau_400_0_longitude(frame(std::f64::consts::TAU / 400.0).longitude())]
#[case::frame_1_0_longitude(frame(1.0).longitude())]
fn huge_finite_longitudes_match_binary_oracle(#[case] axis: PeriodicAxis) {
    let values = [
        f64::MAX,
        -f64::MAX,
        f64::from_bits((2040_u64 << 52) | 0x1234_5678_9ABC),
        f64::from_bits((1800_u64 << 52) | 1),
    ];

    for value in values {
        assert_bits(
            axis.canonicalize(value).unwrap().get(),
            exact_periodic_oracle(value, axis.period()),
        );
    }
}
#[test]
fn shortest_alias_steps_preserve_half_turn_direction() {
    let axis = degrees().longitude();
    let edge: EdgeDirection = axis.shortest_alias_step(170.0, -170.0).unwrap();
    assert_eq!(edge, EdgeDirection::Minor(20.0));
    assert_eq!(edge.delta(axis), 20.0);
    assert_eq!(
        axis.shortest_alias_step(-170.0, 170.0).unwrap(),
        EdgeDirection::Minor(-20.0)
    );
    assert_eq!(
        axis.shortest_alias_step(0.0, 180.0).unwrap(),
        EdgeDirection::HalfTurn(TurnDirection::Positive)
    );
    assert_eq!(
        axis.shortest_alias_step(0.0, -180.0).unwrap(),
        EdgeDirection::HalfTurn(TurnDirection::Negative)
    );
    assert_eq!(
        axis.shortest_alias_step(0.0, 540.0).unwrap(),
        EdgeDirection::HalfTurn(TurnDirection::Positive)
    );
    assert_eq!(
        axis.shortest_alias_step(0.0, -540.0).unwrap(),
        EdgeDirection::HalfTurn(TurnDirection::Negative)
    );

    let grads = frame(std::f64::consts::PI / 200.0).longitude();
    assert_eq!(
        grads.shortest_alias_step(0.0, 400.0).unwrap(),
        EdgeDirection::Minor(0.0)
    );
    assert_eq!(
        grads.shortest_alias_step(0.0, -400.0).unwrap(),
        EdgeDirection::Minor(0.0)
    );
}
#[test]
fn endpoint_intervals_preserve_authentic_endpoints_and_closed_windows() {
    let axis = degrees().longitude();
    let canonical = |longitude| axis.canonicalize(longitude).unwrap();
    let arc = |start, end| CircularInterval::from_endpoints(canonical(start), canonical(end));

    let ordered = arc(-20.0, 10.0);
    assert_eq!(ordered.west_east(), Some((-20.0, 10.0)));
    assert_eq!(
        ordered.windows(axis),
        Some(LongitudeWindows::One([-20.0, 10.0]))
    );
    let wrapped = arc(170.0, -160.0);
    assert_eq!(wrapped.west_east(), Some((170.0, -160.0)));
    assert_eq!(
        wrapped.windows(axis),
        Some(LongitudeWindows::Two([170.0, 180.0], [-180.0, -160.0]))
    );
    assert_eq!(
        arc(170.0, -180.0).windows(axis),
        Some(LongitudeWindows::Two([170.0, 180.0], [-180.0, -180.0]))
    );
    assert_eq!(
        CircularInterval::point(canonical(-180.0)).windows(axis),
        Some(LongitudeWindows::One([-180.0, -180.0]))
    );
    assert_eq!(CircularInterval::Empty.windows(axis), None);
    assert_eq!(
        CircularInterval::Full.windows(axis),
        Some(LongitudeWindows::Full)
    );
    assert_eq!(
        CircularInterval::from_chart_connection(axis.chart_connection(170.0, -170.0).unwrap()),
        arc(170.0, -170.0)
    );
    assert_eq!(
        CircularInterval::from_chart_connection(axis.chart_connection(-170.0, 170.0).unwrap()),
        arc(170.0, -170.0)
    );
    assert_eq!(
        CircularInterval::from_chart_connection(axis.chart_connection(0.0, 360.0).unwrap()),
        CircularInterval::Full
    );

    let start = 0.1_f64;
    let end = 0.2_f64;
    let next_end = end.next_up();
    for endpoint in [end, next_end] {
        let interval = arc(start, endpoint);
        let (actual_start, actual_end) = interval.west_east().unwrap();
        assert_eq!(actual_start.to_bits(), start.to_bits());
        assert_eq!(actual_end.to_bits(), endpoint.to_bits());
    }
}
#[test]
fn extreme_period_windows_and_unions_keep_endpoint_bits_finite() {
    let unit = factor(std::f64::consts::TAU / f64::MAX);
    let axis = PeriodicAxis::try_new("extreme unit", unit).unwrap();
    assert_eq!(axis.period(), f64::MAX);
    let start = axis.half_period().next_down();
    let end = -axis.half_period().next_down();
    let arc = CircularInterval::from_endpoints(
        axis.canonicalize(start).unwrap(),
        axis.canonicalize(end).unwrap(),
    );
    assert_eq!(arc.west_east(), Some((start, end)));
    let windows = arc.windows(axis).unwrap();
    assert_eq!(
        windows,
        LongitudeWindows::Two([start, axis.half_period()], [-axis.half_period(), end],)
    );
    for window in match windows {
        LongitudeWindows::Two(left, right) => [left, right],
        LongitudeWindows::One(window) => [window, window],
        LongitudeWindows::Full => [[-axis.half_period(), axis.half_period()]; 2],
    } {
        assert!(window[0].is_finite() && window[1].is_finite());
    }
    let point = CircularInterval::point(axis.canonicalize(-axis.half_period()).unwrap());
    let first = union_circular_intervals(axis, &[arc, point]);
    let second = union_circular_intervals(axis, &[arc, point]);
    assert_eq!(first, arc);
    assert_eq!(second, arc);
    assert!(first.windows(axis).is_some_and(|windows| match windows {
        LongitudeWindows::One(window) => window.into_iter().all(f64::is_finite),
        LongitudeWindows::Two(left, right) => left.into_iter().chain(right).all(f64::is_finite),
        LongitudeWindows::Full => true,
    }));
}
#[test]
fn largest_gap_union_handles_disconnected_ties_and_full() {
    let axis = degrees().longitude();
    let canonical = |longitude| axis.canonicalize(longitude).unwrap();
    let arc = |start, end| CircularInterval::from_endpoints(canonical(start), canonical(end));
    let point = |longitude| CircularInterval::point(canonical(longitude));
    let disconnected = union_circular_intervals(axis, &[arc(170.0, 175.0), arc(-5.0, 5.0)]);
    assert_eq!(disconnected, arc(-5.0, 175.0));
    let tie = union_circular_intervals(axis, &[
        point(-135.0),
        point(-45.0),
        point(45.0),
        point(135.0),
    ]);
    assert_eq!(tie, arc(-135.0, 135.0));
    assert_eq!(
        union_circular_intervals(axis, &[arc(-180.0, 0.0), arc(0.0, -180.0)]),
        CircularInterval::Full
    );
    assert_eq!(union_circular_intervals(axis, &[]), CircularInterval::Empty);
    assert_eq!(
        union_circular_intervals(axis, &[arc(1.0, 3.0)]),
        arc(1.0, 3.0)
    );
    assert_eq!(
        union_circular_intervals(axis, &[CircularInterval::Full]),
        CircularInterval::Full
    );
}
#[test]
fn largest_gap_union_distinguishes_exact_gaps_that_round_equal() {
    let axis = degrees().longitude();
    let outside = 120.0_f64.next_up();
    let tiny = f64::from_bits(1);
    let lower_gap = (-tiny) - (-outside);
    let upper_gap = outside - (-tiny);
    assert_eq!(lower_gap.to_bits(), upper_gap.to_bits());

    let canonical = |longitude| axis.canonicalize(longitude).unwrap();
    let intervals = [
        CircularInterval::point(canonical(-outside)),
        CircularInterval::point(canonical(-tiny)),
        CircularInterval::point(canonical(outside)),
    ];
    assert_eq!(
        union_circular_intervals(axis, &intervals),
        CircularInterval::from_endpoints(canonical(outside), canonical(-tiny))
    );
}
#[test]
fn seam_transits_preserve_direction_aliases_and_multiple_windings() {
    use crate::geometry::numerics::product::exact_power_of_two;
    let axis = degrees().longitude();
    assert_eq!(axis.seam_transit(170.0, -170.0).unwrap(), 1);
    assert_eq!(axis.seam_transit(-170.0, 170.0).unwrap(), -1);
    assert_eq!(axis.seam_transit(0.0, 360.0).unwrap(), 0);
    assert_eq!(axis.seam_transit(0.0, -360.0).unwrap(), 0);
    assert_eq!(axis.seam_transit(0.0, 180.0).unwrap(), 1);
    assert_eq!(axis.seam_transit(0.0, -180.0).unwrap(), 0);
    assert_eq!(axis.seam_transit(180.0, 0.0).unwrap(), -1);
    assert_eq!(axis.seam_transit(-180.0, 0.0).unwrap(), 0);
    assert_eq!(axis.full_turn_delta(-180.0, 180.0), None);
    assert_eq!(axis.full_turn_delta(180.0, -180.0), None);
    assert_eq!(
        axis.shortest_alias_step(-180.0, 180.0).unwrap(),
        EdgeDirection::Minor(0.0)
    );
    assert_eq!(
        axis.shortest_alias_step(180.0, -180.0).unwrap(),
        EdgeDirection::Minor(0.0)
    );
    assert_eq!(axis.full_turn_delta(0.0, 360.0), Some(360.0));
    assert_eq!(axis.full_turn_delta(360.0, 0.0), Some(-360.0));
    let positive_circle = axis.chart_step(0.0, 360.0).unwrap();
    assert_eq!(positive_circle, ChartStep::Circle(TurnDirection::Positive));
    assert_eq!(positive_circle.transit(axis, 0.0, 360.0).unwrap(), 1);
    let negative_circle = axis.chart_step(360.0, 0.0).unwrap();
    assert_eq!(negative_circle, ChartStep::Circle(TurnDirection::Negative));
    assert_eq!(negative_circle.transit(axis, 360.0, 0.0).unwrap(), -1);
    assert_eq!(axis.full_turn_delta(1.0e-20, 360.0), None);
    assert_eq!(
        axis.chart_step(1.0e-20, 360.0).unwrap(),
        ChartStep::Shortest(EdgeDirection::Minor(-1.0e-20))
    );
    assert_eq!(
        axis.chart_step(180.0_f64.next_down(), -180.0).unwrap(),
        ChartStep::Shortest(EdgeDirection::Minor(exact_power_of_two(-45)))
    );
    assert_eq!(
        axis.chart_step(-180.0, 180.0).unwrap(),
        ChartStep::Shortest(EdgeDirection::Minor(0.0))
    );
    assert_eq!(axis.chart_transit(-180.0, 180.0).unwrap(), 0);
    axis.seam_transit(f64::NAN, 0.0).unwrap_err();
    axis.seam_transit(0.0, f64::INFINITY).unwrap_err();

    let positive = ring(vec![0.0, 120.0, 240.0, 360.0, 480.0, 600.0, 720.0], vec![
        0.0;
        7
    ]);
    let positive_summary =
        RingSummary::try_new(degrees(), &GeographicRing::from_ring(&positive)).unwrap();
    assert_eq!(positive_summary.turn(), RingTurn::Other);
    assert_eq!(positive_summary.chart_turn, 2);
    let negative = ring(
        vec![0.0, -120.0, -240.0, -360.0, -480.0, -600.0, -720.0],
        vec![0.0; 7],
    );
    let negative_summary =
        RingSummary::try_new(degrees(), &GeographicRing::from_ring(&negative)).unwrap();
    assert_eq!(negative_summary.turn(), RingTurn::Other);
    assert_eq!(negative_summary.chart_turn, -2);
}
#[test]
fn chart_connections_keep_canonical_endpoints_and_integer_lifts() {
    let axis = degrees().longitude();
    let eastward = axis.chart_connection(170.0, -170.0).unwrap();
    assert_bits(eastward.start.get(), 170.0);
    assert_bits(eastward.end.get(), -170.0);
    assert_eq!(eastward.source_lift, 1);
    assert_eq!(eastward.source_lift, eastward.seam_transit());
    assert_eq!(
        eastward.step,
        ChartStep::Shortest(EdgeDirection::Minor(20.0))
    );

    let westward = axis.chart_connection(-170.0, 170.0).unwrap();
    assert_bits(westward.start.get(), -170.0);
    assert_bits(westward.end.get(), 170.0);
    assert_eq!(westward.source_lift, -1);
    assert_eq!(westward.source_lift, westward.seam_transit());
    assert_eq!(
        westward.step,
        ChartStep::Shortest(EdgeDirection::Minor(-20.0))
    );

    let circle = axis.chart_connection(0.0, 360.0).unwrap();
    assert_eq!(circle.step, ChartStep::Circle(TurnDirection::Positive));
    assert_eq!(circle.source_lift, 1);
    assert_eq!(circle.source_lift, circle.seam_transit());
    assert_bits(circle.start.get(), 0.0);
    assert_bits(circle.end.get(), 0.0);

    let proper_minor = axis.chart_connection(1.0e-20, 360.0).unwrap();
    assert_eq!(
        proper_minor.step,
        ChartStep::Shortest(EdgeDirection::Minor(-1.0e-20))
    );
    assert_eq!(proper_minor.source_lift, 0);
    assert_eq!(proper_minor.source_lift, proper_minor.seam_transit());
    assert_bits(proper_minor.start.get(), 1.0e-20);
    assert_bits(proper_minor.end.get(), 0.0);

    let positive_half = axis.chart_connection(0.0, 180.0).unwrap();
    assert_eq!(
        positive_half.step,
        ChartStep::Shortest(EdgeDirection::HalfTurn(TurnDirection::Positive))
    );
    assert_eq!(positive_half.source_lift, 1);
    assert_eq!(positive_half.source_lift, positive_half.seam_transit());
    let negative_half = axis.chart_connection(0.0, -180.0).unwrap();
    assert_eq!(
        negative_half.step,
        ChartStep::Shortest(EdgeDirection::HalfTurn(TurnDirection::Negative))
    );
    assert_eq!(negative_half.source_lift, 0);
    assert_eq!(negative_half.source_lift, negative_half.seam_transit());

    let seam_aliases = axis.chart_connection(-180.0, 180.0).unwrap();
    assert_bits(seam_aliases.start.get(), -180.0);
    assert_bits(seam_aliases.end.get(), -180.0);
    assert_eq!(seam_aliases.source_lift, 0);
    assert_eq!(seam_aliases.source_lift, seam_aliases.seam_transit());

    let positive_seam_start = axis.chart_connection(180.0, 0.0).unwrap();
    assert_bits(positive_seam_start.start.get(), -180.0);
    assert_bits(positive_seam_start.end.get(), 0.0);
    assert_eq!(positive_seam_start.source_lift, -1);
    assert_eq!(
        positive_seam_start.source_lift,
        positive_seam_start.seam_transit()
    );
}
#[test]
fn rounded_half_period_span_uses_exact_chart_transit() {
    let axis = degrees().longitude();
    let from = -90.0;
    let to = 90.0_f64.next_up();
    assert_bits(to - from, 180.0);
    assert!(!axis.ordinary_chart_edge(from, to));

    let connection = axis.chart_connection(from, to).unwrap();
    assert_eq!(connection.source_lift, -1);
    assert!(matches!(
        connection.step,
        ChartStep::Shortest(EdgeDirection::Minor(delta)) if delta.is_sign_negative()
    ));
    assert_eq!(connection.seam_transit(), -1);
    assert_eq!(axis.chart_transit(from, to).unwrap(), -1);
}
#[test]
fn seam_transit_summary_accepts_the_polar_cap_without_numeric_winding() {
    let cap = ring(vec![0.1, 90.1, -179.8, -89.7], vec![80.0; 4]);
    let summary = RingSummary::try_new(degrees(), &GeographicRing::from_ring(&cap)).unwrap();
    assert_eq!(summary.turn(), RingTurn::Full(TurnDirection::Positive));
    assert!(!summary.exact_axis_band);
    assert_eq!(
        summary.domain,
        RingDomain::FullLongitude(RingLatitudeSpan::Ordinary)
    );
}
