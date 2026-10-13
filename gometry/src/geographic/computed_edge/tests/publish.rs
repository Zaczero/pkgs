use super::*;

#[test]
fn major_and_period_edges_keep_native_steps_in_degrees_grads_and_radians() {
    let frames = [
        degrees(),
        angular_frame(std::f64::consts::PI / 200.0),
        angular_frame(1.0),
    ];
    for frame in frames {
        let axis = frame.longitude();
        let half = axis.half_period();
        let small = axis.period() / 1000.0;
        for delta in [half + small, -half - small, axis.period() - small] {
            let result = publish_in(
                frame,
                &[Point::new_raw_xy(0.0, 10.0), Point::new_raw_xy(delta, 30.0)],
                false,
                &[delta],
            );
            assert!(result.len() >= 3, "frame={frame:?}, delta={delta}");
            assert_eq!(result.first().unwrap().x, 0.0);
            assert_eq!(
                result.last().unwrap().x,
                axis.canonicalize(delta).unwrap().get(),
                "frame={frame:?}, delta={delta}"
            );
            let mut recovered = 0.0;
            for pair in result.array_windows::<2>() {
                let step = axis.chart_step(pair[0].x, pair[1].x).unwrap();
                assert!(step.delta(axis).abs() <= half);
                recovered += step.delta(axis);
            }
            assert_eq!(recovered, delta, "frame={frame:?}, delta={delta}");
        }

        let full_turn = axis.period();
        let ring = publish_in(
            frame,
            &[Point::new_raw_xy(-half, 0.0), Point::new_raw_xy(half, 0.0)],
            false,
            &[full_turn],
        );
        let mut recovered = 0.0;
        for pair in ring.array_windows::<2>() {
            recovered += axis.chart_step(pair[0].x, pair[1].x).unwrap().delta(axis);
        }
        assert_eq!(recovered, full_turn, "frame={frame:?}");

        let correctly_spelled_half = publish_in(
            frame,
            &[Point::new_raw_xy(0.0, 0.0), Point::new_raw_xy(-half, 1.0)],
            false,
            &[-half],
        );
        assert_eq!(correctly_spelled_half.len(), 2, "frame={frame:?}");
    }
}
#[test]
fn computed_major_period_wrong_half_and_correct_half_edges_keep_their_steps() {
    let positive_major = publish(
        &[
            Point::new_raw_xy(10.0, 20.0),
            Point::new_raw_xy(260.0, 40.0),
        ],
        false,
        &[250.0],
    );
    assert_eq!(positive_major, [
        Point::new_raw_xy(10.0, 20.0),
        Point::new_raw_xy(135.0, 30.0),
        Point::new_raw_xy(-100.0, 40.0),
    ]);

    let wrong_half = publish(
        &[Point::new_raw_xy(0.0, 30.0), Point::new_raw_xy(180.0, 40.0)],
        false,
        &[180.0],
    );
    assert_eq!(wrong_half, [
        Point::new_raw_xy(0.0, 30.0),
        Point::new_raw_xy(90.0, 35.0),
        Point::new_raw_xy(-180.0, 40.0),
    ]);

    let correct_half = publish(
        &[
            Point::new_raw_xy(0.0, 30.0),
            Point::new_raw_xy(-180.0, 40.0),
        ],
        false,
        &[-180.0],
    );
    assert_eq!(correct_half, [
        Point::new_raw_xy(0.0, 30.0),
        Point::new_raw_xy(-180.0, 40.0),
    ]);
}
#[test]
fn major_half_and_near_period_edges_use_their_full_intended_steps() {
    for (end, delta, midpoint) in [
        (187.0, 187.0, 93.5),
        (-187.0, -187.0, -93.5),
        (353.0, 353.0, 176.5),
        (-353.0, -353.0, -176.5),
    ] {
        let result = publish(
            &[Point::new_raw_xy(0.0, 4.0), Point::new_raw_xy(end, 8.0)],
            false,
            &[delta],
        );
        assert_eq!(result, [
            Point::new_raw_xy(0.0, 4.0),
            Point::new_raw_xy(midpoint, 6.0),
            Point::new_raw_xy(degrees().longitude().canonicalize(end).unwrap().get(), 8.0,),
        ]);
    }
}
#[test]
fn computed_period_edges_include_a_seam_anchored_turn() {
    for (start, end, delta) in [
        (10.0, 370.0, 360.0),
        (370.0, 10.0, -360.0),
        (10.0, 730.0, 720.0),
        (10.0, -710.0, -720.0),
        (-180.0, 180.0, 360.0),
    ] {
        let result = publish(
            &[Point::new_raw_xy(start, 0.0), Point::new_raw_xy(end, 0.0)],
            false,
            &[delta],
        );
        let mut turn = 0_i64;
        let mut recovered_delta = 0.0;
        for pair in result.array_windows::<2>() {
            let step = degrees()
                .longitude()
                .chart_step(pair[0].x, pair[1].x)
                .unwrap();
            let step_delta = step.delta(degrees().longitude());
            assert!(step_delta.abs() <= degrees().longitude().half_period());
            recovered_delta += step_delta;
            turn = degrees()
                .longitude()
                .add_chart_transit(turn, pair[0].x, pair[1].x)
                .unwrap();
        }
        assert_eq!(recovered_delta, delta);
        assert_eq!(turn, (delta / 360.0) as i64);
    }
    assert_eq!(
        publish(
            &[
                Point::new_raw_xy(-180.0, 0.0),
                Point::new_raw_xy(180.0, 0.0)
            ],
            false,
            &[360.0],
        ),
        [
            Point::new_raw_xy(-180.0, 0.0),
            Point::new_raw_xy(-90.0, 0.0),
            Point::new_raw_xy(0.0, 0.0),
            Point::new_raw_xy(90.0, 0.0),
            Point::new_raw_xy(-180.0, 0.0),
        ]
    );
}
#[test]
fn stored_chart_circle_step_subdivides_before_canonical_publication() {
    let points = [Point::new_raw_xy(10.0, 0.0), Point::new_raw_xy(370.0, 0.0)];
    let mut budget = ExpansionBudget::new("test", "stored computed edge");
    let output = publish_computed_chain(
        degrees(),
        points.as_slice(),
        false,
        &mut budget,
        |_, _, _| {
            ComputedEdgeIntent::retained_chart_witness(
                degrees(),
                [points[0].x, points[1].x],
                ChartStep::Circle(TurnDirection::Positive),
            )
        },
    )
    .unwrap()
    .unwrap();
    assert_eq!(output, [
        Point::new_raw_xy(10.0, 0.0),
        Point::new_raw_xy(100.0, 0.0),
        Point::new_raw_xy(-170.0, 0.0),
        Point::new_raw_xy(-80.0, 0.0),
        Point::new_raw_xy(10.0, 0.0),
    ]);
}
#[test]
fn subdivided_full_turn_edges_publish_only_minor_pieces() {
    for latitude in [-90.0, 10.0, 90.0] {
        for (start, end, delta) in [
            (-180.0, 180.0, 360.0),
            (180.0, -180.0, -360.0),
            (-180.0, 540.0, 720.0),
            (180.0, -540.0, -720.0),
        ] {
            let points = [
                Point::new_raw_xy(start, latitude),
                Point::new_raw_xy(end, latitude),
            ];
            let output = publish(&points, false, &[delta]);
            let mut recovered = 0.0;
            for [start, end] in output.array_windows::<2>() {
                let axis = degrees().longitude();
                let delta = axis.chart_step(start.x, end.x).unwrap().delta(axis);
                assert!(delta.abs() < 180.0);
                recovered += delta;
            }
            assert_eq!(recovered, delta);
        }
    }
}
#[test]
fn computed_ring_keeps_its_start_and_copies_full_closure() {
    let start = Point::new_raw_axes(10.0, 0.0, ZOrdinate(Some(3.0)), MOrdinate(Some(11.0)));
    let east = Point::new_raw_axes(260.0, 0.0, ZOrdinate(Some(19.0)), MOrdinate(Some(43.0)));
    let north_east = Point::new_raw_axes(260.0, 10.0, ZOrdinate(Some(23.0)), MOrdinate(Some(47.0)));
    let north_west = Point::new_raw_axes(10.0, 10.0, ZOrdinate(Some(7.0)), MOrdinate(Some(13.0)));
    let ring_points = [start, east, north_east, north_west, start];
    let result = publish(&ring_points, true, &[250.0, 0.0, -250.0, 0.0]);

    assert_eq!(result[0], start);
    assert_eq!(result, [
        start,
        Point::new_raw_axes(135.0, 0.0, ZOrdinate(Some(11.0)), MOrdinate(Some(27.0)),),
        Point::new_raw_axes(-100.0, 0.0, ZOrdinate(Some(19.0)), MOrdinate(Some(43.0)),),
        Point::new_raw_axes(-100.0, 10.0, ZOrdinate(Some(23.0)), MOrdinate(Some(47.0)),),
        Point::new_raw_axes(135.0, 10.0, ZOrdinate(Some(15.0)), MOrdinate(Some(30.0)),),
        north_west,
        start,
    ]);
    assert_eq!(result.first(), result.last());
}
#[test]
fn positive_half_period_closing_cap_is_not_rotated_or_split() {
    let ring = [
        Point::new_raw_xy(0.0, 70.0),
        Point::new_raw_xy(90.0, 65.0),
        Point::new_raw_xy(180.0, 70.0),
        Point::new_raw_xy(0.0, 70.0),
    ];
    let frame = degrees();
    let mut budget = ExpansionBudget::new("positive half cap", "computed edges");
    let output = publish_computed_chain(
        frame,
        ring.as_slice(),
        true,
        &mut budget,
        |index, start, end| {
            if index == 2 {
                ComputedEdgeIntent::retained_chart_witness(
                    frame,
                    [start.x, end.x],
                    ChartStep::Shortest(EdgeDirection::HalfTurn(TurnDirection::Positive)),
                )
            } else {
                ComputedEdgeIntent::retained_binary64_pair(frame.longitude(), [start.x, end.x])
            }
        },
    )
    .unwrap()
    .unwrap();
    assert_eq!(output, [
        ring[0],
        ring[1],
        Point::new_raw_xy(-180.0, 70.0),
        ring[0],
    ]);
}
#[test]
fn certified_minor_edges_canonicalize_without_subdivision() {
    let points = [
        Point::new_raw_xy(170.0, 0.0),
        Point::new_raw_xy(180.0, 1.0),
        Point::new_raw_xy(190.0, 0.0),
    ];
    let mut budget = ExpansionBudget::new("test", "certified minor");
    let output = publish_computed_chain(
        degrees(),
        points.as_slice(),
        false,
        &mut budget,
        |_, _, _| Ok(ComputedEdgeIntent::CertifiedMinor),
    )
    .unwrap()
    .unwrap();
    assert_eq!(output, [
        Point::new_raw_xy(170.0, 0.0),
        Point::new_raw_xy(-180.0, 1.0),
        Point::new_raw_xy(-170.0, 0.0),
    ]);
}
#[test]
fn minor_chain_certificate_preserves_spelling_and_complete_closure() {
    let first = Point::new_raw_axes(10.0, 20.0, ZOrdinate(Some(3.0)), MOrdinate(Some(11.0)));
    let middle = Point::new_raw_axes(20.0, 30.0, ZOrdinate(Some(7.0)), MOrdinate(Some(19.0)));
    let last = Point::new_raw_axes(10.0, 20.0, ZOrdinate(Some(13.0)), MOrdinate(Some(23.0)));
    let coordinates = crate::geometry::CoordSeq::try_from_points(&[first, middle, last]).unwrap();
    let mut budget = ExpansionBudget::new("test", "computed edges");
    let output = super::publish_certified_minor_chain(degrees(), &coordinates, true, &mut budget)
        .unwrap()
        .unwrap();
    assert_eq!(output, [first, middle, first]);

    for (start, expected) in [(-0.0, 0.0), (180.0, -180.0)] {
        let coordinates = crate::geometry::CoordSeq::try_from_points(&[
            Point::new_raw_xy(start, 20.0),
            Point::new_raw_xy(90.0, 30.0),
        ])
        .unwrap();
        let output =
            super::publish_certified_minor_chain(degrees(), &coordinates, false, &mut budget)
                .unwrap()
                .unwrap();
        assert_eq!(output, [
            Point::new_raw_xy(expected, 20.0),
            Point::new_raw_xy(90.0, 30.0),
        ]);
    }
}
