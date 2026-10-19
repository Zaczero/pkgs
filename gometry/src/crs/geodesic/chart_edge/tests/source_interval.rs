#[test]
fn retained_producer_branches_keep_long_half_and_multiple_turn_intent() {
    let axis = degrees().longitude();
    let cases = [
        ("positive major", 0.0, 250.0, 1, 0.5, 125.0),
        ("positive half", 0.0, 180.0, 1, 0.25, 45.0),
        (
            "positive half opposite spelling",
            0.0,
            -180.0,
            1,
            0.25,
            45.0,
        ),
        ("negative half", 0.0, -180.0, 0, 0.25, -45.0),
        ("positive two turns", 0.0, 720.0, 2, 0.25, 180.0),
        ("negative two turns", 0.0, -720.0, -2, 0.25, -180.0),
    ];
    for (name, start_x, end_x, periods, parameter, expected_x) in cases {
        let longitude = axis
            .retained_native_endpoints(start_x, end_x, periods)
            .unwrap();
        let edge = ChartEdge::from_retained(
            Point::new_raw_xy(start_x, 0.0),
            Point::new_raw_xy(end_x, 40.0),
            degrees(),
            longitude,
        )
        .unwrap();
        let point = edge.chart_position(parameter).unwrap();
        let source_point = edge
            .source_sheet_point(
                Point::new_raw_xy(start_x, 0.0),
                Point::new_raw_xy(end_x, 40.0),
                parameter,
                point,
            )
            .unwrap();
        assert!(
            (point.x - expected_x).abs() <= 2.0e-14,
            "{name}: actual={}, expected={expected_x}",
            point.x
        );
        assert!(
            (source_point.x - expected_x).abs() <= 2.0e-14,
            "{name}: source sheet={}, expected={expected_x}",
            source_point.x
        );
        let mut chain = Vec::new();
        edge.publish_source_interval(
            Point::new_raw_xy(start_x, 0.0),
            Point::new_raw_xy(end_x, 40.0),
            0.0,
            1.0,
            |parameter| edge.chart_position(parameter),
            |_, point, _| {
                chain.push(point.x);
                Ok(())
            },
            "line_substring",
        )
        .unwrap();
        let expected_chain: &[f64] = match name {
            "positive major" => &[0.0, 125.0, 250.0],
            "positive half" => &[0.0, 90.0, 180.0],
            "positive half opposite spelling" => &[0.0, 90.0, -180.0],
            "negative half" => &[0.0, -90.0, -180.0],
            "positive two turns" => &[0.0, 90.0, 180.0, 270.0, 360.0, 450.0, 540.0, 630.0, 720.0],
            "negative two turns" => &[
                0.0, -90.0, -180.0, -270.0, -360.0, -450.0, -540.0, -630.0, -720.0,
            ],
            _ => unreachable!("the branch table names every retained edge"),
        };
        assert_eq!(chain, expected_chain, "{name}: published branch chain");
    }

    let positive_half = axis.retained_native_endpoints(0.0, -180.0, 1).unwrap();
    assert_eq!(positive_half.periods(), 1);
    let negative_half = axis.retained_native_endpoints(0.0, -180.0, 0).unwrap();
    assert_eq!(negative_half.periods(), 0);
}

#[test]
fn retained_source_connection_preserves_relative_lift_and_public_half_turn_policy() {
    let frame = degrees();
    let axis = frame.longitude();
    let connection = axis.chart_connection(530.0, 190.0).unwrap();
    assert_eq!(connection.source_lift, 1);
    assert_eq!(connection.step.delta(axis), 20.0);
    let edge = ChartEdge::from_retained(
        Point::new_raw_xy(530.0, 10.0),
        Point::new_raw_xy(190.0, 20.0),
        frame,
        connection.retained(),
    )
    .unwrap();
    assert_eq!(edge.chart_position(0.5).unwrap().x, 180.0);
    assert_eq!(edge.chart_position(1.0).unwrap().x, 190.0);
    drop(
        ChartEdge::new(
            Point::new_raw_xy(0.0, 10.0),
            Point::new_raw_xy(180.0, 20.0),
            frame,
        )
        .unwrap_err(),
    );
}

#[rstest::rstest]
#[case::degrees("degrees", degrees(), 360.0, 10.0, 2.0)]
#[case::radians("radians", radians(), std::f64::consts::TAU, 0.0, 0.5)]
#[case::proj_radian_crs(
    "PROJ radian CRS",
    {
        let state = crate::crs::runtime_state_snapshot().unwrap();
        geographic_metric_frame_at(
    "GEOGCS[\"WGS 84 rad\",DATUM[\"WGS_1984\",SPHEROID[\"WGS 84\",6378137,298.257223563]],PRIMEM[\"Greenwich\",0],UNIT[\"radian\",1],AXIS[\"Longitude\",EAST],AXIS[\"Latitude\",NORTH]]",
    &state,
)
.unwrap()
    },
    std::f64::consts::TAU,
    0.0,
    0.5
)]
#[case::epsg_4807_grad_crs(
    "EPSG:4807 grad CRS",
    {
        let state = crate::crs::runtime_state_snapshot().unwrap();
        geographic_metric_frame_at("EPSG:4807", &state).unwrap()
    },
    400.0,
    5.0,
    2.0
)]
#[case::west_radians(
    "west radians",
    {
        let radian_unit = AngularFactor::try_new("radian", 1.0).unwrap();
        GeographicFrame::try_new_with_orientation(
            radian_unit,
            radian_unit,
            0.0,
            GeographicAxisOrientation::from_axis_directions(false, true),
        )
        .unwrap()
    },
    std::f64::consts::TAU,
    0.0,
    0.5
)]
#[case::south_grads(
    "south grads",
    {
        let grad_unit = AngularFactor::try_new("grad", std::f64::consts::TAU / 400.0).unwrap();
        GeographicFrame::try_new_with_orientation(
            grad_unit,
            grad_unit,
            0.0,
            GeographicAxisOrientation::from_axis_directions(true, false),
        )
        .unwrap()
    },
    400.0,
    5.0,
    2.0
)]
#[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
fn source_sheet_publication_uses_rust_admitted_frame_periods_and_branches(
    #[case] name: &str,
    #[case] frame: GeographicFrame,
    #[case] expected_period: f64,
    #[case] base: f64,
    #[case] step: f64,
) {
    let axis = frame.longitude();
    assert_eq!(
        axis.period().to_bits(),
        expected_period.to_bits(),
        "{name} frame period"
    );
    for turns in [-2.0, -1.0, 1.0, 2.0] {
        let source_start = base + turns * axis.period();
        let source_end = source_start + step;
        let start = Point::new_raw_xy(source_start, 0.0);
        let end = Point::new_raw_xy(source_end, 0.0);
        let admitted = axis.chart_connection(source_start, source_end).unwrap();
        let edge = ChartEdge::new(start, end, frame).unwrap();
        assert_eq!(
            edge.retained_longitude().endpoints(),
            &[admitted.start, admitted.end],
            "{name}, turns={turns}: admitted canonical endpoints"
        );
        assert_eq!(
            edge.retained_longitude().periods(),
            i64::from(admitted.source_lift),
            "{name}, turns={turns}: admitted relative lift"
        );
        let canonical = edge.chart_position(0.5).unwrap();
        let published = edge.source_sheet_point(start, end, 0.5, canonical).unwrap();
        let expected = source_start + step / 2.0;
        assert_eq!(
            published.x.to_bits(),
            expected.to_bits(),
            "{name}, turns={turns}"
        );
    }
}

#[test]
fn source_interval_readback_accepts_correctly_rounded_endpoint_differences() {
    let start = Point::new_raw_xy(10.0, 60.0);
    let end = Point::new_raw_xy(12.0, 60.0);
    let edge = ChartEdge::new(start, end, degrees()).unwrap();
    let from = 0.25;
    let to = 0.75_f64.next_down();
    let from_point = edge
        .source_sheet_point(start, end, from, edge.chart_position(from).unwrap())
        .unwrap();
    let to_point = edge
        .source_sheet_point(start, end, to, edge.chart_position(to).unwrap())
        .unwrap();

    assert_eq!(from_point.x, 10.5);
    assert_eq!(to_point.x, 11.5);
    assert_eq!((to_point.x - from_point.x).to_bits(), 1.0_f64.to_bits());
    let exact_interval_delta = 2.0 * (to - from);
    assert_eq!(exact_interval_delta.to_bits(), 0x3FEF_FFFF_FFFF_FFFE);
    assert_ne!(
        (to_point.x - from_point.x).to_bits(),
        exact_interval_delta.to_bits()
    );
    assert!(
        edge.source_interval_reads_correctly(from, to, from_point, to_point)
            .unwrap()
    );
}

#[rstest::rstest]
#[case::increasing_latitude("increasing latitude", 10.0_f64.next_up())]
#[case::decreasing_latitude("decreasing latitude", 10.0_f64.next_down())]
fn source_interval_readback_accepts_monotone_latitude_quantization_with_longitude_motion(
    #[case] direction: &'static str,
    #[case] end_latitude: f64,
) {
    let start = Point::new_raw_xy(0.0, 10.0);
    let end = Point::new_raw_xy(10.0, end_latitude);
    let edge = ChartEdge::new(start, end, degrees()).unwrap();
    let from = 0.125;
    let to = 0.25;
    let from_point = edge
        .source_sheet_point(start, end, from, edge.chart_position(from).unwrap())
        .unwrap();
    let to_point = edge
        .source_sheet_point(start, end, to, edge.chart_position(to).unwrap())
        .unwrap();

    assert_eq!(
        (from_point.x, from_point.y, to_point.x, to_point.y),
        (1.25, 10.0, 2.5, 10.0),
        "{direction}"
    );
    assert!(
        edge.source_interval_reads_correctly(from, to, from_point, to_point)
            .unwrap(),
        "{direction}: exact latitude movement may round monotonically to equality"
    );
}

#[test]
fn source_interval_readback_rejects_a_nonzero_full_xy_collapse() {
    let start = Point::new_raw_xy(0.0, 10.0);
    let end = Point::new_raw_xy(0.0, 12.0);
    let edge = ChartEdge::new(start, end, degrees()).unwrap();
    let from = 0.5_f64;
    let to = from.next_up();
    let from_point = edge
        .source_sheet_point(start, end, from, edge.chart_position(from).unwrap())
        .unwrap();
    let to_point = edge
        .source_sheet_point(start, end, to, edge.chart_position(to).unwrap())
        .unwrap();

    // The exact sites are 11 and 11 + 2^-52. The latter is below half an
    // ULP at 11 and rounds to 11, so both emitted XY endpoints are (0, 11)
    // despite the nonzero source interval.
    assert_eq!((from_point.x, from_point.y), (0.0, 11.0));
    assert_eq!((to_point.x, to_point.y), (0.0, 11.0));
    assert!(
        edge.source_interval_reads_correctly(from, from, from_point, from_point)
            .unwrap(),
        "an exact zero-width interval remains a readable point"
    );
    assert!(
        !edge
            .source_interval_reads_correctly(from, to, from_point, to_point)
            .unwrap(),
        "a nonzero native-latitude interval cannot read back as one point"
    );

    let error = edge
        .publish_source_interval(
            start,
            end,
            from,
            to,
            |parameter| edge.chart_position(parameter),
            |_, _, _| Ok(()),
            "line_substring",
        )
        .unwrap_err();
    assert!(matches!(
        error.kind(),
        crate::error::ErrorKind::Geometry(GeometryErrorKind::UnrepresentableOutput {
            operation: "line_substring",
            ..
        })
    ));
}

#[test]
fn source_interval_readback_keeps_pole_degeneracy_distinct_from_rounded_motion() {
    let from = 0.5_f64;
    let to = from.next_up();
    let pole_start = Point::new_raw_xy(10.0, 90.0);
    let pole_end = Point::new_raw_xy(11.0, 90.0);
    let pole = ChartEdge::new(pole_start, pole_end, degrees()).unwrap();
    let pole_from = pole
        .source_sheet_point(
            pole_start,
            pole_end,
            from,
            pole.chart_position(from).unwrap(),
        )
        .unwrap();
    let pole_to = pole
        .source_sheet_point(pole_start, pole_end, to, pole.chart_position(to).unwrap())
        .unwrap();
    assert_eq!((pole_from.x, pole_from.y), (pole_to.x, pole_to.y));
    assert!(
        pole.source_interval_reads_correctly(from, to, pole_from, pole_to)
            .unwrap(),
        "a retained interval at one typed pole is physically constant"
    );

    let near_pole_end = Point::new_raw_xy(11.0, 89.5);
    let near_pole = ChartEdge::new(pole_start, near_pole_end, degrees()).unwrap();
    let near_from = near_pole
        .source_sheet_point(
            pole_start,
            near_pole_end,
            from,
            near_pole.chart_position(from).unwrap(),
        )
        .unwrap();
    let near_to = near_pole
        .source_sheet_point(
            pole_start,
            near_pole_end,
            to,
            near_pole.chart_position(to).unwrap(),
        )
        .unwrap();
    assert_eq!((near_from.x, near_from.y), (near_to.x, near_to.y));
    assert!(
        !near_pole
            .source_interval_reads_correctly(from, to, near_from, near_to)
            .unwrap(),
        "near-pole metric smallness does not make a nonzero interval constant"
    );
}

#[test]
fn source_sheet_publication_keeps_readable_circles_and_rejects_collapsed_intervals() {
    use crate::geometry::numerics::product::exact_power_of_two;
    let meridian_start = Point::new_raw_xy(370.0, -30.0);
    let meridian_end = Point::new_raw_xy(370.0, 70.0);
    let meridian = ChartEdge::new(meridian_start, meridian_end, degrees()).unwrap();
    assert_eq!(
        meridian
            .source_interval_initial_piece_count(0.25, 0.75)
            .unwrap(),
        1
    );
    let meridian_from = meridian
        .source_sheet_point(
            meridian_start,
            meridian_end,
            0.25,
            meridian.chart_position(0.25).unwrap(),
        )
        .unwrap();
    let meridian_to = meridian
        .source_sheet_point(
            meridian_start,
            meridian_end,
            0.75,
            meridian.chart_position(0.75).unwrap(),
        )
        .unwrap();
    assert_eq!(meridian_from.x.to_bits(), 370.0_f64.to_bits());
    assert_eq!(meridian_to.x.to_bits(), 370.0_f64.to_bits());
    assert!(
        meridian
            .source_interval_reads_correctly(0.25, 0.75, meridian_from, meridian_to)
            .unwrap()
    );

    let circle_start = Point::new_raw_xy(10.0, 0.0);
    let circle_end = Point::new_raw_xy(370.0, 0.0);
    let circle = ChartEdge::new(circle_start, circle_end, degrees()).unwrap();
    assert_eq!(circle.retained_longitude().periods(), 1);
    assert_eq!(
        circle
            .source_interval_initial_piece_count(0.0, 1.0)
            .unwrap(),
        2
    );
    assert!(
        circle
            .source_interval_reads_correctly(0.0, 1.0, circle_start, circle_end)
            .unwrap()
    );
    let mut authentic_circle = Vec::new();
    circle
        .publish_source_interval(
            circle_start,
            circle_end,
            0.0,
            1.0,
            |parameter| circle.chart_position(parameter),
            |parameter, point, refined| {
                authentic_circle.push((parameter, point.x, refined));
                Ok(())
            },
            "line_substring",
        )
        .unwrap();
    assert_eq!(authentic_circle, [(0.0, 10.0, false), (1.0, 370.0, false)]);
    let negative_circle_end = Point::new_raw_xy(-350.0, 0.0);
    let negative_circle = ChartEdge::new(circle_start, negative_circle_end, degrees()).unwrap();
    assert_eq!(negative_circle.retained_longitude().periods(), -1);
    let mut authentic_negative_circle = Vec::new();
    negative_circle
        .publish_source_interval(
            circle_start,
            negative_circle_end,
            0.0,
            1.0,
            |parameter| negative_circle.chart_position(parameter),
            |parameter, point, refined| {
                authentic_negative_circle.push((parameter, point.x, refined));
                Ok(())
            },
            "line_substring",
        )
        .unwrap();
    assert_eq!(authentic_negative_circle, [
        (0.0, 10.0, false),
        (1.0, -350.0, false)
    ]);
    assert_eq!(
        circle
            .source_interval_initial_piece_count(0.25, 1.0)
            .unwrap(),
        2
    );
    let canonical = circle.chart_position(0.25).unwrap();
    let published = circle
        .source_sheet_point(circle_start, circle_end, 0.25, canonical)
        .unwrap();
    assert_eq!(published.x, 100.0);
    let retained_circle = ChartEdge::from_retained(
        circle_start,
        circle_start,
        degrees(),
        degrees()
            .longitude()
            .retained_native_endpoints(10.0, 10.0, 1)
            .unwrap(),
    )
    .unwrap();
    let retained_midpoint = retained_circle
        .source_sheet_point(
            circle_start,
            circle_start,
            0.5,
            retained_circle.chart_position(0.5).unwrap(),
        )
        .unwrap();
    assert_eq!(retained_midpoint.x, 190.0);
    assert!(
        !retained_circle
            .source_interval_reads_correctly(0.0, 1.0, circle_start, circle_start)
            .unwrap(),
        "equal raw endpoints do not read an explicit retained full turn"
    );
    assert!(
        !retained_circle
            .source_interval_reads_correctly(0.0, 0.5, circle_start, retained_midpoint)
            .unwrap(),
        "a generated exact half-turn must be refined before publication"
    );
    assert!(
        !retained_circle
            .source_interval_reads_correctly(0.5, 1.0, retained_midpoint, circle_start)
            .unwrap(),
        "the authentic final spelling selects the opposite half-turn"
    );
    let mut expanded_circle = Vec::new();
    retained_circle
        .publish_source_interval(
            circle_start,
            circle_start,
            0.0,
            1.0,
            |parameter| retained_circle.chart_position(parameter),
            |parameter, point, refined| {
                expanded_circle.push((parameter, point.x, refined));
                Ok(())
            },
            "line_substring",
        )
        .unwrap();
    assert_eq!(expanded_circle, [
        (0.0, 10.0, false),
        (0.25, 100.0, true),
        (0.5, 190.0, true),
        (0.75, 280.0, true),
        (1.0, 10.0, false),
    ]);
    let retained_negative_circle = ChartEdge::from_retained(
        circle_start,
        circle_start,
        degrees(),
        degrees()
            .longitude()
            .retained_native_endpoints(10.0, 10.0, -1)
            .unwrap(),
    )
    .unwrap();
    let mut expanded_negative_circle = Vec::new();
    retained_negative_circle
        .publish_source_interval(
            circle_start,
            circle_start,
            0.0,
            1.0,
            |parameter| retained_negative_circle.chart_position(parameter),
            |parameter, point, refined| {
                expanded_negative_circle.push((parameter, point.x, refined));
                Ok(())
            },
            "line_substring",
        )
        .unwrap();
    assert_eq!(expanded_negative_circle, [
        (0.0, 10.0, false),
        (0.25, -80.0, true),
        (0.5, -170.0, true),
        (0.75, -260.0, true),
        (1.0, 10.0, false),
    ]);

    let large_start = Point::new_raw_xy(360.0 * exact_power_of_two(44), 0.0);
    let large_end = Point::new_raw_xy(large_start.x + 5.0, 0.0);
    let large_edge = ChartEdge::new(large_start, large_end, degrees()).unwrap();
    let large_site = retained_source_sheet_site(
        large_start.x,
        large_edge.retained_longitude(),
        degrees().longitude(),
        0.1,
    )
    .unwrap();
    assert_eq!(
        large_site.to_bits(),
        (large_start.x + 1.0).to_bits(),
        "one rounding follows the exact value just above the half-ULP tie"
    );
    let canonical = large_edge.chart_position(0.1).unwrap();
    assert_eq!(canonical.x, 0.5);
    let published = large_edge
        .source_sheet_point(large_start, large_end, 0.1, canonical)
        .unwrap();
    assert_eq!(published.x.to_bits(), large_site.to_bits());
    assert_ne!(
        published.x.to_bits(),
        (large_start.x + canonical.x).to_bits(),
        "single rounding differs from translating a rounded canonical site"
    );

    let huge_start = Point::new_raw_xy(1.0e22, 0.0);
    let huge_end = Point::new_raw_xy(1.0e22_f64.next_up(), 0.0);
    let huge = ChartEdge::new(huge_start, huge_end, degrees()).unwrap();
    let source_sheet = retained_source_sheet_site(
        huge_start.x,
        huge.retained_longitude(),
        degrees().longitude(),
        0.5,
    )
    .unwrap();
    assert_eq!(source_sheet.to_bits(), huge_start.x.to_bits());
    let _ = retained_source_interval_initial_piece_count(
        huge.retained_longitude(),
        degrees().longitude(),
        0.0,
        0.5,
    )
    .unwrap();
    let canonical = huge.chart_position(0.5).unwrap();
    let collapsed = huge
        .source_sheet_point(huge_start, huge_end, 0.5, canonical)
        .unwrap();
    assert_eq!(collapsed.x.to_bits(), huge_start.x.to_bits());
    assert_eq!(
        degrees()
            .longitude()
            .canonicalize(canonical.x)
            .unwrap()
            .get(),
        -4.0
    );
    assert!(
        !huge
            .source_interval_reads_correctly(0.0, 0.5, huge_start, collapsed)
            .unwrap(),
        "the nonzero first-half source interval collapsed to one longitude"
    );
    let error = huge
        .publish_source_interval(
            huge_start,
            huge_end,
            0.0,
            0.5,
            |parameter| huge.chart_position(parameter),
            |_, _, _| Ok(()),
            "line_substring",
        )
        .unwrap_err();
    assert!(matches!(
        error.kind(),
        crate::error::ErrorKind::Geometry(GeometryErrorKind::UnrepresentableOutput {
            operation: "line_substring",
            ..
        })
    ));
}
