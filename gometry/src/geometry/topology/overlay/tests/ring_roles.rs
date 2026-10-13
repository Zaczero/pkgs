#[test]
fn q_ring_role_rejects_the_globe_complement_of_a_full_turn_cap() -> Result<()> {
    let frame = GeographicFrame::crs84();
    let exact_ports = [(0.0, 10.0), (120.0, 10.0), (240.0, 10.0), (0.0, 10.0)].map(|(x, y)| {
        super::super::exact::ExactChartPoint::from_rationals(
            ExactRational::from_f64(x),
            ExactRational::from_f64(y),
        )
    });
    let complement = Ring::try_from_closed(CoordSeq::try_from_points(&[
        Point::new_raw_xy(0.0, 10.0),
        Point::new_raw_xy(240.0, 10.0),
        Point::new_raw_xy(120.0, 10.0),
        Point::new_raw_xy(0.0, 10.0),
    ])?)?;

    let role = compare_q_ring_role(frame, &complement, &complement, &exact_ports, true)?
        .expect("both polar chart roles are classifiable");
    assert!(!role.exact_is_hole, "the exact orientation selects the cap");
    assert!(
        role.emitted_is_hole,
        "reversal selects its globe complement"
    );
    assert!(!role.matches(), "QC-R rejects the complementary role");
    Ok(())
}

#[test]
fn q_source_axis_band_emission_checks_the_exact_ring_role() {
    let source = Shape::Polygon(Polygon::new(
        Ring::try_from_closed(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(-180.0, 10.0),
                Point::new_raw_xy(-90.0, 10.0),
                Point::new_raw_xy(0.0, 10.0),
                Point::new_raw_xy(90.0, 10.0),
                Point::new_raw_xy(180.0, 10.0),
                Point::new_raw_xy(180.0, 20.0),
                Point::new_raw_xy(90.0, 20.0),
                Point::new_raw_xy(0.0, 20.0),
                Point::new_raw_xy(-90.0, 20.0),
                Point::new_raw_xy(-180.0, 20.0),
                Point::new_raw_xy(-180.0, 10.0),
            ])
            .expect("build the full-longitude source band"),
        )
        .expect("close the full-longitude source band"),
        Vec::new(),
    ));
    let topology = test_topology(std::slice::from_ref(&source));
    let operation = OverlayOp::Union;
    let plan = build_q_plan(&topology, operation).expect("build Q plan");
    assert!(!plan.whole_sphere);
    let PolygonPlan::Regions { rings, .. } = &plan.polygons else {
        panic!("the axis band has a bounded selected region");
    };
    assert!(
        rings
            .iter()
            .any(|ring| matches!(ring, PlannedAreaRing::Source { .. })),
        "the plan keeps the source axis-band ring"
    );

    let source_points = source_point_coordinates(&topology).expect("source points");
    let nearest = QCoordinates::build(&topology).expect("nearest Q coordinates");
    let embedding =
        resolve_q_embedding_xy(&topology, &plan, &source_points, &nearest, operation.name())
            .expect("source axis-band ring is representable");
    assert_eq!(embedding.choice, QEmbeddingChoice::Nearest);
    let result = publish_q_overlay(embedding).expect("publish the source axis band");
    assert_eq!(
        crate::io::to_wkb(&result, None, false).expect("serialize Q result"),
        crate::io::to_wkb(&source, None, false).expect("serialize source band")
    );
}

#[test]
fn q_ring_cycles_keep_the_lifted_alias_after_two_native_periods() {
    let grads_unit = AngularFactor::try_new("grad", std::f64::consts::TAU / 400.0).unwrap();
    let radians_unit = AngularFactor::try_new("radian", 1.0).unwrap();
    let cases: [(&str, GeographicFrame, &[f64], f64, f64); 3] = [
        (
            "degree",
            GeographicFrame::crs84(),
            [
                122.427_194,
                -177.572_806,
                -117.572_806,
                -57.572_806,
                2.427_194,
                62.427_194,
            ]
            .as_slice(),
            842.427_194,
            -597.572_806,
        ),
        (
            "grad",
            GeographicFrame::try_new(grads_unit, grads_unit, 0.0).unwrap(),
            [
                25.0,
                75.0,
                124.999_999_999_999_99,
                174.999_999_999_999_97,
                -174.999_999_999_999_97,
                -124.999_999_999_999_97,
                -74.999_999_999_999_99,
                -24.999_999_999_999_993,
            ]
            .as_slice(),
            825.0,
            -775.0,
        ),
        (
            "radian",
            GeographicFrame::try_new(radians_unit, radians_unit, 0.0).unwrap(),
            [
                0.25,
                1.035_398_163_397_448_3,
                1.820_796_326_794_896_6,
                2.606_194_490_192_345,
                -2.891_592_653_589_793,
                -2.106_194_490_192_345,
                -1.320_796_326_794_896_6,
                -0.535_398_163_397_448_3,
            ]
            .as_slice(),
            12.816_370_614_359_172,
            -12.316_370_614_359_172,
        ),
    ];

    for (name, frame, lap, positive, negative) in cases {
        for (direction, expected) in [(1.0, positive), (-1.0, negative)] {
            let path = if direction > 0.0 {
                lap.to_vec()
            } else {
                std::iter::once(lap[0])
                    .chain(lap[1..].iter().rev().copied())
                    .collect()
            };
            let mut coordinates = path
                .iter()
                .map(|&longitude| (longitude, 0.1))
                .collect::<Vec<_>>();
            let repeated_start = coordinates.len();
            coordinates.push((lap[0], 0.1));
            coordinates.extend(path.iter().skip(1).map(|&longitude| (longitude, 0.1)));
            let high_latitude_vertex = coordinates.len();
            coordinates.push((lap[0], 0.2));
            coordinates.push((lap[0], 0.1));

            let chart_points =
                q_ring_chart_points(frame, &coordinates, &[repeated_start]).unwrap();

            assert_eq!(
                chart_points[high_latitude_vertex].x.to_bits(),
                expected.to_bits(),
                "{name} frame, direction {direction}"
            );
            assert_eq!(
                chart_points.last().unwrap().x.to_bits(),
                chart_points.first().unwrap().x.to_bits(),
                "{name} frame closure"
            );
        }
    }
}

#[test]
fn q_ring_half_period_ties_keep_each_native_seam_spelling() {
    let grads_unit = AngularFactor::try_new("grad", std::f64::consts::TAU / 400.0).unwrap();
    let radians_unit = AngularFactor::try_new("radian", 1.0).unwrap();
    let cases = [
        ("degree", GeographicFrame::crs84(), -180.0),
        (
            "grad",
            GeographicFrame::try_new(grads_unit, grads_unit, 0.0).unwrap(),
            -199.999_999_999_999_97,
        ),
        (
            "radian",
            GeographicFrame::try_new(radians_unit, radians_unit, 0.0).unwrap(),
            -std::f64::consts::PI,
        ),
    ];

    for (name, frame, seam) in cases {
        let chart_points =
            q_ring_chart_points(frame, &[(0.0, 0.1), (seam, 0.1), (0.0, 0.1)], &[]).unwrap();
        assert_eq!(chart_points[1].x.to_bits(), seam.to_bits(), "{name} seam");
        assert_eq!(
            chart_points.last().unwrap().x.to_bits(),
            chart_points.first().unwrap().x.to_bits(),
            "{name} closure"
        );
    }
}
