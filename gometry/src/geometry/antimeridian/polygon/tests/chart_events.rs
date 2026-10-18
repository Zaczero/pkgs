#[test]
fn chart_seam_event_retains_source_records_and_exact_branch() {
    let frame = GeographicFrame::crs84();
    let source = [
        Point::new_axes(170.0, 10.0, ZOrdinate(Some(-0.0)), MOrdinate(Some(100.0))).unwrap(),
        Point::new_axes(-170.0, 30.0, ZOrdinate(Some(20.0)), MOrdinate(Some(300.0))).unwrap(),
    ];
    let longitude = chart_connection(source[0], source[1], frame)
        .unwrap()
        .retained();
    let event = seam_event(&source, longitude, frame, None)
        .unwrap()
        .expect("the retained source edge crosses the seam");

    assert_eq!(event.source[0].x.to_bits(), source[0].x.to_bits());
    assert_eq!(event.source[0].y.to_bits(), source[0].y.to_bits());
    assert_eq!(event.source[0].z().unwrap().to_bits(), (-0.0_f64).to_bits());
    assert_eq!(event.source[0].m(), source[0].m());
    assert_eq!(event.source[1].x.to_bits(), source[1].x.to_bits());
    assert_eq!(event.source[1].y.to_bits(), source[1].y.to_bits());
    assert_eq!(event.source[1].z(), source[1].z());
    assert_eq!(event.source[1].m(), source[1].m());
    assert_eq!(event.longitude, longitude);
    assert_eq!(
        event.parameter,
        ExactRational::new(ExactDyadic::from_i64(1), ExactDyadic::from_i64(2),).unwrap()
    );
    assert_eq!(event.seam_longitude, Some(180.0));
}

#[rstest::rstest]
#[case::line_10_0_11_0_12_0_0_0_1_0_0_0(line(&[10.0, 11.0, 12.0], &[0.0, 1.0, 0.0]))]
#[case::shape_polygon_polygon_new_closed_ring_10_0_20_0_84b03528(Shape::Polygon(Polygon::new(
    closed_ring(&[10.0, 20.0, 20.0, 10.0, 10.0], &[
        10.0, 10.0, 20.0, 20.0, 10.0,
    ]),
    Vec::new(),
)))]
fn canonical_crs84_split_skips_retained_edge_admission(#[case] shape: Shape) {
    let before = source_admission_counts::snapshot().0;
    let split = shape
        .split_antimeridian_in_frame(GeographicFrame::crs84())
        .unwrap();
    assert_eq!(
        source_admission_counts::snapshot().0,
        before,
        "a canonical CRS84 identity split must not admit source edges"
    );
    assert_eq!(split, shape);
}

#[test]
#[expect(
    clippy::float_cmp,
    reason = "this operation classifies exact antimeridian or pole coordinates before chart mapping"
)]
#[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
fn certified_chart_event_skip_falls_back_at_the_periodic_boundary() {
    const CRS: &str = r#"GEOGCS["Custom grad",DATUM["Custom datum",SPHEROID["Custom",7000000,150]],PRIMEM["Greenwich",0],UNIT["grad",0.015707963267948967],AXIS["Longitude",EAST],AXIS["Latitude",NORTH]]"#;
    let state = crate::crs::runtime_state_snapshot().unwrap();
    let frame = crate::crs::info_with_state(CRS, &state)
        .unwrap()
        .geographic_frame()
        .unwrap();
    assert_eq!(frame.longitude().period().to_bits(), 400.0_f64.to_bits());
    assert!(certified_no_chart_events(-10.0, 10.0, frame));
    assert!(certified_no_chart_events(10.0, 20.0, frame));
    assert!(!certified_no_chart_events(100.0, -100.0, frame));
    assert!(!certified_no_chart_events(199.0, -2.0, frame));
    assert!(!certified_no_chart_events(200.0, 0.0, frame));
    assert!(!certified_no_chart_events(0.0, 400.0, frame));
    assert!(!certified_no_chart_events(f64::NAN, 0.0, frame));

    let longitude_scale = frame.longitude().to_degrees(1.0) * frame.longitude_orientation_sign();
    let latitude_scale =
        frame.latitude_unit().native_to_degrees(1.0) * frame.latitude_orientation_sign();
    let no_metric = None::<&crate::geometry::SourceArcMetricAccess<'_, '_>>;
    let Shape::LineString(safe_line) = line(&[-20.0, 0.0, 20.0], &[0.0, 1.0, 0.0]) else {
        unreachable!();
    };
    assert!(line_has_certified_no_chart_events(
        &safe_line,
        frame,
        longitude_scale
    ));
    let before = source_admission_counts::snapshot().0;
    let chart = chart_line(
        &safe_line,
        frame,
        longitude_scale,
        latitude_scale,
        no_metric,
    )
    .unwrap();
    assert_eq!(source_admission_counts::snapshot().0, before);
    assert_eq!(chart.as_coords().iter_coords().count(), 3);

    let Shape::LineString(crossing_line) = line(&[199.0, -2.0], &[0.0, 10.0]) else {
        unreachable!();
    };
    assert!(!line_has_certified_no_chart_events(
        &crossing_line,
        frame,
        longitude_scale
    ));
    let before = source_admission_counts::snapshot().0;
    let chart = chart_line(
        &crossing_line,
        frame,
        longitude_scale,
        latitude_scale,
        no_metric,
    )
    .unwrap();
    assert_eq!(source_admission_counts::snapshot().0 - before, 1);
    assert!(
        chart
            .as_coords()
            .iter_coords()
            .any(|point| point.x == 180.0)
    );

    let ring = closed_ring(&[-20.0, 20.0, 20.0, -20.0, -20.0], &[
        10.0, 10.0, 20.0, 20.0, 10.0,
    ]);
    let before = source_admission_counts::snapshot().0;
    let chart = chart_ring(&ring, frame, longitude_scale, latitude_scale, no_metric).unwrap();
    assert_eq!(source_admission_counts::snapshot().0 - before, 4);
    assert_eq!(chart.iter_coords().count(), 5);
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
fn chart_line_seam_ordinates_use_the_original_root_arclength() {
    let source_points = [
        Point::new_axes(170.0, 0.0, ZOrdinate(Some(0.0)), MOrdinate(Some(1000.0))).unwrap(),
        Point::new_axes(
            -170.0,
            60.0,
            ZOrdinate(Some(100.0)),
            MOrdinate(Some(3000.0)),
        )
        .unwrap(),
    ];
    let line = LineSeq::try_new(CoordSeq::try_from_points(&source_points).unwrap()).unwrap();
    let shape = Shape::LineString(line.clone());
    let state = crate::crs::runtime_state_snapshot().unwrap();
    let chart = crate::crs::with_ellipsoid_metric_at("EPSG:4326", &[&shape], &state, |metric| {
        let metric_access = || Ok(metric);
        chart_line(
            &line,
            GeographicFrame::crs84(),
            1.0,
            1.0,
            Some(&metric_access),
        )
    })
    .unwrap();
    let seam = chart.as_coords().point_at(1);

    assert_eq!(seam.x, 180.0);
    assert_eq!(seam.y, 30.0);
    assert!((seam.z().unwrap() - 50.438_798_221_498_36).abs() <= 2.0e-9);
    assert!((seam.m().unwrap() - 2_008.775_964_429_967_2).abs() <= 4.0e-8);
}

#[test]
#[expect(
    clippy::float_cmp,
    reason = "this operation classifies exact antimeridian or pole coordinates before chart mapping"
)]
#[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
fn segment_transit_ordinates_follow_the_retained_piece_arclength() {
    let source = [
        Point::new_axes(170.0, 10.0, ZOrdinate(Some(10.0)), MOrdinate(Some(1.0))).unwrap(),
        Point::new_axes(-150.0, 30.0, ZOrdinate(Some(50.0)), MOrdinate(Some(5.0))).unwrap(),
    ];
    let line = LineSeq::try_new(CoordSeq::try_from_points(&source).unwrap()).unwrap();
    let shape = Shape::LineString(line);
    let state = crate::crs::runtime_state_snapshot().unwrap();
    let seam_values =
        crate::crs::with_ellipsoid_metric_at("EPSG:4326", &[&shape], &state, |metric| {
            let metric_access = || Ok(metric);
            Ok(
                segment_coords_with_metric(
                    &source,
                    GeographicFrame::crs84(),
                    Some(&metric_access),
                )?
                .into_iter()
                .flatten()
                .filter(|point| point.x.abs() == 180.0)
                .map(|point| (point.y, point.z(), point.m()))
                .collect::<Vec<_>>(),
            )
        })
        .unwrap();
    let expected = (20.339_021_393_918_856, 2.033_902_139_391_885_5);

    assert_eq!(seam_values.len(), 2);
    for (latitude, z, m) in seam_values {
        assert_eq!(latitude, 15.0);
        assert!((z.unwrap() - expected.0).abs() <= 1.0e-7);
        assert!((m.unwrap() - expected.1).abs() <= 1.0e-8);
    }
}

#[test]
#[expect(
    clippy::float_cmp,
    reason = "this operation classifies exact antimeridian or pole coordinates before chart mapping"
)]
#[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
fn split_line_preserves_an_authored_full_turn_circle() {
    let source = Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_points(&[
                Point::new_axes(0.0, 0.0, ZOrdinate(Some(10.0)), MOrdinate(Some(2.0))).unwrap(),
                Point::new_axes(360.0, 0.0, ZOrdinate(Some(110.0)), MOrdinate(Some(202.0)))
                    .unwrap(),
            ])
            .unwrap(),
        )
        .unwrap(),
    );
    let state = crate::crs::runtime_state_snapshot().unwrap();
    let split = crate::crs::with_ellipsoid_metric_at("EPSG:4326", &[&source], &state, |metric| {
        let metric_access = || Ok(metric);
        source
            .split_antimeridian_in_frame_with_metric(GeographicFrame::crs84(), Some(&metric_access))
    })
    .unwrap();
    let Shape::MultiLineString(lines) = &split else {
        panic!("a full-turn circle crossing the cut has two chart pieces");
    };
    assert_eq!(lines.axes(), CoordinateAxes::XYZM);
    let mut points = Vec::new();
    split.for_each_point(|point| points.push(point));
    for (longitude, z, m) in [
        (90.0, 35.0, 52.0),
        (-90.0, 85.0, 152.0),
        (180.0, 60.0, 102.0),
        (-180.0, 60.0, 102.0),
    ] {
        assert!(
            points.iter().any(|point| {
                point.x == longitude && point.z() == Some(z) && point.m() == Some(m)
            }),
            "full-turn output omitted ({longitude}, 0, {z}, {m}): {points:?}"
        );
    }
}

#[test]
#[expect(
    clippy::float_cmp,
    reason = "this operation classifies exact antimeridian or pole coordinates before chart mapping"
)]
#[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
fn tilted_full_turn_seam_and_witnesses_use_the_original_root() {
    let source = Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_points(&[
                Point::new_axes(170.0, 0.0, ZOrdinate(Some(0.0)), MOrdinate(Some(100.0))).unwrap(),
                Point::new_axes(530.0, 60.0, ZOrdinate(Some(100.0)), MOrdinate(Some(1100.0)))
                    .unwrap(),
            ])
            .unwrap(),
        )
        .unwrap(),
    );
    let Shape::LineString(line) = &source else {
        unreachable!();
    };
    let state = crate::crs::runtime_state_snapshot().unwrap();
    crate::crs::with_ellipsoid_metric_at("EPSG:4326", &[&source], &state, |metric| {
        let frame = GeographicFrame::crs84();
        let metric_access = || Ok(metric);
        let chart = chart_line(line, frame, 1.0, 1.0, Some(&metric_access))?;
        let coordinates: Vec<_> = chart.as_coords().iter_coords().collect();
        let seam = *coordinates
            .iter()
            .find(|point| point.x == 180.0)
            .expect("the authored full turn crosses the chart seam");
        let quarter = *coordinates
            .iter()
            .find(|point| point.x == -100.0)
            .expect("the first quarter witness remains on the chart path");

        let piece = [seam, quarter];
        let longitude = frame
            .longitude()
            .chart_connection(piece[0].x, piece[1].x)
            .map(crate::geographic::ChartConnection::retained)
            .map_err(|error| {
                GeometryErrorKind::antimeridian_split_failed(error.to_string())
            })?;
        let midpoint = ExactRational::new(
            ExactDyadic::from_i64(1),
            ExactDyadic::from_i64(2),
        )?;
        let request = SourceArcRequest::new(
            frame,
            &piece,
            piece[0].axes().union(piece[1].axes()),
            &midpoint,
            || Ok(longitude),
        )?;
        let mut root_metric = RootMetricState::default();
        let mut fraction = None;
        let mut work = Unmetered;
        let composed = source_arc_ordinates(
            &request,
            || Ok(metric),
            &mut root_metric,
            &mut fraction,
            &mut work,
        )?;
        let split = source.split_antimeridian_in_frame_with_metric(
            frame,
            Some(&metric_access),
        )?;
        let mut public_seams = Vec::new();
        let mut public_quarter = None;
        split.for_each_point(|point| {
            if point.x.abs() == 180.0 && (point.y - 60.0 / 36.0).abs() <= 2.0e-14 {
                public_seams.push(point);
            }
            if point.x == -100.0 && point.y == 15.0 {
                public_quarter = Some(point);
            }
        });
        assert!(
            public_seams.iter().any(|point| point.x == -180.0)
                && public_seams.iter().any(|point| point.x == 180.0),
            "the final split omitted a seam alias: {public_seams:?}"
        );
        let public_quarter =
            public_quarter.expect("the final split retains the first quarter witness");

        let seam_parameter =
            ExactRational::new(ExactDyadic::from_i64(10), ExactDyadic::from_i64(360))?;
        let exact_seam_latitude =
            ExactRational::from_f64(60.0).try_product(&seam_parameter)?;
        assert_ne!(
            ExactRational::from_f64(seam.y),
            exact_seam_latitude,
            "t=1/36 produces a binary64 seam latitude"
        );

        // Independent 70-digit Decimal Simpson integration, N=8192 and
        // 16384, for the WGS84 first-fundamental-form speed on the root
        // (170,0)->(530,60). The subedge reference uses the rounded
        // binary64 seam latitude, as the emitted piece does.
        let q_seam = 0.033_323_535_838_094_22_f64;
        let q_quarter = 0.296_653_444_529_051_57_f64;
        let q_later_root = 0.166_073_296_867_606_94_f64;
        let q_later_piece = 0.166_073_296_867_606_94_f64;
        let root_z = |q: f64| 100.0 * q;
        let root_m = |q: f64| 100.0 + 1000.0 * q;
        let mut max_absolute_error = 0.0_f64;
        let mut max_relative_error = 0.0_f64;
        let mut measure = |actual: f64, expected: f64| {
            let absolute = (actual - expected).abs();
            max_absolute_error = max_absolute_error.max(absolute);
            max_relative_error = max_relative_error.max(absolute / expected.abs());
            absolute
        };
        for point in &public_seams {
            let expected_z = root_z(q_seam);
            let expected_m = root_m(q_seam);
            assert!(
                measure(point.z().unwrap(), expected_z)
                    <= ulp_error_limit(expected_z, 4.0)
            );
            assert!(
                measure(point.m().unwrap(), expected_m)
                    <= ulp_error_limit(expected_m, 4.0)
            );
        }
        let quarter_z = root_z(q_quarter);
        let quarter_m = root_m(q_quarter);
        assert!(
            measure(public_quarter.z().unwrap(), quarter_z)
                <= ulp_error_limit(quarter_z, 4.0)
        );
        assert!(
            measure(public_quarter.m().unwrap(), quarter_m)
                <= ulp_error_limit(quarter_m, 4.0)
        );
        let later_z = root_z(q_later_piece);
        let later_m = root_m(q_later_piece);
        assert!(
            measure(composed.z.value().unwrap(), later_z)
                <= ulp_error_limit(later_z, 4.0)
        );
        assert!(
            measure(composed.m.value().unwrap(), later_m)
                <= ulp_error_limit(later_m, 4.0)
        );
        assert!(
            (later_z - root_z(q_later_root)).abs() <= ulp_error_limit(later_z, 4.0)
        );
        assert!(
            (later_m - root_m(q_later_root)).abs() <= ulp_error_limit(later_m, 4.0)
        );
        println!(
            "tilted_full_turn_accuracy={{\"max_abs_error\":{max_absolute_error:.17e},\"max_relative_error\":{max_relative_error:.17e},\"seam_latitude\":{:.17e},\"piece_local_t\":0.5}}",
            seam.y,
        );
        Ok(())
    })
    .unwrap();
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
fn seam_piece_ordinate_composition_matches_independent_root_arclength() {
    let source_points = [
        Point::new_axes(170.0, 0.0, ZOrdinate(Some(0.0)), MOrdinate(Some(100.0))).unwrap(),
        Point::new_axes(
            -170.0,
            60.0,
            ZOrdinate(Some(100.0)),
            MOrdinate(Some(1100.0)),
        )
        .unwrap(),
    ];
    let line = LineSeq::try_new(CoordSeq::try_from_points(&source_points).unwrap()).unwrap();
    let source_shape = Shape::LineString(line.clone());
    let frame = GeographicFrame::crs84();
    let state = crate::crs::runtime_state_snapshot().unwrap();
    crate::crs::with_ellipsoid_metric_at("EPSG:4326", &[&source_shape], &state, |metric| {
        let metric_access = || Ok(metric);
        let chart = chart_line(&line, frame, 1.0, 1.0, Some(&metric_access))?;
        let seam = chart.as_coords().point_at(1);
        let piece = [source_points[0], seam];
        let retained = frame
            .longitude()
            .chart_connection(piece[0].x, piece[1].x)
            .map(crate::geographic::ChartConnection::retained)
            .map_err(|error| GeometryErrorKind::antimeridian_split_failed(error.to_string()))?;
        let parameter = ExactRational::new(ExactDyadic::from_i64(1), ExactDyadic::from_i64(2))?;
        let request = SourceArcRequest::new(
            frame,
            &piece,
            piece[0].axes().union(piece[1].axes()),
            &parameter,
            || Ok(retained),
        )?;
        let mut root_metric = RootMetricState::default();
        let mut fraction = None;
        let mut work = Unmetered;
        let composed = source_arc_ordinates(
            &request,
            || Ok(metric),
            &mut root_metric,
            &mut fraction,
            &mut work,
        )?;
        let expected_q = 0.252_865_084_440_217_66_f64;
        assert!((composed.z.value().unwrap() - 100.0 * expected_q).abs() <= 2.0e-8);
        assert!((composed.m.value().unwrap() - (100.0 + 1000.0 * expected_q)).abs() <= 2.0e-7);
        Ok(())
    })
    .unwrap();
}

#[test]
fn chart_line_eligible_interior_request_propagates_metric_failure() {
    let line = LineSeq::try_new(
        CoordSeq::try_from_points(&[
            Point::new_axes(170.0, 0.0, ZOrdinate(Some(0.0)), MOrdinate(Some(1.0))).unwrap(),
            Point::new_axes(-170.0, 60.0, ZOrdinate(Some(100.0)), MOrdinate(Some(2.0))).unwrap(),
        ])
        .unwrap(),
    )
    .unwrap();
    let accesses = std::cell::Cell::new(0);
    let failing_metric = || -> crate::error::Result<&crate::crs::GeodesicMetric> {
        accesses.set(accesses.get() + 1);
        Err(crate::crs::CrsError::invalid("seam metric sentinel"))
    };
    let result = chart_line(
        &line,
        GeographicFrame::crs84(),
        1.0,
        1.0,
        Some(&failing_metric),
    );

    assert!(
        result
            .expect_err("eligible seam ordinates require metric authority")
            .to_string()
            .contains("seam metric sentinel")
    );
    assert_eq!(accesses.get(), 1);

    let xy = LineSeq::try_new(
        CoordSeq::try_from_vecs(vec![170.0, -170.0], vec![0.0, 60.0], None, None).unwrap(),
    )
    .unwrap();
    chart_line(
        &xy,
        GeographicFrame::crs84(),
        1.0,
        1.0,
        Some(&failing_metric),
    )
    .unwrap();
    assert_eq!(accesses.get(), 1);
}

#[test]
#[expect(
    clippy::float_cmp,
    reason = "this operation classifies exact antimeridian or pole coordinates before chart mapping"
)]
#[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
fn crossing_hole_circle_piece_ordinates_compose_to_the_original_root() {
    let shell = xyzm_ring(&[
        (0.0, -30.0, 30.0, 3000.0),
        (360.0, -30.0, 20.0, 2000.0),
        (360.0, 30.0, 20.0, 2000.0),
        (0.0, 30.0, 30.0, 3000.0),
        (0.0, -30.0, 30.0, 3000.0),
    ]);
    let hole = xyzm_ring(&[
        (135.0, 30.0, 21.25, 2125.0),
        (180.0, 25.0, 20.0, 2000.0),
        (-180.0, 20.0, 30.0, 3000.0),
        (-170.0, 18.0, 25.0, 2500.0),
        (-170.0, 12.0, 25.0, 2500.0),
        (-180.0, 10.0, 30.0, 3000.0),
        (180.0, 15.0, 20.0, 2000.0),
        (135.0, 20.0, 25.0, 2500.0),
        (135.0, 30.0, 21.25, 2125.0),
    ]);
    let polygon = Polygon::new(shell, vec![hole]);
    let source = Shape::Polygon(polygon.clone());
    let frame = GeographicFrame::crs84();
    let state = crate::crs::runtime_state_snapshot().unwrap();
    let result = crate::crs::with_ellipsoid_metric_at("EPSG:4326", &[&source], &state, |metric| {
        let metric_access = || Ok(metric);
        let chart = chart_polygon(&polygon, frame, 1.0, 1.0, Some(&metric_access))?;
        take_source_arc_observations();
        let chart_shape = Shape::Polygon(chart.clone());
        let parts = split_polygon(
            &chart_shape,
            &chart,
            PoleClosure::Auto,
            Some(&metric_access),
        )?;
        Ok(parts)
    });
    let observations = take_source_arc_observations();
    let split = result.unwrap_or_else(|error| {
        panic!("hole overlay failed after these source requests {observations:?}: {error}")
    });
    assert!(!split.is_empty());

    let observation = observations
        .iter()
        .find(|observation| {
            observation.source[0].y == 30.0
                && observation.source[1].y == 30.0
                && ((observation.source[0].x == 180.0 && observation.source[1].x == 90.0)
                    || (observation.source[0].x == 90.0 && observation.source[1].x == 180.0))
        })
        .expect("hole overlay requests a node on the upper Circle witness interval");
    let chart_longitude = observation.source[0].x
        + (observation.source[1].x - observation.source[0].x) * observation.parameter;
    let source_longitude = chart_longitude + 180.0;
    let root_fraction = (360.0 - source_longitude) / 360.0;
    let expected_z = 20.0 + 10.0 * root_fraction;
    let expected_m = 2000.0 + 1000.0 * root_fraction;
    assert_eq!(chart_longitude, 135.0);
    assert_eq!(expected_z.to_bits(), 21.25_f64.to_bits());
    assert_eq!(expected_m.to_bits(), 2125.0_f64.to_bits());
    assert_eq!(
        observation.z.expect("shell carries Z").to_bits(),
        21.25_f64.to_bits()
    );
    assert_eq!(
        observation.m.expect("shell carries M").to_bits(),
        2125.0_f64.to_bits()
    );
}
