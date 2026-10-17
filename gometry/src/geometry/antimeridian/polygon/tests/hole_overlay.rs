#[test]
fn captured_polar_annulus_keeps_its_band_and_roots_under_ring_reversal() {
    for sign in [-1.0, 1.0] {
        for reverse_shell in [false, true] {
            for reverse_hole in [false, true] {
                let latitude_ring = |latitude, reverse| {
                    let mut points = [0.0, 60.0, 120.0, 180.0, -120.0, -60.0]
                        .map(|longitude| (longitude, latitude, 5.0, 50.0));
                    if reverse {
                        points.reverse();
                    }
                    xyzm_ring(&points)
                };
                let source = Shape::Polygon(Polygon::new(
                    latitude_ring(sign * 70.0, reverse_shell),
                    vec![latitude_ring(sign * 80.0, reverse_hole)],
                ));
                let split = source
                    .split_antimeridian_captured_in_crs84()
                    .unwrap()
                    .finish_scalar(crate::Frame::None)
                    .unwrap()
                    .into_scalar()
                    .unwrap()
                    .0;
                assert_eq!(split.validate().unwrap(), None, "{split:?}");
                assert_eq!(split.area().to_bits(), 3600.0_f64.to_bits());
                assert_eq!(split.axes(), CoordinateAxes::XYZM);
                for longitude in [-179.0, 0.0, 179.0] {
                    for (latitude, expected) in [(65.0, false), (75.0, true), (85.0, false)] {
                        let probe = Shape::Point(Point::new(longitude, sign * latitude).unwrap());
                        assert_eq!(split.contains(&probe).unwrap(), expected, "{split:?}");
                    }
                }
                split.for_each_point(|point| {
                    assert_eq!(point.z().unwrap().to_bits(), 5.0_f64.to_bits());
                    assert_eq!(point.m().unwrap().to_bits(), 50.0_f64.to_bits());
                });
            }
        }
    }
}

#[test]
fn crossing_hole_subtraction_keeps_a_zero_turn_world_band_in_its_chart() {
    let source = polygon(
        &[(0.0, -30.0), (360.0, -30.0), (360.0, 30.0), (0.0, 30.0)],
        &[&[(175.0, 0.0), (-175.0, 0.0), (-175.0, 5.0), (175.0, 5.0)]],
    );
    let split = split_crs84(&source);

    assert_eq!(split.validate().unwrap(), None, "{split:?}");
    assert_eq!(split.area(), 21_550.0, "{split:?}");
    let mut witnesses = [false; 2];
    split.for_each_point(|point| {
        if point.x.to_bits() == 0.0_f64.to_bits() {
            if point.y.to_bits() == (-30.0_f64).to_bits() {
                witnesses[0] = true;
            }
            if point.y.to_bits() == 30.0_f64.to_bits() {
                witnesses[1] = true;
            }
        }
    });
    assert_eq!(witnesses, [true, true], "{split:?}");
}

#[test]
fn captured_full_turn_shell_subtracts_a_crossing_hole_like_plain_split() {
    let cases = [
        (
            polygon(
                &[(0.0, 60.0), (90.0, 62.0), (180.0, 60.0), (270.0, 62.0)],
                &[&[(170.0, 70.0), (-170.0, 70.0), (-170.0, 75.0), (170.0, 75.0)]],
            ),
            Point::new(175.0, 72.0).unwrap(),
            Point::new(100.0, 72.0).unwrap(),
        ),
        (
            polygon(
                &[(0.0, 50.0), (90.0, 52.0), (180.0, 50.0), (270.0, 52.0)],
                &[&[(0.0, 70.0), (270.0, 72.0), (180.0, 70.0), (90.0, 72.0)]],
            ),
            Point::new(0.0, 80.0).unwrap(),
            Point::new(0.0, 60.0).unwrap(),
        ),
    ];
    for (source, hole_witness, shell_witness) in cases {
        let plain = split_crs84(&source);
        let captured = source
            .split_antimeridian_captured_in_frame_with_metric::<crate::crs::GeodesicMetric>(
                GeographicFrame::crs84(),
                None,
            )
            .unwrap()
            .finish_scalar(crate::Frame::None)
            .unwrap()
            .into_scalar()
            .unwrap()
            .0;

        for split in [&plain, &captured] {
            assert_eq!(split.validate().unwrap(), None, "{split:?}");
            assert!(
                !split.contains(&Shape::Point(hole_witness)).unwrap(),
                "the crossing hole must remain empty: {split:?}"
            );
            assert!(
                split.contains(&Shape::Point(shell_witness)).unwrap(),
                "the surrounding polar shell must remain filled: {split:?}"
            );
        }
        assert_eq!(
            plain.area(),
            captured.area(),
            "plain: {plain:?}\ncaptured: {captured:?}"
        );
    }
}

#[test]
#[expect(clippy::float_cmp, reason = "this operation classifies exact antimeridian or pole coordinates before chart mapping")]
#[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
fn crossing_hole_uses_the_original_grad_ellipsoid_in_the_temporary_degree_chart() {
    const CRS: &str = r#"GEOGCS["Custom grad",DATUM["Custom datum",SPHEROID["Custom",7000000,150]],PRIMEM["Greenwich",0],UNIT["grad",0.015707963267948967],AXIS["Longitude",EAST],AXIS["Latitude",NORTH]]"#;
    let state = crate::crs::runtime_state_snapshot().unwrap();
    let frame = crate::crs::info_with_state(CRS, &state)
        .unwrap()
        .geographic_frame()
        .unwrap();
    assert_eq!(frame.longitude().period().to_bits(), 400.0_f64.to_bits());
    let latitude_scale =
        frame.latitude_unit().native_to_degrees(1.0) * frame.latitude_orientation_sign();
    let chart_bottom = -50.0 * latitude_scale;
    let chart_top = 80.0 * latitude_scale;
    let lower_latitude = 0.0 * latitude_scale;
    let upper_latitude = 50.0 * latitude_scale;
    let parameter_at = |latitude: f64| {
        let numerator = ExactDyadic::from_f64(latitude)
            .try_subtract(ExactDyadic::from_f64(chart_bottom))
            .unwrap();
        let denominator = ExactDyadic::from_f64(chart_top)
            .try_subtract(ExactDyadic::from_f64(chart_bottom))
            .unwrap();
        ExactRational::new(numerator, denominator)
            .unwrap()
            .round()
            .unwrap()
    };

    let shell = xyzm_ring(&[
        (150.0, -50.0, 0.0, 0.0),
        (-150.0, -50.0, 0.0, 0.0),
        (-150.0, 80.0, 100.0, 1000.0),
        (150.0, 80.0, 100.0, 1000.0),
        (150.0, -50.0, 0.0, 0.0),
    ]);
    let shell_only = Shape::Polygon(Polygon::new(shell.clone(), Vec::new()));
    let lower_parameter = parameter_at(lower_latitude);
    let upper_parameter = parameter_at(upper_latitude);
    let (lower_z, lower_m, upper_z, upper_m) =
        crate::crs::with_ellipsoid_metric_at(CRS, &[&shell_only], &state, |metric| {
            let ramp_start = Point::new_axes(
                180.0,
                chart_bottom,
                ZOrdinate(Some(0.0)),
                MOrdinate(Some(0.0)),
            )?;
            let ramp_end = Point::new_axes(
                180.0,
                chart_top,
                ZOrdinate(Some(100.0)),
                MOrdinate(Some(1000.0)),
            )?;
            let edge = crate::crs::ChartEdge::new(ramp_start, ramp_end, GeographicFrame::crs84())?;
            let length = metric.chart_edge_length(&edge)?;
            let lower_fraction =
                metric.chart_edge_arclength_fraction(&edge, lower_parameter, Some(length))?;
            let upper_fraction =
                metric.chart_edge_arclength_fraction(&edge, upper_parameter, Some(length))?;
            let (lower_z, lower_m) = crate::geometry::source_ordinate_values_at_fraction(
                ramp_start,
                ramp_end,
                CoordinateAxes::XYZM,
                lower_fraction,
            );
            let (upper_z, upper_m) = crate::geometry::source_ordinate_values_at_fraction(
                ramp_start,
                ramp_end,
                CoordinateAxes::XYZM,
                upper_fraction,
            );
            Ok((
                lower_z.0.expect("fixture has Z"),
                lower_m.0.expect("fixture has M"),
                upper_z.0.expect("fixture has Z"),
                upper_m.0.expect("fixture has M"),
            ))
        })
        .unwrap();
    let hole = xyzm_ring(&[
        (160.0, 0.0, lower_z, lower_m),
        (160.0, 50.0, upper_z, upper_m),
        (200.0, 50.0, upper_z, upper_m),
        (200.0, 0.0, lower_z, lower_m),
        (160.0, 0.0, lower_z, lower_m),
    ]);
    let source = Shape::Polygon(Polygon::new(shell, vec![hole]));
    let split = crate::crs::with_ellipsoid_metric_at(CRS, &[&source], &state, |metric| {
        let metric_access = || Ok(metric);
        source.split_antimeridian_in_frame_with_metric(frame, Some(&metric_access))
    })
    .unwrap();
    assert_eq!(split.axes(), CoordinateAxes::XYZM);

    let custom_flattening = 1.0 / 150.0;
    let wgs84_flattening = 1.0 / 298.257_223_563;
    for (native_latitude, degree_latitude, parameter) in [
        (0.0, lower_latitude, lower_parameter),
        (50.0, upper_latitude, upper_parameter),
    ] {
        let fraction =
            independent_meridian_fraction(parameter, chart_bottom, chart_top, custom_flattening);
        let chart_fraction = (degree_latitude - chart_bottom) / (chart_top - chart_bottom);
        let wgs84_fraction =
            independent_meridian_fraction(parameter, chart_bottom, chart_top, wgs84_flattening);
        assert!((fraction - chart_fraction).abs() > 1.0e-5);
        assert!((fraction - wgs84_fraction).abs() > 1.0e-7);

        let mut seam_values = Vec::new();
        split.for_each_point(|point| {
            if point.x.abs() == 200.0 && point.y == native_latitude {
                seam_values.push((point.z(), point.m()));
            }
        });
        assert!(
            !seam_values.is_empty(),
            "missing seam cut at {native_latitude}"
        );
        for (z, m) in seam_values {
            assert!((z.expect("split retains Z") - 100.0 * fraction).abs() <= 2.0e-8);
            assert!((m.expect("split retains M") - 1000.0 * fraction).abs() <= 2.0e-7);
        }
    }
}

#[test]
#[expect(clippy::float_cmp, reason = "this operation classifies exact antimeridian or pole coordinates before chart mapping")]
#[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
fn reversed_grad_hole_overlay_retains_meridian_ordinates() {
    const CRS: &str = r#"GEOGCS["West south grad",DATUM["Custom datum",SPHEROID["Custom",7000000,150]],PRIMEM["Greenwich",0],UNIT["grad",0.015707963267948967],AXIS["Longitude",WEST],AXIS["Latitude",SOUTH]]"#;
    let state = crate::crs::runtime_state_snapshot().unwrap();
    let frame = crate::crs::info_with_state(CRS, &state)
        .unwrap()
        .geographic_frame()
        .unwrap();
    assert_eq!(frame.longitude().period().to_bits(), 400.0_f64.to_bits());
    assert_eq!(frame.longitude_orientation_sign(), -1.0);
    assert_eq!(frame.latitude_orientation_sign(), -1.0);

    let longitude_scale = frame.longitude().to_degrees(1.0) * frame.longitude_orientation_sign();
    let latitude_scale =
        frame.latitude_unit().native_to_degrees(1.0) * frame.latitude_orientation_sign();
    let chart_bottom = super::chart_latitude(50.0, frame, latitude_scale).unwrap();
    let chart_top = super::chart_latitude(-80.0, frame, latitude_scale).unwrap();
    let lower_latitude = super::chart_latitude(0.0, frame, latitude_scale).unwrap();
    let upper_latitude = super::chart_latitude(-50.0, frame, latitude_scale).unwrap();
    let parameter_at = |latitude: f64| {
        let numerator = ExactDyadic::from_f64(latitude)
            .try_subtract(ExactDyadic::from_f64(chart_bottom))
            .unwrap();
        let denominator = ExactDyadic::from_f64(chart_top)
            .try_subtract(ExactDyadic::from_f64(chart_bottom))
            .unwrap();
        ExactRational::new(numerator, denominator)
            .unwrap()
            .round()
            .unwrap()
    };
    let lower_parameter = parameter_at(lower_latitude);
    let upper_parameter = parameter_at(upper_latitude);
    let shell = xyzm_ring(&[
        (-150.0, 50.0, 0.0, 0.0),
        (150.0, 50.0, 0.0, 0.0),
        (150.0, -80.0, 100.0, 1000.0),
        (-150.0, -80.0, 100.0, 1000.0),
        (-150.0, 50.0, 0.0, 0.0),
    ]);
    let shell_only = Shape::Polygon(Polygon::new(shell.clone(), Vec::new()));
    let (lower_z, lower_m, upper_z, upper_m) =
        crate::crs::with_ellipsoid_metric_at(CRS, &[&shell_only], &state, |metric| {
            let ramp_start = Point::new_axes(
                180.0,
                chart_bottom,
                ZOrdinate(Some(0.0)),
                MOrdinate(Some(0.0)),
            )?;
            let ramp_end = Point::new_axes(
                180.0,
                chart_top,
                ZOrdinate(Some(100.0)),
                MOrdinate(Some(1000.0)),
            )?;
            let edge = crate::crs::ChartEdge::new(ramp_start, ramp_end, GeographicFrame::crs84())?;
            let length = metric.chart_edge_length(&edge)?;
            let lower_fraction =
                metric.chart_edge_arclength_fraction(&edge, lower_parameter, Some(length))?;
            let upper_fraction =
                metric.chart_edge_arclength_fraction(&edge, upper_parameter, Some(length))?;
            let (lower_z, lower_m) = crate::geometry::source_ordinate_values_at_fraction(
                ramp_start,
                ramp_end,
                CoordinateAxes::XYZM,
                lower_fraction,
            );
            let (upper_z, upper_m) = crate::geometry::source_ordinate_values_at_fraction(
                ramp_start,
                ramp_end,
                CoordinateAxes::XYZM,
                upper_fraction,
            );
            Ok((
                lower_z.0.expect("fixture carries Z"),
                lower_m.0.expect("fixture carries M"),
                upper_z.0.expect("fixture carries Z"),
                upper_m.0.expect("fixture carries M"),
            ))
        })
        .unwrap();
    let hole = xyzm_ring(&[
        (-160.0, 0.0, lower_z, lower_m),
        (-160.0, -50.0, upper_z, upper_m),
        (-200.0, -50.0, upper_z, upper_m),
        (-200.0, 0.0, lower_z, lower_m),
        (-160.0, 0.0, lower_z, lower_m),
    ]);
    let polygon = Polygon::new(shell, vec![hole]);
    let source = Shape::Polygon(polygon.clone());
    let (parts, observations, split) =
        crate::crs::with_ellipsoid_metric_at(CRS, &[&source], &state, |metric| {
            let metric_access = || Ok(metric);
            let chart = chart_polygon(
                &polygon,
                frame,
                longitude_scale,
                latitude_scale,
                Some(&metric_access),
            )?;
            take_source_arc_observations();
            let chart_shape = Shape::Polygon(chart.clone());
            let parts = split_polygon(
                &chart_shape,
                &chart,
                PoleClosure::Auto,
                Some(&metric_access),
            )?;
            let observations = take_source_arc_observations();
            let split =
                source.split_antimeridian_in_frame_with_metric(frame, Some(&metric_access))?;
            Ok((parts, observations, split))
        })
        .unwrap();
    assert!(!parts.is_empty());
    assert_eq!(split.axes(), CoordinateAxes::XYZM);

    let mut matched = 0;
    let mut max_absolute_error = 0.0_f64;
    let mut max_relative_error = 0.0_f64;
    for observation in observations.iter().filter(|observation| {
        observation.source[0].x.abs() == 180.0
            && observation.source[1].x.abs() == 180.0
            && observation.source[0].x.to_bits() == observation.source[1].x.to_bits()
            && observation.source[0].y != observation.source[1].y
    }) {
        let chart_latitude = observation.source[0].y
            + (observation.source[1].y - observation.source[0].y) * observation.parameter;
        let root_parameter = (chart_latitude - chart_bottom) / (chart_top - chart_bottom);
        let q = independent_meridian_fraction(root_parameter, chart_bottom, chart_top, 1.0 / 150.0);
        let expected_z = 100.0 * q;
        let expected_m = 1000.0 * q;
        let observed_z = observation.z.expect("overlay source carries Z");
        let observed_m = observation.m.expect("overlay source carries M");
        let z_error = (observed_z - expected_z).abs();
        let m_error = (observed_m - expected_m).abs();
        max_absolute_error = max_absolute_error.max(z_error).max(m_error);
        max_relative_error = max_relative_error
            .max(z_error / expected_z.abs())
            .max(m_error / expected_m.abs());
        assert!(
            z_error <= ulp_error_limit(expected_z, 4.0),
            "Z error {z_error:e} at t={root_parameter}"
        );
        assert!(
            m_error <= ulp_error_limit(expected_m, 4.0),
            "M error {m_error:e} at t={root_parameter}; source={:?}; parameter={:.17e}; actual={observed_m:.17e}; expected={expected_m:.17e}; chart_latitude={chart_latitude:.17e}",
            observation.source,
            observation.parameter,
        );
        matched += 1;
    }
    assert!(
        matched > 0,
        "hole overlay made no interior seam-edge request"
    );

    let lower_q =
        independent_meridian_fraction(lower_parameter, chart_bottom, chart_top, 1.0 / 150.0);
    let upper_q =
        independent_meridian_fraction(upper_parameter, chart_bottom, chart_top, 1.0 / 150.0);
    for (native_latitude, expected_z, expected_m, source_z, source_m) in [
        (0.0, 100.0 * lower_q, 1000.0 * lower_q, lower_z, lower_m),
        (-50.0, 100.0 * upper_q, 1000.0 * upper_q, upper_z, upper_m),
    ] {
        let values = {
            let mut values = Vec::new();
            split.for_each_point(|point| {
                if point.x.abs() == 200.0 && point.y == native_latitude {
                    values.push((point.z(), point.m()));
                }
            });
            values
        };
        assert!(
            !values.is_empty(),
            "missing final seam point at {native_latitude}"
        );
        for (z, m) in values {
            let z = z.expect("final seam point carries Z");
            let m = m.expect("final seam point carries M");
            let z_error = (z - expected_z).abs();
            let m_error = (m - expected_m).abs();
            max_absolute_error = max_absolute_error.max(z_error).max(m_error);
            max_relative_error = max_relative_error
                .max(z_error / expected_z.abs())
                .max(m_error / expected_m.abs());
            assert!(
                z_error <= ulp_error_limit(expected_z, 4.0),
                "final Z error {z_error:e}"
            );
            assert!(
                m_error <= ulp_error_limit(expected_m, 4.0),
                "final M error {m_error:e}"
            );
            assert_eq!(z.to_bits(), source_z.to_bits());
            assert_eq!(m.to_bits(), source_m.to_bits());
        }
    }
    println!(
        "reversed_grad_hole_accuracy={{\"max_abs_error\":{max_absolute_error:.17e},\"max_relative_error\":{max_relative_error:.17e},\"interior_requests\":{matched}}}"
    );
}
