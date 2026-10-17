#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
fn chart_ring_interpolates_ordinates_at_full_turn_witnesses() {
    let point = |x, y, z, m| Point::new_axes(x, y, ZOrdinate(Some(z)), MOrdinate(Some(m))).unwrap();
    let ring = Ring::closed_coordseq(
        CoordSeq::try_from_points(&[
            point(0.0, -30.0, 0.0, 10.0),
            point(360.0, -30.0, 360.0, 370.0),
            point(360.0, 30.0, 720.0, 730.0),
            point(0.0, 30.0, 1080.0, 1090.0),
            point(0.0, -30.0, 0.0, 10.0),
        ])
        .unwrap(),
    )
    .unwrap();
    let state = crate::crs::runtime_state_snapshot().unwrap();
    let chart = crate::crs::with_ellipsoid_metric_at("EPSG:4326", &[], &state, |metric| {
        let metric_access = || Ok(metric);
        super::chart_ring(
            &ring,
            GeographicFrame::crs84(),
            1.0,
            1.0,
            Some(&metric_access),
        )
    })
    .unwrap();

    assert_eq!(chart.coords().coord_count(), 11);
    assert_eq!(chart.coords().point_at(2), point(0.0, -30.0, 180.0, 190.0));
    assert_eq!(chart.coords().point_at(7), point(0.0, 30.0, 900.0, 910.0));
}

#[test]
fn chart_ring_keeps_a_non_band_explicit_circle_edge() {
    let ring = closed_ring(&[0.0, 360.0, 300.0, 0.0], &[60.0, 60.0, 65.0, 60.0]);

    let chart = super::chart_ring::<crate::crs::GeodesicMetric>(
        &ring,
        GeographicFrame::crs84(),
        1.0,
        1.0,
        None,
    )
    .unwrap();

    assert_eq!(chart.coords().coord_count(), 7);
    assert_eq!(
        chart.coords().first_coord(),
        Some(Point::new_raw_xy(0.0, 60.0))
    );
    assert_eq!(
        chart.coords().last_coord(),
        Some(Point::new_raw_xy(0.0, 60.0))
    );
}
