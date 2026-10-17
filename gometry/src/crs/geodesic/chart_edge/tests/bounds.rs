#[test]
fn chart_edge_bounds_contain_conditioned_positions_across_native_frames() {
    let degree = degrees();
    let grad_unit = AngularFactor::try_new("grad", std::f64::consts::PI / 200.0).unwrap();
    let radian_unit = AngularFactor::try_new("radian", 1.0).unwrap();
    let orientation = GeographicAxisOrientation::from_axis_directions(true, false);
    let frames = [
        degree,
        GeographicFrame::try_new(grad_unit, grad_unit, 0.0).unwrap(),
        GeographicFrame::try_new(radian_unit, radian_unit, 0.0).unwrap(),
        GeographicFrame::try_new(grad_unit, grad_unit, 2.0_f64.to_radians()).unwrap(),
        GeographicFrame::try_new_with_orientation(grad_unit, grad_unit, 0.0, orientation).unwrap(),
    ];
    for frame in frames {
        let axis = frame.longitude();
        let start = Point::new_raw_xy(axis.degrees_to_native(170.0), frame.pole() * 0.6);
        let end = Point::new_raw_xy(axis.degrees_to_native(-170.0), frame.pole() * 0.9);
        let edge = ChartEdge::new(start, end, frame).unwrap();
        let bounds = edge.bounds();
        for sample in 0..=64 {
            let parameter = f64::from(sample) / 64.0;
            let chart = edge.chart_position(parameter).unwrap();
            let (longitude, latitude) = frame.to_geodesic_xy(chart.x, chart.y).unwrap();
            let longitude = degrees().longitude().canonicalize(longitude).unwrap().get();
            let contains_longitude = if bounds.west <= bounds.east {
                (bounds.west..=bounds.east).contains(&longitude)
            } else {
                longitude >= bounds.west || longitude <= bounds.east
            };
            assert!(
                contains_longitude && (bounds.south..=bounds.north).contains(&latitude),
                "frame={frame:?}, t={parameter}, point=({longitude}, {latitude}), bounds={bounds:?}"
            );
        }
    }
}

#[test]
#[cfg_attr(miri, ignore = "Miri cannot execute native PROJ FFI")]
fn huge_source_alias_has_a_conservative_metric_box_and_authentic_endpoint_distance() {
    const CRS: &str = "EPSG:4326";
    let state = crate::crs::runtime_state_snapshot().unwrap();
    crate::crs::with_ellipsoid_metric_at(CRS, &[], &state, |metric| {
        let start = Point::new_raw_xy(1.0e18, 0.0);
        let end = Point::new_raw_xy(0.0, 0.0);
        let query = Point::new_raw_xy(48.0, 0.0);
        let edge = metric.chart_edge(start, end)?;
        let bounds = edge.bounds();
        let lower = metric.point_envelope_bound(query)(bounds);
        let endpoint_distance = metric.segment_length(query, end);
        let independently_known_equatorial_distance = 6_378_137.0 * 48.0_f64.to_radians();

        assert!(lower > 0.0);
        assert!(lower <= endpoint_distance);
        assert!((endpoint_distance - independently_known_equatorial_distance).abs() <= 1.0e-8);
        for parameter in [0.0, 0.25, 0.5, 0.75, 1.0] {
            let target = edge.geodetic_position(parameter)?;
            assert!(lower <= metric.segment_length(query, target));
        }
        assert_eq!(edge.chart_position(1.0)?.x, 0.0);
        Ok(())
    })
    .unwrap();
}

#[test]
fn conditioned_position_retains_small_endpoint_after_large_longitude_delta() {
    let start = Point::new_raw_xy(100.0, 10.0);
    let end = Point::new_raw_xy(1.0e-20, 20.0);
    let edge = ChartEdge::new(start, end, degrees()).unwrap();
    assert_eq!(
        edge.chart_position(1.0).unwrap().x.to_bits(),
        end.x.to_bits()
    );
    let geodesic = wgs84();
    let metric = crate::crs::EllipsoidMetric::for_geodesic_at_frame(&geodesic, 0, degrees());
    assert_eq!(edge.witness(start, end, 1.0, None, &metric).unwrap(), end);
}

#[test]
fn conditioned_position_does_not_lift_a_computed_site_from_a_huge_source_alias() {
    let edge = ChartEdge::new(
        Point::new_raw_xy(1.0e18, 0.0),
        Point::new_raw_xy(0.0, 0.0),
        degrees(),
    )
    .unwrap();
    assert_eq!(edge.chart_position(1.0).unwrap().x, 0.0);
    assert_eq!(edge.geodetic_position(1.0).unwrap().x, 0.0);
}

#[test]
fn explicit_full_turn_circle_has_full_longitude_bounds() {
    let circle = ChartEdge::new(
        Point::new_raw_xy(10.0, 60.0),
        Point::new_raw_xy(370.0, 60.0),
        degrees(),
    )
    .unwrap();
    let bounds = circle.bounds();
    assert_eq!(bounds.west.to_bits(), (-180.0_f64).to_bits());
    assert_eq!(bounds.east.to_bits(), 180.0_f64.to_bits());
}

#[test]
fn chart_edge_bounds_keep_authentic_longitude_endpoints() {
    let start = 0.1;
    let end = 0.2;
    let edge = ChartEdge::new(
        Point::new_raw_xy(start, 10.0),
        Point::new_raw_xy(end, 10.0),
        degrees(),
    )
    .unwrap();
    let bounds = edge.bounds();
    assert_eq!(bounds.west.to_bits(), start.to_bits());
    assert_eq!(bounds.east.to_bits(), end.to_bits());
}
