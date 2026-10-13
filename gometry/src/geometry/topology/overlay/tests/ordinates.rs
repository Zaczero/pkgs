#[test]
fn q_node_ordinates_return_xy_defaults_before_source_metric_access() {
    let frame = GeographicFrame::crs84();
    let seam_crossing = Shape::LineString(
        LineSeq::try_new(
            CoordSeq::try_from_points(&[
                Point::new_axes(
                    170.0,
                    -10.0,
                    ZOrdinate(Some(-30.0)),
                    MOrdinate(Some(-300.0)),
                )
                .unwrap(),
                Point::new_axes(-170.0, -10.0, ZOrdinate(Some(30.0)), MOrdinate(Some(300.0)))
                    .unwrap(),
            ])
            .unwrap(),
        )
        .unwrap(),
    );
    let inputs =
        GeographicInputs::try_from_shapes(frame, std::iter::once(&seam_crossing)).unwrap();
    assert_eq!(
        crate::geometry::geographic_topology_route(inputs.views().iter().copied()),
        crate::geometry::GeographicTopologyRoute::Quotient
    );
    let topology =
        ExactGeographicTopology::from_views_as_set(frame, inputs.views().iter().copied())
            .unwrap();
    let metric_accesses = std::cell::Cell::new(0_usize);
    let no_metric = || -> Result<&crate::crs::GeodesicMetric> {
        metric_accesses.set(metric_accesses.get().wrapping_add(1));
        Err(crate::crs::CrsError::invalid(
            "unexpected source-arc metric request",
        ))
    };

    let mut work = crate::geometry::Unmetered;
    let xy = node_ordinates::<crate::crs::GeodesicMetric>(
        &topology,
        CoordinateAxes::XY,
        Some(&no_metric),
        &mut work,
    )
    .unwrap();
    assert!(
        xy.iter()
            .all(|ordinates| ordinates.z.value().is_none() && ordinates.m.value().is_none())
    );
    assert_eq!(xy.len(), topology.q.nodes.len());
    assert_eq!(metric_accesses.get(), 0);
}
