use super::*;

pub(super) fn degrees() -> GeographicFrame {
    GeographicFrame::crs84()
}

pub(super) fn angular_frame(radians_per_unit: f64) -> GeographicFrame {
    let unit = AngularFactor::try_new("test angle", radians_per_unit).unwrap();
    GeographicFrame::try_new(unit, unit, 0.0).unwrap()
}

pub(super) fn publish(points: &[Point], closed_ring: bool, steps: &[f64]) -> Vec<Point> {
    publish_in(degrees(), points, closed_ring, steps)
}

pub(super) fn publish_in(
    frame: GeographicFrame,
    points: &[Point],
    closed_ring: bool,
    steps: &[f64],
) -> Vec<Point> {
    let mut budget = ExpansionBudget::new("test", "computed edges");
    let intent = |index: usize, start: Point, end: Point| {
        let step = *steps
            .get(index)
            .expect("each source edge has one retained test relation");
        assert_eq!(step.to_bits(), (end.x - start.x).to_bits());
        ComputedEdgeIntent::retained_binary64_pair(frame.longitude(), [start.x, end.x])
    };
    let geodesic = geographiclib_rs::Geodesic::new(6_378_137.0, 1.0 / 298.257_223_563);
    let metric = crate::crs::EllipsoidMetric::for_geodesic_at_frame(&geodesic, 0, frame);
    let metric_access = || Ok(&metric);
    let context = crate::geometry::SourceArcContext::new(
        crate::geometry::TopologyDomain::Geographic(frame),
        Some(&metric_access),
    );
    crate::geometry::publish_native_computed_chain(
        frame,
        points,
        closed_ring,
        &mut budget,
        &context,
        intent,
        |_, _| ComputedVertexIntent::Canonical,
    )
    .unwrap()
    .unwrap_or_else(|| points.to_vec())
}
