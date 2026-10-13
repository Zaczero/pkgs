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
    publish_computed_chain(
        frame,
        points,
        closed_ring,
        &mut budget,
        |index, start, end| {
            let step = *steps
                .get(index)
                .expect("each source edge has one retained test relation");
            assert_eq!(step.to_bits(), (end.x - start.x).to_bits());
            ComputedEdgeIntent::retained_binary64_pair(frame.longitude(), [start.x, end.x])
        },
    )
    .unwrap()
    .unwrap_or_else(|| points.to_vec())
}
