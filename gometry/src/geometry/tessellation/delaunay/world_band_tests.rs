use super::source::retained_constrained_source_plan;
use super::*;

fn geographic_ring(points: &[(f64, f64)]) -> Ring {
    Ring::closed(
        points
            .iter()
            .map(|&(x, y)| Point::new_raw_xy(x, y))
            .collect(),
    )
    .unwrap()
}

#[test]
fn retained_cdt_keeps_zero_turn_world_band_as_source_chart() {
    let ring = geographic_ring(&[
        (0.0, 20.0),
        (120.0, 20.0),
        (240.0, 20.0),
        (360.0, 20.0),
        (360.0, 10.0),
        (240.0, 10.0),
        (120.0, 10.0),
        (0.0, 10.0),
        (0.0, 20.0),
    ]);
    let frame = GeographicFrame::crs84();
    let summary = RingSummary::try_new(frame, &GeographicRing::from_ring(&ring)).unwrap();
    assert_eq!(summary.chart_turn, 0);
    assert!(summary.exact_axis_band);
    assert!(matches!(summary.domain, RingDomain::FullLongitude(_)));

    let shape =
        crate::geometry::Shape::Polygon(crate::geometry::Polygon::new(ring.clone(), Vec::new()));
    let chart = PointSelectionChart::try_from_shape(frame, &shape, shape.bounds()).unwrap();
    let source_plan =
        retained_constrained_source_plan(frame, chart.as_ref(), &[(RingOwner::new(0, 0), &ring)])
            .unwrap()
            .expect("a zero-turn axis band is a continuous source chart");
    let area = exact::ExactRational::from_dyadic(source_plan.chart_area.unwrap())
        .round()
        .unwrap();
    assert_eq!(area, 3_600.0);

    let plan = prepare_constrained_triangle_plan(
        [(RingOwner::new(0, 0), &ring)].into_iter(),
        3_600.0,
        CdtRefinement::Off,
        crate::geometry::TopologyDomain::Geographic(frame),
        chart.as_ref(),
    )
    .unwrap();
    assert_eq!(plan.source_constraints.len(), 8);
    assert_eq!(plan.constraint_pieces.len(), 8);
    assert_eq!(plan.faces.len(), 6);
    let source_uses = plan
        .face_uses
        .iter()
        .flat_map(|face| &face.sides)
        .filter_map(|side| match side.boundary.as_ref() {
            Some(CdtSideBoundaryUse::Source(source)) => plan.source_uses.get(*source),
            Some(CdtSideBoundaryUse::Restriction(_)) | None => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(source_uses.len(), 8);
    let zero = exact::ExactRational::zero();
    let one = exact::ExactRational::one();
    for source_id in 0..8 {
        let source = source_uses
            .iter()
            .find(|source| source.source == source_id)
            .expect("each directed world-band boundary edge remains source-owned");
        assert_eq!(source.owner, RingOwner::new(0, 0));
        assert!(
            (source.start == zero && source.end == one)
                || (source.start == one && source.end == zero),
            "each source use retains its complete directed interval"
        );
    }
    plan.validate_certificate().unwrap();
}
