use super::*;

fn south_polar_fixture() -> (Ring, GeographicFrame, Option<PointSelectionChart>) {
    let ring = Ring::closed(
        [
            (0.0, -80.0),
            (90.0, -80.0),
            (180.0, -80.0),
            (-90.0, -80.0),
            (0.0, -80.0),
        ]
        .into_iter()
        .map(|(x, y)| Point::new_raw_xy(x, y))
        .collect(),
    )
    .unwrap();
    let shape =
        crate::geometry::Shape::Polygon(crate::geometry::Polygon::new(ring.clone(), Vec::new()));
    let frame = GeographicFrame::crs84();
    let chart = PointSelectionChart::try_from_shape(frame, &shape, shape.bounds()).unwrap();
    (ring, frame, chart)
}

fn south_polar_cap_plan() -> ConstrainedMeshPlan {
    let (ring, frame, chart) = south_polar_fixture();
    prepare_constrained_triangle_plan(
        [(RingOwner::new(0, 0), &ring)].into_iter(),
        3_600.0,
        CdtRefinement::Off,
        crate::geometry::TopologyDomain::Geographic(frame),
        chart.as_ref(),
    )
    .unwrap()
}

#[test]
fn retained_cdt_polar_cap_translates_holes_into_the_restricted_chart() {
    let shell = south_polar_fixture().0;
    let hole = Ring::closed(
        [
            (90.0, -82.0),
            (110.0, -82.0),
            (110.0, -84.0),
            (90.0, -84.0),
            (90.0, -82.0),
        ]
        .into_iter()
        .map(|(x, y)| Point::new_raw_xy(x, y))
        .collect(),
    )
    .unwrap();
    let shape =
        crate::geometry::Shape::Polygon(crate::geometry::Polygon::new(shell.clone(), vec![
            hole.clone(),
        ]));
    let frame = GeographicFrame::crs84();
    let chart = PointSelectionChart::try_from_shape(frame, &shape, shape.bounds()).unwrap();
    let rings = [
        (RingOwner::new(0, 0), &shell),
        (RingOwner::new(0, 1), &hole),
    ];

    let source_plan = retained_constrained_source_plan(frame, chart.as_ref(), &rings)
        .unwrap()
        .expect("the hole is translated into the full-turn shell's retained chart");
    assert_eq!(
        exact::ExactRational::from_dyadic(source_plan.chart_area.unwrap())
            .round()
            .unwrap(),
        3_560.0
    );

    let plan = prepare_constrained_triangle_plan(
        rings.into_iter(),
        3_560.0,
        CdtRefinement::Off,
        crate::geometry::TopologyDomain::Geographic(frame),
        chart.as_ref(),
    )
    .unwrap();
    assert_eq!(plan.source_constraints.len(), 8);
    assert_eq!(plan.constraint_pieces.len(), 8);
    assert_eq!(plan.restrictions.len(), 3);
    assert_eq!(
        plan.face_uses
            .iter()
            .flat_map(|face| &face.sides)
            .filter(|side| {
                matches!(
                    side.boundary.as_ref(),
                    Some(CdtSideBoundaryUse::Source(source))
                        if plan.source_uses.get(*source).is_some_and(|use_| use_.owner.ring() == 1)
                )
            })
            .count(),
        4
    );
    plan.validate_certificate().unwrap();
}

#[expect(
    clippy::float_cmp,
    reason = "synthetic pole sites retain the exact represented chart-pole latitude"
)]
#[test]
fn retained_cdt_full_turn_south_cap_keeps_source_ranges_and_restriction_uses() {
    use crate::geographic::{FullTurnPole, RingDomain};
    use crate::geometry::publication::{ChartCutSide, ChartRestriction, SyntheticChartVertex};

    let (ring, frame, chart) = south_polar_fixture();
    let summary = RingSummary::try_new(frame, &GeographicRing::from_ring(&ring)).unwrap();
    assert_eq!(summary.chart_turn, 1);
    assert!(matches!(summary.domain, RingDomain::FullLongitude(_)));
    assert_eq!(summary.bounded_pole(), Some(ChartPole::South));
    assert_eq!(
        summary.full_turn_pole(),
        Some(FullTurnPole::Bounded(ChartPole::South))
    );

    let source_plan =
        retained_constrained_source_plan(frame, chart.as_ref(), &[(RingOwner::new(0, 0), &ring)])
            .unwrap()
            .expect("a bounded full-turn shell receives its retained source chart");
    assert_eq!(
        exact::ExactRational::from_dyadic(source_plan.chart_area.unwrap())
            .round()
            .unwrap(),
        3_600.0
    );

    let plan = south_polar_cap_plan();
    assert_eq!(plan.source_constraints.len(), 4);
    assert_eq!(plan.constraint_pieces.len(), 4);
    assert_eq!(plan.restrictions.len(), 3);
    assert_eq!(plan.restriction_pieces.len(), 3);
    assert!(
        plan.restrictions
            .iter()
            .all(|use_| use_.owner == RingOwner::new(0, 0))
    );
    assert!(
        plan.restriction_pieces
            .iter()
            .all(|piece| piece.owner == RingOwner::new(0, 0))
    );
    assert_eq!(plan.faces.len(), 5);
    let source_uses = plan
        .face_uses
        .iter()
        .flat_map(|face| &face.sides)
        .filter_map(|side| match side.boundary.as_ref() {
            Some(CdtSideBoundaryUse::Source(source)) => plan.source_uses.get(*source),
            Some(CdtSideBoundaryUse::Restriction(_)) | None => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(source_uses.len(), 4);
    for source_id in 0..4 {
        let source = source_uses
            .iter()
            .find(|source| source.source == source_id)
            .expect("every authored cap edge remains a source use");
        assert_eq!(source.owner, RingOwner::new(0, 0));
        assert!(
            (source.start == exact::ExactRational::zero()
                && source.end == exact::ExactRational::one())
                || (source.start == exact::ExactRational::one()
                    && source.end == exact::ExactRational::zero())
        );
    }

    let restriction_uses = plan
        .face_uses
        .iter()
        .flat_map(|face| &face.sides)
        .filter_map(|side| match side.boundary.as_ref() {
            Some(CdtSideBoundaryUse::Restriction(piece)) => plan.restriction_pieces.get(*piece),
            Some(CdtSideBoundaryUse::Source(_)) | None => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(restriction_uses.len(), 3);
    assert_eq!(
        restriction_uses
            .iter()
            .filter(|piece| piece.use_.restriction == ChartRestriction::CutChord)
            .count(),
        2
    );
    for side in [ChartCutSide::West, ChartCutSide::East] {
        assert!(restriction_uses.iter().any(|piece| {
            piece.use_.restriction == ChartRestriction::CutChord
                && matches!(
                    (piece.use_.start, piece.use_.end),
                    (
                        SyntheticChartVertex::Cut { side: start, .. },
                        SyntheticChartVertex::Cut { side: end, .. }
                    ) if start == side && end == side
                )
        }));
    }
    let cap = restriction_uses
        .iter()
        .find(|piece| {
            piece.use_.restriction
                == ChartRestriction::PolarCap {
                    pole: ChartPole::South,
                }
        })
        .expect("the synthetic south-polar cap edge is selected");
    assert!(matches!(
        (cap.use_.start, cap.use_.end),
        (
            SyntheticChartVertex::Pole {
                pole: ChartPole::South,
                ..
            },
            SyntheticChartVertex::Pole {
                pole: ChartPole::South,
                ..
            }
        )
    ));
    assert_ne!(cap.use_.start, cap.use_.end);
    assert!(
        (cap.start == exact::ExactRational::zero() && cap.end == exact::ExactRational::one())
            || (cap.start == exact::ExactRational::one()
                && cap.end == exact::ExactRational::zero())
    );
    assert!(plan.vertices.iter().rev().take(2).all(|point| {
        point.axes == CoordinateAxes::XY && point.y == frame.chart_pole_latitude(ChartPole::South)
    }));
    for site in plan.vertices.len().saturating_sub(2)..plan.vertices.len() {
        assert!(
            plan.site_chart_location(site).is_none(),
            "synthetic pole vertices do not inherit source-site identity"
        );
    }
    assert!(
        plan.provenance
            .iter()
            .rev()
            .take(2)
            .all(|origin| { matches!(origin, cdt::NodeProvenance::ChartRestrictionSite) })
    );
    plan.validate_certificate().unwrap();
}

#[test]
fn retained_cdt_rejects_an_omitted_polar_restriction_use() {
    let mut plan = south_polar_cap_plan();
    let side = plan
        .face_uses
        .iter_mut()
        .flat_map(|face| &mut face.sides)
        .find(|side| {
            matches!(
                side.boundary.as_ref(),
                Some(CdtSideBoundaryUse::Restriction(_))
            )
        })
        .expect("the full-turn cap has a selected synthetic boundary use");
    side.boundary = None;
    assert!(
        plan.validate_certificate().is_err(),
        "omitting a source-less chart-restriction use must invalidate the prepared plan"
    );
}
