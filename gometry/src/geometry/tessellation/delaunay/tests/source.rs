use super::super::source::retained_constrained_source_plan;
use super::*;

#[test]
fn constrained_source_plan_keeps_first_full_signed_zero_point_and_source_ordinals() {
    let point = |x: f64, y: f64, z: f64, m: f64| {
        Point::new_raw_axes(x, y, ZOrdinate(Some(z)), MOrdinate(Some(m)))
    };
    let first = point(-0.0, 0.0, 7.0, 70.0);
    let duplicate = point(0.0, -0.0, 99.0, 990.0);
    let zero_edge_ring =
        crate::geometry::Ring::closed(vec![first, duplicate, first, first]).unwrap();
    let source_ring = crate::geometry::Ring::closed(vec![
        first,
        duplicate,
        point(2.0, 0.0, 8.0, 80.0),
        point(2.0, 2.0, 9.0, 90.0),
        point(-2.0, 2.0, 10.0, 100.0),
        point(-2.0, 0.0, 11.0, 110.0),
        first,
    ])
    .unwrap();
    let rings = [
        (RingOwner::new(7, 0), &zero_edge_ring),
        (RingOwner::new(7, 1), &source_ring),
    ];

    let plan =
        constrained_source_plan(&rings, crate::geometry::TopologyDomain::Cartesian, None).unwrap();

    assert_eq!(
        plan.vertices
            .iter()
            .map(|point| (point.x.to_bits(), point.y.to_bits(), point.z(), point.m(),))
            .collect::<Vec<_>>(),
        [
            (
                (-2.0_f64).to_bits(),
                0.0_f64.to_bits(),
                Some(11.0),
                Some(110.0)
            ),
            (
                (-2.0_f64).to_bits(),
                2.0_f64.to_bits(),
                Some(10.0),
                Some(100.0)
            ),
            (
                (-0.0_f64).to_bits(),
                0.0_f64.to_bits(),
                Some(7.0),
                Some(70.0)
            ),
            (2.0_f64.to_bits(), 0.0_f64.to_bits(), Some(8.0), Some(80.0)),
            (2.0_f64.to_bits(), 2.0_f64.to_bits(), Some(9.0), Some(90.0)),
        ]
    );
    assert_eq!(
        plan.sources
            .iter()
            .map(|source| (source.id, source.start_site, source.end_site))
            .collect::<Vec<_>>(),
        [(0, 2, 3), (1, 3, 4), (2, 4, 1), (3, 1, 0), (4, 0, 2)]
    );
    assert_eq!(
        plan.sources
            .iter()
            .map(|source| (
                source.start.z(),
                source.start.m(),
                source.end.z(),
                source.end.m()
            ))
            .collect::<Vec<_>>(),
        [
            (Some(7.0), Some(70.0), Some(8.0), Some(80.0)),
            (Some(8.0), Some(80.0), Some(9.0), Some(90.0)),
            (Some(9.0), Some(90.0), Some(10.0), Some(100.0)),
            (Some(10.0), Some(100.0), Some(11.0), Some(110.0)),
            (Some(11.0), Some(110.0), Some(7.0), Some(70.0)),
        ]
    );
    assert_eq!(
        cdt::source_ring_owner(0, &plan.source_rings),
        Some(RingOwner::new(7, 1)),
        "the source row follows a ring with no nonzero XY edges"
    );
}

#[test]
fn retained_cdt_chart_keeps_hole_faces_and_live_source_uses() {
    let shell = geographic_ring(&[
        (170.0, -10.0),
        (-170.0, -10.0),
        (-170.0, 10.0),
        (170.0, 10.0),
        (170.0, -10.0),
    ]);
    let hole = geographic_ring(&[
        (175.0, -2.0),
        (-175.0, -2.0),
        (-175.0, 2.0),
        (175.0, 2.0),
        (175.0, -2.0),
    ]);
    let rings = [
        (RingOwner::new(0, 0), &shell),
        (RingOwner::new(0, 1), &hole),
    ];
    let frame = GeographicFrame::crs84();
    let shape =
        crate::geometry::Shape::Polygon(crate::geometry::Polygon::new(shell.clone(), vec![
            hole.clone(),
        ]));
    let chart = PointSelectionChart::try_from_shape(frame, &shape, shape.bounds()).unwrap();
    let source_plan = retained_constrained_source_plan(frame, chart.as_ref(), &rings)
        .unwrap()
        .expect("zero-turn seam rings have one continuous chart");
    let area = exact::ExactRational::from_dyadic(source_plan.chart_area.unwrap())
        .round()
        .unwrap();
    assert_eq!(area, 360.0);

    let plan = prepare_constrained_triangle_plan(
        rings.into_iter(),
        6_800.0,
        CdtRefinement::Off,
        crate::geometry::TopologyDomain::Geographic(frame),
        chart.as_ref(),
    )
    .unwrap();
    let first_source = plan.source_constraints.first().unwrap();
    assert!(plan.site_chart_location(first_source.start_site).is_some());
    assert!(
        plan.source_chart_location(first_source.start_occurrence)
            .is_some()
    );
    assert_eq!(plan.faces.len(), 8);
    assert_eq!(plan.face_uses.len(), 8);
    assert_eq!(plan.source_constraints.len(), 8);
    assert_eq!(
        plan.face_uses
            .iter()
            .flat_map(|face| &face.sides)
            .filter_map(|side| match side.boundary.as_ref() {
                Some(CdtSideBoundaryUse::Source(source)) => plan.source_uses.get(*source),
                Some(CdtSideBoundaryUse::Restriction(_)) | None => None,
            })
            .filter(|source| source.owner.ring() == 1)
            .count(),
        4
    );
    assert!(
        plan.face_uses
            .iter()
            .flat_map(|face| &face.sides)
            .filter(|side| side.boundary.is_some())
            .all(|side| !side.twin_selected)
    );
    let selected_sides = plan
        .face_uses
        .iter()
        .flat_map(|face| &face.sides)
        .collect::<Vec<_>>();
    assert!(
        selected_sides.iter().any(|side| side.twin_selected),
        "the selected mesh contains shared interior sides"
    );
    for side in &selected_sides {
        let selected_twin = side.twin.and_then(|twin| {
            selected_sides
                .iter()
                .copied()
                .find(|candidate| candidate.halfedge == twin)
        });
        assert_eq!(
            side.twin_selected,
            selected_twin.is_some(),
            "selected-side twin status matches the captured selected-face set"
        );
        if let Some(twin) = selected_twin {
            assert_eq!(twin.twin, Some(side.halfedge));
            assert_eq!(
                (twin.start_site, twin.end_site),
                (side.end_site, side.start_site)
            );
        }
    }
    plan.validate_certificate().unwrap();
}

#[test]
fn retained_cdt_distinguishes_sites_with_identical_lifted_proposals() {
    let next = 0.2_f64.next_up();
    let ring = geographic_ring(&[
        (170.0, 0.0),
        (-170.0, 0.0),
        (-100.0, 0.0),
        (0.2, 0.0),
        (next, 0.0),
        (next, 1.0),
        (0.2, 1.0),
        (-100.0, 1.0),
        (-170.0, 1.0),
        (170.0, 1.0),
        (170.0, 0.0),
    ]);
    let rings = [(RingOwner::new(0, 0), &ring)];
    let frame = GeographicFrame::crs84();
    let shape =
        crate::geometry::Shape::Polygon(crate::geometry::Polygon::new(ring.clone(), Vec::new()));
    let chart = PointSelectionChart::try_from_shape(frame, &shape, shape.bounds()).unwrap();
    let plan = prepare_constrained_triangle_plan(
        rings.into_iter(),
        0.0,
        CdtRefinement::Off,
        crate::geometry::TopologyDomain::Geographic(frame),
        chart.as_ref(),
    )
    .unwrap();

    assert_eq!(plan.source_constraints.len(), 10);
    let adjacent = plan
        .source_constraints
        .iter()
        .find(|source| {
            source.start.x.to_bits() == 0.2_f64.to_bits()
                && source.start.y.to_bits() == 0.0_f64.to_bits()
                && source.end.x.to_bits() == next.to_bits()
                && source.end.y.to_bits() == 0.0_f64.to_bits()
        })
        .expect("the adjacent lifted source edge remains admitted");
    assert_ne!(adjacent.start_site, adjacent.end_site);
    assert_eq!(
        plan.chart_proposal(adjacent.start_site).x,
        plan.chart_proposal(adjacent.end_site).x,
        "the binary64 chart proposal rounds the adjacent exact sites together"
    );
    let start = plan.site_chart_location(adjacent.start_site).unwrap();
    let end = plan.site_chart_location(adjacent.end_site).unwrap();
    assert!(start.compare_chart_xy(end).is_lt());
    plan.validate_certificate().unwrap();
}
