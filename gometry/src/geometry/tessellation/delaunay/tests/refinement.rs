use super::*;

#[test]
fn retained_cdt_refinement_keeps_chart_centers_and_boundary_splits() {
    let ring = geographic_ring(&[
        (170.0, 0.0),
        (-170.0, 0.0),
        (-170.0, 20.0),
        (170.0, 20.0),
        (170.0, 0.0),
    ]);
    let shape =
        crate::geometry::Shape::Polygon(crate::geometry::Polygon::new(ring.clone(), Vec::new()));
    let frame = GeographicFrame::crs84();
    let chart = PointSelectionChart::try_from_shape(frame, &shape, shape.bounds()).unwrap();
    let plan = prepare_constrained_triangle_plan(
        [(RingOwner::new(0, 0), &ring)].into_iter(),
        6_800.0,
        CdtRefinement::On {
            min_angle: None,
            max_area: Some(crate::Positive::try_new("max_area", 25.0).unwrap()),
        },
        crate::geometry::TopologyDomain::Geographic(frame),
        chart.as_ref(),
    )
    .unwrap();

    assert!(plan.provenance.iter().any(|origin| {
        matches!(origin, cdt::NodeProvenance::InteriorRefinement {
            construction: cdt::RefinementConstruction::Circumcenter,
            ..
        })
    }));
    assert!(
        plan.provenance
            .iter()
            .any(|origin| matches!(origin, cdt::NodeProvenance::ConstraintSplit { .. }))
    );
    assert!(plan.constraint_pieces.len() > plan.source_constraints.len());
    plan.validate_certificate().unwrap();
}

#[test]
fn retained_cdt_uses_chart_area_to_drop_an_inert_max_area_request() {
    let ring = geographic_ring(&[
        (170.0, 0.0),
        (-170.0, 0.0),
        (-170.0, 10.0),
        (170.0, 8.0),
        (170.0, 0.0),
    ]);
    let shape =
        crate::geometry::Shape::Polygon(crate::geometry::Polygon::new(ring.clone(), Vec::new()));
    let chart =
        PointSelectionChart::try_from_shape(GeographicFrame::crs84(), &shape, shape.bounds())
            .unwrap();
    let refinement = CdtRefinement::On {
        min_angle: None,
        max_area: Some(crate::Positive::try_new("max_area", 1_000.0).unwrap()),
    };
    let plan = prepare_constrained_triangle_plan(
        [(RingOwner::new(0, 0), &ring)].into_iter(),
        6_120.0,
        refinement,
        crate::geometry::TopologyDomain::Geographic(GeographicFrame::crs84()),
        chart.as_ref(),
    )
    .unwrap();
    assert_eq!(plan.refinement, CdtRefinement::Off);
    assert_eq!(plan.faces.len(), 2);
    plan.validate_certificate().unwrap();
}
