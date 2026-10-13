#[test]
fn canonical_full_band_ring_uses_its_chart_orientation() {
    let Shape::Polygon(polygon) = crate::io::parse_wkt(
        "POLYGON ((-180 -80, -60 -80, 60 -80, -180 -80, \
         -180 80, 60 80, -60 80, -180 80, -180 -80))",
    )
    .unwrap() else {
        unreachable!()
    };
    let frame = GeographicFrame::crs84();
    assert_eq!(
        super::super::emission::ring_roles::quotient_ring_role(
            frame,
            &polygon.shell
        )
        .unwrap(),
        Some(false),
    );
    assert_eq!(
        super::super::emission::ring_roles::quotient_ring_role(
            frame,
            &polygon.shell.reversed().unwrap(),
        )
        .unwrap(),
        Some(true),
    );
}

#[test]
fn q_whole_sphere_union_keeps_source_rings_and_never_searches() -> Result<()> {
    let source = Shape::Polygon(Polygon::new(
        Ring::try_from_closed(CoordSeq::try_from_points(&[
            Point::new_raw_xy(-180.0, -90.0),
            Point::new_raw_xy(-90.0, -90.0),
            Point::new_raw_xy(0.0, -90.0),
            Point::new_raw_xy(90.0, -90.0),
            Point::new_raw_xy(180.0, -90.0),
            Point::new_raw_xy(180.0, 90.0),
            Point::new_raw_xy(90.0, 90.0),
            Point::new_raw_xy(0.0, 90.0),
            Point::new_raw_xy(-90.0, 90.0),
            Point::new_raw_xy(-180.0, 90.0),
            Point::new_raw_xy(-180.0, -90.0),
        ])?)?,
        Vec::new(),
    ));
    let topology = test_topology(std::slice::from_ref(&source));
    let operation = OverlayOp::Union;
    let plan = build_q_plan(&topology, operation)?;
    assert!(plan.whole_sphere);
    let source_points = source_point_coordinates(&topology)?;
    let nearest = QCoordinates::build(&topology)?;
    let embedding =
        resolve_q_embedding_xy(&topology, &plan, &source_points, &nearest, operation.name())?;
    assert_eq!(embedding.choice, QEmbeddingChoice::Nearest);
    let result = publish_q_overlay(embedding)?;
    assert_eq!(
        crate::io::to_wkb(&result, None, false)?,
        crate::io::to_wkb(&source, None, false)?,
        "whole-sphere publication keeps the stored source ring"
    );
    Ok(())
}
