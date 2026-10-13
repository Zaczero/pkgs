#[test]
fn q_intersection_classifies_a_shared_ring_edge_as_areal_linework() -> Result<()> {
    let frame = GeographicFrame::crs84();
    let left = crate::io::parse_wkt("POLYGON ((179 0, -179 0, -179.8 -1, 179 0))")?;
    let right = crate::io::parse_wkt("POLYGON ((-179 0, 179 0, 179.8 1, -179 0))")?;
    let shapes = [left, right];
    let inputs = GeographicInputs::try_from_shapes(frame, shapes.iter())
        .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
    let topology =
        ExactGeographicTopology::from_views_as_set(frame, inputs.views().iter().copied())?;
    let plan = build_q_plan(&topology, OverlayOp::Intersection)?;
    assert!(!plan.line_paths.is_empty());
    assert!(
        plan.line_paths
            .iter()
            .all(|line| line.origin == QLineOrigin::ArealBoundary)
    );
    assert!(
        plan.fragment_recipes
            .as_slice()
            .iter()
            .any(|recipe| recipe.use_kind == SelectedUse::ArealBoundaryLine)
    );
    Ok(())
}

#[test]
fn synthetic_seam_shared_edge_intersection_has_a_native_result() -> Result<()> {
    let shapes = [
        crate::io::parse_wkt("POLYGON ((179 0, -179 0, -179.8 -1, 179 0))")?,
        crate::io::parse_wkt("POLYGON ((-179 0, 179 0, 179.8 1, -179 0))")?,
    ];
    let topology = test_topology(&shapes);
    let intersection = exact_geographic_overlay::<crate::crs::GeodesicMetric>(
        &topology,
        OverlayOp::Intersection,
        CoordinateAxes::XY,
        false,
        None,
    )?;

    assert!(!line_edges(&intersection).is_empty());
    let union = exact_geographic_overlay::<crate::crs::GeodesicMetric>(
        &topology,
        OverlayOp::Union,
        CoordinateAxes::XY,
        false,
        None,
    )?;
    assert!(matches!(union, Shape::Polygon(_) | Shape::MultiPolygon(_)));
    Ok(())
}

#[test]
fn synthetic_sliver_dissolve_has_a_native_result() -> Result<()> {
    let seam = crate::io::parse_wkt("POLYGON ((179 0, -179 0, -179 1, 179 1, 179 0))")?;
    let argentina = crate::io::parse_wkt(
        "MULTIPOLYGON (((-58.10222180941994 -33.010937543229446, -58.084289 -33.0009615, -58.085477253434156 -32.99, -58.089648910878246 -32.99, -58.0878711 -32.9965279, -58.10222180941994 -33.010937543229446)), ((-58.11 -33.00166081596568, -58.10769305699579 -32.99, -58.11 -32.99, -58.11 -33.00166081596568)))",
    )?;
    let uruguay = crate::io::parse_wkt(
        "POLYGON ((-58.11 -33.02, -58.08 -33.02, -58.08 -32.99, -58.085477253434156 -32.99, -58.084289 -33.0009615, -58.10256757394891 -33.011129892448544, -58.0878711 -32.9965279, -58.089648910878246 -32.99, -58.10765630407339 -32.99, -58.11 -33.001904214459806, -58.11 -33.02))",
    )?;
    let topology = test_topology(&[seam, argentina, uruguay]);
    let union = exact_geographic_overlay::<crate::crs::GeodesicMetric>(
        &topology,
        OverlayOp::Union,
        CoordinateAxes::XY,
        false,
        None,
    )?;

    assert!(matches!(union, Shape::Polygon(_) | Shape::MultiPolygon(_)));
    Ok(())
}
