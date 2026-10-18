#[test]
fn public_delaunay_keeps_collinear_empty_contract() {
    let source = Shape::MultiPoint(
        crate::geometry::CoordSeq::try_from_points(&[xy(0.0, 0.0), xy(1.0, 0.0), xy(2.0, 0.0)])
            .unwrap()
            .into(),
    );
    assert!(source.delaunay_triangles().unwrap().is_empty());
}

#[test]
fn triangle_sink_rejects_before_shape_allocation() {
    let mut budget = ExpansionBudget::new("triangulate", "method");
    budget.add(GENERATED_ITEM_LIMIT).unwrap();
    let mut sink = BudgetedTriangleSink::new(&mut budget);
    sink.emit(xy(0.0, 0.0), xy(1.0, 0.0), xy(0.0, 1.0))
        .unwrap_err();
    assert!(sink.into_shapes().is_empty());
}

#[test]
fn packed_triangle_sink_rejects_before_allocation() {
    let mut budget = ExpansionBudget::new("triangulate", "method");
    budget.add(GENERATED_ITEM_LIMIT).unwrap();
    let mut sink = BudgetedPackedTriangleSink::new(&mut budget);
    sink.emit(xy(0.0, 0.0), xy(1.0, 0.0), xy(0.0, 1.0))
        .unwrap_err();
    assert!(sink.into_vertices().is_empty());
}

#[test]
fn triangulation_budgets_reject_before_reserving_generated_vertices() {
    let mut delaunay_budget = ExpansionBudget::new("triangulate", "method");
    delaunay_budget.add(GENERATED_ITEM_LIMIT).unwrap();
    assert_eq!(take_delaunay_output_reserves(), 0);
    Shape::MultiPoint(vec![xy(0.0, 0.0), xy(1.0, 0.0), xy(0.0, 1.0)].into())
        .delaunay_triangle_vertices_budgeted(&mut delaunay_budget)
        .unwrap_err();
    assert_eq!(take_delaunay_output_reserves(), 0);

    let source = Shape::Polygon(polygon(
        vec![
            xy(0.0, 0.0),
            xy(1.0, 0.0),
            xy(1.0, 1.0),
            xy(0.0, 1.0),
            xy(0.0, 0.0),
        ],
        Vec::new(),
    ));
    let mut constrained_budget = ExpansionBudget::new("triangulate", "min_angle/max_area");
    constrained_budget.add(GENERATED_ITEM_LIMIT).unwrap();
    assert_eq!(take_constrained_output_reserves(), 0);
    source
        .constrained_delaunay_vertices_budgeted(CdtRefinement::Off, &mut constrained_budget)
        .unwrap_err();
    assert_eq!(take_constrained_output_reserves(), 0);
}

#[test]
fn delaunay_triangles_accept_mixed_axis_collection_without_panicking() {
    let shape = Shape::geometry_collection(vec![
        Shape::Point(xy(0.0, 0.0)),
        Shape::Point(xyz(1.0, 0.0, 1.0)),
        Shape::Point(xy(0.0, 1.0)),
        Shape::Point(xyz(1.0, 1.0, 2.0)),
    ])
    .expect("test collection is valid");
    let triangles = shape
        .delaunay_triangles()
        .expect("mixed-axis collection triangulates row-wise");
    assert!(!triangles.is_empty());
    for triangle in triangles {
        let corners = triangle_corners(&triangle).expect("triangle polygon");
        assert_eq!(corners.len(), 3);
    }
}

#[test]
fn build_area_strict_rejects_mixed_output_axes() {
    let square = |points: Vec<Point>| {
        Shape::LineString(LineSeq::from_proven(
            CoordSeq::try_from_points(&points).unwrap(),
        ))
    };
    let source = Shape::geometry_collection(vec![
        square(vec![
            xy(0.0, 0.0),
            xy(2.0, 0.0),
            xy(2.0, 2.0),
            xy(0.0, 2.0),
            xy(0.0, 0.0),
        ]),
        square(vec![
            xyz(10.0, 0.0, 7.0),
            xyz(12.0, 0.0, 8.0),
            xyz(12.0, 2.0, 9.0),
            xyz(10.0, 2.0, 10.0),
            xyz(10.0, 0.0, 7.0),
        ]),
    ])
    .expect("mixed-axis collection is valid");

    let error = source
        .build_area_with_context(true, &crate::geometry::SourceArcContext::cartesian())
        .expect_err("strict ordinate carry must reject mixed output axes");
    assert!(matches!(
        error.kind(),
        ErrorKind::Geometry(GeometryErrorKind::OrdinateDropped(operation))
            if operation.as_ref() == "build_area"
    ));
}

#[test]
fn polygonize_full_strict_scopes_diagnostic_axes_to_source_rows() {
    let z_line = Shape::LineString(LineSeq::from_proven(
        CoordSeq::try_from_points(&[xyz(0.0, 0.0, 1.0), xyz(1.0, 0.0, 2.0)]).unwrap(),
    ));
    let m_line = Shape::LineString(LineSeq::from_proven(
        CoordSeq::try_from_points(&[
            Point::new_axes(0.0, 0.0, ZOrdinate(None), MOrdinate(Some(10.0)))
                .expect("finite test coordinate"),
            Point::new_axes(-1.0, 0.0, ZOrdinate(None), MOrdinate(Some(20.0)))
                .expect("finite test coordinate"),
        ])
        .unwrap(),
    ));
    let sources = [&z_line, &m_line];

    let full = Shape::polygonize_full_all_with_context(
        &sources,
        true,
        &crate::geometry::SourceArcContext::cartesian(),
    )
    .expect("each diagnostic row has one source axis");

    assert_eq!(full.dangles[0][0].coordinate_axes(), "XYZ");
    assert_eq!(full.dangles[1][0].coordinate_axes(), "XYM");
}

#[test]
fn polygonize_keeps_collinear_source_nodes_and_zero_ordinates() {
    let point = |x, y, m| {
        Point::new_axes(x, y, ZOrdinate(Some(0.0)), MOrdinate(Some(m)))
            .expect("finite XYZM source coordinate")
    };
    let lower_left = point(0.0, 0.0, 10.0);
    let forward_points = [
        lower_left,
        point(1.0, 0.0, 15.0),
        point(2.0, 0.0, 20.0),
        point(2.0, 2.0, 30.0),
        point(0.0, 2.0, 40.0),
        lower_left,
    ];
    let reverse_points = [
        lower_left,
        point(0.0, 2.0, 40.0),
        point(2.0, 2.0, 30.0),
        point(2.0, 0.0, 20.0),
        point(1.0, 0.0, 15.0),
        lower_left,
    ];
    let forward = Shape::LineString(LineSeq::from_proven(
        CoordSeq::try_from_points(&forward_points).unwrap(),
    ));
    let reverse = Shape::LineString(LineSeq::from_proven(
        CoordSeq::try_from_points(&reverse_points).unwrap(),
    ));
    for source in [&forward, &reverse] {
        let polygons = Shape::polygonize_all(&[source], true)
            .expect("a closed XYZM source ring polygonizes strictly");
        assert_eq!(polygons.len(), 1);
        let polygon = polygons
            .first()
            .and_then(|shape| match shape {
                Shape::Polygon(polygon) => Some(polygon),
                Shape::Point(_)
                | Shape::MultiPoint(_)
                | Shape::LineString(_)
                | Shape::MultiLineString(_)
                | Shape::MultiPolygon(_)
                | Shape::GeometryCollection(_)
                | Shape::Empty(..) => None,
            })
            .expect("one source ring produces one polygon");
        assert_eq!(polygon.shell.coords().xs(), &[0.0, 1.0, 2.0, 2.0, 0.0, 0.0]);
        assert_eq!(polygon.shell.coords().ys(), &[0.0, 0.0, 0.0, 2.0, 2.0, 0.0]);
        assert_eq!(polygon.shell.coords().zs(), Some(&[0.0; 6][..]));
        assert_eq!(
            polygon.shell.coords().ms(),
            Some(&[10.0, 15.0, 20.0, 30.0, 40.0, 10.0][..])
        );
        assert!(polygon.holes.is_empty());
    }
}

#[rstest::rstest]
#[case::exponent_77(77)]
#[case::exponent_159(159)]
#[case::exponent_200(200)]
#[case::exponent_300(300)]
fn reciprocal_delaunay_legalizes_with_the_source_incircle(#[case] exponent: i32) {
    let large = 10.0_f64.powi(exponent);
    let tiny = 10.0_f64.powi(exponent.wrapping_neg());
    for swap_axes in [false, true] {
        let point = |x, y| {
            if swap_axes {
                Point::new_raw_xy(y, x)
            } else {
                Point::new_raw_xy(x, y)
            }
        };
        let points = [
            point(-large, 0.0),
            point(0.0, -tiny),
            point(large, 0.0),
            point(0.0, 2.0 * tiny),
        ];
        let triangulation = super::delaunay::delaunay_triangulation(&points).unwrap();
        let has_edge = |left, right| {
            triangulation
                .triangles
                .as_chunks::<3>()
                .0
                .iter()
                .any(|triangle| {
                    triangle
                        .iter()
                        .zip(triangle.iter().cycle().skip(1))
                        .any(|(&start, &end)| {
                            (start == left && end == right) || (start == right && end == left)
                        })
                })
        };
        assert!(has_edge(1, 3), "exp={exponent} swap_axes={swap_axes}");
        assert!(!has_edge(0, 2), "exp={exponent} swap_axes={swap_axes}");
    }
}
