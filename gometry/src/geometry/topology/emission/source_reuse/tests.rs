#[test]
fn q_source_only_points_keep_admitted_latitude_bits_and_chart_identity() {
    let frame = GeographicFrame::crs84();
    let pole = frame.pole();
    for (source_y, identity) in [
        (
            -0.0,
            EmittedNodeKey::Coordinate(0.0_f64.to_bits(), 0.0_f64.to_bits()),
        ),
        (pole.next_up(), EmittedNodeKey::NorthPole),
    ] {
        let source = Point::new_raw_xy(0.0, source_y);
        let output = q_union_one_source(&Shape::Point(source), frame);
        let emitted = emitted_single_point(&output);

        assert_eq!(emitted.y.to_bits(), source_y.to_bits());
        assert_eq!(emitted_node_key(frame, emitted.xy()).unwrap(), identity);
        assert_eq!(emitted_node_key(frame, source.xy()).unwrap(), identity);
    }
}

#[test]
fn q_source_only_whole_sphere_ring_keeps_source_latitude_bits() {
    let frame = GeographicFrame::crs84();
    let source_points = [
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
        Point::new_raw_xy(-180.0, -0.0),
        Point::new_raw_xy(-180.0, -90.0),
    ];
    let shell = Ring::try_from_closed(CoordSeq::try_from_points(&source_points).unwrap())
        .expect("closed pole-to-pole source ring");
    let source = Shape::Polygon(Polygon::new(shell, Vec::new()));
    let inputs = GeographicInputs::try_from_shapes(frame, std::iter::once(&source))
        .expect("source admits");
    let topology =
        ExactGeographicTopology::from_views_as_set(frame, inputs.views().iter().copied())
            .expect("Q topology builds");

    let plan = build_q_plan(&topology, OverlayOp::Union).expect("build Q plan");
    assert!(matches!(
        &plan.polygons,
        PolygonPlan::WholeSphere(source_polygons) if source_polygons == &[0]
    ));
    let output = exact_geographic_overlay::<crate::crs::GeodesicMetric>(
        &topology,
        OverlayOp::Union,
        source.axes(),
        false,
        None,
    )
    .expect("Q source-only union emits");
    let Shape::Polygon(polygon) = output else {
        panic!("Q source-only whole-sphere ring remains a polygon");
    };
    let emitted = polygon.shell.coords().iter_coords().collect::<Vec<_>>();
    let mut expected_bits = source_points
        .iter()
        .map(|point| (point.x.to_bits(), point.y.to_bits()))
        .collect::<Vec<_>>();
    let mut emitted_bits = emitted
        .iter()
        .map(|point| (point.x.to_bits(), point.y.to_bits()))
        .collect::<Vec<_>>();
    expected_bits.sort_unstable();
    emitted_bits.sort_unstable();
    assert_eq!(emitted_bits, expected_bits);
    assert_eq!(
        emitted.first().unwrap().y.to_bits(),
        emitted.last().unwrap().y.to_bits(),
        "the full closing source record is retained"
    );
    assert_eq!(
        emitted
            .iter()
            .find(|point| {
                point.x.to_bits() == (-180.0_f64).to_bits()
                    && point.y.to_bits() == (-0.0_f64).to_bits()
            })
            .map(|point| emitted_node_key(frame, point.xy()).unwrap()),
        Some(EmittedNodeKey::Coordinate(
            (-180.0_f64).to_bits(),
            0.0_f64.to_bits()
        ))
    );
}

#[test]
fn source_boundary_binds_the_pinned_y_sign_after_shape_assembly() -> Result<()> {
    let source = Shape::Polygon(Polygon::new(
        Ring::try_from_closed(CoordSeq::try_from_points(&[
            Point::new_raw_xy(-180.0, -0.0),
            Point::new_raw_xy(180.0, -0.0),
            Point::new_raw_xy(180.0, 10.0),
            Point::new_raw_xy(-180.0, 10.0),
            Point::new_raw_xy(-180.0, -0.0),
        ])?)?,
        Vec::new(),
    ));
    let topology = test_topology(std::slice::from_ref(&source));
    let (plan, _) = build_q_boundary_plan(&topology)?;

    let (unchanged, unchanged_layout) = source_boundary_sink(&topology, &plan)?;
    let Shape::MultiPoint(unchanged_points) = &unchanged else {
        panic!("the seam-only boundary is its collapsed source points");
    };
    assert!(
        unchanged_points
            .iter_coords()
            .any(|point| point.y.to_bits() == (-0.0_f64).to_bits()),
        "the collapsed source writer retains the original negative-zero Y bits"
    );
    assert!(matches!(
        bind_source_shape(&topology, unchanged, unchanged_layout)?,
        LiteralCheck::Pass(_)
    ));

    let (shape, layout) = source_boundary_sink(&topology, &plan)?;
    let Shape::MultiPoint(points) = shape else {
        panic!("the source boundary writer returns a multipoint sink");
    };
    let mut coordinates = points.iter_coords().collect::<Vec<_>>();
    let ordinal = coordinates
        .iter()
        .position(|point| point.y.to_bits() == (-0.0_f64).to_bits())
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("pinned source Y record is missing".into())
        })?;
    let image = layout
        .points
        .get(ordinal)
        .map(|location| location.image)
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("pinned source Y slot has no literal receipt".into())
        })?;
    assert!(
        matches!(
            image,
            QSourceImage::CollapsedStart(_) | QSourceImage::CollapsedEnd(_)
        ),
        "the pinned record must name its source collapsed-edge endpoint"
    );
    coordinates
        .get_mut(ordinal)
        .ok_or_else(|| GeometryErrorKind::Invalid("source point slot is unknown".into()))?
        .y = 0.0;
    let mutated = Shape::MultiPoint(crate::geometry::MultiPoint::from_coordseq(
        CoordSeq::try_from_points(&coordinates)?,
    )?);
    let LiteralCheck::Reject(rejected) = bind_source_shape(&topology, mutated, layout)? else {
        panic!("a final-buffer ±0 mutation must fail the retained source-record check");
    };
    assert!(rejected.contains(&image));
    Ok(())
}

#[test]
fn k0_source_point_y_bits_are_rechecked_in_the_final_shape() -> Result<()> {
    let shapes = [
        Shape::Point(Point::new_raw_xy(5.0, 0.0)),
        Shape::Point(Point::new_raw_xy(10.0, 2.0)),
    ];
    let topology = test_topology(&shapes);
    let plan = build_q_plan(&topology, OverlayOp::Union)?;
    let source_points = source_point_coordinates(&topology)?;
    let nearest = QCoordinates::build(&topology)?;
    let drawing = draw_q_plan_unmetered(&plan, &topology, &nearest, &source_points)?;
    assert!(
        plan.source_reuse_is_proven(
            &topology,
            &drawing,
            &source_points,
            &mut crate::work::Unmetered,
        )?,
        "unchanged explicit source points take the proven K0 writer path"
    );

    let actual = exact_geographic_overlay::<crate::crs::GeodesicMetric>(
        &topology,
        OverlayOp::Union,
        CoordinateAxes::XY,
        false,
        None,
    )?;
    let Shape::MultiPoint(actual_points) = actual else {
        panic!("the two disjoint source points publish as a MultiPoint");
    };
    assert!(
        actual_points.iter_coords().any(|point| {
            point.x.to_bits() == 5.0_f64.to_bits() && point.y.to_bits() == 0.0_f64.to_bits()
        }),
        "K0 retains the source point's positive-zero Y record"
    );

    let (shape, layout) = source_sink(&topology, &plan, &source_points)?;
    let Shape::MultiPoint(points) = shape else {
        panic!("the K0 writer returns a MultiPoint source sink");
    };
    let mut coordinates = points.iter_coords().collect::<Vec<_>>();
    let ordinal = coordinates
        .iter()
        .position(|point| {
            point.x.to_bits() == 5.0_f64.to_bits() && point.y.to_bits() == 0.0_f64.to_bits()
        })
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("pinned K0 source point is missing".into())
        })?;
    let image = layout
        .points
        .get(ordinal)
        .map(|location| location.image)
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("K0 source point has no literal receipt".into())
        })?;
    assert!(
        matches!(image, QSourceImage::SourcePoint(_)),
        "the literal slot retains its original source-point identity"
    );
    coordinates
        .get_mut(ordinal)
        .ok_or_else(|| GeometryErrorKind::Invalid("K0 point slot is unknown".into()))?
        .y = -0.0;
    let mutated = Shape::MultiPoint(crate::geometry::MultiPoint::from_coordseq(
        CoordSeq::try_from_points(&coordinates)?,
    )?);
    let LiteralCheck::Reject(rejected) = bind_source_shape(&topology, mutated, layout)? else {
        panic!("a final K0 buffer sign-bit mutation must fail source binding");
    };
    assert!(rejected.contains(&image));
    Ok(())
}

#[test]
fn source_pole_point_keeps_its_raw_longitude_record() -> Result<()> {
    let shapes = [
        Shape::Point(Point::new_raw_xy(720.0, 90.0)),
        Shape::Point(Point::new_raw_xy(10.0, 0.0)),
    ];
    let topology = test_topology(&shapes);
    let plan = build_q_plan(&topology, OverlayOp::Union)?;
    let source_points = source_point_coordinates(&topology)?;
    let nearest = QCoordinates::build(&topology)?;
    let drawing = draw_q_plan_unmetered(&plan, &topology, &nearest, &source_points)?;
    assert!(plan.source_reuse_is_proven(
        &topology,
        &drawing,
        &source_points,
        &mut crate::work::Unmetered,
    )?);

    let actual = exact_geographic_overlay::<crate::crs::GeodesicMetric>(
        &topology,
        OverlayOp::Union,
        CoordinateAxes::XY,
        false,
        None,
    )?;
    let Shape::MultiPoint(actual_points) = actual else {
        panic!("the source pole point and ordinary point remain a MultiPoint");
    };
    assert!(
        actual_points.iter_coords().any(|point| {
            point.x.to_bits() == 720.0_f64.to_bits() && point.y.to_bits() == 90.0_f64.to_bits()
        }),
        "the selected source-point record owns its raw pole longitude"
    );

    let (shape, layout) = source_sink(&topology, &plan, &source_points)?;
    let pole_source_image = layout
        .points
        .iter()
        .map(|location| location.image)
        .find(|image| match image {
            QSourceImage::SourcePoint(index) => topology
                .sources
                .points
                .get(*index as usize)
                .is_some_and(|source| {
                    source.point.x.to_bits() == 720.0_f64.to_bits()
                        && source.point.y.to_bits() == 90.0_f64.to_bits()
                }),
            QSourceImage::Port(_)
            | QSourceImage::SourceRingStart(_)
            | QSourceImage::SourceRingEnd(_)
            | QSourceImage::CollapsedStart(_)
            | QSourceImage::CollapsedEnd(_)
            | QSourceImage::PublicationSample(_) => false,
        })
        .ok_or_else(|| {
            GeometryErrorKind::Invalid(
                "source pole slot lost its source-record authority".into(),
            )
        })?;
    assert!(matches!(
        bind_source_shape(&topology, shape, layout)?,
        LiteralCheck::Pass(_)
    ));
    assert!(matches!(pole_source_image, QSourceImage::SourcePoint(_)));
    Ok(())
}

#[test]
fn k0_source_ring_y_bits_are_rechecked_in_the_final_shape() -> Result<()> {
    let source = crate::io::parse_wkt("POLYGON ((0 0, 2 0, 2 2, 0 2, 0 0))")?;
    let topology = test_topology(std::slice::from_ref(&source));
    let plan = build_q_plan(&topology, OverlayOp::Union)?;
    let source_points = source_point_coordinates(&topology)?;
    let nearest = QCoordinates::build(&topology)?;
    let drawing = plan.drawing(
        &topology,
        &nearest,
        &source_points,
        &mut crate::work::Unmetered,
    )?;
    assert!(plan.source_reuse_is_proven(
        &topology,
        &drawing,
        &source_points,
        &mut crate::work::Unmetered,
    )?);
    let actual = exact_geographic_overlay::<crate::crs::GeodesicMetric>(
        &topology,
        OverlayOp::Union,
        CoordinateAxes::XY,
        false,
        None,
    )?;
    let Shape::Polygon(actual_polygon) = actual else {
        panic!("the K0 source ring remains one polygon");
    };
    assert!(
        actual_polygon.shell.coords().points().any(|point| {
            point.x.to_bits() == 2.0_f64.to_bits() && point.y.to_bits() == 0.0_f64.to_bits()
        }),
        "the unchanged source ring keeps its positive-zero Y record"
    );

    let (shape, layout) = source_sink(&topology, &plan, &source_points)?;
    let Shape::Polygon(polygon) = shape else {
        panic!("the K0 source ring remains one polygon");
    };
    let mut coordinates = polygon.shell.coords().points().collect::<Vec<_>>();
    let ordinal = coordinates
        .iter()
        .position(|point| {
            point.x.to_bits() == 2.0_f64.to_bits() && point.y.to_bits() == 0.0_f64.to_bits()
        })
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("pinned source ring Y record is missing".into())
        })?;
    let image = layout
        .polygons
        .first()
        .and_then(|polygon| polygon.shell.images.get(ordinal))
        .copied()
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("pinned source ring slot has no literal receipt".into())
        })?;
    assert!(
        matches!(
            image,
            QSourceImage::SourceRingStart(_) | QSourceImage::SourceRingEnd(_)
        ),
        "the literal slot retains its original source-edge endpoint"
    );
    coordinates
        .get_mut(ordinal)
        .ok_or_else(|| GeometryErrorKind::Invalid("source ring slot is unknown".into()))?
        .y = -0.0;
    let shell = Ring::try_from_closed(CoordSeq::try_from_points(&coordinates)?)?;
    let mutated = Shape::Polygon(Polygon::new(shell, polygon.holes.to_vec()));
    let LiteralCheck::Reject(rejected) = bind_source_shape(&topology, mutated, layout)? else {
        panic!("a final K0 source-ring sign-bit mutation must fail binding");
    };
    assert!(rejected.contains(&image));
    Ok(())
}

#[test]
fn computed_q_line_alias_does_not_acquire_source_xy_bit_pinning() -> Result<()> {
    let line = Shape::LineString(LineSeq::try_new(CoordSeq::try_from_points(&[
        Point::new_raw_xy(0.0, 0.0),
        Point::new_raw_xy(1.0, 0.0),
    ])?)?);
    let topology = test_topology(std::slice::from_ref(&line));
    let plan = build_q_plan(&topology, OverlayOp::Union)?;
    let source_points = source_point_coordinates(&topology)?;
    let (shape, layout) = source_sink(&topology, &plan, &source_points)?;
    let image = layout
        .lines
        .first()
        .and_then(|row| row.images.first())
        .copied()
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("computed Q line has no literal image".into())
        })?;
    assert!(matches!(image, QSourceImage::Port(_)));

    let Shape::LineString(line) = shape else {
        panic!("one computed Q line is a LineString sink");
    };
    let mut coordinates = line.as_coords().points().collect::<Vec<_>>();
    let point = coordinates
        .first_mut()
        .ok_or_else(|| GeometryErrorKind::Invalid("computed Q line is empty".into()))?;
    assert_eq!(point.y.to_bits(), 0.0_f64.to_bits());
    point.y = -0.0;
    let alias = Shape::LineString(LineSeq::from_proven(CoordSeq::try_from_points(
        &coordinates,
    )?));
    assert!(matches!(
        bind_source_shape(&topology, alias, layout)?,
        LiteralCheck::Pass(_)
    ));
    Ok(())
}
