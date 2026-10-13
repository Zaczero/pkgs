#[test]
fn native_literal_port_map_follows_final_shape_order() {
    let uses = NativeLiteralUses {
        points: vec![
            LiteralPoint {
                output: 0,
                axes: CoordinateAxes::XY,
                image: EmissionPortId(10),
            },
            LiteralPoint {
                output: 1,
                axes: CoordinateAxes::XY,
                image: EmissionPortId(11),
            },
        ],
        lines: vec![
            LiteralSequence {
                output: 0,
                axes: CoordinateAxes::XY,
                images: vec![EmissionPortId(20), EmissionPortId(21)],
            },
            LiteralSequence {
                output: 1,
                axes: CoordinateAxes::XY,
                images: vec![EmissionPortId(30), EmissionPortId(31)],
            },
        ],
        polygons: Vec::new(),
    };
    let order = CertifiedOverlayOrder {
        points: vec![1, 0],
        lines: vec![1, 0],
        polygons: Vec::new(),
    };
    let mut work = Unmetered;
    let ordered = uses.in_output_order(&order, &mut work).unwrap();
    assert_eq!(
        ordered
            .points
            .iter()
            .map(|point| point.image)
            .collect::<Vec<_>>(),
        [EmissionPortId(11), EmissionPortId(10)],
    );
    assert_eq!(
        ordered
            .points
            .iter()
            .map(|point| point.output)
            .collect::<Vec<_>>(),
        [1, 0],
    );
    assert_eq!(ordered.lines, [
        LiteralSequence {
            output: 1,
            axes: CoordinateAxes::XY,
            images: vec![EmissionPortId(30), EmissionPortId(31)],
        },
        LiteralSequence {
            output: 0,
            axes: CoordinateAxes::XY,
            images: vec![EmissionPortId(20), EmissionPortId(21)],
        },
    ],);
    assert!(ordered.polygons.is_empty());
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "assertions pin refusal before active-column reservation"
)]
fn native_materialization_refuses_before_active_column_reservation() -> Result<()> {
    let mut work = RecordingWork::with_remaining("native columns", 12)?;
    let result = materialize_test_line(CoordinateAxes::XYZM, &mut work);
    let Err(error) = result else {
        return Err(GeometryErrorKind::Invalid(
            "Native materialization completed without funding its active columns".into(),
        )
        .into());
    };
    assert!(matches!(
        error.kind(),
        crate::error::ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
            operation: "native columns",
            constructed: 1,
            ..
        })
    ));
    assert_eq!(work.accepted_ticks(), 12);
    let attempted = &work.attempted;
    assert_eq!(
        work.attempted.last(),
        Some(&2),
        "column refusal should follow funded preparation charges: {attempted:?}"
    );
    Ok(())
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "assertions pin refusal at each unfunded active-column phase"
)]
fn native_materialization_refuses_before_each_active_column_phase() -> Result<()> {
    for remaining_ticks in [12, 20, 28] {
        let mut work = RecordingWork::with_remaining("native columns", remaining_ticks)?;
        let result = materialize_test_line(CoordinateAxes::XYZM, &mut work);
        let Err(error) = result else {
            return Err(GeometryErrorKind::Invalid(
                "Native materialization passed an unfunded column phase".into(),
            )
            .into());
        };
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
                operation: "native columns",
                constructed: 1,
                ..
            })
        ));
        assert_eq!(work.accepted_ticks(), remaining_ticks);
        let attempted = &work.attempted;
        assert_eq!(
            work.attempted.last(),
            Some(&2),
            "allowance {remaining_ticks} should refuse the active-column phase: {attempted:?}"
        );
    }
    Ok(())
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "assertions pin XYZM coordinate values and active-column work counts"
)]
fn native_materialization_charges_only_the_active_columns() -> Result<()> {
    let mut xy_work = RecordingWork::with_remaining("native columns", 128)?;
    let xy = materialize_test_line(CoordinateAxes::XY, &mut xy_work)?;
    let mut xyzm_work = RecordingWork::with_remaining("native columns", 128)?;
    let xyzm = materialize_test_line(CoordinateAxes::XYZM, &mut xyzm_work)?;

    assert_eq!(
        xyzm_work.accepted_ticks() - xy_work.accepted_ticks(),
        12,
        "two extra columns each reserve, copy, and check two coordinate rows"
    );
    let xy_line = xy
        .lines
        .first()
        .ok_or_else(|| GeometryErrorKind::Invalid("materialized XY line is missing".into()))?;
    let xyzm_line = xyzm.lines.first().ok_or_else(|| {
        GeometryErrorKind::Invalid("materialized XYZM line is missing".into())
    })?;
    assert_eq!(xy_line.xs(), xyzm_line.xs());
    assert_eq!(xy_line.ys(), xyzm_line.ys());
    assert_eq!(xyzm_line.zs(), Some(&[1.25, 3.75][..]));
    assert_eq!(xyzm_line.ms(), Some(&[2.5, 4.5][..]));

    let mut xy_ring_work = RecordingWork::with_remaining("native columns", 256)?;
    let xy_ring = materialize_test_ring_with_hole(CoordinateAxes::XY, &mut xy_ring_work)?;
    let mut xyzm_ring_work = RecordingWork::with_remaining("native columns", 256)?;
    let xyzm_ring = materialize_test_ring_with_hole(CoordinateAxes::XYZM, &mut xyzm_ring_work)?;
    assert_eq!(
        xyzm_ring_work.accepted_ticks() - xy_ring_work.accepted_ticks(),
        60,
        "two extra columns are reserved, copied and checked for both five-row rings"
    );
    let xy_polygon = xy_ring.polygons.first().ok_or_else(|| {
        GeometryErrorKind::Invalid("materialized XY polygon is missing".into())
    })?;
    let xyzm_polygon = xyzm_ring.polygons.first().ok_or_else(|| {
        GeometryErrorKind::Invalid("materialized XYZM polygon is missing".into())
    })?;
    assert_eq!(xy_polygon.holes.len(), 1);
    assert_eq!(
        xy_polygon.shell.coords().xs(),
        xyzm_polygon.shell.coords().xs()
    );
    assert_eq!(
        xy_polygon.shell.coords().ys(),
        xyzm_polygon.shell.coords().ys()
    );
    assert_eq!(
        xyzm_polygon.shell.coords().zs(),
        Some(&[1.25, 3.75, 1.25, 3.75, 1.25][..])
    );
    assert_eq!(
        xyzm_polygon.shell.coords().ms(),
        Some(&[2.5, 4.5, 2.5, 4.5, 2.5][..])
    );
    let xy_hole = xy_polygon
        .holes
        .first()
        .ok_or_else(|| GeometryErrorKind::Invalid("materialized XY hole is missing".into()))?;
    let xyzm_hole = xyzm_polygon.holes.first().ok_or_else(|| {
        GeometryErrorKind::Invalid("materialized XYZM hole is missing".into())
    })?;
    assert_eq!(xy_hole.coords().xs(), xyzm_hole.coords().xs());
    assert_eq!(xy_hole.coords().ys(), xyzm_hole.coords().ys());
    assert_eq!(
        xyzm_hole.coords().zs(),
        Some(&[1.25, 3.75, 1.25, 3.75, 1.25][..])
    );
    assert_eq!(
        xyzm_hole.coords().ms(),
        Some(&[2.5, 4.5, 2.5, 4.5, 2.5][..])
    );
    Ok(())
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "assertions pin full closing-record funding and coordinate identity"
)]
fn native_materialization_funds_the_complete_closing_xyzm_record() -> Result<()> {
    let mut limited = RecordingWork::with_remaining("native columns", 17)?;
    let result = materialize_test_ring(CoordinateAxes::XYZM, &mut limited);
    let Err(error) = result else {
        return Err(GeometryErrorKind::Invalid(
            "Native ring materialization passed an unfunded closing-record write".into(),
        )
        .into());
    };
    assert!(matches!(
        error.kind(),
        crate::error::ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
            operation: "native columns",
            constructed: 1,
            ..
        })
    ));
    assert_eq!(limited.accepted_ticks(), 17);
    assert_eq!(limited.attempted.last(), Some(&1));

    let mut funded = RecordingWork::with_remaining("native columns", 128)?;
    let parts = materialize_test_ring(CoordinateAxes::XYZM, &mut funded)?;
    let ring = parts
        .polygons
        .first()
        .ok_or_else(|| GeometryErrorKind::Invalid("materialized ring is missing".into()))?
        .shell
        .coords();
    let first = ring.first().ok_or_else(|| {
        GeometryErrorKind::Invalid("materialized first record is missing".into())
    })?;
    let last = ring.last().ok_or_else(|| {
        GeometryErrorKind::Invalid("materialized closing record is missing".into())
    })?;
    assert_eq!(
        [
            first.x.to_bits(),
            first.y.to_bits(),
            first.z.to_bits(),
            first.m.to_bits()
        ],
        [
            last.x.to_bits(),
            last.y.to_bits(),
            last.z.to_bits(),
            last.m.to_bits()
        ],
    );
    Ok(())
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "assertions pin literal-metadata work refusal before row inspection"
)]
fn native_literal_validation_charges_each_metadata_row_before_inspection() -> Result<()> {
    let mut all_rows = point_literal_test_parts();
    let mut line_rows = line_literal_test_parts()?;
    all_rows.lines.append(&mut line_rows.lines);
    all_rows
        .literal_uses
        .lines
        .append(&mut line_rows.literal_uses.lines);
    let mut polygon_rows = polygon_literal_test_parts(true)?;
    all_rows.polygons.append(&mut polygon_rows.polygons);
    all_rows
        .literal_uses
        .polygons
        .append(&mut polygon_rows.literal_uses.polygons);

    let mut funded = RecordingWork::with_remaining("literal metadata", 100)?;
    all_rows.validate_literal_uses(&mut funded)?;
    assert_eq!(funded.attempted, [1, 1, 1, 1]);
    assert_eq!(funded.accepted, [1, 1, 1, 1]);

    for parts in [
        point_literal_test_parts(),
        line_literal_test_parts()?,
        polygon_literal_test_parts(false)?,
    ] {
        let mut work = RecordingWork::with_remaining("literal metadata", 0)?;
        let error = parts.validate_literal_uses(&mut work).unwrap_err();
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
                operation: "literal metadata",
                constructed: 1,
                ..
            })
        ));
        assert_eq!(work.attempted, [1]);
        assert!(work.accepted.is_empty());
    }

    let parts = polygon_literal_test_parts(true)?;
    let mut work = RecordingWork::with_remaining("literal metadata", 1)?;
    let error = parts.validate_literal_uses(&mut work).unwrap_err();
    assert!(matches!(
        error.kind(),
        crate::error::ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
            operation: "literal metadata",
            constructed: 1,
            ..
        })
    ));
    assert_eq!(work.attempted, [1, 1]);
    assert_eq!(work.accepted, [1]);
    Ok(())
}

#[test]
fn completed_native_drawing_pairs_literal_points_with_port_uses() {
    let shape = Shape::multi_point(
        CoordinateAxes::XY,
        CoordSeq::try_from_points(&[Point::new_raw_xy(3.0, 1.0), Point::new_raw_xy(8.0, 2.0)])
            .unwrap(),
    );
    let layout = NativeLiteralUses {
        points: vec![
            LiteralPoint {
                output: 0,
                axes: CoordinateAxes::XY,
                image: EmissionPortId(4),
            },
            LiteralPoint {
                output: 1,
                axes: CoordinateAxes::XY,
                image: EmissionPortId(9),
            },
        ],
        lines: Vec::new(),
        polygons: Vec::new(),
    };
    let mut visited = Vec::new();
    let mut work = Unmetered;
    visit_shape_literals(
        &shape,
        &layout,
        LiteralRowAddress::scalar(),
        &mut work,
        &mut |_, port, point, _, _, _| {
            visited.push((port, point.x, point.y));
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(visited, [
        (EmissionPortId(4), 3.0, 1.0),
        (EmissionPortId(9), 8.0, 2.0),
    ],);
}

#[test]
fn completed_native_drawing_keeps_the_full_ring_closure_binding() {
    let ring = Ring::try_from_closed(
        CoordSeq::try_from_points(&[
            Point::new_raw_xy(0.0, 0.0),
            Point::new_raw_xy(1.0, 0.0),
            Point::new_raw_xy(0.0, 1.0),
            Point::new_raw_xy(0.0, 0.0),
        ])
        .unwrap(),
    )
    .unwrap();
    let shape = Shape::Polygon(Polygon::new(ring, Vec::new()));
    let layout = NativeLiteralUses {
        points: Vec::new(),
        lines: Vec::new(),
        polygons: vec![NativePolygonLiteralUses {
            output: 0,
            axes: CoordinateAxes::XY,
            shell: LiteralSequence {
                output: 0,
                axes: CoordinateAxes::XY,
                images: vec![
                    EmissionPortId(4),
                    EmissionPortId(5),
                    EmissionPortId(6),
                    EmissionPortId(4),
                ],
            },
            holes: Vec::new(),
        }],
    };
    let mut visited = Vec::new();
    let mut work = Unmetered;
    visit_shape_literals(
        &shape,
        &layout,
        LiteralRowAddress::scalar(),
        &mut work,
        &mut |_, port, point, _, _, _| {
            visited.push((port, point.x, point.y));
            Ok(())
        },
    )
    .unwrap();
    assert_eq!(visited, [
        (EmissionPortId(4), 0.0, 0.0),
        (EmissionPortId(5), 1.0, 0.0),
        (EmissionPortId(6), 0.0, 1.0),
        (EmissionPortId(4), 0.0, 0.0),
    ],);
}
