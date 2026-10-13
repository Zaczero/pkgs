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
    let trace = SourceTrace::from_edge_rows([Vec::new()], &mut work).unwrap();
    let line_traces = vec![
        TracedLine {
            member: 20,
            trace: trace.try_clone_with_work(&mut work).unwrap(),
        },
        TracedLine { member: 30, trace },
    ];
    let (ordered, ordered_traces) =
        native_literal_uses_in_output_order(uses, line_traces, &order, &mut work).unwrap();
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
    assert_eq!(
        ordered_traces
            .iter()
            .map(|line| line.member)
            .collect::<Vec<_>>(),
        [30, 20],
    );
    assert!(ordered.polygons.is_empty());
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
