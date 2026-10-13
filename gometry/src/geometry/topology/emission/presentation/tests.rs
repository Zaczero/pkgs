#[test]
fn same_seam_bank_occurrence_merges_at_a_pole() {
    let frame = GeographicFrame::crs84();
    let make_port = |side, origin| NativePortData {
        node: EmissionNodeId(4),
        point: ExactChartPoint::from_rationals(
            ExactRational::from_f64(match side {
                ChartSide::East => frame.longitude().half_period(),
                ChartSide::West => -frame.longitude().half_period(),
            }),
            ExactRational::from_f64(frame.chart_pole_latitude(ChartPole::North)),
        ),
        spelling: PortSpelling::Seam(side),
        source_record: None,
        occurrences: vec![OccurrenceSource {
            origin,
            source_trace: None,
        }],
    };
    let east_presentation = make_port(ChartSide::East, VertexOrigin::Presentation);
    let east_source = make_port(ChartSide::East, VertexOrigin::SourceNode(QNodeId(3)));
    let mut work = Unmetered;

    let merged =
        merge_native_port_data(frame, &east_presentation, &east_source, &mut work).unwrap();

    assert_eq!(merged.point, east_presentation.point);
    assert!(matches!(
        merged.spelling,
        PortSpelling::Seam(ChartSide::East)
    ));
    assert_eq!(merged.occurrences.len(), 2);
    let west = make_port(ChartSide::West, VertexOrigin::Presentation);
    assert!(
        merge_native_port_data(frame, &east_presentation, &west, &mut work).is_err(),
        "East and West seam-bank copies remain distinct at a pole"
    );
}

#[test]
fn native_pole_presentation_splits_at_a_known_source_occurrence() {
    let frame = GeographicFrame::crs84();
    let pole_latitude = frame.chart_pole_latitude(ChartPole::North);
    let pole = NativePortData {
        node: EmissionNodeId(4),
        point: ExactChartPoint::from_rationals(
            ExactRational::from_f64(120.0),
            ExactRational::from_f64(pole_latitude),
        ),
        spelling: PortSpelling::Pole {
            native_longitude: 120.0,
        },
        source_record: Some(Point::new_raw_xy(120.0, pole_latitude)),
        occurrences: vec![OccurrenceSource {
            origin: VertexOrigin::SourceNode(QNodeId(7)),
            source_trace: None,
        }],
    };
    let lower = NativePortData {
        node: EmissionNodeId(5),
        point: ExactChartPoint::from_rationals(
            ExactRational::from_f64(110.0),
            ExactRational::from_f64(75.0),
        ),
        spelling: PortSpelling::Interior { periods: 0 },
        source_record: None,
        occurrences: Vec::new(),
    };
    let producer = NativeAreaEdge {
        vertices: [pole, lower],
        owner: NativeGraphEdgeOwner::Area { region: 0 },
        kind: NativeAreaEdgeKind::Producer {
            recipe: FragmentRecipeId(0),
            trace_edge: 0,
            producer_start: Box::new(ExactRational::zero()),
            producer_end: ExactRational::one(),
            trace_ranges: Vec::new(),
            known_spans: Vec::new(),
        },
    };
    let isolated_pole = NativePortData {
        node: EmissionNodeId(4),
        point: ExactChartPoint::from_rationals(
            ExactRational::from_f64(100.0),
            ExactRational::from_f64(pole_latitude),
        ),
        spelling: PortSpelling::Pole {
            native_longitude: 100.0,
        },
        source_record: Some(Point::new_raw_xy(100.0, pole_latitude)),
        occurrences: vec![OccurrenceSource {
            origin: VertexOrigin::SourceNode(QNodeId(8)),
            source_trace: None,
        }],
    };
    let seam_vertex = |longitude, spelling| NativePortData {
        node: EmissionNodeId(4),
        point: ExactChartPoint::from_rationals(
            ExactRational::from_f64(longitude),
            ExactRational::from_f64(pole_latitude),
        ),
        spelling: PortSpelling::Seam(spelling),
        source_record: None,
        occurrences: vec![OccurrenceSource {
            origin: VertexOrigin::Presentation,
            source_trace: None,
        }],
    };
    let event = NativeSeamVertices {
        latitude: ExactRational::from_f64(pole_latitude),
        east: seam_vertex(180.0, ChartSide::East),
        west: seam_vertex(-180.0, ChartSide::West),
    };
    let mut budget = EvaluationBudget::new("pole presentation incidence", 0).unwrap();
    let edges = native_pole_edges(
        frame,
        &[event],
        &[(0, ChartPole::North)],
        &[producer],
        &[],
        &[isolated_pole],
        &mut |_, _, _, _| Ok(false),
        &mut budget,
    )
    .unwrap();

    assert_eq!(edges.len(), 3);
    let first = &edges[0];
    let second = &edges[1];
    let third = &edges[2];
    assert_eq!(first.vertices[0].point.x, ExactRational::from_f64(180.0));
    assert_eq!(first.vertices[1].point.x, ExactRational::from_f64(120.0));
    assert_eq!(second.vertices[0].point.x, ExactRational::from_f64(120.0));
    assert_eq!(second.vertices[1].point.x, ExactRational::from_f64(100.0));
    assert_eq!(third.vertices[0].point.x, ExactRational::from_f64(100.0));
    assert_eq!(third.vertices[1].point.x, ExactRational::from_f64(-180.0));
    assert!(matches!(
        &first.vertices[0].spelling,
        PortSpelling::Seam(ChartSide::East)
    ));
    assert!(matches!(
        &third.vertices[1].spelling,
        PortSpelling::Seam(ChartSide::West)
    ));
    let NativeAreaEdgeKind::Restriction(first_use) = &first.kind else {
        panic!("polar closure edges retain their restriction receipt");
    };
    assert_eq!(first_use.restriction, ChartRestriction::PolarCap {
        pole: ChartPole::North
    });
    assert_eq!(first_use.start, SyntheticChartVertex::Cut {
        node: first.vertices[0].node.0,
        side: ChartSide::East,
    });
    let PortSpelling::Pole { native_longitude } = &first.vertices[1].spelling else {
        panic!("the first polar side use ends at a generated pole vertex");
    };
    assert_eq!(first_use.end, SyntheticChartVertex::Pole {
        node: first.vertices[1].node.0,
        pole: ChartPole::North,
        native_longitude_bits: native_longitude.to_bits(),
    });
    for presentation_port in [
        &first.vertices[1],
        &second.vertices[0],
        &second.vertices[1],
        &third.vertices[0],
    ] {
        assert!(matches!(
            &presentation_port.spelling,
            PortSpelling::Pole { .. }
        ));
        assert!(presentation_port.source_record.is_none());
        assert!(matches!(presentation_port.occurrences.as_slice(), [
            OccurrenceSource {
                origin: VertexOrigin::Presentation,
                source_trace: None,
            }
        ]));
    }
}

#[test]
fn native_seam_connectors_keep_cut_roles_and_directed_side_uses() {
    let seam = |node, latitude, side| NativePortData {
        node: EmissionNodeId(node),
        point: ExactChartPoint::from_rationals(
            ExactRational::from_f64(match side {
                ChartSide::East => 180.0,
                ChartSide::West => -180.0,
            }),
            ExactRational::from_f64(latitude),
        ),
        spelling: PortSpelling::Seam(side),
        source_record: None,
        occurrences: vec![OccurrenceSource {
            origin: VertexOrigin::Presentation,
            source_trace: None,
        }],
    };
    let events = [
        NativeSeamVertices {
            latitude: ExactRational::from_f64(0.0),
            east: seam(0, 0.0, ChartSide::East),
            west: seam(0, 0.0, ChartSide::West),
        },
        NativeSeamVertices {
            latitude: ExactRational::from_f64(10.0),
            east: seam(1, 10.0, ChartSide::East),
            west: seam(1, 10.0, ChartSide::West),
        },
    ];
    let bands = [SelectedNativeSeamBand {
        low: ExactRational::from_f64(0.0),
        high: ExactRational::from_f64(10.0),
        west_region: Some(1),
        east_region: Some(0),
    }];
    let mut budget = EvaluationBudget::new("seam restriction receipt", 0).unwrap();
    let edges =
        native_seam_edges(&bands, &events, &mut |_, _, _, _| Ok(false), &mut budget).unwrap();

    assert_eq!(edges.len(), 2);
    let [east, west] = edges.as_slice() else {
        panic!("both selected chart sides have one generated connector");
    };
    let NativeAreaEdgeKind::Restriction(east_use) = &east.kind else {
        panic!("east connector retains its chart restriction");
    };
    let NativeAreaEdgeKind::Restriction(west_use) = &west.kind else {
        panic!("west connector retains its chart restriction");
    };
    assert_eq!(east_use.restriction, ChartRestriction::CutChord);
    assert_eq!(east_use.start, SyntheticChartVertex::Cut {
        node: 0,
        side: ChartSide::East,
    });
    assert_eq!(east_use.end, SyntheticChartVertex::Cut {
        node: 1,
        side: ChartSide::East,
    });
    assert_eq!(west_use.restriction, ChartRestriction::CutChord);
    assert_eq!(west_use.start, SyntheticChartVertex::Cut {
        node: 1,
        side: ChartSide::West,
    });
    assert_eq!(west_use.end, SyntheticChartVertex::Cut {
        node: 0,
        side: ChartSide::West,
    });
}

#[test]
fn overlapping_pole_presentation_arcs_still_fail_known_graph_admission() {
    let frame = GeographicFrame::crs84();
    let pole_latitude = frame.chart_pole_latitude(ChartPole::North);
    let pole_port = |longitude| NativePortData {
        node: EmissionNodeId(4),
        point: ExactChartPoint::from_rationals(
            ExactRational::from_f64(longitude),
            ExactRational::from_f64(pole_latitude),
        ),
        spelling: PortSpelling::Pole {
            native_longitude: longitude,
        },
        source_record: None,
        occurrences: vec![OccurrenceSource {
            origin: VertexOrigin::Presentation,
            source_trace: None,
        }],
    };
    let vertices = [pole_port(80.0), pole_port(140.0)];
    let kind = NativeAreaEdgeKind::Restriction(
        directed_chart_restriction_use(
            ChartRestriction::PolarCap {
                pole: ChartPole::North,
            },
            &vertices,
        )
        .unwrap(),
    );
    let existing = NativeAreaEdge {
        vertices,
        owner: NativeGraphEdgeOwner::Area { region: 0 },
        kind,
    };
    let seam_vertex = |longitude, spelling| NativePortData {
        node: EmissionNodeId(4),
        point: ExactChartPoint::from_rationals(
            ExactRational::from_f64(longitude),
            ExactRational::from_f64(pole_latitude),
        ),
        spelling: PortSpelling::Seam(spelling),
        source_record: None,
        occurrences: vec![OccurrenceSource {
            origin: VertexOrigin::Presentation,
            source_trace: None,
        }],
    };
    let event = NativeSeamVertices {
        latitude: ExactRational::from_f64(pole_latitude),
        east: seam_vertex(180.0, ChartSide::East),
        west: seam_vertex(-180.0, ChartSide::West),
    };
    let mut budget = EvaluationBudget::new("overlapping pole presentations", 0).unwrap();
    let generated = native_pole_edges(
        frame,
        &[event],
        &[(0, ChartPole::North)],
        std::slice::from_ref(&existing),
        &[],
        &[],
        &mut |_, _, _, _| Ok(false),
        &mut budget,
    )
    .unwrap();
    assert_eq!(generated.len(), 1);
    let presentation = generated
        .first()
        .ok_or_else(|| GeometryErrorKind::Invalid("pole arc was not generated".into()))
        .unwrap();
    let exact = vec![
        existing.vertices[0].point.clone(),
        existing.vertices[1].point.clone(),
        presentation.vertices[0].point.clone(),
        presentation.vertices[1].point.clone(),
    ];
    let mut work = Unmetered;
    let points = ExactPoints::from_exact(exact, &mut work).unwrap();

    assert!(
        admit_known_graph(points, vec![[0, 1], [2, 3]], vec![4; 4], &mut work).is_err(),
        "a partial overlap between known presentation arcs remains inadmissible"
    );
}
