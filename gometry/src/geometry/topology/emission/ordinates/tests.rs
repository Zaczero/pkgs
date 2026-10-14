
#[test]
fn emission_ordinate_rows_and_ports_are_reserved_before_progress() {
    let empty = crate::io::parse_wkt("POINT EMPTY").unwrap();
    let topology = endpoint_test_topology(std::slice::from_ref(&empty)).unwrap();
    let mut setup = Unmetered;
    let node_registry = EmissionNodeRegistry::with_sources(&topology, &[], &mut setup).unwrap();
    let roots = [
        RootRequest::new_with_work(
            RootId::from_index(2),
            ExactRational::from_f64(0.25),
            &mut setup,
        )
        .unwrap(),
        RootRequest::new_with_work(
            RootId::from_index(2),
            ExactRational::from_f64(0.5),
            &mut setup,
        )
        .unwrap(),
        RootRequest::new_with_work(
            RootId::from_index(2),
            ExactRational::from_f64(0.75),
            &mut setup,
        )
        .unwrap(),
    ];
    let occurrences = roots
        .iter()
        .enumerate()
        .map(|(index, request)| {
            Ok(WrittenOccurrence {
                origin: VertexOrigin::SourceSample,
                source_trace: Some(TraceRequestLocation::Root {
                    root: request.root(),
                    parameter: request.parameter().try_clone_with_work(&mut setup).unwrap(),
                }),
                request_row: RequestRowId::from_index(index).unwrap(),
            })
        })
        .collect::<Result<Vec<_>>>()
        .unwrap();
    let plan = ExpandedEmissionPlan {
        topology: &topology,
        selected_face_roots: Vec::new(),
        node_registry,
        ports: vec![
            EmissionPort {
                node: EmissionNodeId(0),
                position: PortPosition::Projected {
                    spelling: PortSpelling::Interior { periods: 0 },
                },
                source_record: None,
                occurrence_range: 0..2,
            },
            EmissionPort {
                node: EmissionNodeId(1),
                position: PortPosition::Projected {
                    spelling: PortSpelling::Interior { periods: 0 },
                },
                source_record: None,
                occurrence_range: 2..3,
            },
        ],
        occurrences,
        chords: Vec::new(),
        parts: Vec::new(),
        lines: Vec::new(),
        points: Vec::new(),
        whole_sphere: false,
    };
    let publication = PublicationInventory {
        fragments: Vec::new(),
        samples: Vec::new(),
        counts: Vec::new(),
        ordinates: Vec::new(),
    };

    let targets = {
        let progress = Rc::new(Cell::new(0));
        let roots = ProgressRootCatalog(Rc::clone(&progress));
        let mut budget = EvaluationBudget::new(EMISSION_STORAGE_OPERATION, 0).unwrap();
        let mut work = RecordedWork {
            budget: &mut budget,
            progress: Rc::clone(&progress),
            spent: 0,
            charges: Vec::new(),
        };
        let (ordinates, axes) = resolve_emission_ordinates::<crate::crs::GeodesicMetric, _>(
            &plan,
            &publication,
            &[],
            &roots,
            CoordinateAxes::XYZM,
            false,
            EMISSION_STORAGE_OPERATION,
            None,
            &mut work,
        )
        .unwrap();
        assert_eq!(ordinates.len(), 2);
        assert_eq!(axes, CoordinateAxes::XYZM);
        assert_eq!(progress.get(), 3);
        let request_rows = *work
            .charges
            .first()
            .expect("source occurrence rows reserve their slots first");
        assert_eq!(request_rows.ticks, 3);
        assert_eq!(request_rows.progress, 0);
        let output_ports = *work
            .charges
            .iter()
            .find(|charge| charge.ticks == 2 && charge.progress == 3)
            .expect("output port rows reserve their slots after request resolution");
        [request_rows, output_ports]
    };

    for target in targets {
        let progress = Rc::new(Cell::new(0));
        let roots = ProgressRootCatalog(Rc::clone(&progress));
        let mut budget = emission_budget_refusing_before(target);
        let mut work = RecordedWork {
            budget: &mut budget,
            progress: Rc::clone(&progress),
            spent: 0,
            charges: Vec::new(),
        };
        let Err(error) = resolve_emission_ordinates::<crate::crs::GeodesicMetric, _>(
            &plan,
            &publication,
            &[],
            &roots,
            CoordinateAxes::XYZM,
            false,
            EMISSION_STORAGE_OPERATION,
            None,
            &mut work,
        ) else {
            panic!("Native ordinate row and port storage must be paid before progress");
        };
        assert_emission_work_limit(&error);
        assert_eq!(progress.get(), target.progress);
        let refusal = work
            .charges
            .last()
            .expect("the recorder observes the refused emission storage phase");
        assert!(!refusal.accepted);
        assert_eq!(refusal.ticks, target.ticks);
        assert_eq!(refusal.spent_before, target.spent_before);
        assert_eq!(refusal.progress, target.progress);
    }
}

#[test]
fn live_presentation_use_cannot_inherit_colocated_source_axes() {
    let frame = GeographicFrame::crs84();
    let source = crate::io::parse_wkt("LINESTRING Z (0 0 7, 0 10 9)").unwrap();
    let inputs =
        crate::geographic::GeographicInputs::try_from_shapes(frame, std::iter::once(&source))
            .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))
            .unwrap();
    let topology =
        ExactGeographicTopology::from_views(frame, inputs.views().iter().copied()).unwrap();
    let port_id = *topology
        .q
        .source_edge_ports
        .first()
        .and_then(|ports| ports.first())
        .ok_or_else(|| GeometryErrorKind::Invalid("source port is missing".into()))
        .unwrap();
    let source_node = topology
        .q
        .ports
        .get(port_id.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("source port is unknown".into()))
        .unwrap()
        .node;
    let mut work = Unmetered;
    let node_registry =
        EmissionNodeRegistry::with_sources(&topology, &[source_node], &mut work).unwrap();
    let emission_node = node_registry.source_node(source_node).unwrap();
    let source_record = Point::new_raw_axes(0.0, 0.0, ZOrdinate(Some(7.0)), MOrdinate(None));
    let plan = ExpandedEmissionPlan {
        topology: &topology,
        selected_face_roots: Vec::new(),
        node_registry,
        ports: vec![EmissionPort {
            node: emission_node,
            position: PortPosition::Source {
                spelling: PortSpelling::Interior { periods: 0 },
            },
            source_record: Some(source_record),
            occurrence_range: 0..2,
        }],
        occurrences: vec![
            WrittenOccurrence {
                origin: VertexOrigin::SourceNode(source_node),
                source_trace: None,
                request_row: RequestRowId::from_index(0).unwrap(),
            },
            WrittenOccurrence {
                origin: VertexOrigin::Presentation,
                source_trace: None,
                request_row: RequestRowId::from_index(1).unwrap(),
            },
        ],
        chords: Vec::new(),
        parts: Vec::new(),
        lines: Vec::new(),
        points: Vec::new(),
        whole_sphere: false,
    };
    let publication = PublicationInventory {
        fragments: Vec::new(),
        samples: Vec::new(),
        counts: Vec::new(),
        ordinates: Vec::new(),
    };
    let mut source_ordinates = vec![NodeOrdinates::default(); topology.q.nodes.len()];
    let mut source_z = AxisValue::default();
    source_z.observe(Some(7.0));
    source_ordinates
        .get_mut(source_node.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("source ordinate row is missing".into()))
        .unwrap()
        .z = source_z;
    let metric_access: Option<&SourceArcMetricAccess<'_, '_, crate::crs::GeodesicMetric>> =
        None;
    let mut work = Unmetered;
    let (ordinates, axes) = resolve_emission_ordinates(
        &plan,
        &publication,
        &source_ordinates,
        &topology,
        CoordinateAxes::XYZ,
        false,
        "overlay",
        metric_access,
        &mut work,
    )
    .unwrap();
    assert_eq!(axes, CoordinateAxes::XY);
    assert_eq!(ordinates.len(), 1);
    assert_eq!(
        ordinates.first().map(|value| value.z.value()),
        Some(Some(7.0)),
        "the source use retains its value even though the shared output slot cannot keep Z"
    );
    let longitude = frame
        .longitude()
        .canonicalize(0.0)
        .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))
        .unwrap();
    let sites = [AssignedSite {
        longitude: crate::geographic::RetainedLongitudeSite::new(longitude, 0),
        latitude: 0.0,
    }];
    let written =
        native_port_point(&plan, &sites, &ordinates, EmissionPortId(0), axes).unwrap();
    assert_eq!(written.z(), None);
    assert_eq!(written.m(), None);
    assert_eq!(
        written.x.to_bits(),
        source_record.x.to_bits(),
        "the actual source XY record remains literal while presentation Z is dropped"
    );

    let mut work = Unmetered;
    assert!(
        resolve_emission_ordinates(
            &plan,
            &publication,
            &source_ordinates,
            &topology,
            CoordinateAxes::XYZ,
            true,
            "overlay",
            metric_access,
            &mut work,
        )
        .is_err()
    );
}

#[test]
#[expect(
    clippy::panic_in_result_fn,
    reason = "assertions are the exact ordinate and strict-axis regression oracle"
)]
fn native_source_root_occurrences_preserve_constant_zm_and_resolve_varying_seam_values()
-> Result<()> {
    const CRS: &str = "+proj=longlat +a=7000000 +rf=150 +type=crs";
    let state = crate::crs::runtime_state_snapshot()?;
    crate::crs::with_ellipsoid_metric_at(CRS, &[], &state, |metric| {
        let frame = metric.chart_frame();
        let metric_access = || Ok(metric);
        let metric_access: &SourceArcMetricAccess<'_, '_, crate::crs::EllipsoidMetric<'_>> =
            &metric_access;
        let mut request_work = Unmetered;

        let constant = [
            crate::io::parse_wkt("LINESTRING ZM (170 -30 5 50, -170 70 5 50)")?,
            crate::io::parse_wkt("LINESTRING ZM (170 70 5 50, -170 -30 5 50)")?,
        ];
        let inputs =
            crate::geographic::GeographicInputs::try_from_shapes(frame, constant.iter())
                .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
        let topology =
            ExactGeographicTopology::from_views(frame, inputs.views().iter().copied())?;
        let (cut_port, requests) = source_requests_at_shared_port(&topology, 2, false)?;
        let (ordinates, axes) = resolve_native_root_occurrences(
            &topology,
            cut_port,
            &requests,
            Some(metric_access),
            true,
        )?;
        assert_eq!(axes, CoordinateAxes::XYZM);
        let ordinates = ordinates.first().ok_or_else(|| {
            GeometryErrorKind::Invalid("constant Z/M source port is missing".into())
        })?;
        assert_eq!(ordinates.z.value(), Some(5.0));
        assert_eq!(ordinates.m.value(), Some(50.0));

        let varying = crate::io::parse_wkt("LINESTRING ZM (170 -30 1 5, -170 70 9 45)")?;
        let inputs = crate::geographic::GeographicInputs::try_from_shapes(
            frame,
            std::iter::once(&varying),
        )
        .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
        let topology =
            ExactGeographicTopology::from_views(frame, inputs.views().iter().copied())?;
        let (cut_port, requests) = source_requests_at_shared_port(&topology, 1, true)?;
        let (root, parameter) = requests
            .iter()
            .find(|(root, parameter)| {
                topology
                    .sources
                    .edges
                    .get(root.index())
                    .is_some_and(|edge| {
                        source_edge_varies_in_latitude_and_ordinates(&edge.source)
                            && !parameter.is_zero()
                            && !parameter.is_one()
                    })
            })
            .ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "varying shared port has no interior source-root request".into(),
                )
            })?;
        let edge = topology.sources.edges.get(root.index()).ok_or_else(|| {
            GeometryErrorKind::Invalid("varying source root is unknown".into())
        })?;
        let chart_parameter = parameter.round_with_work(&mut request_work)?;
        let chart_edge = crate::crs::ChartEdge::from_retained(
            edge.source[0],
            edge.source[1],
            frame,
            edge.retained_longitude(),
        )?;
        let fraction =
            metric.chart_edge_arclength_fraction(&chart_edge, chart_parameter, None)?;
        assert_ne!(
            fraction, chart_parameter,
            "the source sample distinguishes ellipsoidal arclength from chart t"
        );
        let expected = source_ordinate_values_at_fraction(
            edge.source[0],
            edge.source[1],
            CoordinateAxes::XYZM,
            fraction,
        );
        let (ordinates, axes) = resolve_native_root_occurrences(
            &topology,
            cut_port,
            &requests,
            Some(metric_access),
            true,
        )?;
        assert_eq!(axes, CoordinateAxes::XYZM);
        let ordinates = ordinates.first().ok_or_else(|| {
            GeometryErrorKind::Invalid("varying source request lost its output port".into())
        })?;
        assert_eq!(
            ordinates.z.value().map(f64::to_bits),
            expected.0.0.map(f64::to_bits)
        );
        assert_eq!(
            ordinates.m.value().map(f64::to_bits),
            expected.1.0.map(f64::to_bits)
        );

        let conflicting = [
            crate::io::parse_wkt("LINESTRING ZM (170 -30 5 50, -170 70 5 50)")?,
            crate::io::parse_wkt("LINESTRING ZM (170 70 7 70, -170 -30 7 70)")?,
        ];
        let inputs =
            crate::geographic::GeographicInputs::try_from_shapes(frame, conflicting.iter())
                .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
        let topology =
            ExactGeographicTopology::from_views(frame, inputs.views().iter().copied())?;
        let (cut_port, requests) = source_requests_at_shared_port(&topology, 2, false)?;
        let Err(error) = resolve_native_root_occurrences(
            &topology,
            cut_port,
            &requests,
            Some(metric_access),
            true,
        ) else {
            return Err(GeometryErrorKind::Invalid(
                "conflicting source ordinates were merged into one value".into(),
            )
            .into());
        };
        assert!(
            error
                .to_string()
                .contains("difference cannot represent Z/M")
        );
        Ok(())
    })
}
