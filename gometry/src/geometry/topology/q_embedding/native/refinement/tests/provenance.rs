    use super::*;

    #[test]
    fn common_refinement_merges_source_requests_at_one_split_parameter() -> Result<()> {
        let mut work = Unmetered;
        let left_request = RootRequest::new_with_work(
            RootId::from_index(2),
            ExactRational::from_f64(0.25),
            &mut work,
        )?;
        let right_request = RootRequest::new_with_work(
            RootId::from_index(7),
            ExactRational::from_f64(0.75),
            &mut work,
        )?;
        let point = ExactChartPoint::from_rationals(
            ExactRational::from_f64(-150.0),
            ExactRational::from_f64(-80.0),
        );
        let source_port_data =
            |request: &RootRequest, work: &mut Unmetered| -> Result<NativePortData> {
                Ok(NativePortData {
                    node: EmissionNodeId(2),
                    point: point.clone(),
                    spelling: PortSpelling::Interior { periods: 0 },
                    source_record: None,
                    occurrences: native_source_sample_occurrences(
                        None,
                        std::slice::from_ref(request),
                        work,
                    )?,
                })
            };
        let event =
            |request: &RootRequest, work: &mut Unmetered| -> Result<NativeKnownSpanBoundary> {
                Ok(NativeKnownSpanBoundary {
                    span: ChartSpanId(0),
                    parameter: ExactRational::zero(),
                    port: QPortId(0),
                    data: source_port_data(request, work)?,
                })
            };
        let producer_port = |node, longitude| NativePortData {
            node: EmissionNodeId(node),
            point: ExactChartPoint::from_rationals(
                ExactRational::from_f64(longitude),
                ExactRational::from_f64(-80.0),
            ),
            spelling: PortSpelling::Interior { periods: 0 },
            source_record: None,
            occurrences: Vec::new(),
        };
        let source = NativeSourceSpanUse {
            span: ChartSpanId(0),
            owner: 0,
            source_edge: SourceEdgeId(0),
            source_lift: -1,
            source_range: ExactSourceRange {
                start: ExactRational::from_f64(0.5),
                end: ExactRational::zero(),
            },
            producer_range: ExactSourceRange {
                start: ExactRational::from_f64(0.5),
                end: ExactRational::one(),
            },
            span_range: ExactSourceRange {
                start: ExactRational::zero(),
                end: ExactRational::one(),
            },
            nodes: [QNodeId(0), QNodeId(1)],
            ports: [QPortId(0), QPortId(1)],
            face_germs: None,
        };
        let edge = NativeAreaEdge {
            vertices: [producer_port(0, -180.0), producer_port(1, -120.0)],
            owner: NativeGraphEdgeOwner::Area { region: 0 },
            kind: NativeAreaEdgeKind::Producer {
                recipe: FragmentRecipeId(4),
                trace_edge: 0,
                producer_start: Box::new(ExactRational::zero()),
                producer_end: ExactRational::one(),
                trace_ranges: Vec::new(),
                known_spans: vec![source.clone(), source],
            },
        };
        let boundaries = [
            event(&left_request, &mut work)?,
            event(&right_request, &mut work)?,
        ];

        let refined = common_refine_native_edges(
            vec![edge],
            &boundaries,
            GeographicFrame::crs84(),
            &mut work,
        )?;

        let split = refined
            .first()
            .and_then(|edge| edge.vertices.get(1))
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("common-refinement source split is missing".into())
            })?;
        assert!(
            split.occurrences.len() >= 2,
            "both directed source-root requests must survive coincident refinement events"
        );
        assert!(split.occurrences.iter().any(|occurrence| matches!(
            &occurrence.source_trace,
            Some(TraceRequestLocation::Root { root, parameter })
                if *root == RootId::from_index(2)
                    && *parameter == ExactRational::from_f64(0.25)
        )));
        assert!(split.occurrences.iter().any(|occurrence| matches!(
            &occurrence.source_trace,
            Some(TraceRequestLocation::Root { root, parameter })
                if *root == RootId::from_index(7)
                    && *parameter == ExactRational::from_f64(0.75)
        )));
        Ok(())
    }

    #[test]
    fn coincident_known_span_boundaries_merge_distinct_source_requests() -> Result<()> {
        let mut work = Unmetered;
        let left_request = RootRequest::new_with_work(
            RootId::from_index(2),
            ExactRational::from_f64(0.25),
            &mut work,
        )?;
        let right_request = RootRequest::new_with_work(
            RootId::from_index(7),
            ExactRational::from_f64(0.75),
            &mut work,
        )?;
        let point = ExactChartPoint::from_rationals(
            ExactRational::from_f64(10.0),
            ExactRational::from_f64(20.0),
        );
        let port_data =
            |requests: &[RootRequest], work: &mut Unmetered| -> Result<NativePortData> {
                Ok(NativePortData {
                    node: EmissionNodeId(3),
                    point: point.clone(),
                    spelling: PortSpelling::Interior { periods: 0 },
                    source_record: None,
                    occurrences: native_source_sample_occurrences(None, requests, work)?,
                })
            };
        let mut unique = Vec::new();
        for request in [&left_request, &right_request] {
            let boundary = NativeKnownSpanBoundary {
                span: ChartSpanId(0),
                parameter: ExactRational::zero(),
                port: QPortId(5),
                data: port_data(std::slice::from_ref(request), &mut work)?,
            };
            append_native_known_span_boundary(
                &mut unique,
                boundary,
                GeographicFrame::crs84(),
                &mut work,
            )?;
        }

        assert_eq!(unique.len(), 1);
        let boundary = unique
            .first()
            .ok_or_else(|| GeometryErrorKind::Invalid("merged known boundary is missing".into()))?;
        assert_eq!(boundary.data.occurrences.len(), 2);
        let first = boundary
            .data
            .occurrences
            .first()
            .and_then(|occurrence| occurrence.source_trace.as_ref())
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("first known-boundary root request is missing".into())
            })?;
        let second = boundary
            .data
            .occurrences
            .get(1)
            .and_then(|occurrence| occurrence.source_trace.as_ref())
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("second known-boundary root request is missing".into())
            })?;
        let TraceRequestLocation::Root {
            root: first_root,
            parameter: first_parameter,
        } = first
        else {
            return Err(GeometryErrorKind::Invalid(
                "first known-boundary occurrence lost its direct root".into(),
            )
            .into());
        };
        let TraceRequestLocation::Root {
            root: second_root,
            parameter: second_parameter,
        } = second
        else {
            return Err(GeometryErrorKind::Invalid(
                "second known-boundary occurrence lost its direct root".into(),
            )
            .into());
        };
        assert_eq!(*first_root, RootId::from_index(2));
        assert_eq!(first_parameter, &ExactRational::from_f64(0.25));
        assert_eq!(*second_root, RootId::from_index(7));
        assert_eq!(second_parameter, &ExactRational::from_f64(0.75));
        Ok(())
    }

    #[test]
    fn known_cut_boundary_keeps_the_directed_source_root_parameter() -> Result<()> {
        let source = NativeSourceSpanUse {
            span: ChartSpanId(0),
            owner: 0,
            source_edge: SourceEdgeId(7),
            source_lift: -1,
            source_range: ExactSourceRange {
                start: ExactRational::from_f64(0.75),
                end: ExactRational::from_f64(0.25),
            },
            producer_range: ExactSourceRange {
                start: ExactRational::zero(),
                end: ExactRational::one(),
            },
            span_range: ExactSourceRange {
                start: ExactRational::one(),
                end: ExactRational::zero(),
            },
            nodes: [QNodeId(3), QNodeId(4)],
            ports: [QPortId(5), QPortId(6)],
            face_germs: None,
        };
        let mut work = Unmetered;

        let at_span_start =
            native_source_request_at_known_boundary(&source, &source.span_range.start, &mut work)?;
        let at_span_end =
            native_source_request_at_known_boundary(&source, &source.span_range.end, &mut work)?;

        assert_eq!(at_span_start.root(), RootId::from_index(7));
        assert_eq!(at_span_start.parameter(), &source.source_range.start);
        assert_eq!(at_span_end.root(), RootId::from_index(7));
        assert_eq!(at_span_end.parameter(), &source.source_range.end);
        Ok(())
    }
