    #![expect(
        clippy::panic_in_result_fn,
        reason = "exact coordinate controls use assertions for independently computed bit expectations"
    )]

    use super::*;
    use crate::Frame;
    use crate::geometry::embedding_completion::{LiteralPoint, LiteralRowLayout};
    use crate::geometry::publication::FinishedGeometry;
    use crate::geometry::storage::{
        GeometryArrayStorage, LogicalRow, PackedStorage, RowSelection, StorageLayout,
    };
    use crate::geometry::{CoordinateAxes, OverlayOp, Shape};

    fn topology(shapes: &[Shape]) -> Result<ExactGeographicTopology> {
        let frame = GeographicFrame::crs84();
        let inputs = crate::geographic::GeographicInputs::try_from_shapes(frame, shapes.iter())
            .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
        ExactGeographicTopology::from_views(frame, inputs.views().iter().copied())
    }

    fn native_union_for_shapes(
        shapes: &[crate::geometry::Shape],
    ) -> Result<crate::geometry::Shape> {
        let topology = topology(shapes)?;
        let mut budget = EvaluationBudget::new(OverlayOp::Union.name(), 0)?;
        let plan = QEmissionPlan::build(&topology, OverlayOp::Union, &mut budget)?;
        let nearest = QCoordinates::build_with_work(&topology, &mut budget)?;
        let publication = plan.fragment_recipes.publish(
            &topology,
            &nearest,
            OverlayOp::Union.name(),
            &mut budget,
        )?;
        let source_points = vec![None; topology.q.nodes.len()];
        let drawing = plan.drawing(&topology, &nearest, &source_points, &mut budget)?;
        let live_keys = drawing.live_node_keys(topology.sources.frame, &mut budget)?;
        let emission =
            plan.embedding_emission(&topology, &source_points, &publication, &mut budget)?;
        let ordinates = vec![NodeOrdinates::default(); topology.q.nodes.len()];
        let certification = QEmbeddingCertification::<crate::crs::GeodesicMetric> {
            topology: &topology,
            emission: &emission,
            nearest: &nearest,
            ordinates: &ordinates,
            declared_axes: crate::geometry::CoordinateAxes::XY,
            strict_axes: false,
            operation: OverlayOp::Union.name(),
            metric_access: None,
        };
        let outcome =
            certify_and_search_q_embedding(&certification, &live_keys, drawing, &mut budget)?;
        let SearchOutcome::Pass(QEmbeddingPass { output, .. }) = outcome else {
            return Err(GeometryErrorKind::Invalid(
                "reduced Native union did not complete its Q embedding".into(),
            )
            .into());
        };
        let super::super::emission::SealedQDrawing::Native(native) = output.sealed else {
            return Err(GeometryErrorKind::Invalid(
                "reduced Native union used source-only publication".into(),
            )
            .into());
        };
        let (shape, _frame) = native.sealed.into_scalar()?;
        Ok(shape)
    }

    fn source_drawing_for_storage(
        point: Point,
        target: StorageTarget,
        work: &mut impl Work,
    ) -> Result<SourceFrozenDrawing> {
        let layout = ShapeLiteralLayout {
            points: vec![LiteralPoint {
                output: 0,
                axes: CoordinateAxes::XYZM,
                image: QSourceImage::PublicationSample(0),
            }],
            lines: Vec::new(),
            polygons: Vec::new(),
        };
        let buffers = match target {
            StorageTarget::Scalar => FrozenLiteralBuffers::shapes(Shape::Point(point), layout),
            StorageTarget::Array | StorageTarget::Groups => {
                let storage =
                    std::sync::Arc::new(GeometryArrayStorage::Packed(PackedStorage::Points {
                        coords: std::sync::Arc::new(CoordSeq::try_from_points(&[point])?),
                        row_map: RowSelection::Identity,
                    }));
                let row = LiteralRowLayout {
                    operation: 0,
                    logical_row: 0,
                    group_row: matches!(target, StorageTarget::Groups).then_some(0),
                    value_row: matches!(target, StorageTarget::Groups).then_some(0),
                    layout,
                };
                match target {
                    StorageTarget::Array => FrozenLiteralBuffers::rows(
                        storage,
                        None,
                        StorageLayout::LogicalRows,
                        vec![Some(row)],
                        work,
                    )?,
                    StorageTarget::Groups => FrozenLiteralBuffers::groups(
                        storage,
                        None,
                        StorageLayout::LogicalRows,
                        vec![Some(row)],
                        vec![0, 1],
                        None,
                        work,
                    )?,
                    StorageTarget::Scalar => unreachable!(),
                }
            },
        };
        Ok(SourceFrozenDrawing { buffers })
    }

    fn read_finished_point(
        finished: FinishedGeometry,
        target: StorageTarget,
    ) -> Result<(Option<Point>, Frame)> {
        Ok(match target {
            StorageTarget::Scalar => {
                let (shape, frame) = finished.into_scalar()?;
                let point = match shape {
                    Shape::Point(point) => Some(point),
                    Shape::MultiPoint(_)
                    | Shape::LineString(_)
                    | Shape::MultiLineString(_)
                    | Shape::Polygon(_)
                    | Shape::MultiPolygon(_)
                    | Shape::GeometryCollection(_)
                    | Shape::Empty(..) => None,
                };
                (point, frame)
            },
            StorageTarget::Array => {
                let (rows, frame) = finished.into_array()?;
                let point = match rows.logical_row(0) {
                    Some(LogicalRow::Present { row, .. }) => row.point(),
                    Some(LogicalRow::Missing { .. }) | None => None,
                };
                (point, frame)
            },
            StorageTarget::Groups => {
                let (groups, frame) = finished.into_groups()?;
                let point = match groups.values.logical_row(0) {
                    Some(LogicalRow::Present { row, .. }) => row.point(),
                    Some(LogicalRow::Missing { .. }) | None => None,
                };
                (point, frame)
            },
        })
    }

    fn native_demand_for_shapes(shapes: &[Shape], operation: OverlayOp) -> Result<(bool, bool)> {
        let topology = topology(shapes)?;
        let mut budget = EvaluationBudget::new(operation.name(), 0)?;
        let plan = QEmissionPlan::build(&topology, operation, &mut budget)?;
        let nearest = QCoordinates::build_with_work(&topology, &mut budget)?;
        let publication =
            plan.fragment_recipes
                .publish(&topology, &nearest, operation.name(), &mut budget)?;
        let source_points = vec![None; topology.q.nodes.len()];
        let drawing = plan.drawing(&topology, &nearest, &source_points, &mut budget)?;
        let ring_roles = match check_q_ring_roles(&topology, &drawing, &mut budget)? {
            QRingRoleCheck::Pass(roles) => roles,
            QRingRoleCheck::NativeRequired(_) => {
                return Err(GeometryErrorKind::Invalid(
                    "Native-demand control unexpectedly needs region assembly".into(),
                )
                .into());
            },
            QRingRoleCheck::Reject { .. } => {
                return Err(GeometryErrorKind::Invalid(
                    "Native-demand fixture has an invalid selected ring role".into(),
                )
                .into());
            },
        };
        let emission =
            plan.embedding_emission(&topology, &source_points, &publication, &mut budget)?;
        let ordinates = vec![NodeOrdinates::default(); topology.q.nodes.len()];
        let problem: QEmbeddingProblem<'_, '_, '_, '_, crate::crs::GeodesicMetric> =
            QEmbeddingProblem::new(
                &topology,
                &emission,
                &nearest,
                &ordinates,
                CoordinateAxes::XY,
                false,
                operation.name(),
                None,
                &mut budget,
            )?;
        let publication_coordinates = vec![None; problem.publication_nodes.nodes().len()];
        let source_ports = topology
            .q
            .ports
            .iter()
            .map(|port| Some(port.point.clone()))
            .collect();
        let leaves = problem
            .publication_chords
            .iter()
            .map(|chord| AssignedProducerLeaf {
                recipe: chord.recipe,
                trace_edge: chord.trace_edge,
                points: chord.exact.clone(),
            })
            .collect();
        let producer = AssignedProducerProjection {
            source_ports,
            leaves,
            rejections: Vec::new(),
        };
        let demand = problem.actual_native_demand(
            &drawing,
            &ring_roles,
            &producer,
            &nearest,
            &publication_coordinates,
            &mut budget,
        )?;

        let mut source_nodes = vec![false; topology.q.nodes.len()];
        for ports in &topology.q.source_edge_ports {
            for &port_id in ports {
                let port = topology.q.ports.get(port_id.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("fixture source edge has an unknown Q port".into())
                })?;
                *source_nodes.get_mut(port.node.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("fixture source edge has an unknown Q node".into())
                })? = true;
            }
        }
        for &port_id in &topology.q.source_point_ports {
            let port = topology.q.ports.get(port_id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("fixture source point has an unknown Q port".into())
            })?;
            *source_nodes.get_mut(port.node.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("fixture source point has an unknown Q node".into())
            })? = true;
        }
        let mut constructed_vertex = false;
        let mut inspect = |vertex: &EmittedVertex<'_>| -> Result<()> {
            let source = *source_nodes
                .get(vertex.port.node.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("fixture drawing has an unknown Q node".into())
                })?;
            if !source && !vertex.port.cut {
                constructed_vertex = true;
            }
            Ok(())
        };
        match &drawing.polygons {
            DrawnPolygons::WholeSphere(polygons) => {
                for polygon in polygons {
                    for ring in polygon {
                        for vertex in &ring.vertices {
                            inspect(vertex)?;
                        }
                    }
                }
            },
            DrawnPolygons::Regions { rings, .. } => {
                for ring in rings {
                    for vertex in &ring.vertices {
                        inspect(vertex)?;
                    }
                }
            },
        }
        Ok((demand, constructed_vertex))
    }

    fn reversed_lift_native_refinement_fixture(
        event: NativePortData,
        event_port: QPortId,
    ) -> (NativeAreaEdge, NativeKnownSpanBoundary) {
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
                known_spans: vec![source],
            },
        };
        let boundary = NativeKnownSpanBoundary {
            span: ChartSpanId(0),
            parameter: ExactRational::zero(),
            port: event_port,
            data: event,
        };
        (edge, boundary)
    }

    struct NativeRefinementSearch {
        classes: Vec<Vec<u8>>,
        visits: Vec<u8>,
    }

    impl Problem for NativeRefinementSearch {
        type Node = u8;
        type Candidate = u8;
        type Key = u8;
        type ClassState = ();
        type Output = u8;

        fn classes(&self) -> &[Vec<Self::Node>] {
            &self.classes
        }

        fn fixed_keys<W: Work>(
            &self,
            _class: usize,
            _variables: &[Self::Node],
            work: &mut W,
        ) -> Result<Vec<Self::Key>> {
            work.charge_into_error(1)?;
            Ok(Vec::new())
        }

        fn compare_nodes<W: Work>(
            &self,
            left: Self::Node,
            right: Self::Node,
            work: &mut W,
        ) -> Result<std::cmp::Ordering> {
            work.charge_into_error(1)?;
            Ok(left.cmp(&right))
        }

        fn fill_candidates<W: Work>(
            &self,
            _node: Self::Node,
            candidates: &mut Vec<Self::Candidate>,
            work: &mut W,
        ) -> Result<()> {
            work.charge_into_error(2)?;
            candidates.extend([0, 1]);
            Ok(())
        }

        fn compare_candidates<W: Work>(
            &self,
            _node: Self::Node,
            left: Self::Candidate,
            right: Self::Candidate,
            work: &mut W,
        ) -> Result<std::cmp::Ordering> {
            work.charge_into_error(1)?;
            Ok(left.cmp(&right))
        }

        fn candidate_key(&self, candidate: Self::Candidate) -> Self::Key {
            candidate
        }

        fn prepare_class<A: SearchAccount>(
            &self,
            _class: usize,
            _variables: &[Self::Node],
            account: &mut A,
        ) -> Result<Self::ClassState> {
            account.charge_evaluations(1)?;
            Ok(())
        }

        fn check_local<A: SearchAccount>(
            &self,
            _class_state: &Self::ClassState,
            _class: usize,
            _variables: &[Self::Node],
            _rank: usize,
            _assigned: &[Option<Self::Candidate>],
            account: &mut A,
        ) -> Result<bool> {
            account.charge_evaluations(1)?;
            Ok(true)
        }

        fn check_global<A: SearchAccount>(
            &mut self,
            assignments: &[ClassAssignment<'_, Self::Node, Self::Candidate>],
            account: &mut A,
        ) -> Result<GlobalCheck<Self::Output>> {
            account.charge_evaluations(1)?;
            let candidate = assignments
                .first()
                .and_then(|assignment| assignment.values.first())
                .copied()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("native refinement search has no assignment".into())
                })?;
            self.visits.push(candidate);
            let prepared = if candidate == 0 {
                Err(GeometryErrorKind::NativeRefinementMismatch.into())
            } else {
                Ok(candidate)
            };
            native_reference_global_check(prepared, || Ok(vec![0]))
        }
    }
































    fn single_sample_native_reference<'topology>(
        topology: &'topology ExactGeographicTopology,
        budget: &mut EvaluationBudget,
        assigned: QCandidate,
    ) -> Result<(NativeReference<'topology>, EmissionPortId)> {
        use super::super::emission::{
            EmissionBuilder, FeatureFamily, FeatureFamilyParent, PublishedSample,
            SampleProducerKind, VertexOrigin,
        };

        let selected = QEmissionPlan::build(topology, OverlayOp::Union, budget)?;
        let exact = ExactChartPoint::from_rationals(
            ExactRational::from_f64(10.0).try_add(&ExactRational::from_f64(2.0_f64.powi(-54)))?,
            ExactRational::from_f64(1.0).try_add(&ExactRational::from_f64(2.0_f64.powi(-54)))?,
        );
        let sample = PublishedSample {
            request_row: RequestRowId::from_index(0)?,
            family: FeatureFamily {
                parent: FeatureFamilyParent::Fragment(FragmentRecipeId(0)),
                producer: SampleProducerKind::Publication,
                occurrence: 0,
            },
            parameter: ExactRational::from_f64(0.5),
            written_vertex: 1,
            point: Point::new_raw_xy(10.0, 1.0),
            exact_point: exact,
            published_chart_point: ExactChartPoint::from_rationals(
                ExactRational::from_f64(10.0),
                ExactRational::from_f64(1.0),
            ),
            source_trace: None,
            endpoint_only: false,
        };
        let mut builder = EmissionBuilder::new(topology, &selected, &[], false, budget)?;
        let sample_nodes = builder.reserve_publication_nodes(&[sample], budget)?;
        let sample_node = *sample_nodes.first().ok_or_else(|| {
            GeometryErrorKind::Invalid("Native literal fixture has no sample node".into())
        })?;
        let assigned_point =
            candidate_chart_point_with_work(topology.sources.frame, assigned, budget)?;
        let port_data = NativePortData {
            node: sample_node,
            point: assigned_point,
            spelling: PortSpelling::Interior { periods: 0 },
            source_record: None,
            occurrences: vec![OccurrenceSource {
                origin: VertexOrigin::SourceSample,
                source_trace: None,
            }],
        };
        let port_id = builder.port(port_data.try_clone_with_work(budget)?)?;
        builder.point(port_id)?;
        let plan = builder.finish();
        let graph_builder = new_native_reference_graph_builder(&[], &[port_data], budget)?;
        let graph = graph_builder.finish(topology.sources.frame, budget)?;
        let dimension =
            admit_logical_layout(&graph.common, Vec::new(), Some(&graph.faces), budget)?;
        let vertex = graph.local_vertex(
            native_emission_port_key(
                plan.ports
                    .get(port_id.0 as usize)
                    .ok_or_else(|| GeometryErrorKind::Invalid("sample port is unknown".into()))?,
            ),
            budget,
        )?;
        let reference = NativeReference {
            plan,
            dimension,
            port_vertices: vec![vertex],
            sites: vec![Some(assigned_emission_site(
                topology.sources.frame,
                assigned,
            )?)],
            feature_nodes: Vec::new(),
            publication_sample_nodes: vec![sample_node],
            line_traces: Vec::new(),
            literal_chord_uses: Vec::new(),
            line_graph_edges: Vec::new(),
            area_edges: Vec::new(),
            pole_sectors: Vec::new(),
            graph,
        };
        Ok((reference, port_id))
    }




    fn fixtures() -> Result<Vec<(&'static str, Vec<crate::geometry::Shape>)>> {
        Ok(vec![
            ("stored_alias", vec![crate::io::parse_wkt(
                "LINESTRING (122.427194 10, 123.427194 11)",
            )?]),
            ("exact_crossing", vec![
                crate::io::parse_wkt("LINESTRING (0 0, 2 2)")?,
                crate::io::parse_wkt("LINESTRING (0 2, 2 0)")?,
            ]),
            ("inexact_crossing_with_pinned_latitude", vec![
                crate::io::parse_wkt("LINESTRING (0 0, 2 0)")?,
                crate::io::parse_wkt("LINESTRING (0 1, 1 -2)")?,
            ]),
            ("seam_cut_and_pole", vec![
                crate::io::parse_wkt(
                    "POLYGON ((-180 10, -90 10, 0 10, 90 10, 180 10, 180 20, 90 20, 0 20, -90 20, -180 20, -180 10))",
                )?,
                crate::io::parse_wkt("POINT (0 90)")?,
            ]),
        ])
    }
