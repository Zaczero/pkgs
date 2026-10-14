    #[test]
    fn published_sample_has_its_own_native_neighbor_domain() -> Result<()> {
        let topology = topology(&[crate::io::parse_wkt("LINESTRING (0 0, 10 10)")?])?;
        let frame = topology.sources.frame;
        let half_ulp = ExactRational::from_f64(2.0_f64.powi(-54));
        let exact = ExactChartPoint::from_rationals(
            ExactRational::from_f64(10.0).try_add(&half_ulp)?,
            ExactRational::from_f64(1.0).try_add(&half_ulp)?,
        );
        let mut work = crate::work::Unmetered;
        let mut nodes = EmissionNodeRegistry::with_sources(&topology, &[], &mut work)?;
        let node = nodes.intern_computed(frame, &exact, &mut work)?;
        let mut publication = PublicationInventory {
            fragments: Vec::new(),
            samples: vec![super::super::emission::PublishedSample {
                request_row: RequestRowId::from_index(0)?,
                family: super::super::emission::FeatureFamily {
                    parent: super::super::emission::FeatureFamilyParent::Fragment(
                        super::super::emission::FragmentRecipeId(0),
                    ),
                    producer: super::super::emission::SampleProducerKind::Publication,
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
            }],
            counts: Vec::new(),
            ordinates: vec![NodeOrdinates::default()],
        };
        let mut budget = EvaluationBudget::new("Q publication domain", 0)?;
        let domains = published_domains(&topology, &publication, &nodes, &[node], &mut budget)?;

        assert_eq!(domains.variables, [node]);
        let nearest = domains
            .nearest
            .get(node.0 as usize)
            .copied()
            .flatten()
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("published nearest site is missing".into())
            })?;
        let candidates = domains
            .domains
            .get(node.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("published domain is missing".into()))?;
        assert_eq!(candidates.len(), 9);
        assert!(candidates.contains(&nearest));
        let fixed_source_axes = vec![[false, false]; topology.q.nodes.len()];
        let producer_witness = PreparedProducerWitness::default();
        let UnifiedQClassBuild {
            source_variable_count: source_base,
            classes,
            node_location: source_location,
            publication_location,
            states,
        } = build_unified_q_classes(
            &UnifiedQClassInputs {
                topology: &topology,
                source_classes: &[],
                publication_variables: &domains.variables,
                publication_nodes: &nodes,
                source_chords: &[],
                publication_chords: &[],
                proximity_pairs: &[],
                cut_rows: &[],
                fan_rows: &[],
                publication_pairs: &[],
                source_domains: &[],
                source_movable_axes: &fixed_source_axes,
                publication_domains: &domains.domains,
                producer_witness: &producer_witness,
            },
            &mut budget,
        )?;
        assert_eq!(source_base, 0);
        assert_eq!(classes, vec![vec![QVariableNode::Published(node)]]);
        assert!(source_location.iter().all(Option::is_none));
        assert_eq!(
            publication_location.get(node.0 as usize).copied().flatten(),
            Some((0, 0))
        );
        assert_eq!(states.len(), 1);

        publication
            .samples
            .get_mut(0)
            .ok_or_else(|| GeometryErrorKind::Invalid("published sample is missing".into()))?
            .published_chart_point
            .y = ExactRational::from_f64(1.0_f64.next_up());
        let error = published_domains(&topology, &publication, &nodes, &[node], &mut budget)
            .err()
            .ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "one-ULP wrong assigned Y must reject its frozen publication image".into(),
                )
            })?;
        assert!(
            error
                .to_string()
                .contains("published nearest image differs from its exact source reference"),
            "wrong assigned-Y rejection came from an unexpected owner: {error}"
        );
        Ok(())
    }
    #[test]
    fn checked_publication_samples_reject_an_omitted_leaf_occurrence() -> Result<()> {
        let line = crate::io::parse_wkt("LINESTRING (0 0, 360 10)")?;
        let topology = topology(&[line])?;
        let mut budget = EvaluationBudget::new("omitted publication sample", 0)?;
        let plan = QEmissionPlan::build(&topology, OverlayOp::Union, &mut budget)?;
        let nearest = QCoordinates::build_with_work(&topology, &mut budget)?;
        let publication = plan.fragment_recipes.publish(
            &topology,
            &nearest,
            OverlayOp::Union.name(),
            &mut budget,
        )?;
        assert!(
            !publication.samples.is_empty(),
            "the full-turn fixture must publish at least one checked sample"
        );
        let source_points = vec![None; topology.sources.points.len()];
        let emission =
            plan.embedding_emission(&topology, &source_points, &publication, &mut budget)?;
        let ordinates = vec![NodeOrdinates::default(); topology.q.nodes.len()];
        let metric_access: Option<&SourceArcMetricAccess<'_, '_, crate::crs::GeodesicMetric>> =
            None;
        let mut problem = QEmbeddingProblem::new(
            &topology,
            &emission,
            &nearest,
            &ordinates,
            crate::geometry::CoordinateAxes::XY,
            false,
            "omitted publication sample",
            metric_access,
            &mut budget,
        )?;

        let omitted_sample = 0;
        let original_chord_count = problem.publication_chords.len();
        problem.publication_chords.retain(|chord| {
            !chord
                .endpoints
                .iter()
                .any(|endpoint| publication_sample_index(endpoint) == Some(omitted_sample))
        });
        assert!(
            problem.publication_chords.len() < original_chord_count,
            "the omitted sample must have producer-leaf occurrences"
        );
        let producer = AssignedProducerProjection {
            source_ports: Vec::new(),
            leaves: problem
                .publication_chords
                .iter()
                .map(|chord| AssignedProducerLeaf {
                    recipe: chord.recipe,
                    trace_edge: chord.trace_edge,
                    points: chord.exact.clone(),
                })
                .collect(),
            rejections: Vec::new(),
        };
        let error = problem
            .checked_publication_sample_images(&producer, &mut budget)
            .err()
            .ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "every publication occurrence must have a checked leaf image".into(),
                )
            })?;
        assert!(
            error
                .to_string()
                .contains("publication sample has no checked source-port or leaf image"),
            "omitted-sample rejection came from an unexpected owner: {error}"
        );
        Ok(())
    }
    #[test]
    fn native_literal_readback_rejects_a_post_pass_published_port_mutation() -> Result<()> {
        use super::super::emission::native_port_point;
        use crate::geometry::Shape;

        let topology = topology(&[crate::io::parse_wkt("POINT (0 0)")?])?;
        let mut budget = EvaluationBudget::new("native literal readback", 0)?;
        let assigned = QCandidate {
            native: XY::new(10.0, 1.0),
            periods: 0,
        };
        let (reference, port_id) =
            single_sample_native_reference(&topology, &mut budget, assigned)?;
        let vertex = *reference
            .port_vertices
            .get(port_id.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("sample graph vertex is unknown".into()))?;
        let sites = [assigned_emission_site(topology.sources.frame, assigned)?];
        let ordinates = [NodeOrdinates::default()];
        let mut record = native_port_point(
            &reference.plan,
            &sites,
            &ordinates,
            port_id,
            crate::geometry::CoordinateAxes::XY,
        )?;
        record.y = record.y.next_up();
        let mut exact_points = reference.graph.points.exact.clone();
        exact_points
            .get_mut(vertex as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("published vertex is unknown".into()))?
            .y = ExactRational::from_f64(record.y);
        let candidate_points = NativeReferencePoints::from_exact(exact_points, &mut budget)?;
        let checked = admit_candidate(
            &reference.graph.common,
            &reference.dimension,
            candidate_points,
            &mut budget,
        )?
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("mutated native test assignment was not admitted".into())
        })?;
        let assignment = NativeCandidateAssignment::issue(&reference, checked)?;
        let mut port_records = vec![None; reference.plan.ports.len()];
        let buffers = FrozenLiteralBuffers::shapes(Shape::Point(record), ShapeLiteralLayout {
            structure: Some(crate::geometry::embedding_completion::literals::ShapeStructure::capture(&Shape::Point(record))),
            points: vec![LiteralPoint {
                output: 0,
                axes: crate::geometry::CoordinateAxes::XY,
                image: port_id,
            }],
            lines: Vec::new(),
            polygons: Vec::new(),
        });
        let receipt = NativeCandidatePublication {
            assignment: &assignment,
            buffers,
            sites: &sites,
            ordinates: &ordinates,
            port_records: &mut port_records,
        };
        assert!(
            matches!(
                finish_geographic(
                    PublicationCall::new(
                        ProducerReceipt::QNative(receipt),
                        topology.publication_frame().clone(),
                        &mut budget,
                    ),
                    StorageTarget::Scalar,
                )?,
                PublicationOutcome::Reject(PublicationRejection::Native)
            ),
            "the common Native binder rejects a post-admission endpoint mutation"
        );
        Ok(())
    }
    #[test]
    fn native_assignment_factory_rejects_crosswired_graph_layout_and_images() -> Result<()> {
        let topology = topology(&[crate::io::parse_wkt("POINT (0 0)")?])?;
        let mut budget = EvaluationBudget::new("crosswired native receiver", 0)?;
        let (reference, _) = single_sample_native_reference(&topology, &mut budget, QCandidate {
            native: XY::new(10.0, 1.0),
            periods: 0,
        })?;
        let (other_reference, _) =
            single_sample_native_reference(&topology, &mut budget, QCandidate {
                native: XY::new(11.0, 1.0),
                periods: 0,
            })?;
        let foreign_assignment = admit_candidate(
            &other_reference.graph.common,
            &other_reference.dimension,
            other_reference.graph.points.clone(),
            &mut budget,
        )?
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("foreign reference assignment was not admitted".into())
        })?;

        assert!(
            NativeCandidateAssignment::issue(&reference, foreign_assignment).is_err(),
            "a graph, layout, and image from another native reference must not issue a receiver"
        );
        Ok(())
    }
    #[test]
    fn native_receiver_rejects_missing_producer_literal_rows() -> Result<()> {
        let topology = topology(&[crate::io::parse_wkt("POINT (0 0)")?])?;
        let mut budget = EvaluationBudget::new("native literal row binding", 0)?;
        let assigned = QCandidate {
            native: XY::new(10.0, 1.0),
            periods: 0,
        };
        let (reference, _) = single_sample_native_reference(&topology, &mut budget, assigned)?;
        let checked = admit_candidate(
            &reference.graph.common,
            &reference.dimension,
            reference.graph.points.clone(),
            &mut budget,
        )?
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("unchanged Native reference assignment was rejected".into())
        })?;
        let assignment = NativeCandidateAssignment::issue(&reference, checked)?;
        let sites = [assigned_emission_site(topology.sources.frame, assigned)?];
        let ordinates = [NodeOrdinates::default()];
        let mut port_records = vec![None; reference.plan.ports.len()];
        let missing_rows = FrozenLiteralBuffers::shapes(
            crate::geometry::Shape::Empty(
                crate::geometry::GeometryKind::Point,
                crate::geometry::CoordinateAxes::XY,
            ),
            ShapeLiteralLayout {
                structure: Some(crate::geometry::embedding_completion::literals::ShapeStructure::primitive(
                    crate::geometry::GeometryKind::Point, crate::geometry::CoordinateAxes::XY, true,
                )),
                points: Vec::<LiteralPoint<EmissionPortId>>::new(),
                lines: Vec::new(),
                polygons: Vec::new(),
            },
        );

        let receipt = NativeCandidatePublication {
            assignment: &assignment,
            buffers: missing_rows,
            sites: &sites,
            ordinates: &ordinates,
            port_records: &mut port_records,
        };
        assert!(
            matches!(
                finish_geographic(
                    PublicationCall::new(
                        ProducerReceipt::QNative(receipt),
                        topology.publication_frame().clone(),
                        &mut budget,
                    ),
                    StorageTarget::Scalar,
                )?,
                PublicationOutcome::Reject(PublicationRejection::Native)
            ),
            "the consuming Q sink must reject a sealed output that omits its point row"
        );
        Ok(())
    }
