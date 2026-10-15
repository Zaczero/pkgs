use super::super::{
    AssignedProducerProjection, AssignedSite, BTreeSet, ChartMetric, EvaluationBudget,
    ExactChartPoint, ExactQuotientKey, ExactRational, GeometryErrorKind, GlobalCheck,
    NativeAreaEdge, NativeAreaEdgeKind, NativeChordPieces, NativeCutFeature, NativeCutOrder,
    NativeCutOrderSite, NativeCutProblem, NativeCutSearchOutcome, NativeGraphEdgeOwner,
    NativePortData, NativeReference, NativeSeamEvent, NativeSeamEventData, NativeSeamVertices,
    NodeAuthority, OccurrenceSource, PublishedEndpoint, QCandidate, QCoordinates,
    QEmbeddingProblem, QFamilyState, QPublishedChord, ResourceError, Result, RootRequest,
    SearchAccount, SearchOutcome, align_native_port_to_seam, candidate_from_emission_site,
    compare_quotient_keys_with_work, emitted_node_key, native_cut_candidate_domain,
    native_cut_order_holds, native_known_source_spans_for_slice, native_port_data,
    native_source_sample_occurrences, quotient_key_for_chart_position_with_work, run_with_budget,
    trace_ranges_for_slice, try_sort_by_with_work,
};
use crate::geometry::topology::emission::SourceTrace;
use crate::work::Work;

impl<'topology, M: ChartMetric> QEmbeddingProblem<'topology, '_, '_, '_, M> {
    pub(super) fn materialize_native_cut_features(
        &mut self,
        producer: &AssignedProducerProjection,
        budget: &mut impl SearchAccount,
    ) -> Result<Vec<NativeCutFeature>> {
        if producer.leaves.len() != self.publication_chords.len() {
            return Err(GeometryErrorKind::Invalid(
                "native cut materialization lost its assigned producer leaves".into(),
            )
            .into());
        }
        let mut output = Vec::new();
        for (chord_index, chord) in self.publication_chords.iter().enumerate() {
            budget.charge_into_error(1)?;
            let leaf = producer.leaves.get(chord_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("native cut producer leaf is unknown".into())
            })?;
            if leaf.recipe != chord.recipe || leaf.trace_edge != chord.trace_edge {
                return Err(GeometryErrorKind::Invalid(
                    "native cut producer projection changed its leaf identity".into(),
                )
                .into());
            }
            let [start, end] = &leaf.points;
            let displacement = end.x.try_subtract_with_work(&start.x, budget)?;
            let recipe = self
                .emission
                .fragment_recipes
                .get(chord.recipe.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("native producer recipe is unknown".into())
                })?;
            let fragment = self
                .emission
                .publication
                .fragments
                .get(chord.recipe.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("native producer trace is unknown".into())
                })?;
            let features = super::super::super::emission::native_cut_features(
                self.topology,
                recipe,
                &fragment.trace,
                chord.trace_edge,
                start,
                end,
                &displacement,
                budget,
            )?;
            output.extend(features);
        }
        let QFamilyState::Native(families) = &mut self.family_state else {
            return Err(GeometryErrorKind::Invalid(
                "native cuts were materialized before Native family activation".into(),
            )
            .into());
        };
        for feature in &mut output {
            budget.charge_into_error(1)?;
            let parent = feature.family.fragment_recipe().ok_or_else(|| {
                GeometryErrorKind::Invalid("native cut has no fragment family".into())
            })?;
            feature.family.occurrence = families.native_cut_occurrence(
                parent,
                feature.trace_edge,
                feature.seam_period_index,
                budget,
            )?;
            families.register(feature.family, budget)?;
        }
        families.synchronize_budget(budget)?;
        Ok(output)
    }
    pub(super) fn all_outer_repair_classes(&self, budget: &mut impl Work) -> Result<Vec<usize>> {
        let variable_count = self.classes.iter().try_fold(0_usize, |count, class| {
            count
                .checked_add(class.len())
                .ok_or(ResourceError::SizeOverflow)
        })?;
        budget.charge_into_error(
            u64::try_from(variable_count).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut variables = Vec::with_capacity(variable_count);
        for class in &self.classes {
            variables.extend_from_slice(class);
        }
        self.classes_for_variables(variables, budget)
    }
    pub(super) fn search_native_cut_candidates(
        &self,
        reference: &NativeReference<'topology>,
        features: &[NativeCutFeature],
        coordinates: &QCoordinates,
        publication_coordinates: &[Option<QCandidate>],
        budget: &mut EvaluationBudget,
    ) -> Result<NativeCutSearchOutcome> {
        let frame = self.topology.sources.frame;
        if reference.publication_sample_nodes.len() != self.emission.publication.samples.len()
            || reference.feature_nodes.len() != features.len()
        {
            return Err(GeometryErrorKind::Invalid(
                "native reference nodes do not match its publication and cut rows".into(),
            )
            .into());
        }
        let nodes = &reference.plan.node_registry;
        if reference.sites.len() != nodes.nodes().len() {
            return Err(GeometryErrorKind::Invalid(
                "native reference sites do not cover every emission node".into(),
            )
            .into());
        }
        budget.charge_into_error(
            u64::try_from(reference.sites.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut fixed_sites = Vec::with_capacity(reference.sites.len());
        for site in &reference.sites {
            budget.charge_into_error(1)?;
            fixed_sites.push(
                site.as_ref()
                    .map(|site| candidate_from_emission_site(*site)),
            );
        }
        budget.charge_into_error(
            u64::try_from(nodes.nodes().len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut has_publication_sample_by_node = vec![false; nodes.nodes().len()];
        for &node in &reference.publication_sample_nodes {
            budget.charge_into_error(1)?;
            if matches!(
                nodes
                    .nodes()
                    .get(node.0 as usize)
                    .map(|value| &value.authority),
                Some(NodeAuthority::Computed(_))
            ) {
                let slot = has_publication_sample_by_node
                    .get_mut(node.0 as usize)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "native publication sample node is outside its registry".into(),
                        )
                    })?;
                *slot = true;
            }
        }

        let mut variable_for_node = vec![None; nodes.nodes().len()];
        let mut points = Vec::new();
        let mut domains = Vec::new();
        let mut feature_sites = Vec::with_capacity(features.len());
        for (&node_id, feature) in reference.feature_nodes.iter().zip(features) {
            let node_index = node_id.0 as usize;
            let node = nodes.nodes().get(node_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("native cut emission node is unknown".into())
            })?;
            match &node.authority {
                NodeAuthority::Source(source_node) => {
                    let index = source_node.0 as usize;
                    let candidate = QCandidate {
                        native: coordinates.xy(*source_node)?,
                        periods: *coordinates.longitude_periods.get(index).ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "native source cut node has no retained lift".into(),
                            )
                        })?,
                    };
                    feature_sites.push(NativeCutOrderSite::Fixed(candidate));
                },
                NodeAuthority::Computed(_) => {
                    let has_publication_sample = *has_publication_sample_by_node
                        .get(node_index)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "native cut publication node has no sample state".into(),
                            )
                        })?;
                    if has_publication_sample {
                        let candidate = publication_coordinates
                            .get(node_index)
                            .copied()
                            .flatten()
                            .or_else(|| self.publication_nearest.get(node_index).copied().flatten())
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "native cut matching a publication sample has no assigned site"
                                        .into(),
                                )
                            })?;
                        feature_sites.push(NativeCutOrderSite::Fixed(candidate));
                    } else {
                        let variable = if let Some(variable) =
                            variable_for_node.get(node_index).copied().flatten()
                        {
                            variable
                        } else {
                            let variable = points.len();
                            let point = feature.point.clone();
                            budget.charge_into_error(3)?;
                            let domain = native_cut_candidate_domain(frame, &point, budget)?;
                            if domain.is_empty() {
                                return Ok(NativeCutSearchOutcome::Reject(
                                    self.all_outer_repair_classes(budget)?,
                                ));
                            }
                            *variable_for_node.get_mut(node_index).ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "native computed cut node has no variable slot".into(),
                                )
                            })? = Some(variable);
                            points.push(point);
                            domains.push(domain);
                            variable
                        };
                        feature_sites.push(NativeCutOrderSite::Variable(variable));
                    }
                },
            }
        }

        let mut event_order = (0..features.len()).collect::<Vec<_>>();
        budget.charge_into_error(
            u64::try_from(event_order.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        try_sort_by_with_work(&mut event_order, budget, |&left, &right, budget| {
            budget.charge_into_error(1)?;
            let left = features.get(left).ok_or_else(|| {
                GeometryErrorKind::Invalid("native cut ordering has an unknown feature".into())
            })?;
            let right = features.get(right).ok_or_else(|| {
                GeometryErrorKind::Invalid("native cut ordering has an unknown feature".into())
            })?;
            let latitude = left.point.y.try_cmp_with_work(&right.point.y, budget)?;
            Ok(latitude.then_with(|| left.family.cmp(&right.family)))
        })?;
        let mut unique_nodes = BTreeSet::new();
        let mut unique_event_order = Vec::with_capacity(event_order.len());
        for event in event_order {
            let node = *reference.feature_nodes.get(event).ok_or_else(|| {
                GeometryErrorKind::Invalid("native cut ordering has an unknown node".into())
            })?;
            if unique_nodes.insert(node) {
                unique_event_order.push(event);
            }
        }
        let mut orders = Vec::new();
        let mut previous_event = None;
        for event in unique_event_order {
            let Some(previous) = previous_event else {
                previous_event = Some(event);
                continue;
            };
            let before_node = *reference.feature_nodes.get(previous).ok_or_else(|| {
                GeometryErrorKind::Invalid("native cut order has no previous node".into())
            })?;
            let after_node = *reference.feature_nodes.get(event).ok_or_else(|| {
                GeometryErrorKind::Invalid("native cut order has no next node".into())
            })?;
            if before_node == after_node {
                continue;
            }
            let before_feature = features.get(previous).ok_or_else(|| {
                GeometryErrorKind::Invalid("native cut order has no previous feature".into())
            })?;
            let after_feature = features.get(event).ok_or_else(|| {
                GeometryErrorKind::Invalid("native cut order has no next feature".into())
            })?;
            let ascending = before_feature
                .point
                .y
                .try_cmp_with_work(&after_feature.point.y, budget)?
                .is_lt();
            if before_feature
                .point
                .y
                .try_cmp_with_work(&after_feature.point.y, budget)?
                .is_eq()
            {
                return Ok(NativeCutSearchOutcome::Reject(
                    self.all_outer_repair_classes(budget)?,
                ));
            }
            let before_site = *feature_sites.get(previous).ok_or_else(|| {
                GeometryErrorKind::Invalid("native cut order has no previous site".into())
            })?;
            let after_site = *feature_sites.get(event).ok_or_else(|| {
                GeometryErrorKind::Invalid("native cut order has no next site".into())
            })?;
            if let (NativeCutOrderSite::Fixed(before), NativeCutOrderSite::Fixed(after)) =
                (before_site, after_site)
            {
                budget.charge_into_error(1)?;
                if !native_cut_order_holds(ascending, before, after) {
                    return Ok(NativeCutSearchOutcome::Reject(
                        self.all_outer_repair_classes(budget)?,
                    ));
                }
            } else {
                orders.push(NativeCutOrder {
                    before: before_site,
                    after: after_site,
                    ascending,
                });
            }
            previous_event = Some(event);
        }

        budget.charge_into_error(
            u64::try_from(nodes.nodes().len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut fixed_keys = Vec::new();
        for node_index in 0..nodes.nodes().len() {
            if variable_for_node
                .get(node_index)
                .copied()
                .flatten()
                .is_some()
            {
                continue;
            }
            let candidate = fixed_sites
                .get(node_index)
                .copied()
                .flatten()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "fixed native reference node has no assigned site".into(),
                    )
                })?;
            let key = emitted_node_key(frame, candidate.native)?;
            fixed_keys.push(key);
        }

        let variable_count = points.len();
        let no_variables = variable_count == 0;
        let complete = |candidates: &[QCandidate],
                        budget: &mut EvaluationBudget|
         -> Result<
            GlobalCheck<super::super::super::emission::CompletedQDrawing>,
        > {
            if candidates.len() != variable_count {
                return Err(GeometryErrorKind::Invalid(
                    "native cut completion candidates do not match their variable table".into(),
                )
                .into());
            }
            let mut feature_candidates = Vec::with_capacity(feature_sites.len());
            for &site in &feature_sites {
                budget.charge_into_error(1)?;
                let candidate = match site {
                    NativeCutOrderSite::Variable(variable) => {
                        *candidates.get(variable).ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "native cut completion omitted a variable".into(),
                            )
                        })?
                    },
                    NativeCutOrderSite::Fixed(candidate) => candidate,
                };
                feature_candidates.push(candidate);
            }
            match self.complete_native_drawing(reference, features, &feature_candidates, budget)? {
                Some(drawing) => Ok(GlobalCheck::Pass(drawing)),
                None if no_variables => Ok(GlobalCheck::Reject(Vec::new())),
                None => Ok(GlobalCheck::Reject(vec![0])),
            }
        };
        if no_variables {
            return match complete(&[], budget)? {
                GlobalCheck::Pass(drawing) => Ok(NativeCutSearchOutcome::Pass(drawing)),
                GlobalCheck::Reject(_) => Ok(NativeCutSearchOutcome::Reject(
                    self.all_outer_repair_classes(budget)?,
                )),
            };
        }
        let variables = (0..points.len()).collect::<Vec<_>>();
        let mut problem = NativeCutProblem {
            frame,
            points,
            domains,
            classes: vec![variables],
            fixed_keys,
            orders,
            complete,
        };
        match run_with_budget(&mut problem, budget)? {
            SearchOutcome::Pass(assignment) => Ok(NativeCutSearchOutcome::Pass(assignment.output)),
            SearchOutcome::Exhausted => Ok(NativeCutSearchOutcome::Reject(
                self.all_outer_repair_classes(budget)?,
            )),
        }
    }
    fn native_endpoint_data(
        &self,
        builder: &super::super::super::emission::EmissionBuilder<'topology>,
        endpoint: &PublishedEndpoint,
        assigned_point: &ExactChartPoint,
        source_requests: &[RootRequest],
        sites: &[Option<AssignedSite>],
        bank: Option<super::super::super::emission::ChartSide>,
        budget: &mut impl Work,
    ) -> Result<NativePortData> {
        let (node, candidate, source_record, occurrences) = match endpoint {
            PublishedEndpoint::Source {
                port,
                source_record,
                sample,
            } => {
                let q_port = self.topology.q.ports.get(port.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("native source endpoint port is unknown".into())
                })?;
                let node = builder.source_node(q_port.node)?;
                let site = sites
                    .get(node.0 as usize)
                    .and_then(Option::as_ref)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "native source endpoint has no assigned site".into(),
                        )
                    })?;
                let candidate = candidate_from_emission_site(*site);
                if let Some(record) = source_record {
                    super::super::super::emission::native_source_record_quotient_key(
                        self.topology.sources.frame,
                        *record,
                        assigned_point,
                        budget,
                    )?;
                }
                let occurrences = if let Some(sample) = sample {
                    if let Some(sample_index) = sample.publication_sample
                        && self
                            .publication_sample_source_ports
                            .get(sample_index)
                            .copied()
                            .flatten()
                            != Some(*port)
                    {
                        return Err(GeometryErrorKind::Invalid(
                            "Native source sample changed its Q-port authority".into(),
                        )
                        .into());
                    }
                    if q_port.cut {
                        native_source_sample_occurrences(
                            sample
                                .trace
                                .as_ref()
                                .map(|trace| trace.try_clone_with_work(budget))
                                .transpose()?,
                            source_requests,
                            budget,
                        )?
                    } else {
                        budget.charge_into_error(2)?;
                        let mut occurrences = Vec::with_capacity(2);
                        if sample.trace.is_some() && !sample.endpoint_only {
                            occurrences.push(OccurrenceSource {
                                origin: super::super::super::emission::VertexOrigin::SourceNode(
                                    q_port.node,
                                ),
                                source_trace: None,
                            });
                        }
                        occurrences.push(OccurrenceSource {
                            origin: sample.trace.as_ref().map_or(
                                super::super::super::emission::VertexOrigin::Presentation,
                                |_| super::super::super::emission::VertexOrigin::SourceSample,
                            ),
                            source_trace: sample
                                .trace
                                .as_ref()
                                .map(|trace| trace.try_clone_with_work(budget))
                                .transpose()?,
                        });
                        occurrences
                    }
                } else if q_port.cut {
                    return Err(GeometryErrorKind::Invalid(
                        "cut publication endpoint has no authenticated source row".into(),
                    )
                    .into());
                } else {
                    vec![OccurrenceSource {
                        origin: super::super::super::emission::VertexOrigin::SourceNode(
                            q_port.node,
                        ),
                        source_trace: None,
                    }]
                };
                (node, candidate, *source_record, occurrences)
            },
            PublishedEndpoint::Published { node, sample } => {
                if self.publication_sample_nodes.get(*sample) != Some(node) {
                    return Err(GeometryErrorKind::Invalid(
                        "Native publication sample changed its node authority".into(),
                    )
                    .into());
                }
                let sample = self
                    .emission
                    .publication
                    .samples
                    .get(*sample)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "native endpoint publication sample is unknown".into(),
                        )
                    })?;
                let site = sites
                    .get(node.0 as usize)
                    .and_then(Option::as_ref)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "native publication endpoint has no assigned site".into(),
                        )
                    })?;
                let candidate = candidate_from_emission_site(*site);
                let occurrence = OccurrenceSource {
                    origin: sample.source_trace.as_ref().map_or(
                        super::super::super::emission::VertexOrigin::Presentation,
                        |_| super::super::super::emission::VertexOrigin::SourceSample,
                    ),
                    source_trace: sample
                        .source_trace
                        .as_ref()
                        .map(|trace| trace.try_clone_with_work(budget))
                        .transpose()?,
                };
                (*node, candidate, None, vec![occurrence])
            },
        };
        native_port_data(
            self.topology.sources.frame,
            node,
            candidate,
            assigned_point,
            source_record,
            occurrences,
            bank,
            budget,
        )
    }
    fn native_endpoint_root_requests(
        &self,
        endpoint: &PublishedEndpoint,
        source_trace: &SourceTrace,
        trace_edge: usize,
        edge_parameter: &ExactRational,
        budget: &mut impl Work,
    ) -> Result<Vec<RootRequest>> {
        let PublishedEndpoint::Source { port, .. } = endpoint else {
            return Ok(Vec::new());
        };
        let q_port = self.topology.q.ports.get(port.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("native source endpoint port is unknown".into())
        })?;
        if q_port.cut {
            source_trace.requests_at(trace_edge, edge_parameter, budget)
        } else {
            Ok(Vec::new())
        }
    }
    fn native_cut_port_data(
        &self,
        builder: &mut super::super::super::emission::EmissionBuilder<'topology>,
        feature: &NativeCutFeature,
        side: super::super::super::emission::ChartSide,
        source_requests: &[RootRequest],
        budget: &mut impl Work,
    ) -> Result<NativePortData> {
        let node = builder.intern_point(&feature.point, budget)?;
        let occurrences = native_source_sample_occurrences(
            feature
                .source_trace
                .as_ref()
                .map(|trace| trace.try_clone_with_work(budget))
                .transpose()?,
            source_requests,
            budget,
        )?;
        let longitude = match side {
            super::super::super::emission::ChartSide::East => {
                ExactRational::from_f64(self.topology.sources.frame.longitude().half_period())
            },
            super::super::super::emission::ChartSide::West => ExactRational::zero()
                .try_subtract_with_work(
                    &ExactRational::from_f64(self.topology.sources.frame.longitude().half_period()),
                    budget,
                )?,
        };
        Ok(NativePortData {
            node,
            point: ExactChartPoint::from_rationals(
                longitude,
                feature.point.y.try_clone_with_work(budget)?,
            ),
            spelling: super::super::super::emission::PortSpelling::Seam(side),
            source_record: None,
            occurrences,
        })
    }
    pub(super) fn native_chord_pieces(
        &self,
        builder: &mut super::super::super::emission::EmissionBuilder<'topology>,
        chord: &QPublishedChord,
        assigned_endpoints: &[ExactChartPoint; 2],
        source_trace: &SourceTrace,
        region: Option<usize>,
        feature_indices: &[usize],
        features: &[NativeCutFeature],
        sites: &[Option<AssignedSite>],
        budget: &mut impl Work,
    ) -> Result<NativeChordPieces> {
        budget.charge_into_error(1)?;
        let recipe = chord.recipe;
        let graph_owner = region.map_or(NativeGraphEdgeOwner::Line, |region| {
            NativeGraphEdgeOwner::Area { region }
        });
        let [start_chart, end_chart] = (*assigned_endpoints).clone();
        let displacement = end_chart.x.try_subtract_with_work(&start_chart.x, budget)?;
        let zero = ExactRational::zero();
        let one = ExactRational::one();
        let direction = displacement.try_cmp_with_work(&zero, budget)?;
        let start_is_seam = matches!(
            quotient_key_for_chart_position_with_work(
                self.topology.sources.frame,
                &start_chart,
                budget,
            )?,
            ExactQuotientKey::Seam(_)
        );
        let end_is_seam = matches!(
            quotient_key_for_chart_position_with_work(
                self.topology.sources.frame,
                &end_chart,
                budget,
            )?,
            ExactQuotientKey::Seam(_)
        );
        if direction.is_eq() && !feature_indices.is_empty() {
            return Err(GeometryErrorKind::Invalid(
                "zero-longitude producer chord has a native cut".into(),
            )
            .into());
        }
        let mut seam_wall_source_spans = None;
        let seam_wall_bank = if let Some(region) = region
            && direction.is_eq()
            && start_is_seam
            && end_is_seam
        {
            let vertical = start_chart.y.try_cmp_with_work(&end_chart.y, budget)?;
            if vertical.is_eq() {
                None
            } else {
                let source_spans = native_known_source_spans_for_slice(
                    self.topology,
                    self.emission.selected_face_roots,
                    Some(region),
                    chord,
                    &zero,
                    &one,
                    budget,
                )?;
                if source_spans.selected_left_region != Some(region) {
                    return Err(GeometryErrorKind::Invalid(
                        "native seam wall has no authenticated selected-left source germ".into(),
                    )
                    .into());
                }
                seam_wall_source_spans = Some(source_spans);
                Some(if vertical.is_gt() {
                    super::super::super::emission::ChartSide::West
                } else {
                    super::super::super::emission::ChartSide::East
                })
            }
        } else {
            None
        };
        let start_bank = seam_wall_bank.map_or_else(
            || {
                if !start_is_seam {
                    None
                } else if direction.is_gt() {
                    Some(super::super::super::emission::ChartSide::West)
                } else if direction.is_lt() {
                    Some(super::super::super::emission::ChartSide::East)
                } else {
                    None
                }
            },
            Some,
        );
        let end_bank = seam_wall_bank.map_or_else(
            || {
                if !end_is_seam {
                    None
                } else if direction.is_gt() {
                    Some(super::super::super::emission::ChartSide::East)
                } else if direction.is_lt() {
                    Some(super::super::super::emission::ChartSide::West)
                } else {
                    None
                }
            },
            Some,
        );
        let start_requests = self.native_endpoint_root_requests(
            &chord.endpoints[0],
            source_trace,
            chord.trace_edge,
            &zero,
            budget,
        )?;
        let end_requests = self.native_endpoint_root_requests(
            &chord.endpoints[1],
            source_trace,
            chord.trace_edge,
            &one,
            budget,
        )?;
        let mut current = self.native_endpoint_data(
            builder,
            &chord.endpoints[0],
            &start_chart,
            &start_requests,
            sites,
            start_bank,
            budget,
        )?;
        let mut end = self.native_endpoint_data(
            builder,
            &chord.endpoints[1],
            &end_chart,
            &end_requests,
            sites,
            end_bank,
            budget,
        )?;
        let current_key = quotient_key_for_chart_position_with_work(
            self.topology.sources.frame,
            &current.point,
            budget,
        )?;
        let start_key = quotient_key_for_chart_position_with_work(
            self.topology.sources.frame,
            &start_chart,
            budget,
        )?;
        let end_key = quotient_key_for_chart_position_with_work(
            self.topology.sources.frame,
            &end.point,
            budget,
        )?;
        let assigned_end_key = quotient_key_for_chart_position_with_work(
            self.topology.sources.frame,
            &end_chart,
            budget,
        )?;
        if !compare_quotient_keys_with_work(&current_key, &start_key, budget)?.is_eq()
            || !compare_quotient_keys_with_work(&end_key, &assigned_end_key, budget)?.is_eq()
        {
            return Err(GeometryErrorKind::Invalid(
                "native producer ports differ from their assigned quotient projection".into(),
            )
            .into());
        }
        let mut output = NativeChordPieces {
            edges: Vec::with_capacity(feature_indices.len().saturating_add(1)),
            break_after: Vec::with_capacity(feature_indices.len().saturating_add(1)),
            seam_events: Vec::new(),
        };
        for (is_seam, chart, endpoint, source_requests) in [
            (
                start_is_seam,
                &start_chart,
                &chord.endpoints[0],
                &start_requests,
            ),
            (end_is_seam, &end_chart, &chord.endpoints[1], &end_requests),
        ] {
            budget.charge_into_error(1)?;
            if !is_seam {
                continue;
            }
            let east = self.native_endpoint_data(
                builder,
                endpoint,
                chart,
                source_requests,
                sites,
                Some(super::super::super::emission::ChartSide::East),
                budget,
            )?;
            let west = self.native_endpoint_data(
                builder,
                endpoint,
                chart,
                source_requests,
                sites,
                Some(super::super::super::emission::ChartSide::West),
                budget,
            )?;
            output.seam_events.push(NativeSeamEventData {
                event: NativeSeamEvent {
                    latitude: chart.y.clone(),
                    output_vertex: true,
                },
                vertices: NativeSeamVertices {
                    latitude: chart.y.clone(),
                    east,
                    west,
                },
            });
        }
        let mut previous_parameter = ExactRational::zero();
        for &feature_index in feature_indices {
            budget.charge_into_error(1)?;
            let feature = features.get(feature_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("native chord references an unknown cut feature".into())
            })?;
            if feature.family.fragment_recipe() != Some(chord.recipe)
                || feature.trace_edge != chord.trace_edge
            {
                return Err(GeometryErrorKind::Invalid(
                    "native cut feature is attached to another producer leaf".into(),
                )
                .into());
            }
            let (entry_side, exit_side) = if direction.is_gt() {
                (
                    super::super::super::emission::ChartSide::East,
                    super::super::super::emission::ChartSide::West,
                )
            } else {
                (
                    super::super::super::emission::ChartSide::West,
                    super::super::super::emission::ChartSide::East,
                )
            };
            let source_requests =
                source_trace.requests_at(chord.trace_edge, &feature.edge_parameter, budget)?;
            let entry =
                self.native_cut_port_data(builder, feature, entry_side, &source_requests, budget)?;
            let exit =
                self.native_cut_port_data(builder, feature, exit_side, &source_requests, budget)?;
            let east = self.native_cut_port_data(
                builder,
                feature,
                super::super::super::emission::ChartSide::East,
                &source_requests,
                budget,
            )?;
            let west = self.native_cut_port_data(
                builder,
                feature,
                super::super::super::emission::ChartSide::West,
                &source_requests,
                budget,
            )?;
            output.seam_events.push(NativeSeamEventData {
                event: NativeSeamEvent {
                    latitude: feature.point.y.clone(),
                    output_vertex: true,
                },
                vertices: NativeSeamVertices {
                    latitude: feature.point.y.clone(),
                    east,
                    west,
                },
            });
            align_native_port_to_seam(self.topology.sources.frame, &mut current, &entry, budget)?;
            output.edges.push(NativeAreaEdge {
                vertices: [current, entry],
                owner: graph_owner,
                kind: NativeAreaEdgeKind::Producer {
                    recipe,
                    trace_edge: chord.trace_edge,
                    producer_start: Box::new(previous_parameter.clone()),
                    producer_end: feature.edge_parameter.clone(),
                    trace_ranges: trace_ranges_for_slice(
                        source_trace,
                        chord.trace_edge,
                        &previous_parameter,
                        &feature.edge_parameter,
                        budget,
                    )?,
                    known_spans: native_known_source_spans_for_slice(
                        self.topology,
                        self.emission.selected_face_roots,
                        region,
                        chord,
                        &previous_parameter,
                        &feature.edge_parameter,
                        budget,
                    )?
                    .uses,
                },
            });
            output.break_after.push(true);
            current = exit;
            previous_parameter = feature.edge_parameter.clone();
        }
        if !matches!(
            &current.spelling,
            super::super::super::emission::PortSpelling::Seam(_)
        ) && matches!(
            &end.spelling,
            super::super::super::emission::PortSpelling::Seam(_)
        ) {
            align_native_port_to_seam(self.topology.sources.frame, &mut current, &end, budget)?;
        } else if matches!(
            &current.spelling,
            super::super::super::emission::PortSpelling::Seam(_)
        ) && !matches!(
            &end.spelling,
            super::super::super::emission::PortSpelling::Seam(_)
        ) {
            align_native_port_to_seam(self.topology.sources.frame, &mut end, &current, budget)?;
        }
        output.edges.push(NativeAreaEdge {
            vertices: [current, end],
            owner: graph_owner,
            kind: NativeAreaEdgeKind::Producer {
                recipe,
                trace_edge: chord.trace_edge,
                producer_start: Box::new(previous_parameter.clone()),
                producer_end: one.clone(),
                trace_ranges: trace_ranges_for_slice(
                    source_trace,
                    chord.trace_edge,
                    &previous_parameter,
                    &one,
                    budget,
                )?,
                known_spans: match seam_wall_source_spans.take() {
                    Some(source_spans) => source_spans.uses,
                    None => {
                        native_known_source_spans_for_slice(
                            self.topology,
                            self.emission.selected_face_roots,
                            region,
                            chord,
                            &previous_parameter,
                            &one,
                            budget,
                        )?
                        .uses
                    },
                },
            },
        });
        output.break_after.push(end_is_seam);
        Ok(output)
    }
}
