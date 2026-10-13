use super::super::*;

impl<M: ChartMetric>
    QEmbeddingProblem<'_, '_, '_, '_, M>
{
    fn publication_identity_conflicts(
        &self,
        coordinates: &QCoordinates,
        live_keys: &[(EmittedNodeKey, QNodeId)],
        publication_coordinates: &[Option<QCandidate>],
        budget: &mut impl Work,
    ) -> Result<(Vec<QVariableNode>, bool)> {
        if self.emission.publication.samples.is_empty() {
            return Ok((Vec::new(), false));
        }
        let frame = self.topology.sources.frame;
        let nodes = &self.publication_nodes;
        if self.publication_sample_nodes.len() != self.emission.publication.samples.len() {
            return Err(GeometryErrorKind::Invalid(
                "Q publication sample node table is incomplete".into(),
            )
            .into());
        }
        let live_count = live_keys
            .len()
            .checked_add(self.emission.publication.samples.len())
            .ok_or(ResourceError::SizeOverflow)?;
        budget.charge_into_error(
            u64::try_from(live_count).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut live = Vec::with_capacity(live_count);
        for &(key, node) in live_keys {
            budget.charge_into_error(1)?;
            live.push((key, nodes.source_node(node)?));
        }
        for sample_index in 0..self.emission.publication.samples.len() {
            budget.charge_into_error(1)?;
            let node = *self
                .publication_sample_nodes
                .get(sample_index)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q publication sample node is unknown".into())
                })?;
            let key = match &nodes
                .nodes()
                .get(node.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q publication node identity is unknown".into())
                })?
                .authority
            {
                NodeAuthority::Source(source_node) => {
                    emitted_node_key(frame, coordinates.xy(*source_node)?)?
                },
                NodeAuthority::Computed(_) => {
                    let candidate = publication_coordinates
                        .get(node.0 as usize)
                        .copied()
                        .flatten()
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "Q publication identity has no assigned native site".into(),
                            )
                        })?;
                    emitted_node_key(frame, candidate.native)?
                },
            };
            live.push((key, node));
        }

        let mut conflicting_nodes = Vec::new();
        let mut unrepresentable = false;
        try_sort_by_with_work(&mut live, budget, |left, right, _work| {
            Ok(left.1.cmp(&right.1).then(left.0.cmp(&right.0)))
        })?;
        for pair in live.windows(2) {
            budget.charge_into_error(1)?;
            let [(left_key, left_node), (right_key, right_node)] = pair else {
                unreachable!("windows(2) contains two publication node rows");
            };
            if left_node != right_node || left_key == right_key {
                continue;
            }
            match &nodes
                .nodes()
                .get(left_node.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q publication node identity is unknown".into())
                })?
                .authority
            {
                NodeAuthority::Source(source) => {
                    conflicting_nodes.push(QVariableNode::Source(*source));
                },
                NodeAuthority::Computed(_) => {
                    conflicting_nodes.push(QVariableNode::Published(*left_node));
                },
            }
        }

        try_sort_by_with_work(&mut live, budget, |left, right, _work| Ok(left.cmp(right)))?;
        let mut start = 0_usize;
        while start < live.len() {
            budget.charge_into_error(1)?;
            let key = live
                .get(start)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q publication key is unknown".into()))?
                .0;
            let mut end = start.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
            while let Some((candidate, _)) = live.get(end) {
                budget.charge_into_error(1)?;
                if *candidate != key {
                    break;
                }
                end = end.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
            }
            let group = live.get(start..end).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q publication key group is invalid".into())
            })?;
            budget.charge_into_error(
                u64::try_from(group.len()).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            let mut distinct = group.iter().map(|(_, node)| *node).collect::<Vec<_>>();
            try_sort_by_with_work(&mut distinct, budget, |left, right, _work| {
                Ok(left.cmp(right))
            })?;
            budget.charge_into_error(
                u64::try_from(distinct.len()).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            distinct.dedup();
            if distinct.len() > 1 {
                let mut has_source = false;
                let mut computed_count = 0_usize;
                let mut movable_conflicts = Vec::new();
                for node in distinct {
                    budget.charge_into_error(1)?;
                    let variable = match &nodes
                        .nodes()
                        .get(node.0 as usize)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "Q publication collision identity is unknown".into(),
                            )
                        })?
                        .authority
                    {
                        NodeAuthority::Source(source) => {
                            has_source = true;
                            QVariableNode::Source(*source)
                        },
                        NodeAuthority::Computed(_) => {
                            computed_count = computed_count
                                .checked_add(1)
                                .ok_or(ResourceError::SizeOverflow)?;
                            QVariableNode::Published(node)
                        },
                    };
                    let location = match variable {
                        QVariableNode::Source(node) => {
                            self.node_location.get(node.0 as usize).copied().flatten()
                        },
                        QVariableNode::Published(node) => self
                            .publication_location
                            .get(node.0 as usize)
                            .copied()
                            .flatten(),
                    };
                    if location.is_some() {
                        movable_conflicts.push(variable);
                    }
                }
                if !has_source || computed_count > 1 {
                    unrepresentable = true;
                }
                if movable_conflicts.is_empty() {
                    unrepresentable = true;
                }
                conflicting_nodes.extend(movable_conflicts);
            }
            start = end;
        }
        try_sort_by_with_work(&mut conflicting_nodes, budget, |left, right, _work| {
            Ok(left.cmp(right))
        })?;
        budget.charge_into_error(
            u64::try_from(conflicting_nodes.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        conflicting_nodes.dedup();
        Ok((conflicting_nodes, unrepresentable))
    }
    fn global_q_port_points(
        &self,
        coordinates: &QCoordinates,
        budget: &mut impl Work,
    ) -> Result<Vec<Option<ExactChartPoint>>> {
        let q = &self.topology.q;
        budget.charge_into_error(
            u64::try_from(q.ports.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut port_points = vec![None; q.ports.len()];
        for &(_, port_id) in &self.emission.selected_ports {
            budget.charge_into_error(1)?;
            let port = q
                .ports
                .get(port_id.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q selected port is unknown".into()))?;
            let point = if port.cut {
                port.point.clone()
            } else {
                let assigned = q_port_assigned_point(
                    self.topology,
                    &self.nearest,
                    coordinates,
                    port_id,
                    budget,
                )?;
                align_native_chart_lift(
                    self.topology.sources.frame,
                    &assigned,
                    &port.point,
                    budget,
                )?
            };
            *port_points.get_mut(port_id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q selected port index is invalid".into())
            })? = Some(point);
        }
        reconstruct_cut_ports(
            self.topology.sources.frame,
            q,
            &self.emission.cut_nodes,
            &mut port_points,
            &self.cut_east,
            budget,
        )?;
        Ok(port_points)
    }
    fn publication_candidate_point(
        &self,
        endpoint: &PublishedEndpoint,
        reference: &ExactChartPoint,
        reference_offset: &ExactRational,
        port_points: &[Option<ExactChartPoint>],
        publication_coordinates: &[Option<QCandidate>],
        budget: &mut impl Work,
    ) -> Result<ExactChartPoint> {
        budget.charge_into_error(1)?;
        let assigned = match endpoint {
            PublishedEndpoint::Source { port, sample, .. } => {
                if let Some(sample) = sample
                    && let Some(sample_index) = sample.publication_sample
                    && self
                        .publication_sample_source_ports
                        .get(sample_index)
                        .copied()
                        .flatten()
                        != Some(*port)
                {
                    return Err(GeometryErrorKind::Invalid(
                        "source publication occurrence changed its Q-port authority".into(),
                    )
                    .into());
                }
                port_points
                    .get(port.0 as usize)
                    .and_then(Option::as_ref)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "Q publication source port has no assigned chart point".into(),
                        )
                    })?
                    .try_clone_with_work(budget)?
            },
            PublishedEndpoint::Published { node, sample } => {
                if self.publication_sample_nodes.get(*sample) != Some(node) {
                    return Err(GeometryErrorKind::Invalid(
                        "computed publication occurrence changed its node authority".into(),
                    )
                    .into());
                }
                let candidate = publication_coordinates
                    .get(node.0 as usize)
                    .copied()
                    .flatten()
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "computed publication node has no assigned candidate".into(),
                        )
                    })?;
                candidate_chart_point_with_work(self.topology.sources.frame, candidate, budget)?
            },
        };
        self.assigned_publication_endpoint_point(
            endpoint,
            reference,
            reference_offset,
            &assigned,
            budget,
        )
    }
    fn assigned_publication_endpoint_point(
        &self,
        endpoint: &PublishedEndpoint,
        reference: &ExactChartPoint,
        reference_offset: &ExactRational,
        assigned: &ExactChartPoint,
        budget: &mut impl Work,
    ) -> Result<ExactChartPoint> {
        let sample_index = match endpoint {
            PublishedEndpoint::Source { sample, .. } => {
                sample.as_ref().and_then(|sample| sample.publication_sample)
            },
            PublishedEndpoint::Published { sample, .. } => Some(*sample),
        };
        if let Some(sample_index) = sample_index {
            let sample = self
                .emission
                .publication
                .samples
                .get(sample_index)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("publication endpoint sample is unknown".into())
                })?;
            if !sample.exact_point.try_eq_with_work(reference, budget)? {
                return Err(GeometryErrorKind::Invalid(
                    "publication endpoint changed its frozen sample reference".into(),
                )
                .into());
            }
        }
        lift_assigned_publication_point(assigned, reference_offset, budget)
    }
    fn publication_graph_rejections(
        &self,
        producer: &AssignedProducerProjection,
        budget: &mut impl Work,
    ) -> Result<Vec<QVariableNode>> {
        if self.publication_pairs.is_empty() {
            return Ok(Vec::new());
        }
        let q = &self.topology.q;
        let port_points = &producer.source_ports;
        let period = ExactRational::from_f64(self.topology.sources.frame.longitude().period());
        let mut rejected = Vec::<QVariableNode>::new();
        for pair in &self.publication_pairs {
            budget.charge_into_error(1)?;
            let (left_nodes, left_exact, left_candidate, left_dependencies) = match pair.left {
                QPublicationChordId::Source(index) => {
                    let chord = self.chords.get(index).ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q publication source chord is unknown".into())
                    })?;
                    budget.charge_into_error(
                        u64::try_from(chord.dependencies.len())
                            .map_err(|_| ResourceError::SizeOverflow)?,
                    )?;
                    let [start_port, end_port] = chord.ports;
                    let exact = [
                        q.ports
                            .get(start_port.0 as usize)
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid("Q source chord port is unknown".into())
                            })?
                            .point
                            .clone(),
                        q.ports
                            .get(end_port.0 as usize)
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid("Q source chord port is unknown".into())
                            })?
                            .point
                            .clone(),
                    ];
                    let candidate = [
                        port_points
                            .get(start_port.0 as usize)
                            .and_then(Option::as_ref)
                            .cloned()
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "Q source chord has no assigned start".into(),
                                )
                            })?,
                        port_points
                            .get(end_port.0 as usize)
                            .and_then(Option::as_ref)
                            .cloned()
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "Q source chord has no assigned end".into(),
                                )
                            })?,
                    ];
                    (
                        chord.nodes.map(QGraphNode::Source),
                        exact,
                        candidate,
                        chord.dependencies.clone(),
                    )
                },
                QPublicationChordId::Published(index) => {
                    let chord = self.publication_chords.get(index).ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q publication chord is unknown".into())
                    })?;
                    let leaf = producer.leaves.get(index).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "Q publication pair has no assigned producer leaf".into(),
                        )
                    })?;
                    if leaf.recipe != chord.recipe || leaf.trace_edge != chord.trace_edge {
                        return Err(GeometryErrorKind::Invalid(
                            "Q publication pair changed its assigned leaf identity".into(),
                        )
                        .into());
                    }
                    budget.charge_into_error(
                        u64::try_from(chord.dependencies.len())
                            .map_err(|_| ResourceError::SizeOverflow)?,
                    )?;
                    (
                        chord.nodes,
                        chord.exact.clone(),
                        leaf.points.clone(),
                        chord.dependencies.clone(),
                    )
                },
            };
            let (right_nodes, right_exact, right_candidate, right_dependencies) = match pair.right {
                QPublicationChordId::Source(index) => {
                    let chord = self.chords.get(index).ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q publication source chord is unknown".into())
                    })?;
                    budget.charge_into_error(
                        u64::try_from(chord.dependencies.len())
                            .map_err(|_| ResourceError::SizeOverflow)?,
                    )?;
                    let [start_port, end_port] = chord.ports;
                    let exact = [
                        q.ports
                            .get(start_port.0 as usize)
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid("Q source chord port is unknown".into())
                            })?
                            .point
                            .clone(),
                        q.ports
                            .get(end_port.0 as usize)
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid("Q source chord port is unknown".into())
                            })?
                            .point
                            .clone(),
                    ];
                    let candidate = [
                        port_points
                            .get(start_port.0 as usize)
                            .and_then(Option::as_ref)
                            .cloned()
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "Q source chord has no assigned start".into(),
                                )
                            })?,
                        port_points
                            .get(end_port.0 as usize)
                            .and_then(Option::as_ref)
                            .cloned()
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "Q source chord has no assigned end".into(),
                                )
                            })?,
                    ];
                    (
                        chord.nodes.map(QGraphNode::Source),
                        exact,
                        candidate,
                        chord.dependencies.clone(),
                    )
                },
                QPublicationChordId::Published(index) => {
                    let chord = self.publication_chords.get(index).ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q publication chord is unknown".into())
                    })?;
                    let leaf = producer.leaves.get(index).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "Q publication pair has no assigned producer leaf".into(),
                        )
                    })?;
                    if leaf.recipe != chord.recipe || leaf.trace_edge != chord.trace_edge {
                        return Err(GeometryErrorKind::Invalid(
                            "Q publication pair changed its assigned leaf identity".into(),
                        )
                        .into());
                    }
                    budget.charge_into_error(
                        u64::try_from(chord.dependencies.len())
                            .map_err(|_| ResourceError::SizeOverflow)?,
                    )?;
                    (
                        chord.nodes,
                        chord.exact.clone(),
                        leaf.points.clone(),
                        chord.dependencies.clone(),
                    )
                },
            };
            let right_exact = [
                translated_exact_point_with_work(
                    &right_exact[0],
                    pair.right_shift,
                    &period,
                    budget,
                )?,
                translated_exact_point_with_work(
                    &right_exact[1],
                    pair.right_shift,
                    &period,
                    budget,
                )?,
            ];
            let right_candidate = [
                translated_exact_point_with_work(
                    &right_candidate[0],
                    pair.right_shift,
                    &period,
                    budget,
                )?,
                translated_exact_point_with_work(
                    &right_candidate[1],
                    pair.right_shift,
                    &period,
                    budget,
                )?,
            ];
            if !pair_preserves_incidence_with_work(
                left_nodes,
                &left_exact,
                &left_candidate,
                right_nodes,
                &right_exact,
                &right_candidate,
                budget,
            )? {
                budget.charge_into_error(
                    u64::try_from(
                        left_dependencies
                            .len()
                            .checked_add(right_dependencies.len())
                            .ok_or(ResourceError::SizeOverflow)?
                            .checked_add(4)
                            .ok_or(ResourceError::SizeOverflow)?,
                    )
                    .map_err(|_| ResourceError::SizeOverflow)?,
                )?;
                rejected.extend(left_dependencies.into_iter().map(QVariableNode::Source));
                rejected.extend(right_dependencies.into_iter().map(QVariableNode::Source));
                rejected.extend(left_nodes.map(|node| match node {
                    QGraphNode::Source(node) => QVariableNode::Source(node),
                    QGraphNode::Published(node) => QVariableNode::Published(node),
                }));
                rejected.extend(right_nodes.map(|node| match node {
                    QGraphNode::Source(node) => QVariableNode::Source(node),
                    QGraphNode::Published(node) => QVariableNode::Published(node),
                }));
            }
        }
        try_sort_by_with_work(&mut rejected, budget, |left, right, _work| {
            Ok(left.cmp(right))
        })?;
        budget.charge_into_error(
            u64::try_from(rejected.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        rejected.dedup();
        Ok(rejected)
    }
    fn certify_assigned_producer(
        &self,
        coordinates: &QCoordinates,
        publication_coordinates: &[Option<QCandidate>],
        source_faces: Option<&QFaceStructure>,
        budget: &mut impl Work,
    ) -> Result<AssignedProducerProjection> {
        let frame = self.topology.sources.frame;
        let q = &self.topology.q;
        budget.charge_into_error(
            u64::try_from(self.publication_chords.len())
                .map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let port_points = self.global_q_port_points(coordinates, budget)?;
        let mut leaves = Vec::with_capacity(self.publication_chords.len());
        let mut rejected = Vec::new();
        for chord in &self.publication_chords {
            budget.charge_into_error(1)?;
            let points = [
                self.publication_candidate_point(
                    &chord.endpoints[0],
                    &chord.exact[0],
                    &chord.reference_offsets[0],
                    &port_points,
                    publication_coordinates,
                    budget,
                )?,
                self.publication_candidate_point(
                    &chord.endpoints[1],
                    &chord.exact[1],
                    &chord.reference_offsets[1],
                    &port_points,
                    publication_coordinates,
                    budget,
                )?,
            ];
            if (points[0] == points[1]) != (chord.exact[0] == chord.exact[1]) {
                budget.charge_into_error(
                    u64::try_from(
                        chord
                            .dependencies
                            .len()
                            .checked_add(2)
                            .ok_or(ResourceError::SizeOverflow)?,
                    )
                    .map_err(|_| ResourceError::SizeOverflow)?,
                )?;
                rejected.extend(
                    chord
                        .dependencies
                        .iter()
                        .copied()
                        .map(QVariableNode::Source),
                );
                for node in chord.nodes {
                    rejected.push(match node {
                        QGraphNode::Source(node) => QVariableNode::Source(node),
                        QGraphNode::Published(node) => QVariableNode::Published(node),
                    });
                }
            }
            leaves.push(AssignedProducerLeaf {
                recipe: chord.recipe,
                trace_edge: chord.trace_edge,
                points,
            });
        }
        if let Some(source_faces) = source_faces {
            budget.charge_into_error(
                u64::try_from(source_faces.outgoing_nodes.len())
                    .map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            let mut local_by_node = BTreeMap::new();
            for (local, &node) in source_faces.outgoing_nodes.iter().enumerate() {
                budget.charge_into_error(1)?;
                if local_by_node.insert(node, local).is_some() {
                    return Err(GeometryErrorKind::Invalid(
                        "assigned producer face projection repeats a source node".into(),
                    )
                    .into());
                }
            }
            for point in &self.prepared_producer.points {
                budget.charge_into_error(1)?;
                let Some(&local) = local_by_node.get(&point.node) else {
                    if point.host.is_some() {
                        budget.charge_into_error(
                            u64::try_from(point.dependencies.len())
                                .map_err(|_| ResourceError::SizeOverflow)?,
                        )?;
                        rejected.extend_from_slice(&point.dependencies);
                    }
                    continue;
                };
                let assigned_host = source_faces
                    .global
                    .source_node_faces
                    .get(local)
                    .copied()
                    .flatten();
                if point.host.is_some() && assigned_host.is_none() {
                    budget.charge_into_error(
                        u64::try_from(point.dependencies.len())
                            .map_err(|_| ResourceError::SizeOverflow)?,
                    )?;
                    rejected.extend_from_slice(&point.dependencies);
                }
            }
            for junction in &self.prepared_producer.junctions {
                let Some(_host) = junction.isolated_host else {
                    continue;
                };
                budget.charge_into_error(1)?;
                let assigned_host = local_by_node
                    .get(&junction.node)
                    .and_then(|local| source_faces.global.source_node_faces.get(*local))
                    .copied()
                    .flatten();
                if assigned_host.is_none() {
                    budget.charge_into_error(
                        u64::try_from(junction.dependencies.len())
                            .map_err(|_| ResourceError::SizeOverflow)?,
                    )?;
                    rejected.extend_from_slice(&junction.dependencies);
                }
            }
        } else {
            for linework in &self.prepared_producer.linework {
                budget.charge_into_error(1)?;
                if linework.use_kind == SelectedUse::ArealBoundaryLine {
                    return Err(GeometryErrorKind::Invalid(
                        "assigned areal-line producer has no source-face projection".into(),
                    )
                    .into());
                }
            }
        }
        for junction in &self.prepared_producer.junctions {
            budget.charge_into_error(1)?;
            if junction.producer_germs.is_empty() {
                continue;
            }
            let mut rays_by_port =
                BTreeMap::<QPortId, BTreeMap<QDartId, AssignedProducerFanRayBuilder>>::new();
            let mut candidate_mismatch = false;
            for germ in &junction.producer_germs {
                budget.charge_into_error(1)?;
                let chord = self.publication_chords.get(germ.chord).ok_or_else(|| {
                    GeometryErrorKind::Invalid("assigned producer fan chord is unknown".into())
                })?;
                let leaf = leaves.get(germ.chord).ok_or_else(|| {
                    GeometryErrorKind::Invalid("assigned producer fan leaf is unknown".into())
                })?;
                if leaf.recipe != chord.recipe || leaf.trace_edge != chord.trace_edge {
                    return Err(GeometryErrorKind::Invalid(
                        "assigned producer leaf projection lost its trace identity".into(),
                    )
                    .into());
                }
                let other_endpoint = match germ.endpoint {
                    0 => 1,
                    1 => 0,
                    _ => {
                        return Err(GeometryErrorKind::Invalid(
                            "assigned producer fan endpoint is outside its chord".into(),
                        )
                        .into());
                    },
                };
                let origin = leaf.points.get(germ.endpoint).cloned().ok_or_else(|| {
                    GeometryErrorKind::Invalid("assigned producer fan endpoint is unknown".into())
                })?;
                let target = leaf.points.get(other_endpoint).cloned().ok_or_else(|| {
                    GeometryErrorKind::Invalid("assigned producer fan target is unknown".into())
                })?;
                if origin == target {
                    let chord_exact = chord.exact.get(germ.endpoint).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "assigned producer reference endpoint is unknown".into(),
                        )
                    })?;
                    let target_exact = chord.exact.get(other_endpoint).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "assigned producer reference target is unknown".into(),
                        )
                    })?;
                    if chord_exact != target_exact {
                        candidate_mismatch = true;
                        break;
                    }
                    continue;
                }
                let producer_parameter = if germ.endpoint == 0 {
                    ExactRational::zero()
                } else {
                    ExactRational::one()
                };
                for &range_index in &germ.source_ranges {
                    budget.charge_into_error(1)?;
                    let range = chord.source_ranges.get(range_index).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "assigned producer fan source range is unknown".into(),
                        )
                    })?;
                    if !producer_range_touches_node(
                        range,
                        &producer_parameter,
                        junction.node,
                        budget,
                    )? {
                        return Err(GeometryErrorKind::Invalid(
                            "assigned producer fan lost its original junction range".into(),
                        )
                        .into());
                    }
                    let Some((port, dart)) = producer_range_fan_dart(
                        q,
                        range,
                        &producer_parameter,
                        junction.node,
                        budget,
                    )?
                    else {
                        return Err(GeometryErrorKind::Invalid(
                            "assigned producer fan range has no directed source germ".into(),
                        )
                        .into());
                    };
                    let selected_root = junction
                        .face_germs
                        .iter()
                        .find(|face_germ| face_germ.dart == dart)
                        .map(|face_germ| face_germ.selected_root)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "assigned producer fan source dart is absent from its node fan"
                                    .into(),
                            )
                        })?;
                    let source_point = port_points
                        .get(port.0 as usize)
                        .and_then(Option::as_ref)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "assigned producer fan source port has no candidate point".into(),
                            )
                        })?;
                    let source_key = quotient_key_for_chart_position_with_work(
                        self.topology.sources.frame,
                        source_point,
                        budget,
                    )?;
                    let origin_key = quotient_key_for_chart_position_with_work(
                        self.topology.sources.frame,
                        &origin,
                        budget,
                    )?;
                    if !compare_quotient_keys_with_work(&source_key, &origin_key, budget)?.is_eq() {
                        candidate_mismatch = true;
                        break;
                    }
                    let target_dart = q.darts.get(dart.0 as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "assigned producer fan has an unknown target dart".into(),
                        )
                    })?;
                    let target_source_point = port_points
                        .get(target_dart.to_port.0 as usize)
                        .and_then(Option::as_ref)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "assigned producer fan target port has no candidate point".into(),
                            )
                        })?;
                    let longitude = self.topology.sources.frame.longitude();
                    let (_, source_lift) = split_rational_native_site_with_work(
                        &target_source_point.x,
                        longitude.period(),
                        longitude.half_period(),
                        budget,
                    )?;
                    let (output_native, output_lift) = split_rational_native_site_with_work(
                        &target.x,
                        longitude.period(),
                        longitude.half_period(),
                        budget,
                    )?;
                    let lift_delta = source_lift.try_subtract_with_work(output_lift, budget)?;
                    let negative_half = ExactRational::from_f64(-longitude.half_period());
                    let aligned_target = if output_native
                        .as_ref()
                        .try_cmp_with_work(&negative_half, budget)?
                        .is_eq()
                    {
                        align_producer_fan_target(
                            &target,
                            target_source_point,
                            lift_delta,
                            frame,
                            budget,
                        )?
                    } else {
                        let longitude_shift = ExactRational::from_f64(longitude.period())
                            .try_product_with_work(
                                &ExactRational::from_dyadic(lift_delta),
                                budget,
                            )?;
                        ExactChartPoint::from_rationals(
                            target.x.try_add_with_work(&longitude_shift, budget)?,
                            target.y.try_clone_with_work(budget)?,
                        )
                    };
                    budget.charge_into_error(1)?;
                    let ray = rays_by_port
                        .entry(port)
                        .or_default()
                        .entry(dart)
                        .or_insert_with(|| AssignedProducerFanRayBuilder {
                            selected_root,
                            targets: Vec::new(),
                        });
                    if ray.selected_root != selected_root {
                        return Err(GeometryErrorKind::Invalid(
                            "assigned producer fan changed a selected source root".into(),
                        )
                        .into());
                    }
                    ray.targets.push(aligned_target);
                }
                if candidate_mismatch {
                    break;
                }
            }
            if candidate_mismatch {
                budget.charge_into_error(
                    u64::try_from(junction.dependencies.len())
                        .map_err(|_| ResourceError::SizeOverflow)?,
                )?;
                rejected.extend_from_slice(&junction.dependencies);
                continue;
            }

            for (port, darts) in rays_by_port {
                budget.charge_into_error(1)?;
                let origin = port_points
                    .get(port.0 as usize)
                    .and_then(Option::as_ref)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "assigned producer fan port has no candidate point".into(),
                        )
                    })?;
                let represented_darts = darts.keys().copied().collect::<BTreeSet<_>>();
                budget.charge_into_error(
                    u64::try_from(
                        represented_darts
                            .len()
                            .checked_add(1)
                            .ok_or(ResourceError::SizeOverflow)?,
                    )
                    .map_err(|_| ResourceError::SizeOverflow)?,
                )?;
                let mut points = NativeReferencePoints {
                    exact: Vec::with_capacity(
                        represented_darts
                            .len()
                            .checked_add(1)
                            .ok_or(ResourceError::SizeOverflow)?,
                    ),
                    rounded: Vec::with_capacity(
                        represented_darts
                            .len()
                            .checked_add(1)
                            .ok_or(ResourceError::SizeOverflow)?,
                    ),
                };
                points.exact.push(origin.clone());
                points.rounded.push(XY::new(
                    origin.x.round_with_work(budget)?,
                    origin.y.round_with_work(budget)?,
                ));
                let mut candidate_rays = Vec::with_capacity(represented_darts.len());
                for (dart, ray) in darts {
                    budget.charge_into_error(1)?;
                    let target = ray.targets.first().ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "assigned producer fan dart has no written leaf".into(),
                        )
                    })?;
                    if target == origin {
                        candidate_mismatch = true;
                        break;
                    }
                    for other in ray.targets.iter().skip(1) {
                        budget.charge_into_error(1)?;
                        if !exact_departures_share_forward_ray(origin, target, other, budget)? {
                            candidate_mismatch = true;
                            break;
                        }
                    }
                    if candidate_mismatch {
                        break;
                    }
                    let point_index = u32::try_from(points.exact.len())
                        .map_err(|_| ResourceError::SizeOverflow)?;
                    points.exact.push(target.clone());
                    points.rounded.push(XY::new(
                        target.x.round_with_work(budget)?,
                        target.y.round_with_work(budget)?,
                    ));
                    candidate_rays.push((dart, point_index, target.clone()));
                }
                if candidate_mismatch {
                    break;
                }
                let mut expected_darts = Vec::new();
                for germ in &junction.face_germs {
                    budget.charge_into_error(1)?;
                    let dart = q.darts.get(germ.dart.0 as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "assigned producer fan has an unknown source dart".into(),
                        )
                    })?;
                    if dart.from_port == port && represented_darts.contains(&germ.dart) {
                        expected_darts.push(germ.dart);
                    }
                }
                if expected_darts.len() != candidate_rays.len() {
                    return Err(GeometryErrorKind::Invalid(
                        "assigned producer fan projection is not reciprocal with the source fan"
                            .into(),
                    )
                    .into());
                }
                if !assigned_producer_fan_order_matches(
                    &points,
                    origin,
                    &mut candidate_rays,
                    &expected_darts,
                    budget,
                )? {
                    candidate_mismatch = true;
                    break;
                }
            }
            if candidate_mismatch {
                budget.charge_into_error(
                    u64::try_from(junction.dependencies.len())
                        .map_err(|_| ResourceError::SizeOverflow)?,
                )?;
                rejected.extend_from_slice(&junction.dependencies);
            }
        }
        try_sort_by_with_work(&mut rejected, budget, |left, right, work| {
            work.charge_into_error(1)?;
            Ok(left.cmp(right))
        })?;
        budget.charge_into_error(
            u64::try_from(rejected.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        rejected.dedup();
        Ok(AssignedProducerProjection {
            source_ports: port_points,
            leaves,
            rejections: rejected,
        })
    }
}

pub(in crate::geometry::topology::q_embedding) fn publication_leaf_replaces_source_span(
    source: &QChord,
    published: &QPublishedChord,
    work: &mut impl Work,
) -> Result<bool> {
    for range in &published.source_ranges {
        work.charge_into_error(1)?;
        if range.ports == source.ports || range.ports == [source.ports[1], source.ports[0]] {
            return Ok(true);
        }
    }
    Ok(false)
}
