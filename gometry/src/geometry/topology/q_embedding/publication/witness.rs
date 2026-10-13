use super::super::{
    ExactGeographicTopology, ExactRational, ExactSourceRange, GeometryErrorKind,
    PreparedProducerWitness, QEmbeddingEmission, QGraphNode, QNodeId, QProducerFaceGerm,
    QProducerGerm, QProducerJunction, QProducerLinework, QProducerPoint, QPublishedChord,
    QSourceSpanRange, QVariableNode, ResourceError, Result, SelectedUse, SourceEdgeKind,
    producer_range_touches_node, span_parameter_at_source, try_sort_by_with_work,
};
use crate::work::Work;

impl PreparedProducerWitness {
    pub(in crate::geometry::topology::q_embedding) fn validate_live_nodes(
        &self,
        live_nodes: &[QNodeId],
        budget: &mut impl Work,
    ) -> Result<()> {
        budget.charge_into_error(
            u64::try_from(live_nodes.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut sorted_live_nodes = Vec::with_capacity(live_nodes.len());
        for &node in live_nodes {
            budget.charge_into_error(1)?;
            sorted_live_nodes.push(node);
        }
        try_sort_by_with_work(&mut sorted_live_nodes, budget, |left, right, _work| {
            Ok(left.cmp(right))
        })?;
        budget.charge_into_error(
            u64::try_from(sorted_live_nodes.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        sorted_live_nodes.dedup();
        for junction in &self.junctions {
            budget.charge_into_error(1)?;
            if junction.producer_germs.is_empty() {
                continue;
            }
            if sorted_live_nodes.binary_search(&junction.node).is_err() {
                return Err(GeometryErrorKind::Invalid(
                    "producer junction is absent from its retained source drawing".into(),
                )
                .into());
            }
        }
        for point in &self.points {
            budget.charge_into_error(1)?;
            if sorted_live_nodes.binary_search(&point.node).is_err() {
                return Err(GeometryErrorKind::Invalid(
                    "producer point is absent from its retained source drawing".into(),
                )
                .into());
            }
        }
        Ok(())
    }

    #[expect(
        clippy::suspicious_operation_groupings,
        reason = "QSourceSpanRange.source_lift is copied from the referenced QSpanOwner.lift"
    )]
    pub(in crate::geometry::topology::q_embedding) fn validate(
        &self,
        topology: &ExactGeographicTopology,
        emission: &QEmbeddingEmission<'_, '_>,
        chords: &[QPublishedChord],
        budget: &mut impl Work,
    ) -> Result<()> {
        let q = &topology.q;
        for chord in chords {
            budget.charge_into_error(1)?;
            for range in &chord.source_ranges {
                budget.charge_into_error(1)?;
                if range.root.index() != range.source_edge.0 as usize
                    || topology
                        .sources
                        .edges
                        .get(range.source_edge.0 as usize)
                        .is_none()
                    || !super::super::super::exact::in_unit_interval_with_work(
                        &range.source_range.start,
                        budget,
                    )?
                    || !super::super::super::exact::in_unit_interval_with_work(
                        &range.source_range.end,
                        budget,
                    )?
                    || !super::super::super::exact::in_unit_interval_with_work(
                        &range.producer_range.start,
                        budget,
                    )?
                    || !super::super::super::exact::in_unit_interval_with_work(
                        &range.producer_range.end,
                        budget,
                    )?
                    || !super::super::super::exact::in_unit_interval_with_work(
                        &range.span_range.start,
                        budget,
                    )?
                    || !super::super::super::exact::in_unit_interval_with_work(
                        &range.span_range.end,
                        budget,
                    )?
                {
                    return Err(GeometryErrorKind::Invalid(
                        "producer witness has an invalid original-root interval".into(),
                    )
                    .into());
                }
                let span = q.spans.get(range.span).ok_or_else(|| {
                    GeometryErrorKind::Invalid("producer witness span is unknown".into())
                })?;
                if span.nodes != range.nodes || span.ports != range.ports {
                    return Err(GeometryErrorKind::Invalid(
                        "producer witness span back-map changed its source endpoints".into(),
                    )
                    .into());
                }
                let owner = span.owners.get(range.owner).ok_or_else(|| {
                    GeometryErrorKind::Invalid("producer witness owner is unknown".into())
                })?;
                if owner.source_edge != range.source_edge || owner.lift != range.source_lift {
                    return Err(GeometryErrorKind::Invalid(
                        "producer witness owner changed its source root or chart lift".into(),
                    )
                    .into());
                }
                let (low, high) = if owner
                    .source_range
                    .start
                    .try_cmp_with_work(&owner.source_range.end, budget)?
                    .is_le()
                {
                    (&owner.source_range.start, &owner.source_range.end)
                } else {
                    (&owner.source_range.end, &owner.source_range.start)
                };
                let source_start_within = range
                    .source_range
                    .start
                    .try_cmp_with_work(low, budget)?
                    .is_ge()
                    && range
                        .source_range
                        .start
                        .try_cmp_with_work(high, budget)?
                        .is_le();
                let source_end_within = range
                    .source_range
                    .end
                    .try_cmp_with_work(low, budget)?
                    .is_ge()
                    && range
                        .source_range
                        .end
                        .try_cmp_with_work(high, budget)?
                        .is_le();
                let indexed = QSourceSpanRange {
                    start: low.try_clone_with_work(budget)?,
                    end: high.try_clone_with_work(budget)?,
                    span: range.span,
                    owner: range.owner,
                    source_lift: owner.lift,
                    source_range: ExactSourceRange {
                        start: owner.source_range.start.try_clone_with_work(budget)?,
                        end: owner.source_range.end.try_clone_with_work(budget)?,
                    },
                    reversed: owner.reversed,
                    nodes: span.nodes,
                    ports: span.ports,
                };
                let expected_start =
                    span_parameter_at_source(&indexed, &range.source_range.start, budget)?;
                let expected_end =
                    span_parameter_at_source(&indexed, &range.source_range.end, budget)?;
                if !source_start_within
                    || !source_end_within
                    || !expected_start
                        .try_cmp_with_work(&range.span_range.start, budget)?
                        .is_eq()
                    || !expected_end
                        .try_cmp_with_work(&range.span_range.end, budget)?
                        .is_eq()
                {
                    return Err(GeometryErrorKind::Invalid(
                        "producer witness root interval has no exact Q-span owner".into(),
                    )
                    .into());
                }
            }
        }
        for junction in &self.junctions {
            budget.charge_into_error(1)?;
            let node = q.nodes.get(junction.node.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("producer witness node is unknown".into())
            })?;
            let ports = q.node_ports.get(node.ports.clone()).ok_or_else(|| {
                GeometryErrorKind::Invalid("producer witness port range is invalid".into())
            })?;
            budget.charge_into_error(
                u64::try_from(ports.len()).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            if ports != junction.ports {
                return Err(GeometryErrorKind::Invalid(
                    "producer witness changed its source port row".into(),
                )
                .into());
            }
            let fan_start = *q
                .outgoing_offsets
                .get(junction.node.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("producer witness node has no fan start".into())
                })?;
            let fan_end = *q
                .outgoing_offsets
                .get(junction.node.0 as usize + 1)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("producer witness node has no fan end".into())
                })?;
            let darts = q.outgoing.get(fan_start..fan_end).ok_or_else(|| {
                GeometryErrorKind::Invalid("producer witness fan range is invalid".into())
            })?;
            budget.charge_into_error(
                u64::try_from(darts.len()).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            if darts.len() != junction.face_germs.len() {
                return Err(GeometryErrorKind::Invalid(
                    "producer witness omitted a source fan sector".into(),
                )
                .into());
            }
            for (&dart, germ) in darts.iter().zip(&junction.face_germs) {
                budget.charge_into_error(1)?;
                if dart != germ.dart {
                    return Err(GeometryErrorKind::Invalid(
                        "producer witness reordered a source fan sector".into(),
                    )
                    .into());
                }
                let local = *q.dart_faces.get(dart.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("producer witness dart has no face".into())
                })?;
                let face = *q
                    .global_face_of_local
                    .get(local.0 as usize)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid("producer witness face has no global ID".into())
                    })?;
                let selected_root = emission
                    .selected_face_roots
                    .get(face.0 as usize)
                    .copied()
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "producer witness face has no selection record".into(),
                        )
                    })?;
                if germ.face != face || germ.selected_root != selected_root {
                    return Err(GeometryErrorKind::Invalid(
                        "producer witness changed a source face germ or selected root".into(),
                    )
                    .into());
                }
            }
            let isolated_host = q
                .source_node_faces
                .get(junction.node.0 as usize)
                .copied()
                .flatten();
            if darts.is_empty() != isolated_host.is_some()
                || isolated_host != junction.isolated_host
            {
                return Err(GeometryErrorKind::Invalid(
                    "producer witness isolated-host fact is incomplete".into(),
                )
                .into());
            }
            for germ in &junction.producer_germs {
                budget.charge_into_error(1)?;
                let chord = chords.get(germ.chord).ok_or_else(|| {
                    GeometryErrorKind::Invalid("producer witness chord is unknown".into())
                })?;
                let endpoint_node = chord.nodes.get(germ.endpoint).copied().ok_or_else(|| {
                    GeometryErrorKind::Invalid("producer witness endpoint is unknown".into())
                })?;
                if endpoint_node != QGraphNode::Source(junction.node) {
                    return Err(GeometryErrorKind::Invalid(
                        "producer witness junction moved from its source node".into(),
                    )
                    .into());
                }
                let endpoint_parameter = if germ.endpoint == 0 {
                    ExactRational::zero()
                } else {
                    ExactRational::one()
                };
                if germ.source_ranges.is_empty() {
                    return Err(GeometryErrorKind::Invalid(
                        "producer witness junction has no source-range back-map".into(),
                    )
                    .into());
                }
                for &range_index in &germ.source_ranges {
                    budget.charge_into_error(1)?;
                    let range = chord.source_ranges.get(range_index).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "producer witness source-range back-map is unknown".into(),
                        )
                    })?;
                    if !producer_range_touches_node(
                        range,
                        &endpoint_parameter,
                        junction.node,
                        budget,
                    )? {
                        return Err(GeometryErrorKind::Invalid(
                            "producer witness source-range back-map lost its fan germ".into(),
                        )
                        .into());
                    }
                }
            }
        }

        for linework in &self.linework {
            budget.charge_into_error(1)?;
            let chord = chords.get(linework.chord).ok_or_else(|| {
                GeometryErrorKind::Invalid("producer witness line chord is unknown".into())
            })?;
            let recipe = emission
                .fragment_recipes
                .get(linework.recipe.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("producer witness line recipe is unknown".into())
                })?;
            if chord.recipe != linework.recipe || recipe.use_kind != linework.use_kind {
                return Err(GeometryErrorKind::Invalid(
                    "producer witness linework changed its selected use".into(),
                )
                .into());
            }
            budget.charge_into_error(
                u64::try_from(linework.source_ranges.len())
                    .map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            if linework.source_ranges.len() != chord.source_ranges.len()
                || linework
                    .source_ranges
                    .iter()
                    .enumerate()
                    .any(|(index, &range)| index != range)
            {
                return Err(GeometryErrorKind::Invalid(
                    "producer witness linework lost its source ranges".into(),
                )
                .into());
            }
        }

        for point in &self.points {
            budget.charge_into_error(1)?;
            let node = q.nodes.get(point.node.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("producer witness point node is unknown".into())
            })?;
            let fan = q
                .outgoing_offsets
                .get(point.node.0 as usize..point.node.0 as usize + 2)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("producer witness point fan is unknown".into())
                })?;
            let [start, end] = fan else {
                return Err(GeometryErrorKind::Invalid(
                    "producer witness point fan offsets are invalid".into(),
                )
                .into());
            };
            let host = q
                .source_node_faces
                .get(point.node.0 as usize)
                .copied()
                .flatten();
            let rows = q.point_rows_for_node(point.node.0 as usize)?;
            budget.charge_into_error(
                u64::try_from(rows.len()).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            if (start == end) != point.host.is_some()
                || host != point.host
                || rows != point.point_rows.as_slice()
                || node.id != point.node
            {
                return Err(GeometryErrorKind::Invalid(
                    "producer witness changed an isolated-point host or source row".into(),
                )
                .into());
            }
        }
        Ok(())
    }

    pub(in crate::geometry::topology::q_embedding) fn source_dependencies(
        &self,
        budget: &mut impl Work,
    ) -> Result<Vec<QNodeId>> {
        let mut dependencies = Vec::new();
        for row in self
            .junctions
            .iter()
            .map(|junction| junction.dependencies.as_slice())
            .chain(
                self.linework
                    .iter()
                    .map(|linework| linework.dependencies.as_slice()),
            )
            .chain(
                self.points
                    .iter()
                    .map(|point| point.dependencies.as_slice()),
            )
        {
            for &dependency in row {
                budget.charge_into_error(1)?;
                if let QVariableNode::Source(node) = dependency {
                    dependencies.push(node);
                }
            }
        }
        try_sort_by_with_work(&mut dependencies, budget, |left, right, _work| {
            Ok(left.cmp(right))
        })?;
        budget.charge_into_error(
            u64::try_from(dependencies.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        dependencies.dedup();
        Ok(dependencies)
    }

    pub(in crate::geometry::topology::q_embedding) fn build(
        topology: &ExactGeographicTopology,
        emission: &QEmbeddingEmission<'_, '_>,
        chords: &[QPublishedChord],
        budget: &mut impl Work,
    ) -> Result<Self> {
        let q = &topology.q;
        budget.charge_into_error(
            u64::try_from(q.spans.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut selected_spans = vec![false; q.spans.len()];
        for &span_index in &emission.spans {
            budget.charge_into_error(1)?;
            *selected_spans.get_mut(span_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("producer selection references an unknown span".into())
            })? = true;
        }
        budget.charge_into_error(
            u64::try_from(
                q.global_face_count
                    .checked_add(q.span_global_faces.len())
                    .ok_or(ResourceError::SizeOverflow)?,
            )
            .map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut host_boundary_nodes = vec![Vec::<QNodeId>::new(); q.global_face_count];
        for (span_index, faces) in q.span_global_faces.iter().enumerate() {
            budget.charge_into_error(1)?;
            let Some([left, right]) = faces else {
                continue;
            };
            let span = q.spans.get(span_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("producer host span is unknown".into())
            })?;
            for face in [*left, *right] {
                budget.charge_into_error(2)?;
                let nodes = host_boundary_nodes
                    .get_mut(face.0 as usize)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid("producer host face is unknown".into())
                    })?;
                nodes.extend(span.nodes);
            }
        }
        for nodes in &mut host_boundary_nodes {
            try_sort_by_with_work(nodes, budget, |left, right, _work| Ok(left.cmp(right)))?;
            budget.charge_into_error(
                u64::try_from(nodes.len()).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            nodes.dedup();
        }
        budget.charge_into_error(
            u64::try_from(
                emission
                    .selected_nodes
                    .len()
                    .checked_add(
                        chords
                            .len()
                            .checked_mul(2)
                            .ok_or(ResourceError::SizeOverflow)?,
                    )
                    .ok_or(ResourceError::SizeOverflow)?,
            )
            .map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        budget.charge_into_error(
            u64::try_from(q.nodes.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut producer_germs_by_node = vec![Vec::<QProducerGerm>::new(); q.nodes.len()];
        let mut source_nodes = emission.selected_nodes.clone();
        for (chord_index, chord) in chords.iter().enumerate() {
            budget.charge_into_error(1)?;
            for endpoint in 0..2 {
                budget.charge_into_error(1)?;
                let graph_node = *chord.nodes.get(endpoint).ok_or_else(|| {
                    GeometryErrorKind::Invalid("producer chord endpoint is unknown".into())
                })?;
                if let QGraphNode::Source(node) = graph_node {
                    source_nodes.push(node);
                    let endpoint_parameter = if endpoint == 0 {
                        ExactRational::zero()
                    } else {
                        ExactRational::one()
                    };
                    let mut source_ranges = Vec::new();
                    for (range_index, range) in chord.source_ranges.iter().enumerate() {
                        budget.charge_into_error(1)?;
                        if producer_range_touches_node(range, &endpoint_parameter, node, budget)? {
                            source_ranges.push(range_index);
                        }
                    }
                    if source_ranges.is_empty() {
                        return Err(GeometryErrorKind::Invalid(
                            "producer chord endpoint lost its source-root junction germ".into(),
                        )
                        .into());
                    }
                    producer_germs_by_node
                        .get_mut(node.0 as usize)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "producer junction has no germ inventory row".into(),
                            )
                        })?
                        .push(QProducerGerm {
                            chord: chord_index,
                            endpoint,
                            source_ranges,
                        });
                }
            }
            for range in &chord.source_ranges {
                budget.charge_into_error(1)?;
                budget.charge_into_error(2)?;
                source_nodes.extend(range.nodes);
            }
        }
        try_sort_by_with_work(&mut source_nodes, budget, |left, right, _work| {
            Ok(left.cmp(right))
        })?;
        budget.charge_into_error(
            u64::try_from(source_nodes.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        source_nodes.dedup();

        let mut junctions = Vec::with_capacity(source_nodes.len());
        for node_id in source_nodes {
            budget.charge_into_error(1)?;
            let node = q.nodes.get(node_id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("producer junction references an unknown Q node".into())
            })?;
            let ports = q.node_ports.get(node.ports.clone()).ok_or_else(|| {
                GeometryErrorKind::Invalid("producer junction has an incomplete port row".into())
            })?;
            budget.charge_into_error(
                u64::try_from(ports.len()).map_err(|_| ResourceError::SizeOverflow)?,
            )?;

            let fan_start = *q.outgoing_offsets.get(node_id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("producer junction has no fan start".into())
            })?;
            let fan_end = *q
                .outgoing_offsets
                .get(node_id.0 as usize + 1)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("producer junction has no fan end".into())
                })?;
            let darts = q.outgoing.get(fan_start..fan_end).ok_or_else(|| {
                GeometryErrorKind::Invalid("producer junction has an invalid fan range".into())
            })?;
            budget.charge_into_error(
                u64::try_from(darts.len()).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            let mut face_germs = Vec::with_capacity(darts.len());
            for &dart in darts {
                budget.charge_into_error(1)?;
                let local = *q.dart_faces.get(dart.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("producer fan dart has no local face".into())
                })?;
                let face = *q
                    .global_face_of_local
                    .get(local.0 as usize)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "producer fan face has no global identity".into(),
                        )
                    })?;
                let selected_root = emission
                    .selected_face_roots
                    .get(face.0 as usize)
                    .copied()
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "producer fan face has no selected-root record".into(),
                        )
                    })?;
                face_germs.push(QProducerFaceGerm {
                    dart,
                    face,
                    selected_root,
                });
            }

            let mut producer_germs = std::mem::take(
                producer_germs_by_node
                    .get_mut(node_id.0 as usize)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "producer junction has no germ inventory row".into(),
                        )
                    })?,
            );
            try_sort_by_with_work(&mut producer_germs, budget, |left, right, budget| {
                budget.charge_into_error(1)?;
                Ok(left
                    .chord
                    .cmp(&right.chord)
                    .then(left.endpoint.cmp(&right.endpoint)))
            })?;
            let isolated_host = if darts.is_empty() {
                Some(
                    q.source_node_faces
                        .get(node_id.0 as usize)
                        .copied()
                        .flatten()
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "isolated producer junction has no exact host face".into(),
                            )
                        })?,
                )
            } else {
                None
            };
            let mut dependencies = vec![QVariableNode::Source(node_id)];
            for &dart_id in darts {
                budget.charge_into_error(1)?;
                let dart = q.darts.get(dart_id.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("producer junction dart is unknown".into())
                })?;
                let span = q.spans.get(dart.span.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("producer junction span is unknown".into())
                })?;
                if selected_spans
                    .get(dart.span.0 as usize)
                    .copied()
                    .unwrap_or(false)
                {
                    budget.charge_into_error(2)?;
                    dependencies.extend(span.nodes.map(QVariableNode::Source));
                }
            }
            for germ in &producer_germs {
                budget.charge_into_error(1)?;
                let chord = chords.get(germ.chord).ok_or_else(|| {
                    GeometryErrorKind::Invalid("producer junction chord is unknown".into())
                })?;
                for node in chord.nodes {
                    budget.charge_into_error(1)?;
                    dependencies.push(match node {
                        QGraphNode::Source(node) => QVariableNode::Source(node),
                        QGraphNode::Published(node) => QVariableNode::Published(node),
                    });
                }
                for &range_index in &germ.source_ranges {
                    budget.charge_into_error(1)?;
                    let range = chord.source_ranges.get(range_index).ok_or_else(|| {
                        GeometryErrorKind::Invalid("producer junction root range is unknown".into())
                    })?;
                    budget.charge_into_error(2)?;
                    dependencies.extend(range.nodes.map(QVariableNode::Source));
                }
            }
            if let Some(host) = isolated_host {
                let host_nodes = host_boundary_nodes.get(host.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("producer junction host is unknown".into())
                })?;
                budget.charge_into_error(
                    u64::try_from(host_nodes.len()).map_err(|_| ResourceError::SizeOverflow)?,
                )?;
                dependencies.extend(host_nodes.iter().copied().map(QVariableNode::Source));
            }
            try_sort_by_with_work(&mut dependencies, budget, |left, right, budget| {
                budget.charge_into_error(1)?;
                Ok(left.cmp(right))
            })?;
            budget.charge_into_error(
                u64::try_from(dependencies.len()).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            dependencies.dedup();
            junctions.push(QProducerJunction {
                node: node_id,
                ports: ports.to_vec(),
                face_germs,
                producer_germs,
                isolated_host,
                dependencies,
            });
        }

        let mut linework = Vec::new();
        for (chord_index, chord) in chords.iter().enumerate() {
            budget.charge_into_error(1)?;
            let recipe = emission
                .fragment_recipes
                .get(chord.recipe.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("producer linework recipe is unknown".into())
                })?;
            if matches!(
                recipe.use_kind,
                SelectedUse::Curve | SelectedUse::ArealBoundaryLine
            ) {
                if chord.source_ranges.is_empty() {
                    return Err(GeometryErrorKind::Invalid(
                        "selected producer linework has no source-root attachment".into(),
                    )
                    .into());
                }
                let mut source_ranges = Vec::with_capacity(chord.source_ranges.len());
                let mut dependencies = Vec::new();
                for node in chord.nodes {
                    budget.charge_into_error(1)?;
                    dependencies.push(match node {
                        QGraphNode::Source(node) => QVariableNode::Source(node),
                        QGraphNode::Published(node) => QVariableNode::Published(node),
                    });
                }
                for (range_index, range) in chord.source_ranges.iter().enumerate() {
                    budget.charge_into_error(1)?;
                    let edge = topology
                        .sources
                        .edges
                        .get(range.source_edge.0 as usize)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "producer linework root edge is unknown".into(),
                            )
                        })?;
                    let supported = matches!(
                        (recipe.use_kind, edge.kind),
                        (
                            SelectedUse::Curve,
                            SourceEdgeKind::Line { .. }
                                | SourceEdgeKind::Ring {
                                    kind: crate::geometry::RingKind::Collapsed,
                                    ..
                                },
                        ) | (SelectedUse::ArealBoundaryLine, SourceEdgeKind::Ring { .. })
                    );
                    if !supported {
                        return Err(GeometryErrorKind::Invalid(
                            "producer linework root has an incompatible selected use".into(),
                        )
                        .into());
                    }
                    source_ranges.push(range_index);
                    budget.charge_into_error(2)?;
                    dependencies.extend(range.nodes.map(QVariableNode::Source));
                }
                try_sort_by_with_work(&mut dependencies, budget, |left, right, budget| {
                    budget.charge_into_error(1)?;
                    Ok(left.cmp(right))
                })?;
                budget.charge_into_error(
                    u64::try_from(dependencies.len()).map_err(|_| ResourceError::SizeOverflow)?,
                )?;
                dependencies.dedup();
                linework.push(QProducerLinework {
                    chord: chord_index,
                    recipe: chord.recipe,
                    use_kind: recipe.use_kind,
                    source_ranges,
                    dependencies,
                });
            }
        }

        let mut points = Vec::new();
        for &node in emission.point_nodes {
            budget.charge_into_error(1)?;
            let fan_offsets = q
                .outgoing_offsets
                .get(node.0 as usize..node.0 as usize + 2)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("producer point has no fan offsets".into())
                })?;
            let [fan_start, fan_end] = fan_offsets else {
                return Err(GeometryErrorKind::Invalid(
                    "producer point has invalid fan offsets".into(),
                )
                .into());
            };
            let is_isolated = fan_start == fan_end;
            let host = q.source_node_faces.get(node.0 as usize).copied().flatten();
            if is_isolated != host.is_some() {
                return Err(GeometryErrorKind::Invalid(
                    "producer point isolate and host facts disagree".into(),
                )
                .into());
            }
            let point_rows = q.point_rows_for_node(node.0 as usize)?;
            budget.charge_into_error(
                u64::try_from(point_rows.len()).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            let mut dependencies = vec![QVariableNode::Source(node)];
            if let Some(host) = host {
                let host_nodes = host_boundary_nodes.get(host.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("producer point host is unknown".into())
                })?;
                budget.charge_into_error(
                    u64::try_from(host_nodes.len()).map_err(|_| ResourceError::SizeOverflow)?,
                )?;
                dependencies.extend(host_nodes.iter().copied().map(QVariableNode::Source));
            }
            try_sort_by_with_work(&mut dependencies, budget, |left, right, budget| {
                budget.charge_into_error(1)?;
                Ok(left.cmp(right))
            })?;
            budget.charge_into_error(
                u64::try_from(dependencies.len()).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            dependencies.dedup();
            points.push(QProducerPoint {
                node,
                host,
                point_rows: point_rows.to_vec(),
                dependencies,
            });
        }
        let witness = Self {
            junctions,
            linework,
            points,
        };
        witness.validate(topology, emission, chords, budget)?;
        witness.validate_live_nodes(&emission.live_nodes, budget)?;
        Ok(witness)
    }
}
