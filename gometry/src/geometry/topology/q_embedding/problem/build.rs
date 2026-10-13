use super::super::*;

impl<'topology, 'emission, 'access, 'metric, M: ChartMetric>
    QEmbeddingProblem<'topology, 'emission, 'access, 'metric, M>
{
    pub(in crate::geometry::topology::q_embedding) fn new(
        topology: &'topology ExactGeographicTopology,
        emission: &'emission QEmbeddingEmission<'topology, 'emission>,
        nearest: &QCoordinates,
        ordinates: &'emission [NodeOrdinates],
        declared_axes: crate::geometry::CoordinateAxes,
        strict_axes: bool,
        operation: &'static str,
        metric_access: Option<&'access SourceArcMetricAccess<'access, 'metric, M>>,
        budget: &mut impl SearchAccount,
    ) -> Result<Self> {
        let q = &topology.q;
        let frame = topology.sources.frame;
        if emission.publication.fragments.len() != emission.fragment_recipes.len()
            || emission.publication.counts.len() != emission.fragment_recipes.len()
            || emission.publication.ordinates.len() != emission.publication.samples.len()
        {
            return Err(GeometryErrorKind::Invalid(
                "Q publication inventory does not match its fragment recipes".into(),
            )
            .into());
        }
        budget.charge_into_error(
            u64::try_from(emission.fragment_recipes.len())
                .map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut publication_counts = vec![0_usize; emission.fragment_recipes.len()];
        let mut publication_chord_offsets: Vec<usize> = Vec::with_capacity(
            emission
                .fragment_recipes
                .len()
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?,
        );
        publication_chord_offsets.push(0);
        for (index, fragment) in emission.publication.fragments.iter().enumerate() {
            budget.charge_into_error(1)?;
            if fragment.recipe.0 as usize != index
                || fragment.trace.rows.edge_count().checked_add(1) != Some(fragment.points.len())
            {
                return Err(GeometryErrorKind::Invalid(
                    "Q publication fragment trace is not indexed by its recipe".into(),
                )
                .into());
            }
            *publication_counts.get_mut(index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q publication count row is unknown".into())
            })? = fragment.trace.samples.len();
            let previous = *publication_chord_offsets.last().ok_or_else(|| {
                GeometryErrorKind::Invalid("Q publication chord prefix is empty".into())
            })?;
            let edge_count = fragment.points.len().checked_sub(1).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q publication fragment has no endpoints".into())
            })?;
            publication_chord_offsets.push(
                previous
                    .checked_add(edge_count)
                    .ok_or(ResourceError::SizeOverflow)?,
            );
        }
        budget.charge_into_error(
            u64::try_from(publication_counts.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        if publication_counts != emission.publication.counts {
            return Err(GeometryErrorKind::Invalid(
                "Q publication sample counts disagree with their traces".into(),
            )
            .into());
        }
        budget.charge_into_error(
            u64::try_from(publication_counts.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut family_occurrences = vec![0_u32; publication_counts.len()];
        for sample in &emission.publication.samples {
            budget.charge_into_error(1)?;
            let family_recipe = sample.family.fragment_recipe().ok_or_else(|| {
                GeometryErrorKind::Invalid("Q publication sample has no fragment family".into())
            })?;
            let parent = family_recipe.0 as usize;
            let occurrence = family_occurrences.get_mut(parent).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q publication sample parent is unknown".into())
            })?;
            let source_trace_valid = match &sample.source_trace {
                None => true,
                Some(TraceRequestLocation::Edge { .. } | TraceRequestLocation::Root { .. }) => {
                    false
                },
                Some(TraceRequestLocation::Sample { recipe, row }) => {
                    let fragment = emission
                        .publication
                        .fragments
                        .get(recipe.0 as usize)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "Q publication sample source trace is unknown".into(),
                            )
                        })?;
                    let trace_sample = fragment.trace.samples.get(row.index());
                    *recipe == family_recipe
                        && fragment.recipe == *recipe
                        && trace_sample.is_some_and(|trace_sample| {
                            trace_sample.request_row == *row
                                && trace_sample.written_vertex == sample.written_vertex
                                && trace_sample.parameter == sample.parameter
                                && trace_sample.has_source
                                && trace_sample.endpoint_only == sample.endpoint_only
                        })
                },
            };
            if sample.family.producer != super::emission::SampleProducerKind::Publication
                || sample.family.occurrence != *occurrence
                || !super::exact::in_unit_interval_with_work(&sample.parameter, budget)?
                || !source_trace_valid
            {
                return Err(GeometryErrorKind::Invalid(
                    "Q publication sample references an unknown producer or source ramp".into(),
                )
                .into());
            }
            *occurrence = occurrence
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
        }
        for (&occurrences, &count) in family_occurrences.iter().zip(&publication_counts) {
            budget.charge_into_error(1)?;
            if usize::try_from(occurrences).map_err(|_| ResourceError::SizeOverflow)? != count {
                return Err(GeometryErrorKind::Invalid(
                    "Q publication families do not cover their materialized samples".into(),
                )
                .into());
            }
        }
        budget.charge_into_error(
            u64::try_from(emission.fragment_recipes.len())
                .map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut seen_fragments = vec![false; emission.fragment_recipes.len()];
        for walk in emission.fragment_walks {
            budget.charge_into_error(1)?;
            match walk.owner {
                RecipeWalkOwner::Cycle(cycle) => {
                    emission.plan.cycles.get(cycle).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "Q recipe walk references an unknown cycle".into(),
                        )
                    })?;
                },
                RecipeWalkOwner::Line(line) => {
                    emission.plan.line_paths.get(line).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "Q recipe walk references an unknown line".into(),
                        )
                    })?;
                },
                RecipeWalkOwner::SourceRing(ring) => {
                    topology.sources.rings.get(ring).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "Q recipe walk references an unknown source ring".into(),
                        )
                    })?;
                },
            }
            for &recipe_id in &walk.fragments {
                budget.charge_into_error(1)?;
                let seen = seen_fragments
                    .get_mut(recipe_id.0 as usize)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q recipe walk has an unknown fragment".into())
                    })?;
                if std::mem::replace(seen, true) {
                    return Err(GeometryErrorKind::Invalid(
                        "Q fragment appears in more than one publication walk".into(),
                    )
                    .into());
                }
            }
        }
        for &seen in &seen_fragments {
            budget.charge_into_error(1)?;
            if !seen {
                return Err(GeometryErrorKind::Invalid(
                    "Q fragment is missing its publication walk".into(),
                )
                .into());
            }
        }
        for (index, recipe) in emission.fragment_recipes.iter().enumerate() {
            budget.charge_into_error(1)?;
            if recipe.id.0 as usize != index {
                return Err(GeometryErrorKind::Invalid(
                    "Q fragment recipes are not indexed by FragmentRecipeId".into(),
                )
                .into());
            }
            let [from_id, to_id] = recipe.endpoints;
            let from = q.ports.get(from_id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q fragment recipe start is unknown".into())
            })?;
            let to = q.ports.get(to_id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q fragment recipe end is unknown".into())
            })?;
            emission.source_nodes.source_node(from.node)?;
            emission.source_nodes.source_node(to.node)?;
            let reference_endpoints = recipe.reference_endpoints(topology, budget)?;
            let publication_fragment =
                emission.publication.fragments.get(index).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "Q fragment has no frozen publication reference".into(),
                    )
                })?;
            if !reference_endpoints[0]
                .try_eq_with_work(&publication_fragment.exact_endpoints[0], budget)?
                || !reference_endpoints[1]
                    .try_eq_with_work(&publication_fragment.exact_endpoints[1], budget)?
            {
                return Err(GeometryErrorKind::Invalid(
                    "Q publication reference differs from its exact recipe endpoints".into(),
                )
                .into());
            }
            let mut has_areal_owner = false;
            for range in &recipe.ranges {
                budget.charge_into_error(1)?;
                let edge = topology
                    .sources
                    .edges
                    .get(range.source_edge.0 as usize)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "Q fragment recipe source edge is unknown".into(),
                        )
                    })?;
                has_areal_owner |= matches!(edge.kind, SourceEdgeKind::Ring { .. });
                if edge.id != range.source_edge
                    || !super::exact::in_unit_interval_with_work(&range.range.start, budget)?
                    || !super::exact::in_unit_interval_with_work(&range.range.end, budget)?
                {
                    return Err(GeometryErrorKind::Invalid(
                        "Q fragment recipe has an invalid directed source range".into(),
                    )
                    .into());
                }
                for producer_parameter in [&range.producer_start, &range.producer_end] {
                    budget.charge_into_error(1)?;
                    if !super::exact::in_unit_interval_with_work(producer_parameter, budget)? {
                        return Err(GeometryErrorKind::Invalid(
                            "Q source range has an invalid affine producer parameter".into(),
                        )
                        .into());
                    }
                }
            }
            if matches!(
                recipe.use_kind,
                super::emission::SelectedUse::AreaBoundary
                    | super::emission::SelectedUse::ArealBoundaryLine
            ) && !has_areal_owner
            {
                return Err(GeometryErrorKind::Invalid(
                    "areal Q recipe lost its source-face ownership".into(),
                )
                .into());
            }
        }
        for planned in planned_area_rings(emission.plan) {
            budget.charge_into_error(1)?;
            let region = match *planned {
                PlannedAreaRing::Source { region, .. } | PlannedAreaRing::Cycle { region, .. } => {
                    region
                },
            };
            let mut low = 0_usize;
            let mut high = emission.selected_regions.len();
            let mut found = false;
            while low < high {
                budget.charge_into_error(1)?;
                let middle = low
                    .checked_add(high.checked_sub(low).ok_or(ResourceError::SizeOverflow)? / 2)
                    .ok_or(ResourceError::SizeOverflow)?;
                let fact = emission.selected_regions.get(middle).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q selected region index is unknown".into())
                })?;
                match fact.root.cmp(&region) {
                    Ordering::Less => {
                        low = middle.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
                    },
                    Ordering::Greater => high = middle,
                    Ordering::Equal => {
                        found = true;
                        break;
                    },
                }
            }
            if !found {
                return Err(GeometryErrorKind::Invalid(
                    "Q area emission references a region without retained pole facts".into(),
                )
                .into());
            }
        }
        budget.charge_into_error(
            u64::try_from(
                q.nodes
                    .len()
                    .checked_mul(3)
                    .ok_or(ResourceError::SizeOverflow)?,
            )
            .map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut exact_positions = Vec::with_capacity(q.nodes.len());
        let mut movable_axes = Vec::with_capacity(q.nodes.len());
        let mut stored_nodes = vec![false; q.nodes.len()];
        for &port_id in q
            .source_edge_ports
            .iter()
            .flatten()
            .chain(&q.source_point_ports)
        {
            budget.charge_into_error(1)?;
            let port = q.ports.get(port_id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q stored-node port is unknown".into())
            })?;
            *stored_nodes
                .get_mut(port.node.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q stored node is unknown".into()))? =
                true;
        }
        for node in &q.nodes {
            budget.charge_into_error(1)?;
            let nearest_xy = nearest.xy(node.id)?;
            let is_stored = stored_nodes
                .get(node.id.0 as usize)
                .copied()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q node has no stored-node state".into())
                })?;
            let exact = if is_stored {
                None
            } else {
                match topology.canonical_node_key(node.id)? {
                    super::q::CanonicalQNodeKey::Chart(key) => {
                        exact_node_position_from_key_with_work(frame, key, budget)?
                    },
                    super::q::CanonicalQNodeKey::Cut { .. } => None,
                }
            };
            let axes = match &exact {
                Some(position) => inexact_axes_with_work(position, nearest_xy, budget)?,
                None => [false, false],
            };
            exact_positions.push(exact);
            movable_axes.push(axes);
        }
        let domains = candidate_domains(
            topology,
            emission,
            nearest,
            &exact_positions,
            &movable_axes,
            budget,
        )?;
        let cut_rows = build_q_cut_rows(topology, emission, budget)?;
        let chords = build_q_chords(topology, emission, nearest, &domains, budget)?;
        let fan_rows = build_q_fan_rows(topology, emission, budget)?;
        let period = ExactRational::from_f64(frame.longitude().period());
        let cut_east = q.cut.try_add_with_work(&period, budget)?;
        budget.charge_into_error(
            u64::try_from(q.nodes.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut cut_index_by_node = vec![None; q.nodes.len()];
        for (cut_index, cut) in emission.cut_nodes.iter().enumerate() {
            budget.charge_into_error(1)?;
            let slot = cut_index_by_node
                .get_mut(cut.node.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q cut node is unknown".into()))?;
            if slot.replace(cut_index).is_some() {
                return Err(GeometryErrorKind::Invalid(
                    "Q embedding emission repeats a cut node".into(),
                )
                .into());
            }
        }
        let exact_faces = if emission.areal_spans.is_empty() {
            None
        } else {
            let exact_chart = subgraph_chart(topology, emission, nearest, None, budget)?;
            Some(QFaceStructure::build(topology, emission, exact_chart)?)
        };
        let source_spans = source_span_range_index(topology, budget)?;
        let BuiltQPublication {
            nodes: publication_nodes,
            sample_nodes: publication_sample_nodes,
            sample_source_ports: publication_sample_source_ports,
            chords: mut publication_chords,
            source_inventory,
            domains: published,
        } = build_q_publication_chords(
            topology,
            emission,
            nearest,
            &domains,
            &source_spans,
            budget,
        )?;
        if publication_sample_nodes.len() != emission.publication.samples.len() {
            return Err(GeometryErrorKind::Invalid(
                "publication sample-to-node mapping is incomplete".into(),
            )
            .into());
        }
        if publication_chord_offsets.last().copied() != Some(publication_chords.len()) {
            return Err(GeometryErrorKind::Invalid(
                "Q publication chord offsets do not cover their fragments".into(),
            )
            .into());
        }
        budget.charge_into_error(
            u64::try_from(ordinates.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut merged_ordinates = ordinates.to_vec();
        for (sample_index, &node_id) in publication_sample_nodes.iter().enumerate() {
            budget.charge_into_error(1)?;
            let node = publication_nodes
                .nodes()
                .get(node_id.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q publication ordinate node is unknown".into())
                })?;
            if let NodeAuthority::Source(source_node) = &node.authority {
                let sample_ordinates = emission
                    .publication
                    .ordinates
                    .get(sample_index)
                    .copied()
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q publication ordinate row is unknown".into())
                    })?;
                let source_ordinates = merged_ordinates
                    .get_mut(source_node.0 as usize)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "Q publication source ordinate node is unknown".into(),
                        )
                    })?;
                source_ordinates.merge(sample_ordinates);
            }
        }
        for chord in &mut publication_chords {
            budget.charge_into_error(1)?;
            for endpoint in &chord.endpoints {
                budget.charge_into_error(1)?;
                let PublishedEndpoint::Published { node, .. } = endpoint else {
                    continue;
                };
                let Some(exact) = published
                    .exact_positions
                    .get(node.0 as usize)
                    .and_then(Option::as_ref)
                else {
                    continue;
                };
                let candidates = published.domains.get(node.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "publication chord endpoint has no candidate domain".into(),
                    )
                })?;
                chord.radius = chord.radius.max(published_candidate_radius(
                    frame, exact, candidates, budget,
                )?);
            }
            let (boxes, image_radii) = chord_boxes(
                chord.nearest,
                chord.radius,
                frame.longitude().period(),
                budget,
            )?;
            chord.boxes = boxes;
            chord.image_radii = image_radii;
        }
        let publication_pairs = build_q_publication_pairs(
            &chords,
            &publication_chords,
            frame.longitude().period(),
            budget,
        )?;
        let prepared_producer =
            PreparedProducerWitness::build(topology, emission, &publication_chords, budget)?;
        let producer_source_dependencies = prepared_producer.source_dependencies(budget)?;
        budget.charge_into_error(
            u64::try_from(emission.live_nodes.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut source_seeds = emission.live_nodes.clone();
        for chord in &chords {
            budget.charge_into_error(1)?;
            for &node in &chord.dependencies {
                budget.charge_into_error(1)?;
                source_seeds.push(node);
            }
        }
        for cut in &cut_rows {
            budget.charge_into_error(1)?;
            for &node in &cut.dependencies {
                budget.charge_into_error(1)?;
                source_seeds.push(node);
            }
        }
        for &node in &producer_source_dependencies {
            budget.charge_into_error(1)?;
            source_seeds.push(node);
        }
        for fan in &fan_rows {
            budget.charge_into_error(1)?;
            for &node in &fan.dependencies {
                budget.charge_into_error(1)?;
                source_seeds.push(node);
            }
        }
        for chord in &publication_chords {
            budget.charge_into_error(1)?;
            if !chord.generated {
                continue;
            }
            for &node in &chord.dependencies {
                budget.charge_into_error(1)?;
                source_seeds.push(node);
            }
            for node in chord.nodes {
                budget.charge_into_error(1)?;
                if let QGraphNode::Source(node) = node {
                    source_seeds.push(node);
                }
            }
        }
        try_sort_by_with_work(&mut source_seeds, budget, |left, right, _work| {
            Ok(left.cmp(right))
        })?;
        budget.charge_into_error(
            u64::try_from(source_seeds.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        source_seeds.dedup();
        let mut source_constructed = 0_usize;
        for &node in &source_seeds {
            budget.charge_into_error(1)?;
            let axes = movable_axes.get(node.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("prepared Q source node has no movability row".into())
            })?;
            if axes.iter().any(|&movable| movable) {
                source_constructed = source_constructed
                    .checked_add(1)
                    .ok_or(ResourceError::SizeOverflow)?;
            }
        }
        budget
            .budget_mut()
            .include_constructed(source_constructed)?;
        let QClosure {
            classes: source_classes,
            proximity_pairs,
            dependency_index,
        } = build_q_proximity_closure(
            topology,
            &chords,
            &domains,
            &cut_rows,
            &fan_rows,
            &source_seeds,
            &movable_axes,
            budget,
        )?;
        let UnifiedQClassBuild {
            source_variable_count: source_base,
            classes,
            node_location,
            publication_location,
            states: class_states,
        } = build_unified_q_classes(
            &UnifiedQClassInputs {
                topology,
                source_classes: &source_classes,
                publication_variables: &published.variables,
                publication_nodes: &publication_nodes,
                source_chords: &chords,
                publication_chords: &publication_chords,
                proximity_pairs: &proximity_pairs,
                cut_rows: &cut_rows,
                fan_rows: &fan_rows,
                publication_pairs: &publication_pairs,
                source_domains: &domains,
                source_movable_axes: &movable_axes,
                publication_domains: &published.domains,
                producer_witness: &prepared_producer,
            },
            budget,
        )?;
        budget.budget_mut().include_constructed(source_base)?;
        Ok(Self {
            topology,
            emission,
            family_state: QFamilyState::Shared {
                original_source_base: source_base,
            },
            ordinates: merged_ordinates,
            metric_access,
            declared_axes,
            strict_axes,
            operation,
            nearest: nearest.clone(),
            exact_positions,
            domains,
            cut_east,
            cut_index_by_node,
            cut_rows,
            chords,
            publication_nodes,
            publication_sample_nodes,
            publication_sample_source_ports,
            publication_chords,
            publication_chord_offsets,
            publication_pairs,
            prepared_producer,
            publication_exact_positions: published.exact_positions,
            publication_nearest: published.nearest,
            publication_domains: published.domains,
            publication_location,
            fan_rows,
            proximity_pairs,
            classes,
            node_location,
            class_states,
            dependency_index,
            live_nodes: &emission.live_nodes,
            source_inventory,
            exact_faces,
        })
    }
}
