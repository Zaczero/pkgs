use super::super::*;

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(in crate::geometry::topology::q_embedding) struct QPublicationPair {
    pub(in crate::geometry::topology::q_embedding) left: QPublicationChordId,
    pub(in crate::geometry::topology::q_embedding) right: QPublicationChordId,
    pub(in crate::geometry::topology::q_embedding) right_shift: i8,
}
#[derive(Clone)]
pub(in crate::geometry::topology::q_embedding) struct QFanRow {
    pub(in crate::geometry::topology::q_embedding) darts: Vec<QDartId>,
    pub(in crate::geometry::topology::q_embedding) dependencies: Vec<QNodeId>,
}
#[derive(Clone)]
pub(in crate::geometry::topology::q_embedding) struct QCutRow {
    pub(in crate::geometry::topology::q_embedding) node: QNodeId,
    pub(in crate::geometry::topology::q_embedding) dependencies: Vec<QNodeId>,
}
#[derive(Clone)]
pub(in crate::geometry::topology::q_embedding) struct QClassState {
    pub(in crate::geometry::topology::q_embedding) pairs_by_rank: Vec<Vec<usize>>,
    pub(in crate::geometry::topology::q_embedding) publication_pairs_by_rank: Vec<Vec<usize>>,
    pub(in crate::geometry::topology::q_embedding) cuts_by_rank: Vec<Vec<usize>>,
    pub(in crate::geometry::topology::q_embedding) fans_by_rank: Vec<Vec<usize>>,
}
pub(in crate::geometry::topology::q_embedding) struct UnifiedQClassBuild {
    pub(in crate::geometry::topology::q_embedding) source_variable_count: usize,
    pub(in crate::geometry::topology::q_embedding) classes: Vec<Vec<QVariableNode>>,
    pub(in crate::geometry::topology::q_embedding) node_location: Vec<Option<QNodeLocation>>,
    pub(in crate::geometry::topology::q_embedding) publication_location: Vec<Option<QNodeLocation>>,
    pub(in crate::geometry::topology::q_embedding) states: Vec<QClassState>,
}
pub(in crate::geometry::topology::q_embedding) type QSourceClass = Vec<QNodeId>;
pub(in crate::geometry::topology::q_embedding) type QNodeLocation = (usize, usize);
pub(in crate::geometry::topology::q_embedding) struct QDependencyIndex {
    pub(in crate::geometry::topology::q_embedding) pairs: Vec<Vec<usize>>,
    pub(in crate::geometry::topology::q_embedding) cuts: Vec<Vec<usize>>,
    pub(in crate::geometry::topology::q_embedding) fans: Vec<Vec<usize>>,
}
pub(in crate::geometry::topology::q_embedding) struct QClosure {
    pub(in crate::geometry::topology::q_embedding) classes: Vec<QSourceClass>,
    pub(in crate::geometry::topology::q_embedding) proximity_pairs: Vec<QProximityPair>,
    pub(in crate::geometry::topology::q_embedding) dependency_index: QDependencyIndex,
}
pub(in crate::geometry::topology::q_embedding) fn union_prepared_dependencies(
    dependencies: &[QVariableNode],
    variable_index: &BTreeMap<QVariableNode, usize>,
    source_movable_axes: &[[bool; 2]],
    publication_variables: &[EmissionNodeId],
    source_domains: &[Vec<QCandidate>],
    publication_domains: &[Vec<QCandidate>],
    union: &mut UnionFind,
    budget: &mut impl Work,
) -> Result<()> {
    let mut indices = Vec::new();
    for &dependency in dependencies {
        budget.charge_into_error(1)?;
        if let Some(&index) = variable_index.get(&dependency) {
            indices.push(index);
            continue;
        }
        let is_movable = match dependency {
            QVariableNode::Source(node) => {
                source_domains.get(node.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("producer source dependency has no domain".into())
                })?;
                source_movable_axes
                    .get(node.0 as usize)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "producer source dependency has no movability row".into(),
                        )
                    })?
                    .iter()
                    .any(|&movable| movable)
            },
            QVariableNode::Published(node) => {
                if publication_domains.get(node.0 as usize).is_none() {
                    return Err(GeometryErrorKind::Invalid(
                        "producer publication dependency has no domain".into(),
                    )
                    .into());
                }
                publication_variables.binary_search(&node).is_ok()
            },
        };
        if is_movable {
            return Err(GeometryErrorKind::Invalid(
                "movable producer dependency is missing from the prepared Q inventory".into(),
            )
            .into());
        }
    }
    try_sort_by_with_work(&mut indices, budget, |left, right, _work| {
        Ok(left.cmp(right))
    })?;
    budget.charge_into_error(
        u64::try_from(indices.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    indices.dedup();
    if let Some(&first) = indices.first() {
        for &index in indices.iter().skip(1) {
            budget.charge_into_error(1)?;
            union.union_with_work(first, index, budget)?;
        }
    }
    Ok(())
}
pub(in crate::geometry::topology::q_embedding) fn build_unified_q_classes(
    inputs: &UnifiedQClassInputs<'_>,
    budget: &mut impl Work,
) -> Result<UnifiedQClassBuild> {
    let UnifiedQClassInputs {
        topology,
        source_classes,
        publication_variables,
        publication_nodes,
        source_chords,
        publication_chords,
        proximity_pairs,
        cut_rows,
        fan_rows,
        publication_pairs,
        source_domains,
        source_movable_axes,
        publication_domains,
        producer_witness,
    } = *inputs;
    let mut source_variable_count = 0_usize;
    for class in source_classes {
        budget.charge_into_error(1)?;
        source_variable_count = source_variable_count
            .checked_add(class.len())
            .ok_or(ResourceError::SizeOverflow)?;
    }
    let variable_count = source_variable_count
        .checked_add(publication_variables.len())
        .ok_or(ResourceError::SizeOverflow)?;
    budget.charge_into_error(
        u64::try_from(variable_count).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut variables = Vec::with_capacity(variable_count);
    for class in source_classes {
        budget.charge_into_error(1)?;
        for &node in class {
            budget.charge_into_error(1)?;
            variables.push(QVariableNode::Source(node));
        }
    }
    for &node in publication_variables {
        budget.charge_into_error(1)?;
        variables.push(QVariableNode::Published(node));
    }
    let mut variable_index = BTreeMap::new();
    for (index, &variable) in variables.iter().enumerate() {
        budget.charge_into_error(1)?;
        variable_index.insert(variable, index);
    }
    budget.charge_into_error(
        u64::try_from(variables.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut union = UnionFind::new(variables.len());
    for class in source_classes {
        budget.charge_into_error(1)?;
        let Some(&first) = class.first() else {
            continue;
        };
        let first = *variable_index
            .get(&QVariableNode::Source(first))
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q source variable has no unified index".into())
            })?;
        for &node in class.iter().skip(1) {
            budget.charge_into_error(1)?;
            let index = *variable_index
                .get(&QVariableNode::Source(node))
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q source variable has no unified index".into())
                })?;
            union.union_with_work(first, index, budget)?;
        }
    }
    let frame = topology.sources.frame;
    for chord in publication_chords {
        budget.charge_into_error(1)?;
        if !chord.generated {
            continue;
        }
        let mut dependencies = Vec::new();
        for &node in &chord.dependencies {
            budget.charge_into_error(1)?;
            if let Some((index, _)) = q_graph_variable_domain(
                QGraphNode::Source(node),
                source_domains,
                publication_domains,
                source_movable_axes,
                publication_variables,
                &variable_index,
                budget,
            )? {
                dependencies.push(index);
            }
        }
        for node in chord.nodes {
            budget.charge_into_error(1)?;
            if let Some((index, _)) = q_graph_variable_domain(
                node,
                source_domains,
                publication_domains,
                source_movable_axes,
                publication_variables,
                &variable_index,
                budget,
            )? {
                dependencies.push(index);
            }
        }
        try_sort_by_with_work(&mut dependencies, budget, |left, right, budget| {
            budget.charge_into_error(1)?;
            Ok(left.cmp(right))
        })?;
        budget.charge_into_error(
            u64::try_from(dependencies.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        dependencies.dedup();
        if let Some(&first) = dependencies.first() {
            for &index in dependencies.iter().skip(1) {
                budget.charge_into_error(1)?;
                union.union_with_work(first, index, budget)?;
            }
        }
        if chord.nodes[0] == chord.nodes[1] {
            continue;
        }
        let left = q_graph_variable_domain(
            chord.nodes[0],
            source_domains,
            publication_domains,
            source_movable_axes,
            publication_variables,
            &variable_index,
            budget,
        )?;
        let right = q_graph_variable_domain(
            chord.nodes[1],
            source_domains,
            publication_domains,
            source_movable_axes,
            publication_variables,
            &variable_index,
            budget,
        )?;
        if let (Some((left_index, left_domain)), Some((right_index, right_domain))) = (left, right)
            && left_index != right_index
            && candidate_domains_share_emitted_key(frame, left_domain, right_domain, budget)?
        {
            union.union_with_work(left_index, right_index, budget)?;
        }
    }
    for &pair in publication_pairs {
        budget.charge_into_error(1)?;
        let dependencies =
            publication_pair_variables(pair, source_chords, publication_chords, budget)?;
        let mut indices = Vec::with_capacity(dependencies.len());
        for &dependency in &dependencies {
            budget.charge_into_error(1)?;
            if let Some(&index) = variable_index.get(&dependency) {
                indices.push(index);
                continue;
            }
            let is_movable = match dependency {
                QVariableNode::Source(node) => {
                    let axes = source_movable_axes.get(node.0 as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "publication dependency source node is unknown".into(),
                        )
                    })?;
                    axes.iter().any(|&movable| movable)
                },
                QVariableNode::Published(node) => {
                    budget.charge_into_error(1)?;
                    if publication_domains.get(node.0 as usize).is_none() {
                        return Err(GeometryErrorKind::Invalid(
                            "publication dependency node has no admitted domain".into(),
                        )
                        .into());
                    }
                    publication_variables.binary_search(&node).is_ok()
                },
            };
            if is_movable {
                return Err(GeometryErrorKind::Invalid(
                    "movable publication dependency is missing from the prepared Q inventory"
                        .into(),
                )
                .into());
            }
        }
        try_sort_by_with_work(&mut indices, budget, |left, right, budget| {
            budget.charge_into_error(1)?;
            Ok(left.cmp(right))
        })?;
        budget.charge_into_error(
            u64::try_from(indices.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        indices.dedup();
        if let Some(&first) = indices.first() {
            for &index in indices.iter().skip(1) {
                budget.charge_into_error(1)?;
                union.union_with_work(first, index, budget)?;
            }
        }
    }
    for junction in &producer_witness.junctions {
        budget.charge_into_error(1)?;
        union_prepared_dependencies(
            &junction.dependencies,
            &variable_index,
            source_movable_axes,
            publication_variables,
            source_domains,
            publication_domains,
            &mut union,
            budget,
        )?;
    }
    for linework in &producer_witness.linework {
        budget.charge_into_error(1)?;
        union_prepared_dependencies(
            &linework.dependencies,
            &variable_index,
            source_movable_axes,
            publication_variables,
            source_domains,
            publication_domains,
            &mut union,
            budget,
        )?;
    }
    for point in &producer_witness.points {
        budget.charge_into_error(1)?;
        union_prepared_dependencies(
            &point.dependencies,
            &variable_index,
            source_movable_axes,
            publication_variables,
            source_domains,
            publication_domains,
            &mut union,
            budget,
        )?;
    }

    let mut groups = BTreeMap::<usize, Vec<QVariableNode>>::new();
    for (index, &variable) in variables.iter().enumerate() {
        budget.charge_into_error(1)?;
        let root = union.find_with_work(index, budget)?;
        groups.entry(root).or_default().push(variable);
    }
    let mut classes = groups.into_values().collect::<Vec<_>>();
    for class in &mut classes {
        try_sort_by_with_work(class, budget, |left, right, budget| {
            budget.charge_into_error(1)?;
            compare_variable_nodes(topology, publication_nodes, *left, *right)
        })?;
    }
    try_sort_by_with_work(&mut classes, budget, |left, right, budget| {
        budget.charge_into_error(1)?;
        let left = *left
            .first()
            .ok_or_else(|| GeometryErrorKind::Invalid("unified Q class is empty".into()))?;
        let right = *right
            .first()
            .ok_or_else(|| GeometryErrorKind::Invalid("unified Q class is empty".into()))?;
        compare_variable_nodes(topology, publication_nodes, left, right)
    })?;

    budget.charge_into_error(
        u64::try_from(
            topology
                .q
                .nodes
                .len()
                .checked_add(publication_nodes.nodes().len())
                .ok_or(ResourceError::SizeOverflow)?,
        )
        .map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut node_location = vec![None; topology.q.nodes.len()];
    let mut publication_location = vec![None; publication_nodes.nodes().len()];
    for (class, variables) in classes.iter().enumerate() {
        budget.charge_into_error(1)?;
        for (rank, &variable) in variables.iter().enumerate() {
            budget.charge_into_error(1)?;
            let slot = match variable {
                QVariableNode::Source(node) => node_location.get_mut(node.0 as usize),
                QVariableNode::Published(node) => publication_location.get_mut(node.0 as usize),
            }
            .ok_or_else(|| GeometryErrorKind::Invalid("unified Q node is unknown".into()))?;
            *slot = Some((class, rank));
        }
    }

    let mut states = Vec::with_capacity(classes.len());
    for class in &classes {
        budget.charge_into_error(1)?;
        budget.charge_into_error(
            u64::try_from(class.len())
                .map_err(|_| ResourceError::SizeOverflow)?
                .checked_mul(4)
                .ok_or(ResourceError::SizeOverflow)?,
        )?;
        states.push(QClassState {
            pairs_by_rank: std::iter::repeat_with(Vec::new).take(class.len()).collect(),
            publication_pairs_by_rank: std::iter::repeat_with(Vec::new).take(class.len()).collect(),
            cuts_by_rank: std::iter::repeat_with(Vec::new).take(class.len()).collect(),
            fans_by_rank: std::iter::repeat_with(Vec::new).take(class.len()).collect(),
        });
    }
    for (pair_index, pair) in proximity_pairs.iter().enumerate() {
        budget.charge_into_error(1)?;
        let left = source_chords.get(pair.left).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q proximity left chord is unknown".into())
        })?;
        let right = source_chords.get(pair.right).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q proximity right chord is unknown".into())
        })?;
        let dependencies = left
            .dependencies
            .iter()
            .chain(&right.dependencies)
            .copied()
            .map(QVariableNode::Source);
        if let Some((class, rank)) = variable_class_rank(
            dependencies,
            &node_location,
            &publication_location,
            source_movable_axes,
            publication_variables,
            budget,
        )? {
            states
                .get_mut(class)
                .and_then(|state| state.pairs_by_rank.get_mut(rank))
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q proximity readiness rank is unknown".into())
                })?
                .push(pair_index);
        }
    }
    for (pair_index, &pair) in publication_pairs.iter().enumerate() {
        budget.charge_into_error(1)?;
        let dependencies =
            publication_pair_variables(pair, source_chords, publication_chords, budget)?;
        if let Some((class, rank)) = variable_class_rank(
            dependencies,
            &node_location,
            &publication_location,
            source_movable_axes,
            publication_variables,
            budget,
        )? {
            states
                .get_mut(class)
                .and_then(|state| state.publication_pairs_by_rank.get_mut(rank))
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "Q publication proximity readiness rank is unknown".into(),
                    )
                })?
                .push(pair_index);
        }
    }
    for (cut_index, cut) in cut_rows.iter().enumerate() {
        budget.charge_into_error(1)?;
        let dependencies = cut.dependencies.iter().copied().map(QVariableNode::Source);
        if let Some((class, rank)) = variable_class_rank(
            dependencies,
            &node_location,
            &publication_location,
            source_movable_axes,
            publication_variables,
            budget,
        )? {
            states
                .get_mut(class)
                .and_then(|state| state.cuts_by_rank.get_mut(rank))
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q cut readiness rank is unknown".into())
                })?
                .push(cut_index);
        }
    }
    for (fan_index, fan) in fan_rows.iter().enumerate() {
        budget.charge_into_error(1)?;
        let dependencies = fan.dependencies.iter().copied().map(QVariableNode::Source);
        if let Some((class, rank)) = variable_class_rank(
            dependencies,
            &node_location,
            &publication_location,
            source_movable_axes,
            publication_variables,
            budget,
        )? {
            states
                .get_mut(class)
                .and_then(|state| state.fans_by_rank.get_mut(rank))
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q fan readiness rank is unknown".into())
                })?
                .push(fan_index);
        }
    }
    Ok(UnifiedQClassBuild {
        source_variable_count,
        classes,
        node_location,
        publication_location,
        states,
    })
}
pub(in crate::geometry::topology::q_embedding) fn build_q_fan_rows(
    topology: &ExactGeographicTopology,
    emission: &QEmbeddingEmission<'_, '_>,
    budget: &mut impl Work,
) -> Result<Vec<QFanRow>> {
    let q = &topology.q;
    budget.charge_into_error(
        u64::try_from(q.darts.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut selected_darts = vec![false; q.darts.len()];
    for &span_index in &emission.spans {
        budget.charge_into_error(1)?;
        let Some(forward) = q.span_darts.get(span_index).copied().flatten() else {
            continue;
        };
        for dart in [forward, QDartId(forward.0 ^ 1)] {
            budget.charge_into_error(1)?;
            *selected_darts
                .get_mut(dart.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q selected dart is unknown".into()))? =
                true;
        }
    }
    let mut rows = Vec::new();
    for &node in &emission.selected_nodes {
        budget.charge_into_error(1)?;
        let start = *q
            .outgoing_offsets
            .get(node.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q fan has no outgoing start".into()))?;
        let end = *q
            .outgoing_offsets
            .get(node.0 as usize + 1)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q fan has no outgoing end".into()))?;
        let exact_row = q.outgoing.get(start..end).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q fan has an invalid outgoing range".into())
        })?;
        let mut darts = Vec::new();
        for &dart in exact_row {
            budget.charge_into_error(1)?;
            if selected_darts
                .get(dart.0 as usize)
                .copied()
                .unwrap_or(false)
            {
                darts.push(dart);
            }
        }
        if darts.len() < 2 {
            continue;
        }
        let mut dependencies = vec![node];
        for &dart_id in &darts {
            budget.charge_into_error(1)?;
            let dart = q.darts.get(dart_id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q selected fan dart is unknown".into())
            })?;
            let span_index = dart.span.0 as usize;
            let span = q.spans.get(span_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q selected fan span is unknown".into())
            })?;
            dependencies.extend(chord_dependencies(
                topology, emission, span.nodes, span.ports, budget,
            )?);
        }
        try_sort_by_with_work(&mut dependencies, budget, |left, right, _work| {
            Ok(left.cmp(right))
        })?;
        budget.charge_into_error(
            u64::try_from(dependencies.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        dependencies.dedup();
        rows.push(QFanRow {
            darts,
            dependencies,
        });
    }
    Ok(rows)
}
pub(in crate::geometry::topology::q_embedding) fn build_q_cut_rows(
    topology: &ExactGeographicTopology,
    emission: &QEmbeddingEmission<'_, '_>,
    budget: &mut impl Work,
) -> Result<Vec<QCutRow>> {
    let q = &topology.q;
    let mut rows = Vec::with_capacity(emission.cut_nodes.len());
    for cut in &emission.cut_nodes {
        budget.charge_into_error(1)?;
        let mut dependencies = Vec::with_capacity(cut.chords.len().saturating_mul(2));
        dependencies.push(cut.node);
        for chord in &cut.chords {
            budget.charge_into_error(1)?;
            for port_id in [chord.west_far_port, chord.east_far_port] {
                budget.charge_into_error(1)?;
                let port = q.ports.get(port_id.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q cut dependency port is unknown".into())
                })?;
                dependencies.push(port.node);
            }
        }
        try_sort_by_with_work(&mut dependencies, budget, |left, right, _work| {
            Ok(left.cmp(right))
        })?;
        budget.charge_into_error(
            u64::try_from(dependencies.len()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        dependencies.dedup();
        rows.push(QCutRow {
            node: cut.node,
            dependencies,
        });
    }
    Ok(rows)
}
pub(in crate::geometry::topology::q_embedding) fn node_is_movable(
    node: QNodeId,
    movable_axes: &[[bool; 2]],
) -> Result<bool> {
    movable_axes
        .get(node.0 as usize)
        .map(|axes| axes.iter().any(|&movable| movable))
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("Q dependency has no movability row".into()).into()
        })
}
pub(in crate::geometry::topology::q_embedding) fn include_closure_variables(
    dependencies: impl IntoIterator<Item = QNodeId>,
    movable_axes: &[[bool; 2]],
    closure: &mut [bool],
    union: &mut UnionFind,
    newly_reached: &mut Vec<QNodeId>,
    budget: &mut impl Work,
) -> Result<()> {
    let mut variables = Vec::new();
    for node in dependencies {
        budget.charge_into_error(1)?;
        if node_is_movable(node, movable_axes)? {
            variables.push(node);
        }
    }
    try_sort_by_with_work(&mut variables, budget, |left, right, _work| {
        Ok(left.cmp(right))
    })?;
    budget.charge_into_error(
        u64::try_from(variables.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    variables.dedup();
    if let Some(&first) = variables.first() {
        for &node in variables.iter().skip(1) {
            budget.charge_into_error(1)?;
            union.union_with_work(first.0 as usize, node.0 as usize, budget)?;
        }
    }
    for node in variables {
        budget.charge_into_error(1)?;
        let reached = closure.get_mut(node.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q closure dependency node is unknown".into())
        })?;
        if !std::mem::replace(reached, true) {
            newly_reached.push(node);
        }
    }
    Ok(())
}
pub(in crate::geometry::topology::q_embedding) fn build_q_classes(
    topology: &ExactGeographicTopology,
    closure: &[bool],
    union: &mut UnionFind,
    budget: &mut impl Work,
) -> Result<Vec<QSourceClass>> {
    let mut roots = Vec::new();
    for (node, &included) in closure.iter().enumerate() {
        budget.charge_into_error(1)?;
        if included {
            roots.push((
                union.find_with_work(node, budget)?,
                QNodeId(u32::try_from(node).map_err(|_| ResourceError::SizeOverflow)?),
            ));
        }
    }
    try_sort_by_with_work(&mut roots, budget, |left, right, budget| {
        budget.charge_into_error(1)?;
        let root = left.0.cmp(&right.0);
        Ok(if root.is_eq() {
            let left_key = &topology
                .q
                .nodes
                .get(left.1.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q class node is unknown".into()))?
                .key;
            let right_key = &topology
                .q
                .nodes
                .get(right.1.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q class node is unknown".into()))?
                .key;
            compare_qnode_keys(left_key, right_key)?.then_with(|| left.1.cmp(&right.1))
        } else {
            root
        })
    })?;
    let mut classes = Vec::<QSourceClass>::new();
    let mut previous_root = None;
    for (root, node) in roots {
        budget.charge_into_error(1)?;
        if previous_root != Some(root) {
            classes.push(Vec::new());
            previous_root = Some(root);
        }
        classes
            .last_mut()
            .expect("each closure node creates or joins a class")
            .push(node);
    }
    try_sort_by_with_work(&mut classes, budget, |left, right, budget| {
        budget.charge_into_error(1)?;
        let left = *left.first().expect("Q variable classes are nonempty");
        let right = *right.first().expect("Q variable classes are nonempty");
        let left_key = &topology
            .q
            .nodes
            .get(left.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q class node is unknown".into()))?
            .key;
        let right_key = &topology
            .q
            .nodes
            .get(right.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q class node is unknown".into()))?
            .key;
        let order = compare_qnode_keys(left_key, right_key)?;
        Ok(if order.is_eq() {
            left.cmp(&right)
        } else {
            order
        })
    })?;

    Ok(classes)
}
