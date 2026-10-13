use super::super::{
    BTreeMap, Bounds, ChartPole, ControlFlow, EmissionNodeId, EmissionNodeRegistry,
    ExactChartPoint, ExactGeographicTopology, ExactRational, GeographicFrame, GeometryErrorKind,
    NodeAuthority, Ordering, QCandidate, QCoordinates, QEmbeddingEmission, QNodeId, QNodeLocation,
    QPortId, QPublicationPair, QPublishedChord, QVariableNode, ResourceError, Result,
    SearchAccount, Segment, SourceSampleOccurrence, TraceRequestLocation, chord_boxes,
    chord_dependencies, compare_qnode_keys, compare_quotient_keys, compare_quotient_keys_with_work,
    for_each_overlapping_bounds_cross_pair_with_work, port_swept_radius, proximity_limit,
    publication_leaf_replaces_source_span, quotient_key_for_chart_position_with_work,
    rounded_chart_point_with_work, segments_within_chebyshev, shifted_segment,
    split_rational_native_site_with_work, try_sort_by_with_work,
};
use crate::work::Work;

#[derive(Clone)]
pub(in crate::geometry::topology::q_embedding) struct QChord {
    pub(in crate::geometry::topology::q_embedding) nodes: [QNodeId; 2],
    pub(in crate::geometry::topology::q_embedding) ports: [QPortId; 2],
    pub(in crate::geometry::topology::q_embedding) nearest: Segment,
    pub(in crate::geometry::topology::q_embedding) boxes: [Bounds; 3],
    pub(in crate::geometry::topology::q_embedding) radius: f64,
    pub(in crate::geometry::topology::q_embedding) image_radii: [f64; 3],
    pub(in crate::geometry::topology::q_embedding) dependencies: Vec<QNodeId>,
}
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(in crate::geometry::topology::q_embedding) enum QGraphNode {
    Source(QNodeId),
    Published(EmissionNodeId),
}
pub(in crate::geometry::topology::q_embedding) fn q_graph_variable_domain<'a>(
    node: QGraphNode,
    source_domains: &'a [Vec<QCandidate>],
    publication_domains: &'a [Vec<QCandidate>],
    source_movable_axes: &[[bool; 2]],
    publication_variables: &[EmissionNodeId],
    variable_index: &BTreeMap<QVariableNode, usize>,
    budget: &mut impl Work,
) -> Result<Option<(usize, &'a [QCandidate])>> {
    budget.charge_into_error(1)?;
    let (variable, domain, movable) = match node {
        QGraphNode::Source(node) => {
            let index = node.0 as usize;
            let domain = source_domains.get(index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q graph source node has no domain".into())
            })?;
            let axes = source_movable_axes.get(index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q graph source node has no movability row".into())
            })?;
            (
                QVariableNode::Source(node),
                domain.as_slice(),
                axes.iter().any(|&movable| movable),
            )
        },
        QGraphNode::Published(node) => {
            budget.charge_into_error(1)?;
            let index = node.0 as usize;
            let domain = publication_domains.get(index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q graph publication node has no domain".into())
            })?;
            (
                QVariableNode::Published(node),
                domain.as_slice(),
                publication_variables.binary_search(&node).is_ok(),
            )
        },
    };
    if !movable {
        return Ok(None);
    }
    budget.charge_into_error(1)?;
    let index = *variable_index.get(&variable).ok_or_else(|| {
        GeometryErrorKind::Invalid(
            "movable Q graph endpoint is absent from its prepared variable inventory".into(),
        )
    })?;
    if domain.is_empty() {
        return Err(GeometryErrorKind::Invalid(
            "movable Q graph endpoint has an empty admitted domain".into(),
        )
        .into());
    }
    Ok(Some((index, domain)))
}
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(in crate::geometry::topology::q_embedding) enum QPublicationChordId {
    Source(usize),
    Published(usize),
}
pub(in crate::geometry::topology::q_embedding) struct LocalPublicationChord {
    pub(in crate::geometry::topology::q_embedding) nodes: [QGraphNode; 2],
    pub(in crate::geometry::topology::q_embedding) exact: [ExactChartPoint; 2],
    pub(in crate::geometry::topology::q_embedding) candidate: [ExactChartPoint; 2],
}
pub(in crate::geometry::topology::q_embedding) fn q_chord(
    topology: &ExactGeographicTopology,
    emission: &QEmbeddingEmission<'_, '_>,
    nearest: &QCoordinates,
    domains: &[Vec<QCandidate>],
    nodes: [QNodeId; 2],
    ports: [QPortId; 2],
    budget: &mut impl Work,
) -> Result<QChord> {
    let q = &topology.q;
    let start = q
        .ports
        .get(ports[0].0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q chord start port is unknown".into()))?;
    let end = q
        .ports
        .get(ports[1].0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q chord end port is unknown".into()))?;
    let start_xy = rounded_chart_point_with_work(&start.point, budget)?;
    let end_xy = rounded_chart_point_with_work(&end.point, budget)?;
    let nearest_segment = Segment {
        start: start_xy,
        end: end_xy,
    };
    let dependencies = chord_dependencies(topology, emission, nodes, ports, budget)?;
    let radius = port_swept_radius(topology, emission, nearest, domains, ports[0], budget)?.max(
        port_swept_radius(topology, emission, nearest, domains, ports[1], budget)?,
    );
    let period = topology.sources.frame.longitude().period();
    let (boxes, image_radii) = chord_boxes(nearest_segment, radius, period, budget)?;
    Ok(QChord {
        nodes,
        ports,
        nearest: nearest_segment,
        boxes,
        radius,
        image_radii,
        dependencies,
    })
}
pub(in crate::geometry::topology::q_embedding) fn collapsed_pole_span(
    topology: &ExactGeographicTopology,
    span_index: usize,
    budget: &mut impl Work,
) -> Result<bool> {
    let q = &topology.q;
    let span = q.spans.get(span_index).ok_or_else(|| {
        GeometryErrorKind::Invalid("Q embedding references an unknown span".into())
    })?;
    if span.nodes[0] != span.nodes[1] {
        return Ok(false);
    }
    let node = q
        .nodes
        .get(span.nodes[0].0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q embedding span has an unknown node".into()))?;
    let Some(pole) = node.pole else {
        return Ok(false);
    };
    let latitude = ExactRational::from_f64(match pole {
        ChartPole::North => topology.sources.frame.chart_pole_latitude(ChartPole::North),
        ChartPole::South => topology.sources.frame.chart_pole_latitude(ChartPole::South),
    });
    let start = q
        .ports
        .get(span.ports[0].0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q embedding span has no start port".into()))?;
    let end = q
        .ports
        .get(span.ports[1].0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q embedding span has no end port".into()))?;
    Ok(start.point.y.try_cmp_with_work(&latitude, budget)?.is_eq()
        && end.point.y.try_cmp_with_work(&latitude, budget)?.is_eq())
}
pub(in crate::geometry::topology::q_embedding) fn build_q_chords(
    topology: &ExactGeographicTopology,
    emission: &QEmbeddingEmission<'_, '_>,
    nearest: &QCoordinates,
    domains: &[Vec<QCandidate>],
    budget: &mut impl Work,
) -> Result<Vec<QChord>> {
    let q = &topology.q;
    let chord_capacity = emission
        .spans
        .len()
        .checked_add(emission.point_nodes.len())
        .ok_or(ResourceError::SizeOverflow)?;
    budget.charge_into_error(
        u64::try_from(chord_capacity).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut chords = Vec::with_capacity(chord_capacity);
    for &span_index in &emission.spans {
        if collapsed_pole_span(topology, span_index, budget)? {
            continue;
        }
        let span = q.spans.get(span_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q embedding references an unknown span".into())
        })?;
        chords.push(q_chord(
            topology, emission, nearest, domains, span.nodes, span.ports, budget,
        )?);
    }
    for &node in emission.point_nodes {
        let q_node = q.nodes.get(node.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q embedding point node is unknown".into())
        })?;
        let ports = q.node_ports.get(q_node.ports.clone()).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q emitted point has an invalid port run".into())
        })?;
        let mut selected_port = None;
        for &port_id in ports {
            budget.charge_into_error(1)?;
            if q.ports
                .get(port_id.0 as usize)
                .is_some_and(|port| !port.cut)
            {
                selected_port = Some(port_id);
                break;
            }
        }
        let port = selected_port.ok_or_else(|| {
            GeometryErrorKind::Invalid("Q emitted point has no non-cut port".into())
        })?;
        chords.push(q_chord(
            topology,
            emission,
            nearest,
            domains,
            [node, node],
            [port, port],
            budget,
        )?);
    }
    Ok(chords)
}
pub(in crate::geometry::topology::q_embedding) fn align_native_chart_lift(
    frame: GeographicFrame,
    point: &ExactChartPoint,
    reference: &ExactChartPoint,
    budget: &mut impl Work,
) -> Result<ExactChartPoint> {
    let longitude = frame.longitude();
    let (_, point_lift) = split_rational_native_site_with_work(
        &point.x,
        longitude.period(),
        longitude.half_period(),
        budget,
    )?;
    let (_, reference_lift) = split_rational_native_site_with_work(
        &reference.x,
        longitude.period(),
        longitude.half_period(),
        budget,
    )?;
    let lift_delta = reference_lift.try_subtract_with_work(point_lift, budget)?;
    let longitude_shift = ExactRational::from_f64(longitude.period())
        .try_product_with_work(&ExactRational::from_dyadic(lift_delta), budget)?;
    Ok(ExactChartPoint::from_rationals(
        point.x.try_add_with_work(&longitude_shift, budget)?,
        point.y.try_clone_with_work(budget)?,
    ))
}
pub(in crate::geometry::topology::q_embedding) fn source_port_for_exact_point(
    topology: &ExactGeographicTopology,
    node: QNodeId,
    point: &ExactChartPoint,
    budget: &mut impl Work,
) -> Result<QPortId> {
    let q_node =
        topology.q.nodes.get(node.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q exact publication node is unknown".into())
        })?;
    let wanted = quotient_key_for_chart_position_with_work(topology.sources.frame, point, budget)?;
    let mut matching_identity = None;
    for &port_id in topology
        .q
        .node_ports
        .get(q_node.ports.clone())
        .ok_or_else(|| GeometryErrorKind::Invalid("Q exact node ports are incomplete".into()))?
    {
        budget.charge_into_error(1)?;
        let port = topology
            .q
            .ports
            .get(port_id.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q exact node port is unknown".into()))?;
        let candidate =
            quotient_key_for_chart_position_with_work(topology.sources.frame, &port.point, budget)?;
        if compare_quotient_keys_with_work(&wanted, &candidate, budget)?.is_eq() {
            if port.point.try_eq_with_work(point, budget)? {
                return Ok(port_id);
            }
            matching_identity.get_or_insert(port_id);
        }
    }
    matching_identity.ok_or_else(|| {
        GeometryErrorKind::Invalid("Q exact publication node has no matching chart port".into())
            .into()
    })
}
pub(in crate::geometry::topology::q_embedding) fn source_sample_at_fragment_endpoint(
    fragment: &super::super::super::emission::PublishedFragment,
    start: bool,
    budget: &mut impl Work,
) -> Result<SourceSampleOccurrence> {
    let edge_count = fragment.trace.rows.edge_count();
    let edge = if start {
        0
    } else {
        edge_count.checked_sub(1).ok_or_else(|| {
            GeometryErrorKind::Invalid("published fragment trace has no endpoint edge".into())
        })?
    };
    let parameter = if start {
        ExactRational::zero()
    } else {
        ExactRational::one()
    };
    let (has_source, endpoint_only) = fragment
        .trace
        .rows
        .request_support_at(edge, &parameter, budget)?;
    let source_trace = has_source.then(|| TraceRequestLocation::Edge {
        recipe: fragment.recipe,
        edge,
        parameter: parameter.clone(),
    });
    Ok(SourceSampleOccurrence {
        trace: source_trace,
        endpoint_only,
        publication_sample: None,
    })
}
pub(in crate::geometry::topology::q_embedding) fn build_q_publication_pairs(
    source: &[QChord],
    published: &[QPublishedChord],
    period: f64,
    budget: &mut impl SearchAccount,
) -> Result<Vec<QPublicationPair>> {
    let mut pairs = Vec::new();
    let mut source_bounds = Vec::with_capacity(source.len());
    for chord in source {
        budget.charge_into_error(1)?;
        source_bounds.push(*chord.boxes.get(1).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q source chord has no central bound".into())
        })?);
    }
    let mut published_images = Vec::with_capacity(published.len().saturating_mul(3));
    let mut published_bounds = Vec::with_capacity(published.len().saturating_mul(3));
    for (chord_index, chord) in published.iter().enumerate() {
        budget.charge_into_error(1)?;
        if !chord.generated {
            continue;
        }
        for (shift_index, shift) in [-1_i8, 0, 1].into_iter().enumerate() {
            budget.charge_into_error(1)?;
            published_images.push((chord_index, shift, shift_index));
            published_bounds.push(*chord.boxes.get(shift_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q publication chord has no periodic bound".into())
            })?);
        }
    }
    for_each_overlapping_bounds_cross_pair_with_work(
        &source_bounds,
        &published_bounds,
        budget,
        |source_index, published_index, work| {
            work.charge_evaluations(1)?;
            let source_chord = source
                .get(source_index)
                .expect("source sweep ordinal belongs to the source chord table");
            let (published_index, shift, shift_index) = *published_images
                .get(published_index)
                .expect("publication sweep ordinal belongs to its translated bounds");
            let published_chord = published
                .get(published_index)
                .expect("publication sweep ordinal belongs to the publication chord table");
            // The emitted leaf replaces this rooted source span; it is not an
            // independent edge whose contact with its own source constrains rounding.
            if publication_leaf_replaces_source_span(source_chord, published_chord, work)? {
                return Ok(ControlFlow::<std::convert::Infallible>::Continue(()));
            }
            let translated = shifted_segment(published_chord.nearest, shift, period);
            let radius = *published_chord
                .image_radii
                .get(shift_index)
                .expect("each publication image has a swept radius");
            if segments_within_chebyshev(
                source_chord.nearest,
                translated,
                proximity_limit(source_chord.radius, radius),
            ) {
                pairs.push(QPublicationPair {
                    left: QPublicationChordId::Source(source_index),
                    right: QPublicationChordId::Published(published_index),
                    right_shift: shift,
                });
            }
            Ok(ControlFlow::<std::convert::Infallible>::Continue(()))
        },
    )?;

    let mut published_center_indices = Vec::new();
    let mut published_centers = Vec::with_capacity(published.len());
    for (index, chord) in published.iter().enumerate() {
        budget.charge_into_error(1)?;
        if !chord.generated {
            continue;
        }
        published_center_indices.push(index);
        published_centers.push(*chord.boxes.get(1).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q publication chord has no central bound".into())
        })?);
    }
    for_each_overlapping_bounds_cross_pair_with_work(
        &published_centers,
        &published_bounds,
        budget,
        |left_index, right_index, work| {
            work.charge_evaluations(1)?;
            let left_index = *published_center_indices
                .get(left_index)
                .expect("left publication sweep ordinal belongs to its chord table");
            let left = published
                .get(left_index)
                .expect("left publication sweep ordinal belongs to its chord table");
            let (right_index, shift, shift_index) = *published_images
                .get(right_index)
                .expect("right publication sweep ordinal belongs to translated bounds");
            if left_index == right_index {
                return Ok(ControlFlow::<std::convert::Infallible>::Continue(()));
            }
            let right = published
                .get(right_index)
                .expect("right publication sweep ordinal belongs to its chord table");
            let translated = shifted_segment(right.nearest, shift, period);
            let radius = *right
                .image_radii
                .get(shift_index)
                .expect("each publication image has a swept radius");
            if !segments_within_chebyshev(
                left.nearest,
                translated,
                proximity_limit(left.radius, radius),
            ) {
                return Ok(ControlFlow::<std::convert::Infallible>::Continue(()));
            }
            let pair = if left_index < right_index {
                QPublicationPair {
                    left: QPublicationChordId::Published(left_index),
                    right: QPublicationChordId::Published(right_index),
                    right_shift: shift,
                }
            } else {
                QPublicationPair {
                    left: QPublicationChordId::Published(right_index),
                    right: QPublicationChordId::Published(left_index),
                    right_shift: shift.checked_neg().expect("periodic shifts are unit steps"),
                }
            };
            pairs.push(pair);
            Ok(ControlFlow::<std::convert::Infallible>::Continue(()))
        },
    )?;
    try_sort_by_with_work(&mut pairs, budget, |left, right, _work| Ok(left.cmp(right)))?;
    budget
        .charge_into_error(u64::try_from(pairs.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    pairs.dedup();
    Ok(pairs)
}
pub(in crate::geometry::topology::q_embedding) fn compare_variable_nodes(
    topology: &ExactGeographicTopology,
    publication_nodes: &EmissionNodeRegistry,
    left: QVariableNode,
    right: QVariableNode,
) -> Result<Ordering> {
    Ok(match (left, right) {
        (QVariableNode::Source(left), QVariableNode::Source(right)) => {
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
            compare_qnode_keys(left_key, right_key)?.then_with(|| left.cmp(&right))
        },
        (QVariableNode::Source(_), QVariableNode::Published(_)) => Ordering::Less,
        (QVariableNode::Published(_), QVariableNode::Source(_)) => Ordering::Greater,
        (QVariableNode::Published(left_id), QVariableNode::Published(right_id)) => {
            let left = publication_nodes
                .nodes()
                .get(left_id.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q publication class node is unknown".into())
                })?;
            let right = publication_nodes
                .nodes()
                .get(right_id.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q publication class node is unknown".into())
                })?;
            match (&left.authority, &right.authority) {
                (NodeAuthority::Computed(left_key), NodeAuthority::Computed(right_key)) => {
                    compare_quotient_keys(left_key, right_key)?.then_with(|| left_id.cmp(&right_id))
                },
                _ => {
                    return Err(GeometryErrorKind::Invalid(
                        "source identity entered the publication variable class".into(),
                    )
                    .into());
                },
            }
        },
    })
}
pub(in crate::geometry::topology::q_embedding) fn publication_pair_variables(
    pair: QPublicationPair,
    source_chords: &[QChord],
    publication_chords: &[QPublishedChord],
    budget: &mut impl Work,
) -> Result<Vec<QVariableNode>> {
    let mut variables = Vec::new();
    for chord_id in [pair.left, pair.right] {
        budget.charge_into_error(1)?;
        match chord_id {
            QPublicationChordId::Source(index) => {
                let chord = source_chords.get(index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q publication source chord is unknown".into())
                })?;
                for &node in &chord.dependencies {
                    budget.charge_into_error(1)?;
                    variables.push(QVariableNode::Source(node));
                }
            },
            QPublicationChordId::Published(index) => {
                let chord = publication_chords.get(index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q publication chord is unknown".into())
                })?;
                for &node in &chord.dependencies {
                    budget.charge_into_error(1)?;
                    variables.push(QVariableNode::Source(node));
                }
                for node in chord.nodes {
                    budget.charge_into_error(1)?;
                    variables.push(match node {
                        QGraphNode::Source(node) => QVariableNode::Source(node),
                        QGraphNode::Published(node) => QVariableNode::Published(node),
                    });
                }
            },
        }
    }
    try_sort_by_with_work(&mut variables, budget, |left, right, _work| {
        Ok(left.cmp(right))
    })?;
    budget.charge_into_error(
        u64::try_from(variables.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    variables.dedup();
    Ok(variables)
}
pub(in crate::geometry::topology::q_embedding) fn prepared_variable_location(
    variable: QVariableNode,
    node_location: &[Option<QNodeLocation>],
    publication_location: &[Option<QNodeLocation>],
    source_movable_axes: &[[bool; 2]],
    publication_variables: &[EmissionNodeId],
    budget: &mut impl Work,
) -> Result<Option<QNodeLocation>> {
    match variable {
        QVariableNode::Source(node) => {
            let location = node_location.get(node.0 as usize).copied().ok_or_else(|| {
                GeometryErrorKind::Invalid("Q predicate dependency node is unknown".into())
            })?;
            let movable_axes = source_movable_axes.get(node.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q predicate dependency has no domain row".into())
            })?;
            if location.is_some() != movable_axes.iter().any(|&movable| movable) {
                return Err(GeometryErrorKind::Invalid(
                    "Q prepared source variables disagree with their admitted domains".into(),
                )
                .into());
            }
            Ok(location)
        },
        QVariableNode::Published(node) => {
            budget.charge_into_error(1)?;
            let location = publication_location
                .get(node.0 as usize)
                .copied()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "Q predicate dependency publication node is unknown".into(),
                    )
                })?;
            let is_variable = publication_variables.binary_search(&node).is_ok();
            if location.is_some() != is_variable {
                return Err(GeometryErrorKind::Invalid(
                    "Q prepared publication variables disagree with their admitted domains".into(),
                )
                .into());
            }
            Ok(location)
        },
    }
}
pub(in crate::geometry::topology::q_embedding) fn variable_class_rank(
    variables: impl IntoIterator<Item = QVariableNode>,
    node_location: &[Option<QNodeLocation>],
    publication_location: &[Option<QNodeLocation>],
    source_movable_axes: &[[bool; 2]],
    publication_variables: &[EmissionNodeId],
    budget: &mut impl Work,
) -> Result<Option<QNodeLocation>> {
    let mut location: Option<QNodeLocation> = None;
    for variable in variables {
        budget.charge_into_error(1)?;
        let candidate = prepared_variable_location(
            variable,
            node_location,
            publication_location,
            source_movable_axes,
            publication_variables,
            budget,
        )?;
        if let Some((class, rank)) = candidate {
            if location.is_some_and(|(previous, _)| previous != class) {
                return Err(GeometryErrorKind::Invalid(
                    "Q local certificate crosses variable classes".into(),
                )
                .into());
            }
            location = Some((
                class,
                location.map_or(rank, |(_, previous_rank)| previous_rank.max(rank)),
            ));
        }
    }
    Ok(location)
}
