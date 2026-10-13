use super::{
    ChartSpan, ChartSpanId, CutChart, ExactGeographicTopology, ExactRational, GeometryErrorKind,
    GlobalFaceId, GlobalFaceStructure, QCandidate, QCoordinates, QDartId, QEmbeddingEmission,
    QNode, QNodeId, QNodeKey, QPort, QPortId, QuotientFaces, ResourceError, Result,
    align_native_chart_lift, assigned_port_point, compare_qnode_keys, global_face_structure,
    reconstruct_cut_ports, try_sort_by_with_work,
};
use crate::work::Work;

pub(in crate::geometry::topology) struct QFaceStructure {
    pub(in crate::geometry::topology) global: GlobalFaceStructure,
    pub(in crate::geometry::topology) outgoing: Vec<QDartId>,
    pub(in crate::geometry::topology) outgoing_offsets: Vec<usize>,
    pub(in crate::geometry::topology) outgoing_nodes: Vec<QNodeId>,
    pub(in crate::geometry::topology) span_nodes: Vec<[QNodeId; 2]>,
    pub(in crate::geometry::topology) component_nodes: Vec<Vec<QNodeId>>,
    pub(in crate::geometry::topology) cut_interval_nodes: Vec<[Option<QNodeId>; 2]>,
    pub(in crate::geometry::topology) isolated_nodes: Vec<Option<QNodeId>>,
}
impl QFaceStructure {
    pub(in crate::geometry::topology) fn build(
        source_topology: &ExactGeographicTopology,
        emission: &QEmbeddingEmission<'_, '_>,
        chart: CutChart,
    ) -> Result<Self> {
        let frame = source_topology.sources.frame;
        let q = &source_topology.q;
        let topology = QuotientFaces::build(frame, chart)?;
        let global = global_face_structure(frame, &topology)?;
        let span_nodes = emission
            .spans
            .iter()
            .map(|&span_index| {
                q.spans
                    .get(span_index)
                    .map(|span| span.nodes)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q emitted face span is unknown".into()).into()
                    })
            })
            .collect::<Result<Vec<_>>>()?;
        let mut component_nodes = vec![Vec::new(); global.component_hosts.len()];
        for (local_node, &component) in topology.node_components.iter().enumerate() {
            let original = *emission.selected_nodes.get(local_node).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q rebuilt face node lost its emission mapping".into())
            })?;
            component_nodes
                .get_mut(component.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q rebuilt face component is unknown".into())
                })?
                .push(original);
        }
        let map_cut_node = |node: Option<QNodeId>| -> Result<Option<QNodeId>> {
            Ok(node
                .map(|node| {
                    emission
                        .selected_nodes
                        .get(node.0 as usize)
                        .copied()
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "Q cut interval node is outside the emitted subgraph".into(),
                            )
                        })
                })
                .transpose()?)
        };
        let cut_interval_nodes = topology
            .cut_interval_boundary_nodes()
            .into_iter()
            .map(|[start, end]| Ok([map_cut_node(start)?, map_cut_node(end)?]))
            .collect::<Result<Vec<_>>>()?;
        let isolated_nodes = global
            .source_node_faces
            .iter()
            .enumerate()
            .map(|(local, face)| {
                if face.is_some() {
                    emission
                        .selected_nodes
                        .get(local)
                        .copied()
                        .map(Some)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "Q isolated face node is outside the emitted subgraph".into(),
                            )
                            .into()
                        })
                } else {
                    Ok(None)
                }
            })
            .collect::<Result<Vec<_>>>()?;
        Ok(Self {
            global,
            outgoing: topology.outgoing,
            outgoing_offsets: topology.outgoing_offsets,
            outgoing_nodes: emission.selected_nodes.clone(),
            span_nodes,
            component_nodes,
            cut_interval_nodes,
            isolated_nodes,
        })
    }
}
pub(in crate::geometry::topology::q_embedding) fn rotation_row_matches(
    exact: &[QDartId],
    emitted: &[QDartId],
) -> bool {
    if exact.len() != emitted.len() {
        return false;
    }
    if exact.is_empty() {
        return true;
    }
    let Some(rotation) = emitted.iter().position(|dart| exact.first() == Some(dart)) else {
        return false;
    };
    for (index, dart) in exact.iter().enumerate() {
        let Some(offset) = rotation.checked_add(index) else {
            return false;
        };
        let Some(emitted_index) = offset.checked_rem(emitted.len()) else {
            return false;
        };
        if emitted.get(emitted_index) != Some(dart) {
            return false;
        }
    }
    true
}
pub(in crate::geometry::topology::q_embedding) fn unique_nodes(
    nodes: impl IntoIterator<Item = QNodeId>,
) -> Vec<QNodeId> {
    let mut nodes = nodes.into_iter().collect::<Vec<_>>();
    nodes.sort_unstable();
    nodes.dedup();
    nodes
}
pub(in crate::geometry::topology) fn face_structure_mismatch_nodes(
    exact: &QFaceStructure,
    emitted: &QFaceStructure,
    fallback: &[QNodeId],
) -> Option<Vec<QNodeId>> {
    fn bind(
        exact: GlobalFaceId,
        emitted: GlobalFaceId,
        exact_to_emitted: &mut [Option<GlobalFaceId>],
        emitted_to_exact: &mut [Option<GlobalFaceId>],
    ) -> bool {
        let Some(exact_slot) = exact_to_emitted.get_mut(exact.0 as usize) else {
            return false;
        };
        let Some(emitted_slot) = emitted_to_exact.get_mut(emitted.0 as usize) else {
            return false;
        };
        if exact_slot.is_some_and(|previous| previous != emitted)
            || emitted_slot.is_some_and(|previous| previous != exact)
        {
            return false;
        }
        *exact_slot = Some(emitted);
        *emitted_slot = Some(exact);
        true
    }

    let exact_global = &exact.global;
    let emitted_global = &emitted.global;
    if exact_global.count != emitted_global.count
        || exact_global.span_global_faces.len() != emitted_global.span_global_faces.len()
        || exact_global.component_hosts.len() != emitted_global.component_hosts.len()
        || exact_global.cut_interval_faces.len() != emitted_global.cut_interval_faces.len()
        || exact_global.source_node_faces.len() != emitted_global.source_node_faces.len()
        || exact.outgoing_offsets != emitted.outgoing_offsets
        || exact.outgoing_nodes != emitted.outgoing_nodes
    {
        return Some(fallback.to_vec());
    }
    for (row_index, row_offsets) in exact.outgoing_offsets.windows(2).enumerate() {
        let (&start, &end) = (row_offsets.first()?, row_offsets.get(1)?);
        let exact_row = exact.outgoing.get(start..end)?;
        let emitted_row = emitted.outgoing.get(start..end)?;
        if !rotation_row_matches(exact_row, emitted_row) {
            return exact
                .outgoing_nodes
                .get(row_index)
                .copied()
                .map(|node| vec![node]);
        }
    }

    let mut exact_to_emitted = vec![None; exact_global.count];
    let mut emitted_to_exact = vec![None; emitted_global.count];
    if exact_global.count == 1
        && !bind(
            GlobalFaceId(0),
            GlobalFaceId(0),
            &mut exact_to_emitted,
            &mut emitted_to_exact,
        )
    {
        return Some(fallback.to_vec());
    }
    for (span, (exact_faces, emitted_faces)) in exact_global
        .span_global_faces
        .iter()
        .zip(&emitted_global.span_global_faces)
        .enumerate()
    {
        match (exact_faces, emitted_faces) {
            (None, None) => {},
            (Some(exact_faces), Some(emitted_faces)) => {
                for (exact_face, emitted_face) in exact_faces.iter().zip(emitted_faces) {
                    if !bind(
                        *exact_face,
                        *emitted_face,
                        &mut exact_to_emitted,
                        &mut emitted_to_exact,
                    ) {
                        return Some(
                            exact
                                .span_nodes
                                .get(span)
                                .copied()
                                .into_iter()
                                .flatten()
                                .collect(),
                        );
                    }
                }
            },
            _ => {
                return Some(
                    exact
                        .span_nodes
                        .get(span)
                        .copied()
                        .into_iter()
                        .flatten()
                        .collect(),
                );
            },
        }
    }
    for (component, (exact_face, emitted_face)) in exact_global
        .component_hosts
        .iter()
        .zip(&emitted_global.component_hosts)
        .enumerate()
    {
        match (exact_face, emitted_face) {
            (None, None) => {},
            (Some(exact_face), Some(emitted_face))
                if bind(
                    *exact_face,
                    *emitted_face,
                    &mut exact_to_emitted,
                    &mut emitted_to_exact,
                ) => {},
            _ => {
                return Some(
                    exact
                        .component_nodes
                        .get(component)
                        .cloned()
                        .unwrap_or_else(|| fallback.to_vec()),
                );
            },
        }
    }
    for (interval, (&exact_face, &emitted_face)) in exact_global
        .cut_interval_faces
        .iter()
        .zip(&emitted_global.cut_interval_faces)
        .enumerate()
    {
        if !bind(
            exact_face,
            emitted_face,
            &mut exact_to_emitted,
            &mut emitted_to_exact,
        ) {
            let nodes = exact
                .cut_interval_nodes
                .get(interval)
                .into_iter()
                .flatten()
                .chain(
                    emitted
                        .cut_interval_nodes
                        .get(interval)
                        .into_iter()
                        .flatten(),
                )
                .flatten()
                .copied();
            let nodes = unique_nodes(nodes);
            return Some(if nodes.is_empty() {
                fallback.to_vec()
            } else {
                nodes
            });
        }
    }
    for (node, (exact_face, emitted_face)) in exact_global
        .source_node_faces
        .iter()
        .zip(&emitted_global.source_node_faces)
        .enumerate()
    {
        match (exact_face, emitted_face) {
            (None, None) => {},
            (Some(exact_face), Some(emitted_face))
                if bind(
                    *exact_face,
                    *emitted_face,
                    &mut exact_to_emitted,
                    &mut emitted_to_exact,
                ) => {},
            _ => {
                return exact
                    .isolated_nodes
                    .get(node)
                    .copied()
                    .flatten()
                    .map(|node| vec![node])
                    .or_else(|| Some(fallback.to_vec()));
            },
        }
    }
    (exact_to_emitted.iter().any(Option::is_none) || emitted_to_exact.iter().any(Option::is_none))
        .then(|| fallback.to_vec())
}
pub(in crate::geometry::topology) fn subgraph_chart(
    topology: &ExactGeographicTopology,
    emission: &QEmbeddingEmission<'_, '_>,
    nearest_coordinates: &QCoordinates,
    assigned_coordinates: Option<&QCoordinates>,
    work: &mut impl Work,
) -> Result<CutChart> {
    let frame = topology.sources.frame;
    let q = &topology.q;
    let period = ExactRational::from_f64(frame.longitude().period());
    let cut_east = q.cut.try_add_with_work(&period, work)?;
    let selected_spans = &emission.spans;
    let selected_nodes = &emission.selected_nodes;
    let mut port_nodes = emission.selected_ports.clone();

    let mut port_points = vec![None; q.ports.len()];
    for &(_, port_id) in &port_nodes {
        let port = q
            .ports
            .get(port_id.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q selected port is unknown".into()))?;
        let point = if let Some(assigned_coordinates) = assigned_coordinates {
            if port.cut {
                port.point.clone()
            } else {
                let nearest_xy = nearest_coordinates.xy(port.node)?;
                let candidate_xy = assigned_coordinates.xy(port.node)?;
                let pole_port_longitude = assigned_coordinates
                    .pole_port_longitude
                    .get(port_id.0 as usize)
                    .copied()
                    .flatten();
                let pole_port_periods = assigned_coordinates
                    .pole_port_periods
                    .get(port_id.0 as usize)
                    .copied()
                    .flatten();
                let periods = *assigned_coordinates
                    .longitude_periods
                    .get(port.node.0 as usize)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q candidate node has no longitude lift".into())
                    })?;
                let assigned = assigned_port_point(
                    frame,
                    q,
                    port_id,
                    nearest_xy,
                    QCandidate {
                        native: candidate_xy,
                        periods,
                    },
                    pole_port_longitude,
                    pole_port_periods,
                    &cut_east,
                    work,
                )?
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "Q candidate does not lift into its exact chart port".into(),
                    )
                })?;
                align_native_chart_lift(frame, &assigned, &port.point, work)?
            }
        } else {
            port.point.clone()
        };
        *port_points.get_mut(port_id.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q selected port ordinal is invalid".into())
        })? = Some(point);
    }

    let cut_latitudes = if assigned_coordinates.is_some() {
        reconstruct_cut_ports(
            frame,
            q,
            &emission.cut_nodes,
            &mut port_points,
            &cut_east,
            work,
        )?
    } else {
        vec![None; q.nodes.len()]
    };

    try_sort_by_with_work(&mut port_nodes, work, |left, right, work| {
        work.charge_into_error(1)?;
        let left_point = port_points
            .get(left.1.0 as usize)
            .and_then(Option::as_ref)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q selected port has no chart point".into())
            })?;
        let right_point = port_points
            .get(right.1.0 as usize)
            .and_then(Option::as_ref)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q selected port has no chart point".into())
            })?;
        let point_order = left_point.compare_lex(right_point)?;
        if point_order.is_eq() {
            let left_key = &q
                .nodes
                .get(left.0.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q selected node is unknown".into()))?
                .key;
            let right_key = &q
                .nodes
                .get(right.0.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q selected node is unknown".into()))?
                .key;
            let key_order = compare_qnode_keys(left_key, right_key)?;
            Ok(if key_order.is_eq() {
                left.1.cmp(&right.1)
            } else {
                key_order
            })
        } else {
            Ok(point_order)
        }
    })?;

    let mut nodes = Vec::with_capacity(selected_nodes.len());
    let mut ports = Vec::with_capacity(port_nodes.len());
    let mut node_ports = Vec::with_capacity(port_nodes.len());
    let mut node_map = vec![None; q.nodes.len()];
    let mut port_map = vec![None; q.ports.len()];
    for &old_node in selected_nodes {
        let old_node_data = q
            .nodes
            .get(old_node.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q selected node is unknown".into()))?;
        let node_id = QNodeId(u32::try_from(nodes.len()).map_err(|_| ResourceError::SizeOverflow)?);
        *node_map.get_mut(old_node.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q selected node ordinal is invalid".into())
        })? = Some(node_id);
        let key = match cut_latitudes
            .get(old_node.0 as usize)
            .and_then(Option::as_ref)
        {
            Some(latitude) if matches!(&old_node_data.key, QNodeKey::Cut(_)) => {
                QNodeKey::Cut(latitude.clone())
            },
            _ => old_node_data.key.clone(),
        };
        nodes.push(QNode {
            id: node_id,
            key,
            pole: old_node_data.pole,
            ports: 0..0,
        });
    }

    for &(old_node, old_port_id) in &port_nodes {
        let old_port = q
            .ports
            .get(old_port_id.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q selected port is unknown".into()))?;
        let node_id = node_map
            .get(old_node.0 as usize)
            .and_then(|node| *node)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q selected port node is absent".into()))?;
        let port_id = QPortId(u32::try_from(ports.len()).map_err(|_| ResourceError::SizeOverflow)?);
        let point = port_points
            .get(old_port_id.0 as usize)
            .and_then(Option::as_ref)
            .cloned()
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q selected port has no chart coordinate".into())
            })?;
        ports.push(QPort {
            id: port_id,
            point,
            node: node_id,
            cut: old_port.cut,
        });
        *port_map.get_mut(old_port_id.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q selected port ordinal is invalid".into())
        })? = Some(port_id);
    }
    for &old_node in selected_nodes {
        let node_id = node_map
            .get(old_node.0 as usize)
            .and_then(|node| *node)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q selected node is absent".into()))?;
        let old_node_data = q
            .nodes
            .get(old_node.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q selected node is unknown".into()))?;
        let start = node_ports.len();
        for &old_port_id in q
            .node_ports
            .get(old_node_data.ports.clone())
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q selected node has an invalid port run".into())
            })?
        {
            if let Some(port_id) = port_map.get(old_port_id.0 as usize).and_then(|port| *port) {
                node_ports.push(port_id);
            }
        }
        try_sort_by_with_work(
            node_ports.get_mut(start..).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q selected node port run is invalid".into())
            })?,
            work,
            |left, right, work| {
                work.charge_into_error(1)?;
                Ok(left.cmp(right))
            },
        )?;
        let end = node_ports.len();
        nodes
            .get_mut(node_id.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q selected node is absent".into()))?
            .ports = start..end;
    }

    let mut spans = Vec::with_capacity(selected_spans.len());
    for &old_span_index in selected_spans {
        let old_span = q.spans.get(old_span_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q selected emission has an unknown span".into())
        })?;
        let ports = [
            *port_map
                .get(old_span.ports[0].0 as usize)
                .and_then(|port| *port)
                .as_ref()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q selected span start port is absent".into())
                })?,
            *port_map
                .get(old_span.ports[1].0 as usize)
                .and_then(|port| *port)
                .as_ref()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q selected span end port is absent".into())
                })?,
        ];
        let nodes = [
            *node_map
                .get(old_span.nodes[0].0 as usize)
                .and_then(|node| *node)
                .as_ref()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q selected span start node is absent".into())
                })?,
            *node_map
                .get(old_span.nodes[1].0 as usize)
                .and_then(|node| *node)
                .as_ref()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q selected span end node is absent".into())
                })?,
        ];
        spans.push(ChartSpan {
            id: ChartSpanId(u32::try_from(spans.len()).map_err(|_| ResourceError::SizeOverflow)?),
            ports,
            nodes,
            owners: Box::default(),
        });
    }
    Ok(CutChart {
        cut: q.cut.clone(),
        cut_east,
        ports,
        nodes,
        node_ports,
        spans,
        source_edge_ports: Vec::new(),
        source_point_ports: Vec::new(),
    })
}
