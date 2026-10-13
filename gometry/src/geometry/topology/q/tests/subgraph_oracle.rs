use super::*;

#[cfg(test)]
pub(in crate::geometry::topology::q) fn b3_subgraph_chart(
    frame: GeographicFrame,
    q: &ExactGeographicQ,
    span_indices: &[usize],
    point_nodes: &[QNodeId],
    emitted: Option<(&[crate::geometry::XY], &[Option<f64>])>,
) -> Result<CutChart> {
    let period = ExactRational::from_f64(frame.longitude().period());
    let cut_east = q.cut.try_add(&period)?;
    let mut selected_spans = span_indices.to_vec();
    selected_spans.sort_unstable();
    selected_spans.dedup();

    let mut port_nodes = Vec::new();
    for &span_index in &selected_spans {
        let span = q.spans.get(span_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q subgraph references an unknown span".into())
        })?;
        for port_id in span.ports {
            let port = q.ports.get(port_id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q subgraph span has an unknown port".into())
            })?;
            port_nodes.push((port.node, port_id));
        }
    }
    for &node_id in point_nodes {
        let node = q.nodes.get(node_id.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q subgraph references an unknown point node".into())
        })?;
        for &port_id in q.node_ports.get(node.ports.clone()).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q subgraph point node has an invalid port run".into())
        })? {
            port_nodes.push((node_id, port_id));
        }
    }
    port_nodes.sort_unstable();
    port_nodes.dedup();

    let mut port_points = vec![None; q.ports.len()];
    for &(_, port_id) in &port_nodes {
        let port = q
            .ports
            .get(port_id.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q subgraph has an unknown port".into()))?;
        let point = if port.cut {
            if emitted.is_some() {
                return Err(GeometryErrorKind::Invalid(
                    "B3 spike does not cover selected cut ports".into(),
                )
                .into());
            }
            port.point.clone()
        } else if let Some((node_coordinates, pole_port_longitude)) = emitted {
            b3_assigned_port_point(
                frame,
                q,
                port_id,
                node_coordinates,
                pole_port_longitude,
                &cut_east,
            )?
        } else {
            port.point.clone()
        };
        *port_points
            .get_mut(port_id.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q subgraph port is unknown".into()))? =
            Some(point);
    }

    let mut selected_nodes = port_nodes.iter().map(|(node, _)| *node).collect::<Vec<_>>();
    selected_nodes.sort_unstable();
    selected_nodes.dedup();
    try_sort_by(&mut port_nodes, |left, right| {
        let left_point = port_points
            .get(left.1.0 as usize)
            .and_then(Option::as_ref)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q subgraph port has no chart point".into())
            })?;
        let right_point = port_points
            .get(right.1.0 as usize)
            .and_then(Option::as_ref)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q subgraph port has no chart point".into())
            })?;
        let point_order = left_point.compare_lex(right_point)?;
        if point_order.is_eq() {
            let left_key = &q
                .nodes
                .get(left.0.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q subgraph node is unknown".into()))?
                .key;
            let right_key = &q
                .nodes
                .get(right.0.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q subgraph node is unknown".into()))?
                .key;
            compare_qnode_keys(left_key, right_key)
        } else {
            Ok(point_order)
        }
    })?;
    port_nodes.dedup_by_key(|(_, port)| *port);

    let mut nodes = Vec::with_capacity(selected_nodes.len());
    let mut ports = Vec::with_capacity(port_nodes.len());
    let mut node_ports = Vec::with_capacity(port_nodes.len());
    let mut node_map = vec![None; q.nodes.len()];
    let mut port_map = vec![None; q.ports.len()];
    for old_node in &selected_nodes {
        let old_node_data = q
            .nodes
            .get(old_node.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q subgraph has an unknown node".into()))?;
        let node_id = QNodeId(u32::try_from(nodes.len()).map_err(|_| ResourceError::SizeOverflow)?);
        *node_map.get_mut(old_node.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q subgraph node has an invalid ordinal".into())
        })? = Some(node_id);
        nodes.push(QNode {
            id: node_id,
            key: old_node_data.key.clone(),
            pole: old_node_data.pole,
            ports: 0..0,
        });
    }

    for &(old_node, old_port_id) in &port_nodes {
        let old_port = q
            .ports
            .get(old_port_id.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q subgraph has an unknown port".into()))?;
        let node_id = node_map
            .get(old_node.0 as usize)
            .and_then(|node| *node)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q subgraph port node is absent".into()))?;
        let port_id = QPortId(u32::try_from(ports.len()).map_err(|_| ResourceError::SizeOverflow)?);
        let point = port_points
            .get(old_port_id.0 as usize)
            .and_then(Option::as_ref)
            .cloned()
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q subgraph port has no chart point".into())
            })?;
        ports.push(QPort {
            id: port_id,
            point,
            node: node_id,
            cut: old_port.cut,
        });
        *port_map.get_mut(old_port_id.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q subgraph port has an invalid ordinal".into())
        })? = Some(port_id);
    }
    for &old_node in &selected_nodes {
        let node_id = node_map
            .get(old_node.0 as usize)
            .and_then(|node| *node)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q subgraph node is absent".into()))?;
        let old_node_data = q
            .nodes
            .get(old_node.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q subgraph node is unknown".into()))?;
        let start = node_ports.len();
        for &old_port_id in q
            .node_ports
            .get(old_node_data.ports.clone())
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q subgraph node has an invalid port run".into())
            })?
        {
            if let Some(port_id) = port_map.get(old_port_id.0 as usize).and_then(|port| *port) {
                node_ports.push(port_id);
            }
        }
        node_ports[start..].sort_unstable();
        let end = node_ports.len();
        nodes
            .get_mut(node_id.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q subgraph node is absent".into()))?
            .ports = start..end;
    }

    let mut spans = Vec::with_capacity(selected_spans.len());
    for old_span_index in selected_spans {
        let old_span = q.spans.get(old_span_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q subgraph references an unknown span".into())
        })?;
        let ports = [
            *port_map
                .get(old_span.ports[0].0 as usize)
                .and_then(|port| *port)
                .as_ref()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q subgraph start port is absent".into())
                })?,
            *port_map
                .get(old_span.ports[1].0 as usize)
                .and_then(|port| *port)
                .as_ref()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q subgraph end port is absent".into())
                })?,
        ];
        let nodes = [
            *node_map
                .get(old_span.nodes[0].0 as usize)
                .and_then(|node| *node)
                .as_ref()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q subgraph start node is absent".into())
                })?,
            *node_map
                .get(old_span.nodes[1].0 as usize)
                .and_then(|node| *node)
                .as_ref()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q subgraph end node is absent".into())
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

#[cfg(test)]
pub(in crate::geometry::topology::q) fn b3_assigned_port_point(
    frame: GeographicFrame,
    q: &ExactGeographicQ,
    port_id: QPortId,
    node_coordinates: &[crate::geometry::XY],
    pole_port_longitude: &[Option<f64>],
    cut_east: &ExactRational,
) -> Result<ExactChartPoint> {
    let port = q
        .ports
        .get(port_id.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q emitted port is unknown".into()))?;
    let node_coordinates = node_coordinates
        .get(port.node.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q emitted node has no coordinate".into()))?;
    let longitude = pole_port_longitude
        .get(port_id.0 as usize)
        .copied()
        .flatten()
        .unwrap_or(node_coordinates.x);
    let axis = frame.longitude();
    axis.canonicalize(longitude)
        .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
    let latitude = crate::geographic::Latitude::try_new(frame, node_coordinates.y)
        .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
    let exact_pole = port
        .point
        .y
        .try_cmp(&ExactRational::from_f64(frame.pole()))?
        .is_eq()
        || port
            .point
            .y
            .try_cmp(&ExactRational::from_f64(-frame.pole()))?
            .is_eq();
    if matches!(
        latitude.identity(frame),
        crate::geographic::SeamIdentity::Pole
    ) != exact_pole
    {
        return Err(GeometryErrorKind::Invalid("Q assignment changes pole identity".into()).into());
    }
    let half = ExactRational::from_f64(axis.half_period());
    let negative_half = ExactRational::zero().try_subtract(&half)?;
    let period = ExactRational::from_f64(axis.period());
    let exact_canonical = if port.point.x.try_cmp(&half)?.is_ge() {
        port.point.x.try_subtract(&period)?
    } else if port.point.x.try_cmp(&negative_half)?.is_lt() {
        port.point.x.try_add(&period)?
    } else {
        port.point.x.clone()
    };
    let exact_seam =
        exact_canonical.try_cmp(&half)?.is_eq() || exact_canonical.try_cmp(&negative_half)?.is_eq();
    let candidate_longitude = axis
        .canonicalize(longitude)
        .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
    if matches!(
        candidate_longitude.identity(axis),
        crate::geographic::SeamIdentity::Seam
    ) && !exact_seam
    {
        return Err(
            GeometryErrorKind::Invalid("Q assignment newly takes seam identity".into()).into(),
        );
    }
    let center = ExactRational::from_f64(longitude);
    let candidates = [
        center.try_subtract(&period)?,
        center.clone(),
        center.try_add(&period)?,
    ];
    let one = ExactRational::one();
    let zero = ExactRational::zero();
    let mut nearest: Option<(ExactRational, ExactRational)> = None;
    for candidate in candidates {
        if candidate.try_cmp(&q.cut)?.is_lt() || candidate.try_cmp(cut_east)?.is_ge() {
            continue;
        }
        let delta = candidate.try_subtract(&port.point.x)?;
        let distance = if delta.try_cmp(&zero)?.is_lt() {
            zero.try_subtract(&delta)?
        } else {
            delta
        };
        let replace = if let Some((previous_x, previous_distance)) = &nearest {
            match distance.try_cmp(previous_distance)? {
                Ordering::Less => true,
                Ordering::Equal => candidate.try_cmp(previous_x)?.is_lt(),
                Ordering::Greater => false,
            }
        } else {
            true
        };
        if replace {
            nearest = Some((candidate, distance));
        }
    }
    let (x, x_distance) = nearest
        .ok_or_else(|| GeometryErrorKind::Invalid("Q assignment has no chart longitude".into()))?;
    let y = ExactRational::from_f64(node_coordinates.y);
    let y_delta = y.try_subtract(&port.point.y)?;
    let y_distance = if y_delta.try_cmp(&zero)?.is_lt() {
        zero.try_subtract(&y_delta)?
    } else {
        y_delta
    };
    if x_distance.try_cmp(&one)?.is_ge() || y_distance.try_cmp(&one)?.is_ge() {
        return Err(GeometryErrorKind::Invalid(
            "Q assignment exceeds the chart lift budget".into(),
        )
        .into());
    }
    Ok(ExactChartPoint::from_rationals(x, y))
}

#[cfg(test)]
pub(in crate::geometry::topology) struct B3SubgraphFaceStructure {
    pub(in crate::geometry::topology) global: GlobalFaceStructure,
    pub(in crate::geometry::topology) outgoing: Vec<QDartId>,
    pub(in crate::geometry::topology) outgoing_offsets: Vec<usize>,
}

#[cfg(test)]
pub(in crate::geometry::topology) fn b3_subgraph_face_structure(
    frame: GeographicFrame,
    q: &ExactGeographicQ,
    span_indices: &[usize],
    point_nodes: &[QNodeId],
    emitted: Option<(&[crate::geometry::XY], &[Option<f64>])>,
) -> Result<B3SubgraphFaceStructure> {
    let chart = b3_subgraph_chart(frame, q, span_indices, point_nodes, emitted)?;
    let topology = QuotientFaces::build(frame, chart)?;
    Ok(B3SubgraphFaceStructure {
        global: global_face_structure(frame, &topology)?,
        outgoing: topology.outgoing,
        outgoing_offsets: topology.outgoing_offsets,
    })
}
