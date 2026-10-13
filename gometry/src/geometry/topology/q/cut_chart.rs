use super::{
    ChartPole, ChartSpan, ChartSpanId, CutChart, ExactChartPoint, ExactGeographicChartInventory,
    ExactQuotientKey, ExactRational, ExactSourceRange, GeographicFrame, GeometryErrorKind,
    NodedExactChart, Ordering, PortCandidate, QNode, QNodeId, QNodeKey, QPort, QPortId, QSpanOwner,
    QuotientNodeId, ResourceError, Result, SpanOccurrence, source_ring_transition, try_sort_by,
};
use crate::work::Work;

pub(in crate::geometry::topology::q) fn lifted_point(
    point: &ExactChartPoint,
    cut: &ExactRational,
    period: &ExactRational,
) -> Result<ExactChartPoint> {
    let x = if point.x.try_cmp(cut)?.is_lt() {
        point.x.try_add(period)?
    } else {
        point.x.clone()
    };
    Ok(ExactChartPoint::from_rationals(x, point.y.clone()))
}

pub(in crate::geometry::topology::q) fn cut_node_key(
    frame: GeographicFrame,
    chart: &NodedExactChart,
    latitude: &ExactRational,
) -> Result<QNodeKey> {
    let north = ExactRational::from_f64(frame.chart_pole_latitude(ChartPole::North));
    let south = ExactRational::from_f64(frame.chart_pole_latitude(ChartPole::South));
    if latitude.try_cmp(&north)?.is_eq() {
        return chart
            .nodes
            .iter()
            .find(|node| matches!(&node.key, ExactQuotientKey::NorthPole))
            .map(|node| QNodeKey::Source(node.id))
            .ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "a cut span reaches a pole without a quotient pole node".into(),
                )
                .into()
            });
    }
    if latitude.try_cmp(&south)?.is_eq() {
        return chart
            .nodes
            .iter()
            .find(|node| matches!(&node.key, ExactQuotientKey::SouthPole))
            .map(|node| QNodeKey::Source(node.id))
            .ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "a cut span reaches a pole without a quotient pole node".into(),
                )
                .into()
            });
    }
    Ok(QNodeKey::Cut(latitude.clone()))
}

pub(in crate::geometry::topology::q) fn qnode_pole(
    chart: &NodedExactChart,
    key: &QNodeKey,
) -> Result<Option<ChartPole>> {
    let QNodeKey::Source(id) = key else {
        return Ok(None);
    };
    let node = chart.nodes.get(id.0 as usize).ok_or_else(|| {
        GeometryErrorKind::Invalid("a Q node references an unknown chart node".into())
    })?;
    Ok(match &node.key {
        ExactQuotientKey::NorthPole => Some(ChartPole::North),
        ExactQuotientKey::SouthPole => Some(ChartPole::South),
        ExactQuotientKey::Coordinate(_) | ExactQuotientKey::Seam(_) => None,
    })
}

pub(in crate::geometry::topology::q) fn add_cut_candidate(
    candidates: &mut Vec<PortCandidate>,
    point: &ExactChartPoint,
    cut: &ExactRational,
    period: &ExactRational,
    node: QNodeKey,
) -> Result<()> {
    let left = ExactChartPoint::from_rationals(cut.clone(), point.y.clone());
    let right = ExactChartPoint::from_rationals(cut.try_add(period)?, point.y.clone());
    candidates.push(PortCandidate {
        point: left,
        node: node.clone(),
        cut: true,
    });
    candidates.push(PortCandidate {
        point: right,
        node,
        cut: true,
    });
    Ok(())
}

pub(in crate::geometry::topology) fn compare_qnode_keys(
    left: &QNodeKey,
    right: &QNodeKey,
) -> Result<Ordering> {
    match (left, right) {
        (QNodeKey::Source(left), QNodeKey::Source(right)) => Ok(left.cmp(right)),
        (QNodeKey::Source(_), QNodeKey::Cut(_)) => Ok(Ordering::Less),
        (QNodeKey::Cut(_), QNodeKey::Source(_)) => Ok(Ordering::Greater),
        (QNodeKey::Cut(left), QNodeKey::Cut(right)) => left.try_cmp(right),
    }
}

pub(in crate::geometry::topology) fn compare_qnode_keys_with_work(
    left: &QNodeKey,
    right: &QNodeKey,
    work: &mut impl Work,
) -> Result<Ordering> {
    match (left, right) {
        (QNodeKey::Source(left), QNodeKey::Source(right)) => Ok(left.cmp(right)),
        (QNodeKey::Source(_), QNodeKey::Cut(_)) => Ok(Ordering::Less),
        (QNodeKey::Cut(_), QNodeKey::Source(_)) => Ok(Ordering::Greater),
        (QNodeKey::Cut(left), QNodeKey::Cut(right)) => left.try_cmp_with_work(right, work),
    }
}

pub(in crate::geometry::topology::q) fn canonical_source_node_key(
    chart: &NodedExactChart,
    node: QuotientNodeId,
) -> Result<QNodeKey> {
    if chart.nodes.get(node.0 as usize).is_none() {
        return Err(GeometryErrorKind::Invalid(
            "a chart port references an unknown quotient node".into(),
        )
        .into());
    }
    Ok(QNodeKey::Source(node))
}

pub(in crate::geometry::topology::q) fn compare_qport_candidates(
    left: &PortCandidate,
    right: &PortCandidate,
) -> Result<Ordering> {
    let point = left.point.compare_lex(&right.point)?;
    Ok(if point.is_eq() {
        compare_qnode_keys(&left.node, &right.node)?
    } else {
        point
    })
}

pub(in crate::geometry::topology::q) fn unique_port_candidates(
    mut candidates: Vec<PortCandidate>,
) -> Result<(Vec<QPort>, Vec<QNodeKey>)> {
    try_sort_by(&mut candidates, compare_qport_candidates)?;
    let mut ports: Vec<QPort> = Vec::with_capacity(candidates.len());
    let mut port_keys = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        if let Some(previous) = ports.last()
            && previous.point.same_position(&candidate.point)?
        {
            let previous_key = port_keys.last().expect("each port has one node key");
            if !compare_qnode_keys(previous_key, &candidate.node)?.is_eq() {
                return Err(GeometryErrorKind::Invalid(
                    "one exact lifted point has two quotient identities".into(),
                )
                .into());
            }
            if !candidate.cut {
                ports
                    .last_mut()
                    .expect("a deduplicated candidate has a previous port")
                    .cut = false;
            }
            continue;
        }
        let id = QPortId(u32::try_from(ports.len()).map_err(|_| ResourceError::SizeOverflow)?);
        ports.push(QPort {
            id,
            point: candidate.point,
            node: QNodeId(0),
            cut: candidate.cut,
        });
        port_keys.push(candidate.node);
    }
    Ok((ports, port_keys))
}

#[expect(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "binary search bounds remain within the sorted exact QPort array"
)]
pub(in crate::geometry::topology::q) fn find_qport_if_present(
    ports: &[QPort],
    point: &ExactChartPoint,
) -> Result<Option<QPortId>> {
    let mut low = 0_usize;
    let mut high = ports.len();
    while low < high {
        let middle = low + (high - low) / 2;
        match ports[middle].point.compare_lex(point)? {
            Ordering::Less => low = middle + 1,
            Ordering::Greater => high = middle,
            Ordering::Equal => return Ok(Some(ports[middle].id)),
        }
    }
    Ok(None)
}

pub(in crate::geometry::topology::q) fn find_qport(
    ports: &[QPort],
    point: &ExactChartPoint,
) -> Result<QPortId> {
    find_qport_if_present(ports, point)?
        .ok_or_else(|| GeometryErrorKind::Invalid("exact Q port was not interned".into()).into())
}

pub(in crate::geometry::topology::q) fn q_port_node(
    ports: &[QPort],
    point: &ExactChartPoint,
) -> Result<Option<QNodeId>> {
    find_qport_if_present(ports, point)?
        .map(|port_id| {
            ports
                .get(port_id.0 as usize)
                .map(|port| port.node)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q query port is unknown".into()).into())
        })
        .transpose()
}

pub(in crate::geometry::topology::q) fn split_owner_range(
    range: &ExactSourceRange,
    parameter: &ExactRational,
    source_forward: bool,
) -> Result<(ExactSourceRange, ExactSourceRange)> {
    let width = range.end.try_subtract(&range.start)?;
    let middle = if source_forward {
        range.start.try_add(&width.try_product(parameter)?)?
    } else {
        range.end.try_subtract(&width.try_product(parameter)?)?
    };
    if source_forward {
        Ok((
            ExactSourceRange {
                start: range.start.clone(),
                end: middle.clone(),
            },
            ExactSourceRange {
                start: middle,
                end: range.end.clone(),
            },
        ))
    } else {
        Ok((
            ExactSourceRange {
                start: middle.clone(),
                end: range.end.clone(),
            },
            ExactSourceRange {
                start: range.start.clone(),
                end: middle,
            },
        ))
    }
}

pub(in crate::geometry::topology::q) fn push_span_occurrence(
    occurrences: &mut Vec<SpanOccurrence>,
    start: ExactChartPoint,
    end: ExactChartPoint,
    owner: &super::super::exact::ExactSpanOwner,
    source_range: ExactSourceRange,
    source_forward: bool,
    chart_shift: i64,
) -> Result<()> {
    let lift = owner
        .lift
        .checked_sub(chart_shift)
        .ok_or(ResourceError::SizeOverflow)?;
    occurrences.push(SpanOccurrence {
        start,
        end,
        source_edge: owner.source_edge,
        source_range,
        source_forward,
        lift,
    });
    Ok(())
}

#[expect(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "source QPort and ChartAtom IDs index the exact noder columns they were issued from"
)]
pub(in crate::geometry::topology::q) fn make_cut_chart(
    frame: GeographicFrame,
    sources: &ExactGeographicChartInventory,
    chart: &NodedExactChart,
    cut: &ExactRational,
) -> Result<CutChart> {
    let period = ExactRational::from_f64(frame.longitude().period());
    let cut_east = cut.try_add(&period)?;
    let port_capacity = chart
        .ports
        .len()
        .checked_add(
            chart
                .atoms
                .len()
                .checked_mul(2)
                .ok_or(ResourceError::SizeOverflow)?,
        )
        .ok_or(ResourceError::SizeOverflow)?;
    let occurrence_capacity = chart
        .atoms
        .len()
        .checked_mul(2)
        .ok_or(ResourceError::SizeOverflow)?;
    let mut candidates = Vec::with_capacity(port_capacity);
    let mut occurrences = Vec::with_capacity(occurrence_capacity);
    for node in &chart.nodes {
        let key = canonical_source_node_key(chart, node.id)?;
        let source_ports = chart.node_ports.get(node.ports.clone()).ok_or_else(|| {
            GeometryErrorKind::Invalid("exact Q node has an invalid port range".into())
        })?;
        for &port_id in source_ports {
            let port = chart.ports.get(port_id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("exact Q node has an unknown port".into())
            })?;
            candidates.push(PortCandidate {
                point: lifted_point(&port.point, cut, &period)?,
                node: key.clone(),
                cut: false,
            });
        }
    }

    for atom in &chart.atoms {
        let start_port = chart.ports.get(atom.ports[0].0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("an exact chart atom has no start port".into())
        })?;
        let end_port = chart.ports.get(atom.ports[1].0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("an exact chart atom has no end port".into())
        })?;
        let start_side = start_port.point.x.try_cmp(cut)?;
        let end_side = end_port.point.x.try_cmp(cut)?;
        if start_side.is_eq() || end_side.is_eq() {
            return Err(GeometryErrorKind::Invalid(
                "the selected Q cut passes through an exact chart port".into(),
            )
            .into());
        }
        let crosses_cut = matches!(
            (start_side, end_side),
            (Ordering::Less, Ordering::Greater) | (Ordering::Greater, Ordering::Less)
        );
        if crosses_cut {
            let parameter = cut
                .try_subtract(&start_port.point.x)?
                .try_quotient(&end_port.point.x.try_subtract(&start_port.point.x)?)?;
            if !super::super::exact::in_unit_interval(&parameter)? {
                return Err(GeometryErrorKind::Invalid(
                    "a Q cut parameter lies outside its source span".into(),
                )
                .into());
            }
            let cut_point = super::super::exact::interpolate_exact_segment(
                &start_port.point,
                &end_port.point,
                &parameter,
            )?;
            let cut_key = cut_node_key(frame, chart, &cut_point.y)?;
            add_cut_candidate(&mut candidates, &cut_point, cut, &period, cut_key)?;
            let lifted_start = lifted_point(&start_port.point, cut, &period)?;
            let lifted_end = lifted_point(&end_port.point, cut, &period)?;
            let (first_start, first_end, second_start, second_end) = if start_side.is_lt() {
                (
                    lifted_start,
                    ExactChartPoint::from_rationals(cut_east.clone(), cut_point.y.clone()),
                    ExactChartPoint::from_rationals(cut.clone(), cut_point.y.clone()),
                    lifted_end,
                )
            } else {
                (
                    lifted_start,
                    ExactChartPoint::from_rationals(cut.clone(), cut_point.y.clone()),
                    ExactChartPoint::from_rationals(cut_east.clone(), cut_point.y.clone()),
                    lifted_end,
                )
            };
            for owner in &atom.owners {
                let source_forward = !owner.reversed;
                let (first_range, second_range) =
                    split_owner_range(&owner.source_range, &parameter, source_forward)?;
                push_span_occurrence(
                    &mut occurrences,
                    first_start.clone(),
                    first_end.clone(),
                    owner,
                    first_range,
                    source_forward,
                    i64::from(start_side.is_lt()),
                )?;
                push_span_occurrence(
                    &mut occurrences,
                    second_start.clone(),
                    second_end.clone(),
                    owner,
                    second_range,
                    source_forward,
                    i64::from(!start_side.is_lt()),
                )?;
            }
        } else {
            let start = lifted_point(&start_port.point, cut, &period)?;
            let end = lifted_point(&end_port.point, cut, &period)?;
            for owner in &atom.owners {
                push_span_occurrence(
                    &mut occurrences,
                    start.clone(),
                    end.clone(),
                    owner,
                    owner.source_range.clone(),
                    !owner.reversed,
                    i64::from(start_side.is_lt()),
                )?;
            }
        }
    }

    let (mut ports, port_keys) = unique_port_candidates(candidates)?;
    let mut node_members = Vec::with_capacity(ports.len());
    for (port, key) in ports.iter().zip(port_keys) {
        node_members.push((key, port.id));
    }
    try_sort_by(&mut node_members, |left, right| {
        let node = compare_qnode_keys(&left.0, &right.0)?;
        Ok(if node.is_eq() {
            left.1.cmp(&right.1)
        } else {
            node
        })
    })?;
    let mut nodes = Vec::new();
    let mut node_ports = Vec::with_capacity(ports.len());
    let mut cursor = 0;
    while cursor < node_members.len() {
        let node_id = QNodeId(u32::try_from(nodes.len()).map_err(|_| ResourceError::SizeOverflow)?);
        let range_start = node_ports.len();
        let key = node_members[cursor].0.clone();
        let pole = qnode_pole(chart, &key)?;
        while cursor < node_members.len()
            && compare_qnode_keys(&key, &node_members[cursor].0)?.is_eq()
        {
            let port = node_members[cursor].1;
            ports[port.0 as usize].node = node_id;
            node_ports.push(port);
            cursor += 1;
        }
        nodes.push(QNode {
            id: node_id,
            key,
            pole,
            ports: range_start..node_ports.len(),
        });
    }

    let mut source_edge_ports = Vec::with_capacity(chart.source_edge_ports.len());
    for source in &chart.source_edge_ports {
        source_edge_ports.push([
            find_qport(
                &ports,
                &lifted_point(&chart.ports[source[0].0 as usize].point, cut, &period)?,
            )?,
            find_qport(
                &ports,
                &lifted_point(&chart.ports[source[1].0 as usize].point, cut, &period)?,
            )?,
        ]);
    }
    let mut source_point_ports = Vec::with_capacity(chart.source_point_ports.len());
    for &source in &chart.source_point_ports {
        source_point_ports.push(find_qport(
            &ports,
            &lifted_point(&chart.ports[source.0 as usize].point, cut, &period)?,
        )?);
    }

    let mut grouped = Vec::with_capacity(occurrences.len());
    for occurrence in occurrences {
        let start = find_qport(&ports, &occurrence.start)?;
        let end = find_qport(&ports, &occurrence.end)?;
        if start == end {
            continue;
        }
        let (endpoints, reversed) = if start < end {
            ([start, end], !occurrence.source_forward)
        } else {
            ([end, start], occurrence.source_forward)
        };
        grouped.push((
            endpoints,
            occurrence.source_edge,
            occurrence.source_range,
            occurrence.lift,
            reversed,
        ));
    }
    try_sort_by(&mut grouped, |left, right| {
        let endpoints = left.0.cmp(&right.0);
        if !endpoints.is_eq() {
            return Ok(endpoints);
        }
        let source = left.1.cmp(&right.1);
        if !source.is_eq() {
            return Ok(source);
        }
        let start = left.2.start.try_cmp(&right.2.start)?;
        Ok(if start.is_eq() {
            left.2.end.try_cmp(&right.2.end)?
        } else {
            start
        })
    })?;
    let mut spans = Vec::new();
    let mut index = 0;
    while index < grouped.len() {
        let endpoints = grouped[index].0;
        let span_id =
            ChartSpanId(u32::try_from(spans.len()).map_err(|_| ResourceError::SizeOverflow)?);
        let mut owners = Vec::new();
        while index < grouped.len() && grouped[index].0 == endpoints {
            let source_edge = grouped[index].1;
            let reversed = grouped[index].4;
            owners.push(QSpanOwner {
                source_edge,
                source_range: grouped[index].2.clone(),
                reversed,
                lift: grouped[index].3,
                ring_transition: source_ring_transition(sources, source_edge, reversed)?,
            });
            index += 1;
        }
        spans.push(ChartSpan {
            id: span_id,
            ports: endpoints,
            nodes: [
                ports[endpoints[0].0 as usize].node,
                ports[endpoints[1].0 as usize].node,
            ],
            owners: owners.into_boxed_slice(),
        });
    }

    Ok(CutChart {
        cut: cut.clone(),
        cut_east,
        ports,
        nodes,
        node_ports,
        spans,
        source_edge_ports,
        source_point_ports,
    })
}
