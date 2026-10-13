use super::super::exact::SourceEdgeKind;
use super::plan::*;
use super::sites::*;
use super::source_reuse::source_edge_at;
use super::*;

pub(in crate::geometry::topology) fn boundary_cycle_line_paths(
    topology: &ExactGeographicTopology,
    cycles: &[BoundaryCycle],
    work: &mut impl Work,
) -> Result<(Vec<PlannedQLine>, Vec<bool>)> {
    let q = &topology.q;
    work.charge_into_error(
        u64::try_from(
            cycles
                .len()
                .checked_add(q.nodes.len())
                .ok_or(ResourceError::SizeOverflow)?,
        )
        .map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut paths = Vec::with_capacity(cycles.len());
    let mut covered = vec![false; q.nodes.len()];
    for cycle in cycles {
        work.charge_into_error(1)?;
        let mut path = Vec::with_capacity(cycle.darts.len());
        for &dart_id in &cycle.darts {
            work.charge_into_error(1)?;
            let dart = q.darts.get(dart_id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q boundary cycle has an unknown dart".into())
            })?;
            let span_index = dart.span.0 as usize;
            let span = q.spans.get(span_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q boundary cycle has an unknown span".into())
            })?;
            let from_endpoint = if dart.from_node == span.nodes[0] {
                SpanEndpoint::Start
            } else if dart.from_node == span.nodes[1] {
                SpanEndpoint::End
            } else {
                return Err(GeometryErrorKind::Invalid(
                    "Q boundary dart does not start at a span endpoint".into(),
                )
                .into());
            };
            let to_node = match from_endpoint {
                SpanEndpoint::Start => span.nodes[1],
                SpanEndpoint::End => span.nodes[0],
            };
            if dart.to_node != to_node {
                return Err(GeometryErrorKind::Invalid(
                    "Q boundary dart does not end at the opposite span endpoint".into(),
                )
                .into());
            }
            *covered.get_mut(dart.from_node.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q boundary dart starts at an unknown node".into())
            })? = true;
            *covered.get_mut(dart.to_node.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q boundary dart ends at an unknown node".into())
            })? = true;
            path.push(LineStep {
                span: span_index,
                from_endpoint,
                owner: boundary_owner_index(topology, span_index, work)?,
            });
        }
        paths.push(QLinePrimitive {
            origin: QLineOrigin::ArealBoundary,
            recipe_walk: RecipeWalkOwner::Line(paths.len()),
            source_row: None,
            close: false,
            path,
        });
    }
    Ok((paths, covered))
}
pub(in crate::geometry::topology) fn line_owner_index(
    topology: &ExactGeographicTopology,
    span_index: usize,
    work: &mut impl Work,
) -> Result<usize> {
    let q = &topology.q;
    let span = q
        .spans
        .get(span_index)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q line span is unknown".into()))?;
    let line_rows = q.line_rows_for_span(span_index)?;
    let collapsed_rows = q.collapsed_rows_for_span(span_index)?;
    let mut ring_owner = None;
    for (owner_index, owner) in span.owners.iter().enumerate() {
        work.charge_into_error(1)?;
        let edge = source_edge_at(topology, owner.source_edge)?;
        let supported = match edge.kind {
            SourceEdgeKind::Line { .. } => {
                work.charge_into_error(1)?;
                line_rows.binary_search(&edge.row).is_ok()
            },
            SourceEdgeKind::Ring {
                hole: false,
                kind: crate::geometry::RingKind::Collapsed,
                ..
            } => {
                work.charge_into_error(1)?;
                collapsed_rows.binary_search(&edge.row).is_ok()
            },
            SourceEdgeKind::Ring { .. } => {
                ring_owner.get_or_insert(owner_index);
                false
            },
        };
        if supported {
            return Ok(owner_index);
        }
    }
    if let Some(owner) = ring_owner {
        return Ok(owner);
    }
    Err(
        GeometryErrorKind::Invalid("selected Q curve span has no source interval owner".into())
            .into(),
    )
}
pub(in crate::geometry::topology) fn boundary_owner_index(
    topology: &ExactGeographicTopology,
    span_index: usize,
    work: &mut impl Work,
) -> Result<usize> {
    let span = topology
        .q
        .spans
        .get(span_index)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q area span is unknown".into()))?;
    for (owner_index, owner) in span.owners.iter().enumerate() {
        work.charge_into_error(1)?;
        if matches!(
            source_edge_at(topology, owner.source_edge)?.kind,
            SourceEdgeKind::Ring { .. }
        ) {
            return Ok(owner_index);
        }
    }
    if !span.owners.is_empty() {
        return Ok(0);
    }
    Err(
        GeometryErrorKind::Invalid("selected Q boundary has no source interval owner".into())
            .into(),
    )
}
pub(in crate::geometry::topology) fn line_steps(
    topology: &ExactGeographicTopology,
    selected: &[Option<SelectedLineSpan>],
    work: &mut impl Work,
) -> Result<Vec<Vec<LineStep>>> {
    let q = &topology.q;
    let node_count = q.nodes.len();
    work.charge_into_error(
        u64::try_from(
            node_count
                .checked_mul(2)
                .and_then(|nodes| nodes.checked_add(q.spans.len()))
                .and_then(|items| items.checked_add(1))
                .ok_or(ResourceError::SizeOverflow)?,
        )
        .map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let mut degree = vec![0_usize; node_count];
    let mut incidences = Vec::new();
    work.charge_into_error(
        u64::try_from(selected.len()).map_err(|_| ResourceError::SizeOverflow)?,
    )?;
    let edge_count = selected.iter().flatten().count();
    incidences.reserve(
        edge_count
            .checked_mul(2)
            .ok_or(ResourceError::SizeOverflow)?,
    );
    for (span_index, (span, selection)) in q.spans.iter().zip(selected).enumerate() {
        work.charge_into_error(1)?;
        if selection.is_none() {
            continue;
        }
        let [start_node, end_node] = span.nodes;
        for (endpoint, node_id) in [
            (SpanEndpoint::Start, start_node),
            (SpanEndpoint::End, end_node),
        ] {
            work.charge_into_error(1)?;
            let node = node_id.0 as usize;
            let next = degree.get_mut(node).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q line span has an unknown node".into())
            })?;
            *next = next.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
            incidences.push((node, span_index, endpoint));
        }
    }
    try_sort_by_with_work(&mut incidences, work, |left, right, _work| {
        Ok(left.cmp(right))
    })?;
    let offset_count = node_count
        .checked_add(1)
        .ok_or(ResourceError::SizeOverflow)?;
    let mut offsets = Vec::with_capacity(offset_count);
    offsets.push(0_usize);
    for count in &degree {
        work.charge_into_error(1)?;
        offsets.push(
            offsets
                .last()
                .copied()
                .expect("line incidence offsets start at zero")
                .checked_add(*count)
                .ok_or(ResourceError::SizeOverflow)?,
        );
    }
    if offsets.last().copied() != Some(incidences.len()) {
        return Err(GeometryErrorKind::Invalid(
            "Q line incidence offsets do not match their entries".into(),
        )
        .into());
    }
    let mut visited = vec![false; q.spans.len()];
    let mut paths = Vec::new();
    for (node, &node_degree) in degree.iter().enumerate() {
        work.charge_into_error(1)?;
        if node_degree == 2 {
            continue;
        }
        let [start, end] = offsets
            .get(node..node.saturating_add(2))
            .ok_or_else(|| GeometryErrorKind::Invalid("Q line node has invalid offsets".into()))?
        else {
            return Err(
                GeometryErrorKind::Invalid("Q line node has invalid offsets".into()).into(),
            );
        };
        let incident = incidences.get(*start..*end).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q line node has an invalid incidence range".into())
        })?;
        for &(_, span, endpoint) in incident {
            work.charge_into_error(1)?;
            let selection = selected.get(span).copied().ok_or_else(|| {
                GeometryErrorKind::Invalid("Q line incidence references an unknown span".into())
            })?;
            let already_visited = visited.get(span).copied().ok_or_else(|| {
                GeometryErrorKind::Invalid("Q line incidence has no visited state".into())
            })?;
            if selection.is_some() && !already_visited {
                paths.push(walk_line_steps(
                    topology,
                    selected,
                    &degree,
                    &offsets,
                    &incidences,
                    &mut visited,
                    span,
                    endpoint,
                    work,
                )?);
            }
        }
    }
    for (span, selection) in selected.iter().enumerate() {
        work.charge_into_error(1)?;
        let already_visited = visited
            .get(span)
            .copied()
            .ok_or_else(|| GeometryErrorKind::Invalid("Q line span has no visited state".into()))?;
        if selection.is_some() && !already_visited {
            paths.push(walk_line_steps(
                topology,
                selected,
                &degree,
                &offsets,
                &incidences,
                &mut visited,
                span,
                SpanEndpoint::Start,
                work,
            )?);
        }
    }
    Ok(paths)
}
pub(in crate::geometry::topology) fn planned_line_paths(
    topology: &ExactGeographicTopology,
    paths: Vec<Vec<LineStep>>,
    work: &mut impl Work,
) -> Result<Vec<PlannedQLine>> {
    let mut output = Vec::new();
    for path in paths {
        work.charge_into_error(1)?;
        let mut current_origin = None;
        let mut current_steps = Vec::new();
        for step in path {
            work.charge_into_error(1)?;
            let span = topology.q.spans.get(step.span).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q line step references an unknown span".into())
            })?;
            let owner = span.owners.get(step.owner).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q line step references an unknown owner".into())
            })?;
            let origin = match source_edge_at(topology, owner.source_edge)?.kind {
                SourceEdgeKind::Line { .. }
                | SourceEdgeKind::Ring {
                    kind: crate::geometry::RingKind::Collapsed,
                    ..
                } => QLineOrigin::Curve,
                SourceEdgeKind::Ring { .. } => QLineOrigin::ArealBoundary,
            };
            if let Some(previous) = current_origin
                && previous != origin
            {
                let index = output.len();
                output.push(QLinePrimitive {
                    origin: previous,
                    recipe_walk: RecipeWalkOwner::Line(index),
                    source_row: None,
                    close: false,
                    path: std::mem::take(&mut current_steps),
                });
            }
            current_origin = Some(origin);
            current_steps.push(step);
        }
        if let Some(origin) = current_origin {
            let index = output.len();
            output.push(QLinePrimitive {
                origin,
                recipe_walk: RecipeWalkOwner::Line(index),
                source_row: None,
                close: false,
                path: current_steps,
            });
        }
    }
    Ok(output)
}
pub(in crate::geometry::topology) fn walk_line_steps(
    topology: &ExactGeographicTopology,
    selected: &[Option<SelectedLineSpan>],
    degree: &[usize],
    offsets: &[usize],
    incidences: &[(usize, usize, SpanEndpoint)],
    visited: &mut [bool],
    start_span: usize,
    start_endpoint: SpanEndpoint,
    work: &mut impl Work,
) -> Result<Vec<LineStep>> {
    let q = &topology.q;
    let mut steps = Vec::new();
    let (mut span_index, mut from_endpoint) = (start_span, start_endpoint);
    loop {
        work.charge_into_error(1)?;
        let already_visited = visited
            .get_mut(span_index)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q line span is unknown".into()))?;
        if *already_visited {
            if span_index == start_span && from_endpoint == start_endpoint {
                break;
            }
            return Err(GeometryErrorKind::Invalid(
                "Q line traversal entered an earlier span".into(),
            )
            .into());
        }
        *already_visited = true;
        let owner = selected
            .get(span_index)
            .copied()
            .flatten()
            .map(|selection| selection.owner)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "selected Q curve span has no source interval owner".into(),
                )
            })?;
        steps.push(LineStep {
            span: span_index,
            from_endpoint,
            owner,
        });
        let span = q
            .spans
            .get(span_index)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q line span is unknown".into()))?;
        let arrived_endpoint = from_endpoint.opposite();
        let [start_node, end_node] = span.nodes;
        let node = match arrived_endpoint {
            SpanEndpoint::Start => start_node,
            SpanEndpoint::End => end_node,
        }
        .0 as usize;
        if degree.get(node).copied() != Some(2) {
            break;
        }
        let start = *offsets.get(node).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q line node has no incidence start".into())
        })?;
        let end = *offsets
            .get(node + 1)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q line node has no incidence end".into()))?;
        let mut next = None;
        for &incidence in incidences.get(start..end).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q line node has an invalid incidence range".into())
        })? {
            work.charge_into_error(1)?;
            if incidence.1 != span_index || incidence.2 != arrived_endpoint {
                next = Some(incidence);
                break;
            }
        }
        let next = next
            .ok_or_else(|| GeometryErrorKind::Invalid("Q line node has no continuation".into()))?;
        if visited.get(next.1).copied().unwrap_or(false) {
            if next.1 == start_span && next.2 == start_endpoint {
                break;
            }
            return Err(GeometryErrorKind::Invalid(
                "Q line cycle closes on a different used span".into(),
            )
            .into());
        }
        span_index = next.1;
        from_endpoint = next.2;
    }
    if steps.is_empty() {
        return Err(GeometryErrorKind::Invalid("Q line traversal emitted no spans".into()).into());
    }
    Ok(steps)
}
