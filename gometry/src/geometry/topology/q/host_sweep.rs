use super::{
    ChartSpan, ExactRational, FallibleAvl, GeometryErrorKind, HostGerm, HostQuery, HostQueryTarget,
    Ordering, QComponentId, QLocalFaceId, QPort, QuotientFaces, Result,
};

pub(in crate::geometry::topology::q) fn span_x_slope(
    ports: &[QPort],
    spans: &[ChartSpan],
    span_id: usize,
    latitude: &ExactRational,
) -> Result<(ExactRational, ExactRational)> {
    let span = spans
        .get(span_id)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q sweep references an unknown span".into()))?;
    let first = ports.get(span.ports[0].0 as usize).ok_or_else(|| {
        GeometryErrorKind::Invalid("Q sweep span has an unknown first port".into())
    })?;
    let second = ports.get(span.ports[1].0 as usize).ok_or_else(|| {
        GeometryErrorKind::Invalid("Q sweep span has an unknown second port".into())
    })?;
    let (low, high) = if first.point.y.try_cmp(&second.point.y)?.is_lt() {
        (&first.point, &second.point)
    } else {
        (&second.point, &first.point)
    };
    let dy = high.y.try_subtract(&low.y)?;
    if dy.is_zero() {
        return Err(GeometryErrorKind::Invalid(
            "a horizontal Q span entered the y-sweep status".into(),
        )
        .into());
    }
    let slope = high.x.try_subtract(&low.x)?.try_quotient(&dy)?;
    let x = low
        .x
        .try_add(&latitude.try_subtract(&low.y)?.try_product(&slope)?)?;
    Ok((x, slope))
}

pub(in crate::geometry::topology::q) fn compare_spans_at(
    ports: &[QPort],
    spans: &[ChartSpan],
    left: usize,
    right: usize,
    latitude: &ExactRational,
    side: i8,
) -> Result<Ordering> {
    if left == right {
        return Ok(Ordering::Equal);
    }
    let (left_x, left_slope) = span_x_slope(ports, spans, left, latitude)?;
    let (right_x, right_slope) = span_x_slope(ports, spans, right, latitude)?;
    let x_order = left_x.try_cmp(&right_x)?;
    if !x_order.is_eq() {
        return Ok(x_order);
    }
    let slope_order = left_slope.try_cmp(&right_slope)?;
    let slope_order = if side < 0 {
        slope_order.reverse()
    } else {
        slope_order
    };
    if slope_order.is_eq() {
        return Err(GeometryErrorKind::Invalid(
            "two exact Q spans share one open-slab tangent germ".into(),
        )
        .into());
    }
    Ok(slope_order)
}

pub(in crate::geometry::topology::q) fn compare_span_to_query(
    ports: &[QPort],
    spans: &[ChartSpan],
    span_id: usize,
    query: &HostGerm,
) -> Result<Ordering> {
    let (x, slope) = span_x_slope(ports, spans, span_id, &query.point.y)?;
    let x_order = x.try_cmp(&query.point.x)?;
    if !x_order.is_eq() {
        return Ok(x_order);
    }
    let edge_motion = slope.try_product(&query.dy)?;
    edge_motion
        .try_subtract(&query.dx)?
        .try_cmp(&ExactRational::zero())
}

/// The face east of a span that a westward host ray crosses.
///
/// The ray runs at the query height perturbed to one side, so the sweep status holds exactly
/// the spans crossing it there, and the region just east of the hit span holds the query. That
/// is the span's right side by orientation, whatever nodes lie at the ray's exact height;
/// resolving the hit through a node fan instead needs a perturbed tie-break that the span
/// orientation never does.
pub(in crate::geometry::topology::q) fn east_face_of_span(
    topology: &QuotientFaces,
    span_id: usize,
) -> Result<QLocalFaceId> {
    let span = topology.spans.get(span_id).ok_or_else(|| {
        GeometryErrorKind::Invalid("host hit references an unknown Q span".into())
    })?;
    let local_faces = topology
        .span_local_faces
        .get(span_id)
        .and_then(|faces| *faces)
        .ok_or_else(|| GeometryErrorKind::Invalid("host hit has no local face pair".into()))?;
    let start = topology
        .ports
        .get(span.ports[0].0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("host hit has no start port".into()))?;
    let end = topology
        .ports
        .get(span.ports[1].0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("host hit has no end port".into()))?;
    if start.point.y.try_cmp(&end.point.y)?.is_lt() {
        Ok(local_faces[1])
    } else {
        Ok(local_faces[0])
    }
}

pub(in crate::geometry::topology::q) fn resolve_host_queries(
    query_indices: &[usize],
    queries: &[HostQuery],
    reference: QComponentId,
    topology: &QuotientFaces,
    status: &FallibleAvl<usize>,
    component_hosts: &mut [Option<QLocalFaceId>],
    isolated_node_hosts: &mut [Option<QLocalFaceId>],
    observe_comparison: &mut impl FnMut(),
) -> Result<()> {
    for &query_index in query_indices {
        let query = queries.get(query_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q sweep has an unknown host query".into())
        })?;
        let mut compare_query = |span: &usize, query: &HostGerm| {
            observe_comparison();
            compare_span_to_query(&topology.ports, &topology.spans, *span, query)
        };
        let hit = status
            .predecessor(&query.germ, &mut compare_query)?
            .copied();
        let host = if let Some(span_id) = hit {
            let span = topology.spans.get(span_id).ok_or_else(|| {
                GeometryErrorKind::Invalid("host query references an unknown Q span".into())
            })?;
            let component = topology
                .node_components
                .get(span.nodes[0].0 as usize)
                .copied()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("host query span has an unknown component".into())
                })?;
            if component == query.component {
                return Err(GeometryErrorKind::Invalid(
                    "a Q component's westward host ray hit its own span".into(),
                )
                .into());
            }
            east_face_of_span(topology, span_id)?
        } else {
            let side = if query.germ.dy.try_cmp(&ExactRational::zero())?.is_lt() {
                -1
            } else {
                1
            };
            topology.cut_index.host_face_on_cut(
                query.component,
                reference,
                &query.germ.point.y,
                side,
            )?
        };
        let host_component = topology
            .local_faces
            .get(host.0 as usize)
            .map(|face| face.component)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q host query found an unknown face".into())
            })?;
        if host_component == query.component {
            return Err(GeometryErrorKind::Invalid(
                "a Q component selected its own local face as a global host".into(),
            )
            .into());
        }
        let target = match query.target {
            HostQueryTarget::Component(component) => component_hosts.get_mut(component.0 as usize),
            HostQueryTarget::IsolatedNode(node) => isolated_node_hosts.get_mut(node.0 as usize),
        }
        .ok_or_else(|| GeometryErrorKind::Invalid("Q host query has an unknown target".into()))?;
        *target = Some(host);
    }
    Ok(())
}
