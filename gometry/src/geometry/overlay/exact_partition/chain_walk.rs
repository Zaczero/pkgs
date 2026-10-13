use super::{ExactSpan, GeometryErrorKind, IndexedTopology, NodeId, Range, Result};

pub(super) fn node_source_range(records: &[(NodeId, u32)], node: NodeId) -> Range<usize> {
    let start = records.partition_point(|(record_node, _)| record_node.0 < node.0);
    let count = records
        .iter()
        .skip(start)
        .take_while(|(record_node, _)| record_node.0 == node.0)
        .count();
    start..start.wrapping_add(count)
}

#[expect(
    clippy::indexing_slicing,
    reason = "the incoming span and node IDs come from the exact chain walk and the CSR row is owned by its topology"
)]
pub(super) fn next_incident_span(
    topology: &IndexedTopology,
    edge_of_slot: &[u32],
    node: NodeId,
    incoming: u32,
) -> Option<u32> {
    let range =
        topology.starts[node.0 as usize] as usize..topology.starts[node.0 as usize + 1] as usize;
    edge_of_slot[range]
        .iter()
        .copied()
        .find(|&span| span != incoming)
}

#[expect(
    clippy::indexing_slicing,
    reason = "span and node IDs are returned by the checked topology walk and address its aligned visit/pin columns"
)]
pub(super) fn walk_exact_chain(
    topology: &IndexedTopology,
    edge_of_slot: &[u32],
    spans: &[ExactSpan],
    node_pin: &[bool],
    visited: &mut [bool],
    origin: NodeId,
    first: u32,
) -> Result<(Vec<NodeId>, Vec<u32>)> {
    let mut nodes = Vec::with_capacity(2);
    nodes.push(origin);
    let mut chain_spans = Vec::new();
    let mut current = origin;
    let mut span_id = first;
    loop {
        let span_index = span_id as usize;
        if visited[span_index] {
            return Err(
                GeometryErrorKind::Invalid("exact chain walk revisited a span".into()).into(),
            );
        }
        visited[span_index] = true;
        let span = spans[span_index];
        let target = if span.start == current {
            span.end
        } else if span.end == current {
            span.start
        } else {
            return Err(GeometryErrorKind::Invalid(
                "exact chain span is detached from its walk".into(),
            )
            .into());
        };
        chain_spans.push(span_id);
        nodes.push(target);
        if node_pin[target.0 as usize] || target == origin {
            break;
        }
        span_id = next_incident_span(topology, edge_of_slot, target, span_id).ok_or_else(|| {
            GeometryErrorKind::Invalid("exact chain has no degree-two continuation".into())
        })?;
        current = target;
    }
    Ok((nodes, chain_spans))
}
