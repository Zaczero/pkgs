use std::collections::{BTreeMap, BTreeSet};

use super::shared_spans::is_declared_pole_pair_span;
use super::{
    Candidate, ExactGeographicTopology, IssueKind, QNodeId, RingRules, SourceEdgeKind,
    SourcePolygonId, SourceRingId, node_witness, offer, source_edge,
};
use crate::error::Result;
use crate::geometry::GeometryErrorKind;

pub(super) fn connectivity_issues(
    topology: &ExactGeographicTopology,
    rules: &[RingRules],
    best: &mut Option<Candidate>,
) -> Result<()> {
    let mut rings_at_node: BTreeMap<QNodeId, BTreeSet<SourceRingId>> = BTreeMap::new();
    for (span_index, span) in topology.q.spans.iter().enumerate() {
        if topology.q.span_is_collapsed_pole(span_index)?
            || is_declared_pole_pair_span(span, topology, rules)?
        {
            continue;
        }
        for owner in &span.owners {
            if let SourceEdgeKind::Ring { ring, .. } =
                source_edge(topology, owner.source_edge)?.kind
            {
                rings_at_node.entry(span.nodes[0]).or_default().insert(ring);
                rings_at_node.entry(span.nodes[1]).or_default().insert(ring);
            }
        }
    }

    let mut graph = crate::collections::UnionFind::new(topology.sources.rings.len());
    for (node, rings) in rings_at_node {
        let mut by_polygon: BTreeMap<SourcePolygonId, Vec<SourceRingId>> = BTreeMap::new();
        for ring_id in rings {
            let ring = topology
                .sources
                .rings
                .get(ring_id.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q touch node has an unknown source ring".into())
                })?;
            by_polygon.entry(ring.polygon).or_default().push(ring_id);
        }
        for (polygon, rings) in by_polygon {
            if rings.len() < 2 {
                continue;
            }
            let touch = graph.push();
            let mut cycle = false;
            for ring in rings {
                cycle |= graph.union(ring.0 as usize, touch);
            }
            if cycle {
                offer(
                    best,
                    Candidate {
                        kind: IssueKind::InteriorDisconnected,
                        polygon,
                        other_polygon: None,
                        rings: None,
                        witness: node_witness(topology, node)?,
                    },
                    &topology.sources,
                )?;
            }
        }
    }
    Ok(())
}
