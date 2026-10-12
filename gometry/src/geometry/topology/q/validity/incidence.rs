use std::collections::BTreeMap;

use super::shared_spans::is_declared_pole_pair_span;
use super::{
    Candidate, ExactGeographicTopology, IssueKind, QNodeId, RingRules, SourceEdgeKind,
    SourceRingId, node_witness, offer, ring_node_incidence, source_edge,
};
use crate::error::Result;
use crate::geometry::GeometryErrorKind;
use crate::geometry::tessellation::exact::{ExactDyadic, ExactRational};

pub(super) fn incidence_issues(
    topology: &ExactGeographicTopology,
    rules: &[RingRules],
    best: &mut Option<Candidate>,
) -> Result<()> {
    let period = ExactRational::from_f64(topology.sources.frame.longitude().period());
    let mut ends: BTreeMap<(QNodeId, SourceRingId), Vec<ExactRational>> = BTreeMap::new();
    for (span_index, span) in topology.q.spans.iter().enumerate() {
        if topology.q.span_is_collapsed_pole(span_index)?
            || is_declared_pole_pair_span(span, topology, rules)?
        {
            continue;
        }
        for owner in &span.owners {
            let edge = source_edge(topology, owner.source_edge)?;
            let SourceEdgeKind::Ring { ring, .. } = edge.kind else {
                continue;
            };
            let shift = ExactRational::from_dyadic(ExactDyadic::from_i64(owner.lift))
                .try_product(&period)?;
            for (node, port_id) in span.nodes.into_iter().zip(span.ports) {
                let port = topology.q.ports.get(port_id.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q span has an unknown endpoint port".into())
                })?;
                let x = port.point.x.try_add(&shift)?;
                ends.entry((node, ring)).or_default().push(x);
            }
        }
    }

    for ((node, ring_id), mut lifted_x) in ends {
        let ring = topology
            .sources
            .rings
            .get(ring_id.0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q node has an unknown source ring".into())
            })?;
        let pole = topology
            .q
            .nodes
            .get(node.0 as usize)
            .is_some_and(|node| node.pole.is_some());
        if ring_node_incidence(ring.summary.chart_turn, pole, &mut lifted_x)?.invalid {
            offer(
                best,
                Candidate {
                    kind: IssueKind::SelfIntersection,
                    polygon: ring.polygon,
                    other_polygon: None,
                    rings: Some((ring_id, None)),
                    witness: node_witness(topology, node)?,
                },
                &topology.sources,
            )?;
        }
    }
    Ok(())
}
