use std::collections::BTreeSet;

use super::shared_spans::offer_ring_pair;
use super::{
    Candidate, ExactGeographicTopology, IssueKind, SourceEdgeKind, SourceRingId, node_witness,
    source_edge,
};
use crate::error::Result;
use crate::geometry::{GeometryErrorKind, fan_alternations};

pub(super) fn crossing_issues(
    topology: &ExactGeographicTopology,
    best: &mut Option<Candidate>,
) -> Result<()> {
    let q = &topology.q;
    for node_index in 0..q.nodes.len() {
        let start = *q
            .outgoing_offsets
            .get(node_index)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q node has no fan start".into()))?;
        let end = *q
            .outgoing_offsets
            .get(node_index + 1)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q node has no fan end".into()))?;
        if end.saturating_sub(start) < 4 {
            continue;
        }
        let node_id =
            q.nodes.get(node_index).map(|node| node.id).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q node fan has no node record".into())
            })?;
        let mut rings_by_ray = Vec::<BTreeSet<SourceRingId>>::with_capacity(end - start);
        let mut all_rings = BTreeSet::new();
        for dart_id in q.outgoing.get(start..end).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q node fan has an invalid dart range".into())
        })? {
            let dart = q
                .darts
                .get(dart_id.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q fan has an unknown dart".into()))?;
            let span = q
                .spans
                .get(dart.span.0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q fan has an unknown span".into()))?;
            let mut rings = BTreeSet::new();
            for owner in &span.owners {
                if let SourceEdgeKind::Ring { ring, .. } =
                    source_edge(topology, owner.source_edge)?.kind
                {
                    rings.insert(ring);
                    all_rings.insert(ring);
                }
            }
            rings_by_ray.push(rings);
        }
        let rings: Vec<_> = all_rings.into_iter().collect();
        for (index, &left) in rings.iter().enumerate() {
            for &right in rings.iter().skip(index).skip(1) {
                let left_record = topology.sources.rings.get(left.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q fan has an unknown source ring".into())
                })?;
                let right_record =
                    topology
                        .sources
                        .rings
                        .get(right.0 as usize)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid("Q fan has an unknown source ring".into())
                        })?;
                let labels = rings_by_ray.iter().filter_map(|ray| {
                    let has_left = ray.contains(&left);
                    let has_right = ray.contains(&right);
                    match (has_left, has_right) {
                        (true, false) => Some(false),
                        (false, true) => Some(true),
                        _ => None,
                    }
                });
                if fan_alternations(labels) {
                    offer_ring_pair(
                        topology,
                        best,
                        IssueKind::RingIntersection,
                        left_record.polygon,
                        left,
                        right_record.polygon,
                        right,
                        node_witness(topology, node_id)?,
                    )?;
                }
            }
        }
    }
    Ok(())
}
