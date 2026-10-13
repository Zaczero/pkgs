use super::{
    Candidate, ChartSpan, ExactChartPoint, ExactGeographicTopology, IssueKind, QSpanOwner,
    RingRules, SourceEdgeKind, SourcePolygonId, SourceRingId, is_pole_pair_excursion, offer,
    same_ring_seam_lift, source_edge, span_witness,
};
use crate::error::Result;
use crate::geometry::GeometryErrorKind;

pub(super) fn shared_span_issues(
    topology: &ExactGeographicTopology,
    rules: &[RingRules],
    best: &mut Option<Candidate>,
) -> Result<()> {
    for (span_index, span) in topology.q.spans.iter().enumerate() {
        if topology.q.span_is_collapsed_pole(span_index)? {
            continue;
        }
        let witness = span_witness(topology, span)?;
        for (left_index, left) in span.owners.iter().enumerate() {
            let left_edge = source_edge(topology, left.source_edge)?;
            let SourceEdgeKind::Ring {
                polygon: left_polygon,
                ring: left_ring,
                ..
            } = left_edge.kind
            else {
                continue;
            };
            for right in span.owners.iter().skip(left_index).skip(1) {
                let right_edge = source_edge(topology, right.source_edge)?;
                let SourceEdgeKind::Ring {
                    polygon: right_polygon,
                    ring: right_ring,
                    ..
                } = right_edge.kind
                else {
                    continue;
                };
                if left_ring == right_ring {
                    let ring = topology
                        .sources
                        .rings
                        .get(left_ring.0 as usize)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid("Q span has an unknown source ring".into())
                        })?;
                    let seam_lift = same_ring_seam_lift(ring, left, right);
                    let pole_pair = is_pole_pair_excursion(
                        rules,
                        left_ring,
                        left.source_edge,
                        right.source_edge,
                    );
                    if !seam_lift && !pole_pair {
                        offer(
                            best,
                            Candidate {
                                kind: IssueKind::SelfIntersection,
                                polygon: left_polygon,
                                other_polygon: None,
                                rings: Some((left_ring, None)),
                                witness: witness.clone(),
                            },
                            &topology.sources,
                        )?;
                    }
                } else if !opposite_seam_interiors(topology, span, left, right) {
                    offer_ring_pair(
                        topology,
                        best,
                        IssueKind::SharedLinework,
                        left_polygon,
                        left_ring,
                        right_polygon,
                        right_ring,
                        witness.clone(),
                    )?;
                }
            }
        }
    }
    Ok(())
}
pub(in crate::geometry::topology::q::validity) fn is_declared_pole_pair_span(
    span: &ChartSpan,
    topology: &ExactGeographicTopology,
    rules: &[RingRules],
) -> Result<bool> {
    if span.owners.len() != 2 {
        return Ok(false);
    }
    let [left, right] = &span.owners[..] else {
        return Ok(false);
    };
    let left_edge = source_edge(topology, left.source_edge)?;
    let right_edge = source_edge(topology, right.source_edge)?;
    let (
        SourceEdgeKind::Ring {
            ring: left_ring, ..
        },
        SourceEdgeKind::Ring {
            ring: right_ring, ..
        },
    ) = (left_edge.kind, right_edge.kind)
    else {
        return Ok(false);
    };
    if left_ring != right_ring {
        return Ok(false);
    }
    Ok(is_pole_pair_excursion(
        rules,
        left_ring,
        left.source_edge,
        right.source_edge,
    ))
}

pub(in crate::geometry::topology::q::validity) fn opposite_seam_interiors(
    topology: &ExactGeographicTopology,
    span: &ChartSpan,
    left: &QSpanOwner,
    right: &QSpanOwner,
) -> bool {
    let nodes = if span.nodes[0] <= span.nodes[1] {
        span.nodes
    } else {
        [span.nodes[1], span.nodes[0]]
    };
    let left_sides = topology
        .q
        .seam_owner_sides
        .iter()
        .filter(|side| side.nodes == nodes && side.edge == left.source_edge);
    let right_sides: Vec<_> = topology
        .q
        .seam_owner_sides
        .iter()
        .filter(|side| side.nodes == nodes && side.edge == right.source_edge)
        .collect();
    left_sides.into_iter().any(|a| {
        right_sides.iter().any(|b| {
            let a_has_interior = a.left_interior != a.right_interior;
            let b_has_interior = b.left_interior != b.right_interior;
            a.polygon != b.polygon
                && a_has_interior
                && b_has_interior
                && a.left_interior != b.left_interior
        })
    })
}

pub(in crate::geometry::topology::q::validity) fn offer_ring_pair(
    topology: &ExactGeographicTopology,
    best: &mut Option<Candidate>,
    kind: IssueKind,
    left_polygon: SourcePolygonId,
    left_ring: SourceRingId,
    right_polygon: SourcePolygonId,
    right_ring: SourceRingId,
    witness: ExactChartPoint,
) -> Result<()> {
    let (polygon, other_polygon, rings) = if left_polygon <= right_polygon {
        (
            left_polygon,
            (left_polygon != right_polygon).then_some(right_polygon),
            Some((left_ring, Some(right_ring))),
        )
    } else {
        (
            right_polygon,
            Some(left_polygon),
            Some((right_ring, Some(left_ring))),
        )
    };
    offer(
        best,
        Candidate {
            kind,
            polygon,
            other_polygon,
            rings,
            witness,
        },
        &topology.sources,
    )
}
