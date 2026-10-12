#![expect(
    dead_code,
    reason = "the geographic coverage simplifier consumes this rooted chain inventory"
)]

use std::cmp::Ordering;
use std::num::NonZeroUsize;

use super::super::exact::{SourceEdgeId, SourceEdgeKind, SourceRingId, SourceRowId};
use super::super::overlay;
use super::super::q::{
    ChartSpanId, ExactGeographicTopology, GlobalFaceId, QNodeId, QPortId, QSpanOwner,
};
use super::GeographicCoverageAnalysis;
use crate::error::{ResourceError, Result};
use crate::geometry::GeometryErrorKind;
use crate::geometry::embedding_search::Work;
use crate::geometry::segments::try_sort_by_with_work;

const JUNCTION: u8 = 1;
const INCIDENCE_CHANGE: u8 = 2;
const POLE_TRANSITION: u8 = 4;
const GERM_TRANSITION: u8 = 8;
const SOURCE_ANCHOR: u8 = 16;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::geometry::topology) enum GeographicChainPinReason {
    Junction,
    IncidenceChange,
    PoleTransition,
    GermTransition,
    SourceAnchor,
}

pub(in crate::geometry::topology) struct GeographicSourceRingAnchor {
    pub(in crate::geometry::topology) row: SourceRowId,
    pub(in crate::geometry::topology) ring: SourceRingId,
    pub(in crate::geometry::topology) source_edge: SourceEdgeId,
    pub(in crate::geometry::topology) port: QPortId,
    pub(in crate::geometry::topology) node: QNodeId,
}

pub(in crate::geometry::topology) struct GeographicPinnedChainNode {
    pub(in crate::geometry::topology) node: QNodeId,
    pub(in crate::geometry::topology) reasons: Vec<GeographicChainPinReason>,
}

pub(in crate::geometry::topology) struct GeographicSharedChainInventory<'q> {
    pub(in crate::geometry::topology) topology: &'q ExactGeographicTopology,
    pub(in crate::geometry::topology) chains: Vec<GeographicSharedChain>,
    /// `Some(n)` stores the zero-based chain index plus one; `None` marks non-boundary spans.
    pub(in crate::geometry::topology) span_to_chain: Vec<Option<NonZeroUsize>>,
    pub(in crate::geometry::topology) pinned_nodes: Vec<GeographicPinnedChainNode>,
    pub(in crate::geometry::topology) source_anchors: Vec<GeographicSourceRingAnchor>,
}

pub(in crate::geometry::topology) struct GeographicSharedChain {
    pub(in crate::geometry::topology) closed: bool,
    pub(in crate::geometry::topology) spans: Vec<GeographicSharedChainSpan>,
    pub(in crate::geometry::topology) row_uses: Vec<GeographicSharedChainUse>,
}

#[derive(Clone)]
pub(in crate::geometry::topology) struct GeographicSharedChainSpan {
    pub(in crate::geometry::topology) span: ChartSpanId,
    pub(in crate::geometry::topology) from_node: QNodeId,
    pub(in crate::geometry::topology) to_node: QNodeId,
    pub(in crate::geometry::topology) from_port: QPortId,
    pub(in crate::geometry::topology) to_port: QPortId,
    /// True when this use follows the Q span's canonical ports.
    pub(in crate::geometry::topology) forward: bool,
}

pub(in crate::geometry::topology) struct GeographicSharedChainUse {
    pub(in crate::geometry::topology) row: SourceRowId,
    pub(in crate::geometry::topology) region_root: GlobalFaceId,
    pub(in crate::geometry::topology) ring: SourceRingId,
    pub(in crate::geometry::topology) hole: bool,
    pub(in crate::geometry::topology) source_forward: bool,
    /// True when the row-side walk follows the chain's canonical direction.
    pub(in crate::geometry::topology) signed_forward: bool,
    pub(in crate::geometry::topology) root_uses: Vec<GeographicSharedChainRootUse>,
}

pub(in crate::geometry::topology) struct GeographicSharedChainRootUse {
    pub(in crate::geometry::topology) chain_span_index: usize,
    pub(in crate::geometry::topology) owner_index: usize,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct RowRingUseKey {
    row: SourceRowId,
    region_root: GlobalFaceId,
    ring: SourceRingId,
    hole: bool,
    source_forward: bool,
}

#[derive(Clone, Copy)]
struct SpanRowRingUse {
    key: RowRingUseKey,
    boundary_forward: bool,
    owner_index: usize,
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ChainUseKey {
    row: SourceRowId,
    region_root: GlobalFaceId,
    ring: SourceRingId,
    hole: bool,
    source_forward: bool,
    signed_forward: bool,
}

#[derive(Clone, Copy)]
struct PendingChainRootUse {
    key: ChainUseKey,
    chain_span_index: usize,
    owner_index: usize,
}

impl GeographicCoverageAnalysis {
    pub(in crate::geometry::topology) fn shared_chain_inventory<'q>(
        &'q self,
        work: &mut impl Work,
    ) -> Result<GeographicSharedChainInventory<'q>> {
        build_shared_chain_inventory(self, work)
    }
}

fn build_shared_chain_inventory<'q>(
    analysis: &'q GeographicCoverageAnalysis,
    work: &mut impl Work,
) -> Result<GeographicSharedChainInventory<'q>> {
    let topology = &analysis.topology;
    let q = &topology.q;
    let row_count = topology.sources.row_count;
    work.charge(u64::try_from(analysis.face_rows.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
    if !analysis.is_valid() || analysis.face_rows.len() != q.global_face_count {
        return Err(GeometryErrorKind::InvalidCoverage {
            operation: "coverage_simplify",
        }
        .into());
    }

    let mut face_owners = Vec::with_capacity(q.global_face_count);
    let mut selected_faces = Vec::with_capacity(q.global_face_count);
    for occupants in &analysis.face_rows {
        work.charge(1)?;
        match occupants.as_slice() {
            [] => {
                face_owners.push(None);
                selected_faces.push(false);
            },
            [row] if *row < row_count => {
                face_owners.push(Some(*row));
                selected_faces.push(true);
            },
            [..] => {
                return Err(GeometryErrorKind::InvalidCoverage {
                    operation: "coverage_simplify",
                }
                .into());
            },
        }
    }
    let mut regions = overlay::coverage_face_regions(q, &face_owners, work)?;
    let face_roots = overlay::selected_face_roots(q, &selected_faces, &mut regions, work)?;

    let span_count = q.spans.len();
    let mut boundary_spans = vec![false; span_count];
    let mut node_degrees = vec![0_usize; q.nodes.len()];
    let mut span_use_offsets = Vec::with_capacity(
        span_count
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?,
    );
    let mut span_uses = Vec::new();
    let mut local_uses = Vec::new();
    span_use_offsets.push(0);
    for (span_index, span) in q.spans.iter().enumerate() {
        work.charge(1)?;
        local_uses.clear();
        let faces = q.span_global_faces.get(span_index).copied().flatten();
        for (owner_index, owner) in span.owners.iter().enumerate() {
            work.charge(1)?;
            let source = topology
                .sources
                .edges
                .get(owner.source_edge.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q chain span has an unknown source root".into())
                })?;
            let SourceEdgeKind::Ring { ring, hole, .. } = source.kind else {
                continue;
            };
            let Some([left, right]) = faces else {
                continue;
            };
            let left_row = face_owners.get(left.0 as usize).copied().ok_or_else(|| {
                GeometryErrorKind::Invalid("Q chain span has an unknown left face".into())
            })?;
            let right_row = face_owners.get(right.0 as usize).copied().ok_or_else(|| {
                GeometryErrorKind::Invalid("Q chain span has an unknown right face".into())
            })?;
            let left_side = left_row == Some(source.row.0 as usize);
            let right_side = right_row == Some(source.row.0 as usize);
            if left_side == right_side {
                continue;
            }
            let face = if left_side { left } else { right };
            let region_root = face_roots
                .get(face.0 as usize)
                .copied()
                .flatten()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "coverage row-side use has no selected Q face root".into(),
                    )
                })?;
            let boundary_forward = left_side;
            local_uses.push(SpanRowRingUse {
                key: RowRingUseKey {
                    row: source.row,
                    region_root: GlobalFaceId(
                        u32::try_from(region_root).map_err(|_| ResourceError::SizeOverflow)?,
                    ),
                    ring,
                    hole,
                    source_forward: boundary_forward != owner.reversed,
                },
                boundary_forward,
                owner_index,
            });
        }
        if !local_uses.is_empty() {
            *boundary_spans.get_mut(span_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q boundary-span table is incomplete".into())
            })? = true;
        }
        if boundary_spans.get(span_index).copied().unwrap_or(false) {
            for node in span.nodes {
                let degree = node_degrees.get_mut(node.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q chain span has an unknown node".into())
                })?;
                *degree = degree.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
            }
        }
        try_sort_by_with_work(&mut local_uses, work, |left, right, _work| {
            Ok(left
                .key
                .cmp(&right.key)
                .then(left.boundary_forward.cmp(&right.boundary_forward))
                .then(left.owner_index.cmp(&right.owner_index)))
        })?;
        work.charge(u64::try_from(local_uses.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
        span_uses.append(&mut local_uses);
        span_use_offsets.push(span_uses.len());
    }

    let (source_anchors, anchor_counts) = source_ring_anchors(topology, work)?;
    let mut pin_reasons = vec![0_u8; q.nodes.len()];
    for node in &q.nodes {
        work.charge(1)?;
        let node_index = node.id.0 as usize;
        if node.pole.is_some() {
            add_pin_reason(&mut pin_reasons, node_index, POLE_TRANSITION)?;
        }
        if anchor_counts.get(node_index).copied().unwrap_or(0) != 0 {
            add_pin_reason(&mut pin_reasons, node_index, SOURCE_ANCHOR)?;
        }
        if non_cut_port_count(topology, node, work)? > 1 {
            add_pin_reason(&mut pin_reasons, node_index, GERM_TRANSITION)?;
        }
        let degree = node_degrees.get(node_index).copied().ok_or_else(|| {
            GeometryErrorKind::Invalid("Q chain node degree is incomplete".into())
        })?;
        if degree != 0 && degree != 2 {
            add_pin_reason(&mut pin_reasons, node_index, JUNCTION)?;
        } else if degree == 2 {
            let incident = incident_boundary_spans(topology, node.id, &boundary_spans, work)?;
            if incident.len() != 2
                || incident.first() == incident.get(1)
                || !span_signatures_continue(
                    topology,
                    *incident.first().ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "degree-two Q node has no first chain span".into(),
                        )
                    })?,
                    *incident.get(1).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "degree-two Q node has no second chain span".into(),
                        )
                    })?,
                    node.id,
                    &span_use_offsets,
                    &span_uses,
                    work,
                )?
            {
                add_pin_reason(&mut pin_reasons, node_index, INCIDENCE_CHANGE)?;
            }
        }
    }
    let pinned = pin_reasons
        .iter()
        .map(|&reasons| reasons != 0)
        .collect::<Vec<_>>();
    let mut pinned_nodes = Vec::new();
    for (node_index, &reasons) in pin_reasons.iter().enumerate() {
        work.charge(1)?;
        if reasons != 0 {
            pinned_nodes.push(GeographicPinnedChainNode {
                node: QNodeId(u32::try_from(node_index).map_err(|_| ResourceError::SizeOverflow)?),
                reasons: pin_reason_list(reasons),
            });
        }
    }

    let mut visited = vec![false; span_count];
    let mut span_to_chain = vec![None; span_count];
    let mut chains = Vec::new();
    collect_open_chains(
        topology,
        &mut visited,
        &pinned,
        &boundary_spans,
        &node_degrees,
        &span_use_offsets,
        &span_uses,
        &mut span_to_chain,
        &mut chains,
        work,
    )?;
    collect_closed_chains(
        topology,
        &mut visited,
        &pinned,
        &boundary_spans,
        &node_degrees,
        &span_use_offsets,
        &span_uses,
        &mut span_to_chain,
        &mut chains,
        work,
    )?;
    for (span_index, offsets) in span_use_offsets.windows(2).enumerate() {
        work.charge(1)?;
        let [start, end] = offsets else {
            return Err(GeometryErrorKind::Invalid(
                "Q chain span offset window is incomplete".into(),
            )
            .into());
        };
        if start != end && !visited.get(span_index).copied().unwrap_or(false) {
            return Err(GeometryErrorKind::Invalid(
                "geographic shared-chain inventory omitted a boundary span".into(),
            )
            .into());
        }
    }
    Ok(GeographicSharedChainInventory {
        topology,
        chains,
        span_to_chain,
        pinned_nodes,
        source_anchors,
    })
}

fn source_ring_anchors(
    topology: &ExactGeographicTopology,
    work: &mut impl Work,
) -> Result<(Vec<GeographicSourceRingAnchor>, Vec<usize>)> {
    let mut anchors = Vec::new();
    let mut counts = vec![0_usize; topology.q.nodes.len()];
    for ring in &topology.sources.rings {
        work.charge(1)?;
        if ring.source_edges.is_empty() {
            continue;
        }
        let edge_index = ring.source_edges.start;
        let port_id = topology
            .q
            .source_edge_ports
            .get(edge_index)
            .and_then(|ports| ports.first())
            .copied()
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("source ring anchor has no Q edge port".into())
            })?;
        let port = topology.q.ports.get(port_id.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("source ring anchor has an unknown Q port".into())
        })?;
        let polygon = topology
            .sources
            .polygons
            .get(ring.polygon.0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("source ring anchor has an unknown polygon".into())
            })?;
        let source_edge = topology
            .sources
            .edges
            .get(edge_index)
            .ok_or_else(|| GeometryErrorKind::Invalid("source ring anchor edge is unknown".into()))?
            .id;
        let count = counts.get_mut(port.node.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("source ring anchor has an unknown Q node".into())
        })?;
        *count = count.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
        anchors.push(GeographicSourceRingAnchor {
            row: polygon.row,
            ring: ring.id,
            source_edge,
            port: port_id,
            node: port.node,
        });
    }
    Ok((anchors, counts))
}

fn non_cut_port_count(
    topology: &ExactGeographicTopology,
    node: &super::super::q::QNode,
    work: &mut impl Work,
) -> Result<usize> {
    let count = node
        .ports
        .end
        .checked_sub(node.ports.start)
        .ok_or(ResourceError::SizeOverflow)?;
    work.charge(u64::try_from(count).map_err(|_| ResourceError::SizeOverflow)?)?;
    let ports = topology
        .q
        .node_ports
        .get(node.ports.clone())
        .ok_or_else(|| GeometryErrorKind::Invalid("Q node port range is incomplete".into()))?;
    let mut non_cut = 0_usize;
    for &port_id in ports {
        let port = topology.q.ports.get(port_id.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q node references an unknown port".into())
        })?;
        if !port.cut {
            non_cut = non_cut.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
        }
    }
    Ok(non_cut)
}

fn incident_boundary_spans(
    topology: &ExactGeographicTopology,
    node: QNodeId,
    boundary_spans: &[bool],
    work: &mut impl Work,
) -> Result<Vec<usize>> {
    let node_index = node.0 as usize;
    let start = *topology.q.outgoing_offsets.get(node_index).ok_or_else(|| {
        GeometryErrorKind::Invalid("Q chain node has no outgoing-fan start".into())
    })?;
    let end_index = node_index
        .checked_add(1)
        .ok_or(ResourceError::SizeOverflow)?;
    let end =
        *topology.q.outgoing_offsets.get(end_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q chain node has no outgoing-fan end".into())
        })?;
    let fan = topology
        .q
        .outgoing
        .get(start..end)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q chain node fan is incomplete".into()))?;
    let mut spans = Vec::new();
    for &dart_id in fan {
        work.charge(1)?;
        let dart = topology.q.darts.get(dart_id.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q chain node fan has an unknown dart".into())
        })?;
        let span_index = dart.span.0 as usize;
        if boundary_spans.get(span_index).copied().unwrap_or(false) {
            spans.push(span_index);
        }
    }
    Ok(spans)
}

fn span_use_range<'a>(
    span: usize,
    offsets: &[usize],
    uses: &'a [SpanRowRingUse],
) -> Result<&'a [SpanRowRingUse]> {
    let end_index = span.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
    let start = *offsets
        .get(span)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q chain span has no source-use start".into()))?;
    let end = *offsets
        .get(end_index)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q chain span has no source-use end".into()))?;
    uses.get(start..end).ok_or_else(|| {
        GeometryErrorKind::Invalid("Q chain span source-use range is incomplete".into()).into()
    })
}

fn span_signatures_continue(
    topology: &ExactGeographicTopology,
    first_span: usize,
    second_span: usize,
    node: QNodeId,
    offsets: &[usize],
    uses: &[SpanRowRingUse],
    work: &mut impl Work,
) -> Result<bool> {
    if first_span == second_span {
        return Ok(false);
    }
    let first_uses = span_use_range(first_span, offsets, uses)?;
    let second_uses = span_use_range(second_span, offsets, uses)?;
    let first_span = topology
        .q
        .spans
        .get(first_span)
        .ok_or_else(|| GeometryErrorKind::Invalid("first Q chain span is unknown".into()))?;
    let second_span = topology
        .q
        .spans
        .get(second_span)
        .ok_or_else(|| GeometryErrorKind::Invalid("second Q chain span is unknown".into()))?;
    let [first_start, _] = first_span.nodes;
    let [second_start, _] = second_span.nodes;
    let first_outward_at_start = node == first_start;
    let second_outward_at_start = node == second_start;

    let mut first_cursor = 0_usize;
    let mut second_cursor = 0_usize;
    loop {
        let (Some(first), Some(second)) =
            (first_uses.get(first_cursor), second_uses.get(second_cursor))
        else {
            return Ok(first_cursor == first_uses.len() && second_cursor == second_uses.len());
        };
        work.charge(1)?;
        if first.key != second.key {
            return Ok(false);
        }
        let key = first.key;
        let mut first_outward = [0_usize; 2];
        while let Some(use_) = first_uses.get(first_cursor).filter(|use_| use_.key == key) {
            let outward = use_.boundary_forward == first_outward_at_start;
            let count = first_outward
                .get_mut(usize::from(outward))
                .ok_or(ResourceError::SizeOverflow)?;
            *count = count.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
            first_cursor = first_cursor
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
            work.charge(1)?;
        }
        let mut second_outward = [0_usize; 2];
        while let Some(use_) = second_uses
            .get(second_cursor)
            .filter(|use_| use_.key == key)
        {
            let outward = use_.boundary_forward == second_outward_at_start;
            let count = second_outward
                .get_mut(usize::from(outward))
                .ok_or(ResourceError::SizeOverflow)?;
            *count = count.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
            second_cursor = second_cursor
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
            work.charge(1)?;
        }
        let [first_incoming, first_outgoing] = first_outward;
        let [second_incoming, second_outgoing] = second_outward;
        if first_incoming != second_outgoing || first_outgoing != second_incoming {
            return Ok(false);
        }
    }
}

fn add_pin_reason(reasons: &mut [u8], node: usize, reason: u8) -> Result<()> {
    let slot = reasons.get_mut(node).ok_or_else(|| {
        GeometryErrorKind::Invalid("Q chain pin reason references an unknown node".into())
    })?;
    *slot |= reason;
    Ok(())
}

fn pin_reason_list(bits: u8) -> Vec<GeographicChainPinReason> {
    let mut reasons = Vec::new();
    for (bit, reason) in [
        (JUNCTION, GeographicChainPinReason::Junction),
        (INCIDENCE_CHANGE, GeographicChainPinReason::IncidenceChange),
        (POLE_TRANSITION, GeographicChainPinReason::PoleTransition),
        (GERM_TRANSITION, GeographicChainPinReason::GermTransition),
        (SOURCE_ANCHOR, GeographicChainPinReason::SourceAnchor),
    ] {
        if bits & bit != 0 {
            reasons.push(reason);
        }
    }
    reasons
}

fn continuation_at_node(
    topology: &ExactGeographicTopology,
    node: QNodeId,
    current_span: usize,
    pinned: &[bool],
    boundary_spans: &[bool],
    node_degrees: &[usize],
    offsets: &[usize],
    uses: &[SpanRowRingUse],
    work: &mut impl Work,
) -> Result<Option<usize>> {
    let node_index = node.0 as usize;
    if pinned.get(node_index).copied().unwrap_or(true)
        || node_degrees.get(node_index).copied() != Some(2)
    {
        return Ok(None);
    }
    let incident = incident_boundary_spans(topology, node, boundary_spans, work)?;
    let [first, second] = incident.as_slice() else {
        return Ok(None);
    };
    let other = if *first == current_span {
        *second
    } else if *second == current_span {
        *first
    } else {
        return Err(GeometryErrorKind::Invalid(
            "Q degree-two continuation does not contain its incoming span".into(),
        )
        .into());
    };
    if !span_signatures_continue(topology, current_span, other, node, offsets, uses, work)? {
        return Ok(None);
    }
    Ok(Some(other))
}

fn directed_chain_span(
    topology: &ExactGeographicTopology,
    span_index: usize,
    from_node: QNodeId,
) -> Result<GeographicSharedChainSpan> {
    let span = topology
        .q
        .spans
        .get(span_index)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q shared-chain span is unknown".into()))?;
    let [start_node, end_node] = span.nodes;
    let [start_port, end_port] = span.ports;
    let (to_node, from_port, to_port, forward) = if from_node == start_node {
        (end_node, start_port, end_port, true)
    } else if from_node == end_node {
        (start_node, end_port, start_port, false)
    } else {
        return Err(GeometryErrorKind::Invalid(
            "shared-chain traversal enters a span at a nonincident node".into(),
        )
        .into());
    };
    Ok(GeographicSharedChainSpan {
        span: span.id,
        from_node,
        to_node,
        from_port,
        to_port,
        forward,
    })
}

fn walk_chain(
    topology: &ExactGeographicTopology,
    start_span: usize,
    start_node: QNodeId,
    visited: &mut [bool],
    pinned: &[bool],
    boundary_spans: &[bool],
    node_degrees: &[usize],
    offsets: &[usize],
    uses: &[SpanRowRingUse],
    work: &mut impl Work,
) -> Result<(Vec<GeographicSharedChainSpan>, bool)> {
    let mut chain = Vec::new();
    let mut current_span = start_span;
    let mut from_node = start_node;
    loop {
        work.charge(1)?;
        let visited_slot = visited.get_mut(current_span).ok_or_else(|| {
            GeometryErrorKind::Invalid("shared-chain visited index is out of range".into())
        })?;
        if std::mem::replace(visited_slot, true) {
            return Err(GeometryErrorKind::Invalid(
                "shared-chain traversal revisited a span before closing".into(),
            )
            .into());
        }
        let directed = directed_chain_span(topology, current_span, from_node)?;
        let next_node = directed.to_node;
        let next = continuation_at_node(
            topology,
            next_node,
            current_span,
            pinned,
            boundary_spans,
            node_degrees,
            offsets,
            uses,
            work,
        )?;
        chain.push(directed);
        let Some(next_span) = next else {
            let closed = chain
                .first()
                .zip(chain.last())
                .is_some_and(|(first, last)| first.from_node == last.to_node);
            return Ok((chain, closed));
        };
        if next_span == start_span {
            if next_node != start_node {
                return Err(GeometryErrorKind::Invalid(
                    "shared-chain cycle closes at the wrong endpoint".into(),
                )
                .into());
            }
            return Ok((chain, true));
        }
        if visited.get(next_span).copied().unwrap_or(true) {
            return Err(GeometryErrorKind::Invalid(
                "shared-chain traversal joins an already visited span".into(),
            )
            .into());
        }
        current_span = next_span;
        from_node = next_node;
    }
}

fn reverse_chain(chain: &mut Vec<GeographicSharedChainSpan>) {
    chain.reverse();
    for span in chain {
        std::mem::swap(&mut span.from_node, &mut span.to_node);
        std::mem::swap(&mut span.from_port, &mut span.to_port);
        span.forward = !span.forward;
    }
}

fn compare_chain_span_order(
    left: &[GeographicSharedChainSpan],
    right: &[GeographicSharedChainSpan],
) -> Ordering {
    left.iter()
        .map(|span| (span.span, span.forward))
        .cmp(right.iter().map(|span| (span.span, span.forward)))
}

fn canonicalize_open_chain(chain: &mut Vec<GeographicSharedChainSpan>) -> Result<()> {
    let first = chain
        .first()
        .ok_or_else(|| GeometryErrorKind::Invalid("geographic shared chain is empty".into()))?;
    let last = chain
        .last()
        .ok_or_else(|| GeometryErrorKind::Invalid("geographic shared chain is empty".into()))?;
    if first.from_node == last.to_node {
        return Err(GeometryErrorKind::Invalid(
            "closed chain was passed to open-chain canonicalization".into(),
        )
        .into());
    }
    if last.to_node < first.from_node {
        reverse_chain(chain);
    }
    Ok(())
}

fn canonicalize_cycle(
    mut chain: Vec<GeographicSharedChainSpan>,
    work: &mut impl Work,
) -> Result<Vec<GeographicSharedChainSpan>> {
    if chain.is_empty() {
        return Err(
            GeometryErrorKind::Invalid("geographic shared-chain cycle is empty".into()).into(),
        );
    }
    let pivot = chain
        .iter()
        .enumerate()
        .min_by_key(|(_, span)| (span.from_node, span.span))
        .map(|(index, _)| index)
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("geographic shared-chain cycle has no anchor".into())
        })?;
    chain.rotate_left(pivot);
    let mut reverse = chain.clone();
    reverse_chain(&mut reverse);
    let reverse_pivot = reverse
        .iter()
        .enumerate()
        .min_by_key(|(_, span)| (span.from_node, span.span))
        .map(|(index, _)| index)
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("reversed geographic chain has no anchor".into())
        })?;
    reverse.rotate_left(reverse_pivot);
    let compare_work = chain
        .len()
        .checked_add(reverse.len())
        .ok_or(ResourceError::SizeOverflow)?;
    work.charge(u64::try_from(compare_work).map_err(|_| ResourceError::SizeOverflow)?)?;
    if compare_chain_span_order(&reverse, &chain).is_lt() {
        Ok(reverse)
    } else {
        Ok(chain)
    }
}

fn chain_row_uses(
    chain_spans: &[GeographicSharedChainSpan],
    offsets: &[usize],
    span_uses: &[SpanRowRingUse],
    work: &mut impl Work,
) -> Result<Vec<GeographicSharedChainUse>> {
    let mut pending = Vec::new();
    for (chain_span_index, chain_span) in chain_spans.iter().enumerate() {
        let span_index = chain_span.span.0 as usize;
        for use_ in span_use_range(span_index, offsets, span_uses)? {
            work.charge(1)?;
            pending.push(PendingChainRootUse {
                key: ChainUseKey {
                    row: use_.key.row,
                    region_root: use_.key.region_root,
                    ring: use_.key.ring,
                    hole: use_.key.hole,
                    source_forward: use_.key.source_forward,
                    signed_forward: use_.boundary_forward == chain_span.forward,
                },
                chain_span_index,
                owner_index: use_.owner_index,
            });
        }
    }
    try_sort_by_with_work(&mut pending, work, |left, right, _work| {
        Ok(left
            .key
            .cmp(&right.key)
            .then(left.chain_span_index.cmp(&right.chain_span_index))
            .then(left.owner_index.cmp(&right.owner_index)))
    })?;

    let mut output = Vec::new();
    let mut cursor = 0_usize;
    while cursor < pending.len() {
        work.charge(1)?;
        let key = pending
            .get(cursor)
            .ok_or_else(|| GeometryErrorKind::Invalid("chain use table is incomplete".into()))?
            .key;
        let start = cursor;
        while pending.get(cursor).is_some_and(|use_| use_.key == key) {
            work.charge(1)?;
            cursor = cursor.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
        }
        let roots = pending
            .get(start..cursor)
            .ok_or_else(|| GeometryErrorKind::Invalid("chain root range is incomplete".into()))?;
        let mut distinct_spans = 0_usize;
        let mut previous_span = None;
        let mut root_uses = Vec::with_capacity(roots.len());
        for root in roots {
            work.charge(1)?;
            if previous_span != Some(root.chain_span_index) {
                distinct_spans = distinct_spans
                    .checked_add(1)
                    .ok_or(ResourceError::SizeOverflow)?;
                previous_span = Some(root.chain_span_index);
            }
            root_uses.push(GeographicSharedChainRootUse {
                chain_span_index: root.chain_span_index,
                owner_index: root.owner_index,
            });
        }
        if distinct_spans != chain_spans.len() {
            return Err(GeometryErrorKind::Invalid(
                "a shared-chain row-ring use is missing a source span".into(),
            )
            .into());
        }
        output.push(GeographicSharedChainUse {
            row: key.row,
            region_root: key.region_root,
            ring: key.ring,
            hole: key.hole,
            source_forward: key.source_forward,
            signed_forward: key.signed_forward,
            root_uses,
        });
    }
    Ok(output)
}

fn push_chain(
    spans: Vec<GeographicSharedChainSpan>,
    closed: bool,
    offsets: &[usize],
    span_uses: &[SpanRowRingUse],
    span_to_chain: &mut [Option<NonZeroUsize>],
    chains: &mut Vec<GeographicSharedChain>,
    work: &mut impl Work,
) -> Result<()> {
    if spans.is_empty() {
        return Err(GeometryErrorKind::Invalid("geographic shared chain is empty".into()).into());
    }
    let encoded_id = chains
        .len()
        .checked_add(1)
        .and_then(NonZeroUsize::new)
        .ok_or(ResourceError::SizeOverflow)?;
    for span in &spans {
        work.charge(1)?;
        let slot = span_to_chain.get_mut(span.span.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("shared-chain span index is out of range".into())
        })?;
        if slot.replace(encoded_id).is_some() {
            return Err(GeometryErrorKind::Invalid(
                "a Q source span belongs to multiple shared chains".into(),
            )
            .into());
        }
    }
    let row_uses = chain_row_uses(&spans, offsets, span_uses, work)?;
    chains.push(GeographicSharedChain {
        closed,
        spans,
        row_uses,
    });
    Ok(())
}

fn collect_open_chains(
    topology: &ExactGeographicTopology,
    visited: &mut [bool],
    pinned: &[bool],
    boundary_spans: &[bool],
    node_degrees: &[usize],
    offsets: &[usize],
    uses: &[SpanRowRingUse],
    span_to_chain: &mut [Option<NonZeroUsize>],
    chains: &mut Vec<GeographicSharedChain>,
    work: &mut impl Work,
) -> Result<()> {
    for (node_index, &is_pinned) in pinned.iter().enumerate() {
        work.charge(1)?;
        if !is_pinned {
            continue;
        }
        let node = QNodeId(u32::try_from(node_index).map_err(|_| ResourceError::SizeOverflow)?);
        let incident = incident_boundary_spans(topology, node, boundary_spans, work)?;
        for span_index in incident {
            if visited.get(span_index).copied().unwrap_or(true) {
                continue;
            }
            let (mut spans, closed) = walk_chain(
                topology,
                span_index,
                node,
                visited,
                pinned,
                boundary_spans,
                node_degrees,
                offsets,
                uses,
                work,
            )?;
            if closed {
                spans = canonicalize_cycle(spans, work)?;
            } else {
                canonicalize_open_chain(&mut spans)?;
            }
            push_chain(spans, closed, offsets, uses, span_to_chain, chains, work)?;
        }
    }
    Ok(())
}

fn collect_closed_chains(
    topology: &ExactGeographicTopology,
    visited: &mut [bool],
    pinned: &[bool],
    boundary_spans: &[bool],
    node_degrees: &[usize],
    offsets: &[usize],
    uses: &[SpanRowRingUse],
    span_to_chain: &mut [Option<NonZeroUsize>],
    chains: &mut Vec<GeographicSharedChain>,
    work: &mut impl Work,
) -> Result<()> {
    for (span_index, &is_boundary) in boundary_spans.iter().enumerate() {
        work.charge(1)?;
        if !is_boundary || visited.get(span_index).copied().unwrap_or(true) {
            continue;
        }
        let span = topology.q.spans.get(span_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("unvisited Q boundary span is unknown".into())
        })?;
        let [start_node, _] = span.nodes;
        let (mut spans, closed) = walk_chain(
            topology,
            span_index,
            start_node,
            visited,
            pinned,
            boundary_spans,
            node_degrees,
            offsets,
            uses,
            work,
        )?;
        if !closed {
            return Err(GeometryErrorKind::Invalid(
                "an unpinned geographic boundary component is not a cycle".into(),
            )
            .into());
        }
        spans = canonicalize_cycle(spans, work)?;
        push_chain(spans, true, offsets, uses, span_to_chain, chains, work)?;
    }
    Ok(())
}

impl GeographicSharedChainInventory<'_> {
    pub(in crate::geometry::topology) fn source_owner(
        &self,
        chain_index: usize,
        use_index: usize,
        root_index: usize,
    ) -> Result<&QSpanOwner> {
        let chain = self.chains.get(chain_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("shared-chain index is out of range".into())
        })?;
        let row_use = chain.row_uses.get(use_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("shared-chain row-use index is out of range".into())
        })?;
        let root_use = row_use.root_uses.get(root_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("shared-chain root-use index is out of range".into())
        })?;
        let chain_span = chain.spans.get(root_use.chain_span_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("shared-chain span reference is out of range".into())
        })?;
        self.topology
            .q
            .spans
            .get(chain_span.span.0 as usize)
            .and_then(|span| span.owners.get(root_use.owner_index))
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("shared-chain source root is out of range".into()).into()
            })
    }
}

#[cfg(test)]
mod tests {
    use super::super::analyze_geographic_coverage;
    use super::*;
    use crate::geographic::GeographicFrame;
    use crate::geometry::Unmetered;

    #[test]
    fn shared_chain_keeps_opposite_row_uses_and_exact_subspan_roots() {
        let rows = [
            crate::io::parse_wkt("POLYGON ((0 0, 2 0, 2 1, 2 2, 0 2, 0 0))")
                .expect("first coverage polygon"),
            crate::io::parse_wkt("POLYGON ((2 0, 3 0, 3 2, 2 2, 2 0))")
                .expect("second coverage polygon"),
        ];
        let analysis =
            analyze_geographic_coverage(&rows, GeographicFrame::crs84(), 0.0, "coverage_simplify")
                .expect("geographic coverage admits");
        let mut work = Unmetered;
        let inventory = analysis
            .shared_chain_inventory(&mut work)
            .expect("shared-chain inventory builds");
        let (chain_index, chain) = inventory
            .chains
            .iter()
            .enumerate()
            .find(|(_, chain)| {
                chain.spans.len() == 2
                    && chain.row_uses.iter().any(|use_| use_.row == SourceRowId(0))
                    && chain.row_uses.iter().any(|use_| use_.row == SourceRowId(1))
            })
            .expect("subdivided shared interface is one maximal chain");

        assert!(!chain.closed);
        assert!(chain.spans.windows(2).all(|pair| {
            pair.first()
                .zip(pair.get(1))
                .is_some_and(|(left, right)| left.to_node == right.from_node)
        }));
        assert!(
            chain
                .spans
                .first()
                .zip(chain.spans.last())
                .is_some_and(|(first, last)| first.from_node < last.to_node)
        );
        let middle = chain
            .spans
            .first()
            .expect("chain has its first Q span")
            .to_node;
        assert!(
            inventory
                .pinned_nodes
                .iter()
                .all(|pinned| pinned.node != middle),
            "a degree-two source subdivision must not split the maximal chain",
        );
        let endpoints = chain
            .spans
            .first()
            .zip(chain.spans.last())
            .map(|(first, last)| [first.from_node, last.to_node])
            .expect("open chain has both endpoints");
        assert!(endpoints.iter().all(|endpoint| {
            inventory.pinned_nodes.iter().any(|node| {
                node.node == *endpoint && node.reasons.contains(&GeographicChainPinReason::Junction)
            })
        }));

        let first_use_index = chain
            .row_uses
            .iter()
            .position(|use_| use_.row == SourceRowId(0))
            .expect("first row uses the shared chain");
        let second_use_index = chain
            .row_uses
            .iter()
            .position(|use_| use_.row == SourceRowId(1))
            .expect("second row uses the shared chain");
        let first_use = chain
            .row_uses
            .get(first_use_index)
            .expect("first row use remains present");
        let second_use = chain
            .row_uses
            .get(second_use_index)
            .expect("second row use remains present");
        assert!(first_use.signed_forward);
        assert!(!second_use.signed_forward);
        assert_eq!(first_use.root_uses.len(), 2);
        assert_eq!(second_use.root_uses.len(), 2);

        let zero = crate::geometry::tessellation::exact::ExactRational::from_f64(0.0);
        let half = crate::geometry::tessellation::exact::ExactRational::from_f64(0.5);
        let one = crate::geometry::tessellation::exact::ExactRational::from_f64(1.0);
        let mut first_row_edges = Vec::new();
        let mut second_row_ranges = Vec::new();
        for (use_index, row_use) in [(first_use_index, first_use), (second_use_index, second_use)] {
            for (root_index, _) in row_use.root_uses.iter().enumerate() {
                let owner = inventory
                    .source_owner(chain_index, use_index, root_index)
                    .expect("row use points to its exact Q source root");
                if row_use.row == SourceRowId(0) {
                    assert!(owner.source_range.start.is_zero());
                    assert!(
                        owner
                            .source_range
                            .end
                            .try_cmp(&one)
                            .expect("exact rational comparison succeeds")
                            .is_eq()
                    );
                    first_row_edges.push(owner.source_edge);
                } else {
                    second_row_ranges.push((
                        owner
                            .source_range
                            .start
                            .try_cmp(&zero)
                            .expect("exact rational comparison succeeds")
                            .is_eq(),
                        owner
                            .source_range
                            .start
                            .try_cmp(&half)
                            .expect("exact rational comparison succeeds")
                            .is_eq(),
                        owner
                            .source_range
                            .end
                            .try_cmp(&half)
                            .expect("exact rational comparison succeeds")
                            .is_eq(),
                        owner
                            .source_range
                            .end
                            .try_cmp(&one)
                            .expect("exact rational comparison succeeds")
                            .is_eq(),
                    ));
                }
            }
        }
        assert_ne!(
            first_row_edges.first(),
            first_row_edges.get(1),
            "the subdivided source ring contributes its two original edge roots",
        );
        assert_eq!(second_row_ranges.len(), 2);
        assert!(second_row_ranges.contains(&(true, false, true, false)));
        assert!(second_row_ranges.contains(&(false, true, false, true)));
    }

    #[test]
    fn shared_chain_inventory_pins_ring_anchors_and_pole_nodes() {
        let rows = [
            crate::io::parse_wkt("POLYGON ((-10 80, 10 80, 10 90, -10 90, -10 80))")
                .expect("polar coverage polygon"),
        ];
        let analysis =
            analyze_geographic_coverage(&rows, GeographicFrame::crs84(), 0.0, "coverage_simplify")
                .expect("polar geographic coverage admits");
        let mut work = Unmetered;
        let inventory = analysis
            .shared_chain_inventory(&mut work)
            .expect("polar shared-chain inventory builds");
        assert_eq!(inventory.source_anchors.len(), 1);
        let anchor = inventory
            .source_anchors
            .first()
            .expect("the polar ring has one source anchor");
        assert!(inventory.pinned_nodes.iter().any(|node| {
            node.node == anchor.node
                && node
                    .reasons
                    .contains(&GeographicChainPinReason::SourceAnchor)
        }));
        assert!(inventory.pinned_nodes.iter().any(|node| {
            node.reasons
                .contains(&GeographicChainPinReason::PoleTransition)
        }));
        assert!(inventory.pinned_nodes.iter().any(|node| {
            node.reasons
                .contains(&GeographicChainPinReason::GermTransition)
        }));
    }

    #[test]
    fn cut_port_aliases_do_not_split_shared_chains() {
        let rows = [crate::io::parse_wkt(
            "POLYGON ((-180 20, -90 20, 0 20, 90 20, 180 20, \
             180 30, 90 30, 0 30, -90 30, -180 30, -180 20))",
        )
        .expect("full-longitude band")];
        let analysis =
            analyze_geographic_coverage(&rows, GeographicFrame::crs84(), 0.0, "coverage_simplify")
                .expect("full-longitude coverage admits");
        let mut work = Unmetered;
        let inventory = analysis
            .shared_chain_inventory(&mut work)
            .expect("full-longitude shared-chain inventory builds");
        assert!(inventory.chains.iter().any(|chain| {
            chain.spans.windows(2).any(|pair| {
                let Some((left, right)) = pair.first().zip(pair.get(1)) else {
                    return false;
                };
                let left_cut = analysis
                    .topology
                    .q
                    .ports
                    .get(left.to_port.0 as usize)
                    .is_some_and(|port| port.cut);
                let right_cut = analysis
                    .topology
                    .q
                    .ports
                    .get(right.from_port.0 as usize)
                    .is_some_and(|port| port.cut);
                left.to_node == right.from_node
                    && left.to_port != right.from_port
                    && (left_cut || right_cut)
            })
        }));
    }

    #[test]
    fn shared_chain_roots_retain_nonzero_longitude_lifts() {
        let rows = [crate::io::parse_wkt(
            "POLYGON ((-180 20, -90 20, 0 20, 90 20, 180 20, \
             180 30, 90 30, 0 30, -90 30, -180 30, -180 20))",
        )
        .expect("full-longitude band")];
        let analysis =
            analyze_geographic_coverage(&rows, GeographicFrame::crs84(), 0.0, "coverage_simplify")
                .expect("full-longitude coverage admits");
        let mut work = Unmetered;
        let inventory = analysis
            .shared_chain_inventory(&mut work)
            .expect("full-longitude shared-chain inventory builds");
        let mut has_nonzero_lift = false;
        for (chain_index, chain) in inventory.chains.iter().enumerate() {
            for (use_index, row_use) in chain.row_uses.iter().enumerate() {
                for (root_index, _) in row_use.root_uses.iter().enumerate() {
                    has_nonzero_lift |= inventory
                        .source_owner(chain_index, use_index, root_index)
                        .expect("row use points to its exact Q source root")
                        .lift
                        != 0;
                }
            }
        }
        assert!(has_nonzero_lift);
    }

    #[test]
    fn shared_chain_inventory_charges_the_supplied_work_budget() {
        let rows =
            [crate::io::parse_wkt("POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))")
                .expect("coverage polygon")];
        let analysis =
            analyze_geographic_coverage(&rows, GeographicFrame::crs84(), 0.0, "coverage_simplify")
                .expect("geographic coverage admits");
        let mut work =
            crate::geometry::embedding_search::EvaluationBudget::with_remaining_ticks_for_test(
                "coverage_simplify",
                0,
                0,
            )
            .expect("empty evaluation budget");
        let error = match analysis.shared_chain_inventory(&mut work) {
            Err(error) => error,
            Ok(_) => panic!("shared-chain inventory consumes work"),
        };
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(GeometryErrorKind::ExactEmbeddingSearchLimit {
                operation: "coverage_simplify",
                ..
            })
        ));
    }
}
