#![expect(
    clippy::arithmetic_side_effects,
    reason = "Q source ordinals and incidence offsets are derived from checked inventory ranges and bounded fan walks"
)]

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use super::super::exact::{
    ExactChartPoint, ExactQuotientKey, SourceEdgeId, SourceEdgeKind, SourceLineId, SourcePolygonId,
    SourceRingId,
};
use super::{ChartSpan, ExactGeographicTopology, QNodeId, QNodeKey, QSpanOwner};
use crate::error::Result;
use crate::geographic::{GeographicFrame, SeamIdentity};
use crate::geometry::predicates::collect_duplicate_points;
use crate::geometry::segments::ordering::try_sort_by;
use crate::geometry::tessellation::exact::{ExactDyadic, ExactRational};
use crate::geometry::{GeometryErrorKind, HashSet, Point, PointKey, Shape, ValidationIssue};

mod connectivity;
mod crossings;
mod faces;
mod incidence;
mod shared_spans;

use self::connectivity::connectivity_issues;
use self::crossings::crossing_issues;
use self::faces::face_issues;
use self::incidence::incidence_issues;
use self::shared_spans::{is_declared_pole_pair_span, opposite_seam_interiors, shared_span_issues};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum IssueKind {
    RingCollapsed,
    SelfIntersection,
    RingIntersection,
    SharedLinework,
    InteriorDisconnected,
    HoleOutsideShell,
    HoleOverlap,
    PolygonOverlap,
}

impl IssueKind {
    const fn rank(self) -> u8 {
        match self {
            Self::RingCollapsed | Self::SelfIntersection => 0,
            Self::RingIntersection => 1,
            Self::SharedLinework => 2,
            Self::InteriorDisconnected => 3,
            Self::HoleOutsideShell => 4,
            Self::HoleOverlap => 5,
            Self::PolygonOverlap => 6,
        }
    }

    const fn code(self) -> &'static str {
        match self {
            Self::RingCollapsed => "ring_collapsed",
            Self::SelfIntersection => "self_intersection",
            Self::RingIntersection => "ring_intersection",
            Self::SharedLinework => "shared_linework",
            Self::InteriorDisconnected => "interior_disconnected",
            Self::HoleOutsideShell => "hole_outside_shell",
            Self::HoleOverlap => "hole_overlap",
            Self::PolygonOverlap => "polygon_overlap",
        }
    }
}

struct Candidate {
    kind: IssueKind,
    polygon: SourcePolygonId,
    other_polygon: Option<SourcePolygonId>,
    rings: Option<(SourceRingId, Option<SourceRingId>)>,
    witness: ExactChartPoint,
}

pub(super) fn issue(
    topology: &ExactGeographicTopology,
    shape: &Shape,
) -> Result<Option<ValidationIssue>> {
    if topology.sources.row_count != 1 {
        return Err(GeometryErrorKind::Invalid(
            "geographic validity requires one source row".into(),
        )
        .into());
    }

    let mut best = None;
    let rules = ring_rules(topology)?;
    for (index, rule) in rules.iter().enumerate() {
        if rule.null_edges.iter().all(|&is_null| is_null) {
            let ring = topology.sources.rings.get(index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q ring rules have no source ring".into())
            })?;
            offer(
                &mut best,
                Candidate {
                    kind: IssueKind::RingCollapsed,
                    polygon: ring.polygon,
                    other_polygon: None,
                    rings: Some((ring.id, None)),
                    witness: ring_witness(topology, ring.id)?,
                },
                &topology.sources,
            )?;
        }
    }

    shared_span_issues(topology, &rules, &mut best)?;
    incidence_issues(topology, &rules, &mut best)?;
    crossing_issues(topology, &mut best)?;
    connectivity_issues(topology, &rules, &mut best)?;
    face_issues(topology, &mut best)?;

    best.map(|candidate| make_issue(topology, shape, &candidate))
        .transpose()
}

pub(super) fn self_intersection_points(
    topology: &ExactGeographicTopology,
    shape: &Shape,
) -> Result<Vec<Point>> {
    let rules = ring_rules(topology)?;
    let period = ExactRational::from_f64(topology.sources.frame.longitude().period());
    let line_boundaries = line_boundary_nodes(topology)?;
    let line_vertex_classes = line_vertex_classes(topology, &line_boundaries)?;
    let mut nodes = BTreeSet::new();
    let mut cross_ring_chart_cuts = BTreeSet::new();
    let mut cut_ring_nodes = BTreeSet::new();
    let mut rings_at_node: BTreeMap<QNodeId, BTreeSet<SourceRingId>> = BTreeMap::new();
    let mut incidences: BTreeMap<(QNodeId, SourceRingId), Vec<ExactRational>> = BTreeMap::new();
    let mut line_positions: BTreeMap<(QNodeId, SourceLineId), Vec<LinePosition>> = BTreeMap::new();

    for (span_index, span) in topology.q.spans.iter().enumerate() {
        if topology.q.span_is_collapsed_pole(span_index)? {
            continue;
        }
        let mut ignored = BTreeSet::new();
        if is_declared_pole_pair_span(span, topology, &rules)? {
            for owner in &span.owners {
                ignored.insert(owner.source_edge);
                if let SourceEdgeKind::Ring { ring, .. } =
                    source_edge(topology, owner.source_edge)?.kind
                {
                    for node in span.nodes {
                        cut_ring_nodes.insert((node, ring));
                    }
                }
            }
        }
        for (left_index, left) in span.owners.iter().enumerate() {
            let left_edge = source_edge(topology, left.source_edge)?;
            for right in span.owners.iter().skip(left_index).skip(1) {
                let right_edge = source_edge(topology, right.source_edge)?;
                match (left_edge.kind, right_edge.kind) {
                    (
                        SourceEdgeKind::Ring {
                            ring: left_ring, ..
                        },
                        SourceEdgeKind::Ring {
                            ring: right_ring, ..
                        },
                    ) if left_ring == right_ring => {
                        let record = topology
                            .sources
                            .rings
                            .get(left_ring.0 as usize)
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "Q span has an unknown source ring".into(),
                                )
                            })?;
                        let same_ring_cut = same_ring_seam_lift(record, left, right);
                        let pole_pair = is_pole_pair_excursion(
                            &rules,
                            left_ring,
                            left.source_edge,
                            right.source_edge,
                        );
                        if same_ring_cut || pole_pair {
                            ignored.insert(left.source_edge);
                            ignored.insert(right.source_edge);
                            for node in span.nodes {
                                cut_ring_nodes.insert((node, left_ring));
                            }
                        } else {
                            nodes.extend(span.nodes);
                        }
                    },
                    (
                        SourceEdgeKind::Ring {
                            ring: left_ring, ..
                        },
                        SourceEdgeKind::Ring {
                            ring: right_ring, ..
                        },
                    ) => {
                        if opposite_seam_interiors(topology, span, left, right) {
                            ignored.insert(left.source_edge);
                            ignored.insert(right.source_edge);
                            for node in span.nodes {
                                cross_ring_chart_cuts
                                    .insert(ordered_rings(node, left_ring, right_ring));
                                cut_ring_nodes.insert((node, left_ring));
                                cut_ring_nodes.insert((node, right_ring));
                            }
                        } else {
                            nodes.extend(span.nodes);
                        }
                    },
                    _ => {
                        if left.source_edge != right.source_edge
                            || left.source_range != right.source_range
                        {
                            nodes.extend(span.nodes);
                        }
                    },
                }
            }
        }

        for owner in &span.owners {
            let edge = source_edge(topology, owner.source_edge)?;
            match edge.kind {
                SourceEdgeKind::Ring { ring, .. } => {
                    for node in span.nodes {
                        rings_at_node.entry(node).or_default().insert(ring);
                    }
                    if ignored.contains(&owner.source_edge) {
                        continue;
                    }
                    let shift = ExactRational::from_dyadic(ExactDyadic::from_i64(owner.lift))
                        .try_product(&period)?;
                    for (node, port_id) in span.nodes.into_iter().zip(span.ports) {
                        let port = topology.q.ports.get(port_id.0 as usize).ok_or_else(|| {
                            GeometryErrorKind::Invalid("Q span has an unknown endpoint port".into())
                        })?;
                        let x = port.point.x.try_add(&shift)?;
                        incidences.entry((node, ring)).or_default().push(x);
                    }
                },
                SourceEdgeKind::Line { line } => {
                    let line_record = topology
                        .sources
                        .lines
                        .get(line.0 as usize)
                        .filter(|record| record.id == line)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid("Q span has an unknown source line".into())
                        })?;
                    if line_record.row != edge.row {
                        return Err(GeometryErrorKind::Invalid(
                            "Q source line belongs to another row".into(),
                        )
                        .into());
                    }
                    let classes = line_vertex_classes.get(line.0 as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q line has no vertex classes".into())
                    })?;
                    for (endpoint, node) in span.nodes.into_iter().enumerate() {
                        let parameter = owner_parameter(owner, endpoint)?;
                        let position =
                            line_position(line_record, owner.source_edge, &parameter, classes)?;
                        line_positions
                            .entry((node, line))
                            .or_default()
                            .push(position);
                    }
                },
            }
        }
    }

    for (node, rings) in &rings_at_node {
        for (index, &left) in rings.iter().enumerate() {
            for &right in rings.iter().skip(index).skip(1) {
                if !cross_ring_chart_cuts.contains(&ordered_rings(*node, left, right)) {
                    nodes.insert(*node);
                }
            }
        }
    }

    let mut lines_at_node: BTreeMap<QNodeId, BTreeSet<SourceLineId>> = BTreeMap::new();
    for ((node, line), mut positions) in line_positions {
        try_sort_by(&mut positions, compare_line_positions)?;
        positions.dedup();
        if positions.len() > 1 {
            nodes.insert(node);
        }
        lines_at_node.entry(node).or_default().insert(line);
    }

    for (node, lines) in &lines_at_node {
        if rings_at_node.contains_key(node) {
            nodes.insert(*node);
        }
        for (index, &left) in lines.iter().enumerate() {
            for &right in lines.iter().skip(index).skip(1) {
                if !is_open_line_boundary(&line_boundaries, left, *node)
                    || !is_open_line_boundary(&line_boundaries, right, *node)
                {
                    nodes.insert(*node);
                }
            }
        }
    }

    for ((node, ring_id), mut lifted_x) in incidences {
        if cut_ring_nodes.contains(&(node, ring_id)) {
            continue;
        }
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
        let incidence = ring_node_incidence(ring.summary.chart_turn, pole, &mut lifted_x)?;
        if incidence.invalid || incidence.multiple_passes {
            nodes.insert(node);
        }
    }

    let mut witnesses = nodes
        .into_iter()
        .map(|node| node_witness(topology, node))
        .collect::<Result<Vec<_>>>()?;
    try_sort_by(&mut witnesses, compare_witness)?;
    let mut seen = HashSet::default();
    let mut points = Vec::new();
    collect_duplicate_points(shape, &mut |point| {
        if seen.insert(PointKey::new(point)) {
            points.push(point);
        }
    });
    for witness in witnesses {
        let point = exact_point(&witness)?;
        if seen.insert(PointKey::new(point)) {
            points.push(point);
        }
    }
    Ok(points)
}

#[derive(Clone, Debug, Eq, PartialEq)]
enum LinePosition {
    Vertex(usize),
    Interior(SourceEdgeId, ExactRational),
}

fn compare_line_positions(left: &LinePosition, right: &LinePosition) -> Result<Ordering> {
    Ok(match (left, right) {
        (LinePosition::Vertex(left), LinePosition::Vertex(right)) => left.cmp(right),
        (LinePosition::Vertex(_), LinePosition::Interior(..)) => Ordering::Less,
        (LinePosition::Interior(..), LinePosition::Vertex(_)) => Ordering::Greater,
        (
            LinePosition::Interior(left_edge, left_t),
            LinePosition::Interior(right_edge, right_t),
        ) => left_edge.cmp(right_edge).then(left_t.try_cmp(right_t)?),
    })
}

fn line_boundary_nodes(topology: &ExactGeographicTopology) -> Result<Vec<Option<[QNodeId; 2]>>> {
    let mut ports = vec![[None; 2]; topology.sources.lines.len()];
    for boundary in &topology.sources.line_boundaries {
        let line = topology
            .sources
            .lines
            .get(boundary.line.0 as usize)
            .filter(|line| line.id == boundary.line)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q line boundary has no source line".into())
            })?;
        if line.row != boundary.row {
            return Err(GeometryErrorKind::Invalid(
                "Q line boundary belongs to another row".into(),
            )
            .into());
        }
        let edge_ports = topology
            .q
            .source_edge_ports
            .get(boundary.edge.0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q line boundary has no source edge ports".into())
            })?;
        let port_id = if boundary.at_end {
            edge_ports[1]
        } else {
            edge_ports[0]
        };
        let node = topology
            .q
            .ports
            .get(port_id.0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q line boundary has an unknown port".into())
            })?
            .node;
        let boundary_index = usize::from(boundary.at_end);
        let slot = ports
            .get_mut(line.id.0 as usize)
            .and_then(|endpoints| endpoints.get_mut(boundary_index))
            .ok_or_else(|| GeometryErrorKind::Invalid("Q line boundary index is invalid".into()))?;
        if slot.is_some_and(|previous| previous != node) {
            return Err(GeometryErrorKind::Invalid(
                "Q line boundary has conflicting endpoint nodes".into(),
            )
            .into());
        }
        *slot = Some(node);
    }
    ports
        .into_iter()
        .map(|endpoints| match endpoints {
            [Some(start), Some(end)] => Ok(Some([start, end])),
            [None, None] => Ok(None),
            _ => Err(GeometryErrorKind::Invalid(
                "Q source line has only one boundary endpoint".into(),
            )
            .into()),
        })
        .collect()
}

fn line_vertex_classes(
    topology: &ExactGeographicTopology,
    boundaries: &[Option<[QNodeId; 2]>],
) -> Result<Vec<Vec<usize>>> {
    topology
        .sources
        .lines
        .iter()
        .map(|line| {
            let edge_count = line.source_edges.len();
            let mut classes = super::UnionFind::new(edge_count.saturating_add(1));
            for (ordinal, source_index) in line.source_edges.clone().enumerate() {
                let edge = topology.sources.edges.get(source_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q source line has an invalid edge range".into())
                })?;
                let [start, end] = source_edge_nodes(topology, edge.id)?;
                let collapsed_longitude = super::super::exact::local_longitude_edge(
                    topology.sources.frame,
                    edge.source[0].x,
                    edge.retained_longitude(),
                )
                .delta_x(topology.sources.frame.longitude().period())?
                .is_zero();
                let collapsed_pole = topology
                    .q
                    .nodes
                    .get(start.0 as usize)
                    .is_some_and(|node| node.pole.is_some());
                if start == end && (collapsed_longitude || collapsed_pole) {
                    classes.union(ordinal, ordinal + 1);
                }
            }
            if boundaries
                .get(line.id.0 as usize)
                .copied()
                .flatten()
                .is_some_and(|[start, end]| start == end)
            {
                classes.union(0, edge_count);
            }
            Ok((0..=edge_count)
                .map(|vertex| classes.find(vertex))
                .collect())
        })
        .collect()
}

fn owner_parameter(owner: &QSpanOwner, endpoint: usize) -> Result<ExactRational> {
    let parameter = match (owner.reversed, endpoint) {
        (false, 0) | (true, 1) => &owner.source_range.start,
        (false, 1) | (true, 0) => &owner.source_range.end,
        _ => {
            return Err(
                GeometryErrorKind::Invalid("Q span endpoint index is invalid".into()).into(),
            );
        },
    };
    Ok(parameter.clone())
}

fn line_position(
    line: &super::super::exact::ExactSourceLine,
    edge: SourceEdgeId,
    parameter: &ExactRational,
    vertex_classes: &[usize],
) -> Result<LinePosition> {
    let edge_index = edge.0 as usize;
    let ordinal = edge_index
        .checked_sub(line.source_edges.start)
        .filter(|&ordinal| ordinal < line.source_edges.len())
        .ok_or_else(|| GeometryErrorKind::Invalid("Q source edge is outside its line".into()))?;
    if parameter.try_cmp(&ExactRational::zero())?.is_eq() {
        return vertex_classes
            .get(ordinal)
            .copied()
            .map(LinePosition::Vertex)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q line start vertex is unknown".into()).into()
            });
    }
    if parameter.try_cmp(&ExactRational::one())?.is_eq() {
        return vertex_classes
            .get(ordinal + 1)
            .copied()
            .map(LinePosition::Vertex)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q line end vertex is unknown".into()).into()
            });
    }
    Ok(LinePosition::Interior(edge, parameter.clone()))
}

fn is_open_line_boundary(
    boundaries: &[Option<[QNodeId; 2]>],
    line: SourceLineId,
    node: QNodeId,
) -> bool {
    boundaries
        .get(line.0 as usize)
        .copied()
        .flatten()
        .is_some_and(|[start, end]| start != end && (start == node || end == node))
}

fn ordered_rings(
    node: QNodeId,
    left: SourceRingId,
    right: SourceRingId,
) -> (QNodeId, SourceRingId, SourceRingId) {
    if left <= right {
        (node, left, right)
    } else {
        (node, right, left)
    }
}

#[derive(Default)]
struct RingRules {
    null_edges: Vec<bool>,
    excursion_pairs: BTreeSet<(SourceEdgeId, SourceEdgeId)>,
}

fn ring_rules(topology: &ExactGeographicTopology) -> Result<Vec<RingRules>> {
    let frame = topology.sources.frame;
    let mut all = Vec::with_capacity(topology.sources.rings.len());
    for ring in &topology.sources.rings {
        let edges = topology
            .sources
            .edges
            .get(ring.source_edges.clone())
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q source ring has an invalid edge range".into())
            })?;
        let mut null_edges = Vec::with_capacity(edges.len());
        for edge in edges {
            let [start_node, end_node] = source_edge_nodes(topology, edge.id)?;
            let delta = super::super::exact::local_longitude_edge(
                frame,
                edge.source[0].x,
                edge.retained_longitude(),
            )
            .delta_x(frame.longitude().period())?;
            let collapsed = start_node == end_node
                && (delta.is_zero()
                    || topology
                        .q
                        .nodes
                        .get(start_node.0 as usize)
                        .is_some_and(|node| node.pole.is_some()));
            null_edges.push(collapsed);
        }
        let mut excursion_pairs = BTreeSet::new();
        if edges.len() >= 3 {
            for start in 0..edges.len() {
                let middle = start.wrapping_add(1) % edges.len();
                let end = start.wrapping_add(2) % edges.len();
                if pole_pair_excursion(edges, start, middle, end, frame) {
                    for edge in [start, middle, end] {
                        *null_edges.get_mut(edge).ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "pole-pair excursion has an unknown ring edge".into(),
                            )
                        })? = true;
                    }
                    let first = edges
                        .get(start)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "pole-pair excursion has no first edge".into(),
                            )
                        })?
                        .id;
                    let last = edges
                        .get(end)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "pole-pair excursion has no last edge".into(),
                            )
                        })?
                        .id;
                    excursion_pairs.insert(ordered_edges(first, last));
                }
            }
        }
        all.push(RingRules {
            null_edges,
            excursion_pairs,
        });
    }
    Ok(all)
}

fn pole_pair_excursion(
    edges: &[super::super::exact::ExactSourceEdge],
    first: usize,
    middle: usize,
    last: usize,
    frame: GeographicFrame,
) -> bool {
    let Some(first_edge) = edges.get(first) else {
        return false;
    };
    let Some(pair_edge) = edges.get(middle) else {
        return false;
    };
    let Some(last_edge) = edges.get(last) else {
        return false;
    };
    let [up_start, up_end] = first_edge.source;
    let [pair_start, pair_end] = pair_edge.source;
    let [down_start, down_end] = last_edge.source;
    let pole = frame.pole();
    let axis = frame.longitude();
    let seam = |x: f64| {
        axis.canonicalize(x)
            .is_ok_and(|longitude| longitude.identity(axis) == SeamIdentity::Seam)
    };
    up_start.x.to_bits() == up_end.x.to_bits()
        && down_start.x.to_bits() == down_end.x.to_bits()
        && up_start.y.abs().to_bits() != pole.to_bits()
        && up_end.y.abs().to_bits() == pole.to_bits()
        && pair_start.y.to_bits() == pair_end.y.to_bits()
        && pair_start.y.abs().to_bits() == pole.to_bits()
        && seam(pair_start.x)
        && seam(pair_end.x)
        && pair_start.x.to_bits() != pair_end.x.to_bits()
        && down_start.y.abs().to_bits() == pole.to_bits()
        && down_end.y.abs().to_bits() != pole.to_bits()
}

fn source_edge_nodes(
    topology: &ExactGeographicTopology,
    edge: SourceEdgeId,
) -> Result<[QNodeId; 2]> {
    let ports = topology
        .q
        .source_edge_ports
        .get(edge.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q source edge has no endpoint ports".into()))?;
    let start = topology.q.ports.get(ports[0].0 as usize).ok_or_else(|| {
        GeometryErrorKind::Invalid("Q source edge has an unknown start port".into())
    })?;
    let end = topology.q.ports.get(ports[1].0 as usize).ok_or_else(|| {
        GeometryErrorKind::Invalid("Q source edge has an unknown end port".into())
    })?;
    Ok([start.node, end.node])
}

fn ordered_edges(left: SourceEdgeId, right: SourceEdgeId) -> (SourceEdgeId, SourceEdgeId) {
    if left <= right {
        (left, right)
    } else {
        (right, left)
    }
}

const fn same_ring_seam_lift(
    ring: &super::super::exact::ExactSourceRing,
    left: &QSpanOwner,
    right: &QSpanOwner,
) -> bool {
    ring.summary.chart_turn == 0
        && left.reversed != right.reversed
        && left.lift.abs_diff(right.lift) == 1
}

fn is_pole_pair_excursion(
    rules: &[RingRules],
    ring: SourceRingId,
    left: SourceEdgeId,
    right: SourceEdgeId,
) -> bool {
    rules
        .get(ring.0 as usize)
        .is_some_and(|rule| rule.excursion_pairs.contains(&ordered_edges(left, right)))
}

struct RingNodeIncidence {
    invalid: bool,
    multiple_passes: bool,
}

fn ring_node_incidence(
    chart_turn: i64,
    pole: bool,
    lifted_x: &mut [ExactRational],
) -> Result<RingNodeIncidence> {
    if pole || chart_turn != 0 {
        let invalid = lifted_x.len() > 2;
        return Ok(RingNodeIncidence {
            invalid,
            multiple_passes: invalid,
        });
    }

    try_sort_by(lifted_x, ExactRational::try_cmp)?;
    let mut group_count = 0;
    let mut invalid = false;
    let mut group_start = 0;
    while group_start < lifted_x.len() {
        let first = lifted_x.get(group_start).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q incidence group has no first position".into())
        })?;
        let mut group_end = group_start.saturating_add(1);
        while group_end < lifted_x.len() {
            let next = lifted_x.get(group_end).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q incidence group has no next position".into())
            })?;
            if !first.try_cmp(next)?.is_eq() {
                break;
            }
            group_end += 1;
        }
        group_count += 1;
        invalid |= group_end - group_start != 2;
        group_start = group_end;
    }
    Ok(RingNodeIncidence {
        invalid,
        multiple_passes: group_count > 1,
    })
}

fn offer(
    best: &mut Option<Candidate>,
    candidate: Candidate,
    sources: &super::super::exact::ExactGeographicChartInventory,
) -> Result<()> {
    if let Some(previous) = best {
        if compare_candidates(&candidate, previous, sources)?.is_lt() {
            *previous = candidate;
        }
    } else {
        *best = Some(candidate);
    }
    Ok(())
}

fn compare_candidates(
    left: &Candidate,
    right: &Candidate,
    sources: &super::super::exact::ExactGeographicChartInventory,
) -> Result<Ordering> {
    let left_rings = ring_ordinals(sources, left.rings)?;
    let right_rings = ring_ordinals(sources, right.rings)?;
    let left_key = (
        left.polygon,
        left.kind.rank(),
        left_rings,
        left.other_polygon,
    );
    let right_key = (
        right.polygon,
        right.kind.rank(),
        right_rings,
        right.other_polygon,
    );
    let key_order = left_key.cmp(&right_key);
    if !key_order.is_eq() {
        return Ok(key_order);
    }
    compare_witness(&left.witness, &right.witness)
}

fn ring_ordinals(
    sources: &super::super::exact::ExactGeographicChartInventory,
    rings: Option<(SourceRingId, Option<SourceRingId>)>,
) -> Result<(usize, usize)> {
    let Some((first, second)) = rings else {
        return Ok((usize::MAX, usize::MAX));
    };
    let first_ring = sources.rings.get(first.0 as usize).ok_or_else(|| {
        GeometryErrorKind::Invalid("validity issue references an unknown source ring".into())
    })?;
    let first_polygon = sources
        .polygons
        .get(first_ring.polygon.0 as usize)
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("validity issue ring has an unknown polygon".into())
        })?;
    let first_ordinal = first.0 as usize - first_polygon.rings.start;
    let second_ordinal = if let Some(second) = second {
        let ring = sources.rings.get(second.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("validity issue references an unknown source ring".into())
        })?;
        let polygon = sources
            .polygons
            .get(ring.polygon.0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("validity issue ring has an unknown polygon".into())
            })?;
        second.0 as usize - polygon.rings.start
    } else {
        usize::MAX
    };
    Ok((first_ordinal, second_ordinal))
}

fn compare_witness(left: &ExactChartPoint, right: &ExactChartPoint) -> Result<Ordering> {
    let y = left.y.try_cmp(&right.y)?;
    Ok(if y.is_eq() {
        left.x.try_cmp(&right.x)?
    } else {
        y
    })
}

fn node_witness(topology: &ExactGeographicTopology, node: QNodeId) -> Result<ExactChartPoint> {
    let q_node = topology.q.nodes.get(node.0 as usize).ok_or_else(|| {
        GeometryErrorKind::Invalid("validity witness has an unknown Q node".into())
    })?;
    match &q_node.key {
        QNodeKey::Cut(latitude) => Ok(ExactChartPoint::from_rationals(
            topology.q.cut.clone(),
            latitude.clone(),
        )),
        QNodeKey::Source(source_node) => {
            let source = topology
                .chart
                .nodes
                .get(source_node.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("validity witness has an unknown chart node".into())
                })?;
            let half = ExactRational::from_f64(topology.sources.frame.longitude().half_period());
            match &source.key {
                ExactQuotientKey::Coordinate(point) => Ok(point.clone()),
                ExactQuotientKey::Seam(latitude) => Ok(ExactChartPoint::from_rationals(
                    ExactRational::zero().try_subtract(&half)?,
                    latitude.clone(),
                )),
                ExactQuotientKey::NorthPole => Ok(ExactChartPoint::from_rationals(
                    ExactRational::zero(),
                    ExactRational::from_f64(
                        topology
                            .sources
                            .frame
                            .chart_pole_latitude(crate::geographic::ChartPole::North),
                    ),
                )),
                ExactQuotientKey::SouthPole => Ok(ExactChartPoint::from_rationals(
                    ExactRational::zero(),
                    ExactRational::from_f64(
                        topology
                            .sources
                            .frame
                            .chart_pole_latitude(crate::geographic::ChartPole::South),
                    ),
                )),
            }
        },
    }
}

fn span_witness(topology: &ExactGeographicTopology, span: &ChartSpan) -> Result<ExactChartPoint> {
    let first = node_witness(topology, span.nodes[0])?;
    let second = node_witness(topology, span.nodes[1])?;
    if compare_witness(&first, &second)?.is_le() {
        Ok(first)
    } else {
        Ok(second)
    }
}

fn ring_witness(topology: &ExactGeographicTopology, ring: SourceRingId) -> Result<ExactChartPoint> {
    let record =
        topology.sources.rings.get(ring.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("validity witness has an unknown ring".into())
        })?;
    let mut best = None;
    for edge_index in record.source_edges.clone() {
        let edge = SourceEdgeId(
            u32::try_from(edge_index).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
        );
        let [start, end] = source_edge_nodes(topology, edge)?;
        for node in [start, end] {
            let point = node_witness(topology, node)?;
            if best
                .as_ref()
                .map(|current| compare_witness(&point, current))
                .transpose()?
                .is_none_or(Ordering::is_lt)
            {
                best = Some(point);
            }
        }
    }
    best.ok_or_else(|| GeometryErrorKind::Invalid("validity ring has no Q witness".into()).into())
}

fn exact_point(point: &ExactChartPoint) -> Result<Point> {
    let [min_x, min_y, max_x, max_y] = point.outward_bounds()?;
    Ok(Point::new_raw_xy(
        f64::midpoint(min_x, max_x),
        f64::midpoint(min_y, max_y),
    ))
}

fn make_issue(
    topology: &ExactGeographicTopology,
    shape: &Shape,
    candidate: &Candidate,
) -> Result<ValidationIssue> {
    let detail = match candidate.kind {
        IssueKind::RingCollapsed => "ring collapses to one geographic quotient location".into(),
        IssueKind::SelfIntersection => format!(
            "{} has a self-intersection",
            ring_label(
                &topology.sources,
                candidate.rings.expect("ring issue has a ring").0
            )?
        ),
        IssueKind::RingIntersection if candidate.other_polygon.is_some() => format!(
            "polygons at indices {} and {} overlap",
            candidate.polygon.0,
            candidate
                .other_polygon
                .expect("cross-polygon issue has a second polygon")
                .0
        ),
        IssueKind::RingIntersection => {
            let (left, right) = candidate.rings.expect("ring issue has two rings");
            format!(
                "{} and {} intersect",
                ring_label(&topology.sources, left)?,
                ring_label(
                    &topology.sources,
                    right.expect("ring intersection has a second ring")
                )?
            )
        },
        IssueKind::SharedLinework if candidate.other_polygon.is_some() => format!(
            "polygons at indices {} and {} share linework",
            candidate.polygon.0,
            candidate
                .other_polygon
                .expect("cross-polygon issue has a second polygon")
                .0
        ),
        IssueKind::SharedLinework => {
            let (left, right) = candidate.rings.expect("shared linework has two rings");
            format!(
                "{} and {} intersect on a line",
                ring_label(&topology.sources, left)?,
                ring_label(
                    &topology.sources,
                    right.expect("shared linework has a second ring")
                )?
            )
        },
        IssueKind::InteriorDisconnected => "interior is disconnected".into(),
        IssueKind::HoleOutsideShell => {
            let ring = candidate.rings.expect("outside-hole issue has a ring").0;
            let ordinal = ring_ordinal(&topology.sources, ring)?;
            format!(
                "interior ring at index {} is not contained within the polygon's exterior",
                ordinal.saturating_sub(1)
            )
        },
        IssueKind::HoleOverlap => {
            let (left, right) = candidate.rings.expect("hole overlap has two rings");
            let left = ring_ordinal(&topology.sources, left)?.saturating_sub(1);
            let right = ring_ordinal(
                &topology.sources,
                right.expect("hole overlap has a second ring"),
            )?
            .saturating_sub(1);
            format!(
                "interior ring at index {} and interior ring at index {} intersect on an area",
                left.min(right),
                left.max(right)
            )
        },
        IssueKind::PolygonOverlap => format!(
            "polygons at indices {} and {} overlap",
            candidate.polygon.0,
            candidate
                .other_polygon
                .expect("polygon overlap has a second polygon")
                .0
        ),
    };
    let path = if candidate.other_polygon.is_some() {
        "$".to_owned()
    } else {
        polygon_path(shape, candidate.polygon)?
    };
    Ok(ValidationIssue::new(
        candidate.kind.code(),
        detail,
        Some(exact_point(&candidate.witness)?),
        path,
    ))
}

fn ring_label(
    sources: &super::super::exact::ExactGeographicChartInventory,
    ring: SourceRingId,
) -> Result<String> {
    let ordinal = ring_ordinal(sources, ring)?;
    Ok(if ordinal == 0 {
        "exterior ring".to_owned()
    } else {
        format!("interior ring at index {}", ordinal - 1)
    })
}

fn ring_ordinal(
    sources: &super::super::exact::ExactGeographicChartInventory,
    ring: SourceRingId,
) -> Result<usize> {
    let record = sources.rings.get(ring.0 as usize).ok_or_else(|| {
        GeometryErrorKind::Invalid("validity issue references an unknown source ring".into())
    })?;
    let polygon = sources
        .polygons
        .get(record.polygon.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("source ring has an unknown polygon".into()))?;
    Ok(ring.0 as usize - polygon.rings.start)
}

fn polygon_path(shape: &Shape, target: SourcePolygonId) -> Result<String> {
    fn visit(shape: &Shape, path: &str, target: usize, next: &mut usize) -> Option<String> {
        match shape {
            Shape::Polygon(_) => {
                let ordinal = *next;
                *next += 1;
                (ordinal == target).then(|| path.to_owned())
            },
            Shape::MultiPolygon(polygons) => {
                for (index, _) in polygons.iter().enumerate() {
                    let ordinal = *next;
                    *next += 1;
                    if ordinal == target {
                        return Some(format!("{path}[{index}]"));
                    }
                }
                None
            },
            Shape::GeometryCollection(collection) => {
                collection.iter().enumerate().find_map(|(index, member)| {
                    visit(member, &format!("{path}[{index}]"), target, next)
                })
            },
            Shape::Point(_)
            | Shape::MultiPoint(_)
            | Shape::LineString(_)
            | Shape::MultiLineString(_)
            | Shape::Empty(..) => None,
        }
    }
    let mut next = 0;
    visit(shape, "$", target.0 as usize, &mut next).ok_or_else(|| {
        GeometryErrorKind::Invalid("validity issue has no source polygon path".into()).into()
    })
}

fn source_edge(
    topology: &ExactGeographicTopology,
    edge: SourceEdgeId,
) -> Result<&super::super::exact::ExactSourceEdge> {
    topology.sources.edges.get(edge.0 as usize).ok_or_else(|| {
        GeometryErrorKind::Invalid("Q span has an unknown source edge".into()).into()
    })
}

#[cfg(test)]
mod tests;
