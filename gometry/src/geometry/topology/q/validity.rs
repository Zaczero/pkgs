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
use crate::geometry::{
    GeometryErrorKind, HashSet, Point, PointKey, Shape, ValidationIssue, fan_alternations,
};

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

fn shared_span_issues(
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

fn source_edge(
    topology: &ExactGeographicTopology,
    edge: SourceEdgeId,
) -> Result<&super::super::exact::ExactSourceEdge> {
    topology.sources.edges.get(edge.0 as usize).ok_or_else(|| {
        GeometryErrorKind::Invalid("Q span has an unknown source edge".into()).into()
    })
}

fn is_declared_pole_pair_span(
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

fn opposite_seam_interiors(
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

fn offer_ring_pair(
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

fn incidence_issues(
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

fn crossing_issues(topology: &ExactGeographicTopology, best: &mut Option<Candidate>) -> Result<()> {
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

fn connectivity_issues(
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

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
enum FaceIssue {
    OutsideHole(SourcePolygonId, SourceRingId),
    HoleOverlap(SourcePolygonId, SourceRingId, SourceRingId),
    PolygonOverlap(SourcePolygonId, SourcePolygonId),
}

fn face_issues(topology: &ExactGeographicTopology, best: &mut Option<Candidate>) -> Result<()> {
    let mut issues = BTreeSet::new();
    for face in &topology.q.face_validity {
        if let Some((polygon, ring)) = face.outside_hole {
            issues.insert(FaceIssue::OutsideHole(polygon, ring));
        }
        if let Some((polygon, first, second)) = face.hole_overlap {
            issues.insert(FaceIssue::HoleOverlap(polygon, first, second));
        }
        if let Some((first, second)) = face.polygon_overlap {
            issues.insert(FaceIssue::PolygonOverlap(first, second));
        }
    }
    for issue in issues {
        match issue {
            FaceIssue::OutsideHole(polygon, ring) => offer(
                best,
                Candidate {
                    kind: IssueKind::HoleOutsideShell,
                    polygon,
                    other_polygon: None,
                    rings: Some((ring, None)),
                    witness: ring_witness(topology, ring)?,
                },
                &topology.sources,
            )?,
            FaceIssue::HoleOverlap(polygon, first, second) => offer(
                best,
                Candidate {
                    kind: IssueKind::HoleOverlap,
                    polygon,
                    other_polygon: None,
                    rings: Some((first, Some(second))),
                    witness: minimum_witness(
                        ring_witness(topology, first)?,
                        ring_witness(topology, second)?,
                    )?,
                },
                &topology.sources,
            )?,
            FaceIssue::PolygonOverlap(first, second) => {
                let first_shell = topology
                    .sources
                    .polygons
                    .get(first.0 as usize)
                    .and_then(|polygon| topology.sources.rings.get(polygon.rings.start))
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q overlap has no first polygon shell".into())
                    })?;
                let second_shell = topology
                    .sources
                    .polygons
                    .get(second.0 as usize)
                    .and_then(|polygon| topology.sources.rings.get(polygon.rings.start))
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q overlap has no second polygon shell".into())
                    })?;
                offer(
                    best,
                    Candidate {
                        kind: IssueKind::PolygonOverlap,
                        polygon: first.min(second),
                        other_polygon: Some(first.max(second)),
                        rings: None,
                        witness: minimum_witness(
                            ring_witness(topology, first_shell.id)?,
                            ring_witness(topology, second_shell.id)?,
                        )?,
                    },
                    &topology.sources,
                )?;
            },
        }
    }
    Ok(())
}

fn minimum_witness(left: ExactChartPoint, right: ExactChartPoint) -> Result<ExactChartPoint> {
    if compare_witness(&left, &right)?.is_le() {
        Ok(left)
    } else {
        Ok(right)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geographic::AngularFactor;
    use crate::geometry::{
        CoordSeq, CoordinateAxes, LineSeq, MOrdinate, MultiLineString, MultiMember, MultiPolygon,
        Polygon, Ring, TopologyDomain, ZOrdinate,
    };

    fn frame() -> GeographicFrame {
        let degrees = AngularFactor::try_new("test degrees", std::f64::consts::PI / 180.0).unwrap();
        GeographicFrame::try_new(degrees, degrees, 0.0).unwrap()
    }

    fn ring(points: &[(f64, f64)]) -> Ring {
        let points = points
            .iter()
            .map(|&(x, y)| Point::new_raw_xy(x, y))
            .collect::<Vec<_>>();
        Ring::from_closed(CoordSeq::try_from_points(&points).unwrap()).unwrap()
    }

    fn line(points: &[(f64, f64)]) -> Shape {
        let points = points
            .iter()
            .map(|&(x, y)| Point::new_raw_xy(x, y))
            .collect::<Vec<_>>();
        Shape::LineString(LineSeq::try_new(CoordSeq::try_from_points(&points).unwrap()).unwrap())
    }

    fn multi_line(lines: &[&[(f64, f64)]]) -> Shape {
        Shape::MultiLineString(
            MultiLineString::try_new(
                CoordinateAxes::XY,
                lines
                    .iter()
                    .map(|points| {
                        let points = points
                            .iter()
                            .map(|&(x, y)| Point::new_raw_xy(x, y))
                            .collect::<Vec<_>>();
                        MultiMember::Value(
                            LineSeq::try_new(CoordSeq::try_from_points(&points).unwrap()).unwrap(),
                        )
                    })
                    .collect(),
            )
            .unwrap(),
        )
    }

    fn polygon(rings: &[Vec<(f64, f64)>]) -> Polygon {
        let (shell, holes) = rings
            .split_first()
            .expect("test polygons include an exterior ring");
        Polygon::new(
            ring(shell),
            holes.iter().map(|points| ring(points)).collect(),
        )
    }

    fn multi_polygon(polygons: &[Vec<Vec<(f64, f64)>>]) -> Shape {
        Shape::MultiPolygon(
            MultiPolygon::try_new(
                CoordinateAxes::XY,
                polygons
                    .iter()
                    .map(|rings| MultiMember::Value(polygon(rings)))
                    .collect(),
            )
            .unwrap(),
        )
    }

    fn shape(rings: &[Vec<(f64, f64)>], multi: bool) -> Shape {
        if multi {
            multi_polygon(
                &rings
                    .iter()
                    .cloned()
                    .map(|ring| vec![ring])
                    .collect::<Vec<_>>(),
            )
        } else {
            Shape::Polygon(polygon(rings))
        }
    }

    #[derive(Clone, Copy)]
    enum Expectation {
        Valid,
        Invalid(&'static str),
    }

    struct RingRow {
        name: String,
        rings: Vec<Vec<(f64, f64)>>,
        multi: bool,
        expectation: Expectation,
        periodic: bool,
    }

    fn rows() -> Vec<RingRow> {
        let mut rows = Vec::new();
        let mut add = |name: String,
                       rings: Vec<Vec<(f64, f64)>>,
                       multi: bool,
                       expectation: Expectation,
                       periodic: bool| {
            rows.push(RingRow {
                name,
                rings,
                multi,
                expectation,
                periodic,
            });
        };
        for &(latitude, pole) in &[(80.0, 90.0), (-80.0, -90.0)] {
            for &longitude in &[0.0, 40.0, -90.0, 100.0, -179.0, 180.0, -180.0, 270.0] {
                let parallel = (0..=4)
                    .map(|index| (longitude + 90.0 * f64::from(index), latitude))
                    .collect::<Vec<_>>();
                let mut hop = parallel.clone();
                hop.extend([
                    (longitude + 360.0, pole),
                    (longitude, pole),
                    (longitude, latitude),
                ]);
                add(
                    format!("hop@{longitude} lat{latitude}"),
                    vec![hop],
                    false,
                    Expectation::Valid,
                    true,
                );

                let mut pole_line = parallel.clone();
                pole_line.push((longitude + 360.0, pole));
                for index in 1..=4 {
                    pole_line.push((longitude + 360.0 - 90.0 * f64::from(index), pole));
                }
                pole_line.push((longitude, latitude));
                add(
                    format!("pole-line@{longitude} lat{latitude}"),
                    vec![pole_line],
                    false,
                    Expectation::Valid,
                    true,
                );

                let mut apex = parallel;
                apex.extend([
                    (longitude + 360.0, pole),
                    (longitude + 360.0, latitude),
                    (longitude, latitude),
                ]);
                add(
                    format!("apex@{longitude} lat{latitude}"),
                    vec![apex],
                    false,
                    Expectation::Invalid("self_intersection"),
                    true,
                );
            }
            let seam_parallel = (0..=4)
                .map(|index| (-180.0 + 90.0 * f64::from(index), latitude))
                .collect::<Vec<_>>();
            let mut pair_a = seam_parallel.clone();
            pair_a.extend([(180.0, pole), (-180.0, pole), (-180.0, latitude)]);
            add(
                format!("U1 pair A lat{latitude}"),
                vec![pair_a],
                false,
                Expectation::Valid,
                true,
            );
            let mut pair_c = seam_parallel;
            pair_c.extend([
                (180.0, pole),
                (-180.0, pole),
                (180.0, pole),
                (-180.0, pole),
                (-180.0, latitude),
            ]);
            add(
                format!("U1 pair repeated C lat{latitude}"),
                vec![pair_c],
                false,
                Expectation::Invalid("self_intersection"),
                true,
            );
        }

        for &longitude in &[-180.0, 0.0, 40.0, -90.0, 180.0] {
            let band = (0..=3)
                .map(|index| (longitude + 120.0 * f64::from(index), -30.0))
                .chain(
                    (0..=3)
                        .rev()
                        .map(|index| (longitude + 120.0 * f64::from(index), 30.0)),
                )
                .chain([(longitude, -30.0)])
                .collect::<Vec<_>>();
            add(
                format!("band 120deg steps @{longitude}"),
                vec![band],
                false,
                Expectation::Valid,
                true,
            );
            let explicit = vec![
                (longitude, -30.0),
                (longitude + 360.0, -30.0),
                (longitude + 360.0, 30.0),
                (longitude, 30.0),
                (longitude, -30.0),
            ];
            let expectation = if longitude.abs().to_bits() == 180.0_f64.to_bits() {
                Expectation::Invalid("self_intersection")
            } else {
                Expectation::Valid
            };
            add(
                format!("band explicit full-turn edges @{longitude}"),
                vec![explicit],
                false,
                expectation,
                true,
            );
        }

        add(
            "keyhole slit".into(),
            vec![vec![
                (0.0, 0.0),
                (40.0, 0.0),
                (40.0, 40.0),
                (20.0, 40.0),
                (20.0, 20.0),
                (20.0, 40.0),
                (0.0, 40.0),
                (0.0, 0.0),
            ]],
            false,
            Expectation::Invalid("self_intersection"),
            false,
        );
        add(
            "outward spur".into(),
            vec![vec![
                (0.0, 0.0),
                (40.0, 0.0),
                (40.0, 40.0),
                (20.0, 40.0),
                (20.0, 60.0),
                (20.0, 40.0),
                (0.0, 40.0),
                (0.0, 0.0),
            ]],
            false,
            Expectation::Invalid("self_intersection"),
            false,
        );
        add(
            "stutter equal vertices".into(),
            vec![vec![
                (0.0, 0.0),
                (0.0, 0.0),
                (40.0, 0.0),
                (40.0, 40.0),
                (0.0, 40.0),
                (0.0, 0.0),
            ]],
            false,
            Expectation::Valid,
            false,
        );
        add(
            "stutter seam pair at lat 0".into(),
            vec![vec![
                (170.0, 0.0),
                (180.0, 0.0),
                (-180.0, 0.0),
                (-170.0, 0.0),
                (-170.0, 10.0),
                (170.0, 10.0),
                (170.0, 0.0),
            ]],
            false,
            Expectation::Valid,
            false,
        );
        add(
            "triangle to pole with seam pair stutter".into(),
            vec![vec![
                (0.0, 80.0),
                (40.0, 80.0),
                (180.0, 90.0),
                (-180.0, 90.0),
                (0.0, 80.0),
            ]],
            false,
            Expectation::Valid,
            true,
        );
        add(
            "triangle touching pole once".into(),
            vec![vec![(0.0, 80.0), (40.0, 80.0), (20.0, 90.0), (0.0, 80.0)]],
            false,
            Expectation::Valid,
            true,
        );
        add(
            "ring touching pole twice".into(),
            vec![vec![
                (0.0, 70.0),
                (30.0, 90.0),
                (60.0, 70.0),
                (90.0, 70.0),
                (120.0, 90.0),
                (150.0, 70.0),
                (0.0, 70.0),
            ]],
            false,
            Expectation::Invalid("self_intersection"),
            true,
        );
        add(
            "ring along the pole line only".into(),
            vec![vec![(0.0, 90.0), (120.0, 90.0), (240.0, 90.0), (0.0, 90.0)]],
            false,
            Expectation::Invalid("ring_collapsed"),
            true,
        );
        add(
            "pinched full-longitude ring".into(),
            vec![vec![
                (0.0, 10.0),
                (120.0, 30.0),
                (240.0, 20.0),
                (360.0, 10.0),
                (240.0, 0.0),
                (120.0, 5.0),
                (0.0, 10.0),
            ]],
            false,
            Expectation::Valid,
            true,
        );
        add(
            "zigzag-sided full-longitude band".into(),
            vec![vec![
                (0.0, 0.0),
                (120.0, 0.0),
                (240.0, 0.0),
                (360.0, 0.0),
                (370.0, 10.0),
                (360.0, 20.0),
                (240.0, 20.0),
                (120.0, 20.0),
                (0.0, 20.0),
                (10.0, 10.0),
                (0.0, 0.0),
            ]],
            false,
            Expectation::Valid,
            true,
        );
        add(
            "ring wider than a period".into(),
            vec![vec![
                (0.0, 0.0),
                (150.0, 0.0),
                (300.0, 0.0),
                (450.0, 0.0),
                (450.0, 20.0),
                (300.0, 20.0),
                (150.0, 20.0),
                (0.0, 20.0),
                (0.0, 0.0),
            ]],
            false,
            Expectation::Invalid("self_intersection"),
            true,
        );
        add(
            "bowtie".into(),
            vec![vec![
                (0.0, 0.0),
                (10.0, 10.0),
                (10.0, 0.0),
                (0.0, 10.0),
                (0.0, 0.0),
            ]],
            false,
            Expectation::Invalid("self_intersection"),
            false,
        );
        add(
            "self T-touch".into(),
            vec![vec![
                (0.0, 0.0),
                (20.0, 0.0),
                (20.0, 10.0),
                (10.0, 0.0),
                (10.0, 10.0),
                (0.0, 10.0),
                (0.0, 0.0),
            ]],
            false,
            Expectation::Invalid("self_intersection"),
            false,
        );
        for (name, ring) in [
            ("bowtie across the seam (explicit lift)", vec![
                (170.0, -10.0),
                (190.0, 10.0),
                (170.0, 10.0),
                (190.0, -10.0),
                (170.0, -10.0),
            ]),
            ("bowtie across the seam (canonical spelling)", vec![
                (170.0, -10.0),
                (-170.0, 10.0),
                (170.0, 10.0),
                (-170.0, -10.0),
                (170.0, -10.0),
            ]),
        ] {
            add(
                name.into(),
                vec![ring],
                false,
                Expectation::Invalid("self_intersection"),
                true,
            );
        }
        add(
            "ordinary box".into(),
            vec![vec![
                (0.0, 0.0),
                (10.0, 0.0),
                (10.0, 10.0),
                (0.0, 10.0),
                (0.0, 0.0),
            ]],
            false,
            Expectation::Valid,
            false,
        );
        add(
            "seam-crossing box (explicit lift)".into(),
            vec![vec![
                (170.0, 0.0),
                (190.0, 0.0),
                (190.0, 10.0),
                (170.0, 10.0),
                (170.0, 0.0),
            ]],
            false,
            Expectation::Valid,
            true,
        );

        let box_ring = |west: f64, east: f64| {
            vec![
                (west, 0.0),
                (east, 0.0),
                (east, 10.0),
                (west, 10.0),
                (west, 0.0),
            ]
        };
        add(
            "E/W seam members".into(),
            vec![box_ring(170.0, 180.0), box_ring(-180.0, -170.0)],
            true,
            Expectation::Valid,
            true,
        );
        add(
            "E/E seam members".into(),
            vec![box_ring(170.0, 180.0), box_ring(180.0, 190.0)],
            true,
            Expectation::Valid,
            true,
        );
        add(
            "adjacent ordinary members".into(),
            vec![box_ring(0.0, 10.0), box_ring(10.0, 20.0)],
            true,
            Expectation::Invalid("shared_linework"),
            false,
        );
        add(
            "duplicate member +360".into(),
            vec![box_ring(0.0, 10.0), box_ring(360.0, 370.0)],
            true,
            Expectation::Invalid("shared_linework"),
            true,
        );
        add(
            "hole sharing shell edge".into(),
            vec![box_ring(0.0, 20.0), vec![
                (0.0, 2.0),
                (5.0, 2.0),
                (5.0, 5.0),
                (0.0, 5.0),
                (0.0, 2.0),
            ]],
            false,
            Expectation::Invalid("shared_linework"),
            false,
        );
        rows
    }

    fn rotate(ring: &[(f64, f64)], offset: usize) -> Vec<(f64, f64)> {
        let (_, open) = ring
            .split_last()
            .expect("the ring corpus stores an explicit closure");
        let offset = offset % open.len();
        let mut rotated = open
            .iter()
            .cycle()
            .skip(offset)
            .take(open.len())
            .copied()
            .collect::<Vec<_>>();
        rotated.push(
            *rotated
                .first()
                .expect("rotating a nonempty open ring keeps one point"),
        );
        rotated
    }

    fn transformed(row: &RingRow, offset: usize, reversed: bool, shift: f64) -> Shape {
        let rings = row
            .rings
            .iter()
            .enumerate()
            .map(|(index, ring)| {
                let mut points = if index == 0 {
                    rotate(ring, offset)
                } else {
                    ring.clone()
                };
                if reversed {
                    let _ = points
                        .pop()
                        .expect("the ring corpus stores an explicit closure");
                    points.reverse();
                    let first = *points
                        .first()
                        .expect("reversing a nonempty open ring keeps one point");
                    points.push(first);
                }
                points
                    .into_iter()
                    .map(|(x, y)| (x + shift, y))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        shape(&rings, row.multi)
    }

    #[derive(Clone, Debug, Eq, PartialEq)]
    struct Signature {
        code: Option<&'static str>,
        detail: Option<String>,
        path: Option<String>,
        location: Option<(u64, u64)>,
    }

    fn signature(shape: &Shape) -> Signature {
        let frame = frame();
        let view = crate::geographic::ScalarGeographicView::try_new(frame, shape).unwrap();
        let topology = ExactGeographicTopology::from_views(frame, std::iter::once(view)).unwrap();
        signature_from_issue(topology.validity_issue(shape).unwrap())
    }

    fn signature_from_issue(issue: Option<ValidationIssue>) -> Signature {
        match issue {
            Some(issue) => Signature {
                code: Some(issue.code),
                detail: Some(issue.detail),
                path: issue.path,
                location: issue
                    .location
                    .map(|point| (point.x.to_bits(), point.y.to_bits())),
            },
            None => Signature {
                code: None,
                detail: None,
                path: None,
                location: None,
            },
        }
    }

    fn assert_expected(row: &RingRow, signature: &Signature) {
        match row.expectation {
            Expectation::Valid => assert_eq!(signature.code, None, "{}", row.name),
            Expectation::Invalid(code) => {
                assert_eq!(signature.code, Some(code), "{}", row.name);
            },
        }
    }

    fn assert_ring_rows(rows: impl IntoIterator<Item = RingRow>) {
        let mut failures = Vec::new();
        for row in rows {
            let closed_ring = row
                .rings
                .first()
                .expect("every rule row contains one source ring");
            let variants = closed_ring
                .len()
                .checked_sub(1)
                .expect("every source ring stores a closure");
            let mut baseline = None;
            for shift in [0.0, 360.0, -360.0] {
                for reversed in [false, true] {
                    for offset in 0..variants {
                        let actual = signature(&transformed(&row, offset, reversed, shift));
                        if let Some(expected) = &baseline {
                            if &actual != expected {
                                failures.push(format!(
                                    "{} rot{offset} reverse={reversed} shift={shift}: {actual:?} != {expected:?}",
                                    row.name
                                ));
                            }
                        } else {
                            assert_expected(&row, &actual);
                            baseline = Some(actual);
                        }
                    }
                }
            }
        }
        assert!(
            failures.is_empty(),
            "spelling-invariant rows failed: {failures:#?}"
        );
    }

    fn ring_group(row: &RingRow) -> u8 {
        let first_longitudes = ["@0 ", "@40 ", "@-90 ", "@100 "];
        let first_cap = row.name.starts_with("U1 pair")
            || first_longitudes
                .iter()
                .any(|longitude| row.name.contains(longitude));
        if row.name.contains("lat80") {
            u8::from(!first_cap)
        } else if row.name.contains("lat-80") {
            2 + u8::from(!first_cap)
        } else if row.name.starts_with("band") {
            4
        } else if row.periodic {
            5
        } else {
            6
        }
    }

    #[test]
    fn source_prototype_contains_84_ring_rule_rows() {
        assert_eq!(rows().len(), 84);
    }

    #[test]
    fn north_cap_ordinary_cut_rules_are_invariant_under_period_lifts() {
        assert_ring_rows(rows().into_iter().filter(|row| ring_group(row) == 0));
    }

    #[test]
    fn north_cap_seam_cut_rules_are_invariant_under_period_lifts() {
        assert_ring_rows(rows().into_iter().filter(|row| ring_group(row) == 1));
    }

    #[test]
    fn south_cap_ordinary_cut_rules_are_invariant_under_period_lifts() {
        assert_ring_rows(rows().into_iter().filter(|row| ring_group(row) == 2));
    }

    #[test]
    fn south_cap_seam_cut_rules_are_invariant_under_period_lifts() {
        assert_ring_rows(rows().into_iter().filter(|row| ring_group(row) == 3));
    }

    #[test]
    fn full_longitude_band_rules_are_invariant_under_period_lifts() {
        assert_ring_rows(rows().into_iter().filter(|row| ring_group(row) == 4));
    }

    #[test]
    fn pole_and_period_contact_rules_are_invariant_under_period_lifts() {
        assert_ring_rows(rows().into_iter().filter(|row| ring_group(row) == 5));
    }

    #[test]
    fn ordinary_and_cross_ring_facts_are_invariant_under_rotation() {
        assert_ring_rows(rows().into_iter().filter(|row| ring_group(row) == 6));
    }

    struct TallyRow {
        name: &'static str,
        polygons: Vec<Vec<Vec<(f64, f64)>>>,
        expected: Option<&'static str>,
    }

    fn box_ring(west: f64, east: f64, south: f64, north: f64) -> Vec<(f64, f64)> {
        vec![
            (west, south),
            (east, south),
            (east, north),
            (west, north),
            (west, south),
        ]
    }

    fn shift_rings(polygons: &[Vec<Vec<(f64, f64)>>], shift: f64) -> Vec<Vec<Vec<(f64, f64)>>> {
        polygons
            .iter()
            .map(|polygon| {
                polygon
                    .iter()
                    .map(|ring| ring.iter().map(|&(x, y)| (x + shift, y)).collect())
                    .collect()
            })
            .collect()
    }

    fn tally_rows() -> Vec<TallyRow> {
        let b = box_ring;
        let lift = |ring: Vec<(f64, f64)>| {
            ring.into_iter()
                .map(|(x, y)| (x + 360.0, y))
                .collect::<Vec<_>>()
        };
        let world_band = vec![vec![
            (-180.0, 0.0),
            (-60.0, 0.0),
            (60.0, 0.0),
            (180.0, 0.0),
            (180.0, 10.0),
            (60.0, 10.0),
            (-60.0, 10.0),
            (-180.0, 10.0),
            (-180.0, 0.0),
        ]];
        vec![
            TallyRow {
                name: "hole inside shell",
                polygons: vec![vec![b(0., 20., 0., 20.), b(5., 10., 5., 10.)]],
                expected: None,
            },
            TallyRow {
                name: "hole inside shell, hole +360",
                polygons: vec![vec![b(0., 20., 0., 20.), lift(b(5., 10., 5., 10.))]],
                expected: None,
            },
            TallyRow {
                name: "hole outside shell",
                polygons: vec![vec![b(0., 20., 0., 20.), b(25., 30., 5., 10.)]],
                expected: Some("hole_outside_shell"),
            },
            TallyRow {
                name: "hole outside shell, +360",
                polygons: vec![vec![b(0., 20., 0., 20.), lift(b(25., 30., 5., 10.))]],
                expected: Some("hole_outside_shell"),
            },
            TallyRow {
                name: "overlapping holes",
                polygons: vec![vec![
                    b(0., 20., 0., 20.),
                    b(2., 8., 5., 10.),
                    b(5., 12., 6., 12.),
                ]],
                expected: Some("ring_intersection"),
            },
            TallyRow {
                name: "nested holes",
                polygons: vec![vec![
                    b(0., 20., 0., 20.),
                    b(2., 12., 2., 12.),
                    b(4., 8., 4., 8.),
                ]],
                expected: Some("hole_overlap"),
            },
            TallyRow {
                name: "hole across seam in crossing shell",
                polygons: vec![vec![b(170., 190., 0., 20.), b(178., 182., 5., 10.)]],
                expected: None,
            },
            TallyRow {
                name: "overlapping members",
                polygons: vec![vec![b(0., 10., 0., 10.)], vec![b(5., 15., 5., 15.)]],
                expected: Some("ring_intersection"),
            },
            TallyRow {
                name: "duplicate member +360",
                polygons: vec![vec![b(0., 10., 0., 10.)], vec![lift(b(0., 10., 0., 10.))]],
                expected: Some("shared_linework"),
            },
            TallyRow {
                name: "adjacent members",
                polygons: vec![vec![b(0., 10., 0., 10.)], vec![b(10., 20., 0., 10.)]],
                expected: Some("shared_linework"),
            },
            TallyRow {
                name: "member inside another's hole",
                polygons: vec![vec![b(0., 20., 0., 20.), b(5., 15., 5., 15.)], vec![b(
                    8., 12., 8., 12.,
                )]],
                expected: None,
            },
            TallyRow {
                name: "a,b(adjacent),d",
                polygons: vec![
                    vec![b(20., 40., 20., 40.)],
                    vec![b(10., 30., 0., 20.)],
                    vec![b(100., 120., 20., 40.)],
                ],
                expected: Some("shared_linework"),
            },
            TallyRow {
                name: "a,b(overlap),d",
                polygons: vec![
                    vec![b(20., 40., 20., 40.)],
                    vec![b(10., 30., 0., 25.)],
                    vec![b(100., 120., 20., 40.)],
                ],
                expected: Some("ring_intersection"),
            },
            TallyRow {
                name: "three disjoint boxes",
                polygons: vec![
                    vec![b(20., 40., 20., 40.)],
                    vec![b(50., 60., 20., 40.)],
                    vec![b(100., 120., 20., 40.)],
                ],
                expected: None,
            },
            TallyRow {
                name: "E/W seam members",
                polygons: vec![vec![b(170., 180., 0., 10.)], vec![b(-180., -170., 0., 10.)]],
                expected: None,
            },
            TallyRow {
                name: "same-side seam overlap",
                polygons: vec![vec![b(180., 190., 0., 10.)], vec![b(-180., -170., 0., 10.)]],
                expected: Some("shared_linework"),
            },
            TallyRow {
                name: "world band + overlapping box",
                polygons: vec![world_band, vec![b(0., 5., 0., 5.)]],
                expected: Some("shared_linework"),
            },
            TallyRow {
                name: "p9: shell + hole + seam cut pair",
                polygons: vec![
                    vec![b(-10., 10., 0., 20.), b(2., 5., 5., 10.)],
                    vec![b(170., 180., 0., 10.)],
                    vec![b(-180., -170., 0., 10.)],
                ],
                expected: None,
            },
            TallyRow {
                name: "p4: overlapping members + seam cut pair",
                polygons: vec![
                    vec![b(-10., 5., 0., 10.)],
                    vec![b(0., 10., 0., 10.)],
                    vec![b(170., 180., 0., 10.)],
                    vec![b(-180., -170., 0., 10.)],
                ],
                expected: Some("shared_linework"),
            },
            TallyRow {
                name: "p11: one-ULP gap + seam cut pair",
                polygons: vec![
                    vec![b(-20., -10.000_000_000_000_002, 0., 10.)],
                    vec![b(-10., 0., 0., 10.)],
                    vec![b(170., 180., 0., 10.)],
                    vec![b(-180., -170., 0., 10.)],
                ],
                expected: None,
            },
            TallyRow {
                name: "p11 control: exact touch + cut pair",
                polygons: vec![
                    vec![b(-20., -10., 0., 10.)],
                    vec![b(-10., 0., 0., 10.)],
                    vec![b(170., 180., 0., 10.)],
                    vec![b(-180., -170., 0., 10.)],
                ],
                expected: Some("shared_linework"),
            },
        ]
    }

    #[test]
    fn cross_ring_face_tallies_use_q_occupancy_and_exact_precedence() {
        let rows = tally_rows();
        assert_eq!(rows.len(), 21);
        for row in rows {
            for shift in [0.0, 360.0, -360.0] {
                let geometry = multi_polygon(&shift_rings(&row.polygons, shift));
                let actual = signature(&geometry);
                if let Some(code) = row.expected {
                    assert_eq!(
                        actual.code,
                        Some(code),
                        "{} shift {shift}: {actual:?}",
                        row.name
                    );
                }
            }
        }
    }

    fn diamond(center_x: f64, center_y: f64, radius: f64) -> Vec<(f64, f64)> {
        vec![
            (center_x - radius, center_y),
            (center_x, center_y - radius),
            (center_x + radius, center_y),
            (center_x, center_y + radius),
            (center_x - radius, center_y),
        ]
    }

    fn shift_ring(ring: &[(f64, f64)], shift: f64) -> Vec<(f64, f64)> {
        ring.iter().map(|&(x, y)| (x + shift, y)).collect()
    }

    struct ConnectivityRow {
        name: &'static str,
        polygons: Vec<Vec<Vec<(f64, f64)>>>,
        expected: Option<&'static str>,
    }

    fn connectivity_rows() -> Vec<ConnectivityRow> {
        let b = box_ring;
        vec![
            ConnectivityRow {
                name: "hole touches shell once",
                polygons: vec![vec![b(0.0, 20.0, 0.0, 20.0), diamond(5.0, 10.0, 5.0)]],
                expected: None,
            },
            ConnectivityRow {
                name: "hole touches shell twice",
                polygons: vec![vec![b(0.0, 20.0, 0.0, 20.0), diamond(10.0, 10.0, 10.0)]],
                expected: Some("interior_disconnected"),
            },
            ConnectivityRow {
                name: "chain of two hole touches",
                polygons: vec![vec![
                    b(0.0, 20.0, 0.0, 20.0),
                    diamond(5.0, 10.0, 5.0),
                    diamond(14.0, 10.0, 4.0),
                ]],
                expected: None,
            },
            ConnectivityRow {
                name: "cycle of two diamond holes",
                polygons: vec![vec![
                    b(0.0, 20.0, 0.0, 20.0),
                    diamond(5.0, 10.0, 5.0),
                    diamond(15.0, 10.0, 5.0),
                ]],
                expected: Some("interior_disconnected"),
            },
            ConnectivityRow {
                name: "hole touches shell twice, +360",
                polygons: vec![vec![
                    b(0.0, 20.0, 0.0, 20.0),
                    shift_ring(&diamond(10.0, 10.0, 10.0), 360.0),
                ]],
                expected: Some("interior_disconnected"),
            },
            ConnectivityRow {
                name: "hole touches shell once, +360",
                polygons: vec![vec![
                    b(0.0, 20.0, 0.0, 20.0),
                    shift_ring(&diamond(5.0, 10.0, 5.0), 360.0),
                ]],
                expected: None,
            },
            ConnectivityRow {
                name: "two holes touch each other and shell in a cycle",
                polygons: vec![vec![
                    b(0.0, 20.0, 0.0, 20.0),
                    vec![
                        (0.0, 10.0),
                        (5.0, 5.0),
                        (10.0, 10.0),
                        (5.0, 15.0),
                        (0.0, 10.0),
                    ],
                    vec![
                        (20.0, 10.0),
                        (15.0, 5.0),
                        (10.0, 10.0),
                        (15.0, 15.0),
                        (20.0, 10.0),
                    ],
                ]],
                expected: Some("interior_disconnected"),
            },
        ]
    }

    #[test]
    fn q_touch_node_cycles_report_disconnected_interiors() {
        let rows = connectivity_rows();
        assert_eq!(rows.len(), 7);
        for row in rows {
            let mut baseline = None;
            for shift in [0.0, 360.0, -360.0] {
                let geometry = multi_polygon(&shift_rings(&row.polygons, shift));
                let actual = signature(&geometry);
                assert_eq!(
                    actual.code, row.expected,
                    "{} shift {shift}: {actual:?}",
                    row.name
                );
                if let Some(expected) = &baseline {
                    assert_eq!(&actual, expected, "{} shift {shift}", row.name);
                } else {
                    baseline = Some(actual);
                }
            }
        }
    }

    #[test]
    fn tv_ar_uy_symmetric_difference_order_012_output_is_valid() {
        let wkt = include_str!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/fixtures/tv-ar-uy-symmetric-difference-order-012.wkt"
        ));
        let geometry = crate::io::parse_wkt(wkt).unwrap();
        assert_eq!(
            crate::geometry::validate_shape_in_frame(
                &geometry,
                TopologyDomain::Geographic(frame())
            )
            .unwrap(),
            None,
            "the TV/AR/UY symmetric-difference output in operand order [0, 1, 2]"
        );
    }

    #[test]
    fn public_validation_accepts_both_opposite_side_seam_spellings() {
        let east = multi_polygon(&[vec![box_ring(170.0, 180.0, 0.0, 10.0)], vec![box_ring(
            180.0, 190.0, 0.0, 10.0,
        )]]);
        let west = multi_polygon(&[vec![box_ring(-190.0, -180.0, 0.0, 10.0)], vec![box_ring(
            -180.0, -170.0, 0.0, 10.0,
        )]]);
        for geometry in [east, west] {
            assert_eq!(
                crate::geometry::validate_shape_in_frame(
                    &geometry,
                    TopologyDomain::Geographic(frame())
                )
                .unwrap(),
                None
            );
            let intersections = crate::geometry::self_intersections_in_frame(
                &geometry,
                TopologyDomain::Geographic(frame()),
            )
            .unwrap();
            assert!(intersections.is_empty(), "{intersections:?}");
        }

        let same_side = multi_polygon(&[vec![box_ring(180.0, 190.0, 0.0, 10.0)], vec![box_ring(
            -180.0, -170.0, 0.0, 10.0,
        )]]);
        assert_eq!(
            crate::geometry::validate_shape_in_frame(
                &same_side,
                TopologyDomain::Geographic(frame())
            )
            .unwrap()
            .map(|issue| issue.code),
            Some("shared_linework")
        );
        assert_eq!(
            crate::geometry::self_intersections_in_frame(
                &same_side,
                TopologyDomain::Geographic(frame())
            )
            .unwrap()
            .len(),
            4
        );

        let zigzag = shape(
            &[vec![
                (0.0, 0.0),
                (120.0, 0.0),
                (240.0, 0.0),
                (360.0, 0.0),
                (370.0, 10.0),
                (360.0, 20.0),
                (240.0, 20.0),
                (120.0, 20.0),
                (0.0, 20.0),
                (10.0, 10.0),
                (0.0, 0.0),
            ]],
            false,
        );
        assert_eq!(
            crate::geometry::validate_shape_in_frame(&zigzag, TopologyDomain::Geographic(frame()))
                .unwrap(),
            None
        );
    }

    #[test]
    fn quotient_validity_keeps_geometry_collection_members_independent() {
        let collection = Shape::geometry_collection(vec![
            Shape::Polygon(polygon(&[box_ring(170.0, 190.0, 0.0, 10.0)])),
            Shape::Polygon(polygon(&[box_ring(175.0, 185.0, 0.0, 10.0)])),
            Shape::Polygon(polygon(&[box_ring(-20.0, -10.0, 0.0, 10.0)])),
        ])
        .unwrap();
        assert_eq!(
            crate::geometry::validate_shape_in_frame(
                &collection,
                TopologyDomain::Geographic(frame())
            )
            .unwrap(),
            None
        );
    }

    #[test]
    fn quotient_polygon_self_intersections_report_canonical_q_nodes() {
        let points = [
            (170.0, -10.0),
            (190.0, 10.0),
            (170.0, 10.0),
            (190.0, -10.0),
            (170.0, -10.0),
        ];
        let make_shape = |shift: f64| {
            Shape::Polygon(Polygon::new(
                ring(&points.map(|(x, y)| (x + shift, y))),
                Vec::new(),
            ))
        };
        let expected = [((-180.0_f64).to_bits(), 0.0_f64.to_bits())];
        let domain = TopologyDomain::Geographic(frame());
        for geometry in [make_shape(0.0), make_shape(360.0)] {
            let points = crate::geometry::self_intersections_in_frame(&geometry, domain).unwrap();
            assert_eq!(
                points
                    .iter()
                    .map(|point| (point.x.to_bits(), point.y.to_bits()))
                    .collect::<Vec<_>>(),
                expected
            );
            let prepared = crate::geometry::PreparedPlan::transient(&geometry);
            assert!(!crate::geometry::is_simple_data_in_frame(&prepared, domain).unwrap());
        }

        let pole_pair = rows()
            .into_iter()
            .find(|row| row.name == "U1 pair A lat80")
            .expect("the cap corpus includes the declared pole-pair excursion");
        let pole_pair = shape(&pole_pair.rings, false);
        assert_eq!(
            crate::geometry::validate_shape_in_frame(&pole_pair, domain).unwrap(),
            None
        );
        assert!(
            crate::geometry::self_intersections_in_frame(&pole_pair, domain)
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn quotient_line_self_intersections_and_multiline_boundaries_use_q_nodes() {
        let domain = TopologyDomain::Geographic(frame());
        let crossing = line(&[(170.0, -10.0), (190.0, 10.0), (170.0, 10.0), (190.0, -10.0)]);
        assert_eq!(
            crate::geometry::self_intersections_in_frame(&crossing, domain).unwrap(),
            [Point::new_raw_xy(-180.0, 0.0)]
        );
        let crossing_alias = line(&[(530.0, -10.0), (550.0, 10.0), (530.0, 10.0), (550.0, -10.0)]);
        assert_eq!(
            crate::geometry::self_intersections_in_frame(&crossing_alias, domain).unwrap(),
            [Point::new_raw_xy(-180.0, 0.0)]
        );
        let prepared = crate::geometry::PreparedPlan::transient(&crossing);
        assert!(!crate::geometry::is_simple_data_in_frame(&prepared, domain).unwrap());

        let stutter = line(&[(170.0, 0.0), (170.0, 0.0), (190.0, 10.0)]);
        assert!(
            crate::geometry::self_intersections_in_frame(&stutter, domain)
                .unwrap()
                .is_empty()
        );
        let prepared = crate::geometry::PreparedPlan::transient(&stutter);
        assert!(crate::geometry::is_simple_data_in_frame(&prepared, domain).unwrap());

        let endpoint_touch = multi_line(&[&[(170.0, 0.0), (180.0, 0.0)], &[
            (180.0, 0.0),
            (180.0, 10.0),
        ]]);
        assert!(
            crate::geometry::self_intersections_in_frame(&endpoint_touch, domain)
                .unwrap()
                .is_empty()
        );
        let prepared = crate::geometry::PreparedPlan::transient(&endpoint_touch);
        assert!(crate::geometry::is_simple_data_in_frame(&prepared, domain).unwrap());

        let interior_crossing = multi_line(&[&[(170.0, -10.0), (190.0, 10.0)], &[
            (170.0, 10.0),
            (190.0, -10.0),
        ]]);
        assert_eq!(
            crate::geometry::self_intersections_in_frame(&interior_crossing, domain).unwrap(),
            [Point::new_raw_xy(-180.0, 0.0)]
        );
        let prepared = crate::geometry::PreparedPlan::transient(&interior_crossing);
        assert!(!crate::geometry::is_simple_data_in_frame(&prepared, domain).unwrap());

        let quotient_ring = line(&[
            (170.0, -10.0),
            (190.0, -10.0),
            (190.0, 10.0),
            (170.0, 10.0),
            (150.0, 0.0),
            (-190.0, -10.0),
        ]);
        let prepared = crate::geometry::PreparedPlan::transient(&quotient_ring);
        assert!(crate::geometry::is_ring_data_in_frame(&prepared, domain).unwrap());

        let closed_multiline =
            multi_line(&[&[(170.0, 0.0), (190.0, 0.0), (180.0, 10.0), (170.0, 0.0)]]);
        let prepared = crate::geometry::PreparedPlan::transient(&closed_multiline);
        assert!(!crate::geometry::is_ring_data_in_frame(&prepared, domain).unwrap());
    }

    #[test]
    fn canonical_precedence_orders_members_then_issue_class() {
        let shell = box_ring(0.0, 20.0, 0.0, 20.0);
        let outside_hole = box_ring(25.0, 30.0, 5.0, 10.0);
        let outer_hole = box_ring(2.0, 12.0, 2.0, 12.0);
        let inner_hole = box_ring(4.0, 8.0, 4.0, 8.0);
        let polygon_with_two_defects =
            Shape::Polygon(polygon(&[shell, outside_hole, outer_hole, inner_hole]));
        let issue = signature(&polygon_with_two_defects);
        assert_eq!(issue.code, Some("hole_outside_shell"));
        assert_eq!(issue.path.as_deref(), Some("$"));

        let bowtie = vec![
            (0.0, 0.0),
            (10.0, 10.0),
            (10.0, 0.0),
            (0.0, 10.0),
            (0.0, 0.0),
        ];
        let members = multi_polygon(&[vec![bowtie], vec![box_ring(40.0, 50.0, 0.0, 10.0)], vec![
            box_ring(45.0, 55.0, 5.0, 15.0),
        ]]);
        let issue = signature(&members);
        assert_eq!(issue.code, Some("self_intersection"));
        assert_eq!(issue.path.as_deref(), Some("$[0]"));
    }

    #[test]
    fn geographic_q_validity_uses_xy_without_losing_zm_storage() {
        let points = [
            (170.0, -10.0),
            (190.0, 10.0),
            (170.0, 10.0),
            (190.0, -10.0),
            (170.0, -10.0),
        ];
        let xy = Shape::Polygon(Polygon::new(ring(&points), Vec::new()));
        let zm_points = points
            .iter()
            .enumerate()
            .map(|(index, &(x, y))| {
                Point::new_raw_axes(
                    x,
                    y,
                    ZOrdinate(Some(index as f64)),
                    MOrdinate(Some((index as f64) * 10.0)),
                )
            })
            .collect::<Vec<_>>();
        let zm_ring = Ring::from_closed(CoordSeq::try_from_points(&zm_points).unwrap()).unwrap();
        let zm = Shape::Polygon(Polygon::new(zm_ring, Vec::new()));
        assert_eq!(signature(&xy), signature(&zm));
    }

    #[test]
    fn exact_partition_route_matches_planar_validity_codes_paths_and_witnesses() {
        let frame = frame();
        let shell = box_ring(0.0, 20.0, 0.0, 20.0);
        let hole_outside = box_ring(25.0, 30.0, 5.0, 10.0);
        let shapes = [
            Shape::Polygon(polygon(&[box_ring(0.0, 10.0, 0.0, 10.0)])),
            Shape::Polygon(Polygon::new(
                ring(&[
                    (0.0, 0.0),
                    (10.0, 10.0),
                    (10.0, 0.0),
                    (0.0, 10.0),
                    (0.0, 0.0),
                ]),
                Vec::new(),
            )),
            Shape::Polygon(polygon(&[shell, hole_outside])),
        ];
        for shape in shapes {
            let view = crate::geographic::ScalarGeographicView::try_new(frame, &shape).unwrap();
            assert_eq!(
                crate::geometry::classify_geographic_chart(view),
                crate::geometry::GeographicTopologyRoute::ExactPartition
            );
            let topology =
                ExactGeographicTopology::from_views(frame, std::iter::once(view)).unwrap();
            assert_eq!(
                signature_from_issue(topology.validity_issue(&shape).unwrap()),
                signature_from_issue(shape.validate().unwrap()),
                "{shape:?}"
            );
        }
    }
}
