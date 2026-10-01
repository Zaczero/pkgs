//! Exact geographic quotient faces and source-owned face occupancy.

use std::cmp::Ordering;
use std::ops::Range;

use super::exact::{
    ExactChartPoint, ExactGeographicChartInventory, ExactQuotientKey, ExactSourceRange,
    FallibleAvl, NodedExactChart, QuotientNodeId, SourceEdgeId, SourceEdgeKind, SourcePolygonId,
    SourceRingId, SourceRowId, point_on_exact_segment, vector_cross, vector_difference,
};
use crate::error::{ResourceError, Result};
use crate::geographic::GeographicFrame;
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::segments::try_sort_by;
use crate::geometry::source_support::RingKind;
use crate::geometry::tessellation::exact::ExactRational;
use crate::geometry::{
    Bounds, De9im, Dimension, GeometryErrorKind, Loc, Point, RingWinding, Segment, SegmentIndex,
    canonical_f64_bits,
};
use crate::heap_size::HeapSize;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) struct QPortId(pub u32);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) struct QNodeId(pub u32);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) struct ChartSpanId(pub u32);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) struct QDartId(pub u32);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) struct QLocalFaceId(pub u32);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) struct QComponentId(pub u32);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) struct GlobalFaceId(pub u32);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(super) struct CutIntervalId(pub u32);

#[derive(Clone, Debug, Eq, PartialEq)]
enum QNodeKey {
    Source(QuotientNodeId),
    Cut(ExactRational),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Pole {
    North,
    South,
}

#[derive(Clone, Debug)]
pub(super) struct QPort {
    pub id: QPortId,
    pub point: ExactChartPoint,
    pub node: QNodeId,
    pub(super) cut: bool,
}

#[derive(Clone, Debug)]
pub(super) struct QNode {
    pub id: QNodeId,
    key: QNodeKey,
    pole: Option<Pole>,
    pub ports: Range<usize>,
}

#[derive(Clone, Debug)]
pub(super) struct QSpanOwner {
    pub source_edge: SourceEdgeId,
    pub source_range: ExactSourceRange,
    /// The source edge runs opposite `ports[0] -> ports[1]`.
    pub reversed: bool,
    ring_transition: Option<RingTransition>,
}

#[derive(Clone, Copy, Debug)]
struct RingTransition {
    ring: SourceRingId,
    /// Crossing from the span's left face to its right face changes winding
    /// by this amount.
    delta: i64,
}

#[derive(Clone, Debug)]
pub(super) struct ChartSpan {
    pub id: ChartSpanId,
    pub ports: [QPortId; 2],
    pub nodes: [QNodeId; 2],
    pub owners: Box<[QSpanOwner]>,
}

#[derive(Clone, Copy, Debug)]
struct SeamCutEdgePair {
    edges: (u32, u32),
    polygons: (u32, u32),
    low_y: f64,
    high_y: f64,
    low_adjacent_edges: (Option<u32>, Option<u32>),
    high_adjacent_edges: (Option<u32>, Option<u32>),
}

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct SeamTouchFilter {
    edge: u32,
    other_polygon: u32,
    latitude_bits: u64,
    adjacent_edge: u32,
    ignore_above: bool,
    ignore_below: bool,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct SeamCutEdgePairs {
    seam_x: f64,
    pairs: Box<[SeamCutEdgePair]>,
    touch_filters: Box<[SeamTouchFilter]>,
}

impl SeamCutEdgePairs {
    fn get(&self, left: u32, right: u32) -> Option<&SeamCutEdgePair> {
        let key = if left < right {
            (left, right)
        } else {
            (right, left)
        };
        self.pairs
            .binary_search_by_key(&key, |seam_cut| seam_cut.edges)
            .ok()
            .and_then(|index| self.pairs.get(index))
    }

    pub(crate) fn contains(&self, left: u32, right: u32) -> bool {
        self.get(left, right).is_some()
    }

    /// Omit only rays inside a certified cut span. A source edge extending
    /// beyond a partial overlap keeps the ray on its non-cut side.
    pub(crate) fn touch_directions(
        &self,
        edge: u32,
        other_edge: u32,
        other_polygon: u32,
        point: crate::geometry::XY,
        segment: Segment,
    ) -> [bool; 2] {
        if canonical_f64_bits(point.x) != canonical_f64_bits(self.seam_x) {
            return [true, true];
        }
        let key = (edge, other_polygon, canonical_f64_bits(point.y), other_edge);
        let start = self
            .touch_filters
            .partition_point(|filter| filter.key() < key);
        let Some(filters) = self.touch_filters.get(start..) else {
            return [true, true];
        };
        let mut include = [true, true];
        for filter in filters.iter().take_while(|filter| filter.key() == key) {
            let into_cut = |endpoint: crate::geometry::XY| {
                (endpoint.y > point.y && filter.ignore_above)
                    || (endpoint.y < point.y && filter.ignore_below)
            };
            include[0] &= !into_cut(segment.start);
            include[1] &= !into_cut(segment.end);
        }
        include
    }
}

impl SeamTouchFilter {
    const fn key(self) -> (u32, u32, u64, u32) {
        (
            self.edge,
            self.other_polygon,
            self.latitude_bits,
            self.adjacent_edge,
        )
    }
}

#[derive(Clone, Copy)]
struct SeamBoundaryOwner {
    edge: SourceEdgeId,
    polygon: SourcePolygonId,
    ring: SourceRingId,
    hole: bool,
    east_spelling: bool,
}

#[derive(Clone, Copy)]
struct SeamOwnerSide {
    nodes: [QNodeId; 2],
    edge: SourceEdgeId,
    polygon: SourcePolygonId,
    east_spelling: bool,
    left_interior: bool,
    right_interior: bool,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct QDart {
    pub(super) span: ChartSpanId,
    pub(super) from_port: QPortId,
    pub(super) to_port: QPortId,
    pub(super) from_node: QNodeId,
    pub(super) to_node: QNodeId,
}

#[derive(Clone, Copy, Debug)]
pub(super) struct QLocalFace {
    pub(super) component: QComponentId,
}

#[derive(Clone, Debug)]
struct PortCandidate {
    point: ExactChartPoint,
    node: QNodeKey,
    cut: bool,
}

#[derive(Clone, Debug)]
struct SpanOccurrence {
    start: ExactChartPoint,
    end: ExactChartPoint,
    source_edge: SourceEdgeId,
    source_range: ExactSourceRange,
    source_forward: bool,
}

#[derive(Clone, Debug)]
struct ExactFaceGerm {
    leading: (ExactRational, ExactRational),
    perturbation: (ExactRational, ExactRational),
}

#[derive(Clone, Debug)]
struct HostGerm {
    point: ExactChartPoint,
    dx: ExactRational,
    dy: ExactRational,
}

struct CutChart {
    cut: ExactRational,
    cut_east: ExactRational,
    ports: Vec<QPort>,
    nodes: Vec<QNode>,
    node_ports: Vec<QPortId>,
    spans: Vec<ChartSpan>,
    source_edge_ports: Vec<[QPortId; 2]>,
    source_point_ports: Vec<QPortId>,
}

#[derive(Clone, Copy)]
struct QFaceChart<'a> {
    cut: &'a ExactRational,
    cut_east: &'a ExactRational,
    ports: &'a [QPort],
    nodes: &'a [QNode],
}

impl CutChart {
    fn face_chart(&self) -> QFaceChart<'_> {
        QFaceChart {
            cut: &self.cut,
            cut_east: &self.cut_east,
            ports: &self.ports,
            nodes: &self.nodes,
        }
    }
}

impl QuotientFaces {
    #[expect(
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        reason = "Q IDs and outgoing CSR offsets are created together from checked source spans"
    )]
    fn build(frame: GeographicFrame, chart_data: CutChart) -> Result<Self> {
        let mut union_find = UnionFind::new(chart_data.nodes.len());
        let mut degree = vec![0_usize; chart_data.nodes.len()];
        let mut face_span = vec![false; chart_data.spans.len()];
        for span in &chart_data.spans {
            if is_collapsed_pole_span(frame, &chart_data, span)? {
                continue;
            }
            face_span[span.id.0 as usize] = true;
            for node in span.nodes {
                let index = node.0 as usize;
                degree[index] = degree[index]
                    .checked_add(1)
                    .ok_or(ResourceError::SizeOverflow)?;
            }
            union_find.union(span.nodes[0].0 as usize, span.nodes[1].0 as usize);
        }

        let mut root_components = vec![None; chart_data.nodes.len()];
        let mut node_components = Vec::with_capacity(chart_data.nodes.len());
        let mut component_count = 0_usize;
        for node in 0..chart_data.nodes.len() {
            let root = union_find.find(node);
            let component = if let Some(component) = root_components[root] {
                component
            } else {
                let component = QComponentId(
                    u32::try_from(component_count).map_err(|_| ResourceError::SizeOverflow)?,
                );
                component_count = component_count
                    .checked_add(1)
                    .ok_or(ResourceError::SizeOverflow)?;
                root_components[root] = Some(component);
                component
            };
            node_components.push(component);
        }

        let face_span_count = face_span.iter().filter(|&&keep| keep).count();
        let dart_count = face_span_count
            .checked_mul(2)
            .ok_or(ResourceError::SizeOverflow)?;
        u32::try_from(dart_count).map_err(|_| ResourceError::SizeOverflow)?;
        let mut darts = Vec::with_capacity(dart_count);
        let mut span_darts = vec![None; chart_data.spans.len()];
        for span in &chart_data.spans {
            if !face_span[span.id.0 as usize] {
                continue;
            }
            let forward =
                QDartId(u32::try_from(darts.len()).map_err(|_| ResourceError::SizeOverflow)?);
            darts.push(QDart {
                span: span.id,
                from_port: span.ports[0],
                to_port: span.ports[1],
                from_node: span.nodes[0],
                to_node: span.nodes[1],
            });
            darts.push(QDart {
                span: span.id,
                from_port: span.ports[1],
                to_port: span.ports[0],
                from_node: span.nodes[1],
                to_node: span.nodes[0],
            });
            span_darts[span.id.0 as usize] = Some(forward);
        }

        let offset_count = chart_data
            .nodes
            .len()
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?;
        let mut outgoing_offsets = Vec::with_capacity(offset_count);
        outgoing_offsets.push(0_usize);
        for count in &degree {
            let next = outgoing_offsets
                .last()
                .copied()
                .expect("the offsets begin with zero")
                .checked_add(*count)
                .ok_or(ResourceError::SizeOverflow)?;
            outgoing_offsets.push(next);
        }
        if outgoing_offsets.last().copied() != Some(darts.len()) {
            return Err(GeometryErrorKind::Invalid(
                "Q fan degrees do not match the dart total".into(),
            )
            .into());
        }
        let mut outgoing = vec![QDartId(0); darts.len()];
        let mut cursor = outgoing_offsets[..chart_data.nodes.len()].to_vec();
        for (index, dart) in darts.iter().enumerate() {
            let node = dart.from_node.0 as usize;
            let position = cursor[node];
            outgoing[position] =
                QDartId(u32::try_from(index).map_err(|_| ResourceError::SizeOverflow)?);
            cursor[node] = position.wrapping_add(1);
        }

        for node_index in 0..chart_data.nodes.len() {
            let start = outgoing_offsets[node_index];
            let end = outgoing_offsets[node_index + 1];
            let row = &mut outgoing[start..end];
            try_sort_by(row, |left, right| {
                compare_q_darts(chart_data.face_chart(), &darts, *left, *right)
            })?;
        }
        let mut dart_positions = vec![0_usize; darts.len()];
        for node_index in 0..chart_data.nodes.len() {
            for (position, dart) in outgoing
                [outgoing_offsets[node_index]..outgoing_offsets[node_index + 1]]
                .iter()
                .enumerate()
            {
                dart_positions[dart.0 as usize] = position;
            }
        }

        let mut dart_faces = vec![None; darts.len()];
        let mut local_faces = Vec::new();
        for seed_index in 0..darts.len() {
            if dart_faces[seed_index].is_some() {
                continue;
            }
            let face = QLocalFaceId(
                u32::try_from(local_faces.len()).map_err(|_| ResourceError::SizeOverflow)?,
            );
            let seed = QDartId(u32::try_from(seed_index).map_err(|_| ResourceError::SizeOverflow)?);
            let component = node_components[darts[seed_index].from_node.0 as usize];
            let mut current = seed;
            loop {
                let current_index = current.0 as usize;
                if let Some(previous) = dart_faces[current_index] {
                    if current == seed && previous == face {
                        break;
                    }
                    return Err(GeometryErrorKind::Invalid(
                        "an exact Q face walk reached a dart from another walk".into(),
                    )
                    .into());
                }
                dart_faces[current_index] = Some(face);
                let dart = darts[current_index];
                let twin = QDartId(current.0 ^ 1);
                let twin_position = dart_positions[twin.0 as usize];
                let target = dart.to_node.0 as usize;
                let row_start = outgoing_offsets[target];
                let row_end = outgoing_offsets[target + 1];
                let row_len = row_end - row_start;
                if row_len == 0 {
                    return Err(GeometryErrorKind::Invalid(
                        "an exact Q dart reaches a node with an empty fan".into(),
                    )
                    .into());
                }
                let next_position = twin_position.wrapping_add(row_len).wrapping_sub(1) % row_len;
                current = outgoing[row_start + next_position];
                if current == seed {
                    if dart_faces[current.0 as usize] != Some(face) {
                        return Err(GeometryErrorKind::Invalid(
                            "an exact Q face walk did not close at its seed".into(),
                        )
                        .into());
                    }
                    break;
                }
            }
            local_faces.push(QLocalFace { component });
        }
        let dart_faces = dart_faces
            .into_iter()
            .map(|face| {
                face.ok_or_else(|| {
                    GeometryErrorKind::Invalid("an exact Q dart has no local face".into()).into()
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let mut span_local_faces = vec![None; chart_data.spans.len()];
        for (span, dart) in chart_data.spans.iter().zip(&span_darts) {
            if let Some(dart) = dart {
                span_local_faces[span.id.0 as usize] = Some([
                    dart_faces[dart.0 as usize],
                    dart_faces[(dart.0 ^ 1) as usize],
                ]);
            }
        }
        validate_spherical_components(
            component_count,
            &chart_data.spans,
            &face_span,
            &node_components,
            &degree,
            &local_faces,
        )?;
        let cut_index = QCutIndex::build(
            frame,
            chart_data.face_chart(),
            &node_components,
            component_count,
            &darts,
            &outgoing,
            &outgoing_offsets,
            &dart_faces,
        )?;

        let cut = chart_data.cut;
        let cut_east = chart_data.cut_east;
        Ok(Self {
            cut,
            cut_east,
            ports: chart_data.ports,
            nodes: chart_data.nodes,
            node_ports: chart_data.node_ports,
            spans: chart_data.spans,
            darts,
            span_darts,
            outgoing,
            outgoing_offsets,
            dart_positions,
            dart_faces,
            local_faces,
            node_components,
            span_local_faces,
            source_edge_ports: chart_data.source_edge_ports,
            source_point_ports: chart_data.source_point_ports,
            cut_index,
        })
    }
}

#[derive(Clone, Debug)]
struct QCutEvent {
    y: ExactRational,
    node: QNodeId,
    component: QComponentId,
    south_face: QLocalFaceId,
    north_face: QLocalFaceId,
}

#[derive(Clone, Debug)]
struct QCutInterval {
    id: CutIntervalId,
    low: ExactRational,
    high: ExactRational,
    low_face: Option<QLocalFaceId>,
    low_component: Option<QComponentId>,
    high_face: Option<QLocalFaceId>,
    high_component: Option<QComponentId>,
}

#[derive(Clone, Debug)]
struct QComponentCutRuns {
    event_range: Range<usize>,
    face_range: Range<usize>,
}

#[derive(Clone, Debug)]
struct HostRoot {
    component: QComponentId,
    face: QLocalFaceId,
    port: Option<QPortId>,
    point: ExactChartPoint,
}

/// Cut-root face and host queries sample the same southern side of the event.
const ROOT_CUT_FACE_SIDE: i8 = -1;

#[derive(Clone, Copy, Debug)]
enum SweepEventKind {
    End(usize),
    Start(usize),
    Query(usize),
    Seed,
}

#[derive(Clone, Debug)]
struct SweepEvent {
    y: ExactRational,
    kind: SweepEventKind,
}

#[derive(Clone, Debug)]
struct HostQuery {
    component: QComponentId,
    target: HostQueryTarget,
    germ: HostGerm,
}

#[derive(Clone, Copy, Debug)]
enum HostQueryTarget {
    Component(QComponentId),
    IsolatedNode(QNodeId),
}

struct GlobalFaceTopology {
    local_to_global: Vec<GlobalFaceId>,
    span_global_faces: Vec<Option<[GlobalFaceId; 2]>>,
    component_hosts: Vec<Option<GlobalFaceId>>,
    cut_interval_faces: Vec<GlobalFaceId>,
    source_node_faces: Vec<Option<GlobalFaceId>>,
    source_point_faces: Vec<Option<GlobalFaceId>>,
    seed_face: Option<GlobalFaceId>,
    seed_winding: Vec<i64>,
    count: usize,
    #[cfg(test)]
    host_status_comparisons: usize,
    #[cfg(test)]
    host_status_max_height: usize,
}

#[derive(Debug)]
pub(super) struct QCutIndex {
    events: Box<[QCutEvent]>,
    intervals: Box<[QCutInterval]>,
    local_event_indices: Box<[usize]>,
    component_runs: Box<[QComponentCutRuns]>,
    run_faces: Box<[QLocalFaceId]>,
    root_ports: Box<[Option<QPortId>]>,
}

impl QCutIndex {
    #[expect(
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        reason = "Q node, component, event, and CSR ordinals are admitted from the exact graph columns"
    )]
    fn build(
        frame: GeographicFrame,
        chart: QFaceChart<'_>,
        node_components: &[QComponentId],
        component_count: usize,
        darts: &[QDart],
        outgoing: &[QDartId],
        outgoing_offsets: &[usize],
        dart_faces: &[QLocalFaceId],
    ) -> Result<Self> {
        let mut events = Vec::new();
        let mut south_pole_face = None;
        let mut north_pole_face = None;
        for node in chart.nodes {
            let start = outgoing_offsets[node.id.0 as usize];
            let end = outgoing_offsets[node.id.0 as usize + 1];
            if start == end {
                continue;
            }
            if let Some(pole) = node.pole {
                let face = face_at_pole_longitude(
                    chart,
                    darts,
                    outgoing,
                    outgoing_offsets,
                    dart_faces,
                    node.id,
                )?;
                let value = (node_components[node.id.0 as usize], face);
                let slot = match pole {
                    Pole::South => &mut south_pole_face,
                    Pole::North => &mut north_pole_face,
                };
                if slot.replace(value).is_some() {
                    return Err(GeometryErrorKind::Invalid(
                        "Q contains more than one quotient node at a pole".into(),
                    )
                    .into());
                }
                continue;
            }
            if let QNodeKey::Cut(y) = &node.key {
                events.push(QCutEvent {
                    y: y.clone(),
                    node: node.id,
                    component: node_components[node.id.0 as usize],
                    south_face: face_in_direction(
                        chart,
                        darts,
                        outgoing,
                        outgoing_offsets,
                        dart_faces,
                        node.id,
                        &(ExactRational::zero(), ExactRational::from_f64(-1.0)),
                    )?,
                    north_face: face_in_direction(
                        chart,
                        darts,
                        outgoing,
                        outgoing_offsets,
                        dart_faces,
                        node.id,
                        &(ExactRational::zero(), ExactRational::one()),
                    )?,
                });
            }
        }
        try_sort_by(&mut events, |left, right| left.y.try_cmp(&right.y))?;
        for pair in events.windows(2) {
            let [left, right] = pair else {
                unreachable!("windows(2) yields adjacent Q cut events");
            };
            if left.y.try_cmp(&right.y)?.is_eq() {
                return Err(GeometryErrorKind::Invalid(
                    "two exact Q cut events have the same quotient latitude".into(),
                )
                .into());
            }
        }

        let mut local_event_indices = (0..events.len()).collect::<Vec<_>>();
        try_sort_by(&mut local_event_indices, |left, right| {
            let left = &events[*left];
            let right = &events[*right];
            let component = left.component.cmp(&right.component);
            Ok(if component.is_eq() {
                left.y.try_cmp(&right.y)?
            } else {
                component
            })
        })?;
        let mut component_event_counts = vec![0_usize; component_count];
        for &event in &local_event_indices {
            let component = events[event].component.0 as usize;
            component_event_counts[component] = component_event_counts[component]
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
        }
        let mut component_event_offsets = Vec::with_capacity(
            component_count
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?,
        );
        component_event_offsets.push(0_usize);
        for count in component_event_counts {
            component_event_offsets.push(
                component_event_offsets
                    .last()
                    .copied()
                    .expect("component offsets start at zero")
                    .checked_add(count)
                    .ok_or(ResourceError::SizeOverflow)?,
            );
        }

        let mut root_ports = vec![None; component_count];
        for port in chart.ports {
            if chart.nodes[port.node.0 as usize].pole.is_some()
                || outgoing_offsets[port.node.0 as usize]
                    == outgoing_offsets[port.node.0 as usize + 1]
            {
                continue;
            }
            let component = node_components[port.node.0 as usize].0 as usize;
            if root_ports[component].is_none() {
                root_ports[component] = Some(port.id);
            }
        }
        let west = (ExactRational::from_f64(-1.0), ExactRational::zero());
        let mut component_runs = Vec::with_capacity(component_count);
        let mut run_faces = Vec::with_capacity(
            events
                .len()
                .checked_add(component_count)
                .ok_or(ResourceError::SizeOverflow)?,
        );
        for component in 0..component_count {
            let event_range =
                component_event_offsets[component]..component_event_offsets[component + 1];
            let local_events = &local_event_indices[event_range.clone()];
            let face_start = run_faces.len();
            if local_events.is_empty() {
                let component_id = QComponentId(
                    u32::try_from(component).map_err(|_| ResourceError::SizeOverflow)?,
                );
                let south = south_pole_face
                    .filter(|(owner, _)| *owner == component_id)
                    .map(|(_, face)| face);
                let north = north_pole_face
                    .filter(|(owner, _)| *owner == component_id)
                    .map(|(_, face)| face);
                if let (Some(south), Some(north)) = (south, north)
                    && south != north
                {
                    return Err(GeometryErrorKind::Invalid(
                        "a Q component changes its cut face without a cut event".into(),
                    )
                    .into());
                }
                let face = if let Some(face) = south.or(north) {
                    Some(face)
                } else if let Some(port) = root_ports[component] {
                    let node = chart.ports[port.0 as usize].node;
                    Some(face_in_direction(
                        chart,
                        darts,
                        outgoing,
                        outgoing_offsets,
                        dart_faces,
                        node,
                        &west,
                    )?)
                } else {
                    None
                };
                if let Some(face) = face {
                    run_faces.push(face);
                }
            } else {
                let first = &events[local_events[0]];
                let component_id = first.component;
                let south_pole = south_pole_face
                    .filter(|(owner, _)| *owner == component_id)
                    .map(|(_, face)| face);
                let mut current = south_pole.unwrap_or(first.south_face);
                if current != first.south_face {
                    let pole_node = chart
                        .nodes
                        .iter()
                        .find(|node| {
                            node.pole == Some(Pole::South)
                                && node_components.get(node.id.0 as usize) == Some(&component_id)
                        })
                        .map(|node| node.id);
                    return Err(GeometryErrorKind::Invalid(
                        format!(
                            "south-pole cut wedge {south_pole:?} at node {pole_node:?} disagrees with component {component_id:?} cut event {:?} at latitude {:?} below-face {:?}",
                            first.node, first.y, first.south_face,
                        )
                        .into(),
                    )
                    .into());
                }
                run_faces.push(current);
                for &event_id in local_events {
                    let event = &events[event_id];
                    if event.south_face != current {
                        return Err(GeometryErrorKind::Invalid(
                            "a Q cut run changed face without an event".into(),
                        )
                        .into());
                    }
                    current = event.north_face;
                    run_faces.push(current);
                }
                let north_pole = north_pole_face
                    .filter(|(owner, _)| *owner == component_id)
                    .map(|(_, face)| face);
                if north_pole.is_some_and(|face| face != current) {
                    return Err(GeometryErrorKind::Invalid(
                        "the last Q cut run disagrees with the north-pole wedge".into(),
                    )
                    .into());
                }
            }
            component_runs.push(QComponentCutRuns {
                event_range,
                face_range: face_start..run_faces.len(),
            });
        }

        let south_y = ExactRational::from_f64(-frame.pole());
        let north_y = ExactRational::from_f64(frame.pole());
        let interval_count = events
            .len()
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?;
        let mut intervals = Vec::with_capacity(interval_count);
        for index in 0..interval_count {
            let low = if index == 0 {
                south_y.clone()
            } else {
                events[index - 1].y.clone()
            };
            let high = if index == events.len() {
                north_y.clone()
            } else {
                events[index].y.clone()
            };
            if !low.try_cmp(&high)?.is_lt() {
                return Err(GeometryErrorKind::Invalid(
                    "Q cut intervals are not strictly ordered".into(),
                )
                .into());
            }
            let low_event = index.checked_sub(1);
            let high_event = (index < events.len()).then_some(index);
            let (low_face, low_component) = low_event
                .map_or(south_pole_face, |event| {
                    Some((events[event].component, events[event].north_face))
                })
                .map_or((None, None), |(component, face)| {
                    (Some(face), Some(component))
                });
            let (high_face, high_component) = high_event
                .map_or(north_pole_face, |event| {
                    Some((events[event].component, events[event].south_face))
                })
                .map_or((None, None), |(component, face)| {
                    (Some(face), Some(component))
                });
            intervals.push(QCutInterval {
                id: CutIntervalId(u32::try_from(index).map_err(|_| ResourceError::SizeOverflow)?),
                low,
                high,
                low_face,
                low_component,
                high_face,
                high_component,
            });
        }
        Ok(Self {
            events: events.into_boxed_slice(),
            intervals: intervals.into_boxed_slice(),
            local_event_indices: local_event_indices.into_boxed_slice(),
            component_runs: component_runs.into_boxed_slice(),
            run_faces: run_faces.into_boxed_slice(),
            root_ports: root_ports.into_boxed_slice(),
        })
    }

    #[expect(
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        reason = "Q component and event IDs index the dense component-local cut runs built above"
    )]
    fn local_face_at(
        &self,
        component: QComponentId,
        latitude: &ExactRational,
        side: i8,
    ) -> Result<QLocalFaceId> {
        let runs = self
            .component_runs
            .get(component.0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q cut query references an unknown component".into())
            })?;
        if runs.face_range.is_empty() {
            return Err(GeometryErrorKind::Invalid(
                "an isolated Q component has no local cut face".into(),
            )
            .into());
        }
        let (mut low, mut high) = (runs.event_range.start, runs.event_range.end);
        while low < high {
            let middle = low + (high - low) / 2;
            let event_id = self.local_event_indices[middle];
            let order = self.events[event_id].y.try_cmp(latitude)?;
            if order.is_lt() || (order.is_eq() && side > 0) {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        let face = runs.face_range.start + (low - runs.event_range.start);
        self.run_faces.get(face).copied().ok_or_else(|| {
            GeometryErrorKind::Invalid("Q cut run has no face for the selected interval".into())
                .into()
        })
    }

    #[expect(
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        reason = "the binary search maintains low <= high <= the sorted exact event count"
    )]
    fn interval_at(&self, latitude: &ExactRational, side: i8) -> Result<&QCutInterval> {
        let mut low = 0_usize;
        let mut high = self.events.len();
        while low < high {
            let middle = low + (high - low) / 2;
            let order = self.events[middle].y.try_cmp(latitude)?;
            if order.is_lt() || (order.is_eq() && side > 0) {
                low = middle + 1;
            } else {
                high = middle;
            }
        }
        self.intervals.get(low).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q cut query is outside the latitude domain".into()).into()
        })
    }

    fn pole_interval_id(&self, pole: Pole) -> Result<CutIntervalId> {
        let interval = match pole {
            Pole::South => self.intervals.first(),
            Pole::North => self.intervals.last(),
        }
        .ok_or_else(|| GeometryErrorKind::Invalid("Q has no pole cut interval".into()))?;
        Ok(interval.id)
    }

    fn host_face_on_cut(
        &self,
        component: QComponentId,
        reference: QComponentId,
        latitude: &ExactRational,
        side: i8,
    ) -> Result<QLocalFaceId> {
        let interval = self.interval_at(latitude, side)?;
        if interval
            .low_component
            .is_some_and(|owner| owner != component)
            && let Some(face) = interval.low_face
        {
            return Ok(face);
        }
        if interval
            .high_component
            .is_some_and(|owner| owner != component)
            && let Some(face) = interval.high_face
        {
            return Ok(face);
        }
        self.local_face_at(reference, latitude, side)
    }

    fn validate_global_intervals(
        &self,
        local_to_global: &[GlobalFaceId],
        reference: QComponentId,
    ) -> Result<Vec<GlobalFaceId>> {
        let mut global_faces = Vec::with_capacity(self.intervals.len());
        for (interval_index, interval) in self.intervals.iter().enumerate() {
            let mut global_face = None;
            for local in [interval.low_face, interval.high_face]
                .into_iter()
                .flatten()
            {
                let global = *local_to_global.get(local.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "Q cut wedge references an unknown local face".into(),
                    )
                })?;
                if let Some((previous, previous_local)) = global_face
                    && previous != global
                {
                    return Err(GeometryErrorKind::Invalid(
                        format!(
                            "paired Q cut interval {interval_index} [{:?}, {:?}] maps local faces {previous_local:?} and {local:?} to different global faces {previous:?} and {global:?}",
                            interval.low, interval.high
                        )
                        .into(),
                    )
                    .into());
                }
                global_face = Some((global, local));
            }
            let global_face = if let Some((global_face, _)) = global_face {
                global_face
            } else {
                let midpoint = interval.low.try_midpoint(&interval.high)?;
                let local = self.local_face_at(reference, &midpoint, 0)?;
                *local_to_global.get(local.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "Q cut interval references an unknown local face".into(),
                    )
                })?
            };
            global_faces.push(global_face);
        }
        Ok(global_faces)
    }
}

fn apply_cut_event_winding(
    topology: &QuotientFaces,
    node_id: QNodeId,
    winding: &mut [i64],
) -> Result<()> {
    let node = topology
        .nodes
        .get(node_id.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q cut event has an unknown node".into()))?;
    let ports = topology
        .node_ports
        .get(node.ports.clone())
        .ok_or_else(|| GeometryErrorKind::Invalid("Q cut event has an invalid port run".into()))?;
    let mut east_port = None;
    for &port_id in ports {
        let port = topology
            .ports
            .get(port_id.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q cut event has an unknown port".into()))?;
        if port.cut && port.point.x.try_cmp(&topology.cut_east)?.is_eq() {
            east_port = Some(port.id);
            break;
        }
    }
    let east_port = east_port.ok_or_else(|| {
        GeometryErrorKind::Invalid("Q cut event has no east-side chart port".into())
    })?;
    let start = *topology
        .outgoing_offsets
        .get(node_id.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q cut event has no fan start".into()))?;
    let end = *topology
        .outgoing_offsets
        .get(node_id.0 as usize + 1)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q cut event has no fan end".into()))?;
    let fan = topology
        .outgoing
        .get(start..end)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q cut event has an invalid fan".into()))?;
    for &dart_id in fan {
        let dart = topology
            .darts
            .get(dart_id.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q cut fan has an unknown dart".into()))?;
        if dart.from_port != east_port {
            continue;
        }
        let span = topology
            .spans
            .get(dart.span.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q cut fan has an unknown span".into()))?;
        for_each_span_ring_transition(&span.owners, &mut |transition| {
            apply_ring_transition(winding, transition, 1)
        })?;
    }
    Ok(())
}

fn apply_ring_transition(
    winding: &mut [i64],
    transition: RingTransition,
    multiplier: i64,
) -> Result<()> {
    let value = winding
        .get_mut(transition.ring.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q transition has an unknown ring".into()))?;
    let delta = transition
        .delta
        .checked_mul(multiplier)
        .ok_or(ResourceError::SizeOverflow)?;
    *value = value
        .checked_add(delta)
        .ok_or(ResourceError::SizeOverflow)?;
    Ok(())
}

fn for_each_span_ring_transition(
    owners: &[QSpanOwner],
    visit: &mut impl FnMut(RingTransition) -> Result<()>,
) -> Result<()> {
    // The cut chart keeps source-edge order, and each ring's source edges are
    // contiguous in the inventory.
    let mut current: Option<RingTransition> = None;
    for owner in owners {
        let Some(transition) = owner.ring_transition else {
            continue;
        };
        if let Some(previous) = current {
            match transition.ring.cmp(&previous.ring) {
                Ordering::Less => {
                    return Err(GeometryErrorKind::Invalid(
                        "Q span source owners are not ordered by ring".into(),
                    )
                    .into());
                },
                Ordering::Equal => {
                    current = Some(RingTransition {
                        delta: previous
                            .delta
                            .checked_add(transition.delta)
                            .ok_or(ResourceError::SizeOverflow)?,
                        ..previous
                    });
                    continue;
                },
                Ordering::Greater => {
                    if previous.delta != 0 {
                        visit(previous)?;
                    }
                },
            }
        }
        current = Some(transition);
    }
    if let Some(transition) = current
        && transition.delta != 0
    {
        visit(transition)?;
    }
    Ok(())
}

fn span_has_ring_transition(span: &ChartSpan) -> Result<bool> {
    let mut found = false;
    for_each_span_ring_transition(&span.owners, &mut |_| {
        found = true;
        Ok(())
    })?;
    Ok(found)
}

fn source_ring_transition(
    sources: &ExactGeographicChartInventory,
    source_edge: SourceEdgeId,
    reversed: bool,
) -> Result<Option<RingTransition>> {
    let edge = sources
        .edges
        .get(source_edge.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q span has an unknown source edge".into()))?;
    let SourceEdgeKind::Ring { ring, .. } = edge.kind else {
        return Ok(None);
    };
    let source_ring = sources
        .rings
        .get(ring.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q span has an unknown source ring".into()))?;
    if !source_ring.has_areal_winding() {
        return Ok(None);
    }
    Ok(Some(RingTransition {
        ring,
        delta: if reversed { 1 } else { -1 },
    }))
}

#[derive(Clone, Copy)]
struct CollapsedShellCandidate {
    row: SourceRowId,
    polygon: SourcePolygonId,
    holes_before: usize,
}

#[derive(Clone, Copy)]
struct CollapsedRingPointCandidate {
    node: QNodeId,
    row: SourceRowId,
    polygon: SourcePolygonId,
    trimmed: bool,
}

fn collapsed_ring_node(
    ring: &super::exact::ExactSourceRing,
    topology: &QuotientFaces,
) -> Result<Option<QNodeId>> {
    if ring.kind != RingKind::Collapsed {
        return Ok(None);
    }
    let mut node = None;
    for edge_index in ring.source_edges.clone() {
        let ports = topology.source_edge_ports.get(edge_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("collapsed ring has no source edge ports".into())
        })?;
        for port_id in ports {
            let endpoint_node = topology
                .ports
                .get(port_id.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("collapsed ring has an unknown Q port".into())
                })?
                .node;
            if node.is_some_and(|node| node != endpoint_node) {
                return Ok(None);
            }
            node = Some(endpoint_node);
        }
    }
    Ok(node)
}

fn collapsed_point_candidates(
    sources: &ExactGeographicChartInventory,
    topology: &QuotientFaces,
    global: &GlobalFaceTopology,
    face_count: usize,
) -> Result<(Vec<CollapsedRingPointCandidate>, CollapsedPointFaces)> {
    let mut collapsed_hole_nodes = Vec::<(SourcePolygonId, QNodeId)>::new();
    for ring in sources
        .rings
        .iter()
        .filter(|ring| ring.hole && ring.kind == RingKind::Collapsed)
    {
        for edge_index in ring.source_edges.clone() {
            let ports = topology.source_edge_ports.get(edge_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("collapsed hole has no source edge ports".into())
            })?;
            for port_id in ports {
                let node = topology
                    .ports
                    .get(port_id.0 as usize)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid("collapsed hole has an unknown Q port".into())
                    })?
                    .node;
                collapsed_hole_nodes.push((ring.polygon, node));
            }
        }
    }
    for span in &topology.spans {
        if span.nodes[0] != span.nodes[1] {
            continue;
        }
        for owner in &span.owners {
            let edge = sources
                .edges
                .get(owner.source_edge.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q collapsed span has an unknown source edge".into())
                })?;
            if let SourceEdgeKind::Ring {
                polygon,
                hole: true,
                kind: RingKind::Collapsed,
                ..
            } = edge.kind
            {
                collapsed_hole_nodes.push((polygon, span.nodes[0]));
            }
        }
    }
    collapsed_hole_nodes.sort_unstable();
    collapsed_hole_nodes.dedup();

    let mut candidates = Vec::<CollapsedRingPointCandidate>::new();
    for (polygon_index, polygon) in sources.polygons.iter().enumerate() {
        let Some(shell) = sources.rings.get(polygon.rings.start) else {
            continue;
        };
        if let Some(node) = collapsed_ring_node(shell, topology)? {
            candidates.push(CollapsedRingPointCandidate {
                node,
                row: polygon.row,
                polygon: SourcePolygonId(
                    u32::try_from(polygon_index).map_err(|_| ResourceError::SizeOverflow)?,
                ),
                trimmed: false,
            });
        }
    }
    for span in &topology.spans {
        if span.nodes[0] != span.nodes[1] {
            continue;
        }
        for owner in &span.owners {
            let edge = sources
                .edges
                .get(owner.source_edge.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q collapsed span has an unknown source edge".into())
                })?;
            if let SourceEdgeKind::Ring {
                polygon,
                hole: false,
                kind: RingKind::Collapsed,
                ..
            } = edge.kind
            {
                candidates.push(CollapsedRingPointCandidate {
                    node: span.nodes[0],
                    row: edge.row,
                    polygon,
                    trimmed: false,
                });
            }
        }
    }
    candidates.sort_unstable_by_key(|candidate| (candidate.node, candidate.polygon));
    candidates.dedup_by_key(|candidate| (candidate.node, candidate.polygon));
    candidates.retain(|candidate| {
        collapsed_hole_nodes
            .binary_search(&(candidate.polygon, candidate.node))
            .is_err()
    });

    let mut face_candidates = Vec::<(usize, usize)>::new();
    for (candidate_index, candidate) in candidates.iter().enumerate() {
        let node_index = candidate.node.0 as usize;
        let start = *topology.outgoing_offsets.get(node_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("collapsed ring point has no fan start".into())
        })?;
        let end = *topology
            .outgoing_offsets
            .get(
                node_index
                    .checked_add(1)
                    .ok_or(ResourceError::SizeOverflow)?,
            )
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("collapsed ring point has no fan end".into())
            })?;
        if start == end {
            let face = global
                .source_node_faces
                .get(node_index)
                .copied()
                .flatten()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "isolated collapsed ring point has no global host face".into(),
                    )
                })?;
            face_candidates.push((face.0 as usize, candidate_index));
            continue;
        }
        let fan = topology.outgoing.get(start..end).ok_or_else(|| {
            GeometryErrorKind::Invalid("collapsed ring point has an invalid fan".into())
        })?;
        for &dart_id in fan {
            let local = *topology.dart_faces.get(dart_id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("collapsed ring point fan has no local face".into())
            })?;
            let face = *global
                .local_to_global
                .get(local.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("collapsed ring point face has no global ID".into())
                })?;
            face_candidates.push((face.0 as usize, candidate_index));
        }
    }
    let face_candidates = CollapsedPointFaces::build(face_count, &mut face_candidates)?;
    Ok((candidates, face_candidates))
}

fn trim_collapsed_points_on_face(
    face: GlobalFaceId,
    face_candidates: &CollapsedPointFaces,
    candidates: &mut [CollapsedRingPointCandidate],
    state: &FaceOccupancyState,
) -> Result<()> {
    let candidates_for_face = face_candidates
        .row(face.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("collapsed point has no face index".into()))?;
    for &candidate_index in candidates_for_face {
        let candidate = candidates.get_mut(candidate_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("collapsed point face index is unknown".into())
        })?;
        let holes = state
            .polygon_holes
            .get(candidate.polygon.0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("collapsed point polygon is unknown".into())
            })?;
        if *holes != 0 {
            candidate.trimmed = true;
        }
    }
    Ok(())
}

fn append_explicit_node_point_support(
    sources: &ExactGeographicChartInventory,
    topology: &QuotientFaces,
    memberships: &mut Vec<(usize, SourceRowId)>,
) -> Result<()> {
    if topology.source_point_ports.len() != sources.points.len() {
        return Err(GeometryErrorKind::Invalid(
            "Q source-point ports do not match the source inventory".into(),
        )
        .into());
    }
    for (source, &port_id) in sources.points.iter().zip(&topology.source_point_ports) {
        let port = topology.ports.get(port_id.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q source point has an unknown port".into())
        })?;
        memberships.push((port.node.0 as usize, source.row));
    }
    for span in &topology.spans {
        if span.nodes[0] != span.nodes[1] {
            continue;
        }
        for owner in &span.owners {
            let edge = sources
                .edges
                .get(owner.source_edge.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q collapsed span has an unknown source edge".into())
                })?;
            if matches!(edge.kind, SourceEdgeKind::Line) {
                memberships.push((span.nodes[0].0 as usize, edge.row));
            }
        }
    }
    Ok(())
}

fn span_has_collapsed_shell(
    span: &ChartSpan,
    sources: &ExactGeographicChartInventory,
) -> Result<bool> {
    for owner in &span.owners {
        let edge = sources
            .edges
            .get(owner.source_edge.0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q span has an unknown source edge".into())
            })?;
        if matches!(edge.kind, SourceEdgeKind::Ring {
            hole: false,
            kind: RingKind::Collapsed,
            ..
        }) {
            return Ok(true);
        }
    }
    Ok(false)
}

fn collect_collapsed_shell_candidates(
    span: &ChartSpan,
    sources: &ExactGeographicChartInventory,
    state: Option<&FaceOccupancyState>,
    shells: &mut Vec<(SourceRowId, SourcePolygonId)>,
    holes: &mut Vec<(SourceRowId, SourcePolygonId)>,
    candidates: &mut Vec<CollapsedShellCandidate>,
) -> Result<()> {
    shells.clear();
    holes.clear();
    candidates.clear();
    for owner in &span.owners {
        let edge = sources
            .edges
            .get(owner.source_edge.0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q span has an unknown source edge".into())
            })?;
        if let SourceEdgeKind::Ring {
            polygon,
            hole,
            kind: RingKind::Collapsed,
            ..
        } = edge.kind
        {
            let membership = (edge.row, polygon);
            if hole {
                holes.push(membership);
            } else {
                shells.push(membership);
            }
        }
    }
    shells.sort_unstable();
    shells.dedup();
    holes.sort_unstable();
    holes.dedup();
    for &(row, polygon) in shells.iter() {
        if holes.binary_search(&(row, polygon)).is_ok() {
            continue;
        }
        let holes_before = if let Some(state) = state {
            *state.polygon_holes.get(polygon.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "collapsed shell references an unknown polygon occupancy".into(),
                )
            })?
        } else {
            0
        };
        candidates.push(CollapsedShellCandidate {
            row,
            polygon,
            holes_before,
        });
    }
    Ok(())
}

impl HeapSize for QCutIndex {
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "HeapSize sums the already-owned cut-index buffers as an estimate"
    )]
    fn heap_bytes(&self) -> usize {
        self.events.len() * std::mem::size_of::<QCutEvent>()
            + self
                .events
                .iter()
                .map(|event| rational_heap_bytes(&event.y))
                .sum::<usize>()
            + self.intervals.len() * std::mem::size_of::<QCutInterval>()
            + self
                .intervals
                .iter()
                .map(|interval| {
                    rational_heap_bytes(&interval.low) + rational_heap_bytes(&interval.high)
                })
                .sum::<usize>()
            + self.local_event_indices.len() * std::mem::size_of::<usize>()
            + self.component_runs.len() * std::mem::size_of::<QComponentCutRuns>()
            + self.run_faces.len() * std::mem::size_of::<QLocalFaceId>()
            + self.root_ports.len() * std::mem::size_of::<Option<QPortId>>()
    }
}

fn span_x_slope(
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

fn compare_spans_at(
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

fn compare_span_to_query(
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

fn east_face_for_span(
    topology: &QuotientFaces,
    span_id: usize,
    query: &HostGerm,
    hit_x: &ExactRational,
) -> Result<QLocalFaceId> {
    let span = topology.spans.get(span_id).ok_or_else(|| {
        GeometryErrorKind::Invalid("host hit references an unknown Q span".into())
    })?;
    for port_id in span.ports {
        let port = topology
            .ports
            .get(port_id.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("host hit has an unknown Q port".into()))?;
        if port.point.y.try_cmp(&query.point.y)?.is_eq() && port.point.x.try_cmp(hit_x)?.is_eq() {
            let leading = (
                query.point.x.try_subtract(&port.point.x)?,
                query.point.y.try_subtract(&port.point.y)?,
            );
            return face_in_germ(
                &topology.ports,
                &topology.nodes,
                &topology.darts,
                &topology.outgoing,
                &topology.outgoing_offsets,
                &topology.dart_faces,
                port.node,
                &ExactFaceGerm {
                    leading,
                    perturbation: (query.dx.clone(), query.dy.clone()),
                },
            );
        }
    }
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

fn resolve_host_queries(
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
            let (hit_x, _) = span_x_slope(
                &topology.ports,
                &topology.spans,
                span_id,
                &query.germ.point.y,
            )?;
            east_face_for_span(topology, span_id, &query.germ, &hit_x)?
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

#[derive(Clone, Debug)]
struct SeedFaceGerm {
    local_face: QLocalFaceId,
    germ: HostGerm,
}

fn select_seed_face_germ(topology: &QuotientFaces) -> Result<Option<SeedFaceGerm>> {
    for (span_index, span) in topology.spans.iter().enumerate() {
        if !span_has_ring_transition(span)?
            || topology
                .span_darts
                .get(span_index)
                .copied()
                .flatten()
                .is_none()
        {
            continue;
        }
        let start = topology
            .ports
            .get(span.ports[0].0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q seed span has no first port".into()))?;
        let end = topology
            .ports
            .get(span.ports[1].0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q seed span has no second port".into()))?;
        let dx = end.point.x.try_subtract(&start.point.x)?;
        let dy = end.point.y.try_subtract(&start.point.y)?;
        if dx.is_zero() && dy.is_zero() {
            return Err(GeometryErrorKind::Invalid("Q seed span is degenerate".into()).into());
        }
        let point = ExactChartPoint::from_rationals(
            start.point.x.try_midpoint(&end.point.x)?,
            start.point.y.try_midpoint(&end.point.y)?,
        );
        let left_face = topology
            .span_local_faces
            .get(span_index)
            .and_then(|faces| *faces)
            .map(|faces| faces[0])
            .ok_or_else(|| GeometryErrorKind::Invalid("Q seed span has no left face".into()))?;
        return Ok(Some(SeedFaceGerm {
            local_face: left_face,
            germ: HostGerm {
                point,
                dx: ExactRational::zero().try_subtract(&dy)?,
                dy: dx,
            },
        }));
    }
    Ok(None)
}

fn node_has_edges(topology: &QuotientFaces, node: QNodeId) -> Result<bool> {
    let index = node.0 as usize;
    let next = index.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
    let start = topology
        .outgoing_offsets
        .get(index)
        .copied()
        .ok_or_else(|| GeometryErrorKind::Invalid("Q node has no fan offset".into()))?;
    let end = topology
        .outgoing_offsets
        .get(next)
        .copied()
        .ok_or_else(|| GeometryErrorKind::Invalid("Q node has no fan end".into()))?;
    Ok(start != end)
}

fn north_pole_ring_windings(sources: &ExactGeographicChartInventory) -> Vec<i64> {
    sources
        .rings
        .iter()
        .map(|ring| {
            if matches!(ring.summary.domain, crate::geographic::RingDomain::SeamOnly) {
                return 0;
            }
            if ring.summary.exact_axis_band {
                if ring.summary.north.get().to_bits() != sources.frame.pole().to_bits() {
                    return 0;
                }
                return match ring.axis_band_winding {
                    Some(RingWinding::CounterClockwise) => 1,
                    Some(RingWinding::Clockwise) => -1,
                    Some(RingWinding::Degenerate) | None => 0,
                };
            }
            if ring.summary.seam_transits == 0 {
                return 0;
            }
            if ring.summary.north.get() > 0.0 {
                ring.summary.seam_transits
            } else {
                0
            }
        })
        .collect()
}

fn apply_span_ring_transitions(
    winding: &mut [i64],
    owners: &[QSpanOwner],
    multiplier: i64,
) -> Result<()> {
    for_each_span_ring_transition(owners, &mut |transition| {
        apply_ring_transition(winding, transition, multiplier)
    })
}

#[expect(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "component, port, face, and status IDs index the dense Q columns that construct them"
)]
fn build_global_face_topology(
    frame: GeographicFrame,
    sources: &ExactGeographicChartInventory,
    topology: &QuotientFaces,
) -> Result<GlobalFaceTopology> {
    let component_count = topology.cut_index.component_runs.len();
    let mut edge_component = vec![false; component_count];
    for face in &topology.local_faces {
        edge_component[face.component.0 as usize] = true;
    }
    let mut roots = vec![None; component_count];
    for component in 0..component_count {
        if !edge_component[component] {
            continue;
        }
        let component_id =
            QComponentId(u32::try_from(component).map_err(|_| ResourceError::SizeOverflow)?);
        let root = if let Some(port) = topology.cut_index.root_ports[component] {
            let port_data = &topology.ports[port.0 as usize];
            let face = if port_data.cut {
                topology.cut_index.local_face_at(
                    component_id,
                    &port_data.point.y,
                    ROOT_CUT_FACE_SIDE,
                )?
            } else {
                face_in_direction(
                    topology.face_chart(),
                    &topology.darts,
                    &topology.outgoing,
                    &topology.outgoing_offsets,
                    &topology.dart_faces,
                    port_data.node,
                    &(ExactRational::from_f64(-1.0), ExactRational::zero()),
                )?
            };
            HostRoot {
                component: component_id,
                face,
                port: (!port_data.cut).then_some(port),
                point: port_data.point.clone(),
            }
        } else {
            let latitude = ExactRational::from_f64(-frame.pole());
            HostRoot {
                component: component_id,
                face: topology
                    .cut_index
                    .local_face_at(component_id, &latitude, 1)?,
                port: None,
                point: ExactChartPoint::from_rationals(topology.cut.clone(), latitude),
            }
        };
        roots[component] = Some(root);
    }

    let mut reference: Option<QComponentId> = None;
    for root in roots.iter().flatten() {
        let replace = if let Some(previous) = reference {
            let previous = roots[previous.0 as usize]
                .as_ref()
                .expect("the reference is an edge-bearing component");
            root.point
                .compare_lex(&previous.point)?
                .then_with(|| root.component.cmp(&previous.component))
                .is_lt()
        } else {
            true
        };
        if replace {
            reference = Some(root.component);
        }
    }
    if reference.is_none() {
        reference = edge_component
            .iter()
            .position(|&is_edge| is_edge)
            .map(|index| QComponentId(u32::try_from(index).expect("component IDs were admitted")));
    }
    let Some(reference) = reference else {
        let span_global_faces = vec![None; topology.spans.len()];
        return Ok(GlobalFaceTopology {
            local_to_global: Vec::new(),
            span_global_faces,
            component_hosts: vec![None; component_count],
            cut_interval_faces: vec![GlobalFaceId(0); topology.cut_index.intervals.len()],
            source_node_faces: vec![Some(GlobalFaceId(0)); topology.nodes.len()],
            source_point_faces: vec![Some(GlobalFaceId(0)); sources.points.len()],
            seed_face: None,
            seed_winding: Vec::new(),
            count: 1,
            #[cfg(test)]
            host_status_comparisons: 0,
            #[cfg(test)]
            host_status_max_height: 0,
        });
    };

    let mut host_faces = vec![None; component_count];
    let mut queries = Vec::new();
    let mut isolated_node_host_by_node = vec![None; topology.nodes.len()];
    let seed = select_seed_face_germ(topology)?;
    let mut seed_ray_winding = vec![0_i64; sources.rings.len()];
    for (component, root) in roots.iter().enumerate() {
        let Some(root) = root else {
            continue;
        };
        if root.component == reference {
            continue;
        }
        if let Some(port) = root.port {
            queries.push(HostQuery {
                component: root.component,
                target: HostQueryTarget::Component(root.component),
                germ: HostGerm {
                    point: topology.ports[port.0 as usize].point.clone(),
                    dx: ExactRational::zero(),
                    dy: ExactRational::zero().try_subtract(&ExactRational::one())?,
                },
            });
        } else {
            let host = topology.cut_index.host_face_on_cut(
                root.component,
                reference,
                &root.point.y,
                ROOT_CUT_FACE_SIDE,
            )?;
            if topology.local_faces[host.0 as usize].component == root.component {
                return Err(GeometryErrorKind::Invalid(
                    "a pole-only Q component selected itself as its host".into(),
                )
                .into());
            }
            host_faces[component] = Some(host);
        }
    }
    for node in &topology.nodes {
        if node_has_edges(topology, node.id)? || node.pole.is_some() {
            continue;
        }
        let port_id = topology
            .node_ports
            .get(node.ports.start)
            .copied()
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("isolated Q node has no chart port".into())
            })?;
        let port = topology.ports.get(port_id.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("isolated Q node has an unknown chart port".into())
        })?;
        queries.push(HostQuery {
            component: topology.node_components[node.id.0 as usize],
            target: HostQueryTarget::IsolatedNode(node.id),
            germ: HostGerm {
                point: port.point.clone(),
                dx: ExactRational::zero(),
                dy: ExactRational::one(),
            },
        });
    }
    let mut events = Vec::new();
    let event_capacity = topology
        .spans
        .len()
        .checked_mul(2)
        .and_then(|count| count.checked_add(queries.len()))
        .and_then(|count| count.checked_add(usize::from(seed.is_some())))
        .ok_or(ResourceError::SizeOverflow)?;
    events.reserve(event_capacity);
    for (span_id, span) in topology.spans.iter().enumerate() {
        if topology.span_darts[span_id].is_none() {
            continue;
        }
        let start = &topology.ports[span.ports[0].0 as usize].point;
        let end = &topology.ports[span.ports[1].0 as usize].point;
        match start.y.try_cmp(&end.y)? {
            Ordering::Less => {
                events.push(SweepEvent {
                    y: start.y.clone(),
                    kind: SweepEventKind::Start(span_id),
                });
                events.push(SweepEvent {
                    y: end.y.clone(),
                    kind: SweepEventKind::End(span_id),
                });
            },
            Ordering::Greater => {
                events.push(SweepEvent {
                    y: end.y.clone(),
                    kind: SweepEventKind::Start(span_id),
                });
                events.push(SweepEvent {
                    y: start.y.clone(),
                    kind: SweepEventKind::End(span_id),
                });
            },
            Ordering::Equal => {},
        }
    }
    for (query, root) in queries.iter().enumerate() {
        events.push(SweepEvent {
            y: root.germ.point.y.clone(),
            kind: SweepEventKind::Query(query),
        });
    }
    if let Some(seed) = &seed {
        events.push(SweepEvent {
            y: seed.germ.point.y.clone(),
            kind: SweepEventKind::Seed,
        });
    }
    try_sort_by(&mut events, |left, right| left.y.try_cmp(&right.y))?;
    let status_capacity = topology
        .span_darts
        .iter()
        .filter(|span| span.is_some())
        .count();
    let mut status = FallibleAvl::with_capacity(status_capacity);
    let mut ends = Vec::new();
    let mut starts = Vec::new();
    let mut queries_below = Vec::new();
    let mut query_events = Vec::new();
    #[cfg(test)]
    let mut host_status_comparisons = 0_usize;
    #[cfg(test)]
    let mut host_status_max_height = 0_usize;
    let mut root_face_by_component = vec![None; component_count];
    let mut host_face_by_component = host_faces;
    for root in roots.iter().flatten() {
        root_face_by_component[root.component.0 as usize] = Some(root.face);
    }
    let mut cursor = 0_usize;
    while cursor < events.len() {
        let event_y = events[cursor].y.clone();
        let group_start = cursor;
        while cursor < events.len() && events[cursor].y.try_cmp(&event_y)?.is_eq() {
            cursor += 1;
        }
        ends.clear();
        starts.clear();
        queries_below.clear();
        query_events.clear();
        let seed_event = events[group_start..cursor]
            .iter()
            .any(|event| matches!(event.kind, SweepEventKind::Seed));
        for event in &events[group_start..cursor] {
            match event.kind {
                SweepEventKind::End(span) => ends.push(span),
                SweepEventKind::Start(span) => starts.push(span),
                SweepEventKind::Query(query) => {
                    let query_data = queries.get(query).ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q sweep has an unknown host query".into())
                    })?;
                    if query_data.germ.dy.try_cmp(&ExactRational::zero())?.is_lt() {
                        queries_below.push(query);
                    } else {
                        query_events.push(query);
                    }
                },
                SweepEventKind::Seed => {},
            }
        }
        {
            let mut observe_comparison = || {
                #[cfg(test)]
                {
                    host_status_comparisons = host_status_comparisons.saturating_add(1);
                }
            };
            resolve_host_queries(
                &queries_below,
                &queries,
                reference,
                topology,
                &status,
                &mut host_face_by_component,
                &mut isolated_node_host_by_node,
                &mut observe_comparison,
            )?;
        }
        for &span in &ends {
            let mut compare_below = |left: &usize, right: &usize| {
                #[cfg(test)]
                {
                    host_status_comparisons = host_status_comparisons.saturating_add(1);
                }
                compare_spans_at(
                    &topology.ports,
                    &topology.spans,
                    *left,
                    *right,
                    &event_y,
                    -1,
                )
            };
            if !status.remove(&span, &mut compare_below)? {
                return Err(GeometryErrorKind::Invalid(
                    "the exact Q host sweep ended an inactive span".into(),
                )
                .into());
            }
            #[cfg(test)]
            {
                host_status_max_height = host_status_max_height.max(status.tree_height());
            }
        }
        try_sort_by(&mut starts, |left, right| {
            #[cfg(test)]
            {
                host_status_comparisons = host_status_comparisons.saturating_add(1);
            }
            compare_spans_at(&topology.ports, &topology.spans, *left, *right, &event_y, 1)
        })?;
        for &span in &starts {
            let mut compare_above = |left: &usize, right: &usize| {
                #[cfg(test)]
                {
                    host_status_comparisons = host_status_comparisons.saturating_add(1);
                }
                compare_spans_at(&topology.ports, &topology.spans, *left, *right, &event_y, 1)
            };
            if !status.insert(span, &mut compare_above)? {
                return Err(GeometryErrorKind::Invalid(
                    "the exact Q host sweep started an active span twice".into(),
                )
                .into());
            }
            #[cfg(test)]
            {
                host_status_max_height = host_status_max_height.max(status.tree_height());
            }
        }
        starts.clear();
        {
            let mut observe_comparison = || {
                #[cfg(test)]
                {
                    host_status_comparisons = host_status_comparisons.saturating_add(1);
                }
            };
            resolve_host_queries(
                &query_events,
                &queries,
                reference,
                topology,
                &status,
                &mut host_face_by_component,
                &mut isolated_node_host_by_node,
                &mut observe_comparison,
            )?;
        }
        ends.clear();
        query_events.clear();
        if seed_event {
            let seed = seed
                .as_ref()
                .ok_or_else(|| GeometryErrorKind::Invalid("Q sweep lost its seed germ".into()))?;
            status.for_each(&mut |span_id| {
                let span = topology.spans.get(*span_id).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q seed sweep has an unknown span".into())
                })?;
                let order = {
                    #[cfg(test)]
                    {
                        host_status_comparisons = host_status_comparisons.saturating_add(1);
                    }
                    compare_span_to_query(&topology.ports, &topology.spans, *span_id, &seed.germ)?
                };
                if order.is_eq() {
                    return Err(GeometryErrorKind::Invalid(
                        "the exact Q seed germ lies on an unmerged span".into(),
                    )
                    .into());
                }
                if order.is_gt() {
                    let start = topology
                        .ports
                        .get(span.ports[0].0 as usize)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid("Q seed span has no first port".into())
                        })?;
                    let end = topology
                        .ports
                        .get(span.ports[1].0 as usize)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid("Q seed span has no second port".into())
                        })?;
                    let vertical_order = start.point.y.try_cmp(&end.point.y)?;
                    if vertical_order.is_eq() {
                        return Err(GeometryErrorKind::Invalid(
                            "a horizontal Q span entered the exact seed status".into(),
                        )
                        .into());
                    }
                    let multiplier = if vertical_order.is_lt() { -1 } else { 1 };
                    apply_span_ring_transitions(&mut seed_ray_winding, &span.owners, multiplier)?;
                }
                Ok(())
            })?;
        }
    }

    let mut seed_winding = north_pole_ring_windings(sources);
    if let Some(seed) = &seed {
        for event in topology.cut_index.events.iter().rev() {
            if event.y.try_cmp(&seed.germ.point.y)?.is_gt() {
                apply_cut_event_winding(topology, event.node, &mut seed_winding)?;
            }
        }
        for (value, delta) in seed_winding.iter_mut().zip(seed_ray_winding) {
            *value = value
                .checked_add(delta)
                .ok_or(ResourceError::SizeOverflow)?;
        }
    }

    let edge_component_count = edge_component.iter().filter(|&&edge| edge).count();
    let mut face_union = UnionFind::new(topology.local_faces.len());
    let mut links = 0_usize;
    for (component, root_face) in root_face_by_component.iter().enumerate() {
        if !edge_component[component] || reference.0 as usize == component {
            continue;
        }
        let root_face = root_face.ok_or_else(|| {
            GeometryErrorKind::Invalid("an edge component has no local root face".into())
        })?;
        let host_face = host_face_by_component[component].ok_or_else(|| {
            GeometryErrorKind::Invalid("an edge component has no exact host face".into())
        })?;
        let root_component = topology.local_faces[root_face.0 as usize].component;
        let host_component = topology.local_faces[host_face.0 as usize].component;
        if root_component.0 as usize != component || host_component == root_component {
            return Err(GeometryErrorKind::Invalid(
                "a Q host link does not connect distinct edge components".into(),
            )
            .into());
        }
        let root = face_union.find(root_face.0 as usize);
        let host = face_union.find(host_face.0 as usize);
        if root == host {
            return Err(GeometryErrorKind::Invalid(
                "Q component host links contain a cycle".into(),
            )
            .into());
        }
        face_union.union(root, host);
        links = links.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
    }
    let expected_links = edge_component_count
        .checked_sub(1)
        .ok_or(ResourceError::SizeOverflow)?;
    if links != expected_links {
        return Err(GeometryErrorKind::Invalid(
            "the Q host sweep did not link every edge component".into(),
        )
        .into());
    }

    let mut root_to_global = vec![None; topology.local_faces.len()];
    let mut local_to_global = Vec::with_capacity(topology.local_faces.len());
    let mut global_face_count = 0_usize;
    for local in 0..topology.local_faces.len() {
        let root = face_union.find(local);
        let global = if let Some(global) = root_to_global[root] {
            global
        } else {
            let global = GlobalFaceId(
                u32::try_from(global_face_count).map_err(|_| ResourceError::SizeOverflow)?,
            );
            root_to_global[root] = Some(global);
            global_face_count = global_face_count
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
            global
        };
        local_to_global.push(global);
    }
    let count = global_face_count;
    let seed_face = seed.as_ref().map_or_else(
        || {
            seed_winding
                .iter()
                .any(|&winding| winding != 0)
                .then_some(GlobalFaceId(0))
        },
        |seed| Some(local_to_global[seed.local_face.0 as usize]),
    );
    let span_global_faces = topology
        .span_local_faces
        .iter()
        .map(|faces| {
            faces
                .map(|faces| {
                    Ok([
                        local_to_global[faces[0].0 as usize],
                        local_to_global[faces[1].0 as usize],
                    ])
                })
                .transpose()
        })
        .collect::<Result<Vec<_>>>()?;
    let mut component_hosts = vec![None; component_count];
    for (component, root) in root_face_by_component.iter().enumerate() {
        if let Some(root) = root {
            component_hosts[component] = Some(local_to_global[root.0 as usize]);
        }
    }
    let cut_interval_faces = topology
        .cut_index
        .validate_global_intervals(&local_to_global, reference)?;
    let mut source_node_faces = Vec::with_capacity(topology.nodes.len());
    for node in &topology.nodes {
        if node_has_edges(topology, node.id)? {
            source_node_faces.push(None);
            continue;
        }
        let local = if let Some(pole) = node.pole {
            let interval = topology.cut_index.pole_interval_id(pole)?;
            *cut_interval_faces.get(interval.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("an isolated Q pole has no cut-interval host".into())
            })?
        } else {
            let local = isolated_node_host_by_node
                .get(node.id.0 as usize)
                .and_then(|face| *face)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("an isolated Q node has no exact host face".into())
                })?;
            *local_to_global.get(local.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("an isolated Q node host is unknown".into())
            })?
        };
        source_node_faces.push(Some(local));
    }
    if topology.source_point_ports.len() != sources.points.len() {
        return Err(GeometryErrorKind::Invalid(
            "Q source-point ports do not match the source inventory".into(),
        )
        .into());
    }
    let mut source_point_faces = Vec::with_capacity(sources.points.len());
    for &port_id in &topology.source_point_ports {
        let port = topology.ports.get(port_id.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q source point has an unknown port".into())
        })?;
        let node_index = port.node.0 as usize;
        let node = topology.nodes.get(node_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q source point has an unknown node".into())
        })?;
        if node_has_edges(topology, node.id)? {
            source_point_faces.push(None);
            continue;
        }
        source_point_faces.push(Some(
            *source_node_faces
                .get(node_index)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q source point has no node host entry".into())
                })?
                .as_ref()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("an isolated Q source point has no host face".into())
                })?,
        ));
    }
    validate_global_euler(topology, edge_component_count, count)?;
    Ok(GlobalFaceTopology {
        local_to_global,
        span_global_faces,
        component_hosts,
        cut_interval_faces,
        source_node_faces,
        source_point_faces,
        seed_face,
        seed_winding,
        count: count.max(1),
        #[cfg(test)]
        host_status_comparisons,
        #[cfg(test)]
        host_status_max_height,
    })
}

fn validate_global_euler(
    topology: &QuotientFaces,
    edge_component_count: usize,
    global_face_count: usize,
) -> Result<()> {
    let vertices = topology
        .outgoing_offsets
        .windows(2)
        .filter(|window| window.first() != window.get(1))
        .count();
    let edges = topology
        .span_darts
        .iter()
        .filter(|dart| dart.is_some())
        .count();
    let characteristic = vertices
        .checked_add(global_face_count)
        .and_then(|value| value.checked_sub(edges));
    let expected = edge_component_count
        .checked_add(1)
        .ok_or(ResourceError::SizeOverflow)?;
    if characteristic != Some(expected) {
        return Err(GeometryErrorKind::Invalid(
            "global Q faces do not satisfy the spherical Euler relation".into(),
        )
        .into());
    }
    Ok(())
}
struct QuotientFaces {
    cut: ExactRational,
    cut_east: ExactRational,
    ports: Vec<QPort>,
    nodes: Vec<QNode>,
    node_ports: Vec<QPortId>,
    spans: Vec<ChartSpan>,
    darts: Vec<QDart>,
    span_darts: Vec<Option<QDartId>>,
    outgoing: Vec<QDartId>,
    outgoing_offsets: Vec<usize>,
    dart_positions: Vec<usize>,
    dart_faces: Vec<QLocalFaceId>,
    local_faces: Vec<QLocalFace>,
    node_components: Vec<QComponentId>,
    span_local_faces: Vec<Option<[QLocalFaceId; 2]>>,
    source_edge_ports: Vec<[QPortId; 2]>,
    source_point_ports: Vec<QPortId>,
    cut_index: QCutIndex,
}

impl QuotientFaces {
    fn face_chart(&self) -> QFaceChart<'_> {
        QFaceChart {
            cut: &self.cut,
            cut_east: &self.cut_east,
            ports: &self.ports,
            nodes: &self.nodes,
        }
    }
}

struct UnionFind {
    parent: Vec<usize>,
    size: Vec<usize>,
}

impl UnionFind {
    fn new(count: usize) -> Self {
        Self {
            parent: (0..count).collect(),
            size: vec![1; count],
        }
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "union-find IDs are admitted from the exact component and local-face counts"
    )]
    fn find(&mut self, mut node: usize) -> usize {
        while self.parent[node] != node {
            self.parent[node] = self.parent[self.parent[node]];
            node = self.parent[node];
        }
        node
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "union-find roots are produced by find over the same admitted arrays"
    )]
    fn union(&mut self, left: usize, right: usize) {
        let (mut left, mut right) = (self.find(left), self.find(right));
        if left == right {
            return;
        }
        if self.size[left] < self.size[right] {
            std::mem::swap(&mut left, &mut right);
        }
        self.parent[right] = left;
        self.size[left] = self.size[left].wrapping_add(self.size[right]);
    }
}

fn is_collapsed_pole_span(
    frame: GeographicFrame,
    chart: &CutChart,
    span: &ChartSpan,
) -> Result<bool> {
    if span.nodes[0] != span.nodes[1] {
        return Ok(false);
    }
    let node = chart
        .nodes
        .get(span.nodes[0].0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q span has an unknown node".into()))?;
    let Some(pole) = node.pole else {
        return Ok(false);
    };
    let latitude = ExactRational::from_f64(match pole {
        Pole::North => frame.pole(),
        Pole::South => -frame.pole(),
    });
    let start = chart
        .ports
        .get(span.ports[0].0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q span has an unknown start port".into()))?;
    let end = chart
        .ports
        .get(span.ports[1].0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q span has an unknown end port".into()))?;
    Ok(start.point.y.try_cmp(&latitude)?.is_eq() && end.point.y.try_cmp(&latitude)?.is_eq())
}

fn compare_q_darts(
    chart: QFaceChart<'_>,
    darts: &[QDart],
    left: QDartId,
    right: QDartId,
) -> Result<Ordering> {
    let left = darts
        .get(left.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q fan references an unknown dart".into()))?;
    let right = darts
        .get(right.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q fan references an unknown dart".into()))?;
    if left.from_node != right.from_node {
        return Err(GeometryErrorKind::Invalid(
            "Q fan comparator received rays from different nodes".into(),
        )
        .into());
    }
    let node = chart
        .nodes
        .get(left.from_node.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q fan has an unknown node".into()))?;
    let left_origin = chart
        .ports
        .get(left.from_port.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q dart has an unknown origin port".into()))?;
    let left_target = chart
        .ports
        .get(left.to_port.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q dart has an unknown target port".into()))?;
    let right_origin = chart
        .ports
        .get(right.from_port.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q dart has an unknown origin port".into()))?;
    let right_target = chart
        .ports
        .get(right.to_port.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q dart has an unknown target port".into()))?;

    if let Some(pole) = node.pole {
        let canonical_longitude = |longitude: &ExactRational| -> Result<ExactRational> {
            if longitude.try_cmp(chart.cut_east)?.is_eq() {
                Ok(chart.cut.clone())
            } else {
                Ok(longitude.clone())
            }
        };
        let left_longitude = canonical_longitude(&left_origin.point.x)?;
        let right_longitude = canonical_longitude(&right_origin.point.x)?;
        let longitude_order = compare_pole_meridians(pole, &left_longitude, &right_longitude)?;
        if !longitude_order.is_eq() {
            return Ok(longitude_order);
        }
        let left_ray = pole_tangent_ray(&left_origin.point, &left_target.point)?;
        let right_ray = pole_tangent_ray(&right_origin.point, &right_target.point)?;
        let order = compare_pole_tangent_rays(pole, &left_ray, &right_ray)?;
        if order.is_eq() {
            return Err(coincident_q_ray_error(
                left.from_node,
                left.span,
                right.span,
                &left_ray,
                &right_ray,
            ));
        }
        return Ok(order);
    }

    let left_ray = vector_difference(&left_origin.point, &left_target.point)?;
    let right_ray = vector_difference(&right_origin.point, &right_target.point)?;
    let order = compare_exact_rays_allow_equal(&left_ray, &right_ray)?;
    if order.is_eq() {
        return Err(coincident_q_ray_error(
            left.from_node,
            left.span,
            right.span,
            &left_ray,
            &right_ray,
        ));
    }
    Ok(order)
}

fn coincident_q_ray_error(
    node: QNodeId,
    left_span: ChartSpanId,
    right_span: ChartSpanId,
    left_ray: &(ExactRational, ExactRational),
    right_ray: &(ExactRational, ExactRational),
) -> crate::error::Error {
    GeometryErrorKind::Invalid(
        format!(
            "Q spans {left_span:?} and {right_span:?} leave node {node:?} with coincident tangent rays {left_ray:?} and {right_ray:?}"
        )
        .into(),
    )
    .into()
}

fn compare_exact_rays(
    left: &(ExactRational, ExactRational),
    right: &(ExactRational, ExactRational),
) -> Result<Ordering> {
    let order = compare_exact_rays_allow_equal(left, right)?;
    if order.is_eq() {
        return Err(GeometryErrorKind::Invalid(
            "two exact Q spans leave a quotient port in the same tangent direction".into(),
        )
        .into());
    }
    Ok(order)
}

fn compare_exact_rays_allow_equal(
    left: &(ExactRational, ExactRational),
    right: &(ExactRational, ExactRational),
) -> Result<Ordering> {
    let half_order = exact_ray_half(left).cmp(&exact_ray_half(right));
    if !half_order.is_eq() {
        return Ok(half_order);
    }
    match vector_cross(left, right)?.sign() {
        ExactSign::Positive => Ok(Ordering::Less),
        ExactSign::Negative => Ok(Ordering::Greater),
        ExactSign::Zero => Ok(Ordering::Equal),
    }
}

fn exact_ray_half(ray: &(ExactRational, ExactRational)) -> u8 {
    u8::from(
        !(ray.1.sign() == ExactSign::Positive
            || (ray.1.is_zero() && ray.0.sign() != ExactSign::Negative)),
    )
}

fn compare_ray_to_germ(
    ray: &(ExactRational, ExactRational),
    germ: &ExactFaceGerm,
) -> Result<Ordering> {
    if germ.leading.0.is_zero() && germ.leading.1.is_zero() {
        return Err(GeometryErrorKind::Invalid(
            "an exact face germ has no leading direction".into(),
        )
        .into());
    }
    let leading_order = compare_exact_rays_allow_equal(ray, &germ.leading)?;
    if !leading_order.is_eq() {
        return Ok(leading_order);
    }
    let leading_half = exact_ray_half(&germ.leading);
    let germ_half = if !germ.leading.1.is_zero() {
        leading_half
    } else if germ.perturbation.1.sign() == ExactSign::Negative {
        1
    } else if germ.perturbation.1.sign() == ExactSign::Positive {
        0
    } else {
        leading_half
    };
    let ray_half = exact_ray_half(ray);
    if ray_half != germ_half {
        return Ok(ray_half.cmp(&germ_half));
    }
    match vector_cross(&germ.leading, &germ.perturbation)?.sign() {
        ExactSign::Positive => Ok(Ordering::Less),
        ExactSign::Negative => Ok(Ordering::Greater),
        ExactSign::Zero => Ok(Ordering::Equal),
    }
}

fn pole_tangent_ray(
    origin: &ExactChartPoint,
    target: &ExactChartPoint,
) -> Result<(ExactRational, ExactRational)> {
    let dx = target.x.try_subtract(&origin.x)?;
    let latitude_delta = target.y.try_subtract(&origin.y)?;
    let absolute_latitude_delta = if latitude_delta.sign() == ExactSign::Negative {
        ExactRational::zero().try_subtract(&latitude_delta)?
    } else {
        latitude_delta
    };
    let dy = ExactRational::zero().try_subtract(&absolute_latitude_delta)?;
    Ok((dx, dy))
}

fn compare_q_dart_to_direction(
    chart: QFaceChart<'_>,
    darts: &[QDart],
    dart: QDartId,
    direction: &(ExactRational, ExactRational),
) -> Result<Ordering> {
    let dart = darts.get(dart.0 as usize).ok_or_else(|| {
        GeometryErrorKind::Invalid("Q direction references an unknown dart".into())
    })?;
    let node = chart.nodes.get(dart.from_node.0 as usize).ok_or_else(|| {
        GeometryErrorKind::Invalid("Q direction references an unknown node".into())
    })?;
    if node.pole.is_some() {
        return Err(GeometryErrorKind::Invalid(
            "a planar direction cannot order a pole fan".into(),
        )
        .into());
    }
    let origin = chart
        .ports
        .get(dart.from_port.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q direction has no origin port".into()))?;
    let target = chart
        .ports
        .get(dart.to_port.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q direction has no target port".into()))?;
    compare_exact_rays(&vector_difference(&origin.point, &target.point)?, direction)
}

#[expect(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "the Q outgoing range is the admitted CSR fan for this dense node ID"
)]
fn face_in_direction(
    chart: QFaceChart<'_>,
    darts: &[QDart],
    outgoing: &[QDartId],
    offsets: &[usize],
    dart_faces: &[QLocalFaceId],
    node: QNodeId,
    direction: &(ExactRational, ExactRational),
) -> Result<QLocalFaceId> {
    let node_index = node.0 as usize;
    let start = *offsets.get(node_index).ok_or_else(|| {
        GeometryErrorKind::Invalid("Q face query references an unknown node".into())
    })?;
    let end = *offsets.get(node_index + 1).ok_or_else(|| {
        GeometryErrorKind::Invalid("Q face query references an incomplete fan".into())
    })?;
    if start == end {
        return Err(
            GeometryErrorKind::Invalid("Q face query reached an isolated node".into()).into(),
        );
    }
    let mut low = start;
    let mut high = end;
    while low < high {
        let middle = low + (high - low) / 2;
        let dart = outgoing[middle];
        match compare_q_dart_to_direction(chart, darts, dart, direction)? {
            Ordering::Less => low = middle + 1,
            Ordering::Equal => {
                return Err(GeometryErrorKind::Invalid(
                    "Q face germ overlaps an exact source span".into(),
                )
                .into());
            },
            Ordering::Greater => high = middle,
        }
    }
    let preceding = if low == start { end - 1 } else { low - 1 };
    let dart = outgoing[preceding];
    dart_faces
        .get(dart.0 as usize)
        .copied()
        .ok_or_else(|| GeometryErrorKind::Invalid("Q face query found no dart face".into()).into())
}

fn compare_q_dart_to_germ(
    ports: &[QPort],
    nodes: &[QNode],
    darts: &[QDart],
    dart: QDartId,
    germ: &ExactFaceGerm,
) -> Result<Ordering> {
    let dart = darts
        .get(dart.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q germ references an unknown dart".into()))?;
    let node = nodes
        .get(dart.from_node.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q germ references an unknown node".into()))?;
    if node.pole.is_some() {
        return Err(GeometryErrorKind::Invalid(
            "a planar face germ cannot order a pole fan".into(),
        )
        .into());
    }
    let origin = ports
        .get(dart.from_port.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q germ has no origin port".into()))?;
    let target = ports
        .get(dart.to_port.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q germ has no target port".into()))?;
    compare_ray_to_germ(&vector_difference(&origin.point, &target.point)?, germ)
}

#[expect(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "the Q outgoing range is the admitted CSR fan for this dense node ID"
)]
fn face_in_germ(
    ports: &[QPort],
    nodes: &[QNode],
    darts: &[QDart],
    outgoing: &[QDartId],
    offsets: &[usize],
    dart_faces: &[QLocalFaceId],
    node: QNodeId,
    germ: &ExactFaceGerm,
) -> Result<QLocalFaceId> {
    let node_index = node.0 as usize;
    let start = *offsets
        .get(node_index)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q germ references an unknown node".into()))?;
    let end = *offsets
        .get(node_index + 1)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q germ references an incomplete fan".into()))?;
    if start == end {
        return Err(GeometryErrorKind::Invalid("Q germ reached an isolated node".into()).into());
    }
    let mut low = start;
    let mut high = end;
    while low < high {
        let middle = low + (high - low) / 2;
        match compare_q_dart_to_germ(ports, nodes, darts, outgoing[middle], germ)? {
            Ordering::Less => low = middle + 1,
            Ordering::Equal => {
                return Err(GeometryErrorKind::Invalid(
                    "Q face germ overlaps an exact source tangent".into(),
                )
                .into());
            },
            Ordering::Greater => high = middle,
        }
    }
    let preceding = if low == start { end - 1 } else { low - 1 };
    let dart = outgoing[preceding];
    dart_faces
        .get(dart.0 as usize)
        .copied()
        .ok_or_else(|| GeometryErrorKind::Invalid("Q germ found no incident face".into()).into())
}

fn compare_pole_dart_to_longitude(
    chart: QFaceChart<'_>,
    darts: &[QDart],
    dart: QDartId,
) -> Result<Ordering> {
    let dart = darts
        .get(dart.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("pole fan references an unknown dart".into()))?;
    let node = chart
        .nodes
        .get(dart.from_node.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("pole fan references an unknown node".into()))?;
    let pole = node.pole.ok_or_else(|| {
        GeometryErrorKind::Invalid("pole fan query reached an ordinary node".into())
    })?;
    let origin = chart
        .ports
        .get(dart.from_port.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("pole dart has no origin port".into()))?;
    let target = chart
        .ports
        .get(dart.to_port.0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("pole dart has no target port".into()))?;
    let longitude = if origin.point.x.try_cmp(chart.cut_east)?.is_eq() {
        chart.cut.clone()
    } else {
        origin.point.x.clone()
    };
    let order = compare_pole_meridians(pole, &longitude, chart.cut)?;
    if !order.is_eq() {
        return Ok(order);
    }
    let cut_germ = (ExactRational::zero(), ExactRational::from_f64(-1.0));
    compare_pole_tangent_rays(
        pole,
        &pole_tangent_ray(&origin.point, &target.point)?,
        &cut_germ,
    )
}

fn compare_pole_meridians(
    pole: Pole,
    left: &ExactRational,
    right: &ExactRational,
) -> Result<Ordering> {
    let order = left.try_cmp(right)?;
    Ok(if pole == Pole::South {
        order.reverse()
    } else {
        order
    })
}

/// The south-pole quotient reverses the complete circular order, including
/// the tangent tie within one meridian.
fn compare_pole_tangent_rays(
    pole: Pole,
    left: &(ExactRational, ExactRational),
    right: &(ExactRational, ExactRational),
) -> Result<Ordering> {
    let order = compare_exact_rays_allow_equal(left, right)?;
    Ok(if pole == Pole::South {
        order.reverse()
    } else {
        order
    })
}

#[expect(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "the Q outgoing range is the admitted CSR fan for this dense pole node ID"
)]
fn face_at_pole_longitude(
    chart: QFaceChart<'_>,
    darts: &[QDart],
    outgoing: &[QDartId],
    offsets: &[usize],
    dart_faces: &[QLocalFaceId],
    node: QNodeId,
) -> Result<QLocalFaceId> {
    let node_index = node.0 as usize;
    let start = *offsets.get(node_index).ok_or_else(|| {
        GeometryErrorKind::Invalid("pole query references an unknown node".into())
    })?;
    let end = *offsets.get(node_index + 1).ok_or_else(|| {
        GeometryErrorKind::Invalid("pole query references an incomplete fan".into())
    })?;
    if start == end {
        return Err(
            GeometryErrorKind::Invalid("pole query reached an isolated pole".into()).into(),
        );
    }
    let mut low = start;
    let mut high = end;
    while low < high {
        let middle = low + (high - low) / 2;
        match compare_pole_dart_to_longitude(chart, darts, outgoing[middle])? {
            Ordering::Less => low = middle + 1,
            Ordering::Equal => {
                return Err(GeometryErrorKind::Invalid(
                    "the selected cut overlaps a pole tangent germ".into(),
                )
                .into());
            },
            Ordering::Greater => high = middle,
        }
    }
    let preceding = if low == start { end - 1 } else { low - 1 };
    let dart = outgoing[preceding];
    dart_faces.get(dart.0 as usize).copied().ok_or_else(|| {
        GeometryErrorKind::Invalid("pole query found no incident face".into()).into()
    })
}

#[expect(
    clippy::indexing_slicing,
    reason = "component IDs are dense ordinals used to index the parallel Euler counts"
)]
fn validate_spherical_components(
    component_count: usize,
    spans: &[ChartSpan],
    face_spans: &[bool],
    node_components: &[QComponentId],
    degrees: &[usize],
    faces: &[QLocalFace],
) -> Result<()> {
    let mut vertices = vec![0_usize; component_count];
    let mut edges = vec![0_usize; component_count];
    let mut face_counts = vec![0_usize; component_count];
    for (node, &degree) in degrees.iter().enumerate() {
        if degree != 0 {
            let component = node_components[node].0 as usize;
            vertices[component] = vertices[component]
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
        }
    }
    for (span, &is_face_span) in spans.iter().zip(face_spans) {
        if is_face_span {
            let component = node_components[span.nodes[0].0 as usize].0 as usize;
            edges[component] = edges[component]
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
        }
    }
    for face in faces {
        let component = face.component.0 as usize;
        face_counts[component] = face_counts[component]
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?;
    }
    for component in 0..component_count {
        if edges[component] == 0 {
            continue;
        }
        let euler = vertices[component]
            .checked_add(face_counts[component])
            .and_then(|count| count.checked_sub(edges[component]));
        if euler != Some(2) {
            return Err(GeometryErrorKind::Invalid(
                "an exact geographic quotient component is not spherical".into(),
            )
            .into());
        }
    }
    Ok(())
}

/// One exact chart quotient graph. Local faces are sewn globally after the
/// rotation system is complete.
#[derive(Debug)]
pub(super) struct ExactGeographicQ {
    pub cut: ExactRational,
    pub ports: Box<[QPort]>,
    pub nodes: Box<[QNode]>,
    pub node_ports: Box<[QPortId]>,
    pub spans: Box<[ChartSpan]>,
    pub darts: Box<[QDart]>,
    pub span_darts: Box<[Option<QDartId>]>,
    pub outgoing: Box<[QDartId]>,
    pub outgoing_offsets: Box<[usize]>,
    pub dart_positions: Box<[usize]>,
    pub dart_faces: Box<[QLocalFaceId]>,
    pub local_faces: Box<[QLocalFace]>,
    pub node_components: Box<[QComponentId]>,
    pub span_local_faces: Box<[Option<[QLocalFaceId; 2]>]>,
    pub source_edge_ports: Box<[[QPortId; 2]]>,
    pub source_point_ports: Box<[QPortId]>,
    pub cut_index: QCutIndex,
    pub component_hosts: Box<[Option<GlobalFaceId>]>,
    pub global_face_of_local: Box<[GlobalFaceId]>,
    pub span_global_faces: Box<[Option<[GlobalFaceId; 2]>]>,
    pub cut_interval_faces: Box<[GlobalFaceId]>,
    pub source_node_faces: Box<[Option<GlobalFaceId>]>,
    pub source_point_faces: Box<[Option<GlobalFaceId>]>,
    pub global_face_count: usize,
    pub face_rows: GlobalFaceRowOccupancy,
    seam_cut_edge_pairs: Option<SeamCutEdgePairs>,
    span_line_rows: GlobalSpanRowSupport,
    span_collapsed_rows: GlobalSpanRowSupport,
    node_point_rows: GlobalNodeRowSupport,
    node_line_boundary_rows: GlobalNodeRowSupport,
    spatial_index: QSpatialIndex,
    #[cfg(test)]
    host_status_comparisons: usize,
    #[cfg(test)]
    host_status_max_height: usize,
    #[cfg(test)]
    seed_face: Option<GlobalFaceId>,
    #[cfg(test)]
    seed_winding: Box<[i64]>,
}

impl ExactGeographicQ {
    pub(super) fn build(
        frame: GeographicFrame,
        sources: &ExactGeographicChartInventory,
        chart: &NodedExactChart,
        capture_seam_cut_edge_pairs: bool,
    ) -> Result<Self> {
        if sources.frame != frame {
            return Err(GeometryErrorKind::Invalid(
                "exact geographic Q frame differs from its source inventory".into(),
            )
            .into());
        }
        let cut = select_cut(frame, chart)?;
        let cut_chart = make_cut_chart(frame, sources, chart, &cut)?;
        let topology = QuotientFaces::build(frame, cut_chart)?;
        let global = build_global_face_topology(frame, sources, &topology)?;
        let (face_rows, span_collapsed_rows, node_point_rows, seam_cut_edge_pairs) =
            GlobalFaceRowOccupancy::build(
                global.count,
                sources,
                &topology,
                &global,
                capture_seam_cut_edge_pairs,
            )?;
        let span_line_rows = build_span_line_rows(sources, &topology.spans)?;
        let node_line_boundary_rows = build_node_line_boundary_rows(sources, &topology)?;
        let spatial_index = QSpatialIndex::build(
            &topology.ports,
            &topology.nodes,
            &topology.spans,
            &node_point_rows,
        )?;
        Ok(Self {
            cut,
            ports: topology.ports.into_boxed_slice(),
            nodes: topology.nodes.into_boxed_slice(),
            node_ports: topology.node_ports.into_boxed_slice(),
            spans: topology.spans.into_boxed_slice(),
            darts: topology.darts.into_boxed_slice(),
            span_darts: topology.span_darts.into_boxed_slice(),
            outgoing: topology.outgoing.into_boxed_slice(),
            outgoing_offsets: topology.outgoing_offsets.into_boxed_slice(),
            dart_positions: topology.dart_positions.into_boxed_slice(),
            dart_faces: topology.dart_faces.into_boxed_slice(),
            local_faces: topology.local_faces.into_boxed_slice(),
            node_components: topology.node_components.into_boxed_slice(),
            span_local_faces: topology.span_local_faces.into_boxed_slice(),
            source_edge_ports: topology.source_edge_ports.into_boxed_slice(),
            source_point_ports: topology.source_point_ports.into_boxed_slice(),
            cut_index: topology.cut_index,
            component_hosts: global.component_hosts.into_boxed_slice(),
            global_face_of_local: global.local_to_global.into_boxed_slice(),
            span_global_faces: global.span_global_faces.into_boxed_slice(),
            cut_interval_faces: global.cut_interval_faces.into_boxed_slice(),
            source_node_faces: global.source_node_faces.into_boxed_slice(),
            source_point_faces: global.source_point_faces.into_boxed_slice(),
            global_face_count: global.count,
            face_rows,
            seam_cut_edge_pairs,
            span_line_rows,
            span_collapsed_rows,
            node_point_rows,
            node_line_boundary_rows,
            spatial_index,
            #[cfg(test)]
            host_status_comparisons: global.host_status_comparisons,
            #[cfg(test)]
            host_status_max_height: global.host_status_max_height,
            #[cfg(test)]
            seed_face: global.seed_face,
            #[cfg(test)]
            seed_winding: global.seed_winding.into_boxed_slice(),
        })
    }

    pub(super) fn line_rows_for_span(&self, span_index: usize) -> Result<&[SourceRowId]> {
        self.span_line_rows.row(span_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q line support rows are incomplete".into()).into()
        })
    }

    pub(super) fn collapsed_rows_for_span(&self, span_index: usize) -> Result<&[SourceRowId]> {
        self.span_collapsed_rows.row(span_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q collapsed support rows are incomplete".into()).into()
        })
    }

    pub(super) fn point_rows_for_node(&self, node_index: usize) -> Result<&[SourceRowId]> {
        self.node_point_rows.row(node_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q point support rows are incomplete".into()).into()
        })
    }

    pub(super) fn span_is_collapsed_pole(&self, span_index: usize) -> Result<bool> {
        let span = self
            .spans
            .get(span_index)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q span index is out of range".into()))?;
        if span.nodes[0] != span.nodes[1] {
            return Ok(false);
        }
        let node = self
            .nodes
            .get(span.nodes[0].0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q span has an unknown node".into()))?;
        Ok(node.pole.is_some())
    }

    fn locate_point(
        &self,
        topology: &ExactGeographicTopology,
        source_point: &ExactChartPoint,
        row: SourceRowId,
    ) -> Result<ExactGeographicPointLocation> {
        let point = lift_query_point(topology.sources.frame, &self.cut, source_point)?;
        let frame = topology.sources.frame;
        if point
            .y
            .try_cmp(&ExactRational::from_f64(frame.pole()))?
            .is_eq()
        {
            if let Some(node) = self
                .nodes
                .iter()
                .find(|node| node.pole == Some(Pole::North))
            {
                return locate_q_node(self, node.id, row);
            }
            let interval = self.cut_index.pole_interval_id(Pole::North)?;
            let face = *self
                .cut_interval_faces
                .get(interval.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q north-pole interval has no global face".into())
                })?;
            return Ok(location_in_global_face(self, face, row));
        }
        if point
            .y
            .try_cmp(&ExactRational::from_f64(-frame.pole()))?
            .is_eq()
        {
            if let Some(node) = self
                .nodes
                .iter()
                .find(|node| node.pole == Some(Pole::South))
            {
                return locate_q_node(self, node.id, row);
            }
            let interval = self.cut_index.pole_interval_id(Pole::South)?;
            let face = *self
                .cut_interval_faces
                .get(interval.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q south-pole interval has no global face".into())
                })?;
            return Ok(location_in_global_face(self, face, row));
        }

        let cut_east = self
            .cut
            .try_add(&ExactRational::from_f64(frame.longitude().period()))?;
        let mut matched_node = q_port_node(&self.ports, &point)?;
        if matched_node.is_none() && point.x.try_cmp(&self.cut)?.is_eq() {
            let east_alias = ExactChartPoint::from_rationals(cut_east.clone(), point.y.clone());
            matched_node = q_port_node(&self.ports, &east_alias)?;
        } else if matched_node.is_none() && point.x.try_cmp(&cut_east)?.is_eq() {
            let west_alias = ExactChartPoint::from_rationals(self.cut.clone(), point.y.clone());
            matched_node = q_port_node(&self.ports, &west_alias)?;
        }
        if let Some(node) = matched_node {
            return locate_q_node(self, node, row);
        }
        if point.x.try_cmp(&self.cut)?.is_eq() || point.x.try_cmp(&cut_east)?.is_eq() {
            return self.location_at_cut(&point.y, row);
        }

        let query = outward_segment_proxy(&point, &point)?;
        let mut boundary_span = None;
        let mut boundary_node = None;
        let mut failure = None;
        let candidates = self
            .spatial_index
            .for_each_candidate(query, |item| match item {
                QSpatialItem::Span(span_index) => {
                    let Some(span) = self.spans.get(span_index) else {
                        failure = Some(
                            GeometryErrorKind::Invalid("Q point index has an unknown span".into())
                                .into(),
                        );
                        return std::ops::ControlFlow::Break(());
                    };
                    let Some(start) = self.ports.get(span.ports[0].0 as usize) else {
                        failure = Some(
                            GeometryErrorKind::Invalid("Q point span has no start port".into())
                                .into(),
                        );
                        return std::ops::ControlFlow::Break(());
                    };
                    let Some(end) = self.ports.get(span.ports[1].0 as usize) else {
                        failure = Some(
                            GeometryErrorKind::Invalid("Q point span has no end port".into())
                                .into(),
                        );
                        return std::ops::ControlFlow::Break(());
                    };
                    match point_on_exact_segment(&point, &start.point, &end.point) {
                        Ok(true) => {
                            if boundary_span.is_some_and(|previous| previous != span_index) {
                                failure = Some(
                                    GeometryErrorKind::Invalid(
                                        "an exact Q query meets un-noded spans".into(),
                                    )
                                    .into(),
                                );
                                std::ops::ControlFlow::Break(())
                            } else {
                                boundary_span = Some(span_index);
                                std::ops::ControlFlow::Break(())
                            }
                        },
                        Ok(false) => std::ops::ControlFlow::Continue(()),
                        Err(error) => {
                            failure = Some(error);
                            std::ops::ControlFlow::Break(())
                        },
                    }
                },
                QSpatialItem::Point(port_id) => {
                    let Some(port) = self.ports.get(port_id.0 as usize) else {
                        failure = Some(
                            GeometryErrorKind::Invalid("Q point index has an unknown port".into())
                                .into(),
                        );
                        return std::ops::ControlFlow::Break(());
                    };
                    match port.point.same_position(&point) {
                        Ok(true) => {
                            boundary_node = Some(port.node);
                            std::ops::ControlFlow::Break(())
                        },
                        Ok(false) => std::ops::ControlFlow::Continue(()),
                        Err(error) => {
                            failure = Some(error);
                            std::ops::ControlFlow::Break(())
                        },
                    }
                },
            })?;
        if let Some(error) = failure {
            return Err(error);
        }
        if let Some(node) = boundary_node {
            return locate_q_node(self, node, row);
        }
        if let Some(span) = boundary_span {
            return Ok(locate_q_span(self, span, row));
        }
        if candidates.is_break() {
            return Err(GeometryErrorKind::Invalid(
                "Q point index stopped without an exact contact".into(),
            )
            .into());
        }
        self.location_from_cut_ray(&point, row)
    }

    fn location_at_cut(
        &self,
        latitude: &ExactRational,
        row: SourceRowId,
    ) -> Result<ExactGeographicPointLocation> {
        let interval = self.cut_index.interval_at(latitude, 1)?;
        let face = *self
            .cut_interval_faces
            .get(interval.id.0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q cut interval has no global face".into())
            })?;
        Ok(location_in_global_face(self, face, row))
    }

    fn location_from_cut_ray(
        &self,
        point: &ExactChartPoint,
        row: SourceRowId,
    ) -> Result<ExactGeographicPointLocation> {
        let interval = self.cut_index.interval_at(&point.y, 1)?;
        let face = *self
            .cut_interval_faces
            .get(interval.id.0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q cut interval has no global face".into())
            })?;
        let mut area_inside = q_face_has_row(self, face, row);
        let cut_point = ExactChartPoint::from_rationals(self.cut.clone(), point.y.clone());
        let query = outward_segment_proxy(&cut_point, point)?;
        let mut failure = None;
        let candidates = self.spatial_index.for_each_candidate(query, |item| {
            let QSpatialItem::Span(span_index) = item else {
                return std::ops::ControlFlow::Continue(());
            };
            match span_crosses_north_ray(self, span_index, point, row) {
                Ok(crosses) => {
                    if crosses {
                        area_inside = !area_inside;
                    }
                    std::ops::ControlFlow::Continue(())
                },
                Err(error) => {
                    failure = Some(error);
                    std::ops::ControlFlow::Break(())
                },
            }
        })?;
        if let Some(error) = failure {
            return Err(error);
        }
        if candidates.is_break() {
            return Err(GeometryErrorKind::Invalid(
                "Q point ray stopped without an exact query result".into(),
            )
            .into());
        }
        Ok(location_in_area_face(area_inside))
    }

    fn classify_rectangle(
        &self,
        topology: &ExactGeographicTopology,
        bounds: Bounds,
        row: SourceRowId,
    ) -> Result<Option<ExactGeographicRectangleLocation>> {
        let Some(windows) = ExactQRectangleWindows::new(topology.sources.frame, &self.cut, bounds)?
        else {
            return Ok(None);
        };
        let corners = [
            Point::new_raw_xy(bounds.minx(), bounds.miny()),
            Point::new_raw_xy(bounds.maxx(), bounds.miny()),
            Point::new_raw_xy(bounds.maxx(), bounds.maxy()),
            Point::new_raw_xy(bounds.minx(), bounds.maxy()),
        ];
        let mut corners_covered = true;
        let mut intersects = false;
        for corner in corners {
            let location = topology.locate_point(corner)?;
            corners_covered &= location.area != Loc::Exterior;
            intersects |= location.geometry != Loc::Exterior;
        }
        let center = Point::new_raw_xy(
            f64::midpoint(bounds.minx(), bounds.maxx()),
            f64::midpoint(bounds.miny(), bounds.maxy()),
        );
        let center_location = topology.locate_point(center)?;
        let covered = corners_covered && center_location.area == Loc::Interior;
        intersects |= center_location.geometry != Loc::Exterior;
        if intersects && !covered {
            return Ok(Some(ExactGeographicRectangleLocation::Boundary));
        }

        let mut area_boundary_enters = false;
        let mut support_intersects = false;
        for rectangle in windows.iter() {
            let query = rectangle.outward_query_segment()?;
            let mut failure: Option<crate::error::Error> = None;
            let candidates = self.spatial_index.for_each_candidate(query, |item| {
                match item {
                    QSpatialItem::Span(span_index) => {
                        let Some(span) = self.spans.get(span_index) else {
                            failure = Some(
                                GeometryErrorKind::Invalid(
                                    "Q rectangle index has an unknown span".into(),
                                )
                                .into(),
                            );
                            return std::ops::ControlFlow::Break(());
                        };
                        let Some(start) = self.ports.get(span.ports[0].0 as usize) else {
                            failure = Some(
                                GeometryErrorKind::Invalid(
                                    "Q rectangle span has no start port".into(),
                                )
                                .into(),
                            );
                            return std::ops::ControlFlow::Break(());
                        };
                        let Some(end) = self.ports.get(span.ports[1].0 as usize) else {
                            failure = Some(
                                GeometryErrorKind::Invalid(
                                    "Q rectangle span has no end port".into(),
                                )
                                .into(),
                            );
                            return std::ops::ControlFlow::Break(());
                        };
                        let faces = self.span_global_faces.get(span_index).copied().flatten();
                        let (area_left, area_right) =
                            faces.map_or((false, false), |[left, right]| {
                                (
                                    q_face_has_row(self, left, row),
                                    q_face_has_row(self, right, row),
                                )
                            });
                        let line = self.span_line_rows.contains(span_index, row);
                        let collapsed = self.span_collapsed_rows.contains(span_index, row);
                        let support = area_left || area_right || line || collapsed;
                        match rectangle.segment_intersects_closed(&start.point, &end.point) {
                            Ok(true) if support => support_intersects = true,
                            Ok(_) => {},
                            Err(error) => {
                                failure = Some(error);
                                return std::ops::ControlFlow::Break(());
                            },
                        }
                        if area_left != area_right {
                            match rectangle.segment_intersects_open(&start.point, &end.point) {
                                Ok(true) => area_boundary_enters = true,
                                Ok(false) => {},
                                Err(error) => {
                                    failure = Some(error);
                                    return std::ops::ControlFlow::Break(());
                                },
                            }
                        }
                    },
                    QSpatialItem::Point(port_id) => {
                        let Some(port) = self.ports.get(port_id.0 as usize) else {
                            failure = Some(
                                GeometryErrorKind::Invalid(
                                    "Q rectangle index has an unknown point port".into(),
                                )
                                .into(),
                            );
                            return std::ops::ControlFlow::Break(());
                        };
                        let node_index = port.node.0 as usize;
                        if self.node_point_rows.contains(node_index, row) {
                            match rectangle.contains_closed(&port.point) {
                                Ok(true) => support_intersects = true,
                                Ok(false) => {},
                                Err(error) => {
                                    failure = Some(error);
                                    return std::ops::ControlFlow::Break(());
                                },
                            }
                        }
                    },
                }
                if (covered && area_boundary_enters) || (!covered && support_intersects) {
                    std::ops::ControlFlow::Break(())
                } else {
                    std::ops::ControlFlow::Continue(())
                }
            })?;
            if let Some(error) = failure {
                return Err(error);
            }
            if candidates.is_break() {
                break;
            }
        }
        if covered && !area_boundary_enters {
            Ok(Some(ExactGeographicRectangleLocation::Interior))
        } else if intersects || support_intersects || area_boundary_enters {
            Ok(Some(ExactGeographicRectangleLocation::Boundary))
        } else {
            Ok(Some(ExactGeographicRectangleLocation::Outside))
        }
    }
}

impl HeapSize for ExactGeographicQ {
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "HeapSize sums the already-owned Q buffers as an estimate"
    )]
    fn heap_bytes(&self) -> usize {
        self.ports.len() * std::mem::size_of::<QPort>()
            + self
                .ports
                .iter()
                .map(|port| point_heap_bytes(&port.point))
                .sum::<usize>()
            + self.nodes.len() * std::mem::size_of::<QNode>()
            + self
                .nodes
                .iter()
                .map(|node| match &node.key {
                    QNodeKey::Source(_) => 0,
                    QNodeKey::Cut(latitude) => rational_heap_bytes(latitude),
                })
                .sum::<usize>()
            + self.node_ports.len() * std::mem::size_of::<QPortId>()
            + self.spans.len() * std::mem::size_of::<ChartSpan>()
            + self
                .spans
                .iter()
                .map(|span| {
                    span.owners.len() * std::mem::size_of::<QSpanOwner>()
                        + span
                            .owners
                            .iter()
                            .map(|owner| source_range_heap_bytes(&owner.source_range))
                            .sum::<usize>()
                })
                .sum::<usize>()
            + self.darts.len() * std::mem::size_of::<QDart>()
            + self.span_darts.len() * std::mem::size_of::<Option<QDartId>>()
            + self.outgoing.len() * std::mem::size_of::<QDartId>()
            + self.outgoing_offsets.len() * std::mem::size_of::<usize>()
            + self.dart_positions.len() * std::mem::size_of::<usize>()
            + self.dart_faces.len() * std::mem::size_of::<QLocalFaceId>()
            + self.local_faces.len() * std::mem::size_of::<QLocalFace>()
            + self.node_components.len() * std::mem::size_of::<QComponentId>()
            + self.span_local_faces.len() * std::mem::size_of::<Option<[QLocalFaceId; 2]>>()
            + self.source_edge_ports.len() * std::mem::size_of::<[QPortId; 2]>()
            + self.source_point_ports.len() * std::mem::size_of::<QPortId>()
            + self.cut_index.heap_bytes()
            + self.component_hosts.len() * std::mem::size_of::<Option<GlobalFaceId>>()
            + self.global_face_of_local.len() * std::mem::size_of::<GlobalFaceId>()
            + self.span_global_faces.len() * std::mem::size_of::<Option<[GlobalFaceId; 2]>>()
            + self.cut_interval_faces.len() * std::mem::size_of::<GlobalFaceId>()
            + self.source_node_faces.len() * std::mem::size_of::<Option<GlobalFaceId>>()
            + self.source_point_faces.len() * std::mem::size_of::<Option<GlobalFaceId>>()
            + self.face_rows.heap_bytes()
            + self.seam_cut_edge_pairs.as_ref().map_or(0, |pairs| {
                pairs.pairs.len() * std::mem::size_of::<SeamCutEdgePair>()
                    + pairs.touch_filters.len() * std::mem::size_of::<SeamTouchFilter>()
            })
            + self.span_line_rows.heap_bytes()
            + self.span_collapsed_rows.heap_bytes()
            + self.node_point_rows.heap_bytes()
            + self.node_line_boundary_rows.heap_bytes()
            + self.spatial_index.heap_bytes()
            + {
                #[cfg(test)]
                {
                    self.seed_winding.len() * std::mem::size_of::<i64>()
                }
                #[cfg(not(test))]
                {
                    0
                }
            }
    }
}

/// One admitted source inventory and the exact chart/Q derived from it.
#[derive(Debug)]
pub(crate) struct ExactGeographicTopology {
    pub(super) sources: ExactGeographicChartInventory,
    pub(super) chart: NodedExactChart,
    pub(super) q: ExactGeographicQ,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ExactGeographicPointLocation {
    /// Location in the complete geometry, including line and point support.
    pub(crate) geometry: Loc,
    /// Location in the filled polygonal support alone.
    pub(crate) area: Loc,
}

/// Spatial relation between a source and a supported, nondegenerate rectangle.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ExactGeographicRectangleLocation {
    Outside,
    Boundary,
    Interior,
}

impl ExactGeographicTopology {
    pub(crate) fn from_views<'a>(
        frame: GeographicFrame,
        views: impl ExactSizeIterator<Item = crate::geographic::ScalarGeographicView<'a>>,
    ) -> Result<Self> {
        Self::build_from_views(frame, views, false)
    }

    pub(crate) fn from_views_with_seam_cut_edge_pairs<'a>(
        frame: GeographicFrame,
        views: impl ExactSizeIterator<Item = crate::geographic::ScalarGeographicView<'a>>,
    ) -> Result<Self> {
        Self::build_from_views(frame, views, true)
    }

    fn build_from_views<'a>(
        frame: GeographicFrame,
        views: impl ExactSizeIterator<Item = crate::geographic::ScalarGeographicView<'a>>,
        capture_seam_cut_edge_pairs: bool,
    ) -> Result<Self> {
        let sources = ExactGeographicChartInventory::collect_views(frame, views)?;
        let mut budget =
            crate::geometry::ExpansionBudget::new("geographic topology", "exact chart noding");
        let chart = sources.node(&mut budget)?;
        let q = ExactGeographicQ::build(frame, &sources, &chart, capture_seam_cut_edge_pairs)?;
        Ok(Self { sources, chart, q })
    }

    pub(crate) const fn seam_cut_edge_pairs(&self) -> Option<&SeamCutEdgePairs> {
        self.q.seam_cut_edge_pairs.as_ref()
    }

    pub(crate) fn relate_pair(&self) -> Result<De9im> {
        exact_pair_relation(self)
    }

    pub(crate) fn locate_point(&self, point: Point) -> Result<ExactGeographicPointLocation> {
        if self.sources.row_count != 1 {
            return Err(GeometryErrorKind::Invalid(
                "an exact geographic point query requires one source row".into(),
            )
            .into());
        }
        let frame = self.sources.frame;
        frame
            .longitude()
            .canonicalize(point.x)
            .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
        let latitude = crate::geographic::Latitude::try_new(frame, point.y)
            .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
        let point = Point::new_raw_xy(point.x, latitude.get());
        let point = super::exact::source_chart_point(frame, point);
        self.q.locate_point(self, &point, SourceRowId(0))
    }

    pub(crate) fn classify_rectangle(
        &self,
        bounds: Bounds,
    ) -> Result<Option<ExactGeographicRectangleLocation>> {
        if self.sources.row_count != 1 {
            return Err(GeometryErrorKind::Invalid(
                "an exact geographic rectangle query requires one source row".into(),
            )
            .into());
        }
        self.q.classify_rectangle(self, bounds, SourceRowId(0))
    }

    /// Whether distinct source boundary edges meet at the seam after Q
    /// identifies its chart copies. One edge can own spans on both ports, so
    /// the contact test uses source-edge identity; callers then use Q's face
    /// occupancy to distinguish chart cuts from ordinary boundary contacts.
    pub(crate) fn has_source_contact_at_seam(&self) -> Result<bool> {
        if self.sources.row_count != 1 {
            return Err(GeometryErrorKind::Invalid(
                "seam validity contacts require one source row".into(),
            )
            .into());
        }

        let seam_nodes = self
            .q
            .nodes
            .iter()
            .map(|node| {
                let QNodeKey::Source(source_node) = &node.key else {
                    return false;
                };
                self.chart
                    .nodes
                    .get(source_node.0 as usize)
                    .is_some_and(|node| matches!(&node.key, ExactQuotientKey::Seam(_)))
            })
            .collect::<Vec<_>>();
        let mut line_edges = Vec::new();
        let mut ring_edges = Vec::new();
        for span in &self.q.spans {
            for node in span.nodes {
                let index = node.0 as usize;
                if !seam_nodes.get(index).copied().ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "exact Q seam contact references an unknown node".into(),
                    )
                })? {
                    continue;
                }
                for owner in &span.owners {
                    let edge = self
                        .sources
                        .edges
                        .get(owner.source_edge.0 as usize)
                        .ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "exact Q seam contact references an unknown source edge".into(),
                            )
                        })?;
                    match edge.kind {
                        SourceEdgeKind::Line => line_edges.push((node, owner.source_edge)),
                        SourceEdgeKind::Ring { polygon, ring, .. } => {
                            ring_edges.push((node, polygon, ring, owner.source_edge));
                        },
                    }
                }
            }
        }
        line_edges.sort_unstable();
        line_edges.dedup();
        for pair in line_edges.windows(2) {
            let [left, right] = pair else {
                unreachable!("windows(2) contains two source contacts");
            };
            if left.0 == right.0 && left.1 != right.1 {
                return Ok(true);
            }
        }

        ring_edges.sort_unstable();
        ring_edges.dedup();
        let mut start = 0;
        while start < ring_edges.len() {
            let Some((node, ..)) = ring_edges.get(start).copied() else {
                return Err(GeometryErrorKind::Invalid(
                    "exact Q seam contact group starts outside its source list".into(),
                )
                .into());
            };
            let mut end = start.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
            while end < ring_edges.len() && ring_edges.get(end).is_some_and(|c| c.0 == node) {
                end = end.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
            }
            let Some(group) = ring_edges.get(start..end) else {
                return Err(GeometryErrorKind::Invalid(
                    "exact Q seam contact group has an invalid range".into(),
                )
                .into());
            };
            let Some((_, polygon, ring, _)) = group.first().copied() else {
                return Err(GeometryErrorKind::Invalid(
                    "exact Q seam contact group is empty".into(),
                )
                .into());
            };
            if group
                .iter()
                .any(|contact| contact.1 != polygon || contact.2 != ring)
            {
                return Ok(true);
            }
            if group.len() == 1 {
                start = end;
                continue;
            }
            if group.len() > 2 {
                return Ok(true);
            }
            let [left_contact, right_contact] = group else {
                return Ok(true);
            };
            let source_ring = self.sources.rings.get(ring.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "exact Q seam contact references an unknown source ring".into(),
                )
            })?;
            let last_edge = source_ring.source_edges.end.saturating_sub(1);
            let left = left_contact.3.0 as usize;
            let right = right_contact.3.0 as usize;
            let adjacent = left.abs_diff(right) == 1
                || (left == source_ring.source_edges.start && right == last_edge)
                || (right == source_ring.source_edges.start && left == last_edge);
            if !adjacent {
                return Ok(true);
            }
            start = end;
        }
        Ok(false)
    }
}

fn lift_query_point(
    frame: GeographicFrame,
    cut: &ExactRational,
    source_point: &ExactChartPoint,
) -> Result<ExactChartPoint> {
    let period = ExactRational::from_f64(frame.longitude().period());
    let cut_east = cut.try_add(&period)?;
    let mut point = source_point.clone();
    if point.x.try_cmp(cut)?.is_lt() {
        point.x = point.x.try_add(&period)?;
    } else if point.x.try_cmp(&cut_east)?.is_gt() {
        point.x = point.x.try_subtract(&period)?;
    }
    if point.x.try_cmp(cut)?.is_lt() || point.x.try_cmp(&cut_east)?.is_gt() {
        return Err(GeometryErrorKind::Invalid(
            "exact geographic query point is outside the Q chart".into(),
        )
        .into());
    }
    Ok(point)
}

fn pair_row_slot(row: SourceRowId) -> Result<usize> {
    match row.0 {
        0 => Ok(0),
        1 => Ok(1),
        _ => Err(GeometryErrorKind::Invalid(
            "exact geographic relation received a source row outside its pair".into(),
        )
        .into()),
    }
}

fn pair_face_membership(q: &ExactGeographicQ, face: GlobalFaceId) -> [bool; 2] {
    [
        q.face_rows.contains(face, SourceRowId(0)),
        q.face_rows.contains(face, SourceRowId(1)),
    ]
}

fn q_face_has_row(q: &ExactGeographicQ, face: GlobalFaceId, row: SourceRowId) -> bool {
    q.face_rows.contains(face, row)
}

const fn area_location(any: bool, all: bool) -> Loc {
    if !any {
        Loc::Exterior
    } else if all {
        Loc::Interior
    } else {
        Loc::Boundary
    }
}

struct CurveSupport {
    has_curve: bool,
    line_boundary: bool,
    collapsed_degree: bool,
}

struct PointSupport {
    area: Loc,
    curve: CurveSupport,
    point: bool,
}

fn point_location_from_support(support: &PointSupport) -> ExactGeographicPointLocation {
    let geometry = if support.area != Loc::Exterior {
        support.area
    } else if support.curve.has_curve {
        if support.curve.line_boundary ^ support.curve.collapsed_degree {
            Loc::Boundary
        } else {
            Loc::Interior
        }
    } else if support.point {
        Loc::Interior
    } else {
        Loc::Exterior
    };
    ExactGeographicPointLocation {
        geometry,
        area: support.area,
    }
}

const fn location_in_area_face(inside: bool) -> ExactGeographicPointLocation {
    let location = if inside { Loc::Interior } else { Loc::Exterior };
    ExactGeographicPointLocation {
        geometry: location,
        area: location,
    }
}

fn location_in_global_face(
    q: &ExactGeographicQ,
    face: GlobalFaceId,
    row: SourceRowId,
) -> ExactGeographicPointLocation {
    location_in_area_face(q_face_has_row(q, face, row))
}

fn q_node_fan(q: &ExactGeographicQ, node_index: usize) -> Result<(usize, usize)> {
    let offsets = q
        .outgoing_offsets
        .get(node_index..)
        .and_then(|tail| tail.get(..2))
        .ok_or_else(|| GeometryErrorKind::Invalid("Q node has no outgoing fan".into()))?;
    let [start, end] = offsets else {
        return Err(GeometryErrorKind::Invalid("Q node has an invalid outgoing fan".into()).into());
    };
    Ok((*start, *end))
}

fn locate_q_node(
    q: &ExactGeographicQ,
    node: QNodeId,
    row: SourceRowId,
) -> Result<ExactGeographicPointLocation> {
    let node_index = node.0 as usize;
    let (start, end) = q_node_fan(q, node_index)?;
    let (area_any, area_all) = if start == end {
        let face = q
            .source_node_faces
            .get(node_index)
            .copied()
            .flatten()
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("an isolated Q node has no host face".into())
            })?;
        let inside = q_face_has_row(q, face, row);
        (inside, inside)
    } else {
        let fan = q.outgoing.get(start..end).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q node has an invalid outgoing fan".into())
        })?;
        let (mut area_any, mut area_all) = (false, true);
        for &dart_id in fan {
            let local = *q.dart_faces.get(dart_id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q node fan has an unknown dart face".into())
            })?;
            let global = *q
                .global_face_of_local
                .get(local.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q node dart face has no global identity".into())
                })?;
            let inside = q_face_has_row(q, global, row);
            area_any |= inside;
            area_all &= inside;
        }
        (area_any, area_all)
    };

    let mut curve = false;
    let mut collapsed_degree = false;
    if start != end {
        let fan = q.outgoing.get(start..end).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q node has an invalid outgoing fan".into())
        })?;
        for &dart_id in fan {
            let dart = q.darts.get(dart_id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q node fan has an unknown dart".into())
            })?;
            let span_index = dart.span.0 as usize;
            let span = q.spans.get(span_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q node fan has an unknown span".into())
            })?;
            if span.nodes[0] == span.nodes[1] {
                continue;
            }
            let line = q.span_line_rows.contains(span_index, row);
            let collapsed = q.span_collapsed_rows.contains(span_index, row);
            curve |= line || collapsed;
            if collapsed && !line {
                collapsed_degree = !collapsed_degree;
            }
        }
    }
    let line_boundary = q.node_line_boundary_rows.contains(node_index, row);
    let point = q.node_point_rows.contains(node_index, row);
    Ok(point_location_from_support(&PointSupport {
        area: area_location(area_any, area_all),
        curve: CurveSupport {
            has_curve: curve,
            line_boundary,
            collapsed_degree,
        },
        point,
    }))
}

fn locate_q_span(
    q: &ExactGeographicQ,
    span_index: usize,
    row: SourceRowId,
) -> ExactGeographicPointLocation {
    let Some([left, right]) = q.span_global_faces.get(span_index).copied().flatten() else {
        let curve = q.span_line_rows.contains(span_index, row)
            || q.span_collapsed_rows.contains(span_index, row);
        let location = if curve { Loc::Interior } else { Loc::Exterior };
        return ExactGeographicPointLocation {
            geometry: location,
            area: Loc::Exterior,
        };
    };
    let left_area = q_face_has_row(q, left, row);
    let right_area = q_face_has_row(q, right, row);
    let line = q.span_line_rows.contains(span_index, row);
    let collapsed = q.span_collapsed_rows.contains(span_index, row);
    point_location_from_support(&PointSupport {
        area: area_location(left_area || right_area, left_area && right_area),
        curve: CurveSupport {
            has_curve: line || collapsed,
            line_boundary: false,
            collapsed_degree: false,
        },
        point: false,
    })
}

fn span_crosses_north_ray(
    q: &ExactGeographicQ,
    span_index: usize,
    point: &ExactChartPoint,
    row: SourceRowId,
) -> Result<bool> {
    let Some([left, right]) = q.span_global_faces.get(span_index).copied().flatten() else {
        return Ok(false);
    };
    if q_face_has_row(q, left, row) == q_face_has_row(q, right, row) {
        return Ok(false);
    }
    let span = q
        .spans
        .get(span_index)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q ray references an unknown span".into()))?;
    let start = q
        .ports
        .get(span.ports[0].0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q ray has no start port".into()))?;
    let end = q
        .ports
        .get(span.ports[1].0 as usize)
        .ok_or_else(|| GeometryErrorKind::Invalid("Q ray has no end port".into()))?;
    let order = start.point.y.try_cmp(&end.point.y)?;
    if order.is_eq() {
        return Ok(false);
    }
    // The north-side germ gives vertices a half-open crossing rule; only the
    // parity of row occupancy changes, so crossings need no global ordering.
    let (low, high) = if order.is_lt() {
        (&start.point, &end.point)
    } else {
        (&end.point, &start.point)
    };
    if point.y.try_cmp(&low.y)?.is_lt() || !point.y.try_cmp(&high.y)?.is_lt() {
        return Ok(false);
    }
    let delta_y = high.y.try_subtract(&low.y)?;
    let slope = high.x.try_subtract(&low.x)?.try_quotient(&delta_y)?;
    let hit_x = low
        .x
        .try_add(&point.y.try_subtract(&low.y)?.try_product(&slope)?)?;
    Ok(hit_x.try_cmp(&q.cut)?.is_gt() && hit_x.try_cmp(&point.x)?.is_lt())
}

fn mark_pair_node_flag(
    flags: &mut [[bool; 2]],
    node: QNodeId,
    side: usize,
    toggle: bool,
) -> Result<()> {
    let flags = flags.get_mut(node.0 as usize).ok_or_else(|| {
        GeometryErrorKind::Invalid("Q node support references an unknown node".into())
    })?;
    let value = flags.get_mut(side).ok_or_else(|| {
        GeometryErrorKind::Invalid("Q node support has an unknown row side".into())
    })?;
    *value = if toggle { !*value } else { true };
    Ok(())
}

fn span_pair_support(q: &ExactGeographicQ, span_index: usize) -> ([bool; 2], [bool; 2]) {
    let collapsed = [
        q.span_collapsed_rows.contains(span_index, SourceRowId(0)),
        q.span_collapsed_rows.contains(span_index, SourceRowId(1)),
    ];
    let line = [
        q.span_line_rows.contains(span_index, SourceRowId(0)),
        q.span_line_rows.contains(span_index, SourceRowId(1)),
    ];
    (line, collapsed)
}

fn build_span_line_rows(
    sources: &ExactGeographicChartInventory,
    spans: &[ChartSpan],
) -> Result<GlobalSpanRowSupport> {
    let mut memberships = Vec::with_capacity(sources.edges.len());
    for (span_index, span) in spans.iter().enumerate() {
        for owner in &span.owners {
            let edge = sources
                .edges
                .get(owner.source_edge.0 as usize)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q span has an unknown source edge".into())
                })?;
            if matches!(edge.kind, SourceEdgeKind::Line) {
                memberships.push((span_index, edge.row));
            }
        }
    }
    GlobalSpanRowSupport::build(spans.len(), &mut memberships)
}

fn build_node_line_boundary_rows(
    sources: &ExactGeographicChartInventory,
    topology: &QuotientFaces,
) -> Result<GlobalNodeRowSupport> {
    let mut memberships = Vec::with_capacity(sources.line_boundaries.len());
    for boundary in &sources.line_boundaries {
        let endpoints = topology
            .source_edge_ports
            .get(boundary.edge.0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q line boundary has no source edge ports".into())
            })?;
        let port_id = if boundary.at_end {
            endpoints.last().copied()
        } else {
            endpoints.first().copied()
        }
        .ok_or_else(|| GeometryErrorKind::Invalid("Q line boundary has no chart port".into()))?;
        let node = topology
            .ports
            .get(port_id.0 as usize)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "Q line boundary references an unknown chart port".into(),
                )
            })?
            .node;
        memberships.push((node.0 as usize, boundary.row));
    }
    memberships.sort_unstable();
    let mut odd_memberships = Vec::with_capacity(memberships.len());
    for run in memberships.chunk_by(|left, right| left == right) {
        if !run.len().is_multiple_of(2)
            && let Some(&membership) = run.first()
        {
            odd_memberships.push(membership);
        }
    }
    GlobalNodeRowSupport::build(topology.nodes.len(), &mut odd_memberships)
}

fn q_node_location(
    node_curve_support: &[[bool; 2]],
    node_line_boundary: &[[bool; 2]],
    node_collapsed_degree: &[[bool; 2]],
    node_point_support: &[[bool; 2]],
    node_index: usize,
    side: usize,
    area: Loc,
) -> Result<Loc> {
    if area != Loc::Exterior {
        return Ok(area);
    }
    let curve = *node_curve_support
        .get(node_index)
        .and_then(|flags| flags.get(side))
        .ok_or_else(|| GeometryErrorKind::Invalid("Q node has no curve support side".into()))?;
    if curve {
        let line_boundary = *node_line_boundary
            .get(node_index)
            .and_then(|flags| flags.get(side))
            .ok_or_else(|| GeometryErrorKind::Invalid("Q node has no line boundary side".into()))?;
        let collapsed_degree = *node_collapsed_degree
            .get(node_index)
            .and_then(|flags| flags.get(side))
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q node has no collapsed-degree side".into())
            })?;
        return Ok(if line_boundary ^ collapsed_degree {
            Loc::Boundary
        } else {
            Loc::Interior
        });
    }
    let point = *node_point_support
        .get(node_index)
        .and_then(|flags| flags.get(side))
        .ok_or_else(|| GeometryErrorKind::Invalid("Q node has no point support side".into()))?;
    Ok(if point { Loc::Interior } else { Loc::Exterior })
}

fn exact_pair_relation(topology: &ExactGeographicTopology) -> Result<De9im> {
    if topology.sources.row_count != 2 {
        return Err(GeometryErrorKind::Invalid(
            "an exact geographic DE-9IM relation requires two source rows".into(),
        )
        .into());
    }
    let q = &topology.q;
    let mut matrix = De9im::empty_disjoint();

    for face in 0..q.global_face_count {
        let face = GlobalFaceId(u32::try_from(face).map_err(|_| ResourceError::SizeOverflow)?);
        let [left, right] = pair_face_membership(q, face);
        matrix.set_at_least(
            if left { Loc::Interior } else { Loc::Exterior },
            if right { Loc::Interior } else { Loc::Exterior },
            Dimension::Surface,
        );
    }

    let mut node_point_support = vec![[false; 2]; q.nodes.len()];
    let mut node_line_boundary = vec![[false; 2]; q.nodes.len()];
    let mut node_curve_support = vec![[false; 2]; q.nodes.len()];
    let mut node_collapsed_degree = vec![[false; 2]; q.nodes.len()];
    for node_index in 0..q.nodes.len() {
        let rows = q.node_point_rows.row(node_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q node point support rows are incomplete".into())
        })?;
        for &row in rows {
            let side = pair_row_slot(row)?;
            let flag = node_point_support
                .get_mut(node_index)
                .and_then(|flags| flags.get_mut(side))
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q node point support row is unknown".into())
                })?;
            *flag = true;
        }
    }
    for (node_index, flags) in node_line_boundary.iter_mut().enumerate() {
        let rows = q.node_line_boundary_rows.row(node_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q node line-boundary rows are incomplete".into())
        })?;
        for &row in rows {
            let side = pair_row_slot(row)?;
            *flags.get_mut(side).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q line boundary has an unknown pair row".into())
            })? = true;
        }
    }

    for (span_index, span) in q.spans.iter().enumerate() {
        if span.nodes[0] == span.nodes[1] {
            continue;
        }
        let (line_support, collapsed_support) = span_pair_support(q, span_index);
        let mut curve_support = line_support;
        for (curve, collapsed) in curve_support.iter_mut().zip(collapsed_support) {
            *curve |= collapsed;
        }

        for (side, ((line, collapsed), curve)) in line_support
            .into_iter()
            .zip(collapsed_support)
            .zip(curve_support)
            .enumerate()
        {
            if curve {
                mark_pair_node_flag(&mut node_curve_support, span.nodes[0], side, false)?;
                mark_pair_node_flag(&mut node_curve_support, span.nodes[1], side, false)?;
            }
            if collapsed && !line {
                mark_pair_node_flag(&mut node_collapsed_degree, span.nodes[0], side, true)?;
                mark_pair_node_flag(&mut node_collapsed_degree, span.nodes[1], side, true)?;
            }
        }

        let area_sides =
            if let Some([left, right]) = q.span_global_faces.get(span_index).copied().flatten() {
                [
                    pair_face_membership(q, left),
                    pair_face_membership(q, right),
                ]
            } else {
                [[false; 2]; 2]
            };
        let [left_areas, right_areas] = area_sides;
        let mut locations = [Loc::Exterior; 2];
        for (side, ((left_area, right_area), curve)) in left_areas
            .into_iter()
            .zip(right_areas)
            .zip(curve_support)
            .enumerate()
        {
            let location = if left_area && right_area {
                Loc::Interior
            } else if left_area || right_area {
                Loc::Boundary
            } else if curve {
                Loc::Interior
            } else {
                Loc::Exterior
            };
            *locations.get_mut(side).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q span has an unknown pair row".into())
            })? = location;
        }
        let [left_location, right_location] = locations;
        if left_location != Loc::Exterior || right_location != Loc::Exterior {
            matrix.set_at_least(left_location, right_location, Dimension::Curve);
        }
    }

    let node_fans = q.outgoing_offsets.windows(2);
    if node_fans.len() != q.nodes.len() {
        return Err(
            GeometryErrorKind::Invalid("Q node fans do not match the node table".into()).into(),
        );
    }
    for (node_index, offsets) in node_fans.enumerate() {
        let [start, end] = offsets else {
            return Err(
                GeometryErrorKind::Invalid("Q node has an invalid outgoing fan".into()).into(),
            );
        };
        let (start, end) = (*start, *end);
        let (area_any, area_all) = if start == end {
            let face = q
                .source_node_faces
                .get(node_index)
                .copied()
                .flatten()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid("an isolated Q node has no host face".into())
                })?;
            let membership = pair_face_membership(q, face);
            (membership, membership)
        } else {
            let fan = q.outgoing.get(start..end).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q node has an invalid outgoing fan".into())
            })?;
            let (mut area_any, mut area_all) = ([false; 2], [true; 2]);
            for &dart_id in fan {
                let local = *q.dart_faces.get(dart_id.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q node fan has an unknown dart face".into())
                })?;
                let global = *q
                    .global_face_of_local
                    .get(local.0 as usize)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q node dart face has no global identity".into())
                    })?;
                let membership = pair_face_membership(q, global);
                for ((any, all), member) in
                    area_any.iter_mut().zip(area_all.iter_mut()).zip(membership)
                {
                    *any |= member;
                    *all &= member;
                }
            }
            (area_any, area_all)
        };
        let mut locations = [Loc::Exterior; 2];
        for (side, (any, all)) in area_any.iter().copied().zip(area_all).enumerate() {
            *locations.get_mut(side).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q node has an unknown pair row".into())
            })? = q_node_location(
                &node_curve_support,
                &node_line_boundary,
                &node_collapsed_degree,
                &node_point_support,
                node_index,
                side,
                area_location(any, all),
            )?;
        }
        let [left_location, right_location] = locations;
        if left_location != Loc::Exterior || right_location != Loc::Exterior {
            matrix.set_at_least(left_location, right_location, Dimension::Point);
        }
    }
    Ok(matrix)
}

impl HeapSize for ExactGeographicTopology {
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "HeapSize sums the retained source inventory, exact chart, and global Q estimates"
    )]
    fn heap_bytes(&self) -> usize {
        self.sources.heap_bytes() + self.chart.heap_bytes() + self.q.heap_bytes()
    }
}

#[derive(Debug)]
struct QRowCsr<T> {
    offsets: Box<[usize]>,
    rows: Box<[T]>,
}

impl<T: Copy + Ord> QRowCsr<T> {
    fn build(row_count: usize, memberships: &mut Vec<(usize, T)>) -> Result<Self> {
        memberships.sort_unstable();
        memberships.dedup();
        let offset_count = row_count
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?;
        let mut offsets = vec![0_usize; offset_count];
        for &(row, _) in memberships.iter() {
            let offset = row.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
            let count = offsets.get_mut(offset).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q row CSR references an unknown item".into())
            })?;
            *count = count.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
        }
        let mut total = 0_usize;
        for offset in offsets.iter_mut().skip(1) {
            total = total
                .checked_add(*offset)
                .ok_or(ResourceError::SizeOverflow)?;
            *offset = total;
        }
        if total != memberships.len() {
            return Err(GeometryErrorKind::Invalid(
                "Q row CSR offsets do not match their membership count".into(),
            )
            .into());
        }
        let rows = memberships
            .iter()
            .map(|&(_, value)| value)
            .collect::<Vec<_>>();
        Ok(Self {
            offsets: offsets.into_boxed_slice(),
            rows: rows.into_boxed_slice(),
        })
    }

    fn row(&self, index: usize) -> Option<&[T]> {
        let next = index.checked_add(1)?;
        let start = *self.offsets.get(index)?;
        let end = *self.offsets.get(next)?;
        self.rows.get(start..end)
    }

    fn contains(&self, index: usize, value: T) -> bool {
        let Some(row) = self.row(index) else {
            return false;
        };
        row.binary_search(&value).is_ok()
    }
}

impl<T> QRowCsr<T> {
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "HeapSize sums the published CSR buffers as an estimate"
    )]
    fn heap_bytes(&self) -> usize {
        self.offsets.len() * std::mem::size_of::<usize>()
            + self.rows.len() * std::mem::size_of::<T>()
    }
}

type GlobalSpanRowSupport = QRowCsr<SourceRowId>;
type GlobalNodeRowSupport = QRowCsr<SourceRowId>;
type CollapsedPointFaces = QRowCsr<usize>;

#[derive(Clone, Copy, Debug)]
enum QSpatialItem {
    Span(usize),
    Point(QPortId),
}

/// Candidate-only R-tree: every indexed envelope is outward-rounded from Q
/// coordinates, and query hits are refined against their exact source.
struct QSpatialIndex {
    index: SegmentIndex,
    indexed_items: Box<[QSpatialItem]>,
    unindexed_items: Box<[QSpatialItem]>,
    #[cfg(test)]
    candidate_visits: std::sync::atomic::AtomicUsize,
}

impl QSpatialIndex {
    fn build(
        ports: &[QPort],
        nodes: &[QNode],
        spans: &[ChartSpan],
        node_point_rows: &GlobalNodeRowSupport,
    ) -> Result<Self> {
        let mut point_port_count = 0_usize;
        for (node_index, node) in nodes.iter().enumerate() {
            let point_rows = node_point_rows.row(node_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q node point rows are incomplete".into())
            })?;
            if !point_rows.is_empty() {
                point_port_count = point_port_count
                    .checked_add(node.ports.len())
                    .ok_or(ResourceError::SizeOverflow)?;
            }
        }
        let item_capacity = spans
            .len()
            .checked_add(point_port_count)
            .ok_or(ResourceError::SizeOverflow)?;
        let mut proxies = Vec::with_capacity(item_capacity);
        let mut indexed_items = Vec::with_capacity(item_capacity);
        let mut unindexed_items = Vec::new();
        for (span_index, span) in spans.iter().enumerate() {
            let start = ports
                .get(span.ports[0].0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q span has no start port".into()))?;
            let end = ports
                .get(span.ports[1].0 as usize)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q span has no end port".into()))?;
            let item = QSpatialItem::Span(span_index);
            if let Some(proxy) = outward_segment_proxy(&start.point, &end.point)? {
                proxies.push(proxy);
                indexed_items.push(item);
            } else {
                unindexed_items.push(item);
            }
        }
        for (node_index, node) in nodes.iter().enumerate() {
            let rows = node_point_rows.row(node_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q node point rows are incomplete".into())
            })?;
            if rows.is_empty() {
                continue;
            }
            let node_ports = ports.get(node.ports.clone()).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q point support has an invalid port range".into())
            })?;
            for port in node_ports {
                let item = QSpatialItem::Point(port.id);
                if let Some(proxy) = outward_segment_proxy(&port.point, &port.point)? {
                    proxies.push(proxy);
                    indexed_items.push(item);
                } else {
                    unindexed_items.push(item);
                }
            }
        }
        if proxies.len() != indexed_items.len() {
            return Err(
                GeometryErrorKind::Invalid("Q spatial index columns diverged".into()).into(),
            );
        }
        Ok(Self {
            index: SegmentIndex::build_from_iter(proxies),
            indexed_items: indexed_items.into_boxed_slice(),
            unindexed_items: unindexed_items.into_boxed_slice(),
            #[cfg(test)]
            candidate_visits: std::sync::atomic::AtomicUsize::new(0),
        })
    }

    fn for_each_candidate<B>(
        &self,
        query: Option<Segment>,
        mut visit: impl FnMut(QSpatialItem) -> std::ops::ControlFlow<B>,
    ) -> Result<std::ops::ControlFlow<B>> {
        if let Some(query) = query {
            for entry in self.index.intersecting_candidates(query) {
                let item = self
                    .indexed_items
                    .get(entry.ordinal)
                    .copied()
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q spatial index has an unknown item".into())
                    })?;
                #[cfg(test)]
                self.candidate_visits
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if let std::ops::ControlFlow::Break(value) = visit(item) {
                    return Ok(std::ops::ControlFlow::Break(value));
                }
            }
        } else {
            for &item in &self.indexed_items {
                #[cfg(test)]
                self.candidate_visits
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                if let std::ops::ControlFlow::Break(value) = visit(item) {
                    return Ok(std::ops::ControlFlow::Break(value));
                }
            }
        }
        for &item in &self.unindexed_items {
            #[cfg(test)]
            self.candidate_visits
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            if let std::ops::ControlFlow::Break(value) = visit(item) {
                return Ok(std::ops::ControlFlow::Break(value));
            }
        }
        Ok(std::ops::ControlFlow::Continue(()))
    }

    #[cfg(test)]
    fn candidate_visits(&self) -> usize {
        self.candidate_visits
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    #[cfg(test)]
    fn reset_candidate_visits(&self) {
        self.candidate_visits
            .store(0, std::sync::atomic::Ordering::Relaxed);
    }
}

impl HeapSize for QSpatialIndex {
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "HeapSize estimates the retained spatial-index and ID buffers"
    )]
    fn heap_bytes(&self) -> usize {
        self.index.heap_bytes()
            + self.indexed_items.len() * std::mem::size_of::<QSpatialItem>()
            + self.unindexed_items.len() * std::mem::size_of::<QSpatialItem>()
    }
}

impl std::fmt::Debug for QSpatialIndex {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QSpatialIndex")
            .field("indexed_items", &self.indexed_items.len())
            .field("unindexed_items", &self.unindexed_items.len())
            .finish_non_exhaustive()
    }
}

fn outward_segment_proxy(
    start: &ExactChartPoint,
    end: &ExactChartPoint,
) -> Result<Option<Segment>> {
    let [start_min_x, start_min_y, start_max_x, start_max_y] = start.outward_bounds()?;
    let [end_min_x, end_min_y, end_max_x, end_max_y] = end.outward_bounds()?;
    let bounds = [
        start_min_x.min(end_min_x),
        start_min_y.min(end_min_y),
        start_max_x.max(end_max_x),
        start_max_y.max(end_max_y),
    ];
    if !bounds.into_iter().all(f64::is_finite) {
        return Ok(None);
    }
    let [min_x, min_y, max_x, max_y] = bounds;
    Ok(Some(Segment {
        start: crate::geometry::XY::new(min_x, min_y),
        end: crate::geometry::XY::new(max_x, max_y),
    }))
}

struct ExactQRectangleWindows {
    first: ExactQRectangle,
    second: Option<ExactQRectangle>,
}

impl ExactQRectangleWindows {
    fn new(frame: GeographicFrame, cut: &ExactRational, bounds: Bounds) -> Result<Option<Self>> {
        let [min_x, min_y, max_x, max_y] =
            [bounds.minx(), bounds.miny(), bounds.maxx(), bounds.maxy()];
        if ![min_x, min_y, max_x, max_y].into_iter().all(f64::is_finite)
            || min_x >= max_x
            || min_y >= max_y
            || min_x <= -frame.longitude().half_period()
            || max_x >= frame.longitude().half_period()
            || min_y <= -frame.pole()
            || max_y >= frame.pole()
        {
            return Ok(None);
        }
        let axis = frame.longitude();
        let (Ok(canonical_min), Ok(canonical_max)) =
            (axis.canonicalize(min_x), axis.canonicalize(max_x))
        else {
            return Ok(None);
        };
        if canonical_min.get().to_bits() != min_x.to_bits()
            || canonical_max.get().to_bits() != max_x.to_bits()
        {
            return Ok(None);
        }
        let center_x = f64::midpoint(min_x, max_x);
        let center_y = f64::midpoint(min_y, max_y);
        if !(min_x < center_x && center_x < max_x && min_y < center_y && center_y < max_y) {
            return Ok(None);
        }

        let min_x = ExactRational::from_f64(min_x);
        let max_x = ExactRational::from_f64(max_x);
        let min_y = ExactRational::from_f64(min_y);
        let max_y = ExactRational::from_f64(max_y);
        let period = ExactRational::from_f64(axis.period());
        if !max_x.try_subtract(&min_x)?.try_cmp(&period)?.is_lt() {
            return Ok(None);
        }
        // A rectangle crossing Q's artificial cut is split into its two
        // chart windows; seam and pole rectangles above decline to pair-Q.
        let crosses_cut = min_x.try_cmp(cut)?.is_lt() && max_x.try_cmp(cut)?.is_gt();
        let (first, second) = if crosses_cut {
            let cut_east = cut.try_add(&period)?;
            (
                ExactQRectangle::new(cut.clone(), max_x, min_y.clone(), max_y.clone()),
                Some(ExactQRectangle::new(
                    min_x.try_add(&period)?,
                    cut_east,
                    min_y,
                    max_y,
                )),
            )
        } else if max_x.try_cmp(cut)?.is_le() {
            (
                ExactQRectangle::new(
                    min_x.try_add(&period)?,
                    max_x.try_add(&period)?,
                    min_y,
                    max_y,
                ),
                None,
            )
        } else {
            (ExactQRectangle::new(min_x, max_x, min_y, max_y), None)
        };
        Ok(Some(Self { first, second }))
    }

    fn iter(&self) -> impl Iterator<Item = &ExactQRectangle> {
        std::iter::once(&self.first).chain(self.second.iter())
    }
}

struct ExactQRectangle {
    min_x: ExactRational,
    min_y: ExactRational,
    max_x: ExactRational,
    max_y: ExactRational,
    corners: [ExactChartPoint; 4],
}

impl ExactQRectangle {
    fn new(
        min_x: ExactRational,
        max_x: ExactRational,
        min_y: ExactRational,
        max_y: ExactRational,
    ) -> Self {
        let corners = [
            ExactChartPoint::from_rationals(min_x.clone(), min_y.clone()),
            ExactChartPoint::from_rationals(max_x.clone(), min_y.clone()),
            ExactChartPoint::from_rationals(max_x.clone(), max_y.clone()),
            ExactChartPoint::from_rationals(min_x.clone(), max_y.clone()),
        ];
        Self {
            min_x,
            min_y,
            max_x,
            max_y,
            corners,
        }
    }

    fn outward_query_segment(&self) -> Result<Option<Segment>> {
        let (min_x, _) = self.min_x.outward_bounds()?;
        let (min_y, _) = self.min_y.outward_bounds()?;
        let (_, max_x) = self.max_x.outward_bounds()?;
        let (_, max_y) = self.max_y.outward_bounds()?;
        if ![min_x, min_y, max_x, max_y].into_iter().all(f64::is_finite) {
            return Ok(None);
        }
        Ok(Some(Segment {
            start: crate::geometry::XY::new(min_x, min_y),
            end: crate::geometry::XY::new(max_x, max_y),
        }))
    }

    fn contains_closed(&self, point: &ExactChartPoint) -> Result<bool> {
        Ok(point.x.try_cmp(&self.min_x)?.is_ge()
            && point.x.try_cmp(&self.max_x)?.is_le()
            && point.y.try_cmp(&self.min_y)?.is_ge()
            && point.y.try_cmp(&self.max_y)?.is_le())
    }

    fn segment_intersects_closed(
        &self,
        start: &ExactChartPoint,
        end: &ExactChartPoint,
    ) -> Result<bool> {
        if self.contains_closed(start)? || self.contains_closed(end)? {
            return Ok(true);
        }
        let [southwest, southeast, northeast, northwest] = &self.corners;
        for (left, right) in [
            (southwest, southeast),
            (southeast, northeast),
            (northeast, northwest),
            (northwest, southwest),
        ] {
            if exact_segments_intersect(start, end, left, right)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn segment_intersects_open(
        &self,
        start: &ExactChartPoint,
        end: &ExactChartPoint,
    ) -> Result<bool> {
        let mut low = ExactRational::zero();
        let mut high = ExactRational::one();
        let delta_x = end.x.try_subtract(&start.x)?;
        if !clip_open_parameter(
            &start.x,
            &delta_x,
            &self.min_x,
            &self.max_x,
            &mut low,
            &mut high,
        )? {
            return Ok(false);
        }
        let delta_y = end.y.try_subtract(&start.y)?;
        clip_open_parameter(
            &start.y,
            &delta_y,
            &self.min_y,
            &self.max_y,
            &mut low,
            &mut high,
        )
    }
}

fn clip_open_parameter(
    origin: &ExactRational,
    delta: &ExactRational,
    minimum: &ExactRational,
    maximum: &ExactRational,
    low: &mut ExactRational,
    high: &mut ExactRational,
) -> Result<bool> {
    if delta.is_zero() {
        return Ok(origin.try_cmp(minimum)?.is_gt() && origin.try_cmp(maximum)?.is_lt());
    }
    let at_minimum = minimum.try_subtract(origin)?.try_quotient(delta)?;
    let at_maximum = maximum.try_subtract(origin)?.try_quotient(delta)?;
    let (lower, upper) = if at_minimum.try_cmp(&at_maximum)?.is_lt() {
        (at_minimum, at_maximum)
    } else {
        (at_maximum, at_minimum)
    };
    if lower.try_cmp(low)?.is_gt() {
        *low = lower;
    }
    if upper.try_cmp(high)?.is_lt() {
        *high = upper;
    }
    Ok(low.try_cmp(high)?.is_lt())
}

fn exact_segments_intersect(
    left_start: &ExactChartPoint,
    left_end: &ExactChartPoint,
    right_start: &ExactChartPoint,
    right_end: &ExactChartPoint,
) -> Result<bool> {
    let left_start_orientation = exact_orientation(left_start, left_end, right_start)?;
    let left_end_orientation = exact_orientation(left_start, left_end, right_end)?;
    let right_start_orientation = exact_orientation(right_start, right_end, left_start)?;
    let right_end_orientation = exact_orientation(right_start, right_end, left_end)?;
    if signs_are_opposite(left_start_orientation, left_end_orientation)
        && signs_are_opposite(right_start_orientation, right_end_orientation)
    {
        return Ok(true);
    }
    Ok((left_start_orientation == ExactSign::Zero
        && point_on_exact_segment(right_start, left_start, left_end)?)
        || (left_end_orientation == ExactSign::Zero
            && point_on_exact_segment(right_end, left_start, left_end)?)
        || (right_start_orientation == ExactSign::Zero
            && point_on_exact_segment(left_start, right_start, right_end)?)
        || (right_end_orientation == ExactSign::Zero
            && point_on_exact_segment(left_end, right_start, right_end)?))
}

fn exact_orientation(
    start: &ExactChartPoint,
    end: &ExactChartPoint,
    point: &ExactChartPoint,
) -> Result<ExactSign> {
    let direction = vector_difference(start, end)?;
    let offset = vector_difference(start, point)?;
    Ok(vector_cross(&direction, &offset)?.sign())
}

const fn signs_are_opposite(left: ExactSign, right: ExactSign) -> bool {
    matches!(
        (left, right),
        (ExactSign::Positive, ExactSign::Negative) | (ExactSign::Negative, ExactSign::Positive)
    )
}

#[derive(Debug)]
/// A row occupies half-open intervals in the dual walk's face preorder.
/// Crossing and rollback record only changed rows, so nesting does not copy
/// enclosing memberships into every descendant face.
pub(super) struct GlobalFaceRowOccupancy {
    face_order: Box<[usize]>,
    face_counts: Box<[usize]>,
    row_offsets: Box<[usize]>,
    intervals: Box<[[usize; 2]]>,
}

fn seam_boundary_owner(
    sources: &ExactGeographicChartInventory,
    frame: GeographicFrame,
    owner: &QSpanOwner,
) -> Result<Option<SeamBoundaryOwner>> {
    if owner.ring_transition.is_none() {
        return Ok(None);
    }
    let edge = sources
        .edges
        .get(owner.source_edge.0 as usize)
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("Q seam span has an unknown source edge".into())
        })?;
    let SourceEdgeKind::Ring {
        polygon,
        ring,
        hole,
        ..
    } = edge.kind
    else {
        return Ok(None);
    };
    let [start, end] = edge.source;
    if start.x.to_bits() != end.x.to_bits() || start.y.to_bits() == end.y.to_bits() {
        return Ok(None);
    }
    let axis = frame.longitude();
    let start_longitude = axis
        .canonicalize(start.x)
        .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
    let end_longitude = axis
        .canonicalize(end.x)
        .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
    if start_longitude.identity(axis) != crate::geographic::SeamIdentity::Seam
        || end_longitude.identity(axis) != crate::geographic::SeamIdentity::Seam
    {
        return Ok(None);
    }
    Ok(Some(SeamBoundaryOwner {
        edge: owner.source_edge,
        polygon,
        ring,
        hole,
        east_spelling: start.x.is_sign_positive(),
    }))
}

impl GlobalFaceRowOccupancy {
    #[expect(
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        reason = "GlobalFaceId arcs and CSR offsets are built from the checked global face count"
    )]
    fn build(
        face_count: usize,
        sources: &ExactGeographicChartInventory,
        topology: &QuotientFaces,
        global: &GlobalFaceTopology,
        capture_seam_cut_edge_pairs: bool,
    ) -> Result<(
        Self,
        GlobalSpanRowSupport,
        GlobalNodeRowSupport,
        Option<SeamCutEdgePairs>,
    )> {
        let offset_count = face_count
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?;
        let (mut collapsed_points, collapsed_point_faces) =
            collapsed_point_candidates(sources, topology, global, face_count)?;
        let mut node_point_memberships = Vec::with_capacity(sources.points.len());
        append_explicit_node_point_support(sources, topology, &mut node_point_memberships)?;
        let Some(seed_face) = global.seed_face else {
            if global.seed_winding.iter().any(|&winding| winding != 0) {
                return Err(GeometryErrorKind::Invalid(
                    "Q has seed winding without a global seed face".into(),
                )
                .into());
            }
            let mut shell_candidates = Vec::new();
            let mut hole_candidates = Vec::new();
            let mut collapsed_shells = Vec::new();
            let mut span_collapsed_memberships = Vec::new();
            for (span_index, span) in topology.spans.iter().enumerate() {
                collect_collapsed_shell_candidates(
                    span,
                    sources,
                    None,
                    &mut shell_candidates,
                    &mut hole_candidates,
                    &mut collapsed_shells,
                )?;
                span_collapsed_memberships.extend(
                    collapsed_shells
                        .iter()
                        .map(|candidate| (span_index, candidate.row)),
                );
            }
            node_point_memberships.extend(
                collapsed_points
                    .iter()
                    .filter(|candidate| !candidate.trimmed)
                    .map(|candidate| (candidate.node.0 as usize, candidate.row)),
            );
            return Ok((
                Self {
                    face_order: vec![0; face_count].into_boxed_slice(),
                    face_counts: vec![0; face_count].into_boxed_slice(),
                    row_offsets: vec![0; sources.row_count + 1].into_boxed_slice(),
                    intervals: Box::new([]),
                },
                GlobalSpanRowSupport::build(topology.spans.len(), &mut span_collapsed_memberships)?,
                GlobalNodeRowSupport::build(topology.nodes.len(), &mut node_point_memberships)?,
                capture_seam_cut_edge_pairs.then(SeamCutEdgePairs::default),
            ));
        };
        if global.seed_winding.len() != sources.rings.len() {
            return Err(GeometryErrorKind::Invalid(
                "Q seed winding does not cover every source ring".into(),
            )
            .into());
        }
        let seed_index = seed_face.0 as usize;
        if seed_index >= face_count {
            return Err(
                GeometryErrorKind::Invalid("Q seed has an unknown global face".into()).into(),
            );
        }

        let mut arc_count = 0_usize;
        for (span_index, faces) in global.span_global_faces.iter().enumerate() {
            let Some([left, right]) = faces else {
                continue;
            };
            let additional = if left == right {
                let span = topology.spans.get(span_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q face table has an unknown span".into())
                })?;
                usize::from(span_has_collapsed_shell(span, sources)?)
            } else {
                2
            };
            arc_count = arc_count
                .checked_add(additional)
                .ok_or(ResourceError::SizeOverflow)?;
        }
        let mut arcs = Vec::with_capacity(arc_count);
        for (span_index, faces) in global.span_global_faces.iter().enumerate() {
            let Some([left, right]) = faces else {
                continue;
            };
            let span = topology.spans.get(span_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q face table has an unknown span".into())
            })?;
            if left == right {
                if span_has_ring_transition(span)? {
                    return Err(GeometryErrorKind::Invalid(
                        format!(
                            "Q span {:?} nodes {:?}, ports {:?}, local faces {:?}, owners {:?}, has ring transitions {:?} on the same global face {left:?}",
                            span.id,
                            span.nodes,
                            span.ports,
                            topology.span_local_faces.get(span_index),
                            span.owners
                                .iter()
                                .filter_map(|owner| sources
                                    .edges
                                    .get(owner.source_edge.0 as usize)
                                    .map(|edge| (
                                        owner.source_edge,
                                        edge.source,
                                        owner.reversed,
                                        &owner.source_range,
                                    )))
                                .collect::<Vec<_>>(),
                            span.owners
                                .iter()
                                .filter_map(|owner| owner.ring_transition)
                                .collect::<Vec<_>>()
                        )
                        .into(),
                    )
                    .into());
                }
                if span_has_collapsed_shell(span, sources)? {
                    arcs.push(FaceArc {
                        from: *left,
                        to: *right,
                        span: span_index,
                        forward: true,
                    });
                }
                continue;
            }
            arcs.push(FaceArc {
                from: *left,
                to: *right,
                span: span_index,
                forward: true,
            });
            arcs.push(FaceArc {
                from: *right,
                to: *left,
                span: span_index,
                forward: false,
            });
        }
        if arcs.len() != arc_count {
            return Err(GeometryErrorKind::Invalid(
                "global face dual arc count changed during construction".into(),
            )
            .into());
        }
        let mut degrees = vec![0_usize; face_count];
        for arc in &arcs {
            let degree = degrees.get_mut(arc.from.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q dual arc references an unknown global face".into())
            })?;
            *degree = degree.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
        }
        let mut offsets = Vec::with_capacity(offset_count);
        offsets.push(0_usize);
        for degree in degrees {
            let next = offsets
                .last()
                .copied()
                .expect("face offsets start at zero")
                .checked_add(degree)
                .ok_or(ResourceError::SizeOverflow)?;
            offsets.push(next);
        }
        if offsets.last().copied() != Some(arcs.len()) {
            return Err(GeometryErrorKind::Invalid(
                "global face dual offsets do not match the arc count".into(),
            )
            .into());
        }
        let mut positions = offsets[..face_count].to_vec();
        let mut ordered_arcs = arcs.clone();
        for arc in arcs {
            let position = &mut positions[arc.from.0 as usize];
            ordered_arcs[*position] = arc;
            *position += 1;
        }
        let arcs = ordered_arcs;

        let mut state = FaceOccupancyState::new(sources, &global.seed_winding)?;
        trim_collapsed_points_on_face(
            seed_face,
            &collapsed_point_faces,
            &mut collapsed_points,
            &state,
        )?;
        let mut seen = vec![false; face_count];
        let mut face_order = vec![0; face_count];
        let mut face_counts = vec![0; face_count];
        let mut span_collapsed_pending = vec![true; topology.spans.len()];
        let mut span_collapsed_memberships = Vec::<(usize, SourceRowId)>::new();
        let mut seam_owner_sides = Vec::new();
        let mut shell_candidates = Vec::new();
        let mut hole_candidates = Vec::new();
        let mut collapsed_shells = Vec::new();
        seen[seed_index] = true;
        state.append_face(seed_face, &mut face_order, &mut face_counts);
        let seed_start = offsets[seed_index];
        let seed_end = offsets[seed_index + 1];
        let mut stack = vec![FaceWalkFrame {
            next_arc: seed_start,
            end_arc: seed_end,
            undo: None,
        }];
        while !stack.is_empty() {
            let arc = {
                let frame = stack.last_mut().expect("the face walk stack is nonempty");
                if frame.next_arc == frame.end_arc {
                    None
                } else {
                    let arc = arcs.get(frame.next_arc).copied().ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q face walk has an invalid arc offset".into())
                    })?;
                    frame.next_arc = frame
                        .next_arc
                        .checked_add(1)
                        .ok_or(ResourceError::SizeOverflow)?;
                    Some(arc)
                }
            };
            if let Some(arc) = arc {
                let target = arc.to.0 as usize;
                let target_seen = seen.get(target).copied().ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q dual arc has an unknown target face".into())
                })?;
                let span = topology.spans.get(arc.span).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q dual arc has an unknown span".into())
                })?;
                let multiplier = if arc.forward { 1 } else { -1 };
                let pending = span_collapsed_pending.get_mut(arc.span).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q dual arc has no collapsed-shell state".into())
                })?;
                if *pending {
                    let seam_owners = if capture_seam_cut_edge_pairs {
                        span.owners
                            .iter()
                            .map(|owner| seam_boundary_owner(sources, sources.frame, owner))
                            .collect::<Result<Vec<_>>>()?
                            .into_iter()
                            .flatten()
                            .collect::<Vec<_>>()
                    } else {
                        Vec::new()
                    };
                    let seam_before = seam_owners
                        .iter()
                        .map(|&owner| state.seam_owner_interior(owner))
                        .collect::<Result<Vec<_>>>()?;
                    collect_collapsed_shell_candidates(
                        span,
                        sources,
                        Some(&state),
                        &mut shell_candidates,
                        &mut hole_candidates,
                        &mut collapsed_shells,
                    )?;
                    state.apply(&span.owners, multiplier, sources)?;
                    for (&owner, &before) in seam_owners.iter().zip(&seam_before) {
                        let after = state.seam_owner_interior(owner)?;
                        let (left_interior, right_interior) = if arc.forward {
                            (before, after)
                        } else {
                            (after, before)
                        };
                        let [first, second] = span.nodes;
                        seam_owner_sides.push(SeamOwnerSide {
                            nodes: if first <= second {
                                [first, second]
                            } else {
                                [second, first]
                            },
                            edge: owner.edge,
                            polygon: owner.polygon,
                            east_spelling: owner.east_spelling,
                            left_interior,
                            right_interior,
                        });
                    }
                    for candidate in &collapsed_shells {
                        let holes_after = state
                            .polygon_holes
                            .get(candidate.polygon.0 as usize)
                            .copied()
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "collapsed shell has an unknown polygon occupancy".into(),
                                )
                            })?;
                        if candidate.holes_before == 0 && holes_after == 0 {
                            span_collapsed_memberships.push((arc.span, candidate.row));
                        }
                    }
                    *pending = false;
                    if target_seen {
                        state.apply(&span.owners, -multiplier, sources)?;
                        continue;
                    }
                } else {
                    if target_seen {
                        continue;
                    }
                    state.apply(&span.owners, multiplier, sources)?;
                }
                *seen.get_mut(target).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q dual target has no visited state".into())
                })? = true;
                state.append_face(arc.to, &mut face_order, &mut face_counts);
                trim_collapsed_points_on_face(
                    arc.to,
                    &collapsed_point_faces,
                    &mut collapsed_points,
                    &state,
                )?;
                let start = *offsets.get(target).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q dual target has no adjacency start".into())
                })?;
                let end = *offsets.get(target + 1).ok_or_else(|| {
                    GeometryErrorKind::Invalid("Q dual target has no adjacency end".into())
                })?;
                stack.push(FaceWalkFrame {
                    next_arc: start,
                    end_arc: end,
                    undo: Some((arc.span, -multiplier)),
                });
            } else {
                let frame = stack.pop().expect("the face walk stack is nonempty");
                if let Some((span_index, multiplier)) = frame.undo {
                    let span = topology.spans.get(span_index).ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q face rollback has an unknown span".into())
                    })?;
                    state.apply(&span.owners, multiplier, sources)?;
                }
            }
        }
        if seen.iter().any(|&visited| !visited) {
            return Err(GeometryErrorKind::Invalid(
                "the global Q face dual is disconnected".into(),
            )
            .into());
        }
        node_point_memberships.extend(
            collapsed_points
                .iter()
                .filter(|candidate| !candidate.trimmed)
                .map(|candidate| (candidate.node.0 as usize, candidate.row)),
        );

        for (row, &start) in state.interval_starts.iter().enumerate() {
            if let Some(start) = start
                && start < state.cursor
            {
                state.intervals.push((
                    SourceRowId(u32::try_from(row).map_err(|_| ResourceError::SizeOverflow)?),
                    [start, state.cursor],
                ));
            }
        }
        let mut row_offsets = vec![0_usize; sources.row_count + 1];
        for &(row, _) in &state.intervals {
            row_offsets[row.0 as usize + 1] += 1;
        }
        for index in 1..row_offsets.len() {
            row_offsets[index] += row_offsets[index - 1];
        }
        let mut positions = row_offsets[..sources.row_count].to_vec();
        let mut intervals = vec![[0; 2]; state.intervals.len()];
        for (row, interval) in state.intervals {
            let position = &mut positions[row.0 as usize];
            intervals[*position] = interval;
            *position += 1;
        }
        let mut write = 0;
        // Non-tree crossings can close and reopen a row without visiting a face.
        for row in 0..sources.row_count {
            let start = row_offsets[row];
            let end = row_offsets[row + 1];
            row_offsets[row] = write;
            let row_start = write;
            for read in start..end {
                let interval = intervals[read];
                if write > row_start && intervals[write - 1][1] == interval[0] {
                    intervals[write - 1][1] = interval[1];
                } else {
                    intervals[write] = interval;
                    write += 1;
                }
            }
        }
        row_offsets[sources.row_count] = write;
        intervals.truncate(write);
        let span_collapsed_rows =
            GlobalSpanRowSupport::build(topology.spans.len(), &mut span_collapsed_memberships)?;
        let node_point_rows =
            GlobalNodeRowSupport::build(topology.nodes.len(), &mut node_point_memberships)?;
        let seam_cut_edge_pairs = if capture_seam_cut_edge_pairs {
            Some(classify_seam_cut_edge_pairs(
                seam_owner_sides,
                sources,
                sources.frame,
            )?)
        } else {
            None
        };
        Ok((
            Self {
                face_order: face_order.into_boxed_slice(),
                face_counts: face_counts.into_boxed_slice(),
                row_offsets: row_offsets.into_boxed_slice(),
                intervals: intervals.into_boxed_slice(),
            },
            span_collapsed_rows,
            node_point_rows,
            seam_cut_edge_pairs,
        ))
    }

    #[expect(
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        reason = "face and source row IDs belong to the admitted topology; interval offsets cover every row"
    )]
    pub(super) fn contains(&self, face: GlobalFaceId, row: SourceRowId) -> bool {
        let face = face.0 as usize;
        let row = row.0 as usize;
        if row + 1 >= self.row_offsets.len() {
            return false;
        }
        let count = self.face_counts[face];
        if count == 0 {
            return false;
        }
        if count + 1 == self.row_offsets.len() {
            return true;
        }
        let order = self.face_order[face];
        let intervals = &self.intervals[self.row_offsets[row]..self.row_offsets[row + 1]];
        let end = intervals.partition_point(|interval| interval[0] <= order);
        end != 0 && order < intervals[end - 1][1]
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "global face IDs belong to the admitted topology"
    )]
    pub(super) fn count(&self, face: GlobalFaceId) -> usize {
        self.face_counts[face.0 as usize]
    }

    #[cfg(test)]
    fn rows_for(&self, face: GlobalFaceId) -> Vec<SourceRowId> {
        self.row_offsets
            .windows(2)
            .enumerate()
            .map(|(row, _)| SourceRowId(u32::try_from(row).unwrap()))
            .filter(|&row| self.contains(face, row))
            .collect()
    }
}

fn classify_seam_cut_edge_pairs(
    mut sides: Vec<SeamOwnerSide>,
    sources: &ExactGeographicChartInventory,
    frame: GeographicFrame,
) -> Result<SeamCutEdgePairs> {
    sides.sort_unstable_by_key(|side| {
        (
            side.nodes,
            side.edge,
            side.polygon,
            side.east_spelling,
            side.left_interior,
            side.right_interior,
        )
    });
    sides.dedup_by_key(|side| {
        (
            side.nodes,
            side.edge,
            side.polygon,
            side.east_spelling,
            side.left_interior,
            side.right_interior,
        )
    });

    let mut pair_spans = Vec::new();
    let mut start = 0;
    while start < sides.len() {
        let Some(first) = sides.get(start) else {
            break;
        };
        let nodes = first.nodes;
        let mut end = start.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
        while sides.get(end).is_some_and(|side| side.nodes == nodes) {
            end = end.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
        }
        let Some(group) = sides.get(start..end) else {
            break;
        };
        for (index, left) in group.iter().enumerate() {
            for right in group.iter().skip(index).skip(1) {
                if left.edge == right.edge || left.east_spelling == right.east_spelling {
                    continue;
                }
                let left_has_interior = left.left_interior != left.right_interior;
                let right_has_interior = right.left_interior != right.right_interior;
                let opposite_interiors = left_has_interior
                    && right_has_interior
                    && left.left_interior != right.left_interior;
                let (edges, polygons) = if left.edge < right.edge {
                    (
                        (left.edge.0, right.edge.0),
                        (left.polygon.0, right.polygon.0),
                    )
                } else {
                    (
                        (right.edge.0, left.edge.0),
                        (right.polygon.0, left.polygon.0),
                    )
                };
                pair_spans.push((edges, polygons, opposite_interiors));
            }
        }
        start = end;
    }
    pair_spans.sort_unstable_by_key(|&(edges, ..)| edges);

    let mut edge_pairs = Vec::new();
    let mut cursor = 0;
    while cursor < pair_spans.len() {
        let Some(&((left, right), polygons, _)) = pair_spans.get(cursor) else {
            break;
        };
        let mut end = cursor.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
        let mut every_overlap_span_is_a_cut =
            pair_spans.get(cursor).is_some_and(|&(_, _, cut)| cut);
        while let Some(&(next_edges, _, cut)) = pair_spans.get(end) {
            if next_edges != (left, right) {
                break;
            }
            every_overlap_span_is_a_cut &= cut;
            end = end.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
        }
        if every_overlap_span_is_a_cut {
            let left_index = usize::try_from(left).map_err(|_| ResourceError::SizeOverflow)?;
            let right_index = usize::try_from(right).map_err(|_| ResourceError::SizeOverflow)?;
            let left_edge = sources.edges.get(left_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("a seam cut owner has an unknown source edge".into())
            })?;
            let right_edge = sources.edges.get(right_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("a seam cut owner has an unknown source edge".into())
            })?;
            let [left_start, left_end] = left_edge.source;
            let [right_start, right_end] = right_edge.source;
            let low_y = left_start
                .y
                .min(left_end.y)
                .max(right_start.y.min(right_end.y));
            let high_y = left_start
                .y
                .max(left_end.y)
                .min(right_start.y.max(right_end.y));
            if low_y < high_y {
                let left_ring = match left_edge.kind {
                    SourceEdgeKind::Ring { ring, .. } => ring,
                    SourceEdgeKind::Line => {
                        return Err(GeometryErrorKind::Invalid(
                            "a seam cut owner is not a ring".into(),
                        )
                        .into());
                    },
                };
                let right_ring = match right_edge.kind {
                    SourceEdgeKind::Ring { ring, .. } => ring,
                    SourceEdgeKind::Line => {
                        return Err(GeometryErrorKind::Invalid(
                            "a seam cut owner is not a ring".into(),
                        )
                        .into());
                    },
                };
                edge_pairs.push(SeamCutEdgePair {
                    edges: (left, right),
                    polygons,
                    low_y,
                    high_y,
                    low_adjacent_edges: (
                        seam_edge_adjacent_at_y(sources, left, low_y, left_ring),
                        seam_edge_adjacent_at_y(sources, right, low_y, right_ring),
                    ),
                    high_adjacent_edges: (
                        seam_edge_adjacent_at_y(sources, left, high_y, left_ring),
                        seam_edge_adjacent_at_y(sources, right, high_y, right_ring),
                    ),
                });
            }
        }
        cursor = end;
    }
    let mut touch_filters = Vec::with_capacity(edge_pairs.len().saturating_mul(4));
    for pair in &edge_pairs {
        for (edge, polygon, adjacent, cut_y, ignore_above, ignore_below) in [
            (
                pair.edges.0,
                pair.polygons.1,
                pair.low_adjacent_edges.1,
                pair.low_y,
                true,
                false,
            ),
            (
                pair.edges.0,
                pair.polygons.1,
                pair.high_adjacent_edges.1,
                pair.high_y,
                false,
                true,
            ),
            (
                pair.edges.1,
                pair.polygons.0,
                pair.low_adjacent_edges.0,
                pair.low_y,
                true,
                false,
            ),
            (
                pair.edges.1,
                pair.polygons.0,
                pair.high_adjacent_edges.0,
                pair.high_y,
                false,
                true,
            ),
        ] {
            if let Some(adjacent_edge) = adjacent {
                touch_filters.push(SeamTouchFilter {
                    edge,
                    other_polygon: polygon,
                    latitude_bits: canonical_f64_bits(cut_y),
                    adjacent_edge,
                    ignore_above,
                    ignore_below,
                });
            }
        }
    }
    touch_filters.sort_unstable();
    touch_filters.dedup();
    Ok(SeamCutEdgePairs {
        seam_x: frame.longitude().half_period(),
        pairs: edge_pairs.into_boxed_slice(),
        touch_filters: touch_filters.into_boxed_slice(),
    })
}

fn seam_edge_adjacent_at_y(
    sources: &ExactGeographicChartInventory,
    edge: u32,
    y: f64,
    ring: SourceRingId,
) -> Option<u32> {
    // LineworkChains removes zero-length stutters, so seek the nearest staged
    // neighbor rather than assuming the adjacent source-edge ordinal survives.
    let edge_index = usize::try_from(edge).ok()?;
    let source_edge = sources.edges.get(edge_index)?;
    let [start, end] = source_edge.source;
    let at_start = canonical_f64_bits(start.y) == canonical_f64_bits(y);
    let at_end = canonical_f64_bits(end.y) == canonical_f64_bits(y);
    let previous = if at_start {
        true
    } else if at_end {
        false
    } else {
        return None;
    };
    let source_ring = sources.rings.get(ring.0 as usize)?;
    let range = &source_ring.source_edges;
    let mut candidate = edge_index;
    for _ in 0..range.len() {
        candidate = if previous {
            if candidate == range.start {
                range.end.checked_sub(1)?
            } else {
                candidate.checked_sub(1)?
            }
        } else if candidate.checked_add(1)? == range.end {
            range.start
        } else {
            candidate.checked_add(1)?
        };
        let candidate_edge = sources.edges.get(candidate)?;
        let [candidate_start, candidate_end] = candidate_edge.source;
        if canonical_f64_bits(candidate_start.x) != canonical_f64_bits(candidate_end.x)
            || canonical_f64_bits(candidate_start.y) != canonical_f64_bits(candidate_end.y)
        {
            return Some(candidate_edge.id.0);
        }
    }
    None
}

#[derive(Clone, Copy)]
struct FaceArc {
    from: GlobalFaceId,
    to: GlobalFaceId,
    span: usize,
    forward: bool,
}

struct FaceWalkFrame {
    next_arc: usize,
    end_arc: usize,
    undo: Option<(usize, i64)>,
}

struct FaceOccupancyState {
    winding: Vec<i64>,
    polygon_shells: Vec<usize>,
    polygon_holes: Vec<usize>,
    row_polygon_counts: Vec<usize>,
    active_count: usize,
    interval_starts: Vec<Option<usize>>,
    intervals: Vec<(SourceRowId, [usize; 2])>,
    cursor: usize,
}

impl FaceOccupancyState {
    #[expect(
        clippy::indexing_slicing,
        reason = "ring, polygon, and row IDs index counters sized from the admitted inventory"
    )]
    fn new(sources: &ExactGeographicChartInventory, winding: &[i64]) -> Result<Self> {
        if winding.len() != sources.rings.len() {
            return Err(GeometryErrorKind::Invalid(
                "Q face seed has the wrong source-ring winding count".into(),
            )
            .into());
        }
        let mut polygon_shells = vec![0_usize; sources.polygons.len()];
        let mut polygon_holes = vec![0_usize; sources.polygons.len()];
        for (ring, &ring_winding) in sources.rings.iter().zip(winding) {
            if ring_winding == 0 {
                continue;
            }
            let polygon = if ring.hole {
                polygon_holes.get_mut(ring.polygon.0 as usize)
            } else {
                polygon_shells.get_mut(ring.polygon.0 as usize)
            }
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q source ring has an unknown polygon".into())
            })?;
            *polygon = polygon.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
        }
        let mut row_polygon_counts = vec![0_usize; sources.row_count];
        for ((polygon, &shells), &holes) in sources
            .polygons
            .iter()
            .zip(&polygon_shells)
            .zip(&polygon_holes)
        {
            if shells != 0 && holes == 0 {
                let count = row_polygon_counts
                    .get_mut(polygon.row.0 as usize)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid("Q polygon has an unknown row".into())
                    })?;
                *count = count.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
            }
        }
        let interval_starts = row_polygon_counts
            .iter()
            .map(|&count| (count != 0).then_some(0))
            .collect::<Vec<_>>();
        let active_count = interval_starts
            .iter()
            .filter(|start| start.is_some())
            .count();
        Ok(Self {
            winding: winding.to_vec(),
            polygon_shells,
            polygon_holes,
            row_polygon_counts,
            active_count,
            interval_starts,
            intervals: Vec::new(),
            cursor: 0,
        })
    }

    fn apply(
        &mut self,
        owners: &[QSpanOwner],
        multiplier: i64,
        sources: &ExactGeographicChartInventory,
    ) -> Result<()> {
        for_each_span_ring_transition(owners, &mut |transition| {
            self.apply_transition(transition, multiplier, sources)
        })
    }

    fn seam_owner_interior(&self, owner: SeamBoundaryOwner) -> Result<bool> {
        let ring_inside = self
            .winding
            .get(owner.ring.0 as usize)
            .map(|&winding| winding != 0)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q seam owner has an unknown ring".into()))?;
        let polygon = owner.polygon.0 as usize;
        let (Some(&shells), Some(&holes)) = (
            self.polygon_shells.get(polygon),
            self.polygon_holes.get(polygon),
        ) else {
            return Err(
                GeometryErrorKind::Invalid("Q seam owner has an unknown polygon".into()).into(),
            );
        };
        let polygon_inside = shells != 0 && holes == 0;
        Ok((if owner.hole {
            !ring_inside
        } else {
            ring_inside
        }) && polygon_inside)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "source ring and polygon IDs index state vectors sized from the same inventory"
    )]
    fn apply_transition(
        &mut self,
        transition: RingTransition,
        multiplier: i64,
        sources: &ExactGeographicChartInventory,
    ) -> Result<()> {
        let index = transition.ring.0 as usize;
        let winding = self
            .winding
            .get_mut(index)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q transition has an unknown ring".into()))?;
        let delta = transition
            .delta
            .checked_mul(multiplier)
            .ok_or(ResourceError::SizeOverflow)?;
        let next = winding
            .checked_add(delta)
            .ok_or(ResourceError::SizeOverflow)?;
        let was_inside = *winding != 0;
        let is_inside = next != 0;
        *winding = next;
        if was_inside == is_inside {
            return Ok(());
        }
        let ring = sources.rings.get(index).ok_or_else(|| {
            GeometryErrorKind::Invalid("Q transition has an unknown source ring".into())
        })?;
        let polygon_index = ring.polygon.0 as usize;
        let (Some(&shells), Some(&holes)) = (
            self.polygon_shells.get(polygon_index),
            self.polygon_holes.get(polygon_index),
        ) else {
            return Err(
                GeometryErrorKind::Invalid("Q transition has an unknown polygon".into()).into(),
            );
        };
        let before_polygon = shells != 0 && holes == 0;
        let count = if ring.hole {
            self.polygon_holes.get_mut(polygon_index)
        } else {
            self.polygon_shells.get_mut(polygon_index)
        }
        .ok_or_else(|| GeometryErrorKind::Invalid("Q transition has an unknown polygon".into()))?;
        *count = if is_inside {
            count.checked_add(1).ok_or(ResourceError::SizeOverflow)?
        } else {
            count.checked_sub(1).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q polygon ring occupancy underflowed".into())
            })?
        };
        let after_polygon =
            self.polygon_shells[polygon_index] != 0 && self.polygon_holes[polygon_index] == 0;
        if before_polygon == after_polygon {
            return Ok(());
        }
        let row = sources
            .polygons
            .get(polygon_index)
            .ok_or_else(|| {
                GeometryErrorKind::Invalid("Q polygon state has no source polygon".into())
            })?
            .row;
        let row_count = self
            .row_polygon_counts
            .get_mut(row.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q polygon has an unknown row".into()))?;
        *row_count = if after_polygon {
            row_count
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?
        } else {
            row_count.checked_sub(1).ok_or_else(|| {
                GeometryErrorKind::Invalid("Q row polygon occupancy underflowed".into())
            })?
        };
        if *row_count == 0 {
            self.deactivate_row(row)?;
        } else if !before_polygon {
            self.activate_row(row)?;
        }
        Ok(())
    }

    #[expect(
        clippy::arithmetic_side_effects,
        reason = "active row count is bounded by the admitted row inventory"
    )]
    fn activate_row(&mut self, row: SourceRowId) -> Result<()> {
        let start = self
            .interval_starts
            .get_mut(row.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q active row is unknown".into()))?;
        if start.is_none() {
            *start = Some(self.cursor);
            self.active_count += 1;
        }
        Ok(())
    }

    #[expect(
        clippy::arithmetic_side_effects,
        reason = "taking an active interval removes exactly one counted row"
    )]
    fn deactivate_row(&mut self, row: SourceRowId) -> Result<()> {
        let start = self
            .interval_starts
            .get_mut(row.0 as usize)
            .ok_or_else(|| GeometryErrorKind::Invalid("Q inactive row is unknown".into()))?;
        let start = start.take().ok_or_else(|| {
            GeometryErrorKind::Invalid("Q row occupancy disappeared before its last polygon".into())
        })?;
        if start < self.cursor {
            self.intervals.push((row, [start, self.cursor]));
        }
        self.active_count -= 1;
        Ok(())
    }

    #[expect(
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        reason = "each admitted global face is visited once"
    )]
    const fn append_face(&mut self, face: GlobalFaceId, order: &mut [usize], counts: &mut [usize]) {
        order[face.0 as usize] = self.cursor;
        counts[face.0 as usize] = self.active_count;
        self.cursor += 1;
    }
}

impl HeapSize for GlobalFaceRowOccupancy {
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "HeapSize sums the already-published face order and row interval buffers as an estimate"
    )]
    fn heap_bytes(&self) -> usize {
        (self.face_order.len() + self.face_counts.len() + self.row_offsets.len())
            * std::mem::size_of::<usize>()
            + self.intervals.len() * std::mem::size_of::<[usize; 2]>()
    }
}

fn point_heap_bytes(point: &ExactChartPoint) -> usize {
    point.heap_bytes()
}

fn rational_heap_bytes(value: &ExactRational) -> usize {
    super::exact::rational_heap_bytes(value)
}

#[expect(
    clippy::arithmetic_side_effects,
    reason = "both exact parameters are part of one owned source-range record"
)]
fn source_range_heap_bytes(range: &ExactSourceRange) -> usize {
    rational_heap_bytes(&range.start) + rational_heap_bytes(&range.end)
}

fn consider_cut_gap(
    low: &ExactRational,
    high: &ExactRational,
    half: &ExactRational,
    negative_half: &ExactRational,
    period: &ExactRational,
    largest_gap: &mut Option<ExactRational>,
    cut: &mut Option<ExactRational>,
) -> Result<()> {
    let width = high.try_subtract(low)?;
    let is_larger = if let Some(largest) = largest_gap {
        width.try_cmp(largest)?.is_gt()
    } else {
        true
    };
    if is_larger {
        let mut midpoint = low.try_midpoint(high)?;
        if midpoint.try_cmp(half)?.is_ge() {
            midpoint = midpoint.try_subtract(period)?;
        }
        if midpoint.try_cmp(negative_half)?.is_lt() {
            midpoint = midpoint.try_add(period)?;
        }
        *largest_gap = Some(width);
        *cut = Some(midpoint);
    }
    Ok(())
}

fn select_cut(frame: GeographicFrame, chart: &NodedExactChart) -> Result<ExactRational> {
    let axis = frame.longitude();
    let half = ExactRational::from_f64(axis.half_period());
    let negative_half = ExactRational::from_f64(-axis.half_period());
    let period = ExactRational::from_f64(axis.period());
    let mut longitudes = Vec::with_capacity(chart.ports.len());
    for port in &chart.ports {
        let longitude = if port.point.x.try_cmp(&half)?.is_eq() {
            negative_half.clone()
        } else {
            port.point.x.clone()
        };
        longitudes.push(longitude);
    }
    try_sort_by(&mut longitudes, ExactRational::try_cmp)?;
    let mut unique: Vec<ExactRational> = Vec::with_capacity(longitudes.len());
    for longitude in longitudes {
        if let Some(previous) = unique.last()
            && previous.try_cmp(&longitude)?.is_eq()
        {
            continue;
        }
        unique.push(longitude);
    }
    if unique.is_empty() {
        return Ok(ExactRational::zero());
    }

    let mut largest_gap: Option<ExactRational> = None;
    let mut cut: Option<ExactRational> = None;
    for adjacent in unique.windows(2) {
        let [low, high] = adjacent else {
            unreachable!("windows(2) yields adjacent longitudes");
        };
        consider_cut_gap(
            low,
            high,
            &half,
            &negative_half,
            &period,
            &mut largest_gap,
            &mut cut,
        )?;
    }
    let low = unique
        .last()
        .expect("nonempty longitude list has a last value");
    let high = unique
        .first()
        .expect("nonempty longitude list has a first value")
        .try_add(&period)?;
    consider_cut_gap(
        low,
        &high,
        &half,
        &negative_half,
        &period,
        &mut largest_gap,
        &mut cut,
    )?;
    Ok(cut.expect("a nonempty longitude set has a periodic gap"))
}

fn lifted_point(
    point: &ExactChartPoint,
    cut: &ExactRational,
    period: &ExactRational,
) -> Result<ExactChartPoint> {
    let x = if point.x.try_cmp(cut)?.is_lt() {
        point.x.try_add(period)?
    } else {
        point.x.clone()
    };
    Ok(ExactChartPoint::from_rationals(x, point.y.clone()))
}

fn cut_node_key(
    frame: GeographicFrame,
    chart: &NodedExactChart,
    latitude: &ExactRational,
) -> Result<QNodeKey> {
    let north = ExactRational::from_f64(frame.pole());
    let south = ExactRational::from_f64(-frame.pole());
    if latitude.try_cmp(&north)?.is_eq() {
        return chart
            .nodes
            .iter()
            .find(|node| matches!(&node.key, ExactQuotientKey::NorthPole))
            .map(|node| QNodeKey::Source(node.id))
            .ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "a cut span reaches a pole without a quotient pole node".into(),
                )
                .into()
            });
    }
    if latitude.try_cmp(&south)?.is_eq() {
        return chart
            .nodes
            .iter()
            .find(|node| matches!(&node.key, ExactQuotientKey::SouthPole))
            .map(|node| QNodeKey::Source(node.id))
            .ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "a cut span reaches a pole without a quotient pole node".into(),
                )
                .into()
            });
    }
    Ok(QNodeKey::Cut(latitude.clone()))
}

fn qnode_pole(chart: &NodedExactChart, key: &QNodeKey) -> Result<Option<Pole>> {
    let QNodeKey::Source(id) = key else {
        return Ok(None);
    };
    let node = chart.nodes.get(id.0 as usize).ok_or_else(|| {
        GeometryErrorKind::Invalid("a Q node references an unknown chart node".into())
    })?;
    Ok(match &node.key {
        ExactQuotientKey::NorthPole => Some(Pole::North),
        ExactQuotientKey::SouthPole => Some(Pole::South),
        ExactQuotientKey::Coordinate(_) | ExactQuotientKey::Seam(_) => None,
    })
}

fn add_cut_candidate(
    candidates: &mut Vec<PortCandidate>,
    point: &ExactChartPoint,
    cut: &ExactRational,
    period: &ExactRational,
    node: QNodeKey,
) -> Result<()> {
    let left = ExactChartPoint::from_rationals(cut.clone(), point.y.clone());
    let right = ExactChartPoint::from_rationals(cut.try_add(period)?, point.y.clone());
    candidates.push(PortCandidate {
        point: left,
        node: node.clone(),
        cut: true,
    });
    candidates.push(PortCandidate {
        point: right,
        node,
        cut: true,
    });
    Ok(())
}

fn compare_qnode_keys(left: &QNodeKey, right: &QNodeKey) -> Result<Ordering> {
    match (left, right) {
        (QNodeKey::Source(left), QNodeKey::Source(right)) => Ok(left.cmp(right)),
        (QNodeKey::Source(_), QNodeKey::Cut(_)) => Ok(Ordering::Less),
        (QNodeKey::Cut(_), QNodeKey::Source(_)) => Ok(Ordering::Greater),
        (QNodeKey::Cut(left), QNodeKey::Cut(right)) => left.try_cmp(right),
    }
}

fn canonical_source_node_key(chart: &NodedExactChart, node: QuotientNodeId) -> Result<QNodeKey> {
    if chart.nodes.get(node.0 as usize).is_none() {
        return Err(GeometryErrorKind::Invalid(
            "a chart port references an unknown quotient node".into(),
        )
        .into());
    }
    Ok(QNodeKey::Source(node))
}

fn compare_qport_candidates(left: &PortCandidate, right: &PortCandidate) -> Result<Ordering> {
    let point = left.point.compare_lex(&right.point)?;
    Ok(if point.is_eq() {
        compare_qnode_keys(&left.node, &right.node)?
    } else {
        point
    })
}

fn unique_port_candidates(
    mut candidates: Vec<PortCandidate>,
) -> Result<(Vec<QPort>, Vec<QNodeKey>)> {
    try_sort_by(&mut candidates, compare_qport_candidates)?;
    let mut ports: Vec<QPort> = Vec::with_capacity(candidates.len());
    let mut port_keys = Vec::with_capacity(candidates.len());
    for candidate in candidates {
        if let Some(previous) = ports.last()
            && previous.point.same_position(&candidate.point)?
        {
            let previous_key = port_keys.last().expect("each port has one node key");
            if !compare_qnode_keys(previous_key, &candidate.node)?.is_eq() {
                return Err(GeometryErrorKind::Invalid(
                    "one exact lifted point has two quotient identities".into(),
                )
                .into());
            }
            if !candidate.cut {
                ports
                    .last_mut()
                    .expect("a deduplicated candidate has a previous port")
                    .cut = false;
            }
            continue;
        }
        let id = QPortId(u32::try_from(ports.len()).map_err(|_| ResourceError::SizeOverflow)?);
        ports.push(QPort {
            id,
            point: candidate.point,
            node: QNodeId(0),
            cut: candidate.cut,
        });
        port_keys.push(candidate.node);
    }
    Ok((ports, port_keys))
}

#[expect(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "binary search bounds remain within the sorted exact QPort array"
)]
fn find_qport_if_present(ports: &[QPort], point: &ExactChartPoint) -> Result<Option<QPortId>> {
    let mut low = 0_usize;
    let mut high = ports.len();
    while low < high {
        let middle = low + (high - low) / 2;
        match ports[middle].point.compare_lex(point)? {
            Ordering::Less => low = middle + 1,
            Ordering::Greater => high = middle,
            Ordering::Equal => return Ok(Some(ports[middle].id)),
        }
    }
    Ok(None)
}

fn find_qport(ports: &[QPort], point: &ExactChartPoint) -> Result<QPortId> {
    find_qport_if_present(ports, point)?
        .ok_or_else(|| GeometryErrorKind::Invalid("exact Q port was not interned".into()).into())
}

fn q_port_node(ports: &[QPort], point: &ExactChartPoint) -> Result<Option<QNodeId>> {
    find_qport_if_present(ports, point)?
        .map(|port_id| {
            ports
                .get(port_id.0 as usize)
                .map(|port| port.node)
                .ok_or_else(|| GeometryErrorKind::Invalid("Q query port is unknown".into()).into())
        })
        .transpose()
}

fn split_owner_range(
    range: &ExactSourceRange,
    parameter: &ExactRational,
    source_forward: bool,
) -> Result<(ExactSourceRange, ExactSourceRange)> {
    let width = range.end.try_subtract(&range.start)?;
    let middle = if source_forward {
        range.start.try_add(&width.try_product(parameter)?)?
    } else {
        range.end.try_subtract(&width.try_product(parameter)?)?
    };
    if source_forward {
        Ok((
            ExactSourceRange {
                start: range.start.clone(),
                end: middle.clone(),
            },
            ExactSourceRange {
                start: middle,
                end: range.end.clone(),
            },
        ))
    } else {
        Ok((
            ExactSourceRange {
                start: middle.clone(),
                end: range.end.clone(),
            },
            ExactSourceRange {
                start: range.start.clone(),
                end: middle,
            },
        ))
    }
}

fn push_span_occurrence(
    occurrences: &mut Vec<SpanOccurrence>,
    start: ExactChartPoint,
    end: ExactChartPoint,
    owner: &super::exact::ExactSpanOwner,
    source_range: ExactSourceRange,
    source_forward: bool,
) {
    occurrences.push(SpanOccurrence {
        start,
        end,
        source_edge: owner.source_edge,
        source_range,
        source_forward,
    });
}

#[expect(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "source QPort and ChartAtom IDs index the exact noder columns they were issued from"
)]
fn make_cut_chart(
    frame: GeographicFrame,
    sources: &ExactGeographicChartInventory,
    chart: &NodedExactChart,
    cut: &ExactRational,
) -> Result<CutChart> {
    let period = ExactRational::from_f64(frame.longitude().period());
    let cut_east = cut.try_add(&period)?;
    let port_capacity = chart
        .ports
        .len()
        .checked_add(
            chart
                .atoms
                .len()
                .checked_mul(2)
                .ok_or(ResourceError::SizeOverflow)?,
        )
        .ok_or(ResourceError::SizeOverflow)?;
    let occurrence_capacity = chart
        .atoms
        .len()
        .checked_mul(2)
        .ok_or(ResourceError::SizeOverflow)?;
    let mut candidates = Vec::with_capacity(port_capacity);
    let mut occurrences = Vec::with_capacity(occurrence_capacity);
    for node in &chart.nodes {
        let key = canonical_source_node_key(chart, node.id)?;
        let source_ports = chart.node_ports.get(node.ports.clone()).ok_or_else(|| {
            GeometryErrorKind::Invalid("exact Q node has an invalid port range".into())
        })?;
        for &port_id in source_ports {
            let port = chart.ports.get(port_id.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("exact Q node has an unknown port".into())
            })?;
            candidates.push(PortCandidate {
                point: lifted_point(&port.point, cut, &period)?,
                node: key.clone(),
                cut: false,
            });
        }
    }

    for atom in &chart.atoms {
        let start_port = chart.ports.get(atom.ports[0].0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("an exact chart atom has no start port".into())
        })?;
        let end_port = chart.ports.get(atom.ports[1].0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("an exact chart atom has no end port".into())
        })?;
        let start_side = start_port.point.x.try_cmp(cut)?;
        let end_side = end_port.point.x.try_cmp(cut)?;
        if start_side.is_eq() || end_side.is_eq() {
            return Err(GeometryErrorKind::Invalid(
                "the selected Q cut passes through an exact chart port".into(),
            )
            .into());
        }
        let crosses_cut = matches!(
            (start_side, end_side),
            (Ordering::Less, Ordering::Greater) | (Ordering::Greater, Ordering::Less)
        );
        if crosses_cut {
            let parameter = cut
                .try_subtract(&start_port.point.x)?
                .try_quotient(&end_port.point.x.try_subtract(&start_port.point.x)?)?;
            if !super::exact::in_unit_interval(&parameter)? {
                return Err(GeometryErrorKind::Invalid(
                    "a Q cut parameter lies outside its source span".into(),
                )
                .into());
            }
            let cut_point = super::exact::interpolate_exact_segment(
                &start_port.point,
                &end_port.point,
                &parameter,
            )?;
            let cut_key = cut_node_key(frame, chart, &cut_point.y)?;
            add_cut_candidate(&mut candidates, &cut_point, cut, &period, cut_key)?;
            let lifted_start = lifted_point(&start_port.point, cut, &period)?;
            let lifted_end = lifted_point(&end_port.point, cut, &period)?;
            let (first_start, first_end, second_start, second_end) = if start_side.is_lt() {
                (
                    lifted_start,
                    ExactChartPoint::from_rationals(cut_east.clone(), cut_point.y.clone()),
                    ExactChartPoint::from_rationals(cut.clone(), cut_point.y.clone()),
                    lifted_end,
                )
            } else {
                (
                    lifted_start,
                    ExactChartPoint::from_rationals(cut.clone(), cut_point.y.clone()),
                    ExactChartPoint::from_rationals(cut_east.clone(), cut_point.y.clone()),
                    lifted_end,
                )
            };
            for owner in &atom.owners {
                let source_forward = !owner.reversed;
                let (first_range, second_range) =
                    split_owner_range(&owner.source_range, &parameter, source_forward)?;
                push_span_occurrence(
                    &mut occurrences,
                    first_start.clone(),
                    first_end.clone(),
                    owner,
                    first_range,
                    source_forward,
                );
                push_span_occurrence(
                    &mut occurrences,
                    second_start.clone(),
                    second_end.clone(),
                    owner,
                    second_range,
                    source_forward,
                );
            }
        } else {
            let start = lifted_point(&start_port.point, cut, &period)?;
            let end = lifted_point(&end_port.point, cut, &period)?;
            for owner in &atom.owners {
                push_span_occurrence(
                    &mut occurrences,
                    start.clone(),
                    end.clone(),
                    owner,
                    owner.source_range.clone(),
                    !owner.reversed,
                );
            }
        }
    }

    let (mut ports, port_keys) = unique_port_candidates(candidates)?;
    let mut node_members = Vec::with_capacity(ports.len());
    for (port, key) in ports.iter().zip(port_keys) {
        node_members.push((key, port.id));
    }
    try_sort_by(&mut node_members, |left, right| {
        let node = compare_qnode_keys(&left.0, &right.0)?;
        Ok(if node.is_eq() {
            left.1.cmp(&right.1)
        } else {
            node
        })
    })?;
    let mut nodes = Vec::new();
    let mut node_ports = Vec::with_capacity(ports.len());
    let mut cursor = 0;
    while cursor < node_members.len() {
        let node_id = QNodeId(u32::try_from(nodes.len()).map_err(|_| ResourceError::SizeOverflow)?);
        let range_start = node_ports.len();
        let key = node_members[cursor].0.clone();
        let pole = qnode_pole(chart, &key)?;
        while cursor < node_members.len()
            && compare_qnode_keys(&key, &node_members[cursor].0)?.is_eq()
        {
            let port = node_members[cursor].1;
            ports[port.0 as usize].node = node_id;
            node_ports.push(port);
            cursor += 1;
        }
        nodes.push(QNode {
            id: node_id,
            key,
            pole,
            ports: range_start..node_ports.len(),
        });
    }

    let mut source_edge_ports = Vec::with_capacity(chart.source_edge_ports.len());
    for source in &chart.source_edge_ports {
        source_edge_ports.push([
            find_qport(
                &ports,
                &lifted_point(&chart.ports[source[0].0 as usize].point, cut, &period)?,
            )?,
            find_qport(
                &ports,
                &lifted_point(&chart.ports[source[1].0 as usize].point, cut, &period)?,
            )?,
        ]);
    }
    let mut source_point_ports = Vec::with_capacity(chart.source_point_ports.len());
    for &source in &chart.source_point_ports {
        source_point_ports.push(find_qport(
            &ports,
            &lifted_point(&chart.ports[source.0 as usize].point, cut, &period)?,
        )?);
    }

    let mut grouped = Vec::with_capacity(occurrences.len());
    for occurrence in occurrences {
        let start = find_qport(&ports, &occurrence.start)?;
        let end = find_qport(&ports, &occurrence.end)?;
        if start == end {
            continue;
        }
        let (endpoints, reversed) = if start < end {
            ([start, end], !occurrence.source_forward)
        } else {
            ([end, start], occurrence.source_forward)
        };
        grouped.push((
            endpoints,
            occurrence.source_edge,
            occurrence.source_range,
            reversed,
        ));
    }
    try_sort_by(&mut grouped, |left, right| {
        let endpoints = left.0.cmp(&right.0);
        if !endpoints.is_eq() {
            return Ok(endpoints);
        }
        let source = left.1.cmp(&right.1);
        if !source.is_eq() {
            return Ok(source);
        }
        let start = left.2.start.try_cmp(&right.2.start)?;
        Ok(if start.is_eq() {
            left.2.end.try_cmp(&right.2.end)?
        } else {
            start
        })
    })?;
    let mut spans = Vec::new();
    let mut index = 0;
    while index < grouped.len() {
        let endpoints = grouped[index].0;
        let span_id =
            ChartSpanId(u32::try_from(spans.len()).map_err(|_| ResourceError::SizeOverflow)?);
        let mut owners = Vec::new();
        while index < grouped.len() && grouped[index].0 == endpoints {
            let source_edge = grouped[index].1;
            let reversed = grouped[index].3;
            owners.push(QSpanOwner {
                source_edge,
                source_range: grouped[index].2.clone(),
                reversed,
                ring_transition: source_ring_transition(sources, source_edge, reversed)?,
            });
            index += 1;
        }
        spans.push(ChartSpan {
            id: span_id,
            ports: endpoints,
            nodes: [
                ports[endpoints[0].0 as usize].node,
                ports[endpoints[1].0 as usize].node,
            ],
            owners: owners.into_boxed_slice(),
        });
    }

    Ok(CutChart {
        cut: cut.clone(),
        cut_east,
        ports,
        nodes,
        node_ports,
        spans,
        source_edge_ports,
        source_point_ports,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geographic::AngularFactor;
    use crate::geometry::{
        CoordSeq, CoordinateAxes, ExpansionBudget, LineSeq, MultiMember, MultiPolygon, Polygon,
        Ring, Shape,
    };

    fn degrees() -> GeographicFrame {
        let unit = AngularFactor::try_new("test degrees", std::f64::consts::PI / 180.0).unwrap();
        GeographicFrame::try_new(unit, unit, 0.0).unwrap()
    }

    fn ring(points: &[(f64, f64)]) -> Ring {
        Ring::from_closed(
            CoordSeq::try_from_points(
                &points
                    .iter()
                    .map(|&(x, y)| crate::geometry::Point::new_raw_xy(x, y))
                    .collect::<Vec<_>>(),
            )
            .unwrap(),
        )
        .unwrap()
    }

    fn line(points: &[(f64, f64)]) -> Shape {
        let coordinates = CoordSeq::try_from_points(
            &points
                .iter()
                .map(|&(x, y)| crate::geometry::Point::new_raw_xy(x, y))
                .collect::<Vec<_>>(),
        )
        .unwrap();
        Shape::LineString(LineSeq::try_new(coordinates).unwrap())
    }

    fn q_graph_for_shapes(shapes: &[Shape]) -> (ExactGeographicChartInventory, ExactGeographicQ) {
        let frame = degrees();
        let inputs =
            crate::geographic::GeographicInputs::try_from_shapes(frame, shapes.iter()).unwrap();
        let sources = ExactGeographicChartInventory::collect(&inputs).unwrap();
        let mut budget = ExpansionBudget::new("test", "exact chart");
        let chart = sources.node(&mut budget).unwrap();
        let q = ExactGeographicQ::build(frame, &sources, &chart, false).unwrap();
        (sources, q)
    }

    fn q_graph(shape: &Shape) -> (ExactGeographicChartInventory, ExactGeographicQ) {
        q_graph_for_shapes(std::slice::from_ref(shape))
    }

    fn exact_topology(shape: &Shape) -> ExactGeographicTopology {
        let frame = degrees();
        let view =
            crate::geographic::ScalarGeographicView::try_new(frame, shape).expect("fixture admits");
        ExactGeographicTopology::from_views(frame, std::iter::once(view)).unwrap()
    }

    fn exact_topology_with_seam_cuts(shape: &Shape) -> ExactGeographicTopology {
        let frame = degrees();
        let view =
            crate::geographic::ScalarGeographicView::try_new(frame, shape).expect("fixture admits");
        ExactGeographicTopology::from_views_with_seam_cut_edge_pairs(frame, std::iter::once(view))
            .unwrap()
    }

    fn multipolygon(polygons: Vec<Polygon>) -> Shape {
        Shape::MultiPolygon(
            MultiPolygon::try_new(
                CoordinateAxes::XY,
                polygons.into_iter().map(MultiMember::Value).collect(),
            )
            .unwrap(),
        )
    }

    fn seam_copy_edges(topology: &ExactGeographicTopology) -> Vec<(u32, bool)> {
        topology
            .sources
            .edges
            .iter()
            .filter_map(|edge| {
                let SourceEdgeKind::Ring { .. } = edge.kind else {
                    return None;
                };
                let [start, end] = edge.source;
                (start.x.to_bits() == end.x.to_bits()
                    && start.y.to_bits() != end.y.to_bits()
                    && start.x.abs().to_bits() == 180.0_f64.to_bits())
                .then_some((edge.id.0, start.x.is_sign_positive()))
            })
            .collect()
    }

    #[test]
    fn exact_q_reports_source_contacts_at_the_seam() {
        let crossing = Shape::Polygon(Polygon::new(
            ring(&[
                (170.0, -10.0),
                (-170.0, 10.0),
                (170.0, 10.0),
                (-170.0, -10.0),
                (170.0, -10.0),
            ]),
            Vec::new(),
        ));
        assert!(
            exact_topology(&crossing)
                .has_source_contact_at_seam()
                .unwrap()
        );

        let ordinary_crossing = Shape::Polygon(Polygon::new(
            ring(&[
                (170.0, -10.0),
                (-170.0, -10.0),
                (-170.0, 10.0),
                (170.0, 10.0),
                (170.0, -10.0),
            ]),
            Vec::new(),
        ));
        assert!(
            !exact_topology(&ordinary_crossing)
                .has_source_contact_at_seam()
                .unwrap()
        );

        let simple_line = line(&[(170.0, 0.0), (-170.0, 0.0)]);
        assert!(
            !exact_topology(&simple_line)
                .has_source_contact_at_seam()
                .unwrap()
        );

        let crossing_line = line(&[
            (170.0, -10.0),
            (-170.0, 10.0),
            (170.0, 10.0),
            (-170.0, -10.0),
        ]);
        assert!(
            exact_topology(&crossing_line)
                .has_source_contact_at_seam()
                .unwrap()
        );
    }

    #[test]
    fn exact_q_classifies_only_opposite_side_seam_overlap_as_a_cut() {
        let split_members = multipolygon(vec![
            Polygon::new(
                ring(&[
                    (-180.0, -10.0),
                    (-170.0, -10.0),
                    (-170.0, 10.0),
                    (-180.0, 10.0),
                    (-180.0, -10.0),
                ]),
                Vec::new(),
            ),
            Polygon::new(
                ring(&[
                    (180.0, 10.0),
                    (170.0, 10.0),
                    (170.0, -10.0),
                    (180.0, -10.0),
                    (180.0, 10.0),
                ]),
                Vec::new(),
            ),
        ]);
        let topology = exact_topology_with_seam_cuts(&split_members);
        let cut_pairs = topology.seam_cut_edge_pairs().unwrap();
        let copies = seam_copy_edges(&topology);
        let west = copies.iter().find(|(_, east)| !east).unwrap().0;
        let east = copies.iter().find(|(_, east)| *east).unwrap().0;
        assert!(cut_pairs.contains(west, east));
        assert!(topology.has_source_contact_at_seam().unwrap());

        let partial_overlap = multipolygon(vec![
            Polygon::new(
                ring(&[
                    (-180.0, -10.0),
                    (-170.0, -10.0),
                    (-170.0, 10.0),
                    (-180.0, 10.0),
                    (-180.0, -10.0),
                ]),
                Vec::new(),
            ),
            Polygon::new(
                ring(&[
                    (180.0, 0.0),
                    (170.0, 0.0),
                    (170.0, 15.0),
                    (180.0, 15.0),
                    (180.0, 0.0),
                ]),
                Vec::new(),
            ),
        ]);
        let topology = exact_topology_with_seam_cuts(&partial_overlap);
        let cut_pairs = topology.seam_cut_edge_pairs().unwrap();
        let copies = seam_copy_edges(&topology);
        let west = copies.iter().find(|(_, east)| !east).unwrap().0;
        let east = copies.iter().find(|(_, east)| *east).unwrap().0;
        assert!(cut_pairs.contains(west, east));

        let same_side = multipolygon(vec![
            Polygon::new(
                ring(&[
                    (-180.0, -10.0),
                    (-170.0, -10.0),
                    (-170.0, 10.0),
                    (-180.0, 10.0),
                    (-180.0, -10.0),
                ]),
                Vec::new(),
            ),
            Polygon::new(
                ring(&[
                    (180.0, 0.0),
                    (-170.0, 0.0),
                    (-170.0, 15.0),
                    (180.0, 15.0),
                    (180.0, 0.0),
                ]),
                Vec::new(),
            ),
        ]);
        let topology = exact_topology_with_seam_cuts(&same_side);
        let cut_pairs = topology.seam_cut_edge_pairs().unwrap();
        let copies = seam_copy_edges(&topology);
        let west = copies.iter().find(|(_, east)| !east).unwrap().0;
        let east = copies.iter().find(|(_, east)| *east).unwrap().0;
        assert!(!cut_pairs.contains(west, east));
    }

    fn nested_box_shapes(count: usize) -> Vec<Shape> {
        (1..=count)
            .rev()
            .map(|size| {
                let half = size as f64 / 100.0;
                Shape::Polygon(Polygon::new(
                    ring(&[
                        (-half, -half),
                        (half, -half),
                        (half, half),
                        (-half, half),
                        (-half, -half),
                    ]),
                    vec![],
                ))
            })
            .collect()
    }

    fn source_shell_faces(
        q: &ExactGeographicQ,
        sources: &ExactGeographicChartInventory,
        edge_index: usize,
    ) -> (GlobalFaceId, GlobalFaceId) {
        let edge = sources
            .edges
            .get(edge_index)
            .expect("the fixture source edge exists")
            .id;
        for (span_index, span) in q.spans.iter().enumerate() {
            if let Some(owner) = span.owners.iter().find(|owner| owner.source_edge == edge) {
                let [left, right] = q
                    .span_global_faces
                    .get(span_index)
                    .copied()
                    .flatten()
                    .expect("a shell source span has two incident Q faces");
                return if owner.reversed {
                    (right, left)
                } else {
                    (left, right)
                };
            }
        }
        panic!("the selected source edge has no Q span");
    }

    fn assert_source_transition_cycles(q: &ExactGeographicQ) {
        let Some(seed_face) = q.seed_face else {
            assert!(q.seed_winding.is_empty());
            return;
        };
        let seed = seed_face.0 as usize;
        let mut face_windings = vec![None::<Vec<i64>>; q.global_face_count];
        face_windings[seed] = Some(q.seed_winding.to_vec());
        let mut pending = vec![seed_face];
        while let Some(face) = pending.pop() {
            let face_index = face.0 as usize;
            let current = face_windings[face_index]
                .as_ref()
                .expect("every queued face has a winding")
                .clone();
            for (span_index, sides) in q.span_global_faces.iter().enumerate() {
                let Some([left, right]) = sides else {
                    continue;
                };
                let span = &q.spans[span_index];
                if left == right {
                    assert!(!span_has_ring_transition(span).unwrap());
                    continue;
                }
                let (neighbor, multiplier) = if *left == face {
                    (*right, 1)
                } else if *right == face {
                    (*left, -1)
                } else {
                    continue;
                };
                let mut expected = current.clone();
                for_each_span_ring_transition(&span.owners, &mut |transition| {
                    let winding = expected
                        .get_mut(transition.ring.0 as usize)
                        .expect("source ring IDs index the seed winding");
                    let delta = transition
                        .delta
                        .checked_mul(multiplier)
                        .expect("test transition multiplication cannot overflow");
                    *winding = winding
                        .checked_add(delta)
                        .expect("test winding stays within the source edge count");
                    Ok(())
                })
                .unwrap();
                let neighbor_index = neighbor.0 as usize;
                if let Some(previous) = &face_windings[neighbor_index] {
                    assert_eq!(
                        previous, &expected,
                        "inconsistent source ring transition cycle"
                    );
                } else {
                    face_windings[neighbor_index] = Some(expected);
                    pending.push(neighbor);
                }
            }
        }
        assert!(face_windings.iter().all(Option::is_some));
    }

    fn assert_global_cut_event_wedges(q: &ExactGeographicQ) {
        assert_eq!(
            q.cut_interval_faces.len(),
            q.cut_index
                .events
                .len()
                .checked_add(1)
                .expect("test event count fits a CSR offset")
        );
        for (event, interval_pair) in q
            .cut_index
            .events
            .iter()
            .zip(q.cut_interval_faces.windows(2))
        {
            let [south_interval, north_interval] = interval_pair else {
                unreachable!("windows(2) returns two cut intervals");
            };
            assert_eq!(
                q.global_face_of_local
                    .get(event.south_face.0 as usize)
                    .copied()
                    .expect("cut event south face has a global ID"),
                *south_interval
            );
            assert_eq!(
                q.global_face_of_local
                    .get(event.north_face.0 as usize)
                    .copied()
                    .expect("cut event north face has a global ID"),
                *north_interval
            );
            assert!(
                q.nodes
                    .get(event.node.0 as usize)
                    .expect("cut event has a Q node")
                    .ports
                    .len()
                    >= 2
            );
        }
    }

    fn pole_fan_degree(q: &ExactGeographicQ, pole: Pole) -> usize {
        let node = q
            .nodes
            .iter()
            .find(|node| node.pole == Some(pole))
            .expect("the source ring has a quotient pole node");
        let Some([start, end]) = q.outgoing_offsets.windows(2).nth(node.id.0 as usize) else {
            unreachable!("pole node has an outgoing fan offset pair");
        };
        end.checked_sub(*start)
            .expect("outgoing fan offsets are monotone")
    }

    #[test]
    fn ordinary_source_ring_has_two_exact_quotient_faces() {
        let shell = ring(&[
            (0.0, 0.0),
            (10.0, 0.0),
            (10.0, 10.0),
            (0.0, 10.0),
            (0.0, 0.0),
        ]);
        let shape = Shape::Polygon(Polygon::new(shell, vec![]));
        assert_eq!(q_graph(&shape).1.global_face_count, 2);
    }

    #[test]
    fn seam_crossing_source_ring_has_two_exact_quotient_faces() {
        let shell = ring(&[
            (170.0, -10.0),
            (-170.0, -10.0),
            (-170.0, 10.0),
            (170.0, 10.0),
            (170.0, -10.0),
        ]);
        let shape = Shape::Polygon(Polygon::new(shell, vec![]));
        let (sources, q) = q_graph(&shape);
        let (inside, outside) = source_shell_faces(&q, &sources, 0);
        assert_eq!(q.global_face_count, 2);
        assert_eq!(q.face_rows.rows_for(inside), &[SourceRowId(0)]);
        assert!(q.face_rows.rows_for(outside).is_empty());
        assert_global_cut_event_wedges(&q);
        assert_source_transition_cycles(&q);
    }

    #[test]
    fn horizontal_full_turn_ring_has_two_exact_quotient_faces() {
        let shell = ring(&[
            (0.0, 80.0),
            (90.0, 80.0),
            (180.0, 80.0),
            (-90.0, 80.0),
            (0.0, 80.0),
        ]);
        let shape = Shape::Polygon(Polygon::new(shell, vec![]));
        let (sources, q) = q_graph(&shape);
        let (north_cap, south_side) = source_shell_faces(&q, &sources, 0);
        assert_eq!(q.global_face_count, 2);
        assert_eq!(q.face_rows.rows_for(north_cap), &[SourceRowId(0)]);
        assert!(q.face_rows.rows_for(south_side).is_empty());
    }

    #[test]
    fn south_horizontal_full_turn_ring_keeps_its_summary_selected_cap() {
        let shell = ring(&[
            (0.0, -80.0),
            (90.0, -80.0),
            (180.0, -80.0),
            (-90.0, -80.0),
            (0.0, -80.0),
        ]);
        let shape = Shape::Polygon(Polygon::new(shell, vec![]));
        let (sources, q) = q_graph(&shape);
        let (north_side, south_cap) = source_shell_faces(&q, &sources, 0);
        assert_eq!(q.global_face_count, 2);
        assert!(q.face_rows.rows_for(north_side).is_empty());
        assert_eq!(q.face_rows.rows_for(south_cap), &[SourceRowId(0)]);
    }

    #[test]
    fn varying_latitude_full_turn_ring_changes_occupancy_at_source_edges() {
        let shell = ring(&[
            (0.0, 70.0),
            (90.0, 75.0),
            (180.0, 80.0),
            (-90.0, 75.0),
            (0.0, 70.0),
        ]);
        let shape = Shape::Polygon(Polygon::new(shell, vec![]));
        let (sources, q) = q_graph(&shape);
        let (north_side, south_side) = source_shell_faces(&q, &sources, 0);
        assert_eq!(q.global_face_count, 2);
        assert_eq!(q.face_rows.rows_for(north_side), &[SourceRowId(0)]);
        assert!(q.face_rows.rows_for(south_side).is_empty());
        assert_global_cut_event_wedges(&q);
        assert_source_transition_cycles(&q);
    }

    #[test]
    fn reversed_varying_latitude_full_turn_preserves_signed_winding_fill() {
        let shell = ring(&[
            (0.0, 70.0),
            (-90.0, 75.0),
            (180.0, 80.0),
            (90.0, 75.0),
            (0.0, 70.0),
        ]);
        let shape = Shape::Polygon(Polygon::new(shell, vec![]));
        let (sources, q) = q_graph(&shape);
        let (outside, inside) = source_shell_faces(&q, &sources, 0);
        assert_eq!(sources.rings.first().unwrap().summary.seam_transits, -1);
        assert_eq!(q.face_rows.rows_for(inside), &[SourceRowId(0)]);
        assert!(q.face_rows.rows_for(outside).is_empty());
        assert_source_transition_cycles(&q);
    }

    #[test]
    fn varying_full_turn_crossing_the_equator_uses_source_winding() {
        let shell = ring(&[
            (0.0, -10.0),
            (90.0, 10.0),
            (180.0, 20.0),
            (-90.0, 10.0),
            (0.0, -10.0),
        ]);
        let shape = Shape::Polygon(Polygon::new(shell, vec![]));
        let (sources, q) = q_graph(&shape);
        let (left, right) = source_shell_faces(&q, &sources, 0);
        assert_eq!(sources.rings.first().unwrap().summary.seam_transits, 1);
        assert_eq!(q.global_face_count, 2);
        assert_eq!(q.face_rows.rows_for(left), &[SourceRowId(0)]);
        assert!(q.face_rows.rows_for(right).is_empty());
        assert_global_cut_event_wedges(&q);
        assert_source_transition_cycles(&q);
    }

    #[test]
    fn reversed_varying_full_turn_crossing_the_equator_keeps_north_pole_occupied() {
        let polygon = Shape::Polygon(Polygon::new(
            ring(&[
                (0.0, -10.0),
                (-90.0, 10.0),
                (180.0, 20.0),
                (90.0, 10.0),
                (0.0, -10.0),
            ]),
            vec![],
        ));
        let collection = Shape::geometry_collection(vec![
            polygon,
            Shape::Point(crate::geometry::Point::new_raw_xy(37.0, 90.0)),
        ])
        .unwrap();
        let (sources, q) = q_graph(&collection);
        assert_eq!(sources.rings.first().unwrap().summary.seam_transits, -1);
        let north = q
            .source_point_faces
            .first()
            .copied()
            .flatten()
            .expect("the north pole point has an exact cut-interval host");
        assert_eq!(q.face_rows.rows_for(north), &[SourceRowId(0)]);
        assert_source_transition_cycles(&q);
    }

    #[test]
    fn multi_turn_ring_keeps_its_signed_source_transition_multiplicity() {
        let shell = ring(&[
            (0.0, 10.0),
            (120.0, 10.0),
            (240.0, 10.0),
            (360.0, 10.0),
            (480.0, 10.0),
            (600.0, 10.0),
            (720.0, 10.0),
            (0.0, 10.0),
        ]);
        let shape = Shape::Polygon(Polygon::new(shell, vec![]));
        let (sources, q) = q_graph(&shape);
        let (north_cap, south_side) = source_shell_faces(&q, &sources, 0);
        assert_eq!(sources.rings.first().unwrap().summary.seam_transits, 2);
        assert!(q.spans.iter().any(|span| {
            let transitions = span
                .owners
                .iter()
                .filter_map(|owner| owner.ring_transition)
                .filter(|transition| transition.ring == SourceRingId(0))
                .collect::<Vec<_>>();
            transitions.len() >= 2 && transitions.iter().all(|transition| transition.delta == -1)
        }));
        assert_eq!(q.face_rows.rows_for(north_cap), &[SourceRowId(0)]);
        assert!(q.face_rows.rows_for(south_side).is_empty());
        assert_source_transition_cycles(&q);
    }

    #[test]
    fn negative_multi_turn_horizontal_ring_keeps_its_summary_selected_cap() {
        let shell = ring(&[
            (0.0, 10.0),
            (-120.0, 10.0),
            (-240.0, 10.0),
            (-360.0, 10.0),
            (-480.0, 10.0),
            (-600.0, 10.0),
            (-720.0, 10.0),
            (0.0, 10.0),
        ]);
        let shape = Shape::Polygon(Polygon::new(shell, vec![]));
        let (sources, q) = q_graph(&shape);
        let (south_side, north_cap) = source_shell_faces(&q, &sources, 0);
        assert_eq!(sources.rings.first().unwrap().summary.seam_transits, -2);
        assert!(q.spans.iter().any(|span| {
            let transitions = span
                .owners
                .iter()
                .filter_map(|owner| owner.ring_transition)
                .filter(|transition| transition.ring == SourceRingId(0))
                .collect::<Vec<_>>();
            transitions.len() >= 2 && transitions.iter().all(|transition| transition.delta == 1)
        }));
        assert!(q.face_rows.rows_for(south_side).is_empty());
        assert_eq!(q.face_rows.rows_for(north_cap), &[SourceRowId(0)]);
        assert_source_transition_cycles(&q);
    }

    #[test]
    fn exact_full_longitude_axis_band_occupies_only_its_source_band() {
        let shell = ring(&[
            (-180.0, -10.0),
            (-90.0, -10.0),
            (0.0, -10.0),
            (90.0, -10.0),
            (180.0, -10.0),
            (180.0, 10.0),
            (90.0, 10.0),
            (0.0, 10.0),
            (-90.0, 10.0),
            (-180.0, 10.0),
            (-180.0, -10.0),
        ]);
        let shape = Shape::Polygon(Polygon::new(shell, vec![]));
        let (sources, q) = q_graph(&shape);
        let (band_side, exterior_side) = source_shell_faces(&q, &sources, 0);
        assert!(sources.rings.first().unwrap().summary.exact_axis_band);
        assert_eq!(q.global_face_count, 3);
        assert_eq!(q.face_rows.rows_for(band_side), &[SourceRowId(0)]);
        assert!(q.face_rows.rows_for(exterior_side).is_empty());
        assert_global_cut_event_wedges(&q);
        assert_source_transition_cycles(&q);
    }

    #[test]
    fn seam_alias_rectangle_is_collapsed_line_support() {
        let shape = Shape::Polygon(Polygon::new(
            ring(&[
                (-180.0, -85.0),
                (180.0, -85.0),
                (180.0, 85.0),
                (-180.0, 85.0),
                (-180.0, -85.0),
            ]),
            vec![],
        ));
        let topology = exact_topology(&shape);
        assert_eq!(topology.sources.rings[0].kind, RingKind::Collapsed);
        assert_eq!(
            topology.sources.rings[0].summary.domain,
            crate::geographic::RingDomain::SeamOnly
        );
        let central = topology
            .locate_point(crate::geometry::Point::new_raw_xy(0.0, 0.0))
            .unwrap();
        assert_eq!(central.geometry, Loc::Exterior);
        assert_eq!(central.area, Loc::Exterior);
        let seam = topology
            .locate_point(crate::geometry::Point::new_raw_xy(180.0, 0.0))
            .unwrap();
        assert_eq!(seam.geometry, Loc::Interior);
        assert_eq!(seam.area, Loc::Exterior);
    }

    #[test]
    fn pole_to_pole_full_longitude_axis_band_occupies_the_sphere() {
        let world = Shape::Polygon(Polygon::new(
            ring(&[
                (-180.0, -90.0),
                (-90.0, -90.0),
                (0.0, -90.0),
                (90.0, -90.0),
                (180.0, -90.0),
                (180.0, 90.0),
                (90.0, 90.0),
                (0.0, 90.0),
                (-90.0, 90.0),
                (-180.0, 90.0),
                (-180.0, -90.0),
            ]),
            vec![],
        ));
        let topology = exact_topology(&world);
        assert!(topology.sources.rings[0].summary.exact_axis_band);
        for point in [
            crate::geometry::Point::new_raw_xy(0.0, 0.0),
            crate::geometry::Point::new_raw_xy(179.0, 0.0),
            crate::geometry::Point::new_raw_xy(-179.0, 0.0),
        ] {
            assert_eq!(
                topology.locate_point(point).unwrap(),
                ExactGeographicPointLocation {
                    geometry: Loc::Interior,
                    area: Loc::Interior,
                }
            );
        }
    }

    #[test]
    fn north_pole_ring_vertex_keeps_its_exact_cut_fan_wedge() {
        let shell = ring(&[
            (-120.0, 80.0),
            (0.0, 80.0),
            (120.0, 80.0),
            (0.0, 90.0),
            (-120.0, 80.0),
        ]);
        let shape = Shape::Polygon(Polygon::new(shell, vec![]));
        let (sources, q) = q_graph(&shape);
        let (inside, outside) = source_shell_faces(&q, &sources, 0);
        assert!(pole_fan_degree(&q, Pole::North) >= 2);
        assert_eq!(q.global_face_count, 2);
        assert_eq!(q.face_rows.rows_for(inside), &[SourceRowId(0)]);
        assert!(q.face_rows.rows_for(outside).is_empty());
        assert!(
            q.face_rows
                .rows_for(*q.cut_interval_faces.last().unwrap())
                .is_empty()
        );
        assert_global_cut_event_wedges(&q);
        assert_source_transition_cycles(&q);
    }

    #[test]
    fn admitted_exterior_pole_ulp_is_normalized_before_chart_noding() {
        let pole = 90.0_f64;
        let exterior_ulp = pole.next_up();
        let cap = Shape::Polygon(Polygon::new(
            ring(&[
                (-10.0, 80.0),
                (10.0, 80.0),
                (10.0, pole),
                (-10.0, pole),
                (-10.0, 80.0),
            ]),
            vec![],
        ));
        let admitted_cap = Shape::Polygon(Polygon::new(
            ring(&[
                (-10.0, 80.0),
                (10.0, 80.0),
                (10.0, exterior_ulp),
                (-10.0, exterior_ulp),
                (-10.0, 80.0),
            ]),
            vec![],
        ));
        let (sources, q) = q_graph_for_shapes(&[cap, admitted_cap]);

        assert!(sources.edges.iter().any(|edge| {
            edge.source
                .iter()
                .any(|point| point.y.to_bits() == exterior_ulp.to_bits())
        }));
        assert!(q.ports.iter().all(|port| {
            port.point
                .y
                .try_cmp(&ExactRational::from_f64(pole))
                .unwrap()
                .is_le()
        }));
    }

    #[test]
    fn south_pole_ring_vertex_keeps_its_exact_cut_fan_wedge() {
        let shell = ring(&[
            (-120.0, -80.0),
            (0.0, -80.0),
            (120.0, -80.0),
            (0.0, -90.0),
            (-120.0, -80.0),
        ]);
        let shape = Shape::Polygon(Polygon::new(shell, vec![]));
        let (sources, q) = q_graph(&shape);
        let (left, right) = source_shell_faces(&q, &sources, 0);
        assert!(pole_fan_degree(&q, Pole::South) >= 2);
        assert!(q.face_rows.rows_for(left).is_empty());
        assert_eq!(q.face_rows.rows_for(right), &[SourceRowId(0)]);
        assert!(
            q.face_rows
                .rows_for(*q.cut_interval_faces.first().unwrap())
                .is_empty()
        );
        assert_global_cut_event_wedges(&q);
        assert_source_transition_cycles(&q);
    }

    #[test]
    fn isolated_source_points_resolve_hosts_in_the_global_face_table() {
        let polygon = Shape::Polygon(Polygon::new(
            ring(&[
                (0.0, 0.0),
                (10.0, 0.0),
                (10.0, 10.0),
                (0.0, 10.0),
                (0.0, 0.0),
            ]),
            vec![],
        ));
        let collection = Shape::geometry_collection(vec![
            polygon,
            Shape::Point(crate::geometry::Point::new_raw_xy(5.0, 5.0)),
            Shape::Point(crate::geometry::Point::new_raw_xy(20.0, 20.0)),
        ])
        .unwrap();
        let (_, q) = q_graph(&collection);
        let inside = q
            .source_point_faces
            .first()
            .copied()
            .flatten()
            .expect("the interior point has an exact host face");
        let outside = q
            .source_point_faces
            .get(1)
            .copied()
            .flatten()
            .expect("the exterior point has an exact host face");
        assert_eq!(q.face_rows.rows_for(inside), &[SourceRowId(0)]);
        assert!(q.face_rows.rows_for(outside).is_empty());
        assert_ne!(inside, outside);
    }

    #[test]
    fn isolated_collapsed_ring_node_resolves_a_global_host_without_point_sources() {
        let outer = Shape::Polygon(Polygon::new(
            ring(&[
                (0.0, 0.0),
                (10.0, 0.0),
                (10.0, 10.0),
                (0.0, 10.0),
                (0.0, 0.0),
            ]),
            vec![],
        ));
        let collapsed = Shape::Polygon(Polygon::new(
            ring(&[(5.0, 5.0), (5.0, 5.0), (5.0, 5.0), (5.0, 5.0)]),
            vec![],
        ));
        let (sources, q) = q_graph_for_shapes(&[outer, collapsed]);
        assert!(sources.points.is_empty());
        let edge = sources
            .edges
            .iter()
            .find(|edge| {
                matches!(edge.kind, SourceEdgeKind::Ring {
                    polygon,
                    ..
                } if polygon.0 == 1)
            })
            .expect("the collapsed polygon has source ring edges");
        let port_id = q
            .source_edge_ports
            .get(edge.id.0 as usize)
            .and_then(|ports| ports.first().copied())
            .expect("the collapsed ring edge has source chart ports");
        let node = q
            .ports
            .get(port_id.0 as usize)
            .expect("the collapsed ring edge has a Q port")
            .node;
        let host = q
            .source_node_faces
            .get(node.0 as usize)
            .copied()
            .flatten()
            .expect("the isolated collapsed ring node has an exact host");
        assert_eq!(q.face_rows.rows_for(host), &[SourceRowId(0)]);
    }

    #[test]
    fn isolated_north_pole_point_uses_its_quotient_cut_host_face() {
        let cap = Shape::Polygon(Polygon::new(
            ring(&[
                (0.0, 80.0),
                (90.0, 80.0),
                (180.0, 80.0),
                (-90.0, 80.0),
                (0.0, 80.0),
            ]),
            vec![],
        ));
        let collection = Shape::geometry_collection(vec![
            cap,
            Shape::Point(crate::geometry::Point::new_raw_xy(37.0, 90.0)),
        ])
        .unwrap();
        let (_, q) = q_graph(&collection);
        let north = q
            .source_point_faces
            .first()
            .copied()
            .flatten()
            .expect("the isolated pole point has an exact cut-interval host");
        assert_eq!(q.face_rows.rows_for(north), &[SourceRowId(0)]);
    }

    #[test]
    fn exact_point_query_locates_areal_faces_and_boundaries() {
        let source = Shape::Polygon(Polygon::new(
            ring(&[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)]),
            vec![],
        ));
        let topology = exact_topology(&source);
        for (point, expected) in [
            (Point::new_raw_xy(2.0, 2.0), ExactGeographicPointLocation {
                geometry: Loc::Interior,
                area: Loc::Interior,
            }),
            (Point::new_raw_xy(0.0, 2.0), ExactGeographicPointLocation {
                geometry: Loc::Boundary,
                area: Loc::Boundary,
            }),
            (Point::new_raw_xy(8.0, 8.0), ExactGeographicPointLocation {
                geometry: Loc::Exterior,
                area: Loc::Exterior,
            }),
        ] {
            assert_eq!(topology.locate_point(point).unwrap(), expected);
        }
        assert!(matches!(
            topology.locate_point(Point::new_raw_xy(0.0, 91.0)),
            Err(error)
                if matches!(
                    error.kind(),
                    crate::error::ErrorKind::Geometry(GeometryErrorKind::Invalid(_))
                )
        ));
    }

    #[test]
    fn exact_point_query_includes_line_and_isolated_point_support() {
        let line = Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new_raw_xy(0.0, 0.0),
                    Point::new_raw_xy(10.0, 0.0),
                ])
                .unwrap(),
            )
            .unwrap(),
        );
        let line_topology = exact_topology(&line);
        assert_eq!(
            line_topology
                .locate_point(Point::new_raw_xy(5.0, 0.0))
                .unwrap(),
            ExactGeographicPointLocation {
                geometry: Loc::Interior,
                area: Loc::Exterior,
            }
        );
        assert_eq!(
            line_topology
                .locate_point(Point::new_raw_xy(0.0, 0.0))
                .unwrap()
                .geometry,
            Loc::Boundary
        );

        let point = Shape::Point(Point::new_raw_xy(12.0, 3.0));
        let point_topology = exact_topology(&point);
        assert_eq!(
            point_topology
                .locate_point(Point::new_raw_xy(12.0, 3.0))
                .unwrap(),
            ExactGeographicPointLocation {
                geometry: Loc::Interior,
                area: Loc::Exterior,
            }
        );
        assert_eq!(
            point_topology
                .locate_point(Point::new_raw_xy(12.0, 4.0))
                .unwrap()
                .geometry,
            Loc::Exterior
        );
    }

    #[test]
    fn exact_point_query_uses_mod2_line_boundaries() {
        let source = Shape::geometry_collection(vec![
            Shape::LineString(
                LineSeq::try_new(
                    CoordSeq::try_from_points(&[
                        Point::new_raw_xy(0.0, 0.0),
                        Point::new_raw_xy(1.0, 0.0),
                    ])
                    .unwrap(),
                )
                .unwrap(),
            ),
            Shape::LineString(
                LineSeq::try_new(
                    CoordSeq::try_from_points(&[
                        Point::new_raw_xy(1.0, 0.0),
                        Point::new_raw_xy(2.0, 0.0),
                    ])
                    .unwrap(),
                )
                .unwrap(),
            ),
        ])
        .unwrap();
        assert_eq!(
            exact_topology(&source)
                .locate_point(Point::new_raw_xy(1.0, 0.0))
                .unwrap(),
            ExactGeographicPointLocation {
                geometry: Loc::Interior,
                area: Loc::Exterior,
            }
        );
    }

    #[test]
    fn exact_point_query_maps_antimeridian_aliases_to_the_same_q_face() {
        let source = Shape::Polygon(Polygon::new(
            ring(&[
                (170.0, -10.0),
                (-170.0, -10.0),
                (-170.0, 10.0),
                (170.0, 10.0),
                (170.0, -10.0),
            ]),
            vec![],
        ));
        let topology = exact_topology(&source);
        for longitude in [-180.0, 180.0] {
            assert_eq!(
                topology
                    .locate_point(Point::new_raw_xy(longitude, 0.0))
                    .unwrap()
                    .geometry,
                Loc::Interior
            );
        }
        assert_eq!(
            topology
                .locate_point(Point::new_raw_xy(0.0, 0.0))
                .unwrap()
                .geometry,
            Loc::Exterior
        );
    }

    #[test]
    fn exact_point_query_respects_holes_and_full_turn_fill() {
        let donut = Shape::Polygon(Polygon::new(
            ring(&[
                (0.0, 0.0),
                (10.0, 0.0),
                (10.0, 10.0),
                (0.0, 10.0),
                (0.0, 0.0),
            ]),
            vec![ring(&[
                (3.0, 3.0),
                (7.0, 3.0),
                (7.0, 7.0),
                (3.0, 7.0),
                (3.0, 3.0),
            ])],
        ));
        let donut_topology = exact_topology(&donut);
        assert_eq!(
            donut_topology
                .locate_point(Point::new_raw_xy(2.0, 5.0))
                .unwrap()
                .geometry,
            Loc::Interior
        );
        assert_eq!(
            donut_topology
                .locate_point(Point::new_raw_xy(5.0, 5.0))
                .unwrap()
                .geometry,
            Loc::Exterior
        );

        let cap = Shape::Polygon(Polygon::new(
            ring(&[
                (0.0, 80.0),
                (90.0, 80.0),
                (180.0, 80.0),
                (-90.0, 80.0),
                (0.0, 80.0),
            ]),
            vec![],
        ));
        let cap_topology = exact_topology(&cap);
        assert_eq!(
            cap_topology
                .locate_point(Point::new_raw_xy(0.0, 85.0))
                .unwrap()
                .geometry,
            Loc::Interior
        );
        assert_eq!(
            cap_topology
                .locate_point(Point::new_raw_xy(0.0, 0.0))
                .unwrap()
                .geometry,
            Loc::Exterior
        );
        assert_eq!(
            cap_topology
                .locate_point(Point::new_raw_xy(123.0, 90.0))
                .unwrap()
                .geometry,
            Loc::Interior
        );
    }

    #[test]
    fn polar_hole_intersection_with_its_pole_cap_is_empty() {
        let polar_hole = Shape::Polygon(Polygon::new(
            ring(&[
                (0.0, 70.0),
                (60.0, 70.0),
                (120.0, 70.0),
                (180.0, 70.0),
                (-120.0, 70.0),
                (-60.0, 70.0),
                (0.0, 70.0),
            ]),
            vec![ring(&[
                (0.0, 80.0),
                (60.0, 80.0),
                (120.0, 80.0),
                (180.0, 80.0),
                (-120.0, 80.0),
                (-60.0, 80.0),
                (0.0, 80.0),
            ])],
        ));
        let pole_cap = Shape::Polygon(Polygon::new(
            ring(&[(120.0, 90.0), (110.0, 85.0), (130.0, 85.0), (120.0, 90.0)]),
            Vec::new(),
        ));

        let intersection = crate::geometry::overlay_all(
            &[&polar_hole, &pole_cap],
            crate::geometry::TopologyDomain::Geographic(GeographicFrame::crs84()),
            crate::geometry::OverlayOp::Intersection,
            crate::geometry::Strictness::Strict,
        )
        .unwrap();

        assert!(intersection.is_empty(), "{intersection:?}");
    }

    #[test]
    fn exact_queries_keep_only_collapsed_shell_support_outside_areal_holes() {
        let source = Shape::Polygon(Polygon::new(
            ring(&[(0.0, 0.0), (10.0, 0.0), (5.0, 0.0), (0.0, 0.0)]),
            vec![ring(&[
                (2.0, -1.0),
                (8.0, -1.0),
                (8.0, 1.0),
                (2.0, 1.0),
                (2.0, -1.0),
            ])],
        ));
        let topology = exact_topology(&source);
        assert_eq!(
            topology.locate_point(Point::new_raw_xy(1.0, 0.0)).unwrap(),
            ExactGeographicPointLocation {
                geometry: Loc::Interior,
                area: Loc::Exterior,
            }
        );
        assert_eq!(
            topology
                .locate_point(Point::new_raw_xy(5.0, 0.0))
                .unwrap()
                .geometry,
            Loc::Exterior
        );
        assert_eq!(
            topology
                .classify_rectangle(Bounds::new_raw(0.5, -0.5, 1.5, 0.5))
                .unwrap(),
            Some(ExactGeographicRectangleLocation::Boundary)
        );
        assert_eq!(
            topology
                .classify_rectangle(Bounds::new_raw(4.0, -0.5, 6.0, 0.5))
                .unwrap(),
            Some(ExactGeographicRectangleLocation::Outside)
        );
    }

    #[test]
    fn exact_rectangle_query_classifies_cover_contact_and_disjoint_regions() {
        let source = Shape::Polygon(Polygon::new(
            ring(&[
                (0.0, 0.0),
                (10.0, 0.0),
                (10.0, 10.0),
                (0.0, 10.0),
                (0.0, 0.0),
            ]),
            vec![],
        ));
        let topology = exact_topology(&source);
        for (bounds, expected) in [
            (
                Bounds::new_raw(2.0, 2.0, 8.0, 8.0),
                ExactGeographicRectangleLocation::Interior,
            ),
            (
                Bounds::new_raw(0.0, 2.0, 8.0, 8.0),
                ExactGeographicRectangleLocation::Interior,
            ),
            (
                Bounds::new_raw(8.0, 2.0, 12.0, 8.0),
                ExactGeographicRectangleLocation::Boundary,
            ),
            (
                Bounds::new_raw(12.0, 12.0, 14.0, 14.0),
                ExactGeographicRectangleLocation::Outside,
            ),
        ] {
            assert_eq!(topology.classify_rectangle(bounds).unwrap(), Some(expected));
        }
        assert_eq!(
            topology
                .classify_rectangle(Bounds::new_raw(-180.0, 1.0, -179.0, 2.0))
                .unwrap(),
            None
        );

        let donut = Shape::Polygon(Polygon::new(
            ring(&[
                (0.0, 0.0),
                (10.0, 0.0),
                (10.0, 10.0),
                (0.0, 10.0),
                (0.0, 0.0),
            ]),
            vec![ring(&[
                (3.0, 3.0),
                (7.0, 3.0),
                (7.0, 7.0),
                (3.0, 7.0),
                (3.0, 3.0),
            ])],
        ));
        let donut_topology = exact_topology(&donut);
        assert_eq!(
            donut_topology
                .classify_rectangle(Bounds::new_raw(4.0, 4.0, 6.0, 6.0))
                .unwrap(),
            Some(ExactGeographicRectangleLocation::Outside)
        );
        assert_eq!(
            donut_topology
                .classify_rectangle(Bounds::new_raw(2.0, 2.0, 5.0, 5.0))
                .unwrap(),
            Some(ExactGeographicRectangleLocation::Boundary)
        );

        let line = Shape::LineString(
            LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    Point::new_raw_xy(0.0, 5.0),
                    Point::new_raw_xy(10.0, 5.0),
                ])
                .unwrap(),
            )
            .unwrap(),
        );
        assert_eq!(
            exact_topology(&line)
                .classify_rectangle(Bounds::new_raw(2.0, 4.0, 8.0, 6.0))
                .unwrap(),
            Some(ExactGeographicRectangleLocation::Boundary)
        );

        let point = Shape::Point(Point::new_raw_xy(5.0, 5.0));
        assert_eq!(
            exact_topology(&point)
                .classify_rectangle(Bounds::new_raw(4.0, 4.0, 6.0, 6.0))
                .unwrap(),
            Some(ExactGeographicRectangleLocation::Boundary)
        );
        assert_eq!(
            topology
                .classify_rectangle(Bounds::new_raw(0.0, 85.0, 1.0, 90.0))
                .unwrap(),
            None
        );
    }

    #[test]
    fn exact_rectangle_query_splits_at_the_selected_q_cut() {
        let source = Shape::Polygon(Polygon::new(
            ring(&[
                (170.0, -10.0),
                (-170.0, -10.0),
                (-170.0, 10.0),
                (170.0, 10.0),
                (170.0, -10.0),
            ]),
            vec![],
        ));
        let topology = exact_topology(&source);
        assert_eq!(
            topology
                .classify_rectangle(Bounds::new_raw(-1.0, -1.0, 1.0, 1.0))
                .unwrap(),
            Some(ExactGeographicRectangleLocation::Outside)
        );

        let cap = Shape::Polygon(Polygon::new(
            ring(&[
                (0.0, 80.0),
                (90.0, 80.0),
                (180.0, 80.0),
                (-90.0, 80.0),
                (0.0, 80.0),
            ]),
            vec![],
        ));
        assert_eq!(
            exact_topology(&cap)
                .classify_rectangle(Bounds::new_raw(-140.0, 81.0, -130.0, 82.0))
                .unwrap(),
            Some(ExactGeographicRectangleLocation::Interior)
        );
    }

    #[test]
    fn exact_point_query_visits_only_nearby_indexed_candidates() {
        let dense = Shape::geometry_collection(nested_box_shapes(64)).unwrap();
        let dense_topology = exact_topology(&dense);
        let dense_span_count = dense_topology.q.spans.len();
        dense_topology.q.spatial_index.reset_candidate_visits();
        assert_eq!(
            dense_topology
                .locate_point(Point::new_raw_xy(0.0, 80.0))
                .unwrap()
                .geometry,
            Loc::Exterior
        );
        assert!(
            dense_topology.q.spatial_index.candidate_visits() < dense_span_count / 8,
            "dense point query visited {} of {dense_span_count} span candidates",
            dense_topology.q.spatial_index.candidate_visits()
        );

        let sparse = Shape::geometry_collection(
            (0..256)
                .map(|index| {
                    Shape::Point(Point::new_raw_xy(
                        -120.0 + f64::from(index) * 0.9,
                        40.0 + f64::from(index % 12),
                    ))
                })
                .collect(),
        )
        .unwrap();
        let sparse_topology = exact_topology(&sparse);
        sparse_topology.q.spatial_index.reset_candidate_visits();
        assert_eq!(
            sparse_topology
                .locate_point(Point::new_raw_xy(0.0, 0.0))
                .unwrap()
                .geometry,
            Loc::Exterior
        );
        assert_eq!(sparse_topology.q.spatial_index.candidate_visits(), 0);
    }

    #[test]
    fn line_only_q_hosts_an_isolated_point_in_its_single_global_face() {
        let line = Shape::LineString(
            crate::geometry::LineSeq::try_new(
                CoordSeq::try_from_points(&[
                    crate::geometry::Point::new_raw_xy(0.0, 0.0),
                    crate::geometry::Point::new_raw_xy(10.0, 0.0),
                ])
                .unwrap(),
            )
            .unwrap(),
        );
        let collection = Shape::geometry_collection(vec![
            line,
            Shape::Point(crate::geometry::Point::new_raw_xy(20.0, 5.0)),
        ])
        .unwrap();
        let (_, q) = q_graph(&collection);
        assert_eq!(q.global_face_count, 1);
        assert_eq!(q.face_rows.count(GlobalFaceId(0)), 0);
        assert!(q.face_rows.intervals.is_empty());
        assert_eq!(q.source_point_faces.as_ref(), &[Some(GlobalFaceId(0))]);
    }

    #[test]
    fn nested_disconnected_shells_link_the_inner_exterior_to_the_host_face() {
        let outer = Shape::Polygon(Polygon::new(
            ring(&[
                (0.0, 0.0),
                (10.0, 0.0),
                (10.0, 10.0),
                (0.0, 10.0),
                (0.0, 0.0),
            ]),
            vec![],
        ));
        let inner = Shape::Polygon(Polygon::new(
            ring(&[(3.0, 3.0), (7.0, 3.0), (7.0, 7.0), (3.0, 7.0), (3.0, 3.0)]),
            vec![],
        ));
        let collection = Shape::geometry_collection(vec![outer, inner]).unwrap();
        let (sources, q) = q_graph(&collection);
        let (outer_inside, outer_outside) = source_shell_faces(&q, &sources, 0);
        let (inner_inside, inner_outside) = source_shell_faces(&q, &sources, 4);
        assert_eq!(q.global_face_count, 3);
        assert_eq!(inner_outside, outer_inside);
        assert_ne!(inner_inside, inner_outside);
        assert_ne!(outer_outside, outer_inside);
        assert_eq!(q.face_rows.rows_for(outer_inside), &[SourceRowId(0)]);
        assert_eq!(q.face_rows.rows_for(inner_inside), &[SourceRowId(0)]);
        assert!(q.face_rows.rows_for(outer_outside).is_empty());
    }

    #[test]
    fn disjoint_shell_exteriors_share_one_global_face() {
        let rectangle = |west: f64| {
            Shape::Polygon(Polygon::new(
                ring(&[
                    (west, 0.0),
                    (west + 4.0, 0.0),
                    (west + 4.0, 4.0),
                    (west, 4.0),
                    (west, 0.0),
                ]),
                vec![],
            ))
        };
        let collection = Shape::geometry_collection(vec![rectangle(0.0), rectangle(20.0)]).unwrap();
        let (sources, q) = q_graph(&collection);
        let (first_inside, first_outside) = source_shell_faces(&q, &sources, 0);
        let (second_inside, second_outside) = source_shell_faces(&q, &sources, 4);
        assert_eq!(q.global_face_count, 3);
        assert_eq!(first_outside, second_outside);
        assert_ne!(first_inside, first_outside);
        assert_ne!(second_inside, second_outside);
        assert_ne!(first_inside, second_inside);
        assert_eq!(q.face_rows.rows_for(first_inside), &[SourceRowId(0)]);
        assert_eq!(q.face_rows.rows_for(second_inside), &[SourceRowId(0)]);
        assert!(q.face_rows.rows_for(first_outside).is_empty());
    }

    #[test]
    fn nested_shell_hole_and_disconnected_island_publish_exact_row_occupancy() {
        let annulus = Shape::Polygon(Polygon::new(
            ring(&[
                (0.0, 0.0),
                (10.0, 0.0),
                (10.0, 10.0),
                (0.0, 10.0),
                (0.0, 0.0),
            ]),
            vec![ring(&[
                (3.0, 3.0),
                (3.0, 7.0),
                (7.0, 7.0),
                (7.0, 3.0),
                (3.0, 3.0),
            ])],
        ));
        let island = Shape::Polygon(Polygon::new(
            ring(&[(4.0, 4.0), (6.0, 4.0), (6.0, 6.0), (4.0, 6.0), (4.0, 4.0)]),
            vec![],
        ));
        let (sources, q) = q_graph_for_shapes(&[annulus, island]);
        let (shell_left, shell_right) = source_shell_faces(&q, &sources, 0);
        let (hole_left, hole_right) = source_shell_faces(&q, &sources, 4);
        let (island_left, island_right) = source_shell_faces(&q, &sources, 8);
        assert_eq!(q.global_face_count, 4);
        assert_eq!(q.face_rows.rows_for(shell_left), &[SourceRowId(0)]);
        assert!(q.face_rows.rows_for(shell_right).is_empty());
        assert_eq!(q.face_rows.rows_for(hole_left), &[SourceRowId(0)]);
        assert!(q.face_rows.rows_for(hole_right).is_empty());
        assert_eq!(q.face_rows.rows_for(island_left), &[SourceRowId(1)]);
        assert!(q.face_rows.rows_for(island_right).is_empty());
        assert_eq!(hole_right, island_right);
        assert_source_transition_cycles(&q);
    }

    #[test]
    fn nested_component_host_sweep_and_row_work_scale_with_the_graph() {
        let small_shapes = nested_box_shapes(64);
        let (_, small) = q_graph_for_shapes(&small_shapes);
        let large_shapes = nested_box_shapes(256);
        let (_, large) = q_graph_for_shapes(&large_shapes);
        assert_eq!(small.global_face_count, 65);
        assert_eq!(large.global_face_count, 257);
        assert_eq!(large.spans.len(), 1_024);
        assert_eq!(small.face_rows.intervals.len(), 64);
        assert_eq!(large.face_rows.intervals.len(), 256);
        for face in 0..large.global_face_count {
            let face = GlobalFaceId(u32::try_from(face).unwrap());
            let depth = large.face_rows.count(face);
            for row in 0..256 {
                assert_eq!(
                    large
                        .face_rows
                        .contains(face, SourceRowId(u32::try_from(row).unwrap())),
                    row < depth,
                );
            }
        }
        assert!(large.host_status_max_height <= 22);
        assert!(large.host_status_comparisons < 256 * 1_024);
        assert!(large.host_status_comparisons < small.host_status_comparisons * 6);
    }
}
