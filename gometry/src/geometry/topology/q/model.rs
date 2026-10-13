use super::*;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(in crate::geometry::topology) struct QPortId(pub u32);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(in crate::geometry::topology) struct QNodeId(pub u32);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(in crate::geometry::topology) struct ChartSpanId(pub u32);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(in crate::geometry::topology) struct QDartId(pub u32);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(in crate::geometry::topology) struct QLocalFaceId(pub u32);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(in crate::geometry::topology) struct QComponentId(pub u32);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(in crate::geometry::topology) struct GlobalFaceId(pub u32);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::geometry) enum PoleSector {
    Fan {
        pole: ChartPole,
        clockwise: QDartId,
        counterclockwise: QDartId,
        face: GlobalFaceId,
    },
    Isolated {
        pole: ChartPole,
        node: QNodeId,
        face: GlobalFaceId,
    },
    Terminal {
        pole: ChartPole,
        face: GlobalFaceId,
    },
}

impl PoleSector {
    pub(in crate::geometry) const fn face(self) -> GlobalFaceId {
        match self {
            Self::Fan { face, .. } | Self::Isolated { face, .. } | Self::Terminal { face, .. } => {
                face
            },
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(in crate::geometry::topology) struct CutIntervalId(pub u32);

#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::geometry::topology) enum QNodeKey {
    Source(QuotientNodeId),
    Cut(ExactRational),
}

#[derive(Clone, Debug)]
pub(in crate::geometry::topology) struct QPort {
    pub id: QPortId,
    pub point: ExactChartPoint,
    pub node: QNodeId,
    pub(in crate::geometry::topology) cut: bool,
}

#[derive(Clone, Debug)]
pub(in crate::geometry::topology) struct QNode {
    pub id: QNodeId,
    pub(in crate::geometry::topology) key: QNodeKey,
    pub(in crate::geometry::topology) pole: Option<ChartPole>,
    pub ports: Range<usize>,
}

#[derive(Clone, Debug)]
pub(in crate::geometry::topology) struct QSpanOwner {
    pub source_edge: SourceEdgeId,
    pub source_range: ExactSourceRange,
    /// The source edge runs opposite `ports[0] -> ports[1]`.
    pub reversed: bool,
    /// Periods from this Q span to the source ring's continuous lift.
    pub lift: i64,
    pub(in crate::geometry::topology::q) ring_transition: Option<RingTransition>,
}

#[derive(Clone, Copy, Debug)]
pub(in crate::geometry::topology::q) struct RingTransition {
    pub(in crate::geometry::topology::q) ring: SourceRingId,
    /// Crossing from the span's left face to its right face changes winding
    /// by this amount.
    pub(in crate::geometry::topology::q) delta: i64,
}

#[derive(Clone, Debug)]
pub(in crate::geometry::topology) struct ChartSpan {
    pub id: ChartSpanId,
    pub ports: [QPortId; 2],
    pub nodes: [QNodeId; 2],
    pub owners: Box<[QSpanOwner]>,
}

#[derive(Clone, Copy)]
pub(in crate::geometry::topology::q) struct SeamBoundaryOwner {
    pub(in crate::geometry::topology::q) edge: SourceEdgeId,
    pub(in crate::geometry::topology::q) polygon: SourcePolygonId,
    pub(in crate::geometry::topology::q) ring: SourceRingId,
    pub(in crate::geometry::topology::q) hole: bool,
}

#[derive(Clone, Copy, Debug)]
pub(in crate::geometry::topology::q) struct SeamOwnerSide {
    pub(in crate::geometry::topology::q) nodes: [QNodeId; 2],
    pub(in crate::geometry::topology::q) edge: SourceEdgeId,
    pub(in crate::geometry::topology::q) polygon: SourcePolygonId,
    pub(in crate::geometry::topology::q) left_interior: bool,
    pub(in crate::geometry::topology::q) right_interior: bool,
}

#[derive(Clone, Copy, Debug)]
pub(in crate::geometry::topology) struct QDart {
    pub(in crate::geometry::topology) span: ChartSpanId,
    pub(in crate::geometry::topology) from_port: QPortId,
    pub(in crate::geometry::topology) to_port: QPortId,
    pub(in crate::geometry::topology) from_node: QNodeId,
    pub(in crate::geometry::topology) to_node: QNodeId,
}

#[derive(Clone, Copy, Debug)]
pub(in crate::geometry::topology) struct QLocalFace {
    pub(in crate::geometry::topology) component: QComponentId,
}

#[derive(Clone, Debug)]
pub(in crate::geometry::topology::q) struct PortCandidate {
    pub(in crate::geometry::topology::q) point: ExactChartPoint,
    pub(in crate::geometry::topology::q) node: QNodeKey,
    pub(in crate::geometry::topology::q) cut: bool,
}

#[derive(Clone, Debug)]
pub(in crate::geometry::topology::q) struct SpanOccurrence {
    pub(in crate::geometry::topology::q) start: ExactChartPoint,
    pub(in crate::geometry::topology::q) end: ExactChartPoint,
    pub(in crate::geometry::topology::q) source_edge: SourceEdgeId,
    pub(in crate::geometry::topology::q) source_range: ExactSourceRange,
    pub(in crate::geometry::topology::q) source_forward: bool,
    pub(in crate::geometry::topology::q) lift: i64,
}

#[derive(Clone, Debug)]
pub(in crate::geometry::topology::q) struct HostGerm {
    pub(in crate::geometry::topology::q) point: ExactChartPoint,
    pub(in crate::geometry::topology::q) dx: ExactRational,
    pub(in crate::geometry::topology::q) dy: ExactRational,
}

pub(in crate::geometry::topology) struct CutChart {
    pub(in crate::geometry::topology) cut: ExactRational,
    pub(in crate::geometry::topology) cut_east: ExactRational,
    pub(in crate::geometry::topology) ports: Vec<QPort>,
    pub(in crate::geometry::topology) nodes: Vec<QNode>,
    pub(in crate::geometry::topology) node_ports: Vec<QPortId>,
    pub(in crate::geometry::topology) spans: Vec<ChartSpan>,
    pub(in crate::geometry::topology) source_edge_ports: Vec<[QPortId; 2]>,
    pub(in crate::geometry::topology) source_point_ports: Vec<QPortId>,
}

#[derive(Clone, Copy)]
pub(in crate::geometry::topology) struct QFaceChart<'a> {
    pub(in crate::geometry::topology) cut: &'a ExactRational,
    pub(in crate::geometry::topology) cut_east: &'a ExactRational,
    pub(in crate::geometry::topology) ports: &'a [QPort],
    pub(in crate::geometry::topology) nodes: &'a [QNode],
}

impl CutChart {
    pub(in crate::geometry::topology) fn face_chart(&self) -> QFaceChart<'_> {
        QFaceChart {
            cut: &self.cut,
            cut_east: &self.cut_east,
            ports: &self.ports,
            nodes: &self.nodes,
        }
    }
}

#[derive(Clone, Debug)]
pub(in crate::geometry::topology::q) struct QCutEvent {
    pub(in crate::geometry::topology::q) y: ExactRational,
    pub(in crate::geometry::topology::q) node: QNodeId,
    pub(in crate::geometry::topology::q) component: QComponentId,
    pub(in crate::geometry::topology::q) south_face: QLocalFaceId,
    pub(in crate::geometry::topology::q) north_face: QLocalFaceId,
}

#[derive(Clone, Debug)]
pub(in crate::geometry::topology::q) struct QCutInterval {
    pub(in crate::geometry::topology::q) id: CutIntervalId,
    pub(in crate::geometry::topology::q) low: ExactRational,
    pub(in crate::geometry::topology::q) high: ExactRational,
    pub(in crate::geometry::topology::q) low_face: Option<QLocalFaceId>,
    pub(in crate::geometry::topology::q) low_component: Option<QComponentId>,
    pub(in crate::geometry::topology::q) high_face: Option<QLocalFaceId>,
    pub(in crate::geometry::topology::q) high_component: Option<QComponentId>,
}

#[derive(Clone, Debug)]
pub(in crate::geometry::topology::q) struct QComponentCutRuns {
    pub(in crate::geometry::topology::q) event_range: Range<usize>,
    pub(in crate::geometry::topology::q) face_range: Range<usize>,
}

#[derive(Clone, Debug)]
pub(in crate::geometry::topology::q) struct HostRoot {
    pub(in crate::geometry::topology::q) component: QComponentId,
    pub(in crate::geometry::topology::q) face: QLocalFaceId,
    pub(in crate::geometry::topology::q) port: Option<QPortId>,
    pub(in crate::geometry::topology::q) point: ExactChartPoint,
}

/// Cut-root face and host queries sample the same southern side of the event.
pub(in crate::geometry::topology::q) const ROOT_CUT_FACE_SIDE: i8 = -1;

#[derive(Clone, Copy, Debug)]
pub(in crate::geometry::topology::q) enum SweepEventKind {
    End(usize),
    Start(usize),
    Query(usize),
    Seed,
}

#[derive(Clone, Debug)]
pub(in crate::geometry::topology::q) struct SweepEvent {
    pub(in crate::geometry::topology::q) y: ExactRational,
    pub(in crate::geometry::topology::q) kind: SweepEventKind,
}

#[derive(Clone, Debug)]
pub(in crate::geometry::topology::q) struct HostQuery {
    pub(in crate::geometry::topology::q) component: QComponentId,
    pub(in crate::geometry::topology::q) target: HostQueryTarget,
    pub(in crate::geometry::topology::q) germ: HostGerm,
}

#[derive(Clone, Copy, Debug)]
pub(in crate::geometry::topology::q) enum HostQueryTarget {
    Component(QComponentId),
    IsolatedNode(QNodeId),
}

pub(in crate::geometry::topology::q) struct GlobalFaceTopology {
    pub(in crate::geometry::topology::q) local_to_global: Vec<GlobalFaceId>,
    pub(in crate::geometry::topology::q) span_global_faces: Vec<Option<[GlobalFaceId; 2]>>,
    pub(in crate::geometry::topology::q) component_hosts: Vec<Option<GlobalFaceId>>,
    pub(in crate::geometry::topology::q) cut_interval_faces: Vec<GlobalFaceId>,
    pub(in crate::geometry::topology::q) source_node_faces: Vec<Option<GlobalFaceId>>,
    pub(in crate::geometry::topology::q) source_point_faces: Vec<Option<GlobalFaceId>>,
    pub(in crate::geometry::topology::q) seed_face: Option<GlobalFaceId>,
    pub(in crate::geometry::topology::q) seed_winding: Vec<i64>,
    pub(in crate::geometry::topology::q) count: usize,
    #[cfg(test)]
    pub(in crate::geometry::topology::q) host_status_comparisons: usize,
    #[cfg(test)]
    pub(in crate::geometry::topology::q) host_status_max_height: usize,
}

pub(in crate::geometry::topology) struct GlobalFaceStructure {
    pub(in crate::geometry::topology) local_to_global: Vec<GlobalFaceId>,
    pub(in crate::geometry::topology) span_global_faces: Vec<Option<[GlobalFaceId; 2]>>,
    pub(in crate::geometry::topology) component_hosts: Vec<Option<GlobalFaceId>>,
    pub(in crate::geometry::topology) cut_interval_faces: Vec<GlobalFaceId>,
    pub(in crate::geometry::topology) source_node_faces: Vec<Option<GlobalFaceId>>,
    pub(in crate::geometry::topology) count: usize,
    pub(in crate::geometry::topology::q) seed: Option<SeedFaceGerm>,
    pub(in crate::geometry::topology::q) seed_ray_spans: Vec<SeedRaySpan>,
    #[cfg(test)]
    pub(in crate::geometry::topology::q) host_status_comparisons: usize,
    #[cfg(test)]
    pub(in crate::geometry::topology::q) host_status_max_height: usize,
}

#[derive(Clone, Copy)]
pub(in crate::geometry::topology::q) struct SeedRaySpan {
    pub(in crate::geometry::topology::q) span: usize,
    pub(in crate::geometry::topology::q) multiplier: i64,
}

#[derive(Debug)]
pub(in crate::geometry::topology) struct QCutIndex {
    pub(in crate::geometry::topology::q) events: Box<[QCutEvent]>,
    pub(in crate::geometry::topology::q) intervals: Box<[QCutInterval]>,
    pub(in crate::geometry::topology::q) local_event_indices: Box<[usize]>,
    pub(in crate::geometry::topology::q) component_runs: Box<[QComponentCutRuns]>,
    pub(in crate::geometry::topology::q) run_faces: Box<[QLocalFaceId]>,
    pub(in crate::geometry::topology::q) root_ports: Box<[Option<QPortId>]>,
}
pub(in crate::geometry::topology) struct QuotientFaces {
    pub(in crate::geometry::topology::q) cut: ExactRational,
    pub(in crate::geometry::topology::q) cut_east: ExactRational,
    pub(in crate::geometry::topology::q) ports: Vec<QPort>,
    pub(in crate::geometry::topology::q) nodes: Vec<QNode>,
    pub(in crate::geometry::topology::q) node_ports: Vec<QPortId>,
    pub(in crate::geometry::topology::q) spans: Vec<ChartSpan>,
    pub(in crate::geometry::topology::q) darts: Vec<QDart>,
    pub(in crate::geometry::topology::q) span_darts: Vec<Option<QDartId>>,
    pub(in crate::geometry::topology) outgoing: Vec<QDartId>,
    pub(in crate::geometry::topology) outgoing_offsets: Vec<usize>,
    pub(in crate::geometry::topology::q) dart_positions: Vec<usize>,
    pub(in crate::geometry::topology::q) dart_faces: Vec<QLocalFaceId>,
    pub(in crate::geometry::topology::q) local_faces: Vec<QLocalFace>,
    pub(in crate::geometry::topology) node_components: Vec<QComponentId>,
    pub(in crate::geometry::topology::q) span_local_faces: Vec<Option<[QLocalFaceId; 2]>>,
    pub(in crate::geometry::topology::q) source_edge_ports: Vec<[QPortId; 2]>,
    pub(in crate::geometry::topology::q) source_point_ports: Vec<QPortId>,
    pub(in crate::geometry::topology::q) cut_index: QCutIndex,
}

impl QuotientFaces {
    pub(in crate::geometry::topology::q) fn face_chart(&self) -> QFaceChart<'_> {
        QFaceChart {
            cut: &self.cut,
            cut_east: &self.cut_east,
            ports: &self.ports,
            nodes: &self.nodes,
        }
    }
}

/// One exact chart quotient graph. Local faces are sewn globally after the
/// rotation system is complete.
#[derive(Debug)]
pub(in crate::geometry::topology) struct ExactGeographicQ {
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
    pub face_validity: Box<[QFaceValidity]>,
    pub(in crate::geometry::topology::q) seam_owner_sides: Box<[SeamOwnerSide]>,
    pub(in crate::geometry::topology::q) span_line_rows: GlobalSpanRowSupport,
    pub(in crate::geometry::topology::q) span_collapsed_rows: GlobalSpanRowSupport,
    pub(in crate::geometry::topology::q) node_point_rows: GlobalNodeRowSupport,
    pub(in crate::geometry::topology::q) node_line_boundary_rows: GlobalNodeRowSupport,
    pub(in crate::geometry::topology::q) spatial_index: QSpatialIndex,
    #[cfg(test)]
    pub(in crate::geometry::topology::q) host_status_comparisons: usize,
    #[cfg(test)]
    pub(in crate::geometry::topology::q) host_status_max_height: usize,
    #[cfg(test)]
    pub(in crate::geometry::topology::q) seed_face: Option<GlobalFaceId>,
    #[cfg(test)]
    pub(in crate::geometry::topology::q) seed_winding: Box<[i64]>,
}

/// One admitted source inventory and the exact chart/Q derived from it.
#[derive(Debug)]
pub(crate) struct ExactGeographicTopology {
    pub(in crate::geometry::topology) sources: ExactGeographicChartInventory,
    pub(in crate::geometry::topology) chart: NodedExactChart,
    pub(in crate::geometry::topology) q: ExactGeographicQ,
    pub(in crate::geometry) publication_frame: crate::Frame,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct ExactGeographicPointLocation {
    /// Location in the complete geometry, including line and point support.
    pub(crate) geometry: Loc,
    /// Location in the filled polygonal support alone.
    pub(crate) area: Loc,
}

pub(in crate::geometry::topology) struct QRowLocations<'a> {
    pub(in crate::geometry::topology::q) topology: &'a ExactGeographicTopology,
    pub(in crate::geometry::topology::q) row: SourceRowId,
}

pub(in crate::geometry::topology) enum CanonicalQNodeKey<'a> {
    Chart(&'a ExactQuotientKey),
    Cut {
        longitude: &'a ExactRational,
        latitude: &'a ExactRational,
    },
}

pub(in crate::geometry::topology::q) fn point_heap_bytes(point: &ExactChartPoint) -> usize {
    point.heap_bytes()
}

pub(in crate::geometry::topology::q) fn rational_heap_bytes(value: &ExactRational) -> usize {
    super::super::exact::rational_heap_bytes(value)
}

#[expect(
    clippy::arithmetic_side_effects,
    reason = "both exact parameters are part of one owned source-range record"
)]
pub(in crate::geometry::topology::q) fn source_range_heap_bytes(range: &ExactSourceRange) -> usize {
    rational_heap_bytes(&range.start) + rational_heap_bytes(&range.end)
}
