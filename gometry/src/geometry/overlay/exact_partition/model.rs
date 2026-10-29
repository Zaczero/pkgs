use std::cell::Cell;
use std::ops::Range;

use super::{
    AreaLocationCells, ExactDyadic, Point, PointArena, PointKey, RingKind, RootMetricState,
    Segment, SegmentProvenance, XY, embedding,
};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(in crate::geometry) struct NodeId(pub u32);

/// What a node is: a stored endpoint, or the crossing of two unique segments'
/// carriers, whose exact homogeneous point is built only for a comparison its
/// rounded coordinates cannot decide.
#[derive(Debug)]
pub(in crate::geometry) enum NodeRecipe {
    Stored,
    Grid([i64; 2]),
    LargeGrid(Box<[ExactDyadic; 2]>),
    Constructed {
        exact: std::cell::OnceCell<Box<[ExactDyadic; 3]>>,
    },
    Crossing {
        segments: [u32; 2],
        exact: std::cell::OnceCell<Box<[ExactDyadic; 3]>>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::geometry) struct ExactSpan {
    pub start: NodeId,
    pub end: NodeId,
    pub unique: u32,
}

#[derive(Debug)]
pub(in crate::geometry) struct ExactMetricNode {
    pub(super) points: crate::geometry::ExactLocationPoints,
    pub(super) node: u32,
}

impl ExactMetricNode {
    #[expect(
        clippy::indexing_slicing,
        reason = "the node ordinal is admitted from the same frozen exact point table this metric handle owns"
    )]
    pub(in crate::geometry) fn homogeneous(&self) -> &[ExactDyadic; 3] {
        &self.points[self.node as usize]
    }
}

#[derive(Debug)]
pub(in crate::geometry) struct ExactMetricSpan {
    pub start: ExactMetricNode,
    pub end: ExactMetricNode,
    pub start_parameter: [ExactDyadic; 2],
    pub end_parameter: [ExactDyadic; 2],
    pub source: Segment,
}

#[derive(Debug)]
pub(in crate::geometry) struct ExactMetricSupport {
    /// Exact node coordinates shared with `area_location_cells`.
    pub exact_points: crate::geometry::ExactLocationPoints,
    pub spans: Vec<ExactMetricSpan>,
    pub points: Vec<ExactMetricNode>,
    pub faces: Vec<Vec<Vec<ExactMetricNode>>>,
    /// Present only when the selected support has positive area.
    pub area_location_cells: Option<AreaLocationCells>,
    /// Partition-span incidence selected by occupied/unoccupied global-face
    /// adjacency. The order is the same as `spans`; lower-dimensional support
    /// hidden by a positive-area face is deliberately absent. Ordinary metric
    /// support leaves this unbuilt.
    pub boundary_spans: Option<Vec<bool>>,
    /// Point incidence parallel to `points`. Positive-area support does not
    /// retain isolated points as a metric boundary target. Ordinary metric
    /// support leaves this unbuilt.
    pub boundary_points: Option<Vec<bool>>,
}

/// One maximal walk over exact spans, cut at an incidence junction or at a
/// change in the caller's ownership class. `nodes` contains the span
/// endpoints in walk order; a cyclic walk repeats its first node at the end.
#[derive(Debug)]
pub(in crate::geometry) struct ExactChainView {
    pub nodes: Vec<NodeId>,
    pub spans: Vec<u32>,
    pub shared: bool,
}

/// Exact incidence views used by coverage consumers. `chain_of_span` maps
/// every partition span to the one chain that owns its simplification
/// decision.
#[derive(Debug)]
pub(in crate::geometry) struct ExactChainViews {
    pub chains: Vec<ExactChainView>,
    pub chain_of_span: Vec<u32>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::geometry) struct ExactLineSource {
    pub operand: u32,
    pub reversed: bool,
}

#[derive(Debug)]
pub(in crate::geometry) struct ExactPartition<'a> {
    pub(super) unique: &'a [Segment],
    /// Every node's point, indexed by `NodeId`. A stored node keeps its first
    /// source spelling: `PointKey` canonicalizes signed zero for identity, and
    /// emission must not do the same to an endpoint already in the input.
    pub(super) points: PointArena<'a>,
    pub spans: Vec<ExactSpan>,
    pub provenance: SegmentProvenance,
    pub isolated: Vec<NodeId>,
    /// Node ownership in explicit input-point occurrence order, including duplicates.
    pub point_nodes: Vec<NodeId>,
    pub point_sources: Vec<(NodeId, u32)>,
    /// Source-ring winding at an interior face adjacent to the rectangle
    /// boundary. Clipped partitions omit outside arcs, so face zero need not
    /// have one source-ring winding; full-ring partitions leave this absent.
    pub(super) winding_seed: Option<(u32, Vec<i64>)>,
    pub(super) ordinate_bindings: Option<OrdinateBindings>,
    pub(super) kept_nodes: KeptNodes,
    pub(super) embedding_domain: embedding::EmbeddingDomain,
}

/// Source carriers through each node, retained only for Z/M interpolation.
#[derive(Debug)]
pub(super) struct OrdinateBindings {
    pub(super) starts: Vec<u32>,
    pub(super) unique: Vec<u32>,
}

/// Which nodes emission keeps as vertices, beyond those selection requires.
#[derive(Debug)]
pub(in crate::geometry) enum KeptNodes {
    /// Only the nodes selection requires.
    Selected,
    /// Every node: coverage, polygonization, and geographic charts preserve source vertices.
    All,
    /// Every node no polygon ring passes through, indexed by node: linework
    /// keeps its vertices whatever else shares the arrangement, while ring
    /// vertices on a straight run dissolve.
    OffRings(Vec<bool>),
}

/// Fallible Cartesian source inventory shared by overlay, relation, clipping,
/// repair, and raw linework consumers.
///
/// The inventory is deliberately source-oriented: independently stored
/// duplicate/reversed segment occurrences remain in `source_*` until the exact
/// partition has attributed them to one unique edge. Ring identity and operand
/// identity therefore stay available for winding and set-operation selection.
pub(in crate::geometry) struct ExactSourceInventory {
    pub(super) source_segments: Vec<Segment>,
    pub(super) unique: Vec<Segment>,
    pub(super) provenance: SegmentProvenance,
    pub(super) source_operands: Vec<u32>,
    pub(super) source_rings: Vec<Option<u32>>,
    pub(super) ring_kinds: Vec<RingKind>,
    pub(super) ring_polygons: Vec<u32>,
    pub(super) ring_holes: Vec<bool>,
    pub(super) polygon_count: usize,
    pub(super) polygon_operands: Vec<u32>,
    pub(super) polygon_ring_ranges: Vec<Range<usize>>,
    pub(super) source_points: Option<Vec<SourceArcRecord>>,
    pub(super) polygon_rings: Vec<Vec<Range<usize>>>,
    pub(super) ring_count: usize,
    pub(super) isolated: Vec<XY>,
    pub(super) isolated_points: Vec<Point>,
    pub(super) isolated_operands: Vec<u32>,
    pub(super) line_boundaries: Vec<(u32, PointKey)>,
}

pub(super) struct SourceArcRecord {
    pub(super) points: [Point; 2],
    pub(super) metric: Cell<RootMetricState>,
}

impl SourceArcRecord {
    pub(super) fn new(points: [Point; 2]) -> Self {
        Self {
            points,
            metric: Cell::new(RootMetricState::default()),
        }
    }
}

/// Exact source components before partition deduplication.
///
/// Occurrence-preserving ingress keeps every component occurrence, including
/// members reached through shared collection owners; set ingress can fold
/// repeated shared owners before they enter this buffer.
pub(in crate::geometry) struct StagedExactSources {
    pub(super) segments: Vec<Segment>,
    pub(super) source_points: Option<Vec<SourceArcRecord>>,
    /// Source endpoint Z/M values are unnecessary for XY-only consumers.
    pub(super) xy_only: bool,
    pub(super) source_operands: Vec<u32>,
    pub(super) source_rings: Vec<Option<u32>>,
    pub(super) ring_kinds: Vec<RingKind>,
    pub(super) ring_polygons: Vec<u32>,
    pub(super) ring_holes: Vec<bool>,
    pub(super) polygon_count: usize,
    pub(super) polygon_operands: Vec<u32>,
    pub(super) polygon_ring_ranges: Vec<Range<usize>>,
    pub(super) polygon_rings: Vec<Vec<Range<usize>>>,
    pub(super) ring_count: usize,
    pub(super) isolated: Vec<XY>,
    pub(super) isolated_points: Vec<Point>,
    pub(super) isolated_operands: Vec<u32>,
    pub(super) line_boundaries: Vec<(u32, PointKey)>,
    pub(super) relation_components: Option<Box<RelationComponents>>,
}

#[derive(Default)]
pub(super) struct RelationComponents {
    pub(super) components: Vec<StagedSourceComponent>,
    pub(super) source_order: Vec<StagedRelationItem>,
}

/// A source line or ring and the contiguous pool segment range it contributed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(in crate::geometry) struct StagedSourceComponent {
    pub(in crate::geometry) operand: u32,
    pub(in crate::geometry) segments: Range<usize>,
    pub(in crate::geometry) ring: Option<u32>,
    pub(in crate::geometry) ring_kind: Option<RingKind>,
    pub(in crate::geometry) has_nonzero_segment: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::geometry) enum StagedRelationItem {
    Component(usize),
    Isolated(usize),
}
