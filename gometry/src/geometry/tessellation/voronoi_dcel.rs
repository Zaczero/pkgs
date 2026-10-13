//! Exact labelled Voronoi subdivision and its sole binary64 projection.

#![expect(
    clippy::float_cmp,
    clippy::inconsistent_struct_constructor,
    clippy::items_after_statements,
    clippy::needless_range_loop,
    clippy::unwrap_used,
    reason = "the DCEL mirrors the numbered construction and its proof tests keep mutations visibly local"
)]

use std::cell::RefCell;
use std::cmp::Ordering;
use std::collections::{BTreeSet, VecDeque};
use std::ops::ControlFlow;

use crate::collections::try_hash_map_with_capacity;
use crate::error::Result;
use crate::geographic::PointSelectionChart;
use crate::geographic::retained_exact::sites::retained_site_longitude;
use crate::geometry::area::{RingWinding, open_xy_cycle_winding};
use crate::geometry::arrangement::{IndexedTopology, IndexedTopologyScratch};
use crate::geometry::embedding_search::{
    ClassAssignment, EvaluationBudget, GlobalCheck, Problem, ProximityChord, Search, SearchAccount,
    build_proximity_classes, exact_chord_radius, possible_swept_bounds, proximity_limit,
};
use crate::geometry::graph::{RayAxis, RayPoints, visit_component_faces};
use crate::geometry::numerics::product::ExactSign;
use crate::geometry::overlay::{
    NodeId, OverlayNodeReceipt, OverlaySourceEdgeUse,
    binary_cartesian_difference_without_partition_budget,
    binary_cartesian_intersection_with_edge_roles, emitted_pair_preserves_incidence,
    segments_within_chebyshev,
};
use crate::geometry::segment_index::YStabbingIndex;
use crate::geometry::segments::ordering::{try_dedup_by, try_sort_by};
use crate::geometry::tessellation::{
    CertifiedDelaunay, CertifiedPrimalEdge, DelaunayComplex, EMPTY, Site, VoronoiAssignmentReceipt,
    VoronoiCellReceipt, VoronoiChartCutSide, VoronoiClipAtomReceipt, VoronoiClipSegmentReceipt,
    VoronoiClipSourceRoot, VoronoiDartReceipt, VoronoiDiagramReceipt, VoronoiDirectedEdgeUse,
    VoronoiEdgeOrigin, VoronoiEdgeReceipt, VoronoiEdgeSourceUse, VoronoiFrameSide, VoronoiProducer,
    VoronoiRingEdgeRole, VoronoiSeparatorReceipt, VoronoiSiteReceipt, VoronoiSourceSiteDecision,
    VoronoiSourceSiteReceipt, VoronoiVertexOrigin, VoronoiVertexReceipt, delaunay_complex, exact,
};
use crate::geometry::types::ensure_coordseq_vertex_capacity;
use crate::geometry::{
    Bounds, CoordSeq, CsrOffsetColumn, ExpansionBudget, GeometryErrorKind, Point, PointKey,
    Polygon, PolygonPointIndex, Ring, Segment, SegmentParameter, Shape, Strictness,
    VoronoiBoundary, XY, coverage_is_valid, for_each_candidate_pair,
    for_each_overlapping_bounds_cross_pair, polygon_is_valid, single_chain,
};
#[cfg(test)]
use crate::geometry::{
    GENERATED_ITEM_LIMIT, LineSeq, tessellation::certified_delaunay, tessellation::shape,
};

macro_rules! dense_id {
    ($name:ident) => {
        #[repr(transparent)]
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        struct $name(u32);
    };
}

dense_id!(SiteId);
dense_id!(VertexId);
dense_id!(EdgeId);
dense_id!(BoundaryFaceId);
dense_id!(EmbeddedVertexId);
dense_id!(ClipSegmentId);

#[repr(u8)]
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
enum FrameSide {
    Bottom,
    Right,
    Top,
    Left,
}

impl FrameSide {
    const ALL: [Self; 4] = [Self::Bottom, Self::Right, Self::Top, Self::Left];

    const fn clip_segment(self) -> ClipSegmentId {
        ClipSegmentId(self as u32)
    }

    const fn next(self) -> Self {
        match self {
            Self::Bottom => Self::Right,
            Self::Right => Self::Top,
            Self::Top => Self::Left,
            Self::Left => Self::Bottom,
        }
    }

    const fn previous(self) -> Self {
        match self {
            Self::Bottom => Self::Left,
            Self::Right => Self::Bottom,
            Self::Top => Self::Right,
            Self::Left => Self::Top,
        }
    }

    const fn start(self, rectangle: &[exact::ExactPoint; 4]) -> &exact::ExactPoint {
        let [bottom_left, bottom_right, top_right, top_left] = rectangle;
        match self {
            Self::Bottom => bottom_left,
            Self::Right => bottom_right,
            Self::Top => top_right,
            Self::Left => top_left,
        }
    }

    const fn end(self, rectangle: &[exact::ExactPoint; 4]) -> &exact::ExactPoint {
        self.next().start(rectangle)
    }

    const fn start_xy(self, rectangle: &[XY; 4]) -> XY {
        let [bottom_left, bottom_right, top_right, top_left] = rectangle;
        match self {
            Self::Bottom => *bottom_left,
            Self::Right => *bottom_right,
            Self::Top => *top_right,
            Self::Left => *top_left,
        }
    }

    const fn end_xy(self, rectangle: &[XY; 4]) -> XY {
        self.next().start_xy(rectangle)
    }

    fn endpoints_xy(self, rectangle: &[XY; 4]) -> [XY; 2] {
        [self.start_xy(rectangle), self.end_xy(rectangle)]
    }

    #[expect(
        clippy::arithmetic_side_effects,
        reason = "the loop walks the four closed FrameSide variants and returns before exceeding that finite cycle"
    )]
    fn steps_counterclockwise_to(self, end: Self) -> usize {
        let mut side = self;
        let mut steps = 0;
        while side != end {
            side = side.next();
            steps += 1;
        }
        steps
    }

    #[expect(
        clippy::arithmetic_side_effects,
        reason = "the loop walks the four closed FrameSide variants and returns before exceeding that finite cycle"
    )]
    fn steps_clockwise_to(self, end: Self) -> usize {
        let mut side = self;
        let mut steps = 0;
        while side != end {
            side = side.previous();
            steps += 1;
        }
        steps
    }
}

mod admission;
mod assembly;
mod clip;
mod collinear;
mod dual;
mod embedding;
mod receipt;
mod scaffold;
mod subdivision;
use admission::{
    BoxIndex, admit_embedded_clip, clip_certificate_data, clip_source_trace_error,
    exact_chart_point, exact_cycle_error, exact_segment_parameter, normalize_clip_with_chart,
    rounded_segment_covered_by_clip, validate_assembled_exact_cycles, validate_owner_cycle_facts,
};
#[cfg(test)]
use admission::{
    admit_box_ordinals, first_owner_cycle_failure, for_each_assembled_pair, for_each_touch_fan,
    normalize_clip, rounded_box, validate_exact_cycle_nesting, validate_exact_cycles,
    validate_exact_rectangle,
};
#[cfg(test)]
use assembly::assemble_separator_group;
use assembly::{ExactCellPart, ExactEdgeChain, ExactSimpleRing, assemble_cells, assemble_edges};
#[cfg(test)]
use clip::in_rectangle;
use clip::{
    axis_aligned_rectangle, between, clip_hull_ray_to_rectangle, clip_rectangle,
    clip_segment_to_rectangle, exact_bisector_frame_intersections, orient_separator,
    sort_unique_along,
};
#[cfg(test)]
use collinear::build_collinear_edges;
pub(super) use collinear::{build_collinear_edges_with_chart, build_collinear_polygons_with_chart};
use dual::dual_build_product;
#[cfg(test)]
use dual::{
    certify_convex_cell, dual_build, dual_build_rings, dual_embed, dual_vertex_point,
    reverse_tagged,
};
#[cfg(test)]
use embedding::fans::rounded_fan_angle_cmp;
use embedding::fans::stored_xy_cycle_matches_orientation;
#[cfg(test)]
use embedding::graph::VoronoiConflict;
#[cfg(test)]
use embedding::graph::nearest_site;
use embedding::graph::{certified_site_adjacency, nearest_site_walk};
use embedding::publish::embed;
use receipt::{
    chart_vertex_origins, clip_segment_receipts, collinear_polygon_receipt, embedded_exact_ring,
    exact_diagram_receipt, exact_xy_coordinates, receipt_frame_side, receipt_ring_uses,
    site_receipts,
};
pub(super) use receipt::{
    empty_polygon_outputs, lone_site_empty_edges, lone_site_empty_polygon_outputs,
    lone_site_polygon_outputs,
};
use scaffold::{
    build_scaffold, emit_clipped_separators, fixed_coordinates, lex_position,
    node_scaffold_and_separators,
};
use subdivision::{
    ExactSubdivision, ExactVertex, build_exact_subdivision, graft_components, selected_cell_label,
    uf_union, walk_label,
};
#[cfg(test)]
use subdivision::{RegionLabel, crossed_label, validate_exact_subdivision};

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum OwnerTransition {
    Preserve,
    Boundary {
        left: Option<SiteId>,
        right: Option<SiteId>,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum PrimitiveOwner {
    Preserve,
    Boundary {
        left: Option<SiteId>,
        right: Option<SiteId>,
    },
    FrameNearest,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct Transition {
    frame_delta: i8,
    clip_delta: i32,
    owner: OwnerTransition,
}

#[expect(
    clippy::arithmetic_side_effects,
    reason = "frame and clip deltas are sums of validated frame/clip segment multiplicities, bounded away from the signed minimum before reversal"
)]
impl Transition {
    const fn reversed(self) -> Self {
        Self {
            frame_delta: -self.frame_delta,
            clip_delta: -self.clip_delta,
            owner: match self.owner {
                OwnerTransition::Preserve => OwnerTransition::Preserve,
                OwnerTransition::Boundary { left, right } => OwnerTransition::Boundary {
                    left: right,
                    right: left,
                },
            },
        }
    }
}

struct ScaffoldPrimitive {
    line: exact::ExactLine,
    start: exact::ExactPoint,
    end: exact::ExactPoint,
    frame_delta: i8,
    clip_delta: i32,
    owner: PrimitiveOwner,
    clip_source: Option<ClipSegmentId>,
    frame_side: Option<FrameSide>,
    events: Vec<exact::ExactPoint>,
}

struct ClippedSeparator {
    pair: [SiteId; 2],
    start: exact::ExactPoint,
    end: exact::ExactPoint,
    events: Vec<exact::ExactPoint>,
}

struct NormalizedClip<'a> {
    rings: Vec<Vec<exact::ExactPoint>>,
    fixed: Vec<Vec<XY>>,
    chart_fixed: Vec<Vec<XY>>,
    source_roots: Vec<Vec<VoronoiClipSourceRoot>>,
    source: Option<&'a Polygon>,
    geometry: Option<Polygon>,
    polygon_baseline: Option<usize>,
    contacts: Vec<ExactCycleContact>,
}

impl NormalizedClip<'_> {
    /// Extent of the outer fixed ring as `[minx, maxx, miny, maxy]`. The
    /// boundary-tiling certificate and its regressions index the sides in that
    /// order.
    fn frame_bounds(&self) -> [f64; 4] {
        let mut bounds = [
            f64::INFINITY,
            f64::NEG_INFINITY,
            f64::INFINITY,
            f64::NEG_INFINITY,
        ];
        for point in self
            .fixed
            .first()
            .expect("normalized clips have an outer ring")
        {
            bounds[0] = bounds[0].min(point.x);
            bounds[1] = bounds[1].max(point.x);
            bounds[2] = bounds[2].min(point.y);
            bounds[3] = bounds[3].max(point.y);
        }
        bounds
    }
}

#[derive(Clone)]
struct AtomSeed {
    start: exact::ExactPoint,
    end: exact::ExactPoint,
    frame_delta: i8,
    clip_delta: i32,
    owner: PrimitiveOwner,
    separator: Option<[SiteId; 2]>,
    clip_source: Option<ClipSegmentId>,
    frame_side: Option<FrameSide>,
}

struct NodedVertex {
    point: exact::ExactPoint,
    fixed_xy: Option<XY>,
}

struct AtomicEdge {
    endpoints: [VertexId; 2],
    forward: Transition,
    separator: Option<[SiteId; 2]>,
    clip_sources: Vec<ClipSegmentId>,
    frame_side: Option<FrameSide>,
}

struct NodedAtoms {
    vertices: Vec<NodedVertex>,
    edges: Vec<AtomicEdge>,
}

#[derive(Clone)]
struct ExactPairTouch {
    rings: [usize; 2],
    point: exact::ExactPoint,
    directions: [Vec<exact::ExactPoint>; 2],
}

#[derive(Clone)]
struct ExactCycleContact {
    segments: [usize; 2],
    point: exact::ExactPoint,
}

#[derive(Clone, Copy)]
struct NodedExactCycle<'a> {
    points: &'a [exact::ExactPoint],
    ring: &'a ExactSimpleRing,
}

#[derive(Clone, Copy)]
struct ExactVertexSegment {
    vertex: VertexId,
    segment: usize,
    ring: usize,
    neighbor: VertexId,
}

struct NodedSegmentIndex {
    offsets: Vec<usize>,
    endpoints: Vec<[VertexId; 2]>,
    incidences: Vec<ExactVertexSegment>,
}

#[derive(Clone, Copy)]
struct NormalizedSourceRing {
    ring: usize,
    segment_offset: usize,
    reversed: bool,
}

struct ClipCertificateEdge {
    ring: usize,
    atom: EdgeId,
    start: exact::ExactPoint,
    end: exact::ExactPoint,
    rounded: [XY; 2],
}

#[derive(Clone, Copy)]
struct ClipSourceSegment<'a> {
    segment: u32,
    ring: usize,
    start: &'a exact::ExactPoint,
    end: &'a exact::ExactPoint,
    start_vertex: VertexId,
    end_vertex: VertexId,
}

#[derive(Default)]
struct ClipSourceRow {
    vertices: Vec<VertexId>,
    edges: Vec<EdgeId>,
}

#[derive(Clone)]
struct ClipTraceAtom {
    edge: EdgeId,
    reversed: bool,
    segment: u32,
    start_parameter: exact::ExactRational,
    end_parameter: exact::ExactRational,
}

#[derive(Default)]
struct ClipTraceRing {
    atoms: Vec<ClipTraceAtom>,
}

struct ClipTrace {
    rings: Vec<ClipTraceRing>,
    atom_count: usize,
}

#[derive(Clone, Copy, Debug)]
struct ClipBoundaryFailure {
    ring: usize,
    atom: Option<EdgeId>,
}

#[derive(Clone, Copy, Debug)]
struct ClipHostFailure {
    chord: usize,
    boundary: ClipBoundaryFailure,
}

struct ClipRingParity {
    generation: usize,
    generations: Vec<usize>,
    inside: Vec<bool>,
    shell_inside: bool,
    holes_inside: usize,
}

struct ClipCertificate {
    edges: Vec<ClipCertificateEdge>,
    polygon: Polygon,
}

#[cfg(test)]
struct ValidatedExactCycles {
    contacts: Vec<ExactCycleContact>,
}

#[derive(Clone, Copy)]
pub(super) enum OwnerShellPosition {
    Outside,
    BoundaryOnly,
    Inside,
}

#[derive(Clone, Copy)]
pub(super) struct OwnerCycleContainment {
    pub(super) shell_position: OwnerShellPosition,
    pub(super) nested_descendant: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum OwnerCycleFailure {
    OutsideShell,
    BoundaryOnly,
    NestedHole,
}

type OpenClipRing = (Vec<exact::ExactPoint>, Vec<XY>, Vec<XY>, Vec<Option<usize>>);

fn voronoi_error(message: impl Into<String>) -> crate::error::Error {
    GeometryErrorKind::voronoi(message.into())
}

#[expect(
    clippy::indexing_slicing,
    reason = "the ordinal comes from CertifiedDelaunay built against this admitted canonical site slice"
)]
pub(super) const fn site_at(sites: &[Site], ordinal: usize) -> &Site {
    &sites[ordinal]
}

const fn site_by_id(sites: &[Site], site: SiteId) -> &Site {
    site_at(sites, site.0 as usize)
}

#[expect(
    clippy::indexing_slicing,
    reason = "VertexId was assigned from the lexicographically interned event_points that produced this NodedVertex column"
)]
const fn noded_vertex(vertices: &[NodedVertex], id: VertexId) -> &NodedVertex {
    &vertices[id.0 as usize]
}

fn exact_embedding_unrepresentable(
    operation: &'static str,
    constructed: usize,
) -> crate::error::Error {
    GeometryErrorKind::ExactEmbeddingUnrepresentable {
        operation,
        constructed,
    }
    .into()
}

struct SiteAdjacency {
    offsets: Vec<u32>,
    neighbors: Vec<SiteId>,
}

pub(super) struct EmbeddedVoronoi {
    polygons: Vec<EmbeddedPolygon>,
    edges: EmbeddedEdges,
    receipt: std::sync::Arc<VoronoiDiagramReceipt>,
}

#[derive(Clone)]
pub(super) struct EmbeddedPolygon {
    pub(super) polygon: Polygon,
    pub(super) owner: Option<u32>,
    pub(super) edge_roles: Vec<Vec<VoronoiRingEdgeRole>>,
    pub(super) producer_vertices: Vec<Vec<OverlayNodeReceipt>>,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum VoronoiProduct {
    Polygons,
    Edges,
    #[cfg(test)]
    Both,
}

impl VoronoiProduct {
    const fn includes_polygons(self) -> bool {
        match self {
            Self::Polygons => true,
            Self::Edges => false,
            #[cfg(test)]
            Self::Both => true,
        }
    }

    const fn includes_edges(self) -> bool {
        match self {
            Self::Polygons => false,
            Self::Edges => true,
            #[cfg(test)]
            Self::Both => true,
        }
    }
}

/// Voronoi edges before any line geometry exists. Exact and collinear lanes
/// emit one packed XY column; the dual lane certifies cancellation on raw
/// segments and packs only when a caller asks for a column or rows.
#[derive(Clone)]
enum EmbeddedEdgeData {
    Packed {
        coords: CoordSeq,
        offsets: CsrOffsetColumn,
    },
    Segments(Vec<[XY; 2]>),
}

#[derive(Clone)]
pub(super) struct EmbeddedEdges {
    data: EmbeddedEdgeData,
    receipt: std::sync::Arc<VoronoiDiagramReceipt>,
}

pub(super) fn empty_packed_edges(chart: Option<&PointSelectionChart>) -> Result<EmbeddedEdges> {
    empty_edges_with_receipt(std::sync::Arc::new(site_receipts(
        VoronoiProducer::Empty,
        &[],
        chart,
    )?))
}

fn empty_edges_with_receipt(
    receipt: std::sync::Arc<VoronoiDiagramReceipt>,
) -> Result<EmbeddedEdges> {
    let ends = vec![0];
    Ok(EmbeddedEdges {
        data: EmbeddedEdgeData::Packed {
            coords: CoordSeq::try_from_vecs(Vec::new(), Vec::new(), None, None)?,
            offsets: CsrOffsetColumn::try_new(ends, 0)?,
        },
        receipt,
    })
}

fn pack_separator_chains(
    chains: &[ExactEdgeChain],
    vertices: usize,
    xy_of: impl Fn(VertexId) -> Result<XY>,
) -> Result<EmbeddedEdgeData> {
    ensure_coordseq_vertex_capacity(vertices)?;
    let offset_count = chains
        .len()
        .checked_add(1)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    let mut xs = Vec::new();
    let mut ys = Vec::new();
    xs.reserve(vertices);
    ys.reserve(vertices);
    let mut ends = Vec::with_capacity(offset_count);
    ends.push(0);
    for chain in chains {
        for &vertex in &chain.vertices {
            let point = xy_of(vertex)?;
            xs.push(point.x);
            ys.push(point.y);
        }
        ends.push(xs.len());
    }
    Ok(EmbeddedEdgeData::Packed {
        coords: CoordSeq::try_from_vecs(xs, ys, None, None)?,
        offsets: CsrOffsetColumn::try_new(ends, vertices)?,
    })
}

impl EmbeddedEdges {
    /// Canonical form: each dual segment runs from its lexicographically
    /// smaller endpoint, and the list is ordered by both endpoints. Packed
    /// chains keep their construction order.
    fn canonical(self) -> Self {
        let Self { data, receipt } = self;
        let data = match data {
            EmbeddedEdgeData::Packed { coords, offsets } => {
                EmbeddedEdgeData::Packed { coords, offsets }
            },
            EmbeddedEdgeData::Segments(mut segments) => {
                for segment in &mut segments {
                    if lex_xy(segment[0], segment[1]).is_gt() {
                        segment.swap(0, 1);
                    }
                }
                segments.sort_unstable_by(|left, right| {
                    lex_xy(left[0], right[0]).then(lex_xy(left[1], right[1]))
                });
                EmbeddedEdgeData::Segments(segments)
            },
        };
        Self { data, receipt }
    }

    #[cfg(test)]
    pub(super) fn into_line_seqs(self) -> Result<Vec<LineSeq>> {
        match self.canonical().data {
            EmbeddedEdgeData::Packed { coords, offsets } => {
                let ends = offsets.as_slice();
                ends.array_windows::<2>()
                    .map(|&[start, end]| {
                        let range = start as usize..end as usize;
                        LineSeq::try_new(CoordSeq::try_from_vecs(
                            coords.xs()[range.clone()].to_vec(),
                            coords.ys()[range].to_vec(),
                            None,
                            None,
                        )?)
                    })
                    .collect::<std::result::Result<Vec<_>, _>>()
            },
            EmbeddedEdgeData::Segments(segments) => segments
                .into_iter()
                .map(|[a, b]| {
                    LineSeq::try_new(CoordSeq::try_from_vecs(
                        vec![a.x, b.x],
                        vec![a.y, b.y],
                        None,
                        None,
                    )?)
                })
                .collect::<std::result::Result<Vec<_>, _>>(),
        }
    }

    /// One packed XY line column with CSR vertex offsets, built directly:
    /// no per-edge geometry, no per-edge allocation.
    #[cfg(test)]
    pub(super) fn into_line_column(self) -> Result<(CoordSeq, CsrOffsetColumn)> {
        self.into_line_column_with_receipt()
            .map(|(coords, offsets, _)| (coords, offsets))
    }

    pub(super) fn into_line_column_with_receipt(
        self,
    ) -> Result<(
        CoordSeq,
        CsrOffsetColumn,
        std::sync::Arc<VoronoiDiagramReceipt>,
    )> {
        let Self { data, receipt } = self.canonical();
        match data {
            EmbeddedEdgeData::Packed { coords, offsets } => Ok((coords, offsets, receipt)),
            EmbeddedEdgeData::Segments(segments) => {
                let vertices = segments
                    .len()
                    .checked_mul(2)
                    .ok_or(crate::error::ResourceError::SizeOverflow)?;
                ensure_coordseq_vertex_capacity(vertices)?;
                let offset_count = segments
                    .len()
                    .checked_add(1)
                    .ok_or(crate::error::ResourceError::SizeOverflow)?;
                let mut xs = Vec::new();
                let mut ys = Vec::new();
                let mut ends = Vec::new();
                xs.reserve(vertices);
                ys.reserve(vertices);
                ends.reserve(offset_count);
                for [a, b] in &segments {
                    xs.extend([a.x, b.x]);
                    ys.extend([a.y, b.y]);
                }
                ends.extend((0..vertices).step_by(2).chain(std::iter::once(vertices)));
                Ok((
                    CoordSeq::try_from_vecs(xs, ys, None, None)?,
                    CsrOffsetColumn::try_new(ends, vertices)?,
                    receipt,
                ))
            },
        }
    }
}

impl EmbeddedVoronoi {
    #[cfg(test)]
    pub(super) fn into_polygons(self) -> Vec<Polygon> {
        self.polygons
            .into_iter()
            .map(|output| output.polygon)
            .collect()
    }

    pub(super) fn into_polygon_outputs(
        self,
    ) -> (Vec<EmbeddedPolygon>, std::sync::Arc<VoronoiDiagramReceipt>) {
        (self.polygons, self.receipt)
    }

    pub(super) fn into_edges(self) -> EmbeddedEdges {
        self.edges
    }
}

fn fan_target(subdivision: &ExactSubdivision, edge: EdgeId, vertex: VertexId) -> VertexId {
    let [start, end] = subdivision.edge(edge).endpoints;
    if start == vertex { end } else { start }
}

struct LiveVertices(Vec<bool>);

impl LiveVertices {
    fn new(vertex_count: usize) -> Self {
        Self(vec![false; vertex_count])
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "VertexId values are minted from the exact vertex column that sized this liveness map"
    )]
    fn mark(&mut self, vertex: VertexId) {
        self.0[vertex.0 as usize] = true;
    }

    fn iter(&self) -> std::slice::Iter<'_, bool> {
        self.0.iter()
    }
}

struct LiveEdgeIncidence {
    offsets: Vec<usize>,
    edges: Vec<EdgeId>,
}

impl LiveEdgeIncidence {
    #[expect(
        clippy::indexing_slicing,
        clippy::arithmetic_side_effects,
        reason = "live edges are a subset of the admit_counts-checked subdivision, so endpoint counts fit its u32 slot bound; each VertexId indexes the prefix, cursor, and incident-edge columns built from those same endpoints"
    )]
    fn build(subdivision: &ExactSubdivision, live_edges: &[EdgeId], vertex_count: usize) -> Self {
        let mut offsets = vec![0_usize; vertex_count + 1];
        for &edge_id in live_edges {
            for vertex in subdivision.edge(edge_id).endpoints {
                offsets[vertex.0 as usize + 1] += 1;
            }
        }
        let mut total = 0;
        for offset in offsets.iter_mut().skip(1) {
            total += *offset;
            *offset = total;
        }

        let mut edges = Vec::with_capacity(total);
        edges.resize(total, EdgeId(0));
        let mut cursors = Vec::with_capacity(vertex_count);
        cursors.extend(offsets.iter().take(vertex_count).copied());
        for &edge_id in live_edges {
            for vertex in subdivision.edge(edge_id).endpoints {
                let cursor = &mut cursors[vertex.0 as usize];
                edges[*cursor] = edge_id;
                *cursor += 1;
            }
        }
        Self { offsets, edges }
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "adjacent prefix offsets delimit every live-edge row in this same CSR column"
    )]
    fn rows(&self) -> impl Iterator<Item = (VertexId, &[EdgeId])> + '_ {
        self.offsets
            .array_windows::<2>()
            .enumerate()
            .map(|(vertex, &[start, end])| (VertexId(vertex as u32), &self.edges[start..end]))
    }
}

struct EmbeddedVertices(Vec<Option<EmbeddedVertexId>>);

impl EmbeddedVertices {
    fn new(vertex_count: usize) -> Self {
        Self(vec![None; vertex_count])
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "VertexId values are minted from the exact vertex column that sized this exact-to-embedded map"
    )]
    fn set(&mut self, vertex: VertexId, embedded: EmbeddedVertexId) {
        self.0[vertex.0 as usize] = Some(embedded);
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "live VertexId entries map to EmbeddedVertexId values assigned as points are appended to the paired output column"
    )]
    fn point(&self, vertex: VertexId, points: &[XY]) -> Result<XY> {
        let embedded = self.0[vertex.0 as usize]
            .ok_or_else(|| voronoi_error("live edge has a non-live endpoint"))?;
        Ok(points[embedded.0 as usize])
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "the live VertexId was mapped when this same output coordinate column was built"
    )]
    fn set_point(&self, vertex: VertexId, points: &mut [XY], point: XY) {
        let embedded =
            self.0[vertex.0 as usize].expect("search assignments name live Voronoi vertices");
        points[embedded.0 as usize] = point;
    }
}

#[derive(Clone, Copy)]
struct VoronoiRing {
    part: usize,
    hole: Option<usize>,
}

impl VoronoiRing {
    fn exact(self, parts: &[ExactCellPart]) -> &ExactSimpleRing {
        let part = parts
            .get(self.part)
            .expect("Voronoi ring part belongs to its cell table");
        self.hole.map_or(&part.shell, |hole| {
            part.holes
                .get(hole)
                .expect("Voronoi hole ordinal belongs to its part")
        })
    }
}

struct VoronoiFan {
    origin: VertexId,
    edges: Vec<EdgeId>,
    exact_order: Vec<EdgeId>,
}

struct VoronoiPoints<'a> {
    vertices: &'a EmbeddedVertices,
    coordinates: &'a [XY],
}

impl RayPoints for VoronoiPoints<'_> {
    fn rounded(&self, index: u32) -> XY {
        self.vertices
            .point(VertexId(index), self.coordinates)
            .expect("ray sweep queries only live Voronoi vertices")
    }

    fn compare_axis(&self, left: u32, right: u32, axis: RayAxis) -> Result<Ordering> {
        let (left, right) = (self.rounded(left), self.rounded(right));
        let (left, right) = match axis {
            RayAxis::X => (left.x, right.x),
            RayAxis::Y => (left.y, right.y),
        };
        Ok(if left < right {
            Ordering::Less
        } else if left > right {
            Ordering::Greater
        } else {
            Ordering::Equal
        })
    }

    fn orientation(&self, a: u32, b: u32, c: u32) -> Result<ExactSign> {
        let [a, b, c] = [a, b, c].map(|index| self.rounded(index));
        Ok(exact::orient2d(a, b, c))
    }
}

struct VoronoiHostGraph {
    owner: SiteId,
    vertices: Vec<u32>,
    edges: Vec<[u32; 2]>,
    sides: Vec<[u32; 2]>,
    chords: Vec<usize>,
}

impl VoronoiHostGraph {
    fn edge(&self, index: usize) -> [u32; 2] {
        *self
            .edges
            .get(index)
            .expect("owner-local edge index belongs to its host graph")
    }

    fn side_keys(&self, index: usize) -> [u32; 2] {
        *self
            .sides
            .get(index)
            .expect("owner-local side index belongs to its host graph")
    }

    fn vertex(&self, index: u32) -> VertexId {
        VertexId(
            *self
                .vertices
                .get(index as usize)
                .expect("owner-local vertex ID belongs to its dense vertex table"),
        )
    }

    fn chord(&self, index: usize) -> usize {
        *self
            .chords
            .get(index)
            .expect("owner-local chord index belongs to its host graph")
    }
}

struct VoronoiOwnerPoints<'a, 'p> {
    points: &'a VoronoiPoints<'p>,
    vertices: &'a [u32],
}

impl VoronoiOwnerPoints<'_, '_> {
    #[expect(
        clippy::indexing_slicing,
        reason = "owner-local node IDs are dense ordinals paired with this graph's global-vertex column"
    )]
    const fn global(&self, node: u32) -> u32 {
        self.vertices[node as usize]
    }
}

impl RayPoints for VoronoiOwnerPoints<'_, '_> {
    fn rounded(&self, index: u32) -> XY {
        RayPoints::rounded(self.points, self.global(index))
    }

    fn compare_axis(&self, left: u32, right: u32, axis: RayAxis) -> Result<Ordering> {
        RayPoints::compare_axis(self.points, self.global(left), self.global(right), axis)
    }

    fn orientation(&self, a: u32, b: u32, c: u32) -> Result<ExactSign> {
        RayPoints::orientation(self.points, self.global(a), self.global(b), self.global(c))
    }
}

/// Exact DCEL items and the nearest coordinates against which its embedding is
/// certified. Each host graph contains one owner's rings, so foreign boundaries
/// are transparent to its shell and hole relationships.
struct VoronoiEmbedding<'a> {
    subdivision: &'a ExactSubdivision,
    clip: &'a NormalizedClip<'a>,
    clip_trace: &'a ClipTrace,
    sites: &'a [Site],
    parts: &'a [ExactCellPart],
    includes_polygons: bool,
    live_nodes: &'a [VertexId],
    live_edges: &'a [EdgeId],
    vertices: &'a EmbeddedVertices,
    coordinates: &'a [XY],
    chords: Vec<ProximityChord>,
    incident_chords: Vec<Vec<usize>>,
    fans: Vec<VoronoiFan>,
    rings: Vec<VoronoiRing>,
    rings_by_node: Vec<Vec<usize>>,
    host_graphs: Vec<VoronoiHostGraph>,
    separator_chords: Vec<usize>,
}

#[derive(Clone)]
struct VoronoiClassState {
    pairs_by_rank: Vec<Vec<usize>>,
    separators_by_rank: Vec<Vec<usize>>,
    site_pairs_by_rank: Vec<Vec<usize>>,
    fans_by_rank: Vec<Vec<usize>>,
    rings_by_rank: Vec<Vec<usize>>,
}

struct VoronoiProximity {
    pairs: Vec<(usize, usize)>,
    site_pairs: Vec<(usize, usize)>,
    classes: Vec<VoronoiClassState>,
}

struct VoronoiProblem<'p, 'a> {
    embedding: &'p VoronoiEmbedding<'a>,
    classes: Vec<Vec<VertexId>>,
    search_seeds: Vec<VertexId>,
    node_location: Vec<Option<(usize, usize)>>,
    radius_cache: RefCell<Vec<Option<f64>>>,
    movable_cache: RefCell<Vec<Option<bool>>>,
    proximity: RefCell<Option<VoronoiProximity>>,
    initial_evaluations: u64,
}

fn polygon_output_count(
    shell_positions: usize,
    hole_positions: impl IntoIterator<Item = usize>,
) -> Result<usize> {
    ensure_coordseq_vertex_capacity(shell_positions)?;
    let (positions, holes) = hole_positions.into_iter().try_fold(
        (shell_positions, 0_usize),
        |(positions, holes), hole_positions| -> Result<(usize, usize)> {
            ensure_coordseq_vertex_capacity(hole_positions)?;
            Ok((
                positions
                    .checked_add(hole_positions)
                    .ok_or(crate::error::ResourceError::SizeOverflow)?,
                holes
                    .checked_add(1)
                    .ok_or(crate::error::ResourceError::SizeOverflow)?,
            ))
        },
    )?;
    let containers = holes
        .checked_add(2)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    positions
        .checked_add(containers)
        .ok_or_else(|| crate::error::ResourceError::SizeOverflow.into())
}

fn polygon_output_baseline(polygon: &Polygon) -> Result<usize> {
    polygon_output_count(
        polygon.shell.len(),
        polygon.holes.iter().map(|hole| hole.len()),
    )
}

#[cfg(test)]
pub(super) fn build(
    sites: &[Site],
    boundary: VoronoiBoundary<'_>,
    budget: &mut ExpansionBudget,
    combinatorial_dual: bool,
    mesh: &CertifiedDelaunay,
) -> Result<EmbeddedVoronoi> {
    build_product(
        sites,
        boundary,
        budget,
        combinatorial_dual,
        mesh,
        VoronoiProduct::Both,
    )
}

#[cfg(test)]
pub(super) fn build_product(
    sites: &[Site],
    boundary: VoronoiBoundary<'_>,
    budget: &mut ExpansionBudget,
    combinatorial_dual: bool,
    mesh: &CertifiedDelaunay,
    product: VoronoiProduct,
) -> Result<EmbeddedVoronoi> {
    build_product_with_chart(
        sites,
        boundary,
        budget,
        combinatorial_dual,
        mesh,
        product,
        None,
    )
}

pub(super) fn build_product_with_chart(
    sites: &[Site],
    boundary: VoronoiBoundary<'_>,
    budget: &mut ExpansionBudget,
    combinatorial_dual: bool,
    mesh: &CertifiedDelaunay,
    product: VoronoiProduct,
    chart: Option<&PointSelectionChart>,
) -> Result<EmbeddedVoronoi> {
    if let Some(last) = sites.len().checked_sub(1) {
        u32::try_from(last).map_err(|_| crate::error::ResourceError::SizeOverflow)?;
    }
    if sites
        .iter()
        .enumerate()
        .any(|(index, site)| site.id != index)
    {
        return Err(voronoi_error("Voronoi sites are not canonically indexed"));
    }
    let mut normal_or_zero = true;
    for site in sites {
        let xy = site.rounded_xy()?;
        normal_or_zero &= (xy.x == 0.0 || xy.x.is_normal()) && (xy.y == 0.0 || xy.y.is_normal());
    }
    if combinatorial_dual
        && sites.len() >= 3
        && normal_or_zero
        && matches!(
            boundary,
            VoronoiBoundary::Envelope | VoronoiBoundary::Padded
        )
    {
        return dual_build_product(sites, boundary, budget, mesh, product, chart);
    }
    let clip = normalize_clip_with_chart(sites, boundary, product.includes_polygons(), chart)?;
    if mesh.collinear_order().is_some() {
        return Err(voronoi_error(
            "collinear sites require the exact one-dimensional lane",
        ));
    }
    let complex = delaunay_complex(mesh, sites)?;
    let primal_edges = mesh.primal_edges();
    let adjacency = certified_site_adjacency(&primal_edges, sites.len())?;
    let rect = clip_rectangle(&clip)?;
    let scaffold = build_scaffold(&rect, &clip)?;
    let separators = emit_clipped_separators(sites, &primal_edges, &complex, &rect)?;
    drop((complex, primal_edges));
    let atoms =
        node_scaffold_and_separators(scaffold, separators, sites, &adjacency, &clip, &rect)?;
    let mut subdivision = build_exact_subdivision(atoms)?;
    graft_components(&mut subdivision, sites, &adjacency)?;
    let parts = assemble_cells(&subdivision)?;
    let chains = if product.includes_edges() {
        assemble_edges(&subdivision)?
    } else {
        Vec::new()
    };
    embed(
        &subdivision,
        &parts,
        &chains,
        &clip,
        sites,
        product,
        budget,
        VoronoiProducer::Exact,
        chart,
    )
}

#[cfg(test)]
pub(super) fn build_edges(
    sites: &[Site],
    boundary: VoronoiBoundary<'_>,
    budget: &mut ExpansionBudget,
    combinatorial_dual: bool,
    mesh: &CertifiedDelaunay,
) -> Result<EmbeddedEdges> {
    build_edges_with_chart(sites, boundary, budget, combinatorial_dual, mesh, None)
}

pub(super) fn build_edges_with_chart(
    sites: &[Site],
    boundary: VoronoiBoundary<'_>,
    budget: &mut ExpansionBudget,
    combinatorial_dual: bool,
    mesh: &CertifiedDelaunay,
    chart: Option<&PointSelectionChart>,
) -> Result<EmbeddedEdges> {
    Ok(build_product_with_chart(
        sites,
        boundary,
        budget,
        combinatorial_dual,
        mesh,
        VoronoiProduct::Edges,
        chart,
    )?
    .into_edges())
}

#[cfg(test)]
#[path = "voronoi_dcel/chart_cut_receipt_tests.rs"]
mod chart_cut_receipt_tests;
#[cfg(test)]
mod tests;

// =====================================================================
// The certified combinatorial dual.
//
// The Voronoi DCEL is the dual graph of the ALREADY-CERTIFIED Delaunay
// complex. The cell of site `p` is the radial walk of the faces incident to
// `p`, mapped through `DelaunayComplex::component_of_face` to one exact
// circumcenter each. That walk is pure half-edge combinatorics: it evaluates
// NO geometric predicate. Exact geometry is then needed only for
//   (a) capping the two unbounded rays of a hull cell on the private frame,
//   (b) clipping each cell to the clip rectangle, and
//   (c) the rounding certificate.
// Everything `emit_clipped_separators` / `node_scaffold_and_separators` /
// `graft_components` / `assemble_cells` does is replaced by (a) and (b).
//
// Rectangle clips (`Envelope` / `Padded`) use the direct half-edge lane. The
// general polygon lane retains the exact clipped-DCEL construction below: the
// certified complex fixes the unclipped abstract dual, but clipping can delete
// every separator around an isolated clip ring, whose owning site must still
// be established geometrically.
// =====================================================================

/// A dual ring vertex. `Center(c)` names the one exact center stored by the
/// ring collection; generated vertices own only frame, ray, or clip points.
#[cfg_attr(test, derive(Clone))]
enum DualVertex {
    Center(usize),
    Generated(Box<exact::ExactPoint>),
}

/// Preserve both full-width endpoint IDs and orientation; two `u32` IDs plus
/// a direction bit cannot be packed losslessly into one `u64`.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct DirectedDualEdge {
    low: u32,
    high: u32,
    endpoints: [u32; 2],
    from: u32,
    to: u32,
    forward: bool,
    owner: u32,
    origin: VoronoiEdgeOrigin,
    clip_segment: Option<u32>,
    dart: u32,
}

struct DualClipUse {
    segment: u32,
    start_parameter: exact::ExactRational,
    end_parameter: exact::ExactRational,
}

#[derive(Clone, Copy)]
struct DualEdgeDraft {
    endpoints: [u32; 2],
    coordinates: [XY; 2],
    first: DirectedDualEdge,
    second: Option<DirectedDualEdge>,
}

/// The edge leaving a dual ring vertex. Consecutive Voronoi vertices around a
/// site lie on the bisector of the site and one neighbour, so a clip crossing
/// on such an edge is a bisector meeting an axis-aligned line: a certified
/// one-dimensional rounding. Frame and clip sides are axis-aligned, and their
/// crossings with another axis-aligned line are exact doubles.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum EdgeTag {
    Bisector(SiteId),
    Frame(FrameSide),
}

struct Tagged {
    vertex: DualVertex,
    leaving: EdgeTag,
}

#[cfg_attr(test, derive(Clone))]
struct DualRings {
    rings: Vec<Vec<DualVertex>>,
    centers: Vec<exact::ExactPoint>,
    center_sites: Vec<[u32; 3]>,
    owners: Vec<SiteId>,
    edge_tags: Vec<Vec<EdgeTag>>,
}

/// The radial walk. `triangles[e]` is the origin of half-edge `e`; the walk
/// `incoming -> halfedges[next(incoming)]` rotates around the shared endpoint.
/// Starting from a hull half-edge when one exists makes an unbounded cell's
/// walk begin and end on the hull. Faces for all sites share one flat backing
/// allocation; the offsets are the only per-site state.
#[cfg(test)]
mod convex_cell_certificate;

struct IncidentFaces {
    offsets: Vec<usize>,
    faces: Vec<usize>,
    /// Parallel to `faces`: the site across the edge from each face to the
    /// next one around the site (for the last face of a hull cell, the far
    /// end of the outgoing hull edge).
    neighbors: Vec<SiteId>,
    boundaries: Vec<Option<(usize, usize)>>,
}

struct IncidentFaceRow<'a> {
    faces: &'a [usize],
    neighbors: &'a [SiteId],
    boundary: Option<(usize, usize)>,
}

/// Where one site's walk writes its faces and neighbours.
struct IncidenceOutput<'a> {
    faces: &'a mut [usize],
    neighbors: &'a mut [SiteId],
    flat: &'a dyn Fn(usize) -> usize,
}

fn lex_xy(left: XY, right: XY) -> Ordering {
    left.x.total_cmp(&right.x).then(left.y.total_cmp(&right.y))
}
