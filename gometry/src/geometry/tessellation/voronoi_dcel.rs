//! Exact labelled Voronoi subdivision and its sole binary64 projection.

#![expect(
    clippy::clone_on_copy,
    clippy::explicit_iter_loop,
    clippy::float_cmp,
    clippy::inconsistent_struct_constructor,
    clippy::items_after_statements,
    clippy::missing_const_for_fn,
    clippy::needless_range_loop,
    clippy::unnecessary_semicolon,
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
use crate::geometry::area::{RingWinding, open_xy_cycle_winding};
use crate::geometry::arrangement::{IndexedTopology, IndexedTopologyScratch};
use crate::geometry::embedding_search::{
    ClassAssignment, EvaluationBudget, GlobalCheck, Problem, ProximityChord, Search, SearchAccount,
    Work, build_proximity_classes, exact_chord_radius, possible_swept_bounds, proximity_limit,
};
use crate::geometry::exact_arith::{ExactSign, retained_site_longitude};
use crate::geometry::overlay::{
    NodeId, OverlayNodeReceipt, OverlaySourceEdgeUse, RayAxis, RayPoints,
    binary_cartesian_difference_without_partition_budget,
    binary_cartesian_intersection_with_edge_roles, emitted_pair_preserves_incidence,
    segments_within_chebyshev, visit_component_faces,
};
use crate::geometry::segment_index::YStabbingIndex;
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
    for_each_overlapping_bounds_cross_pair, polygon_is_valid, single_chain, try_dedup_by,
    try_sort_by,
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

    fn start(self, rectangle: &[exact::ExactPoint; 4]) -> &exact::ExactPoint {
        let [bottom_left, bottom_right, top_right, top_left] = rectangle;
        match self {
            Self::Bottom => bottom_left,
            Self::Right => bottom_right,
            Self::Top => top_right,
            Self::Left => top_left,
        }
    }

    fn end(self, rectangle: &[exact::ExactPoint; 4]) -> &exact::ExactPoint {
        self.next().start(rectangle)
    }

    fn start_xy(self, rectangle: &[XY; 4]) -> XY {
        let [bottom_left, bottom_right, top_right, top_left] = rectangle;
        match self {
            Self::Bottom => *bottom_left,
            Self::Right => *bottom_right,
            Self::Top => *top_right,
            Self::Left => *top_left,
        }
    }

    fn end_xy(self, rectangle: &[XY; 4]) -> XY {
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
mod scaffold;
mod subdivision;
use admission::{
    BoxIndex, admit_box_ordinals, admit_embedded_clip, clip_certificate_data,
    clip_source_trace_error, exact_chart_point, exact_cycle_error, exact_segment_parameter,
    first_owner_cycle_failure, for_each_assembled_pair, for_each_touch_fan,
    normalize_clip_with_chart, open_ring, rotate_ring, rounded_segment_covered_by_clip,
    validate_assembled_exact_cycles, validate_exact_rectangle, validate_owner_cycle_facts,
};
#[cfg(test)]
use admission::{normalize_clip, rounded_box, validate_exact_cycle_nesting, validate_exact_cycles};
#[cfg(test)]
use assembly::assemble_separator_group;
use assembly::{ExactCellPart, ExactEdgeChain, ExactSimpleRing, assemble_cells, assemble_edges};
#[cfg(test)]
use clip::in_rectangle;
use clip::{
    SegmentIntersectionExt as _, axis_aligned_rectangle, between, clip_hull_ray_to_rectangle,
    clip_rectangle, clip_segment_to_rectangle, exact_bisector_frame_intersections,
    orient_separator, sort_unique_along,
};
#[cfg(test)]
use collinear::build_collinear_edges;
pub(super) use collinear::{build_collinear_edges_with_chart, build_collinear_polygons_with_chart};
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
pub(super) fn site_at(sites: &[Site], ordinal: usize) -> &Site {
    &sites[ordinal]
}

fn site_by_id(sites: &[Site], site: SiteId) -> &Site {
    site_at(sites, site.0 as usize)
}

#[expect(
    clippy::indexing_slicing,
    reason = "VertexId was assigned from the lexicographically interned event_points that produced this NodedVertex column"
)]
fn noded_vertex(vertices: &[NodedVertex], id: VertexId) -> &NodedVertex {
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

#[derive(Debug)]
enum DualEmbeddingError {
    Unrepresentable,
    Other(crate::error::Error),
}

impl From<crate::error::Error> for DualEmbeddingError {
    fn from(error: crate::error::Error) -> Self {
        Self::Other(error)
    }
}

impl From<crate::error::ResourceError> for DualEmbeddingError {
    fn from(error: crate::error::ResourceError) -> Self {
        Self::Other(error.into())
    }
}

struct SiteAdjacency {
    offsets: Vec<u32>,
    neighbors: Vec<SiteId>,
}

impl SiteAdjacency {
    #[expect(
        clippy::indexing_slicing,
        reason = "SiteId is admitted against the canonical site count and offsets has one terminal entry for each site row"
    )]
    fn neighbors(&self, site: SiteId) -> &[SiteId] {
        let site = site.0 as usize;
        let start = self.offsets[site] as usize;
        let end = self.offsets[site + 1] as usize;
        &self.neighbors[start..end]
    }
}

#[expect(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "CertifiedPrimalEdge endpoints are site ordinals from the already-admitted mesh; degrees/cursors/neighbors share that domain, and cumulative u32 offsets are checked before publication"
)]
fn certified_site_adjacency(
    edges: &[CertifiedPrimalEdge],
    site_count: usize,
) -> Result<SiteAdjacency> {
    let mut degrees = vec![0_usize; site_count];
    for edge in edges {
        let sites = match *edge {
            CertifiedPrimalEdge::Interior { sites, .. }
            | CertifiedPrimalEdge::Hull { sites, .. } => sites,
        };
        let [left, right] = sites;
        degrees[left] += 1;
        degrees[right] += 1;
    }
    let mut offsets = Vec::with_capacity(site_count + 1);
    offsets.push(0_u32);
    for degree in degrees {
        let next = (offsets.last().copied().unwrap_or(0) as usize)
            .checked_add(degree)
            .ok_or_else(|| voronoi_error("too many site adjacencies"))?;
        offsets.push(u32::try_from(next).map_err(|_| voronoi_error("too many site adjacencies"))?);
    }
    let neighbor_count = offsets.last().copied().unwrap_or(0) as usize;
    let mut neighbors = Vec::with_capacity(neighbor_count);
    neighbors.resize(neighbor_count, SiteId(0));
    let mut cursors = Vec::with_capacity(site_count);
    cursors.extend(
        offsets
            .iter()
            .take(site_count)
            .map(|offset| *offset as usize),
    );
    for edge in edges {
        let sites = match *edge {
            CertifiedPrimalEdge::Interior { sites, .. }
            | CertifiedPrimalEdge::Hull { sites, .. } => sites,
        };
        let [left, right] = sites;
        neighbors[cursors[left]] = SiteId(right as u32);
        cursors[left] += 1;
        neighbors[cursors[right]] = SiteId(left as u32);
        cursors[right] += 1;
    }
    for &[start, end] in offsets.array_windows::<2>() {
        neighbors[start as usize..end as usize].sort_unstable();
    }
    Ok(SiteAdjacency { offsets, neighbors })
}

fn nearest_site_walk(
    point: &exact::ExactPoint,
    sites: &[Site],
    adjacency: &SiteAdjacency,
    mut current: SiteId,
) -> Result<SiteId> {
    for _ in 0..sites.len() {
        let neighbors = adjacency.neighbors(current);
        let mut best = current;
        let nearer = |candidate: SiteId, known: SiteId| {
            exact::squared_distance_cmp_points(
                point,
                &site_by_id(sites, candidate).exact_point,
                &site_by_id(sites, known).exact_point,
            )
        };
        for &candidate in neighbors {
            if nearer(candidate, best)? == Ordering::Less {
                best = candidate;
            }
        }
        if best != current {
            current = best;
            continue;
        }
        for &candidate in neighbors {
            if nearer(candidate, current)? == Ordering::Equal {
                return Err(voronoi_error(
                    "private exact frame atom has a tied nearest site",
                ));
            }
        }
        return Ok(current);
    }
    Err(voronoi_error("certified nearest-site walk did not descend"))
}

#[cfg(test)]
fn nearest_site(point: &exact::ExactPoint, sites: &[Site]) -> Result<SiteId> {
    let mut nearest = 0;
    let mut tied = false;
    for candidate in 1..sites.len() {
        match exact::squared_distance_cmp_points(
            point,
            &sites[candidate].exact_point,
            &sites[nearest].exact_point,
        )? {
            Ordering::Less => {
                nearest = candidate;
                tied = false;
            },
            Ordering::Equal => tied = true,
            Ordering::Greater => {},
        }
    }
    if tied {
        return Err(voronoi_error(
            "private exact frame atom has a tied nearest site",
        ));
    }
    Ok(SiteId(
        u32::try_from(nearest).map_err(|_| voronoi_error("too many Voronoi sites"))?,
    ))
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

pub(super) fn empty_packed_edges() -> Result<EmbeddedEdges> {
    empty_edges_with_receipt(std::sync::Arc::new(VoronoiDiagramReceipt::empty(
        VoronoiProducer::Empty,
    )))
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
        for &vertex in chain.vertices.iter() {
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
    fn global(&self, node: u32) -> u32 {
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

impl<'a> VoronoiEmbedding<'a> {
    fn chord(&self, index: usize) -> ProximityChord {
        *self
            .chords
            .get(index)
            .expect("chord index belongs to the Voronoi embedding")
    }

    fn ring(&self, index: usize) -> VoronoiRing {
        *self
            .rings
            .get(index)
            .expect("ring index belongs to the Voronoi embedding")
    }

    fn ring_owner(&self, index: usize) -> SiteId {
        let ring = self.ring(index);
        self.parts
            .get(ring.part)
            .expect("ring part belongs to the Voronoi embedding")
            .owner
    }

    fn visit_clip_ring_vertices(&self, ring: usize, mut visit: impl FnMut(VertexId)) {
        let ring = self
            .clip_trace
            .rings
            .get(ring)
            .expect("clip-host failure names an admitted clip ring");
        for atom in &ring.atoms {
            for vertex in self.subdivision.edge(atom.edge).endpoints {
                visit(vertex);
            }
        }
    }

    fn new(
        subdivision: &'a ExactSubdivision,
        clip: &'a NormalizedClip<'a>,
        clip_trace: &'a ClipTrace,
        parts: &'a [ExactCellPart],
        includes_polygons: bool,
        live_nodes: &'a [VertexId],
        live_edges: &'a [EdgeId],
        vertices: &'a EmbeddedVertices,
        coordinates: &'a [XY],
        sites: &'a [Site],
    ) -> Result<Self> {
        let vertex_count = subdivision.vertices.len();
        let mut chords = Vec::with_capacity(live_edges.len());
        let mut incident_chords = Vec::new();
        incident_chords.resize_with(vertex_count, Vec::new);
        for (index, &edge_id) in live_edges.iter().enumerate() {
            let [start, end] = subdivision.edge(edge_id).endpoints;
            let nearest = Segment {
                start: vertices.point(start, coordinates)?,
                end: vertices.point(end, coordinates)?,
            };
            let chord = ProximityChord {
                nodes: [start.0, end.0],
                nearest,
                can_move: [start, end].map(|vertex| subdivision.vertex(vertex).fixed_xy.is_none()),
            };
            incident_chords
                .get_mut(start.0 as usize)
                .expect("live edge start belongs to the admitted vertex table")
                .push(index);
            if end != start {
                incident_chords
                    .get_mut(end.0 as usize)
                    .expect("live edge end belongs to the admitted vertex table")
                    .push(index);
            }
            chords.push(chord);
        }
        let incidence = LiveEdgeIncidence::build(subdivision, live_edges, vertex_count);
        let xy_of = |vertex| vertices.point(vertex, coordinates);
        let mut fans = Vec::new();
        for (origin, edges) in incidence.rows() {
            if edges.len() <= 2
                || (!includes_polygons
                    && edges
                        .iter()
                        .all(|edge| subdivision.edge(*edge).separator.is_none()))
            {
                continue;
            }
            let mut exact_order = Vec::new();
            sort_fan(
                origin,
                edges,
                FanSortMode::Exact,
                subdivision,
                &xy_of,
                &mut exact_order,
            )?;
            fans.push(VoronoiFan {
                origin,
                edges: edges.to_vec(),
                exact_order,
            });
        }
        let mut rings = Vec::new();
        for (part, exact_part) in parts.iter().enumerate() {
            rings.push(VoronoiRing { part, hole: None });
            rings.extend(
                exact_part
                    .holes
                    .iter()
                    .enumerate()
                    .map(|(hole, _)| VoronoiRing {
                        part,
                        hole: Some(hole),
                    }),
            );
        }
        let mut rings_by_node = Vec::new();
        rings_by_node.resize_with(vertex_count, Vec::new);
        for (ring_index, ring_ref) in rings.iter().copied().enumerate() {
            let (_, open_vertices) = ring_ref
                .exact(parts)
                .vertices
                .split_last()
                .expect("simple Voronoi rings are closed");
            for &vertex in open_vertices {
                rings_by_node
                    .get_mut(vertex.0 as usize)
                    .expect("ring vertex belongs to the admitted vertex table")
                    .push(ring_index);
            }
        }

        let mut edge_to_chord = Vec::new();
        edge_to_chord.resize(subdivision.edges.len(), usize::MAX);
        for (chord, &edge_id) in live_edges.iter().enumerate() {
            *edge_to_chord
                .get_mut(edge_id.0 as usize)
                .expect("live edge belongs to the exact subdivision") = chord;
        }
        let mut host_graphs = Vec::new();
        if includes_polygons {
            let unassigned_part = u32::try_from(parts.len())
                .map_err(|_| crate::error::ResourceError::SizeOverflow)?;
            let mut face_part = Vec::new();
            face_part.resize(subdivision.labels.len(), unassigned_part);
            for (part_index, part) in parts.iter().enumerate() {
                let part_index = u32::try_from(part_index)
                    .map_err(|_| crate::error::ResourceError::SizeOverflow)?;
                for ring in std::iter::once(&part.shell).chain(&part.holes) {
                    let face = face_part
                        .get_mut(ring.face.0 as usize)
                        .ok_or_else(exact_cycle_error)?;
                    if *face != unassigned_part && *face != part_index {
                        return Err(voronoi_error(
                            "one exact face belongs to multiple emitted Voronoi parts",
                        ));
                    }
                    *face = part_index;
                }
            }
            let mut edge_faces = Vec::new();
            edge_faces.resize(subdivision.edges.len(), [u32::MAX; 2]);
            for (slot, &edge) in subdivision.edge_of_slot.iter().enumerate() {
                let dart = subdivision.dart(slot as u32);
                let endpoints = subdivision.edge(EdgeId(edge)).endpoints;
                let side = usize::from(dart.origin != endpoints[0]);
                *edge_faces
                    .get_mut(edge as usize)
                    .and_then(|sides| sides.get_mut(side))
                    .ok_or_else(exact_cycle_error)? = dart.face.0;
            }
            let mut part_start = 0;
            while part_start < parts.len() {
                let owner = parts
                    .get(part_start)
                    .expect("owner group starts within the sorted part table")
                    .owner;
                let mut part_end = part_start.wrapping_add(1);
                while part_end < parts.len()
                    && parts
                        .get(part_end)
                        .expect("owner group ends within the sorted part table")
                        .owner
                        == owner
                {
                    part_end = part_end.wrapping_add(1);
                }
                let owner_parts = parts
                    .get(part_start..part_end)
                    .expect("owner parts form a bounded slice");
                let mut owner_edges = Vec::new();
                for part in owner_parts {
                    for ring in std::iter::once(&part.shell).chain(&part.holes) {
                        owner_edges.extend_from_slice(&ring.edges);
                    }
                }
                owner_edges.sort_unstable();
                owner_edges.dedup();
                let mut owner_vertices = Vec::with_capacity(owner_edges.len());
                for &edge in &owner_edges {
                    owner_vertices.extend(subdivision.edge(edge).endpoints.map(|vertex| vertex.0));
                }
                owner_vertices.sort_unstable();
                owner_vertices.dedup();
                let mut edges = Vec::with_capacity(owner_edges.len());
                let mut sides = Vec::with_capacity(owner_edges.len());
                let mut chords = Vec::with_capacity(owner_edges.len());
                for edge in owner_edges {
                    let endpoints = subdivision.edge(edge).endpoints;
                    let mut local_endpoints = [0_u32; 2];
                    for (local, vertex) in local_endpoints.iter_mut().zip(endpoints) {
                        *local = u32::try_from(
                            owner_vertices
                                .binary_search(&vertex.0)
                                .map_err(|_| exact_cycle_error())?,
                        )
                        .map_err(|_| crate::error::ResourceError::SizeOverflow)?;
                    }
                    let exact_faces = *edge_faces
                        .get(edge.0 as usize)
                        .ok_or_else(exact_cycle_error)?;
                    let mut owner_sides = [0_u32; 2];
                    for (owner_side, face) in owner_sides.iter_mut().zip(exact_faces) {
                        if face == u32::MAX {
                            return Err(exact_cycle_error());
                        }
                        let part = face_part
                            .get(face as usize)
                            .copied()
                            .ok_or_else(exact_cycle_error)?;
                        if part != unassigned_part
                            && parts
                                .get(part as usize)
                                .is_some_and(|part| part.owner == owner)
                        {
                            *owner_side = part
                                .checked_add(1)
                                .ok_or(crate::error::ResourceError::SizeOverflow)?;
                        }
                    }
                    let chord = *edge_to_chord
                        .get(edge.0 as usize)
                        .filter(|&&chord| chord != usize::MAX)
                        .ok_or_else(exact_cycle_error)?;
                    edges.push(local_endpoints);
                    sides.push(owner_sides);
                    chords.push(chord);
                }
                host_graphs.push(VoronoiHostGraph {
                    owner,
                    vertices: owner_vertices,
                    edges,
                    sides,
                    chords,
                });
                part_start = part_end;
            }
        }
        let separator_chords = live_edges
            .iter()
            .enumerate()
            .filter_map(|(index, &edge)| {
                subdivision.edge(edge).separator.is_some().then_some(index)
            })
            .collect();

        Ok(Self {
            subdivision,
            clip,
            clip_trace,
            sites,
            parts,
            includes_polygons,
            live_nodes,
            live_edges,
            vertices,
            coordinates,
            chords,
            incident_chords,
            fans,
            rings,
            rings_by_node,
            host_graphs,
            separator_chords,
        })
    }

    fn xy(&self, vertex: VertexId, coordinates: &[XY]) -> Result<XY> {
        self.vertices.point(vertex, coordinates)
    }

    fn exact_movable(&self, vertex: VertexId) -> bool {
        self.subdivision.vertex(vertex).fixed_xy.is_none()
    }

    fn compare_nodes(&self, left: VertexId, right: VertexId) -> Result<Ordering> {
        self.subdivision
            .vertex(left)
            .point
            .compare_lex(&self.subdivision.vertex(right).point)
    }

    fn ring_orientation(&self, ring: usize, coordinates: &[XY]) -> Result<bool> {
        let ring = self
            .rings
            .get(ring)
            .copied()
            .expect("ring index belongs to the Voronoi ring table")
            .exact(self.parts);
        rounded_ring_orientation_matches(&ring.vertices, ring.orientation, |vertex| {
            self.xy(vertex, coordinates)
        })
    }

    fn extend_ring_seeds(&self, ring: usize, seeds: &mut Vec<VertexId>) {
        let ring = self
            .rings
            .get(ring)
            .copied()
            .expect("ring index belongs to the Voronoi ring table")
            .exact(self.parts);
        let (_, open_vertices) = ring
            .vertices
            .split_last()
            .expect("simple Voronoi rings are closed");
        seeds.extend_from_slice(open_vertices);
    }

    fn segment(&self, chord: usize, coordinates: &[XY]) -> Result<Segment> {
        let [start, end] = self
            .subdivision
            .edge(
                *self
                    .live_edges
                    .get(chord)
                    .expect("chord index belongs to the Voronoi edge table"),
            )
            .endpoints;
        Ok(Segment {
            start: self.xy(start, coordinates)?,
            end: self.xy(end, coordinates)?,
        })
    }

    fn certifies_incidence_pair(&self, left: usize, right: usize) -> bool {
        self.includes_polygons
            || [left, right].into_iter().any(|chord| {
                let edge = *self
                    .live_edges
                    .get(chord)
                    .expect("incidence-pair index belongs to the live edge table");
                self.subdivision.edge(edge).separator.is_some()
            })
    }

    fn clip_host_failures<A: SearchAccount>(
        &self,
        coordinates: &[XY],
        account: &mut A,
    ) -> Result<Vec<ClipHostFailure>> {
        if self.separator_chords.is_empty() {
            return Ok(Vec::new());
        }
        let certificate = clip_certificate_data(
            self.subdivision,
            self.clip,
            self.clip_trace,
            |vertex| self.xy(vertex, coordinates),
            account,
        )?;
        let clip_edge_count = certificate.edges.len();
        let separator_count = self.separator_chords.len();
        let index_units = clip_edge_count
            .div_ceil(16)
            .checked_add(separator_count.div_ceil(16))
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        account.charge_evaluations(
            u64::try_from(index_units).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
        )?;
        let bounds = |start: XY, end: XY| {
            Bounds::new_raw(
                start.x.min(end.x),
                start.y.min(end.y),
                start.x.max(end.x),
                start.y.max(end.y),
            )
        };
        let clip_bounds: Vec<_> = certificate
            .edges
            .iter()
            .map(|edge| bounds(edge.rounded[0], edge.rounded[1]))
            .collect();
        let mut segments = Vec::with_capacity(separator_count);
        for &chord in &self.separator_chords {
            segments.push(self.segment(chord, coordinates)?);
        }
        let separator_bounds: Vec<_> = segments
            .iter()
            .map(|segment| bounds(segment.start, segment.end))
            .collect();

        let sweep_count = clip_edge_count
            .checked_add(separator_count)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        account.charge_evaluations(
            u64::try_from(sweep_count.div_ceil(16))
                .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
        )?;
        let mut candidate_pairs = Vec::new();
        let mut callback_error = None;
        let completion = for_each_overlapping_bounds_cross_pair(
            &separator_bounds,
            &clip_bounds,
            |segment, edge| {
                if callback_error.is_some() {
                    return ControlFlow::Break(());
                }
                if let Err(error) = account.charge_evaluations(1) {
                    callback_error = Some(error);
                    return ControlFlow::Break(());
                }
                candidate_pairs.push((segment, edge));
                ControlFlow::Continue(())
            },
        )?;
        if let Some(error) = callback_error {
            return Err(error);
        }
        debug_assert!(completion.is_continue());
        candidate_pairs.sort_unstable();
        let candidate_units = u64::try_from(candidate_pairs.len().div_ceil(16))
            .map_err(|_| crate::error::ResourceError::SizeOverflow)?;
        account.charge_evaluations(candidate_units)?;

        let offset_count = separator_count
            .checked_add(1)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        let mut candidate_offsets = Vec::with_capacity(offset_count);
        let mut candidate_edges = Vec::with_capacity(candidate_pairs.len());
        candidate_offsets.push(0);
        let mut pairs = candidate_pairs.into_iter().peekable();
        for segment in 0..separator_count {
            while pairs
                .peek()
                .is_some_and(|&(candidate, _)| candidate == segment)
            {
                let (_, edge) = pairs
                    .next()
                    .expect("the candidate segment was checked above");
                candidate_edges.push(edge);
            }
            candidate_offsets.push(candidate_edges.len());
        }
        if pairs.next().is_some() {
            return Err(clip_source_trace_error());
        }

        account.charge_evaluations(
            u64::try_from(clip_edge_count.div_ceil(16))
                .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
        )?;
        let edge_ids: Vec<_> = (0..clip_edge_count).collect();
        let y_index = YStabbingIndex::build(edge_ids, |&edge| {
            let [start, end] = certificate
                .edges
                .get(edge)
                .expect("YStabbingIndex ordinals align with the assigned clip edges")
                .rounded;
            (
                start.x.min(end.x),
                start.x.max(end.x),
                start.y.min(end.y),
                start.y.max(end.y),
            )
        })?;
        account.charge_evaluations(
            u64::try_from(self.clip_trace.atom_count.div_ceil(16))
                .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
        )?;
        let point_index = PolygonPointIndex::build(&certificate.polygon)?;
        let mut events = Vec::new();
        let mut parity = ClipRingParity::new(self.clip_trace.rings.len());
        let mut failures = Vec::new();
        for (segment_index, (&chord, &[start, end])) in self
            .separator_chords
            .iter()
            .zip(candidate_offsets.array_windows::<2>())
            .enumerate()
        {
            let candidates = candidate_edges
                .get(start..end)
                .ok_or_else(clip_source_trace_error)?;
            let segment = *segments
                .get(segment_index)
                .expect("separator segment ordinal belongs to the built segment table");
            if let Some(boundary) = rounded_segment_covered_by_clip(
                segment,
                &certificate.edges,
                candidates,
                &point_index,
                &y_index,
                &mut events,
                &mut parity,
                account,
            )? {
                failures.push(ClipHostFailure { chord, boundary });
            }
        }
        Ok(failures)
    }

    fn separator_preserved(
        &self,
        chord: usize,
        mut xy_of: impl FnMut(VertexId) -> Result<XY>,
    ) -> Result<bool> {
        let edge_id = *self
            .live_edges
            .get(chord)
            .expect("separator chord belongs to the live edge table");
        let edge = self.subdivision.edge(edge_id);
        if edge.separator.is_none() {
            return Ok(true);
        }
        let [start, end] = edge.endpoints;
        let (start, end) = (
            exact::ExactPoint::from_xy(xy_of(start)?),
            exact::ExactPoint::from_xy(xy_of(end)?),
        );
        let side_of = |site: SiteId| {
            exact::orient_points(&start, &end, &site_by_id(self.sites, site).exact_point)
        };
        let (left, right) = match edge.forward.owner {
            OwnerTransition::Boundary { left, right } if left.is_some() || right.is_some() => {
                (left, right)
            },
            OwnerTransition::Boundary { .. } | OwnerTransition::Preserve => {
                return Err(voronoi_error("live separator has incomplete ownership"));
            },
        };
        let left_inside = match left {
            Some(site) => side_of(site)? == ExactSign::Positive,
            None => true,
        };
        let right_inside = match right {
            Some(site) => side_of(site)? == ExactSign::Negative,
            None => true,
        };
        Ok(left_inside && right_inside)
    }
}

#[derive(Clone, Copy, Debug)]
enum VoronoiConflict {
    Collision(VertexId, VertexId),
    Contact(usize, usize),
    ClipHost(ClipHostFailure),
    Fan(VertexId),
    Separator(usize),
    Ring(usize),
    Host {
        owner: SiteId,
        root: VertexId,
        hit: Option<usize>,
    },
}

impl VoronoiEmbedding<'_> {
    fn collect_conflicts(
        &self,
        coordinates: &[XY],
        budget: &mut EvaluationBudget,
    ) -> Result<Vec<VoronoiConflict>> {
        let mut conflicts = Vec::new();
        let mut owners = try_hash_map_with_capacity(self.live_nodes.len())?;
        for &vertex in self.live_nodes {
            let point = self.xy(vertex, coordinates)?;
            if let Some(previous) = owners.insert(PointKey::new(point), vertex)
                && previous != vertex
            {
                conflicts.push(VoronoiConflict::Collision(previous, vertex));
            }
        }

        let mut segments = Vec::with_capacity(self.chords.len());
        for chord in 0..self.chords.len() {
            segments.push(self.segment(chord, coordinates)?);
        }
        let pair_completion = for_each_candidate_pair(&segments, single_chain, |left, right| {
            if self.certifies_incidence_pair(left, right) {
                let endpoints = |index: usize| {
                    let chord = self.chord(index);
                    let [start, end] = chord.nodes;
                    let segment = *segments
                        .get(index)
                        .expect("candidate-pair index belongs to the emitted segment table");
                    [(NodeId(start), segment.start), (NodeId(end), segment.end)]
                };
                if !emitted_pair_preserves_incidence(endpoints(left), endpoints(right)) {
                    conflicts.push(VoronoiConflict::Contact(left, right));
                }
            }
            ControlFlow::<()>::Continue(())
        })?;
        debug_assert!(pair_completion.is_continue());

        let mut marked = Vec::new();
        let mut marked_boxes = Vec::new();
        let mut marked_chords = Vec::new();
        for (chord, &edge_id) in self.live_edges.iter().enumerate() {
            if self.subdivision.edge(edge_id).separator.is_none() {
                continue;
            }
            let segment = *segments
                .get(chord)
                .expect("separator chord belongs to the emitted segment table");
            marked.push([segment.start, segment.end]);
            marked_boxes.push(Some(Bounds::new_raw(
                segment.start.x.min(segment.end.x),
                segment.start.y.min(segment.end.y),
                segment.start.x.max(segment.end.x),
                segment.start.y.max(segment.end.y),
            )));
            marked_chords.push(chord);
        }
        let marked_index = BoxIndex::build(&marked_boxes)?;
        for site in self.sites {
            let query = &site.exact_point;
            let visit = |separator: usize| -> Result<()> {
                let [start, end] = *marked
                    .get(separator)
                    .expect("separator index belongs to the marked segment table");
                if exact::segment_intersection(
                    &exact::ExactPoint::from_xy(start),
                    &exact::ExactPoint::from_xy(end),
                    query,
                    query,
                )?
                .is_point()
                {
                    conflicts.push(VoronoiConflict::Separator(
                        *marked_chords
                            .get(separator)
                            .expect("marked edge maps to its emitted chord"),
                    ));
                }
                Ok(())
            };
            marked_index.for_each_containing(Some(site.rounded_xy()?), visit)?;
        }

        for chord in 0..self.live_edges.len() {
            if !self.separator_preserved(chord, |vertex| self.xy(vertex, coordinates))? {
                conflicts.push(VoronoiConflict::Separator(chord));
            }
        }
        for failure in self.clip_host_failures(coordinates, budget)? {
            conflicts.push(VoronoiConflict::ClipHost(failure));
        }

        for fan in &self.fans {
            let xy_of = |vertex| self.xy(vertex, coordinates);
            let mut rounded_order = Vec::new();
            if sort_fan(
                fan.origin,
                &fan.edges,
                FanSortMode::Rounded,
                self.subdivision,
                &xy_of,
                &mut rounded_order,
            )? != FanSortOutcome::Sorted
                || rounded_order != fan.exact_order
            {
                conflicts.push(VoronoiConflict::Fan(fan.origin));
            }
        }
        for ring in 0..self.rings.len() {
            if !self.ring_orientation(ring, coordinates)? {
                conflicts.push(VoronoiConflict::Ring(ring));
            }
        }

        let points = VoronoiPoints {
            vertices: self.vertices,
            coordinates,
        };
        for host_graph in &self.host_graphs {
            let owner_points = VoronoiOwnerPoints {
                points: &points,
                vertices: &host_graph.vertices,
            };
            let host_completion = visit_component_faces(
                &owner_points,
                host_graph.vertices.len(),
                host_graph.edges.len(),
                |edge| host_graph.edge(edge),
                |edge| host_graph.side_keys(edge),
                |root, hit| {
                    conflicts.push(VoronoiConflict::Host {
                        owner: host_graph.owner,
                        root: host_graph.vertex(root),
                        hit: hit.map(|edge| host_graph.chord(edge)),
                    });
                    ControlFlow::Continue(())
                },
            )?;
            debug_assert!(host_completion.is_continue());
        }
        Ok(conflicts)
    }

    fn seed_nodes(&self, conflicts: &[VoronoiConflict]) -> Vec<VertexId> {
        let mut seeds = Vec::new();
        for &conflict in conflicts {
            match conflict {
                VoronoiConflict::Collision(first, second) => {
                    seeds.extend([first, second]);
                },
                VoronoiConflict::Contact(left, right) => {
                    for chord in [left, right] {
                        let edge = self.chord(chord);
                        seeds.extend([VertexId(edge.nodes[0]), VertexId(edge.nodes[1])]);
                    }
                },
                VoronoiConflict::ClipHost(failure) => {
                    let chord = self.chord(failure.chord);
                    seeds.extend([VertexId(chord.nodes[0]), VertexId(chord.nodes[1])]);
                    self.visit_clip_ring_vertices(failure.boundary.ring, |node| {
                        seeds.push(node);
                    });
                },
                VoronoiConflict::Fan(origin) => {
                    seeds.push(origin);
                    if let Some(fan) = self.fans.iter().find(|fan| fan.origin == origin) {
                        for edge_id in &fan.edges {
                            let [start, end] = self.subdivision.edge(*edge_id).endpoints;
                            seeds.extend([start, end]);
                        }
                    }
                },
                VoronoiConflict::Separator(chord) => {
                    let edge = self.chord(chord);
                    seeds.extend([VertexId(edge.nodes[0]), VertexId(edge.nodes[1])]);
                },
                VoronoiConflict::Ring(ring) => self.extend_ring_seeds(ring, &mut seeds),
                VoronoiConflict::Host { owner, root, hit } => {
                    let rings = self
                        .rings_by_node
                        .get(root.0 as usize)
                        .expect("host root belongs to the Voronoi vertex domain");
                    let mut found_owner_ring = false;
                    for ring in rings
                        .iter()
                        .copied()
                        .filter(|&ring| self.ring_owner(ring) == owner)
                    {
                        found_owner_ring = true;
                        self.extend_ring_seeds(ring, &mut seeds);
                    }
                    if !found_owner_ring {
                        seeds.push(root);
                    }
                    if let Some(hit) = hit {
                        let edge = self.chord(hit);
                        seeds.extend([VertexId(edge.nodes[0]), VertexId(edge.nodes[1])]);
                    }
                },
            }
        }
        seeds.sort_unstable();
        seeds.dedup();
        seeds
    }
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

impl<'p, 'a> VoronoiProblem<'p, 'a> {
    fn new(embedding: &'p VoronoiEmbedding<'a>, seeds: &[VertexId]) -> Result<Self> {
        let node_count = embedding.subdivision.vertices.len();
        let mut movable_cache = vec![None; node_count];
        let mut search_seeds = seeds.to_vec();
        search_seeds.sort_unstable();
        search_seeds.dedup();
        let seed_ids: Vec<_> = seeds.iter().map(|node| node.0).collect();
        let proximity = build_proximity_classes(
            node_count,
            &embedding.chords,
            &embedding.incident_chords,
            &seed_ids,
            &mut movable_cache,
            |node| Ok(embedding.exact_movable(VertexId(node))),
            |left, right| {
                let (left, right) = (VertexId(left), VertexId(right));
                let left_seeded = search_seeds.binary_search(&left).is_ok();
                let right_seeded = search_seeds.binary_search(&right).is_ok();
                Ok(if left_seeded == right_seeded {
                    embedding.compare_nodes(left, right)?
                } else {
                    right_seeded.cmp(&left_seeded)
                })
            },
            segments_within_chebyshev,
        )?;
        let classes: Vec<Vec<VertexId>> = proximity
            .classes
            .into_iter()
            .map(|class| class.into_iter().map(VertexId).collect())
            .collect();
        Ok(Self {
            embedding,
            classes,
            search_seeds,
            node_location: proximity.node_location,
            radius_cache: RefCell::new(proximity.radii),
            movable_cache: RefCell::new(movable_cache),
            proximity: RefCell::new(None),
            initial_evaluations: proximity.initial_evaluations,
        })
    }

    fn local_xy(&self, node: VertexId, class: usize, assigned: &[Option<XY>]) -> XY {
        match self.node_location.get(node.0 as usize).copied().flatten() {
            Some((owner, rank)) if owner == class => assigned
                .get(rank)
                .copied()
                .flatten()
                .expect("ready Voronoi predicates have each class endpoint assigned"),
            _ => self
                .embedding
                .xy(node, self.embedding.coordinates)
                .expect("every checked Voronoi vertex is live"),
        }
    }

    fn predicate_class_rank(
        &self,
        nodes: impl IntoIterator<Item = VertexId>,
    ) -> Result<Option<(usize, usize)>> {
        let mut class = None;
        let mut ready_rank = 0;
        for node in nodes {
            if let Some((owner, rank)) = self.node_location.get(node.0 as usize).copied().flatten()
            {
                if class.is_some_and(|known| known != owner) {
                    return Err(voronoi_error(
                        "embedding proximity split one Voronoi predicate across classes",
                    ));
                }
                class = Some(owner);
                ready_rank = ready_rank.max(rank);
            }
        }
        Ok(class.map(|class| (class, ready_rank)))
    }

    fn ring_class_rank(&self, nodes: impl IntoIterator<Item = VertexId>) -> Option<(usize, usize)> {
        let mut class = None;
        let mut ready_rank = 0;
        for node in nodes {
            if let Some((owner, rank)) = self.node_location.get(node.0 as usize).copied().flatten()
            {
                if class.is_some_and(|known| known != owner) {
                    return None;
                }
                class = Some(owner);
                ready_rank = ready_rank.max(rank);
            }
        }
        class.map(|class| (class, ready_rank))
    }

    fn prepare_proximity<A: SearchAccount>(&self, account: &mut A) -> Result<()> {
        if self.proximity.borrow().is_some() {
            return Ok(());
        }
        let all_bounds: Vec<_> = self
            .embedding
            .chords
            .iter()
            .copied()
            .map(possible_swept_bounds)
            .collect();
        let mut left_indices = Vec::new();
        let mut left_bounds = Vec::new();
        for (index, chord) in self.embedding.chords.iter().enumerate() {
            if chord.nodes.iter().any(|&node| {
                self.node_location
                    .get(node as usize)
                    .copied()
                    .flatten()
                    .is_some()
            }) {
                left_indices.push(index);
                left_bounds.push(
                    *all_bounds
                        .get(index)
                        .expect("Voronoi chord bounds align with the chord table"),
                );
            }
        }
        account.charge_evaluations(
            u64::try_from(self.embedding.chords.len().div_ceil(16))
                .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
        )?;
        let mut pairs = Vec::new();
        let mut radius_cache = self.radius_cache.borrow_mut();
        let mut movable_cache = self.movable_cache.borrow_mut();
        let mut callback_error = None;
        let flow =
            for_each_overlapping_bounds_cross_pair(&left_bounds, &all_bounds, |left, right| {
                if callback_error.is_some() {
                    return ControlFlow::Break(());
                }
                let left_index = *left_indices
                    .get(left)
                    .expect("overlap pair left index belongs to the frontier table");
                if left_index == right {
                    return ControlFlow::Continue(());
                }
                let left_chord = self.embedding.chord(left_index);
                let right_chord = self.embedding.chord(right);
                if !self.embedding.certifies_incidence_pair(left_index, right) {
                    return ControlFlow::Continue(());
                }
                let radii = (|| {
                    Ok((
                        exact_chord_radius(
                            left_chord,
                            &mut radius_cache,
                            left_index,
                            &mut movable_cache,
                            &mut |node| Ok(self.embedding.exact_movable(VertexId(node))),
                        )?,
                        exact_chord_radius(
                            right_chord,
                            &mut radius_cache,
                            right,
                            &mut movable_cache,
                            &mut |node| Ok(self.embedding.exact_movable(VertexId(node))),
                        )?,
                    ))
                })();
                let (left_radius, right_radius) = match radii {
                    Ok(radii) => radii,
                    Err(error) => {
                        callback_error = Some(error);
                        return ControlFlow::Break(());
                    },
                };
                if segments_within_chebyshev(
                    left_chord.nearest,
                    right_chord.nearest,
                    proximity_limit(left_radius, right_radius),
                ) {
                    pairs.push((left_index.min(right), left_index.max(right)));
                }
                ControlFlow::Continue(())
            })?;
        if let Some(error) = callback_error {
            return Err(error);
        }
        debug_assert!(flow.is_continue());
        pairs.sort_unstable();
        pairs.dedup();
        drop(radius_cache);
        drop(movable_cache);

        let mut sites = Vec::with_capacity(self.embedding.sites.len());
        for site in self.embedding.sites {
            let point = site.rounded_xy()?;
            sites.push(Bounds::new_raw(point.x, point.y, point.x, point.y));
        }
        let mut separator_bounds = Vec::new();
        let mut separator_indices = Vec::new();
        for &chord in &self.embedding.separator_chords {
            if self.embedding.chord(chord).nodes.iter().any(|&node| {
                self.node_location
                    .get(node as usize)
                    .copied()
                    .flatten()
                    .is_some()
            }) {
                separator_indices.push(chord);
                separator_bounds.push(
                    *all_bounds
                        .get(chord)
                        .expect("separator bounds align with the emitted chord table"),
                );
            }
        }
        account.charge_evaluations(
            u64::try_from(sites.len().div_ceil(16))
                .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
        )?;
        let mut site_pairs = Vec::new();
        let site_pair_completion = for_each_overlapping_bounds_cross_pair(
            &separator_bounds,
            &sites,
            |separator, site| {
                site_pairs.push((
                    *separator_indices
                        .get(separator)
                        .expect("separator index belongs to the prepared table"),
                    site,
                ));
                ControlFlow::<()>::Continue(())
            },
        )?;
        debug_assert!(site_pair_completion.is_continue());
        site_pairs.sort_unstable();
        site_pairs.dedup();

        let mut classes = Vec::with_capacity(self.classes.len());
        for variables in &self.classes {
            classes.push(VoronoiClassState {
                pairs_by_rank: vec![Vec::new(); variables.len()],
                separators_by_rank: vec![Vec::new(); variables.len()],
                site_pairs_by_rank: vec![Vec::new(); variables.len()],
                fans_by_rank: vec![Vec::new(); variables.len()],
                rings_by_rank: vec![Vec::new(); variables.len()],
            });
        }
        for (pair_index, &(left, right)) in pairs.iter().enumerate() {
            let left = self.embedding.chord(left);
            let right = self.embedding.chord(right);
            if let Some((class, rank)) = self.predicate_class_rank([
                VertexId(left.nodes[0]),
                VertexId(left.nodes[1]),
                VertexId(right.nodes[0]),
                VertexId(right.nodes[1]),
            ])? {
                classes
                    .get_mut(class)
                    .expect("pair class belongs to the Voronoi problem")
                    .pairs_by_rank
                    .get_mut(rank)
                    .expect("pair rank belongs to its Voronoi class")
                    .push(pair_index);
            }
        }
        for (pair_index, &(chord, _site)) in site_pairs.iter().enumerate() {
            let edge = self.embedding.chord(chord);
            if let Some((class, rank)) =
                self.predicate_class_rank([VertexId(edge.nodes[0]), VertexId(edge.nodes[1])])?
            {
                classes
                    .get_mut(class)
                    .expect("site-pair class belongs to the Voronoi problem")
                    .site_pairs_by_rank
                    .get_mut(rank)
                    .expect("site-pair rank belongs to its Voronoi class")
                    .push(pair_index);
            }
        }
        for &chord in &self.embedding.separator_chords {
            let edge = self.embedding.chord(chord);
            if let Some((class, rank)) =
                self.predicate_class_rank([VertexId(edge.nodes[0]), VertexId(edge.nodes[1])])?
            {
                classes
                    .get_mut(class)
                    .expect("separator class belongs to the Voronoi problem")
                    .separators_by_rank
                    .get_mut(rank)
                    .expect("separator rank belongs to its Voronoi class")
                    .push(chord);
            }
        }
        for (fan_index, fan) in self.embedding.fans.iter().enumerate() {
            let mut nodes = Vec::with_capacity(fan.edges.len().saturating_mul(2).saturating_add(1));
            nodes.push(fan.origin);
            for edge_id in &fan.edges {
                let [start, end] = self.embedding.subdivision.edge(*edge_id).endpoints;
                nodes.extend([start, end]);
            }
            nodes.sort_unstable();
            nodes.dedup();
            if let Some((class, rank)) = self.predicate_class_rank(nodes.iter().copied())? {
                classes
                    .get_mut(class)
                    .expect("fan class belongs to the Voronoi problem")
                    .fans_by_rank
                    .get_mut(rank)
                    .expect("fan rank belongs to its Voronoi class")
                    .push(fan_index);
            }
        }
        for (ring_index, ring) in self.embedding.rings.iter().copied().enumerate() {
            let exact_ring = ring.exact(self.embedding.parts);
            let (_, open_vertices) = exact_ring
                .vertices
                .split_last()
                .expect("simple Voronoi rings are closed");
            if let Some((class, rank)) = self.ring_class_rank(open_vertices.iter().copied()) {
                classes
                    .get_mut(class)
                    .expect("ring class belongs to the Voronoi problem")
                    .rings_by_rank
                    .get_mut(rank)
                    .expect("ring rank belongs to its Voronoi class")
                    .push(ring_index);
            }
        }
        *self.proximity.borrow_mut() = Some(VoronoiProximity {
            pairs,
            site_pairs,
            classes,
        });
        Ok(())
    }
}

impl VoronoiProblem<'_, '_> {
    fn mark_node_class(&self, node: VertexId, blamed: &mut [bool]) {
        if let Some((class, _)) = self.node_location.get(node.0 as usize).copied().flatten() {
            *blamed
                .get_mut(class)
                .expect("class ID belongs to the Voronoi search") = true;
        }
    }

    fn check_pair(&self, left: usize, right: usize, class: usize, assigned: &[Option<XY>]) -> bool {
        let endpoints = |chord_index: usize| {
            let chord = self.embedding.chord(chord_index);
            let [start, end] = chord.nodes;
            [
                (
                    NodeId(start),
                    self.local_xy(VertexId(start), class, assigned),
                ),
                (NodeId(end), self.local_xy(VertexId(end), class, assigned)),
            ]
        };
        emitted_pair_preserves_incidence(endpoints(left), endpoints(right))
    }

    fn check_fan(&self, fan_index: usize, class: usize, assigned: &[Option<XY>]) -> Result<bool> {
        let fan = self
            .embedding
            .fans
            .get(fan_index)
            .expect("Voronoi fan index belongs to the prepared fan table");
        let xy_of = |node| Ok(self.local_xy(node, class, assigned));
        let mut rounded_order = Vec::new();
        match sort_fan(
            fan.origin,
            &fan.edges,
            FanSortMode::Rounded,
            self.embedding.subdivision,
            &xy_of,
            &mut rounded_order,
        )? {
            FanSortOutcome::Sorted => Ok(rounded_order == fan.exact_order),
            FanSortOutcome::AngularTie => Ok(false),
        }
    }

    fn check_ring_orientation(
        &self,
        ring_index: usize,
        class: usize,
        assigned: &[Option<XY>],
    ) -> Result<bool> {
        let ring = self
            .embedding
            .rings
            .get(ring_index)
            .copied()
            .expect("ring index belongs to the Voronoi ring table")
            .exact(self.embedding.parts);
        rounded_ring_orientation_matches(&ring.vertices, ring.orientation, |vertex| {
            Ok(self.local_xy(vertex, class, assigned))
        })
    }

    fn check_site_pair(&self, pair: usize, class: usize, assigned: &[Option<XY>]) -> Result<bool> {
        let proximity = self.proximity.borrow();
        let proximity = proximity
            .as_ref()
            .expect("site-pair rows are prepared before local search");
        let (chord, site) = *proximity
            .site_pairs
            .get(pair)
            .expect("site-pair ordinal belongs to the prepared table");
        let [start_node, end_node] = self.embedding.chord(chord).nodes;
        let [start, end] = [
            self.local_xy(VertexId(start_node), class, assigned),
            self.local_xy(VertexId(end_node), class, assigned),
        ];
        let start = exact::ExactPoint::from_xy(start);
        let end = exact::ExactPoint::from_xy(end);
        let site = exact::ExactPoint::from_xy(
            self.embedding
                .sites
                .get(site)
                .expect("site-pair site belongs to the canonical site table")
                .point
                .xy(),
        );
        Ok(!exact::segment_intersection(&start, &end, &site, &site)?.is_point())
    }

    fn check_separator(&self, chord: usize, class: usize, assigned: &[Option<XY>]) -> Result<bool> {
        self.embedding
            .separator_preserved(chord, |vertex| Ok(self.local_xy(vertex, class, assigned)))
    }
}

impl Problem for VoronoiProblem<'_, '_> {
    type Node = VertexId;
    type Candidate = XY;
    type Key = PointKey;
    type ClassState = VoronoiClassState;
    type Output = ();

    fn classes(&self) -> &[Vec<Self::Node>] {
        &self.classes
    }

    fn fixed_keys<W: Work>(
        &self,
        class: usize,
        _variables: &[Self::Node],
        work: &mut W,
    ) -> Result<Vec<Self::Key>> {
        work.charge_product_into_error(self.embedding.live_nodes.len(), 1)?;
        let mut keys = Vec::with_capacity(self.embedding.live_nodes.len());
        for &node in self.embedding.live_nodes {
            work.charge_into_error(1)?;
            if self
                .node_location
                .get(node.0 as usize)
                .copied()
                .flatten()
                .is_none_or(|(owner, _)| owner != class)
            {
                work.charge_into_error(1)?;
                keys.push(PointKey::new(
                    self.embedding.xy(node, self.embedding.coordinates)?,
                ));
            }
        }
        Ok(keys)
    }

    fn compare_nodes<W: Work>(
        &self,
        left: Self::Node,
        right: Self::Node,
        _work: &mut W,
    ) -> Result<Ordering> {
        let left_seeded = self.search_seeds.binary_search(&left).is_ok();
        let right_seeded = self.search_seeds.binary_search(&right).is_ok();
        Ok(if left_seeded == right_seeded {
            self.embedding.compare_nodes(left, right)?
        } else {
            right_seeded.cmp(&left_seeded)
        })
    }

    fn fill_candidates<W: Work>(
        &self,
        node: Self::Node,
        candidates: &mut Vec<Self::Candidate>,
        work: &mut W,
    ) -> Result<()> {
        let nearest = self.embedding.xy(node, self.embedding.coordinates)?;
        let x_values = [nearest.x.next_down(), nearest.x, nearest.x.next_up()];
        let y_values = [nearest.y.next_down(), nearest.y, nearest.y.next_up()];
        let candidate_count = x_values
            .len()
            .checked_mul(y_values.len())
            .expect("three candidate coordinates per axis fit in usize");
        work.charge_product_into_error(candidate_count, 1)?;
        candidates.reserve(candidate_count);
        for &x in &x_values {
            for &y in &y_values {
                let candidate = XY::new(x, y);
                if candidate.x.is_finite() && candidate.y.is_finite() {
                    candidates.push(candidate);
                }
            }
        }
        Ok(())
    }

    fn compare_candidates<W: Work>(
        &self,
        node: Self::Node,
        left: Self::Candidate,
        right: Self::Candidate,
        _work: &mut W,
    ) -> Result<Ordering> {
        let exact = &self.embedding.subdivision.vertex(node).point;
        Ok(exact::squared_distance_cmp_point(exact, left, right)?
            .then_with(|| left.x.total_cmp(&right.x))
            .then_with(|| left.y.total_cmp(&right.y)))
    }

    fn candidate_key(&self, candidate: Self::Candidate) -> Self::Key {
        PointKey::new(candidate)
    }

    fn prepare_class<A: SearchAccount>(
        &self,
        class: usize,
        _variables: &[Self::Node],
        account: &mut A,
    ) -> Result<Self::ClassState> {
        self.prepare_proximity(account)?;
        let prepared = self.proximity.borrow();
        Ok(prepared
            .as_ref()
            .expect("Voronoi proximity state is prepared before class search")
            .classes
            .get(class)
            .expect("class ID belongs to the Voronoi search")
            .clone())
    }

    fn check_local<A: SearchAccount>(
        &self,
        class_state: &Self::ClassState,
        class: usize,
        _variables: &[Self::Node],
        rank: usize,
        assigned: &[Option<Self::Candidate>],
        account: &mut A,
    ) -> Result<bool> {
        let prepared = self.proximity.borrow();
        let prepared = prepared
            .as_ref()
            .expect("Voronoi proximity state is prepared before local search");
        for &pair in class_state
            .pairs_by_rank
            .get(rank)
            .expect("rank belongs to the Voronoi class")
        {
            account.charge_evaluations(1)?;
            let (left, right) = *prepared
                .pairs
                .get(pair)
                .expect("pair index belongs to the Voronoi proximity table");
            if !self.check_pair(left, right, class, assigned) {
                return Ok(false);
            }
        }
        for &chord in class_state
            .separators_by_rank
            .get(rank)
            .expect("rank belongs to the Voronoi class")
        {
            account.charge_evaluations(1)?;
            if !self.check_separator(chord, class, assigned)? {
                return Ok(false);
            }
        }
        for &site_pair in class_state
            .site_pairs_by_rank
            .get(rank)
            .expect("rank belongs to the Voronoi class")
        {
            account.charge_evaluations(1)?;
            if !self.check_site_pair(site_pair, class, assigned)? {
                return Ok(false);
            }
        }
        for &fan in class_state
            .fans_by_rank
            .get(rank)
            .expect("rank belongs to the Voronoi class")
        {
            account.charge_evaluations(1)?;
            if !self.check_fan(fan, class, assigned)? {
                return Ok(false);
            }
        }
        for &ring in class_state
            .rings_by_rank
            .get(rank)
            .expect("rank belongs to the Voronoi class")
        {
            let vertices = self
                .embedding
                .ring(ring)
                .exact(self.embedding.parts)
                .vertices
                .len();
            account.charge_evaluations(
                u64::try_from(vertices.div_ceil(16))
                    .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
            )?;
            if !self.check_ring_orientation(ring, class, assigned)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    fn check_global<A: SearchAccount>(
        &mut self,
        assignments: &[ClassAssignment<'_, Self::Node, Self::Candidate>],
        account: &mut A,
    ) -> Result<GlobalCheck<Self::Output>> {
        let mut coordinates = self.embedding.coordinates.to_vec();
        for assignment in assignments {
            for (&node, &candidate) in assignment.variables.iter().zip(assignment.values) {
                self.embedding
                    .vertices
                    .set_point(node, &mut coordinates, candidate);
            }
        }
        let ring_vertices = self
            .embedding
            .rings
            .iter()
            .try_fold(0_usize, |count, ring| {
                count
                    .checked_add(ring.exact(self.embedding.parts).vertices.len())
                    .ok_or(crate::error::ResourceError::SizeOverflow)
            })?;
        let host_items = self
            .embedding
            .host_graphs
            .iter()
            .try_fold(0_usize, |count, graph| {
                count
                    .checked_add(graph.vertices.len())
                    .and_then(|count| count.checked_add(graph.edges.len()))
                    .ok_or(crate::error::ResourceError::SizeOverflow)
            })?;
        let units = ring_vertices
            .div_ceil(16)
            .checked_add(host_items.div_ceil(16))
            .and_then(|units| units.checked_add(self.embedding.live_nodes.len().div_ceil(16)))
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        account.charge_evaluations(
            u64::try_from(units).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
        )?;

        let mut blamed = vec![false; self.classes.len()];
        let mut rejected = false;
        let mut coordinates_by_key = try_hash_map_with_capacity(self.embedding.live_nodes.len())?;
        for &node in self.embedding.live_nodes {
            let point = self.embedding.xy(node, &coordinates)?;
            if let Some(previous) = coordinates_by_key.insert(PointKey::new(point), node)
                && previous != node
            {
                rejected = true;
                self.mark_node_class(previous, &mut blamed);
                self.mark_node_class(node, &mut blamed);
            }
        }
        for ring in 0..self.embedding.rings.len() {
            if !self.embedding.ring_orientation(ring, &coordinates)? {
                rejected = true;
                let exact = self.embedding.ring(ring).exact(self.embedding.parts);
                let (_, open_vertices) = exact
                    .vertices
                    .split_last()
                    .expect("simple Voronoi rings are closed");
                for &node in open_vertices {
                    self.mark_node_class(node, &mut blamed);
                }
            }
        }
        for failure in self.embedding.clip_host_failures(&coordinates, account)? {
            rejected = true;
            let chord = self.embedding.chord(failure.chord);
            self.mark_node_class(VertexId(chord.nodes[0]), &mut blamed);
            self.mark_node_class(VertexId(chord.nodes[1]), &mut blamed);
            self.embedding
                .visit_clip_ring_vertices(failure.boundary.ring, |node| {
                    self.mark_node_class(node, &mut blamed);
                });
            if let Some(atom) = failure.boundary.atom {
                for node in self.embedding.subdivision.edge(atom).endpoints {
                    self.mark_node_class(node, &mut blamed);
                }
            }
        }
        let points = VoronoiPoints {
            vertices: self.embedding.vertices,
            coordinates: &coordinates,
        };
        for host_graph in &self.embedding.host_graphs {
            let owner_points = VoronoiOwnerPoints {
                points: &points,
                vertices: &host_graph.vertices,
            };
            let host_flow = visit_component_faces(
                &owner_points,
                host_graph.vertices.len(),
                host_graph.edges.len(),
                |edge| host_graph.edge(edge),
                |edge| host_graph.side_keys(edge),
                |root, hit| {
                    rejected = true;
                    let root = host_graph.vertex(root);
                    let rings = self
                        .embedding
                        .rings_by_node
                        .get(root.0 as usize)
                        .expect("host root belongs to the Voronoi vertex domain");
                    let mut found_owner_ring = false;
                    for ring in rings
                        .iter()
                        .copied()
                        .filter(|&ring| self.embedding.ring_owner(ring) == host_graph.owner)
                    {
                        found_owner_ring = true;
                        let exact = self.embedding.ring(ring).exact(self.embedding.parts);
                        let (_, open_vertices) = exact
                            .vertices
                            .split_last()
                            .expect("simple Voronoi rings are closed");
                        for &node in open_vertices {
                            self.mark_node_class(node, &mut blamed);
                        }
                    }
                    if !found_owner_ring {
                        self.mark_node_class(root, &mut blamed);
                    }
                    if let Some(hit) = hit {
                        let edge = self.embedding.chord(host_graph.chord(hit));
                        self.mark_node_class(VertexId(edge.nodes[0]), &mut blamed);
                        self.mark_node_class(VertexId(edge.nodes[1]), &mut blamed);
                    }
                    ControlFlow::Continue(())
                },
            )?;
            debug_assert!(host_flow.is_continue());
        }
        let classes = blamed
            .into_iter()
            .enumerate()
            .filter_map(|(class, blamed)| blamed.then_some(class))
            .collect::<Vec<_>>();
        Ok(if rejected {
            GlobalCheck::Reject(classes)
        } else {
            GlobalCheck::Pass(())
        })
    }

    fn initial_evaluations(&self) -> u64 {
        self.initial_evaluations
    }
}

fn rounded_fan_angle_cmp(origin: XY, left: XY, right: XY) -> Option<Ordering> {
    let half = |target: XY| target.y > origin.y || (target.y == origin.y && target.x >= origin.x);
    match (half(left), half(right)) {
        (true, false) => Some(Ordering::Less),
        (false, true) => Some(Ordering::Greater),
        _ => match exact::orient2d(origin, left, right) {
            ExactSign::Positive => Some(Ordering::Less),
            ExactSign::Negative => Some(Ordering::Greater),
            ExactSign::Zero => None,
        },
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FanSortMode {
    Exact,
    Rounded,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum FanSortOutcome {
    Sorted,
    AngularTie,
}

fn sort_fan(
    vertex: VertexId,
    incident: &[EdgeId],
    mode: FanSortMode,
    subdivision: &ExactSubdivision,
    xy_of: &impl Fn(VertexId) -> Result<XY>,
    order: &mut Vec<EdgeId>,
) -> Result<FanSortOutcome> {
    let exact_origin = &subdivision.vertex(vertex).point;
    let rounded_origin = (mode == FanSortMode::Rounded)
        .then(|| xy_of(vertex))
        .transpose()?;
    let compare = |left: EdgeId, right: EdgeId| -> Result<Option<Ordering>> {
        let left = fan_target(subdivision, left, vertex);
        let right = fan_target(subdivision, right, vertex);
        if let Some(origin) = rounded_origin {
            Ok(rounded_fan_angle_cmp(origin, xy_of(left)?, xy_of(right)?))
        } else {
            exact::angle_ccw_cmp(
                exact_origin,
                &subdivision.vertex(left).point,
                &subdivision.vertex(right).point,
            )
            .map(Some)
        }
    };

    order.clear();
    order.reserve(incident.len());
    order.extend_from_slice(incident);
    try_sort_by(order, |&left, &right| {
        Ok(compare(left, right)?.unwrap_or(Ordering::Equal))
    })?;
    for &[left, right] in order.array_windows::<2>() {
        if compare(left, right)?.is_none() {
            return Ok(FanSortOutcome::AngularTie);
        }
    }
    if let Some((&first, rest)) = order.split_first()
        && let Some(&last) = rest.last()
        && compare(last, first)?.is_none()
    {
        return Ok(FanSortOutcome::AngularTie);
    }
    if let Some((least, _)) = order.iter().enumerate().min_by_key(|(_, edge)| *edge) {
        order.rotate_left(least);
    }
    Ok(FanSortOutcome::Sorted)
}

fn rounded_ring_orientation_matches(
    vertices: &[VertexId],
    orientation: ExactSign,
    mut xy_of: impl FnMut(VertexId) -> Result<XY>,
) -> Result<bool> {
    let (_, open_vertices) = vertices
        .split_last()
        .expect("simple Voronoi rings are closed");
    let rounded = open_vertices
        .iter()
        .map(|&vertex| xy_of(vertex))
        .collect::<Result<Vec<_>>>()?;
    Ok(stored_xy_cycle_matches_orientation(&rounded, orientation))
}

fn stored_xy_cycle_matches_orientation(rounded: &[XY], orientation: ExactSign) -> bool {
    match open_xy_cycle_winding(rounded) {
        RingWinding::Clockwise => orientation == ExactSign::Negative,
        RingWinding::CounterClockwise => orientation == ExactSign::Positive,
        RingWinding::Degenerate => false,
    }
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

fn embed(
    subdivision: &ExactSubdivision,
    parts: &[ExactCellPart],
    chains: &[ExactEdgeChain],
    clip: &NormalizedClip<'_>,
    sites: &[Site],
    product: VoronoiProduct,
    budget: &mut ExpansionBudget,
    producer: VoronoiProducer,
    chart: Option<&PointSelectionChart>,
) -> Result<EmbeddedVoronoi> {
    let mut live = LiveVertices::new(subdivision.vertices.len());
    let mut live_edges = Vec::new();
    for part in parts {
        for ring in std::iter::once(&part.shell).chain(&part.holes) {
            live_edges.reserve(ring.edges.len());
            live_edges.extend_from_slice(&ring.edges);
            for vertex in &ring.vertices {
                live.mark(*vertex);
            }
        }
    }
    for chain in chains {
        live_edges.reserve(chain.edges.len());
        live_edges.extend_from_slice(&chain.edges);
        for vertex in &chain.vertices {
            live.mark(*vertex);
        }
    }
    for (edge_index, edge) in subdivision.edges.iter().enumerate() {
        if !edge.clip_sources.is_empty() {
            live_edges.push(EdgeId(edge_index as u32));
            for vertex in edge.endpoints {
                live.mark(vertex);
            }
        }
    }
    live_edges.sort_unstable();
    live_edges.dedup();
    for point in clip.rings.iter().flatten() {
        let vertex = subdivision
            .vertex_at(point)?
            .ok_or_else(|| voronoi_error("exact clip vertex is absent from the snap arena"))?;
        live.mark(vertex);
    }
    let mut points = Vec::new();
    let mut exact_to_embedded = EmbeddedVertices::new(subdivision.vertices.len());
    let mut live_nodes = Vec::new();
    for (index, (vertex, &is_live)) in subdivision.vertices.iter().zip(live.iter()).enumerate() {
        if !is_live {
            continue;
        }
        let point = vertex
            .fixed_xy
            .map_or_else(|| vertex.point.round_nearest_even(), Ok)?;
        let embedded = EmbeddedVertexId(points.len() as u32);
        let vertex = VertexId(index as u32);
        exact_to_embedded.set(vertex, embedded);
        live_nodes.push(vertex);
        points.push(point);
    }
    live_nodes.sort_unstable();
    let operation = if product.includes_polygons() {
        "voronoi_cells"
    } else {
        "voronoi_edge_lines"
    };
    let mut search_budget = EvaluationBudget::new(operation, 0)?;
    let clip_trace = ClipTrace::build(subdivision, clip, &mut search_budget)?;
    let (assignment, constructed) = {
        let embedding = VoronoiEmbedding::new(
            subdivision,
            clip,
            &clip_trace,
            parts,
            product.includes_polygons(),
            &live_nodes,
            &live_edges,
            &exact_to_embedded,
            &points,
            sites,
        )?;
        let conflicts = embedding.collect_conflicts(&points, &mut search_budget)?;
        if conflicts.is_empty() {
            (None, 0)
        } else {
            let seeds = embedding.seed_nodes(&conflicts);
            let problem = VoronoiProblem::new(&embedding, &seeds)?;
            let constructed = problem.classes.iter().try_fold(0_usize, |count, class| {
                count
                    .checked_add(class.len())
                    .ok_or(crate::error::ResourceError::SizeOverflow)
            })?;
            search_budget.set_constructed(constructed)?;
            (
                Some(Search::with_budget(problem, search_budget).run()?),
                constructed,
            )
        }
    };
    if let Some(assignment) = assignment {
        for class in assignment.classes {
            for (node, candidate) in class.variables.into_iter().zip(class.values) {
                exact_to_embedded.set_point(node, &mut points, candidate);
            }
        }
        let embedding = VoronoiEmbedding::new(
            subdivision,
            clip,
            &clip_trace,
            parts,
            product.includes_polygons(),
            &live_nodes,
            &live_edges,
            &exact_to_embedded,
            &points,
            sites,
        )?;
        let mut verification_budget = EvaluationBudget::new(operation, constructed)?;
        if !embedding
            .collect_conflicts(&points, &mut verification_budget)?
            .is_empty()
        {
            return Err(exact_embedding_unrepresentable(operation, constructed));
        }
    }
    let mut published = 0_usize;
    if product.includes_polygons() {
        for part in parts {
            let part_count = polygon_output_count(
                part.shell.vertices.len(),
                part.holes.iter().map(|hole| hole.vertices.len()),
            )?;
            published = published
                .checked_add(part_count)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
        }
    }
    let edge_positions = if product.includes_edges() {
        chains.iter().try_fold(0_usize, |count, chain| {
            count
                .checked_add(chain.vertices.len())
                .ok_or(crate::error::ResourceError::SizeOverflow)
        })?
    } else {
        0
    };
    published = published
        .checked_add(edge_positions)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    if product.includes_edges() {
        published = published
            .checked_add(chains.len())
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
    }
    let baseline = if product.includes_polygons() {
        clip.polygon_baseline.unwrap_or(0)
    } else {
        0
    };
    budget.add(published.saturating_sub(baseline))?;
    let mut receipt = exact_diagram_receipt(
        producer,
        sites,
        subdivision,
        parts,
        chains,
        clip,
        &clip_trace,
        &exact_to_embedded,
        &points,
        chart,
    )?;
    let mut labelled = Vec::with_capacity(if product.includes_polygons() {
        parts.len()
    } else {
        0
    });
    for part in parts {
        if walk_label(subdivision, part.face).owner != Some(part.owner) {
            return Err(voronoi_error("embedded cell lost its exact face owner"));
        }
        if !product.includes_polygons() {
            continue;
        }
        let (shell, shell_roles, shell_vertices) = embedded_exact_ring(
            subdivision,
            &part.shell,
            &exact_to_embedded,
            &points,
            clip,
            &clip_trace,
        )?;
        let holes_with_roles = part
            .holes
            .iter()
            .map(|hole| {
                embedded_exact_ring(
                    subdivision,
                    hole,
                    &exact_to_embedded,
                    &points,
                    clip,
                    &clip_trace,
                )
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let holes: Vec<_> = holes_with_roles
            .iter()
            .map(|(ring, ..)| ring.clone())
            .collect();
        let edge_roles: Vec<_> = std::iter::once(shell_roles)
            .chain(holes_with_roles.iter().map(|(_, roles, _)| roles.clone()))
            .collect();
        let producer_vertices = std::iter::once(shell_vertices)
            .chain(
                holes_with_roles
                    .into_iter()
                    .map(|(_, _, vertices)| vertices),
            )
            .collect();
        let polygon = Polygon::new(shell, holes);
        if !polygon_is_valid(&polygon)? {
            return Err(exact_embedding_unrepresentable(operation, constructed));
        }
        labelled.push((part.owner, EmbeddedPolygon {
            polygon,
            owner: Some(part.owner.0),
            edge_roles,
            producer_vertices,
        }));
    }
    if product.includes_polygons() {
        if receipt.cells.len() != labelled.len() {
            return Err(voronoi_error(
                "exact cell receipts do not match published polygon parts",
            ));
        }
        for (cell, (owner, output)) in receipt.cells.iter_mut().zip(&labelled) {
            if cell.owner != owner.0 || output.owner != Some(owner.0) {
                return Err(voronoi_error(
                    "exact cell receipt lost its published site owner",
                ));
            }
            let (shell, holes) = output
                .edge_roles
                .split_first()
                .ok_or_else(|| voronoi_error("published exact cell has no shell roles"))?;
            cell.shell = receipt_ring_uses(shell)?;
            cell.holes = holes
                .iter()
                .map(|ring| receipt_ring_uses(ring))
                .collect::<Result<Vec<_>>>()?;
        }
    }
    if product.includes_polygons() {
        let embedded_ring = |ring: &[exact::ExactPoint]| -> Result<Ring> {
            let capacity = ring
                .len()
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            ensure_coordseq_vertex_capacity(capacity)?;
            let mut coordinates = Vec::with_capacity(capacity);
            for point in ring {
                let vertex = subdivision
                    .vertex_at(point)?
                    .ok_or_else(|| voronoi_error("generated clip vertex is not live"))?;
                let xy = exact_to_embedded.point(vertex, &points)?;
                coordinates.push(Point::new_raw_xy(xy.x, xy.y));
            }
            let first = *coordinates
                .first()
                .expect("normalized clip rings contain at least one coordinate");
            coordinates.push(first);
            Ring::closed(coordinates)
        };
        let (clip_shell, clip_holes) = clip
            .rings
            .split_first()
            .expect("normalized Voronoi clips contain their outer ring");
        let shell = embedded_ring(clip_shell)?;
        let holes = clip_holes
            .iter()
            .map(|ring| embedded_ring(ring))
            .collect::<std::result::Result<Vec<_>, _>>()?;
        let comparison_polygon = Polygon::new(shell, holes);
        admit_embedded_clip(&comparison_polygon)?;
        let comparison_clip = Shape::Polygon(comparison_polygon);
        if clip.source.is_some() {
            let source = Shape::Polygon(
                clip.geometry
                    .as_ref()
                    .expect("source clips retain their normalized chart geometry")
                    .clone(),
            );
            let source_gap = binary_cartesian_difference_without_partition_budget(
                &source,
                &comparison_clip,
                Strictness::Lenient,
            )?;
            let source_spill = binary_cartesian_difference_without_partition_budget(
                &comparison_clip,
                &source,
                Strictness::Lenient,
            )?;
            if !source_gap.is_empty() || !source_spill.is_empty() {
                return Err(voronoi_error(
                    "embedded Voronoi clip differs topologically from its source",
                ));
            }
        };
        if labelled.is_empty() {
            return Err(voronoi_error(
                "exact Voronoi subdivision produced no labelled cells",
            ));
        }
        let mut shape_rows = Vec::with_capacity(labelled.len());
        shape_rows.extend(
            labelled
                .iter()
                .map(|(_, output)| Shape::Polygon(output.polygon.clone())),
        );
        if !coverage_is_valid(&shape_rows, 0.0)? {
            return Err(exact_embedding_unrepresentable(operation, constructed));
        }
        let mut row_boxes = Vec::with_capacity(shape_rows.len());
        row_boxes.extend(shape_rows.iter().map(Shape::bounds));
        let row_index = BoxIndex::build(&row_boxes)?;
        for (site_index, site) in sites.iter().enumerate() {
            let rounded_site = site.rounded_xy()?;
            let point = Shape::Point(Point::new_raw_xy(rounded_site.x, rounded_site.y));
            if !comparison_clip.covers(&point)? {
                continue;
            }
            let mut own = false;
            #[expect(
                clippy::indexing_slicing,
                reason = "row_index is built from shape_rows cloned in the same order as labelled, so every hit is an ordinal into this parallel cell table"
            )]
            let verify_row = |row: usize| {
                let (owner, output) = &labelled[row];
                if Shape::Polygon(output.polygon.clone()).covers(&point)? {
                    if owner.0 as usize != site_index {
                        return Err(exact_embedding_unrepresentable(operation, constructed));
                    }
                    own = true;
                }
                Ok(())
            };
            row_index.for_each_containing(Some(rounded_site), verify_row)?;
            if !own {
                return Err(exact_embedding_unrepresentable(operation, constructed));
            }
        }
        // Exact vertices are rounded once and shared by every incident cell.  The
        // certificates above preserve the subdivision topology, validity, and
        // interior-disjointness, but binary64 overlay may still observe an ULP-scale
        // gap between the rounded partition and the source clip.  That numerical
        // equality is deliberately not part of the topological-snap contract.
    }
    let edge_data = if product.includes_edges() {
        pack_separator_chains(chains, edge_positions, |vertex| {
            exact_to_embedded.point(vertex, &points)
        })?
    } else {
        EmbeddedEdgeData::Segments(Vec::new())
    };
    let polygons = labelled.into_iter().map(|(_, output)| output).collect();
    let receipt = std::sync::Arc::new(receipt);
    let edges = EmbeddedEdges {
        data: edge_data,
        receipt: std::sync::Arc::clone(&receipt),
    };
    Ok(EmbeddedVoronoi {
        polygons,
        edges,
        receipt,
    })
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

#[expect(
    clippy::arithmetic_side_effects,
    reason = "edge is an admitted ordinal in a three-slot Delaunay face, so its side is 0, 1, or 2 and the wrapped face-local successor stays in range"
)]
const fn dual_next_halfedge(edge: usize) -> usize {
    if edge % 3 == 2 { edge - 2 } else { edge + 1 }
}

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

fn same_undirected_dual_edge(left: DirectedDualEdge, right: DirectedDualEdge) -> bool {
    left.low == right.low && left.high == right.high
}

fn dual_clip_segment_for_edge(
    clip: &NormalizedClip<'_>,
    start: &exact::ExactPoint,
    end: &exact::ExactPoint,
) -> Result<Option<DualClipUse>> {
    let zero = exact::ExactRational::zero();
    let one = exact::ExactRational::one();
    let mut segment = 0_usize;
    for ring in &clip.rings {
        for (clip_start, clip_end) in ring.iter().zip(ring.iter().cycle().skip(1)) {
            if exact::orient_points(clip_start, clip_end, start)? == ExactSign::Zero
                && exact::orient_points(clip_start, clip_end, end)? == ExactSign::Zero
            {
                let start_parameter = exact_segment_parameter(clip_start, clip_end, start)?;
                let end_parameter = exact_segment_parameter(clip_start, clip_end, end)?;
                let in_range = |parameter: &exact::ExactRational| -> Result<bool> {
                    Ok(parameter.try_cmp(&zero)? != Ordering::Less
                        && parameter.try_cmp(&one)? != Ordering::Greater)
                };
                if in_range(&start_parameter)? && in_range(&end_parameter)? {
                    return Ok(Some(DualClipUse {
                        segment: u32::try_from(segment)
                            .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
                        start_parameter,
                        end_parameter,
                    }));
                }
            }
            segment = segment
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
        }
    }
    Ok(None)
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

impl Tagged {
    fn clone_tagged(&self) -> Self {
        Self {
            vertex: match &self.vertex {
                DualVertex::Center(component) => DualVertex::Center(*component),
                DualVertex::Generated(point) => DualVertex::Generated(Box::new((**point).clone())),
            },
            leaving: self.leaving,
        }
    }
}

/// The ring's vertices, without their edge tags.
fn untag(ring: Vec<Tagged>) -> Vec<DualVertex> {
    ring.into_iter().map(|tagged| tagged.vertex).collect()
}

fn reverse_tagged(ring: &mut [Tagged]) {
    // Reversal turns each edge around: the edge leaving a vertex afterwards
    // is the one that used to arrive at it.
    ring.reverse();
    if let Some(first) = ring.first().map(|tagged| tagged.leaving) {
        let mut next = first;
        for tagged in ring.iter_mut().rev() {
            std::mem::swap(&mut tagged.leaving, &mut next);
        }
    }
}

fn dedup_tagged(ring: &mut Vec<Tagged>, centers: &[exact::ExactPoint]) -> Result<()> {
    let same = |left: &Tagged, right: &Tagged| {
        dual_vertex_point(&left.vertex, centers)
            .same_position(dual_vertex_point(&right.vertex, centers))
    };
    let mut failure = None;
    ring.dedup_by(|later, earlier| {
        let same = failure.is_none()
            && same(later, earlier).unwrap_or_else(|error| {
                failure = Some(error);
                false
            });
        if same {
            earlier.leaving = later.leaving;
        }
        same
    });
    if let Some(error) = failure {
        return Err(error);
    }
    while let (Some(first), Some(last)) = (ring.first(), ring.last()) {
        if ring.len() <= 1 || !same(first, last)? {
            break;
        }
        ring.pop();
    }
    Ok(())
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

#[expect(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "half-edge and site ordinals come from the same certified mesh and admitted site slice; the count pass bounds each row and its total slots"
)]
fn dual_incident_faces(mesh: &CertifiedDelaunay, site_count: usize) -> IncidentFaces {
    let triangles = mesh.triangles();
    let halfedges = mesh.halfedges();
    let flat = |edge: usize| dual_site_at_halfedge(triangles, edge);
    let mut start_edge = Vec::with_capacity(site_count);
    start_edge.resize(site_count, usize::MAX);
    for edge in 0..halfedges.len() {
        let endpoint = flat(dual_next_halfedge(edge));
        if start_edge[endpoint] == usize::MAX || halfedges[edge] == EMPTY {
            start_edge[endpoint] = edge;
        }
    }
    let mut offsets: Vec<usize> = Vec::new();
    let mut boundaries = Vec::new();
    offsets.reserve(site_count + 1);
    boundaries.reserve(site_count);
    offsets.push(0);
    for site in 0..site_count {
        let start = start_edge[site];
        let (length, boundary) = incident_faces_for_site(start, halfedges, None);
        offsets.push(offsets.last().copied().unwrap_or(0) + length);
        boundaries.push(boundary);
    }
    let total = offsets.last().copied().unwrap_or(0);
    let mut faces = Vec::new();
    let mut neighbors = Vec::new();
    faces.reserve(total);
    neighbors.reserve(total);
    faces.resize(total, 0);
    neighbors.resize(total, SiteId(0));
    for (&start, &[row_start, row_end]) in start_edge.iter().zip(offsets.array_windows::<2>()) {
        incident_faces_for_site(
            start,
            halfedges,
            Some(IncidenceOutput {
                faces: &mut faces[row_start..row_end],
                neighbors: &mut neighbors[row_start..row_end],
                flat: &flat,
            }),
        );
    }
    IncidentFaces {
        offsets,
        faces,
        neighbors,
        boundaries,
    }
}

#[expect(
    clippy::indexing_slicing,
    reason = "each certified Delaunay face owns exactly three consecutive half-edge origins"
)]
fn dual_site_at_halfedge(triangles: &[[usize; 3]], halfedge: usize) -> usize {
    triangles[halfedge / 3][halfedge % 3]
}

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

impl IncidentFaces {
    #[expect(
        clippy::indexing_slicing,
        reason = "offsets, face rows, neighbor rows, and boundary records are built in the same dense site order"
    )]
    fn rows(&self) -> impl Iterator<Item = IncidentFaceRow<'_>> + '_ {
        self.offsets
            .array_windows::<2>()
            .zip(self.boundaries.iter())
            .map(move |(&[start, end], &boundary)| IncidentFaceRow {
                faces: &self.faces[start..end],
                neighbors: &self.neighbors[start..end],
                boundary,
            })
    }
}

/// Where one site's walk writes its faces and neighbours.
struct IncidenceOutput<'a> {
    faces: &'a mut [usize],
    neighbors: &'a mut [SiteId],
    flat: &'a dyn Fn(usize) -> usize,
}

#[expect(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "the certified half-edge walk's count pass reserves this site's face/neighbor row and every next-halfedge ordinal belongs to its mesh"
)]
fn incident_faces_for_site(
    start: usize,
    halfedges: &[usize],
    mut output: Option<IncidenceOutput<'_>>,
) -> (usize, Option<(usize, usize)>) {
    if start == usize::MAX {
        return (0, None);
    }
    let mut length = 0;
    let mut incoming = start;
    loop {
        let outgoing = dual_next_halfedge(incoming);
        if let Some(output) = output.as_mut() {
            output.faces[length] = incoming / 3;
            // `outgoing` runs from the site to the neighbour shared with the
            // next face around it.
            output.neighbors[length] = SiteId((output.flat)(dual_next_halfedge(outgoing)) as u32);
        }
        length += 1;
        let next = halfedges[outgoing];
        if next == EMPTY {
            // `start` was a hull half-edge (q -> site) with no twin and
            // `outgoing` is the hull half-edge (site -> r).
            return (length, Some((start, outgoing)));
        }
        incoming = next;
        if incoming == start {
            return (length, None);
        }
    }
}

/// Cap one unbounded dual ray on the private frame. Identical construction to
/// the shipped hull-edge lane in `emit_clipped_separators`.
fn dual_ray_hit(
    sites: &[Site],
    pair: [usize; 2],
    opposite: usize,
    frame: &[exact::ExactPoint; 4],
) -> Result<(FrameSide, exact::ExactPoint)> {
    let [left_site, right_site] = pair;
    let [left, right] = [
        &site_at(sites, left_site).exact_point,
        &site_at(sites, right_site).exact_point,
    ];
    let opposite_sign = exact::orient_points(left, right, &site_at(sites, opposite).exact_point)?;
    let line = exact::ExactLine::perpendicular_bisector_points(left, right)?;
    for hit in exact_bisector_frame_intersections([left, right], &line, frame)? {
        if exact::orient_points(left, right, &hit.1)? != opposite_sign {
            return Ok(hit);
        }
    }
    Err(voronoi_error(
        "certified Voronoi hull ray misses exact frame",
    ))
}

fn dual_close_hull_chain(
    faces: &[usize],
    neighbors: &[SiteId],
    entry_site: SiteId,
    complex: &DelaunayComplex,
    center_points: &[exact::ExactPoint],
    entry_ray: &(FrameSide, exact::ExactPoint),
    exit_ray: &(FrameSide, exact::ExactPoint),
    frame: &[exact::ExactPoint; 4],
    exterior: bool,
) -> Result<Vec<Tagged>> {
    let generated = |point: &exact::ExactPoint, leaving| -> Result<Tagged> {
        Ok(Tagged {
            vertex: DualVertex::Generated(Box::new(point.clone())),
            leaving,
        })
    };
    let entry_side = entry_ray.0;
    let exit_side = exit_ray.0;
    let mut ring = Vec::with_capacity(faces.len().saturating_add(6));
    ring.push(generated(&entry_ray.1, EdgeTag::Bisector(entry_site))?);
    ring.extend(
        faces
            .iter()
            .zip(neighbors)
            .map(|(&face, &neighbor)| Tagged {
                vertex: DualVertex::Center(complex.component_of_face(face)),
                leaving: EdgeTag::Bisector(neighbor),
            }),
    );
    ring.push(generated(&exit_ray.1, EdgeTag::Frame(exit_side))?);
    if exterior {
        let steps = match exit_side.steps_clockwise_to(entry_side) {
            0 => FrameSide::ALL.len(),
            steps => steps,
        };
        let mut side = exit_side;
        for _ in 0..steps {
            ring.push(generated(
                side.start(frame),
                EdgeTag::Frame(side.previous()),
            )?);
            side = side.previous();
        }
    } else {
        let steps = exit_side.steps_counterclockwise_to(entry_side);
        let mut side = exit_side;
        for _ in 0..steps {
            side = side.next();
            ring.push(generated(side.start(frame), EdgeTag::Frame(side))?);
        }
    }
    dedup_tagged(&mut ring, center_points)?;
    if dual_cycle_orientation(&ring, center_points)? == ExactSign::Negative {
        reverse_tagged(&mut ring);
    }
    Ok(ring)
}

#[expect(
    clippy::indexing_slicing,
    reason = "center component IDs come from the same DelaunayComplex whose exact centers are passed to every dual traversal"
)]
fn dual_vertex_point<'a>(
    vertex: &'a DualVertex,
    centers: &'a [exact::ExactPoint],
) -> &'a exact::ExactPoint {
    match vertex {
        DualVertex::Center(component) => &centers[*component],
        DualVertex::Generated(point) => point,
    }
}

fn dual_cycle_orientation(ring: &[Tagged], centers: &[exact::ExactPoint]) -> Result<ExactSign> {
    if ring.len() < 3 {
        return Err(voronoi_error("exact cycle has fewer than three vertices"));
    }
    let mut points = ring
        .iter()
        .chain(ring.iter().take(2))
        .map(|tagged| dual_vertex_point(&tagged.vertex, centers));
    let mut start = points
        .next()
        .expect("an exact cycle has at least three vertices");
    let mut middle = points
        .next()
        .expect("an exact cycle has at least three vertices");
    for end in points {
        let sign = exact::orient_points(start, middle, end)?;
        if sign != ExactSign::Zero {
            return Ok(sign);
        }
        start = middle;
        middle = end;
    }
    Err(voronoi_error("exact cycle has zero orientation"))
}

#[expect(
    clippy::arithmetic_side_effects,
    reason = "an admitted simple Voronoi cell has exact winding 0, 1, or -1 for this point-in-cycle query"
)]
fn dual_point_in_cycle(
    cycle: &[Tagged],
    centers: &[exact::ExactPoint],
    point: &exact::ExactPoint,
) -> Result<exact::PointInCycle> {
    let mut winding = 0_i32;
    for (start, end) in cycle.iter().zip(cycle.iter().cycle().skip(1)) {
        let start = dual_vertex_point(&start.vertex, centers);
        let end = dual_vertex_point(&end.vertex, centers);
        let orientation = exact::orient_points(start, end, point)?;
        if orientation == ExactSign::Zero && between(point, start, end)? {
            return Ok(exact::PointInCycle::Boundary);
        }
        let start_y = start.compare_y(point)?;
        let end_y = end.compare_y(point)?;
        if start_y != Ordering::Greater
            && end_y == Ordering::Greater
            && orientation == ExactSign::Positive
        {
            winding += 1;
        } else if end_y != Ordering::Greater
            && start_y == Ordering::Greater
            && orientation == ExactSign::Negative
        {
            winding -= 1;
        }
    }
    Ok(if winding == 0 {
        exact::PointInCycle::Outside
    } else {
        exact::PointInCycle::Inside
    })
}

/// Sutherland-Hodgman against a CCW convex rectangle, in exact arithmetic.
/// "Inside" is `orient_points(side_start, side_end, p) != Negative`, i.e. the
/// same exact predicate the rest of the module uses; a crossing vertex is the
/// exact intersection of two support lines, never a rounded one.
/// Sutherland-Hodgman against the clip rectangle, in exact arithmetic.
///
/// The clip is axis-aligned, so the half-plane test collapses from a general
/// orientation determinant to ONE coordinate comparison. For the bottom side
/// `a = (minx, miny) -> b = (maxx, miny)` the determinant is
/// `(maxx - minx) * (py - miny)`, and `maxx > minx` is checked once by the
/// caller, so its sign IS the sign of `py - miny`. The same reduction holds on
/// the other three sides. Identical verdict, one filtered comparison instead of
/// six interval operations. A degenerate rectangle falls back to the general
/// orientation test.
fn dual_clip_convex(
    ring: Vec<Tagged>,
    site: &Site,
    sites: &[Site],
    rect: &[exact::ExactPoint; 4],
    axis_aligned: bool,
    centers: &[exact::ExactPoint],
) -> Result<Vec<Tagged>> {
    // Certified crossings need the clip sides as doubles.
    let corners = if sites.iter().all(|site| site.retained_chart_site.is_none()) {
        match rect.each_ref().map(exact::ExactPoint::as_double) {
            [Some(a), Some(b), Some(c), Some(d)] if axis_aligned => Some([a, b, c, d]),
            _ => None,
        }
    } else {
        None
    };
    let mut current = ring;
    for side in FrameSide::ALL {
        if current.is_empty() {
            return Ok(current);
        }
        let (a, b) = (side.start(rect), side.end(rect));
        let inside = |point: &exact::ExactPoint| -> Result<bool> {
            if !axis_aligned {
                return Ok(exact::orient_points(a, b, point)? != ExactSign::Negative);
            }
            Ok(match side {
                FrameSide::Bottom => point.compare_y(a)? != Ordering::Less,
                FrameSide::Right => point.compare_x(a)? != Ordering::Greater,
                FrameSide::Top => point.compare_y(a)? != Ordering::Greater,
                FrameSide::Left => point.compare_x(a)? != Ordering::Less,
            })
        };
        // The crossing of the edge `from -> to` with this side. A bisector
        // edge meets the side at a certified rounding; an axis-aligned edge
        // meets it at an exact double; anything the certificate declines is
        // the exact intersection of the two support lines, as before.
        let crossing = |from: &exact::ExactPoint,
                        tag: EdgeTag,
                        to: &exact::ExactPoint|
         -> Result<Option<exact::ExactPoint>> {
            if let Some(corners) = corners {
                let [corner_start, corner_end] = side.endpoints_xy(&corners);
                match tag {
                    EdgeTag::Bisector(neighbor) => {
                        let neighbor = site_by_id(sites, neighbor);
                        if let (Some(site), Some(neighbor)) = (
                            site.exact_point.as_double(),
                            neighbor.exact_point.as_double(),
                        ) && let exact::FrameHit::Point(hit) =
                            exact::ExactPoint::certified_bisector_hit([site, neighbor], [
                                corner_start,
                                corner_end,
                            ])
                        {
                            return Ok(Some(hit));
                        }
                    },
                    EdgeTag::Frame(_) => {
                        // An edge along a frame or clip side shares one exact
                        // coordinate: a ray hit's side coordinate is exact in
                        // its bounds, a corner is a double. The crossing with
                        // a perpendicular side is then an exact double.
                        let [from_x, from_y] = from.bounds();
                        let [to_x, to_y] = to.bounds();
                        let shared = |from: exact::Interval, to: exact::Interval| {
                            (from.lo == from.hi && to.lo == to.hi && from.lo == to.lo)
                                .then_some(from.lo)
                        };
                        let hit = match side {
                            FrameSide::Bottom | FrameSide::Top => {
                                shared(from_x, to_x).map(|x| XY::new(x, corner_start.y))
                            },
                            FrameSide::Right | FrameSide::Left => {
                                shared(from_y, to_y).map(|y| XY::new(corner_start.x, y))
                            },
                        };
                        if let Some(hit) = hit
                            && hit.x.is_finite()
                            && hit.y.is_finite()
                        {
                            return Ok(Some(exact::ExactPoint::from_xy(hit)));
                        }
                    },
                }
            }
            let boundary = exact::ExactLine::through_points(a, b)?;
            let edge = exact::ExactLine::through_points(from, to)?;
            let hit = exact::line_intersection(&boundary, &edge)?;
            Ok(hit.is_finite().then_some(hit))
        };
        let mut next = Vec::with_capacity(current.len().saturating_add(1));
        let Some(mut previous) = current.last() else {
            return Ok(current);
        };
        let mut previous_in = inside(dual_vertex_point(&previous.vertex, centers))?;
        for this in &current {
            let this_point = dual_vertex_point(&this.vertex, centers);
            let this_in = inside(this_point)?;
            if this_in != previous_in {
                let tag = previous.leaving;
                let previous_point = dual_vertex_point(&previous.vertex, centers);
                if let Some(hit) = crossing(previous_point, tag, this_point)? {
                    next.push(Tagged {
                        vertex: DualVertex::Generated(Box::new(hit)),
                        // Entering: the rest of the original edge follows.
                        // Leaving: the new edge runs along this side.
                        leaving: if this_in { tag } else { EdgeTag::Frame(side) },
                    });
                }
            }
            if this_in {
                next.push(this.clone_tagged());
            }
            previous = this;
            previous_in = this_in;
        }
        dedup_tagged(&mut next, centers)?;
        current = next;
    }
    Ok(current)
}

fn dual_build_rings(
    sites: &[Site],
    mesh: &CertifiedDelaunay,
    complex: DelaunayComplex,
    clip: &NormalizedClip<'_>,
) -> Result<DualRings> {
    let centers = &complex.centers;
    let mut site_points = Vec::with_capacity(sites.len());
    site_points.extend(sites.iter().map(|site| site.exact_point.clone()));
    let frame_inputs = || {
        site_points
            .iter()
            .chain(clip.rings.iter().flatten())
            .chain(centers.iter())
    };
    let frame = if let Some(frame) = exact::enclosing_frame_binary64(frame_inputs())? {
        frame
    } else {
        let mut inputs = Vec::with_capacity(frame_inputs().count());
        inputs.extend(frame_inputs().cloned());
        exact::enclosing_frame(&inputs)?
    };
    let [clip_outer] = clip.rings.as_slice() else {
        return Err(voronoi_error(
            "certified rectangular clip requires one outer ring",
        ));
    };
    let [a, b, c, d] = clip_outer.as_slice() else {
        return Err(voronoi_error(
            "certified rectangular clip requires four corners",
        ));
    };
    let rect = [a.clone(), b.clone(), c.clone(), d.clone()];
    // Certified once: the clip really is a positively-oriented axis-aligned
    // rectangle with strictly positive extent. Only then may the half-plane
    // test collapse to a coordinate comparison.
    let axis_aligned = axis_aligned_rectangle(&rect)?;

    // A box certified to lie inside the rectangle, for the interior fast
    // lane: the inner edges of the corners' certified intervals. A padded
    // corner is a dyadic that need not be a double, and this stays exact for
    // it; an unbounded interval fails every strict comparison and keeps the
    // cell on the exact lane.
    let interior_bounds = axis_aligned.then(|| {
        let [bottom_left, _, top_right, _] = &rect;
        let [low_x, low_y] = bottom_left.bounds();
        let [high_x, high_y] = top_right.bounds();
        [low_x.hi, high_x.lo, low_y.hi, high_y.lo]
    });
    // The incidence walk has one rotational sense for the whole mesh; the
    // first interior cell settles it exactly and every later one reuses it.
    let mut walk_clockwise: Option<bool> = None;

    let triangles = mesh.triangles();
    let flat = |edge: usize| dual_site_at_halfedge(triangles, edge);
    let incidence = dual_incident_faces(mesh, sites.len());
    let mut rings = Vec::with_capacity(sites.len());
    let mut owners = Vec::with_capacity(sites.len());
    let mut edge_tags = Vec::with_capacity(sites.len());

    for (site, incidence_row) in sites.iter().zip(incidence.rows()) {
        let faces = incidence_row.faces;
        let boundary = incidence_row.boundary;
        let neighbors = incidence_row.neighbors;
        let ring = if let Some((first_hull, last_hull)) = boundary {
            // (q -> site) and (site -> r): the two hull edges bounding the cell.
            let entry_site = flat(first_hull);
            let entry_neighbor = flat(dual_next_halfedge(first_hull));
            let entry_opposite = flat(dual_next_halfedge(dual_next_halfedge(first_hull)));
            let exit_site = flat(last_hull);
            let exit_neighbor = flat(dual_next_halfedge(last_hull));
            let exit_opposite = flat(dual_next_halfedge(dual_next_halfedge(last_hull)));
            let entry_pair = [entry_site, entry_neighbor];
            let exit_pair = [exit_site, exit_neighbor];
            let entry_ray = dual_ray_hit(sites, entry_pair, entry_opposite, &frame)?;
            let exit_ray = dual_ray_hit(sites, exit_pair, exit_opposite, &frame)?;
            let site_id = SiteId(entry_site as u32);
            let site_point = &site.exact_point;
            // Positive topology fixes the open center chain. The two frame
            // paths are complementary clipped faces; exact ownership decides
            // only which one contains this site, including the zero-separator
            // isolated-ring case. Ring handedness is normalization, not a
            // second candidate.
            let interior = dual_close_hull_chain(
                faces, neighbors, site_id, &complex, centers, &entry_ray, &exit_ray, &frame, false,
            )?;
            if dual_point_in_cycle(&interior, centers, site_point)? == exact::PointInCycle::Inside {
                interior
            } else {
                let exterior = dual_close_hull_chain(
                    faces, neighbors, site_id, &complex, centers, &entry_ray, &exit_ray, &frame,
                    true,
                )?;
                if dual_point_in_cycle(&exterior, centers, site_point)?
                    != exact::PointInCycle::Inside
                {
                    return Err(voronoi_error(
                        "dual hull cell has no admissible frame closure",
                    ));
                }
                exterior
            }
        } else {
            // Interior fast lane. Two distinct components around one site have
            // distinct exact centers (both circles pass through the site, so a
            // shared center would be one circle, hence one component), which
            // makes consecutive-id dedup exact. The certified mesh orders the
            // faces counter-clockwise, and every center strictly inside the
            // rectangle means the clip is the identity; the embedding's
            // certificate still proves the rounded cell.
            let mut ring: Vec<Tagged> = Vec::with_capacity(faces.len().saturating_add(6));
            let mut interior = interior_bounds.is_some();
            for (&face, &neighbor) in faces.iter().zip(neighbors) {
                let component = complex.component_of_face(face);
                match ring.last_mut() {
                    // Cocircular faces share the vertex; the edge leaving it
                    // is the last face's.
                    Some(last) if matches!(last.vertex, DualVertex::Center(id) if id == component) =>
                    {
                        last.leaving = EdgeTag::Bisector(neighbor);
                    },
                    _ => ring.push(Tagged {
                        vertex: DualVertex::Center(component),
                        leaving: EdgeTag::Bisector(neighbor),
                    }),
                }
                interior = interior
                    && interior_bounds
                        .is_some_and(|bounds| complex.center(component).strictly_inside(bounds));
            }
            if ring.len() > 1 {
                let first = ring.first().map(|tagged| &tagged.vertex);
                let last = ring.last().map(|tagged| &tagged.vertex);
                if let (Some(DualVertex::Center(first)), Some(DualVertex::Center(last))) =
                    (first, last)
                    && first == last
                {
                    let leaving = ring
                        .last()
                        .expect("a repeated closing center has a final edge")
                        .leaving;
                    ring.pop();
                    ring.first_mut()
                        .expect("a repeated closing center has a first edge")
                        .leaving = leaving;
                }
            }
            if interior && ring.len() >= 3 {
                let clockwise = if let Some(clockwise) = walk_clockwise {
                    clockwise
                } else {
                    let clockwise = dual_cycle_orientation(&ring, centers)? == ExactSign::Negative;
                    walk_clockwise = Some(clockwise);
                    clockwise
                };
                if clockwise {
                    reverse_tagged(&mut ring);
                }
                edge_tags.push(ring.iter().map(|tagged| tagged.leaving).collect());
                rings.push(untag(ring));
                owners.push(SiteId(
                    u32::try_from(site.id)
                        .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
                ));
                continue;
            }
            dedup_tagged(&mut ring, centers)?;
            if ring.len() >= 3 && dual_cycle_orientation(&ring, centers)? == ExactSign::Negative {
                reverse_tagged(&mut ring);
            }
            ring
        };
        let clipped = dual_clip_convex(ring, site, sites, &rect, axis_aligned, centers)?;
        if clipped.len() < 3 {
            return Err(voronoi_error(
                "dual Voronoi cell degenerated under the clip",
            ));
        }
        edge_tags.push(clipped.iter().map(|tagged| tagged.leaving).collect());
        rings.push(untag(clipped));
        owners.push(SiteId(
            u32::try_from(site.id).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
        ));
    }
    Ok(DualRings {
        rings,
        centers: complex.centers,
        center_sites: complex.center_sites,
        owners,
        edge_tags,
    })
}

fn lex_xy(left: XY, right: XY) -> Ordering {
    left.x.total_cmp(&right.x).then(left.y.total_cmp(&right.y))
}

/// Accept a rounded cell on its doubles alone: a positively oriented, convex,
/// simple ring that covers `site`. Every turn is a left turn or a forward
/// collinear step (a clipped hull cell keeps the points where its rays meet a
/// rectangle side, so consecutive vertices on one side are collinear), never
/// a right turn, a repeated vertex, or a reversal; and the lexicographic edge
/// direction changes exactly twice around the ring, which pins the turning
/// number to one: a pentagram turns left at every vertex and changes direction
/// four times. `site` may lie on an edge. Each sign comes from the exact
/// predicate, so a declined float filter costs one dyadic evaluation, never a
/// whole exact cell, and a refusal here only routes the cell to the exact
/// certificates.
#[expect(
    clippy::arithmetic_side_effects,
    reason = "direction_changes is bounded by the number of vertices in this admitted ring"
)]
fn certify_convex_cell(ring: &[XY], site: XY) -> bool {
    let count = ring.len();
    if count < 3 {
        return false;
    }
    let mut direction_changes = 0_usize;
    let mut cyclic = ring.iter().cycle();
    let mut a = *cyclic.next().expect("ring has at least three vertices");
    let mut b = *cyclic.next().expect("ring has at least three vertices");
    for &c in cyclic.take(count) {
        let (ahead, onward) = (lex_xy(a, b), lex_xy(b, c));
        // Three vertices on one axis-aligned side (a clipped hull cell's
        // frame or clip edge) are exactly collinear; no filter can certify
        // a zero, so decide it on the shared coordinate.
        let turn = if (a.x == b.x && b.x == c.x) || (a.y == b.y && b.y == c.y) {
            ExactSign::Zero
        } else {
            exact::orient2d(a, b, c)
        };
        match turn {
            ExactSign::Positive => {},
            ExactSign::Zero if ahead == onward && ahead != Ordering::Equal => {},
            ExactSign::Zero | ExactSign::Negative => return false,
        }
        if ahead != onward {
            direction_changes += 1;
        }
        if exact::orient2d(a, b, site) == ExactSign::Negative {
            return false;
        }
        (a, b) = (b, c);
    }
    direction_changes == 2
}

/// Round the dual once and run the certificates that survive the redesign.
///
/// What is UNCHANGED from `embed`: the sole nearest-even rounding, the global
/// injectivity check over distinct exact vertices, the per-ring orientation
/// check against the rounded ring, and `polygon_is_valid` per cell.
///
/// What is GONE, because the structure it certified no longer exists: the
/// arrangement's rounded no-crossing scan and the per-vertex exact-vs-rounded
/// fan-order comparison. The dual never builds that arrangement. Instead,
/// rounded edge cancellation proves that the valid cells have exactly the
/// rectangular clip boundary, and exact point membership checks each canonical
/// site's rounded cell. Polygon clips remain on the exact arrangement lane.
fn dual_embed(
    rings: &DualRings,
    clip: &NormalizedClip<'_>,
    sites: &[Site],
    product: VoronoiProduct,
    budget: &mut ExpansionBudget,
    chart: Option<&PointSelectionChart>,
) -> std::result::Result<EmbeddedVoronoi, DualEmbeddingError> {
    if rings.rings.len() != sites.len() {
        return Err(DualEmbeddingError::Unrepresentable);
    }
    // Vertex identity WITHOUT an exact positional sort.
    //
    // A `Center` vertex IS its component, so its rounding is memoised once per
    // component. Everything else is hashed on its ROUNDED key, and a key
    // collision is then resolved EXACTLY: two exact points that share a rounded
    // key are the same vertex only if `same_position` says so, and otherwise the
    // rounding is not injective and the diagram is refused. That is the same
    // verdict the sort produced -- rounding is a function, so equal exact points
    // always land in the same bucket -- reached in O(V) instead of O(V log V)
    // exact comparisons.
    // Every distinct exact vertex gets one dense id. Center component IDs
    // remain separate producer identities and map to these interned vertices;
    // a clip hit may be the first occurrence of a center's exact position.
    if let Some(last) = rings.centers.len().checked_sub(1) {
        u32::try_from(last).map_err(|_| crate::error::ResourceError::SizeOverflow)?;
    }
    let total = rings.rings.iter().try_fold(0_usize, |count, ring| {
        count
            .checked_add(ring.len())
            .ok_or(crate::error::ResourceError::SizeOverflow)
    })?;
    let mut center_rounded: Vec<Option<(XY, u32)>> = Vec::with_capacity(rings.centers.len());
    center_rounded.resize(rings.centers.len(), None);
    let mut exact_by_id = Vec::new();
    // Distinct vertices are the centers plus a boundary's worth of generated
    // points; sizing for every ring reference would triple the table.
    let mut interned = try_hash_map_with_capacity(rings.centers.len())?;
    let mut vertex_xy: Vec<XY> = Vec::with_capacity(rings.centers.len());
    #[expect(
        clippy::collapsible_if,
        reason = "keep the center-only memo lookup on its measured base instruction path"
    )]
    #[expect(
        clippy::indexing_slicing,
        reason = "center components index the admitted center map; dense IDs occupy its prefix or are assigned immediately before their coordinate is published, and the intern table only returns those IDs"
    )]
    fn resolve<'r>(
        vertex: &'r DualVertex,
        centers: &'r [exact::ExactPoint],
        center_rounded: &mut [Option<(XY, u32)>],
        exact_by_id: &mut Vec<exact::ExactPoint>,
        interned: &mut crate::collections::HashMap<PointKey, (&'r exact::ExactPoint, u32)>,
        vertex_xy: &mut Vec<XY>,
    ) -> std::result::Result<(XY, u32), DualEmbeddingError> {
        if let DualVertex::Center(component) = *vertex {
            if let Some(memo) = center_rounded[component] {
                return Ok(memo);
            }
        }
        let point = dual_vertex_point(vertex, centers);
        let mut xy = point.round_nearest_even()?;
        let key = PointKey::new(xy);
        let id = match interned.entry(key) {
            std::collections::hash_map::Entry::Occupied(existing) => {
                let &(representative, id) = existing.get();
                if !representative.same_position(point)? {
                    return Err(DualEmbeddingError::Unrepresentable);
                }
                // Equal exact generated vertices can reach signed zero through
                // different algebraic expressions. Reuse the first admitted
                // representative so shared binary64 vertices are bit-identical.
                xy = representative.round_nearest_even()?;
                id
            },
            std::collections::hash_map::Entry::Vacant(slot) => {
                let id = u32::try_from(vertex_xy.len())
                    .map_err(|_| crate::error::ResourceError::SizeOverflow)?;
                vertex_xy.push(xy);
                exact_by_id.push(point.clone());
                slot.insert((point, id));
                id
            },
        };
        if let DualVertex::Center(component) = *vertex {
            center_rounded[component] = Some((xy, id));
        }
        Ok((xy, id))
    }
    let mut offsets = Vec::new();
    let mut rounded_all: Vec<XY> = Vec::new();
    let mut ids_all: Vec<u32> = Vec::new();
    let mut edge_tags = rings.edge_tags.clone();
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "the offset column has one terminal sentinel in addition to one row per existing non-zero-sized ring"
    )]
    offsets.reserve(rings.rings.len() + 1);
    rounded_all.reserve(total);
    ids_all.reserve(total);
    offsets.push(0);
    for ring in &rings.rings {
        for vertex in ring {
            let (xy, id) = resolve(
                vertex,
                &rings.centers,
                &mut center_rounded,
                &mut exact_by_id,
                &mut interned,
                &mut vertex_xy,
            )?;
            rounded_all.push(xy);
            ids_all.push(id);
        }
        offsets.push(rounded_all.len());
    }
    for (ring_index, (window, site)) in offsets.array_windows::<2>().zip(sites).enumerate() {
        let [row_start, row_end] = *window;
        let rounded = rounded_all
            .get_mut(row_start..row_end)
            .ok_or(DualEmbeddingError::Unrepresentable)?;
        // The ring starts at its lexicographically smallest rounded vertex,
        // the canonical form both lanes publish; rounding is injective here,
        // so the minimum is unique.
        let rotation = rounded
            .iter()
            .enumerate()
            .min_by(|(_, left), (_, right)| lex_xy(**left, **right))
            .map_or(0, |(index, _)| index);
        rounded.rotate_left(rotation);
        ids_all
            .get_mut(row_start..row_end)
            .ok_or(DualEmbeddingError::Unrepresentable)?
            .rotate_left(rotation);
        let tags = edge_tags
            .get_mut(ring_index)
            .ok_or(DualEmbeddingError::Unrepresentable)?;
        if tags.len() != rounded.len() {
            return Err(DualEmbeddingError::Unrepresentable);
        }
        tags.rotate_left(rotation);
        let site_xy = site.rounded_xy()?;
        let first = rounded
            .first()
            .expect("dual_build_rings admits only cells with at least three vertices");
        let closed = rounded.iter().chain(std::iter::once(first));
        if !certify_convex_cell(rounded, site_xy) {
            if !stored_xy_cycle_matches_orientation(rounded, ExactSign::Positive) {
                return Err(DualEmbeddingError::Unrepresentable);
            }
            let mut rounded_ring = Vec::with_capacity(rounded.len());
            rounded_ring.extend(rounded.iter().map(|&xy| exact::ExactPoint::from_xy(xy)));
            let capacity = rounded
                .len()
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            ensure_coordseq_vertex_capacity(capacity)?;
            let mut points = Vec::with_capacity(capacity);
            points.extend(closed.clone().map(|xy| Point::new_raw_xy(xy.x, xy.y)));
            if !polygon_is_valid(&Polygon::new(Ring::closed(points)?, Vec::new()))? {
                return Err(DualEmbeddingError::Unrepresentable);
            }
            if exact::point_in_cycle(&rounded_ring, &site.exact_point)?
                == exact::PointInCycle::Outside
            {
                return Err(DualEmbeddingError::Unrepresentable);
            }
        }
    }
    // Completeness by EDGE CANCELLATION rather than by a full overlay union.
    //
    // Every rounded cell edge is entered directed. An edge interior to the
    // partition is walked once in each direction by its two owners and cancels;
    // an edge on the outer boundary is walked exactly once. So: if every
    // undirected edge occurs either twice with opposite directions or once, and
    // the once-occurring edges tile the clip rectangle's four sides exactly with
    // no gap and no overlap, then the union of the cells IS the clip rectangle.
    // Combined with positive simple-cell validation and exact site ownership,
    // this is the same partition statement the union/difference pair made,
    // computed on stored binary64 with no overlay. Winding-number membership
    // alone cannot prove this: a gap and an overlap can leave every tested site
    // inside its own cell, while cancellation and directed boundary tiling
    // reject both.
    // Sort by the undirected endpoint pair, then direction, so the two owners
    // of each interior edge become an adjacent pair.
    let mut directed: Vec<DirectedDualEdge> = Vec::with_capacity(total);
    let mut by_dart: Vec<Option<DirectedDualEdge>> = Vec::with_capacity(total);
    by_dart.resize(total, None);
    let mut clip_uses_by_dart = Vec::with_capacity(total);
    let mut next_dart = 0_u32;
    for (ring_index, window) in offsets.array_windows::<2>().enumerate() {
        let [start, end] = *window;
        #[expect(
            clippy::indexing_slicing,
            reason = "each row offset was pushed around appending this same ring's dense vertex IDs"
        )]
        let ids = &ids_all[start..end];
        let tags = edge_tags
            .get(ring_index)
            .ok_or(DualEmbeddingError::Unrepresentable)?;
        let owner = rings
            .owners
            .get(ring_index)
            .ok_or(DualEmbeddingError::Unrepresentable)?
            .0;
        if ids.len() != tags.len() {
            return Err(DualEmbeddingError::Unrepresentable);
        }
        if ids.is_empty() {
            return Err(DualEmbeddingError::Unrepresentable);
        }
        for (edge_index, (&from, &to)) in ids.iter().zip(ids.iter().cycle().skip(1)).enumerate() {
            let from_xy = *vertex_xy
                .get(from as usize)
                .ok_or(DualEmbeddingError::Unrepresentable)?;
            let to_xy = *vertex_xy
                .get(to as usize)
                .ok_or(DualEmbeddingError::Unrepresentable)?;
            let (endpoints, forward) = if lex_xy(from_xy, to_xy).is_lt() {
                ([from, to], true)
            } else {
                ([to, from], false)
            };
            let (low, high) = if from <= to { (from, to) } else { (to, from) };
            let exact_from = exact_by_id
                .get(from as usize)
                .ok_or(DualEmbeddingError::Unrepresentable)?;
            let exact_to = exact_by_id
                .get(to as usize)
                .ok_or(DualEmbeddingError::Unrepresentable)?;
            let clip_use = dual_clip_segment_for_edge(clip, exact_from, exact_to)?;
            let clip_segment = clip_use.as_ref().map(|use_| use_.segment);
            let tag = *tags
                .get(edge_index)
                .ok_or(DualEmbeddingError::Unrepresentable)?;
            let origin = match tag {
                EdgeTag::Bisector(neighbor) => {
                    let mut pair = [owner, neighbor.0];
                    pair.sort_unstable();
                    VoronoiEdgeOrigin::Bisector { sites: pair }
                },
                EdgeTag::Frame(_) if clip.source.is_some() => {
                    let segment = clip_segment.ok_or(DualEmbeddingError::Unrepresentable)?;
                    VoronoiEdgeOrigin::SourceClip { segment }
                },
                EdgeTag::Frame(side) => VoronoiEdgeOrigin::Frame {
                    side: receipt_frame_side(side),
                },
            };
            let edge = DirectedDualEdge {
                low,
                high,
                endpoints,
                from,
                to,
                forward,
                owner,
                origin,
                clip_segment,
                dart: next_dart,
            };
            next_dart = next_dart
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            *by_dart
                .get_mut(edge.dart as usize)
                .ok_or(DualEmbeddingError::Unrepresentable)? = Some(edge);
            clip_uses_by_dart.push(clip_use);
            directed.push(edge);
        }
    }
    if next_dart as usize != total {
        return Err(DualEmbeddingError::Unrepresentable);
    }
    directed.sort_unstable();
    let [min_x, max_x, min_y, max_y] = clip.frame_bounds();
    let mut sides: [Vec<(f64, f64)>; 4] = [Vec::new(), Vec::new(), Vec::new(), Vec::new()];
    let [bottom, right, top, left] = &mut sides;
    // Each interior edge takes two directed entries.
    let mut internal_edge_count = 0_usize;
    let mut edge_drafts = Vec::with_capacity(directed.len());
    for run in directed.chunk_by(|left, right| same_undirected_dual_edge(*left, *right)) {
        let (first, second) = match run {
            [left, right] => {
                if left.forward == right.forward
                    || left.owner == right.owner
                    || left.origin != right.origin
                    || left.clip_segment != right.clip_segment
                {
                    return Err(DualEmbeddingError::Unrepresentable);
                }
                internal_edge_count = internal_edge_count
                    .checked_add(1)
                    .ok_or(crate::error::ResourceError::SizeOverflow)?;
                (*left, Some(*right))
            },
            [edge] => (*edge, None),
            _ => return Err(DualEmbeddingError::Unrepresentable),
        };
        let coordinates = [
            *vertex_xy
                .get(first.endpoints[0] as usize)
                .ok_or(DualEmbeddingError::Unrepresentable)?,
            *vertex_xy
                .get(first.endpoints[1] as usize)
                .ok_or(DualEmbeddingError::Unrepresentable)?,
        ];
        edge_drafts.push(DualEdgeDraft {
            endpoints: first.endpoints,
            coordinates,
            first,
            second,
        });
        if second.is_some() {
            continue;
        }
        let [a, b] = coordinates;
        let (a, b) = if first.forward { (a, b) } else { (b, a) };
        let side = if a.y == min_y && b.y == min_y {
            FrameSide::Bottom
        } else if a.x == max_x && b.x == max_x {
            FrameSide::Right
        } else if a.y == max_y && b.y == max_y {
            FrameSide::Top
        } else if a.x == min_x && b.x == min_x {
            FrameSide::Left
        } else {
            return Err(DualEmbeddingError::Unrepresentable);
        };
        let correctly_directed = match side {
            FrameSide::Bottom => a.x < b.x,
            FrameSide::Right => a.y < b.y,
            FrameSide::Top => a.x > b.x,
            FrameSide::Left => a.y > b.y,
        };
        if !correctly_directed {
            return Err(DualEmbeddingError::Unrepresentable);
        }
        let (lo, hi) = match side {
            FrameSide::Bottom | FrameSide::Top => (a.x.min(b.x), a.x.max(b.x)),
            FrameSide::Right | FrameSide::Left => (a.y.min(b.y), a.y.max(b.y)),
        };
        match side {
            FrameSide::Bottom => &mut *bottom,
            FrameSide::Right => &mut *right,
            FrameSide::Top => &mut *top,
            FrameSide::Left => &mut *left,
        }
        .push((lo, hi));
    }
    for (frame_side, side) in FrameSide::ALL.into_iter().zip([bottom, right, top, left]) {
        side.sort_unstable_by(|left, right| left.0.total_cmp(&right.0));
        let (start, end) = match frame_side {
            FrameSide::Bottom | FrameSide::Top => (min_x, max_x),
            FrameSide::Right | FrameSide::Left => (min_y, max_y),
        };
        let mut cursor = start;
        for &(lo, hi) in side.iter() {
            if lo != cursor {
                return Err(DualEmbeddingError::Unrepresentable);
            }
            cursor = hi;
        }
        if cursor != end {
            return Err(DualEmbeddingError::Unrepresentable);
        }
    }
    edge_drafts.sort_unstable_by(|left, right| {
        lex_xy(left.coordinates[0], right.coordinates[0])
            .then(lex_xy(left.coordinates[1], right.coordinates[1]))
    });
    let mut edge_id_by_pair = try_hash_map_with_capacity(edge_drafts.len())?;
    for (index, draft) in edge_drafts.iter().enumerate() {
        let edge = u32::try_from(index).map_err(|_| crate::error::ResourceError::SizeOverflow)?;
        if edge_id_by_pair
            .insert((draft.first.low, draft.first.high), edge)
            .is_some()
        {
            return Err(DualEmbeddingError::Unrepresentable);
        }
    }
    let mut published = 0_usize;
    if product.includes_polygons() {
        for ring in &rings.rings {
            let positions = ring
                .len()
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            let cell_count = polygon_output_count(positions, std::iter::empty())?;
            published = published
                .checked_add(cell_count)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
        }
    }
    if product.includes_edges() {
        let edge_positions = internal_edge_count
            .checked_mul(2)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        published = published
            .checked_add(edge_positions)
            .and_then(|count| count.checked_add(internal_edge_count))
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
    }
    let baseline = if product.includes_polygons() {
        clip.polygon_baseline.unwrap_or(0)
    } else {
        0
    };
    budget.add(published.saturating_sub(baseline))?;

    let mut receipt = site_receipts(VoronoiProducer::Dual, sites)?;
    receipt.clip_segments = clip_segment_receipts(clip, None)?;
    let mut vertex_origins: Vec<Vec<VoronoiVertexOrigin>> = std::iter::repeat_with(Vec::new)
        .take(vertex_xy.len())
        .collect();
    for (component, rounded) in center_rounded.iter().enumerate() {
        if let Some((_, id)) = rounded {
            let sites = *rings
                .center_sites
                .get(component)
                .ok_or(DualEmbeddingError::Unrepresentable)?;
            vertex_origins
                .get_mut(*id as usize)
                .ok_or(DualEmbeddingError::Unrepresentable)?
                .push(VoronoiVertexOrigin::Circumcenter {
                    component: u32::try_from(component)
                        .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
                    sites,
                });
        }
    }
    let mut twin_by_dart = Vec::with_capacity(directed.len());
    twin_by_dart.resize(directed.len(), None);
    let mut internal_edges = Vec::with_capacity(if product.includes_edges() {
        internal_edge_count
    } else {
        0
    });
    for (index, draft) in edge_drafts.iter().enumerate() {
        let edge_id =
            u32::try_from(index).map_err(|_| crate::error::ResourceError::SizeOverflow)?;
        if let Some(second) = draft.second {
            *twin_by_dart
                .get_mut(draft.first.dart as usize)
                .ok_or(DualEmbeddingError::Unrepresentable)? = Some(second.dart);
            *twin_by_dart
                .get_mut(second.dart as usize)
                .ok_or(DualEmbeddingError::Unrepresentable)? = Some(draft.first.dart);
        }
        let [start_id, end_id] = draft.endpoints;
        let [start, end] = draft.coordinates;
        let mut origins = vec![draft.first.origin];
        let source_segment = if clip.source.is_some() {
            draft.first.clip_segment
        } else {
            None
        };
        if let Some(segment) = source_segment {
            let source_origin = VoronoiEdgeOrigin::SourceClip { segment };
            if !origins.contains(&source_origin) {
                origins.push(source_origin);
            }
        }
        let separator = match draft.first.origin {
            VoronoiEdgeOrigin::Bisector { sites } => Some(sites),
            VoronoiEdgeOrigin::SourceClip { .. } | VoronoiEdgeOrigin::Frame { .. } => None,
        };
        let frame_side = match draft.first.origin {
            VoronoiEdgeOrigin::Frame { side } => Some(side),
            VoronoiEdgeOrigin::SourceClip { .. } | VoronoiEdgeOrigin::Bisector { .. } => None,
        };
        let mut owner_transition = [None, None];
        for use_edge in [Some(draft.first), draft.second].into_iter().flatten() {
            let [left_owner, right_owner] = &mut owner_transition;
            if use_edge.forward {
                *left_owner = Some(use_edge.owner);
            } else {
                *right_owner = Some(use_edge.owner);
            }
        }
        receipt.edges.push(VoronoiEdgeReceipt {
            id: edge_id,
            endpoints: [start_id, end_id],
            separator,
            clip_sources: source_segment.into_iter().collect(),
            frame_side,
            owner_transition,
            origins,
        });
        if let Some(sites) = separator {
            receipt.separators.push(VoronoiSeparatorReceipt {
                sites,
                vertices: [start_id, end_id].to_vec(),
                edges: vec![edge_id],
            });
            if product.includes_edges() {
                internal_edges.push([start, end]);
            }
        }
        if let Some(segment) = draft.first.clip_segment {
            let clip_use = clip_uses_by_dart
                .get(draft.first.dart as usize)
                .and_then(Option::as_ref)
                .ok_or(DualEmbeddingError::Unrepresentable)?;
            if clip_use.segment != segment {
                return Err(DualEmbeddingError::Unrepresentable);
            }
            let (start_parameter, end_parameter) = if draft.first.forward {
                (
                    clip_use.start_parameter.clone(),
                    clip_use.end_parameter.clone(),
                )
            } else {
                (
                    clip_use.end_parameter.clone(),
                    clip_use.start_parameter.clone(),
                )
            };
            let reversed = start_parameter.try_cmp(&end_parameter)? == Ordering::Greater;
            let (low_parameter, high_parameter) = if reversed {
                (end_parameter.clone(), start_parameter.clone())
            } else {
                (start_parameter.clone(), end_parameter.clone())
            };
            let clip_segment = receipt
                .clip_segments
                .get_mut(segment as usize)
                .ok_or(DualEmbeddingError::Unrepresentable)?;
            clip_segment.atoms.push(VoronoiClipAtomReceipt {
                edge: edge_id,
                reversed,
                start_parameter: low_parameter,
                end_parameter: high_parameter,
            });
            if clip.source.is_some() {
                for (vertex_id, parameter) in
                    [(start_id, &start_parameter), (end_id, &end_parameter)]
                {
                    for root in &clip_segment.source_roots {
                        let source_parameter = if root.reversed {
                            exact::ExactRational::one().try_subtract(parameter)?
                        } else {
                            parameter.clone()
                        };
                        vertex_origins
                            .get_mut(vertex_id as usize)
                            .ok_or(DualEmbeddingError::Unrepresentable)?
                            .push(VoronoiVertexOrigin::SourceClip {
                                segment,
                                parameter: source_parameter,
                                source_ring: Some(root.ring),
                                source_edge: Some(root.edge),
                            });
                    }
                }
            }
        }
        for vertex_id in [start_id, end_id] {
            if let Some(sites) = separator {
                vertex_origins
                    .get_mut(vertex_id as usize)
                    .ok_or(DualEmbeddingError::Unrepresentable)?
                    .push(VoronoiVertexOrigin::Bisector { sites });
            }
            if let Some(side) = frame_side {
                vertex_origins
                    .get_mut(vertex_id as usize)
                    .ok_or(DualEmbeddingError::Unrepresentable)?
                    .push(VoronoiVertexOrigin::Frame { side });
            }
        }
    }
    for site in sites {
        let site_id =
            u32::try_from(site.id).map_err(|_| crate::error::ResourceError::SizeOverflow)?;
        let key = PointKey::new(site.rounded_xy()?);
        if let Some((representative, vertex_id)) = interned.get(&key)
            && representative.same_position(&site.exact_point)?
        {
            vertex_origins
                .get_mut(*vertex_id as usize)
                .ok_or(DualEmbeddingError::Unrepresentable)?
                .push(VoronoiVertexOrigin::SourceSite { site: site_id });
        }
    }
    for (id, exact_point) in exact_by_id.iter().enumerate() {
        vertex_origins
            .get_mut(id)
            .ok_or(DualEmbeddingError::Unrepresentable)?
            .extend(chart_vertex_origins(chart, exact_point)?);
        let embedded = *vertex_xy
            .get(id)
            .ok_or(DualEmbeddingError::Unrepresentable)?;
        let origins = std::mem::take(
            vertex_origins
                .get_mut(id)
                .ok_or(DualEmbeddingError::Unrepresentable)?,
        );
        receipt.vertices.push(VoronoiVertexReceipt {
            id: u32::try_from(id).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
            exact_xy: exact_xy_coordinates(exact_point)?,
            origins,
            assignment: Some(embedded),
        });
        receipt.assignments.push(VoronoiAssignmentReceipt {
            vertex: u32::try_from(id).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
            embedded: u32::try_from(id).map_err(|_| crate::error::ResourceError::SizeOverflow)?,
            point: embedded,
        });
    }
    let mut edge_roles_by_ring = Vec::with_capacity(rings.rings.len());
    let mut dart_receipts = Vec::with_capacity(directed.len());
    for (ring_index, window) in offsets.array_windows::<2>().enumerate() {
        let [start, end] = *window;
        let ids = ids_all
            .get(start..end)
            .ok_or(DualEmbeddingError::Unrepresentable)?;
        let owner = rings
            .owners
            .get(ring_index)
            .ok_or(DualEmbeddingError::Unrepresentable)?
            .0;
        let mut cell_uses = Vec::with_capacity(ids.len());
        let mut role_row = Vec::with_capacity(ids.len());
        for (&from, &to) in ids.iter().zip(ids.iter().cycle().skip(1)) {
            let (low, high) = if from <= to { (from, to) } else { (to, from) };
            let edge_id = *edge_id_by_pair
                .get(&(low, high))
                .ok_or(DualEmbeddingError::Unrepresentable)?;
            let dart_id = u32::try_from(dart_receipts.len())
                .map_err(|_| crate::error::ResourceError::SizeOverflow)?;
            let directed_edge = by_dart
                .get(dart_id as usize)
                .and_then(|edge| *edge)
                .ok_or(DualEmbeddingError::Unrepresentable)?;
            if directed_edge.from != from || directed_edge.to != to || directed_edge.owner != owner
            {
                return Err(DualEmbeddingError::Unrepresentable);
            }
            let forward = directed_edge.forward;
            let edge_record = receipt
                .edges
                .get(edge_id as usize)
                .ok_or(DualEmbeddingError::Unrepresentable)?;
            let mut source_uses = Vec::new();
            if let Some(segment) = directed_edge.clip_segment
                && clip.source.is_some()
            {
                let clip_use = clip_uses_by_dart
                    .get(dart_id as usize)
                    .and_then(Option::as_ref)
                    .ok_or(DualEmbeddingError::Unrepresentable)?;
                if clip_use.segment != segment {
                    return Err(DualEmbeddingError::Unrepresentable);
                }
                let start_parameter = &clip_use.start_parameter;
                let end_parameter = &clip_use.end_parameter;
                let clip_segment = receipt
                    .clip_segments
                    .get(segment as usize)
                    .ok_or(DualEmbeddingError::Unrepresentable)?;
                for root in &clip_segment.source_roots {
                    let source_start = if root.reversed {
                        exact::ExactRational::one().try_subtract(start_parameter)?
                    } else {
                        start_parameter.clone()
                    };
                    let source_end = if root.reversed {
                        exact::ExactRational::one().try_subtract(end_parameter)?
                    } else {
                        end_parameter.clone()
                    };
                    source_uses.push(VoronoiEdgeSourceUse {
                        source_segment: segment,
                        source_operand: None,
                        source_ring: Some(
                            u32::try_from(root.ring)
                                .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
                        ),
                        source_edge: Some(
                            u32::try_from(root.edge)
                                .map_err(|_| crate::error::ResourceError::SizeOverflow)?,
                        ),
                        forward: source_start.try_cmp(&source_end)? != Ordering::Greater,
                        start_parameter: source_start,
                        end_parameter: source_end,
                    });
                }
            }
            role_row.push(VoronoiRingEdgeRole {
                edge: Some(edge_id),
                forward,
                origins: edge_record.origins.clone(),
                source_uses,
            });
            cell_uses.push(VoronoiDirectedEdgeUse {
                edge: edge_id,
                forward,
            });
            dart_receipts.push(VoronoiDartReceipt {
                id: dart_id,
                origin: from,
                target: to,
                twin: twin_by_dart.get(dart_id as usize).copied().flatten(),
                edge: edge_id,
                face: None,
                owner: Some(owner),
            });
        }
        receipt.cells.push(VoronoiCellReceipt {
            owner,
            face: None,
            shell: cell_uses,
            holes: Vec::new(),
        });
        edge_roles_by_ring.push(role_row);
    }
    receipt.darts = dart_receipts;
    let edge_data = EmbeddedEdgeData::Segments(internal_edges);
    let mut polygons = Vec::new();
    if product.includes_polygons() {
        polygons.reserve(rings.rings.len());
        for (ring_index, window) in offsets.array_windows::<2>().enumerate() {
            let [start, end] = *window;
            let rounded = rounded_all
                .get(start..end)
                .expect("row offsets bracket appends to this rounded vertex column");
            let first = rounded
                .first()
                .expect("dual_build_rings admits only cells with at least three vertices");
            let capacity = rounded
                .len()
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            ensure_coordseq_vertex_capacity(capacity)?;
            let mut xs = Vec::with_capacity(capacity);
            let mut ys = Vec::with_capacity(capacity);
            xs.extend(rounded.iter().map(|xy| xy.x));
            ys.extend(rounded.iter().map(|xy| xy.y));
            xs.push(first.x);
            ys.push(first.y);
            let ring = Ring::closed_coordseq(CoordSeq::try_from_vecs(xs, ys, None, None)?)?;
            let edge_roles = vec![
                edge_roles_by_ring
                    .get(ring_index)
                    .cloned()
                    .ok_or(DualEmbeddingError::Unrepresentable)?,
            ];
            let producer_ids = ids_all
                .get(start..end)
                .ok_or(DualEmbeddingError::Unrepresentable)?;
            let producer_vertices = vec![
                producer_ids
                    .iter()
                    .map(|&id| {
                        Ok(OverlayNodeReceipt {
                            id,
                            exact_point: exact_by_id
                                .get(id as usize)
                                .ok_or(DualEmbeddingError::Unrepresentable)?
                                .clone(),
                            assignment: *vertex_xy
                                .get(id as usize)
                                .ok_or(DualEmbeddingError::Unrepresentable)?,
                        })
                    })
                    .collect::<std::result::Result<Vec<_>, DualEmbeddingError>>()?,
            ];
            let owner = rings
                .owners
                .get(ring_index)
                .ok_or(DualEmbeddingError::Unrepresentable)?
                .0;
            polygons.push(EmbeddedPolygon {
                polygon: Polygon::new(ring, Vec::new()),
                owner: Some(owner),
                edge_roles,
                producer_vertices,
            });
        }
    }
    let receipt = std::sync::Arc::new(receipt);
    Ok(EmbeddedVoronoi {
        polygons,
        edges: EmbeddedEdges {
            data: edge_data,
            receipt: std::sync::Arc::clone(&receipt),
        },
        receipt,
    })
}

#[cfg(test)]
pub(super) fn dual_build(
    sites: &[Site],
    boundary: VoronoiBoundary<'_>,
    budget: &mut ExpansionBudget,
    mesh: &CertifiedDelaunay,
) -> Result<EmbeddedVoronoi> {
    dual_build_product(sites, boundary, budget, mesh, VoronoiProduct::Both, None)
}

fn dual_build_product(
    sites: &[Site],
    boundary: VoronoiBoundary<'_>,
    budget: &mut ExpansionBudget,
    mesh: &CertifiedDelaunay,
    product: VoronoiProduct,
    chart: Option<&PointSelectionChart>,
) -> Result<EmbeddedVoronoi> {
    let clip = normalize_clip_with_chart(sites, boundary, product.includes_polygons(), chart)?;
    if mesh.collinear_order().is_some() {
        return Err(voronoi_error(
            "collinear sites require the exact one-dimensional lane",
        ));
    }
    let complex = delaunay_complex(mesh, sites)?;
    let rings = dual_build_rings(sites, mesh, complex, &clip)?;
    match dual_embed(&rings, &clip, sites, product, budget, chart) {
        Ok(result) => Ok(result),
        Err(DualEmbeddingError::Unrepresentable) => {
            // The optimized lane remains byte-identical when its nearest
            // embedding passes. Only a binary64 certificate failure uses the
            // general DCEL lane, where constructed vertices can move to
            // adjacent floats under the complete cell/face certificate.
            build_product_with_chart(sites, boundary, budget, false, mesh, product, chart)
        },
        Err(DualEmbeddingError::Other(error)) => Err(error),
    }
}
