//! Exact source partition of unique stored segments.
//!
//! Borrows unique undirected representatives and existing CSR provenance.
//! A node is a stored endpoint or the crossing of two unique segments. Every
//! node carries its nearest-even binary64, certified in double-double for a
//! crossing; a crossing's exact homogeneous point is built only when a
//! comparison ties on the rounded coordinates, since rounding is monotone.
//! Exact cyclic walks and leftward ray contacts define global faces; stored-ring
//! winding selects cells and an emission plan. Emission checks rounded
//! incidence, cyclic order, and global-face identity.
//!
//! Candidates come from the shared fallible sweep over unique segments plus
//! extra isolated points as zero-length items. Contact-derived cuts, compact
//! nodes, and canonical atoms are generated items. Sweep scratch is
//! [`ResourceError`], not the item cap.

use std::borrow::{Borrow, Cow};
use std::cell::Cell;
use std::cmp::Ordering;
#[cfg(test)]
use std::ops::ControlFlow;
use std::ops::Range;
use std::sync::Arc;

#[cfg(test)]
use crate::collections::HashMap;
use crate::error::ResourceError;
use crate::geographic::GeographicFrame;
use crate::geometry::arrangement::{IndexedTopology, IndexedTopologyScratch};
use crate::geometry::embedding_search::EvaluationBudget;
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::ordinates::{
    NodeOrdinates, RequestRowInput, RootCatalog, RootId, RootMetricState, RootRange, RootRequest,
    SourceArcContext, SourceArcMetricAccess, SourceArcRequest, resolve_request_rows,
    resolve_source_root, source_arc_ordinates,
};
use crate::geometry::overlay::noding::{
    SegmentProvenance, SegmentSource, dedup_undirected_segments_sourced, placement_offsets,
};
use crate::geometry::overlay::{build_certified_overlay_shape, ray_crossing};
use crate::geometry::segments::{
    RectContact, SegmentClipInterval, clip_segment_rect_interval, segment_vs_rect,
    stored_cross_sign, stored_orientation_sign,
};
use crate::geometry::tessellation::exact::{ExactDyadic, ExactRational};
use crate::geometry::topology::SourceTrace;
use crate::geometry::{
    Bounds, ChartMetric, Contact, CoordSeq, CoordinateAxes, Coordinates as _, ExpansionBudget,
    GeometryErrorKind, GeometryKind, LineSeq, Loc, MOrdinate, MultiLineString, Point, PointKey,
    Polygon, PolygonizeFull, RepairMethod, Result, Ring, RingKind, Segment, Shape,
    SharedShapeOwners, Unmetered, Work, XY, ZOrdinate, line_segments, remove_antenna_vertices,
    same_point, segment_contact_exact, storage,
};
#[cfg(test)]
use crate::geometry::{for_each_candidate_pair, single_chain};

mod construction;
mod coverage;
mod embedding;
mod emission;
mod location;
#[cfg(test)]
mod metrics;
mod rays;
mod relate;

#[cfg(test)]
thread_local! {
    static SOURCE_LINE_RUNS_CALLS: Cell<usize> = const { Cell::new(0) };
    static SUBDIVISION_ROOT_XY_POSITION_CALLS: Cell<usize> = const { Cell::new(0) };
    static SUBDIVISION_RESOLUTION_BATCHES: std::cell::RefCell<Vec<(usize, usize, usize)>> =
        const { std::cell::RefCell::new(Vec::new()) };
    static SOURCE_SPAN_VIEW_SORTS: Cell<usize> = const { Cell::new(0) };
    static SOURCE_SPAN_VIEW_ORDERED: Cell<usize> = const { Cell::new(0) };
    static SOURCE_SPAN_ENDPOINT_PARAMETERS: Cell<usize> = const { Cell::new(0) };
    static SOURCE_SPAN_INTERIOR_PARAMETERS: Cell<usize> = const { Cell::new(0) };
}

#[cfg(test)]
fn reset_source_line_runs_calls() {
    SOURCE_LINE_RUNS_CALLS.with(|calls| calls.set(0));
}

#[cfg(test)]
fn source_line_runs_calls() -> usize {
    SOURCE_LINE_RUNS_CALLS.with(Cell::get)
}

#[cfg(test)]
pub(crate) fn reset_subdivision_root_xy_position_calls() {
    SUBDIVISION_ROOT_XY_POSITION_CALLS.with(|calls| calls.set(0));
}

#[cfg(test)]
pub(crate) fn subdivision_root_xy_position_calls() -> usize {
    SUBDIVISION_ROOT_XY_POSITION_CALLS.with(Cell::get)
}

#[cfg(test)]
pub(crate) fn reset_subdivision_resolution_batches() {
    SUBDIVISION_RESOLUTION_BATCHES.with(|batches| batches.borrow_mut().clear());
}

#[cfg(test)]
pub(crate) fn take_subdivision_resolution_batches() -> Vec<(usize, usize, usize)> {
    SUBDIVISION_RESOLUTION_BATCHES.with(|batches| std::mem::take(&mut *batches.borrow_mut()))
}

#[cfg(test)]
pub(crate) fn reset_source_span_view_order_counts() {
    SOURCE_SPAN_VIEW_SORTS.with(|count| count.set(0));
    SOURCE_SPAN_VIEW_ORDERED.with(|count| count.set(0));
    SOURCE_SPAN_ENDPOINT_PARAMETERS.with(|count| count.set(0));
    SOURCE_SPAN_INTERIOR_PARAMETERS.with(|count| count.set(0));
}

#[cfg(test)]
pub(crate) fn source_span_view_order_counts() -> (usize, usize, usize, usize) {
    (
        SOURCE_SPAN_VIEW_SORTS.with(Cell::get),
        SOURCE_SPAN_VIEW_ORDERED.with(Cell::get),
        SOURCE_SPAN_ENDPOINT_PARAMETERS.with(Cell::get),
        SOURCE_SPAN_INTERIOR_PARAMETERS.with(Cell::get),
    )
}

pub(in crate::geometry) use construction::Forest as ExactPointForest;
use construction::{
    AxisValue, PointArena, interpolate_ordinate, parameter_along, partition_unique_segments,
    partition_unique_segments_in_window, partition_unique_segments_unbudgeted,
};
pub(in crate::geometry) use embedding::{EmbeddingDomain, segments_within_chebyshev};
use emission::{
    BoundaryPlan, CellFacts, CoverageFaceState, NodeParts, polygonize_emission,
    polygonize_full_emission,
};
pub(in crate::geometry) use emission::{
    GlobalFaces, emitted_pair_preserves_incidence, walk_is_shell_with_work,
};
use location::AreaLocationCells;
pub(in crate::geometry) use location::AreaLocationStrategy;
#[cfg(test)]
pub(in crate::geometry) use metrics::{
    partition_builds, partition_metrics, reset_partition_builds,
};
#[cfg(test)]
pub(crate) use metrics::{partition_calls, reset_partition_calls};
#[cfg(test)]
use metrics::{record_partition_call, update as update_partition_metrics};
pub(in crate::geometry) use rays::{
    RayPoints, compare_departures_with_work, shoot_left, shoot_left_with_work,
    visit_component_faces,
};

pub(in crate::geometry) use self::Axis as RayAxis;

pub(in crate::geometry) fn clip_polygonal_rectangle<P: AsRef<Polygon>>(
    polygons: &[P],
    rectangle: &Polygon,
    bounds: Bounds,
    axes: CoordinateAxes,
    strict: bool,
    rectangle_first: bool,
) -> Result<Option<Shape>> {
    if polygons.is_empty() {
        return Ok(Some(Shape::typed_empty(GeometryKind::Polygon, axes)));
    }
    let Some((inventory, intervals)) =
        ExactSourceInventory::from_polygon_rect_clip(polygons, rectangle, bounds, rectangle_first)?
    else {
        return Ok(None);
    };
    let mut budget = ExpansionBudget::new("clip_by_rect", "rectangle-local exact partition");
    let mut partition = inventory.partition_in_window(&intervals, bounds, &mut budget)?;
    let (topology, edges) = partition.local_topology()?;
    let global = partition.global_faces(&topology, &edges)?;
    partition.winding_seed =
        inventory.rectangle_winding_seed(&partition, &topology, &edges, &global, bounds)?;
    let result = inventory.overlay_shape_with_faces::<crate::crs::GeodesicMetric>(
        &partition,
        &topology,
        &edges,
        &global,
        crate::geometry::OverlayOp::Intersection,
        axes,
        strict,
        &crate::geometry::SourceArcContext::cartesian(),
    )?;
    Ok(Some(if result.is_empty() {
        Shape::typed_empty(GeometryKind::Polygon, axes)
    } else {
        result.normalize()?
    }))
}

#[cfg(test)]
use crate::geometry::segments::crossing_homogeneous;
#[cfg(test)]
use crate::geometry::tessellation::exact::{compare_ratios, round_ratio};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(in crate::geometry) struct NodeId(pub u32);

pub(super) struct ResolvedNodePoints<'e, 'p> {
    embedding: &'e embedding::Embedding<'p>,
    by_node: Option<Vec<Option<(ZOrdinate, MOrdinate)>>>,
    by_xy: Option<Vec<Option<XY>>>,
}

impl ResolvedNodePoints<'_, '_> {
    pub(super) fn coordinates(&self) -> &[XY] {
        self.embedding.coordinates()
    }

    pub(super) fn get(&self, node: NodeId, xy: XY) -> Option<Point> {
        let xy = self
            .by_xy
            .as_ref()
            .and_then(|points| points.get(node.0 as usize))
            .copied()
            .flatten()
            .unwrap_or(xy);
        let Some(by_node) = &self.by_node else {
            return Some(Point::new_raw_xy(xy.x, xy.y));
        };
        let (z, m) = by_node.get(node.0 as usize).copied().flatten()?;
        Some(Point::new_raw_axes(xy.x, xy.y, z, m))
    }

    pub(super) fn ordinates(&self, node: NodeId) -> Option<(ZOrdinate, MOrdinate)> {
        self.by_node.as_ref().map_or_else(
            || Some((ZOrdinate(None), MOrdinate(None))),
            |by_node| by_node.get(node.0 as usize).copied().flatten(),
        )
    }

    pub(super) fn point(&self, node: NodeId, xy: XY) -> Point {
        self.get(node, xy)
            .expect("emitted node has a resolved point")
    }
}

impl<'e, 'p> ResolvedNodePoints<'e, 'p> {
    const fn from_embedding(
        embedding: &'e embedding::Embedding<'p>,
        by_node: Option<Vec<Option<(ZOrdinate, MOrdinate)>>>,
    ) -> Self {
        Self {
            embedding,
            by_node,
            by_xy: None,
        }
    }

    const fn from_embedding_with_xy(
        embedding: &'e embedding::Embedding<'p>,
        by_node: Option<Vec<Option<(ZOrdinate, MOrdinate)>>>,
        by_xy: Vec<Option<XY>>,
    ) -> Self {
        Self {
            embedding,
            by_node,
            by_xy: Some(by_xy),
        }
    }
}

fn resolve_traced_node_requests<'e, 'p, M: ChartMetric, W: Work, F>(
    partition: &'p ExactPartition<'_>,
    embedding: &'e embedding::Embedding<'p>,
    nodes: &[NodeId],
    mut requests_by_node: Vec<Vec<RootRequest>>,
    axes: CoordinateAxes,
    root_positions: Option<&SubdivisionRootCatalog>,
    root_xy_nodes: Option<&[bool]>,
    fallback_inventory: Option<&ExactSourceInventory>,
    reuse_existing_source_vertices: bool,
    context: &SourceArcContext<'_, '_, M>,
    work: &mut W,
    resolve_rows: &mut F,
) -> Result<ResolvedNodePoints<'e, 'p>>
where
    F: FnMut(&[RequestRowInput], &mut W) -> Result<Vec<NodeOrdinates>>,
{
    if root_positions.is_some() != root_xy_nodes.is_some() {
        return Err(GeometryErrorKind::Invalid(
            "subdivision root positions and node selection disagree".into(),
        )
        .into());
    }
    let has_root_xy_nodes = if let Some(root_xy_nodes) = root_xy_nodes {
        if root_xy_nodes.len() != partition.node_count() {
            return Err(GeometryErrorKind::Invalid(
                "subdivision root-position node selection has the wrong length".into(),
            )
            .into());
        }
        let mut any = false;
        for &node in nodes {
            any |= *root_xy_nodes.get(node.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "subdivision root-position node is outside its partition".into(),
                )
            })?;
        }
        any
    } else {
        false
    };
    if !axes.has_z() && !axes.has_m() && !has_root_xy_nodes {
        return Ok(ResolvedNodePoints::from_embedding(embedding, None));
    }
    work.charge(u64::try_from(partition.node_count()).map_err(|_| ResourceError::SizeOverflow)?)?;
    let rooted_xy = if let Some(root_positions) = root_positions
        && has_root_xy_nodes
    {
        let root_xy_nodes = root_xy_nodes.ok_or_else(|| {
            GeometryErrorKind::Invalid("subdivision root-position node selection is missing".into())
        })?;
        let mut rooted_xy = vec![None; partition.node_count()];
        for &node in nodes {
            if !root_xy_nodes.get(node.0 as usize).copied().ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "subdivision root-position node is outside its partition".into(),
                )
            })? {
                continue;
            }
            let requests = requests_by_node.get(node.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("subdivision emission node is unknown".into())
            })?;
            let mut position = None;
            let mut agrees = true;
            for request in requests {
                let Some(candidate) = root_positions.source_node_position(
                    request.root(),
                    request.parameter(),
                    work,
                )?
                else {
                    continue;
                };
                if position.is_some_and(|previous| !same_point(previous, candidate)) {
                    agrees = false;
                    break;
                }
                position = Some(candidate);
            }
            if agrees {
                let slot = rooted_xy.get_mut(node.0 as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "subdivision source position node is outside its partition".into(),
                    )
                })?;
                *slot = position;
            }
        }
        Some(rooted_xy)
    } else {
        None
    };
    if !axes.has_z() && !axes.has_m() {
        return Ok(match rooted_xy {
            Some(rooted_xy) => {
                ResolvedNodePoints::from_embedding_with_xy(embedding, None, rooted_xy)
            },
            None => ResolvedNodePoints::from_embedding(embedding, None),
        });
    }
    let mut by_node = vec![None; partition.node_count()];
    let mut fallback_nodes = Vec::new();
    let mut request_rows = Vec::with_capacity(nodes.len());
    let mut request_row_nodes = Vec::with_capacity(nodes.len());
    for &node in nodes {
        work.charge(1)?;
        if reuse_existing_source_vertices
            && let Some(fallback_inventory) = fallback_inventory
            && let Some(ordinates) =
                fallback_inventory.existing_source_vertex_ordinates(partition, node, axes, 0)?
            && (!axes.has_z() || ordinates.z.value().is_some())
            && (!axes.has_m() || ordinates.m.value().is_some())
        {
            let slot = by_node.get_mut(node.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "existing subdivision vertex is outside the partition".into(),
                )
            })?;
            *slot = Some((
                ZOrdinate(ordinates.z.value()),
                MOrdinate(ordinates.m.value()),
            ));
            continue;
        }
        let requests = requests_by_node.get_mut(node.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("subdivision emission node is unknown".into())
        })?;
        if requests.is_empty() {
            if fallback_inventory.is_some() {
                fallback_nodes.push(node);
            }
            continue;
        }
        request_rows.push(RequestRowInput {
            axes,
            requests: std::mem::take(requests),
        });
        request_row_nodes.push(node);
    }
    let ordinates = resolve_rows(&request_rows, work)?;
    if ordinates.len() != request_rows.len() {
        return Err(GeometryErrorKind::Invalid(
            "original-root resolution returned incomplete traced-node ordinates".into(),
        )
        .into());
    }
    for (node, ordinate) in request_row_nodes.into_iter().zip(ordinates) {
        let slot = by_node.get_mut(node.0 as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("subdivision output node is outside its partition".into())
        })?;
        *slot = Some((ZOrdinate(ordinate.z.value()), MOrdinate(ordinate.m.value())));
    }
    if let Some(fallback_inventory) = fallback_inventory {
        if !fallback_nodes.is_empty() {
            work.charge(
                u64::try_from(fallback_nodes.len()).map_err(|_| ResourceError::SizeOverflow)?,
            )?;
            let fallback = fallback_inventory.resolve_node_points(
                partition,
                embedding,
                fallback_nodes.iter().copied(),
                axes,
                context,
                None,
            )?;
            for node in fallback_nodes {
                if let Some(ordinates) = fallback.ordinates(node) {
                    let slot = by_node.get_mut(node.0 as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "fallback subdivision node is outside the partition".into(),
                        )
                    })?;
                    *slot = Some(ordinates);
                }
            }
        }
    }
    Ok(match rooted_xy {
        Some(rooted_xy) => {
            ResolvedNodePoints::from_embedding_with_xy(embedding, Some(by_node), rooted_xy)
        },
        None => ResolvedNodePoints::from_embedding(embedding, Some(by_node)),
    })
}

fn mark_root_xy_node(
    root_xy_nodes: &mut [bool],
    node: NodeId,
    local_parameter: &ExactRational,
    appended_requests: usize,
) -> Result<()> {
    if appended_requests == 0 || local_parameter.is_zero() || local_parameter.is_one() {
        return Ok(());
    }
    let slot = root_xy_nodes.get_mut(node.0 as usize).ok_or_else(|| {
        GeometryErrorKind::Invalid("subdivision root-position node is outside its partition".into())
    })?;
    *slot = true;
    Ok(())
}

#[cfg(test)]
struct ArealSources {
    segments: Vec<Segment>,
    source_rings: Vec<Option<u32>>,
    polygon_rings: Vec<Range<usize>>,
    ring_count: usize,
    collapsed_rings: Vec<(u32, Range<usize>)>,
}

struct ArealCells {
    faces: Vec<bool>,
    spans: Vec<bool>,
    nodes: Vec<bool>,
}

struct BoundarySelection {
    cells: ArealCells,
    boundary_nodes: Vec<NodeId>,
    has_curve_support: bool,
    has_point_support: bool,
}

impl ArealCells {
    fn emission_plan(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
        edges: &[u32],
        global: &GlobalFaces,
    ) -> Result<BoundaryPlan> {
        self.emission_plan_with_protected(partition, topology, edges, global, None, None)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "Slot, twin, face, occupancy, and span IDs are established by the same topology and selection"
    )]
    fn selected_boundary_spans(
        &self,
        topology: &IndexedTopology,
        edges: &[u32],
        global: &GlobalFaces,
    ) -> Vec<bool> {
        let mut selected = Vec::with_capacity(self.spans.len());
        selected.resize(self.spans.len(), false);
        for (slot, &span) in edges.iter().enumerate() {
            let left = global.walk_faces[topology.face_of[slot] as usize] as usize;
            let twin = topology.twins[slot] as usize;
            let right = global.walk_faces[topology.face_of[twin] as usize] as usize;
            if self.faces[left] && !self.faces[right] {
                selected[span as usize] = true;
            }
        }
        selected
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "lower_dimensions returns admitted NodeIds and each topology column shares the partition node/face domains"
    )]
    fn emission_plan_with_protected(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
        edges: &[u32],
        global: &GlobalFaces,
        extra_protected: Option<&[bool]>,
        preserved_spans: Option<&[bool]>,
    ) -> Result<BoundaryPlan> {
        let (lines, points) = self.lower_dimensions(partition, topology, edges, global);
        let mut plan = global.boundary_plan_with_protected(
            &lines,
            topology,
            edges,
            partition,
            &self.faces,
            extra_protected,
            preserved_spans,
        )?;
        plan.points.reserve(points.len());
        for node in points {
            let host = global.node_host_face(topology, node);
            plan.points.push((node, plan.face_quotient[host as usize]));
        }
        Ok(plan)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "edge and face IDs come from this partition's topology, while covered masks are sized from the same span and node counts"
    )]
    fn lower_dimensions(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
        edges: &[u32],
        global: &GlobalFaces,
    ) -> (Vec<u32>, Vec<NodeId>) {
        let mut covered_spans = Vec::with_capacity(self.spans.len());
        covered_spans.resize(self.spans.len(), false);
        for (&span, &walk) in edges.iter().zip(&topology.face_of) {
            let face = global.walk_faces[walk as usize];
            covered_spans[span as usize] |= self.faces[face as usize];
        }
        let mut covered_nodes = Vec::with_capacity(self.nodes.len());
        covered_nodes.resize(self.nodes.len(), false);
        let mut lines = Vec::new();
        for (index, ((span, &selected), &covered)) in partition
            .spans
            .iter()
            .zip(&self.spans)
            .zip(&covered_spans)
            .enumerate()
        {
            if selected || covered {
                covered_nodes[span.start.0 as usize] = true;
                covered_nodes[span.end.0 as usize] = true;
            }
            if selected && !covered {
                lines.push(index as u32);
            }
        }
        for node in &partition.isolated {
            let component = topology.component_of[node.0 as usize];
            covered_nodes[node.0 as usize] |=
                self.faces[global.component_hosts[component as usize] as usize];
        }
        let mut points = Vec::new();
        for (index, (&selected, &covered)) in self.nodes.iter().zip(&covered_nodes).enumerate() {
            if selected && !covered {
                points.push(NodeId(index as u32));
            }
        }
        (lines, points)
    }
}

#[cfg(test)]
impl ArealSources {
    fn new(polygons: &[impl AsRef<Polygon>]) -> Result<Self> {
        let mut sources = Self {
            segments: Vec::new(),
            source_rings: Vec::new(),
            polygon_rings: Vec::new(),
            ring_count: 0,
            collapsed_rings: Vec::new(),
        };
        sources.polygon_rings.reserve(polygons.len());
        for polygon in polygons {
            let first_ring = sources.ring_count;
            for ring in polygon.as_ref().rings() {
                let ring_id =
                    u32::try_from(sources.ring_count).map_err(|_| ResourceError::SizeOverflow)?;
                sources.ring_count = sources
                    .ring_count
                    .checked_add(1)
                    .ok_or(ResourceError::SizeOverflow)?;
                let collapsed = crate::geometry::ring_is_collinear(ring.coords());
                let first_segment = sources.segments.len();
                let count = ring.len().saturating_sub(1);
                let total = sources
                    .segments
                    .len()
                    .checked_add(count)
                    .ok_or(ResourceError::SizeOverflow)?;
                u32::try_from(total).map_err(|_| ResourceError::SizeOverflow)?;
                sources.segments.reserve(count);
                sources.source_rings.reserve(count);
                let first = ring
                    .iter_coords()
                    .next()
                    .expect("a Ring has at least four coordinates");
                let mut pairs = ring.segment_pairs().peekable();
                while let Some([start, mut end]) = pairs.next() {
                    if pairs.peek().is_none() {
                        end = first;
                    }
                    sources.segments.push(Segment {
                        start: start.xy(),
                        end: end.xy(),
                    });
                    sources.source_rings.push((!collapsed).then_some(ring_id));
                }
                if collapsed {
                    sources
                        .collapsed_rings
                        .push((ring_id, first_segment..sources.segments.len()));
                }
            }
            sources.polygon_rings.push(first_ring..sources.ring_count);
        }
        Ok(sources)
    }

    fn occupied(&self, windings: &[i64], odd: bool) -> bool {
        let filled = |ring: usize| {
            if odd {
                windings[ring] % 2 != 0
            } else {
                windings[ring] != 0
            }
        };
        self.polygon_rings.iter().any(|rings| {
            let mut rings = rings.clone();
            rings.next().is_some_and(filled) && !rings.any(filled)
        })
    }

    fn cells(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
        edges: &[u32],
        global: &GlobalFaces,
        odd: bool,
    ) -> Result<ArealCells> {
        let mut cells = ArealCells {
            faces: Vec::new(),
            spans: Vec::new(),
            nodes: Vec::new(),
        };
        cells.faces.reserve(global.count as usize);
        cells.faces.resize(global.count as usize, false);
        cells.spans.reserve(partition.spans.len());
        cells.spans.resize(partition.spans.len(), false);
        cells.nodes.reserve(partition.node_count());
        cells.nodes.resize(partition.node_count(), false);
        partition.visit_face_windings(
            topology,
            edges,
            global,
            &self.source_rings,
            self.ring_count,
            |face, winding| {
                cells.faces[face as usize] = self.occupied(winding, odd);
                Ok(())
            },
        )?;
        for (slot, &span) in edges.iter().enumerate() {
            let face = global.walk_faces[topology.face_of[slot] as usize];
            cells.spans[span as usize] |= cells.faces[face as usize];
        }
        for (index, span) in partition.spans.iter().enumerate() {
            cells.spans[index] |= partition
                .owners(index)
                .iter()
                .any(|&(source, _)| self.source_rings[source as usize].is_none());
            if cells.spans[index] {
                cells.nodes[span.start.0 as usize] = true;
                cells.nodes[span.end.0 as usize] = true;
            }
        }
        for &(node, source) in &partition.point_sources {
            cells.nodes[node.0 as usize] |= self.source_rings[source as usize].is_none();
        }
        for node in &partition.isolated {
            let component = topology.component_of[node.0 as usize];
            let host = global.component_hosts[component as usize];
            cells.nodes[node.0 as usize] |= cells.faces[host as usize];
        }
        Ok(cells)
    }
}

/// What a node is: a stored endpoint, or the crossing of two unique segments'
/// carriers, whose exact homogeneous point is built only for a comparison its
/// rounded coordinates cannot decide.
#[derive(Debug)]
pub(in crate::geometry) enum NodeRecipe {
    Stored,
    Constructed {
        exact: std::cell::OnceCell<Box<[ExactDyadic; 3]>>,
    },
    Crossing {
        segments: [u32; 2],
        exact: std::cell::OnceCell<Box<[ExactDyadic; 3]>>,
    },
}

#[derive(Clone, Copy)]
pub(in crate::geometry) enum Axis {
    X,
    Y,
}

impl Axis {
    const fn of(self, point: XY) -> f64 {
        match self {
            Self::X => point.x,
            Self::Y => point.y,
        }
    }

    const fn index(self) -> usize {
        match self {
            Self::X => 0,
            Self::Y => 1,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::geometry) struct ExactSpan {
    pub start: NodeId,
    pub end: NodeId,
    pub unique: u32,
}

#[derive(Debug)]
pub(in crate::geometry) struct ExactMetricNode {
    points: crate::geometry::ExactLocationPoints,
    node: u32,
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
pub(in crate::geometry) struct ExactLineEmission<'p> {
    embedding: embedding::Embedding<'p>,
    items: Vec<[NodeId; 2]>,
    owners: Vec<ExactLineSource>,
    offsets: Vec<u32>,
}

impl ExactLineEmission<'_> {
    pub(in crate::geometry) const fn len(&self) -> usize {
        self.items.len()
    }

    pub(in crate::geometry) fn segments_with_nodes(
        &self,
    ) -> impl ExactSizeIterator<Item = ([NodeId; 2], Segment)> + '_ {
        let coordinates = self.embedding.coordinates();
        self.items
            .iter()
            .copied()
            .map(move |nodes| (nodes, embedding::Embedding::segment_at(coordinates, nodes)))
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "the span ordinal is admitted by ExactPartition and offsets has one checked terminal entry"
    )]
    pub(in crate::geometry) fn sources(&self, span: usize) -> &[ExactLineSource] {
        let start = self.offsets[span] as usize;
        let end = self.offsets[span.wrapping_add(1)] as usize;
        &self.owners[start..end]
    }
}

#[derive(Debug)]
pub(in crate::geometry) struct ExactPartition<'a> {
    unique: &'a [Segment],
    /// Every node's point, indexed by `NodeId`. A stored node keeps its first
    /// source spelling: `PointKey` canonicalizes signed zero for identity, and
    /// emission must not do the same to an endpoint already in the input.
    points: PointArena<'a>,
    pub spans: Vec<ExactSpan>,
    pub provenance: SegmentProvenance,
    pub isolated: Vec<NodeId>,
    /// Node ownership in explicit input-point occurrence order, including duplicates.
    pub point_nodes: Vec<NodeId>,
    pub point_sources: Vec<(NodeId, u32)>,
    /// Source-ring winding at an interior face adjacent to the rectangle
    /// boundary. Clipped partitions omit outside arcs, so face zero need not
    /// have one source-ring winding; full-ring partitions leave this absent.
    winding_seed: Option<(u32, Vec<i64>)>,
    ordinate_bindings: Option<OrdinateBindings>,
    kept_nodes: KeptNodes,
    embedding_domain: embedding::EmbeddingDomain,
}

/// Source carriers through each node, retained only for Z/M interpolation.
#[derive(Debug)]
struct OrdinateBindings {
    starts: Vec<u32>,
    unique: Vec<u32>,
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
    source_segments: Vec<Segment>,
    unique: Vec<Segment>,
    provenance: SegmentProvenance,
    source_operands: Vec<u32>,
    source_rings: Vec<Option<u32>>,
    ring_kinds: Vec<RingKind>,
    ring_polygons: Vec<u32>,
    ring_holes: Vec<bool>,
    polygon_count: usize,
    polygon_operands: Vec<u32>,
    polygon_ring_ranges: Vec<Range<usize>>,
    source_points: Option<Vec<SourceArcRecord>>,
    polygon_rings: Vec<Vec<Range<usize>>>,
    ring_count: usize,
    isolated: Vec<XY>,
    isolated_points: Vec<Point>,
    isolated_operands: Vec<u32>,
    line_boundaries: Vec<(u32, PointKey)>,
}

struct SourceArcRecord {
    points: [Point; 2],
    metric: Cell<RootMetricState>,
}

impl SourceArcRecord {
    #[expect(
        clippy::large_types_passed_by_value,
        reason = "this constructor transfers the newly staged source endpoint pair into its owning record"
    )]
    fn new(points: [Point; 2]) -> Self {
        Self {
            points,
            metric: Cell::new(RootMetricState::default()),
        }
    }
}

pub(in crate::geometry) struct SubdivisionRootCatalog {
    inventory: ExactSourceInventory,
    frame: GeographicFrame,
    root_xy_positions_needed: bool,
}

impl SubdivisionRootCatalog {
    pub(in crate::geometry) fn needs_root_xy_positions(
        shape: &Shape,
        bounds: Option<Bounds>,
        frame: GeographicFrame,
    ) -> bool {
        // The envelope certifies the common local case without another vertex
        // walk; wider envelopes need an edge-level chart check before rooting.
        if bounds.is_some_and(|bounds| {
            frame
                .longitude()
                .ordinary_chart_edge(bounds.minx(), bounds.maxx())
        }) {
            return false;
        }
        let mut needed = false;
        let Ok(()) = shape.try_for_each_segment_chain(|chain| {
            for segment in line_segments(chain) {
                needed |= !frame
                    .longitude()
                    .ordinary_chart_edge(segment.start.x, segment.end.x);
            }
            Ok::<_, std::convert::Infallible>(())
        });
        needed
    }

    pub(in crate::geometry) fn from_line_shape(
        shape: &Shape,
        frame: GeographicFrame,
        root_xy_positions_needed: bool,
    ) -> Result<(Self, Vec<SourceTrace>)> {
        let source = LineSource::from_shape(shape).ok_or_else(|| {
            GeometryErrorKind::Invalid(
                "ordinary subdivision roots require a line source shape".into(),
            )
        })?;
        let inventory = ExactSourceInventory::from_shapes(&[shape])?;
        let mut line_traces = Vec::with_capacity(match source {
            LineSource::LineString(_) => 1,
            LineSource::MultiLineString(lines) => lines.len(),
        });
        let mut source_edge = 0_usize;
        let mut work = Unmetered;
        let mut push_line = |line: &LineSeq| -> Result<()> {
            let mut edge_rows = Vec::with_capacity(line.len().saturating_sub(1));
            for _ in line.segment_pairs() {
                let root_index =
                    u32::try_from(source_edge).map_err(|_| ResourceError::SizeOverflow)?;
                source_edge = source_edge
                    .checked_add(1)
                    .ok_or(ResourceError::SizeOverflow)?;
                let zero = ExactRational::zero();
                let one = ExactRational::one();
                edge_rows.push(vec![RootRange::from_mapped_with_work(
                    RootId::from_index(root_index),
                    zero.clone(),
                    one.clone(),
                    zero,
                    one,
                    &mut work,
                )?]);
            }
            line_traces.push(SourceTrace::from_edge_rows(edge_rows, &mut work)?);
            Ok(())
        };
        match source {
            LineSource::LineString(line) => push_line(line)?,
            LineSource::MultiLineString(lines) => {
                for line in lines.iter() {
                    push_line(line)?;
                }
            },
        }
        if source_edge != inventory.source_segments.len() {
            return Err(GeometryErrorKind::Invalid(
                "subdivision root traces do not cover the original line inventory".into(),
            )
            .into());
        }
        Ok((
            Self {
                inventory,
                frame,
                root_xy_positions_needed,
            },
            line_traces,
        ))
    }
    pub(in crate::geometry) fn from_polygon_shape(
        shape: &Shape,
        frame: GeographicFrame,
        root_xy_positions_needed: bool,
    ) -> Result<(Self, Vec<SourceTrace>)> {
        let ring_ranges = polygon_source_ring_ranges(shape)?;
        let inventory = ExactSourceInventory::from_shapes(&[shape])?;
        let source_edge_count = ring_ranges.iter().try_fold(0_usize, |count, range| {
            count
                .checked_add(range.len())
                .ok_or(ResourceError::SizeOverflow)
        })?;
        if source_edge_count != inventory.source_segments.len() {
            return Err(GeometryErrorKind::Invalid(
                "polygon root traces do not cover the original ring inventory".into(),
            )
            .into());
        }
        let mut traces = Vec::with_capacity(ring_ranges.len());
        let mut source_edge = 0_usize;
        let mut work = Unmetered;
        for range in ring_ranges {
            let mut edge_rows = Vec::with_capacity(range.len());
            while source_edge < range.end {
                let root = RootId::from_index(
                    u32::try_from(source_edge).map_err(|_| ResourceError::SizeOverflow)?,
                );
                let zero = ExactRational::zero();
                let one = ExactRational::one();
                edge_rows.push(vec![RootRange::from_mapped_with_work(
                    root,
                    zero.clone(),
                    one.clone(),
                    zero,
                    one,
                    &mut work,
                )?]);
                source_edge = source_edge
                    .checked_add(1)
                    .ok_or(ResourceError::SizeOverflow)?;
            }
            traces.push(SourceTrace::from_edge_rows(edge_rows, &mut work)?);
        }
        Ok((
            Self {
                inventory,
                frame,
                root_xy_positions_needed,
            },
            traces,
        ))
    }

    pub(in crate::geometry) const fn root_xy_positions_needed(&self) -> bool {
        self.root_xy_positions_needed
    }

    pub(in crate::geometry) fn resolve_output_requests<M: ChartMetric>(
        &self,
        rows: &[RequestRowInput],
        metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
        work: &mut impl Work,
    ) -> Result<Vec<NodeOrdinates>> {
        let mut output = Vec::with_capacity(rows.len());
        #[cfg(test)]
        let mut request_count = 0_usize;
        #[cfg(test)]
        let mut resolved_count = 0_usize;
        for row in rows {
            work.charge(1)?;
            let mut ordinates = NodeOrdinates::default();
            let mut state = RootMetricState::default();
            let mut previous: Option<&RootRequest> = None;
            for request in &row.requests {
                work.charge(1)?;
                #[cfg(test)]
                {
                    request_count = request_count.saturating_add(1);
                }
                let duplicate = if let Some(previous) = previous {
                    previous.root() == request.root()
                        && previous
                            .parameter()
                            .try_cmp_with_work(request.parameter(), work)?
                            .is_eq()
                } else {
                    false
                };
                previous = Some(request);
                if duplicate {
                    continue;
                }
                let mut fraction = None;
                ordinates.merge(self.resolve(
                    request.root(),
                    request.parameter(),
                    row.axes,
                    metric_access,
                    &mut state,
                    &mut fraction,
                    work,
                )?);
                #[cfg(test)]
                {
                    resolved_count = resolved_count.saturating_add(1);
                }
            }
            output.push(ordinates);
        }
        #[cfg(test)]
        SUBDIVISION_RESOLUTION_BATCHES.with(|batches| {
            batches
                .borrow_mut()
                .push((rows.len(), request_count, resolved_count));
        });
        Ok(output)
    }

    fn source_pair(&self, root: RootId) -> Result<[Point; 2]> {
        if let Some(points) = self.inventory.source_points.as_ref() {
            return points
                .get(root.index())
                .map(|record| record.points)
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "ordinary subdivision trace references an unknown original source edge"
                            .into(),
                    )
                    .into()
                });
        }
        let segment = self
            .inventory
            .source_segments
            .get(root.index())
            .ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "ordinary subdivision trace references an unknown original source edge".into(),
                )
            })?;
        Ok([segment.start.point(), segment.end.point()])
    }

    fn source_displacement(
        &self,
        source: &[Point; 2],
    ) -> Result<(crate::geographic::RetainedChartDisplacement, f64)> {
        let longitude = self.frame.longitude();
        let connection = longitude
            .chart_connection(source[0].x, source[1].x)
            .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?;
        let displacement = connection.retained();
        Ok((displacement, displacement.delta_native(longitude)))
    }

    fn source_position(
        &self,
        root: RootId,
        parameter: &ExactRational,
        work: &mut impl Work,
    ) -> Result<Option<XY>> {
        if parameter.is_zero() || parameter.is_one() {
            return Ok(None);
        }
        let source = self.source_pair(root)?;
        let [start, end] = source;
        let (_, delta) = self.source_displacement(&source)?;
        let parameter = parameter.round_with_work(work)?;
        let lifted_x =
            crate::geometry::interpolate_subdivision_ordinate(start.x, start.x + delta, parameter);
        let y = crate::geometry::interpolate_subdivision_ordinate(start.y, end.y, parameter);
        let x = self
            .frame
            .longitude()
            .canonicalize(lifted_x)
            .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?
            .get();
        Ok(Some(XY::new(x, y)))
    }

    fn source_node_position(
        &self,
        root: RootId,
        parameter: &ExactRational,
        work: &mut impl Work,
    ) -> Result<Option<XY>> {
        #[cfg(test)]
        SUBDIVISION_ROOT_XY_POSITION_CALLS.with(|calls| {
            calls.set(calls.get().saturating_add(1));
        });
        if parameter.is_zero() || parameter.is_one() {
            return Ok(None);
        }
        let source = self.source_pair(root)?;
        if self
            .frame
            .longitude()
            .ordinary_chart_edge(source[0].x, source[1].x)
        {
            return Ok(None);
        }
        let raw_delta = source[1].x - source[0].x;
        let (_, delta) = self.source_displacement(&source)?;
        if delta.to_bits() == raw_delta.to_bits() {
            return Ok(None);
        }
        self.source_position(root, parameter, work)
    }

    fn seam_parameter(&self, root: RootId, work: &mut impl Work) -> Result<Option<ExactRational>> {
        let source = self.source_pair(root)?;
        let start = source[0].x;
        if self
            .frame
            .longitude()
            .ordinary_chart_edge(source[0].x, source[1].x)
        {
            return Ok(None);
        }
        let (_, delta) = self.source_displacement(&source)?;
        if delta == 0.0 {
            return Ok(None);
        }
        let end = start + delta;
        let (low, high) = if start < end {
            (start, end)
        } else {
            (end, start)
        };
        let longitude = self.frame.longitude();
        let seam_index = ((low - longitude.half_period()) / longitude.period()).floor();
        let seam = longitude.half_period() + (seam_index + 1.0) * longitude.period();
        if seam <= low || seam >= high {
            return Ok(None);
        }
        let numerator = ExactRational::from_f64(seam)
            .try_subtract_with_work(&ExactRational::from_f64(start), work)?;
        let parameter = numerator.try_quotient_with_work(&ExactRational::from_f64(delta), work)?;
        Ok(Some(parameter))
    }
}

impl RootCatalog for SubdivisionRootCatalog {
    fn resolve<M: ChartMetric, W: Work>(
        &self,
        root: RootId,
        parameter: &ExactRational,
        axes: CoordinateAxes,
        metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
        state: &mut RootMetricState,
        fraction: &mut Option<f64>,
        work: &mut W,
    ) -> Result<NodeOrdinates> {
        if !axes.has_z() && !axes.has_m() {
            return Ok(NodeOrdinates::default());
        }
        let record = self
            .inventory
            .source_points
            .as_ref()
            .and_then(|points| points.get(root.index()))
            .ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "ordinary subdivision trace references an unknown original source edge".into(),
                )
            })?;
        let source = record.points;
        let (displacement, _) = self.source_displacement(&source)?;
        let mut metric_state = record.metric.get();
        let result = resolve_source_root(
            self.frame,
            &source,
            displacement,
            parameter,
            axes,
            metric_access,
            &mut metric_state,
            fraction,
            work,
        );
        record.metric.set(metric_state);
        *state = metric_state;
        result
    }
}

struct SourceSpanTraceView {
    span: u32,
    reversed: bool,
    source_start: ExactRational,
    source_end: ExactRational,
}

struct SourceLineTraceViews {
    source_ids: Vec<usize>,
    views: Vec<Vec<SourceSpanTraceView>>,
}

struct SourceLineSeamCut {
    parameter: ExactRational,
    coordinate: XY,
    requests: Vec<RootRequest>,
}

struct SourceLineSeamSample {
    coordinate: XY,
    requests: Vec<RootRequest>,
}

#[derive(Clone, Copy)]
enum TracedSourceLineVertex {
    Node(NodeId),
    Seam(usize),
}

struct TracedSourceLineRun {
    member: usize,
    vertices: Vec<TracedSourceLineVertex>,
    trace_slices: Vec<(usize, ExactRational, ExactRational)>,
}

fn mapped_root_ranges_between_requests<W: Work>(
    start_requests: &[RootRequest],
    end_requests: &[RootRequest],
    work: &mut W,
) -> Result<Vec<RootRange>> {
    let zero = ExactRational::zero();
    let one = ExactRational::one();
    let mut ranges = Vec::new();
    for start in start_requests {
        work.charge(1)?;
        let Some(end) = end_requests
            .iter()
            .find(|request| request.root() == start.root())
        else {
            continue;
        };
        ranges.push(RootRange::from_mapped_with_work(
            start.root(),
            start.parameter().clone(),
            end.parameter().clone(),
            zero.clone(),
            one.clone(),
            work,
        )?);
    }
    Ok(ranges)
}

trait SubdivisionTraceWork: Work {
    fn embed<'p>(
        &mut self,
        partition: &'p ExactPartition<'_>,
        emissions: &[&embedding::Emission],
        operation: &'static str,
    ) -> Result<embedding::Embedding<'p>>;
}

impl SubdivisionTraceWork for EvaluationBudget {
    fn embed<'p>(
        &mut self,
        partition: &'p ExactPartition<'_>,
        emissions: &[&embedding::Emission],
        operation: &'static str,
    ) -> Result<embedding::Embedding<'p>> {
        partition.embed_with_budget(emissions, operation, self)
    }
}

impl SubdivisionTraceWork for Unmetered {
    fn embed<'p>(
        &mut self,
        partition: &'p ExactPartition<'_>,
        emissions: &[&embedding::Emission],
        operation: &'static str,
    ) -> Result<embedding::Embedding<'p>> {
        partition.embed(emissions, operation)
    }
}

struct SourceLineRun {
    member: usize,
    nodes: Vec<NodeId>,
    trace_slices: Vec<(usize, ExactRational, ExactRational)>,
}

fn push_source_line_run<W: Work + ?Sized>(
    runs: &mut Vec<SourceLineRun>,
    member: usize,
    nodes: &mut Vec<NodeId>,
    trace_slices: &mut Vec<(usize, ExactRational, ExactRational)>,
    traced: bool,
    work: &mut W,
) -> Result<()> {
    if nodes.len() < 2 {
        nodes.clear();
        trace_slices.clear();
        return Ok(());
    }
    if traced {
        let expected_edges = nodes
            .len()
            .checked_sub(1)
            .ok_or(ResourceError::SizeOverflow)?;
        if trace_slices.len() != expected_edges {
            return Err(GeometryErrorKind::Invalid(
                "source line run vertices do not match their trace slices".into(),
            )
            .into());
        }
        work.charge(1)?;
    } else if !trace_slices.is_empty() {
        return Err(
            GeometryErrorKind::Invalid("untraced source line run has trace slices".into()).into(),
        );
    }
    runs.push(SourceLineRun {
        member,
        nodes: std::mem::take(nodes),
        trace_slices: std::mem::take(trace_slices),
    });
    Ok(())
}

pub(in crate::geometry) struct SubdivisionPart {
    pub shape: Shape,
    pub traces: Option<Vec<SourceTrace>>,
    pub polygon_traces: Option<Vec<SourceTrace>>,
}

fn remove_node_antennas(mut nodes: Vec<NodeId>) -> Option<Vec<NodeId>> {
    if nodes.first() == nodes.last() {
        nodes.pop();
    }
    let mut nodes = remove_antenna_vertices(nodes, |left, right| left == right)?;
    let first = *nodes.first()?;
    nodes.push(first);
    Some(nodes)
}

impl ExactSourceInventory {
    /// Stage each operand as a set, visiting repeated shared collection owners once.
    pub(in crate::geometry) fn from_shapes<S: Borrow<Shape>>(shapes: &[S]) -> Result<Self> {
        Self::from_rows(
            shapes
                .iter()
                .map(|shape| storage::ShapeRow::Shape(shape.borrow())),
        )
    }

    pub(in crate::geometry) fn from_rows<'a>(
        rows: impl ExactSizeIterator<Item = storage::ShapeRow<'a>>,
    ) -> Result<Self> {
        Self::from_staged(StagedExactSources::from_rows(rows)?)
    }

    /// Preserve every source occurrence for consumers whose result observes
    /// multiplicity, such as DE-9IM line-boundary facts and polygonize diagnostics.
    pub(in crate::geometry) fn from_shapes_with_occurrences<S: Borrow<Shape>>(
        shapes: &[S],
    ) -> Result<Self> {
        Self::from_rows_with_occurrences(
            shapes
                .iter()
                .map(|shape| storage::ShapeRow::Shape(shape.borrow())),
        )
    }

    pub(in crate::geometry) fn from_rows_with_occurrences<'a>(
        rows: impl ExactSizeIterator<Item = storage::ShapeRow<'a>>,
    ) -> Result<Self> {
        Self::stage_rows(rows)
    }

    fn stage_rows<'a>(rows: impl ExactSizeIterator<Item = storage::ShapeRow<'a>>) -> Result<Self> {
        Self::from_staged(StagedExactSources::from_rows_with_occurrences(rows, false)?)
    }

    /// Build only the sources that can select occupied two-dimensional faces.
    ///
    /// Point and line sources can subdivide an areal boundary, but cannot alter
    /// its selected faces. Their extra boundary crosses telescope exactly
    /// across the homogeneous subdivisions, so they do not belong in this
    /// area-only partition.
    fn from_occupied_area_shape(shape: &Shape) -> Result<Option<Self>> {
        let mut staged = StagedExactSources::new(1);
        let mut collection_owners = SharedShapeOwners::for_collections();
        staged.append_occupied_area_shape(shape, 0, &mut collection_owners)?;
        if staged.polygon_count == 0 {
            return Ok(None);
        }
        Self::from_staged(staged).map(Some)
    }

    /// Stage polygonal inputs as one operand for their union, or `None` when
    /// an input is not polygonal. Union selects a cell any operand occupies,
    /// and a polygonal operand is the union of its polygons, so the selection
    /// is unchanged while each per-operand cell fact keeps one entry instead
    /// of one per input. Line inputs keep their own operands: line boundary
    /// counts are per operand.
    pub(in crate::geometry) fn from_polygonal_union(shapes: &[&Shape]) -> Result<Option<Self>> {
        if !shapes
            .iter()
            .all(|shape| matches!(shape, Shape::Polygon(_) | Shape::MultiPolygon(_)))
        {
            return Ok(None);
        }
        let mut staged = StagedExactSources::new(1);
        let mut owners = SharedShapeOwners::for_collections();
        for shape in shapes {
            staged.append_shape(shape, 0, &mut owners)?;
        }
        Self::from_staged(staged).map(Some)
    }

    fn from_staged(staged: StagedExactSources) -> Result<Self> {
        let (unique, provenance) =
            dedup_undirected_segments_sourced(staged.segments.iter().copied())?;
        Ok(Self {
            source_segments: staged.segments,
            unique,
            provenance,
            source_operands: staged.source_operands,
            source_rings: staged.source_rings,
            ring_kinds: staged.ring_kinds,
            ring_polygons: staged.ring_polygons,
            ring_holes: staged.ring_holes,
            polygon_count: staged.polygon_count,
            polygon_operands: staged.polygon_operands,
            polygon_ring_ranges: staged.polygon_ring_ranges,
            source_points: staged.source_points,
            polygon_rings: staged.polygon_rings,
            ring_count: staged.ring_count,
            isolated: staged.isolated,
            isolated_points: staged.isolated_points,
            isolated_operands: staged.isolated_operands,
            line_boundaries: staged.line_boundaries,
        })
    }

    fn isolated_point_occurrences<'a>(
        &'a self,
        partition: &'a ExactPartition<'_>,
    ) -> impl Iterator<Item = (NodeId, Point, u32)> + 'a {
        debug_assert_eq!(self.isolated.len(), self.isolated_points.len());
        debug_assert_eq!(self.isolated.len(), self.isolated_operands.len());
        debug_assert_eq!(self.isolated.len(), partition.point_nodes.len());
        partition
            .point_nodes
            .iter()
            .copied()
            .zip(self.isolated_points.iter().copied())
            .zip(self.isolated_operands.iter().copied())
            .map(|((node, point), operand)| (node, point, operand))
    }

    pub(in crate::geometry) fn from_polygon_operands<P: AsRef<Polygon>, Q: AsRef<Polygon>>(
        left: &[P],
        right: &[Q],
    ) -> Result<Self> {
        let mut staged = StagedExactSources::new(2);
        for polygon in left {
            staged.append_polygon(polygon.as_ref(), 0)?;
        }
        for polygon in right {
            staged.append_polygon(polygon.as_ref(), 1)?;
        }
        Self::from_staged(staged)
    }

    fn from_polygon_rect_clip<P: AsRef<Polygon>>(
        polygons: &[P],
        rectangle: &Polygon,
        bounds: Bounds,
        rectangle_first: bool,
    ) -> Result<Option<(Self, Vec<SegmentClipInterval>)>> {
        let mut staged = StagedExactSources::new(2);
        if rectangle_first {
            staged.append_polygon(rectangle, 1)?;
        }
        for polygon in polygons {
            staged.append_polygon(polygon.as_ref(), 0)?;
        }
        if !rectangle_first {
            staged.append_polygon(rectangle, 1)?;
        }

        let mut active_segments = Vec::new();
        let mut active_sources = Vec::new();
        for (source, (&segment, &operand)) in staged
            .segments
            .iter()
            .zip(&staged.source_operands)
            .enumerate()
        {
            if operand == 0 {
                let interior = |point: XY| {
                    point.x > bounds.minx()
                        && point.x < bounds.maxx()
                        && point.y > bounds.miny()
                        && point.y < bounds.maxy()
                };
                let contact = if interior(segment.start) || interior(segment.end) {
                    RectContact::Open
                } else {
                    segment_vs_rect(segment, bounds)
                };
                if contact == RectContact::Closed {
                    // Contact-only cells use the full partition's point/edge owner.
                    return Ok(None);
                }
                if contact == RectContact::None {
                    continue;
                }
            }
            let source = u32::try_from(source).map_err(|_| ResourceError::SizeOverflow)?;
            active_segments.push(segment);
            active_sources.push(source);
        }
        let (unique, local_provenance) =
            dedup_undirected_segments_sourced(active_segments.iter().copied())?;
        let mut offsets = Vec::with_capacity(
            unique
                .len()
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?,
        );
        let mut entries = Vec::with_capacity(active_sources.len());
        offsets.push(0);
        for owners in local_provenance.iter() {
            for &(active, reversed) in owners {
                let source = active_sources
                    .get(active as usize)
                    .copied()
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "rectangle-local provenance lost its full source ordinal".into(),
                        )
                    })?;
                entries.push((source, reversed));
            }
            offsets.push(u32::try_from(entries.len()).map_err(|_| ResourceError::SizeOverflow)?);
        }
        let provenance = SegmentProvenance::from_csr(offsets, entries);

        let mut intervals = Vec::with_capacity(unique.len());
        for &segment in &unique {
            intervals.push(clip_segment_rect_interval(segment, bounds)?.ok_or_else(|| {
                GeometryErrorKind::Invalid(
                    "a rectangle-contact segment has no closed clip interval".into(),
                )
            })?);
        }

        Ok(Some((
            Self {
                source_segments: staged.segments,
                unique,
                provenance,
                source_operands: staged.source_operands,
                source_rings: staged.source_rings,
                ring_kinds: staged.ring_kinds,
                ring_polygons: staged.ring_polygons,
                ring_holes: staged.ring_holes,
                polygon_count: staged.polygon_count,
                polygon_operands: staged.polygon_operands,
                polygon_ring_ranges: staged.polygon_ring_ranges,
                source_points: staged.source_points,
                polygon_rings: staged.polygon_rings,
                ring_count: staged.ring_count,
                isolated: staged.isolated,
                isolated_points: staged.isolated_points,
                isolated_operands: staged.isolated_operands,
                line_boundaries: staged.line_boundaries,
            },
            intervals,
        )))
    }

    pub(in crate::geometry) fn from_segments(segments: &[Segment]) -> Result<Self> {
        let mut staged = StagedExactSources::new(1);
        staged.reserve_segments(segments.len());
        for &segment in segments {
            staged.push_segment(
                segment,
                &[segment.start.point(), segment.end.point()],
                0,
                None,
            );
        }
        Self::from_staged(staged)
    }

    pub(in crate::geometry) fn from_raw_loops(loops: &[(Vec<f64>, Vec<f64>)]) -> Result<Self> {
        let mut staged = StagedExactSources::new(1);
        for (xs, ys) in loops {
            let ring_id =
                u32::try_from(staged.ring_count).map_err(|_| ResourceError::SizeOverflow)?;
            staged.ring_count = staged
                .ring_count
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
            // Raw winding callers pass open coordinate columns for a cyclic
            // loop; the final-to-first edge is part of that source contract.
            // This ingress is not the open-line inventory used by overlay.
            let closed = xs.len() >= 3;
            let coordinate_pairs = || xs.iter().copied().zip(ys.iter().copied());
            let mut endpoints = coordinate_pairs();
            let first = endpoints.next().map(|(x, y)| XY::new(x, y));
            let last = endpoints.next_back().map(|(x, y)| XY::new(x, y));
            let repeated_closure = closed
                && first
                    .zip(last)
                    .is_some_and(|(first, last)| same_point(first, last));
            let count = xs.len().saturating_sub(usize::from(repeated_closure));
            staged.ring_kinds.push(RingKind::Winding);
            staged.reserve_segments(count);
            let points = || {
                coordinate_pairs().take(count).map(|(x, y)| {
                    let point = Point::new_raw_xy(x, y);
                    (point.xy(), point)
                })
            };
            if let Some(first) = points().next() {
                for ((start_xy, start), (end_xy, end)) in
                    points().zip(points().skip(1).chain(std::iter::once(first)))
                {
                    staged.push_segment(
                        Segment {
                            start: start_xy,
                            end: end_xy,
                        },
                        &[start, end],
                        0,
                        closed.then_some(ring_id),
                    );
                }
            }
        }
        Self::from_staged(staged)
    }

    pub(in crate::geometry) fn partition(
        &self,
        budget: &mut ExpansionBudget,
    ) -> Result<ExactPartition<'_>> {
        self.partition_with_budget(Some(budget))
    }

    pub(in crate::geometry) fn partition_without_budget(&self) -> Result<ExactPartition<'_>> {
        self.partition_with_budget(None)
    }

    fn partition_with_budget(
        &self,
        budget: Option<&mut ExpansionBudget>,
    ) -> Result<ExactPartition<'_>> {
        if self.has_source_ordinates() {
            self.partition_with_ordinates::<true>(budget)
        } else {
            self.partition_with_ordinates::<false>(budget)
        }
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "active provenance source ordinals address the staged source-point column"
    )]
    fn has_source_ordinates(&self) -> bool {
        let Some(points) = &self.source_points else {
            return false;
        };
        self.provenance.iter().flatten().any(|&(source, _)| {
            points[source as usize]
                .points
                .iter()
                .any(|point| point.z().is_some() || point.m().is_some())
        })
    }

    fn partition_with_ordinates<const ORDINATES: bool>(
        &self,
        budget: Option<&mut ExpansionBudget>,
    ) -> Result<ExactPartition<'_>> {
        self.record_partition_inventory();
        let partition = match budget {
            Some(budget) => partition_unique_segments::<ORDINATES>(
                &self.unique,
                &self.provenance,
                &self.isolated,
                budget,
            )?,
            None => partition_unique_segments_unbudgeted::<ORDINATES>(
                &self.unique,
                &self.provenance,
                &self.isolated,
            )?,
        };
        Ok(self.finish_partition(partition, embedding::EmbeddingDomain::Plane))
    }

    fn partition_in_window(
        &self,
        intervals: &[SegmentClipInterval],
        bounds: Bounds,
        budget: &mut ExpansionBudget,
    ) -> Result<ExactPartition<'_>> {
        self.record_partition_inventory();
        let partition = if self.has_source_ordinates() {
            partition_unique_segments_in_window::<true>(
                &self.unique,
                &self.provenance,
                intervals,
                bounds,
                budget,
            )?
        } else {
            partition_unique_segments_in_window::<false>(
                &self.unique,
                &self.provenance,
                intervals,
                bounds,
                budget,
            )?
        };
        Ok(self.finish_partition(partition, embedding::EmbeddingDomain::Rectangle(bounds)))
    }

    #[expect(
        clippy::float_cmp,
        reason = "a degenerate rectangle is defined by exact equality of its admitted bounds"
    )]
    #[expect(
        clippy::indexing_slicing,
        reason = "slot, walk, span, source, and ring IDs are admitted by the checked local topology and staged inventory"
    )]
    fn rectangle_winding_seed(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
        edges: &[u32],
        global: &GlobalFaces,
        bounds: Bounds,
    ) -> Result<Option<(u32, Vec<i64>)>> {
        if bounds.minx() == bounds.maxx() || bounds.miny() == bounds.maxy() {
            return Ok(None);
        }

        let clip_polygon = self
            .polygon_operands
            .iter()
            .position(|&operand| operand == 1)
            .ok_or(ResourceError::SizeOverflow)?;
        let clip_ring = self
            .polygon_ring_ranges
            .get(clip_polygon)
            .map(|range| range.start)
            .ok_or(ResourceError::SizeOverflow)?;
        for (slot, &span_id) in edges.iter().enumerate() {
            if global.walk_faces[topology.face_of[slot] as usize] != 0 {
                continue;
            }
            let span_index = span_id as usize;
            if !partition
                .owners(span_index)
                .iter()
                .any(|&(source, _)| self.source_operands[source as usize] == 1)
            {
                continue;
            }
            let span = partition.spans[span_index];
            let Some(outward) = rectangle_side_outward(partition, span.start, span.end, bounds)?
            else {
                continue;
            };
            let twin = topology.twins[slot] as usize;
            let face = global.walk_faces[topology.face_of[twin] as usize];
            if face == 0 {
                return Err(GeometryErrorKind::Invalid(
                    "rectangle boundary does not separate an interior face".into(),
                )
                .into());
            }

            for node in [span.start, span.end] {
                if !partition.is_stored(node) {
                    continue;
                }
                let point = partition.nearest_xy(node);
                let is_corner = [
                    XY::new(bounds.minx(), bounds.miny()),
                    XY::new(bounds.maxx(), bounds.miny()),
                    XY::new(bounds.maxx(), bounds.maxy()),
                    XY::new(bounds.minx(), bounds.maxy()),
                ]
                .into_iter()
                .any(|corner| same_point(point, corner));
                if !is_corner {
                    continue;
                }
                let (mut winding, boundary) = self.source_winding_at_stored(point);
                if !boundary {
                    let clip = winding
                        .get_mut(clip_ring)
                        .ok_or(ResourceError::SizeOverflow)?;
                    *clip = 1;
                    return Ok(Some((face, winding)));
                }
            }

            let start = partition.exact(span.start)?;
            let end = partition.exact(span.end)?;
            let probe = homogeneous_midpoint(&start, &end)?;
            let direction = outward.map(i8::wrapping_neg);
            let mut winding = self.source_winding_at_symbolic(&probe, direction)?;
            let clip = winding
                .get_mut(clip_ring)
                .ok_or(ResourceError::SizeOverflow)?;
            *clip = 1;
            return Ok(Some((face, winding)));
        }
        Err(GeometryErrorKind::Invalid(
            "rectangle-local partition has no exterior side interval for a winding seed".into(),
        )
        .into())
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "source and ring IDs are admitted by source staging, and winding is sized to ring_count"
    )]
    fn source_winding_at_stored(&self, point: XY) -> (Vec<i64>, bool) {
        let mut winding = vec![0_i64; self.ring_count];
        let mut boundary = false;
        for (source, &segment) in self.source_segments.iter().enumerate() {
            if self.source_operands[source] != 0 {
                continue;
            }
            let Some(ring) = self.source_rings[source] else {
                continue;
            };
            if self.ring_kinds[ring as usize] != RingKind::Winding {
                continue;
            }
            boundary |= point.x >= segment.start.x.min(segment.end.x)
                && point.x <= segment.start.x.max(segment.end.x)
                && point.y >= segment.start.y.min(segment.end.y)
                && point.y <= segment.start.y.max(segment.end.y)
                && stored_orientation_sign(
                    segment.start.x,
                    segment.start.y,
                    segment.end.x,
                    segment.end.y,
                    point.x,
                    point.y,
                ) == ExactSign::Zero;
            winding[ring as usize] =
                winding[ring as usize].wrapping_add(i64::from(ray_crossing(segment, point)));
        }
        (winding, boundary)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "source and ring IDs are admitted by source staging, and winding is sized to ring_count"
    )]
    fn source_winding_at_symbolic(
        &self,
        point: &[ExactDyadic; 3],
        direction: [i8; 2],
    ) -> Result<Vec<i64>> {
        let mut winding = vec![0_i64; self.ring_count];
        for (source, &segment) in self.source_segments.iter().enumerate() {
            if self.source_operands[source] != 0 {
                continue;
            }
            let Some(ring) = self.source_rings[source] else {
                continue;
            };
            if self.ring_kinds[ring as usize] != RingKind::Winding {
                continue;
            }
            winding[ring as usize] = winding[ring as usize]
                .wrapping_add(i64::from(symbolic_ray_crossing(segment, point, direction)?));
        }
        Ok(winding)
    }

    #[cfg_attr(
        not(test),
        expect(
            clippy::missing_const_for_fn,
            reason = "test-only partition metrics update thread-local counters"
        )
    )]
    fn record_partition_inventory(&self) {
        #[cfg(test)]
        record_partition_call();
        #[cfg(test)]
        update_partition_metrics(|metrics| {
            metrics.partition_builds = metrics.partition_builds.wrapping_add(1);
            metrics.source_segments = self.source_segments.len();
            metrics.unique_source_segments = self.unique.len();
        });
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "span owners and node IDs are admitted by the same exact partition"
    )]
    fn finish_partition<'a>(
        &self,
        mut partition: ExactPartition<'a>,
        embedding_domain: embedding::EmbeddingDomain,
    ) -> ExactPartition<'a> {
        #[cfg(test)]
        update_partition_metrics(|metrics| {
            metrics.canonical_spans = partition.spans.len();
            metrics.canonical_nodes = partition.node_count();
        });
        let mut on_ring = Vec::with_capacity(partition.node_count());
        on_ring.resize(partition.node_count(), false);
        for (index, span) in partition.spans.iter().enumerate() {
            if partition
                .owners(index)
                .iter()
                .any(|&(source, _)| self.source_rings[source as usize].is_some())
            {
                on_ring[span.start.0 as usize] = true;
                on_ring[span.end.0 as usize] = true;
            }
        }
        partition.kept_nodes = KeptNodes::OffRings(on_ring);
        partition.embedding_domain = embedding_domain;
        partition
    }

    pub(in crate::geometry) fn source_segments(&self) -> &[Segment] {
        &self.source_segments
    }

    pub(in crate::geometry) fn source_operands(&self) -> &[u32] {
        &self.source_operands
    }

    /// Exact closed support for scalar metrics, before coordinate publication.
    ///
    /// Face walks carry filled membership; selected spans retain their original
    /// carriers and exact parameter windows.
    pub(in crate::geometry) fn metric_support(
        &self,
        partition: &ExactPartition<'_>,
        area_location: AreaLocationStrategy,
    ) -> Result<ExactMetricSupport> {
        self.metric_support_impl(partition, false, area_location)
    }

    /// Exact closed support plus the areal boundary incidence needed by
    /// directed filled-set metrics. Boundary flags follow global-face
    /// adjacency without constructing an emission plan.
    pub(in crate::geometry) fn metric_support_with_boundary(
        &self,
        partition: &ExactPartition<'_>,
        area_location: AreaLocationStrategy,
    ) -> Result<ExactMetricSupport> {
        self.metric_support_impl(partition, true, area_location)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "node, span, walk, and dart IDs address the checked partition topology; boundary flags remain parallel to partition spans"
    )]
    fn metric_support_impl(
        &self,
        partition: &ExactPartition<'_>,
        with_boundary: bool,
        area_location: AreaLocationStrategy,
    ) -> Result<ExactMetricSupport> {
        let (topology, edges) = partition.local_topology()?;
        let global = partition.global_faces(&topology, &edges)?;
        let cells = self.select_cells(
            partition,
            &topology,
            &edges,
            &global,
            crate::geometry::OverlayOp::Union,
            false,
        )?;
        let exact_points = partition.freeze_location_points()?;
        let has_selected_area = cells.faces.iter().any(|&selected| selected);
        let area_location_cells = match (has_selected_area, area_location) {
            (false, _) | (true, AreaLocationStrategy::BoundedScan) => None,
            (true, AreaLocationStrategy::PersistentSlabs) => Some(self.area_location_cells(
                partition,
                &topology,
                &edges,
                &global,
                &cells.faces,
                Arc::clone(&exact_points),
            )?),
        };
        let has_selected_area = with_boundary && has_selected_area;
        let selected_boundary_spans = with_boundary.then(|| {
            if has_selected_area {
                cells.selected_boundary_spans(&topology, &edges, &global)
            } else {
                cells.spans.clone()
            }
        });
        let mut spans = Vec::new();
        let mut boundary_spans = with_boundary.then(Vec::new);
        spans.reserve(cells.spans.iter().filter(|&&value| value).count());
        if let Some(boundary_spans) = boundary_spans.as_mut() {
            boundary_spans.reserve(cells.spans.iter().filter(|&&value| value).count());
        }
        let mut covered = Vec::with_capacity(partition.node_count());
        covered.resize(partition.node_count(), false);
        for (index, (span, &selected)) in partition.spans.iter().zip(&cells.spans).enumerate() {
            if !selected {
                continue;
            }
            let source = partition.unique[span.unique as usize];
            let start_parameter = parameter_along(source, &exact_points[span.start.0 as usize])?;
            let end_parameter = parameter_along(source, &exact_points[span.end.0 as usize])?;
            covered[span.start.0 as usize] = true;
            covered[span.end.0 as usize] = true;
            spans.push(ExactMetricSpan {
                start: ExactMetricNode {
                    points: Arc::clone(&exact_points),
                    node: span.start.0,
                },
                end: ExactMetricNode {
                    points: Arc::clone(&exact_points),
                    node: span.end.0,
                },
                start_parameter,
                end_parameter,
                source: partition.unique[span.unique as usize],
            });
            if let Some(boundary_spans) = boundary_spans.as_mut() {
                boundary_spans.push(
                    selected_boundary_spans
                        .as_ref()
                        .expect("boundary incidence was requested")[index],
                );
            }
        }
        let mut points = Vec::new();
        let mut boundary_points = with_boundary.then(Vec::new);
        points.reserve(cells.nodes.iter().filter(|&&value| value).count());
        for (index, (&selected, &is_covered)) in cells.nodes.iter().zip(&covered).enumerate() {
            if selected && !is_covered {
                points.push(ExactMetricNode {
                    points: Arc::clone(&exact_points),
                    node: index as u32,
                });
                if let Some(boundary_points) = boundary_points.as_mut() {
                    boundary_points.push(!has_selected_area);
                }
            }
        }
        let mut faces = Vec::with_capacity(global.count as usize);
        faces.resize_with(global.count as usize, Vec::new);
        for (walk, &[start, end]) in topology.face_starts.array_windows::<2>().enumerate() {
            let face = global.walk_faces[walk] as usize;
            if face == 0 || !cells.faces[face] {
                continue;
            }
            let (start, end) = (start as usize, end as usize);
            let mut cycle = Vec::with_capacity(end.saturating_sub(start));
            for &slot in &topology.face_slots[start..end] {
                let node = NodeId(topology.owners[slot as usize]);
                cycle.push(ExactMetricNode {
                    points: Arc::clone(&exact_points),
                    node: node.0,
                });
            }
            faces[face].push(cycle);
        }
        faces.retain(|cycles| !cycles.is_empty());
        Ok(ExactMetricSupport {
            exact_points,
            spans,
            points,
            faces,
            area_location_cells,
            boundary_spans,
            boundary_points,
        })
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "node_bindings is a CSR row lookup for a NodeId admitted by the same ExactPartition"
    )]
    fn node_bindings<'a>(&self, node: NodeId, partition: &'a ExactPartition<'_>) -> &'a [u32] {
        let Some(bindings) = &partition.ordinate_bindings else {
            return &[];
        };
        let index = node.0 as usize;
        let start = bindings.starts[index] as usize;
        let end = bindings.starts[index + 1] as usize;
        &bindings.unique[start..end]
    }

    fn source_ordinate_points(&self, source: u32) -> Result<Option<&[Point; 2]>> {
        self.source_points
            .as_ref()
            .map(|points| {
                points
                    .get(source as usize)
                    .map(|record| &record.points)
                    .ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "exact partition ordinate binding references an unknown source".into(),
                        )
                        .into()
                    })
            })
            .transpose()
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "unique, source, and point IDs come from this inventory's checked provenance and partition"
    )]
    fn resolve_node_ordinates<M: ChartMetric>(
        &self,
        partition: &ExactPartition<'_>,
        node: NodeId,
        axes: crate::geometry::CoordinateAxes,
        context: &SourceArcContext<'_, '_, M>,
        source_scope: Option<u32>,
    ) -> Result<(ZOrdinate, MOrdinate)> {
        if !axes.has_z() && !axes.has_m() {
            return Ok((ZOrdinate(None), MOrdinate(None)));
        }
        let mut z = AxisValue::default();
        let mut m = AxisValue::default();
        let source_frame = match context.domain() {
            crate::geometry::TopologyDomain::Geographic(frame) => Some(frame),
            crate::geometry::TopologyDomain::Cartesian => None,
        };
        let bindings = self.node_bindings(node, partition);
        if !bindings.is_empty() {
            let point = partition.exact(node)?;
            for &unique in bindings {
                // The parameter from the unique segment's start, and from its
                // end for a source stored in reverse.
                let [num, den] = parameter_along(partition.unique[unique as usize], &point)?;
                let from_end = den.clone().try_subtract(num.clone())?;
                for &(source, reversed) in self.provenance.owners(unique as usize) {
                    if source_scope.is_some_and(|operand| {
                        self.source_operands.get(source as usize).copied() != Some(operand)
                    }) {
                        continue;
                    }
                    let Some(records) = self.source_points.as_ref() else {
                        continue;
                    };
                    let along = if reversed { &from_end } else { &num };
                    let record = records.get(source as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "exact partition ordinate binding references an unknown source".into(),
                        )
                    })?;
                    let points = &record.points;
                    let [start, end] = *points;
                    if let Some(source_frame) = source_frame {
                        let parameter = ExactRational::new(along.clone(), den.clone())?;
                        let request =
                            SourceArcRequest::new(source_frame, points, axes, &parameter, || {
                                source_frame
                                    .longitude()
                                    .chart_connection(start.x, end.x)
                                    .map(|connection| connection.retained())
                                    .map_err(|error| {
                                        GeometryErrorKind::Invalid(error.to_string().into()).into()
                                    })
                            })?;
                        let mut fraction = None;
                        let mut work = Unmetered;
                        let mut metric_state = record.metric.get();
                        let ordinates = source_arc_ordinates(
                            &request,
                            || {
                                let access = context.metric_access().ok_or_else(|| {
                                crate::crs::CrsError::invalid(
                                    "geographic source-ordinate carry requires the operation's CRS metric",
                                )
                            })?;
                                access()
                            },
                            &mut metric_state,
                            &mut fraction,
                            &mut work,
                        );
                        record.metric.set(metric_state);
                        let ordinates = ordinates?;
                        if axes.has_z() {
                            z.observe(ordinates.z.value());
                        }
                        if axes.has_m() {
                            m.observe(ordinates.m.value());
                        }
                    } else {
                        if axes.has_z() {
                            z.observe(interpolate_ordinate(start.z(), end.z(), along, &den)?);
                        }
                        if axes.has_m() {
                            m.observe(interpolate_ordinate(start.m(), end.m(), along, &den)?);
                        }
                    }
                }
            }
        }
        for &(bound, source) in &partition.point_sources {
            if bound != node
                || source_scope.is_some_and(|operand| {
                    self.source_operands.get(source as usize).copied() != Some(operand)
                })
            {
                continue;
            }
            let Some(points) = self.source_ordinate_points(source)? else {
                continue;
            };
            for point in points {
                if axes.has_z() {
                    z.observe(point.z());
                }
                if axes.has_m() {
                    m.observe(point.m());
                }
            }
        }
        for (bound, point, operand) in self.isolated_point_occurrences(partition) {
            if bound != node || source_scope.is_some_and(|source| operand != source) {
                continue;
            }
            if axes.has_z() {
                z.observe(point.z());
            }
            if axes.has_m() {
                m.observe(point.m());
            }
        }
        Ok((ZOrdinate(z.value()), MOrdinate(m.value())))
    }

    fn existing_source_vertex_ordinates(
        &self,
        partition: &ExactPartition<'_>,
        node: NodeId,
        axes: CoordinateAxes,
        source_scope: u32,
    ) -> Result<Option<NodeOrdinates>> {
        if !axes.has_z() && !axes.has_m() {
            return Ok(None);
        }
        let Some(node_key) = partition.points.stored_key(node.0) else {
            return Ok(None);
        };
        let mut ordinates = NodeOrdinates::default();
        let mut found = false;
        for &unique in self.node_bindings(node, partition) {
            for &(source, _) in self.provenance.owners(unique as usize) {
                if self.source_operands.get(source as usize).copied() != Some(source_scope) {
                    continue;
                }
                let Some(points) = self.source_ordinate_points(source)? else {
                    continue;
                };
                let [first, _] = points;
                if axes.has_z() && !first.axes().has_z() || axes.has_m() && !first.axes().has_m() {
                    continue;
                }
                for point in points {
                    if PointKey::new(*point) != node_key {
                        continue;
                    }
                    found = true;
                    if axes.has_z() {
                        ordinates.z.observe(point.z());
                    }
                    if axes.has_m() {
                        ordinates.m.observe(point.m());
                    }
                }
            }
        }
        for &(bound, source) in &partition.point_sources {
            if bound != node
                || self.source_operands.get(source as usize).copied() != Some(source_scope)
            {
                continue;
            }
            let Some(points) = self.source_ordinate_points(source)? else {
                continue;
            };
            let [first, _] = points;
            if axes.has_z() && !first.axes().has_z() || axes.has_m() && !first.axes().has_m() {
                continue;
            }
            for point in points {
                if PointKey::new(*point) != node_key {
                    continue;
                }
                found = true;
                if axes.has_z() {
                    ordinates.z.observe(point.z());
                }
                if axes.has_m() {
                    ordinates.m.observe(point.m());
                }
            }
        }
        for (bound, point, operand) in self.isolated_point_occurrences(partition) {
            if bound != node
                || operand != source_scope
                || PointKey::new(point) != node_key
                || axes.has_z() && !point.axes().has_z()
                || axes.has_m() && !point.axes().has_m()
            {
                continue;
            }
            found = true;
            if axes.has_z() {
                ordinates.z.observe(point.z());
            }
            if axes.has_m() {
                ordinates.m.observe(point.m());
            }
        }
        Ok(found.then_some(ordinates))
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "the binding-start windows and node-aligned output are built from the same ExactPartition"
    )]
    fn ordinate_protected_nodes(
        &self,
        partition: &ExactPartition<'_>,
        axes: crate::geometry::CoordinateAxes,
    ) -> Result<Option<Vec<bool>>> {
        if !axes.has_z() && !axes.has_m() {
            return Ok(None);
        }
        let Some(bindings) = &partition.ordinate_bindings else {
            return Ok(None);
        };
        let mut protected = Vec::with_capacity(partition.node_count());
        protected.resize(partition.node_count(), false);
        for (node, &[start, end]) in bindings.starts.array_windows::<2>().enumerate() {
            for &unique in &bindings.unique[start as usize..end as usize] {
                let mut source_has_ordinate = false;
                for &(source, _) in self.provenance.owners(unique as usize) {
                    let Some(points) = self.source_ordinate_points(source)? else {
                        continue;
                    };
                    source_has_ordinate |= (axes.has_z()
                        && (points[0].z().is_some() || points[1].z().is_some()))
                        || (axes.has_m() && (points[0].m().is_some() || points[1].m().is_some()));
                }
                if source_has_ordinate {
                    protected[node] = true;
                    break;
                }
            }
        }
        Ok(Some(protected))
    }

    pub(super) fn resolve_line_emission_points<
        'e,
        'p,
        M: ChartMetric,
        I: IntoIterator<Item = NodeId>,
    >(
        &self,
        partition: &ExactPartition<'_>,
        emission: &'e ExactLineEmission<'p>,
        nodes: I,
        declared_axes: crate::geometry::CoordinateAxes,
        context: &SourceArcContext<'_, '_, M>,
        source_scope: Option<u32>,
    ) -> Result<ResolvedNodePoints<'e, 'p>> {
        self.resolve_node_points(
            partition,
            &emission.embedding,
            nodes,
            declared_axes,
            context,
            source_scope,
        )
    }

    fn resolve_node_points<'e, 'p, M: ChartMetric, I: IntoIterator<Item = NodeId>>(
        &self,
        partition: &ExactPartition<'_>,
        embedding: &'e embedding::Embedding<'p>,
        nodes: I,
        declared_axes: crate::geometry::CoordinateAxes,
        context: &SourceArcContext<'_, '_, M>,
        source_scope: Option<u32>,
    ) -> Result<ResolvedNodePoints<'e, 'p>> {
        if !declared_axes.has_z() && !declared_axes.has_m() {
            return Ok(ResolvedNodePoints::from_embedding(embedding, None));
        }
        let mut nodes = nodes.into_iter().peekable();
        if nodes.peek().is_none() {
            return Ok(ResolvedNodePoints::from_embedding(embedding, None));
        }
        let mut by_node = vec![None; partition.node_count()];
        self.resolve_node_points_into(
            partition,
            nodes,
            declared_axes,
            context,
            source_scope,
            &mut by_node,
        )?;
        Ok(ResolvedNodePoints::from_embedding(embedding, Some(by_node)))
    }

    fn resolve_node_points_into<M: ChartMetric, I: IntoIterator<Item = NodeId>>(
        &self,
        partition: &ExactPartition<'_>,
        nodes: I,
        declared_axes: CoordinateAxes,
        context: &SourceArcContext<'_, '_, M>,
        source_scope: Option<u32>,
        by_node: &mut [Option<(ZOrdinate, MOrdinate)>],
    ) -> Result<()> {
        if !declared_axes.has_z() && !declared_axes.has_m() {
            return Ok(());
        }
        debug_assert_eq!(by_node.len(), partition.node_count());
        for node in nodes {
            let ordinates = by_node.get_mut(node.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("exact overlay output references an unknown node".into())
            })?;
            if ordinates.is_none() {
                *ordinates = Some(self.resolve_node_ordinates(
                    partition,
                    node,
                    declared_axes,
                    context,
                    source_scope,
                )?);
            }
        }
        Ok(())
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "each staged source ring ID indexes the admitted ring-kind table"
    )]
    fn winding_source_rings(&self) -> Vec<Option<u32>> {
        self.source_rings
            .iter()
            .map(|&ring| ring.filter(|&ring| self.ring_kinds[ring as usize] == RingKind::Winding))
            .collect()
    }

    pub(in crate::geometry) fn line_emission<'p>(
        &self,
        partition: &'p ExactPartition<'_>,
        operation: &'static str,
    ) -> Result<ExactLineEmission<'p>> {
        partition.emit_lines(&self.source_operands, operation)
    }

    pub(in crate::geometry) fn node_shape<M: ChartMetric>(
        &self,
        partition: &ExactPartition<'_>,
        declared_axes: crate::geometry::CoordinateAxes,
        strict: bool,
        context: &SourceArcContext<'_, '_, M>,
    ) -> Result<Shape> {
        if partition.spans.is_empty() {
            return Ok(Shape::multi_line_string(declared_axes, Vec::new()));
        }
        let (embedding, items) = {
            let spans = (0..partition.spans.len()).map(|span| span as u32);
            let emission = partition.span_emission(spans);
            let emissions = [&emission];
            let embedding = partition.embed(&emissions, "node")?;
            (embedding, emission.items)
        };
        if !declared_axes.has_z() && !declared_axes.has_m() {
            let coordinates = embedding.coordinates();
            let mut lines = Vec::with_capacity(items.len());
            for item in items {
                lines.push(LineSeq::from_proven(xy_sequence_for_nodes(
                    &item,
                    coordinates,
                )?));
            }
            return Ok(Shape::multi_line_string(
                crate::geometry::CoordinateAxes::XY,
                lines,
            ));
        }
        let by_node = self.resolve_node_points(
            partition,
            &embedding,
            items.iter().flatten().copied(),
            declared_axes,
            context,
            None,
        )?;
        let axes = bucket_axes(
            items.iter().flatten().copied(),
            &by_node,
            declared_axes,
            strict,
            "node",
        )?;
        let coordinates = by_node.coordinates();
        let mut lines = Vec::with_capacity(items.len());
        for item in items {
            let points = vec![
                with_axes(
                    by_node.point(item[0], embedding::Embedding::xy_at(coordinates, item[0])),
                    axes,
                ),
                with_axes(
                    by_node.point(item[1], embedding::Embedding::xy_at(coordinates, item[1])),
                    axes,
                ),
            ];
            lines.push(LineSeq::from_proven(CoordSeq::try_from_points(&points)?));
        }
        Ok(Shape::multi_line_string(axes, lines))
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "span, node, dart, face, and operand IDs come from the checked partition and its aligned diagnostic rows"
    )]
    pub(in crate::geometry) fn polygonize_full<M: ChartMetric>(
        &self,
        partition: &mut ExactPartition<'_>,
        declared_axes: CoordinateAxes,
        row_axes: &[CoordinateAxes],
        strict: bool,
        context: &SourceArcContext<'_, '_, M>,
    ) -> Result<PolygonizeFull> {
        if row_axes.len() != self.operand_count() {
            return Err(GeometryErrorKind::Invalid(
                "polygonize diagnostic axes do not match source rows".into(),
            )
            .into());
        }
        partition.kept_nodes = KeptNodes::All;
        let (topology, edges) = partition.local_topology()?;
        let global = partition.global_faces(&topology, &edges)?;
        let mut same_face = vec![false; partition.spans.len()];
        for (slot, &span) in edges.iter().enumerate() {
            let twin = topology.twins[slot] as usize;
            if slot > twin {
                continue;
            }
            let span = span as usize;
            let face = global.walk_faces[topology.face_of[slot] as usize];
            let other_face = global.walk_faces[topology.face_of[twin] as usize];
            same_face[span] = face == other_face;
        }

        let mut degree: Vec<usize> = topology
            .starts
            .array_windows::<2>()
            .map(|&[start, end]| end.wrapping_sub(start) as usize)
            .collect();
        let mut pending: Vec<usize> = degree
            .iter()
            .enumerate()
            .filter_map(|(node, &degree)| (degree == 1).then_some(node))
            .collect();
        let mut dangle_spans = vec![false; partition.spans.len()];
        while let Some(node) = pending.pop() {
            if degree[node] != 1 {
                continue;
            }
            let start = topology.starts[node] as usize;
            let end = topology.starts[node.wrapping_add(1)] as usize;
            let span = edges[start..end]
                .iter()
                .map(|&span| span as usize)
                .find(|&span| !dangle_spans[span])
                .expect("a degree-one node has one remaining incident span");
            dangle_spans[span] = true;

            let span = partition.spans[span];
            let other = if span.start.0 as usize == node {
                span.end.0 as usize
            } else {
                span.start.0 as usize
            };
            degree[node] = 0;
            degree[other] = degree[other].wrapping_sub(1);
            if degree[other] == 1 {
                pending.push(other);
            }
        }
        drop(pending);
        drop(degree);

        let mut cuts = Vec::new();
        let mut dangles = Vec::new();
        let mut invalid_rings = Vec::new();
        for rows in [&mut cuts, &mut dangles, &mut invalid_rings] {
            rows.reserve(self.polygon_rings.len());
            rows.resize_with(self.polygon_rings.len(), Vec::new);
        }
        let mut diagnostic_spans = vec![false; partition.spans.len()];
        for index in 0..partition.spans.len() {
            let owners = partition.owners(index);
            let mut rows = Vec::with_capacity(owners.len());
            for &(source, _) in owners {
                let row = self.source_operands[source as usize];
                if !rows.contains(&row) {
                    rows.push(row);
                }
            }
            for row in rows {
                let row = row as usize;
                if owners.len() > 1 {
                    invalid_rings[row].push((index, true));
                } else if dangle_spans[index] {
                    dangles[row].push((index, false));
                } else if same_face[index] {
                    cuts[row].push((index, false));
                } else {
                    continue;
                }
                diagnostic_spans[index] = true;
            }
        }

        let emission =
            polygonize_full_emission(partition, &topology, &edges, &global, &diagnostic_spans)?;
        let embedding = partition.embed(&[&emission], "polygonize_full")?;
        let groups = emission::polygonize_node_groups(partition, &topology, &global, &embedding)?;
        let polygons = self.polygon_shapes_from_node_groups(
            partition,
            &embedding,
            groups,
            declared_axes,
            strict,
            "polygonize_full",
            context,
            None,
        )?;
        let cuts = self
            .diagnostic_shapes_for_rows(partition, &embedding, cuts, row_axes, strict, context)?;
        let dangles = self.diagnostic_shapes_for_rows(
            partition, &embedding, dangles, row_axes, strict, context,
        )?;
        let invalid_rings = self.diagnostic_shapes_for_rows(
            partition,
            &embedding,
            invalid_rings,
            row_axes,
            strict,
            context,
        )?;
        Ok(PolygonizeFull {
            polygons,
            cuts,
            dangles,
            invalid_rings,
        })
    }

    /// Measure the occupied Cartesian support without serializing rounded
    /// boundary coordinates. Face selection follows the same stored-ring
    /// winding fold as ordinary overlay; the selected homogeneous face
    /// integrals are accumulated as one exact rational before final rounding.
    #[expect(
        clippy::indexing_slicing,
        reason = "face, dart, twin, and node IDs address vectors built by this partition's checked topology"
    )]
    pub(in crate::geometry) fn occupied_cartesian_area(
        &self,
        partition: &ExactPartition<'_>,
    ) -> Result<f64> {
        debug_assert_eq!(
            self.operand_count(),
            1,
            "occupied area is an operation over one composed operand"
        );
        let (topology, edges) = partition.local_topology()?;
        let global = partition.global_faces(&topology, &edges)?;
        let winding_rings = self.winding_source_rings();
        let mut selected = Vec::with_capacity(global.count as usize);
        selected.resize(global.count as usize, false);
        let mut occupancy = PolygonOccupancy::new(self, false);
        if let Some((_, seed)) = &partition.winding_seed {
            occupancy.seed(seed)?;
        }
        partition.visit_face_windings_with_hook(
            &topology,
            &edges,
            &global,
            &winding_rings,
            self.ring_count,
            &mut occupancy,
            |winding, ring, direction, occupancy| {
                occupancy.cross(winding, ring, direction);
                Ok(())
            },
            |face, _, occupancy| {
                selected[face as usize] = occupancy.operand_occupied(0);
                Ok(())
            },
        )?;
        let mut area = crate::geometry::ExactRationalArea::new();
        for (walk, &[start, end]) in topology.face_starts.array_windows::<2>().enumerate() {
            if !selected[global.walk_faces[walk] as usize] {
                continue;
            }
            let range = start as usize..end as usize;
            for &slot in &topology.face_slots[range] {
                let slot = slot as usize;
                let twin_face =
                    global.walk_faces[topology.face_of[topology.twins[slot] as usize] as usize];
                if selected[twin_face as usize] {
                    continue;
                }
                area.add_homogeneous_cross(
                    &*partition.exact(NodeId(topology.owners[slot]))?,
                    &*partition.exact(NodeId(topology.targets[slot]))?,
                )?;
            }
        }
        area.round_abs_half()
    }

    /// Serialize the occupied Cartesian support from the same selected faces
    /// used by the exact fill selection. The caller may reuse this
    /// named owner for metrics whose conversion is not planar, such as the
    /// geographic area metric.
    pub(in crate::geometry) fn occupied_cartesian_support(
        &self,
        partition: &ExactPartition<'_>,
    ) -> Result<Shape> {
        let (points, lines, polygons) =
            self.overlay_parts_with_fill(partition, crate::geometry::OverlayOp::Union, "area")?;
        build_certified_overlay_shape(points, lines, polygons)
    }

    /// Publish the Boundary-location cells of one polygonal operand. Occupied
    /// area cycles remain closed linework; only uncovered residual boundary
    /// nodes become points. Lower-dimensional spans are never emitted.
    #[expect(
        clippy::indexing_slicing,
        reason = "source darts and node IDs are admitted by this partition's plan and topology"
    )]
    pub(in crate::geometry) fn occupied_boundary_shape<M: ChartMetric>(
        &self,
        partition: &ExactPartition<'_>,
        declared_axes: CoordinateAxes,
        context: &SourceArcContext<'_, '_, M>,
    ) -> Result<Shape> {
        debug_assert_eq!(self.operand_count(), 1);
        let (topology, edges) = partition.local_topology()?;
        let global = partition.global_faces(&topology, &edges)?;
        let selection = self.boundary_cells(partition, &topology, &edges, &global)?;
        let has_area = selection.cells.faces.iter().any(|&selected| selected);
        let ordinate_protected = self.ordinate_protected_nodes(partition, declared_axes)?;
        let mut plan = global.boundary_plan_with_protected(
            &[],
            &topology,
            &edges,
            partition,
            &selection.cells.faces,
            ordinate_protected.as_deref(),
            None,
        )?;

        let mut covered_by_area_walk = vec![false; partition.node_count()];
        for chord in plan.walks.iter().flatten() {
            for &dart in &plan.source_darts[chord.sources.clone()] {
                covered_by_area_walk[topology.owners[dart as usize] as usize] = true;
                covered_by_area_walk[topology.targets[dart as usize] as usize] = true;
            }
        }
        plan.points.reserve(selection.boundary_nodes.len());
        for node in selection.boundary_nodes {
            if covered_by_area_walk[node.0 as usize] {
                continue;
            }
            let host = global.node_host_face(&topology, node);
            plan.points.push((node, plan.face_quotient[host as usize]));
        }

        let emission = plan.emission(&topology, &edges);
        let embedding = partition.embed(&[&emission], "boundary")?;
        let parts = plan.emit_boundary_node_parts(partition, &emission)?;
        let by_node = self.resolve_node_points(
            partition,
            &embedding,
            parts
                .points
                .iter()
                .copied()
                .chain(parts.lines.iter().flatten().copied()),
            declared_axes,
            context,
            None,
        )?;
        self.boundary_node_parts_to_shape_resolved(
            parts,
            &by_node,
            declared_axes,
            has_area,
            selection.has_curve_support,
            selection.has_point_support,
        )
    }

    const fn operand_count(&self) -> usize {
        self.polygon_rings.len()
    }

    /// Select and serialize all relatively open cells for one Cartesian
    /// operation. The exact partition is the only incidence owner; this
    /// method never reconstructs a graph from rounded coordinates.
    #[cfg(test)]
    pub(in crate::geometry) fn overlay_parts(
        &self,
        partition: &ExactPartition<'_>,
        operation: crate::geometry::OverlayOp,
    ) -> Result<(Vec<Point>, Vec<CoordSeq>, Vec<Polygon>)> {
        self.overlay_parts_with_fill(partition, operation, operation.name())
    }

    pub(in crate::geometry) fn overlay_shape<M: ChartMetric>(
        &self,
        partition: &ExactPartition<'_>,
        operation: crate::geometry::OverlayOp,
        declared_axes: crate::geometry::CoordinateAxes,
        strict: bool,
        context: &SourceArcContext<'_, '_, M>,
    ) -> Result<Shape> {
        let (topology, edges) = partition.local_topology()?;
        let global = partition.global_faces(&topology, &edges)?;
        self.overlay_shape_with_faces(
            partition,
            &topology,
            &edges,
            &global,
            operation,
            declared_axes,
            strict,
            context,
        )
    }

    pub(in crate::geometry) fn overlay_polygon_parts_with_preserved_operand(
        &self,
        operation: crate::geometry::OverlayOp,
        preserved_operand: u32,
    ) -> Result<Vec<(Polygon, Vec<Vec<bool>>)>> {
        let partition = self.partition_without_budget()?;
        let (topology, edges) = partition.local_topology()?;
        let global = partition.global_faces(&topology, &edges)?;
        let cells = self.select_cells(&partition, &topology, &edges, &global, operation, false)?;
        let mut preserved_spans = Vec::with_capacity(partition.spans.len());
        for span in 0..partition.spans.len() {
            let preserved = partition
                .provenance
                .owners(span)
                .iter()
                .any(|&(source, _)| {
                    self.source_operands.get(source as usize).copied() == Some(preserved_operand)
                });
            preserved_spans.push(preserved);
        }
        let plan = cells.emission_plan_with_protected(
            &partition,
            &topology,
            &edges,
            &global,
            None,
            Some(&preserved_spans),
        )?;
        plan.emit_xy_polygon_parts_with_source_roles(
            &partition,
            &topology,
            &edges,
            operation.name(),
            &preserved_spans,
        )
    }

    fn overlay_shape_with_faces<M: ChartMetric>(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
        edges: &[u32],
        global: &GlobalFaces,
        operation: crate::geometry::OverlayOp,
        declared_axes: crate::geometry::CoordinateAxes,
        strict: bool,
        context: &SourceArcContext<'_, '_, M>,
    ) -> Result<Shape> {
        let cells = self.select_cells(partition, topology, edges, global, operation, false)?;
        let ordinate_protected = self.ordinate_protected_nodes(partition, declared_axes)?;
        let plan = cells.emission_plan_with_protected(
            partition,
            topology,
            edges,
            global,
            ordinate_protected.as_deref(),
            None,
        )?;
        if declared_axes == CoordinateAxes::XY && plan.points.is_empty() && plan.lines.is_empty() {
            let (_, _, polygons) =
                plan.emit_xy_parts(partition, topology, edges, operation.name())?;
            return Ok(Shape::narrow_polygons(CoordinateAxes::XY, polygons));
        }
        if !declared_axes.has_z() && !declared_axes.has_m() {
            let (points, lines, polygons) =
                plan.emit_xy_parts(partition, topology, edges, operation.name())?;
            return assemble_node_parts(
                points,
                lines,
                polygons,
                crate::geometry::CoordinateAxes::XY,
                crate::geometry::CoordinateAxes::XY,
                crate::geometry::CoordinateAxes::XY,
            );
        }
        let emission = plan.emission(topology, edges);
        let embedding = partition.embed(&[&emission], operation.name())?;
        let parts = plan.build_node_parts(partition, topology, &emission)?;
        let by_node = self.resolve_node_points(
            partition,
            &embedding,
            parts
                .points
                .iter()
                .copied()
                .chain(parts.lines.iter().flatten().copied())
                .chain(
                    parts
                        .polygons
                        .iter()
                        .flat_map(|polygon| polygon.iter().flatten().copied()),
                ),
            declared_axes,
            context,
            None,
        )?;
        self.node_parts_to_shape_resolved(
            parts,
            &embedding,
            &by_node,
            declared_axes,
            strict,
            operation.name(),
            None,
        )
    }

    /// Select both closed children of one subdivision cut from one exact
    /// arrangement. The cut rectangles are separate polygon operands, but
    /// their seam and every source crossing share the same node recipes,
    /// topology, rounding pass, and ordinate resolution.
    /// The axis-aligned cut ordinate is exactly representable and pinned by
    /// the embedding. Dissolving a straight seam in one child cannot move a
    /// vertex retained by the other child off that seam.
    pub(in crate::geometry) fn subdivision_parts<M: ChartMetric>(
        &self,
        partition: &ExactPartition<'_>,
        source_shape: &Shape,
        declared_axes: crate::geometry::CoordinateAxes,
        strict: bool,
        clip_operands: [usize; 2],
        budget: &mut ExpansionBudget,
        context: &SourceArcContext<'_, '_, M>,
    ) -> Result<[Shape; 2]> {
        let geographic_frame = match context.domain() {
            crate::geometry::TopologyDomain::Cartesian => None,
            crate::geometry::TopologyDomain::Geographic(frame) => Some(frame),
        };
        let source_operand = 0;
        let operands = self.operand_count();
        if clip_operands
            .iter()
            .any(|&operand| operand >= operands || operand == source_operand)
        {
            return Err(GeometryErrorKind::Invalid(
                "exact subdivision clip operand is out of range".into(),
            )
            .into());
        }

        let (topology, edges) = partition.local_topology()?;
        let global = partition.global_faces(&topology, &edges)?;
        let winding_rings = self.winding_source_rings();
        let facts =
            self.build_cell_facts(partition, &topology, &edges, &global, &winding_rings, false)?;
        if let Some(line_source) = LineSource::from_shape(source_shape) {
            return self.subdivision_line_parts(
                partition,
                line_source,
                &topology,
                &global,
                &facts,
                declared_axes,
                strict,
                clip_operands,
                context,
            );
        }
        let ordinate_protected = self.ordinate_protected_nodes(partition, declared_axes)?;
        let preserved_spans = geographic_frame.is_some().then(|| {
            let mut roles = Vec::with_capacity(partition.spans.len());
            for span in 0..partition.spans.len() {
                roles.push(
                    partition
                        .provenance
                        .owners(span)
                        .iter()
                        .any(|&(source, _)| {
                            self.source_operands.get(source as usize).copied()
                                == Some(source_operand as u32)
                        }),
                );
            }
            roles
        });

        let mut plans = Vec::with_capacity(clip_operands.len());
        for &clip_operand in &clip_operands {
            let cells = self.select_subdivision_cells(
                partition,
                &topology,
                &global,
                &facts,
                source_operand,
                clip_operand,
            );
            plans.push(cells.emission_plan_with_protected(
                partition,
                &topology,
                &edges,
                &global,
                ordinate_protected.as_deref(),
                preserved_spans.as_deref(),
            )?);
        }
        let polygon_edge_roles = preserved_spans
            .as_ref()
            .map(|preserved_spans| {
                plans
                    .iter()
                    .map(|plan| {
                        plan.polygon_edge_roles(partition, &topology, &edges, preserved_spans)
                    })
                    .collect::<Result<Vec<_>>>()
            })
            .transpose()?;

        let emissions: Vec<_> = plans
            .iter()
            .map(|plan| plan.emission(&topology, &edges))
            .collect();
        let emission_refs: Vec<_> = emissions.iter().collect();
        let embedding = partition.embed(&emission_refs, "subdivide")?;

        let mut node_parts = Vec::with_capacity(plans.len());
        for (plan, emission) in plans.iter().zip(&emissions) {
            node_parts.push(plan.build_node_parts(partition, &topology, emission)?);
        }

        let by_node = self.resolve_node_points(
            partition,
            &embedding,
            emissions
                .iter()
                .flat_map(|emission| emission.items.iter().flatten().copied()),
            declared_axes,
            context,
            None,
        )?;
        let mut parts = node_parts.into_iter();
        let mut polygon_edge_roles = polygon_edge_roles.unwrap_or_default().into_iter();
        let first_roles = polygon_edge_roles.next();
        let first_publication = match (first_roles, geographic_frame) {
            (Some(roles), Some(frame)) => Some((roles, frame, &mut *budget)),
            _ => None,
        };
        let first = self.node_parts_to_shape_resolved(
            parts
                .next()
                .expect("two subdivision plans produce two outputs"),
            &embedding,
            &by_node,
            declared_axes,
            strict,
            "subdivide",
            first_publication,
        )?;
        let second_roles = polygon_edge_roles.next();
        let second_publication = match (second_roles, geographic_frame) {
            (Some(roles), Some(frame)) => Some((roles, frame, &mut *budget)),
            _ => None,
        };
        let second = self.node_parts_to_shape_resolved(
            parts
                .next()
                .expect("two subdivision plans produce two outputs"),
            &embedding,
            &by_node,
            declared_axes,
            strict,
            "subdivide",
            second_publication,
        )?;
        Ok([first, second])
    }

    pub(in crate::geometry) fn subdivision_parts_with_budget<M: ChartMetric, C: RootCatalog>(
        &self,
        partition: &ExactPartition<'_>,
        source_shape: &Shape,
        source_traces: Option<&[SourceTrace]>,
        roots: &C,
        declared_axes: CoordinateAxes,
        strict: bool,
        clip_operands: [usize; 2],
        budget: &mut ExpansionBudget,
        context: &SourceArcContext<'_, '_, M>,
        trace_work: &mut EvaluationBudget,
    ) -> Result<[SubdivisionPart; 2]> {
        let mut resolve_rows = |rows: &[RequestRowInput], work: &mut EvaluationBudget| {
            resolve_request_rows(rows, roots, context.metric_access(), work)
        };
        self.subdivision_parts_traced(
            partition,
            source_shape,
            source_traces,
            roots,
            None,
            declared_axes,
            strict,
            clip_operands,
            budget,
            context,
            trace_work,
            false,
            &mut resolve_rows,
        )
    }

    pub(in crate::geometry) fn subdivision_parts_with_roots<M: ChartMetric>(
        &self,
        partition: &ExactPartition<'_>,
        source_shape: &Shape,
        source_traces: Option<&[SourceTrace]>,
        roots: &SubdivisionRootCatalog,
        declared_axes: CoordinateAxes,
        strict: bool,
        clip_operands: [usize; 2],
        budget: &mut ExpansionBudget,
        context: &SourceArcContext<'_, '_, M>,
        trace_work: &mut Unmetered,
    ) -> Result<[SubdivisionPart; 2]> {
        let root_positions = roots.root_xy_positions_needed().then_some(roots);
        let mut resolve_rows = |rows: &[RequestRowInput], work: &mut Unmetered| {
            roots.resolve_output_requests(rows, context.metric_access(), work)
        };
        match source_traces {
            None => self.subdivision_parts_traced(
                partition,
                source_shape,
                None,
                roots,
                root_positions,
                declared_axes,
                strict,
                clip_operands,
                budget,
                context,
                trace_work,
                false,
                &mut resolve_rows,
            ),
            Some(source_traces) => {
                if LineSource::from_shape(source_shape).is_some() {
                    return self.subdivision_parts_traced(
                        partition,
                        source_shape,
                        Some(source_traces),
                        roots,
                        root_positions,
                        declared_axes,
                        strict,
                        clip_operands,
                        budget,
                        context,
                        trace_work,
                        true,
                        &mut resolve_rows,
                    );
                }
                self.subdivision_polygon_parts_with_roots(
                    partition,
                    source_shape,
                    source_traces,
                    roots,
                    declared_axes,
                    strict,
                    clip_operands,
                    budget,
                    context,
                    trace_work,
                )
            },
        }
    }

    pub(in crate::geometry) fn subdivision_polygon_parts_with_roots<M: ChartMetric>(
        &self,
        partition: &ExactPartition<'_>,
        source_shape: &Shape,
        source_traces: &[SourceTrace],
        roots: &SubdivisionRootCatalog,
        declared_axes: CoordinateAxes,
        strict: bool,
        clip_operands: [usize; 2],
        budget: &mut ExpansionBudget,
        context: &SourceArcContext<'_, '_, M>,
        trace_work: &mut Unmetered,
    ) -> Result<[SubdivisionPart; 2]> {
        let ring_ranges = polygon_source_ring_ranges(source_shape)?;
        if source_traces.len() != ring_ranges.len() {
            return Err(GeometryErrorKind::Invalid(
                "polygon source traces do not match the shell and hole rings".into(),
            )
            .into());
        }
        for (range, trace) in ring_ranges.iter().zip(source_traces) {
            if trace.edge_count() != range.len() {
                return Err(GeometryErrorKind::Invalid(
                    "polygon source trace edge count differs from its ring".into(),
                )
                .into());
            }
        }
        let operands = self.operand_count();
        if clip_operands
            .iter()
            .any(|&operand| operand >= operands || operand == 0)
        {
            return Err(GeometryErrorKind::Invalid(
                "exact subdivision clip operand is out of range".into(),
            )
            .into());
        }

        let (topology, edges) = partition.local_topology()?;
        let global = partition.global_faces(&topology, &edges)?;
        let winding_rings = self.winding_source_rings();
        let facts =
            self.build_cell_facts(partition, &topology, &edges, &global, &winding_rings, false)?;
        let protected = self.ordinate_protected_nodes(partition, declared_axes)?;
        let preserved_spans = {
            let mut roles = Vec::with_capacity(partition.spans.len());
            for span in 0..partition.spans.len() {
                roles.push(
                    partition
                        .provenance
                        .owners(span)
                        .iter()
                        .any(|&(source, _)| {
                            self.source_operands.get(source as usize).copied() == Some(0)
                        }),
                );
            }
            roles
        };
        let mut plans = Vec::with_capacity(clip_operands.len());
        for &clip_operand in &clip_operands {
            let cells = self.select_subdivision_cells(
                partition,
                &topology,
                &global,
                &facts,
                0,
                clip_operand,
            );
            plans.push(cells.emission_plan_with_protected(
                partition,
                &topology,
                &edges,
                &global,
                protected.as_deref(),
                Some(&preserved_spans),
            )?);
        }
        let polygon_edge_roles = plans
            .iter()
            .map(|plan| plan.polygon_edge_roles(partition, &topology, &edges, &preserved_spans))
            .collect::<Result<Vec<_>>>()?;
        let emissions: Vec<_> = plans
            .iter()
            .map(|plan| plan.emission(&topology, &edges))
            .collect();
        let emission_refs: Vec<_> = emissions.iter().collect();
        let embedding = partition.embed(&emission_refs, "subdivide")?;
        let mut node_parts = Vec::with_capacity(plans.len());
        for (plan, emission) in plans.iter().zip(&emissions) {
            node_parts.push(plan.build_node_parts(partition, &topology, emission)?);
        }
        trace_work.charge(
            u64::try_from(partition.node_count()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut nodes = Vec::new();
        let mut seen = vec![false; partition.node_count()];
        for emission in &emissions {
            for &[start, end] in &emission.items {
                for node in [start, end] {
                    trace_work.charge(1)?;
                    let admitted = seen.get_mut(node.0 as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "subdivision emission references an unknown node".into(),
                        )
                    })?;
                    if !*admitted {
                        *admitted = true;
                        nodes.push(node);
                    }
                }
            }
        }
        let views = self.source_line_trace_views(partition, trace_work)?;
        let root_positions = roots.root_xy_positions_needed().then_some(roots);
        let (requests_by_node, root_xy_nodes) = self.polygon_node_root_requests(
            partition,
            &nodes,
            &ring_ranges,
            source_traces,
            &views,
            root_positions.is_some(),
            trace_work,
        )?;
        let mut polygon_traces = Vec::with_capacity(plans.len());
        for (parts, roles) in node_parts.iter().zip(&polygon_edge_roles) {
            polygon_traces.push(self.polygon_ring_traces_from_nodes(
                &parts.polygons,
                roles,
                &requests_by_node,
                trace_work,
            )?);
        }
        let mut resolve_rows = |rows: &[RequestRowInput], work: &mut Unmetered| {
            roots.resolve_output_requests(rows, context.metric_access(), work)
        };
        let by_node = resolve_traced_node_requests(
            partition,
            &embedding,
            &nodes,
            requests_by_node,
            declared_axes,
            root_positions,
            root_xy_nodes.as_deref(),
            Some(self),
            true,
            context,
            trace_work,
            &mut resolve_rows,
        )?;
        let old_polygon_xy = node_parts
            .iter()
            .map(|parts| self.polygon_node_xy(&parts.polygons, &by_node))
            .collect::<Result<Vec<_>>>()?;
        let mut node_parts = node_parts.into_iter();
        let mut polygon_edge_roles = polygon_edge_roles.into_iter();
        let first_roles = polygon_edge_roles.next().ok_or_else(|| {
            GeometryErrorKind::Invalid("first polygon edge-role plan is unknown".into())
        })?;
        let first_publication = Some((first_roles.clone(), roots.frame, &mut *budget));
        let first = self.node_parts_to_shape_resolved(
            node_parts.next().ok_or_else(|| {
                GeometryErrorKind::Invalid("first subdivision polygon part is unknown".into())
            })?,
            &embedding,
            &by_node,
            declared_axes,
            strict,
            "subdivide",
            first_publication,
        )?;
        let first_traces = self.align_published_polygon_traces(
            &first,
            &old_polygon_xy[0],
            &first_roles,
            polygon_traces.remove(0),
            roots,
            roots.frame,
            trace_work,
        )?;
        let first_traces = (!first_traces.is_empty()).then_some(first_traces);
        let second_roles = polygon_edge_roles.next().ok_or_else(|| {
            GeometryErrorKind::Invalid("second polygon edge-role plan is unknown".into())
        })?;
        let second_publication = Some((second_roles.clone(), roots.frame, &mut *budget));
        let second = self.node_parts_to_shape_resolved(
            node_parts.next().ok_or_else(|| {
                GeometryErrorKind::Invalid("second subdivision polygon part is unknown".into())
            })?,
            &embedding,
            &by_node,
            declared_axes,
            strict,
            "subdivide",
            second_publication,
        )?;
        let second_traces = self.align_published_polygon_traces(
            &second,
            &old_polygon_xy[1],
            &second_roles,
            polygon_traces.remove(0),
            roots,
            roots.frame,
            trace_work,
        )?;
        let second_traces = (!second_traces.is_empty()).then_some(second_traces);
        Ok([
            SubdivisionPart {
                shape: first,
                traces: None,
                polygon_traces: first_traces,
            },
            SubdivisionPart {
                shape: second,
                traces: None,
                polygon_traces: second_traces,
            },
        ])
    }

    fn subdivision_parts_traced<
        M: ChartMetric,
        C: RootCatalog,
        W: SubdivisionTraceWork,
        F: FnMut(&[RequestRowInput], &mut W) -> Result<Vec<NodeOrdinates>>,
    >(
        &self,
        partition: &ExactPartition<'_>,
        source_shape: &Shape,
        source_traces: Option<&[SourceTrace]>,
        roots: &C,
        root_positions: Option<&SubdivisionRootCatalog>,
        declared_axes: crate::geometry::CoordinateAxes,
        strict: bool,
        clip_operands: [usize; 2],
        budget: &mut ExpansionBudget,
        context: &SourceArcContext<'_, '_, M>,
        trace_work: &mut W,
        reuse_existing_source_vertices: bool,
        resolve_rows: &mut F,
    ) -> Result<[SubdivisionPart; 2]> {
        let Some(source_traces) = source_traces else {
            let [left, right] = self.subdivision_parts(
                partition,
                source_shape,
                declared_axes,
                strict,
                clip_operands,
                budget,
                context,
            )?;
            return Ok([
                SubdivisionPart {
                    shape: left,
                    traces: None,
                    polygon_traces: None,
                },
                SubdivisionPart {
                    shape: right,
                    traces: None,
                    polygon_traces: None,
                },
            ]);
        };
        let source = LineSource::from_shape(source_shape).ok_or_else(|| {
            GeometryErrorKind::Invalid("source trace was attached to a non-line shape".into())
        })?;
        let ranges = source_line_ranges(source)?;
        if source_traces.len() != ranges.len() {
            return Err(GeometryErrorKind::Invalid(
                "source traces do not match the subdivision line members".into(),
            )
            .into());
        }
        let operands = self.operand_count();
        if clip_operands
            .iter()
            .any(|&operand| operand >= operands || operand == 0)
        {
            return Err(GeometryErrorKind::Invalid(
                "exact subdivision clip operand is out of range".into(),
            )
            .into());
        }
        let (topology, edges) = partition.local_topology()?;
        let global = partition.global_faces(&topology, &edges)?;
        let winding_rings = self.winding_source_rings();
        let facts =
            self.build_cell_facts(partition, &topology, &edges, &global, &winding_rings, false)?;
        let mut selected = Vec::with_capacity(clip_operands.len());
        for &clip_operand in &clip_operands {
            let cells = self.select_subdivision_cells(
                partition,
                &topology,
                &global,
                &facts,
                0,
                clip_operand,
            );
            selected.push(cells.spans);
        }
        let emissions: Vec<_> = selected
            .iter()
            .map(|spans| {
                partition.span_emission(
                    spans
                        .iter()
                        .enumerate()
                        .filter_map(|(span, &keep)| keep.then_some(span as u32)),
                )
            })
            .collect();
        let emission_refs: Vec<_> = emissions.iter().collect();
        trace_work.charge(
            u64::try_from(partition.node_count()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut nodes = Vec::new();
        let mut seen = vec![false; partition.node_count()];
        for emission in &emissions {
            for &[start, end] in &emission.items {
                for node in [start, end] {
                    trace_work.charge(1)?;
                    let admitted = seen.get_mut(node.0 as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "subdivision emission references an unknown node".into(),
                        )
                    })?;
                    if !*admitted {
                        *admitted = true;
                        nodes.push(node);
                    }
                }
            }
        }
        let embedding = trace_work.embed(partition, &emission_refs, "subdivide")?;
        let views = self.source_line_trace_views(partition, trace_work)?;
        let mut run_plans = Vec::with_capacity(selected.len());
        for spans in &selected {
            run_plans.push(self.source_line_runs(
                partition,
                &ranges,
                &views,
                spans,
                source_traces,
                trace_work,
            )?);
        }
        let by_node = self.resolve_traced_source_line_points(
            partition,
            &embedding,
            &nodes,
            &run_plans,
            source_traces,
            declared_axes,
            root_positions,
            reuse_existing_source_vertices.then_some(self),
            reuse_existing_source_vertices,
            context,
            trace_work,
            resolve_rows,
        )?;
        let mut parts = Vec::with_capacity(run_plans.len());
        for runs in run_plans {
            parts.push(self.emit_traced_source_line_shape(
                source.kind(),
                runs,
                source_traces,
                &by_node,
                roots,
                root_positions,
                declared_axes,
                strict,
                context,
                trace_work,
            )?);
        }
        let mut parts = parts.into_iter();
        Ok([
            parts.next().expect("two subdivision selections"),
            parts.next().expect("two subdivision selections"),
        ])
    }

    fn source_line_trace_views<W: Work + ?Sized>(
        &self,
        partition: &ExactPartition<'_>,
        work: &mut W,
    ) -> Result<SourceLineTraceViews> {
        let source_count = self
            .source_operands
            .iter()
            .filter(|&&operand| operand == 0)
            .count();
        let mut source_ids = Vec::with_capacity(source_count);
        for (source, &operand) in self.source_operands.iter().enumerate() {
            work.charge(1)?;
            if operand == 0 {
                source_ids.push(source);
            }
        }
        let views = partition.source_span_trace_views(&self.source_segments, &source_ids, work)?;
        if views.len() != source_ids.len() {
            return Err(GeometryErrorKind::Invalid(
                "ordinary source trace views do not match the source rows".into(),
            )
            .into());
        }
        Ok(SourceLineTraceViews { source_ids, views })
    }

    fn polygon_node_root_requests<W: Work>(
        &self,
        partition: &ExactPartition<'_>,
        nodes: &[NodeId],
        ring_ranges: &[Range<usize>],
        ring_traces: &[SourceTrace],
        views: &SourceLineTraceViews,
        track_root_xy_nodes: bool,
        work: &mut W,
    ) -> Result<(Vec<Vec<RootRequest>>, Option<Vec<bool>>)> {
        if ring_ranges.len() != ring_traces.len() {
            return Err(GeometryErrorKind::Invalid(
                "polygon source ring ranges do not match their root traces".into(),
            )
            .into());
        }
        let edge_count = ring_ranges.iter().try_fold(0_usize, |count, range| {
            count
                .checked_add(range.len())
                .ok_or(ResourceError::SizeOverflow)
        })?;
        if edge_count != views.source_ids.len() || views.source_ids.len() != views.views.len() {
            return Err(GeometryErrorKind::Invalid(
                "polygon operand-0 source rows do not match the ring trace inventory".into(),
            )
            .into());
        }
        let mut ring_of_source = vec![None; self.source_segments.len()];
        for (ring, range) in ring_ranges.iter().enumerate() {
            for source in range.clone() {
                let slot = ring_of_source.get_mut(source).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "polygon ring ranges are outside the source segment inventory".into(),
                    )
                })?;
                *slot = Some((ring, source - range.start));
            }
        }
        let mut visible_nodes = vec![false; partition.node_count()];
        for &node in nodes {
            let slot = visible_nodes.get_mut(node.0 as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("polygon emission node is unknown".into())
            })?;
            *slot = true;
        }
        let mut requests_by_node = Vec::new();
        requests_by_node.resize_with(partition.node_count(), Vec::new);
        let mut root_xy_nodes = track_root_xy_nodes.then(|| vec![false; partition.node_count()]);
        for (&full_source, source_views) in views.source_ids.iter().zip(&views.views) {
            work.charge(1)?;
            if self.source_operands.get(full_source).copied() != Some(0) {
                return Err(GeometryErrorKind::Invalid(
                    "polygon trace remap includes a clip-operand source".into(),
                )
                .into());
            }
            let (ring, edge) = ring_of_source
                .get(full_source)
                .copied()
                .flatten()
                .ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "polygon source remap has no shell or hole trace".into(),
                    )
                })?;
            let trace = ring_traces.get(ring).ok_or_else(|| {
                GeometryErrorKind::Invalid("polygon source ring trace is unknown".into())
            })?;
            let ring_range = ring_ranges.get(ring).ok_or_else(|| {
                GeometryErrorKind::Invalid("polygon ring source range is unknown".into())
            })?;
            if trace.edge_count() != ring_range.len() {
                return Err(GeometryErrorKind::Invalid(
                    "polygon source trace edge count differs from its ring".into(),
                )
                .into());
            }
            for view in source_views {
                work.charge(1)?;
                let span = *partition.spans.get(view.span as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid("polygon source trace span is unknown".into())
                })?;
                let endpoints = if view.reversed {
                    [
                        (span.start, &view.source_end),
                        (span.end, &view.source_start),
                    ]
                } else {
                    [
                        (span.start, &view.source_start),
                        (span.end, &view.source_end),
                    ]
                };
                for (node, parameter) in endpoints {
                    if !visible_nodes.get(node.0 as usize).copied().unwrap_or(false) {
                        continue;
                    }
                    let row = requests_by_node.get_mut(node.0 as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "polygon source trace node is outside the partition".into(),
                        )
                    })?;
                    let added = trace.append_requests_at(edge, parameter, row, work)?;
                    work.charge(u64::try_from(added).map_err(|_| ResourceError::SizeOverflow)?)?;
                    if let Some(root_xy_nodes) = root_xy_nodes.as_mut() {
                        mark_root_xy_node(root_xy_nodes, node, parameter, added)?;
                    }
                }
            }
        }
        Ok((requests_by_node, root_xy_nodes))
    }

    fn polygon_ring_traces_from_nodes<W: Work>(
        &self,
        polygons: &[Vec<Vec<NodeId>>],
        computed_edges: &[Vec<Vec<bool>>],
        requests_by_node: &[Vec<RootRequest>],
        work: &mut W,
    ) -> Result<Vec<SourceTrace>> {
        if polygons.len() != computed_edges.len() {
            return Err(GeometryErrorKind::Invalid(
                "polygon ring traces do not match the emitted polygon parts".into(),
            )
            .into());
        }
        let ring_count = polygons.iter().try_fold(0_usize, |count, polygon| {
            count
                .checked_add(polygon.len())
                .ok_or(ResourceError::SizeOverflow)
        })?;
        let mut traces = Vec::with_capacity(ring_count);
        for (polygon_index, polygon) in polygons.iter().enumerate() {
            let roles = computed_edges.get(polygon_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("polygon edge-role part is unknown".into())
            })?;
            if roles.len() != polygon.len() {
                return Err(GeometryErrorKind::Invalid(
                    "polygon edge roles do not match shell and hole rings".into(),
                )
                .into());
            }
            for (ring, roles) in polygon.iter().zip(roles) {
                let edge_count = ring.len().saturating_sub(1);
                if roles.len() != edge_count {
                    return Err(GeometryErrorKind::Invalid(
                        "polygon edge roles do not match a closed ring".into(),
                    )
                    .into());
                }
                let mut edge_rows = Vec::with_capacity(edge_count);
                for (edge, &computed) in roles.iter().enumerate() {
                    work.charge(1)?;
                    if computed {
                        edge_rows.push(Vec::new());
                        continue;
                    }
                    let start = *ring.get(edge).ok_or_else(|| {
                        GeometryErrorKind::Invalid("polygon trace start node is unknown".into())
                    })?;
                    let end = *ring.get(edge.wrapping_add(1)).ok_or_else(|| {
                        GeometryErrorKind::Invalid("polygon trace end node is unknown".into())
                    })?;
                    let start_requests =
                        requests_by_node.get(start.0 as usize).ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "polygon trace start node has no source request row".into(),
                            )
                        })?;
                    let end_requests = requests_by_node.get(end.0 as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "polygon trace end node has no source request row".into(),
                        )
                    })?;
                    edge_rows.push(mapped_root_ranges_between_requests(
                        start_requests,
                        end_requests,
                        work,
                    )?);
                }
                traces.push(SourceTrace::from_edge_rows(edge_rows, work)?);
            }
        }
        Ok(traces)
    }

    fn polygon_node_xy(
        &self,
        polygons: &[Vec<Vec<NodeId>>],
        by_node: &ResolvedNodePoints<'_, '_>,
    ) -> Result<Vec<Vec<Vec<XY>>>> {
        let coordinates = by_node.coordinates();
        polygons
            .iter()
            .map(|polygon| {
                polygon
                    .iter()
                    .map(|ring| {
                        ring.iter()
                            .map(|&node| {
                                Ok(by_node
                                    .point(node, embedding::Embedding::xy_at(coordinates, node))
                                    .xy())
                            })
                            .collect()
                    })
                    .collect()
            })
            .collect()
    }

    fn align_published_polygon_traces<W: Work>(
        &self,
        shape: &Shape,
        old_xy: &[Vec<Vec<XY>>],
        computed_edges: &[Vec<Vec<bool>>],
        source_traces: Vec<SourceTrace>,
        roots: &SubdivisionRootCatalog,
        frame: GeographicFrame,
        work: &mut W,
    ) -> Result<Vec<SourceTrace>> {
        let mut polygons = Vec::new();
        collect_polygon_parts(shape, &mut polygons);
        if polygons.len() != old_xy.len() || polygons.len() != computed_edges.len() {
            return Err(GeometryErrorKind::Invalid(
                "published polygons do not match their subdivision trace rows".into(),
            )
            .into());
        }
        let trace_count = source_traces.len();
        let mut source_traces = source_traces.into_iter();
        let mut traces = Vec::with_capacity(trace_count);
        for (polygon_index, polygon) in polygons.into_iter().enumerate() {
            let old_rings = old_xy.get(polygon_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("unpublished polygon trace rows are unknown".into())
            })?;
            let edge_roles = computed_edges.get(polygon_index).ok_or_else(|| {
                GeometryErrorKind::Invalid("published polygon edge roles are unknown".into())
            })?;
            let published_rings = polygon.rings().collect::<Vec<_>>();
            if old_rings.len() != edge_roles.len() || published_rings.len() != old_rings.len() {
                return Err(GeometryErrorKind::Invalid(
                    "published shell and hole rows do not match their source roles".into(),
                )
                .into());
            }
            for ((old_ring, roles), published_ring) in
                old_rings.iter().zip(edge_roles).zip(published_rings)
            {
                let source_trace = source_traces.next().ok_or_else(|| {
                    GeometryErrorKind::Invalid("polygon ring source trace is unknown".into())
                })?;
                let old_edge_count = old_ring.len().saturating_sub(1);
                if roles.len() != old_edge_count || source_trace.edge_count() != old_edge_count {
                    return Err(GeometryErrorKind::Invalid(
                        "polygon trace rows do not match their stored ring edges".into(),
                    )
                    .into());
                }
                let published = published_ring.coords();
                let published_edge_count = published.coord_count().saturating_sub(1);
                let source_start = *old_ring.first().ok_or_else(|| {
                    GeometryErrorKind::Invalid("published polygon source ring is empty".into())
                })?;
                let mut published_start = None;
                for index in 0..published_edge_count {
                    if same_published_polygon_xy(
                        frame,
                        published.nth_coord(index).xy(),
                        source_start,
                    )? {
                        published_start = Some(index);
                        break;
                    }
                }
                let Some(published_start) = published_start else {
                    return Err(GeometryErrorKind::Invalid(
                        "published polygon ring lost its source start vertex".into(),
                    )
                    .into());
                };

                let mut slices = Vec::with_capacity(published_edge_count);
                let zero = ExactRational::zero();
                let one = ExactRational::one();
                let mut output_edge = 0_usize;
                for (edge, &computed) in roles.iter().enumerate() {
                    let target = *old_ring.get(edge.wrapping_add(1)).ok_or_else(|| {
                        GeometryErrorKind::Invalid("polygon trace endpoint is unknown".into())
                    })?;
                    let mut intermediate = Vec::new();
                    loop {
                        if output_edge >= published_edge_count {
                            let published_vertices = (0..published_edge_count)
                                .map(|index| published.nth_coord(index).xy())
                                .collect::<Vec<_>>();
                            return Err(GeometryErrorKind::Invalid(
                                format!(
                                    "polygon publication lost ring endpoint at source edge {edge}; start={published_start}, output_edge={output_edge}/{published_edge_count}, old_ring={old_ring:?}, published={published_vertices:?}"
                                )
                                .into(),
                            )
                            .into());
                        }
                        let endpoint_index =
                            published_start.wrapping_add(output_edge).wrapping_add(1)
                                % published_edge_count;
                        let endpoint = published.nth_coord(endpoint_index).xy();
                        output_edge = output_edge
                            .checked_add(1)
                            .ok_or(ResourceError::SizeOverflow)?;
                        if same_published_polygon_xy(frame, endpoint, target)? {
                            break;
                        }
                        intermediate.push(endpoint);
                    }
                    if computed {
                        slices.extend(
                            (0..intermediate.len().wrapping_add(1))
                                .map(|_| (edge, zero.clone(), one.clone())),
                        );
                        continue;
                    }
                    if intermediate.is_empty() {
                        slices.push((edge, zero.clone(), one.clone()));
                        continue;
                    }
                    let cuts =
                        self.source_line_seam_cuts(&source_trace, edge, &zero, &one, roots, work)?;
                    if cuts.len() != intermediate.len() {
                        return Err(GeometryErrorKind::Invalid(
                            "published polygon seam vertices do not match their source trace"
                                .into(),
                        )
                        .into());
                    }
                    for (cut, &endpoint) in cuts.iter().zip(&intermediate) {
                        if !same_published_polygon_xy(frame, endpoint, cut.coordinate)? {
                            return Err(GeometryErrorKind::Invalid(
                                "published polygon seam vertex differs from its source trace"
                                    .into(),
                            )
                            .into());
                        }
                    }
                    let mut start = zero.clone();
                    for cut in cuts {
                        let end = cut.parameter;
                        slices.push((edge, start, end.clone()));
                        start = end;
                    }
                    slices.push((edge, start, one.clone()));
                }
                if output_edge != published_edge_count || slices.len() != published_edge_count {
                    return Err(GeometryErrorKind::Invalid(
                        "polygon publication added an untraced edge".into(),
                    )
                    .into());
                }
                slices.rotate_right(published_start);
                traces.push(source_trace.compose_edges(&slices, work)?);
            }
        }
        if source_traces.next().is_some() {
            return Err(GeometryErrorKind::Invalid(
                "polygon source traces include an unassigned ring".into(),
            )
            .into());
        }
        Ok(traces)
    }

    fn resolve_traced_source_line_points<
        'e,
        'p,
        M: ChartMetric,
        W: Work,
        F: FnMut(&[RequestRowInput], &mut W) -> Result<Vec<NodeOrdinates>>,
    >(
        &self,
        partition: &'p ExactPartition<'_>,
        embedding: &'e embedding::Embedding<'p>,
        nodes: &[NodeId],
        run_plans: &[Vec<SourceLineRun>],
        source_traces: &[SourceTrace],
        axes: CoordinateAxes,
        root_positions: Option<&SubdivisionRootCatalog>,
        fallback_inventory: Option<&ExactSourceInventory>,
        reuse_existing_source_vertices: bool,
        context: &SourceArcContext<'_, '_, M>,
        work: &mut W,
        resolve_rows: &mut F,
    ) -> Result<ResolvedNodePoints<'e, 'p>> {
        if !axes.has_z() && !axes.has_m() && root_positions.is_none() {
            return Ok(ResolvedNodePoints::from_embedding(embedding, None));
        }
        work.charge(
            u64::try_from(partition.node_count()).map_err(|_| ResourceError::SizeOverflow)?,
        )?;
        let mut requests_by_node = Vec::new();
        requests_by_node.resize_with(partition.node_count(), Vec::new);
        let mut root_xy_nodes = root_positions.map(|_| vec![false; partition.node_count()]);
        for runs in run_plans {
            for run in runs {
                if run.nodes.len() != run.trace_slices.len().saturating_add(1) {
                    return Err(GeometryErrorKind::Invalid(
                        "traced source line run vertices do not match its edge slices".into(),
                    )
                    .into());
                }
                let trace = source_traces.get(run.member).ok_or_else(|| {
                    GeometryErrorKind::Invalid("source line trace member is unknown".into())
                })?;
                for (edge_index, (source_edge, start, end)) in run.trace_slices.iter().enumerate() {
                    let start_node = *run.nodes.get(edge_index).ok_or_else(|| {
                        GeometryErrorKind::Invalid("source line trace start node is unknown".into())
                    })?;
                    let end_node = *run.nodes.get(edge_index.wrapping_add(1)).ok_or_else(|| {
                        GeometryErrorKind::Invalid("source line trace end node is unknown".into())
                    })?;
                    let start_row =
                        requests_by_node
                            .get_mut(start_node.0 as usize)
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "source line trace start node is outside the partition".into(),
                                )
                            })?;
                    let start_added =
                        trace.append_requests_at(*source_edge, start, start_row, work)?;
                    work.charge(
                        u64::try_from(start_added).map_err(|_| ResourceError::SizeOverflow)?,
                    )?;
                    if let Some(root_xy_nodes) = root_xy_nodes.as_mut() {
                        mark_root_xy_node(root_xy_nodes, start_node, start, start_added)?;
                    }
                    let end_row =
                        requests_by_node
                            .get_mut(end_node.0 as usize)
                            .ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "source line trace end node is outside the partition".into(),
                                )
                            })?;
                    let end_added = trace.append_requests_at(*source_edge, end, end_row, work)?;
                    work.charge(
                        u64::try_from(end_added).map_err(|_| ResourceError::SizeOverflow)?,
                    )?;
                    if let Some(root_xy_nodes) = root_xy_nodes.as_mut() {
                        mark_root_xy_node(root_xy_nodes, end_node, end, end_added)?;
                    }
                }
            }
        }
        resolve_traced_node_requests(
            partition,
            embedding,
            nodes,
            requests_by_node,
            axes,
            root_positions,
            root_xy_nodes.as_deref(),
            fallback_inventory,
            reuse_existing_source_vertices,
            context,
            work,
            resolve_rows,
        )
    }

    fn source_line_seam_cuts<W: Work>(
        &self,
        trace: &SourceTrace,
        source_edge: usize,
        start: &ExactRational,
        end: &ExactRational,
        roots: &SubdivisionRootCatalog,
        work: &mut W,
    ) -> Result<Vec<SourceLineSeamCut>> {
        let start_requests = trace.requests_at(source_edge, start, work)?;
        let end_requests = trace.requests_at(source_edge, end, work)?;
        let mut cuts = Vec::<SourceLineSeamCut>::new();
        for start_request in start_requests {
            work.charge(1)?;
            let Some(end_request) = end_requests
                .iter()
                .find(|request| request.root() == start_request.root())
            else {
                continue;
            };
            let Some(root_parameter) = roots.seam_parameter(start_request.root(), work)? else {
                continue;
            };
            let root_order = start_request
                .parameter()
                .try_cmp_with_work(end_request.parameter(), work)?;
            if root_order.is_eq() {
                continue;
            }
            let before = start_request
                .parameter()
                .try_cmp_with_work(&root_parameter, work)?;
            let after = end_request
                .parameter()
                .try_cmp_with_work(&root_parameter, work)?;
            let crosses = if root_order.is_lt() {
                before.is_lt() && after.is_gt()
            } else {
                before.is_gt() && after.is_lt()
            };
            if !crosses {
                continue;
            }
            let root_width = end_request
                .parameter()
                .try_subtract_with_work(start_request.parameter(), work)?;
            let root_offset =
                root_parameter.try_subtract_with_work(start_request.parameter(), work)?;
            let edge_fraction = root_offset.try_quotient_with_work(&root_width, work)?;
            let edge_width = end.try_subtract_with_work(start, work)?;
            let parameter = start.try_add_with_work(
                &edge_width.try_product_with_work(&edge_fraction, work)?,
                work,
            )?;
            if parameter.try_cmp_with_work(start, work)?.is_le()
                || parameter.try_cmp_with_work(end, work)?.is_ge()
            {
                continue;
            }
            let coordinate = roots.source_position(start_request.root(), &root_parameter, work)?;
            let Some(coordinate) = coordinate else {
                return Err(GeometryErrorKind::Invalid(
                    "an original seam crossing has no source-chart position".into(),
                )
                .into());
            };
            let requests = trace.requests_at(source_edge, &parameter, work)?;
            let mut insert_at = cuts.len();
            let mut duplicate = false;
            for (index, existing) in cuts.iter().enumerate() {
                match parameter.try_cmp_with_work(&existing.parameter, work)? {
                    Ordering::Less => {
                        insert_at = index;
                        break;
                    },
                    Ordering::Equal => {
                        duplicate = true;
                        break;
                    },
                    Ordering::Greater => {},
                }
            }
            if !duplicate {
                cuts.insert(insert_at, SourceLineSeamCut {
                    parameter,
                    coordinate,
                    requests,
                });
            }
        }
        Ok(cuts)
    }

    fn subdivision_line_parts<M: ChartMetric>(
        &self,
        partition: &ExactPartition<'_>,
        source: LineSource<'_>,
        topology: &IndexedTopology,
        global: &GlobalFaces,
        facts: &CellFacts,
        declared_axes: crate::geometry::CoordinateAxes,
        strict: bool,
        clip_operands: [usize; 2],
        context: &SourceArcContext<'_, '_, M>,
    ) -> Result<[Shape; 2]> {
        let mut selected = Vec::with_capacity(clip_operands.len());
        for &clip_operand in &clip_operands {
            let cells =
                self.select_subdivision_cells(partition, topology, global, facts, 0, clip_operand);
            selected.push(cells.spans);
        }

        let emissions: Vec<_> = selected
            .iter()
            .map(|spans| {
                partition.span_emission(
                    spans
                        .iter()
                        .enumerate()
                        .filter_map(|(span, &selected)| selected.then_some(span as u32)),
                )
            })
            .collect();
        let emission_refs: Vec<_> = emissions.iter().collect();
        let embedding = partition.embed(&emission_refs, "subdivide")?;
        let by_node = self.resolve_node_points(
            partition,
            &embedding,
            emissions
                .iter()
                .flat_map(|emission| emission.items.iter().flatten().copied()),
            declared_axes,
            context,
            None,
        )?;
        let views = partition.source_span_views(&self.source_segments)?;
        let ranges = source_line_ranges(source)?;
        let mut outputs = Vec::with_capacity(selected.len());
        for spans in selected {
            outputs.push(self.emit_source_line_shape(
                partition,
                source.kind(),
                &ranges,
                &views,
                &spans,
                &by_node,
                declared_axes,
                strict,
            )?);
        }
        let mut outputs = outputs.into_iter();
        Ok([
            outputs.next().expect("two subdivision line selections"),
            outputs.next().expect("two subdivision line selections"),
        ])
    }

    fn source_line_runs<W: Work + ?Sized>(
        &self,
        partition: &ExactPartition<'_>,
        ranges: &[Range<usize>],
        views: &SourceLineTraceViews,
        selected: &[bool],
        source_traces: &[SourceTrace],
        work: &mut W,
    ) -> Result<Vec<SourceLineRun>> {
        #[cfg(test)]
        SOURCE_LINE_RUNS_CALLS.with(|calls| {
            calls.set(calls.get().saturating_add(1));
        });
        if source_traces.len() != ranges.len() {
            return Err(GeometryErrorKind::Invalid(
                "source trace rows do not match the line members".into(),
            )
            .into());
        }
        if views.source_ids.len() != views.views.len() {
            return Err(GeometryErrorKind::Invalid(
                "exact subdivision line view remap does not match its source rows".into(),
            )
            .into());
        }
        work.charge(u64::try_from(selected.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut runs = Vec::new();
        let mut source_offset = 0_usize;
        for (member, range) in ranges.iter().enumerate() {
            work.charge(1)?;
            if range.start != source_offset {
                return Err(GeometryErrorKind::Invalid(
                    "line member source ranges are not contiguous".into(),
                )
                .into());
            }
            source_offset = range.end;
            let trace = source_traces.get(member).ok_or_else(|| {
                GeometryErrorKind::Invalid("source trace member is unknown".into())
            })?;
            if trace.edge_count() != range.len() {
                return Err(GeometryErrorKind::Invalid(
                    "source trace edge count differs from its line member".into(),
                )
                .into());
            }
            let mut run = Vec::new();
            let mut trace_slices = Vec::new();
            for source in range.clone() {
                let (Some(full_source), Some(source_views)) = (
                    views.source_ids.get(source).copied(),
                    views.views.get(source),
                ) else {
                    continue;
                };
                if self.source_operands.get(full_source).copied() != Some(0) {
                    return Err(GeometryErrorKind::Invalid(
                        "exact subdivision line remap includes a clip-operand source".into(),
                    )
                    .into());
                }
                for view in source_views {
                    work.charge(1)?;
                    let is_selected = *selected.get(view.span as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid(
                            "source trace span is outside the selected span table".into(),
                        )
                    })?;
                    if !is_selected {
                        push_source_line_run(
                            &mut runs,
                            member,
                            &mut run,
                            &mut trace_slices,
                            true,
                            work,
                        )?;
                        continue;
                    }
                    let span = *partition.spans.get(view.span as usize).ok_or_else(|| {
                        GeometryErrorKind::Invalid("source trace span is unknown".into())
                    })?;
                    let (start, end) = if view.reversed {
                        (span.end, span.start)
                    } else {
                        (span.start, span.end)
                    };
                    if run.last().is_some_and(|&last| last != start) {
                        push_source_line_run(
                            &mut runs,
                            member,
                            &mut run,
                            &mut trace_slices,
                            true,
                            work,
                        )?;
                    }
                    if run.is_empty() {
                        run.reserve(2);
                        run.push(start);
                    }
                    if run.last().copied() != Some(end) {
                        run.push(end);
                        work.charge(1)?;
                        trace_slices.push((
                            source
                                .checked_sub(range.start)
                                .ok_or(ResourceError::SizeOverflow)?,
                            view.source_start.clone(),
                            view.source_end.clone(),
                        ));
                    }
                }
            }
            push_source_line_run(&mut runs, member, &mut run, &mut trace_slices, true, work)?;
        }
        if source_offset != views.source_ids.len() {
            return Err(GeometryErrorKind::Invalid(
                "line member source ranges omit partition source segments".into(),
            )
            .into());
        }
        Ok(runs)
    }

    fn emit_traced_source_line_shape<M: ChartMetric, C: RootCatalog, W: Work>(
        &self,
        source_kind: GeometryKind,
        runs: Vec<SourceLineRun>,
        source_traces: &[SourceTrace],
        by_node: &ResolvedNodePoints,
        roots: &C,
        root_positions: Option<&SubdivisionRootCatalog>,
        declared_axes: CoordinateAxes,
        strict: bool,
        context: &SourceArcContext<'_, '_, M>,
        work: &mut W,
    ) -> Result<SubdivisionPart> {
        if root_positions.is_none() {
            let node_count = runs.iter().try_fold(0_usize, |count, run| {
                count
                    .checked_add(run.nodes.len())
                    .ok_or(ResourceError::SizeOverflow)
            })?;
            work.charge(u64::try_from(node_count).map_err(|_| ResourceError::SizeOverflow)?)?;
            let mut line_nodes = Vec::with_capacity(node_count);
            for run in &runs {
                line_nodes.extend(run.nodes.iter().copied());
            }
            let line_axes = bucket_axes(
                line_nodes.iter().copied(),
                by_node,
                declared_axes,
                strict,
                "subdivide",
            )?;
            let coordinates = by_node.coordinates();
            let mut lines = Vec::with_capacity(runs.len());
            for run in &runs {
                let mut points = Vec::with_capacity(run.nodes.len());
                for &node in &run.nodes {
                    work.charge(1)?;
                    points.push(with_axes(
                        by_node.point(node, embedding::Embedding::xy_at(coordinates, node)),
                        line_axes,
                    ));
                }
                lines.push(LineSeq::from_proven(CoordSeq::try_from_points(&points)?));
            }
            if lines.is_empty() {
                return Ok(SubdivisionPart {
                    shape: Shape::typed_empty(source_kind, declared_axes),
                    traces: Some(Vec::new()),
                    polygon_traces: None,
                });
            }
            work.charge(u64::try_from(runs.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
            let mut traces = Vec::with_capacity(runs.len());
            for run in runs {
                let source_trace = source_traces.get(run.member).ok_or_else(|| {
                    GeometryErrorKind::Invalid("source line trace member is unknown".into())
                })?;
                traces.push(source_trace.compose_edges(&run.trace_slices, work)?);
            }
            return Ok(SubdivisionPart {
                shape: Shape::narrow_lines(line_axes, lines),
                traces: Some(traces),
                polygon_traces: None,
            });
        }

        let root_positions = root_positions.expect("ordinary trace path has its root catalog");
        let mut seam_samples = Vec::new();
        let mut traced_runs = Vec::with_capacity(runs.len());
        for run in runs {
            if run.nodes.len() != run.trace_slices.len().saturating_add(1) {
                return Err(GeometryErrorKind::Invalid(
                    "traced source line run vertices do not match its edge slices".into(),
                )
                .into());
            }
            let trace = source_traces.get(run.member).ok_or_else(|| {
                GeometryErrorKind::Invalid("source line trace member is unknown".into())
            })?;
            let mut vertices =
                Vec::with_capacity(run.nodes.len().saturating_add(run.trace_slices.len()));
            let first = *run.nodes.first().ok_or_else(|| {
                GeometryErrorKind::Invalid("traced source line run has no first vertex".into())
            })?;
            vertices.push(TracedSourceLineVertex::Node(first));
            let mut trace_slices = Vec::new();
            for (edge_index, (source_edge, start, end)) in run.trace_slices.iter().enumerate() {
                let cuts = self.source_line_seam_cuts(
                    trace,
                    *source_edge,
                    start,
                    end,
                    root_positions,
                    work,
                )?;
                let mut slice_start = start.clone();
                for cut in cuts {
                    trace_slices.push((
                        *source_edge,
                        slice_start,
                        cut.parameter.try_clone_with_work(work)?,
                    ));
                    let sample_index = seam_samples.len();
                    seam_samples.push(SourceLineSeamSample {
                        coordinate: cut.coordinate,
                        requests: cut.requests,
                    });
                    vertices.push(TracedSourceLineVertex::Seam(sample_index));
                    slice_start = cut.parameter;
                }
                trace_slices.push((*source_edge, slice_start, end.clone()));
                let node = *run.nodes.get(edge_index.wrapping_add(1)).ok_or_else(|| {
                    GeometryErrorKind::Invalid("source line trace end node is unknown".into())
                })?;
                vertices.push(TracedSourceLineVertex::Node(node));
            }
            traced_runs.push(TracedSourceLineRun {
                member: run.member,
                vertices,
                trace_slices,
            });
        }

        let seam_ordinates = if declared_axes.has_z() || declared_axes.has_m() {
            let mut request_rows = Vec::with_capacity(seam_samples.len());
            for sample in &mut seam_samples {
                request_rows.push(RequestRowInput {
                    axes: declared_axes,
                    requests: std::mem::take(&mut sample.requests),
                });
            }
            resolve_request_rows(&request_rows, roots, context.metric_access(), work)?
        } else {
            vec![NodeOrdinates::default(); seam_samples.len()]
        };
        if seam_ordinates.len() != seam_samples.len() {
            return Err(GeometryErrorKind::Invalid(
                "original-root seam resolution returned incomplete source ordinates".into(),
            )
            .into());
        }
        let node_count = traced_runs.iter().try_fold(0_usize, |count, run| {
            count
                .checked_add(
                    run.vertices
                        .iter()
                        .filter(|vertex| matches!(vertex, TracedSourceLineVertex::Node(_)))
                        .count(),
                )
                .ok_or(ResourceError::SizeOverflow)
        })?;
        work.charge(u64::try_from(node_count).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut line_nodes = Vec::with_capacity(node_count);
        for run in &traced_runs {
            for vertex in &run.vertices {
                if let TracedSourceLineVertex::Node(node) = vertex {
                    line_nodes.push(*node);
                }
            }
        }
        let mut line_axes = bucket_axes(
            line_nodes.iter().copied(),
            by_node,
            declared_axes,
            strict,
            "subdivide",
        )?;
        let mut has_z = line_axes.has_z();
        let mut has_m = line_axes.has_m();
        for ordinates in &seam_ordinates {
            has_z &= ordinates.z.value().is_some();
            has_m &= ordinates.m.value().is_some();
        }
        if strict && (has_z != declared_axes.has_z() || has_m != declared_axes.has_m()) {
            return Err(GeometryErrorKind::ordinate_dropped("subdivide"));
        }
        line_axes = CoordinateAxes::new(crate::geometry::HasZ(has_z), crate::geometry::HasM(has_m));

        let coordinates = by_node.coordinates();
        let mut lines = Vec::with_capacity(traced_runs.len());
        for run in &traced_runs {
            let mut points = Vec::with_capacity(run.vertices.len());
            for vertex in &run.vertices {
                work.charge(1)?;
                points.push(match *vertex {
                    TracedSourceLineVertex::Node(node) => with_axes(
                        by_node.point(node, embedding::Embedding::xy_at(coordinates, node)),
                        line_axes,
                    ),
                    TracedSourceLineVertex::Seam(sample_index) => {
                        let sample = seam_samples.get(sample_index).ok_or_else(|| {
                            GeometryErrorKind::Invalid("source seam sample is unknown".into())
                        })?;
                        let ordinates =
                            seam_ordinates.get(sample_index).copied().ok_or_else(|| {
                                GeometryErrorKind::Invalid(
                                    "source seam ordinate row is unknown".into(),
                                )
                            })?;
                        Point::new_raw_axes(
                            sample.coordinate.x,
                            sample.coordinate.y,
                            crate::geometry::ZOrdinate(
                                line_axes.has_z().then(|| ordinates.z.value()).flatten(),
                            ),
                            crate::geometry::MOrdinate(
                                line_axes.has_m().then(|| ordinates.m.value()).flatten(),
                            ),
                        )
                    },
                });
            }
            lines.push(LineSeq::from_proven(CoordSeq::try_from_points(&points)?));
        }
        if lines.is_empty() {
            return Ok(SubdivisionPart {
                shape: Shape::typed_empty(source_kind, declared_axes),
                traces: Some(Vec::new()),
                polygon_traces: None,
            });
        }
        work.charge(u64::try_from(traced_runs.len()).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut traces = Vec::with_capacity(traced_runs.len());
        for run in traced_runs {
            let source_trace = source_traces.get(run.member).ok_or_else(|| {
                GeometryErrorKind::Invalid("source line trace member is unknown".into())
            })?;
            traces.push(source_trace.compose_edges(&run.trace_slices, work)?);
        }
        Ok(SubdivisionPart {
            shape: Shape::narrow_lines(line_axes, lines),
            traces: Some(traces),
            polygon_traces: None,
        })
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "span IDs and their selection bits are produced from the same ExactPartition, so both indexed columns share its span domain"
    )]
    fn emit_source_line_shape(
        &self,
        partition: &ExactPartition<'_>,
        source_kind: GeometryKind,
        ranges: &[Range<usize>],
        views: &[Vec<(u32, bool)>],
        selected: &[bool],
        by_node: &ResolvedNodePoints,
        declared_axes: crate::geometry::CoordinateAxes,
        strict: bool,
    ) -> Result<Shape> {
        let mut node_runs = Vec::with_capacity(selected.len());
        let mut source_offset = 0;
        for range in ranges {
            debug_assert_eq!(range.start, source_offset);
            source_offset = range.end;
            let mut run = Vec::new();
            for source in range.clone() {
                for &(span_id, reversed) in views.get(source).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "exact subdivision line provenance lost a source segment".into(),
                    )
                })? {
                    if !selected[span_id as usize] {
                        if run.len() >= 2 {
                            node_runs.push(std::mem::take(&mut run));
                        } else {
                            run.clear();
                        }
                        continue;
                    }
                    let span = partition.spans[span_id as usize];
                    let (start, end) = if reversed {
                        (span.end, span.start)
                    } else {
                        (span.start, span.end)
                    };
                    if run.last().copied() != Some(start) && run.len() >= 2 {
                        node_runs.push(std::mem::take(&mut run));
                    }
                    if run.is_empty() {
                        run.reserve(2);
                        run.push(start);
                    }
                    if run.last().copied() != Some(end) {
                        run.push(end);
                    }
                }
            }
            if run.len() >= 2 {
                node_runs.push(run);
            }
        }

        let line_axes = bucket_axes(
            node_runs.iter().flatten().copied(),
            by_node,
            declared_axes,
            strict,
            "subdivide",
        )?;
        let coordinates = by_node.coordinates();
        let mut lines = Vec::with_capacity(node_runs.len());
        for run in node_runs {
            let mut points = Vec::with_capacity(run.len());
            for node in run {
                points.push(with_axes(
                    by_node.point(node, embedding::Embedding::xy_at(coordinates, node)),
                    line_axes,
                ));
            }
            lines.push(LineSeq::from_proven(CoordSeq::try_from_points(&points)?));
        }
        if lines.is_empty() {
            return Ok(Shape::typed_empty(source_kind, declared_axes));
        }
        Ok(Shape::narrow_lines(line_axes, lines))
    }

    fn coordinates_from_nodes(
        &self,
        nodes: &[NodeId],
        by_node: &ResolvedNodePoints,
        axes: CoordinateAxes,
    ) -> Result<CoordSeq> {
        let mut xs = Vec::with_capacity(nodes.len());
        let mut ys = Vec::with_capacity(nodes.len());
        let mut zs = axes.has_z().then(|| Vec::with_capacity(nodes.len()));
        let mut ms = axes.has_m().then(|| Vec::with_capacity(nodes.len()));
        for &node in nodes {
            let point = by_node.point(
                node,
                embedding::Embedding::xy_at(by_node.coordinates(), node),
            );
            xs.push(point.x);
            ys.push(point.y);
            if let Some(zs) = &mut zs {
                zs.push(point.z().expect("retained Z has a resolved ordinate"));
            }
            if let Some(ms) = &mut ms {
                ms.push(point.m().expect("retained M has a resolved ordinate"));
            }
        }
        CoordSeq::try_from_vecs(xs, ys, zs, ms)
    }

    fn node_lines_to_shape_resolved(
        &self,
        lines: Vec<Vec<NodeId>>,
        by_node: &ResolvedNodePoints,
        axes: CoordinateAxes,
    ) -> Result<Option<Shape>> {
        if lines.is_empty() {
            return Ok(None);
        }
        let mut output = Vec::with_capacity(lines.len());
        for line in lines {
            output.push(LineSeq::from_proven(
                self.coordinates_from_nodes(&line, by_node, axes)?,
            ));
        }
        Ok(Some(Shape::narrow_lines(axes, output)))
    }

    fn boundary_node_parts_to_shape_resolved(
        &self,
        parts: NodeParts,
        by_node: &ResolvedNodePoints,
        declared_axes: CoordinateAxes,
        has_area: bool,
        has_curve_support: bool,
        has_point_support: bool,
    ) -> Result<Shape> {
        let point_axes = bucket_axes(
            parts.points.iter().copied(),
            by_node,
            declared_axes,
            false,
            "boundary",
        )?;
        let line_axes = bucket_axes(
            parts.lines.iter().flatten().copied(),
            by_node,
            declared_axes,
            false,
            "boundary",
        )?;
        let points = if parts.points.is_empty() {
            None
        } else {
            Some(Shape::multi_point(
                point_axes,
                self.coordinates_from_nodes(&parts.points, by_node, point_axes)?,
            ))
        };
        let lines = self.node_lines_to_shape_resolved(parts.lines, by_node, line_axes)?;

        if has_area {
            let lines = lines.unwrap_or_else(|| {
                Shape::typed_empty(GeometryKind::MultiLineString, declared_axes)
            });
            return match points {
                Some(points) => Shape::geometry_collection(vec![lines, points]),
                None => Ok(lines),
            };
        }
        if has_curve_support {
            return Ok(points
                .unwrap_or_else(|| Shape::typed_empty(GeometryKind::MultiPoint, declared_axes)));
        }
        if has_point_support {
            return Ok(Shape::typed_empty(
                GeometryKind::GeometryCollection,
                declared_axes,
            ));
        }
        Ok(Shape::typed_empty(
            GeometryKind::MultiLineString,
            declared_axes,
        ))
    }

    fn node_parts_to_shape_resolved(
        &self,
        parts: NodeParts,
        embedding: &embedding::Embedding<'_>,
        by_node: &ResolvedNodePoints,
        declared_axes: crate::geometry::CoordinateAxes,
        strict: bool,
        operation: &str,
        polygon_publication: Option<(
            crate::geometry::PolygonPartEdgeRoles,
            crate::geographic::GeographicFrame,
            &mut ExpansionBudget,
        )>,
    ) -> Result<Shape> {
        let mut polygon_publication = polygon_publication;
        if !declared_axes.has_z() && !declared_axes.has_m() {
            let xy = embedding.coordinates();
            let mut points = Vec::with_capacity(parts.points.len());
            for node in parts.points {
                let point = embedding::Embedding::xy_at(xy, node);
                points.push(Point::new_raw_xy(point.x, point.y));
            }
            let mut lines = Vec::with_capacity(parts.lines.len());
            for line in parts.lines {
                lines.push(xy_sequence_for_nodes(&line, xy)?);
            }
            let mut polygons = polygons_from_xy_nodes(parts.polygons, xy)?;
            if let Some((roles, frame, budget)) = polygon_publication.take() {
                polygons = crate::geometry::publish_polygon_parts_with_edge_roles(
                    polygons, &roles, frame, budget,
                )?;
            }
            return assemble_node_parts(
                points,
                lines,
                polygons,
                crate::geometry::CoordinateAxes::XY,
                crate::geometry::CoordinateAxes::XY,
                crate::geometry::CoordinateAxes::XY,
            );
        }

        let point_axes = bucket_axes(
            parts.points.iter().copied(),
            by_node,
            declared_axes,
            strict,
            operation,
        )?;
        let line_axes = bucket_axes(
            parts.lines.iter().flatten().copied(),
            by_node,
            declared_axes,
            strict,
            operation,
        )?;
        let polygon_axes = bucket_axes(
            parts
                .polygons
                .iter()
                .flat_map(|polygon| polygon.iter().flatten().copied()),
            by_node,
            declared_axes,
            strict,
            operation,
        )?;

        let xy = by_node.coordinates();
        let mut points = Vec::with_capacity(parts.points.len());
        for node in parts.points {
            points.push(with_axes(
                by_node.point(node, embedding::Embedding::xy_at(xy, node)),
                point_axes,
            ));
        }
        let mut lines = Vec::with_capacity(parts.lines.len());
        for line in parts.lines {
            lines.push(self.coordinates_from_nodes(&line, by_node, line_axes)?);
        }
        let mut polygons = Vec::with_capacity(parts.polygons.len());
        for polygon in parts.polygons {
            let mut rings = Vec::with_capacity(polygon.len());
            for ring in polygon {
                rings.push(Ring::try_from_closed(self.coordinates_from_nodes(
                    &ring,
                    by_node,
                    polygon_axes,
                )?)?);
            }
            let mut ring_iter = rings.into_iter();
            let shell = ring_iter.next().expect("polygon has a shell");
            let mut holes = Vec::with_capacity(ring_iter.size_hint().0);
            holes.extend(ring_iter);
            polygons.push(Polygon::new(shell, holes));
        }
        if let Some((roles, frame, budget)) = polygon_publication.take() {
            polygons = crate::geometry::publish_polygon_parts_with_edge_roles(
                polygons, &roles, frame, budget,
            )?;
        }
        assemble_node_parts(points, lines, polygons, point_axes, line_axes, polygon_axes)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "topology IDs and CSR rows are built from this partition; CellFacts admission checked each row-major product and both operand IDs are validated by subdivision_parts"
    )]
    fn select_subdivision_cells(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
        global: &GlobalFaces,
        facts: &CellFacts,
        source_operand: usize,
        clip_operand: usize,
    ) -> ArealCells {
        let operands = self.operand_count();
        let mut cells = ArealCells {
            faces: Vec::new(),
            spans: Vec::new(),
            nodes: Vec::new(),
        };
        cells.faces.reserve(global.count as usize);
        let source_faces = facts
            .face_membership
            .iter()
            .skip(source_operand)
            .step_by(operands);
        let clip_faces = facts
            .face_membership
            .iter()
            .skip(clip_operand)
            .step_by(operands);
        cells.faces.extend(
            source_faces
                .zip(clip_faces)
                .map(|(&source, &clip)| source && clip),
        );
        cells.spans.reserve(partition.spans.len());
        let source_spans = facts
            .span_support
            .iter()
            .skip(source_operand)
            .step_by(operands);
        let clip_spans = facts
            .span_support
            .iter()
            .skip(clip_operand)
            .step_by(operands);
        cells.spans.extend(
            source_spans
                .zip(clip_spans)
                .map(|(&source, &clip)| source && clip),
        );
        cells.nodes.reserve(partition.node_count());
        cells.nodes.resize(partition.node_count(), false);
        for (span, &selected) in partition.spans.iter().zip(&cells.spans) {
            if selected {
                cells.nodes[span.start.0 as usize] = true;
                cells.nodes[span.end.0 as usize] = true;
            }
        }

        let source_points = facts
            .point_membership
            .iter()
            .skip(source_operand)
            .step_by(operands);
        let clip_points = facts
            .point_membership
            .iter()
            .skip(clip_operand)
            .step_by(operands);
        let face_membership = |face: u32, operand: usize| {
            let offset = (face as usize).wrapping_mul(operands).wrapping_add(operand);
            facts.face_membership[offset]
        };
        for (((selected, starts), &component), (&source_point, &clip_point)) in cells
            .nodes
            .iter_mut()
            .zip(topology.starts.array_windows::<2>())
            .zip(&topology.component_of)
            .zip(source_points.zip(clip_points))
        {
            let &[start, end] = starts;
            let range = start as usize..end as usize;
            let memberships = |operand: usize, point_membership: bool| {
                let face_membership = if range.is_empty() {
                    let host = global.component_hosts[component as usize];
                    face_membership(host, operand)
                } else {
                    range.clone().any(|slot| {
                        let face = global.walk_faces[topology.face_of[slot] as usize];
                        face_membership(face, operand)
                    })
                };
                face_membership || point_membership
            };
            *selected |=
                memberships(source_operand, source_point) && memberships(clip_operand, clip_point);
        }
        cells
    }

    pub(in crate::geometry) fn repair_polygons<M: ChartMetric>(
        &self,
        partition: &ExactPartition<'_>,
        method: RepairMethod,
        declared_axes: CoordinateAxes,
        strict: bool,
        context: &SourceArcContext<'_, '_, M>,
    ) -> Result<Vec<Shape>> {
        let (topology, edges) = partition.local_topology()?;
        let global = partition.global_faces(&topology, &edges)?;
        let cells = match method {
            RepairMethod::Linework => {
                let winding_rings = self.winding_source_rings();
                self.select_cells_with_winding_rings(
                    partition,
                    &topology,
                    &edges,
                    &global,
                    crate::geometry::OverlayOp::Union,
                    true,
                    &winding_rings,
                )?
            },
            RepairMethod::Structure => self.select_cells(
                partition,
                &topology,
                &edges,
                &global,
                crate::geometry::OverlayOp::Union,
                false,
            )?,
        };
        let protected = self.ordinate_protected_nodes(partition, declared_axes)?;
        let plan = cells.emission_plan_with_protected(
            partition,
            &topology,
            &edges,
            &global,
            protected.as_deref(),
            None,
        )?;
        let emission = plan.emission(&topology, &edges);
        let embedding = partition.embed(&[&emission], "repair")?;
        let parts = plan.build_node_parts(partition, &topology, &emission)?;
        let mut groups = Vec::with_capacity(parts.polygons.len());
        for rings in parts.polygons {
            let mut ring_iter = rings.into_iter();
            let shell = ring_iter.next().expect("repair polygon has a shell");
            let Some(shell) = remove_node_antennas(shell) else {
                continue;
            };
            let mut polygon = Vec::with_capacity(ring_iter.size_hint().0.wrapping_add(1));
            polygon.push(shell);
            for hole in ring_iter {
                if let Some(hole) = remove_node_antennas(hole) {
                    polygon.push(hole);
                }
            }
            groups.push(polygon);
        }
        self.polygon_shapes_from_node_groups(
            partition,
            &embedding,
            groups,
            declared_axes,
            strict,
            "repair",
            context,
            None,
        )
    }

    fn overlay_parts_with_fill(
        &self,
        partition: &ExactPartition<'_>,
        operation: crate::geometry::OverlayOp,
        embedding_operation: &'static str,
    ) -> Result<(Vec<Point>, Vec<CoordSeq>, Vec<Polygon>)> {
        let (topology, edges) = partition.local_topology()?;
        let global = partition.global_faces(&topology, &edges)?;
        let cells = self.select_cells(partition, &topology, &edges, &global, operation, false)?;
        let plan = cells.emission_plan(partition, &topology, &edges, &global)?;
        plan.emit_xy_parts(partition, &topology, &edges, embedding_operation)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "face and dart IDs are produced by the checked local/global topology built from this partition"
    )]
    pub(in crate::geometry) fn linework_polygons<M: ChartMetric>(
        &self,
        partition: &mut ExactPartition<'_>,
        build_area: bool,
        operation: &'static str,
        declared_axes: CoordinateAxes,
        strict: bool,
        context: &SourceArcContext<'_, '_, M>,
    ) -> Result<Vec<Shape>> {
        partition.kept_nodes = KeptNodes::All;
        let (topology, edges) = partition.local_topology()?;
        let global = partition.global_faces(&topology, &edges)?;
        if !build_area {
            let emission = polygonize_emission(partition, &topology, &edges, &global)?;
            let embedding = partition.embed(&[&emission], operation)?;
            let groups =
                emission::polygonize_node_groups(partition, &topology, &global, &embedding)?;
            return self.polygon_shapes_from_node_groups(
                partition,
                &embedding,
                groups,
                declared_axes,
                strict,
                operation,
                context,
                None,
            );
        }
        let mut cycles: Vec<Vec<Vec<NodeId>>> = Vec::with_capacity(global.count as usize);
        cycles.resize_with(global.count as usize, Vec::new);
        for (walk, &[start, end]) in topology.face_starts.array_windows::<2>().enumerate() {
            let (start, end) = (start as usize, end as usize);
            let mut cycle = Vec::with_capacity(end.saturating_sub(start));
            for &slot in &topology.face_slots[start..end] {
                cycle.push(NodeId(topology.owners[slot as usize]));
            }
            let face = global.walk_faces[walk] as usize;
            cycles[face].push(cycle);
        }
        let mut samples = Vec::with_capacity(global.count as usize);
        samples.resize(global.count as usize, None);
        for (face_cycles, sample_slot) in cycles.iter().zip(&mut samples).skip(1) {
            for &node in face_cycles.iter().flatten() {
                let earlier = match *sample_slot {
                    None => true,
                    Some(prior) => partition.compare_nodes(node, prior)?.is_lt(),
                };
                if earlier {
                    *sample_slot = Some(node);
                }
            }
        }
        // A face's nesting depth counts the other faces whose cycles contain
        // its sample. Rounding is monotone, so a cycle can contain the sample
        // only if the sample's rounded coordinates lie within the rounded
        // bounds of the cycle's nodes; samples sorted by x visit only those.
        let mut by_x = Vec::with_capacity(global.count as usize);
        for (face, sample) in samples.iter().enumerate() {
            if let Some(sample) = sample {
                by_x.push((partition.nearest_xy(*sample), face, *sample));
            }
        }
        by_x.sort_unstable_by(|left, right| left.0.x.total_cmp(&right.0.x));
        let mut depths = vec![0_u32; global.count as usize];
        for (other, other_cycles) in cycles.iter().enumerate().skip(1) {
            let Some([min_x, min_y, max_x, max_y]) = other_cycles
                .iter()
                .flatten()
                .map(|&node| partition.nearest_xy(node))
                .map(|xy| [xy.x, xy.y, xy.x, xy.y])
                .reduce(|[a, b, c, d], [x, y, z, w]| [a.min(x), b.min(y), c.max(z), d.max(w)])
            else {
                continue;
            };
            let first = by_x.partition_point(|(xy, ..)| xy.x < min_x);
            for &(xy, face, sample) in by_x.iter().skip(first) {
                if xy.x > max_x {
                    break;
                }
                if face == other || xy.y < min_y || xy.y > max_y {
                    continue;
                }
                for cycle in other_cycles {
                    if cycle_contains_node(partition, cycle, sample)? {
                        depths[face] = depths[face].wrapping_add(1);
                        break;
                    }
                }
            }
        }
        let mut selected = Vec::with_capacity(global.count as usize);
        selected.extend(
            samples
                .iter()
                .zip(&depths)
                .enumerate()
                .map(|(face, (sample, &depth))| {
                    face != 0 && sample.is_some() && depth.is_multiple_of(2)
                }),
        );
        let cells = ArealCells {
            faces: selected,
            spans: {
                let mut values = Vec::with_capacity(partition.spans.len());
                values.resize(partition.spans.len(), false);
                values
            },
            nodes: {
                let mut values = Vec::with_capacity(partition.node_count());
                values.resize(partition.node_count(), false);
                values
            },
        };
        let plan = cells.emission_plan(partition, &topology, &edges, &global)?;
        let emission = plan.emission(&topology, &edges);
        let embedding = partition.embed(&[&emission], operation)?;
        let parts = plan.build_node_parts(partition, &topology, &emission)?;
        debug_assert!(parts.points.is_empty() && parts.lines.is_empty());
        self.polygon_shapes_from_node_groups(
            partition,
            &embedding,
            parts.polygons,
            declared_axes,
            strict,
            operation,
            context,
            None,
        )
    }

    fn polygon_shapes_from_node_groups<M: ChartMetric>(
        &self,
        partition: &ExactPartition<'_>,
        embedding: &embedding::Embedding<'_>,
        groups: Vec<Vec<Vec<NodeId>>>,
        declared_axes: CoordinateAxes,
        strict: bool,
        operation: &'static str,
        context: &SourceArcContext<'_, '_, M>,
        source_scope: Option<u32>,
    ) -> Result<Vec<Shape>> {
        let by_node = self.resolve_node_points(
            partition,
            embedding,
            groups
                .iter()
                .flat_map(|polygon| polygon.iter().flatten().copied()),
            declared_axes,
            context,
            source_scope,
        )?;
        let mut output = Vec::with_capacity(groups.len());
        for polygon in groups {
            let axes = bucket_axes(
                polygon.iter().flatten().copied(),
                &by_node,
                declared_axes,
                strict,
                operation,
            )?;
            let mut rings = Vec::with_capacity(polygon.len());
            for mut nodes in polygon {
                if let Some(&first) = nodes.first()
                    && nodes.last() != Some(&first)
                {
                    nodes.push(first);
                }
                rings.push(Ring::try_from_closed(
                    self.coordinates_from_nodes(&nodes, &by_node, axes)?,
                )?);
            }
            let mut rings = rings.into_iter();
            let shell = rings.next().expect("polygon node group has a shell");
            let mut holes = Vec::with_capacity(rings.size_hint().0);
            holes.extend(rings);
            output.push(Shape::Polygon(Polygon::new(shell, holes)));
        }
        Ok(output)
    }

    fn diagnostic_shapes_for_rows<M: ChartMetric>(
        &self,
        partition: &ExactPartition<'_>,
        embedding: &embedding::Embedding<'_>,
        rows: Vec<Vec<(usize, bool)>>,
        row_axes: &[CoordinateAxes],
        strict: bool,
        context: &SourceArcContext<'_, '_, M>,
    ) -> Result<Vec<Vec<Shape>>> {
        let mut output = Vec::with_capacity(rows.len());
        for (row, spans) in rows.into_iter().enumerate() {
            let declared_axes = *row_axes.get(row).ok_or_else(|| {
                GeometryErrorKind::Invalid("polygonize diagnostic row has no source axes".into())
            })?;
            let mut lines = Vec::with_capacity(spans.len());
            for (span, closed) in spans {
                let span = partition.spans.get(span).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "polygonize diagnostic references an unknown source span".into(),
                    )
                })?;
                let mut line_nodes = Vec::with_capacity(if closed { 3 } else { 2 });
                line_nodes.extend([span.start, span.end]);
                if closed {
                    line_nodes.push(span.start);
                }
                lines.push(line_nodes);
            }
            let source_scope = Some(u32::try_from(row).map_err(|_| ResourceError::SizeOverflow)?);
            let by_node = self.resolve_node_points(
                partition,
                embedding,
                lines.iter().flatten().copied(),
                declared_axes,
                context,
                source_scope,
            )?;
            let mut diagnostics = Vec::with_capacity(lines.len());
            for line_nodes in lines {
                let axes = bucket_axes(
                    line_nodes.iter().copied(),
                    &by_node,
                    declared_axes,
                    strict,
                    "polygonize_full",
                )?;
                diagnostics.push(Shape::LineString(LineSeq::from_proven(
                    self.coordinates_from_nodes(&line_nodes, &by_node, axes)?,
                )));
            }
            diagnostics.sort_unstable_by(compare_diagnostic_lines);
            output.push(diagnostics);
        }
        Ok(output)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "face IDs yielded by visit_face_windings index the selected vector sized from GlobalFaces"
    )]
    pub(in crate::geometry) fn winding_polygons(
        &self,
        partition: &ExactPartition<'_>,
        keep: impl Fn(i64) -> bool,
    ) -> Result<Vec<Polygon>> {
        let (topology, edges) = partition.local_topology()?;
        let global = partition.global_faces(&topology, &edges)?;
        let winding_rings = self.winding_source_rings();
        let mut selected = Vec::with_capacity(global.count as usize);
        selected.resize(global.count as usize, false);
        partition.visit_face_windings(
            &topology,
            &edges,
            &global,
            &winding_rings,
            self.ring_count,
            |face, winding| {
                selected[face as usize] = keep(winding.iter().sum());
                Ok(())
            },
        )?;
        self.emit_selected_winding_polygons(
            partition, &topology, &edges, &global, selected, "buffer",
        )
    }

    /// Select the union of independently wound polygon groups from one exact
    /// source partition. Each range contains one polygon's canonical shell
    /// followed by its canonical holes; a face belongs when any group's
    /// signed winding is negative.
    #[expect(
        clippy::indexing_slicing,
        reason = "face IDs yielded by visit_face_windings index the selected vector sized from GlobalFaces"
    )]
    pub(in crate::geometry) fn winding_group_union_polygons(
        &self,
        partition: &ExactPartition<'_>,
        groups: &[Range<usize>],
    ) -> Result<Vec<Polygon>> {
        let mut cursor = 0_usize;
        for group in groups {
            if group.start != cursor || group.start >= group.end || group.end > self.ring_count {
                return Err(GeometryErrorKind::Invalid(
                    "exact winding polygon groups are not contiguous".into(),
                )
                .into());
            }
            cursor = group.end;
        }
        if cursor != self.ring_count {
            return Err(GeometryErrorKind::Invalid(
                "exact winding polygon groups do not cover all rings".into(),
            )
            .into());
        }
        let (topology, edges) = partition.local_topology()?;
        let global = partition.global_faces(&topology, &edges)?;
        let winding_rings = self.winding_source_rings();
        let mut selected = Vec::with_capacity(global.count as usize);
        selected.resize(global.count as usize, false);
        partition.visit_face_windings(
            &topology,
            &edges,
            &global,
            &winding_rings,
            self.ring_count,
            |face, winding| {
                let mut occupied = false;
                for group in groups {
                    let mut sum = 0_i64;
                    for index in group.clone() {
                        let value = *winding.get(index).ok_or_else(|| {
                            GeometryErrorKind::Invalid(
                                "exact winding group references an unknown ring".into(),
                            )
                        })?;
                        sum = sum.checked_add(value).ok_or(ResourceError::SizeOverflow)?;
                    }
                    occupied |= sum <= -1;
                    if occupied {
                        break;
                    }
                }
                selected[face as usize] = occupied;
                Ok(())
            },
        )?;
        self.emit_selected_winding_polygons(
            partition, &topology, &edges, &global, selected, "buffer",
        )
    }

    fn emit_selected_winding_polygons(
        &self,
        partition: &ExactPartition<'_>,
        topology: &IndexedTopology,
        edges: &[u32],
        global: &GlobalFaces,
        selected: Vec<bool>,
        operation: &'static str,
    ) -> Result<Vec<Polygon>> {
        let cells = ArealCells {
            faces: selected,
            spans: {
                let mut values = Vec::with_capacity(partition.spans.len());
                values.resize(partition.spans.len(), false);
                values
            },
            nodes: {
                let mut values = Vec::with_capacity(partition.node_count());
                values.resize(partition.node_count(), false);
                values
            },
        };
        let plan = cells.emission_plan(partition, topology, edges, global)?;
        let (_, _, polygons) = plan.emit_xy_parts(partition, topology, edges, operation)?;
        Ok(polygons)
    }
}

pub(crate) fn occupied_cartesian_area(shape: &Shape) -> Result<f64> {
    let Some(inventory) = ExactSourceInventory::from_occupied_area_shape(shape)? else {
        return Ok(0.0);
    };
    let mut budget = ExpansionBudget::new("area", "exact partition");
    let partition = inventory.partition(&mut budget)?;
    inventory.occupied_cartesian_area(&partition)
}

pub(crate) fn occupied_cartesian_support(shape: &Shape) -> Result<Shape> {
    let inventory = ExactSourceInventory::from_shapes(&[shape])?;
    let mut budget = ExpansionBudget::new("area", "exact partition");
    let partition = inventory.partition(&mut budget)?;
    inventory.occupied_cartesian_support(&partition)
}

#[cfg(test)]
pub(crate) fn cartesian_support_nonempty(shape: &Shape) -> Result<bool> {
    let inventory = ExactSourceInventory::from_shapes(&[shape])?;
    let mut budget = ExpansionBudget::new("support", "exact partition");
    let partition = inventory.partition(&mut budget)?;
    let (topology, edges) = partition.local_topology()?;
    let global = partition.global_faces(&topology, &edges)?;
    let cells = inventory.select_cells(
        &partition,
        &topology,
        &edges,
        &global,
        crate::geometry::OverlayOp::Union,
        false,
    )?;
    Ok(cells.faces.iter().any(|&value| value)
        || cells.spans.iter().any(|&value| value)
        || cells.nodes.iter().any(|&value| value))
}

#[expect(
    clippy::indexing_slicing,
    reason = "the only indirect access is by a span ID produced by the same admitted partition"
)]
pub(crate) fn exact_intersects_bounds(shape: &Shape, bounds: Bounds) -> Result<bool> {
    let inventory = ExactSourceInventory::from_shapes(&[shape])?;
    let mut budget = ExpansionBudget::new("intersects", "exact partition");
    let partition = inventory.partition(&mut budget)?;
    for [x, y] in bounds.corners() {
        if crate::geometry::cartesian_point_covers(shape, Point::new_raw_xy(x, y)) {
            return Ok(true);
        }
    }
    let (topology, edges) = partition.local_topology()?;
    let global = partition.global_faces(&topology, &edges)?;
    let cells = inventory.select_cells(
        &partition,
        &topology,
        &edges,
        &global,
        crate::geometry::OverlayOp::Union,
        false,
    )?;
    for (index, &selected) in cells.nodes.iter().enumerate() {
        if selected && partition.node_in_bounds(NodeId(index as u32), bounds)? {
            return Ok(true);
        }
    }
    let [[x0, y0], [x1, y1], [x2, y2], [x3, y3]] = bounds.corners();
    let rectangle = [
        Segment {
            start: XY::new(x0, y0),
            end: XY::new(x1, y1),
        },
        Segment {
            start: XY::new(x1, y1),
            end: XY::new(x2, y2),
        },
        Segment {
            start: XY::new(x2, y2),
            end: XY::new(x3, y3),
        },
        Segment {
            start: XY::new(x3, y3),
            end: XY::new(x0, y0),
        },
    ];
    for (&span, &selected) in partition.spans.iter().zip(&cells.spans) {
        if !selected {
            continue;
        }
        let segment = partition.unique[span.unique as usize];
        if bounds.contains_point(segment.start)
            || bounds.contains_point(segment.end)
            || rectangle
                .iter()
                .copied()
                .any(|candidate| segment_contact_exact(segment, candidate) != Contact::Disjoint)
        {
            return Ok(true);
        }
    }
    Ok(false)
}

fn compare_diagnostic_lines(left: &Shape, right: &Shape) -> Ordering {
    let key = |shape: &Shape| {
        let Shape::LineString(line) = shape else {
            unreachable!("polygonize diagnostics are line strings")
        };
        (
            PointKey::new(line.first().expect("diagnostic line has a start")),
            PointKey::new(line.point_at(1)),
        )
    };
    key(left).cmp(&key(right))
}

/// Exact source components before partition deduplication.
///
/// Occurrence-preserving ingress keeps every component occurrence, including
/// members reached through shared collection owners; set ingress can fold
/// repeated shared owners before they enter this buffer.
pub(in crate::geometry) struct StagedExactSources {
    segments: Vec<Segment>,
    source_points: Option<Vec<SourceArcRecord>>,
    /// Source endpoint Z/M values are unnecessary for XY-only consumers.
    xy_only: bool,
    source_operands: Vec<u32>,
    source_rings: Vec<Option<u32>>,
    ring_kinds: Vec<RingKind>,
    ring_polygons: Vec<u32>,
    ring_holes: Vec<bool>,
    polygon_count: usize,
    polygon_operands: Vec<u32>,
    polygon_ring_ranges: Vec<Range<usize>>,
    polygon_rings: Vec<Vec<Range<usize>>>,
    ring_count: usize,
    isolated: Vec<XY>,
    isolated_points: Vec<Point>,
    isolated_operands: Vec<u32>,
    line_boundaries: Vec<(u32, PointKey)>,
    relation_components: Option<Box<RelationComponents>>,
}

#[derive(Default)]
struct RelationComponents {
    components: Vec<StagedSourceComponent>,
    source_order: Vec<StagedRelationItem>,
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

impl StagedExactSources {
    fn new(operand_count: usize) -> Self {
        Self::with_xy_only(operand_count, false)
    }

    fn with_xy_only(operand_count: usize, xy_only: bool) -> Self {
        let mut polygon_rings = Vec::with_capacity(operand_count);
        polygon_rings.resize_with(operand_count, Vec::new);
        Self {
            segments: Vec::new(),
            source_points: None,
            xy_only,
            source_operands: Vec::new(),
            source_rings: Vec::new(),
            ring_kinds: Vec::new(),
            ring_polygons: Vec::new(),
            ring_holes: Vec::new(),
            polygon_count: 0,
            polygon_operands: Vec::new(),
            polygon_ring_ranges: Vec::new(),
            polygon_rings,
            ring_count: 0,
            isolated: Vec::new(),
            isolated_points: Vec::new(),
            isolated_operands: Vec::new(),
            line_boundaries: Vec::new(),
            relation_components: None,
        }
    }

    fn from_rows<'a>(rows: impl ExactSizeIterator<Item = storage::ShapeRow<'a>>) -> Result<Self> {
        Self::stage_rows(
            rows,
            false,
            |_| {},
            |staged, row, operand| {
                let mut owners = SharedShapeOwners::for_collections();
                staged.append_row(row, operand, &mut owners)
            },
        )
    }

    /// Stage every source occurrence. XY-only consumers can skip retaining
    /// Z/M endpoint columns while keeping the same source walk and XY data.
    pub(in crate::geometry) fn from_rows_with_occurrences<'a>(
        rows: impl ExactSizeIterator<Item = storage::ShapeRow<'a>>,
        xy_only: bool,
    ) -> Result<Self> {
        Self::stage_rows(rows, xy_only, |_| {}, Self::append_occurrence_row)
    }

    /// Relation staging keeps each component boundary without partition
    /// deduplication, including closed lines with no endpoint occurrences.
    pub(in crate::geometry) fn from_rows_for_relation<'a>(
        rows: impl ExactSizeIterator<Item = storage::ShapeRow<'a>>,
    ) -> Result<Self> {
        Self::stage_rows(
            rows,
            true,
            |staged| staged.relation_components = Some(Box::default()),
            Self::append_occurrence_row,
        )
    }

    fn append_occurrence_row(
        staged: &mut Self,
        row: storage::ShapeRow<'_>,
        operand: u32,
    ) -> Result<()> {
        match row {
            storage::ShapeRow::Shape(shape) => staged.append_shape_with_occurrences(shape, operand),
            storage::ShapeRow::Handle(plan) => {
                staged.append_shape_with_occurrences(plan.shape(), operand)
            },
            row @ (storage::ShapeRow::Point(_) | storage::ShapeRow::Packed(_)) => {
                staged.append_row(row, operand, &mut SharedShapeOwners::for_collections())
            },
        }
    }

    fn stage_rows<'a>(
        rows: impl ExactSizeIterator<Item = storage::ShapeRow<'a>>,
        xy_only: bool,
        initialize: impl FnOnce(&mut Self),
        mut append: impl FnMut(&mut Self, storage::ShapeRow<'a>, u32) -> Result<()>,
    ) -> Result<Self> {
        let mut staged = Self::with_xy_only(rows.len(), xy_only);
        initialize(&mut staged);
        for (operand, row) in rows.enumerate() {
            let operand = u32::try_from(operand).map_err(|_| ResourceError::SizeOverflow)?;
            append(&mut staged, row, operand)?;
        }
        Ok(staged)
    }

    pub(in crate::geometry) fn segments(&self) -> &[Segment] {
        &self.segments
    }

    #[cfg(test)]
    pub(in crate::geometry) fn source_operands(&self) -> &[u32] {
        &self.source_operands
    }

    #[cfg(test)]
    pub(in crate::geometry) fn source_rings(&self) -> &[Option<u32>] {
        &self.source_rings
    }

    pub(in crate::geometry) fn ring_kinds(&self) -> &[RingKind] {
        &self.ring_kinds
    }

    #[cfg(test)]
    pub(in crate::geometry) fn ring_polygons(&self) -> &[u32] {
        &self.ring_polygons
    }

    #[cfg(test)]
    pub(in crate::geometry) fn ring_holes(&self) -> &[bool] {
        &self.ring_holes
    }

    #[cfg(test)]
    pub(in crate::geometry) fn polygon_operands(&self) -> &[u32] {
        &self.polygon_operands
    }

    #[cfg(test)]
    pub(in crate::geometry) fn polygon_ring_ranges(&self) -> &[Range<usize>] {
        &self.polygon_ring_ranges
    }

    #[cfg(test)]
    pub(in crate::geometry) fn polygon_rings(&self) -> &[Vec<Range<usize>>] {
        &self.polygon_rings
    }

    pub(in crate::geometry) fn isolated(&self) -> &[XY] {
        &self.isolated
    }

    #[cfg(test)]
    pub(in crate::geometry) fn isolated_operands(&self) -> &[u32] {
        &self.isolated_operands
    }

    #[cfg(test)]
    pub(in crate::geometry) fn line_boundaries(&self) -> &[(u32, PointKey)] {
        &self.line_boundaries
    }

    pub(in crate::geometry) fn relation_components(&self) -> Option<&[StagedSourceComponent]> {
        self.relation_components
            .as_deref()
            .map(|layout| layout.components.as_slice())
    }

    pub(in crate::geometry) fn relation_source_order(&self) -> Option<&[StagedRelationItem]> {
        self.relation_components
            .as_deref()
            .map(|layout| layout.source_order.as_slice())
    }

    fn append_row(
        &mut self,
        row: storage::ShapeRow<'_>,
        operand: u32,
        owners: &mut SharedShapeOwners,
    ) -> Result<()> {
        use crate::geometry::storage::{PackedRow, ShapeRow};
        match row {
            ShapeRow::Handle(plan) => self.append_shape(plan.shape(), operand, owners),
            ShapeRow::Shape(shape) => self.append_shape(shape, operand, owners),
            ShapeRow::Point(point) => {
                self.append_isolated_point(point, operand);
                Ok(())
            },
            ShapeRow::Packed(PackedRow::Points {
                coords, physical, ..
            }) => {
                self.append_isolated_point(coords.point_at(physical), operand);
                Ok(())
            },
            ShapeRow::Packed(row @ PackedRow::Lines { coords, .. }) => {
                let coords = crate::geometry::storage::row::CoordRow::new(
                    coords,
                    row.vertex_window().expect("line row has a vertex window"),
                );
                self.append_line_coords(&coords, operand);
                Ok(())
            },
            ShapeRow::Packed(row @ PackedRow::Polygons { coords, .. }) => {
                let rings = row.polygon_rings().expect("polygon row has a ring window");
                if rings.is_empty() {
                    return Ok(());
                }
                self.append_polygon_rings(
                    rings.map(|ring| {
                        crate::geometry::storage::row::CoordRow::new(
                            coords,
                            row.polygon_ring_window(ring)
                                .expect("admitted polygon ring"),
                        )
                    }),
                    operand,
                )
            },
        }
    }

    fn append_shape(
        &mut self,
        shape: &Shape,
        operand: u32,
        owners: &mut SharedShapeOwners,
    ) -> Result<()> {
        if let Shape::GeometryCollection(collection) = shape {
            if owners.first_collection(collection.as_slice(), collection.members_are_shared()) {
                for member in collection {
                    self.append_shape(member, operand, owners)?;
                }
            }
            return Ok(());
        }
        self.append_shape_with_occurrences(shape, operand)
    }

    fn append_shape_with_occurrences(&mut self, shape: &Shape, operand: u32) -> Result<()> {
        shape.try_for_each_component(&mut |component| self.append_component(component, operand))
    }

    fn append_component(
        &mut self,
        component: crate::geometry::Component<'_>,
        operand: u32,
    ) -> Result<()> {
        match component {
            crate::geometry::Component::Point(point) => {
                self.append_isolated_point(point, operand);
                Ok(())
            },
            crate::geometry::Component::LineString(line) => {
                self.append_line_coords(line.as_coords(), operand);
                Ok(())
            },
            crate::geometry::Component::Polygon(polygon) => self.append_polygon(polygon, operand),
        }
    }

    fn append_occupied_area_shape(
        &mut self,
        shape: &Shape,
        operand: u32,
        collection_owners: &mut SharedShapeOwners,
    ) -> Result<()> {
        match shape {
            Shape::Point(..)
            | Shape::MultiPoint(..)
            | Shape::LineString(..)
            | Shape::MultiLineString(..)
            | Shape::Empty(..) => {},
            Shape::Polygon(polygon) => self.append_polygon(polygon, operand)?,
            Shape::MultiPolygon(polygons) => {
                for polygon in polygons {
                    self.append_polygon(polygon, operand)?;
                }
            },
            Shape::GeometryCollection(collection) => {
                let members = collection.as_slice();
                if !collection_owners.first_collection(members, collection.members_are_shared()) {
                    return Ok(());
                }
                if !members.iter().any(|member| {
                    matches!(
                        member,
                        Shape::Polygon(..)
                            | Shape::MultiPolygon(..)
                            | Shape::GeometryCollection(..)
                    )
                }) {
                    return Ok(());
                }
                for member in members {
                    self.append_occupied_area_shape(member, operand, collection_owners)?;
                }
            },
        }
        Ok(())
    }

    /// One point with no segment to carry it: the three isolated columns move
    /// together, so they reserve together.
    fn append_isolated_point(&mut self, point: Point, operand: u32) {
        let index = self.isolated.len();
        self.isolated.push(point.xy());
        self.isolated_points.push(point);
        self.isolated_operands.push(operand);
        if let Some(components) = self.relation_components.as_deref_mut() {
            components
                .source_order
                .push(StagedRelationItem::Isolated(index));
        }
    }

    fn append_line_coords(
        &mut self,
        line: &(impl crate::geometry::Coordinates + ?Sized),
        operand: u32,
    ) {
        self.retain_source_ordinates(line);
        self.reserve_segments(line.coord_count().saturating_sub(1));
        let segment_start = self.segments.len();
        let mut first = None;
        let mut last = None;
        for [start, end] in line.segment_pairs() {
            let (start_xy, end_xy) = (start.xy(), end.xy());
            if !same_point(start_xy, end_xy) {
                first.get_or_insert(start_xy);
                last = Some(end_xy);
            }
            self.push_segment(
                Segment {
                    start: start_xy,
                    end: end_xy,
                },
                &[start, end],
                operand,
                None,
            );
        }
        if let Some(components) = self.relation_components.as_deref_mut() {
            let component = components.components.len();
            let has_nonzero_segment = first.is_some();
            components.components.push(StagedSourceComponent {
                operand,
                segments: segment_start..self.segments.len(),
                ring: None,
                ring_kind: None,
                has_nonzero_segment,
            });
            if has_nonzero_segment {
                components
                    .source_order
                    .push(StagedRelationItem::Component(component));
            }
        }
        if first.is_none() {
            self.isolated.reserve(line.coord_count());
            self.isolated_points.reserve(line.coord_count());
            self.isolated_operands.reserve(line.coord_count());
            for point in line.iter_coords() {
                self.append_isolated_point(point, operand);
            }
            return;
        }
        if let (Some(first), Some(last)) = (first, last)
            && !same_point(first, last)
        {
            self.line_boundaries.reserve(2);
            self.line_boundaries.push((operand, PointKey::new(first)));
            self.line_boundaries.push((operand, PointKey::new(last)));
        }
    }

    fn append_polygon(&mut self, polygon: &Polygon, operand: u32) -> Result<()> {
        self.append_polygon_rings(polygon.rings(), operand)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "operand IDs are admitted by the source inventory's operand count"
    )]
    fn append_polygon_rings<C: crate::geometry::Coordinates>(
        &mut self,
        rings: impl IntoIterator<Item = C>,
        operand: u32,
    ) -> Result<()> {
        let first_ring = self.ring_count;
        let polygon_id =
            u32::try_from(self.polygon_count).map_err(|_| ResourceError::SizeOverflow)?;
        self.polygon_count = self
            .polygon_count
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?;
        self.polygon_operands.push(operand);
        for (ring_index, ring) in rings.into_iter().enumerate() {
            let ring_id =
                u32::try_from(self.ring_count).map_err(|_| ResourceError::SizeOverflow)?;
            self.ring_count = self
                .ring_count
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
            let kind = if crate::geometry::ring_is_collinear(&ring) {
                RingKind::Collapsed
            } else {
                RingKind::Winding
            };
            let segment_start = self.segments.len();
            self.ring_polygons.push(polygon_id);
            self.ring_holes.push(ring_index > 0);
            self.ring_kinds.push(kind);
            let count = ring.coord_count().saturating_sub(1);
            self.retain_source_ordinates(&ring);
            self.reserve_segments(count);
            let mut pairs = ring.segment_pairs();
            let [first, first_end] = pairs.next().expect("a Ring has at least four coordinates");
            let mut pairs = std::iter::once([first, first_end]).chain(pairs).peekable();
            while let Some([start, mut end]) = pairs.next() {
                if pairs.peek().is_none() {
                    end = first;
                }
                let segment = Segment {
                    start: start.xy(),
                    end: end.xy(),
                };
                self.push_segment(segment, &[start, end], operand, Some(ring_id));
            }
            if let Some(components) = self.relation_components.as_deref_mut() {
                let component = components.components.len();
                let has_nonzero_segment = kind == RingKind::Winding
                    || ring
                        .iter_coords()
                        .any(|point| !same_point(first.xy(), point.xy()));
                components.components.push(StagedSourceComponent {
                    operand,
                    segments: segment_start..self.segments.len(),
                    ring: Some(ring_id),
                    ring_kind: Some(kind),
                    has_nonzero_segment,
                });
                components
                    .source_order
                    .push(StagedRelationItem::Component(component));
            }
        }
        self.polygon_rings[operand as usize].push(first_ring..self.ring_count);
        self.polygon_ring_ranges.push(first_ring..self.ring_count);
        Ok(())
    }

    fn reserve_segments(&mut self, count: usize) {
        self.segments.reserve(count);
        if let Some(points) = &mut self.source_points {
            points.reserve(count);
        }
        self.source_operands.reserve(count);
        self.source_rings.reserve(count);
    }

    fn retain_source_ordinates(
        &mut self,
        coordinates: &(impl crate::geometry::Coordinates + ?Sized),
    ) {
        if !self.xy_only
            && self.source_points.is_none()
            && (coordinates.z_column().is_some() || coordinates.m_column().is_some())
        {
            // Earlier XY sources have no ordinates, so their stored carriers
            // recover the prefix when a later source first supplies Z/M.
            let mut points = Vec::with_capacity(self.segments.capacity());
            points.extend(
                self.segments.iter().map(|segment| {
                    SourceArcRecord::new([segment.start.point(), segment.end.point()])
                }),
            );
            self.source_points = Some(points);
        }
    }

    fn push_segment(
        &mut self,
        segment: Segment,
        source_points: &[Point; 2],
        operand: u32,
        ring: Option<u32>,
    ) {
        if let Some(points) = &mut self.source_points {
            points.push(SourceArcRecord::new(*source_points));
        }
        self.segments.push(segment);
        self.source_operands.push(operand);
        self.source_rings.push(ring);
    }
}

fn select_operation(operation: crate::geometry::OverlayOp, membership: &[bool]) -> bool {
    match operation {
        crate::geometry::OverlayOp::Union => membership.iter().any(|&value| value),
        crate::geometry::OverlayOp::Intersection => membership.iter().all(|&value| value),
        crate::geometry::OverlayOp::Difference => {
            membership.first().copied().unwrap_or(false)
                && membership.iter().skip(1).all(|&value| !value)
        },
        crate::geometry::OverlayOp::SymmetricDifference => {
            membership.iter().filter(|&&value| value).count() % 2 == 1
        },
    }
}

const fn ring_occupied(winding: i64, odd: bool) -> bool {
    if odd { winding % 2 != 0 } else { winding != 0 }
}

fn rectangle_side_outward(
    partition: &ExactPartition<'_>,
    start: NodeId,
    end: NodeId,
    bounds: Bounds,
) -> Result<Option<[i8; 2]>> {
    for (axis, value, direction) in [
        (Axis::X, bounds.minx(), [-1, 0]),
        (Axis::X, bounds.maxx(), [1, 0]),
        (Axis::Y, bounds.miny(), [0, -1]),
        (Axis::Y, bounds.maxy(), [0, 1]),
    ] {
        if partition.compare_axis_to(start, axis, value)? == Ordering::Equal
            && partition.compare_axis_to(end, axis, value)? == Ordering::Equal
        {
            return Ok(Some(direction));
        }
    }
    Ok(None)
}

pub(in crate::geometry) fn homogeneous_midpoint(
    left: &[ExactDyadic; 3],
    right: &[ExactDyadic; 3],
) -> Result<[ExactDyadic; 3]> {
    let denominator = left[2]
        .try_product(&right[2])?
        .try_product(&ExactDyadic::from_f64(2.0))?;
    let mut x = left[0]
        .try_product(&right[2])?
        .try_add(right[0].try_product(&left[2])?)?;
    let mut y = left[1]
        .try_product(&right[2])?
        .try_add(right[1].try_product(&left[2])?)?;
    let mut denominator = denominator;
    if denominator.sign() == ExactSign::Negative {
        x = x.neg();
        y = y.neg();
        denominator = denominator.neg();
    }
    Ok([x, y, denominator])
}

fn symbolic_ray_crossing(
    segment: Segment,
    point: &[ExactDyadic; 3],
    direction: [i8; 2],
) -> Result<i32> {
    let (start, end) = (segment.start, segment.end);
    let above = |ordinate: f64| -> Result<bool> {
        let relative = ExactDyadic::from_f64(ordinate)
            .try_product(&point[2])?
            .try_subtract(point[1].clone())?;
        Ok(match relative.sign() {
            ExactSign::Positive => true,
            ExactSign::Negative => false,
            ExactSign::Zero => direction[1] < 0,
        })
    };
    let (start_above, end_above) = (above(start.y)?, above(end.y)?);
    if start_above == end_above {
        return Ok(0);
    }

    let dx = ExactDyadic::from_f64(end.x).try_subtract(ExactDyadic::from_f64(start.x))?;
    let dy = ExactDyadic::from_f64(end.y).try_subtract(ExactDyadic::from_f64(start.y))?;
    let px = point[0]
        .clone()
        .try_subtract(ExactDyadic::from_f64(start.x).try_product(&point[2])?)?;
    let py = point[1]
        .clone()
        .try_subtract(ExactDyadic::from_f64(start.y).try_product(&point[2])?)?;
    let orientation = dx.try_product(&py)?.try_subtract(dy.try_product(&px)?)?;
    let orientation = if orientation.sign() == ExactSign::Zero {
        let dx_sign = exact_sign_i8(dx.sign());
        let dy_sign = exact_sign_i8(dy.sign());
        (dx_sign
            .wrapping_mul(direction[1])
            .wrapping_sub(dy_sign.wrapping_mul(direction[0])))
        .signum()
    } else {
        exact_sign_i8(orientation.sign())
    };
    let right = if end.y > start.y {
        orientation > 0
    } else {
        orientation < 0
    };
    if right {
        Ok(if end.y > start.y { 1 } else { -1 })
    } else {
        Ok(0)
    }
}

const fn exact_sign_i8(sign: ExactSign) -> i8 {
    match sign {
        ExactSign::Negative => -1,
        ExactSign::Zero => 0,
        ExactSign::Positive => 1,
    }
}

/// Which polygons the face walk currently stands in, kept up to date as it
/// crosses one ring edge at a time: a polygon is occupied when its shell ring
/// is filled and none of its holes is, and every operand counts its occupied
/// polygons. A face then reads each operand in O(1) instead of rescanning
/// every polygon.
struct PolygonOccupancy<'source> {
    odd: bool,
    /// Admitted source identity remains authoritative even when operand staging
    /// order differs from operand order.
    ring_polygons: &'source [u32],
    ring_holes: &'source [bool],
    polygon_operands: &'source [u32],
    ring_count: usize,
    shell_filled: Vec<bool>,
    filled_holes: Vec<u32>,
    occupied: Vec<u32>,
}

impl<'source> PolygonOccupancy<'source> {
    fn new(source: &'source ExactSourceInventory, odd: bool) -> Self {
        let polygons = source.polygon_operands.len();
        let mut shell_filled = Vec::with_capacity(polygons);
        shell_filled.resize(polygons, false);
        let filled_holes = vec![0; polygons];
        let occupied = vec![0; source.operand_count()];
        Self {
            odd,
            ring_polygons: &source.ring_polygons,
            ring_holes: &source.ring_holes,
            polygon_operands: &source.polygon_operands,
            ring_count: source.ring_count,
            shell_filled,
            filled_holes,
            occupied,
        }
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "source-staged ring IDs and polygon IDs index the borrowed canonical owner columns"
    )]
    fn seed(&mut self, winding: &[i64]) -> Result<()> {
        if winding.len() != self.ring_count {
            return Err(GeometryErrorKind::Invalid(
                "polygon occupancy seed has the wrong ring count".into(),
            )
            .into());
        }
        for ((&polygon, &hole), &winding) in
            self.ring_polygons.iter().zip(self.ring_holes).zip(winding)
        {
            let filled = ring_occupied(winding, self.odd);
            let polygon = polygon as usize;
            if !hole {
                self.shell_filled[polygon] = filled;
            } else if filled {
                self.filled_holes[polygon] = self.filled_holes[polygon].wrapping_add(1);
            }
        }
        for (polygon, &filled_shell) in self.shell_filled.iter().enumerate() {
            if filled_shell && self.filled_holes[polygon] == 0 {
                let operand = self.polygon_operands[polygon] as usize;
                self.occupied[operand] = self.occupied[operand].wrapping_add(1);
            }
        }
        Ok(())
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "ring and polygon IDs were admitted by source staging"
    )]
    fn cross(&mut self, winding: &mut [i64], ring: u32, direction: i64) {
        let ring = ring as usize;
        let before = ring_occupied(winding[ring], self.odd);
        winding[ring] = winding[ring].wrapping_add(direction);
        let after = ring_occupied(winding[ring], self.odd);
        if before == after {
            return;
        }
        let Some((&polygon, &hole)) = self.ring_polygons.get(ring).zip(self.ring_holes.get(ring))
        else {
            return;
        };
        let polygon = polygon as usize;
        let was = self.shell_filled[polygon] && self.filled_holes[polygon] == 0;
        if !hole {
            self.shell_filled[polygon] = after;
        } else if after {
            self.filled_holes[polygon] = self.filled_holes[polygon].wrapping_add(1);
        } else {
            self.filled_holes[polygon] = self.filled_holes[polygon].wrapping_sub(1);
        }
        let now = self.shell_filled[polygon] && self.filled_holes[polygon] == 0;
        if was != now {
            let count = &mut self.occupied[self.polygon_operands[polygon] as usize];
            if now {
                *count = count.wrapping_add(1);
            } else {
                *count = count.wrapping_sub(1);
            }
        }
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "operand indexes are admitted from ExactSourceInventory::polygon_rings"
    )]
    fn operand_occupied(&self, operand: usize) -> bool {
        self.occupied[operand] != 0
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "polygon IDs are admitted by the source inventory used to construct occupancy"
    )]
    fn polygon_holes_occupied(&self, polygon: usize) -> bool {
        self.filled_holes[polygon] != 0
    }
}

#[expect(
    clippy::indexing_slicing,
    reason = "ring IDs in each stored polygon range were admitted with the source ring inventory"
)]
fn rings_occupied(winding: &[i64], mut rings: Range<usize>, odd: bool) -> bool {
    rings
        .next()
        .is_some_and(|ring| ring_occupied(winding[ring], odd))
        && rings.all(|ring| !ring_occupied(winding[ring], odd))
}

fn bucket_axes<I: IntoIterator<Item = NodeId>>(
    nodes: I,
    points: &ResolvedNodePoints,
    declared: crate::geometry::CoordinateAxes,
    strict: bool,
    operation: &str,
) -> Result<crate::geometry::CoordinateAxes> {
    if !declared.has_z() && !declared.has_m() {
        return Ok(crate::geometry::CoordinateAxes::XY);
    }
    let mut nodes = nodes.into_iter();
    let Some(first) = nodes.next() else {
        return Ok(crate::geometry::CoordinateAxes::XY);
    };
    let mut has_z = declared.has_z();
    let mut has_m = declared.has_m();
    for node in std::iter::once(first).chain(nodes) {
        let (z, m) = points.ordinates(node).ok_or_else(|| {
            GeometryErrorKind::Invalid("exact overlay output lost a live node".into())
        })?;
        has_z &= z.0.is_some();
        has_m &= m.0.is_some();
    }
    let axes = crate::geometry::CoordinateAxes::new(
        crate::geometry::HasZ(has_z),
        crate::geometry::HasM(has_m),
    );
    if strict && axes != declared {
        return Err(GeometryErrorKind::ordinate_dropped(operation));
    }
    Ok(axes)
}

fn xy_sequence_for_nodes(nodes: &[NodeId], coordinates: &[XY]) -> Result<CoordSeq> {
    let mut xs = Vec::with_capacity(nodes.len());
    let mut ys = Vec::with_capacity(nodes.len());
    for &node in nodes {
        let point = embedding::Embedding::xy_at(coordinates, node);
        xs.push(point.x);
        ys.push(point.y);
    }
    CoordSeq::try_from_vecs(xs, ys, None, None)
}

fn polygons_from_xy_nodes(
    source: Vec<Vec<Vec<NodeId>>>,
    coordinates: &[XY],
) -> Result<Vec<Polygon>> {
    let mut polygons = Vec::with_capacity(source.len());
    for polygon in source {
        let mut rings = Vec::with_capacity(polygon.len());
        for ring in polygon {
            rings.push(Ring::try_from_closed(xy_sequence_for_nodes(
                &ring,
                coordinates,
            )?)?);
        }
        let mut rings = rings.into_iter();
        let shell = rings.next().expect("polygon has a shell");
        let mut holes = Vec::with_capacity(rings.size_hint().0);
        holes.extend(rings);
        polygons.push(Polygon::new(shell, holes));
    }
    Ok(polygons)
}

fn assemble_node_parts(
    points: Vec<Point>,
    lines: Vec<CoordSeq>,
    polygons: Vec<Polygon>,
    point_axes: crate::geometry::CoordinateAxes,
    line_axes: crate::geometry::CoordinateAxes,
    polygon_axes: crate::geometry::CoordinateAxes,
) -> Result<Shape> {
    let point_part = (!points.is_empty())
        .then(|| Shape::narrow_points(point_axes, points))
        .transpose()?;
    let line_part = (!lines.is_empty()).then(|| {
        let mut proven = Vec::with_capacity(lines.len());
        for line in lines {
            proven.push(LineSeq::from_proven(line));
        }
        Shape::narrow_lines(line_axes, proven)
    });
    let polygon_part =
        (!polygons.is_empty()).then(|| Shape::narrow_polygons(polygon_axes, polygons));
    let mut output = Vec::with_capacity(3);
    output.extend([point_part, line_part, polygon_part].into_iter().flatten());
    match <[Shape; 1]>::try_from(output) {
        Ok([single]) => Ok(single),
        Err(parts) => Shape::geometry_collection(parts),
    }
}

fn with_axes(point: Point, axes: crate::geometry::CoordinateAxes) -> Point {
    Point::new_raw_axes(
        point.x,
        point.y,
        crate::geometry::ZOrdinate(axes.has_z().then(|| point.z().expect("retained Z"))),
        crate::geometry::MOrdinate(axes.has_m().then(|| point.m().expect("retained M"))),
    )
}

const fn location(occupied: bool) -> Loc {
    if occupied {
        Loc::Interior
    } else {
        Loc::Exterior
    }
}

/// Certify a rounded embedding whose endpoint identities were decided by a
/// different exact construction (currently Voronoi). ExactPartition owns the
/// final noding/incidence pass; the external owner retains cocircular centers,
/// clipping policy, and fan order.
#[cfg(test)]
pub(in crate::geometry) fn certify_emitted_incidence(items: &[([u32; 2], Segment)]) -> Result<()> {
    let mut coordinates: HashMap<u32, PointKey> =
        HashMap::with_hasher(crate::collections::collection_hash_state()?.clone());
    let mut by_coordinate: HashMap<PointKey, u32> =
        HashMap::with_hasher(crate::collections::collection_hash_state()?.clone());
    let endpoint_capacity = items
        .len()
        .checked_mul(2)
        .ok_or(ResourceError::SizeOverflow)?;
    coordinates.reserve(endpoint_capacity);
    by_coordinate.reserve(endpoint_capacity);
    let mut segments = Vec::with_capacity(items.len());
    for &([start_id, end_id], segment) in items {
        for (id, point) in [(start_id, segment.start), (end_id, segment.end)] {
            let key = PointKey::new(point);
            if let Some(previous) = coordinates.insert(id, key)
                && previous != key
            {
                return Err(GeometryErrorKind::ExactNodeCollision {
                    first: id,
                    second: id,
                }
                .into());
            }
            if let Some(previous) = by_coordinate.insert(key, id)
                && previous != id
            {
                return Err(GeometryErrorKind::ExactNodeCollision {
                    first: previous,
                    second: id,
                }
                .into());
            }
        }
        segments.push(segment);
    }
    let inventory = ExactSourceInventory::from_segments(&segments)?;
    let mut budget = ExpansionBudget::new("exact_partition", "Voronoi incidence");
    let partition = inventory.partition(&mut budget)?;
    drop(partition.local_topology()?);
    if let ControlFlow::Break((first, second)) =
        for_each_candidate_pair(&segments, single_chain, |left, right| {
            let endpoints = |index: usize| {
                let ([start, end], segment) = *items
                    .get(index)
                    .expect("candidate-pair index belongs to the source item table");
                [(NodeId(start), segment.start), (NodeId(end), segment.end)]
            };
            if emitted_pair_preserves_incidence(endpoints(left), endpoints(right)) {
                ControlFlow::Continue(())
            } else {
                ControlFlow::Break((left, right))
            }
        })?
    {
        return Err(GeometryErrorKind::ExactUnexpectedContact { first, second }.into());
    }
    Ok(())
}

fn cycle_contains_node(
    partition: &ExactPartition<'_>,
    cycle: &[NodeId],
    query: NodeId,
) -> Result<bool> {
    let mut inside = false;
    for (&a, &b) in cycle
        .iter()
        .zip(cycle.iter().skip(1).chain(cycle.iter().take(1)))
    {
        let y_a = partition.compare_axis(a, query, Axis::Y)?;
        let y_b = partition.compare_axis(b, query, Axis::Y)?;
        let a_is_below = y_a != Ordering::Greater;
        let b_is_below = y_b != Ordering::Greater;
        // The orientation decides only an edge that straddles the ray or
        // whose extent could hold the query.
        let may_hold = y_a != Ordering::Greater
            && y_b != Ordering::Less
            && partition.compare_axis(query, a, Axis::X)? != Ordering::Less
            && partition.compare_axis(query, b, Axis::X)? != Ordering::Greater;
        if a_is_below == b_is_below && !may_hold {
            continue;
        }
        let orientation = partition.orientation(a, b, query)?;
        if may_hold && orientation == ExactSign::Zero {
            return Ok(false);
        }
        if a_is_below == b_is_below {
            continue;
        }
        let crosses_left = (a_is_below && orientation == ExactSign::Negative)
            || (b_is_below && orientation == ExactSign::Positive);
        if crosses_left {
            inside = !inside;
        }
    }
    Ok(inside)
}

fn node_source_range(records: &[(NodeId, u32)], node: NodeId) -> Range<usize> {
    let start = records.partition_point(|(record_node, _)| record_node.0 < node.0);
    let count = records
        .iter()
        .skip(start)
        .take_while(|(record_node, _)| record_node.0 == node.0)
        .count();
    start..start.wrapping_add(count)
}

#[derive(Clone, Copy)]
enum LineSource<'a> {
    LineString(&'a LineSeq),
    MultiLineString(&'a MultiLineString),
}

impl<'a> LineSource<'a> {
    const fn from_shape(shape: &'a Shape) -> Option<Self> {
        match shape {
            Shape::LineString(line) => Some(Self::LineString(line)),
            Shape::MultiLineString(lines) => Some(Self::MultiLineString(lines)),
            Shape::Point(_)
            | Shape::MultiPoint(_)
            | Shape::Polygon(_)
            | Shape::MultiPolygon(_)
            | Shape::GeometryCollection(_)
            | Shape::Empty(..) => None,
        }
    }

    const fn kind(self) -> GeometryKind {
        match self {
            Self::LineString(_) => GeometryKind::LineString,
            Self::MultiLineString(_) => GeometryKind::MultiLineString,
        }
    }
}

fn source_line_ranges(source: LineSource<'_>) -> Result<Vec<Range<usize>>> {
    let mut ranges = Vec::with_capacity(match source {
        LineSource::LineString(_) => 1,
        LineSource::MultiLineString(lines) => lines.len(),
    });
    let mut offset = 0_usize;
    let mut push = |length: usize| -> Result<()> {
        let end = offset
            .checked_add(length.saturating_sub(1))
            .ok_or(ResourceError::SizeOverflow)?;
        ranges.push(offset..end);
        offset = end;
        Ok(())
    };
    match source {
        LineSource::LineString(line) => push(line.len())?,
        LineSource::MultiLineString(lines) => {
            for line in lines.iter() {
                push(line.len())?;
            }
        },
    }
    Ok(ranges)
}

fn polygon_source_ring_ranges(shape: &Shape) -> Result<Vec<Range<usize>>> {
    let polygons: Vec<&Polygon> = match shape {
        Shape::Polygon(polygon) => vec![polygon],
        Shape::MultiPolygon(polygons) => polygons.iter().collect(),
        _ => {
            return Err(GeometryErrorKind::Invalid(
                "subdivision ring traces require a polygonal source shape".into(),
            )
            .into());
        },
    };
    let mut ranges = Vec::new();
    let mut offset = 0_usize;
    for polygon in polygons {
        for ring in polygon.rings() {
            let start = offset;
            for _ in ring.segment_pairs() {
                offset = offset.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
            }
            ranges.push(start..offset);
        }
    }
    Ok(ranges)
}

fn collect_polygon_parts<'a>(shape: &'a Shape, polygons: &mut Vec<&'a Polygon>) {
    match shape {
        Shape::Polygon(polygon) => polygons.push(polygon),
        Shape::MultiPolygon(multipolygon) => polygons.extend(multipolygon.iter()),
        Shape::GeometryCollection(collection) => {
            for member in collection {
                collect_polygon_parts(member, polygons);
            }
        },
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Empty(..) => {},
    }
}

fn same_published_polygon_xy(frame: GeographicFrame, left: XY, right: XY) -> Result<bool> {
    let longitude = frame.longitude();
    let left_x = longitude
        .canonicalize(left.x)
        .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?
        .get();
    let right_x = longitude
        .canonicalize(right.x)
        .map_err(|error| GeometryErrorKind::Invalid(error.to_string().into()))?
        .get();
    Ok(same_point(
        XY::new(left_x, left.y),
        XY::new(right_x, right.y),
    ))
}

#[expect(
    clippy::indexing_slicing,
    reason = "the incoming span and node IDs come from the exact chain walk and the CSR row is owned by its topology"
)]
fn next_incident_span(
    topology: &IndexedTopology,
    edge_of_slot: &[u32],
    node: NodeId,
    incoming: u32,
) -> Option<u32> {
    let range =
        topology.starts[node.0 as usize] as usize..topology.starts[node.0 as usize + 1] as usize;
    edge_of_slot[range]
        .iter()
        .copied()
        .find(|&span| span != incoming)
}

#[expect(
    clippy::indexing_slicing,
    reason = "span and node IDs are returned by the checked topology walk and address its aligned visit/pin columns"
)]
fn walk_exact_chain(
    topology: &IndexedTopology,
    edge_of_slot: &[u32],
    spans: &[ExactSpan],
    node_pin: &[bool],
    visited: &mut [bool],
    origin: NodeId,
    first: u32,
) -> Result<(Vec<NodeId>, Vec<u32>)> {
    let mut nodes = Vec::with_capacity(2);
    nodes.push(origin);
    let mut chain_spans = Vec::new();
    let mut current = origin;
    let mut span_id = first;
    loop {
        let span_index = span_id as usize;
        if visited[span_index] {
            return Err(
                GeometryErrorKind::Invalid("exact chain walk revisited a span".into()).into(),
            );
        }
        visited[span_index] = true;
        let span = spans[span_index];
        let target = if span.start == current {
            span.end
        } else if span.end == current {
            span.start
        } else {
            return Err(GeometryErrorKind::Invalid(
                "exact chain span is detached from its walk".into(),
            )
            .into());
        };
        chain_spans.push(span_id);
        nodes.push(target);
        if node_pin[target.0 as usize] || target == origin {
            break;
        }
        span_id = next_incident_span(topology, edge_of_slot, target, span_id).ok_or_else(|| {
            GeometryErrorKind::Invalid("exact chain has no degree-two continuation".into())
        })?;
        current = target;
    }
    Ok((nodes, chain_spans))
}

fn graph_edge_departure<E, W>(edge_nodes: &E, edge: usize, origin: u32, work: &mut W) -> Result<u32>
where
    E: Fn(usize) -> Result<[u32; 2]> + ?Sized,
    W: Work + ?Sized,
{
    work.charge(1)?;
    let [start, end] = edge_nodes(edge)?;
    if start == origin {
        Ok(end)
    } else if end == origin {
        Ok(start)
    } else {
        Err(
            GeometryErrorKind::Invalid("graph departure edge is not incident to its origin".into())
                .into(),
        )
    }
}

fn graph_contact_walk<R, E, W>(
    points: &R,
    topology: &IndexedTopology,
    edge_of_slot: &[u32],
    edge_nodes: &E,
    edge: usize,
    work: &mut W,
) -> Result<u32>
where
    R: RayPoints + ?Sized,
    E: Fn(usize) -> Result<[u32; 2]> + ?Sized,
    W: Work + ?Sized,
{
    work.charge(1)?;
    let [start, end] = edge_nodes(edge)?;
    let upper = match points.compare_axis(start, end, Axis::Y)? {
        Ordering::Less => end,
        Ordering::Greater => start,
        Ordering::Equal => {
            return Err(
                GeometryErrorKind::Invalid("leftward ray contact is horizontal".into()).into(),
            );
        },
    };
    let start = *topology.starts.get(upper as usize).ok_or_else(|| {
        GeometryErrorKind::Invalid("leftward ray contact node has no departure row".into())
    })? as usize;
    let end = *topology
        .starts
        .get(
            (upper as usize)
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?,
        )
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("leftward ray contact row is incomplete".into())
        })? as usize;
    for slot in start..end {
        work.charge(1)?;
        if edge_of_slot.get(slot).copied()
            == Some(u32::try_from(edge).map_err(|_| ResourceError::SizeOverflow)?)
        {
            let face = *topology.face_of.get(slot).ok_or_else(|| {
                GeometryErrorKind::Invalid("leftward ray contact has no face walk".into())
            })?;
            return face
                .checked_add(1)
                .ok_or_else(|| ResourceError::SizeOverflow.into());
        }
    }
    Err(
        GeometryErrorKind::Invalid("leftward ray contact edge has no upper-end departure".into())
            .into(),
    )
}

fn graph_exterior_walk<R, E, W>(
    points: &R,
    topology: &IndexedTopology,
    edge_of_slot: &[u32],
    edge_nodes: &E,
    root: u32,
    work: &mut W,
) -> Result<Option<u32>>
where
    R: RayPoints + ?Sized,
    E: Fn(usize) -> Result<[u32; 2]> + ?Sized,
    W: Work + ?Sized,
{
    let start = *topology.starts.get(root as usize).ok_or_else(|| {
        GeometryErrorKind::Invalid("graph component root has no departure row".into())
    })? as usize;
    let end = *topology
        .starts
        .get(
            (root as usize)
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?,
        )
        .ok_or_else(|| {
            GeometryErrorKind::Invalid("graph component root row is incomplete".into())
        })? as usize;
    let mut chosen = None;
    let mut has_upper = false;
    for slot in start..end {
        work.charge(1)?;
        let target = *topology.targets.get(slot).ok_or_else(|| {
            GeometryErrorKind::Invalid("graph component departure has no target".into())
        })?;
        work.charge(1)?;
        let upper = !points.compare_axis(target, root, Axis::Y)?.is_lt();
        if chosen.is_none() || (upper && !has_upper) {
            chosen = Some(slot);
            has_upper = upper;
            continue;
        }
        if upper != has_upper {
            continue;
        }
        let previous = chosen.ok_or_else(|| {
            GeometryErrorKind::Invalid("graph exterior fan lost its chosen departure".into())
        })?;
        let previous_edge = *edge_of_slot.get(previous).ok_or_else(|| {
            GeometryErrorKind::Invalid("graph exterior fan references an unknown edge".into())
        })? as usize;
        let current_edge = *edge_of_slot.get(slot).ok_or_else(|| {
            GeometryErrorKind::Invalid("graph exterior fan references an unknown edge".into())
        })? as usize;
        let previous_target = graph_edge_departure(edge_nodes, previous_edge, root, work)?;
        let current_target = graph_edge_departure(edge_nodes, current_edge, root, work)?;
        if compare_departures_with_work(points, root, previous_target, current_target, work)?
            == Ordering::Less
        {
            chosen = Some(slot);
        }
    }
    chosen
        .map(|slot| {
            topology.face_of.get(slot).copied().ok_or_else(|| {
                GeometryErrorKind::Invalid("graph exterior departure has no face".into()).into()
            })
        })
        .transpose()
}

pub(in crate::geometry) fn graph_global_faces_with_work<R, E, W>(
    points: &R,
    topology: &IndexedTopology,
    edge_count: usize,
    edge_nodes: E,
    edge_of_slot: &[u32],
    work: &mut W,
) -> Result<GlobalFaces>
where
    R: RayPoints + ?Sized,
    E: Fn(usize) -> Result<[u32; 2]>,
    W: Work,
{
    let node_count = topology.component_of.len();
    let slots = edge_count
        .checked_mul(2)
        .ok_or(ResourceError::SizeOverflow)?;
    if edge_of_slot.len() != slots
        || topology.targets.len() != slots
        || topology.face_of.len() != slots
        || topology.starts.len()
            != node_count
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?
    {
        return Err(GeometryErrorKind::Invalid(
            "graph face inputs do not share one indexed topology".into(),
        )
        .into());
    }
    work.charge(u64::try_from(node_count).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut component_count = 0_usize;
    for &component in &topology.component_of {
        work.charge(1)?;
        component_count = component_count.max(
            usize::try_from(component)
                .map_err(|_| ResourceError::SizeOverflow)?
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?,
        );
    }
    work.charge(u64::try_from(component_count).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut minima = vec![None; component_count];
    for (node, &component) in topology.component_of.iter().enumerate() {
        work.charge(1)?;
        let component = usize::try_from(component).map_err(|_| ResourceError::SizeOverflow)?;
        let minimum = minima.get_mut(component).ok_or_else(|| {
            GeometryErrorKind::Invalid("indexed graph component ID is unknown".into())
        })?;
        let node = u32::try_from(node).map_err(|_| ResourceError::SizeOverflow)?;
        if let Some(previous) = *minimum {
            work.charge(1)?;
            let x = points.compare_axis(node, previous, Axis::X)?;
            let order = if x.is_eq() {
                work.charge(1)?;
                points.compare_axis(node, previous, Axis::Y)?
            } else {
                x
            };
            if order.is_lt() {
                *minimum = Some(node);
            }
        } else {
            *minimum = Some(node);
        }
    }
    work.charge(u64::try_from(component_count).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut roots = Vec::with_capacity(component_count);
    for minimum in minima {
        work.charge(1)?;
        roots.push(minimum.ok_or_else(|| {
            GeometryErrorKind::Invalid("indexed graph component has no root node".into())
        })?);
    }

    let walks = topology.face_starts.len().checked_sub(1).ok_or_else(|| {
        GeometryErrorKind::Invalid("indexed graph face starts have no initial sentinel".into())
    })?;
    let size = walks.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
    let limit = u32::try_from(size).map_err(|_| ResourceError::SizeOverflow)?;
    work.charge(u64::try_from(size).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut links: Vec<u32> = (0..limit).collect();
    work.charge(u64::try_from(component_count).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut component_hosts = Vec::with_capacity(component_count);
    let hits = if roots.len() == 1 {
        work.charge(1)?;
        vec![None]
    } else {
        shoot_left_with_work(points, edge_count, &edge_nodes, &roots, work)?
    };
    if hits.len() != roots.len() {
        return Err(GeometryErrorKind::Invalid(
            "leftward ray sweep returned an incomplete component-host table".into(),
        )
        .into());
    }
    for (&root, hit) in roots.iter().zip(hits) {
        work.charge(1)?;
        let host = if let Some(edge) = hit {
            graph_contact_walk(points, topology, edge_of_slot, &edge_nodes, edge, work)?
        } else {
            0
        };
        component_hosts.push(host);
        if let Some(exterior) =
            graph_exterior_walk(points, topology, edge_of_slot, &edge_nodes, root, work)?
        {
            let link = usize::try_from(exterior)
                .map_err(|_| ResourceError::SizeOverflow)?
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
            *links.get_mut(link).ok_or_else(|| {
                GeometryErrorKind::Invalid("graph exterior face link is unknown".into())
            })? = host;
        }
    }

    for start in 1..size {
        work.charge(1)?;
        let mut root = u32::try_from(start).map_err(|_| ResourceError::SizeOverflow)?;
        let mut steps = 0_usize;
        loop {
            work.charge(1)?;
            let parent = *links.get(root as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("graph face link is outside its table".into())
            })?;
            if parent == root {
                break;
            }
            root = parent;
            steps = steps.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
            if steps >= size {
                return Err(GeometryErrorKind::Invalid(
                    "graph component exterior links contain a cycle".into(),
                )
                .into());
            }
        }
        let mut node = u32::try_from(start).map_err(|_| ResourceError::SizeOverflow)?;
        let mut steps = 0_usize;
        loop {
            work.charge(1)?;
            let parent = *links.get(node as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("graph face link is outside its table".into())
            })?;
            if parent == root {
                break;
            }
            *links.get_mut(node as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("graph face link is outside its table".into())
            })? = root;
            node = parent;
            steps = steps.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
            if steps >= size {
                return Err(GeometryErrorKind::Invalid(
                    "graph component exterior links contain a cycle".into(),
                )
                .into());
            }
        }
    }

    work.charge(u64::try_from(size).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut labels = vec![0_u32; size];
    let mut count = 1_u32;
    for node in 1..size {
        work.charge(1)?;
        let link = *links.get(node).ok_or_else(|| {
            GeometryErrorKind::Invalid("graph face link is outside its table".into())
        })?;
        if link == node as u32 {
            *labels.get_mut(node).ok_or_else(|| {
                GeometryErrorKind::Invalid("graph face label is outside its table".into())
            })? = count;
            count = count.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
        }
    }
    for host in &mut component_hosts {
        work.charge(1)?;
        let root = *links.get(*host as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("graph component host is outside its link table".into())
        })?;
        *host = *labels.get(root as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("graph component host has no global label".into())
        })?;
    }
    work.charge(u64::try_from(walks).map_err(|_| ResourceError::SizeOverflow)?)?;
    let mut walk_faces = Vec::with_capacity(walks);
    for &root in links.iter().skip(1) {
        work.charge(1)?;
        walk_faces.push(*labels.get(root as usize).ok_or_else(|| {
            GeometryErrorKind::Invalid("graph local face has no global label".into())
        })?);
    }
    Ok(GlobalFaces {
        walk_faces,
        component_hosts,
        count,
    })
}

impl ExactPartition<'_> {
    pub(in crate::geometry) const fn node_count(&self) -> usize {
        self.points.len()
    }

    pub(in crate::geometry) fn preserve_source_vertices(&mut self) {
        self.kept_nodes = KeptNodes::All;
    }

    pub(in crate::geometry) fn set_geographic_frame(
        &mut self,
        frame: crate::geographic::GeographicFrame,
    ) {
        self.kept_nodes = KeptNodes::All;
        self.embedding_domain = embedding::EmbeddingDomain::Geographic(frame);
    }

    /// A node's nearest-even binary64 point.
    #[expect(
        clippy::indexing_slicing,
        reason = "NodeId is admitted by this partition's PointArena"
    )]
    pub(in crate::geometry) fn nearest_xy(&self, node: NodeId) -> XY {
        self.points.rounded[node.0 as usize]
    }

    fn is_stored(&self, node: NodeId) -> bool {
        self.points.is_stored(node.0)
    }

    fn stored_key(&self, node: usize) -> Option<PointKey> {
        self.points.stored_key(node as u32)
    }

    /// A node's exact homogeneous point: built in place for a stored node,
    /// constructed once and kept for a crossing.
    pub(in crate::geometry) fn exact(&self, node: NodeId) -> Result<Cow<'_, [ExactDyadic; 3]>> {
        self.points.exact(node.0)
    }

    fn exact_owned(&self, node: NodeId) -> Result<[ExactDyadic; 3]> {
        match self.exact(node)? {
            Cow::Owned(point) => Ok(point),
            Cow::Borrowed(point) => Ok(point.clone()),
        }
    }

    fn compare_axis(&self, left: NodeId, right: NodeId, axis: Axis) -> Result<Ordering> {
        self.points.compare_axis(left.0, right.0, axis)
    }

    fn compare_axis_to(&self, node: NodeId, axis: Axis, value: f64) -> Result<Ordering> {
        self.points.compare_axis_to(node.0, axis, value)
    }

    fn orientation(&self, a: NodeId, b: NodeId, c: NodeId) -> Result<ExactSign> {
        self.points.orientation(a.0, b.0, c.0)
    }

    fn source_span_view_is_ordered(&self, segment: Segment, view: &[(u32, bool)]) -> Result<bool> {
        let entry = |(span_id, reversed): (u32, bool)| -> Result<NodeId> {
            let span = self.spans.get(span_id as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("source span view references an unknown span".into())
            })?;
            Ok(if reversed { span.end } else { span.start })
        };
        let exit = |(span_id, reversed): (u32, bool)| -> Result<NodeId> {
            let span = self.spans.get(span_id as usize).ok_or_else(|| {
                GeometryErrorKind::Invalid("source span view references an unknown span".into())
            })?;
            Ok(if reversed { span.start } else { span.end })
        };
        let first = *view.first().ok_or_else(|| {
            GeometryErrorKind::Invalid("source span view has no first span".into())
        })?;
        let last = *view.last().ok_or_else(|| {
            GeometryErrorKind::Invalid("source span view has no last span".into())
        })?;
        if self.stored_key(entry(first)?.0 as usize) != Some(PointKey::new(segment.start))
            || self.stored_key(exit(last)?.0 as usize) != Some(PointKey::new(segment.end))
        {
            return Ok(false);
        }
        for pair in view.windows(2) {
            let [left, right] = pair else {
                continue;
            };
            if exit(*left)? != entry(*right)? {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Return each source segment's exact atomic span run in source direction.
    ///
    /// The partition provenance is the only source-to-atom relation here.
    /// Ordering the inherited atoms from the stored source endpoint gives
    /// callers a view of the source spelling without rebuilding a coordinate
    /// graph.
    #[expect(
        clippy::indexing_slicing,
        reason = "span IDs are assigned from self.spans; each zipped source segment owns one pre-sized view"
    )]
    pub(in crate::geometry) fn source_span_views(
        &self,
        source_segments: &[Segment],
    ) -> Result<Vec<Vec<(u32, bool)>>> {
        let mut views = Vec::with_capacity(source_segments.len());
        views.resize_with(source_segments.len(), Vec::new);
        for (span, owners) in self.provenance.iter().enumerate() {
            for &(source, reversed) in owners {
                let view = views.get_mut(source as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "exact partition provenance references an unknown source".into(),
                    )
                })?;
                view.push((span as u32, reversed));
            }
        }
        for (segment, view) in source_segments.iter().zip(&mut views) {
            if view.len() < 2 {
                continue;
            }
            // Provenance enumeration often already forms a contiguous source
            // run; only genuinely unordered rows need the exact sweep sort.
            if self.source_span_view_is_ordered(*segment, view)? {
                #[cfg(test)]
                SOURCE_SPAN_VIEW_ORDERED.with(|count| count.set(count.get().saturating_add(1)));
                continue;
            }
            #[cfg(test)]
            SOURCE_SPAN_VIEW_SORTS.with(|count| count.set(count.get().saturating_add(1)));
            // Each atom's entry is its node nearest the source start; ordered
            // along the source, every exit is the next atom's entry.
            let entry = |(span, reversed): (u32, bool)| {
                let span = self.spans[span as usize];
                if reversed { span.end.0 } else { span.start.0 }
            };
            let exit = |(span, reversed): (u32, bool)| {
                let span = self.spans[span as usize];
                if reversed { span.start } else { span.end }
            };
            let mut ordered = std::mem::take(view);
            self.points
                .order_along(*segment, &mut ordered, entry, |_, _| {
                    Err(GeometryErrorKind::Invalid(
                        "exact partition source atoms share an entry node".into(),
                    )
                    .into())
                })?;
            let first = NodeId(entry(ordered[0]));
            let last = exit(ordered[ordered.len().wrapping_sub(1)]);
            if self.stored_key(first.0 as usize) != Some(PointKey::new(segment.start))
                || self.stored_key(last.0 as usize) != Some(PointKey::new(segment.end))
            {
                return Err(GeometryErrorKind::Invalid(
                    "exact partition source atoms do not reach their endpoint".into(),
                )
                .into());
            }
            if ordered
                .array_windows::<2>()
                .any(|&[left, right]| exit(left).0 != entry(right))
            {
                return Err(GeometryErrorKind::Invalid(
                    "exact partition source atoms are not a contiguous run".into(),
                )
                .into());
            }
            *view = ordered;
        }
        Ok(views)
    }

    fn source_span_trace_views<W: Work + ?Sized>(
        &self,
        source_segments: &[Segment],
        source_ids: &[usize],
        work: &mut W,
    ) -> Result<Vec<Vec<SourceSpanTraceView>>> {
        let views = self.source_span_views(source_segments)?;
        let mut total = 0_usize;
        for &source in source_ids {
            let row = views.get(source).ok_or_else(|| {
                GeometryErrorKind::Invalid("source span trace row is unknown".into())
            })?;
            total = total
                .checked_add(row.len())
                .ok_or(ResourceError::SizeOverflow)?;
        }
        work.charge(u64::try_from(total).map_err(|_| ResourceError::SizeOverflow)?)?;
        let mut traced = Vec::with_capacity(source_ids.len());
        for &source in source_ids {
            work.charge(1)?;
            let segment = *source_segments.get(source).ok_or_else(|| {
                GeometryErrorKind::Invalid("source span trace row is unknown".into())
            })?;
            let source_start = PointKey::new(segment.start);
            let source_end = PointKey::new(segment.end);
            let source_parameter = |node: NodeId| -> Result<ExactRational> {
                if let Some(key) = self.stored_key(node.0 as usize) {
                    if key == source_start {
                        #[cfg(test)]
                        SOURCE_SPAN_ENDPOINT_PARAMETERS
                            .with(|count| count.set(count.get().saturating_add(1)));
                        return Ok(ExactRational::zero());
                    }
                    if key == source_end {
                        #[cfg(test)]
                        SOURCE_SPAN_ENDPOINT_PARAMETERS
                            .with(|count| count.set(count.get().saturating_add(1)));
                        return Ok(ExactRational::one());
                    }
                }
                #[cfg(test)]
                SOURCE_SPAN_INTERIOR_PARAMETERS
                    .with(|count| count.set(count.get().saturating_add(1)));
                let point = self.exact(node)?;
                let [numerator, denominator] = parameter_along(segment, &point)?;
                ExactRational::new(numerator, denominator)
            };
            let row = views.get(source).ok_or_else(|| {
                GeometryErrorKind::Invalid("source span view row is unknown".into())
            })?;
            let mut output = Vec::with_capacity(row.len());
            for &(span, reversed) in row {
                work.charge(1)?;
                let span_data = self.spans.get(span as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "source span trace references an unknown exact span".into(),
                    )
                })?;
                let [start, end] = if reversed {
                    [span_data.end, span_data.start]
                } else {
                    [span_data.start, span_data.end]
                };
                let start_parameter = source_parameter(start)?;
                let end_parameter = source_parameter(end)?;
                if start_parameter.try_cmp(&end_parameter)?.is_ge() {
                    return Err(GeometryErrorKind::Invalid(
                        "source span trace does not advance along its edge".into(),
                    )
                    .into());
                }
                output.push(SourceSpanTraceView {
                    span,
                    reversed,
                    source_start: start_parameter,
                    source_end: end_parameter,
                });
            }
            traced.push(output);
        }
        Ok(traced)
    }

    /// Build maximal chain views directly from exact span incidence.
    ///
    /// `source_operands` maps the partition's source ordinals to coverage row
    /// identities. An atom is shared only when its exact provenance names two
    /// distinct rows. A chain ends at every non-degree-two node and wherever
    /// that shared/exterior class changes.
    #[expect(
        clippy::indexing_slicing,
        reason = "span and node IDs come from this partition; degree-two continuation rows are built from its checked topology"
    )]
    pub(in crate::geometry) fn coverage_chain_views(
        &self,
        source_operands: &[u32],
    ) -> Result<ExactChainViews> {
        let (topology, edge_of_slot) = self.local_topology()?;
        let mut shared_by_span = Vec::with_capacity(self.spans.len());
        for owners in self.provenance.iter() {
            let mut first = None;
            let mut second = None;
            let mut count = 0_usize;
            for &(source, _) in owners {
                let operand = *source_operands.get(source as usize).ok_or_else(|| {
                    GeometryErrorKind::Invalid(
                        "exact partition provenance references an unknown source".into(),
                    )
                })?;
                if Some(operand) == first || Some(operand) == second {
                    continue;
                }
                if first.is_none() {
                    first = Some(operand);
                } else if second.is_none() {
                    second = Some(operand);
                }
                count = count.wrapping_add(1);
            }
            shared_by_span.push(count == 2);
        }

        let mut node_pin = Vec::with_capacity(self.node_count());
        node_pin.resize(self.node_count(), false);
        for (node, &[start, end]) in topology.starts.array_windows::<2>().enumerate() {
            let slots = start as usize..end as usize;
            node_pin[node] = slots.len() != 2
                || shared_by_span[edge_of_slot[slots.start] as usize]
                    != shared_by_span[edge_of_slot[slots.start.wrapping_add(1)] as usize];
        }

        let mut visited = Vec::with_capacity(self.spans.len());
        visited.resize(self.spans.len(), false);
        let mut chain_of_span = Vec::with_capacity(self.spans.len());
        chain_of_span.resize(self.spans.len(), u32::MAX);
        let mut chains = Vec::with_capacity(self.spans.len());

        let append_chain = |seed: usize,
                            origin: NodeId,
                            cycle: bool,
                            visited: &mut [bool],
                            chain_of_span: &mut [u32],
                            chains: &mut Vec<ExactChainView>|
         -> Result<()> {
            let (mut nodes, spans) = walk_exact_chain(
                &topology,
                &edge_of_slot,
                &self.spans,
                &node_pin,
                visited,
                origin,
                seed as u32,
            )?;
            debug_assert_eq!(cycle, !node_pin[origin.0 as usize]);
            let mut spans = spans;
            if cycle {
                let count = spans.len();
                let mut minimum = 0;
                for index in 1..count {
                    if self.compare_nodes(nodes[index], nodes[minimum])? == Ordering::Less {
                        minimum = index;
                    }
                }
                nodes[..count].rotate_left(minimum);
                nodes[count] = nodes[0];
                spans.rotate_left(minimum);
                if self.compare_nodes(nodes[1], nodes[count.wrapping_sub(1)])? == Ordering::Greater
                {
                    nodes[1..count].reverse();
                    spans.reverse();
                }
            }
            let shared = shared_by_span[seed];
            debug_assert!(
                spans
                    .iter()
                    .all(|&span| shared_by_span[span as usize] == shared)
            );
            let chain = u32::try_from(chains.len()).map_err(|_| ResourceError::SizeOverflow)?;
            for &span in &spans {
                chain_of_span[span as usize] = chain;
            }
            chains.push(ExactChainView {
                nodes,
                spans,
                shared,
            });
            Ok(())
        };

        // Open chains start at pinned junctions. Consume them before an
        // unpinned interior span can split one walk and leave a visited suffix.
        for seed in 0..self.spans.len() {
            if visited[seed] {
                continue;
            }
            let span = self.spans[seed];
            let start_pinned = node_pin[span.start.0 as usize];
            let end_pinned = node_pin[span.end.0 as usize];
            if !start_pinned && !end_pinned {
                continue;
            }
            let origin = if start_pinned {
                if node_pin[span.end.0 as usize]
                    && self.compare_nodes(span.end, span.start)? == Ordering::Less
                {
                    span.end
                } else {
                    span.start
                }
            } else {
                span.end
            };
            append_chain(
                seed,
                origin,
                false,
                &mut visited,
                &mut chain_of_span,
                &mut chains,
            )?;
        }
        // No unvisited span touches a pin now. Every remaining component has
        // degree two at every node and is therefore a closed cycle.
        for seed in 0..self.spans.len() {
            if visited[seed] {
                continue;
            }
            let span = self.spans[seed];
            debug_assert!(!node_pin[span.start.0 as usize]);
            debug_assert!(!node_pin[span.end.0 as usize]);
            append_chain(
                seed,
                span.start,
                true,
                &mut visited,
                &mut chain_of_span,
                &mut chains,
            )?;
        }
        Ok(ExactChainViews {
            chains,
            chain_of_span,
        })
    }

    pub(in crate::geometry) fn owners(&self, span: usize) -> &[SegmentSource] {
        self.provenance.owners(span)
    }

    fn node_in_bounds(&self, node: NodeId, bounds: Bounds) -> Result<bool> {
        Ok(
            self.compare_axis_to(node, Axis::X, bounds.minx())? != Ordering::Less
                && self.compare_axis_to(node, Axis::X, bounds.maxx())? != Ordering::Greater
                && self.compare_axis_to(node, Axis::Y, bounds.miny())? != Ordering::Less
                && self.compare_axis_to(node, Axis::Y, bounds.maxy())? != Ordering::Greater,
        )
    }

    #[cfg(test)]
    fn round_nodes(&self, nodes: &[NodeId]) -> Vec<XY> {
        nodes.iter().map(|&node| self.nearest_xy(node)).collect()
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "span IDs and source operands match the admitted span and provenance rows"
    )]
    fn emit_lines(
        &self,
        source_operands: &[u32],
        operation: &'static str,
    ) -> Result<ExactLineEmission<'_>> {
        let spans = (0..self.spans.len()).map(|span| span as u32);
        let emission = self.span_emission(spans);
        let emissions = [&emission];
        let embedding = self.embed(&emissions, operation)?;

        let mut offsets = Vec::new();
        let offset_capacity = self
            .spans
            .len()
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?;
        offsets.reserve(offset_capacity);
        offsets.push(0);
        let mut owners = Vec::new();
        for span_index in 0..self.spans.len() {
            for &(source, reversed) in self.owners(span_index) {
                let operand = source_operands[source as usize];
                owners.push(ExactLineSource { operand, reversed });
            }
            offsets.push(u32::try_from(owners.len()).map_err(|_| ResourceError::SizeOverflow)?);
        }
        Ok(ExactLineEmission {
            embedding,
            items: emission.items,
            owners,
            offsets,
        })
    }

    pub(in crate::geometry) fn compare_nodes(
        &self,
        left: NodeId,
        right: NodeId,
    ) -> Result<Ordering> {
        match self.compare_axis(left, right, Axis::X)? {
            Ordering::Less => Ok(Ordering::Less),
            Ordering::Equal => self.compare_axis(left, right, Axis::Y),
            Ordering::Greater => Ok(Ordering::Greater),
        }
    }

    #[expect(
        clippy::float_cmp,
        reason = "exact rounded coordinates are ordered by their stored binary64 values to keep emitted topology deterministic"
    )]
    #[expect(
        clippy::indexing_slicing,
        reason = "departure indexes are span IDs in the same partition used to build the local topology"
    )]
    fn compare_departures(&self, origin: NodeId, left: usize, right: usize) -> Ordering {
        let departure = |index: usize| {
            let span = self.spans[index];
            debug_assert!(span.start == origin || span.end == origin);
            let source = self.unique[span.unique as usize];
            if span.start == origin {
                (source.start, source.end)
            } else {
                (source.end, source.start)
            }
        };
        // An atom follows its representative source's increasing parameter.
        let (a, b) = departure(left);
        let (c, d) = departure(right);
        let upper = |start: XY, end: XY| end.y > start.y || (end.y == start.y && end.x > start.x);
        let halves = upper(c, d).cmp(&upper(a, b));
        if halves != Ordering::Equal {
            return halves;
        }
        let sign = stored_cross_sign(a, b, c, d);
        debug_assert!(
            left == right || sign != ExactSign::Zero,
            "distinct atoms share a departure ray"
        );
        match sign {
            ExactSign::Positive => Ordering::Less,
            ExactSign::Negative => Ordering::Greater,
            ExactSign::Zero => Ordering::Equal,
        }
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "NodeIds and edge IDs come from this partition; checked CSR degree prefixes bound every topology row and cursor"
    )]
    pub(in crate::geometry) fn local_topology(&self) -> Result<(IndexedTopology, Vec<u32>)> {
        let vertices = self.node_count();
        let slots = IndexedTopology::admit_counts(vertices, self.spans.len())?;
        let mut topology = IndexedTopology::default();
        let vertex_capacity = vertices.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
        topology.starts.reserve(vertex_capacity);
        topology.starts.resize(vertex_capacity, 0);
        for span in &self.spans {
            let start = span.start.0.wrapping_add(1) as usize;
            topology.starts[start] = topology.starts[start]
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
            let end = span.end.0.wrapping_add(1) as usize;
            topology.starts[end] = topology.starts[end]
                .checked_add(1)
                .ok_or(ResourceError::SizeOverflow)?;
        }
        let mut total = 0_u32;
        for offset in topology.starts.iter_mut().skip(1) {
            total = total
                .checked_add(*offset)
                .ok_or(ResourceError::SizeOverflow)?;
            *offset = total;
        }
        let mut cursor = Vec::with_capacity(vertices);
        cursor.extend(topology.starts.iter().take(vertices).copied());
        let mut edge_of_slot = vec![0; slots];
        for (edge, span) in self.spans.iter().enumerate() {
            for node in [span.start, span.end] {
                let node = node.0 as usize;
                edge_of_slot[cursor[node] as usize] = edge as u32;
                cursor[node] = cursor[node].wrapping_add(1);
            }
        }
        for (node, &[start, end]) in topology.starts.array_windows::<2>().enumerate() {
            let range = start as usize..end as usize;
            // Degree-two nodes are ordered too: visibility wedges read the
            // angular origin.
            let row = &mut edge_of_slot[range];
            if let [left, right] = row {
                if self.compare_departures(NodeId(node as u32), *left as usize, *right as usize)
                    == Ordering::Greater
                {
                    std::mem::swap(left, right);
                }
            } else {
                row.sort_unstable_by(|&left, &right| {
                    self.compare_departures(NodeId(node as u32), left as usize, right as usize)
                });
            }
        }
        topology.targets.reserve(slots);
        for (node, &[start, end]) in topology.starts.array_windows::<2>().enumerate() {
            let range = start as usize..end as usize;
            for &edge in &edge_of_slot[range] {
                let span = self.spans[edge as usize];
                topology.targets.push(if span.start.0 == node as u32 {
                    span.end.0
                } else {
                    span.start.0
                });
            }
        }
        topology.owners.reserve(slots);
        topology.twins.reserve(slots);
        topology.component_of.reserve(vertices);
        topology.face_of.reserve(slots);
        topology
            .face_starts
            .reserve(slots.checked_add(1).ok_or(ResourceError::SizeOverflow)?);
        topology.face_slots.reserve(slots);
        let mut scratch = IndexedTopologyScratch::default();
        scratch.component_stack.reserve(vertices);
        scratch.successor.reserve(slots);
        scratch.first_slot.reserve(self.spans.len());
        topology.finish(&edge_of_slot, &mut scratch);
        Ok((topology, edge_of_slot))
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "component IDs are densely assigned by IndexedTopology::finish and minima is appended in that same order"
    )]
    fn component_minima(&self, topology: &IndexedTopology) -> Result<Vec<NodeId>> {
        let mut minima: Vec<NodeId> = Vec::new();
        for (node, &component) in topology.component_of.iter().enumerate() {
            let node = NodeId(node as u32);
            if component as usize == minima.len() {
                minima.push(node);
            } else {
                let minimum = &mut minima[component as usize];
                if self.compare_nodes(node, *minimum)? == Ordering::Less {
                    *minimum = node;
                }
            }
        }
        Ok(minima)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "root node and departure slots are IDs from this checked IndexedTopology"
    )]
    fn exterior_walk(
        &self,
        topology: &IndexedTopology,
        edges: &[u32],
        root: NodeId,
    ) -> Result<Option<u32>> {
        let range = topology.starts[root.0 as usize] as usize
            ..topology.starts[root.0 as usize + 1] as usize;
        let mut chosen = None;
        let mut has_upper = false;
        for slot in range {
            let target = NodeId(topology.targets[slot]);
            let upper = self.compare_axis(target, root, Axis::Y)? != Ordering::Less;
            // The leftward wedge follows the highest upper ray, or the highest
            // lower ray when every departure points down and right.
            if chosen.is_none() || (upper && !has_upper) {
                chosen = Some(slot);
                has_upper = upper;
                continue;
            }
            if upper != has_upper {
                continue;
            }
            let prior = chosen.expect("a departure was selected before comparing its successor");
            if self.compare_departures(root, edges[prior] as usize, edges[slot] as usize)
                == Ordering::Less
            {
                chosen = Some(slot);
            }
        }
        Ok(chosen.map(|slot| topology.face_of[slot]))
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "face IDs are bounded by the checked global walk count and links/labels are allocated to that count"
    )]
    fn global_faces(&self, topology: &IndexedTopology, edges: &[u32]) -> Result<GlobalFaces> {
        let minima = self.component_minima(topology)?;
        let walks = topology.face_starts.len().saturating_sub(1);
        let size = walks.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
        u32::try_from(size).map_err(|_| ResourceError::SizeOverflow)?;
        let mut links = Vec::with_capacity(size);
        links.extend(0..size as u32);
        let mut component_hosts = Vec::with_capacity(minima.len());
        // Each component's host is the face just east of the first span a
        // leftward ray from its minimum meets. One component has no other
        // component to meet: it lies in the unbounded face.
        let hits = if minima.len() == 1 {
            let none = vec![None];
            none
        } else {
            let mut queries = Vec::with_capacity(minima.len());
            queries.extend(minima.iter().map(|root| root.0));
            shoot_left(
                &self.points,
                self.spans.len(),
                |index| [self.spans[index].start.0, self.spans[index].end.0],
                &queries,
            )?
        };
        for (&root, hit) in minima.iter().zip(hits) {
            let host = hit.map_or(0, |span| {
                self.contact_walk(topology, edges, span).wrapping_add(1)
            });
            component_hosts.push(host);
            if let Some(exterior) = self.exterior_walk(topology, edges, root)? {
                // Every link reaches a component with a strictly smaller
                // minimum x, so exterior links cannot form a cycle.
                links[exterior.wrapping_add(1) as usize] = host;
            }
        }
        for start in 1..size {
            let mut root = start as u32;
            while links[root as usize] != root {
                root = links[root as usize];
            }
            let mut node = start as u32;
            while links[node as usize] != root {
                let next = links[node as usize];
                links[node as usize] = root;
                node = next;
            }
        }
        let mut labels = vec![0; size];
        let mut count = 1_u32;
        for node in 1..size {
            if links[node] == node as u32 {
                labels[node] = count;
                count = count.wrapping_add(1);
            }
        }
        for host in &mut component_hosts {
            *host = labels[links[*host as usize] as usize];
        }
        let mut walk_faces = Vec::with_capacity(walks);
        walk_faces.extend(links[1..].iter().map(|&root| labels[root as usize]));
        Ok(GlobalFaces {
            walk_faces,
            component_hosts,
            count,
        })
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "dart and edge IDs are generated by local_topology and provenance rows are bounded by its checked CSR"
    )]
    fn cross_face_winding<T>(
        &self,
        topology: &IndexedTopology,
        edges: &[u32],
        source_rings: &[Option<u32>],
        winding: &mut [i64],
        slot: u32,
        direction: i64,
        state: &mut T,
        cross_ring: &mut impl FnMut(&mut [i64], u32, i64, &mut T) -> Result<()>,
    ) -> Result<()> {
        let slot = slot as usize;
        let edge = edges[slot] as usize;
        let backward = topology.owners[slot] != self.spans[edge].start.0;
        for &(source, reversed) in self.owners(edge) {
            if let Some(ring) = source_rings[source as usize] {
                let direction = if reversed ^ backward {
                    direction
                } else {
                    direction.wrapping_neg()
                };
                cross_ring(winding, ring, direction, state)?;
            }
        }
        Ok(())
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "ring IDs passed to the visit callback are admitted by source staging and winding is sized from ring_count"
    )]
    fn visit_face_windings(
        &self,
        topology: &IndexedTopology,
        edges: &[u32],
        faces: &GlobalFaces,
        source_rings: &[Option<u32>],
        ring_count: usize,
        mut visit: impl FnMut(u32, &[i64]) -> Result<()>,
    ) -> Result<()> {
        let mut state = ();
        self.visit_face_windings_with_hook(
            topology,
            edges,
            faces,
            source_rings,
            ring_count,
            &mut state,
            |winding, ring, direction, ()| {
                let index = ring as usize;
                winding[index] = winding[index].wrapping_add(direction);
                Ok(())
            },
            |face, winding, ()| visit(face, winding),
        )
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "walk, face, twin, and ring IDs index arrays sized from checked topology and source counts"
    )]
    fn visit_face_windings_with_hook<T>(
        &self,
        topology: &IndexedTopology,
        edges: &[u32],
        faces: &GlobalFaces,
        source_rings: &[Option<u32>],
        ring_count: usize,
        state: &mut T,
        mut cross_ring: impl FnMut(&mut [i64], u32, i64, &mut T) -> Result<()>,
        mut visit: impl FnMut(u32, &[i64], &T) -> Result<()>,
    ) -> Result<()> {
        let count = faces.count as usize;
        let mut starts = Vec::new();
        let start_count = count.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
        starts.reserve(start_count);
        starts.resize(start_count, 0_usize);
        for &walk in &topology.face_of {
            let face = faces.walk_faces[walk as usize] as usize;
            let next = face.wrapping_add(1);
            starts[next] = starts[next].wrapping_add(1);
        }
        let mut total = 0_usize;
        for offset in starts.iter_mut().skip(1) {
            total = total.wrapping_add(*offset);
            *offset = total;
        }
        let mut cursor = Vec::with_capacity(count);
        cursor.extend_from_slice(&starts[..count]);
        let mut slots = vec![0_u32; edges.len()];
        for (slot, &walk) in topology.face_of.iter().enumerate() {
            let face = faces.walk_faces[walk as usize] as usize;
            slots[cursor[face]] = slot as u32;
            cursor[face] = cursor[face].wrapping_add(1);
        }
        let mut winding = vec![0_i64; ring_count];
        let seed_face = if let Some((face, seed)) = &self.winding_seed {
            if seed.len() != ring_count {
                return Err(GeometryErrorKind::Invalid(
                    "exact partition winding seed has the wrong ring count".into(),
                )
                .into());
            }
            if *face == 0 || *face as usize >= count {
                return Err(GeometryErrorKind::Invalid(
                    "exact partition winding seed is not an interior face".into(),
                )
                .into());
            }
            winding.copy_from_slice(seed);
            *face
        } else {
            0
        };
        let mut visited = Vec::with_capacity(count);
        visited.resize(count, false);
        let mut stack = Vec::with_capacity(count);
        visited[0] = true;
        if seed_face == 0 {
            visit(0, &winding, state)?;
        } else {
            // The omitted source arcs can change winding across the unbounded
            // local face. Seed inside the clip instead and keep that face out
            // of the propagation path.
            visited[seed_face as usize] = true;
            visit(seed_face, &winding, state)?;
        }
        stack.push((seed_face, starts[seed_face as usize], None));
        while let Some((face, next, incoming)) = stack.last_mut() {
            if *next == starts[(*face as usize).wrapping_add(1)] {
                if let Some(slot) = *incoming {
                    self.cross_face_winding(
                        topology,
                        edges,
                        source_rings,
                        &mut winding,
                        slot,
                        -1,
                        state,
                        &mut cross_ring,
                    )?;
                }
                stack.pop();
                continue;
            }
            let slot = slots[*next];
            *next = next.wrapping_add(1);
            let twin = topology.twins[slot as usize] as usize;
            let neighbor = faces.walk_faces[topology.face_of[twin] as usize];
            if visited[neighbor as usize] {
                continue;
            }
            self.cross_face_winding(
                topology,
                edges,
                source_rings,
                &mut winding,
                slot,
                1,
                state,
                &mut cross_ring,
            )?;
            visited[neighbor as usize] = true;
            visit(neighbor, &winding, state)?;
            stack.push((neighbor, starts[neighbor as usize], Some(slot)));
        }
        #[cfg(debug_assertions)]
        {
            // The window seed deliberately avoids face zero, whose winding is
            // not constant after source arcs outside the rectangle are omitted.
            // This can leave zero-area walks outside the seed's dual component;
            // every open face must still have a path within the window.
            let walk_has_area = |start: u32, end: u32| {
                let mut points = (start as usize..end as usize)
                    .map(|slot| self.nearest_xy(NodeId(topology.owners[slot])));
                let Some(first) = points.next() else {
                    return false;
                };
                let Some(second) = points.find(|&point| !same_point(point, first)) else {
                    return false;
                };
                points.any(|point| {
                    stored_orientation_sign(first.x, first.y, second.x, second.y, point.x, point.y)
                        != ExactSign::Zero
                })
            };
            let unvisited_open_faces = faces
                .walk_faces
                .iter()
                .enumerate()
                .filter_map(|(walk, &face)| (!visited[face as usize]).then_some((walk, face)))
                .filter_map(|(walk, face)| {
                    let (Some(&start), Some(&end)) = (
                        topology.face_starts.get(walk),
                        topology.face_starts.get(walk.wrapping_add(1)),
                    ) else {
                        unreachable!("global walk ordinal comes from this topology");
                    };
                    walk_has_area(start, end).then_some(face)
                })
                .collect::<Vec<_>>();
            debug_assert!(
                unvisited_open_faces.is_empty(),
                "seeded face-winding left open faces {unvisited_open_faces:?} unvisited"
            );
        }
        if let Some((_, seed)) = &self.winding_seed {
            debug_assert_eq!(&winding, seed);
        } else {
            debug_assert!(winding.iter().all(|&value| value == 0));
        }
        Ok(())
    }

    /// The walk on the east side of `span`: the departure from its upper
    /// endpoint keeps that side on its left.
    #[expect(
        clippy::indexing_slicing,
        reason = "the contact span comes from the same partition and its departure row comes from local_topology"
    )]
    fn contact_walk(&self, topology: &IndexedTopology, edges: &[u32], span: usize) -> u32 {
        let atom = self.spans[span];
        let source = self.unique[atom.unique as usize];
        let origin = if source.end.y > source.start.y {
            atom.end
        } else {
            atom.start
        };
        let slot = (topology.starts[origin.0 as usize] as usize
            ..topology.starts[origin.0 as usize + 1] as usize)
            .find(|&slot| edges[slot] as usize == span)
            .expect("contact atom has a departure slot");
        topology.face_of[slot]
    }
}

#[cfg(test)]
mod tests;
