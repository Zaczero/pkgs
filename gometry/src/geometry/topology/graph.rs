//! Source-owned planar topology over either ordinary XY or a geographic
//! longitude quotient.
//!
//! The graph is deliberately built from the source geometry, not from a
//! split output geometry. Geographic crossing segments are charted only long
//! enough to run the planar noder; their seam endpoints are identified by the
//! quotient key, and pole endpoints are identified independently of
//! longitude. Consequently a chart cut can be a graph node without becoming
//! a source boundary. Areal support stays in source polygons, where holes
//! remain exclusions rather than independent regions.

use super::staging::oriented_open_ring_direction;
use crate::collections::{HashMap, HashMapExt as _, HashSet, HashSetExt as _};
use crate::geometry::point_location::PolygonPointIndex;
use crate::geometry::segment_index::{EdgeYIndex, YStabbingIndex};
use crate::geometry::{
    CoordSeq, Coordinates as _, Dimension, Point, PointKey, Polygon, RectContact,
    SEGMENT_INDEX_MIN_PAIRS, Segment, SegmentIndex, Shape, XY, canonical_f64_bits, dimension,
    point_on_segment, same_point, segment_vs_rect, shared_segment_part,
};
use crate::heap_size::HeapSize;

/// The coordinate quotient used by a topology graph.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum TopologySpace {
    Cartesian,
    Geographic,
}

impl TopologySpace {
    const fn is_geographic(self) -> bool {
        matches!(self, Self::Geographic)
    }
}

/// Complete coordinate domain for a topology graph.
///
/// Geographic topology cannot exist without the native frame that defines its
/// quotient seam and poles. Keeping that frame in the variant removes the
/// former `Geographic` plus `None` combination.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum TopologyDomain {
    Cartesian,
    Geographic(crate::geographic::GeographicFrame),
}

impl TopologyDomain {
    pub(crate) const fn space(self) -> TopologySpace {
        match self {
            Self::Cartesian => TopologySpace::Cartesian,
            Self::Geographic(_) => TopologySpace::Geographic,
        }
    }

    pub(crate) const fn is_geographic(self) -> bool {
        matches!(self, Self::Geographic(_))
    }
}

/// Dense graph node identifier.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub(crate) struct TopologyNodeId(u32);

impl TopologyNodeId {
    pub(crate) const fn index(self) -> usize {
        self.0 as usize
    }
}

crate::heapless!(TopologyNodeId);

/// Source support carried by one edge before charting and noding.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SourceSupport {
    Line,
    /// A degenerate or unoriented polygon edge: boundary support only.
    AreaBoundary,
    /// An oriented polygon edge with an interior-side transition.
    AreaInterior {
        polygon: u32,
        interior_left: bool,
    },
}

/// One source edge before charting and noding.
#[derive(Clone, Copy, Debug)]
pub(crate) struct SourceEdge {
    pub(crate) start: Point,
    pub(crate) end: Point,
    pub(crate) segment: Segment,
    pub(crate) support: SourceSupport,
    pub(crate) component: u32,
    pub(crate) source_row: u32,
}

/// One explicit point source, including points from collapsed line members.
#[derive(Clone, Copy, Debug)]
pub(crate) struct SourcePoint {
    pub(crate) point: Point,
    pub(crate) collapsed_line: bool,
}

/// Source support attached to a quotient node. These flags intentionally keep
/// point support separate from line vertices and polygon ring vertices.
#[expect(
    clippy::struct_excessive_bools,
    reason = "a support set is four independent flags; a bitset would only rename them"
)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct NodeSupport {
    pub(crate) point: bool,
    pub(crate) line: bool,
    pub(crate) area_boundary: bool,
    pub(crate) line_boundary: bool,
    pub(crate) area_interior: AreaNodeSupport,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) enum AreaNodeSupport {
    #[default]
    None,
    Partial,
    Complete,
}

/// A noded edge. `sources` indexes [`TopologyGraph::source_edges`], while the
/// edge's endpoints are quotient node ids rather than raw coordinate keys.
#[derive(Clone, Debug)]
pub(crate) struct TopologyEdge {
    pub(crate) segment: Segment,
    pub(crate) sources: Box<[u32]>,
    pub(crate) is_boundary: bool,
    pub(crate) line_support: bool,
    pub(crate) area_interior_support: bool,
}

/// A quotient node retains the first source/chart coordinate as its display
/// coordinate. Equality is carried by the graph's key, so seam aliases can
/// retain their original sign without making two topology nodes.
#[derive(Clone, Copy, Debug)]
pub(crate) struct TopologyNode {
    pub(crate) point: XY,
    pub(crate) support: NodeSupport,
    location: TopologyLocation,
}

/// One source-owned topology graph for a complete geometry operand.
pub(crate) struct TopologyGraph {
    pub(crate) domain: TopologyDomain,
    pub(crate) effective_dimension: Option<Dimension>,
    pub(crate) boundary_dimension: Option<Dimension>,
    pub(crate) nodes: Box<[TopologyNode]>,
    pub(crate) edges: Box<[TopologyEdge]>,
    pub(crate) source_edges: Box<[SourceEdge]>,
    pub(crate) source_points: Box<[SourcePoint]>,
    /// Original polygonal support. It is never used as edge provenance, which
    /// keeps holes from becoming interior support and split seams from
    /// becoming source boundaries.
    pub(crate) area_support: Box<[AreaSupport]>,
    pub(crate) line_boundary: Box<[TopologyNodeId]>,
    pub(crate) is_collection: bool,
    pub(crate) lineal_overlap_collection: bool,
    /// Exact quotient-node/source-row line incidence for overlay point tests.
    line_source_nodes: HashSet<(TopologyKey, u32)>,
    node_lookup: HashMap<TopologyKey, TopologyNodeId>,
    support: TopologySupport,
}

impl HeapSize for TopologyGraph {
    fn heap_bytes(&self) -> usize {
        self.nodes.heap_bytes()
            + self.edges.heap_bytes()
            + self.source_edges.heap_bytes()
            + self.source_points.heap_bytes()
            + self.area_support.heap_bytes()
            + self.line_boundary.heap_bytes()
            + self.line_source_nodes.capacity()
                * std::mem::size_of::<(TopologyKey, u32)>()
            + self.support.heap_bytes()
            // `HashMap` has no crate-wide accounting implementation; charge
            // its key/value arrays conservatively without exposing its table
            // representation to the topology layer.
            + self.node_lookup.capacity()
                * (std::mem::size_of::<TopologyKey>()
                    + std::mem::size_of::<TopologyNodeId>())
    }
}

impl HeapSize for TopologyNode {
    fn heap_bytes(&self) -> usize {
        0
    }
}

impl HeapSize for TopologyEdge {
    fn heap_bytes(&self) -> usize {
        self.sources.heap_bytes()
    }
}

impl HeapSize for SourceEdge {
    fn heap_bytes(&self) -> usize {
        0
    }
}

impl HeapSize for SourcePoint {
    fn heap_bytes(&self) -> usize {
        0
    }
}

impl HeapSize for AreaSupport {
    fn heap_bytes(&self) -> usize {
        match self {
            Self::Cartesian { source } => source.heap_bytes(),
            Self::Geographic {
                source,
                shell,
                holes,
            } => source.heap_bytes() + shell.heap_bytes() + holes.heap_bytes(),
        }
    }
}

impl HeapSize for ChartRing {
    fn heap_bytes(&self) -> usize {
        self.points.heap_bytes() + self.edges.heap_bytes()
    }
}

impl TopologyGraph {
    /// Build one graph from one complete source geometry. The domain changes
    /// only point equivalence and source-edge charting; the noder remains the
    /// existing planar XY noder in both spaces.
    fn build(shape: &Shape, domain: TopologyDomain) -> crate::error::Result<Self> {
        let mut builder = GraphBuilder::new(domain);
        builder.collect(shape, 0)?;
        builder.finish(shape)
    }

    pub(crate) fn cartesian_many(shapes: &[&Shape]) -> crate::error::Result<Self> {
        let mut builder = GraphBuilder::new_many(TopologyDomain::Cartesian);
        for (source_row, shape) in shapes.iter().enumerate() {
            builder.collect(shape, source_row as u32)?;
        }
        builder.finish_many(shapes.iter().copied())
    }

    pub(crate) fn geographic_many(
        inputs: &crate::geographic::GeographicInputs<'_>,
    ) -> crate::error::Result<Self> {
        let mut builder = GraphBuilder::new_many(TopologyDomain::Geographic(inputs.frame()));
        for (source_row, view) in inputs.views().iter().enumerate() {
            builder.collect(view.shape(), source_row as u32)?;
        }
        builder.finish_many(inputs.views().iter().map(|view| view.shape()))
    }

    pub(crate) fn cartesian(shape: &Shape) -> crate::error::Result<Self> {
        Self::build(shape, TopologyDomain::Cartesian)
    }

    pub(crate) fn geographic(
        view: crate::geographic::ScalarGeographicView<'_>,
    ) -> crate::error::Result<Self> {
        Self::build(view.shape(), TopologyDomain::Geographic(view.frame()))
    }

    pub(crate) fn node_for(&self, point: XY) -> Option<TopologyNodeId> {
        self.node_lookup.get(&self.key(point)).copied()
    }

    /// The final source location of a graph node. Graph construction resolves
    /// this after face condensation and collection-wide boundary parity.
    pub(crate) fn node_location(&self, point: XY) -> Option<TopologyLocation> {
        let node = self.node_for(point)?;
        let location = self.nodes[node.index()].location;
        debug_assert_eq!(location, self.locate_point(point));
        Some(location)
    }

    #[cfg(test)]
    pub(crate) fn equivalent(&self, left: XY, right: XY) -> bool {
        self.key(left) == self.key(right)
    }

    pub(super) fn has_line_boundary(&self, point: XY) -> bool {
        self.node_for(point)
            .is_some_and(|id| self.nodes[id.index()].support.line_boundary)
    }

    pub(crate) fn has_area_boundary(&self, point: XY) -> bool {
        self.node_for(point)
            .is_some_and(|id| self.nodes[id.index()].support.area_boundary)
            || self
                .support
                .for_each_edge_at(self.domain, &self.edges, point, |edge| {
                    if edge.is_boundary {
                        std::ops::ControlFlow::Break(())
                    } else {
                        std::ops::ControlFlow::Continue(())
                    }
                })
                .is_some()
    }

    pub(crate) fn contains_area_interior(&self, point: XY) -> bool {
        if self.node_for(point).is_some_and(|id| {
            self.nodes[id.index()].support.area_interior == AreaNodeSupport::Complete
        }) {
            return true;
        }
        self.for_each_area_interior(point, |_| std::ops::ControlFlow::Break(()))
            .is_some()
    }

    pub(crate) fn for_each_area_interior<B>(
        &self,
        point: XY,
        visit: impl FnMut(u32) -> std::ops::ControlFlow<B>,
    ) -> Option<B> {
        self.support
            .for_each_area_interior(&self.area_support, point, visit)
    }

    pub(crate) const fn has_area_interior(&self) -> bool {
        self.support.area.has_area()
    }

    /// Decide whether a supported nondegenerate rectangle is covered by this
    /// graph. The corners establish closed-set membership; an indexed source
    /// boundary entering the open rectangle is the only remaining way for the
    /// rectangle to include exterior area.
    pub(crate) fn covers_rectangle(&self, bounds: crate::geometry::Bounds) -> Option<bool> {
        self.support.covers_rectangle(self, bounds)
    }

    pub(crate) fn covers_point(&self, point: XY) -> bool {
        self.locate_point(point) != TopologyLocation::Exterior
    }

    pub(super) fn has_point_support(&self, point: XY) -> bool {
        self.support.point_support.contains(&self.key(point))
    }

    pub(crate) fn has_point_support_any(&self) -> bool {
        !self.source_points.is_empty()
    }

    pub(crate) const fn edge_has_line_source(&self, edge: &TopologyEdge) -> bool {
        edge.line_support
    }

    pub(crate) fn edge_has_line_source_for_row(
        &self,
        edge: &TopologyEdge,
        source_row: u32,
    ) -> bool {
        edge.sources.iter().any(|&source| {
            let source = self.source_edges[source as usize];
            source.source_row == source_row && matches!(source.support, SourceSupport::Line)
        })
    }

    pub(crate) fn has_line_source_at_node(&self, point: XY, source_row: u32) -> bool {
        self.line_source_nodes
            .contains(&(self.key(point), source_row))
    }

    pub(crate) fn edge_has_area_source(&self, edge: &TopologyEdge) -> bool {
        edge.sources.iter().any(|&source| {
            matches!(
                self.source_edges[source as usize].support,
                SourceSupport::AreaBoundary | SourceSupport::AreaInterior { .. }
            )
        })
    }

    pub(crate) fn edge_has_area_source_for_row(
        &self,
        edge: &TopologyEdge,
        source_row: u32,
    ) -> bool {
        edge.sources.iter().any(|&source| {
            let source = self.source_edges[source as usize];
            source.source_row == source_row
                && matches!(
                    source.support,
                    SourceSupport::AreaBoundary | SourceSupport::AreaInterior { .. }
                )
        })
    }

    pub(crate) fn area_source_side_for_row(
        &self,
        edge: &TopologyEdge,
        start: XY,
        end: XY,
        source_row: u32,
    ) -> Option<bool> {
        let mut side = None;
        for &source in &edge.sources {
            let source = self.source_edges[source as usize];
            if source.source_row == source_row
                && matches!(
                    source.support,
                    SourceSupport::AreaBoundary | SourceSupport::AreaInterior { .. }
                )
            {
                let direction = segment_direction_matches(Segment { start, end }, source.segment);
                // Opposite traversals can be a zero-area spur, not a face boundary.
                if side.is_some_and(|previous| previous != direction) {
                    return None;
                }
                side = Some(direction);
            }
        }
        side
    }

    pub(crate) fn has_interior_support_other_than(&self, point: XY) -> bool {
        let key = self.key(point);
        self.support.point_support.len() > 1 || !self.support.point_support.contains(&key)
    }

    pub(crate) fn has_boundary_support_other_than(&self, point: XY) -> bool {
        let key = self.key(point);
        self.support.boundary_support.len() > 1 || !self.support.boundary_support.contains(&key)
    }

    pub(crate) fn has_line_support(&self, point: XY) -> bool {
        self.node_for(point)
            .is_some_and(|id| self.nodes[id.index()].support.line)
            || self
                .support
                .for_each_edge_at(self.domain, &self.edges, point, |edge| {
                    if edge.line_support {
                        std::ops::ControlFlow::Break(())
                    } else {
                        std::ops::ControlFlow::Continue(())
                    }
                })
                .is_some()
    }

    pub(crate) fn locate_point(&self, point: XY) -> TopologyLocation {
        self.locate_point_impl(point, true)
    }

    pub(crate) fn locate_point_for_overlay(&self, point: XY) -> TopologyLocation {
        self.locate_point_impl(point, false)
    }

    fn locate_point_impl(
        &self,
        point: XY,
        include_collapsed_line_points: bool,
    ) -> TopologyLocation {
        let mut area_interior = false;
        if let Some(node) = self.node_for(point) {
            match self.nodes[node.index()].support.area_interior {
                AreaNodeSupport::Complete => return TopologyLocation::Interior,
                AreaNodeSupport::Partial => {
                    area_interior = self.contains_area_interior(point);
                    if !area_interior {
                        return TopologyLocation::Boundary;
                    }
                },
                AreaNodeSupport::None => {},
            }
        }
        if area_interior || self.contains_area_interior(point) {
            return TopologyLocation::Interior;
        }
        if self
            .support
            .for_each_edge_at(self.domain, &self.edges, point, |edge| {
                if edge.area_interior_support {
                    std::ops::ControlFlow::Break(())
                } else {
                    std::ops::ControlFlow::Continue(())
                }
            })
            .is_some()
        {
            return TopologyLocation::Interior;
        }
        if self.has_area_boundary(point) {
            return TopologyLocation::Boundary;
        }
        if self.has_line_boundary(point) {
            return TopologyLocation::Boundary;
        }
        let point_support = if include_collapsed_line_points {
            self.has_point_support(point)
        } else {
            self.support
                .noncollapsed_point_support
                .contains(&self.key(point))
        };
        if self.has_line_support(point) || point_support {
            return TopologyLocation::Interior;
        }
        TopologyLocation::Exterior
    }

    pub(crate) fn locate_edge_piece(&self, segment: Segment) -> TopologyLocation {
        let midpoint = XY::new(
            f64::midpoint(segment.start.x, segment.end.x),
            f64::midpoint(segment.start.y, segment.end.y),
        );
        // A noded open piece can be only one ULP wide. Its real midpoint then
        // rounds to an endpoint, which may sit on the other graph's boundary.
        // A non-boundary endpoint still classifies that open piece exactly.
        if same_point(midpoint, segment.start) || same_point(midpoint, segment.end) {
            for point in [segment.start, segment.end] {
                let location = self.locate_point(point);
                if location != TopologyLocation::Boundary {
                    return location;
                }
            }
        }
        if self.contains_area_interior(midpoint) {
            return TopologyLocation::Interior;
        }
        if self
            .support
            .for_each_edge_at(self.domain, &self.edges, midpoint, |edge| {
                if edge.area_interior_support {
                    std::ops::ControlFlow::Break(())
                } else {
                    std::ops::ControlFlow::Continue(())
                }
            })
            .is_some()
        {
            return TopologyLocation::Interior;
        }
        if self.has_area_boundary(midpoint) {
            return TopologyLocation::Boundary;
        }
        if self.has_line_support(midpoint) {
            return TopologyLocation::Interior;
        }
        TopologyLocation::Exterior
    }

    pub(crate) fn key(&self, point: XY) -> TopologyKey {
        quotient_key(self.domain, point)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TopologyLocation {
    Interior,
    Boundary,
    Exterior,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) enum TopologyKey {
    Coordinate(PointKey),
    Seam(u64),
    Pole(bool),
}

#[expect(
    clippy::float_cmp,
    reason = "a pole or antimeridian is one specific representable coordinate, and the question is whether a vertex IS that coordinate; a tolerance would admit vertices merely near it"
)]
fn quotient_key(domain: TopologyDomain, point: XY) -> TopologyKey {
    if let TopologyDomain::Geographic(frame) = domain {
        if point.y == frame.pole() {
            return TopologyKey::Pole(true);
        }
        if point.y == -frame.pole() {
            return TopologyKey::Pole(false);
        }
        let longitude = frame
            .longitude()
            .canonicalize(point.x)
            .expect("validated geographic view admits every graph coordinate")
            .get();
        if longitude == -frame.longitude().half_period() {
            return TopologyKey::Seam(canonical_f64_bits(point.y));
        }
        return TopologyKey::Coordinate(PointKey::new(XY::new(longitude, point.y)));
    }
    TopologyKey::Coordinate(PointKey::new(point))
}

#[expect(
    clippy::float_cmp,
    reason = "a pole or antimeridian is one specific representable coordinate, and the question is whether a vertex IS that coordinate; a tolerance would admit vertices merely near it"
)]
fn point_on_graph_segment(domain: TopologyDomain, point: XY, segment: Segment) -> bool {
    if let TopologyDomain::Geographic(frame) = domain {
        if point.y.abs() == frame.pole() && (segment.start.y == point.y || segment.end.y == point.y)
        {
            return true;
        }
        let period = frame.longitude().period();
        let x = translated_longitude(&[segment.start, segment.end], point.x, period);
        point_on_segment(XY::new(x, point.y), segment.start, segment.end)
    } else {
        point_on_segment(point, segment.start, segment.end)
    }
}

fn segment_direction_matches(left: Segment, right: Segment) -> bool {
    let use_x = (right.end.x - right.start.x).abs() >= (right.end.y - right.start.y).abs();
    let (left_start, left_end, right_start, right_end) = if use_x {
        (left.start.x, left.end.x, right.start.x, right.end.x)
    } else {
        (left.start.y, left.end.y, right.start.y, right.end.y)
    };
    left_end.total_cmp(&left_start) == right_end.total_cmp(&right_start)
}

struct ChartEdge {
    segment: Segment,
    source: u32,
}

#[derive(Clone, Debug)]
pub(crate) enum AreaSupport {
    /// Cartesian containment and relation construction use the source polygon
    /// directly, so retaining a second XY ring representation is needless.
    Cartesian { source: Polygon },
    /// Geographic containment needs a seam-continuous chart and pole summary.
    Geographic {
        source: Polygon,
        shell: ChartRing,
        holes: Box<[ChartRing]>,
    },
}

impl AreaSupport {
    pub(crate) const fn source(&self) -> &Polygon {
        match self {
            Self::Cartesian { source } | Self::Geographic { source, .. } => source,
        }
    }

    pub(crate) fn geographic_rings(&self) -> (&ChartRing, &[ChartRing]) {
        match self {
            Self::Geographic { shell, holes, .. } => (shell, holes),
            Self::Cartesian { .. } => {
                unreachable!("Cartesian area support has no geographic chart")
            },
        }
    }
}

impl AsRef<Polygon> for AreaSupport {
    fn as_ref(&self) -> &Polygon {
        self.source()
    }
}

#[expect(
    clippy::struct_excessive_bools,
    reason = "pole and boundary contacts are four independent facts about a ring"
)]
#[derive(Clone, Debug)]
pub(crate) struct ChartRing {
    pub(crate) points: Box<[XY]>,
    pub(crate) north_pole: bool,
    pub(crate) south_pole: bool,
    north_boundary: bool,
    south_boundary: bool,
    period: f64,
    pole: f64,
    full_band: bool,
    south: f64,
    north: f64,
    min_x: f64,
    max_x: f64,
    min_y: f64,
    max_y: f64,
    edges: EdgeYIndex,
}

struct StagedChartRing {
    ring: ChartRing,
    segments: Vec<[Option<Segment>; 2]>,
}

struct CartesianAreaCandidate {
    polygon: u32,
    index: PolygonPointIndex,
}

impl HeapSize for CartesianAreaCandidate {
    fn heap_bytes(&self) -> usize {
        self.index.heap_bytes()
    }
}

#[derive(Clone, Copy)]
struct GeographicAreaCandidate {
    polygon: u32,
    min_x: f64,
    max_x: f64,
    y_min: f64,
    y_max: f64,
    chart_south: f64,
    chart_north: f64,
    north_pole: bool,
    south_pole: bool,
    full_band: bool,
}

impl HeapSize for GeographicAreaCandidate {
    fn heap_bytes(&self) -> usize {
        0
    }
}

enum AreaMembership {
    Cartesian {
        polygons: YStabbingIndex<CartesianAreaCandidate>,
    },
    Geographic {
        frame: crate::geographic::GeographicFrame,
        polygons: YStabbingIndex<GeographicAreaCandidate>,
    },
}

impl AreaMembership {
    fn build(domain: TopologyDomain, areas: &[AreaSupport]) -> crate::error::Result<Self> {
        Ok(match domain {
            TopologyDomain::Cartesian => {
                let candidates = areas
                    .iter()
                    .enumerate()
                    .filter(|(_, support)| {
                        crate::geometry::relate::polygon_has_nondegenerate_area(support.source())
                    })
                    .map(|(polygon, support)| {
                        Ok(CartesianAreaCandidate {
                            polygon: polygon as u32,
                            index: PolygonPointIndex::build(support.source())?,
                        })
                    })
                    .collect::<crate::error::Result<Vec<_>>>()?;
                Self::Cartesian {
                    polygons: YStabbingIndex::build(candidates, |candidate| {
                        candidate.index.envelope().unwrap_or((
                            f64::INFINITY,
                            f64::NEG_INFINITY,
                            f64::INFINITY,
                            f64::NEG_INFINITY,
                        ))
                    })?,
                }
            },
            TopologyDomain::Geographic(frame) => {
                let candidates = areas
                    .iter()
                    .enumerate()
                    .filter_map(|(polygon, support)| {
                        let (shell, _) = support.geographic_rings();
                        if !crate::geometry::relate::polygon_has_nondegenerate_area(
                            support.source(),
                        ) && !shell.north_pole
                            && !shell.south_pole
                            && !shell.full_band
                        {
                            return None;
                        }
                        Some(GeographicAreaCandidate {
                            polygon: polygon as u32,
                            min_x: shell.min_x,
                            max_x: shell.max_x,
                            // Chart seam vertices can extend beyond the source
                            // ring's vertex latitude extrema.
                            y_min: if shell.south_pole {
                                -shell.pole
                            } else {
                                shell.min_y
                            },
                            y_max: if shell.north_pole {
                                shell.pole
                            } else {
                                shell.max_y
                            },
                            chart_south: shell.min_y,
                            chart_north: shell.max_y,
                            north_pole: shell.north_pole,
                            south_pole: shell.south_pole,
                            full_band: shell.full_band,
                        })
                    })
                    .collect();
                Self::Geographic {
                    frame,
                    // Longitude is periodic here, so an X-envelope on the
                    // index would reject a legitimate wrapped copy;
                    // `may_contain` owns that filter against all three copies.
                    polygons: YStabbingIndex::build(candidates, |candidate| {
                        (
                            f64::NEG_INFINITY,
                            f64::INFINITY,
                            candidate.y_min,
                            candidate.y_max,
                        )
                    })?,
                }
            },
        })
    }

    const fn has_area(&self) -> bool {
        match self {
            Self::Cartesian { polygons, .. } => polygons.len() != 0,
            Self::Geographic { polygons, .. } => polygons.len() != 0,
        }
    }

    fn for_each_area_interior<B>(
        &self,
        areas: &[AreaSupport],
        point: XY,
        mut visit: impl FnMut(u32) -> std::ops::ControlFlow<B>,
    ) -> Option<B> {
        match self {
            Self::Cartesian { polygons, .. } => {
                polygons.for_each_at_point(point.x, point.y, |candidate| {
                    if candidate.index.contains_xy_graph_oracle(point.x, point.y) {
                        visit(candidate.polygon)
                    } else {
                        std::ops::ControlFlow::Continue(())
                    }
                })
            },
            Self::Geographic {
                frame, polygons, ..
            } => {
                let longitude = frame
                    .longitude()
                    .canonicalize(point.x)
                    .expect("validated geographic point")
                    .get();
                let period = frame.longitude().period();
                let copies = [longitude - period, longitude, longitude + period];
                polygons.for_each_at_y(point.y, |candidate| {
                    if !candidate.may_contain(point.y, &copies, frame.pole(), period) {
                        return std::ops::ControlFlow::Continue(());
                    }
                    if polygon_contains_interior(
                        TopologySpace::Geographic,
                        &areas[candidate.polygon as usize],
                        point,
                    ) {
                        visit(candidate.polygon)
                    } else {
                        std::ops::ControlFlow::Continue(())
                    }
                })
            },
        }
    }
}

impl GeographicAreaCandidate {
    #[expect(
        clippy::float_cmp,
        reason = "a pole or antimeridian is one specific representable coordinate, and the question is whether a vertex IS that coordinate; a tolerance would admit vertices merely near it"
    )]
    fn may_contain(&self, y: f64, copies: &[f64; 3], pole: f64, period: f64) -> bool {
        if self.full_band
            || (self.north_pole && y > self.chart_north)
            || (self.south_pole && y < self.chart_south)
            || (y.abs() == pole && (self.north_pole || self.south_pole))
            || self.max_x - self.min_x >= period
        {
            return true;
        }
        copies.iter().any(|&x| self.min_x <= x && x <= self.max_x)
    }
}

impl HeapSize for AreaMembership {
    fn heap_bytes(&self) -> usize {
        match self {
            Self::Cartesian { polygons, .. } => polygons.heap_bytes(),
            Self::Geographic { polygons, .. } => polygons.heap_bytes(),
        }
    }
}

struct TopologySupport {
    edge_index: crate::geometry::SegmentIndex,
    area: AreaMembership,
    point_support: HashSet<TopologyKey>,
    noncollapsed_point_support: HashSet<TopologyKey>,
    boundary_support: HashSet<TopologyKey>,
}

impl TopologySupport {
    fn new(
        domain: TopologyDomain,
        edges: &[TopologyEdge],
        nodes: &[TopologyNode],
        source_points: &[SourcePoint],
        area: AreaMembership,
    ) -> crate::error::Result<Self> {
        let point_support = source_points
            .iter()
            .map(|source| quotient_key(domain, source.point.xy()))
            .collect();
        let noncollapsed_point_support = source_points
            .iter()
            .filter(|source| !source.collapsed_line)
            .map(|source| quotient_key(domain, source.point.xy()))
            .collect();
        let boundary_support = nodes
            .iter()
            .filter(|node| node.support.area_boundary || node.support.line_boundary)
            .map(|node| quotient_key(domain, node.point))
            .collect();
        Ok(Self {
            edge_index: crate::geometry::SegmentIndex::build_from_iter(
                edges.iter().map(|edge| edge.segment),
            )?,
            area,
            point_support,
            noncollapsed_point_support,
            boundary_support,
        })
    }

    #[expect(
        clippy::float_cmp,
        reason = "a pole or antimeridian is one specific representable coordinate, and the question is whether a vertex IS that coordinate; a tolerance would admit vertices merely near it"
    )]
    fn for_each_edge_at<B>(
        &self,
        domain: TopologyDomain,
        edges: &[TopologyEdge],
        point: XY,
        mut visit: impl FnMut(&TopologyEdge) -> std::ops::ControlFlow<B>,
    ) -> Option<B> {
        match domain {
            TopologyDomain::Geographic(frame) => {
                let longitude = frame
                    .longitude()
                    .canonicalize(point.x)
                    .expect("validated geographic point")
                    .get();
                let period = frame.longitude().period();
                let half_period = frame.longitude().half_period();
                let mut visit_copy = |x: f64, skip_full_period: bool| {
                    let query = Segment {
                        start: XY::new(x, point.y),
                        end: XY::new(x, point.y),
                    };
                    for candidate in self.edge_index.intersecting_candidates(query) {
                        if skip_full_period
                            && ((candidate.segment.start.x == -half_period
                                && candidate.segment.end.x == half_period)
                                || (candidate.segment.start.x == half_period
                                    && candidate.segment.end.x == -half_period))
                        {
                            continue;
                        }
                        let edge = &edges[candidate.ordinal];
                        if point_on_graph_segment(domain, point, edge.segment)
                            && let std::ops::ControlFlow::Break(value) = visit(edge)
                        {
                            return Some(value);
                        }
                    }
                    None
                };
                if longitude == -half_period {
                    visit_copy(longitude, false).or_else(|| visit_copy(longitude + period, true))
                } else {
                    visit_copy(longitude, false)
                }
            },
            TopologyDomain::Cartesian => {
                let query = Segment {
                    start: point,
                    end: point,
                };
                for candidate in self.edge_index.intersecting_candidates(query) {
                    let edge = &edges[candidate.ordinal];
                    if point_on_graph_segment(domain, point, edge.segment)
                        && let std::ops::ControlFlow::Break(value) = visit(edge)
                    {
                        return Some(value);
                    }
                }
                None
            },
        }
    }

    fn for_each_area_interior<B>(
        &self,
        areas: &[AreaSupport],
        point: XY,
        visit: impl FnMut(u32) -> std::ops::ControlFlow<B>,
    ) -> Option<B> {
        self.area.for_each_area_interior(areas, point, visit)
    }

    fn covers_rectangle(
        &self,
        graph: &TopologyGraph,
        bounds: crate::geometry::Bounds,
    ) -> Option<bool> {
        if !rectangle_query_supported(graph.domain, bounds) {
            return None;
        }
        let center = XY::new(
            f64::midpoint(bounds.minx(), bounds.maxx()),
            f64::midpoint(bounds.miny(), bounds.maxy()),
        );
        if !(bounds.minx() < center.x
            && center.x < bounds.maxx()
            && bounds.miny() < center.y
            && center.y < bounds.maxy())
        {
            return None;
        }
        if !self.area.has_area() {
            return Some(false);
        }
        if bounds
            .corners()
            .into_iter()
            .map(|[x, y]| XY::new(x, y))
            .any(|point| graph.locate_point(point) == TopologyLocation::Exterior)
        {
            return Some(false);
        }
        let probe = Segment {
            start: XY::new(bounds.minx(), bounds.miny()),
            end: XY::new(bounds.maxx(), bounds.maxy()),
        };
        for candidate in self.edge_index.intersecting_candidates(probe) {
            let edge = graph.edges.get(candidate.ordinal)?;
            if edge.is_boundary && segment_vs_rect(edge.segment, bounds) == RectContact::Open {
                return Some(false);
            }
        }
        // A line component can mark a point Interior while it lies in an
        // areal hole; covering a two-dimensional rectangle needs area
        // support, not merely generic point location.
        Some(
            graph.locate_point(center) != TopologyLocation::Exterior
                && graph.contains_area_interior(center),
        )
    }
}

fn rectangle_query_supported(domain: TopologyDomain, bounds: crate::geometry::Bounds) -> bool {
    if !bounds.minx().is_finite()
        || !bounds.maxx().is_finite()
        || !bounds.miny().is_finite()
        || !bounds.maxy().is_finite()
        || bounds.minx() >= bounds.maxx()
        || bounds.miny() >= bounds.maxy()
    {
        return false;
    }
    let TopologyDomain::Geographic(frame) = domain else {
        return true;
    };
    // A geographic rectangle touching the seam or a pole is not one planar
    // disk; leave those quotient singularities to the authoritative relation.
    let axis = frame.longitude();
    let canonical = |value: f64| {
        axis.canonicalize(value)
            .is_ok_and(|canonical| canonical.get().to_bits() == value.to_bits())
    };
    bounds.maxx() - bounds.minx() < axis.period()
        && bounds.minx() > -axis.half_period()
        && bounds.maxx() < axis.half_period()
        && bounds.miny() > -frame.pole()
        && bounds.maxy() < frame.pole()
        && canonical(bounds.minx())
        && canonical(bounds.maxx())
}

impl HeapSize for TopologySupport {
    fn heap_bytes(&self) -> usize {
        self.edge_index.heap_bytes()
            + self.area.heap_bytes()
            + self
                .point_support
                .capacity()
                .saturating_mul(std::mem::size_of::<TopologyKey>())
            + self
                .noncollapsed_point_support
                .capacity()
                .saturating_mul(std::mem::size_of::<TopologyKey>())
            + self
                .boundary_support
                .capacity()
                .saturating_mul(std::mem::size_of::<TopologyKey>())
    }
}

struct GraphBuilder {
    domain: TopologyDomain,
    source_edges: Vec<SourceEdge>,
    source_points: Vec<SourcePoint>,
    chart_edges: Vec<ChartEdge>,
    area_support: Vec<AreaSupport>,
    nodes: Vec<TopologyNode>,
    node_lookup: HashMap<TopologyKey, TopologyNodeId>,
    line_boundary: HashSet<TopologyKey>,
    next_component: u32,
}

impl GraphBuilder {
    fn new(domain: TopologyDomain) -> Self {
        Self {
            domain,
            source_edges: Vec::new(),
            source_points: Vec::new(),
            chart_edges: Vec::new(),
            area_support: Vec::new(),
            nodes: Vec::new(),
            node_lookup: HashMap::new(),
            line_boundary: HashSet::new(),
            next_component: 0,
        }
    }

    fn new_many(domain: TopologyDomain) -> Self {
        Self {
            domain,
            source_edges: Vec::new(),
            source_points: Vec::new(),
            chart_edges: Vec::new(),
            area_support: Vec::new(),
            nodes: Vec::new(),
            node_lookup: HashMap::new(),
            line_boundary: HashSet::new(),
            next_component: 0,
        }
    }

    fn collect(&mut self, shape: &Shape, source_row: u32) -> crate::error::Result<()> {
        match shape {
            Shape::Empty(..) => {},
            Shape::Point(point) => {
                self.push_source_point(*point);
            },
            Shape::MultiPoint(points) => {
                for point in points.points() {
                    self.push_source_point(point);
                }
            },
            Shape::LineString(line) => self.collect_line(line, source_row),
            Shape::MultiLineString(lines) => {
                for line in lines {
                    self.collect_line(line, source_row);
                }
            },
            Shape::Polygon(polygon) => self.collect_polygon(polygon, source_row)?,
            Shape::MultiPolygon(polygons) => {
                for polygon in polygons {
                    self.collect_polygon(polygon, source_row)?;
                }
            },
            Shape::GeometryCollection(parts) => {
                for part in parts {
                    self.collect(part, source_row)?;
                }
            },
        }
        Ok(())
    }

    const fn component(&mut self) -> u32 {
        let component = self.next_component;
        self.next_component = self.next_component.saturating_add(1);
        component
    }

    fn collect_line(&mut self, line: &CoordSeq, source_row: u32) {
        let component = self.component();
        let mut first = None;
        let mut last = None;

        let mut has_edge = false;
        for [start, end] in line.segment_pairs() {
            if same_point(start, end) {
                continue;
            }
            has_edge = true;
            if first.is_none() {
                first = Some(self.key(start.xy()));
            }
            last = Some(self.key(end.xy()));
            self.push_source_edge(SourceSupport::Line, component, source_row, start, end);
        }
        if has_edge {
            // OGC line boundaries are the mod-2 endpoints of the complete
            // collection, not a per-member union. Quotient keys make seam and
            // pole aliases participate in the same parity fold.
            self.toggle_line_boundary(first.expect("a non-empty line has a first edge"));
            self.toggle_line_boundary(last.expect("a non-empty line has a last edge"));
        } else {
            // A collapsed line has point interior support, not a line
            // boundary. Keep every source coordinate and its axes.
            for point in line.iter_coords() {
                self.push_collapsed_line_point(point);
            }
        }
    }

    fn collect_polygon(&mut self, polygon: &Polygon, source_row: u32) -> crate::error::Result<()> {
        // A polygon whose every ring is collinear encloses nothing, so it IS
        // its linework: staging it as area support would give it an interior
        // it does not have, and a boundary its rings do not bound. Routing it
        // here makes it relate exactly as the equivalent closed line, which is
        // the one rule a caller can predict.
        // Cartesian only: a ring collinear in lon/lat can still bound a polar
        // cap on the sphere, where "encloses nothing" is not a planar test.
        if !self.domain.space().is_geographic()
            && polygon
                .rings()
                .all(|ring| crate::geometry::ring_is_collinear(ring.coords()))
        {
            for ring in polygon.rings() {
                self.collect_line(ring.coords(), source_row);
            }
            return Ok(());
        }
        let mut staged_rings = Vec::new();
        let support = if self.domain.space().is_geographic() {
            let StagedChartRing {
                ring: shell,
                segments: shell_segments,
            } = self.chart_ring(polygon.shell.coords())?;
            staged_rings.push(shell_segments);
            let mut holes = Vec::with_capacity(polygon.holes.len());
            for hole in polygon.holes.iter() {
                let StagedChartRing { ring, segments } = self.chart_ring(hole.coords())?;
                holes.push(ring);
                staged_rings.push(segments);
            }
            AreaSupport::Geographic {
                source: polygon.clone(),
                shell,
                holes: holes.into_boxed_slice(),
            }
        } else {
            AreaSupport::Cartesian {
                source: polygon.clone(),
            }
        };
        self.area_support.push(support);
        let component = self.component();
        let area_polygon = (self.area_support.len() - 1) as u32;
        for (ring_index, (ring, is_hole)) in std::iter::once((&polygon.shell, false))
            .chain(polygon.holes.iter().map(|ring| (ring, true)))
            .enumerate()
        {
            let staged_segments = staged_rings.get(ring_index);
            for point in ring.iter_coords() {
                self.ensure_node(point.xy(), |support| support.area_boundary = true);
            }
            let interior_left = oriented_open_ring_direction(ring.coords(), is_hole);
            for (index, [start, end]) in ring.segment_pairs().enumerate() {
                if same_point(start, end) {
                    continue;
                }
                self.push_source_edge_charted(
                    interior_left.map_or(SourceSupport::AreaBoundary, |interior_left| {
                        SourceSupport::AreaInterior {
                            polygon: area_polygon,
                            interior_left,
                        }
                    }),
                    component,
                    source_row,
                    start,
                    end,
                    staged_segments.as_ref().map_or_else(
                        || {
                            chart_segments(self.domain, Segment {
                                start: start.xy(),
                                end: end.xy(),
                            })
                        },
                        |segments| segments[index],
                    ),
                );
            }
        }
        Ok(())
    }

    fn push_source_point(&mut self, point: Point) {
        self.push_source_point_kind(point, false);
    }

    fn push_collapsed_line_point(&mut self, point: Point) {
        self.push_source_point_kind(point, true);
    }

    fn push_source_point_kind(&mut self, point: Point, collapsed_line: bool) {
        self.source_points.push(SourcePoint {
            point,
            collapsed_line,
        });
        self.ensure_node(point.xy(), |support| support.point = true);
    }

    fn push_source_edge(
        &mut self,
        support: SourceSupport,
        component: u32,
        source_row: u32,
        start: Point,
        end: Point,
    ) {
        self.push_source_edge_charted(
            support,
            component,
            source_row,
            start,
            end,
            chart_segments(self.domain, Segment {
                start: start.xy(),
                end: end.xy(),
            }),
        );
    }

    #[expect(
        clippy::large_types_passed_by_value,
        reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
    )]
    fn push_source_edge_charted(
        &mut self,
        support: SourceSupport,
        component: u32,
        source_row: u32,
        start: Point,
        end: Point,
        charts: [Option<Segment>; 2],
    ) {
        let segment = Segment {
            start: start.xy(),
            end: end.xy(),
        };
        let source = self.source_edges.len() as u32;
        self.source_edges.push(SourceEdge {
            start,
            end,
            segment,
            support,
            component,
            source_row,
        });
        for chart in charts.into_iter().flatten() {
            self.ensure_node(chart.start, |node| match support {
                SourceSupport::Line => node.line = true,
                SourceSupport::AreaBoundary | SourceSupport::AreaInterior { .. } => {
                    node.area_boundary = true;
                },
            });
            self.ensure_node(chart.end, |node| match support {
                SourceSupport::Line => node.line = true,
                SourceSupport::AreaBoundary | SourceSupport::AreaInterior { .. } => {
                    node.area_boundary = true;
                },
            });
            self.chart_edges.push(ChartEdge {
                segment: chart,
                source,
            });
        }
    }

    #[expect(
        clippy::float_cmp,
        reason = "a pole or antimeridian is one specific representable coordinate, and the question is whether a vertex IS that coordinate; a tolerance would admit vertices merely near it"
    )]
    fn chart_ring(&self, ring: &CoordSeq) -> crate::error::Result<StagedChartRing> {
        let mut points: Vec<XY> = Vec::with_capacity(ring.len());
        let mut segments = Vec::with_capacity(ring.len().saturating_sub(1));
        for [start, end] in ring.segment_pairs() {
            let charts = chart_segments(self.domain, Segment {
                start: start.xy(),
                end: end.xy(),
            });
            segments.push(charts);
            for chart in charts.into_iter().flatten() {
                let period = match self.domain {
                    TopologyDomain::Cartesian => 1.0,
                    TopologyDomain::Geographic(frame) => frame.longitude().period(),
                };
                let chart_start = lift_chart_longitude(chart.start, points.last().copied(), period);
                if points.last().copied() != Some(chart_start) {
                    points.push(chart_start);
                }
                let chart_end = lift_chart_longitude(chart.end, points.last().copied(), period);
                if points.last().copied() != Some(chart_end) {
                    points.push(chart_end);
                }
            }
        }
        if points.len() > 1 && same_point(points[0], *points.last().expect("non-empty ring")) {
            points.pop();
        }
        let (min_x, max_x) = points
            .iter()
            .fold((f64::INFINITY, f64::NEG_INFINITY), |(min, max), point| {
                (min.min(point.x), max.max(point.x))
            });
        let ys: Vec<f64> = points.iter().map(|point| point.y).collect();
        let frame_pole = match self.domain {
            TopologyDomain::Cartesian => f64::INFINITY,
            TopologyDomain::Geographic(frame) => frame.pole(),
        };
        let north_boundary = points.iter().any(|point| point.y == frame_pole);
        let south_boundary = points.iter().any(|point| point.y == -frame_pole);
        let min_y = points
            .iter()
            .map(|point| point.y)
            .fold(f64::INFINITY, f64::min);
        let max_y = points
            .iter()
            .map(|point| point.y)
            .fold(f64::NEG_INFINITY, f64::max);
        let summary = match self.domain {
            TopologyDomain::Cartesian => None,
            TopologyDomain::Geographic(frame) => Some(
                crate::geographic::RingSummary::try_new(
                    frame,
                    &crate::geographic::GeographicRing::from_ring(
                        &crate::geometry::Ring::from_closed(ring.clone()),
                    ),
                )
                .expect("validated geographic ring has an exact summary"),
            ),
        };
        let north_pole = summary.is_some_and(|summary| {
            matches!(summary.turn, crate::geographic::RingTurn::Full(_))
                && summary.south.get() > 0.0
        });
        let south_pole = summary.is_some_and(|summary| {
            matches!(summary.turn, crate::geographic::RingTurn::Full(_))
                && summary.north.get() < 0.0
        });
        Ok(StagedChartRing {
            ring: ChartRing {
                points: points.into_boxed_slice(),
                north_pole,
                south_pole,
                north_boundary,
                south_boundary,
                period: match self.domain {
                    TopologyDomain::Cartesian => 0.0,
                    TopologyDomain::Geographic(frame) => frame.longitude().period(),
                },
                pole: frame_pole,
                full_band: summary.is_some_and(|summary| {
                    matches!(
                        summary.domain,
                        crate::geographic::RingDomain::FullLongitude(_)
                    ) && summary.south.get() < summary.north.get()
                }),
                south: summary.map_or(f64::NEG_INFINITY, |summary| summary.south.get()),
                north: summary.map_or(f64::INFINITY, |summary| summary.north.get()),
                min_x,
                max_x,
                min_y,
                max_y,
                edges: EdgeYIndex::build_closed(&ys)?,
            },
            segments,
        })
    }

    fn key(&self, point: XY) -> TopologyKey {
        quotient_key(self.domain, point)
    }

    fn ensure_node(&mut self, point: XY, update: impl FnOnce(&mut NodeSupport)) -> TopologyNodeId {
        let key = self.key(point);
        if let Some(&id) = self.node_lookup.get(&key) {
            update(&mut self.nodes[id.index()].support);
            return id;
        }
        let id = TopologyNodeId(self.nodes.len() as u32);
        let mut support = NodeSupport::default();
        update(&mut support);
        self.node_lookup.insert(key, id);
        self.nodes.push(TopologyNode {
            point,
            support,
            location: TopologyLocation::Exterior,
        });
        id
    }

    fn toggle_line_boundary(&mut self, key: TopologyKey) {
        if !self.line_boundary.insert(key) {
            self.line_boundary.remove(&key);
        }
    }

    fn finish(self, shape: &Shape) -> crate::error::Result<TopologyGraph> {
        self.finish_dimensions(
            dimension(shape),
            matches!(shape, Shape::GeometryCollection(_)),
            shape_has_polygonal_members(shape),
        )
    }

    fn finish_many<'s>(
        self,
        shapes: impl IntoIterator<Item = &'s Shape>,
    ) -> crate::error::Result<TopologyGraph> {
        let mut effective_dimension = None;
        let mut has_polygonal_members = false;
        for shape in shapes {
            effective_dimension = match (effective_dimension, dimension(shape)) {
                (current, None) => current,
                (None, next) => next,
                (Some(current), Some(next)) => Some(current.max(next)),
            };
            has_polygonal_members |= shape_has_polygonal_members(shape);
        }
        self.finish_dimensions(effective_dimension, true, has_polygonal_members)
    }

    fn finish_dimensions(
        mut self,
        declared_dimension: Option<Dimension>,
        is_collection: bool,
        has_polygonal_members: bool,
    ) -> crate::error::Result<TopologyGraph> {
        // A shape declares surface dimension for holding a polygon at all, but
        // a polygon that encloses nothing was staged as linework and left no
        // area support behind. Reading the dimension back off what was staged
        // keeps that shape from claiming a two-dimensional interior, and stays
        // right in the geographic domain, where a ring collinear in lon/lat can
        // still bound a polar cap and therefore does stage area support.
        let effective_dimension = match declared_dimension {
            Some(Dimension::Surface) if self.area_support.is_empty() => {
                (!self.source_edges.is_empty()).then_some(Dimension::Curve)
            },
            other => other,
        };
        let (atomic, provenance) = crate::geometry::overlay::self_node_segments_sourced(
            self.chart_edges.iter().map(|edge| edge.segment),
        )?;
        let mut bundles: HashMap<EdgeBundleKey, EdgeBundle> = HashMap::with_capacity(atomic.len());
        for (piece, owners) in atomic.into_iter().zip(provenance.iter()) {
            if same_point(piece.start, piece.end) {
                continue;
            }
            let start = self.ensure_node(piece.start, |_| {});
            let end = self.ensure_node(piece.end, |_| {});
            let (key, segment, piece_reversed) = edge_bundle_key(self.domain, start, end, piece);
            let bundle = bundles
                .entry(key)
                .or_insert_with(|| EdgeBundle::new(segment));
            for &(chart_index, reversed) in owners {
                let chart = &self.chart_edges[chart_index as usize];
                let source = self.source_edges[chart.source as usize];
                if !bundle.sources.contains(&chart.source) {
                    bundle.sources.push(chart.source);
                }
                match source.support {
                    SourceSupport::Line | SourceSupport::AreaBoundary => {},
                    SourceSupport::AreaInterior {
                        polygon,
                        interior_left,
                    } => {
                        // The noding provenance already says whether this
                        // piece runs against its source; the direction is
                        // combinatorial, never a projection.
                        let reversed = reversed ^ piece_reversed;
                        let direction = if interior_left == reversed { -1 } else { 1 };
                        if let Some((_, net)) = bundle
                            .area_transitions
                            .iter_mut()
                            .find(|(id, _)| *id == polygon)
                        {
                            *net += direction;
                        } else {
                            bundle.area_transitions.push((polygon, direction));
                        }
                    },
                }
            }
        }

        let mut bundles: Vec<_> = bundles.into_iter().collect();
        bundles.sort_unstable_by_key(|(key, _)| *key);
        let mut edges = Vec::with_capacity(bundles.len());
        let mut bundle_endpoints = Vec::with_capacity(bundles.len());
        let mut area_transitions = Vec::with_capacity(bundles.len());
        for (key, bundle) in bundles {
            let line_support = bundle.sources.iter().any(|&source| {
                matches!(
                    self.source_edges[source as usize].support,
                    SourceSupport::Line
                )
            });
            let degenerate_area_boundary = bundle.area_transitions.is_empty()
                && bundle.sources.iter().any(|&source| {
                    matches!(
                        self.source_edges[source as usize].support,
                        SourceSupport::AreaBoundary | SourceSupport::AreaInterior { .. }
                    )
                });
            edges.push(TopologyEdge {
                segment: bundle.segment,
                sources: bundle.sources.into_boxed_slice(),
                is_boundary: degenerate_area_boundary,
                line_support,
                area_interior_support: false,
            });
            bundle_endpoints.push((key.start.0, key.end.0));
            area_transitions.push(bundle.area_transitions);
        }
        let area_membership = AreaMembership::build(self.domain, &self.area_support)?;
        if matches!(self.domain, TopologyDomain::Cartesian) {
            let arrangement_points: Vec<_> = self.nodes.iter().map(|node| node.point).collect();
            let arrangement = crate::geometry::Arrangement::from_indexed_edges(
                &arrangement_points,
                &bundle_endpoints,
            );
            condense_area_face_support(
                &self.area_support,
                &mut self.nodes,
                &mut edges,
                &area_transitions,
                &area_membership,
                &arrangement,
                &bundle_endpoints,
            );
        } else {
            let TopologyDomain::Geographic(frame) = self.domain else {
                unreachable!("Cartesian topology was handled above")
            };
            condense_geographic_area_face_support(
                frame,
                &self.area_support,
                &mut self.nodes,
                &mut edges,
                &area_transitions,
                &area_membership,
                &self.node_lookup,
            );
        }

        let line_boundary = self
            .line_boundary
            .iter()
            .filter_map(|key| self.node_lookup.get(key).copied())
            .collect::<Vec<_>>()
            .into_boxed_slice();
        for &id in &line_boundary {
            self.nodes[id.index()].support.line_boundary = true;
        }
        let mut line_source_nodes = HashSet::new();
        for edge in &edges {
            for &source in &edge.sources {
                let source = self.source_edges[source as usize];
                if !matches!(source.support, SourceSupport::Line) {
                    continue;
                }
                for point in [edge.segment.start, edge.segment.end] {
                    line_source_nodes.insert((self.key(point), source.source_row));
                }
            }
        }
        let lineal_overlap_collection = is_collection
            && !has_polygonal_members
            && has_cross_component_line_overlap(&self.source_edges)?;
        let boundary_dimension = if edges.iter().any(|edge| edge.is_boundary) {
            Some(Dimension::Curve)
        } else if !line_boundary.is_empty()
            || self.nodes.iter().any(|node| node.support.area_boundary)
        {
            Some(Dimension::Point)
        } else {
            None
        };
        edges.sort_by(|left, right| {
            left.sources
                .first()
                .cmp(&right.sources.first())
                .then_with(|| {
                    PointKey::new(left.segment.start).cmp(&PointKey::new(right.segment.start))
                })
                .then_with(|| {
                    PointKey::new(left.segment.end).cmp(&PointKey::new(right.segment.end))
                })
        });
        let support = TopologySupport::new(
            self.domain,
            &edges,
            &self.nodes,
            &self.source_points,
            area_membership,
        )?;
        let mut graph = TopologyGraph {
            domain: self.domain,
            effective_dimension,
            boundary_dimension,
            nodes: self.nodes.into_boxed_slice(),
            edges: edges.into_boxed_slice(),
            source_edges: self.source_edges.into_boxed_slice(),
            source_points: self.source_points.into_boxed_slice(),
            area_support: self.area_support.into_boxed_slice(),
            line_boundary,
            is_collection,
            lineal_overlap_collection,
            line_source_nodes,
            node_lookup: self.node_lookup,
            support,
        };
        let node_locations: Vec<_> = graph
            .nodes
            .iter()
            .map(|node| graph.locate_point(node.point))
            .collect();
        for (node, location) in graph.nodes.iter_mut().zip(node_locations) {
            node.location = location;
        }
        Ok(graph)
    }
}

fn chart_segments(domain: TopologyDomain, segment: Segment) -> [Option<Segment>; 2] {
    let TopologyDomain::Geographic(frame) = domain else {
        return [Some(segment), None];
    };
    let axis = frame.longitude();
    let start_x = axis
        .canonicalize(segment.start.x)
        .expect("validated longitude")
        .get();
    let mut delta = axis
        .shortest_edge(segment.start.x, segment.end.x)
        .expect("validated longitude")
        .delta(axis);
    if delta == 0.0
        && let Some(full_turn) = axis.full_turn_delta(segment.start.x, segment.end.x)
    {
        delta = full_turn;
    }
    let start = XY::new(start_x, segment.start.y);
    // Select the chart copy from the delta, but retain the source endpoint's
    // exact canonical spelling so adjacent ring segments share a node.
    let candidate_end_x = start_x + delta;
    let canonical_end_x = axis
        .canonicalize(segment.end.x)
        .expect("validated longitude")
        .get();
    let turns = ((candidate_end_x - canonical_end_x) / axis.period()).round();
    let end = XY::new(canonical_end_x + turns * axis.period(), segment.end.y);
    let half = axis.half_period();
    if end.x > half {
        let y = crate::geometry::antimeridian::crossing_latitude_in_frame(
            segment.end.point(),
            segment.start.point(),
            frame,
        );
        return [
            Some(Segment {
                start,
                end: XY::new(half, y),
            }),
            Some(Segment {
                start: XY::new(-half, y),
                end: XY::new(end.x - axis.period(), end.y),
            }),
        ];
    }
    if end.x < -half {
        let y = crate::geometry::antimeridian::crossing_latitude_in_frame(
            segment.start.point(),
            segment.end.point(),
            frame,
        );
        return [
            Some(Segment {
                start,
                end: XY::new(-half, y),
            }),
            Some(Segment {
                start: XY::new(half, y),
                end: XY::new(end.x + axis.period(), end.y),
            }),
        ];
    }
    [Some(Segment { start, end }), None]
}

struct EdgeBundle {
    segment: Segment,
    sources: Vec<u32>,
    area_transitions: Vec<(u32, i32)>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct RayGermKey {
    longitude: u64,
    dx: u64,
    dy: u64,
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
struct EdgeBundleKey {
    start: TopologyNodeId,
    end: TopologyNodeId,
    start_germ: RayGermKey,
    end_germ: RayGermKey,
}

impl EdgeBundle {
    const fn new(segment: Segment) -> Self {
        Self {
            segment,
            sources: Vec::new(),
            area_transitions: Vec::new(),
        }
    }
}

#[expect(
    clippy::float_cmp,
    reason = "a pole or antimeridian is one specific representable coordinate, and the question is whether a vertex IS that coordinate; a tolerance would admit vertices merely near it"
)]
fn ray_germ_key(domain: TopologyDomain, origin: XY, target: XY) -> RayGermKey {
    let longitude = match domain {
        TopologyDomain::Geographic(frame) if origin.y.abs() == frame.pole() => {
            canonical_f64_bits(origin.x)
        },
        TopologyDomain::Cartesian | TopologyDomain::Geographic(_) => 0,
    };
    RayGermKey {
        longitude,
        dx: canonical_f64_bits(target.x - origin.x),
        dy: canonical_f64_bits(target.y - origin.y),
    }
}

fn edge_bundle_key(
    domain: TopologyDomain,
    start: TopologyNodeId,
    end: TopologyNodeId,
    segment: Segment,
) -> (EdgeBundleKey, Segment, bool) {
    let start_germ = ray_germ_key(domain, segment.start, segment.end);
    let end_germ = ray_germ_key(domain, segment.end, segment.start);
    let reverse = (end, end_germ) < (start, start_germ);
    if !domain.is_geographic() {
        let (start, end, start_germ, end_germ) = if reverse {
            (end, start, end_germ, start_germ)
        } else {
            (start, end, start_germ, end_germ)
        };
        // Cartesian diagnostics retain the first noded source direction. The
        // canonical key only identifies coincident pieces; it must not choose
        // their public display orientation.
        return (
            EdgeBundleKey {
                start,
                end,
                start_germ,
                end_germ,
            },
            segment,
            false,
        );
    }
    if reverse {
        (
            EdgeBundleKey {
                start: end,
                end: start,
                start_germ: end_germ,
                end_germ: start_germ,
            },
            Segment {
                start: segment.end,
                end: segment.start,
            },
            true,
        )
    } else {
        (
            EdgeBundleKey {
                start,
                end,
                start_germ,
                end_germ,
            },
            segment,
            false,
        )
    }
}

#[derive(Clone, Copy)]
struct QuotientDart {
    edge: usize,
    from: TopologyNodeId,
    to: TopologyNodeId,
    segment: Segment,
}

#[expect(
    clippy::float_cmp,
    reason = "a pole or antimeridian is one specific representable coordinate, and the question is whether a vertex IS that coordinate; a tolerance would admit vertices merely near it"
)]
fn compare_quotient_rays(
    frame: crate::geographic::GeographicFrame,
    origin: XY,
    left: &QuotientDart,
    right: &QuotientDart,
) -> std::cmp::Ordering {
    if origin.y.abs() != frame.pole() {
        return crate::geometry::arrangement::compare_departures(
            origin,
            left.segment.end,
            left.edge as u32,
            right.segment.end,
            right.edge as u32,
        );
    }
    let axis = frame.longitude();
    let left_lon = axis
        .canonicalize(left.segment.start.x)
        .expect("validated pole longitude")
        .get();
    let right_lon = axis
        .canonicalize(right.segment.start.x)
        .expect("validated pole longitude")
        .get();
    let longitude_order = left_lon.total_cmp(&right_lon);
    let longitude_order = if origin.y == frame.pole() {
        longitude_order
    } else {
        longitude_order.reverse()
    };
    if longitude_order != std::cmp::Ordering::Equal {
        return longitude_order;
    }
    // Equal pole longitudes are rays in the pole quotient's tangent chart.
    // Reflecting pole distance preserves this walk's clockwise meridian
    // convention while the shared comparator certifies their exact order.
    let direction = |dart: &QuotientDart| {
        XY::new(
            dart.segment.end.x - dart.segment.start.x,
            -(origin.y - dart.segment.end.y).abs(),
        )
    };
    crate::geometry::arrangement::compare_departures(
        XY::new(0.0, 0.0),
        direction(left),
        left.edge as u32,
        direction(right),
        right.edge as u32,
    )
}

/// One step of union-find with path halving.
const fn quotient_root(parent: &mut [usize], mut node: usize) -> usize {
    while parent[node] != node {
        parent[node] = parent[parent[node]];
        node = parent[node];
    }
    node
}

/// The half-edge arrangement of a geographic quotient graph: every edge as a
/// pair of opposite darts, the darts around each node in ray order, and the
/// faces those two together induce.
///
/// The phases below share this state by necessity — the face walk reads the
/// rotation system, and the interior propagation reads both the faces and the
/// component each belongs to — so they are methods on it rather than free
/// functions threading seven buffers between them.
struct QuotientArrangement {
    darts: Vec<QuotientDart>,
    /// Darts leaving each node, in ray order around it.
    outgoing: Vec<Vec<usize>>,
    /// Each dart's index within its origin's `outgoing` row.
    position: Vec<usize>,
    /// The face each dart borders.
    face_of: Vec<usize>,
    /// Per face: its connected component, and the dart the walk entered by.
    face_component: Vec<usize>,
    face_seed: Vec<usize>,
}

impl QuotientArrangement {
    /// Stage every edge as two opposite darts, order the rotation system
    /// around each node, and walk the faces that induces.
    fn build(
        frame: crate::geographic::GeographicFrame,
        edges: &[TopologyEdge],
        origins: &[XY],
        node_id: impl Fn(XY) -> TopologyNodeId,
    ) -> Self {
        let mut darts = Vec::with_capacity(edges.len() * 2);
        let mut outgoing = vec![Vec::<usize>::new(); origins.len()];
        let mut parent: Vec<usize> = (0..origins.len()).collect();
        for (edge, data) in edges.iter().enumerate() {
            let start = node_id(data.segment.start);
            let end = node_id(data.segment.end);
            let forward = darts.len();
            darts.push(QuotientDart {
                edge,
                from: start,
                to: end,
                segment: data.segment,
            });
            darts.push(QuotientDart {
                edge,
                from: end,
                to: start,
                segment: Segment {
                    start: data.segment.end,
                    end: data.segment.start,
                },
            });
            outgoing[start.index()].push(forward);
            outgoing[end.index()].push(forward + 1);
            let left = quotient_root(&mut parent, start.index());
            let right = quotient_root(&mut parent, end.index());
            if left != right {
                parent[right] = left;
            }
        }
        let mut arrangement = Self {
            darts,
            outgoing,
            position: Vec::new(),
            face_of: Vec::new(),
            face_component: Vec::new(),
            face_seed: Vec::new(),
        };
        arrangement.sort_rotation_system(frame, origins);
        arrangement.walk_faces(&mut parent);
        arrangement
    }

    /// Order the darts around each node by the direction they leave it, and
    /// record where each one sits, which is what makes the face walk below a
    /// lookup rather than a search.
    fn sort_rotation_system(&mut self, frame: crate::geographic::GeographicFrame, origins: &[XY]) {
        let Self {
            darts,
            outgoing,
            position,
            ..
        } = self;
        for (node, row) in outgoing.iter_mut().enumerate() {
            let origin = origins[node];
            row.sort_by(|&left, &right| {
                compare_quotient_rays(frame, origin, &darts[left], &darts[right])
            });
        }
        *position = vec![0_usize; darts.len()];
        for row in outgoing.iter() {
            for (index, &dart) in row.iter().enumerate() {
                position[dart] = index;
            }
        }
    }

    /// Walk each face by repeatedly taking the dart before the twin in its
    /// origin's rotation: the standard half-edge face traversal.
    fn walk_faces(&mut self, parent: &mut [usize]) {
        self.face_of = vec![usize::MAX; self.darts.len()];
        for seed in 0..self.darts.len() {
            if self.face_of[seed] != usize::MAX {
                continue;
            }
            let face = self.face_component.len();
            self.face_component
                .push(quotient_root(parent, self.darts[seed].from.index()));
            self.face_seed.push(seed);
            let mut dart = seed;
            loop {
                self.face_of[dart] = face;
                let twin = dart ^ 1;
                let row = &self.outgoing[self.darts[dart].to.index()];
                let twin_position = self.position[twin];
                dart = row[(twin_position + row.len() - 1) % row.len()];
                if dart == seed {
                    break;
                }
                assert_eq!(
                    self.face_of[dart],
                    usize::MAX,
                    "quotient face walk must close"
                );
            }
        }
    }

    /// The face adjacency and one seed per component, handed to the shared
    /// labelling pass.
    fn face_interior(
        &self,
        polygons: &[AreaSupport],
        edges: &[TopologyEdge],
        transitions: &[Vec<(u32, i32)>],
        area_membership: &AreaMembership,
    ) -> Vec<bool> {
        let mut adjacency = vec![Vec::<(usize, usize)>::new(); self.face_component.len()];
        for edge in 0..edges.len() {
            let left = self.face_of[edge * 2];
            let right = self.face_of[edge * 2 + 1];
            if left != right {
                adjacency[left].push((right, edge));
                adjacency[right].push((left, edge));
            }
        }
        let mut seed_for_component = HashMap::new();
        for (face, &component) in self.face_component.iter().enumerate() {
            seed_for_component.entry(component).or_insert(face);
        }
        let seeds: Vec<FaceSeed> = seed_for_component
            .values()
            .map(|&face| {
                let dart = self.face_seed[face];
                let edge = self.darts[dart].edge;
                FaceSeed {
                    face,
                    segment: self.darts[dart].segment,
                    edge,
                    forward: dart == edge * 2,
                }
            })
            .collect();
        propagate_face_interior(
            self.face_component.len(),
            &adjacency,
            &seeds,
            polygons,
            transitions,
            area_membership,
        )
    }
}

fn condense_geographic_area_face_support(
    frame: crate::geographic::GeographicFrame,
    polygons: &[AreaSupport],
    nodes: &mut [TopologyNode],
    edges: &mut [TopologyEdge],
    transitions: &[Vec<(u32, i32)>],
    area_membership: &AreaMembership,
    node_lookup: &HashMap<TopologyKey, TopologyNodeId>,
) {
    if polygons.is_empty() || edges.is_empty() {
        return;
    }
    let domain = TopologyDomain::Geographic(frame);
    let origins: Vec<XY> = nodes.iter().map(|node| node.point).collect();
    let arrangement = QuotientArrangement::build(frame, edges, &origins, |point| {
        node_lookup[&quotient_key(domain, point)]
    });
    let face_interior = arrangement.face_interior(polygons, edges, transitions, area_membership);
    let (darts, outgoing, face_of) = (
        &arrangement.darts,
        &arrangement.outgoing,
        &arrangement.face_of,
    );
    // A quotient singularity can make both darts share one face. In that
    // case, recover the two source-side memberships instead of treating the
    // singular face as covering both sides.
    let edge_sides: Vec<Option<(bool, bool)>> = edges
        .iter()
        .enumerate()
        .map(|(edge, data)| {
            (face_of[edge * 2] == face_of[edge * 2 + 1]).then(|| {
                geographic_edge_side_support(
                    area_membership,
                    polygons,
                    data.segment,
                    &transitions[edge],
                )
            })
        })
        .collect();
    for (edge, data) in edges.iter_mut().enumerate() {
        let (left, right) = edge_sides[edge].unwrap_or((
            face_interior[face_of[edge * 2]],
            face_interior[face_of[edge * 2 + 1]],
        ));
        let degenerate_boundary = data.is_boundary;
        data.area_interior_support = left && right;
        data.is_boundary = left != right || (degenerate_boundary && !(left && right));
    }
    for (node, row) in outgoing.iter().enumerate() {
        if row.is_empty() {
            continue;
        }
        let mut any = false;
        let mut all = true;
        for &dart in row {
            let edge = darts[dart].edge;
            let interior =
                edge_sides[edge].map_or(face_interior[face_of[dart]], |(left, right)| {
                    if dart == edge * 2 { left } else { right }
                });
            any |= interior;
            all &= interior;
        }
        let degenerate_boundary = nodes[node].support.area_boundary;
        nodes[node].support.area_interior = if all {
            AreaNodeSupport::Complete
        } else if any {
            AreaNodeSupport::Partial
        } else {
            AreaNodeSupport::None
        };
        nodes[node].support.area_boundary = !all && (any || degenerate_boundary);
    }
}

#[derive(Clone, Default)]
struct ActivePolygons {
    ids: HashSet<u32>,
    count: usize,
}

impl ActivePolygons {
    fn insert(&mut self, polygon: u32) {
        if self.ids.insert(polygon) {
            self.count += 1;
        }
    }

    fn set(&mut self, polygon: u32, inside: bool) {
        if inside {
            self.insert(polygon);
        } else if self.ids.remove(&polygon) {
            self.count -= 1;
        }
    }

    fn toggle(&mut self, polygon: u32) {
        if self.ids.remove(&polygon) {
            self.count -= 1;
        } else {
            self.ids.insert(polygon);
            self.count += 1;
        }
    }

    const fn count(&self) -> usize {
        self.count
    }

    const fn any(&self) -> bool {
        self.count != 0
    }
}

fn geographic_edge_side_support(
    area_membership: &AreaMembership,
    polygons: &[AreaSupport],
    segment: Segment,
    transitions: &[(u32, i32)],
) -> (bool, bool) {
    let midpoint = XY::new(
        f64::midpoint(segment.start.x, segment.end.x),
        f64::midpoint(segment.start.y, segment.end.y),
    );
    let mut left = ActivePolygons::default();
    let _: Option<()> = area_membership.for_each_area_interior(polygons, midpoint, |polygon| {
        left.insert(polygon);
        std::ops::ControlFlow::Continue(())
    });
    let mut right = left.clone();
    for &(polygon, net) in transitions {
        if net > 0 {
            left.set(polygon, true);
            right.set(polygon, false);
        } else if net < 0 {
            left.set(polygon, false);
            right.set(polygon, true);
        }
    }
    (left.any(), right.any())
}

/// Where a face-labelling walk starts: one face per connected component,
/// with the boundary segment whose midpoint seeds its membership.
struct FaceSeed {
    face: usize,
    segment: Segment,
    edge: usize,
    /// Whether `segment` runs along its edge's own direction.
    forward: bool,
}

/// Label every face inside or outside the area, by locating ONE seed face per
/// connected component and toggling membership across each edge the walk
/// crosses.
///
/// Both condensers land here. Only the geometry that produces the faces
/// differs — the planar one reads a prebuilt arrangement, the geographic one
/// walks the quotient half-edge structure it builds itself — while the
/// labelling is one algorithm and lives here once.
fn propagate_face_interior(
    face_count: usize,
    adjacency: &[Vec<(usize, usize)>],
    seeds: &[FaceSeed],
    polygons: &[AreaSupport],
    transitions: &[Vec<(u32, i32)>],
    area_membership: &AreaMembership,
) -> Vec<bool> {
    let mut face_interior = vec![false; face_count];
    let mut visited = vec![false; face_count];
    for seed in seeds {
        let midpoint = XY::new(
            f64::midpoint(seed.segment.start.x, seed.segment.end.x),
            f64::midpoint(seed.segment.start.y, seed.segment.end.y),
        );
        let mut membership = ActivePolygons::default();
        let _: Option<()> = area_membership.for_each_area_interior(polygons, midpoint, |polygon| {
            membership.insert(polygon);
            std::ops::ControlFlow::Continue(())
        });
        for &(polygon, net) in &transitions[seed.edge] {
            if net != 0 {
                membership.set(polygon, (net > 0) == seed.forward);
            }
        }
        let mut aggregate = membership.count();
        visited[seed.face] = true;
        face_interior[seed.face] = aggregate != 0;
        let mut stack: Vec<(usize, usize, Option<usize>)> = vec![(seed.face, 0, None)];
        while let Some((face, next, incoming)) = stack.pop() {
            if next == adjacency[face].len() {
                if let Some(edge) = incoming {
                    toggle_membership(
                        &mut membership,
                        &mut aggregate,
                        transitions[edge].as_slice(),
                    );
                }
                continue;
            }
            stack.push((face, next + 1, incoming));
            let (neighbor, edge) = adjacency[face][next];
            if visited[neighbor] {
                continue;
            }
            toggle_membership(
                &mut membership,
                &mut aggregate,
                transitions[edge].as_slice(),
            );
            visited[neighbor] = true;
            face_interior[neighbor] = aggregate != 0;
            stack.push((neighbor, 0, Some(edge)));
        }
    }
    face_interior
}

fn toggle_membership(
    membership: &mut ActivePolygons,
    aggregate: &mut usize,
    transitions: &[(u32, i32)],
) {
    for &(polygon, net) in transitions {
        if net == 0 {
            continue;
        }
        membership.toggle(polygon);
        *aggregate = membership.count();
    }
}

fn condense_area_face_support(
    polygons: &[AreaSupport],
    nodes: &mut [TopologyNode],
    edges: &mut [TopologyEdge],
    transitions: &[Vec<(u32, i32)>],
    area_membership: &AreaMembership,
    arrangement: &crate::geometry::Arrangement,
    bundle_endpoints: &[(u32, u32)],
) {
    if polygons.is_empty() || edges.is_empty() {
        return;
    }
    let has_area = polygons
        .iter()
        .any(|support| crate::geometry::relate::polygon_has_nondegenerate_area(support.source()));
    let mut edge_of_slot = vec![usize::MAX; arrangement.targets.len()];
    // Traverse the dual graph with one mutable polygon-membership set. Edge
    // provenance toggles only the polygons whose boundary is crossed, so face
    // labeling is linear after one seed classification per edge component.
    let mut adjacency = vec![Vec::new(); arrangement.faces.len()];
    for (edge, &(start, end)) in bundle_endpoints.iter().enumerate() {
        let slot = arrangement.slot(start, end);
        let twin = arrangement.twin_slot(slot);
        let left = arrangement.face_of[slot] as usize;
        let right = arrangement.face_of[twin] as usize;
        edge_of_slot[slot] = edge;
        edge_of_slot[twin] = edge;
        if left != right {
            adjacency[left].push((right, edge));
            adjacency[right].push((left, edge));
        }
    }
    let mut outer: Vec<Option<usize>> = vec![None; arrangement.component_count as usize];
    for (face, data) in arrangement.faces.iter().enumerate() {
        let candidate = &mut outer[data.component as usize];
        if candidate.is_none_or(|current| {
            data.area_sign
                .prefers_as_outer_face_than(arrangement.faces[current].area_sign)
        }) {
            *candidate = Some(face);
        }
    }
    let seeds: Vec<FaceSeed> = outer
        .into_iter()
        .flatten()
        .map(|face| {
            let slot = arrangement.face_slots[arrangement.face_starts[face] as usize] as usize;
            let segment = Segment {
                start: arrangement.points[arrangement.owners[slot] as usize],
                end: arrangement.points[arrangement.targets[slot] as usize],
            };
            let edge = edge_of_slot[slot];
            FaceSeed {
                face,
                segment,
                edge,
                forward: segment_direction_matches(segment, edges[edge].segment),
            }
        })
        .collect();
    let face_interior = propagate_face_interior(
        arrangement.faces.len(),
        &adjacency,
        &seeds,
        polygons,
        transitions,
        area_membership,
    );
    for (edge, &(start, end)) in edges.iter_mut().zip(bundle_endpoints) {
        let slot = arrangement.slot(start, end);
        let twin = arrangement.twin_slot(slot);
        let left = face_interior[arrangement.face_of[slot] as usize];
        let right = face_interior[arrangement.face_of[twin] as usize];
        edge.area_interior_support = left && right;
        edge.is_boundary = left != right;
    }
    for vertex in 0..arrangement.points.len() {
        let range = arrangement.starts[vertex] as usize..arrangement.starts[vertex + 1] as usize;
        let mut sectors = range.map(|slot| face_interior[arrangement.face_of[slot] as usize]);
        let Some(first) = sectors.next() else {
            continue;
        };
        let (mut any, mut all) = (first, first);
        for interior in sectors {
            any |= interior;
            all &= interior;
        }
        nodes[vertex].support.area_interior = if all {
            AreaNodeSupport::Complete
        } else if any {
            AreaNodeSupport::Partial
        } else {
            AreaNodeSupport::None
        };
        nodes[vertex].support.area_boundary =
            (!has_area && nodes[vertex].support.area_boundary) || any && !all;
    }
}

fn has_cross_component_line_overlap(edges: &[SourceEdge]) -> crate::error::Result<bool> {
    let lines: Vec<_> = edges
        .iter()
        .filter(|edge| matches!(edge.support, SourceSupport::Line))
        .collect();
    if lines.len().saturating_mul(lines.len().saturating_sub(1)) / 2 < SEGMENT_INDEX_MIN_PAIRS {
        return Ok(lines.iter().enumerate().any(|(index, left)| {
            lines[index + 1..].iter().any(|right| {
                left.component != right.component
                    && shared_segment_part(left.segment, right.segment)
                        .is_some_and(|(_, run)| !same_point(run[0], run[1]))
            })
        }));
    }
    let index = SegmentIndex::build_from_iter(lines.iter().map(|edge| edge.segment))?;
    Ok(lines.iter().enumerate().any(|(ordinal, left)| {
        index.intersecting_candidates(left.segment).any(|right| {
            right.ordinal > ordinal
                && left.component != lines[right.ordinal].component
                && shared_segment_part(left.segment, lines[right.ordinal].segment)
                    .is_some_and(|(_, run)| !same_point(run[0], run[1]))
        })
    }))
}

fn shape_has_polygonal_members(shape: &Shape) -> bool {
    match shape {
        Shape::Polygon(_) | Shape::MultiPolygon(_) => true,
        Shape::GeometryCollection(parts) => parts.iter().any(shape_has_polygonal_members),
        Shape::Empty(..)
        | Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_) => false,
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RingLocation {
    Inside,
    Boundary,
    Outside,
}

fn polygon_contains_interior(space: TopologySpace, polygon: &AreaSupport, point: XY) -> bool {
    if !space.is_geographic() {
        return polygon.source().contains_point(point.point());
    }
    let (shell, holes) = polygon.geographic_rings();
    if geographic_ring_location(shell, point) != RingLocation::Inside {
        return false;
    }
    holes
        .iter()
        .all(|hole| geographic_ring_location(hole, point) == RingLocation::Outside)
}

#[expect(
    clippy::float_cmp,
    reason = "a pole or antimeridian is one specific representable coordinate, and the question is whether a vertex IS that coordinate; a tolerance would admit vertices merely near it"
)]
fn geographic_ring_location(ring: &ChartRing, point: XY) -> RingLocation {
    if point.y == ring.pole || point.y == -ring.pole {
        let north = point.y == ring.pole;
        if (north && ring.north_boundary) || (!north && ring.south_boundary) {
            return RingLocation::Boundary;
        }
        let encloses = if north {
            ring.north_pole
        } else {
            ring.south_pole
        };
        return if encloses {
            RingLocation::Inside
        } else {
            RingLocation::Outside
        };
    }
    if ring.full_band {
        if point.y == ring.south || point.y == ring.north {
            return RingLocation::Boundary;
        }
        return if point.y > ring.south && point.y < ring.north {
            RingLocation::Inside
        } else {
            RingLocation::Outside
        };
    }
    if (ring.north_pole && point.y > ring.max_y) || (ring.south_pole && point.y < ring.min_y) {
        // Once a polar ring's parallel is passed on the pole side, every
        // longitude is in the same quotient face. The ordinary chart ray
        // cannot express that cap because its pole-side closure is implicit.
        return RingLocation::Inside;
    }
    let x = translated_longitude_bounds(ring.min_x, ring.max_x, point.x, ring.period);
    let query = XY::new(x, point.y);
    if chart_ring_edges_for_y(ring, query.y, |segment| {
        if point_on_segment(query, segment.start, segment.end) {
            std::ops::ControlFlow::Break(())
        } else {
            std::ops::ControlFlow::Continue(())
        }
    })
    .is_some()
    {
        return RingLocation::Boundary;
    }
    let mut inside = false;
    let _ = chart_ring_edges_for_y(ring, query.y, |segment| {
        if (segment.start.y > query.y) != (segment.end.y > query.y)
            && crate::geometry::ray_crossing_is_right(
                segment.start.x,
                segment.start.y,
                segment.end.x,
                segment.end.y,
                query.x,
                query.y,
            )
        {
            inside = !inside;
        }
        std::ops::ControlFlow::<()>::Continue(())
    });
    if ring.points.len() >= 3 && inside {
        RingLocation::Inside
    } else {
        RingLocation::Outside
    }
}

fn chart_ring_edges_for_y<B>(
    ring: &ChartRing,
    y: f64,
    mut visit: impl FnMut(Segment) -> std::ops::ControlFlow<B>,
) -> Option<B> {
    if let Some(edges) = ring.edges.dense_band_edges(y) {
        for &edge in edges {
            let edge = edge.as_usize();
            let next = if edge + 1 < ring.points.len() {
                edge + 1
            } else {
                0
            };
            if let std::ops::ControlFlow::Break(value) = visit(Segment {
                start: ring.points[edge],
                end: ring.points[next],
            }) {
                return Some(value);
            }
        }
        None
    } else {
        ring.edges.for_each_edge(y, |edge| {
            let edge = edge.as_usize();
            let next = if edge + 1 < ring.points.len() {
                edge + 1
            } else {
                0
            };
            visit(Segment {
                start: ring.points[edge],
                end: ring.points[next],
            })
        })
    }
}

fn lift_chart_longitude(point: XY, previous: Option<XY>, period: f64) -> XY {
    let Some(previous) = previous else {
        return point;
    };
    // Choose the longitude copy closest to the preceding chart point. This
    // also joins a seam alias after the preceding point has already been
    // lifted, and keeps exact full-period jumps
    // on one chart representative as the published splitter does.
    let turns = ((previous.x - point.x) / period).round();
    let x = point.x + period * turns;
    XY::new(x, point.y)
}

fn translated_longitude(chart: &[XY], longitude: f64, period: f64) -> f64 {
    let (min, max) = chart
        .iter()
        .fold((f64::INFINITY, f64::NEG_INFINITY), |(min, max), point| {
            (min.min(point.x), max.max(point.x))
        });
    translated_longitude_bounds(min, max, longitude, period)
}

fn translated_longitude_bounds(min: f64, max: f64, longitude: f64, period: f64) -> f64 {
    if !min.is_finite() || !max.is_finite() {
        return longitude;
    }
    let center = f64::midpoint(min, max);
    longitude + period * ((center - longitude) / period).round()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::{CoordinateAxes, GeometryKind, LineSeq, MultiMember, Ring};

    fn degrees() -> crate::geographic::GeographicFrame {
        crate::crs::info("EPSG:4326")
            .unwrap()
            .geographic_frame()
            .unwrap()
    }

    fn geographic_graph(shape: &Shape) -> TopologyGraph {
        TopologyGraph::geographic(
            crate::geographic::ScalarGeographicView::try_new(degrees(), shape)
                .expect("fixture admits"),
        )
        .unwrap()
    }

    fn geographic_graph_in(
        shape: &Shape,
        frame: crate::geographic::GeographicFrame,
    ) -> TopologyGraph {
        TopologyGraph::geographic(
            crate::geographic::ScalarGeographicView::try_new(frame, shape).expect("fixture admits"),
        )
        .unwrap()
    }

    fn point(x: f64, y: f64) -> Point {
        Point::new_raw_xy(x, y)
    }

    fn line(points: &[(f64, f64)]) -> LineSeq {
        LineSeq::try_new(
            CoordSeq::try_from_points(
                &points.iter().map(|&(x, y)| point(x, y)).collect::<Vec<_>>(),
            )
            .unwrap(),
        )
        .expect("line coordinate sequence must contain at least two vertices")
    }

    fn ring(points: &[(f64, f64)]) -> Ring {
        Ring::from_closed(
            CoordSeq::try_from_points(
                &points.iter().map(|&(x, y)| point(x, y)).collect::<Vec<_>>(),
            )
            .unwrap(),
        )
    }

    fn polygon(shell: &[(f64, f64)], holes: &[&[(f64, f64)]]) -> Polygon {
        Polygon::new(ring(shell), holes.iter().map(|hole| ring(hole)).collect())
            .expect("test polygon is valid")
    }

    fn collection(shapes: Vec<Shape>) -> Shape {
        Shape::geometry_collection(shapes).expect("test collection is valid")
    }

    fn edge_ordinals_at(graph: &TopologyGraph, point: XY) -> Vec<usize> {
        let mut ordinals = Vec::new();
        let _: Option<()> =
            graph
                .support
                .for_each_edge_at(graph.domain, &graph.edges, point, |edge| {
                    let ordinal = graph
                        .edges
                        .iter()
                        .position(|candidate| std::ptr::eq(candidate, edge))
                        .expect("edge callback points into the graph edge slice");
                    ordinals.push(ordinal);
                    std::ops::ControlFlow::Continue(())
                });
        ordinals.sort_unstable();
        ordinals
    }

    fn brute_edge_ordinals_at(graph: &TopologyGraph, point: XY) -> Vec<usize> {
        graph
            .edges
            .iter()
            .enumerate()
            .filter_map(|(ordinal, edge)| {
                point_on_graph_segment(graph.domain, point, edge.segment).then_some(ordinal)
            })
            .collect()
    }

    #[test]
    fn geographic_seam_aliases_share_one_node() {
        let shape = Shape::MultiPoint(
            crate::geometry::MultiPoint::try_new(CoordinateAxes::XY, vec![
                MultiMember::Value(point(-180.0, 5.0)),
                MultiMember::Value(point(180.0, 5.0)),
            ])
            .expect("homogeneous points"),
        );
        let graph = geographic_graph(&shape);
        assert_eq!(graph.nodes.len(), 1);
        assert!(graph.equivalent(XY::new(-180.0, 5.0), XY::new(180.0, 5.0)));
    }

    #[test]
    fn geographic_pole_longitudes_share_one_node() {
        let shape = Shape::MultiPoint(
            crate::geometry::MultiPoint::try_new(CoordinateAxes::XY, vec![
                MultiMember::Value(point(-30.0, 90.0)),
                MultiMember::Value(point(120.0, 90.0)),
            ])
            .expect("homogeneous points"),
        );
        let graph = geographic_graph(&shape);
        assert_eq!(graph.nodes.len(), 1);
        assert_eq!(graph.node_for(XY::new(0.0, 90.0)), Some(TopologyNodeId(0)));
    }

    #[test]
    fn chart_cut_is_line_interior_not_source_boundary() {
        let shape = Shape::LineString(line(&[(170.0, 0.0), (-170.0, 0.0)]));
        let graph = geographic_graph(&shape);
        assert_eq!(graph.edges.len(), 2);
        assert!(!graph.has_line_boundary(XY::new(180.0, 0.0)));
        assert!(graph.has_line_boundary(XY::new(170.0, 0.0)));
        assert!(graph.has_line_boundary(XY::new(-170.0, 0.0)));
    }

    #[test]
    fn donut_hole_is_not_areal_interior_support() {
        let shape = Shape::Polygon(polygon(
            &[
                (0.0, 0.0),
                (10.0, 0.0),
                (10.0, 10.0),
                (0.0, 10.0),
                (0.0, 0.0),
            ],
            &[&[(3.0, 3.0), (7.0, 3.0), (7.0, 7.0), (3.0, 7.0), (3.0, 3.0)]],
        ));
        let graph = TopologyGraph::cartesian(&shape).unwrap();
        assert!(graph.contains_area_interior(XY::new(1.0, 1.0)));
        assert!(!graph.contains_area_interior(XY::new(5.0, 5.0)));
        assert!(graph.has_area_boundary(XY::new(5.0, 3.0)));
    }

    #[expect(
        clippy::float_cmp,
        reason = "a pole or antimeridian is one specific representable coordinate, and the question is whether a vertex IS that coordinate; a tolerance would admit vertices merely near it"
    )]
    #[test]
    fn geographic_donut_hole_stays_outside_across_the_seam() {
        let shape = Shape::Polygon(polygon(
            &[
                (170.0, -10.0),
                (-170.0, -10.0),
                (-170.0, 10.0),
                (170.0, 10.0),
                (170.0, -10.0),
            ],
            &[&[
                (175.0, -5.0),
                (-175.0, -5.0),
                (-175.0, 5.0),
                (175.0, 5.0),
                (175.0, -5.0),
            ]],
        ));
        let graph = geographic_graph(&shape);
        assert_eq!(
            geographic_ring_location(
                graph.area_support[0].geographic_rings().0,
                XY::new(179.0, 8.0),
            ),
            RingLocation::Inside,
        );
        assert!(graph.contains_area_interior(XY::new(179.0, 8.0)));
        assert!(!graph.contains_area_interior(XY::new(-180.0, 0.0)));
        let seam = graph
            .nodes
            .iter()
            .find(|node| node.point.x.abs() == 180.0)
            .expect("crossing polygon has a chart seam node")
            .point;
        assert!(graph.has_area_boundary(seam));
    }

    #[test]
    fn geographic_polar_ring_uses_the_pole_quotient_for_interior_support() {
        let shape = Shape::Polygon(polygon(
            &[
                (-180.0, 80.0),
                (-90.0, 80.0),
                (0.0, 80.0),
                (90.0, 80.0),
                (180.0, 80.0),
                (-180.0, 80.0),
            ],
            &[],
        ));
        let graph = geographic_graph(&shape);
        assert!(graph.contains_area_interior(XY::new(-120.0, 85.0)));
        assert!(graph.contains_area_interior(XY::new(120.0, 85.0)));
        assert!(graph.contains_area_interior(XY::new(0.0, 90.0)));
        assert!(!graph.contains_area_interior(XY::new(0.0, 70.0)));
    }

    #[test]
    fn geographic_full_longitude_band_survives_as_quotient_self_loops() {
        let shape = Shape::Polygon(polygon(
            &[
                (-180.0, -10.0),
                (180.0, -10.0),
                (180.0, 10.0),
                (-180.0, 10.0),
                (-180.0, -10.0),
            ],
            &[],
        ));
        let graph = geographic_graph(&shape);
        assert_eq!(
            graph.locate_point(XY::new(0.0, 0.0)),
            TopologyLocation::Interior
        );
        assert_eq!(
            graph.locate_point(XY::new(179.0, 0.0)),
            TopologyLocation::Interior
        );
        assert_eq!(
            graph.locate_point(XY::new(0.0, 10.0)),
            TopologyLocation::Boundary
        );
        assert!(
            graph
                .edges
                .iter()
                .any(|edge| { graph.equivalent(edge.segment.start, edge.segment.end) })
        );
    }

    #[test]
    fn geographic_edge_lookup_is_unique_at_ordinary_and_seam_points() {
        let shape = collection(vec![
            Shape::LineString(line(&[(-10.0, -1.0), (10.0, 3.0)])),
            Shape::LineString(line(&[(170.0, -2.0), (-170.0, 4.0)])),
            Shape::LineString(line(&[(-180.0, -3.0), (180.0, 3.0)])),
        ]);
        let graph = geographic_graph(&shape);
        for point in [
            XY::new(0.0, 1.0),
            XY::new(360.0, 1.0),
            XY::new(180.0, 0.0),
            XY::new(-180.0, 0.0),
            XY::new(180.0, 3.0),
        ] {
            assert_eq!(
                edge_ordinals_at(&graph, point),
                brute_edge_ordinals_at(&graph, point),
                "edge lookup mismatch at {point:?}",
            );
        }
    }

    #[test]
    fn geographic_point_queries_reuse_index_without_allocations() {
        let shape = collection(vec![
            Shape::LineString(line(&[(-10.0, -1.0), (10.0, 3.0)])),
            Shape::LineString(line(&[(170.0, -2.0), (-170.0, 4.0)])),
            Shape::LineString(line(&[(-180.0, -3.0), (180.0, 3.0)])),
        ]);
        let graph = geographic_graph(&shape);
        let points = [
            XY::new(0.0, 1.0),
            XY::new(360.0, 1.0),
            XY::new(180.0, 0.0),
            XY::new(-180.0, 0.0),
            XY::new(23.0, 30.0),
            XY::new(-123.0, 90.0),
        ];
        let ((), allocations) = crate::test_support::allocation_count(|| {
            for _ in 0..8 {
                for point in points {
                    let _ = graph.locate_point(point);
                }
            }
        });
        assert_eq!(allocations, 0);
    }

    #[test]
    fn geographic_high_degree_absent_queries_reuse_index_without_allocations() {
        let lines = (0..64)
            .map(|index| {
                let offset = (f64::from(index) + 0.5) / 64.0 - 0.5;
                Shape::LineString(line(&[(-1.0, -1.0 + offset), (1.0, 1.0 + offset)]))
            })
            .collect();
        let graph = geographic_graph(&collection(lines));
        let ((), allocations) = crate::test_support::allocation_count(|| {
            for _ in 0..8 {
                let _ = graph.locate_point(XY::new(0.0, 0.0));
                let _ = graph.locate_point(XY::new(360.0, 0.0));
                let _ = graph.locate_point(XY::new(180.0, 30.0));
                let _ = graph.locate_point(XY::new(12.0, 30.0));
            }
        });
        assert_eq!(allocations, 0);
    }

    #[test]
    fn geographic_polar_hole_excludes_the_pole_without_synthetic_boundary() {
        let shape = Shape::Polygon(polygon(
            &[
                (-180.0, 70.0),
                (-90.0, 70.0),
                (0.0, 70.0),
                (90.0, 70.0),
                (180.0, 70.0),
                (-180.0, 70.0),
            ],
            &[&[
                (-180.0, 80.0),
                (-90.0, 80.0),
                (0.0, 80.0),
                (90.0, 80.0),
                (180.0, 80.0),
                (-180.0, 80.0),
            ]],
        ));
        let graph = geographic_graph(&shape);
        assert_eq!(
            graph.locate_point(XY::new(0.0, 75.0)),
            TopologyLocation::Interior
        );
        assert_eq!(
            graph.locate_point(XY::new(0.0, 85.0)),
            TopologyLocation::Exterior
        );
        assert_eq!(
            graph.locate_point(XY::new(0.0, 90.0)),
            TopologyLocation::Exterior
        );
    }

    #[test]
    fn geographic_duplicate_polygons_aggregate_by_or() {
        let member = Shape::Polygon(polygon(
            &[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)],
            &[],
        ));
        for count in [2, 3] {
            let graph = geographic_graph(&collection(vec![member.clone(); count]));
            assert_eq!(
                graph.locate_point(XY::new(2.0, 2.0)),
                TopologyLocation::Interior
            );
            assert_eq!(
                graph.locate_point(XY::new(2.0, 0.0)),
                TopologyLocation::Boundary
            );
        }
    }

    #[test]
    fn geographic_opposite_retrace_does_not_create_a_face_transition() {
        let shape = Shape::Polygon(polygon(
            &[(0.0, 0.0), (4.0, 0.0), (0.0, 0.0), (0.0, 0.0)],
            &[],
        ));
        let graph = geographic_graph(&shape);
        assert_eq!(
            graph.locate_point(XY::new(2.0, 0.0)),
            TopologyLocation::Boundary
        );
        assert_eq!(
            graph.locate_point(XY::new(2.0, 1.0)),
            TopologyLocation::Exterior
        );
    }

    #[test]
    fn geographic_grad_frame_uses_native_period_and_pole() {
        let unit = crate::geographic::AngularFactor::try_new("grad", std::f64::consts::TAU / 400.0)
            .unwrap();
        let frame = crate::geographic::GeographicFrame::try_new(unit, unit, 0.0).unwrap();
        let shape = Shape::Polygon(polygon(
            &[
                (-200.0, 80.0),
                (-100.0, 80.0),
                (0.0, 80.0),
                (100.0, 80.0),
                (200.0, 80.0),
                (-200.0, 80.0),
            ],
            &[],
        ));
        let graph = geographic_graph_in(&shape, frame);
        assert!(frame.longitude().period() > 399.0);
        assert_eq!(
            graph.locate_point(XY::new(0.0, frame.pole())),
            TopologyLocation::Interior
        );
        assert_eq!(
            graph.locate_point(XY::new(0.0, 70.0)),
            TopologyLocation::Exterior
        );
    }

    #[test]
    fn line_boundary_parity_is_collection_wide() {
        let lines = crate::geometry::MultiLineString::try_new(CoordinateAxes::XY, vec![
            MultiMember::Value(line(&[(0.0, 0.0), (1.0, 0.0)])),
            MultiMember::Value(line(&[(1.0, 0.0), (2.0, 0.0)])),
        ])
        .expect("homogeneous lines");
        let graph = TopologyGraph::cartesian(&Shape::MultiLineString(lines)).unwrap();
        assert_eq!(graph.line_boundary.len(), 2);
        assert!(!graph.has_line_boundary(XY::new(1.0, 0.0)));
        assert!(graph.has_line_boundary(XY::new(0.0, 0.0)));
        assert!(graph.has_line_boundary(XY::new(2.0, 0.0)));
    }

    fn brute_contains_area_interior(graph: &TopologyGraph, point: XY) -> bool {
        graph
            .area_support
            .iter()
            .any(|polygon| polygon_contains_interior(graph.domain.space(), polygon, point))
    }

    fn brute_has_area_boundary(graph: &TopologyGraph, point: XY) -> bool {
        graph
            .node_for(point)
            .is_some_and(|id| graph.nodes[id.index()].support.area_boundary)
            || graph.edges.iter().any(|edge| {
                edge.is_boundary && point_on_graph_segment(graph.domain, point, edge.segment)
            })
    }

    fn brute_has_line_support(graph: &TopologyGraph, point: XY) -> bool {
        graph.edges.iter().any(|edge| {
            edge.sources.iter().any(|&source| {
                matches!(
                    graph.source_edges[source as usize].support,
                    SourceSupport::Line
                )
            }) && point_on_graph_segment(graph.domain, point, edge.segment)
        })
    }

    fn brute_locate_point(
        graph: &TopologyGraph,
        point: XY,
        include_collapsed_line_points: bool,
    ) -> TopologyLocation {
        if let Some(node) = graph.node_for(point) {
            match graph.nodes[node.index()].support.area_interior {
                AreaNodeSupport::Complete => return TopologyLocation::Interior,
                AreaNodeSupport::Partial if !brute_contains_area_interior(graph, point) => {
                    return TopologyLocation::Boundary;
                },
                AreaNodeSupport::Partial | AreaNodeSupport::None => {},
            }
        }
        if brute_contains_area_interior(graph, point)
            || graph.edges.iter().any(|edge| {
                edge.area_interior_support
                    && point_on_graph_segment(graph.domain, point, edge.segment)
            })
        {
            return TopologyLocation::Interior;
        }
        let area_boundary = brute_has_area_boundary(graph, point);
        let line_boundary = graph.has_line_boundary(point);
        if area_boundary || line_boundary {
            return TopologyLocation::Boundary;
        }
        let point_support = if include_collapsed_line_points {
            graph
                .source_points
                .iter()
                .any(|source| graph.equivalent(source.point.xy(), point))
        } else {
            graph
                .source_points
                .iter()
                .any(|source| !source.collapsed_line && graph.equivalent(source.point.xy(), point))
        };
        if point_support || brute_has_line_support(graph, point) {
            return TopologyLocation::Interior;
        }
        TopologyLocation::Exterior
    }

    fn assert_indexed_locations_match_brute(
        graph: &TopologyGraph,
        points: impl IntoIterator<Item = XY>,
    ) {
        for point in points {
            assert_eq!(
                graph.locate_point(point),
                brute_locate_point(graph, point, true),
                "indexed location mismatch at {point:?} in {:?}",
                graph.domain
            );
            assert_eq!(
                graph.locate_point_for_overlay(point),
                brute_locate_point(graph, point, false),
                "indexed overlay location mismatch at {point:?} in {:?}",
                graph.domain
            );
        }
    }

    #[test]
    fn indexed_support_matches_brute_locations_at_source_noded_and_aliased_points() {
        let cartesian = collection(vec![
            Shape::Polygon(polygon(
                &[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)],
                &[&[(1.0, 1.0), (3.0, 1.0), (3.0, 3.0), (1.0, 3.0), (1.0, 1.0)]],
            )),
            Shape::LineString(line(&[(-1.0, 2.0), (2.0, 2.0), (5.0, 2.0)])),
            Shape::Point(point(8.0, 8.0)),
        ]);
        let geographic = collection(vec![
            Shape::Polygon(polygon(
                &[
                    (170.0, 0.0),
                    (-170.0, 0.0),
                    (-170.0, 12.0),
                    (170.0, 12.0),
                    (170.0, 0.0),
                ],
                &[&[
                    (175.0, 3.0),
                    (-175.0, 3.0),
                    (-175.0, 8.0),
                    (175.0, 8.0),
                    (175.0, 3.0),
                ]],
            )),
            Shape::Polygon(polygon(
                &[
                    (-180.0, 80.0),
                    (-90.0, 80.0),
                    (0.0, 80.0),
                    (90.0, 80.0),
                    (180.0, 80.0),
                    (-180.0, 80.0),
                ],
                &[],
            )),
            Shape::LineString(line(&[(175.0, 6.0), (180.0, 6.0), (-175.0, 6.0)])),
        ]);
        for graph in [
            TopologyGraph::cartesian(&cartesian).unwrap(),
            geographic_graph(&geographic),
        ] {
            let mut points = Vec::new();
            points.extend(graph.nodes.iter().map(|node| node.point));
            for edge in &graph.edges {
                points.push(edge.segment.start);
                points.push(edge.segment.end);
                points.push(XY::new(
                    f64::midpoint(edge.segment.start.x, edge.segment.end.x),
                    f64::midpoint(edge.segment.start.y, edge.segment.end.y),
                ));
            }
            points.extend(graph.source_points.iter().map(|source| source.point.xy()));
            if let TopologyDomain::Geographic(frame) = graph.domain {
                let period = frame.longitude().period();
                let aliases = points.clone();
                points.extend(aliases.into_iter().flat_map(|point| {
                    [
                        XY::new(point.x - period, point.y),
                        point,
                        XY::new(point.x + period, point.y),
                    ]
                }));
                points.extend([
                    XY::new(-137.0, frame.pole()),
                    XY::new(223.0, frame.pole()),
                    XY::new(-137.0, -frame.pole()),
                ]);
            }
            let one_ulp = f64::from_bits(1.0_f64.to_bits() + 1);
            points.extend([XY::new(one_ulp, 1.0), XY::new(1.0, one_ulp)]);
            assert_indexed_locations_match_brute(&graph, points);
        }
    }

    #[test]
    fn area_support_callback_returns_individual_polygon_ids() {
        let shape = collection(vec![
            Shape::Polygon(polygon(
                &[(0.0, 0.0), (4.0, 0.0), (4.0, 4.0), (0.0, 4.0), (0.0, 0.0)],
                &[],
            )),
            Shape::Polygon(polygon(
                &[(2.0, 2.0), (6.0, 2.0), (6.0, 6.0), (2.0, 6.0), (2.0, 2.0)],
                &[],
            )),
        ]);
        let graph = TopologyGraph::cartesian(&shape).unwrap();
        let mut ids = Vec::new();
        let _: Option<()> = graph.for_each_area_interior(XY::new(3.0, 3.0), |polygon| {
            ids.push(polygon);
            std::ops::ControlFlow::Continue(())
        });
        ids.sort_unstable();
        assert_eq!(ids, [0, 1]);
        assert!(graph.contains_area_interior(XY::new(3.0, 3.0)));
        assert!(!graph.contains_area_interior(XY::new(1.0, 5.0)));

        let geographic = Shape::Polygon(polygon(
            &[
                (170.0, 0.0),
                (-170.0, 0.0),
                (-170.0, 10.0),
                (170.0, 10.0),
                (170.0, 0.0),
            ],
            &[],
        ));
        let graph = geographic_graph(&geographic);
        for longitude in [-181.0, 179.0, 539.0] {
            let mut ids = Vec::new();
            let _: Option<()> = graph.for_each_area_interior(XY::new(longitude, 5.0), |polygon| {
                ids.push(polygon);
                std::ops::ControlFlow::Continue(())
            });
            assert_eq!(ids, [0]);
        }
    }

    #[test]
    fn resolved_node_locations_match_full_point_location() {
        let cartesian = collection(vec![
            Shape::Polygon(polygon(
                &[(0.0, 0.0), (2.0, 0.0), (2.0, 2.0), (0.0, 2.0), (0.0, 0.0)],
                &[],
            )),
            Shape::LineString(line(&[(-1.0, 1.0), (1.0, 1.0), (3.0, 1.0)])),
        ]);
        let geographic = collection(vec![
            Shape::Polygon(polygon(
                &[
                    (170.0, 0.0),
                    (-170.0, 0.0),
                    (-170.0, 10.0),
                    (170.0, 10.0),
                    (170.0, 0.0),
                ],
                &[],
            )),
            Shape::LineString(line(&[(175.0, 5.0), (180.0, 5.0), (-175.0, 5.0)])),
        ]);
        for graph in [
            TopologyGraph::cartesian(&cartesian).unwrap(),
            geographic_graph(&geographic),
        ] {
            for node in &graph.nodes {
                assert_eq!(node.location, graph.locate_point(node.point));
            }
        }
    }

    #[test]
    fn overlapping_line_members_keep_shared_support_and_cancel_duplicate_boundary() {
        let collection = collection(vec![
            Shape::LineString(line(&[(0.0, 0.0), (2.0, 0.0)])),
            Shape::LineString(line(&[(0.0, 0.0), (2.0, 0.0)])),
            Shape::Point(point(4.0, 4.0)),
        ]);
        let graph = TopologyGraph::cartesian(&collection).unwrap();
        assert_eq!(graph.effective_dimension, Some(Dimension::Curve));
        assert!(graph.line_boundary.is_empty());
        assert_eq!(graph.edges.len(), 1);
        assert_eq!(graph.edges[0].sources.len(), 2);
        assert!(graph.nodes.iter().any(|node| node.support.point));
    }

    #[test]
    fn noding_preserves_one_source_record_on_each_atomic_piece() {
        let collection = collection(vec![
            Shape::LineString(line(&[(0.0, 0.0), (2.0, 2.0)])),
            Shape::LineString(line(&[(0.0, 2.0), (2.0, 0.0)])),
        ]);
        let graph = TopologyGraph::cartesian(&collection).unwrap();
        assert_eq!(graph.edges.len(), 4);
        assert!(graph.edges.iter().all(|edge| edge.sources.len() == 1));
        assert!(!graph.has_line_boundary(XY::new(1.0, 1.0)));
    }

    fn assert_edge_endpoints_and_provenance(graph: &TopologyGraph) {
        for edge in &graph.edges {
            assert!(graph.node_for(edge.segment.start).is_some());
            assert!(graph.node_for(edge.segment.end).is_some());
            assert!(!edge.sources.is_empty());
            for &source in &edge.sources {
                let source = &graph.source_edges[source as usize];
                assert!(
                    chart_segments(graph.domain, source.segment)
                        .into_iter()
                        .flatten()
                        .any(|chart| {
                            point_on_segment(edge.segment.start, chart.start, chart.end)
                                && point_on_segment(edge.segment.end, chart.start, chart.end)
                        })
                );
            }
        }
    }

    #[test]
    fn cartesian_noded_bundles_keep_endpoint_provenance() {
        let shape = collection(vec![
            Shape::LineString(line(&[(0.0, 0.0), (2.0, 2.0)])),
            Shape::LineString(line(&[(0.0, 2.0), (2.0, 0.0)])),
        ]);
        let graph = TopologyGraph::cartesian(&shape).unwrap();
        assert_eq!(graph.edges.len(), 4);
        assert_edge_endpoints_and_provenance(&graph);
    }

    #[test]
    fn geographic_seam_bundles_keep_endpoint_provenance() {
        let shape = Shape::Polygon(polygon(
            &[
                (170.0, 0.0),
                (-170.0, 0.0),
                (-170.0, 10.0),
                (170.0, 10.0),
                (170.0, 0.0),
            ],
            &[],
        ));
        let graph = geographic_graph(&shape);
        assert!(graph.edges.iter().any(|edge| {
            graph.equivalent(edge.segment.start, XY::new(180.0, 0.0))
                || graph.equivalent(edge.segment.end, XY::new(180.0, 0.0))
        }));
        assert_edge_endpoints_and_provenance(&graph);
    }

    #[test]
    fn geographic_chart_keeps_exact_one_ulp_source_endpoint() {
        let one_ulp_shape = geographic_one_ulp_endpoint_fixture();
        let one_ulp_graph = geographic_graph(&one_ulp_shape);
        let endpoint = f64::from_bits((-170.0_f64).to_bits() + 1);
        // A rounded `start + delta` would leave a second node at the shared
        // source endpoint instead of selecting its exact canonical copy.
        assert_eq!(one_ulp_graph.nodes.len(), 6);
        let endpoint_node = one_ulp_graph
            .node_for(XY::new(endpoint, 0.0))
            .expect("one-ULP source endpoint must own a graph node");
        assert_eq!(
            one_ulp_graph.nodes[endpoint_node.index()].point,
            XY::new(endpoint, 0.0)
        );
        assert_edge_endpoints_and_provenance(&one_ulp_graph);
    }

    fn geographic_one_ulp_endpoint_fixture() -> Shape {
        let endpoint = f64::from_bits((-170.0_f64).to_bits() + 1);
        Shape::Polygon(polygon(
            &[
                (170.0, 0.0),
                (endpoint, 0.0),
                (endpoint, 10.0),
                (170.0, 10.0),
                (170.0, 0.0),
            ],
            &[],
        ))
    }

    fn quotient_dart(edge: usize, start: XY, end: XY) -> QuotientDart {
        QuotientDart {
            edge,
            from: TopologyNodeId(0),
            to: TopologyNodeId(1),
            segment: Segment { start, end },
        }
    }

    #[test]
    fn quotient_fan_orders_adjacent_diagonals_geometrically() {
        let high = f64::from_bits(1.0_f64.to_bits() + 1);
        assert_eq!(
            crate::geometry::predicates::pseudo_angle(1.0, 1.0),
            crate::geometry::predicates::pseudo_angle(1.0, high),
            "adjacent diagonal rays collide after pseudo-angle rounding"
        );
        let upper = quotient_dart(0, XY::new(0.0, 0.0), XY::new(1.0, high));
        let lower = quotient_dart(1, XY::new(0.0, 0.0), XY::new(1.0, 1.0));
        assert_eq!(
            compare_quotient_rays(degrees(), XY::new(0.0, 0.0), &upper, &lower),
            std::cmp::Ordering::Greater,
        );
    }

    #[test]
    fn pole_fan_orders_equal_longitude_quotient_directions_geometrically() {
        let high = f64::from_bits(1.0_f64.to_bits() + 1);
        let wider = quotient_dart(0, XY::new(0.0, 90.0), XY::new(high, 89.0));
        let narrower = quotient_dart(1, XY::new(0.0, 90.0), XY::new(1.0, 89.0));
        assert_eq!(
            compare_quotient_rays(degrees(), XY::new(0.0, 90.0), &wider, &narrower),
            std::cmp::Ordering::Greater,
        );
    }

    #[test]
    fn typed_empty_has_no_effective_dimension() {
        let shape = Shape::Empty(GeometryKind::Polygon, CoordinateAxes::XYZM);
        let graph = TopologyGraph::cartesian(&shape).unwrap();
        assert_eq!(graph.effective_dimension, None);
        assert!(graph.nodes.is_empty());
        assert!(graph.edges.is_empty());
    }

    #[test]
    fn mixed_collection_keeps_highest_nonempty_dimension() {
        let collection = collection(vec![
            Shape::Empty(GeometryKind::Point, CoordinateAxes::XYZ),
            Shape::LineString(line(&[(0.0, 0.0), (1.0, 0.0)])),
            Shape::Polygon(polygon(
                &[(2.0, 0.0), (4.0, 0.0), (4.0, 2.0), (2.0, 2.0), (2.0, 0.0)],
                &[],
            )),
        ]);
        let graph = TopologyGraph::cartesian(&collection).unwrap();
        assert_eq!(graph.effective_dimension, Some(Dimension::Surface));
    }

    #[test]
    fn aggregate_face_support_does_not_bridge_nearby_components() {
        let shape = collection(vec![
            Shape::Polygon(polygon(
                &[
                    (-1.01e-7, -1e-9),
                    (-9.9e-8, -1e-9),
                    (-9.9e-8, 1e-9),
                    (-1.01e-7, 1e-9),
                    (-1.01e-7, -1e-9),
                ],
                &[],
            )),
            Shape::Polygon(polygon(
                &[
                    (9.9e-8, -1e-9),
                    (1.01e-7, -1e-9),
                    (1.01e-7, 1e-9),
                    (9.9e-8, 1e-9),
                    (9.9e-8, -1e-9),
                ],
                &[],
            )),
        ]);
        let graph = TopologyGraph::cartesian(&shape).unwrap();
        assert_eq!(
            graph.locate_point(XY::new(0.0, 0.0)),
            TopologyLocation::Exterior
        );
    }

    #[test]
    fn aggregate_face_support_absorbs_shared_edges_but_not_point_pinches() {
        let adjacent = collection(vec![
            Shape::Polygon(polygon(
                &[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)],
                &[],
            )),
            Shape::Polygon(polygon(
                &[(1.0, 0.0), (2.0, 0.0), (2.0, 1.0), (1.0, 1.0), (1.0, 0.0)],
                &[],
            )),
        ]);
        let graph = TopologyGraph::cartesian(&adjacent).unwrap();
        assert_eq!(
            graph.locate_point(XY::new(1.0, 0.5)),
            TopologyLocation::Interior
        );
        assert_eq!(
            graph.locate_point(XY::new(1.0, 0.0)),
            TopologyLocation::Boundary
        );

        let left = Shape::Polygon(polygon(
            &[(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)],
            &[],
        ));
        let right = Shape::Polygon(polygon(
            &[(1.0, 0.0), (2.0, 0.0), (2.0, 1.0), (1.0, 1.0), (1.0, 0.0)],
            &[],
        ));
        let duplicates = Shape::geometry_collection(vec![left.clone(), left.clone(), left, right])
            .expect("test collection is valid");
        assert_eq!(
            TopologyGraph::cartesian(&duplicates)
                .unwrap()
                .locate_point(XY::new(1.0, 0.5)),
            TopologyLocation::Interior
        );

        let pinch = collection(vec![
            Shape::Polygon(polygon(
                &[(0.0, 0.0), (-2.0, -1.0), (-2.0, 1.0), (0.0, 0.0)],
                &[],
            )),
            Shape::Polygon(polygon(
                &[(0.0, 0.0), (2.0, 1.0), (2.0, -1.0), (0.0, 0.0)],
                &[],
            )),
        ]);
        let graph = TopologyGraph::cartesian(&pinch).unwrap();
        assert_eq!(
            graph.locate_point(XY::new(0.0, 0.0)),
            TopologyLocation::Boundary
        );
    }
}
