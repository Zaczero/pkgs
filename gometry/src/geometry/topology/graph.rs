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

use crate::collections::{HashMap, HashMapExt as _, HashSet, HashSetExt as _};
use crate::geometry::topology::ring_contains_interior_open;
use crate::geometry::{
    CoordSeq, CoordinateAxes, Coordinates as _, DimMode, Dimension, Point, PointKey, Polygon,
    Segment, Shape, XY, canonical_f64_bits, dimension, point_on_segment, ring_encloses_pole,
    same_point, segment_projection, shared_segment_part,
};

/// The coordinate quotient used by a topology graph.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TopologySpace {
    Cartesian,
    Geographic,
}

impl TopologySpace {
    const fn is_geographic(self) -> bool {
        matches!(self, Self::Geographic)
    }
}

/// Dense graph node identifier.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub(crate) struct TopologyNodeId(u32);

impl TopologyNodeId {
    pub(crate) const fn index(self) -> usize {
        self.0 as usize
    }
}

/// Source edge category. A line edge contributes line interior support;
/// polygon ring edges contribute areal boundary support.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SourceEdgeKind {
    Line,
    Area,
}

/// One source edge before charting and noding.
#[derive(Clone, Copy, Debug)]
pub(crate) struct SourceEdge {
    pub(crate) start: Point,
    pub(crate) end: Point,
    pub(crate) segment: Segment,
    pub(crate) kind: SourceEdgeKind,
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
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(crate) struct NodeSupport {
    pub(crate) point: bool,
    pub(crate) line: bool,
    pub(crate) area_boundary: bool,
    pub(crate) line_boundary: bool,
}

/// A noded edge. `sources` indexes [`TopologyGraph::source_edges`], while the
/// edge's endpoints are quotient node ids rather than raw coordinate keys.
#[derive(Clone, Debug)]
pub(crate) struct TopologyEdge {
    pub(crate) segment: Segment,
    pub(crate) sources: Box<[u32]>,
    pub(crate) is_boundary: bool,
}

/// A quotient node retains the first source/chart coordinate as its display
/// coordinate. Equality is carried by the graph's key, so seam aliases can
/// retain their original sign without making two topology nodes.
#[derive(Clone, Copy, Debug)]
pub(crate) struct TopologyNode {
    pub(crate) point: XY,
    pub(crate) support: NodeSupport,
}

/// One source-owned topology graph for a complete geometry operand.
pub(crate) struct TopologyGraph {
    pub(crate) space: TopologySpace,
    pub(crate) axes: CoordinateAxes,
    pub(crate) declared_dimension: Option<Dimension>,
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
    node_lookup: HashMap<TopologyKey, TopologyNodeId>,
}

impl TopologyGraph {
    /// Build one graph from one complete source geometry. `space` changes
    /// only point equivalence and source-edge charting; the noder remains the
    /// existing planar XY noder in both spaces.
    pub(super) fn build(shape: &Shape, space: TopologySpace) -> Self {
        let mut builder = GraphBuilder::new(shape, space);
        builder.collect(shape, 0);
        builder.finish(shape)
    }

    pub(crate) fn cartesian_many(shapes: &[&Shape]) -> Self {
        let mut builder = GraphBuilder::new_many(shapes, TopologySpace::Cartesian);
        for (source_row, shape) in shapes.iter().enumerate() {
            builder.collect(shape, source_row as u32);
        }
        builder.finish_many(shapes)
    }

    pub(crate) fn cartesian(shape: &Shape) -> Self {
        Self::build(shape, TopologySpace::Cartesian)
    }

    pub(crate) fn geographic(shape: &Shape) -> Self {
        Self::build(shape, TopologySpace::Geographic)
    }

    pub(crate) fn node_for(&self, point: XY) -> Option<TopologyNodeId> {
        self.node_lookup.get(&self.key(point)).copied()
    }

    pub(crate) fn equivalent(&self, left: XY, right: XY) -> bool {
        self.key(left) == self.key(right)
    }

    pub(super) fn has_line_boundary(&self, point: XY) -> bool {
        self.node_for(point)
            .is_some_and(|id| self.nodes[id.index()].support.line_boundary)
    }

    pub(super) fn has_area_boundary(&self, point: XY) -> bool {
        self.node_for(point)
            .is_some_and(|id| self.nodes[id.index()].support.area_boundary)
            || self.edges.iter().any(|edge| {
                edge.is_boundary && point_on_graph_segment(self.space, point, edge.segment)
            })
    }

    pub(crate) fn contains_area_interior(&self, point: XY) -> bool {
        area_support_contains(self.space, &self.area_support, point)
    }

    pub(crate) fn has_area_interior(&self) -> bool {
        self.area_support
            .iter()
            .any(|support| crate::geometry::relate::polygon_has_nondegenerate_area(&support.source))
    }

    pub(super) fn has_point_support(&self, point: XY) -> bool {
        self.source_points
            .iter()
            .any(|source| self.equivalent(source.point.xy(), point))
    }

    pub(crate) fn has_point_support_any(&self) -> bool {
        !self.source_points.is_empty()
    }

    pub(crate) fn edge_has_line_source(&self, edge: &TopologyEdge) -> bool {
        edge.sources.iter().any(|&source| {
            matches!(
                self.source_edges[source as usize].kind,
                SourceEdgeKind::Line
            )
        })
    }

    pub(crate) fn edge_has_line_source_for_row(
        &self,
        edge: &TopologyEdge,
        source_row: u32,
    ) -> bool {
        edge.sources.iter().any(|&source| {
            let source = self.source_edges[source as usize];
            source.source_row == source_row && matches!(source.kind, SourceEdgeKind::Line)
        })
    }

    pub(crate) fn has_line_source_at_node(&self, point: XY, source_row: u32) -> bool {
        self.edges.iter().any(|edge| {
            (self.equivalent(edge.segment.start, point) || self.equivalent(edge.segment.end, point))
                && self.edge_has_line_source_for_row(edge, source_row)
        })
    }

    pub(crate) fn edge_has_area_source(&self, edge: &TopologyEdge) -> bool {
        edge.sources.iter().any(|&source| {
            matches!(
                self.source_edges[source as usize].kind,
                SourceEdgeKind::Area
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
        edge.sources.iter().find_map(|&source| {
            let source = self.source_edges[source as usize];
            (source.source_row == source_row && matches!(source.kind, SourceEdgeKind::Area))
                .then(|| segment_direction_matches(Segment { start, end }, source.segment))
        })
    }

    pub(crate) fn has_interior_support_other_than(&self, point: XY) -> bool {
        self.source_points
            .iter()
            .any(|source| !self.equivalent(source.point.xy(), point))
    }

    pub(crate) fn has_boundary_support_other_than(&self, point: XY) -> bool {
        self.nodes.iter().any(|node| {
            (node.support.area_boundary || node.support.line_boundary)
                && !self.equivalent(node.point, point)
        })
    }

    pub(crate) fn has_line_support(&self, point: XY) -> bool {
        self.edges.iter().any(|edge| {
            edge.sources.iter().any(|&source| {
                matches!(
                    self.source_edges[source as usize].kind,
                    SourceEdgeKind::Line
                )
            }) && point_on_graph_segment(self.space, point, edge.segment)
        })
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
        if self.contains_area_interior(point) {
            return TopologyLocation::Interior;
        }
        if self.area_interior_straddles_point(point) {
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
            self.source_points
                .iter()
                .any(|source| !source.collapsed_line && self.equivalent(source.point.xy(), point))
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
        if self.contains_area_interior(midpoint) {
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

    fn area_interior_straddles_point(&self, point: XY) -> bool {
        if !self.has_area_interior() {
            return false;
        }
        let scale = 1e-7;
        let inside = |x: f64, y: f64| self.contains_area_interior(XY::new(x, y));
        (inside(point.x - scale, point.y) && inside(point.x + scale, point.y))
            || (inside(point.x, point.y - scale) && inside(point.x, point.y + scale))
    }

    fn key(&self, point: XY) -> TopologyKey {
        quotient_key(self.space, point)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TopologyLocation {
    Interior,
    Boundary,
    Exterior,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
enum TopologyKey {
    Coordinate(PointKey),
    Seam(u64),
    Pole(bool),
}

fn quotient_key(space: TopologySpace, point: XY) -> TopologyKey {
    if space.is_geographic() {
        if point.y == 90.0 {
            return TopologyKey::Pole(true);
        }
        if point.y == -90.0 {
            return TopologyKey::Pole(false);
        }
        let longitude = canonical_longitude(point.x);
        if longitude == 180.0 {
            return TopologyKey::Seam(canonical_f64_bits(point.y));
        }
        return TopologyKey::Coordinate(PointKey::new(XY::new(longitude, point.y)));
    }
    TopologyKey::Coordinate(PointKey::new(point))
}

fn canonical_longitude(longitude: f64) -> f64 {
    let longitude = longitude.rem_euclid(360.0);
    let longitude = if longitude > 180.0 {
        longitude - 360.0
    } else {
        longitude
    };
    if longitude == 0.0 { 0.0 } else { longitude }
}

fn point_on_graph_segment(space: TopologySpace, point: XY, segment: Segment) -> bool {
    if space.is_geographic() {
        let x = translated_longitude(&[segment.start, segment.end], point.x);
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
pub(crate) struct AreaSupport {
    pub(crate) source: Polygon,
    pub(crate) shell: ChartRing,
    pub(crate) holes: Box<[ChartRing]>,
}

#[derive(Clone, Debug)]
pub(crate) struct ChartRing {
    pub(crate) points: Box<[XY]>,
    pub(crate) north_pole: bool,
    pub(crate) south_pole: bool,
}

struct GraphBuilder {
    space: TopologySpace,
    axes: CoordinateAxes,
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
    fn new(shape: &Shape, space: TopologySpace) -> Self {
        Self {
            space,
            axes: shape.axes(),
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

    fn new_many(shapes: &[&Shape], space: TopologySpace) -> Self {
        let axes = shapes
            .iter()
            .fold(CoordinateAxes::XY, |axes, shape| axes.union(shape.axes()));
        Self {
            space,
            axes,
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

    fn collect(&mut self, shape: &Shape, source_row: u32) {
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
            Shape::Polygon(polygon) => self.collect_polygon(polygon, source_row),
            Shape::MultiPolygon(polygons) => {
                for polygon in polygons {
                    self.collect_polygon(polygon, source_row);
                }
            },
            Shape::GeometryCollection(parts) => {
                for part in parts {
                    self.collect(part, source_row);
                }
            },
        }
    }

    fn component(&mut self) -> u32 {
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
            self.push_source_edge(SourceEdgeKind::Line, component, source_row, start, end);
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

    fn collect_polygon(&mut self, polygon: &Polygon, source_row: u32) {
        let shell = self.chart_ring(polygon.shell.coords());
        let holes = polygon
            .holes
            .iter()
            .map(|ring| self.chart_ring(ring.coords()))
            .collect();
        self.area_support.push(AreaSupport {
            source: polygon.clone(),
            shell,
            holes,
        });
        let component = self.component();
        for ring in std::iter::once(&polygon.shell).chain(polygon.holes.iter()) {
            for point in ring.iter_coords() {
                self.ensure_node(point.xy(), |support| support.area_boundary = true);
            }
            for [start, end] in ring.segment_pairs() {
                if same_point(start, end) {
                    continue;
                }
                self.push_source_edge(SourceEdgeKind::Area, component, source_row, start, end);
            }
        }
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
        kind: SourceEdgeKind,
        component: u32,
        source_row: u32,
        start: Point,
        end: Point,
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
            kind,
            component,
            source_row,
        });
        for chart in self.chart_segments(segment) {
            let support = kind;
            self.ensure_node(chart.start, |node| match support {
                SourceEdgeKind::Line => node.line = true,
                SourceEdgeKind::Area => node.area_boundary = true,
            });
            self.ensure_node(chart.end, |node| match support {
                SourceEdgeKind::Line => node.line = true,
                SourceEdgeKind::Area => node.area_boundary = true,
            });
            self.chart_edges.push(ChartEdge {
                segment: chart,
                source,
            });
        }
    }

    fn chart_segments(&self, segment: Segment) -> Vec<Segment> {
        if !self.space.is_geographic() {
            return vec![segment];
        }
        let start = segment.start.point();
        let end = segment.end.point();
        let pieces = crate::geometry::antimeridian::segment_coords(&[start, end]);
        if pieces.is_empty() {
            return vec![segment];
        }
        pieces
            .into_iter()
            .flat_map(|piece| {
                piece
                    .array_windows::<2>()
                    .filter_map(|pair| {
                        (!same_point(pair[0], pair[1])).then_some(Segment {
                            start: pair[0].xy(),
                            end: pair[1].xy(),
                        })
                    })
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    fn chart_ring(&self, ring: &CoordSeq) -> ChartRing {
        let mut points: Vec<XY> = Vec::with_capacity(ring.len());
        for [start, end] in ring.segment_pairs() {
            for chart in self.chart_segments(Segment {
                start: start.xy(),
                end: end.xy(),
            }) {
                let chart_start = lift_chart_longitude(chart.start, points.last().copied());
                if points.last().copied() != Some(chart_start) {
                    points.push(chart_start);
                }
                let chart_end = lift_chart_longitude(chart.end, points.last().copied());
                if points.last().copied() != Some(chart_end) {
                    points.push(chart_end);
                }
            }
        }
        if points.len() > 1 && same_point(points[0], *points.last().expect("non-empty ring")) {
            points.pop();
        }
        ChartRing {
            points: points.into_boxed_slice(),
            north_pole: ring_encloses_pole(ring, true),
            south_pole: ring_encloses_pole(ring, false),
        }
    }

    fn key(&self, point: XY) -> TopologyKey {
        quotient_key(self.space, point)
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
        self.nodes.push(TopologyNode { point, support });
        id
    }

    fn toggle_line_boundary(&mut self, key: TopologyKey) {
        if !self.line_boundary.insert(key) {
            self.line_boundary.remove(&key);
        }
    }

    fn finish(self, shape: &Shape) -> TopologyGraph {
        self.finish_dimensions(
            dimension(shape, DimMode::Declared),
            dimension(shape, DimMode::Effective),
            matches!(shape, Shape::GeometryCollection(_)),
            shape_has_polygonal_members(shape),
        )
    }

    fn finish_many(self, shapes: &[&Shape]) -> TopologyGraph {
        let declared_dimension = shapes
            .iter()
            .filter_map(|shape| dimension(shape, DimMode::Declared))
            .max();
        let effective_dimension = shapes
            .iter()
            .filter_map(|shape| dimension(shape, DimMode::Effective))
            .max();
        self.finish_dimensions(
            declared_dimension,
            effective_dimension,
            true,
            shapes
                .iter()
                .any(|shape| shape_has_polygonal_members(shape)),
        )
    }

    fn finish_dimensions(
        mut self,
        declared_dimension: Option<Dimension>,
        effective_dimension: Option<Dimension>,
        is_collection: bool,
        has_polygonal_members: bool,
    ) -> TopologyGraph {
        let (atomic, provenance) = crate::geometry::overlay::self_node_segments_sourced(
            &self
                .chart_edges
                .iter()
                .map(|edge| edge.segment)
                .collect::<Vec<_>>(),
        );
        let mut bundles: HashMap<(TopologyNodeId, TopologyNodeId), EdgeBundle> =
            HashMap::with_capacity(atomic.len());
        for (piece, owners) in atomic.into_iter().zip(provenance.iter()) {
            if same_point(piece.start, piece.end) {
                continue;
            }
            let start = self.ensure_node(piece.start, |_| {});
            let end = self.ensure_node(piece.end, |_| {});
            let key = if start.index() <= end.index() {
                (start, end)
            } else {
                (end, start)
            };
            let bundle = bundles.entry(key).or_insert_with(|| EdgeBundle::new(piece));
            for &(chart_index, _reversed) in owners {
                let chart = &self.chart_edges[chart_index as usize];
                let source = self.source_edges[chart.source as usize];
                if !bundle.sources.contains(&chart.source) {
                    bundle.sources.push(chart.source);
                }
                match source.kind {
                    SourceEdgeKind::Line => {},
                    SourceEdgeKind::Area => {
                        bundle.area_count += 1;
                        bundle.area_net += source_direction(piece, chart.segment);
                    },
                }
            }
        }

        let mut edges = Vec::with_capacity(bundles.len());
        for bundle in bundles.into_values() {
            let midpoint = XY::new(
                f64::midpoint(bundle.segment.start.x, bundle.segment.end.x),
                f64::midpoint(bundle.segment.start.y, bundle.segment.end.y),
            );
            let area_interior = bundle.area_count > 0
                && area_support_contains(self.space, &self.area_support, midpoint);
            let is_boundary = !area_interior
                && (bundle.area_count % 2 == 1 || (bundle.area_count >= 2 && bundle.area_net != 0));
            self.ensure_node(bundle.segment.start, |_| {});
            self.ensure_node(bundle.segment.end, |_| {});
            edges.push(TopologyEdge {
                segment: bundle.segment,
                sources: bundle.sources.into_boxed_slice(),
                is_boundary,
            });
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
        let lineal_overlap_collection = is_collection
            && !has_polygonal_members
            && has_cross_component_line_overlap(&self.source_edges);
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
        TopologyGraph {
            space: self.space,
            axes: self.axes,
            declared_dimension,
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
            node_lookup: self.node_lookup,
        }
    }
}

struct EdgeBundle {
    segment: Segment,
    sources: Vec<u32>,
    area_count: usize,
    area_net: i32,
}

impl EdgeBundle {
    fn new(segment: Segment) -> Self {
        Self {
            segment,
            sources: Vec::new(),
            area_count: 0,
            area_net: 0,
        }
    }
}

fn source_direction(piece: Segment, source: Segment) -> i32 {
    let start = segment_projection(piece.start, source);
    let end = segment_projection(piece.end, source);
    if end.cmp_along(&start).is_ge() { 1 } else { -1 }
}

fn has_cross_component_line_overlap(edges: &[SourceEdge]) -> bool {
    edges.iter().enumerate().any(|(index, left)| {
        matches!(left.kind, SourceEdgeKind::Line)
            && edges[index + 1..].iter().any(|right| {
                matches!(right.kind, SourceEdgeKind::Line)
                    && left.component != right.component
                    && shared_segment_part(left.segment, right.segment)
                        .is_some_and(|(_, run)| !same_point(run[0], run[1]))
            })
    })
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

fn area_support_contains(space: TopologySpace, polygons: &[AreaSupport], point: XY) -> bool {
    polygons
        .iter()
        .any(|polygon| polygon_contains_interior(space, polygon, point))
}

fn polygon_contains_interior(space: TopologySpace, polygon: &AreaSupport, point: XY) -> bool {
    if !space.is_geographic() {
        return polygon.source.contains_point(point.point());
    }
    if geographic_ring_location(&polygon.shell, point) != RingLocation::Inside {
        return false;
    }
    polygon
        .holes
        .iter()
        .all(|hole| geographic_ring_location(hole, point) == RingLocation::Outside)
}

fn geographic_ring_location(ring: &ChartRing, point: XY) -> RingLocation {
    if point.y == 90.0 || point.y == -90.0 {
        let north = point.y == 90.0;
        if ring.points.iter().any(|candidate| candidate.y == point.y) {
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
    let (min_y, max_y) = ring
        .points
        .iter()
        .fold((f64::INFINITY, f64::NEG_INFINITY), |(min, max), point| {
            (min.min(point.y), max.max(point.y))
        });
    if (ring.north_pole && point.y > max_y) || (ring.south_pole && point.y < min_y) {
        // Once a polar ring's parallel is passed on the pole side, every
        // longitude is in the same quotient face. The ordinary chart ray
        // cannot express that cap because its pole-side closure is implicit.
        return RingLocation::Inside;
    }
    let chart = &ring.points;
    let x = translated_longitude(&chart, point.x);
    let query = XY::new(x, point.y);
    if chart
        .array_windows::<2>()
        .any(|pair| point_on_segment(query, pair[0], pair[1]))
    {
        return RingLocation::Boundary;
    }
    if chart.len() >= 3 && ring_contains_interior_open(&chart, query) {
        RingLocation::Inside
    } else {
        RingLocation::Outside
    }
}

fn lift_chart_longitude(point: XY, previous: Option<XY>) -> XY {
    let Some(previous) = previous else {
        return point;
    };
    // Choose the longitude copy closest to the preceding chart point. This
    // also joins a seam alias after the preceding point has already been
    // lifted (for example, `190 -> -180`), and keeps exact 360-degree jumps
    // on one chart representative as the published splitter does.
    let turns = ((previous.x - point.x) / 360.0).round();
    let x = point.x + 360.0 * turns;
    XY::new(x, point.y)
}

fn translated_longitude(chart: &[XY], longitude: f64) -> f64 {
    let (min, max) = chart
        .iter()
        .fold((f64::INFINITY, f64::NEG_INFINITY), |(min, max), point| {
            (min.min(point.x), max.max(point.x))
        });
    if !min.is_finite() || !max.is_finite() {
        return longitude;
    }
    let center = f64::midpoint(min, max);
    longitude + 360.0 * ((center - longitude) / 360.0).round()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::{GeometryKind, LineSeq, MultiMember, Ring};

    fn point(x: f64, y: f64) -> Point {
        Point::new_unchecked_xy(x, y)
    }

    fn line(points: &[(f64, f64)]) -> LineSeq {
        LineSeq::from_trusted(CoordSeq::from(
            points.iter().map(|&(x, y)| point(x, y)).collect::<Vec<_>>(),
        ))
    }

    fn ring(points: &[(f64, f64)]) -> Ring {
        Ring::from_trusted_closed(CoordSeq::from(
            points.iter().map(|&(x, y)| point(x, y)).collect::<Vec<_>>(),
        ))
    }

    fn polygon(shell: &[(f64, f64)], holes: &[&[(f64, f64)]]) -> Polygon {
        Polygon::new(ring(shell), holes.iter().map(|hole| ring(hole)).collect())
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
        let graph = TopologyGraph::geographic(&shape);
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
        let graph = TopologyGraph::geographic(&shape);
        assert_eq!(graph.nodes.len(), 1);
        assert_eq!(graph.node_for(XY::new(0.0, 90.0)), Some(TopologyNodeId(0)));
    }

    #[test]
    fn chart_cut_is_line_interior_not_source_boundary() {
        let shape = Shape::LineString(line(&[(170.0, 0.0), (-170.0, 0.0)]));
        let graph = TopologyGraph::geographic(&shape);
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
        let graph = TopologyGraph::cartesian(&shape);
        assert!(graph.contains_area_interior(XY::new(1.0, 1.0)));
        assert!(!graph.contains_area_interior(XY::new(5.0, 5.0)));
        assert!(graph.has_area_boundary(XY::new(5.0, 3.0)));
    }

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
        let graph = TopologyGraph::geographic(&shape);
        assert_eq!(
            geographic_ring_location(&graph.area_support[0].shell, XY::new(179.0, 8.0)),
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
        let graph = TopologyGraph::geographic(&shape);
        assert!(graph.contains_area_interior(XY::new(-120.0, 85.0)));
        assert!(graph.contains_area_interior(XY::new(120.0, 85.0)));
        assert!(graph.contains_area_interior(XY::new(0.0, 90.0)));
        assert!(!graph.contains_area_interior(XY::new(0.0, 70.0)));
    }

    #[test]
    fn line_boundary_parity_is_collection_wide() {
        let lines = crate::geometry::MultiLineString::try_new(CoordinateAxes::XY, vec![
            MultiMember::Value(line(&[(0.0, 0.0), (1.0, 0.0)])),
            MultiMember::Value(line(&[(1.0, 0.0), (2.0, 0.0)])),
        ])
        .expect("homogeneous lines");
        let graph = TopologyGraph::cartesian(&Shape::MultiLineString(lines));
        assert_eq!(graph.line_boundary.len(), 2);
        assert!(!graph.has_line_boundary(XY::new(1.0, 0.0)));
        assert!(graph.has_line_boundary(XY::new(0.0, 0.0)));
        assert!(graph.has_line_boundary(XY::new(2.0, 0.0)));
    }

    #[test]
    fn overlapping_line_members_keep_shared_support_and_cancel_duplicate_boundary() {
        let collection = Shape::GeometryCollection(
            vec![
                Shape::LineString(line(&[(0.0, 0.0), (2.0, 0.0)])),
                Shape::LineString(line(&[(0.0, 0.0), (2.0, 0.0)])),
                Shape::Point(point(4.0, 4.0)),
            ]
            .into(),
        );
        let graph = TopologyGraph::cartesian(&collection);
        assert_eq!(graph.effective_dimension, Some(Dimension::Curve));
        assert!(graph.line_boundary.is_empty());
        assert_eq!(graph.edges.len(), 1);
        assert_eq!(graph.edges[0].sources.len(), 2);
        assert!(graph.nodes.iter().any(|node| node.support.point));
    }

    #[test]
    fn noding_preserves_one_source_record_on_each_atomic_piece() {
        let collection = Shape::GeometryCollection(
            vec![
                Shape::LineString(line(&[(0.0, 0.0), (2.0, 2.0)])),
                Shape::LineString(line(&[(0.0, 2.0), (2.0, 0.0)])),
            ]
            .into(),
        );
        let graph = TopologyGraph::cartesian(&collection);
        assert_eq!(graph.edges.len(), 4);
        assert!(graph.edges.iter().all(|edge| edge.sources.len() == 1));
        assert!(!graph.has_line_boundary(XY::new(1.0, 1.0)));
    }

    #[test]
    fn typed_empty_keeps_axes_and_declared_dimension() {
        let shape = Shape::Empty(GeometryKind::Polygon, CoordinateAxes::XYZM);
        let graph = TopologyGraph::cartesian(&shape);
        assert_eq!(graph.axes, CoordinateAxes::XYZM);
        assert_eq!(graph.declared_dimension, Some(Dimension::Surface));
        assert_eq!(graph.effective_dimension, None);
        assert!(graph.nodes.is_empty());
        assert!(graph.edges.is_empty());
    }

    #[test]
    fn mixed_collection_keeps_highest_nonempty_dimension() {
        let collection = Shape::GeometryCollection(
            vec![
                Shape::Empty(GeometryKind::Point, CoordinateAxes::XYZ),
                Shape::LineString(line(&[(0.0, 0.0), (1.0, 0.0)])),
                Shape::Polygon(polygon(
                    &[(2.0, 0.0), (4.0, 0.0), (4.0, 2.0), (2.0, 2.0), (2.0, 0.0)],
                    &[],
                )),
            ]
            .into(),
        );
        let graph = TopologyGraph::cartesian(&collection);
        assert_eq!(graph.effective_dimension, Some(Dimension::Surface));
        assert_eq!(graph.axes, CoordinateAxes::XYZ);
    }
}
