#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use super::*;
use crate::HeapSize;
use crate::geometry::*;

#[derive(Clone, Copy)]
pub(crate) struct TopoPoint {
    pub(crate) point: Point,
    pub(crate) on: Loc,
}

#[derive(Clone, Copy)]
pub(crate) struct TopoEdge {
    pub(crate) segment: Segment,
    pub(crate) on: Loc,
    pub(crate) absorbs_points: bool,
}

#[derive(Clone, Copy)]
pub(crate) enum RawTopoEdgeKind {
    Line,
    Area,
}

#[derive(Clone, Copy)]
pub(crate) struct RawTopoEdge {
    pub(crate) segment: Segment,
    pub(crate) kind: RawTopoEdgeKind,
}

#[derive(Clone, Copy)]
pub(crate) struct PairEdgeLabel {
    pub(crate) left: Option<Loc>,
    pub(crate) right: Option<Loc>,
}

/// Cached whole-operand topology used by DE-9IM paths that need mod-2
/// boundary semantics across multi/collection operands.
#[expect(
    clippy::struct_excessive_bools,
    reason = "independent cached operand-shape predicates, not a state enum"
)]
pub(crate) struct RelateTopology {
    pub(crate) effective_dim: Option<Dimension>,
    pub(crate) boundary_dim: Option<Dimension>,
    pub(crate) declared_dim: Option<Dimension>,
    pub(crate) points: Box<[TopoPoint]>,
    pub(crate) edges: Box<[TopoEdge]>,
    pub(crate) area_polygons: Box<[Polygon]>,
    pub(crate) line_boundary: HashSet<PointKey>,
    pub(crate) area_boundary: HashSet<PointKey>,
    pub(crate) lineal_overlap_collection: bool,
    pub(crate) is_collection: bool,
    pub(crate) collection_has_puntal_support: bool,
    pub(crate) collection_has_overlapping_lineal_support: bool,
    pub(crate) bounds: Option<Bounds>,
}

impl RelateTopology {
    pub(crate) fn heap_bytes(&self) -> usize {
        self.points.heap_bytes()
            + self.edges.heap_bytes()
            + self
                .area_polygons
                .iter()
                .map(|polygon| {
                    std::iter::once(polygon.shell.coords())
                        .chain(polygon.holes.iter().map(crate::Ring::coords))
                        .map(CoordSeq::coordinate_bytes)
                        .sum::<usize>()
                })
                .sum::<usize>()
            + self.line_boundary.heap_bytes()
            + self.area_boundary.heap_bytes()
    }

    pub(crate) fn build(shape: &Shape) -> Self {
        let mut builder = RelateTopologyBuilder::default();
        builder.collect(shape);
        builder.finish(shape)
    }

    /// [`Self::locate_point`] but, when the exact geometry test would fall
    /// through to `Exterior`, trust the noding provenance: a node that is the
    /// endpoint of one of this operand's atomic edge pieces lies on that
    /// operand's linework even if its rounded coordinate is no longer exactly
    /// collinear with the original segment. The hint's own-operand ON-label
    /// (`Interior` for line interior, `Boundary` for an odd line/area
    /// endpoint) then applies, subject to the area-interior absorption that
    /// `locate_point` already resolved above the fallthrough.
    pub(crate) fn locate_point_hinted(&self, point: Point, hint: Option<Loc>) -> Loc {
        match self.locate_point(point) {
            Loc::Exterior => hint.unwrap_or(Loc::Exterior),
            located => located,
        }
    }

    pub(crate) fn locate_point(&self, point: Point) -> Loc {
        if self
            .area_polygons
            .iter()
            .any(|polygon| polygon.contains_point(point))
        {
            return Loc::Interior;
        }
        if self.area_interior_straddles_point(point) {
            return Loc::Interior;
        }
        if self.edges.iter().any(|edge| {
            edge.on == Loc::Boundary
                && point_on_segment(point, edge.segment.start, edge.segment.end)
        }) {
            return Loc::Boundary;
        }
        if self.has_boundary_point(point) {
            return Loc::Boundary;
        }
        if self.edges.iter().any(|edge| {
            edge.absorbs_points && point_on_segment(point, edge.segment.start, edge.segment.end)
        }) {
            return Loc::Interior;
        }
        if self
            .points
            .iter()
            .any(|candidate| same_point(candidate.point, point))
            || self
                .edges
                .iter()
                .any(|edge| point_on_segment(point, edge.segment.start, edge.segment.end))
        {
            return Loc::Interior;
        }
        Loc::Exterior
    }

    pub(crate) fn locate_edge_piece(&self, segment: Segment) -> Loc {
        let midpoint = segment_midpoint(segment);
        if self
            .area_polygons
            .iter()
            .any(|polygon| polygon.contains_point(midpoint))
        {
            return Loc::Interior;
        }
        if self.edges.iter().any(|edge| {
            edge.on == Loc::Boundary
                && point_on_segment(midpoint, edge.segment.start, edge.segment.end)
        }) {
            return Loc::Boundary;
        }
        if self.edges.iter().any(|edge| {
            edge.on == Loc::Interior
                && point_on_segment(midpoint, edge.segment.start, edge.segment.end)
        }) {
            return Loc::Interior;
        }
        Loc::Exterior
    }

    pub(crate) fn topology_edge_label(&self, segment: Segment) -> Option<Loc> {
        let midpoint = segment_midpoint(segment);
        self.edges
            .iter()
            .find(|edge| point_on_segment(midpoint, edge.segment.start, edge.segment.end))
            .map(|edge| edge.on)
    }

    pub(crate) fn has_boundary_point(&self, point: Point) -> bool {
        if self.area_contains_point(point) || self.area_interior_straddles_point(point) {
            return false;
        }
        let key = PointKey::new(point);
        (self.area_boundary.contains(&key)
            && (self.area_polygons.is_empty() || self.has_boundary_edge_point(point)))
            || (self.line_boundary.contains(&key) && !self.area_contains_point(point))
    }

    pub(crate) fn has_line_boundary_point(&self, point: Point) -> bool {
        self.line_boundary.contains(&PointKey::new(point))
    }

    pub(crate) fn area_contains_point(&self, point: Point) -> bool {
        self.area_polygons
            .iter()
            .any(|polygon| polygon.contains_point(point))
    }

    pub(crate) fn area_interior_straddles_point(&self, point: Point) -> bool {
        if self.area_polygons.is_empty() {
            return false;
        }
        let scale = 1e-7;
        let inside = |x: f64, y: f64| {
            let probe = Point::new_unchecked_xy(x, y);
            self.area_polygons
                .iter()
                .any(|polygon| polygon.contains_point(probe))
        };
        (inside(point.x - scale, point.y) && inside(point.x + scale, point.y))
            || (inside(point.x, point.y - scale) && inside(point.x, point.y + scale))
    }

    pub(crate) fn has_boundary_edge_point(&self, point: Point) -> bool {
        self.edges.iter().any(|edge| {
            edge.on == Loc::Boundary
                && point_on_segment(point, edge.segment.start, edge.segment.end)
        })
    }
}

crate::heapless!(TopoPoint, TopoEdge);

pub(crate) const fn merge_topo_loc(slot: &mut Option<Loc>, loc: Loc) {
    match (*slot, loc) {
        (Some(Loc::Interior), _) | (_, Loc::Interior) => *slot = Some(Loc::Interior),
        (Some(Loc::Boundary), _) | (_, Loc::Boundary) => *slot = Some(Loc::Boundary),
        _ => *slot = Some(loc),
    }
}

pub(crate) struct EdgeBundle {
    pub(crate) segment: Segment,
    pub(crate) line: bool,
    pub(crate) area_count: usize,
    pub(crate) area_net: i32,
}

impl EdgeBundle {
    pub(crate) fn label(&self, area_polygons: &[Polygon]) -> Loc {
        if self.area_count > 0 && area_polygons.is_empty() {
            return Loc::Interior;
        }
        if self.in_area_interior(area_polygons)
            || self.has_area_interior_on_both_sides(area_polygons)
        {
            return Loc::Interior;
        }
        // Odd boundary parity is boundary; an even bundle is boundary only
        // when its directed winding does not cancel (`area_net != 0`).
        if self.area_count % 2 == 1 || (self.area_count >= 2 && self.area_net != 0) {
            Loc::Boundary
        } else {
            Loc::Interior
        }
    }

    pub(crate) fn absorbs_points(&self, area_polygons: &[Polygon]) -> bool {
        self.in_area_interior(area_polygons)
            || self.has_area_interior_on_both_sides(area_polygons)
            || (self.area_count >= 2 && self.area_net == 0)
    }

    pub(crate) fn in_area_interior(&self, area_polygons: &[Polygon]) -> bool {
        let midpoint = segment_midpoint(self.segment);
        area_polygons
            .iter()
            .any(|polygon| polygon.contains_point(midpoint))
    }

    pub(crate) fn has_area_interior_on_both_sides(&self, area_polygons: &[Polygon]) -> bool {
        if self.area_count == 0 || area_polygons.is_empty() {
            return false;
        }
        let dx = self.segment.end.x - self.segment.start.x;
        let dy = self.segment.end.y - self.segment.start.y;
        let len2 = dx * dx + dy * dy;
        if len2 == 0.0 {
            return false;
        }
        let length = len2.sqrt();
        let scale = length.max(1.0) * 1e-7;
        let nx = -dy / length * scale;
        let ny = dx / length * scale;
        let midpoint = segment_midpoint(self.segment);
        let left = Point::new_unchecked_xy(midpoint.x + nx, midpoint.y + ny);
        let right = Point::new_unchecked_xy(midpoint.x - nx, midpoint.y - ny);
        area_polygons
            .iter()
            .any(|polygon| polygon.contains_point(left))
            && area_polygons
                .iter()
                .any(|polygon| polygon.contains_point(right))
    }
}

pub(crate) fn source_direction(piece: Segment, source: Segment) -> i32 {
    let start = segment_projection_fraction(piece.start, source);
    let end = segment_projection_fraction(piece.end, source);
    if end >= start { 1 } else { -1 }
}

pub(crate) fn finalize_topology_edges(
    raw_edges: &[RawTopoEdge],
    area_polygons: &[Polygon],
) -> Vec<TopoEdge> {
    if raw_edges.is_empty() {
        return Vec::new();
    }
    let segments: Vec<_> = raw_edges.iter().map(|edge| edge.segment).collect();
    let (atomic, sources) = overlay::self_node_segments_sourced(&segments);
    let mut bundles: HashMap<(PointKey, PointKey), EdgeBundle> =
        HashMap::with_capacity(atomic.len());
    for (piece, source) in atomic.into_iter().zip(sources) {
        if same_point(piece.start, piece.end) {
            continue;
        }
        let bundle = bundles
            .entry(undirected_segment_edge_key(piece))
            .or_insert(EdgeBundle {
                segment: piece,
                line: false,
                area_count: 0,
                area_net: 0,
            });
        match raw_edges[source as usize].kind {
            RawTopoEdgeKind::Line => {
                bundle.line = true;
            },
            RawTopoEdgeKind::Area => {
                bundle.area_count += 1;
                bundle.area_net += source_direction(piece, raw_edges[source as usize].segment);
            },
        }
    }
    bundles
        .into_values()
        .filter_map(|bundle| {
            (bundle.line || bundle.area_count > 0).then_some(TopoEdge {
                segment: bundle.segment,
                on: bundle.label(area_polygons),
                absorbs_points: bundle.absorbs_points(area_polygons),
            })
        })
        .collect()
}

pub(crate) fn boundary_dimension_from(
    points: &[TopoPoint],
    edges: &[TopoEdge],
) -> Option<Dimension> {
    if edges.iter().any(|edge| edge.on == Loc::Boundary) {
        Some(Dimension::Curve)
    } else if points.iter().any(|point| point.on == Loc::Boundary) {
        Some(Dimension::Point)
    } else {
        None
    }
}

pub(crate) fn effective_dimension_from(
    shape: &Shape,
    points: &[TopoPoint],
    edges: &[TopoEdge],
    areas: &[Polygon],
) -> Option<Dimension> {
    if declared_dimension(shape) == Some(Dimension::Surface) {
        return effective_dimension(shape);
    }
    if !areas.is_empty() {
        Some(Dimension::Surface)
    } else if !edges.is_empty() {
        Some(Dimension::Curve)
    } else if !points.is_empty() {
        Some(Dimension::Point)
    } else {
        None
    }
}

#[derive(Default)]
pub(crate) struct RelateTopologyBuilder {
    pub(crate) points: Vec<TopoPoint>,
    pub(crate) point_keys: HashMap<PointKey, usize>,
    pub(crate) edges: Vec<RawTopoEdge>,
    pub(crate) boundary: HashSet<PointKey>,
    pub(crate) area_boundary: HashSet<PointKey>,
    pub(crate) area_polygons: Vec<Polygon>,
}

impl RelateTopologyBuilder {
    pub(crate) fn collect(&mut self, shape: &Shape) {
        match shape {
            Shape::Empty(..) => {},
            Shape::Point(point) => self.push_point(*point, Loc::Interior),
            Shape::MultiPoint(points) => {
                for point in points {
                    self.push_point(point, Loc::Interior);
                }
            },
            Shape::LineString(line) => self.collect_line(line),
            Shape::MultiLineString(lines) => {
                for line in lines {
                    self.collect_line(line);
                }
            },
            Shape::Polygon(polygon) => self.collect_polygon(polygon),
            Shape::MultiPolygon(polygons) => {
                for polygon in polygons {
                    self.collect_polygon(polygon);
                }
            },
            Shape::GeometryCollection(parts) => {
                for part in parts {
                    self.collect(part);
                }
            },
        }
    }

    pub(crate) fn collect_line(&mut self, line: &CoordSeq) {
        let first_edge = self.edges.len();
        for [start, end] in line.segment_pairs() {
            if !same_point(start, end) {
                self.edges.push(RawTopoEdge {
                    segment: Segment {
                        start: start.xy(),
                        end: end.xy(),
                    },
                    kind: RawTopoEdgeKind::Line,
                });
            }
        }
        if self.edges.len() == first_edge {
            if let Some(point) = line.first() {
                self.push_point(point, Loc::Interior);
            }
            return;
        }
        let first = self.edges[first_edge].segment.start;
        let last = self.edges.last().map_or(first, |edge| edge.segment.end);
        self.toggle_boundary(first);
        self.toggle_boundary(last);
    }

    pub(crate) fn collect_polygon(&mut self, polygon: &Polygon) {
        if polygon_has_nondegenerate_area(polygon) {
            self.area_polygons.push(polygon.clone());
        }
        for ring in polygon.rings() {
            let first_edge = self.edges.len();
            for [start, end] in ring.segment_pairs() {
                if !same_point(start, end) {
                    self.edges.push(RawTopoEdge {
                        segment: Segment {
                            start: start.xy(),
                            end: end.xy(),
                        },
                        kind: RawTopoEdgeKind::Area,
                    });
                    self.area_boundary.insert(PointKey::new(start));
                    self.area_boundary.insert(PointKey::new(end));
                    self.push_point(start.xy().point(), Loc::Boundary);
                    self.push_point(end.xy().point(), Loc::Boundary);
                }
            }
            if self.edges.len() == first_edge
                && let Some(point) = ring.first()
            {
                // A fully collapsed ring (all coordinates identical) keeps a
                // declared polygonal origin: its degenerate support behaves as
                // boundary, so a coincident line endpoint grades `BB`, not the
                // line-interior `IB` an `Interior` label would mint.
                self.area_boundary.insert(PointKey::new(point));
                self.push_point(point, Loc::Boundary);
            }
        }
    }

    pub(crate) fn finish(mut self, shape: &Shape) -> RelateTopology {
        let line_boundary = std::mem::take(&mut self.boundary);
        let boundary: Vec<_> = line_boundary.iter().copied().collect();
        for key in boundary {
            self.push_point(key.xy().point(), Loc::Boundary);
        }
        let edges = finalize_topology_edges(&self.edges, &self.area_polygons);
        let effective_dim =
            effective_dimension_from(shape, &self.points, &edges, &self.area_polygons);
        let boundary_dim = boundary_dimension_from(&self.points, &edges);
        let declared_dim = declared_dimension(shape);
        let has_overlapping_lineal = collection_has_overlapping_lineal_members(shape);
        RelateTopology {
            effective_dim,
            boundary_dim,
            declared_dim,
            points: self.points.into_boxed_slice(),
            edges: edges.into_boxed_slice(),
            area_polygons: self.area_polygons.into_boxed_slice(),
            line_boundary,
            area_boundary: self.area_boundary,
            lineal_overlap_collection: has_overlapping_lineal
                && !shape_has_polygonal_members(shape),
            is_collection: matches!(shape, Shape::GeometryCollection(_)),
            collection_has_puntal_support: matches!(shape, Shape::GeometryCollection(_))
                && shape_has_puntal_support(shape),
            collection_has_overlapping_lineal_support: has_overlapping_lineal,
            bounds: shape.bounds(),
        }
    }

    pub(crate) fn push_point(&mut self, point: Point, on: Loc) {
        let key = PointKey::new(point);
        match self.point_keys.entry(key) {
            std::collections::hash_map::Entry::Vacant(entry) => {
                let index = self.points.len();
                entry.insert(index);
                self.points.push(TopoPoint {
                    point: point.to_xy(),
                    on,
                });
            },
            std::collections::hash_map::Entry::Occupied(entry) => {
                let stored = &mut self.points[*entry.get()];
                match (stored.on, on) {
                    (Loc::Interior, _) | (_, Loc::Interior) => stored.on = Loc::Interior,
                    (Loc::Boundary, _) | (_, Loc::Boundary) => stored.on = Loc::Boundary,
                    (Loc::Exterior, Loc::Exterior) => {},
                }
            },
        }
    }

    pub(crate) fn toggle_boundary(&mut self, point: XY) {
        let key = PointKey::new(point);
        if !self.boundary.insert(key) {
            self.boundary.remove(&key);
        }
    }
}

pub(crate) fn declared_dimension(shape: &Shape) -> Option<Dimension> {
    dimension(shape, DimMode::Declared)
}
