use crate::collections::{HashSet, HashSetExt as _};
use crate::geometry::relate::{De9im, Loc, polygon_has_nondegenerate_area};
use crate::geometry::topology::{
    AreaSupport, TopologyDomain, TopologyGraph, TopologyKey, TopologyLocation, TopologyNodeId,
    TopologySpace,
};
use crate::geometry::{
    Bounds, CoordSeq, Coordinates as _, HashMap, HashMapExt as _, PointKey, Polygon, PreparedPlan,
    Ring, Segment, Shape, XY, overlay, same_point, segments_intersect,
};

#[derive(Clone, Copy)]
pub(crate) struct GeographicPrepared<'a> {
    plan: &'a PreparedPlan,
    frame_cache: &'a crate::geometry::FrameDependentCaches,
    frame: crate::geographic::GeographicFrame,
}

impl GeographicPrepared<'_> {
    pub(crate) const fn new<'a>(
        plan: &'a PreparedPlan,
        frame_cache: &'a crate::geometry::FrameDependentCaches,
        frame: crate::geographic::GeographicFrame,
    ) -> GeographicPrepared<'a> {
        GeographicPrepared {
            plan,
            frame_cache,
            frame,
        }
    }

    fn graph(self) -> std::sync::Arc<TopologyGraph> {
        self.frame_cache
            .geographic_topology_graph(self.plan, self.frame)
    }
}

/// Compute the complete relation matrix from the two source-owned topology
/// graphs. Every named predicate is a projection of this result.
pub(crate) fn graph_relate_shapes(left: &Shape, right: &Shape) -> De9im {
    graph_relate_shapes_in(left, right, TopologySpace::Cartesian)
}

#[cfg(test)]
fn graph_relate_shapes_geographic(left: &Shape, right: &Shape) -> De9im {
    graph_relate_shapes_in(left, right, TopologySpace::Geographic)
}

fn graph_relate_shapes_in(left: &Shape, right: &Shape, space: TopologySpace) -> De9im {
    let left = PreparedPlan::transient(left);
    let right = PreparedPlan::transient(right);
    graph_relate_plans(&left, &right, space)
}

pub(crate) fn graph_relate_data(left: &PreparedPlan, right: &PreparedPlan) -> De9im {
    if let Some(matrix) = try_cartesian_areal_relate(left.shape(), right.shape()) {
        return matrix;
    }
    graph_relate_data_oracle(left, right)
}

/// The source-graph relation path, retained as the complete fallback and the
/// direct arrangement fold's test oracle.
fn graph_relate_data_oracle(left: &PreparedPlan, right: &PreparedPlan) -> De9im {
    graph_relate_data_with_graphs(left, right, left.topology_graph(), right.topology_graph())
}

/// Compute a Cartesian relation from topology graphs already resolved by the
/// caller. The direct areal fold remains the first admission path.
fn graph_relate_data_with_graphs(
    left: &PreparedPlan,
    right: &PreparedPlan,
    left_graph: &TopologyGraph,
    right_graph: &TopologyGraph,
) -> De9im {
    if let Some(matrix) = try_areal_relate(left.shape(), right.shape()) {
        return matrix;
    }
    if let Some(point) = shape_point(right.shape()) {
        return graph_relate_to_point(left_graph, point);
    }
    if let Some(point) = shape_point(left.shape()) {
        return graph_relate_to_point(right_graph, point).transpose();
    }
    graph_relate(left_graph, right_graph)
}

/// Compute a Cartesian relation while reusing the left operand's resolved
/// graph. The right graph remains lazy so direct areal pairs do no graph work.
pub(crate) fn graph_relate_data_with_left_graph(
    left: &PreparedPlan,
    left_graph: &TopologyGraph,
    right: &PreparedPlan,
) -> De9im {
    if let Some(matrix) = try_cartesian_areal_relate(left.shape(), right.shape()) {
        return matrix;
    }
    if let Some(point) = shape_point(right.shape()) {
        return graph_relate_to_point(left_graph, point);
    }
    if let Some(point) = shape_point(left.shape()) {
        return graph_relate_to_point(right.topology_graph(), point).transpose();
    }
    graph_relate(left_graph, right.topology_graph())
}

/// Compute a Cartesian relation while reusing the right operand's resolved
/// graph. The left graph remains lazy so direct areal pairs do no graph work.
pub(crate) fn graph_relate_data_with_right_graph(
    left: &PreparedPlan,
    right: &PreparedPlan,
    right_graph: &TopologyGraph,
) -> De9im {
    if let Some(matrix) = try_cartesian_areal_relate(left.shape(), right.shape()) {
        return matrix;
    }
    if let Some(point) = shape_point(right.shape()) {
        return graph_relate_to_point(left.topology_graph(), point);
    }
    if let Some(point) = shape_point(left.shape()) {
        return graph_relate_to_point(right_graph, point).transpose();
    }
    graph_relate(left.topology_graph(), right_graph)
}

pub(crate) fn graph_relate_data_geographic(
    left: GeographicPrepared<'_>,
    right: GeographicPrepared<'_>,
) -> De9im {
    debug_assert_eq!(left.frame, right.frame);
    if let Some(matrix) =
        try_areal_relate_geographic(left.plan.shape(), right.plan.shape(), left.frame)
    {
        return matrix;
    }
    if let Some(point) = shape_point(right.plan.shape()) {
        if let Some(left_point) = shape_point(left.plan.shape()) {
            return point_relate_point(left_point, point, left.frame).transpose();
        }
        let left_graph = left.graph();
        return graph_relate_to_point(&left_graph, point);
    }
    if let Some(point) = shape_point(left.plan.shape()) {
        let right_graph = right.graph();
        return graph_relate_to_point(&right_graph, point).transpose();
    }
    let left_graph = left.graph();
    let right_graph = right.graph();
    graph_relate(&left_graph, &right_graph)
}

/// Compute a geographic relation from topology graphs already resolved by the
/// caller. This keeps the direct areal fold ahead of graph assembly.
pub(crate) fn graph_relate_data_geographic_with_left_graph(
    left: &PreparedPlan,
    left_graph: &TopologyGraph,
    right: GeographicPrepared<'_>,
) -> De9im {
    if let Some(matrix) = try_areal_relate_geographic(left.shape(), right.plan.shape(), right.frame)
    {
        return matrix;
    }
    if let Some(point) = shape_point(right.plan.shape()) {
        if let Some(left_point) = shape_point(left.shape()) {
            return point_relate_point(left_point, point, right.frame).transpose();
        }
        return graph_relate_to_point(left_graph, point);
    }
    if let Some(point) = shape_point(left.shape()) {
        let right_graph = right.graph();
        return graph_relate_to_point(&right_graph, point).transpose();
    }
    let right_graph = right.graph();
    graph_relate(left_graph, &right_graph)
}

/// Compute a geographic relation while reusing the right operand's resolved
/// graph. The left graph remains lazy so direct areal pairs do no graph work.
pub(crate) fn graph_relate_data_geographic_with_right_graph(
    left: GeographicPrepared<'_>,
    right: &PreparedPlan,
    right_graph: &TopologyGraph,
) -> De9im {
    if let Some(matrix) = try_areal_relate_geographic(left.plan.shape(), right.shape(), left.frame)
    {
        return matrix;
    }
    if let Some(point) = shape_point(right.shape()) {
        if let Some(left_point) = shape_point(left.plan.shape()) {
            return point_relate_point(left_point, point, left.frame).transpose();
        }
        let left_graph = left.graph();
        return graph_relate_to_point(&left_graph, point);
    }
    if let Some(point) = shape_point(left.plan.shape()) {
        return graph_relate_to_point(right_graph, point).transpose();
    }
    let left_graph = left.graph();
    graph_relate(&left_graph, right_graph)
}

fn graph_relate_plans(left: &PreparedPlan, right: &PreparedPlan, space: TopologySpace) -> De9im {
    match space {
        TopologySpace::Cartesian => try_areal_relate_in(left.shape(), right.shape(), space)
            .unwrap_or_else(|| graph_relate_data_oracle(left, right)),
        TopologySpace::Geographic => {
            let unit = crate::geographic::AngularFactor::try_new(
                "test degrees",
                std::f64::consts::PI / 180.0,
            )
            .unwrap();
            let frame = crate::geographic::GeographicFrame::try_new(unit, unit, 0.0).unwrap();
            let left_cache = crate::geometry::FrameDependentCaches::default();
            let right_cache = crate::geometry::FrameDependentCaches::default();
            graph_relate_data_geographic(
                GeographicPrepared::new(left, &left_cache, frame),
                GeographicPrepared::new(right, &right_cache, frame),
            )
        },
    }
}

fn try_areal_relate_in(left: &Shape, right: &Shape, space: TopologySpace) -> Option<De9im> {
    (space == TopologySpace::Cartesian)
        .then(|| try_cartesian_areal_relate(left, right))
        .flatten()
}

fn try_cartesian_areal_relate(left: &Shape, right: &Shape) -> Option<De9im> {
    try_convex_relate(left, right).or_else(|| try_areal_relate(left, right))
}

fn try_convex_relate(left: &Shape, right: &Shape) -> Option<De9im> {
    let (Shape::Polygon(left), Shape::Polygon(right)) = (left, right) else {
        return None;
    };
    let relation = crate::geometry::convex_contact::try_classify(left, right)?;
    Some(match relation {
        crate::geometry::convex_contact::ConvexRelation::Disjoint => De9im(*b"FF2FF1212"),
        crate::geometry::convex_contact::ConvexRelation::ProperCrossing => De9im(*b"212101212"),
        crate::geometry::convex_contact::ConvexRelation::LeftContainsRight => De9im(*b"212FF1FF2"),
        crate::geometry::convex_contact::ConvexRelation::RightContainsLeft => De9im(*b"2FF1FF212"),
    })
}

fn try_areal_relate_geographic(
    left: &Shape,
    right: &Shape,
    frame: crate::geographic::GeographicFrame,
) -> Option<De9im> {
    let left_bounds = left.bounds()?;
    let right_bounds = right.bounds()?;
    (crate::geometry::raw_coordinate_bounds_are_sound_in_frame(Some(left), left_bounds, frame)
        && crate::geometry::raw_coordinate_bounds_are_sound_in_frame(
            Some(right),
            right_bounds,
            frame,
        ))
    .then(|| try_areal_relate(left, right))
    .flatten()
}

/// Fold a complete DE-9IM matrix from the one arrangement shared by two
/// nondegenerate Cartesian areal operands. Source graphs retain boundary-only
/// degeneracies, so those inputs deliberately stay on the graph path.
fn try_areal_relate(left: &Shape, right: &Shape) -> Option<De9im> {
    let mut left_polygons = Vec::new();
    let mut right_polygons = Vec::new();
    collect_areal_polygons(left, &mut left_polygons)?;
    collect_areal_polygons(right, &mut right_polygons)?;
    if left_polygons.is_empty() || right_polygons.is_empty() {
        return None;
    }

    let built = overlay::build_areal_arrangement(&left_polygons, &right_polygons);
    let mut matrix = De9im::empty_disjoint();
    for winding in &built.windings {
        matrix.set_at_least(
            areal_location(winding[0]),
            areal_location(winding[1]),
            crate::geometry::Dimension::Surface,
        );
    }
    built
        .arrangement
        .for_each_edge_piece(|_multiplicity, _from, _to, left_face, right_face| {
            matrix.set_at_least(
                areal_edge_location(
                    built.windings[left_face as usize][0],
                    built.windings[right_face as usize][0],
                ),
                areal_edge_location(
                    built.windings[left_face as usize][1],
                    built.windings[right_face as usize][1],
                ),
                crate::geometry::Dimension::Curve,
            );
        });
    for vertex in 0..built.arrangement.points.len() {
        let range = built.arrangement.starts[vertex] as usize
            ..built.arrangement.starts[vertex + 1] as usize;
        let mut left_any = false;
        let mut left_all = true;
        let mut right_any = false;
        let mut right_all = true;
        for slot in range {
            let winding = built.windings[built.arrangement.face_of[slot] as usize];
            let left_inside = winding[0] >= 1;
            let right_inside = winding[1] >= 1;
            left_any |= left_inside;
            left_all &= left_inside;
            right_any |= right_inside;
            right_all &= right_inside;
        }
        matrix.set_at_least(
            areal_vertex_location(left_any, left_all),
            areal_vertex_location(right_any, right_all),
            crate::geometry::Dimension::Point,
        );
    }
    Some(matrix)
}

fn collect_areal_polygons<'a>(shape: &'a Shape, polygons: &mut Vec<&'a Polygon>) -> Option<()> {
    match shape {
        Shape::Polygon(polygon) => collect_areal_polygon(polygon, polygons),
        Shape::MultiPolygon(parts) => {
            for polygon in parts {
                collect_areal_polygon(polygon, polygons)?;
            }
            Some(())
        },
        Shape::GeometryCollection(parts) => {
            for part in parts {
                collect_areal_polygons(part, polygons)?;
            }
            Some(())
        },
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Empty(..) => None,
    }
}

fn collect_areal_polygon<'a>(polygon: &'a Polygon, polygons: &mut Vec<&'a Polygon>) -> Option<()> {
    if polygon.rings().any(|ring| {
        ring.coord_count() < Ring::MIN_VERTICES_CLOSED
            || crate::geometry::ring_winding(ring).is_degenerate()
            || ring
                .segment_pairs()
                .any(|[start, end]| same_point(start, end))
    }) {
        return None;
    }
    polygons.push(polygon);
    Some(())
}

const fn areal_location(winding: i32) -> Loc {
    if winding >= 1 {
        Loc::Interior
    } else {
        Loc::Exterior
    }
}

const fn areal_edge_location(left_winding: i32, right_winding: i32) -> Loc {
    match (left_winding >= 1, right_winding >= 1) {
        (true, true) => Loc::Interior,
        (false, false) => Loc::Exterior,
        _ => Loc::Boundary,
    }
}

const fn areal_vertex_location(any: bool, all: bool) -> Loc {
    if all {
        Loc::Interior
    } else if any {
        Loc::Boundary
    } else {
        Loc::Exterior
    }
}

/// Exact `intersects` certificate for a rectangle row. A closed rectangle
/// intersects a source iff one of its corners is on the source, one source
/// graph node lies in the rectangle, or their boundary segments meet. The
/// general case still returns the complete matrix below this certificate.
pub(crate) fn graph_intersects_rectangle_data(
    source: &PreparedPlan,
    source_graph: Option<&TopologyGraph>,
    geographic_source: Option<GeographicPrepared<'_>>,
    rectangle: &Shape,
) -> Option<bool> {
    let bounds = rectangle.bounds()?;
    if !is_axis_aligned_rectangle(rectangle, bounds) {
        return None;
    }
    let geographic_graph;
    let graph = if let Some(graph) = source_graph {
        graph
    } else if let Some(source) = geographic_source {
        geographic_graph = source.graph();
        geographic_graph.as_ref()
    } else {
        source.topology_graph()
    };
    if bounds
        .corners()
        .into_iter()
        .map(|[x, y]| XY::new(x, y))
        .any(|point| graph.locate_point(point) != TopologyLocation::Exterior)
        || graph
            .nodes
            .iter()
            .any(|node| bounds.contains_xy(node.point))
    {
        return Some(true);
    }
    let rectangle_edges = rectangle_edges(bounds);
    Some(graph.edges.iter().any(|edge| {
        bounds.contains_xy(edge.segment.start)
            || bounds.contains_xy(edge.segment.end)
            || rectangle_edges
                .iter()
                .copied()
                .any(|candidate| segments_intersect(edge.segment, candidate))
    }))
}

fn rectangle_edges(bounds: Bounds) -> [Segment; 4] {
    let corners = bounds.corners();
    [
        Segment {
            start: XY::new(corners[0][0], corners[0][1]),
            end: XY::new(corners[1][0], corners[1][1]),
        },
        Segment {
            start: XY::new(corners[1][0], corners[1][1]),
            end: XY::new(corners[2][0], corners[2][1]),
        },
        Segment {
            start: XY::new(corners[2][0], corners[2][1]),
            end: XY::new(corners[3][0], corners[3][1]),
        },
        Segment {
            start: XY::new(corners[3][0], corners[3][1]),
            end: XY::new(corners[0][0], corners[0][1]),
        },
    ]
}

#[expect(
    clippy::float_cmp,
    reason = "the certificate compares vertices with their exact bounds"
)]
/// Whether `shape` covers `bounds` exactly: a hole-free polygon whose every
/// vertex sits on the box, whose every step runs along one of its sides, and
/// which reaches all four corners. Extra collinear vertices along a side are
/// admitted; a degenerate box is not.
pub(crate) fn is_axis_aligned_rectangle(shape: &Shape, bounds: Bounds) -> bool {
    let Shape::Polygon(polygon) = shape else {
        return false;
    };
    if !polygon.holes.is_empty() || bounds.minx() == bounds.maxx() || bounds.miny() == bounds.maxy()
    {
        return false;
    }
    let shell = polygon.shell.coords();
    if shell.coord_count() < 5 {
        return false;
    }
    let (xs, ys) = (shell.xs(), shell.ys());
    let mut seen = [false; 4];
    for point in shell.points().take(shell.coord_count() - 1) {
        let on_vertical = point.x == bounds.minx() || point.x == bounds.maxx();
        let on_horizontal = point.y == bounds.miny() || point.y == bounds.maxy();
        if !on_vertical && !on_horizontal {
            return false;
        }
        if on_vertical && on_horizontal {
            let col = usize::from(point.x == bounds.maxx());
            let row = usize::from(point.y == bounds.maxy());
            seen[col * 2 + row] = true;
        }
    }
    if (0..shell.coord_count() - 1)
        .any(|index| xs[index] != xs[index + 1] && ys[index] != ys[index + 1])
    {
        return false;
    }
    seen.iter().all(|&corner| corner)
}

fn graph_relate(left: &TopologyGraph, right: &TopologyGraph) -> De9im {
    assert_eq!(
        left.domain, right.domain,
        "relation graphs must share a coordinate domain"
    );
    if let Some(point) = single_point(right) {
        return graph_relate_to_point(left, point);
    }
    if let Some(point) = single_point(left) {
        return graph_relate_to_point(right, point).transpose();
    }
    graph_relate_with_node_cells(left, right, add_node_cells)
}

fn graph_relate_with_node_cells(
    left: &TopologyGraph,
    right: &TopologyGraph,
    add_nodes: impl FnOnce(
        &mut De9im,
        &TopologyGraph,
        &TopologyGraph,
        &[XY],
        &HashMap<TopologyKey, NodeToken>,
        &HashMap<PointKey, EdgeLabel>,
    ),
) -> De9im {
    let mut matrix = De9im::empty_disjoint();
    add_face_cells(&mut matrix, left, right);

    let (edges, hints) = pair_edges(left, right);
    add_edge_cells(&mut matrix, left, right, &edges);

    let mut nodes = Vec::with_capacity(left.nodes.len() + right.nodes.len() + edges.len() * 2);
    let mut seen = HashSet::new();
    let mut node_owners = HashMap::new();
    let mut transient = HashSet::new();
    for node in left.nodes.iter().chain(right.nodes.iter()) {
        push_node(
            &mut nodes,
            &mut seen,
            &mut node_owners,
            &mut transient,
            node.point,
            left,
            right,
        );
    }
    for (segment, _) in &edges {
        push_node(
            &mut nodes,
            &mut seen,
            &mut node_owners,
            &mut transient,
            segment.start,
            left,
            right,
        );
        push_node(
            &mut nodes,
            &mut seen,
            &mut node_owners,
            &mut transient,
            segment.end,
            left,
            right,
        );
    }
    add_nodes(&mut matrix, left, right, &nodes, &node_owners, &hints);
    suppress_lineal_overlap_artifacts(&mut matrix, left, right);
    matrix
}

#[cfg(test)]
fn graph_relate_with_full_node_locations(left: &TopologyGraph, right: &TopologyGraph) -> De9im {
    graph_relate_with_node_cells(left, right, |matrix, left, right, nodes, _, hints| {
        let no_node_owners = HashMap::new();
        add_node_cells(matrix, left, right, nodes, &no_node_owners, hints);
    })
}

fn single_point(graph: &TopologyGraph) -> Option<XY> {
    (graph.edges.is_empty() && graph.area_support.is_empty() && graph.source_points.len() == 1)
        .then(|| graph.source_points[0].point.xy())
}

fn shape_point(shape: &Shape) -> Option<XY> {
    match shape {
        Shape::Point(point) => Some(point.xy()),
        _ => None,
    }
}

fn point_relate_point(left: XY, right: XY, frame: crate::geographic::GeographicFrame) -> De9im {
    let mut matrix = De9im::empty_disjoint();
    let left_latitude = crate::geographic::Latitude::try_new(frame, left.y)
        .expect("validated geographic latitude")
        .get();
    let right_latitude = crate::geographic::Latitude::try_new(frame, right.y)
        .expect("validated geographic latitude")
        .get();
    let same_latitude = left_latitude.to_bits() == right_latitude.to_bits();
    let latitude_is_pole = left_latitude.abs().to_bits() == frame.pole().to_bits();
    let same_longitude = frame
        .longitude()
        .canonicalize(left.x)
        .expect("validated geographic longitude")
        == frame
            .longitude()
            .canonicalize(right.x)
            .expect("validated geographic longitude");
    if same_latitude && (latitude_is_pole || same_longitude) {
        matrix.set_at_least(
            Loc::Interior,
            Loc::Interior,
            crate::geometry::Dimension::Point,
        );
    } else {
        matrix.set_at_least(
            Loc::Interior,
            Loc::Exterior,
            crate::geometry::Dimension::Point,
        );
        matrix.set_at_least(
            Loc::Exterior,
            Loc::Interior,
            crate::geometry::Dimension::Point,
        );
    }
    matrix
}

fn graph_relate_to_point(graph: &TopologyGraph, point: XY) -> De9im {
    let mut matrix = De9im::empty_disjoint();
    let location = graph.locate_point(point);
    matrix.set_at_least(
        to_loc(location),
        Loc::Interior,
        crate::geometry::Dimension::Point,
    );
    if let Some(dimension) = graph.effective_dimension
        && (dimension != crate::geometry::Dimension::Point
            || graph.has_interior_support_other_than(point))
    {
        matrix.set_at_least(Loc::Interior, Loc::Exterior, dimension);
    }
    if let Some(dimension) = graph.boundary_dimension
        && (dimension != crate::geometry::Dimension::Point
            || graph.has_boundary_support_other_than(point))
    {
        matrix.set_at_least(Loc::Boundary, Loc::Exterior, dimension);
    }
    matrix
}

fn add_face_cells(matrix: &mut De9im, left: &TopologyGraph, right: &TopologyGraph) {
    let left_polygons = area_polygons(left);
    let right_polygons = area_polygons(right);
    if left.effective_dimension == Some(crate::geometry::Dimension::Surface)
        && left_polygons.is_empty()
    {
        matrix.set_at_least(
            Loc::Interior,
            Loc::Exterior,
            crate::geometry::Dimension::Surface,
        );
    }
    if right.effective_dimension == Some(crate::geometry::Dimension::Surface)
        && right_polygons.is_empty()
    {
        matrix.set_at_least(
            Loc::Exterior,
            Loc::Interior,
            crate::geometry::Dimension::Surface,
        );
    }
    match (left_polygons.is_empty(), right_polygons.is_empty()) {
        (true, true) => {},
        (false, true) => matrix.set_at_least(
            Loc::Interior,
            Loc::Exterior,
            crate::geometry::Dimension::Surface,
        ),
        (true, false) => matrix.set_at_least(
            Loc::Exterior,
            Loc::Interior,
            crate::geometry::Dimension::Surface,
        ),
        (false, false) => {
            let arrangement = if left.domain.space() == TopologySpace::Geographic {
                let (left_polygons, right_polygons) = geographic_area_polygons(left, right);
                overlay::build_areal_arrangement(&left_polygons, &right_polygons)
            } else {
                overlay::build_areal_arrangement(&left_polygons, &right_polygons)
            };
            for winding in arrangement.windings {
                let left_loc = if winding[0] >= 1 {
                    Loc::Interior
                } else {
                    Loc::Exterior
                };
                let right_loc = if winding[1] >= 1 {
                    Loc::Interior
                } else {
                    Loc::Exterior
                };
                matrix.set_at_least(left_loc, right_loc, crate::geometry::Dimension::Surface);
            }
        },
    }
    if left.effective_dimension.is_none() {
        set_dimension(
            matrix,
            Loc::Exterior,
            Loc::Interior,
            right.effective_dimension,
        );
        set_dimension(
            matrix,
            Loc::Exterior,
            Loc::Boundary,
            right.boundary_dimension,
        );
    }
    if right.effective_dimension.is_none() {
        set_dimension(
            matrix,
            Loc::Interior,
            Loc::Exterior,
            left.effective_dimension,
        );
        set_dimension(
            matrix,
            Loc::Boundary,
            Loc::Exterior,
            left.boundary_dimension,
        );
    }
}

fn set_dimension(
    matrix: &mut De9im,
    row: Loc,
    col: Loc,
    dimension: Option<crate::geometry::Dimension>,
) {
    if let Some(dimension) = dimension {
        matrix.set_at_least(row, col, dimension);
    }
}

fn geographic_area_polygons(
    left: &TopologyGraph,
    right: &TopologyGraph,
) -> (Vec<Polygon>, Vec<Polygon>) {
    let TopologyDomain::Geographic(left_frame) = left.domain else {
        unreachable!("geographic relation has a geographic left graph")
    };
    let TopologyDomain::Geographic(right_frame) = right.domain else {
        unreachable!("geographic relation has a geographic right graph")
    };
    debug_assert_eq!(left_frame, right_frame);
    let period = left_frame.longitude().period();
    let anchor = area_center(left)
        .or_else(|| area_center(right))
        .unwrap_or(0.0);
    (
        chart_area_polygons(left, anchor, period),
        chart_area_polygons(right, anchor, period),
    )
}

fn area_center(graph: &TopologyGraph) -> Option<f64> {
    graph
        .area_support
        .iter()
        .filter(|support| polygon_has_nondegenerate_area(support.source()))
        .map(|support| {
            let (shell, _) = support.geographic_rings();
            ring_center(&shell.points)
        })
        .next()
}

fn chart_area_polygons(graph: &TopologyGraph, anchor: f64, period: f64) -> Vec<Polygon> {
    graph
        .area_support
        .iter()
        .filter(|support| polygon_has_nondegenerate_area(support.source()))
        .filter_map(|support| {
            let (support_shell, support_holes) = support.geographic_rings();
            let shell_center = ring_center(&support_shell.points);
            let shift = chart_shift(anchor, shell_center, period);
            let shell = chart_ring(&support_shell.points, shift)?;
            let holes = support_holes
                .iter()
                .filter_map(|hole| chart_ring(&hole.points, shift))
                .collect();
            Some(Polygon::new(shell, holes))
        })
        .collect()
}

fn ring_center(points: &[XY]) -> f64 {
    let (min, max) = points
        .iter()
        .fold((f64::INFINITY, f64::NEG_INFINITY), |(min, max), point| {
            (min.min(point.x), max.max(point.x))
        });
    f64::midpoint(min, max)
}

fn chart_shift(anchor: f64, center: f64, period: f64) -> f64 {
    period * ((anchor - center) / period).round()
}

fn chart_ring(points: &[XY], shift: f64) -> Option<Ring> {
    if points.len() < 3 {
        return None;
    }
    let mut coords: Vec<_> = points
        .iter()
        .map(|point| XY::new(point.x + shift, point.y).point())
        .collect();
    coords.push(coords[0]);
    Some(Ring::from_closed(CoordSeq::from(coords)))
}

fn area_polygons(graph: &TopologyGraph) -> Vec<&AreaSupport> {
    graph
        .area_support
        .iter()
        .filter(|support| polygon_has_nondegenerate_area(support.source()))
        .collect()
}

#[derive(Clone, Copy)]
struct EdgeLabel {
    left: Option<TopologyLocation>,
    right: Option<TopologyLocation>,
}

fn pair_edges(
    left: &TopologyGraph,
    right: &TopologyGraph,
) -> (Vec<(Segment, EdgeLabel)>, HashMap<PointKey, EdgeLabel>) {
    if left.edges.is_empty() || right.edges.is_empty() {
        let (graph, left_present) = if left.edges.is_empty() {
            (right, false)
        } else {
            (left, true)
        };
        let mut edges = Vec::with_capacity(graph.edges.len());
        let mut hints = HashMap::with_capacity(graph.edges.len() * 2);
        for edge in graph.edges.iter() {
            let location = edge_location(edge);
            let label = if left_present {
                EdgeLabel {
                    left: Some(location),
                    right: None,
                }
            } else {
                EdgeLabel {
                    left: None,
                    right: Some(location),
                }
            };
            for point in [edge.segment.start, edge.segment.end] {
                let hint = hints.entry(PointKey::new(point)).or_insert(EdgeLabel {
                    left: None,
                    right: None,
                });
                let slot = if left_present {
                    &mut hint.left
                } else {
                    &mut hint.right
                };
                merge_location(slot, location);
            }
            edges.push((edge.segment, label));
        }
        return (edges, hints);
    }
    let split = left.edges.len();
    let (atomic, provenance) = overlay::self_node_segments_sourced(
        left.edges
            .iter()
            .map(|edge| edge.segment)
            .chain(right.edges.iter().map(|edge| edge.segment)),
    );
    if atomic.len() == left.edges.len() + right.edges.len()
        && provenance.iter().all(|owners| owners.len() == 1)
    {
        let edges = atomic
            .into_iter()
            .zip(provenance.iter())
            .map(|(segment, owners)| {
                let owner = owners[0].0 as usize;
                let label = if owner < split {
                    EdgeLabel {
                        left: Some(edge_location(&left.edges[owner])),
                        right: None,
                    }
                } else {
                    EdgeLabel {
                        left: None,
                        right: Some(edge_location(&right.edges[owner - split])),
                    }
                };
                (segment, label)
            })
            .collect();
        return (edges, HashMap::new());
    }
    let mut by_segment = HashMap::with_capacity(atomic.len());
    let mut hints = HashMap::with_capacity(atomic.len());
    for (segment, owners) in atomic.into_iter().zip(provenance.iter()) {
        if same_point(segment.start, segment.end) {
            continue;
        }
        let mut label = EdgeLabel {
            left: None,
            right: None,
        };
        for &(owner, _) in owners {
            let owner = owner as usize;
            let (slot, location) = if owner < split {
                (&mut label.left, edge_location(&left.edges[owner]))
            } else {
                (&mut label.right, edge_location(&right.edges[owner - split]))
            };
            merge_location(slot, location);
            for point in [segment.start, segment.end] {
                let hint = hints.entry(PointKey::new(point)).or_insert(EdgeLabel {
                    left: None,
                    right: None,
                });
                let slot = if owner < split {
                    &mut hint.left
                } else {
                    &mut hint.right
                };
                merge_location(slot, location);
            }
        }
        by_segment.insert(
            crate::geometry::undirected_segment_edge_key(segment),
            (segment, label),
        );
    }
    (by_segment.into_values().collect(), hints)
}

fn edge_location(edge: &crate::geometry::topology::TopologyEdge) -> TopologyLocation {
    if edge.is_boundary {
        TopologyLocation::Boundary
    } else {
        TopologyLocation::Interior
    }
}

fn merge_location(slot: &mut Option<TopologyLocation>, location: TopologyLocation) {
    *slot = Some(match (*slot, location) {
        (Some(TopologyLocation::Interior), _) | (_, TopologyLocation::Interior) => {
            TopologyLocation::Interior
        },
        (Some(TopologyLocation::Boundary), _) | (_, TopologyLocation::Boundary) => {
            TopologyLocation::Boundary
        },
        _ => TopologyLocation::Exterior,
    });
}

fn add_edge_cells(
    matrix: &mut De9im,
    left: &TopologyGraph,
    right: &TopologyGraph,
    edges: &[(Segment, EdgeLabel)],
) {
    for &(segment, label) in edges {
        let left_loc = label
            .left
            .unwrap_or_else(|| left.locate_edge_piece(segment));
        let right_loc = label
            .right
            .unwrap_or_else(|| right.locate_edge_piece(segment));
        if left_loc != TopologyLocation::Exterior || right_loc != TopologyLocation::Exterior {
            matrix.set_at_least(
                to_loc(left_loc),
                to_loc(right_loc),
                crate::geometry::Dimension::Curve,
            );
        }
    }
}

#[derive(Clone, Copy, Eq, Hash, PartialEq)]
enum NodeToken {
    Left(TopologyNodeId),
    Right(TopologyNodeId),
}

fn push_node(
    nodes: &mut Vec<XY>,
    seen: &mut HashSet<NodeToken>,
    owners: &mut HashMap<TopologyKey, NodeToken>,
    transient: &mut HashSet<TopologyKey>,
    point: XY,
    left: &TopologyGraph,
    right: &TopologyGraph,
) {
    let token = left
        .node_for(point)
        .map(NodeToken::Left)
        .or_else(|| right.node_for(point).map(NodeToken::Right));
    let Some(token) = token else {
        // Both graphs share a quotient, so one key decides transient equality.
        if !transient.insert(left.key(point)) {
            return;
        }
        nodes.push(point);
        return;
    };
    if seen.insert(token) {
        owners.insert(left.key(point), token);
        nodes.push(point);
    }
}

fn add_node_cells(
    matrix: &mut De9im,
    left: &TopologyGraph,
    right: &TopologyGraph,
    nodes: &[XY],
    owners: &HashMap<TopologyKey, NodeToken>,
    hints: &HashMap<PointKey, EdgeLabel>,
) {
    for &point in nodes {
        let hint = hints.get(&PointKey::new(point)).copied();
        let owner = owners.get(&left.key(point)).copied();
        let left_loc = locate_node_with_hint(
            left,
            point,
            matches!(owner, Some(NodeToken::Left(_))),
            hint.and_then(|hint| hint.left),
        );
        let right_loc = locate_node_with_hint(
            right,
            point,
            matches!(owner, Some(NodeToken::Right(_))),
            hint.and_then(|hint| hint.right),
        );
        if left_loc != TopologyLocation::Exterior || right_loc != TopologyLocation::Exterior {
            matrix.set_at_least(
                to_loc(left_loc),
                to_loc(right_loc),
                crate::geometry::Dimension::Point,
            );
        }
    }
}

fn suppress_lineal_overlap_artifacts(
    matrix: &mut De9im,
    left: &TopologyGraph,
    right: &TopologyGraph,
) {
    if left.lineal_overlap_collection
        && !right.is_collection
        && (matrix.0[3] != b'0' || !right.has_area_interior())
    {
        matrix.clear(Loc::Boundary, Loc::Exterior);
    }
    if right.lineal_overlap_collection
        && !left.is_collection
        && (matrix.0[1] != b'0' || !left.has_area_interior())
    {
        matrix.clear(Loc::Exterior, Loc::Boundary);
    }
    if left.lineal_overlap_collection && right.is_collection && !left.has_point_support_any() {
        matrix.clear(Loc::Interior, Loc::Boundary);
    }
    if right.lineal_overlap_collection && left.is_collection && !right.has_point_support_any() {
        matrix.clear(Loc::Boundary, Loc::Interior);
    }
}

fn locate_node_with_hint(
    graph: &TopologyGraph,
    point: XY,
    owns_node: bool,
    hint: Option<TopologyLocation>,
) -> TopologyLocation {
    let location = if owns_node {
        graph
            .node_location(point)
            .expect("node provenance must resolve in its owning graph")
    } else {
        graph.locate_point(point)
    };
    match location {
        TopologyLocation::Exterior => hint.unwrap_or(TopologyLocation::Exterior),
        location => location,
    }
}

const fn to_loc(location: TopologyLocation) -> Loc {
    match location {
        TopologyLocation::Interior => Loc::Interior,
        TopologyLocation::Boundary => Loc::Boundary,
        TopologyLocation::Exterior => Loc::Exterior,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::{CoordSeq, LineSeq, Point, Polygon, PreparedPlan, Ring};

    fn rectangle(minx: f64, miny: f64, maxx: f64, maxy: f64) -> Polygon {
        Polygon::new(
            Ring::from_closed(vec![
                Point::new_raw_xy(minx, miny),
                Point::new_raw_xy(maxx, miny),
                Point::new_raw_xy(maxx, maxy),
                Point::new_raw_xy(minx, maxy),
                Point::new_raw_xy(minx, miny),
            ]),
            Vec::new(),
        )
    }

    fn donut() -> Polygon {
        Polygon::new(rectangle(0.0, 0.0, 10.0, 10.0).shell, vec![
            Ring::from_closed(vec![
                Point::new_raw_xy(3.0, 3.0),
                Point::new_raw_xy(7.0, 3.0),
                Point::new_raw_xy(7.0, 7.0),
                Point::new_raw_xy(3.0, 7.0),
                Point::new_raw_xy(3.0, 3.0),
            ]),
        ])
    }

    fn polygon(points: &[[f64; 2]]) -> Polygon {
        Polygon::new(
            Ring::from_closed(
                points
                    .iter()
                    .map(|&[x, y]| Point::new_raw_xy(x, y))
                    .collect::<Vec<_>>(),
            ),
            Vec::new(),
        )
    }

    #[test]
    fn geographic_near_vertical_box_keeps_planar_boundary_dimension() {
        let hexagon = Shape::Polygon(polygon(&[
            [1.0, 0.0],
            [0.5000000000000001, 0.8660254037844386],
            [-0.4999999999999998, 0.8660254037844387],
            [-1.0, 0.00000000000000012246467991473532],
            [-0.5000000000000004, -0.8660254037844384],
            [0.5000000000000001, -0.8660254037844386],
            [1.0, 0.0],
        ]));
        for (maxx, expected) in [
            (0.49, b"212101212"),
            (0.5, b"212101212"),
            (0.5000000000000001, b"212101212"),
            (0.51, b"212101212"),
        ] {
            let rectangle = crate::geometry::geographic_bounds_to_shape(
                Bounds::new(-10.0, -10.0, maxx, 10.0).expect("ordered bounds"),
                crate::crs::info("EPSG:4326")
                    .expect("known geographic CRS")
                    .geographic_frame()
                    .expect("geographic CRS"),
            )
            .expect("geographic rectangle");
            let geographic = graph_relate_shapes_geographic(&hexagon, &rectangle);
            assert_eq!(geographic.0, *expected, "maxx={maxx}");
            assert_eq!(
                graph_relate_shapes_geographic(&rectangle, &hexagon).0,
                geographic.transpose().0,
                "maxx={maxx}: transpose"
            );
            assert_eq!(
                graph_relate_shapes(&hexagon, &rectangle).0,
                *expected,
                "maxx={maxx}: planar"
            );
        }
    }

    fn assert_areal_fast_path(name: &str, left: Shape, right: Shape) {
        let expected = graph_relate_data_oracle(
            &PreparedPlan::transient(&left),
            &PreparedPlan::transient(&right),
        );
        let fast = try_areal_relate(&left, &right).expect("areal fast-path admission");
        assert_eq!(fast.0, expected.0, "{name}: forward");
        let reverse = try_areal_relate(&right, &left).expect("reverse areal fast-path admission");
        assert_eq!(reverse.0, expected.transpose().0, "{name}: reverse");
        assert_eq!(
            graph_relate_data(
                &PreparedPlan::transient(&left),
                &PreparedPlan::transient(&right)
            )
            .0,
            expected.0,
            "{name}: dispatch"
        );
    }

    fn assert_areal_fallback(name: &str, left: Shape, right: Shape) {
        assert!(
            try_areal_relate(&left, &right).is_none(),
            "{name}: admission"
        );
        assert_eq!(
            graph_relate_data(
                &PreparedPlan::transient(&left),
                &PreparedPlan::transient(&right)
            )
            .0,
            graph_relate_data_oracle(
                &PreparedPlan::transient(&left),
                &PreparedPlan::transient(&right),
            )
            .0,
            "{name}: fallback"
        );
    }

    fn assert_convex_relation_case(
        name: &str,
        left: &Shape,
        right: &Shape,
        eligible: bool,
        expected: &str,
    ) {
        let left_plan = PreparedPlan::transient(left);
        let right_plan = PreparedPlan::transient(right);
        let oracle = graph_relate_data_oracle(&left_plan, &right_plan);
        assert_eq!(oracle.text(), expected, "{name}: oracle");
        assert_eq!(
            try_convex_relate(left, right).is_some(),
            eligible,
            "{name}: convex admission"
        );
        assert_eq!(
            graph_relate_data(&left_plan, &right_plan).text(),
            expected,
            "{name}: dispatch"
        );

        let reverse_left = PreparedPlan::transient(right);
        let reverse_right = PreparedPlan::transient(left);
        let reverse_oracle = graph_relate_data_oracle(&reverse_left, &reverse_right);
        assert_eq!(
            graph_relate_data(&reverse_left, &reverse_right).0,
            reverse_oracle.0,
            "{name}: reverse"
        );
        if eligible {
            assert_eq!(reverse_oracle.0, oracle.transpose().0, "{name}: transpose");
        }
    }

    #[test]
    fn convex_relation_dispatch_matches_the_arrangement_oracle() {
        let ccw_outer = Shape::Polygon(rectangle(0.0, 0.0, 4.0, 4.0));
        let cw_outer = Shape::Polygon(polygon(&[
            [0.0, 0.0],
            [0.0, 4.0],
            [4.0, 4.0],
            [4.0, 0.0],
            [0.0, 0.0],
        ]));
        let ccw_inner = Shape::Polygon(rectangle(1.0, 1.0, 2.0, 2.0));
        let cw_inner = Shape::Polygon(polygon(&[
            [1.0, 1.0],
            [1.0, 2.0],
            [2.0, 2.0],
            [2.0, 1.0],
            [1.0, 1.0],
        ]));
        let crossing = Shape::Polygon(rectangle(-1.0, 1.0, 5.0, 3.0));
        let disjoint = Shape::Polygon(rectangle(8.0, 0.0, 9.0, 1.0));
        let huge_crossing = Shape::Polygon(rectangle(1.5e150, 1.5e150, 2.5e150, 2.5e150));
        let huge_base = Shape::Polygon(rectangle(1.0e150, 1.0e150, 2.0e150, 2.0e150));
        let tiny_crossing = Shape::Polygon(rectangle(1.5e-150, 1.5e-150, 2.5e-150, 2.5e-150));
        let tiny_base = Shape::Polygon(rectangle(1.0e-150, 1.0e-150, 2.0e-150, 2.0e-150));
        let cancellation = Shape::Polygon(polygon(&[
            [0.0, 0.0],
            [1.0, 1.0 + 2.0_f64.powi(-27)],
            [1.0 - 2.0_f64.powi(-27), 1.0],
            [0.0, 0.0],
        ]));

        for (name, left, right, expected) in [
            ("ccw containment", &ccw_outer, &ccw_inner, "212FF1FF2"),
            ("cw containment", &cw_outer, &cw_inner, "212FF1FF2"),
            ("reverse containment", &cw_inner, &ccw_outer, "2FF1FF212"),
            ("proper crossing", &ccw_outer, &crossing, "212101212"),
            ("clockwise crossing", &cw_outer, &crossing, "212101212"),
            ("disjoint", &ccw_outer, &disjoint, "FF2FF1212"),
            ("huge crossing", &huge_base, &huge_crossing, "212101212"),
            ("tiny crossing", &tiny_base, &tiny_crossing, "212101212"),
            ("exact cancellation", &cancellation, &disjoint, "FF2FF1212"),
        ] {
            assert_convex_relation_case(name, left, right, true, expected);
        }
    }

    #[test]
    fn convex_relation_falls_back_for_ambiguous_or_unsupported_shapes() {
        let square = Shape::Polygon(rectangle(0.0, 0.0, 2.0, 2.0));
        let far = Shape::Polygon(rectangle(8.0, 0.0, 9.0, 1.0));
        let equal = Shape::Polygon(rectangle(0.0, 0.0, 2.0, 2.0));
        let edge_touch = Shape::Polygon(rectangle(2.0, 0.0, 4.0, 2.0));
        let vertex_touch = Shape::Polygon(rectangle(2.0, 2.0, 4.0, 4.0));
        let collinear_overlap = Shape::Polygon(rectangle(1.0, 0.0, 3.0, 2.0));
        let hole = Shape::Polygon(donut());
        let concave = Shape::Polygon(polygon(&[
            [0.0, 0.0],
            [4.0, 0.0],
            [4.0, 1.0],
            [1.0, 1.0],
            [1.0, 4.0],
            [0.0, 4.0],
            [0.0, 0.0],
        ]));
        let invalid = Shape::Polygon(polygon(&[
            [0.0, 0.0],
            [3.0, 3.0],
            [0.0, 3.0],
            [3.0, 0.0],
            [0.0, 0.0],
        ]));
        let collapsed = Shape::Polygon(polygon(&[[0.0, 0.0], [1.0, 0.0], [2.0, 0.0], [0.0, 0.0]]));
        let nonstrict = Shape::Polygon(polygon(&[
            [0.0, 0.0],
            [1.0, 0.0],
            [2.0, 0.0],
            [2.0, 2.0],
            [0.0, 2.0],
            [0.0, 0.0],
        ]));

        for (name, left, right, expected) in [
            ("equal", &square, &equal, "2FFF1FFF2"),
            ("edge touch", &square, &edge_touch, "FF2F11212"),
            ("vertex touch", &square, &vertex_touch, "FF2F01212"),
            (
                "collinear overlap",
                &square,
                &collinear_overlap,
                "212111212",
            ),
            ("hole", &hole, &square, "212F11FF2"),
            ("concave", &concave, &square, "212111212"),
            ("invalid", &invalid, &square, "FF2111212"),
            ("collapsed", &collapsed, &square, "FF2F0F212"),
            ("nonstrict", &nonstrict, &far, "FF2FF1212"),
        ] {
            assert_convex_relation_case(name, left, right, false, expected);
        }
    }

    #[test]
    fn convex_relation_rejects_a_two_unique_point_ring() {
        let a = Point::new_raw_xy(10.0, 10.0);
        let b = Point::new_raw_xy(11.0, 10.0);
        // `Ring::from_closed` is the internal transport constructor. The final
        // `a` is the stored closing duplicate for the raw A-B-A ring.
        let two_unique = Shape::Polygon(Polygon::new(
            Ring::from_closed(vec![a, b, a, a]),
            Vec::new(),
        ));
        let square = Shape::Polygon(rectangle(0.0, 0.0, 2.0, 2.0));
        let left = PreparedPlan::transient(&two_unique);
        let right = PreparedPlan::transient(&square);
        let oracle = graph_relate_data_oracle(&left, &right);
        assert!(try_convex_relate(&two_unique, &square).is_none());
        assert_eq!(graph_relate_data(&left, &right).0, oracle.0);

        let reverse_left = PreparedPlan::transient(&square);
        let reverse_right = PreparedPlan::transient(&two_unique);
        let reverse_oracle = graph_relate_data_oracle(&reverse_left, &reverse_right);
        assert!(try_convex_relate(&square, &two_unique).is_none());
        assert_eq!(
            graph_relate_data(&reverse_left, &reverse_right).0,
            reverse_oracle.0
        );
    }

    #[test]
    fn strict_convexity_rejects_short_rings_but_permissive_convexity_remains() {
        let rings = [
            CoordSeq::from(Vec::<Point>::new()),
            CoordSeq::from(vec![Point::new_raw_xy(0.0, 0.0)]),
            CoordSeq::from(vec![
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(1.0, 0.0),
                Point::new_raw_xy(0.0, 0.0),
            ]),
        ];
        for ring in rings {
            assert!(crate::geometry::predicates::shell_is_convex(&ring));
            assert!(!crate::geometry::predicates::shell_is_strictly_convex(
                &ring
            ));
        }
    }

    #[test]
    fn direct_areal_relation_matches_graph_oracle() {
        let shell_with_hole = Shape::Polygon(donut());
        let cases = vec![
            (
                "disjoint",
                Shape::Polygon(rectangle(0.0, 0.0, 1.0, 1.0)),
                Shape::Polygon(rectangle(2.0, 0.0, 3.0, 1.0)),
            ),
            (
                "nested",
                Shape::Polygon(rectangle(0.0, 0.0, 4.0, 4.0)),
                Shape::Polygon(rectangle(1.0, 1.0, 2.0, 2.0)),
            ),
            (
                "equal",
                Shape::Polygon(rectangle(0.0, 0.0, 2.0, 2.0)),
                Shape::Polygon(rectangle(0.0, 0.0, 2.0, 2.0)),
            ),
            (
                "overlap",
                Shape::Polygon(rectangle(0.0, 0.0, 2.0, 2.0)),
                Shape::Polygon(rectangle(1.0, 1.0, 3.0, 3.0)),
            ),
            (
                "shared edge",
                Shape::Polygon(rectangle(0.0, 0.0, 2.0, 2.0)),
                Shape::Polygon(rectangle(2.0, 0.0, 4.0, 2.0)),
            ),
            (
                "shared corner",
                Shape::Polygon(rectangle(0.0, 0.0, 2.0, 2.0)),
                Shape::Polygon(rectangle(2.0, 2.0, 4.0, 4.0)),
            ),
            (
                "transversal",
                Shape::Polygon(rectangle(-2.0, -1.0, 2.0, 1.0)),
                Shape::Polygon(polygon(&[
                    [-1.0, -2.0],
                    [1.0, -2.0],
                    [1.0, 2.0],
                    [-1.0, 2.0],
                    [-1.0, -2.0],
                ])),
            ),
            (
                "inside hole",
                shell_with_hole.clone(),
                Shape::Polygon(rectangle(4.0, 4.0, 6.0, 6.0)),
            ),
            (
                "crosses hole",
                shell_with_hole,
                Shape::Polygon(rectangle(2.0, 2.0, 5.0, 5.0)),
            ),
            (
                "collinear overlap",
                Shape::Polygon(rectangle(0.0, 0.0, 2.0, 2.0)),
                Shape::Polygon(rectangle(1.0, 0.0, 3.0, 2.0)),
            ),
            (
                "t junction",
                Shape::Polygon(rectangle(0.0, 0.0, 2.0, 2.0)),
                Shape::Polygon(polygon(&[
                    [2.0, -1.0],
                    [4.0, -1.0],
                    [4.0, 1.0],
                    [2.0, 1.0],
                    [2.0, 0.0],
                    [2.0, -1.0],
                ])),
            ),
            (
                "repeated vertex",
                Shape::Polygon(polygon(&[
                    [0.0, 0.0],
                    [3.0, 0.0],
                    [3.0, 3.0],
                    [0.0, 3.0],
                    [3.0, 0.0],
                    [0.0, 0.0],
                ])),
                Shape::Polygon(rectangle(1.0, -1.0, 2.0, 4.0)),
            ),
            (
                "general position",
                Shape::Polygon(polygon(&[
                    [0.1, 0.2],
                    [3.7, 0.4],
                    [2.9, 3.6],
                    [-0.3, 2.1],
                    [0.1, 0.2],
                ])),
                Shape::Polygon(polygon(&[[1.2, -0.4], [4.1, 1.3], [1.8, 4.2], [1.2, -0.4]])),
            ),
            (
                "near collinear",
                Shape::Polygon(polygon(&[
                    [0.0, 0.0],
                    [4.0, 1e-12],
                    [4.0, 2.0],
                    [0.0, 2.0],
                    [0.0, 0.0],
                ])),
                Shape::Polygon(rectangle(1.0, -1.0, 3.0, 1.0)),
            ),
            (
                "exact sign near cancellation",
                Shape::Polygon(polygon(&[
                    [0.0, 0.0],
                    [1.0, 1.0 + 2.0_f64.powi(-27)],
                    [1.0 - 2.0_f64.powi(-27), 1.0],
                    [0.0, 0.0],
                ])),
                Shape::Polygon(rectangle(3.0, 0.0, 4.0, 1.0)),
            ),
            (
                "huge finite",
                Shape::Polygon(rectangle(1e150, 1e150, 2e150, 2e150)),
                Shape::Polygon(rectangle(1.5e150, 1.5e150, 2.5e150, 2.5e150)),
            ),
            (
                "tiny finite",
                Shape::Polygon(rectangle(1e-150, 1e-150, 2e-150, 2e-150)),
                Shape::Polygon(rectangle(1.5e-150, 1.5e-150, 2.5e-150, 2.5e-150)),
            ),
        ];
        for (name, left, right) in cases {
            assert_areal_fast_path(name, left, right);
        }

        let adjacent = Shape::MultiPolygon(
            vec![rectangle(0.0, 0.0, 2.0, 2.0), rectangle(2.0, 0.0, 4.0, 2.0)].into(),
        );
        let overlapping = Shape::MultiPolygon(
            vec![rectangle(0.0, 0.0, 3.0, 3.0), rectangle(1.0, 1.0, 4.0, 4.0)].into(),
        );
        let nested = Shape::MultiPolygon(
            vec![rectangle(0.0, 0.0, 5.0, 5.0), rectangle(1.0, 1.0, 2.0, 2.0)].into(),
        );
        let duplicate = Shape::MultiPolygon(
            vec![rectangle(0.0, 0.0, 3.0, 3.0), rectangle(0.0, 0.0, 3.0, 3.0)].into(),
        );
        let pinched = Shape::geometry_collection(vec![
            Shape::Polygon(polygon(&[[0.0, 0.0], [-2.0, -1.0], [-2.0, 1.0], [
                0.0, 0.0,
            ]])),
            Shape::Polygon(polygon(&[[0.0, 0.0], [2.0, 1.0], [2.0, -1.0], [0.0, 0.0]])),
        ])
        .expect("test collection is valid");
        let polygon_only_collection = Shape::geometry_collection(vec![
            Shape::Polygon(rectangle(10.0, 0.0, 12.0, 2.0)),
            Shape::Polygon(rectangle(12.0, 0.0, 14.0, 2.0)),
            Shape::Polygon(rectangle(20.0, 0.0, 23.0, 3.0)),
            Shape::Polygon(rectangle(21.0, 1.0, 24.0, 4.0)),
            Shape::Polygon(rectangle(30.0, 0.0, 35.0, 5.0)),
            Shape::Polygon(rectangle(31.0, 1.0, 32.0, 2.0)),
            Shape::Polygon(rectangle(40.0, 0.0, 43.0, 3.0)),
            Shape::Polygon(rectangle(40.0, 0.0, 43.0, 3.0)),
            Shape::Polygon(polygon(&[[50.0, 0.0], [48.0, -1.0], [48.0, 1.0], [
                50.0, 0.0,
            ]])),
            Shape::Polygon(polygon(&[[50.0, 0.0], [52.0, 1.0], [52.0, -1.0], [
                50.0, 0.0,
            ]])),
        ])
        .expect("test collection is valid");
        for (name, left, right) in [
            (
                "adjacent multipolygon",
                adjacent,
                Shape::Polygon(rectangle(1.0, 0.5, 3.0, 1.5)),
            ),
            (
                "overlapping multipolygon",
                overlapping,
                Shape::Polygon(rectangle(2.0, 0.0, 5.0, 3.0)),
            ),
            (
                "nested multipolygon",
                nested.clone(),
                Shape::Polygon(rectangle(1.25, 1.25, 1.75, 1.75)),
            ),
            (
                "covered member boundary",
                nested,
                Shape::Polygon(rectangle(1.25, 1.25, 1.75, 1.75)),
            ),
            (
                "duplicate multipolygon",
                duplicate,
                Shape::Polygon(rectangle(1.0, 1.0, 2.0, 2.0)),
            ),
            (
                "point pinched collection",
                pinched,
                Shape::Polygon(rectangle(-0.5, -0.5, 0.5, 0.5)),
            ),
            (
                "polygon-only collection",
                polygon_only_collection,
                Shape::Polygon(rectangle(11.0, -2.0, 51.0, 2.0)),
            ),
        ] {
            assert_areal_fast_path(name, left, right);
        }
    }

    #[test]
    fn direct_areal_relation_declines_unsupported_operands() {
        let areal = Shape::Polygon(rectangle(0.0, 0.0, 2.0, 2.0));
        let line = Shape::LineString(
            LineSeq::try_new(CoordSeq::from(vec![
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(2.0, 2.0),
            ]))
            .expect("line has two vertices"),
        );
        assert_areal_fallback("line member", areal.clone(), line);
        assert_areal_fallback(
            "zero length edge",
            Shape::Polygon(polygon(&[
                [0.0, 0.0],
                [2.0, 0.0],
                [2.0, 0.0],
                [2.0, 2.0],
                [0.0, 2.0],
                [0.0, 0.0],
            ])),
            areal.clone(),
        );
        assert_areal_fallback(
            "collapsed ring",
            Shape::Polygon(polygon(&[[0.0, 0.0], [1.0, 0.0], [2.0, 0.0], [0.0, 0.0]])),
            areal.clone(),
        );
        assert!(try_areal_relate_in(&areal, &areal, TopologySpace::Geographic).is_none());
    }

    #[test]
    fn prepared_matrix_matches_shape_matrix_for_curated_pairs() {
        let pairs = [
            (
                Shape::Polygon(rectangle(0.0, 0.0, 4.0, 4.0)),
                Shape::Polygon(rectangle(2.0, 2.0, 6.0, 6.0)),
            ),
            (
                Shape::Polygon(donut()),
                Shape::Point(Point::new_raw_xy(5.0, 5.0)),
            ),
            (
                Shape::LineString(
                    LineSeq::try_new(CoordSeq::from(vec![
                        Point::new_raw_xy(0.0, 0.0),
                        Point::new_raw_xy(2.0, 2.0),
                    ]))
                    .expect("line coordinate sequence must contain at least two vertices"),
                ),
                Shape::LineString(
                    LineSeq::try_new(CoordSeq::from(vec![
                        Point::new_raw_xy(0.0, 2.0),
                        Point::new_raw_xy(2.0, 0.0),
                    ]))
                    .expect("line coordinate sequence must contain at least two vertices"),
                ),
            ),
        ];
        for (left, right) in pairs {
            let expected = graph_relate_shapes(&left, &right);
            let left = PreparedPlan::new(left);
            let right = PreparedPlan::new(right);
            assert_eq!(graph_relate_data(&left, &right).text(), expected.text());
        }
    }

    #[test]
    fn empty_graph_keeps_the_nonempty_operand_dimensions() {
        let empty = Shape::Empty(
            crate::geometry::GeometryKind::Polygon,
            crate::geometry::CoordinateAxes::XY,
        );
        let point = Shape::Point(Point::new_raw_xy(0.0, 0.0));
        let line = Shape::LineString(
            LineSeq::try_new(CoordSeq::from(vec![
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(1.0, 0.0),
            ]))
            .expect("line coordinate sequence must contain at least two vertices"),
        );
        let polygon = Shape::Polygon(rectangle(0.0, 0.0, 1.0, 1.0));
        for (other, expected) in [
            (&point, "FFFFFF0F2"),
            (&line, "FFFFFF102"),
            (&polygon, "FFFFFF212"),
        ] {
            assert_eq!(graph_relate_shapes(&empty, other).text(), expected);
            assert_eq!(
                graph_relate_shapes(other, &empty).text(),
                graph_relate_shapes(&empty, other).transpose().text()
            );
        }
        assert_eq!(graph_relate_shapes(&empty, &empty).text(), "FFFFFFFF2");
    }

    fn geographic_graph_oracle(
        left: &Shape,
        right: &Shape,
        frame: crate::geographic::GeographicFrame,
    ) -> De9im {
        let left = PreparedPlan::transient(left);
        let right = PreparedPlan::transient(right);
        let left_cache = crate::geometry::FrameDependentCaches::default();
        let right_cache = crate::geometry::FrameDependentCaches::default();
        let left = GeographicPrepared::new(&left, &left_cache, frame).graph();
        let right = GeographicPrepared::new(&right, &right_cache, frame).graph();
        graph_relate(&left, &right)
    }

    fn assert_rectangle_cover_certificate(
        name: &str,
        source: &Shape,
        bounds: Bounds,
        frame: Option<crate::geographic::GeographicFrame>,
    ) {
        let source_graph = frame.map_or_else(
            || TopologyGraph::cartesian(source),
            |frame| TopologyGraph::geographic(source, frame),
        );
        let candidate = Shape::Polygon(rectangle(
            bounds.minx(),
            bounds.miny(),
            bounds.maxx(),
            bounds.maxy(),
        ));
        let candidate_graph = frame.map_or_else(
            || TopologyGraph::cartesian(&candidate),
            |frame| TopologyGraph::geographic(&candidate, frame),
        );
        let expected = graph_relate(&source_graph, &candidate_graph).is_covers();
        assert_eq!(
            source_graph.covers_rectangle(bounds),
            Some(expected),
            "{name}"
        );
    }

    #[test]
    fn rectangle_cover_certificate_matches_graph_relation() {
        let outer = Shape::Polygon(rectangle(0.0, 0.0, 10.0, 10.0));
        let donut = Shape::Polygon(donut());
        let line = Shape::LineString(
            LineSeq::try_new(CoordSeq::from(vec![
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(10.0, 10.0),
            ]))
            .unwrap(),
        );
        for (name, source, bounds) in [
            (
                "cartesian interior",
                &outer,
                Bounds::new_raw(2.0, 2.0, 8.0, 8.0),
            ),
            (
                "cartesian boundary aligned",
                &outer,
                Bounds::new_raw(0.0, 2.0, 8.0, 8.0),
            ),
            (
                "cartesian crossing",
                &outer,
                Bounds::new_raw(8.0, 2.0, 12.0, 8.0),
            ),
            (
                "cartesian disjoint",
                &outer,
                Bounds::new_raw(12.0, 12.0, 14.0, 14.0),
            ),
            (
                "cartesian hole",
                &donut,
                Bounds::new_raw(4.0, 4.0, 6.0, 6.0),
            ),
            (
                "cartesian hole crossing",
                &donut,
                Bounds::new_raw(2.0, 2.0, 5.0, 5.0),
            ),
            ("cartesian line", &line, Bounds::new_raw(2.0, 2.0, 8.0, 8.0)),
        ] {
            assert_rectangle_cover_certificate(name, source, bounds, None);
        }
    }

    #[test]
    fn geographic_rectangle_cover_certificate_matches_graph_relation() {
        let frame = crate::crs::info("EPSG:4326")
            .unwrap()
            .geographic_frame()
            .unwrap();
        let outer = Shape::Polygon(rectangle(0.0, 0.0, 10.0, 10.0));
        let donut = Shape::Polygon(donut());
        let seam = Shape::Polygon(polygon(&[
            [170.0, -10.0],
            [-170.0, -10.0],
            [-170.0, 10.0],
            [170.0, 10.0],
            [170.0, -10.0],
        ]));
        let polar = Shape::Polygon(polygon(&[
            [-10.0, 80.0],
            [10.0, 80.0],
            [10.0, 90.0],
            [-10.0, 90.0],
            [-10.0, 80.0],
        ]));
        for (name, source, bounds) in [
            (
                "geographic interior",
                &outer,
                Bounds::new_raw(2.0, 2.0, 8.0, 8.0),
            ),
            (
                "geographic boundary aligned",
                &outer,
                Bounds::new_raw(0.0, 2.0, 8.0, 8.0),
            ),
            (
                "geographic hole",
                &donut,
                Bounds::new_raw(4.0, 4.0, 6.0, 6.0),
            ),
            (
                "geographic hole crossing",
                &donut,
                Bounds::new_raw(2.0, 2.0, 5.0, 5.0),
            ),
            (
                "geographic seam interior",
                &seam,
                Bounds::new_raw(175.0, -5.0, 179.0, 5.0),
            ),
            (
                "geographic seam exterior",
                &seam,
                Bounds::new_raw(-5.0, -5.0, 5.0, 5.0),
            ),
        ] {
            assert_rectangle_cover_certificate(name, source, bounds, Some(frame));
        }
        assert_rectangle_cover_certificate(
            "geographic polar interior",
            &polar,
            Bounds::new_raw(0.0, 81.0, 1.0, 82.0),
            Some(frame),
        );
    }

    #[test]
    fn rectangle_cover_certificate_rejects_an_exact_hole() {
        let bounds = Bounds::new_raw(3.0, 3.0, 7.0, 7.0);
        let source = Shape::Polygon(donut());
        let cartesian = TopologyGraph::cartesian(&source);
        assert_eq!(cartesian.covers_rectangle(bounds), Some(false));

        let line = Shape::LineString(
            LineSeq::try_new(CoordSeq::from(vec![
                Point::new_raw_xy(4.0, 5.0),
                Point::new_raw_xy(6.0, 5.0),
            ]))
            .unwrap(),
        );
        let mixed = Shape::geometry_collection(vec![source.clone(), line]).unwrap();
        let mixed = TopologyGraph::cartesian(&mixed);
        assert_eq!(mixed.covers_rectangle(bounds), Some(false));

        let frame = crate::crs::info("EPSG:4326")
            .unwrap()
            .geographic_frame()
            .unwrap();
        let geographic = TopologyGraph::geographic(&source, frame);
        assert_eq!(geographic.covers_rectangle(bounds), Some(false));
    }

    #[test]
    fn rectangle_cover_certificate_fails_open_for_an_adjacent_float_midpoint() {
        let minx: f64 = 1.0;
        let maxx = minx.next_up();
        let bounds = Bounds::new_raw(minx, 0.0, maxx, 1.0);
        assert_eq!(f64::midpoint(minx, maxx), minx);

        let source = Shape::Polygon(rectangle(minx, 0.0, maxx, 1.0));
        let cartesian = TopologyGraph::cartesian(&source);
        assert_eq!(cartesian.covers_rectangle(bounds), None);

        let frame = crate::crs::info("EPSG:4326")
            .unwrap()
            .geographic_frame()
            .unwrap();
        let geographic = TopologyGraph::geographic(&source, frame);
        assert_eq!(geographic.covers_rectangle(bounds), None);
    }

    #[test]
    fn rectangle_cover_certificate_fails_open_at_geographic_singularities() {
        let frame = crate::crs::info("EPSG:4326")
            .unwrap()
            .geographic_frame()
            .unwrap();
        let source = Shape::Polygon(polygon(&[
            [-10.0, 80.0],
            [10.0, 80.0],
            [10.0, 90.0],
            [-10.0, 90.0],
            [-10.0, 80.0],
        ]));
        let graph = TopologyGraph::geographic(&source, frame);
        for bounds in [
            Bounds::new_raw(-180.0, 81.0, -179.0, 85.0),
            Bounds::new_raw(0.0, 85.0, 10.0, 90.0),
            Bounds::new_raw(-180.0, -1.0, 180.0, 1.0),
        ] {
            assert_eq!(graph.covers_rectangle(bounds), None);
        }
    }

    #[test]
    fn geographic_areal_relation_uses_the_safe_cartesian_fold() {
        let frame = crate::crs::info("EPSG:4326")
            .unwrap()
            .geographic_frame()
            .unwrap();
        let with_hole = Shape::Polygon(donut());
        let multipolygon = Shape::MultiPolygon(
            vec![
                rectangle(-10.0, -10.0, -6.0, -6.0),
                rectangle(-4.0, -10.0, 0.0, -6.0),
            ]
            .into(),
        );
        for (name, left, right) in [
            (
                "disjoint",
                Shape::Polygon(rectangle(-20.0, 0.0, -18.0, 2.0)),
                Shape::Polygon(rectangle(-16.0, 0.0, -14.0, 2.0)),
            ),
            (
                "nested",
                Shape::Polygon(rectangle(-20.0, 0.0, -10.0, 10.0)),
                Shape::Polygon(rectangle(-18.0, 2.0, -16.0, 4.0)),
            ),
            (
                "overlapping",
                Shape::Polygon(rectangle(-20.0, 0.0, -16.0, 4.0)),
                Shape::Polygon(rectangle(-18.0, 2.0, -14.0, 6.0)),
            ),
            (
                "shared edge",
                Shape::Polygon(rectangle(-20.0, 0.0, -18.0, 2.0)),
                Shape::Polygon(rectangle(-18.0, 0.0, -16.0, 2.0)),
            ),
            (
                "shared corner",
                Shape::Polygon(rectangle(-20.0, 0.0, -18.0, 2.0)),
                Shape::Polygon(rectangle(-18.0, 2.0, -16.0, 4.0)),
            ),
            (
                "hole",
                with_hole,
                Shape::Polygon(rectangle(2.0, 2.0, 5.0, 5.0)),
            ),
            (
                "multipolygon",
                multipolygon,
                Shape::Polygon(rectangle(-8.0, -8.0, -2.0, -4.0)),
            ),
        ] {
            let expected = geographic_graph_oracle(&left, &right, frame);
            assert_eq!(
                try_areal_relate_geographic(&left, &right, frame)
                    .expect("safe geographic areal admission")
                    .0,
                expected.0,
                "{name}: fold"
            );
            let left_plan = PreparedPlan::transient(&left);
            let right_plan = PreparedPlan::transient(&right);
            let left_cache = crate::geometry::FrameDependentCaches::default();
            let right_cache = crate::geometry::FrameDependentCaches::default();
            assert_eq!(
                graph_relate_data_geographic(
                    GeographicPrepared::new(&left_plan, &left_cache, frame),
                    GeographicPrepared::new(&right_plan, &right_cache, frame),
                )
                .0,
                expected.0,
                "{name}: prepared dispatch"
            );
            assert_eq!(
                graph_relate_shapes_geographic(&left, &right).0,
                expected.0,
                "{name}: shape dispatch"
            );
        }
    }

    #[test]
    fn geographic_areal_relation_declines_quotient_aliases() {
        let frame = crate::crs::info("EPSG:4326")
            .unwrap()
            .geographic_frame()
            .unwrap();
        let ordinary = Shape::Polygon(rectangle(10.0, 0.0, 12.0, 2.0));
        let seam = Shape::Polygon(polygon(&[
            [170.0, 0.0],
            [-170.0, 0.0],
            [-170.0, 10.0],
            [170.0, 10.0],
            [170.0, 0.0],
        ]));
        let pole = Shape::Polygon(rectangle(10.0, 80.0, 12.0, 90.0));
        let noncanonical = Shape::Polygon(rectangle(350.0, 0.0, 360.0, 2.0));
        for (name, left) in [
            ("seam", seam),
            ("pole", pole),
            ("longitude 360", noncanonical),
        ] {
            assert!(
                try_areal_relate_geographic(&left, &ordinary, frame).is_none(),
                "{name}: fold admission"
            );
            let expected = geographic_graph_oracle(&left, &ordinary, frame);
            let left_plan = PreparedPlan::transient(&left);
            let right_plan = PreparedPlan::transient(&ordinary);
            let left_cache = crate::geometry::FrameDependentCaches::default();
            let right_cache = crate::geometry::FrameDependentCaches::default();
            assert_eq!(
                graph_relate_data_geographic(
                    GeographicPrepared::new(&left_plan, &left_cache, frame),
                    GeographicPrepared::new(&right_plan, &right_cache, frame),
                )
                .0,
                expected.0,
                "{name}: fallback"
            );
        }
    }

    #[test]
    fn geographic_cap_point_matrices_are_stable() {
        let polygon = Shape::Polygon(Polygon::new(
            Ring::from_closed(CoordSeq::from(vec![
                Point::new_raw_xy(170.0, 80.0),
                Point::new_raw_xy(-170.0, 80.0),
                Point::new_raw_xy(-170.0, 90.0),
                Point::new_raw_xy(170.0, 90.0),
            ])),
            Vec::new(),
        ));
        for point in [
            Point::new_raw_xy(180.0, 85.0),
            Point::new_raw_xy(-180.0, 85.0),
            Point::new_raw_xy(179.999, 85.0),
            Point::new_raw_xy(-179.999, 85.0),
        ] {
            assert_eq!(
                graph_relate_shapes_geographic(&polygon, &Shape::Point(point)).text(),
                "0F2FF1FF2"
            );
        }
        for point in [
            Point::new_raw_xy(0.0, 90.0),
            Point::new_raw_xy(90.0, 90.0),
            Point::new_raw_xy(-90.0, 90.0),
        ] {
            assert_eq!(
                graph_relate_shapes_geographic(&polygon, &Shape::Point(point)).text(),
                "FF20F1FF2"
            );
        }
    }

    #[test]
    fn geographic_pole_line_intersects_cap_in_both_orders() {
        let line = Shape::LineString(
            LineSeq::try_new(CoordSeq::from(vec![
                Point::new_raw_xy(160.0, 90.0),
                Point::new_raw_xy(180.0, 90.0),
            ]))
            .expect("line coordinate sequence must contain at least two vertices"),
        );
        let cap = Shape::Polygon(polygon(&[
            [-10.0, 80.0],
            [10.0, 80.0],
            [10.0, 90.0],
            [-10.0, 90.0],
            [-10.0, 80.0],
        ]));
        assert!(graph_relate_shapes_geographic(&line, &cap).is_intersects());
        assert!(graph_relate_shapes_geographic(&cap, &line).is_intersects());
    }

    #[test]
    fn geographic_pole_cap_vertex_is_boundary() {
        let cap = Shape::Polygon(polygon(&[
            [-10.0, 80.0],
            [10.0, 80.0],
            [10.0, 90.0],
            [-10.0, 90.0],
            [-10.0, 80.0],
        ]));
        let pole = Shape::Point(Point::new_raw_xy(0.0, 90.0));
        assert_eq!(
            graph_relate_shapes_geographic(&cap, &pole).text(),
            "FF20F1FF2"
        );
    }

    #[test]
    fn geographic_split_crossing_keeps_boundary_point_relation() {
        let raw = Shape::Polygon(polygon(&[
            [170.0, 40.0],
            [-170.0, 40.0],
            [-170.0, 50.0],
            [170.0, 50.0],
            [170.0, 40.0],
        ]));
        let split = raw.split_antimeridian().expect("crossing polygon splits");
        let point = Shape::Point(Point::new_raw_xy(170.0, 40.0));
        let raw_point_relation = graph_relate_shapes_geographic(&raw, &point).text();
        let split_point_relation = graph_relate_shapes_geographic(&split, &point).text();
        assert_eq!(raw_point_relation, "FF20F1FF2");
        assert_eq!(split_point_relation, raw_point_relation);
        let probe = Shape::Polygon(rectangle(172.0, 42.0, 178.0, 48.0));
        let raw_probe_relation = graph_relate_shapes_geographic(&raw, &probe).text();
        let split_probe_relation = graph_relate_shapes_geographic(&split, &probe).text();
        assert_eq!(raw_probe_relation, "212FF1FF2");
        assert_eq!(split_probe_relation, raw_probe_relation);
    }

    #[test]
    fn geographic_aggregate_contacts_use_quotient_face_locations() {
        let adjacent = Shape::geometry_collection(vec![
            Shape::Polygon(rectangle(0.0, 0.0, 1.0, 1.0)),
            Shape::Polygon(rectangle(1.0, 0.0, 2.0, 1.0)),
        ])
        .expect("test collection is valid");
        let pinch = Shape::geometry_collection(vec![
            Shape::Polygon(Polygon::new(
                Ring::from_closed(vec![
                    Point::new_raw_xy(0.0, 0.0),
                    Point::new_raw_xy(-2.0, -1.0),
                    Point::new_raw_xy(-2.0, 1.0),
                    Point::new_raw_xy(0.0, 0.0),
                ]),
                Vec::new(),
            )),
            Shape::Polygon(Polygon::new(
                Ring::from_closed(vec![
                    Point::new_raw_xy(0.0, 0.0),
                    Point::new_raw_xy(2.0, 1.0),
                    Point::new_raw_xy(2.0, -1.0),
                    Point::new_raw_xy(0.0, 0.0),
                ]),
                Vec::new(),
            )),
        ])
        .expect("test collection is valid");
        let frame = crate::crs::info("EPSG:4326")
            .unwrap()
            .geographic_frame()
            .unwrap();
        for (geometry, point, expected) in [
            (&adjacent, Point::new_raw_xy(1.0, 0.5), "0F2FF1FF2"),
            (&adjacent, Point::new_raw_xy(1.0, 0.0), "FF20F1FF2"),
            (&pinch, Point::new_raw_xy(0.0, 0.0), "FF20F1FF2"),
        ] {
            let point = Shape::Point(point);
            assert_eq!(
                graph_relate_shapes_geographic(geometry, &point).text(),
                expected
            );
            let left = PreparedPlan::new(geometry.clone());
            let right = PreparedPlan::new(point);
            let left_cache = crate::geometry::FrameDependentCaches::default();
            let right_cache = crate::geometry::FrameDependentCaches::default();
            assert_eq!(
                graph_relate_data_geographic(
                    GeographicPrepared::new(&left, &left_cache, frame),
                    GeographicPrepared::new(&right, &right_cache, frame),
                )
                .text(),
                expected
            );
        }
    }

    #[test]
    fn geographic_grad_pole_aliases_match_after_native_latitude_admission() {
        let frame = crate::crs::info("EPSG:4807")
            .unwrap()
            .geographic_frame()
            .unwrap();
        let mut raw_pole: f64 = 100.0;
        for _ in 0..7 {
            raw_pole = raw_pole.next_up();
        }
        assert_eq!(raw_pole.to_bits(), frame.pole().to_bits());
        let left = PreparedPlan::new(Shape::Point(Point::new_raw_xy(0.0, frame.pole())));
        let right = PreparedPlan::new(Shape::Point(Point::new_raw_xy(200.0, frame.pole())));
        let left_cache = crate::geometry::FrameDependentCaches::default();
        let right_cache = crate::geometry::FrameDependentCaches::default();
        assert!(
            graph_relate_data_geographic(
                GeographicPrepared::new(&left, &left_cache, frame),
                GeographicPrepared::new(&right, &right_cache, frame),
            )
            .is_intersects()
        );
    }

    #[test]
    fn mixed_relation_node_locations_match_full_location_oracle() {
        let left = Shape::geometry_collection(vec![
            Shape::Polygon(rectangle(0.0, 0.0, 2.0, 2.0)),
            Shape::LineString(
                LineSeq::try_new(CoordSeq::from(vec![
                    Point::new_raw_xy(-1.0, 1.0),
                    Point::new_raw_xy(1.0, 1.0),
                    Point::new_raw_xy(3.0, 1.0),
                ]))
                .expect("line coordinate sequence must contain at least two vertices"),
            ),
            Shape::Point(Point::new_raw_xy(1.0, 1.0)),
        ])
        .expect("test collection is valid");
        let right = Shape::geometry_collection(vec![
            Shape::Polygon(rectangle(1.0, -1.0, 3.0, 1.0)),
            Shape::LineString(
                LineSeq::try_new(CoordSeq::from(vec![
                    Point::new_raw_xy(1.0, 1.0),
                    Point::new_raw_xy(1.0, 3.0),
                ]))
                .expect("line coordinate sequence must contain at least two vertices"),
            ),
        ])
        .expect("test collection is valid");
        let left_graph = TopologyGraph::cartesian(&left);
        let right_graph = TopologyGraph::cartesian(&right);
        assert_eq!(
            graph_relate(&left_graph, &right_graph).text(),
            graph_relate_with_full_node_locations(&left_graph, &right_graph).text(),
        );

        let geographic_left = Shape::geometry_collection(vec![
            Shape::Polygon(polygon(&[
                [170.0, 0.0],
                [-170.0, 0.0],
                [-170.0, 10.0],
                [170.0, 10.0],
                [170.0, 0.0],
            ])),
            Shape::LineString(
                LineSeq::try_new(CoordSeq::from(vec![
                    Point::new_raw_xy(175.0, 5.0),
                    Point::new_raw_xy(180.0, 5.0),
                    Point::new_raw_xy(-175.0, 5.0),
                ]))
                .expect("line coordinate sequence must contain at least two vertices"),
            ),
            Shape::Point(Point::new_raw_xy(180.0, 5.0)),
        ])
        .expect("test collection is valid");
        let geographic_right = Shape::geometry_collection(vec![
            Shape::Polygon(rectangle(175.0, -5.0, 179.0, 5.0)),
            Shape::LineString(
                LineSeq::try_new(CoordSeq::from(vec![
                    Point::new_raw_xy(180.0, 5.0),
                    Point::new_raw_xy(180.0, 15.0),
                ]))
                .expect("line coordinate sequence must contain at least two vertices"),
            ),
        ])
        .expect("test collection is valid");
        let frame = crate::crs::info("EPSG:4326")
            .expect("EPSG:4326 must be available")
            .geographic_frame()
            .expect("EPSG:4326 must have a geographic frame");
        let left_graph = TopologyGraph::geographic(&geographic_left, frame);
        let right_graph = TopologyGraph::geographic(&geographic_right, frame);
        assert_eq!(
            graph_relate(&left_graph, &right_graph).text(),
            graph_relate_with_full_node_locations(&left_graph, &right_graph).text(),
        );
    }
}
