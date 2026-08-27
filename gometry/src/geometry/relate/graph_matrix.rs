use crate::collections::{HashSet, HashSetExt as _};
use crate::geometry::relate::{De9im, Loc, polygon_has_nondegenerate_area};
use crate::geometry::topology::{TopologyGraph, TopologyLocation, TopologyNodeId, TopologySpace};
use crate::geometry::{
    Bounds, CoordSeq, Coordinates as _, HashMap, HashMapExt as _, PointKey, Polygon, PreparedPlan,
    Ring, Segment, Shape, XY, overlay, same_point, segments_intersect,
};

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
    if let Some(point) = shape_point(right.shape()) {
        return graph_relate_to_point(left.topology_graph(), point);
    }
    if let Some(point) = shape_point(left.shape()) {
        return graph_relate_to_point(right.topology_graph(), point).transpose();
    }
    graph_relate(left.topology_graph(), right.topology_graph())
}

pub(crate) fn graph_relate_data_geographic(left: &PreparedPlan, right: &PreparedPlan) -> De9im {
    if let Some(point) = shape_point(right.shape()) {
        return graph_relate_to_point(left.geographic_topology_graph(), point);
    }
    if let Some(point) = shape_point(left.shape()) {
        return graph_relate_to_point(right.geographic_topology_graph(), point).transpose();
    }
    graph_relate(
        left.geographic_topology_graph(),
        right.geographic_topology_graph(),
    )
}

fn graph_relate_plans(left: &PreparedPlan, right: &PreparedPlan, space: TopologySpace) -> De9im {
    match space {
        TopologySpace::Cartesian => graph_relate(left.topology_graph(), right.topology_graph()),
        TopologySpace::Geographic => graph_relate(
            left.geographic_topology_graph(),
            right.geographic_topology_graph(),
        ),
    }
}

/// Exact `intersects` certificate for a rectangle row. A closed rectangle
/// intersects a source iff one of its corners is on the source, one source
/// graph node lies in the rectangle, or their boundary segments meet. The
/// general case still returns the complete matrix below this certificate.
pub(crate) fn graph_intersects_rectangle_data(
    source: &PreparedPlan,
    rectangle: &Shape,
    geographic: bool,
) -> Option<bool> {
    let bounds = rectangle.bounds()?;
    if !is_axis_aligned_rectangle_for_certificate(rectangle, bounds) {
        return None;
    }
    let graph = if geographic {
        source.geographic_topology_graph()
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
fn is_axis_aligned_rectangle_for_certificate(shape: &Shape, bounds: Bounds) -> bool {
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
    if let Some(point) = single_point(right) {
        return graph_relate_to_point(left, point);
    }
    if let Some(point) = single_point(left) {
        return graph_relate_to_point(right, point).transpose();
    }
    let mut matrix = De9im::empty_disjoint();
    add_face_cells(&mut matrix, left, right);

    let (edges, hints) = pair_edges(left, right);
    add_edge_cells(&mut matrix, left, right, &edges);

    let mut nodes = Vec::with_capacity(left.nodes.len() + right.nodes.len() + edges.len() * 2);
    let mut seen = HashSet::with_capacity(nodes.capacity());
    for node in left.nodes.iter().chain(right.nodes.iter()) {
        push_node(&mut nodes, &mut seen, node.point, left, right);
    }
    for (segment, _) in &edges {
        push_node(&mut nodes, &mut seen, segment.start, left, right);
        push_node(&mut nodes, &mut seen, segment.end, left, right);
    }
    add_node_cells(&mut matrix, left, right, &nodes, &hints);
    suppress_lineal_overlap_artifacts(&mut matrix, left, right);
    matrix
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
            let (left_polygons, right_polygons) = if left.space == TopologySpace::Geographic {
                geographic_area_polygons(left, right)
            } else {
                (left_polygons, right_polygons)
            };
            let arrangement = overlay::build_areal_arrangement(&left_polygons, &right_polygons);
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
    let anchor = area_center(left)
        .or_else(|| area_center(right))
        .unwrap_or(0.0);
    (
        chart_area_polygons(left, anchor),
        chart_area_polygons(right, anchor),
    )
}

fn area_center(graph: &TopologyGraph) -> Option<f64> {
    graph
        .area_support
        .iter()
        .filter(|support| polygon_has_nondegenerate_area(&support.source))
        .map(|support| ring_center(&support.shell.points))
        .next()
}

fn chart_area_polygons(graph: &TopologyGraph, anchor: f64) -> Vec<Polygon> {
    graph
        .area_support
        .iter()
        .filter(|support| polygon_has_nondegenerate_area(&support.source))
        .filter_map(|support| {
            let shell_center = ring_center(&support.shell.points);
            let shift = chart_shift(anchor, shell_center);
            let shell = chart_ring(&support.shell.points, shift)?;
            let holes = support
                .holes
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

fn chart_shift(anchor: f64, center: f64) -> f64 {
    360.0 * ((anchor - center) / 360.0).round()
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
    Some(Ring::from_trusted_closed(CoordSeq::from(coords)))
}

fn area_polygons(graph: &TopologyGraph) -> Vec<crate::geometry::Polygon> {
    graph
        .area_support
        .iter()
        .filter(|support| polygon_has_nondegenerate_area(&support.source))
        .map(|support| support.source.clone())
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
    let mut source = Vec::with_capacity(left.edges.len() + right.edges.len());
    source.extend(left.edges.iter().map(|edge| edge.segment));
    source.extend(right.edges.iter().map(|edge| edge.segment));
    if source.is_empty() {
        return (Vec::new(), HashMap::new());
    }
    let split = left.edges.len();
    let (atomic, provenance) = overlay::self_node_segments_sourced(&source);
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
    point: XY,
    left: &TopologyGraph,
    right: &TopologyGraph,
) {
    let token = left
        .node_for(point)
        .map(NodeToken::Left)
        .or_else(|| right.node_for(point).map(NodeToken::Right));
    let Some(token) = token else {
        // New cross-operand intersection points are not in either source
        // graph. They are uncommon; retain the quotient-aware fallback for
        // those transient points.
        if nodes
            .iter()
            .any(|known| left.equivalent(*known, point) || right.equivalent(*known, point))
        {
            return;
        }
        nodes.push(point);
        return;
    };
    if seen.insert(token) {
        nodes.push(point);
    }
}

fn add_node_cells(
    matrix: &mut De9im,
    left: &TopologyGraph,
    right: &TopologyGraph,
    nodes: &[XY],
    hints: &HashMap<PointKey, EdgeLabel>,
) {
    for &point in nodes {
        let hint = hints.get(&PointKey::new(point)).copied();
        let left_loc = locate_with_hint(left, point, hint.and_then(|hint| hint.left));
        let right_loc = locate_with_hint(right, point, hint.and_then(|hint| hint.right));
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

fn locate_with_hint(
    graph: &TopologyGraph,
    point: XY,
    hint: Option<TopologyLocation>,
) -> TopologyLocation {
    match graph.locate_point(point) {
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
            Ring::from_trusted_closed(vec![
                Point::new_unchecked_xy(minx, miny),
                Point::new_unchecked_xy(maxx, miny),
                Point::new_unchecked_xy(maxx, maxy),
                Point::new_unchecked_xy(minx, maxy),
                Point::new_unchecked_xy(minx, miny),
            ]),
            Vec::new(),
        )
    }

    fn donut() -> Polygon {
        Polygon::new(rectangle(0.0, 0.0, 10.0, 10.0).shell, vec![
            Ring::from_trusted_closed(vec![
                Point::new_unchecked_xy(3.0, 3.0),
                Point::new_unchecked_xy(7.0, 3.0),
                Point::new_unchecked_xy(7.0, 7.0),
                Point::new_unchecked_xy(3.0, 7.0),
                Point::new_unchecked_xy(3.0, 3.0),
            ]),
        ])
    }

    fn arrangement_oracle(left: &[Polygon], right: &[Polygon]) -> De9im {
        let built = overlay::build_areal_arrangement(left, right);
        let mut matrix = De9im::empty_disjoint();
        for &winding in &built.windings {
            let (left_inside, right_inside) = (winding[0] >= 1, winding[1] >= 1);
            if left_inside && right_inside {
                matrix.set_at_least(
                    Loc::Interior,
                    Loc::Interior,
                    crate::geometry::Dimension::Surface,
                );
            } else if left_inside {
                matrix.set_at_least(
                    Loc::Interior,
                    Loc::Exterior,
                    crate::geometry::Dimension::Surface,
                );
            } else if right_inside {
                matrix.set_at_least(
                    Loc::Exterior,
                    Loc::Interior,
                    crate::geometry::Dimension::Surface,
                );
            }
        }
        let mut left_incident = vec![false; built.arrangement.vertex_count()];
        let mut right_incident = vec![false; built.arrangement.vertex_count()];
        let mut shared_edge = false;
        built
            .arrangement
            .for_each_edge_piece(|multiplicity, from, to, left_face, _right_face| {
                let left_edge = multiplicity[0] != 0;
                let right_edge = multiplicity[1] != 0;
                if left_edge {
                    left_incident[from as usize] = true;
                    left_incident[to as usize] = true;
                }
                if right_edge {
                    right_incident[from as usize] = true;
                    right_incident[to as usize] = true;
                }
                match (left_edge, right_edge) {
                    (true, true) => shared_edge = true,
                    (true, false) => {
                        let location = if built.windings[left_face as usize][1] >= 1 {
                            Loc::Interior
                        } else {
                            Loc::Exterior
                        };
                        matrix.set_at_least(
                            location,
                            Loc::Boundary,
                            crate::geometry::Dimension::Curve,
                        );
                    },
                    (false, true) => {
                        let location = if built.windings[left_face as usize][0] >= 1 {
                            Loc::Interior
                        } else {
                            Loc::Exterior
                        };
                        matrix.set_at_least(
                            Loc::Boundary,
                            location,
                            crate::geometry::Dimension::Curve,
                        );
                    },
                    (false, false) => {},
                }
            });
        if shared_edge {
            matrix.set_at_least(
                Loc::Boundary,
                Loc::Boundary,
                crate::geometry::Dimension::Curve,
            );
        } else if left_incident
            .iter()
            .zip(&right_incident)
            .any(|(&left, &right)| left && right)
        {
            matrix.set_at_least(
                Loc::Boundary,
                Loc::Boundary,
                crate::geometry::Dimension::Point,
            );
        }
        matrix
    }

    #[test]
    fn graph_matrix_matches_arrangement_oracle_on_enumerated_areal_cases() {
        let cases = [
            (
                "shared_edge",
                rectangle(0.0, 0.0, 2.0, 2.0),
                rectangle(2.0, 0.0, 4.0, 2.0),
                "FF2F11212",
            ),
            (
                "shared_corner",
                rectangle(0.0, 0.0, 2.0, 2.0),
                rectangle(2.0, 2.0, 4.0, 4.0),
                "FF2F01212",
            ),
            (
                "overlap",
                rectangle(0.0, 0.0, 2.0, 2.0),
                rectangle(1.0, 1.0, 3.0, 3.0),
                "212101212",
            ),
            (
                "identical",
                rectangle(0.0, 0.0, 2.0, 2.0),
                rectangle(0.0, 0.0, 2.0, 2.0),
                "2FFF1FFF2",
            ),
            (
                "donut_hole",
                donut(),
                rectangle(3.0, 3.0, 5.0, 5.0),
                "FF2F11212",
            ),
        ];
        for (name, left, right, expected) in cases {
            let left_shape = Shape::Polygon(left.clone());
            let right_shape = Shape::Polygon(right.clone());
            let oracle = arrangement_oracle(&[left], &[right]);
            assert_eq!(oracle.text(), expected, "{name}: oracle fixture");
            assert_eq!(
                graph_relate_shapes(&left_shape, &right_shape).text(),
                expected,
                "{name}: graph"
            );
        }
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
                Shape::Point(Point::new_unchecked_xy(5.0, 5.0)),
            ),
            (
                Shape::LineString(LineSeq::from_trusted(CoordSeq::from(vec![
                    Point::new_unchecked_xy(0.0, 0.0),
                    Point::new_unchecked_xy(2.0, 2.0),
                ]))),
                Shape::LineString(LineSeq::from_trusted(CoordSeq::from(vec![
                    Point::new_unchecked_xy(0.0, 2.0),
                    Point::new_unchecked_xy(2.0, 0.0),
                ]))),
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
        let point = Shape::Point(Point::new_unchecked_xy(0.0, 0.0));
        let line = Shape::LineString(LineSeq::from_trusted(CoordSeq::from(vec![
            Point::new_unchecked_xy(0.0, 0.0),
            Point::new_unchecked_xy(1.0, 0.0),
        ])));
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

    #[test]
    fn geographic_cap_point_matrices_are_stable() {
        let polygon = Shape::Polygon(Polygon::new(
            Ring::from_trusted_closed(CoordSeq::from(vec![
                Point::new_unchecked_xy(170.0, 80.0),
                Point::new_unchecked_xy(-170.0, 80.0),
                Point::new_unchecked_xy(-170.0, 90.0),
                Point::new_unchecked_xy(170.0, 90.0),
            ])),
            Vec::new(),
        ));
        for point in [
            Point::new_unchecked_xy(180.0, 85.0),
            Point::new_unchecked_xy(-180.0, 85.0),
            Point::new_unchecked_xy(179.999, 85.0),
            Point::new_unchecked_xy(-179.999, 85.0),
        ] {
            assert_eq!(
                graph_relate_shapes_geographic(&polygon, &Shape::Point(point)).text(),
                "0F2FF1FF2"
            );
        }
        for point in [
            Point::new_unchecked_xy(0.0, 90.0),
            Point::new_unchecked_xy(90.0, 90.0),
            Point::new_unchecked_xy(-90.0, 90.0),
        ] {
            assert_eq!(
                graph_relate_shapes_geographic(&polygon, &Shape::Point(point)).text(),
                "FF20F1FF2"
            );
        }
    }
}
