#[cfg(test)]
use ahash::HashMapExt as _;

use crate::boundary::geographic::invalid_lonlat_error;
use crate::collections::HashSet;
use crate::error::{ResourceError, Result};
use crate::geographic::{GeographicFrame, ScalarGeographicView};
use crate::geometry::overlay::ExactSourceInventory;
#[cfg(test)]
use crate::geometry::predicates;
use crate::geometry::relate::{De9im, Loc, polygon_has_nondegenerate_area};
use crate::geometry::topology::{
    AreaSupport, TopologyGraph, TopologyKey, TopologyLocation, TopologyNodeId,
};
use crate::geometry::{
    Bounds, CoordSeq, Coordinates as _, HashMap, PointKey, Polygon, PreparedPlan, Ring, Segment,
    Shape, XY, overlay, same_point, segments_intersect, topology,
};
use crate::{try_push_vec, try_reserve_vec};

#[derive(Clone, Copy)]
pub(crate) struct GeographicPrepared<'a> {
    plan: &'a PreparedPlan,
    frame_cache: &'a crate::geometry::FrameDependentCaches,
    view: ScalarGeographicView<'a>,
    bounds_sound: bool,
}

impl<'a> GeographicPrepared<'a> {
    /// The operand's plan and frame, for a caller running the pre-topology
    /// gates before admitting this pair to a relation.
    pub(crate) const fn plan(&self) -> &'a PreparedPlan {
        self.plan
    }

    pub(crate) const fn frame(&self) -> GeographicFrame {
        self.view.frame()
    }

    pub(crate) const fn view(self) -> ScalarGeographicView<'a> {
        self.view
    }

    /// Whether the operand's raw box stands in for it in this frame.
    pub(crate) const fn bounds_sound(&self) -> bool {
        self.bounds_sound
    }

    /// Admit `plan.shape()` once per frame; later pairs reuse the admission
    /// and its box fact from the plan's frame cache.
    pub(crate) fn try_new(
        plan: &'a PreparedPlan,
        frame_cache: &'a crate::geometry::FrameDependentCaches,
        frame: GeographicFrame,
    ) -> crate::error::Result<Self> {
        if let Some(bounds_sound) = frame_cache.admitted_bounds_sound(frame) {
            return Ok(Self {
                plan,
                frame_cache,
                view: ScalarGeographicView::previously_admitted(frame, plan.shape()),
                bounds_sound,
            });
        }
        let view = ScalarGeographicView::try_new(frame, plan.shape())
            .map_err(|diagnostic| invalid_lonlat_error(diagnostic.native.x, diagnostic.native.y))?;
        let bounds_sound = plan.bounds().is_some_and(|bounds| {
            crate::geometry::raw_coordinate_bounds_are_sound_in_frame(
                Some(plan.shape()),
                bounds,
                frame,
            )
        });
        frame_cache.record_admission(frame, bounds_sound);
        Ok(Self {
            plan,
            frame_cache,
            view,
            bounds_sound,
        })
    }
}

impl GeographicPrepared<'_> {
    pub(crate) fn graph(self) -> Result<std::sync::Arc<TopologyGraph>> {
        self.frame_cache.geographic_topology_graph(self.view)
    }
}

/// Compute the complete Cartesian relation matrix on the exact partition.
/// Every named predicate is a projection of this result.
pub(crate) fn graph_relate_shapes(left: &Shape, right: &Shape) -> Result<De9im> {
    graph_relate_data(
        &PreparedPlan::transient(left),
        &PreparedPlan::transient(right),
    )
}

/// The geographic relation the tests read: a degree frame stands in for a
/// CRS, which is why this never leaves the test build.
#[cfg(test)]
fn graph_relate_shapes_geographic(left: &Shape, right: &Shape) -> De9im {
    let unit =
        crate::geographic::AngularFactor::try_new("test degrees", std::f64::consts::PI / 180.0)
            .unwrap();
    let frame = GeographicFrame::try_new(unit, unit, 0.0).unwrap();
    let left = PreparedPlan::transient(left);
    let right = PreparedPlan::transient(right);
    let left_cache = crate::geometry::FrameDependentCaches::default();
    let right_cache = crate::geometry::FrameDependentCaches::default();
    graph_relate_data_geographic(
        GeographicPrepared::try_new(&left, &left_cache, frame).expect("fixture admits"),
        GeographicPrepared::try_new(&right, &right_cache, frame).expect("fixture admits"),
    )
    .unwrap()
}

pub(crate) fn graph_relate_data(left: &PreparedPlan, right: &PreparedPlan) -> Result<De9im> {
    exact_cartesian_relate(left.shape(), right.shape())
}

fn exact_cartesian_relate(left: &Shape, right: &Shape) -> Result<De9im> {
    let inputs = [left, right];
    let inventory = ExactSourceInventory::from_shapes(&inputs)?;
    let mut budget = crate::geometry::ExpansionBudget::new("relate", "exact partition");
    let partition = inventory.partition(&mut budget)?;
    inventory.relate(&partition)
}

#[expect(
    clippy::large_types_passed_by_value,
    reason = "GeographicPrepared is a Copy view of three borrows; the relate functions take the completed prepare step by value"
)]
pub(crate) fn graph_relate_data_geographic(
    left: GeographicPrepared<'_>,
    right: GeographicPrepared<'_>,
) -> Result<De9im> {
    debug_assert_eq!(left.frame(), right.frame());
    if let Some(matrix) = try_areal_relate_geographic(
        left.plan.shape(),
        || left.bounds_sound(),
        right.plan.shape(),
        || right.bounds_sound(),
    )? {
        return Ok(matrix);
    }
    if let Some(point) = shape_point(right.plan.shape()) {
        if let Some(left_point) = shape_point(left.plan.shape()) {
            return Ok(point_relate_point(left_point, point, left.frame()).transpose());
        }
        let left_graph = left.graph()?;
        return Ok(graph_relate_to_point(&left_graph, point));
    }
    if let Some(point) = shape_point(left.plan.shape()) {
        let right_graph = right.graph()?;
        return Ok(graph_relate_to_point(&right_graph, point).transpose());
    }
    let left_graph = left.graph()?;
    let right_graph = right.graph()?;
    graph_relate(&left_graph, &right_graph)
}

/// Compute a geographic relation from topology graphs already resolved by the
/// caller. This keeps the direct areal fold ahead of graph assembly.
#[expect(
    clippy::large_types_passed_by_value,
    reason = "GeographicPrepared is a Copy view of three borrows; the relate functions take the completed prepare step by value"
)]
pub(crate) fn graph_relate_data_geographic_with_left_graph(
    left: &PreparedPlan,
    left_graph: &TopologyGraph,
    right: GeographicPrepared<'_>,
) -> Result<De9im> {
    if let Some(matrix) = try_areal_relate_geographic(
        left.shape(),
        || plan_bounds_sound(left, right.frame()),
        right.plan.shape(),
        || right.bounds_sound(),
    )? {
        return Ok(matrix);
    }
    if let Some(point) = shape_point(right.plan.shape()) {
        if let Some(left_point) = shape_point(left.shape()) {
            return Ok(point_relate_point(left_point, point, right.frame()).transpose());
        }
        return Ok(graph_relate_to_point(left_graph, point));
    }
    if let Some(point) = shape_point(left.shape()) {
        let right_graph = right.graph()?;
        return Ok(graph_relate_to_point(&right_graph, point).transpose());
    }
    let right_graph = right.graph()?;
    graph_relate(left_graph, &right_graph)
}

/// Compute a geographic relation while reusing the right operand's resolved
/// graph. The left graph remains lazy so direct areal pairs do no graph work.
#[expect(
    clippy::large_types_passed_by_value,
    reason = "GeographicPrepared is a Copy view of three borrows; the relate functions take the completed prepare step by value"
)]
pub(crate) fn graph_relate_data_geographic_with_right_graph(
    left: GeographicPrepared<'_>,
    right: &PreparedPlan,
    right_graph: &TopologyGraph,
) -> Result<De9im> {
    if let Some(matrix) = try_areal_relate_geographic(
        left.plan.shape(),
        || left.bounds_sound(),
        right.shape(),
        || plan_bounds_sound(right, left.frame()),
    )? {
        return Ok(matrix);
    }
    if let Some(point) = shape_point(right.shape()) {
        if let Some(left_point) = shape_point(left.plan.shape()) {
            return Ok(point_relate_point(left_point, point, left.frame()).transpose());
        }
        let left_graph = left.graph()?;
        return Ok(graph_relate_to_point(&left_graph, point));
    }
    if let Some(point) = shape_point(left.plan.shape()) {
        return Ok(graph_relate_to_point(right_graph, point).transpose());
    }
    let left_graph = left.graph()?;
    graph_relate(&left_graph, right_graph)
}

/// Whether a scalar operand broadcast against `row` reaches the direct
/// polygon/line fold instead of two source topology graphs.
///
/// The asymmetry is the point: a polygon scalar keeps a reusable graph across
/// the whole broadcast, so caching it is right, while a line scalar has no
/// reusable support and would build a graph only for the fold to transpose the
/// polygon-side answer. Batch seams ask this before resolving that graph.
pub(crate) const fn line_scalar_prefers_direct_polygon_relate(scalar: &Shape, row: &Shape) -> bool {
    matches!((scalar, row), (Shape::LineString(_), Shape::Polygon(_)))
}

/// The direct areal fold reads raw coordinate boxes, so it runs only when both
/// boxes stand in for their operands; points and lines never take it.
fn try_areal_relate_geographic(
    left: &Shape,
    left_bounds_sound: impl FnOnce() -> bool,
    right: &Shape,
    right_bounds_sound: impl FnOnce() -> bool,
) -> Result<Option<De9im>> {
    let areal = |shape: &Shape| {
        matches!(
            shape,
            Shape::Polygon(_) | Shape::MultiPolygon(_) | Shape::GeometryCollection(_)
        )
    };
    if areal(left) && areal(right) && left_bounds_sound() && right_bounds_sound() {
        try_areal_relate(left, right)
    } else {
        Ok(None)
    }
}

fn plan_bounds_sound(plan: &PreparedPlan, frame: crate::geographic::GeographicFrame) -> bool {
    plan.bounds().is_some_and(|bounds| {
        crate::geometry::raw_coordinate_bounds_are_sound_in_frame(Some(plan.shape()), bounds, frame)
    })
}

/// Fold a complete DE-9IM matrix from the one arrangement shared by two
/// nondegenerate Cartesian areal operands. Source graphs retain boundary-only
/// degeneracies, so those inputs deliberately stay on the graph path.
pub(super) fn try_areal_relate(left: &Shape, right: &Shape) -> Result<Option<De9im>> {
    let mut left_polygons = Vec::new();
    let mut right_polygons = Vec::new();
    if collect_areal_polygons(left, &mut left_polygons)?.is_none()
        || collect_areal_polygons(right, &mut right_polygons)?.is_none()
        || left_polygons.is_empty()
        || right_polygons.is_empty()
    {
        return Ok(None);
    }

    let built = overlay::build_areal_arrangement(&left_polygons, &right_polygons)?;
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
            let left_inside = winding[0] != 0;
            let right_inside = winding[1] != 0;
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
    Ok(Some(matrix))
}

/// The polygons of an areal shape whose every ring is sound (see
/// [`ring_is_sound`]), or `None` when the shape is not areal or any ring
/// is not.
pub(super) fn collect_areal_polygons<'a>(
    shape: &'a Shape,
    polygons: &mut Vec<&'a Polygon>,
) -> Result<Option<()>> {
    let first = polygons.len();
    if collect_polygons(shape, polygons)?.is_none() {
        return Ok(None);
    }
    Ok(polygons[first..]
        .iter()
        .all(|polygon| polygon.rings().all(ring_is_sound))
        .then_some(()))
}

/// Every polygon of an areal shape, structurally, or `None` when the shape
/// is not areal at all.
pub(crate) fn collect_polygons<'a>(
    shape: &'a Shape,
    polygons: &mut Vec<&'a Polygon>,
) -> Result<Option<()>> {
    match shape {
        Shape::Polygon(polygon) => {
            try_push_vec(polygons, polygon)?;
            Ok(Some(()))
        },
        Shape::MultiPolygon(parts) => {
            for polygon in parts.iter() {
                try_push_vec(polygons, polygon)?;
            }
            Ok(Some(()))
        },
        Shape::GeometryCollection(parts) => {
            for part in parts {
                if collect_polygons(part, polygons)?.is_none() {
                    return Ok(None);
                }
            }
            Ok(Some(()))
        },
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Empty(..) => Ok(None),
    }
}

/// A ring the areal engines can reason about: closed with real extent and
/// a nonzero exact area, and for the arrangement fold no zero-length
/// segment either.
pub(crate) fn ring_is_sound(ring: &Ring) -> bool {
    ring_has_area(ring)
        && !ring
            .segment_pairs()
            .any(|[start, end]| same_point(start, end))
}

/// A closed ring with real extent and a nonzero exact area.
pub(crate) fn ring_has_area(ring: &Ring) -> bool {
    ring.coord_count() >= Ring::MIN_VERTICES_CLOSED
        && !crate::geometry::ring_winding(ring).is_degenerate()
}

const fn areal_location(winding: i32) -> Loc {
    if winding != 0 {
        Loc::Interior
    } else {
        Loc::Exterior
    }
}

const fn areal_edge_location(left_winding: i32, right_winding: i32) -> Loc {
    match (left_winding != 0, right_winding != 0) {
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

/// Exact `intersects` check for a supported rectangle against a source graph.
/// A closed rectangle intersects a source iff one of its corners is on the
/// source, one source graph node lies in the rectangle, or their boundary
/// segments meet. Callers own rectangle admission before reaching this scan.
pub(crate) fn graph_intersects_bounds(graph: &TopologyGraph, bounds: Bounds) -> bool {
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
        return true;
    }
    let rectangle_edges = rectangle_edges(bounds);
    graph.edges.iter().any(|edge| {
        bounds.contains_xy(edge.segment.start)
            || bounds.contains_xy(edge.segment.end)
            || rectangle_edges
                .iter()
                .copied()
                .any(|candidate| segments_intersect(edge.segment, candidate))
    })
}

const fn rectangle_edges(bounds: Bounds) -> [Segment; 4] {
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
/// Whether `shape` is exactly the box `bounds`: a hole-free polygon whose every
/// vertex sits on the box, whose every step runs along one of its sides, which
/// reaches all four corners, and whose winding encloses the box. Extra
/// collinear vertices along a side are admitted; a degenerate box, or a ring
/// whose traversals cancel, is not.
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
    seen.iter().all(|&corner| corner) && ring_has_area(&polygon.shell)
}

#[expect(
    clippy::panic_in_result_fn,
    reason = "a shared frame is an internal construction invariant, not a public input"
)]
fn graph_relate(left: &TopologyGraph, right: &TopologyGraph) -> Result<De9im> {
    assert_eq!(
        left.frame, right.frame,
        "relation graphs must share a frame"
    );
    if let Some(point) = single_point(right) {
        return Ok(graph_relate_to_point(left, point));
    }
    if let Some(point) = single_point(left) {
        return Ok(graph_relate_to_point(right, point).transpose());
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
) -> Result<De9im> {
    let mut matrix = De9im::empty_disjoint();
    add_face_cells(&mut matrix, left, right)?;

    let (edges, hints) = pair_edges(left, right)?;
    add_edge_cells(&mut matrix, left, right, &edges);

    let node_capacity = left
        .nodes
        .len()
        .checked_add(right.nodes.len())
        .and_then(|count| count.checked_add(edges.len().checked_mul(2)?))
        .ok_or(ResourceError::SizeOverflow)?;
    let mut nodes = Vec::new();
    try_reserve_vec(&mut nodes, node_capacity)?;
    let mut seen = crate::collections::try_hash_set_with_capacity(node_capacity)?;
    let mut node_owners = crate::collections::try_hash_map_with_capacity(node_capacity)?;
    let mut transient = crate::collections::try_hash_set_with_capacity(node_capacity)?;
    for node in left.nodes.iter().chain(right.nodes.iter()) {
        push_node(
            &mut nodes,
            &mut seen,
            &mut node_owners,
            &mut transient,
            node.point,
            left,
            right,
        )?;
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
        )?;
        push_node(
            &mut nodes,
            &mut seen,
            &mut node_owners,
            &mut transient,
            segment.end,
            left,
            right,
        )?;
    }
    add_nodes(&mut matrix, left, right, &nodes, &node_owners, &hints);
    suppress_lineal_overlap_artifacts(&mut matrix, left, right);
    Ok(matrix)
}

#[cfg(test)]
fn graph_relate_with_full_node_locations(left: &TopologyGraph, right: &TopologyGraph) -> De9im {
    graph_relate_with_node_cells(left, right, |matrix, left, right, nodes, _, hints| {
        let no_node_owners = HashMap::new();
        add_node_cells(matrix, left, right, nodes, &no_node_owners, hints);
    })
    .unwrap()
}

fn single_point(graph: &TopologyGraph) -> Option<XY> {
    (graph.edges.is_empty() && graph.area_support.is_empty() && graph.source_points.len() == 1)
        .then(|| graph.source_points[0].point.xy())
}

const fn shape_point(shape: &Shape) -> Option<XY> {
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

fn add_face_cells(matrix: &mut De9im, left: &TopologyGraph, right: &TopologyGraph) -> Result<()> {
    let left_polygons = area_polygons(left)?;
    let right_polygons = area_polygons(right)?;
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
            let (left_polygons, right_polygons) = geographic_area_polygons(left, right)?;
            let arrangement = overlay::build_areal_arrangement(&left_polygons, &right_polygons)?;
            for winding in arrangement.windings {
                let left_loc = if winding[0] != 0 {
                    Loc::Interior
                } else {
                    Loc::Exterior
                };
                let right_loc = if winding[1] != 0 {
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
    Ok(())
}

const fn set_dimension(
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
) -> crate::error::Result<(Vec<Polygon>, Vec<Polygon>)> {
    debug_assert_eq!(left.frame, right.frame);
    let period = left.frame.longitude().period();
    let anchor = area_center(left)
        .or_else(|| area_center(right))
        .unwrap_or(0.0);
    Ok((
        chart_area_polygons(left, anchor, period)?,
        chart_area_polygons(right, anchor, period)?,
    ))
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

fn chart_area_polygons(
    graph: &TopologyGraph,
    anchor: f64,
    period: f64,
) -> crate::error::Result<Vec<Polygon>> {
    let mut polygons = Vec::new();
    try_reserve_vec(&mut polygons, graph.area_support.len())?;
    for support in graph
        .area_support
        .iter()
        .filter(|support| polygon_has_nondegenerate_area(support.source()))
    {
        let (support_shell, support_holes) = support.geographic_rings();
        let shell_center = ring_center(&support_shell.points);
        let shift = chart_shift(anchor, shell_center, period);
        let Some(shell) = chart_ring(&support_shell.points, shift)? else {
            continue;
        };
        let mut holes = Vec::new();
        try_reserve_vec(&mut holes, support_holes.len())?;
        for hole in support_holes {
            if let Some(hole) = chart_ring(&hole.points, shift)? {
                try_push_vec(&mut holes, hole)?;
            }
        }
        try_push_vec(&mut polygons, Polygon::new(shell, holes))?;
    }
    Ok(polygons)
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

fn chart_ring(points: &[XY], shift: f64) -> crate::error::Result<Option<Ring>> {
    if points.len() < 3 {
        return Ok(None);
    }
    let coordinate_count = points
        .len()
        .checked_add(1)
        .ok_or(ResourceError::SizeOverflow)?;
    let mut coords = Vec::new();
    try_reserve_vec(&mut coords, coordinate_count)?;
    for point in points {
        try_push_vec(&mut coords, XY::new(point.x + shift, point.y).point())?;
    }
    let first = coords[0];
    try_push_vec(&mut coords, first)?;
    Ok(Ring::from_closed(CoordSeq::try_from_points(&coords)?))
}

fn area_polygons(graph: &TopologyGraph) -> Result<Vec<&AreaSupport>> {
    let mut polygons = Vec::new();
    try_reserve_vec(&mut polygons, graph.area_support.len())?;
    for support in &graph.area_support {
        if polygon_has_nondegenerate_area(support.source()) {
            try_push_vec(&mut polygons, support)?;
        }
    }
    Ok(polygons)
}

#[derive(Clone, Copy)]
struct EdgeLabel {
    left: Option<TopologyLocation>,
    right: Option<TopologyLocation>,
}

type PairedEdges = (Vec<(Segment, EdgeLabel)>, HashMap<PointKey, EdgeLabel>);

fn pair_edges(left: &TopologyGraph, right: &TopologyGraph) -> crate::error::Result<PairedEdges> {
    if left.edges.is_empty() || right.edges.is_empty() {
        let (graph, left_present) = if left.edges.is_empty() {
            (right, false)
        } else {
            (left, true)
        };
        let mut edges = Vec::new();
        try_reserve_vec(&mut edges, graph.edges.len())?;
        let hint_capacity = graph
            .edges
            .len()
            .checked_mul(2)
            .ok_or(ResourceError::SizeOverflow)?;
        let mut hints = crate::collections::try_hash_map_with_capacity(hint_capacity)?;
        for edge in &graph.edges {
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
            try_push_vec(&mut edges, (edge.segment, label))?;
        }
        return Ok((edges, hints));
    }
    let split = left.edges.len();
    let (atomic, provenance) = overlay::self_node_segments_sourced(
        left.edges
            .iter()
            .map(|edge| edge.segment)
            .chain(right.edges.iter().map(|edge| edge.segment)),
    )?;
    if atomic.len()
        == left
            .edges
            .len()
            .checked_add(right.edges.len())
            .ok_or(ResourceError::SizeOverflow)?
        && provenance.iter().all(|owners| owners.len() == 1)
    {
        let mut edges = Vec::new();
        try_reserve_vec(&mut edges, atomic.len())?;
        for (segment, owners) in atomic.into_iter().zip(provenance.iter()) {
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
            try_push_vec(&mut edges, (segment, label))?;
        }
        let hints = HashMap::with_hasher(crate::collections::collection_hash_state()?.clone());
        return Ok((edges, hints));
    }
    let mut by_segment = crate::collections::try_hash_map_with_capacity(atomic.len())?;
    let hint_capacity = atomic
        .len()
        .checked_mul(2)
        .ok_or(ResourceError::SizeOverflow)?;
    let mut hints = crate::collections::try_hash_map_with_capacity(hint_capacity)?;
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
            merge_coincident_piece(slot, location);
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
        // Two atomic pieces with the same ends are one piece. That happens
        // when one graph already split an edge at a crossing during its own
        // noding and the other's unsplit copy is now cut at the same computed
        // point: overwriting here dropped the first graph's ownership of it,
        // and a shared segment read as belonging to one side only.
        match by_segment.entry(crate::geometry::undirected_segment_edge_key(segment)) {
            std::collections::hash_map::Entry::Occupied(mut entry) => {
                let (_, existing): &mut (Segment, EdgeLabel) = entry.get_mut();
                for (slot, location) in [
                    (&mut existing.left, label.left),
                    (&mut existing.right, label.right),
                ] {
                    if let Some(location) = location {
                        merge_coincident_piece(slot, location);
                    }
                }
            },
            std::collections::hash_map::Entry::Vacant(entry) => {
                entry.insert((segment, label));
            },
        }
    }
    let mut edges = Vec::new();
    try_reserve_vec(&mut edges, by_segment.len())?;
    edges.extend(by_segment.into_values());
    Ok((edges, hints))
}

const fn edge_location(edge: &topology::TopologyEdge) -> TopologyLocation {
    if edge.is_boundary {
        TopologyLocation::Boundary
    } else {
        TopologyLocation::Interior
    }
}

/// Merge the location of one graph's coincident piece copies. Copies of one
/// graph with different flags exist only in degenerate input — a zero-area
/// ring's forward and return edges — where the geometry has no interior and
/// the boundary flag is the one that means something. A node, by contrast,
/// merges through [`merge_location`], where an interior edge outranks a
/// boundary one.
const fn merge_coincident_piece(slot: &mut Option<TopologyLocation>, location: TopologyLocation) {
    *slot = Some(match (*slot, location) {
        (Some(TopologyLocation::Boundary), _) | (_, TopologyLocation::Boundary) => {
            TopologyLocation::Boundary
        },
        (Some(TopologyLocation::Interior), _) | (_, TopologyLocation::Interior) => {
            TopologyLocation::Interior
        },
        _ => TopologyLocation::Exterior,
    });
}

const fn merge_location(slot: &mut Option<TopologyLocation>, location: TopologyLocation) {
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
) -> Result<()> {
    let token = left
        .node_for(point)
        .map(NodeToken::Left)
        .or_else(|| right.node_for(point).map(NodeToken::Right));
    let Some(token) = token else {
        // Both graphs share a quotient, so one key decides transient equality.
        if !transient.insert(left.key(point)) {
            return Ok(());
        }
        try_push_vec(nodes, point)?;
        return Ok(());
    };
    if seen.insert(token) {
        owners.insert(left.key(point), token);
        try_push_vec(nodes, point)?;
    }
    Ok(())
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
    if owns_node {
        return graph
            .node_location(point)
            .expect("node provenance must resolve in its owning graph");
    }
    // A hint is edge provenance: this graph has an edge ending at the node
    // with that location. An area-boundary edge makes the node a boundary
    // node outright — a node the pairing computed sits within an ULP of that
    // edge, and re-locating the rounded point can land it just inside the
    // area, which read a boundary crossing as an interior touch and lost the
    // `BI` cell. A line edge says only that the node is on the line; whether
    // it is a mod-2 end is the node's business, so location decides there,
    // with the hint rescuing a rounded point that missed the line entirely.
    match (hint, graph.locate_point(point)) {
        (Some(TopologyLocation::Boundary), _) => TopologyLocation::Boundary,
        (Some(hint), TopologyLocation::Exterior) => hint,
        (_, location) => location,
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
mod tests;
