use Dimension::{Curve as DimCurve, Point as DimPoint, Surface as DimSurface};
use ahash::HashSetExt as _;

use crate::geometry::relate::{
    De9im, LinealOperand, Loc, PairEdgeLabel, PuntalOperand, RelateTopology, areal_relate_data,
    areal_relate_pattern_shapes, areal_relate_shapes, empty_relate, line_is_collapsed,
    lineal_relate_shapes, merge_topo_loc, mixed_relate_data, mixed_relate_shapes,
    multiline_is_collapsed, polygon_has_nondegenerate_area, polygon_parts, puntal_relate,
};
use crate::geometry::{
    CoordSeq, Coordinates as _, Dimension, HashMap, HashMapExt as _, HashSet, Point, PointKey,
    RUN_NODING_MIN, Segment, Shape, ShapeData, for_each_candidate_pair, overlay, point_on_segment,
    relate_ng, same_point, segment_envelopes_disjoint, shared_segment_part, single_chain,
    undirected_segment_edge_key,
};

pub(crate) fn native_relate_shapes(left: &Shape, right: &Shape) -> De9im {
    if left.is_empty() || right.is_empty() {
        return empty_relate(left, right);
    }
    if let Some(matrix) =
        areal_relate_shapes(left, right).or_else(|| lineal_relate_shapes(left, right))
    {
        return matrix;
    }
    if let Some(mut matrix) = mixed_relate_shapes(left, right) {
        if let Some(degenerate) = degenerate_polygonal_lineal_overlap(left, right) {
            matrix.set_at_least(Loc::Interior, Loc::Interior, DimCurve);
            if degenerate.two_point_collapse {
                if degenerate.polygonal_is_left {
                    matrix.clear(Loc::Boundary, Loc::Interior);
                } else {
                    matrix.clear(Loc::Interior, Loc::Boundary);
                }
            }
        }
        return matrix;
    }
    if requires_mod2_topology(left, right) {
        let left_topology = RelateTopology::build(left);
        let right_topology = RelateTopology::build(right);
        return mod2_relate(&left_topology, &right_topology);
    }
    if let Some(puntal) = PuntalOperand::from_shape(left) {
        return puntal_relate(&puntal, right);
    }
    if let Some(puntal) = PuntalOperand::from_shape(right) {
        return puntal_relate(&puntal, left).transpose();
    }
    mod2_relate(&RelateTopology::build(left), &RelateTopology::build(right))
}

pub(crate) fn native_relate_pattern_shapes(
    left: &Shape,
    right: &Shape,
    pattern: relate_ng::CompiledPattern<'_>,
) -> bool {
    if let Some(matches) = areal_relate_pattern_shapes(left, right, pattern) {
        return matches;
    }
    pattern.matches(native_relate_shapes(left, right))
}

pub(crate) fn native_relate_data(left: &ShapeData, right: &ShapeData) -> De9im {
    native_relate_data_for(
        left,
        right,
        crate::geometry::PointProbeUse::OneShot(0),
        crate::geometry::PointProbeUse::OneShot(0),
    )
}

pub(crate) fn native_relate_data_for(
    left: &ShapeData,
    right: &ShapeData,
    left_mode: crate::geometry::PointProbeUse,
    right_mode: crate::geometry::PointProbeUse,
) -> De9im {
    if left.shape().is_empty() || right.shape().is_empty() {
        return empty_relate(left.shape(), right.shape());
    }
    if let Some(matrix) = areal_relate_data(left, right, left_mode, right_mode)
        .or_else(|| lineal_relate_shapes(left.shape(), right.shape()))
    {
        return matrix;
    }
    if let Some(mut matrix) = mixed_relate_data(left, right, left_mode, right_mode) {
        if let Some(degenerate) = degenerate_polygonal_lineal_overlap(left.shape(), right.shape()) {
            matrix.set_at_least(Loc::Interior, Loc::Interior, DimCurve);
            if degenerate.two_point_collapse {
                if degenerate.polygonal_is_left {
                    matrix.clear(Loc::Boundary, Loc::Interior);
                } else {
                    matrix.clear(Loc::Interior, Loc::Boundary);
                }
            }
        }
        return matrix;
    }
    if requires_mod2_topology(left.shape(), right.shape()) {
        return mod2_relate(left.relate_topology(), right.relate_topology());
    }
    if let Some(puntal) = PuntalOperand::from_shape(left.shape()) {
        return puntal_relate(&puntal, right.shape());
    }
    if let Some(puntal) = PuntalOperand::from_shape(right.shape()) {
        return puntal_relate(&puntal, left.shape()).transpose();
    }
    native_relate_shapes(left.shape(), right.shape())
}

pub(crate) fn requires_mod2_topology(left: &Shape, right: &Shape) -> bool {
    shape_needs_mod2_topology(left)
        || shape_needs_mod2_topology(right)
        || (matches!(left, Shape::MultiLineString(_)) && PuntalOperand::from_shape(right).is_some())
        || (matches!(right, Shape::MultiLineString(_)) && PuntalOperand::from_shape(left).is_some())
}

pub(crate) fn shape_has_puntal_support(shape: &Shape) -> bool {
    match shape {
        Shape::Point(_) | Shape::MultiPoint(_) => true,
        Shape::GeometryCollection(parts) => parts.iter().any(shape_has_puntal_support),
        Shape::Empty(..)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Polygon(_)
        | Shape::MultiPolygon(_) => false,
    }
}

pub(crate) fn shape_has_polygonal_members(shape: &Shape) -> bool {
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

/// Cross-member lineal overlap scans above this segment count route through
/// the monotone-run candidate sweep instead of the nested pair loop.
const COLLECTION_LINEAL_OVERLAP_SWEEP_MIN: usize = 64;

pub(crate) fn collection_has_overlapping_lineal_members(shape: &Shape) -> bool {
    let Shape::GeometryCollection(parts) = shape else {
        return false;
    };
    let mut segments = Vec::new();
    let mut next_member = 0;
    for part in parts {
        collect_lineal_member_segments(part, &mut next_member, &mut segments);
    }
    if segments.len() >= COLLECTION_LINEAL_OVERLAP_SWEEP_MIN {
        let pool: Vec<Segment> = segments.iter().map(|&(_, segment)| segment).collect();
        let members: Vec<usize> = segments.iter().map(|&(member, _)| member).collect();
        let flow = for_each_candidate_pair::<RUN_NODING_MIN>(&pool, single_chain, |left, right| {
            if members[left] == members[right] {
                return std::ops::ControlFlow::Continue(());
            }
            let (left_segment, right_segment) = (pool[left], pool[right]);
            if segment_envelopes_disjoint(left_segment, right_segment) {
                return std::ops::ControlFlow::Continue(());
            }
            let Some((_, run)) = shared_segment_part(left_segment, right_segment) else {
                return std::ops::ControlFlow::Continue(());
            };
            if !same_point(run[0], run[1]) {
                return std::ops::ControlFlow::Break(());
            }
            std::ops::ControlFlow::Continue(())
        });
        return matches!(flow, std::ops::ControlFlow::Break(()));
    }
    for (index, &(left_member, left_segment)) in segments.iter().enumerate() {
        for &(right_member, right_segment) in &segments[index + 1..] {
            if left_member == right_member {
                continue;
            }
            if segment_envelopes_disjoint(left_segment, right_segment) {
                continue;
            }
            let Some((_, run)) = shared_segment_part(left_segment, right_segment) else {
                continue;
            };
            if !same_point(run[0], run[1]) {
                return true;
            }
        }
    }
    false
}

pub(crate) fn collect_lineal_member_segments(
    shape: &Shape,
    next_member: &mut usize,
    segments: &mut Vec<(usize, Segment)>,
) {
    match shape {
        Shape::LineString(line) => {
            let member = *next_member;
            *next_member += 1;
            push_lineal_member_segments(line, member, segments);
        },
        Shape::MultiLineString(lines) => {
            for line in lines {
                let member = *next_member;
                *next_member += 1;
                push_lineal_member_segments(line, member, segments);
            }
        },
        Shape::GeometryCollection(parts) => {
            for part in parts {
                collect_lineal_member_segments(part, next_member, segments);
            }
        },
        Shape::Empty(..)
        | Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::Polygon(_)
        | Shape::MultiPolygon(_) => {},
    }
}

pub(crate) fn push_lineal_member_segments(
    line: &CoordSeq,
    member: usize,
    segments: &mut Vec<(usize, Segment)>,
) {
    for [start, end] in line.segment_pairs() {
        if !same_point(start, end) {
            segments.push((member, Segment {
                start: start.xy(),
                end: end.xy(),
            }));
        }
    }
}

pub(crate) struct DegeneratePolygonalLineal {
    pub(crate) polygonal_is_left: bool,
    pub(crate) two_point_collapse: bool,
}

pub(crate) fn degenerate_polygonal_lineal_overlap(
    left: &Shape,
    right: &Shape,
) -> Option<DegeneratePolygonalLineal> {
    let (polygonal, lineal) =
        if polygon_parts(left).is_some() && LinealOperand::from_shape(right).is_some() {
            (left, right)
        } else if polygon_parts(right).is_some() && LinealOperand::from_shape(left).is_some() {
            (right, left)
        } else {
            return None;
        };
    if !shape_needs_mod2_topology(polygonal) {
        return None;
    }
    let line = LinealOperand::from_shape(lineal)?;
    polygonal_line_support(polygonal)
        .iter()
        .any(|edge| {
            line.segments
                .iter()
                .any(|segment| shared_segment_part(*edge, *segment).is_some())
        })
        .then_some(DegeneratePolygonalLineal {
            polygonal_is_left: std::ptr::eq(polygonal, left),
            two_point_collapse: polygonal_unique_xy_count(polygonal) <= 2,
        })
}

pub(crate) fn polygonal_line_support(shape: &Shape) -> Vec<Segment> {
    let mut segments = Vec::new();
    if let Some(polygons) = polygon_parts(shape) {
        for polygon in polygons {
            for ring in polygon.rings() {
                for [start, end] in ring.segment_pairs() {
                    if !same_point(start, end) {
                        segments.push(Segment {
                            start: start.xy(),
                            end: end.xy(),
                        });
                    }
                }
            }
        }
    }
    segments
}

pub(crate) fn polygonal_unique_xy_count(shape: &Shape) -> usize {
    let mut keys = HashSet::new();
    if let Some(polygons) = polygon_parts(shape) {
        for polygon in polygons {
            for ring in polygon.rings() {
                for point in ring {
                    keys.insert(PointKey::new(point));
                }
            }
        }
    }
    keys.len()
}

pub(crate) fn shape_needs_mod2_topology(shape: &Shape) -> bool {
    match shape {
        // GeometryCollection always needs the topology engine (mixed-dimension
        // members, cross-member linework). MultiPolygon only when every part is
        // area-degenerate — a singleton (or multi) with real area shares the
        // Polygon areal/mixed lanes so a shell-collinear line is boundary, not
        // interior (DE-9IM parity with the equivalent Polygon).
        Shape::GeometryCollection(_) => true,
        Shape::LineString(line) => line_is_collapsed(line),
        Shape::MultiLineString(lines) => multiline_is_collapsed(lines),
        Shape::Polygon(polygon) => !polygon_has_nondegenerate_area(polygon),
        Shape::MultiPolygon(polygons) => {
            polygons.is_empty() || !polygons.iter().any(polygon_has_nondegenerate_area)
        },
        Shape::Empty(..) | Shape::Point(_) | Shape::MultiPoint(_) => false,
    }
}

/// The topology-backed pair engine for every finite shape pair.
pub(crate) fn mod2_relate(left: &RelateTopology, right: &RelateTopology) -> De9im {
    if left
        .bounds
        .zip(right.bounds)
        .is_some_and(|(left, right)| !left.intersects(right))
    {
        let mut matrix = De9im::empty_disjoint();
        add_disjoint_support(&mut matrix, left, right);
        suppress_lineal_overlap_boundary_artifacts(&mut matrix, left, right);
        return matrix;
    }

    let mut matrix = De9im::empty_disjoint();
    add_face_pass(&mut matrix, left, right);
    let (edges, node_hints) = collect_pair_edges(left, right);
    add_edge_pass(&mut matrix, left, right, &edges);
    let nodes = collect_pair_nodes(left, right, &edges);
    add_node_pass(&mut matrix, left, right, &nodes, &node_hints);
    add_shared_line_boundary_nodes(&mut matrix, left, right);
    add_overlapping_line_boundary_columns(&mut matrix, left, right);
    add_line_boundary_pass(&mut matrix, left, right);
    add_degenerate_declared_area_pass(&mut matrix, left, right);
    suppress_lineal_overlap_boundary_artifacts(&mut matrix, left, right);
    matrix
}

#[expect(
    clippy::iter_over_hash_type,
    reason = "each boundary visit only joins an idempotent DE-9IM lattice state, so iteration order is unobservable"
)]
pub(crate) fn add_overlapping_line_boundary_columns(
    matrix: &mut De9im,
    left: &RelateTopology,
    right: &RelateTopology,
) {
    if left.collection_has_overlapping_lineal_support && right.is_collection {
        for key in &right.line_boundary {
            let point = key.xy().point();
            if topology_has_line_support(left, point) {
                matrix.set_at_least(Loc::Interior, Loc::Boundary, DimPoint);
            }
        }
    }
    if right.collection_has_overlapping_lineal_support && left.is_collection {
        for key in &left.line_boundary {
            let point = key.xy().point();
            if topology_has_line_support(right, point) {
                matrix.set_at_least(Loc::Boundary, Loc::Interior, DimPoint);
            }
        }
    }
}

pub(crate) fn topology_has_line_support(topology: &RelateTopology, point: Point) -> bool {
    topology
        .edges
        .iter()
        .any(|edge| point_on_segment(point, edge.segment.start, edge.segment.end))
}

#[expect(
    clippy::iter_over_hash_type,
    reason = "each boundary visit only joins an idempotent DE-9IM lattice state, so iteration order is unobservable"
)]
pub(crate) fn add_shared_line_boundary_nodes(
    matrix: &mut De9im,
    left: &RelateTopology,
    right: &RelateTopology,
) {
    for key in &left.line_boundary {
        if !right.line_boundary.contains(key) {
            continue;
        }
        let point = key.xy().point();
        if !left.area_contains_point(point) && !right.area_contains_point(point) {
            matrix.set_at_least(Loc::Boundary, Loc::Boundary, DimPoint);
        }
    }
}

pub(crate) fn suppress_lineal_overlap_boundary_artifacts(
    matrix: &mut De9im,
    left: &RelateTopology,
    right: &RelateTopology,
) {
    if left.lineal_overlap_collection
        && !right.is_collection
        && (!matrix.is_dimension(Loc::Boundary, Loc::Interior, Dimension::Point)
            || right.area_polygons.is_empty())
    {
        matrix.clear(Loc::Boundary, Loc::Exterior);
    }
    if right.lineal_overlap_collection
        && !left.is_collection
        && (!matrix.is_dimension(Loc::Interior, Loc::Boundary, Dimension::Point)
            || left.area_polygons.is_empty())
    {
        matrix.clear(Loc::Exterior, Loc::Boundary);
    }
    if left.lineal_overlap_collection && !left.collection_has_puntal_support && right.is_collection
    {
        matrix.clear(Loc::Interior, Loc::Boundary);
    }
    if right.lineal_overlap_collection && !right.collection_has_puntal_support && left.is_collection
    {
        matrix.clear(Loc::Boundary, Loc::Interior);
    }
}

/// Per-node operand incidence derived from the mutual edge noding.
///
/// The noder places interior crossing points at rounded coordinates that an
/// exact collinearity predicate no longer accepts as lying on the operand's
/// own segment. The atomic edge pieces, however, carry exact provenance: a
/// node that is the shared endpoint of an atomic piece sourced from an
/// operand provably lies on that operand's linework. This hint records, per
/// node `PointKey`, the strongest ON-label of each operand's incident pieces,
/// so node/edge classification reads the noding structure rather than
/// re-testing a rounded coordinate against an exact segment.
#[derive(Default, Clone, Copy)]
pub(crate) struct NodeHint {
    pub(crate) left: Option<Loc>,
    pub(crate) right: Option<Loc>,
}

pub(crate) fn collect_pair_edges(
    left: &RelateTopology,
    right: &RelateTopology,
) -> (Vec<(Segment, PairEdgeLabel)>, HashMap<PointKey, NodeHint>) {
    let mut combined = Vec::with_capacity(left.edges.len() + right.edges.len());
    combined.extend(left.edges.iter().map(|edge| edge.segment));
    combined.extend(right.edges.iter().map(|edge| edge.segment));
    if combined.is_empty() {
        return (Vec::new(), HashMap::new());
    }
    let split = left.edges.len();
    let (atomic, sources) = overlay::self_node_segments_sourced(&combined);
    let mut by_key: HashMap<(PointKey, PointKey), (Segment, PairEdgeLabel)> =
        HashMap::with_capacity(atomic.len());
    let mut node_hints: HashMap<PointKey, NodeHint> = HashMap::with_capacity(atomic.len());
    for (piece, owners) in atomic.into_iter().zip(sources.iter()) {
        if same_point(piece.start, piece.end) {
            continue;
        }
        for &(source, _reversed) in owners {
            let source = source as usize;
            let is_left = source < split;
            let loc = if is_left {
                left.edges[source].on
            } else {
                right.edges[source - split].on
            };
            for endpoint in [piece.start, piece.end] {
                let hint = node_hints.entry(PointKey::new(endpoint)).or_default();
                let slot = if is_left {
                    &mut hint.left
                } else {
                    &mut hint.right
                };
                merge_topo_loc(slot, loc);
            }
            let entry = by_key.entry(undirected_segment_edge_key(piece)).or_insert((
                piece,
                PairEdgeLabel {
                    left: None,
                    right: None,
                },
            ));
            if is_left {
                merge_topo_loc(&mut entry.1.left, loc);
            } else {
                merge_topo_loc(&mut entry.1.right, loc);
            }
        }
    }
    (by_key.into_values().collect(), node_hints)
}

pub(crate) fn collect_pair_nodes(
    left: &RelateTopology,
    right: &RelateTopology,
    edges: &[(Segment, PairEdgeLabel)],
) -> Vec<Point> {
    let mut keys = HashSet::new();
    let mut nodes = Vec::new();
    let mut push = |point: Point| {
        if keys.insert(PointKey::new(point)) {
            nodes.push(point.to_xy());
        }
    };
    for point in left.points.iter().chain(right.points.iter()) {
        push(point.point);
    }
    for key in left.line_boundary.iter().chain(right.line_boundary.iter()) {
        push(key.xy().point());
    }
    for key in left.area_boundary.iter().chain(right.area_boundary.iter()) {
        push(key.xy().point());
    }
    for left_point in &left.points {
        if left_point.on != Loc::Boundary {
            continue;
        }
        for right_point in &right.points {
            if right_point.on == Loc::Boundary && same_point(left_point.point, right_point.point) {
                push(left_point.point);
            }
        }
    }
    for edge in left.edges.iter().chain(right.edges.iter()) {
        push(edge.segment.start.point());
        push(edge.segment.end.point());
    }
    for (edge, _) in edges {
        push(edge.start.point());
        push(edge.end.point());
    }
    nodes
}

pub(crate) fn add_face_pass(matrix: &mut De9im, left: &RelateTopology, right: &RelateTopology) {
    match (
        left.area_polygons.is_empty(),
        right.area_polygons.is_empty(),
    ) {
        (true, true) => {},
        (false, true) => matrix.set_at_least(Loc::Interior, Loc::Exterior, DimSurface),
        (true, false) => matrix.set_at_least(Loc::Exterior, Loc::Interior, DimSurface),
        (false, false) => {
            let built = overlay::build_areal_arrangement(&left.area_polygons, &right.area_polygons);
            for &winding in &built.windings {
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
                matrix.set_at_least(left_loc, right_loc, DimSurface);
            }
        },
    }
}

pub(crate) fn add_edge_pass(
    matrix: &mut De9im,
    left: &RelateTopology,
    right: &RelateTopology,
    edges: &[(Segment, PairEdgeLabel)],
) {
    for &(segment, label) in edges {
        let left_loc = label
            .left
            .or_else(|| left.topology_edge_label(segment))
            .unwrap_or_else(|| left.locate_edge_piece(segment));
        let right_loc = label
            .right
            .or_else(|| right.topology_edge_label(segment))
            .unwrap_or_else(|| right.locate_edge_piece(segment));
        matrix.set_at_least(left_loc, right_loc, DimCurve);
    }
}

pub(crate) fn add_node_pass(
    matrix: &mut De9im,
    left: &RelateTopology,
    right: &RelateTopology,
    nodes: &[Point],
    node_hints: &HashMap<PointKey, NodeHint>,
) {
    for &point in nodes {
        let hint = node_hints
            .get(&PointKey::new(point))
            .copied()
            .unwrap_or_default();
        let left_loc = left.locate_point_hinted(point, hint.left);
        let right_loc = right.locate_point_hinted(point, hint.right);
        let shared_boundary_point =
            left.has_boundary_point(point) && right.has_boundary_point(point);
        if shared_boundary_point {
            matrix.set_at_least(Loc::Boundary, Loc::Boundary, DimPoint);
        }
        if !shared_boundary_point
            && left.has_line_boundary_point(point)
            && right.has_line_boundary_point(point)
            && !left.area_contains_point(point)
            && !right.area_contains_point(point)
        {
            matrix.set_at_least(Loc::Boundary, Loc::Boundary, DimPoint);
        }
        if left.has_boundary_point(point)
            && left.has_line_boundary_point(point)
            && !right.has_boundary_point(point)
            && right_loc != Loc::Boundary
        {
            matrix.set_at_least(Loc::Boundary, right_loc, DimPoint);
        }
        if right.has_boundary_point(point)
            && right.has_line_boundary_point(point)
            && !left.has_boundary_point(point)
            && left_loc != Loc::Boundary
        {
            matrix.set_at_least(left_loc, Loc::Boundary, DimPoint);
        }
        let shared_boundary_interior = shared_boundary_point
            && matches!(
                (left_loc, right_loc),
                (Loc::Boundary, Loc::Interior) | (Loc::Interior, Loc::Boundary)
            );
        if !shared_boundary_interior && (left_loc != Loc::Exterior || right_loc != Loc::Exterior) {
            matrix.set_at_least(left_loc, right_loc, DimPoint);
        }
    }
}

#[expect(
    clippy::iter_over_hash_type,
    reason = "each boundary visit only joins an idempotent DE-9IM lattice state, so iteration order is unobservable"
)]
pub(crate) fn add_line_boundary_pass(
    matrix: &mut De9im,
    left: &RelateTopology,
    right: &RelateTopology,
) {
    for key in &left.line_boundary {
        let point = key.xy().point();
        if !left.has_boundary_point(point) {
            continue;
        }
        if right.has_boundary_point(point) {
            matrix.set_at_least(Loc::Boundary, Loc::Boundary, DimPoint);
            if right.area_contains_point(point) {
                matrix.set_at_least(Loc::Boundary, Loc::Interior, DimPoint);
            }
            continue;
        }
        let right_loc = right.locate_point(point);
        if right_loc != Loc::Exterior {
            matrix.set_at_least(Loc::Boundary, right_loc, DimPoint);
        }
    }
    for key in &right.line_boundary {
        let point = key.xy().point();
        if !right.has_boundary_point(point) {
            continue;
        }
        if left.has_boundary_point(point) {
            matrix.set_at_least(Loc::Boundary, Loc::Boundary, DimPoint);
            if left.area_contains_point(point) {
                matrix.set_at_least(Loc::Interior, Loc::Boundary, DimPoint);
            }
            continue;
        }
        let left_loc = left.locate_point(point);
        if left_loc != Loc::Exterior {
            matrix.set_at_least(left_loc, Loc::Boundary, DimPoint);
        }
    }
}

pub(crate) fn add_degenerate_declared_area_pass(
    matrix: &mut De9im,
    left: &RelateTopology,
    right: &RelateTopology,
) {
    if left.declared_dim == Some(DimSurface) && left.area_polygons.is_empty() {
        matrix.set_at_least(Loc::Interior, Loc::Exterior, DimSurface);
    }
    if right.declared_dim == Some(DimSurface) && right.area_polygons.is_empty() {
        matrix.set_at_least(Loc::Exterior, Loc::Interior, DimSurface);
    }
}

pub(crate) fn add_disjoint_support(
    matrix: &mut De9im,
    left: &RelateTopology,
    right: &RelateTopology,
) {
    if let Some(dim) = left.effective_dim {
        matrix.set_at_least(Loc::Interior, Loc::Exterior, dim);
    }
    if (!left.collection_has_overlapping_lineal_support || right.area_polygons.is_empty())
        && let Some(dim) = left.boundary_dim
    {
        matrix.set_at_least(Loc::Boundary, Loc::Exterior, dim);
    }
    if let Some(dim) = right.effective_dim {
        matrix.set_at_least(Loc::Exterior, Loc::Interior, dim);
    }
    if (!right.collection_has_overlapping_lineal_support || left.area_polygons.is_empty())
        && let Some(dim) = right.boundary_dim
    {
        matrix.set_at_least(Loc::Exterior, Loc::Boundary, dim);
    }
}
