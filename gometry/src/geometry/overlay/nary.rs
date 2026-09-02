use std::borrow::Borrow;
use std::ops::ControlFlow;

use crate::error::Result;
use crate::geometry::overlay::{
    OverlayOp, build_overlay_shape, dedup_points, overlay_declared_axes,
};
use crate::geometry::topology::TopologyLocation;
use crate::geometry::{
    AreaSign, Arrangement, Bounds, CoordinateAxes, Coordinates as _, Dimension, GeometryErrorKind,
    GeometryKind, HashMap, HashMapExt as _, Point, PointKey, Segment, Shape, TopologyDomain,
    TopologyGraph, XY, carry_overlay_ordinates, ordered_edge, point_on_segment, same_point,
    segment_midpoint,
};

/// Evaluate one or more operands in one canonical overlay arrangement.
///
/// The source order is normalized before noding. Unless disjoint bounds settle
/// the operation, all operands participate in the same topology graph and
/// every output bucket is assembled once from that graph.
pub(crate) fn overlay_all<S: Borrow<Shape>>(
    geometries: &[S],
    domain: TopologyDomain,
    op: OverlayOp,
    strictness: crate::geometry::Strictness,
) -> Result<Shape> {
    overlay_all_with_bounds_certificates(geometries, domain, op, strictness, true)
}

#[cfg(test)]
pub(super) fn overlay_all_without_bounds_certificates<S: Borrow<Shape>>(
    geometries: &[S],
    domain: TopologyDomain,
    op: OverlayOp,
    strictness: crate::geometry::Strictness,
) -> Result<Shape> {
    overlay_all_with_bounds_certificates(geometries, domain, op, strictness, false)
}

fn overlay_all_with_bounds_certificates<S: Borrow<Shape>>(
    geometries: &[S],
    domain: TopologyDomain,
    op: OverlayOp,
    strictness: crate::geometry::Strictness,
    bounds_certificates: bool,
) -> Result<Shape> {
    if geometries.is_empty() {
        return Err(GeometryErrorKind::EmptyGeometrySequence {
            operation: op.name(),
        }
        .into());
    }

    let bounds_facts = overlay_bounds_facts(geometries, domain, op);

    if bounds_certificates
        && geometries.len() > 1
        && op == OverlayOp::Intersection
        && bounds_facts.raw_coordinate_bounds_sound
        && intersection_bounds_empty(&bounds_facts.bounds)
    {
        return Ok(empty_nary_overlay_shape(
            &geometries
                .iter()
                .map(|geometry| geometry.borrow())
                .collect::<Vec<_>>(),
            op,
        ));
    }

    let mut sources: Vec<Shape> = geometries
        .iter()
        .map(|geometry| {
            let shape = geometry.borrow();
            if let TopologyDomain::Geographic(frame) = domain
                && crate::geometry::crosses_antimeridian_in_frame(shape, frame)
            {
                let normalized = shape.split_antimeridian_in_frame(frame)?.normalize();
                Ok(normalized)
            } else {
                let normalized = shape.normalize();
                Ok(normalized)
            }
        })
        .collect::<Result<_>>()?;

    // The left operand remains first for the only non-commutative operation.
    // Every other operation gets a canonical source order before graph
    // construction, so input permutation cannot affect noding provenance.
    if op == OverlayOp::Difference {
        let (left, right) = sources.split_at_mut(1);
        right.sort_unstable_by(crate::geometry::compare_shapes);
        let _ = left;
    } else {
        sources.sort_unstable_by(crate::geometry::compare_shapes);
    }

    let refs: Vec<&Shape> = sources.iter().collect();
    let input_crosses = match domain {
        TopologyDomain::Cartesian => false,
        TopologyDomain::Geographic(frame) => {
            crate::geometry::crosses_antimeridian_in_frame(geometries[0].borrow(), frame)
        },
    };
    if geometries.len() == 1 && op == OverlayOp::Intersection && !input_crosses {
        return Ok(geometries[0].borrow().clone());
    }
    let shape = if bounds_certificates
        && geometries.len() > 1
        && bounds_facts.raw_coordinate_bounds_sound
        && disjoint_bounds_certify(&bounds_facts.bounds, op)
    {
        assemble_disjoint_overlay(&refs, domain, op, bounds_facts.raw_polygon_loops_eligible)?
    } else {
        assemble_overlay(&refs, domain, op, bounds_facts.raw_polygon_loops_eligible)?
    };
    let shape = if shape.is_empty() {
        empty_nary_overlay_shape(&refs, op)
    } else {
        // Normalize before carrying ordinates: canonical geometry ordering is
        // XY-defined, while ordinate restoration is a separate provenance pass.
        shape.normalize()
    };
    carry_overlay_ordinates(shape, &refs, op, strictness.is_strict())
}

struct OverlayBoundsFacts {
    bounds: Vec<Option<Bounds>>,
    raw_coordinate_bounds_sound: bool,
    raw_polygon_loops_eligible: bool,
}

fn overlay_bounds_facts<S: Borrow<Shape>>(
    inputs: &[S],
    domain: TopologyDomain,
    op: OverlayOp,
) -> OverlayBoundsFacts {
    let bounds = inputs
        .iter()
        .map(|input| input.borrow().bounds())
        .collect::<Vec<_>>();
    let raw_coordinate_bounds_sound = match domain {
        TopologyDomain::Cartesian => true,
        TopologyDomain::Geographic(frame) => inputs.iter().zip(&bounds).all(|(input, bounds)| {
            bounds.is_some_and(|bounds| {
                crate::geometry::raw_coordinate_bounds_are_sound_in_frame(
                    Some(input.borrow()),
                    bounds,
                    frame,
                )
            })
        }),
    };
    let raw_polygon_loops_eligible = op == OverlayOp::Union
        && raw_coordinate_bounds_sound
        && !inputs
            .iter()
            .any(|input| has_nonempty_point_or_line_support(input.borrow()));
    OverlayBoundsFacts {
        bounds,
        raw_coordinate_bounds_sound,
        raw_polygon_loops_eligible,
    }
}

fn disjoint_bounds_certify(bounds: &[Option<Bounds>], op: OverlayOp) -> bool {
    match op {
        OverlayOp::Difference => bounds
            .first()
            .and_then(|bounds| *bounds)
            .is_some_and(|left| {
                bounds[1..]
                    .iter()
                    .filter_map(|right| *right)
                    .all(|right| bounds_strictly_disjoint(left, right))
            }),
        OverlayOp::Union | OverlayOp::SymmetricDifference => {
            crate::geometry::for_each_indexed_overlapping_bounds_pair(bounds, |_, _| {
                ControlFlow::Break(())
            })
            .is_continue()
        },
        OverlayOp::Intersection => false,
    }
}

fn bounds_strictly_disjoint(left: Bounds, right: Bounds) -> bool {
    left.maxx() < right.minx()
        || right.maxx() < left.minx()
        || left.maxy() < right.miny()
        || right.maxy() < left.miny()
}

fn intersection_bounds_empty(bounds: &[Option<Bounds>]) -> bool {
    let Some((first, rest)) = bounds.split_first() else {
        return false;
    };
    let Some(mut bounds) = *first else {
        return true;
    };
    for next in rest {
        let Some(next) = *next else {
            return true;
        };
        if !bounds.intersects(next) {
            return true;
        }
        bounds = Bounds::new_raw(
            bounds.minx().max(next.minx()),
            bounds.miny().max(next.miny()),
            bounds.maxx().min(next.maxx()),
            bounds.maxy().min(next.maxy()),
        );
    }
    false
}

fn assemble_overlay(
    inputs: &[&Shape],
    domain: TopologyDomain,
    op: OverlayOp,
    raw_polygon_loops_eligible: bool,
) -> Result<Shape> {
    let graph = match domain {
        TopologyDomain::Cartesian => TopologyGraph::cartesian_many(inputs),
        TopologyDomain::Geographic(frame) => TopologyGraph::geographic_many(inputs, frame),
    };
    let source_graphs: Vec<TopologyGraph> = inputs
        .iter()
        .map(|shape| match domain {
            TopologyDomain::Cartesian => TopologyGraph::cartesian(shape),
            TopologyDomain::Geographic(frame) => TopologyGraph::geographic(shape, frame),
        })
        .collect();
    let polygons = overlay_polygons(&graph, &source_graphs, op, raw_polygon_loops_eligible);
    // Overlay edges and candidate points are numerous for high-resolution
    // boundaries; prepare the retained polygons once instead of rescanning
    // every output ring for each candidate.
    let polygon_indexes: Vec<crate::geometry::PolygonPointIndex> = polygons
        .iter()
        .map(crate::geometry::PolygonPointIndex::build)
        .collect();
    let lines = overlay_lines(&graph, &source_graphs, &polygon_indexes, op);
    let points = overlay_points(&graph, &source_graphs, &polygon_indexes, &lines, op);
    Ok(build_overlay_shape(points, lines, polygons))
}

/// Assemble disconnected sources independently, then use the ordinary overlay
/// sink so result narrowing and mixed-dimension ordering stay graph-identical.
fn assemble_disjoint_overlay(
    inputs: &[&Shape],
    domain: TopologyDomain,
    op: OverlayOp,
    raw_polygon_loops_eligible: bool,
) -> Result<Shape> {
    let sources = if op == OverlayOp::Difference {
        &inputs[..1]
    } else {
        inputs
    };
    let mut points = Vec::new();
    let mut lines = Vec::new();
    let mut polygons = Vec::new();
    for source in sources {
        let shape = assemble_overlay(&[*source], domain, op, raw_polygon_loops_eligible)?;
        append_overlay_parts(shape, &mut points, &mut lines, &mut polygons);
    }
    Ok(build_overlay_shape(points, lines, polygons))
}

fn append_overlay_parts(
    shape: Shape,
    points: &mut Vec<Point>,
    lines: &mut Vec<crate::geometry::CoordSeq>,
    polygons: &mut Vec<crate::geometry::Polygon>,
) {
    match shape {
        Shape::Point(point) => points.push(point),
        Shape::MultiPoint(multi) => points.extend(multi.iter()),
        Shape::LineString(line) => lines.push(line.as_coords().clone()),
        Shape::MultiLineString(multi) => {
            lines.extend(multi.iter().map(|line| line.as_coords().clone()));
        },
        Shape::Polygon(polygon) => polygons.push(polygon),
        Shape::MultiPolygon(multi) => polygons.extend(multi.iter().cloned()),
        Shape::GeometryCollection(geometries) => {
            for geometry in &geometries {
                append_overlay_parts(geometry.clone(), points, lines, polygons);
            }
        },
        Shape::Empty(..) => {},
    }
}

fn overlay_polygons(
    graph: &TopologyGraph,
    sources: &[TopologyGraph],
    op: OverlayOp,
    raw_polygon_loops_eligible: bool,
) -> Vec<crate::geometry::Polygon> {
    if raw_polygon_loops_eligible {
        let loops = source_polygon_loops(sources);
        if !loops.is_empty() {
            return crate::geometry::winding_region(&loops, |winding| winding >= 1)
                .into_iter()
                .map(|polygon| canonicalize_polygon(polygon, graph.domain))
                .collect();
        }
    }
    let area_edges: Vec<(usize, Segment)> = graph
        .edges
        .iter()
        .enumerate()
        .filter(|(_, edge)| graph.edge_has_area_source(edge))
        .map(|(index, edge)| (index, edge.segment))
        .collect();
    if area_edges.is_empty() {
        return Vec::new();
    }
    let arranged_edges = match graph.domain {
        TopologyDomain::Cartesian => area_edges.clone(),
        TopologyDomain::Geographic(frame)
            if !graph
                .nodes
                .iter()
                .any(|node| node.point.y.abs() == frame.pole()) =>
        {
            unwrap_geographic_area_edges(graph, frame, &area_edges)
        },
        TopologyDomain::Geographic(_) => area_edges.clone(),
    };
    let segments: Vec<Segment> = arranged_edges.iter().map(|(_, segment)| *segment).collect();
    let mut edge_by_endpoints: HashMap<(PointKey, PointKey), usize> =
        HashMap::with_capacity(arranged_edges.len());
    for &(edge_index, segment) in &arranged_edges {
        edge_by_endpoints
            .entry(ordered_edge(
                PointKey::new(segment.start),
                PointKey::new(segment.end),
            ))
            .or_insert(edge_index);
    }

    // The joint graph already split every area edge at every source contact.
    // The arrangement below is therefore the one and only face assembly for
    // both binary and aggregate overlays.
    let arrangement = Arrangement::new_with_rings(&segments);
    let selected: Vec<i32> = arrangement
        .faces
        .iter()
        .map(|face| {
            let Some(probe) = face_probe(face.ring.as_slice(), face.decision_area.sign()) else {
                return 0;
            };
            let area_membership: Vec<bool> = sources
                .iter()
                .enumerate()
                .map(|(source_row, source)| {
                    face_area_side(&face.ring, graph, source_row as u32, &edge_by_endpoints)
                        .unwrap_or_else(|| source.contains_area_interior(probe))
                })
                .collect();
            if keep_area_face(&area_membership, op) {
                1
            } else {
                0
            }
        })
        .collect();
    crate::geometry::assemble_region_polygons(arrangement.region_rings(&selected, |keep| keep != 0))
        .into_iter()
        .map(|polygon| canonicalize_polygon(polygon, graph.domain))
        .collect()
}

fn has_nonempty_point_or_line_support(shape: &Shape) -> bool {
    match shape {
        Shape::Point(_) | Shape::LineString(_) => !shape.is_empty(),
        Shape::MultiPoint(points) => !points.is_empty(),
        Shape::MultiLineString(lines) => lines.iter().any(|line| !line.is_empty()),
        Shape::GeometryCollection(geometries) => {
            geometries.iter().any(has_nonempty_point_or_line_support)
        },
        Shape::Polygon(_) | Shape::MultiPolygon(_) | Shape::Empty(..) => false,
    }
}

fn unwrap_geographic_area_edges(
    graph: &TopologyGraph,
    frame: crate::geographic::GeographicFrame,
    edges: &[(usize, Segment)],
) -> Vec<(usize, Segment)> {
    let axis = frame.longitude();
    let mut adjacency = vec![Vec::<(usize, f64)>::new(); graph.nodes.len()];
    for &(_, segment) in edges {
        let start = graph
            .node_for(segment.start)
            .expect("area edge start has a graph node")
            .index();
        let end = graph
            .node_for(segment.end)
            .expect("area edge end has a graph node")
            .index();
        let mut delta = axis
            .shortest_edge(segment.start.x, segment.end.x)
            .expect("validated geographic longitude")
            .delta(axis);
        if delta == 0.0
            && let Some(full_turn) = axis.full_turn_delta(segment.start.x, segment.end.x)
        {
            delta = full_turn;
        }
        adjacency[start].push((end, delta));
        adjacency[end].push((start, -delta));
    }

    let mut longitudes = vec![None; graph.nodes.len()];
    for seed in 0..graph.nodes.len() {
        if longitudes[seed].is_some() {
            continue;
        }
        longitudes[seed] = Some(graph.nodes[seed].point.x);
        let mut stack = vec![seed];
        while let Some(node) = stack.pop() {
            let longitude = longitudes[node].expect("assigned chart longitude");
            for &(neighbor, delta) in &adjacency[node] {
                if longitudes[neighbor].is_none() {
                    longitudes[neighbor] = Some(longitude + delta);
                    stack.push(neighbor);
                }
            }
        }
    }

    edges
        .iter()
        .map(|&(index, segment)| {
            let start = graph
                .node_for(segment.start)
                .expect("area edge start has a graph node")
                .index();
            let end = graph
                .node_for(segment.end)
                .expect("area edge end has a graph node")
                .index();
            (index, Segment {
                start: XY::new(
                    longitudes[start].expect("area edge start has a chart longitude"),
                    segment.start.y,
                ),
                end: XY::new(
                    longitudes[end].expect("area edge end has a chart longitude"),
                    segment.end.y,
                ),
            })
        })
        .collect()
}

fn source_polygon_loops(sources: &[TopologyGraph]) -> Vec<(Vec<f64>, Vec<f64>)> {
    sources
        .iter()
        .flat_map(|source| source.area_support.iter())
        .flat_map(|support| support.source().rings())
        .filter_map(|ring| {
            let mut points: Vec<Point> = ring.iter_coords().collect();
            if points.len() > 1 && same_point(points[0], *points.last().expect("non-empty ring")) {
                points.pop();
            }
            (points.len() >= 3).then(|| points.into_iter().map(|point| (point.x, point.y)).unzip())
        })
        .collect()
}

fn face_area_side(
    ring: &[XY],
    graph: &TopologyGraph,
    source_row: u32,
    edge_by_endpoints: &HashMap<(PointKey, PointKey), usize>,
) -> Option<bool> {
    ring.array_windows::<2>().find_map(|[start, end]| {
        let key = ordered_edge(PointKey::new(*start), PointKey::new(*end));
        let edge_index = *edge_by_endpoints.get(&key)?;
        graph.area_source_side_for_row(&graph.edges[edge_index], *start, *end, source_row)
    })
}

fn canonicalize_polygon(
    polygon: crate::geometry::Polygon,
    domain: TopologyDomain,
) -> crate::geometry::Polygon {
    let canonicalize_point = |point: Point| match domain {
        TopologyDomain::Cartesian => point,
        TopologyDomain::Geographic(frame) => Point::new_raw_xy(
            frame
                .longitude()
                .canonicalize(point.x)
                .expect("derived geographic longitude is finite")
                .get(),
            point.y,
        ),
    };
    let shell = crate::geometry::Ring::from_closed(crate::geometry::CoordSeq::from(
        drop_collinear_vertices(
            polygon
                .shell
                .coords()
                .iter_coords()
                .map(canonicalize_point)
                .collect(),
        ),
    ));
    let holes = polygon
        .holes
        .iter()
        .map(|hole| {
            crate::geometry::Ring::from_closed(crate::geometry::CoordSeq::from(
                drop_collinear_vertices(
                    hole.coords()
                        .iter_coords()
                        .map(canonicalize_point)
                        .collect(),
                ),
            ))
        })
        .collect();
    crate::geometry::Polygon::new(shell, holes)
}

fn drop_collinear_vertices(mut vertices: Vec<Point>) -> Vec<Point> {
    if vertices.len() <= 4 {
        return vertices;
    }
    vertices.pop();
    loop {
        if vertices.len() <= 3 {
            break;
        }
        let Some(index) = (0..vertices.len()).find(|&index| {
            let previous = vertices[(index + vertices.len() - 1) % vertices.len()];
            let current = vertices[index];
            let next = vertices[(index + 1) % vertices.len()];
            point_on_segment(current, previous, next)
        }) else {
            break;
        };
        vertices.remove(index);
    }
    vertices.push(vertices[0]);
    vertices
}

fn face_probe(ring: &[XY], sign: AreaSign) -> Option<XY> {
    if ring.len() < 4 || sign == AreaSign::Zero {
        return None;
    }
    // Probe just to the left of an edge, which is the side represented by the
    // face walk. Sampling the side rather than the ring centroid keeps nested
    // disconnected cycles in distinct arrangement faces.
    let start = ring[0];
    let end = ring[1];
    let coordinate_scale = start
        .x
        .abs()
        .max(start.y.abs())
        .max(end.x.abs())
        .max(end.y.abs());
    if !coordinate_scale.is_finite() || coordinate_scale == 0.0 {
        return None;
    }
    let dx = end.x / coordinate_scale - start.x / coordinate_scale;
    let dy = end.y / coordinate_scale - start.y / coordinate_scale;
    let length = dx.hypot(dy);
    if !length.is_finite() || length == 0.0 {
        return None;
    }
    let midpoint = XY::new(f64::midpoint(start.x, end.x), f64::midpoint(start.y, end.y));
    let normal_x = -dy / length;
    let normal_y = dx / length;
    let mut offset = (coordinate_scale * 1e-7 * length).max(f64::from_bits(1));
    loop {
        let probe = XY::new(
            midpoint.x + normal_x * offset,
            midpoint.y + normal_y * offset,
        );
        if probe.x.is_finite() && probe.y.is_finite() {
            return Some(probe);
        }
        offset *= 0.5;
        if offset == 0.0 {
            return None;
        }
    }
}

fn keep_area_face(membership: &[bool], op: OverlayOp) -> bool {
    match op {
        OverlayOp::Union => membership.iter().any(|&inside| inside),
        OverlayOp::Intersection => {
            !membership.is_empty() && membership.iter().all(|&inside| inside)
        },
        OverlayOp::Difference => {
            membership.first().copied().unwrap_or(false)
                && membership[1..].iter().all(|&inside| !inside)
        },
        OverlayOp::SymmetricDifference => {
            membership.iter().filter(|&&inside| inside).count() % 2 == 1
        },
    }
}

fn overlay_lines(
    graph: &TopologyGraph,
    sources: &[TopologyGraph],
    polygon_indexes: &[crate::geometry::PolygonPointIndex],
    op: OverlayOp,
) -> Vec<crate::geometry::CoordSeq> {
    let mut lines = Vec::new();
    for edge in &graph.edges {
        let line_source = graph.edge_has_line_source(edge);
        if !line_source && op != OverlayOp::Intersection {
            continue;
        }
        let midpoint = segment_midpoint(edge.segment);
        let covered: Vec<bool> = sources
            .iter()
            .enumerate()
            .map(|(source_row, source)| {
                source.locate_edge_piece(edge.segment) != TopologyLocation::Exterior
                    || graph.edge_has_line_source_for_row(edge, source_row as u32)
            })
            .collect();
        let line_membership: Vec<bool> = sources
            .iter()
            .enumerate()
            .map(|(source_row, source)| {
                source.has_line_support(midpoint.xy())
                    || graph.edge_has_line_source_for_row(edge, source_row as u32)
            })
            .collect();
        let result_covers = polygon_indexes
            .iter()
            .any(|polygon| polygon.covers_point(midpoint));
        let keep = match op {
            OverlayOp::Intersection => covered.iter().all(|&present| present) && !result_covers,
            OverlayOp::Union => line_membership.iter().any(|&present| present) && !result_covers,
            OverlayOp::Difference => {
                line_membership.first().copied().unwrap_or(false)
                    && covered[1..].iter().all(|&present| !present)
                    && !result_covers
            },
            OverlayOp::SymmetricDifference => {
                line_membership.iter().filter(|&&present| present).count() % 2 == 1
                    && !result_covers
            },
        };
        if keep {
            lines.push(crate::geometry::CoordSeq::from(vec![
                edge.segment.start,
                edge.segment.end,
            ]));
        }
    }
    lines
}

fn overlay_points(
    graph: &TopologyGraph,
    sources: &[TopologyGraph],
    polygon_indexes: &[crate::geometry::PolygonPointIndex],
    lines: &[crate::geometry::CoordSeq],
    op: OverlayOp,
) -> Vec<Point> {
    let mut candidates: Vec<Point> = graph
        .source_points
        .iter()
        .filter(|source| !source.collapsed_line)
        .map(|source| source.point.xy().point())
        .collect();
    if op == OverlayOp::Intersection {
        candidates.extend(graph.nodes.iter().map(|node| node.point.point()));
    }
    candidates.retain(|point| {
        let membership: Vec<bool> = sources
            .iter()
            .enumerate()
            .map(|(source_row, source)| {
                source.locate_point_for_overlay(point.xy()) != TopologyLocation::Exterior
                    || (op == OverlayOp::Intersection
                        && graph.has_line_source_at_node(point.xy(), source_row as u32))
            })
            .collect();
        let keep = match op {
            OverlayOp::Union => membership.iter().any(|&present| present),
            OverlayOp::Intersection => {
                !membership.is_empty() && membership.iter().all(|&present| present)
            },
            OverlayOp::Difference => {
                membership.first().copied().unwrap_or(false)
                    && membership[1..].iter().all(|&present| !present)
            },
            OverlayOp::SymmetricDifference => {
                membership.iter().filter(|&&present| present).count() % 2 == 1
            },
        };
        keep && !polygon_indexes
            .iter()
            .any(|polygon| polygon.covers_point(*point))
            && !lines.iter().any(|line| {
                line.segment_pairs()
                    .any(|[start, end]| point_on_segment(*point, start, end))
            })
    });
    dedup_points(&mut candidates);
    candidates
}

pub(crate) fn empty_nary_overlay_shape(inputs: &[&Shape], op: OverlayOp) -> Shape {
    debug_assert!(!inputs.is_empty());
    let dimension = match op {
        OverlayOp::Intersection => inputs
            .iter()
            .map(|shape| shape.topological_dimension())
            .min()
            .expect("non-empty inputs"),
        OverlayOp::Difference => inputs[0].topological_dimension(),
        OverlayOp::Union | OverlayOp::SymmetricDifference => inputs
            .iter()
            .map(|shape| shape.topological_dimension())
            .max()
            .expect("non-empty inputs"),
    };
    empty_shape_for_dimension_with_axes(dimension, overlay_declared_axes(inputs, op))
}

pub(crate) fn empty_shape_for_dimension_with_axes(
    dimension: Dimension,
    axes: CoordinateAxes,
) -> Shape {
    match dimension {
        Dimension::Point => Shape::typed_empty(GeometryKind::Point, axes),
        Dimension::Curve => Shape::typed_empty(GeometryKind::LineString, axes),
        Dimension::Surface => Shape::typed_empty(GeometryKind::Polygon, axes),
    }
}
