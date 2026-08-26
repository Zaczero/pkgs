use std::borrow::Borrow;

use crate::error::Result;
use crate::geometry::overlay::{
    OverlayOp, build_overlay_shape, dedup_points, overlay_declared_axes,
};
use crate::geometry::topology::TopologyLocation;
use crate::geometry::{
    AreaSign, Arrangement, CoordinateAxes, Coordinates as _, Dimension, GeometryErrorKind,
    GeometryKind, Point, Segment, Shape, TopologyGraph, XY, carry_overlay_ordinates,
    point_on_segment, same_point, segment_midpoint,
};

/// Evaluate one or more operands in one canonical overlay arrangement.
///
/// The source order is normalized before noding. This is not a reduction: all
/// operands participate in the same topology graph and every output bucket is
/// assembled once from that graph.
pub(crate) fn overlay_all<S: Borrow<Shape>>(
    geometries: &[S],
    geographic: bool,
    op: OverlayOp,
    strictness: crate::geometry::Strictness,
) -> Result<Shape> {
    if geometries.is_empty() {
        return Err(GeometryErrorKind::EmptyGeometrySequence {
            operation: op.name(),
        }
        .into());
    }

    let mut sources: Vec<Shape> = geometries
        .iter()
        .map(|geometry| {
            let shape = geometry.borrow();
            if geographic && shape.crosses_antimeridian() {
                crate::geometry::topology_split(shape).normalize()
            } else {
                shape.normalize()
            }
        })
        .collect();

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
    if geometries.len() == 1
        && op == OverlayOp::Intersection
        && !(geographic && geometries[0].borrow().crosses_antimeridian())
    {
        return Ok(geometries[0].borrow().clone());
    }
    let shape = assemble_overlay(&refs, op)?;
    let shape = if shape.is_empty() {
        empty_nary_overlay_shape(&refs, op)
    } else {
        // Normalize before carrying ordinates: canonical geometry ordering is
        // XY-defined, while ordinate restoration is a separate provenance pass.
        shape.normalize()
    };
    carry_overlay_ordinates(shape, &refs, op, strictness.is_strict())
}

fn assemble_overlay(inputs: &[&Shape], op: OverlayOp) -> Result<Shape> {
    let graph = TopologyGraph::cartesian_many(inputs);
    let source_graphs: Vec<TopologyGraph> = inputs
        .iter()
        .map(|shape| TopologyGraph::cartesian(shape))
        .collect();
    let polygons = overlay_polygons(&graph, &source_graphs, op);
    let lines = overlay_lines(&graph, &source_graphs, &polygons, op);
    let points = overlay_points(&graph, &source_graphs, &polygons, &lines, op);
    Ok(build_overlay_shape(points, lines, polygons))
}

fn overlay_polygons(
    graph: &TopologyGraph,
    sources: &[TopologyGraph],
    op: OverlayOp,
) -> Vec<crate::geometry::Polygon> {
    if op == OverlayOp::Union
        && graph.source_points.is_empty()
        && !graph
            .edges
            .iter()
            .any(|edge| graph.edge_has_line_source(edge))
    {
        let loops = source_polygon_loops(sources);
        if !loops.is_empty() {
            return crate::geometry::winding_region(&loops, |winding| winding >= 1)
                .into_iter()
                .map(canonicalize_polygon)
                .collect();
        }
    }
    let segments: Vec<Segment> = graph
        .edges
        .iter()
        .filter(|edge| graph.edge_has_area_source(edge))
        .map(|edge| edge.segment)
        .collect();
    if segments.is_empty() {
        return Vec::new();
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
                    face_area_side(&face.ring, graph, source_row as u32)
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
        .map(canonicalize_polygon)
        .collect()
}

fn source_polygon_loops(sources: &[TopologyGraph]) -> Vec<(Vec<f64>, Vec<f64>)> {
    sources
        .iter()
        .flat_map(|source| source.area_support.iter())
        .flat_map(|support| support.source.rings())
        .filter_map(|ring| {
            let mut points: Vec<Point> = ring.iter_coords().collect();
            if points.len() > 1 && same_point(points[0], *points.last().expect("non-empty ring")) {
                points.pop();
            }
            (points.len() >= 3).then(|| points.into_iter().map(|point| (point.x, point.y)).unzip())
        })
        .collect()
}

fn face_area_side(ring: &[XY], graph: &TopologyGraph, source_row: u32) -> Option<bool> {
    ring.array_windows::<2>().find_map(|[start, end]| {
        graph.edges.iter().find_map(|edge| {
            let same_edge = (same_point(edge.segment.start, *start)
                && same_point(edge.segment.end, *end))
                || (same_point(edge.segment.start, *end) && same_point(edge.segment.end, *start));
            same_edge.then(|| graph.area_source_side_for_row(edge, *start, *end, source_row))?
        })
    })
}

fn canonicalize_polygon(polygon: crate::geometry::Polygon) -> crate::geometry::Polygon {
    let shell = crate::geometry::Ring::from_trusted_closed(crate::geometry::CoordSeq::from(
        drop_collinear_vertices(polygon.shell.coords()),
    ));
    let holes = polygon
        .holes
        .iter()
        .map(|hole| {
            crate::geometry::Ring::from_trusted_closed(crate::geometry::CoordSeq::from(
                drop_collinear_vertices(hole.coords()),
            ))
        })
        .collect();
    crate::geometry::Polygon::new(shell, holes)
}

fn drop_collinear_vertices(coords: &crate::geometry::CoordSeq) -> Vec<Point> {
    let mut vertices: Vec<Point> = coords.iter_coords().collect();
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
    polygons: &[crate::geometry::Polygon],
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
        let result_covers = polygons
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
    polygons: &[crate::geometry::Polygon],
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
        keep && !polygons.iter().any(|polygon| polygon.covers_point(*point))
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
