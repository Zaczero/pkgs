use std::borrow::Borrow;
use std::ops::ControlFlow;

use ahash::HashSetExt as _;

use crate::error::Result;
use crate::geometry::overlay::{
    OverlayOp, Strictness, build_overlay_shape, build_overlay_shape_from_chains,
    dedup_points, merge_chain_edges, overlay_declared_axes,
};
use crate::geometry::topology::{SourceSupport, TopologyLocation};
use crate::geometry::{
    AreaSign, Arrangement, Bounds, CoordinateAxes, Coordinates as _, Dimension, GeometryErrorKind,
    GeometryKind, HashMap, HashMapExt as _, HashSet, Point, PointBatchTester, PointKey, Segment,
    SegmentIndex, Shape, TopologyDomain, TopologyGraph, XY, carry_direct_points_ordinates,
    carry_direct_overlay_ordinates, carry_overlay_ordinates, carry_overlay_ordinates_from_graph,
    column_all_finite, ordered_edge,
    point_on_segment, same_point, segment_midpoint, shell_is_convex,
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
pub(super) fn overlay_all_line_oracle(
    geometries: &[&Shape],
    op: OverlayOp,
    strictness: crate::geometry::Strictness,
) -> Result<Shape> {
    if geometries.is_empty() {
        return Err(GeometryErrorKind::EmptyGeometrySequence {
            operation: op.name(),
        }
        .into());
    }
    let mut sources = geometries
        .iter()
        .map(|shape| shape.normalize())
        .collect::<Result<Vec<_>>>()?;
    if op == OverlayOp::Difference {
        let (left, right) = sources.split_at_mut(1);
        right.sort_unstable_by(crate::geometry::compare_shapes);
        let _ = left;
    } else {
        sources.sort_unstable_by(crate::geometry::compare_shapes);
    }
    let refs = sources.iter().collect::<Vec<_>>();
    let shape = assemble_overlay(&refs, TopologyDomain::Cartesian, op, false)?;
    let shape = if shape.is_empty() {
        empty_nary_overlay_shape(&refs, op)
    } else {
        shape.normalize()?
    };
    carry_overlay_ordinates(shape, &refs, op, strictness.is_strict())
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

#[cfg(test)]
pub(super) fn binary_intersection_without_convex_shortcut(
    left: &Shape,
    right: &Shape,
    strictness: crate::geometry::Strictness,
) -> Result<Shape> {
    let mut sources = vec![left.normalize()?, right.normalize()?];
    sources.sort_unstable_by(crate::geometry::compare_shapes);
    let refs: Vec<&Shape> = sources.iter().collect();
    let shape = assemble_overlay(
        &refs,
        TopologyDomain::Cartesian,
        OverlayOp::Intersection,
        false,
    )?;
    let shape = if shape.is_empty() {
        empty_nary_overlay_shape(&refs, OverlayOp::Intersection)
    } else {
        shape.normalize()?
    };
    carry_overlay_ordinates(
        shape,
        &refs,
        OverlayOp::Intersection,
        strictness.is_strict(),
    )
}

pub(super) fn overlay_all_with_bounds_certificates<S: Borrow<Shape>>(
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

    if domain == TopologyDomain::Cartesian
        && geometries.len() == 2
        && op == OverlayOp::Intersection
        && let Some(shape) = direct_point_intersection(geometries, strictness)?
    {
        return Ok(shape);
    }

    let mut sources: Vec<Shape> = geometries
        .iter()
        .map(|geometry| {
            let shape = geometry.borrow();
            if let TopologyDomain::Geographic(frame) = domain
                && crate::geometry::crosses_antimeridian_in_frame(shape, frame)
            {
                let normalized = shape.split_antimeridian_in_frame(frame)?.normalize()?;
                Ok(normalized)
            } else {
                let normalized = shape.normalize()?;
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
    let components = if bounds_certificates
        && geometries.len() > 1
        && bounds_facts.raw_coordinate_bounds_sound
    {
        match op {
            OverlayOp::Union | OverlayOp::SymmetricDifference => {
                let bounds = refs.iter().map(|shape| shape.bounds()).collect::<Vec<_>>();
                Some(bounds_components(&bounds)).filter(|components| components.len() > 1)
            },
            OverlayOp::Difference | OverlayOp::Intersection => disjoint_bounds_certify(&bounds_facts.bounds, op)
                .then(|| (0..refs.len()).map(|index| vec![index]).collect()),
        }
    } else {
        None
    };
    let disjoint_bounds = components.is_some();
    let convex_shape = if !disjoint_bounds
        && domain == TopologyDomain::Cartesian
        && geometries.len() == 2
        && op == OverlayOp::Intersection
    {
        convex_polygon_intersection(&refs)
    } else {
        None
    };
    let polygon_line = if !disjoint_bounds
        && domain == TopologyDomain::Cartesian
        && geometries.len() == 2
        && op == OverlayOp::Intersection
    {
        assemble_polygon_line_intersection(&refs)?
    } else {
        None
    };
    let line_only_eligible = !disjoint_bounds
        && domain == TopologyDomain::Cartesian
        && refs.iter().all(|input| nondegenerate_line_only(input));
    let (shape, line_graph) = if let Some(components) = components {
        (
            assemble_components_overlay(
                &refs,
                &components,
                domain,
                op,
                bounds_facts.raw_polygon_loops_eligible,
            )?,
            None,
        )
    } else if let Some(shape) = convex_shape {
        (shape, None)
    } else if let Some((shape, graph)) = polygon_line {
        (shape, Some(graph))
    } else if line_only_eligible {
        return finish_direct_overlay(assemble_line_only_overlay(&refs, op)?, &refs, op, strictness);
    } else {
        (
            assemble_overlay(&refs, domain, op, bounds_facts.raw_polygon_loops_eligible)?,
            None,
        )
    };
    let shape = if shape.is_empty() {
        empty_nary_overlay_shape(&refs, op)
    } else {
        // Normalize before carrying ordinates: canonical geometry ordering is
        // XY-defined, while ordinate restoration is a separate provenance pass.
        shape.normalize()?
    };
    if let Some(graph) = line_graph.as_ref() {
        carry_overlay_ordinates_from_graph(shape, &refs, op, strictness.is_strict(), graph)
    } else {
        carry_overlay_ordinates(shape, &refs, op, strictness.is_strict())
    }
}

/// The tail every graph-free lane shares: the empty-result kind, canonical
/// ordering, and Z/M carried straight from the input vertices and segments.
fn finish_direct_overlay(
    shape: Shape,
    inputs: &[&Shape],
    op: OverlayOp,
    strictness: Strictness,
) -> Result<Shape> {
    let shape = if shape.is_empty() {
        empty_nary_overlay_shape(inputs, op)
    } else {
        shape.normalize()?
    };
    carry_direct_overlay_ordinates(shape, inputs, op, strictness.is_strict())
}

#[derive(Clone, Copy)]
enum DirectPointCarrier<'a> {
    Line(&'a crate::geometry::LineSeq),
    MultiLine(&'a crate::geometry::MultiLineString),
    Polygon(&'a crate::geometry::Polygon),
    MultiPolygon(&'a crate::geometry::MultiPolygon),
}

impl DirectPointCarrier<'_> {
    fn covers_point(&self, point: Point) -> bool {
        match self {
            Self::Line(line) => line
                .segment_pairs()
                .any(|[start, end]| point_on_segment(point, start, end)),
            Self::MultiLine(lines) => lines.iter().any(|line| {
                line.segment_pairs()
                    .any(|[start, end]| point_on_segment(point, start, end))
            }),
            Self::Polygon(polygon) => polygon.covers_point(point),
            Self::MultiPolygon(polygons) => {
                polygons.iter().any(|polygon| polygon.covers_point(point))
            },
        }
    }
}

fn direct_point_intersection<S: Borrow<Shape>>(
    inputs: &[S],
    strictness: Strictness,
) -> Result<Option<Shape>> {
    let [left, right] = inputs else {
        return Ok(None);
    };
    let (left, right) = (left.borrow(), right.borrow());
    let source_refs = [left, right];
    if let (Some(point), Some(carrier)) = (scalar_point(left), direct_point_carrier(right)) {
        return direct_scalar_point_intersection(point, carrier, &source_refs, strictness);
    }
    if let (Some(point), Some(carrier)) = (scalar_point(right), direct_point_carrier(left)) {
        return direct_scalar_point_intersection(point, carrier, &source_refs, strictness);
    }
    if let (Some(points), Some(carrier)) = (multipoint(left), direct_point_carrier(right)) {
        return direct_multipoint_intersection(points, carrier, &source_refs, strictness);
    }
    if let (Some(points), Some(carrier)) = (multipoint(right), direct_point_carrier(left)) {
        return direct_multipoint_intersection(points, carrier, &source_refs, strictness);
    }
    Ok(None)
}

fn scalar_point(shape: &Shape) -> Option<Point> {
    match shape {
        Shape::Point(point) => Some(*point),
        _ => None,
    }
}

fn multipoint(shape: &Shape) -> Option<&crate::geometry::MultiPoint> {
    match shape {
        Shape::MultiPoint(points) => Some(points),
        _ => None,
    }
}

fn direct_point_carrier(shape: &Shape) -> Option<DirectPointCarrier<'_>> {
    match shape {
        Shape::LineString(line) if crate::geometry::relate::line_has_nonzero_segment(line) => {
            Some(DirectPointCarrier::Line(line))
        },
        Shape::MultiLineString(lines)
            if !lines.has_empty_members()
                && lines
                    .iter()
                    .all(|line| crate::geometry::relate::line_has_nonzero_segment(line)) =>
        {
            Some(DirectPointCarrier::MultiLine(lines))
        },
        Shape::Polygon(polygon) if crate::geometry::polygon_is_valid(polygon) => {
            Some(DirectPointCarrier::Polygon(polygon))
        },
        Shape::MultiPolygon(polygons) if shape.validate().is_none() => {
            Some(DirectPointCarrier::MultiPolygon(polygons))
        },
        _ => None,
    }
}

fn direct_scalar_point_intersection(
    point: Point,
    carrier: DirectPointCarrier<'_>,
    source_refs: &[&Shape],
    strictness: Strictness,
) -> Result<Option<Shape>> {
    if !carrier.covers_point(point) {
        return Ok(Some(empty_nary_overlay_shape(
            source_refs,
            OverlayOp::Intersection,
        )));
    }
    Ok(Some(crate::geometry::carry_direct_point_ordinates(
        point,
        source_refs,
        strictness.is_strict(),
    )?))
}

fn direct_multipoint_intersection(
    points: &crate::geometry::MultiPoint,
    carrier: DirectPointCarrier<'_>,
    source_refs: &[&Shape],
    strictness: Strictness,
) -> Result<Option<Shape>> {
    if points.is_empty() {
        return Ok(Some(empty_nary_overlay_shape(
            source_refs,
            OverlayOp::Intersection,
        )));
    }
    let mut candidates = direct_multipoint_candidates(points, carrier);
    if candidates.is_empty() {
        return Ok(Some(empty_nary_overlay_shape(
            source_refs,
            OverlayOp::Intersection,
        )));
    }
    dedup_points(&mut candidates);
    candidates.sort_by(crate::geometry::compare_points);
    let axes = candidates
        .first()
        .map_or(CoordinateAxes::XY, |point| point.axes);
    let output = Shape::narrow_points(axes, candidates);
    Ok(Some(carry_direct_points_ordinates(
        output,
        source_refs,
        strictness.is_strict(),
    )?))
}

fn direct_multipoint_candidates(
    points: &crate::geometry::MultiPoint,
    carrier: DirectPointCarrier<'_>,
) -> Vec<Point> {
    match carrier {
        DirectPointCarrier::Line(line) => {
            let index =
                SegmentIndex::build_from_iter(line.segment_pairs().map(|[start, end]| Segment {
                    start: start.xy(),
                    end: end.xy(),
                }));
            points
                .points()
                .filter(|point| index.covers_point(*point))
                .collect()
        },
        DirectPointCarrier::MultiLine(lines) => {
            let index = SegmentIndex::build_from_iter(
                lines
                    .iter()
                    .flat_map(|line| line.segment_pairs())
                    .map(|[start, end]| Segment {
                        start: start.xy(),
                        end: end.xy(),
                    }),
            );
            points
                .points()
                .filter(|point| index.covers_point(*point))
                .collect()
        },
        DirectPointCarrier::Polygon(polygon) => {
            let tester = PointBatchTester::from_polygon(polygon);
            points
                .points()
                .filter(|point| tester.covers_point(*point))
                .collect()
        },
        DirectPointCarrier::MultiPolygon(polygons) => {
            let tester = PointBatchTester::from_multipolygon(polygons);
            points
                .points()
                .filter(|point| tester.covers_point(*point))
                .collect()
        },
    }
}

/// Assemble a binary Cartesian polygon/line intersection from one joint graph.
/// Direct polygonal and lineal operands keep source membership unambiguous;
/// everything else remains on the generic path rather than rebuilding source
/// graphs here.
fn assemble_polygon_line_intersection(inputs: &[&Shape]) -> Result<Option<(Shape, TopologyGraph)>> {
    let [left, right] = inputs else {
        return Ok(None);
    };
    let polygonal = |shape: &Shape| {
        matches!(shape, Shape::Polygon(_) | Shape::MultiPolygon(_)) && !shape.is_empty()
    };
    let linear = |shape: &Shape| {
        !shape.is_empty()
            && match shape {
                Shape::LineString(line) => crate::geometry::relate::line_has_nonzero_segment(line),
                Shape::MultiLineString(lines) => {
                    !lines.has_empty_members()
                        && lines
                            .iter()
                            .all(|line| crate::geometry::relate::line_has_nonzero_segment(line))
                },
                _ => false,
            }
    };
    let (polygon_row, line_row) = if polygonal(left) && linear(right) {
        (0, 1)
    } else if linear(left) && polygonal(right) {
        (1, 0)
    } else {
        return Ok(None);
    };

    let graph = TopologyGraph::cartesian_many(inputs);
    if graph.source_points.is_empty()
        && !graph.edges.is_empty()
        && !graph.area_support.is_empty()
        && graph.source_edges.iter().all(|source| {
            (source.source_row == polygon_row
                && matches!(
                    source.support,
                    SourceSupport::AreaBoundary | SourceSupport::AreaInterior { .. }
                ))
                || (source.source_row == line_row && matches!(source.support, SourceSupport::Line))
        })
        && graph
            .source_edges
            .iter()
            .any(|source| source.source_row == polygon_row)
        && graph.source_edges.iter().any(|source| {
            source.source_row == line_row && matches!(source.support, SourceSupport::Line)
        })
    {
        let (points, lines) = polygon_line_overlay(&graph, polygon_row, line_row);
        return Ok(Some((
            build_overlay_shape(points, lines, Vec::new())?,
            graph,
        )));
    }
    Ok(None)
}

fn polygon_line_overlay(
    graph: &TopologyGraph,
    polygon_row: u32,
    line_row: u32,
) -> (Vec<Point>, Vec<crate::geometry::CoordSeq>) {
    let mut lines = Vec::with_capacity(graph.edges.len());
    for edge in &graph.edges {
        if !graph.edge_has_line_source_for_row(edge, line_row) {
            continue;
        }
        let midpoint = segment_midpoint(edge.segment).xy();
        let location = if graph.contains_area_interior(midpoint) {
            TopologyLocation::Interior
        } else if graph.has_area_boundary(midpoint)
            || graph.edge_has_area_source_for_row(edge, polygon_row)
        {
            TopologyLocation::Boundary
        } else {
            TopologyLocation::Exterior
        };
        if location != TopologyLocation::Exterior {
            lines.push(crate::geometry::CoordSeq::from(vec![
                edge.segment.start,
                edge.segment.end,
            ]));
        }
    }

    let points = graph
        .nodes
        .iter()
        .filter(|node| {
            graph.has_line_source_at_node(node.point, line_row)
                && (graph.contains_area_interior(node.point) || graph.has_area_boundary(node.point))
        })
        .map(|node| node.point.point())
        .collect();
    (points, lines)
}

/// The polygon-only intersection lane is valid only when the result cannot
/// contain an independent boundary contact. Two valid convex shells with a
/// transverse boundary crossing have a positive-area, connected intersection;
/// touch-only, nested, disjoint, and degenerate cases deliberately defer to
/// the mixed-dimension arrangement.
fn convex_polygon_intersection(inputs: &[&Shape]) -> Option<Shape> {
    let [left, right] = inputs else {
        return None;
    };
    let (Shape::Polygon(left), Shape::Polygon(right)) = (*left, *right) else {
        return None;
    };
    if !left.holes.is_empty() || !right.holes.is_empty() {
        return None;
    }
    let left_shell = left.shell.coords();
    let right_shell = right.shell.coords();
    let closed_finite = |shell: &crate::geometry::CoordSeq| {
        shell.len() >= 4
            && shell
                .first_coord()
                .zip(shell.last_coord())
                .is_some_and(|(first, last)| same_point(first, last))
            && column_all_finite(shell.xs())
            && column_all_finite(shell.ys())
    };
    if !closed_finite(left_shell)
        || !closed_finite(right_shell)
        || !shell_is_convex(left_shell)
        || !shell_is_convex(right_shell)
        || !crate::geometry::polygon_is_valid(left)
        || !crate::geometry::polygon_is_valid(right)
    {
        return None;
    }
    let output = crate::geometry::clean_union::clean_overlay(
        std::slice::from_ref(left),
        std::slice::from_ref(right),
        OverlayOp::Intersection,
    )?;
    matches!(&output, Shape::Polygon(_)).then_some(output)
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

/// Bounds-overlap components, built from the existing overlap sweep.
fn bounds_components(bounds: &[Option<Bounds>]) -> Vec<Vec<usize>> {
    fn root(parent: &mut [usize], mut node: usize) -> usize {
        while parent[node] != node {
            parent[node] = parent[parent[node]];
            node = parent[node];
        }
        node
    }
    let mut parent: Vec<usize> = (0..bounds.len()).collect();
    let _ = crate::geometry::for_each_indexed_overlapping_bounds_pair(bounds, |left, right| {
        let (a, b) = (root(&mut parent, left), root(&mut parent, right));
        if a != b {
            parent[b] = a;
        }
        ControlFlow::Continue(())
    });
    let mut slots = vec![usize::MAX; bounds.len()];
    let mut components = Vec::new();
    for index in 0..bounds.len() {
        let representative = root(&mut parent, index);
        if slots[representative] == usize::MAX {
            slots[representative] = components.len();
            components.push(vec![index]);
        } else {
            components[slots[representative]].push(index);
        }
    }
    components
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
    // `raw_polygon_loops_eligible` already means: a union, of operands with no
    // point or line support, whose raw coordinate bounds are sound. Such a
    // union is purely areal — neither the line sink nor the point sink can
    // emit anything — and the winding region is computed from the operands'
    // own rings. So the joint graph and the per-source graphs below are built
    // and then read for nothing but those rings, which is the whole noding
    // pipeline charged to a lane that does not need it.
    // Two areal operands are what `binary_areal_overlay` is for: both operands'
    // rings noded into ONE arrangement whose faces carry `[left, right]`
    // winding numbers. It is already the engine behind `clip_by_rect` and the
    // native areal relate; the public binary overlay path never reached it and
    // paid the n-ary topology graph instead.
    //
    // Not for intersection. Two areal operands that meet along a boundary
    // intersect in a *line*, and an area-only answer would drop it — the rule
    // `binary_areal_overlay`'s own clean lane encodes in its `op` guard, that
    // `overlay_lines` encodes by admitting a non-line-source edge only for
    // intersection, and that `convex_intersection_defers_contacts_and_
    // concavity_to_joint_graph` exists to hold. The other three operations on
    // two areal operands are areal.
    if domain == TopologyDomain::Cartesian
        && op != OverlayOp::Intersection
        && let [left, right] = inputs
        && let (Some(left_polygons), Some(right_polygons)) =
            (areal_only_polygons(left), areal_only_polygons(right))
        && !left_polygons.is_empty()
        && !right_polygons.is_empty()
    {
        return build_overlay_shape(
            Vec::new(),
            Vec::new(),
            crate::geometry::binary_areal_overlay(&left_polygons, &right_polygons, op),
        );
    }
    if raw_polygon_loops_eligible {
        let loops = shape_polygon_loops(inputs);
        if !loops.is_empty() {
            let polygons = crate::geometry::winding_region(&loops, |winding| winding != 0)
                .into_iter()
                .map(|polygon| canonicalize_polygon(polygon, domain))
                .collect();
            return build_overlay_shape(Vec::new(), Vec::new(), polygons);
        }
    }
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
    let polygon_edges = output_polygon_edges(&polygons);
    let lines = overlay_lines(&graph, &source_graphs, &polygon_indexes, &polygon_edges, op);
    let points = overlay_points(&graph, &source_graphs, &polygon_indexes, &lines, op);
    build_overlay_shape(points, lines, polygons)
}

/// Assemble each bounds-connected component independently, then feed their
/// parts into the one shared output tail so canonicalization and ordinate
/// restoration still see the whole operation.
fn assemble_components_overlay(
    inputs: &[&Shape],
    components: &[Vec<usize>],
    domain: TopologyDomain,
    op: OverlayOp,
    raw_polygon_loops_eligible: bool,
) -> Result<Shape> {
    let mut points = Vec::new();
    let mut lines = Vec::new();
    let mut polygons = Vec::new();
    for component in components {
        if op == OverlayOp::Difference && !component.contains(&0) {
            continue;
        }
        let subset = component.iter().map(|&index| inputs[index]).collect::<Vec<_>>();
        let shape = assemble_overlay(&subset, domain, op, raw_polygon_loops_eligible)?;
        append_overlay_parts(shape, &mut points, &mut lines, &mut polygons);
    }
    build_overlay_shape(points, lines, polygons)
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
    build_overlay_shape(points, lines, polygons)
}

/// Overlay Cartesian linework operands straight from noding provenance:
/// the joint segment pool is self-noded once, every atomic piece comes back
/// with the input rows that own it, and the operation is a membership rule
/// on that owner set. No topology graph, no node or edge maps, no
/// re-location of pieces the noding already attributed.
///
/// A piece is in the union if any row owns it, in the intersection if every
/// row does, in the difference if only row 0 does, and in the symmetric
/// difference if an odd number do — a stretch two rows share is one piece
/// with two owners, which is exactly what makes it drop out of a difference
/// and into an intersection. Points arise only for the intersection: a
/// vertex both rows carry, or a place where one row's segment was cut at a
/// vertex or crossing of the other's, that no output line covers. The
/// dissolve into maximal chains and the point survival check are the same
/// `build_overlay_shape` every overlay lane ends in.
fn assemble_line_only_overlay(inputs: &[&Shape], op: OverlayOp) -> Result<Shape> {
    let mut pool: Vec<Segment> = Vec::new();
    let mut row_of: Vec<u32> = Vec::new();
    for (row, shape) in inputs.iter().enumerate() {
        for line in line_only_parts(shape) {
            for [start, end] in line.segment_pairs() {
                let (start, end) = (start.xy(), end.xy());
                if same_point(start, end) {
                    continue;
                }
                pool.push(Segment { start, end });
                row_of.push(row as u32);
            }
        }
    }
    let rows = inputs.len();
    if let Some(shape) = cut_lines_overlay(inputs, &pool, op)? {
        return Ok(shape);
    }
    let (atomic, provenance) = super::self_node_segments_sourced(pool.iter().copied());

    // Noding splits each source segment independently. Coincident atomic
    // copies are one arrangement piece, so merge them before membership.
    let mut merged: HashMap<(PointKey, PointKey), (Segment, Vec<u32>)> =
        HashMap::with_capacity(atomic.len());
    for (piece, &segment) in atomic.iter().enumerate() {
        let entry = merged
            .entry(crate::geometry::undirected_segment_edge_key(segment))
            .or_insert((segment, Vec::new()));
        entry.1.extend(
            provenance
                .owners(piece)
                .iter()
                .map(|&(owner, _)| row_of[owner as usize]),
        );
    }
    let atomic: Vec<(Segment, Vec<u32>)> = merged
        .into_values()
        .map(|(segment, mut owner_rows)| {
            owner_rows.sort_unstable();
            owner_rows.dedup();
            (segment, owner_rows)
        })
        .collect();
    let keep = |owner_rows: &[u32]| match op {
        OverlayOp::Union => !owner_rows.is_empty(),
        OverlayOp::Intersection => owner_rows.len() == rows,
        OverlayOp::Difference => owner_rows == [0],
        OverlayOp::SymmetricDifference => owner_rows.len() % 2 == 1,
    };
    let mut kept: Vec<Segment> = Vec::with_capacity(atomic.len());
    // Intersection points: every vertex of the noded arrangement, with the
    // rows whose pieces end there and the rows whose source vertices sit
    // there; the ones every row reaches are shared points, and
    // `build_overlay_shape` drops those an output line already covers.
    let mut node_rows: HashMap<PointKey, Vec<u32>> = HashMap::new();
    let mut note_node = |point: XY, owner_rows: &[u32], node_rows: &mut HashMap<PointKey, Vec<u32>>| {
        let entry = node_rows.entry(PointKey::new(point)).or_default();
        for &row in owner_rows {
            if !entry.contains(&row) {
                entry.push(row);
            }
        }
    };
    for (segment, owner_rows) in &atomic {
        if op == OverlayOp::Intersection {
            note_node(segment.start, owner_rows, &mut node_rows);
            note_node(segment.end, owner_rows, &mut node_rows);
        }
        if keep(owner_rows) {
            kept.push(*segment);
        }
    }
    let points = if op == OverlayOp::Intersection {
        for (row, shape) in inputs.iter().enumerate() {
            for line in line_only_parts(shape) {
                for point in line.iter() {
                    note_node(point.xy(), &[row as u32], &mut node_rows);
                }
            }
        }
        node_rows
            .into_iter()
            .filter(|(_, owner_rows)| owner_rows.len() == rows)
            .map(|(key, _)| key.xy().point())
            .collect()
    } else {
        Vec::new()
    };
    build_overlay_shape_from_chains(points, merge_chain_edges(&kept), Vec::new())
}

struct Crossing {
    cut_ordinals: Vec<u32>,
    kept_cuts: usize,
    rows: Vec<usize>,
    kept_landing: bool,
    splits: bool,
}

struct CrossingTable {
    crossings: HashMap<PointKey, Crossing>,
    split_vertices: HashSet<(usize, usize)>,
}

fn crossing_table(
    lines: &[(usize, &crate::geometry::CoordSeq)],
    pool: &[Segment],
    scan: &super::NodingScan,
    keep_row: impl Fn(usize) -> bool,
) -> CrossingTable {
    let mut site_of = Vec::with_capacity(pool.len());
    for (line_index, &(row, line)) in lines.iter().enumerate() {
        for (vertex_index, [start, end]) in line.segment_pairs().enumerate() {
            if !same_point(start.xy(), end.xy()) {
                site_of.push((row, line_index, vertex_index));
            }
        }
    }
    let mut crossings = HashMap::with_capacity(scan.events.len());
    for &(ordinal, point) in &scan.events {
        let (row, _, _) = site_of[ordinal as usize];
        let entry = crossings.entry(PointKey::new(point)).or_insert(Crossing {
            cut_ordinals: Vec::new(), kept_cuts: 0, rows: Vec::new(), kept_landing: false, splits: false,
        });
        if entry.cut_ordinals.contains(&ordinal) {
            continue;
        }
        entry.cut_ordinals.push(ordinal);
        entry.kept_cuts += usize::from(keep_row(row));
        if !entry.rows.contains(&row) {
            entry.rows.push(row);
        }
    }
    for &(_, point, landing) in &scan.touches {
        let (row, _, _) = site_of[landing as usize];
        let entry = crossings.get_mut(&PointKey::new(point)).expect("a touch is a cut");
        if !entry.rows.contains(&row) {
            entry.rows.push(row);
        }
        entry.kept_landing |= keep_row(row);
    }
    for crossing in crossings.values_mut() {
        crossing.splits = crossing.kept_cuts >= 2 || (crossing.kept_cuts >= 1 && crossing.kept_landing);
    }
    let mut split_vertices = HashSet::new();
    for &(_, point, landing) in &scan.touches {
        if !crossings[&PointKey::new(point)].splits {
            continue;
        }
        let (row, line_index, vertex_index) = site_of[landing as usize];
        if !keep_row(row) {
            continue;
        }
        let segment = pool[landing as usize];
        split_vertices.insert((
            line_index,
            if same_point(point, segment.start) { vertex_index } else { vertex_index + 1 },
        ));
    }
    CrossingTable { crossings, split_vertices }
}

/// Rebuild input chains directly when noding found only interior crossings.
/// Overlaps, junctions and closed lines retain the generic provenance path.
fn cut_lines_overlay(inputs: &[&Shape], pool: &[Segment], op: OverlayOp) -> Result<Option<Shape>> {
    let scan = super::full_noding_events(pool);
    if scan.overlaps_found || scan.junctions_found || consecutive_lines_touch(inputs) || !scan.events.is_empty() && inputs.iter().any(|shape| {
        line_only_parts(shape).into_iter().any(|line| {
            line.first().zip(line.last()).is_some_and(|(first, last)| same_point(first.xy(), last.xy()))
        })
    }) {
        return Ok(None);
    }
    let lines: Vec<_> = inputs.iter().enumerate()
        .flat_map(|(row, shape)| line_only_parts(shape).into_iter().map(move |line| (row, line)))
        .collect();
    let rows = inputs.len();
    let keep_row = |row| match op {
        OverlayOp::Union | OverlayOp::SymmetricDifference => true,
        OverlayOp::Intersection => false,
        OverlayOp::Difference => row == 0,
    };
    let CrossingTable { crossings, split_vertices } = crossing_table(&lines, pool, &scan, keep_row);
    let mut events = scan.events;
    events.sort_unstable_by_key(|&(ordinal, _)| ordinal);
    let mut chains = Vec::new();
    let mut ordinal = 0;
    let mut cursor = 0;
    let mut cuts = Vec::new();
    let mut points = Vec::new();
    for (line_index, &(row, line)) in lines.iter().enumerate() {
        let kept = keep_row(row);
        points.clear();
        for (vertex_index, [start, end]) in line.segment_pairs().enumerate() {
            let (start, end) = (start.xy(), end.xy());
            if same_point(start, end) {
                continue;
            }
            if kept && vertex_index > 0 && points.len() >= 2 && split_vertices.contains(&(line_index, vertex_index)) {
                chains.push(std::mem::take(&mut points).into());
            }
            cuts.clear();
            while cursor < events.len() && events[cursor].0 as usize == ordinal {
                cuts.push(events[cursor].1);
                cursor += 1;
            }
            ordinal += 1;
            if !kept {
                continue;
            }
            if points.last().is_none_or(|last: &Point| !same_point(last.xy(), start)) {
                points.push(start.point());
            }
            cuts.sort_by_cached_key(|cut| crate::geometry::segments::point_distance_squared(start, *cut).to_bits());
            for cut in cuts.iter().copied() {
                if points.last().is_some_and(|last| same_point(last.xy(), cut)) {
                    continue;
                }
                points.push(cut.point());
                if crossings[&PointKey::new(cut)].splits {
                    chains.push(std::mem::take(&mut points).into());
                    points.push(cut.point());
                }
            }
            points.push(end.point());
        }
        if points.len() >= 2 {
            chains.push(std::mem::take(&mut points).into());
        }
    }
    let mut shared_points: Vec<Point> = if op == OverlayOp::Intersection {
        crossings.iter().filter(|(_, crossing)| crossing.rows.len() == rows)
            .map(|(key, _)| key.xy().point()).collect()
    } else { Vec::new() };
    shared_points.sort_unstable_by(|a, b| (a.x, a.y).partial_cmp(&(b.x, b.y)).expect("finite"));
    build_overlay_shape_from_chains(shared_points, chains, Vec::new()).map(Some)
}

/// The sweep reports non-consecutive endpoint contacts. Adjacent source lines
/// meet in consecutive pool ordinals, so their junction is checked here.
fn consecutive_lines_touch(inputs: &[&Shape]) -> bool {
    let mut previous: Option<(PointKey, PointKey)> = None;
    for shape in inputs {
        for line in line_only_parts(shape) {
            let (Some(first), Some(last)) = (line.first(), line.last()) else {
                continue;
            };
            let (first, last) = (PointKey::new(first.xy()), PointKey::new(last.xy()));
            let (previous_first, previous_last) = previous.unwrap_or((first, last));
            if previous.is_some()
                && [first, last]
                    .iter()
                    .any(|end| *end == previous_first || *end == previous_last)
            {
                return true;
            }
            previous = Some((first, last));
        }
    }
    false
}

/// The line parts of a line-only operand (see [`nondegenerate_line_only`]).
fn line_only_parts(shape: &Shape) -> Vec<&crate::geometry::CoordSeq> {
    match shape {
        Shape::LineString(line) => vec![line],
        Shape::MultiLineString(lines) => lines.iter().map(|line| &**line).collect(),
        Shape::GeometryCollection(geometries) => geometries.iter().flat_map(line_only_parts).collect(),
        _ => Vec::new(),
    }
}

pub(super) fn nondegenerate_line_only(shape: &Shape) -> bool {
    match shape {
        Shape::LineString(line) => crate::geometry::relate::line_has_nonzero_segment(line),
        Shape::MultiLineString(lines) => {
            !lines.has_empty_members()
                && lines
                    .iter()
                    .all(|line| crate::geometry::relate::line_has_nonzero_segment(line))
        },
        Shape::GeometryCollection(geometries) => {
            !geometries.is_empty() && geometries.iter().all(nondegenerate_line_only)
        },
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::Polygon(_)
        | Shape::MultiPolygon(_)
        | Shape::Empty(..) => false,
    }
}

/// Assemble Cartesian line overlays directly from the joint graph's source
/// provenance. The generic path remains authoritative when an edge carries
/// anything other than line support or a source row is not representable.
fn line_only_overlay(
    graph: &TopologyGraph,
    source_count: usize,
    op: OverlayOp,
) -> Option<(Vec<Point>, Vec<crate::geometry::CoordSeq>)> {
    if !matches!(op, OverlayOp::Union | OverlayOp::Intersection)
        || graph.edges.is_empty()
        || !graph.source_points.is_empty()
        || !graph.area_support.is_empty()
        || graph
            .edges
            .iter()
            .any(|edge| !graph.edge_has_only_line_sources(edge, source_count))
    {
        return None;
    }

    let mut lines = Vec::with_capacity(graph.edges.len());
    for edge in &graph.edges {
        let keep = match op {
            OverlayOp::Union => true,
            OverlayOp::Intersection => (0..source_count)
                .all(|source_row| graph.edge_has_line_source_for_row(edge, source_row as u32)),
            OverlayOp::Difference | OverlayOp::SymmetricDifference => false,
        };
        if keep {
            lines.push(crate::geometry::CoordSeq::from(vec![
                edge.segment.start,
                edge.segment.end,
            ]));
        }
    }

    let points = if op == OverlayOp::Intersection {
        graph
            .nodes
            .iter()
            .filter(|node| {
                (0..source_count)
                    .all(|source_row| graph.has_line_source_at_node(node.point, source_row as u32))
            })
            .map(|node| node.point.point())
            .collect()
    } else {
        Vec::new()
    };
    Some((points, lines))
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
            return crate::geometry::winding_region(&loops, |winding| winding != 0)
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
        TopologyDomain::Cartesian => area_edges,
        TopologyDomain::Geographic(frame)
            if !graph
                .nodes
                .iter()
                .any(|node| node.point.y.abs() == frame.pole()) =>
        {
            unwrap_geographic_area_edges(graph, frame, &area_edges)
        },
        TopologyDomain::Geographic(_) => area_edges,
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

/// The shape's polygons, when it carries area and nothing else. `None` for an
/// operand with any point or lineal support, which the joint winding
/// arrangement has no representation for.
fn areal_only_polygons(shape: &Shape) -> Option<Vec<&crate::geometry::Polygon>> {
    fn visit<'a>(shape: &'a Shape, out: &mut Vec<&'a crate::geometry::Polygon>) -> bool {
        match shape {
            Shape::Polygon(polygon) => {
                out.push(polygon);
                true
            },
            Shape::MultiPolygon(polygons) => {
                out.extend(polygons.iter());
                true
            },
            Shape::GeometryCollection(parts) => parts.iter().all(|part| visit(part, out)),
            Shape::Empty(..) => true,
            Shape::Point(_)
            | Shape::MultiPoint(_)
            | Shape::LineString(_)
            | Shape::MultiLineString(_) => false,
        }
    }

    let mut out = Vec::new();
    visit(shape, &mut out).then_some(out)
}

/// The operands' own polygon rings as winding loops, in the order the graph
/// builder would collect them. [`source_polygon_loops`] reads the same rings
/// back out of built graphs; this reads them from the shapes, so the
/// winding-union lane never has to build one.
fn shape_polygon_loops(inputs: &[&Shape]) -> Vec<(Vec<f64>, Vec<f64>)> {
    fn visit(shape: &Shape, out: &mut Vec<(Vec<f64>, Vec<f64>)>) {
        match shape {
            Shape::Polygon(polygon) => push_polygon_loops(polygon, out),
            Shape::MultiPolygon(polygons) => {
                for polygon in polygons {
                    push_polygon_loops(polygon, out);
                }
            },
            Shape::GeometryCollection(parts) => {
                for part in parts {
                    visit(part, out);
                }
            },
            _ => {},
        }
    }

    let mut out = Vec::new();
    for input in inputs {
        visit(input, &mut out);
    }
    out
}

fn push_polygon_loops(polygon: &crate::geometry::Polygon, out: &mut Vec<(Vec<f64>, Vec<f64>)>) {
    for ring in polygon.rings() {
        if let Some(loop_columns) = ring_loop_columns(ring) {
            out.push(loop_columns);
        }
    }
}

/// One ring as open `(xs, ys)` columns — the closing repeat dropped, and
/// nothing shorter than a triangle.
fn ring_loop_columns(ring: &crate::geometry::Ring) -> Option<(Vec<f64>, Vec<f64>)> {
    let mut points: Vec<Point> = ring.iter_coords().collect();
    if points.len() > 1 && same_point(points[0], *points.last().expect("non-empty ring")) {
        points.pop();
    }
    (points.len() >= 3).then(|| points.into_iter().map(|point| (point.x, point.y)).unzip())
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

fn output_polygon_edges(polygons: &[crate::geometry::Polygon]) -> HashSet<(PointKey, PointKey)> {
    let mut edges = HashSet::with_capacity(
        polygons
            .iter()
            .map(crate::geometry::Polygon::coord_count)
            .sum(),
    );
    for polygon in polygons {
        for ring in polygon.rings() {
            for [start, end] in ring.segment_pairs() {
                edges.insert(ordered_edge(PointKey::new(start), PointKey::new(end)));
            }
        }
    }
    edges
}

fn overlay_lines(
    graph: &TopologyGraph,
    sources: &[TopologyGraph],
    polygon_indexes: &[crate::geometry::PolygonPointIndex],
    polygon_edges: &HashSet<(PointKey, PointKey)>,
    op: OverlayOp,
) -> Vec<crate::geometry::CoordSeq> {
    let mut lines = Vec::new();
    for edge in &graph.edges {
        let edge_key = ordered_edge(
            PointKey::new(edge.segment.start),
            PointKey::new(edge.segment.end),
        );
        if polygon_edges.contains(&edge_key) {
            continue;
        }
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
