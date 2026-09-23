use std::borrow::Borrow;
use std::ops::ControlFlow;

use crate::boundary::geographic::invalid_lonlat_error;
use crate::error::{ResourceError, Result};
use crate::geographic::{GeographicInputs, GeographicViewError};
use crate::geometry::overlay::{
    ExactSourceInventory, OverlayOp, Strictness, build_certified_overlay_shape,
    build_overlay_shape, dedup_points, overlay_declared_axes,
};
use crate::geometry::predicates::ring_classify_point;
use crate::geometry::topology::TopologyLocation;
use crate::geometry::{
    AreaSign, Arrangement, Bounds, CoordSeq, CoordinateAxes, Coordinates as _, Dimension,
    GeometryErrorKind, GeometryKind, HashMap, HashSet, Point, PointKey, Segment, Shape,
    TopologyDomain, TopologyGraph, XY, carry_overlay_ordinates, ordered_edge, point_on_segment,
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

    match domain {
        TopologyDomain::Cartesian => {
            let mut refs = Vec::with_capacity(geometries.len());
            refs.extend(geometries.iter().map(std::borrow::Borrow::borrow));
            exact_cartesian_overlay(refs, op, strictness, bounds_certificates)
        },
        TopologyDomain::Geographic(frame) => {
            let originals = GeographicInputs::try_from_shapes(
                frame,
                geometries.iter().map(std::borrow::Borrow::borrow),
            )
            .map_err(|error| overlay_view_error(&error))?;
            overlay_all_geographic_from(&originals, op, strictness, bounds_certificates)
        },
    }
}

/// The one live Cartesian overlay route. Source inventory, exact incidence,
/// cell selection, dimensional absorption, and checked emission all belong to
/// `ExactPartition`; this sink only serializes the certified plan and carries
/// ordinates afterwards. Inputs whose bounds never meet cannot interact, so
/// each bounds-connected component is partitioned alone.
fn exact_cartesian_overlay(
    mut inputs: Vec<&Shape>,
    operation: OverlayOp,
    strictness: Strictness,
    bounds_certificates: bool,
) -> Result<Shape> {
    if inputs.len() == 1 && operation == OverlayOp::Intersection {
        return Ok(inputs[0].clone());
    }
    if operation == OverlayOp::Difference {
        inputs[1..].sort_unstable_by(|left, right| crate::geometry::compare_shapes(left, right));
    } else {
        inputs.sort_unstable_by(|left, right| crate::geometry::compare_shapes(left, right));
    }
    let inputs = inputs.as_slice();
    // Declared axes are a fact about the whole input, not any one component.
    let declared_axes = overlay_declared_axes(inputs, operation);
    let strict = strictness.is_strict();
    let components = if bounds_certificates && inputs.len() > 1 {
        let mut bounds = Vec::with_capacity(inputs.len());
        for shape in inputs {
            bounds.push(shape.bounds());
        }
        if operation == OverlayOp::Intersection && intersection_bounds_empty(&bounds) {
            return Ok(empty_nary_overlay_shape(inputs, operation));
        }
        interacting_components(&bounds, operation)?
    } else {
        None
    };
    let shape = match components {
        None => exact_partition_overlay(inputs, operation, declared_axes, strict)?,
        Some(components) => {
            let mut shapes = Vec::with_capacity(components.len());
            let mut subset = Vec::new();
            for component in components {
                subset.clear();
                subset.reserve(component.len());
                subset.extend(component.iter().map(|&index| inputs[index]));
                shapes.push(exact_partition_overlay(
                    &subset,
                    operation,
                    declared_axes,
                    strict,
                )?);
            }
            join_components(&shapes)?
        },
    };
    Ok(if shape.is_empty() {
        empty_nary_overlay_shape(inputs, operation)
    } else {
        shape.normalize()?
    })
}

/// Certified per-component results joined as one arrangement emits them.
/// Components never meet, so nothing is reclassified between them, but a
/// bucket keeps an ordinate only when every part in it carries one: the rule
/// one arrangement applies to its nodes.
fn join_components(shapes: &[Shape]) -> Result<Shape> {
    let mut points = Vec::new();
    let mut lines = Vec::new();
    let mut polygons = Vec::new();
    for shape in shapes {
        append_overlay_parts(shape, &mut points, &mut lines, &mut polygons)?;
    }
    let common = |axes: &mut dyn Iterator<Item = CoordinateAxes>| {
        axes.reduce(CoordinateAxes::intersection)
            .unwrap_or(CoordinateAxes::XY)
    };
    let restricted = |coords: &CoordSeq, axes: CoordinateAxes| -> Result<CoordSeq> {
        let coords = if axes.has_z() {
            coords.clone()
        } else {
            coords.set_z(None, false)?
        };
        if axes.has_m() {
            Ok(coords)
        } else {
            coords.set_m(None, false)
        }
    };
    let point_axes = common(&mut points.iter().map(|point| point.axes()));
    for point in &mut points {
        *point = Point::new_raw_axes(
            point.x,
            point.y,
            crate::geometry::ZOrdinate(point.z().filter(|_| point_axes.has_z())),
            crate::geometry::MOrdinate(point.m().filter(|_| point_axes.has_m())),
        );
    }
    let line_axes = common(&mut lines.iter().map(CoordSeq::axes));
    for line in &mut lines {
        if line.axes() != line_axes {
            *line = restricted(line, line_axes)?;
        }
    }
    let polygon_axes = common(&mut polygons.iter().map(crate::geometry::Polygon::axes));
    for polygon in &mut polygons {
        if polygon.axes() != polygon_axes {
            let mut holes = Vec::with_capacity(polygon.holes.len());
            for hole in polygon.holes.iter() {
                holes.push(crate::geometry::Ring::try_from_closed(restricted(
                    hole.coords(),
                    polygon_axes,
                )?)?);
            }
            let shell = crate::geometry::Ring::try_from_closed(restricted(
                polygon.shell.coords(),
                polygon_axes,
            )?)?;
            *polygon = crate::geometry::Polygon::new(shell, holes);
        }
    }
    build_certified_overlay_shape(points, lines, polygons)
}

/// One exact partition over `inputs`, already in canonical source order.
fn exact_partition_overlay(
    inputs: &[&Shape],
    operation: OverlayOp,
    declared_axes: CoordinateAxes,
    strict: bool,
) -> Result<Shape> {
    let union = if operation == OverlayOp::Union {
        ExactSourceInventory::from_polygonal_union(inputs)?
    } else {
        None
    };
    let inventory = match union {
        Some(inventory) => inventory,
        None => ExactSourceInventory::from_shapes(inputs)?,
    };
    let mut budget = crate::geometry::ExpansionBudget::new(operation.name(), "exact partition");
    let partition = inventory.partition(&mut budget)?;
    inventory.overlay_shape(&partition, operation, declared_axes, strict)
}

pub(in crate::geometry) fn overlay_all_geographic_from(
    originals: &GeographicInputs<'_>,
    op: OverlayOp,
    strictness: crate::geometry::Strictness,
    bounds_certificates: bool,
) -> Result<Shape> {
    overlay_admitted(originals, op, strictness, bounds_certificates, false)
}

pub(crate) fn occupied_geographic_support(
    shape: &Shape,
    frame: crate::geographic::GeographicFrame,
) -> Result<Shape> {
    let originals = GeographicInputs::try_from_shapes(frame, std::iter::once(shape))
        .map_err(|error| overlay_view_error(&error))?;
    overlay_admitted(
        &originals,
        OverlayOp::Union,
        Strictness::Lenient,
        false,
        true,
    )
}

fn overlay_admitted(
    originals: &GeographicInputs<'_>,
    op: OverlayOp,
    strictness: crate::geometry::Strictness,
    bounds_certificates: bool,
    force_graph: bool,
) -> Result<Shape> {
    let frame = originals.frame();
    let bounds_facts = overlay_bounds_facts_geographic(originals, op);
    let certify = bounds_certificates
        && originals.views().len() > 1
        && bounds_facts.raw_coordinate_bounds_sound;
    if certify && op == OverlayOp::Intersection && intersection_bounds_empty(&bounds_facts.bounds) {
        let mut inputs = Vec::with_capacity(originals.views().len());
        for view in originals.views() {
            inputs.push(view.shape());
        }
        return Ok(empty_nary_overlay_shape(&inputs, op));
    }

    let mut sources = Vec::new();
    sources.reserve_exact(originals.views().len());
    for view in originals.views() {
        let shape = if crate::geometry::crosses_antimeridian_in_frame(view.shape(), frame) {
            view.shape()
                .split_antimeridian_in_frame(frame)?
                .normalize()?
        } else {
            view.shape().normalize()?
        };
        sources.push(shape);
    }

    // The left operand remains first for the only non-commutative operation.
    // Every other operation gets a canonical source order before graph
    // construction, so input permutation cannot affect noding provenance.
    if op == OverlayOp::Difference {
        sources[1..].sort_unstable_by(crate::geometry::compare_shapes);
    } else {
        sources.sort_unstable_by(crate::geometry::compare_shapes);
    }

    let mut refs = Vec::with_capacity(sources.len());
    refs.extend(sources.iter());
    let noded = GeographicInputs::try_from_shapes(frame, refs.iter().copied())
        .map_err(|error| overlay_view_error(&error))?;
    if originals.views().len() == 1
        && op == OverlayOp::Intersection
        && !crate::geometry::crosses_antimeridian_in_frame(originals.views()[0].shape(), frame)
    {
        return Ok(originals.views()[0].shape().clone());
    }
    let components = if certify {
        let mut bounds = Vec::with_capacity(refs.len());
        for shape in &refs {
            bounds.push(shape.bounds());
        }
        interacting_components(&bounds, op)?
    } else {
        None
    };
    let raw_polygon_loops_eligible = bounds_facts.raw_polygon_loops_eligible && !force_graph;
    let shape = if let Some(components) = components {
        let mut points = Vec::new();
        let mut lines = Vec::new();
        let mut polygons = Vec::new();
        for component in &components {
            let subset = noded.selected(component);
            let shape = assemble_overlay(&subset, op, raw_polygon_loops_eligible)?;
            append_overlay_parts(&shape, &mut points, &mut lines, &mut polygons)?;
        }
        build_overlay_shape(points, &lines, &polygons)?
    } else {
        assemble_overlay(&noded, op, raw_polygon_loops_eligible)?
    };
    let shape = if shape.is_empty() {
        empty_nary_overlay_shape(&refs, op)
    } else {
        // Normalize before carrying ordinates: canonical geometry ordering is
        // XY-defined, while ordinate restoration is a separate provenance pass.
        shape.normalize()?
    };
    carry_overlay_ordinates(shape, &refs, op, strictness.is_strict())
}

struct OverlayBoundsFacts {
    bounds: Vec<Option<Bounds>>,
    raw_coordinate_bounds_sound: bool,
    raw_polygon_loops_eligible: bool,
}

pub(in crate::geometry) fn overlay_view_error(error: &GeographicViewError) -> crate::error::Error {
    invalid_lonlat_error(error.native.x, error.native.y)
}

fn overlay_bounds_facts_from(
    bounds: Vec<Option<Bounds>>,
    raw_coordinate_bounds_sound: bool,
    op: OverlayOp,
    has_point_or_line: bool,
) -> OverlayBoundsFacts {
    OverlayBoundsFacts {
        bounds,
        raw_coordinate_bounds_sound,
        raw_polygon_loops_eligible: op == OverlayOp::Union
            && raw_coordinate_bounds_sound
            && !has_point_or_line,
    }
}

fn overlay_bounds_facts_geographic(
    originals: &GeographicInputs<'_>,
    op: OverlayOp,
) -> OverlayBoundsFacts {
    let mut bounds = Vec::with_capacity(originals.views().len());
    for view in originals.views() {
        bounds.push(view.shape().bounds());
    }
    let raw_coordinate_bounds_sound =
        originals.views().iter().zip(&bounds).all(|(view, bounds)| {
            bounds.is_some_and(|bounds| {
                crate::geometry::raw_coordinate_bounds_are_sound_in_frame(
                    Some(view.shape()),
                    bounds,
                    view.frame(),
                )
            })
        });
    overlay_bounds_facts_from(
        bounds,
        raw_coordinate_bounds_sound,
        op,
        originals
            .views()
            .iter()
            .any(|view| has_nonempty_point_or_line_support(view.shape())),
    )
}

/// The bounds-connected groups of inputs to assemble, when that differs from
/// one arrangement over every input. Inputs whose bounds never meet cannot
/// interact: union and symmetric difference assemble each component alone,
/// and a difference keeps only its left operand's component, because nothing
/// outside it can subtract from it. An input without bounds meets nothing.
/// `None` means one arrangement over all inputs, decided from the union-find
/// roots before any group is materialized.
fn interacting_components(
    bounds: &[Option<Bounds>],
    op: OverlayOp,
) -> Result<Option<Vec<Vec<usize>>>> {
    const fn root(parent: &mut [usize], mut node: usize) -> usize {
        while parent[node] != node {
            parent[node] = parent[parent[node]];
            node = parent[node];
        }
        node
    }
    if op == OverlayOp::Intersection {
        return Ok(None);
    }
    let count = bounds.len();
    let mut parent = Vec::with_capacity(count);
    parent.extend(0..count);
    let ControlFlow::Continue(()): ControlFlow<std::convert::Infallible> =
        crate::geometry::for_each_indexed_overlapping_bounds_pair(bounds, |left, right| {
            let (a, b) = (root(&mut parent, left), root(&mut parent, right));
            parent[a.max(b)] = a.min(b);
            ControlFlow::Continue(())
        })?;
    // Every root is its component's smallest index, so input 0 roots the left
    // operand's component.
    for node in 0..count {
        parent[node] = root(&mut parent, node);
    }
    let mut components: Vec<Vec<usize>> = Vec::new();
    if op == OverlayOp::Difference {
        let left = parent.iter().filter(|&&root| root == 0).count();
        if left == count {
            return Ok(None);
        }
        let mut component = Vec::with_capacity(left);
        component.extend((0..count).filter(|&node| parent[node] == 0));
        components.push(component);
        return Ok(Some(components));
    }
    let roots = (0..count).filter(|&node| parent[node] == node).count();
    if roots == 1 {
        return Ok(None);
    }
    let mut slot_of_root = Vec::with_capacity(count);
    slot_of_root.resize(count, usize::MAX);
    components.reserve(roots);
    for (node, &root) in parent.iter().enumerate() {
        if slot_of_root[root] == usize::MAX {
            slot_of_root[root] = components.len();
            components.push(Vec::new());
        }
        components[slot_of_root[root]].push(node);
    }
    Ok(Some(components))
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
    inputs: &GeographicInputs<'_>,
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
    if raw_polygon_loops_eligible {
        let loops = shape_polygon_loops(inputs);
        if !loops.is_empty() {
            let mut polygons = crate::geometry::winding_region(&loops, |winding| winding != 0)?;
            for polygon in &mut polygons {
                *polygon = canonicalize_polygon(polygon, inputs.frame())?;
            }
            return build_overlay_shape(Vec::new(), &[], &polygons);
        }
    }
    assemble_joint_graph_overlay(inputs, op, raw_polygon_loops_eligible)
}

/// Mixed-dimensional overlay from joint noding and exact face-side membership.
fn assemble_joint_graph_overlay(
    inputs: &GeographicInputs<'_>,
    op: OverlayOp,
    raw_polygon_loops_eligible: bool,
) -> Result<Shape> {
    let graph = TopologyGraph::geographic_many(inputs)?;
    let mut source_graphs = Vec::with_capacity(inputs.views().len());
    for view in inputs.views() {
        source_graphs.push(TopologyGraph::geographic(*view)?);
    }
    let polygons = overlay_polygons(&graph, &source_graphs, op, raw_polygon_loops_eligible)?;
    // Overlay edges and candidate points are numerous for high-resolution
    // boundaries; prepare the retained polygons once instead of rescanning
    // every output ring for each candidate.
    let mut polygon_indexes = Vec::with_capacity(polygons.len());
    for polygon in &polygons {
        polygon_indexes.push(crate::geometry::PolygonPointIndex::build(polygon)?);
    }
    let polygon_edges = output_polygon_edges(&polygons)?;
    let lines = overlay_lines(&graph, &source_graphs, &polygon_indexes, &polygon_edges, op)?;
    let points = overlay_points(&graph, &source_graphs, &polygon_indexes, &lines, op)?;
    build_overlay_shape(points, &lines, &polygons)
}

fn append_overlay_parts(
    shape: &Shape,
    points: &mut Vec<Point>,
    lines: &mut Vec<CoordSeq>,
    polygons: &mut Vec<crate::geometry::Polygon>,
) -> Result<()> {
    shape.try_for_each_component(&mut |component| {
        match component {
            crate::geometry::Component::Point(point) => points.push(point),
            crate::geometry::Component::LineString(line) => {
                lines.push(line.as_coords().clone());
            },
            crate::geometry::Component::Polygon(polygon) => {
                polygons.push(polygon.clone());
            },
        }
        Ok(())
    })
}

#[expect(
    clippy::float_cmp,
    reason = "a pole or antimeridian is one specific representable coordinate, and the question is whether a vertex IS that coordinate; a tolerance would admit vertices merely near it"
)]
fn overlay_polygons(
    graph: &TopologyGraph,
    sources: &[TopologyGraph],
    op: OverlayOp,
    raw_polygon_loops_eligible: bool,
) -> Result<Vec<crate::geometry::Polygon>> {
    if raw_polygon_loops_eligible {
        let loops = source_polygon_loops(sources);
        if !loops.is_empty() {
            let mut polygons = crate::geometry::winding_region(&loops, |winding| winding != 0)?;
            for polygon in &mut polygons {
                *polygon = canonicalize_polygon(polygon, graph.frame)?;
            }
            return Ok(polygons);
        }
    }
    let mut area_edges = Vec::with_capacity(graph.edges.len());
    for (index, edge) in graph
        .edges
        .iter()
        .enumerate()
        .filter(|(_, edge)| graph.edge_has_area_source(edge))
    {
        area_edges.push((index, edge.segment));
    }
    if area_edges.is_empty() {
        return Ok(Vec::new());
    }
    let arranged_edges = if graph
        .nodes
        .iter()
        .any(|node| node.point.y.abs() == graph.frame.pole())
    {
        area_edges
    } else {
        unwrap_geographic_area_edges(graph, graph.frame, &area_edges)
    };
    let mut segments = Vec::with_capacity(arranged_edges.len());
    for &(_, segment) in &arranged_edges {
        segments.push(segment);
    }
    let mut edge_by_endpoints: HashMap<(PointKey, PointKey), usize> =
        HashMap::with_hasher(crate::collections::collection_hash_state()?.clone());
    edge_by_endpoints.reserve(arranged_edges.len());
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
    let arrangement = Arrangement::new_with_rings(&segments)?;
    let mut selected = Vec::with_capacity(arrangement.faces.len());
    let mut area_membership = Vec::new();
    for face in &arrangement.faces {
        if face.ring.len() < 4 || face.area_sign == AreaSign::Zero {
            selected.push(0);
            continue;
        }
        let mut probe = None;
        area_membership.clear();
        area_membership.reserve(sources.len());
        for (source_row, source) in sources.iter().enumerate() {
            let inside = face_area_side(
                &face.ring,
                graph,
                source_row as u32,
                source.area_support.len() == 1,
                &edge_by_endpoints,
            )
            .unwrap_or_else(|| {
                let probe = *probe.get_or_insert_with(|| {
                    face_probe(&face.ring, face.area_sign)
                        .unwrap_or_else(|| longest_edge_midpoint(&face.ring))
                });
                source.contains_area_interior(probe)
            });
            area_membership.push(inside);
        }
        selected.push(i32::from(keep_area_face(&area_membership, op)));
    }
    drop(area_membership);
    let mut regions = crate::geometry::assemble_region_polygons(
        arrangement.region_rings(&selected, |keep| keep != 0),
    )?;
    for polygon in &mut regions {
        *polygon = canonicalize_polygon(polygon, graph.frame)?;
    }
    Ok(regions)
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

/// The meridian crossed by the fewest area edges, taken between consecutive
/// distinct vertex longitudes. An edge that ends on a meridian does not cross
/// it.
fn least_crossed_meridian(
    axis: crate::geographic::PeriodicAxis,
    edges: &[(usize, Segment)],
) -> f64 {
    let canonical = |longitude: f64| {
        axis.canonicalize(longitude)
            .expect("validated geographic longitude")
            .get()
    };
    let mut vertices = Vec::with_capacity(edges.len());
    for &(_, segment) in edges {
        vertices.push(canonical(segment.start.x));
    }
    vertices.sort_unstable_by(f64::total_cmp);
    vertices.dedup();
    let period = axis.period();
    let Some(&first) = vertices.first() else {
        return -axis.half_period();
    };
    // Candidate meridians sit midway between neighbouring vertex longitudes,
    // including the gap that wraps past the seam.
    let mut candidates = Vec::with_capacity(vertices.len());
    for pair in vertices.array_windows::<2>() {
        candidates.push(f64::midpoint(pair[0], pair[1]));
    }
    let last = *vertices.last().expect("nonempty vertices");
    candidates.push(f64::midpoint(last, first + period));
    let mut crossings = vec![0_i64; candidates.len() + 1];
    let add = |from: f64, to: f64, crossings: &mut Vec<i64>| {
        let lo = candidates.partition_point(|&candidate| candidate <= from);
        let hi = candidates.partition_point(|&candidate| candidate < to);
        if lo < hi {
            crossings[lo] += 1;
            crossings[hi] -= 1;
        }
    };
    for &(_, segment) in edges {
        let from = canonical(segment.start.x);
        let delta = axis
            .shortest_edge(segment.start.x, segment.end.x)
            .expect("validated geographic longitude")
            .delta(axis);
        let (lo, hi) = if delta >= 0.0 {
            (from, from + delta)
        } else {
            (from + delta, from)
        };
        // Map the interval onto [first, first + period) and split at the wrap.
        let shift = ((lo - first) / period).floor() * period;
        let (lo, hi) = (lo - shift, hi - shift);
        if hi <= first + period {
            add(lo, hi, &mut crossings);
        } else {
            add(lo, first + period, &mut crossings);
            add(first, hi - period, &mut crossings);
        }
    }
    let mut running = 0;
    let mut best = (i64::MAX, candidates[0]);
    for (index, &candidate) in candidates.iter().enumerate() {
        running += crossings[index];
        if running < best.0 {
            best = (running, candidate);
        }
    }
    best.1
}

fn unwrap_geographic_area_edges(
    graph: &TopologyGraph,
    frame: crate::geographic::GeographicFrame,
    edges: &[(usize, Segment)],
) -> Vec<(usize, Segment)> {
    let axis = frame.longitude();
    let mut adjacency = Vec::with_capacity(graph.nodes.len());
    adjacency.resize_with(graph.nodes.len(), Vec::new);
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

    let mut longitudes = Vec::with_capacity(graph.nodes.len());
    longitudes.resize(graph.nodes.len(), None);
    // Chart extent of each connected boundary component, in walk order.
    let mut components: Vec<(Vec<usize>, f64, f64)> = Vec::new();
    for seed in 0..graph.nodes.len() {
        if longitudes[seed].is_some() || adjacency[seed].is_empty() {
            continue;
        }
        longitudes[seed] = Some(graph.nodes[seed].point.x);
        let mut members = Vec::new();
        let mut stack = Vec::with_capacity(graph.nodes.len());
        stack.push(seed);
        let (mut west, mut east) = (f64::INFINITY, f64::NEG_INFINITY);
        while let Some(node) = stack.pop() {
            let longitude = longitudes[node].expect("assigned chart longitude");
            members.push(node);
            west = west.min(longitude);
            east = east.max(longitude);
            for &(neighbor, delta) in &adjacency[node] {
                if longitudes[neighbor].is_none() {
                    longitudes[neighbor] = Some(longitude + delta);
                    stack.push(neighbor);
                }
            }
        }
        components.push((members, west, east));
    }
    // Each component is unwrapped from its own seed, so a hole or island that
    // shares no boundary with its surroundings can land a full turn away from
    // them. Place every component that the least-crossed meridian does not
    // cut into the one window east of that meridian; the chart is then the
    // sphere cut open along it, and nesting between components survives.
    let period = axis.period();
    let cut = least_crossed_meridian(axis, edges);
    for (members, west, east) in &components {
        let turns = ((west - cut) / period).floor();
        let window_west = cut + turns * period;
        if *east > window_west + period {
            continue;
        }
        let shift = -turns * period;
        if shift != 0.0 {
            for &node in members {
                longitudes[node] = longitudes[node].map(|longitude| longitude + shift);
            }
        }
    }

    let mut unwrapped = Vec::with_capacity(edges.len());
    for &(index, segment) in edges {
        let start = graph
            .node_for(segment.start)
            .expect("area edge start has a graph node")
            .index();
        let end = graph
            .node_for(segment.end)
            .expect("area edge end has a graph node")
            .index();
        unwrapped.push((index, Segment {
            start: XY::new(
                longitudes[start].expect("area edge start has a chart longitude"),
                segment.start.y,
            ),
            end: XY::new(
                longitudes[end].expect("area edge end has a chart longitude"),
                segment.end.y,
            ),
        }));
    }
    unwrapped
}

/// The operands' own polygon rings as winding loops, in the order the graph
/// builder would collect them. [`source_polygon_loops`] reads the same rings
/// back out of built graphs; this reads them from the shapes, so the
/// winding-union lane never has to build one.
fn shape_polygon_loops(inputs: &GeographicInputs<'_>) -> Vec<(Vec<f64>, Vec<f64>)> {
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
    for view in inputs.views() {
        visit(view.shape(), &mut out);
    }
    out
}

fn push_polygon_loops(polygon: &crate::geometry::Polygon, out: &mut Vec<(Vec<f64>, Vec<f64>)>) {
    for ring in polygon.rings() {
        out.push(ring_loop_columns(ring));
    }
}

/// One admitted ring as open `(xs, ys)` columns, without its closing repeat.
fn ring_loop_columns(ring: &crate::geometry::Ring) -> (Vec<f64>, Vec<f64>) {
    let (_, xs) = ring
        .coords()
        .xs()
        .split_last()
        .expect("ring has a closing coordinate");
    let (_, ys) = ring
        .coords()
        .ys()
        .split_last()
        .expect("ring has a closing coordinate");
    (xs.to_vec(), ys.to_vec())
}

fn source_polygon_loops(sources: &[TopologyGraph]) -> Vec<(Vec<f64>, Vec<f64>)> {
    let mut loops = Vec::new();
    for source in sources {
        for support in &source.area_support {
            for ring in support.source().rings() {
                loops.push(ring_loop_columns(ring));
            }
        }
    }
    loops
}

/// The face's membership in one source, read from the source's own edges on
/// the face ring. Being inside any one part puts the face inside the source,
/// but being outside one part does not when another part may cover the face.
fn face_area_side(
    ring: &[XY],
    graph: &TopologyGraph,
    source_row: u32,
    single_part: bool,
    edge_by_endpoints: &HashMap<(PointKey, PointKey), usize>,
) -> Option<bool> {
    let mut outside = false;
    for [start, end] in ring.array_windows::<2>() {
        let key = ordered_edge(PointKey::new(*start), PointKey::new(*end));
        let Some(&edge_index) = edge_by_endpoints.get(&key) else {
            continue;
        };
        let edge = &graph.edges[edge_index];
        match graph.area_source_side_for_row(edge, *start, *end, source_row) {
            Some(true) => return Some(true),
            Some(false) => outside = true,
            None => {},
        }
    }
    (outside && single_part).then_some(false)
}

/// A seam longitude has two spellings, and a stored edge longer than half a
/// turn that ends on the seam means the ring winds the globe. Spell each seam
/// vertex with the alias nearest its incoming off-seam neighbour, so every
/// canonicalized edge keeps the short step the chart edge took.
fn spell_seam_vertices(points: &mut [(Point, bool)], period: f64) {
    let Some(first_off_seam) = points.iter().position(|&(_, on_seam)| !on_seam) else {
        return;
    };
    let mut reference = points[first_off_seam].0.x;
    let len = points.len();
    for offset in 1..len {
        let (point, on_seam) = &mut points[(first_off_seam + offset) % len];
        if *on_seam {
            point.x = [point.x - period, point.x, point.x + period]
                .into_iter()
                .min_by(|a, b| (a - reference).abs().total_cmp(&(b - reference).abs()))
                .expect("three aliases");
        } else {
            reference = point.x;
        }
    }
}

fn canonicalize_polygon(
    polygon: &crate::geometry::Polygon,
    frame: crate::geographic::GeographicFrame,
) -> Result<crate::geometry::Polygon> {
    let axis = frame.longitude();
    let canonicalize = |ring: &crate::geometry::Ring| {
        let mut points = Vec::with_capacity(ring.coord_count());
        for point in ring.coords().iter_coords() {
            let longitude = axis
                .canonicalize(point.x)
                .expect("derived geographic longitude is finite");
            let at_pole = crate::geographic::Latitude::try_new(frame, point.y)
                .expect("derived geographic latitude is admitted")
                .identity(frame)
                == crate::geographic::SeamIdentity::Pole;
            let on_seam =
                longitude.identity(axis) == crate::geographic::SeamIdentity::Seam && !at_pole;
            points.push((Point::new_raw_xy(longitude.get(), point.y), on_seam));
        }
        spell_seam_vertices(&mut points, axis.period());
        let mut xs = Vec::new();
        let mut ys = Vec::new();
        xs.reserve(points.len());
        ys.reserve(points.len());
        for (point, _) in points {
            xs.push(point.x);
            ys.push(point.y);
        }
        crate::geometry::Ring::try_from_closed(CoordSeq::try_from_vecs(xs, ys, None, None)?)
    };
    let shell = canonicalize(&polygon.shell)?;
    let mut holes = Vec::with_capacity(polygon.holes.len());
    for hole in polygon.holes.iter() {
        holes.push(canonicalize(hole)?);
    }
    Ok(crate::geometry::Polygon::new(shell, holes))
}

fn face_probe(ring: &[XY], sign: AreaSign) -> Option<XY> {
    if ring.len() < 4 || sign == AreaSign::Zero {
        return None;
    }
    // Probe just to the left of an edge, which is the side represented by the
    // face walk. Sampling the side rather than the ring centroid keeps nested
    // disconnected cycles in distinct arrangement faces.
    // The probe is accepted only after an exact ring classification confirms
    // that it is on the represented side; a finite point on the wrong side
    // would silently answer membership for a neighboring face.
    let [start, end] = *ring.first_chunk::<2>()?;
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
    // A short edge at a large coordinate can round every smaller offset back
    // onto the edge. Start at one coordinate-scale ULP so halving has a
    // representable point to test.
    let mut offset = (coordinate_scale * 1e-7 * length)
        .max(coordinate_scale * f64::EPSILON)
        .max(f64::from_bits(1));
    loop {
        let probe = XY::new(
            midpoint.x + normal_x * offset,
            midpoint.y + normal_y * offset,
        );
        if !probe.x.is_finite() || !probe.y.is_finite() {
            offset *= 0.5;
            if offset == 0.0 {
                return None;
            }
            continue;
        }
        let class = ring_classify_point(ring, probe.point());
        let lies_in_face = match sign {
            AreaSign::Negative => class == crate::geometry::RingClass::Exterior,
            AreaSign::Zero => false,
            AreaSign::Positive => class == crate::geometry::RingClass::Interior,
        };
        if lies_in_face {
            return Some(probe);
        }
        offset *= 0.5;
        if offset == 0.0 {
            return None;
        }
    }
}

/// Stand-in probe for a sliver face with no representable interior point.
/// The arrangement splits every source edge at every contact, so a face edge's
/// midpoint meets a source's boundary only along that edge, whose side it
/// already reported as exterior; its interior membership is the face's.
fn longest_edge_midpoint(ring: &[XY]) -> XY {
    let [start, end] = ring
        .array_windows::<2>()
        .max_by(|[a, b], [c, d]| {
            (b.x - a.x)
                .hypot(b.y - a.y)
                .total_cmp(&(d.x - c.x).hypot(d.y - c.y))
        })
        .copied()
        .expect("a nondegenerate face ring has edges");
    XY::new(f64::midpoint(start.x, end.x), f64::midpoint(start.y, end.y))
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

fn output_polygon_edges(
    polygons: &[crate::geometry::Polygon],
) -> Result<HashSet<(PointKey, PointKey)>> {
    let capacity = polygons.iter().try_fold(0_usize, |count, polygon| {
        count
            .checked_add(polygon.coord_count())
            .ok_or(ResourceError::SizeOverflow)
    })?;
    let mut edges = HashSet::with_hasher(crate::collections::collection_hash_state()?.clone());
    edges.reserve(capacity);
    for polygon in polygons {
        for ring in polygon.rings() {
            for [start, end] in ring.segment_pairs() {
                edges.insert(ordered_edge(PointKey::new(start), PointKey::new(end)));
            }
        }
    }
    Ok(edges)
}

fn overlay_lines(
    graph: &TopologyGraph,
    sources: &[TopologyGraph],
    polygon_indexes: &[crate::geometry::PolygonPointIndex],
    polygon_edges: &HashSet<(PointKey, PointKey)>,
    op: OverlayOp,
) -> Result<Vec<CoordSeq>> {
    let mut lines = Vec::with_capacity(graph.edges.len());
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
        let mut all_covered = true;
        let mut other_covered = false;
        let mut any_line = false;
        let mut first_line = false;
        let mut odd_lines = false;
        for (source_row, source) in sources.iter().enumerate() {
            let covered = source.locate_edge_piece(edge.segment) != TopologyLocation::Exterior
                || graph.edge_has_line_source_for_row(edge, source_row as u32);
            let line = source.has_line_support(midpoint.xy())
                || graph.edge_has_line_source_for_row(edge, source_row as u32);
            all_covered &= covered;
            if source_row == 0 {
                first_line = line;
            } else {
                other_covered |= covered;
            }
            any_line |= line;
            odd_lines ^= line;
        }
        let result_covers = polygon_indexes.iter().try_fold(false, |covered, polygon| {
            Ok::<_, crate::error::Error>(covered || polygon.covers_point(midpoint)?)
        })?;
        let keep = match op {
            OverlayOp::Intersection => all_covered && !result_covers,
            OverlayOp::Union => any_line && !result_covers,
            OverlayOp::Difference => first_line && !other_covered && !result_covers,
            OverlayOp::SymmetricDifference => odd_lines && !result_covers,
        };
        if keep {
            lines.push(CoordSeq::try_from_xy(&[
                edge.segment.start,
                edge.segment.end,
            ])?);
        }
    }
    Ok(lines)
}

fn overlay_points(
    graph: &TopologyGraph,
    sources: &[TopologyGraph],
    polygon_indexes: &[crate::geometry::PolygonPointIndex],
    lines: &[CoordSeq],
    op: OverlayOp,
) -> Result<Vec<Point>> {
    let candidate_capacity = graph
        .source_points
        .len()
        .checked_add(if op == OverlayOp::Intersection {
            graph.nodes.len()
        } else {
            0
        })
        .ok_or(ResourceError::SizeOverflow)?;
    let mut kept = Vec::new();
    let mut membership = Vec::new();
    let mut initialized = false;
    let mut visit = |point: Point| -> Result<()> {
        if !initialized {
            kept.reserve(candidate_capacity);
            membership.reserve(sources.len());
            initialized = true;
        }
        membership.clear();
        for (source_row, source) in sources.iter().enumerate() {
            let value = source.locate_point_for_overlay(point.xy()) != TopologyLocation::Exterior
                || (op == OverlayOp::Intersection
                    && graph.has_line_source_at_node(point.xy(), source_row as u32));
            membership.push(value);
        }
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
        let covered = polygon_indexes.iter().try_fold(false, |covered, polygon| {
            Ok::<_, crate::error::Error>(covered || polygon.covers_point(point)?)
        })?;
        if keep
            && !covered
            && !lines.iter().any(|line| {
                line.segment_pairs()
                    .any(|[start, end]| point_on_segment(point, start, end))
            })
        {
            kept.push(point);
        }
        Ok(())
    };
    for source in graph
        .source_points
        .iter()
        .filter(|source| !source.collapsed_line)
    {
        visit(source.point.xy().point())?;
    }
    if op == OverlayOp::Intersection {
        for node in &graph.nodes {
            visit(node.point.point())?;
        }
    }
    drop(visit);
    drop(membership);
    if !initialized {
        return Ok(Vec::new());
    }
    dedup_points(&mut kept)?;
    Ok(kept)
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

pub(crate) const fn empty_shape_for_dimension_with_axes(
    dimension: Dimension,
    axes: CoordinateAxes,
) -> Shape {
    match dimension {
        Dimension::Point => Shape::typed_empty(GeometryKind::Point, axes),
        Dimension::Curve => Shape::typed_empty(GeometryKind::LineString, axes),
        Dimension::Surface => Shape::typed_empty(GeometryKind::Polygon, axes),
    }
}
