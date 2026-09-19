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
    relate, same_point, segment_midpoint,
};
use crate::try_push_vec;

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
    let shape = assemble_overlay(OverlayInputs::Cartesian(&refs), op, false)?;
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
pub(super) fn binary_intersection_joint_graph(
    left: &Shape,
    right: &Shape,
    strictness: crate::geometry::Strictness,
) -> Result<Shape> {
    let mut sources = [left.normalize()?, right.normalize()?];
    sources.sort_unstable_by(crate::geometry::compare_shapes);
    let refs: Vec<&Shape> = sources.iter().collect();
    let shape = assemble_joint_graph_overlay(
        OverlayInputs::Cartesian(&refs),
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

    match domain {
        TopologyDomain::Cartesian => {
            let mut refs = Vec::new();
            crate::try_reserve_vec(&mut refs, geometries.len())?;
            refs.extend(geometries.iter().map(std::borrow::Borrow::borrow));
            exact_cartesian_overlay(&refs, op, strictness, bounds_certificates)
        },
        TopologyDomain::Geographic(frame) => {
            let originals = GeographicInputs::try_from_shapes(
                frame,
                geometries.iter().map(std::borrow::Borrow::borrow),
            )
            .map_err(overlay_view_error)?;
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
    inputs: &[&Shape],
    operation: OverlayOp,
    strictness: Strictness,
    bounds_certificates: bool,
) -> Result<Shape> {
    if inputs.len() == 1 && operation == OverlayOp::Intersection {
        return Ok(inputs[0].clone());
    }
    let mut ordered = Vec::new();
    crate::try_reserve_vec(&mut ordered, inputs.len())?;
    ordered.extend_from_slice(inputs);
    if operation == OverlayOp::Difference {
        ordered[1..].sort_unstable_by(|left, right| crate::geometry::compare_shapes(left, right));
    } else {
        ordered.sort_unstable_by(|left, right| crate::geometry::compare_shapes(left, right));
    }
    let inputs = ordered.as_slice();
    // Declared axes are a fact about the whole input, not any one component.
    let declared_axes = overlay_declared_axes(inputs, operation);
    let strict = strictness.is_strict();
    let components = if bounds_certificates && inputs.len() > 1 {
        let mut bounds = Vec::new();
        crate::try_reserve_vec(&mut bounds, inputs.len())?;
        for shape in inputs {
            try_push_vec(&mut bounds, shape.bounds())?;
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
            let mut shapes = Vec::new();
            crate::try_reserve_vec(&mut shapes, components.len())?;
            let mut subset = Vec::new();
            for component in components {
                subset.clear();
                crate::try_reserve_vec(&mut subset, component.len())?;
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
            let mut holes = Vec::new();
            crate::try_reserve_vec(&mut holes, polygon.holes.len())?;
            for hole in polygon.holes.iter() {
                holes.push(crate::geometry::Ring::from_closed(restricted(
                    hole.coords(),
                    polygon_axes,
                )?));
            }
            let shell = crate::geometry::Ring::from_closed(restricted(
                polygon.shell.coords(),
                polygon_axes,
            )?);
            *polygon = crate::geometry::Polygon::new(shell, holes)?;
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
        .map_err(overlay_view_error)?;
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
    let bounds_facts = overlay_bounds_facts_geographic(originals, op)?;
    let certify = bounds_certificates
        && originals.views().len() > 1
        && bounds_facts.raw_coordinate_bounds_sound;
    if certify && op == OverlayOp::Intersection && intersection_bounds_empty(&bounds_facts.bounds) {
        let mut inputs = Vec::new();
        crate::try_reserve_vec(&mut inputs, originals.views().len())?;
        for view in originals.views() {
            try_push_vec(&mut inputs, view.shape())?;
        }
        return Ok(empty_nary_overlay_shape(&inputs, op));
    }

    let mut sources = Vec::new();
    sources
        .try_reserve_exact(originals.views().len())
        .map_err(crate::error::Error::from)?;
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

    let mut refs = Vec::new();
    crate::try_reserve_vec(&mut refs, sources.len())?;
    refs.extend(sources.iter());
    let noded = GeographicInputs::try_from_shapes(frame, refs.iter().copied())
        .map_err(overlay_view_error)?;
    if originals.views().len() == 1
        && op == OverlayOp::Intersection
        && !crate::geometry::crosses_antimeridian_in_frame(originals.views()[0].shape(), frame)
    {
        return Ok(originals.views()[0].shape().clone());
    }
    let components = if certify {
        let mut bounds = Vec::new();
        crate::try_reserve_vec(&mut bounds, refs.len())?;
        for shape in &refs {
            try_push_vec(&mut bounds, shape.bounds())?;
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
            let subset = noded.try_selected(component).map_err(overlay_view_error)?;
            let shape = assemble_overlay(
                OverlayInputs::Geographic(&subset),
                op,
                raw_polygon_loops_eligible,
            )?;
            append_overlay_parts(&shape, &mut points, &mut lines, &mut polygons)?;
        }
        build_overlay_shape(points, &lines, &polygons)?
    } else {
        assemble_overlay(
            OverlayInputs::Geographic(&noded),
            op,
            raw_polygon_loops_eligible,
        )?
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

#[derive(Clone, Copy)]
enum OverlayInputs<'a, 's> {
    #[cfg_attr(
        not(test),
        expect(
            dead_code,
            reason = "Cartesian overlays run on the exact partition; the Cartesian graph route remains its differential test oracle"
        )
    )]
    Cartesian(&'a [&'s Shape]),
    Geographic(&'a GeographicInputs<'s>),
}

impl<'s> OverlayInputs<'_, 's> {
    const fn domain(self) -> TopologyDomain {
        match self {
            Self::Cartesian(_) => TopologyDomain::Cartesian,
            Self::Geographic(inputs) => TopologyDomain::Geographic(inputs.frame()),
        }
    }

    fn len(self) -> usize {
        match self {
            Self::Cartesian(inputs) => inputs.len(),
            Self::Geographic(inputs) => inputs.views().len(),
        }
    }

    fn shape(self, index: usize) -> &'s Shape {
        match self {
            Self::Cartesian(inputs) => inputs[index],
            Self::Geographic(inputs) => inputs.views()[index].shape(),
        }
    }
}

pub(in crate::geometry) fn overlay_view_error(error: GeographicViewError) -> crate::error::Error {
    match error {
        GeographicViewError::Resource => ResourceError::Allocation.into(),
        GeographicViewError::Diagnostic(diagnostic) => {
            invalid_lonlat_error(diagnostic.native.x, diagnostic.native.y)
        },
    }
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
) -> Result<OverlayBoundsFacts> {
    let mut bounds = Vec::new();
    crate::try_reserve_vec(&mut bounds, originals.views().len())?;
    for view in originals.views() {
        try_push_vec(&mut bounds, view.shape().bounds())?;
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
    Ok(overlay_bounds_facts_from(
        bounds,
        raw_coordinate_bounds_sound,
        op,
        originals
            .views()
            .iter()
            .any(|view| has_nonempty_point_or_line_support(view.shape())),
    ))
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
    let mut parent = Vec::new();
    crate::try_reserve_vec(&mut parent, count)?;
    parent.extend(0..count);
    let _ = crate::geometry::for_each_indexed_overlapping_bounds_pair(bounds, |left, right| {
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
        let mut component = Vec::new();
        crate::try_reserve_vec(&mut component, left)?;
        component.extend((0..count).filter(|&node| parent[node] == 0));
        try_push_vec(&mut components, component)?;
        return Ok(Some(components));
    }
    let roots = (0..count).filter(|&node| parent[node] == node).count();
    if roots == 1 {
        return Ok(None);
    }
    let mut slot_of_root = Vec::new();
    crate::try_reserve_vec(&mut slot_of_root, count)?;
    slot_of_root.resize(count, usize::MAX);
    crate::try_reserve_vec(&mut components, roots)?;
    for (node, &root) in parent.iter().enumerate() {
        if slot_of_root[root] == usize::MAX {
            slot_of_root[root] = components.len();
            components.push(Vec::new());
        }
        try_push_vec(&mut components[slot_of_root[root]], node)?;
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
    inputs: OverlayInputs<'_, '_>,
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
    // Intersection also needs the arrangement's boundary contacts: an
    // area-only answer would drop touching lines and isolated points.
    if let OverlayInputs::Cartesian(&[left, right]) = inputs
        && let (Some(left_polygons), Some(right_polygons)) =
            (areal_only_polygons(left)?, areal_only_polygons(right)?)
        && !left_polygons.is_empty()
        && !right_polygons.is_empty()
    {
        if op != OverlayOp::Intersection {
            return build_overlay_shape(
                Vec::new(),
                &[],
                &crate::geometry::binary_areal_overlay(&left_polygons, &right_polygons, op)?,
            );
        }
        // Intersection of two areal operands is mixed-dimension: boundaries
        // that touch yield lines and points. The same arrangement carries
        // them (it is how the native areal relate reads its boundary rows),
        // for rings the areal engines can reason about; a collapsed or
        // zero-length ring keeps the graph path, which retains such
        // degeneracies as boundary.
        if left_polygons
            .iter()
            .chain(&right_polygons)
            .all(|polygon| polygon.rings().all(relate::ring_is_sound))
        {
            let (points, lines, polygons) =
                super::build_areal_arrangement(&left_polygons, &right_polygons)?
                    .intersection_parts()?;
            return build_overlay_shape(points, &lines, &polygons);
        }
    }
    if raw_polygon_loops_eligible {
        let loops = shape_polygon_loops(inputs)?;
        if !loops.is_empty() {
            let polygons = crate::geometry::winding_region(&loops, |winding| winding != 0)?
                .into_iter()
                .map(|polygon| canonicalize_polygon(polygon, inputs.domain()))
                .collect::<Result<Vec<_>>>()?;
            return build_overlay_shape(Vec::new(), &[], &polygons);
        }
    }
    assemble_joint_graph_overlay(inputs, op, raw_polygon_loops_eligible)
}

/// Mixed-dimensional overlay from joint noding and exact face-side membership.
fn assemble_joint_graph_overlay(
    inputs: OverlayInputs<'_, '_>,
    op: OverlayOp,
    raw_polygon_loops_eligible: bool,
) -> Result<Shape> {
    let graph = match inputs {
        OverlayInputs::Cartesian(shapes) => TopologyGraph::cartesian_many(shapes),
        OverlayInputs::Geographic(batch) => TopologyGraph::geographic_many(batch),
    }?;
    let mut source_graphs = Vec::new();
    crate::try_reserve_vec(&mut source_graphs, inputs.len())?;
    match inputs {
        OverlayInputs::Cartesian(shapes) => {
            for shape in shapes.iter().copied() {
                try_push_vec(&mut source_graphs, TopologyGraph::cartesian(shape)?)?;
            }
        },
        OverlayInputs::Geographic(batch) => {
            for view in batch.views() {
                try_push_vec(&mut source_graphs, TopologyGraph::geographic(*view)?)?;
            }
        },
    }
    let polygons = overlay_polygons(&graph, &source_graphs, op, raw_polygon_loops_eligible)?;
    // Overlay edges and candidate points are numerous for high-resolution
    // boundaries; prepare the retained polygons once instead of rescanning
    // every output ring for each candidate.
    let mut polygon_indexes = Vec::new();
    crate::try_reserve_vec(&mut polygon_indexes, polygons.len())?;
    for polygon in &polygons {
        polygon_indexes.push(crate::geometry::PolygonPointIndex::build(polygon)?);
    }
    let polygon_edges = output_polygon_edges(&polygons)?;
    let lines = overlay_lines(&graph, &source_graphs, &polygon_indexes, &polygon_edges, op)?;
    let points = overlay_points(&graph, &source_graphs, &polygon_indexes, &lines, op)?;
    build_overlay_shape(points, &lines, &polygons)
}

#[cfg(test)]
pub(super) fn nondegenerate_line_only(shape: &Shape) -> bool {
    match shape {
        Shape::LineString(line) => relate::line_has_nonzero_segment(line),
        Shape::MultiLineString(lines) => {
            !lines.has_empty_members()
                && lines
                    .iter()
                    .all(|line| relate::line_has_nonzero_segment(line))
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

fn append_overlay_parts(
    shape: &Shape,
    points: &mut Vec<Point>,
    lines: &mut Vec<CoordSeq>,
    polygons: &mut Vec<crate::geometry::Polygon>,
) -> Result<()> {
    match shape {
        Shape::Point(point) => try_push_vec(points, *point)?,
        Shape::MultiPoint(multi) => {
            for point in multi {
                try_push_vec(points, point)?;
            }
        },
        Shape::LineString(line) => try_push_vec(lines, line.as_coords().clone())?,
        Shape::MultiLineString(multi) => {
            for line in multi.iter() {
                try_push_vec(lines, line.as_coords().clone())?;
            }
        },
        Shape::Polygon(polygon) => try_push_vec(polygons, polygon.clone())?,
        Shape::MultiPolygon(multi) => {
            for polygon in multi.iter() {
                try_push_vec(polygons, polygon.clone())?;
            }
        },
        Shape::GeometryCollection(geometries) => {
            for geometry in geometries {
                append_overlay_parts(geometry, points, lines, polygons)?;
            }
        },
        Shape::Empty(..) => {},
    }
    Ok(())
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
        let loops = source_polygon_loops(sources)?;
        if !loops.is_empty() {
            let polygons = crate::geometry::winding_region(&loops, |winding| winding != 0)?;
            let mut canonical = Vec::new();
            crate::try_reserve_vec(&mut canonical, polygons.len())?;
            for polygon in polygons {
                try_push_vec(&mut canonical, canonicalize_polygon(polygon, graph.domain)?)?;
            }
            return Ok(canonical);
        }
    }
    let mut area_edges = Vec::new();
    crate::try_reserve_vec(&mut area_edges, graph.edges.len())?;
    for (index, edge) in graph
        .edges
        .iter()
        .enumerate()
        .filter(|(_, edge)| graph.edge_has_area_source(edge))
    {
        try_push_vec(&mut area_edges, (index, edge.segment))?;
    }
    if area_edges.is_empty() {
        return Ok(Vec::new());
    }
    let arranged_edges = match graph.domain {
        TopologyDomain::Geographic(frame)
            if !graph
                .nodes
                .iter()
                .any(|node| node.point.y.abs() == frame.pole()) =>
        {
            unwrap_geographic_area_edges(graph, frame, &area_edges)?
        },
        TopologyDomain::Cartesian | TopologyDomain::Geographic(_) => area_edges,
    };
    let mut segments = Vec::new();
    crate::try_reserve_vec(&mut segments, arranged_edges.len())?;
    for &(_, segment) in &arranged_edges {
        try_push_vec(&mut segments, segment)?;
    }
    let mut edge_by_endpoints: HashMap<(PointKey, PointKey), usize> =
        HashMap::with_hasher(crate::collections::collection_hash_state()?.clone());
    edge_by_endpoints
        .try_reserve(arranged_edges.len())
        .map_err(crate::error::Error::from)?;
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
    let mut selected = Vec::new();
    crate::try_reserve_vec(&mut selected, arrangement.faces.len())?;
    for face in &arrangement.faces {
        if face.ring.len() < 4 || face.area_sign == AreaSign::Zero {
            try_push_vec(&mut selected, 0)?;
            continue;
        }
        let mut probe = None;
        let mut area_membership = Vec::new();
        crate::try_reserve_vec(&mut area_membership, sources.len())?;
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
            try_push_vec(&mut area_membership, inside)?;
        }
        try_push_vec(
            &mut selected,
            i32::from(keep_area_face(&area_membership, op)),
        )?;
    }
    let regions = crate::geometry::assemble_region_polygons(
        arrangement.region_rings(&selected, |keep| keep != 0),
    )?;
    let mut canonical = Vec::new();
    crate::try_reserve_vec(&mut canonical, regions.len())?;
    for polygon in regions {
        try_push_vec(&mut canonical, canonicalize_polygon(polygon, graph.domain)?)?;
    }
    Ok(canonical)
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
) -> Result<f64> {
    let canonical = |longitude: f64| {
        axis.canonicalize(longitude)
            .expect("validated geographic longitude")
            .get()
    };
    let mut vertices = Vec::new();
    crate::try_reserve_vec(&mut vertices, edges.len())?;
    for &(_, segment) in edges {
        try_push_vec(&mut vertices, canonical(segment.start.x))?;
    }
    vertices.sort_by(f64::total_cmp);
    vertices.dedup();
    let period = axis.period();
    let Some(&first) = vertices.first() else {
        return Ok(-axis.half_period());
    };
    // Candidate meridians sit midway between neighbouring vertex longitudes,
    // including the gap that wraps past the seam.
    let mut candidates = Vec::new();
    crate::try_reserve_vec(&mut candidates, vertices.len())?;
    for pair in vertices.array_windows::<2>() {
        try_push_vec(&mut candidates, f64::midpoint(pair[0], pair[1]))?;
    }
    let last = *vertices.last().expect("nonempty vertices");
    try_push_vec(&mut candidates, f64::midpoint(last, first + period))?;
    let mut crossings = Vec::new();
    crate::try_reserve_vec(&mut crossings, candidates.len() + 1)?;
    crossings.resize(candidates.len() + 1, 0_i64);
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
    Ok(best.1)
}

fn unwrap_geographic_area_edges(
    graph: &TopologyGraph,
    frame: crate::geographic::GeographicFrame,
    edges: &[(usize, Segment)],
) -> Result<Vec<(usize, Segment)>> {
    let axis = frame.longitude();
    let mut adjacency = Vec::new();
    crate::try_reserve_vec(&mut adjacency, graph.nodes.len())?;
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
        try_push_vec(&mut adjacency[start], (end, delta))?;
        try_push_vec(&mut adjacency[end], (start, -delta))?;
    }

    let mut longitudes = Vec::new();
    crate::try_reserve_vec(&mut longitudes, graph.nodes.len())?;
    longitudes.resize(graph.nodes.len(), None);
    // Chart extent of each connected boundary component, in walk order.
    let mut components: Vec<(Vec<usize>, f64, f64)> = Vec::new();
    for seed in 0..graph.nodes.len() {
        if longitudes[seed].is_some() || adjacency[seed].is_empty() {
            continue;
        }
        longitudes[seed] = Some(graph.nodes[seed].point.x);
        let mut members = Vec::new();
        let mut stack = Vec::new();
        crate::try_reserve_vec(&mut stack, graph.nodes.len())?;
        try_push_vec(&mut stack, seed)?;
        let (mut west, mut east) = (f64::INFINITY, f64::NEG_INFINITY);
        while let Some(node) = stack.pop() {
            let longitude = longitudes[node].expect("assigned chart longitude");
            try_push_vec(&mut members, node)?;
            west = west.min(longitude);
            east = east.max(longitude);
            for &(neighbor, delta) in &adjacency[node] {
                if longitudes[neighbor].is_none() {
                    longitudes[neighbor] = Some(longitude + delta);
                    try_push_vec(&mut stack, neighbor)?;
                }
            }
        }
        try_push_vec(&mut components, (members, west, east))?;
    }
    // Each component is unwrapped from its own seed, so a hole or island that
    // shares no boundary with its surroundings can land a full turn away from
    // them. Place every component that the least-crossed meridian does not
    // cut into the one window east of that meridian; the chart is then the
    // sphere cut open along it, and nesting between components survives.
    let period = axis.period();
    let cut = least_crossed_meridian(axis, edges)?;
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

    let mut unwrapped = Vec::new();
    crate::try_reserve_vec(&mut unwrapped, edges.len())?;
    for &(index, segment) in edges {
        let start = graph
            .node_for(segment.start)
            .expect("area edge start has a graph node")
            .index();
        let end = graph
            .node_for(segment.end)
            .expect("area edge end has a graph node")
            .index();
        try_push_vec(
            &mut unwrapped,
            (index, Segment {
                start: XY::new(
                    longitudes[start].expect("area edge start has a chart longitude"),
                    segment.start.y,
                ),
                end: XY::new(
                    longitudes[end].expect("area edge end has a chart longitude"),
                    segment.end.y,
                ),
            }),
        )?;
    }
    Ok(unwrapped)
}

/// The shape's polygons, when it carries area and nothing else. `None` for an
/// operand with any point or lineal support, which the joint winding
/// arrangement has no representation for.
fn areal_only_polygons(shape: &Shape) -> Result<Option<Vec<&crate::geometry::Polygon>>> {
    fn visit<'a>(shape: &'a Shape, out: &mut Vec<&'a crate::geometry::Polygon>) -> Result<bool> {
        match shape {
            Shape::Polygon(polygon) => {
                try_push_vec(out, polygon)?;
                Ok(true)
            },
            Shape::MultiPolygon(polygons) => {
                for polygon in polygons.iter() {
                    try_push_vec(out, polygon)?;
                }
                Ok(true)
            },
            Shape::GeometryCollection(parts) => {
                for part in parts {
                    if !visit(part, out)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            },
            Shape::Empty(..) => Ok(true),
            Shape::Point(_)
            | Shape::MultiPoint(_)
            | Shape::LineString(_)
            | Shape::MultiLineString(_) => Ok(false),
        }
    }

    let mut out = Vec::new();
    Ok(visit(shape, &mut out)?.then_some(out))
}

/// The operands' own polygon rings as winding loops, in the order the graph
/// builder would collect them. [`source_polygon_loops`] reads the same rings
/// back out of built graphs; this reads them from the shapes, so the
/// winding-union lane never has to build one.
fn shape_polygon_loops(inputs: OverlayInputs<'_, '_>) -> Result<Vec<(Vec<f64>, Vec<f64>)>> {
    fn visit(shape: &Shape, out: &mut Vec<(Vec<f64>, Vec<f64>)>) -> Result<()> {
        match shape {
            Shape::Polygon(polygon) => push_polygon_loops(polygon, out)?,
            Shape::MultiPolygon(polygons) => {
                for polygon in polygons {
                    push_polygon_loops(polygon, out)?;
                }
            },
            Shape::GeometryCollection(parts) => {
                for part in parts {
                    visit(part, out)?;
                }
            },
            _ => {},
        }
        Ok(())
    }

    let mut out = Vec::new();
    for index in 0..inputs.len() {
        visit(inputs.shape(index), &mut out)?;
    }
    Ok(out)
}

fn push_polygon_loops(
    polygon: &crate::geometry::Polygon,
    out: &mut Vec<(Vec<f64>, Vec<f64>)>,
) -> Result<()> {
    for ring in polygon.rings() {
        if let Some(loop_columns) = ring_loop_columns(ring)? {
            try_push_vec(out, loop_columns)?;
        }
    }
    Ok(())
}

/// One ring as open `(xs, ys)` columns — the closing repeat dropped, and
/// nothing shorter than a triangle.
fn ring_loop_columns(ring: &crate::geometry::Ring) -> Result<Option<(Vec<f64>, Vec<f64>)>> {
    let mut points = Vec::new();
    crate::try_reserve_vec(&mut points, ring.coord_count())?;
    for point in ring.iter_coords() {
        try_push_vec(&mut points, point)?;
    }
    if points.len() > 1 && same_point(points[0], *points.last().expect("non-empty ring")) {
        points.pop();
    }
    if points.len() < 3 {
        return Ok(None);
    }
    let mut xs = Vec::new();
    let mut ys = Vec::new();
    crate::try_reserve_pair(&mut xs, &mut ys, points.len())?;
    for point in points {
        try_push_vec(&mut xs, point.x)?;
        try_push_vec(&mut ys, point.y)?;
    }
    Ok(Some((xs, ys)))
}

fn source_polygon_loops(sources: &[TopologyGraph]) -> Result<Vec<(Vec<f64>, Vec<f64>)>> {
    let mut loops = Vec::new();
    for source in sources {
        for support in &source.area_support {
            for ring in support.source().rings() {
                if let Some(columns) = ring_loop_columns(ring)? {
                    try_push_vec(&mut loops, columns)?;
                }
            }
        }
    }
    Ok(loops)
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
    polygon: crate::geometry::Polygon,
    domain: TopologyDomain,
) -> Result<crate::geometry::Polygon> {
    let TopologyDomain::Geographic(frame) = domain else {
        return Ok(polygon);
    };
    let axis = frame.longitude();
    let canonicalize = |ring: &crate::geometry::Ring| {
        let mut points = Vec::new();
        crate::try_reserve_vec(&mut points, ring.coord_count())?;
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
            try_push_vec(
                &mut points,
                (Point::new_raw_xy(longitude.get(), point.y), on_seam),
            )?;
        }
        spell_seam_vertices(&mut points, axis.period());
        let points: Vec<Point> = points.into_iter().map(|(point, _)| point).collect();
        CoordSeq::try_from_points(&points).map(crate::geometry::Ring::from_closed)
    };
    let shell = canonicalize(&polygon.shell)?;
    let mut holes = Vec::new();
    crate::try_reserve_vec(&mut holes, polygon.holes.len())?;
    for hole in polygon.holes.iter() {
        try_push_vec(&mut holes, canonicalize(hole)?)?;
    }
    crate::geometry::Polygon::new(shell, holes)
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
    edges
        .try_reserve(capacity)
        .map_err(crate::error::Error::from)?;
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
    let mut lines = Vec::new();
    crate::try_reserve_vec(&mut lines, graph.edges.len())?;
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
        let mut covered = Vec::new();
        crate::try_reserve_vec(&mut covered, sources.len())?;
        let mut line_membership = Vec::new();
        crate::try_reserve_vec(&mut line_membership, sources.len())?;
        for (source_row, source) in sources.iter().enumerate() {
            try_push_vec(
                &mut covered,
                source.locate_edge_piece(edge.segment) != TopologyLocation::Exterior
                    || graph.edge_has_line_source_for_row(edge, source_row as u32),
            )?;
            try_push_vec(
                &mut line_membership,
                source.has_line_support(midpoint.xy())
                    || graph.edge_has_line_source_for_row(edge, source_row as u32),
            )?;
        }
        let result_covers = polygon_indexes.iter().try_fold(false, |covered, polygon| {
            Ok::<_, crate::error::Error>(covered || polygon.covers_point(midpoint)?)
        })?;
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
            try_push_vec(
                &mut lines,
                CoordSeq::try_from_xy(&[edge.segment.start, edge.segment.end])?,
            )?;
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
    let mut candidates = Vec::new();
    crate::try_reserve_vec(&mut candidates, candidate_capacity)?;
    for source in graph
        .source_points
        .iter()
        .filter(|source| !source.collapsed_line)
    {
        try_push_vec(&mut candidates, source.point.xy().point())?;
    }
    if op == OverlayOp::Intersection {
        for node in &graph.nodes {
            try_push_vec(&mut candidates, node.point.point())?;
        }
    }
    let mut kept = Vec::new();
    crate::try_reserve_vec(&mut kept, candidates.len())?;
    for point in candidates {
        let mut membership = Vec::new();
        crate::try_reserve_vec(&mut membership, sources.len())?;
        for (source_row, source) in sources.iter().enumerate() {
            let value = source.locate_point_for_overlay(point.xy()) != TopologyLocation::Exterior
                || (op == OverlayOp::Intersection
                    && graph.has_line_source_at_node(point.xy(), source_row as u32));
            try_push_vec(&mut membership, value)?;
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
            try_push_vec(&mut kept, point)?;
        }
    }
    let mut candidates = kept;
    dedup_points(&mut candidates)?;
    Ok(candidates)
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
