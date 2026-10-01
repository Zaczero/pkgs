use std::borrow::Borrow;
use std::ops::ControlFlow;

use crate::boundary::geographic::invalid_lonlat_error;
use crate::error::{ResourceError, Result};
use crate::geographic::{GeographicInputs, GeographicViewError};
use crate::geometry::overlay::{
    ExactSourceInventory, OverlayOp, Strictness, build_certified_overlay_shape, merge_chains,
    overlay_declared_axes,
};
use crate::geometry::segments::crossing_point;
use crate::geometry::tessellation::exact::ExactDyadic;
use crate::geometry::topology::overlay::exact_geographic_overlay;
use crate::geometry::{
    Bounds, Contact, CoordSeq, CoordinateAxes, Coordinates as _, Dimension,
    GeographicTopologyRoute, GeometryErrorKind, GeometryKind, HashSet, Loc, Point, PointKey,
    PreparedPlan, Segment, SegmentParameter, Shape, SharedShapeOwners, TopologyDomain, XY,
    carry_overlay_ordinates, cartesian_point_location, clip_segment_parameter,
    exact_crossing_parameter, for_each_candidate_pair, point_on_segment, segment_contact_exact,
    undirected_segment_edge_key, unique_xy_points,
};

/// Canonical operand order for every overlay entry: difference keeps its left
/// operand first and orders the subtrahends; other operations order all
/// operands. A macro so the generic owner keeps its original single body on the
/// ordinary path; a helper call there measured as overhead on every overlay.
macro_rules! sort_overlay_inputs {
    ($inputs:expr, $operation:expr) => {
        if $operation == OverlayOp::Difference {
            let (_, right) = ($inputs)
                .split_first_mut()
                .expect("difference input includes its left operand");
            right.sort_unstable_by(|left, right| crate::geometry::compare_shapes(left, right));
        } else {
            ($inputs).sort_unstable_by(|left, right| crate::geometry::compare_shapes(left, right));
        }
    };
}

#[derive(Clone, Copy)]
enum CartesianBinaryLane {
    Puntal,
    LinePolygon,
}

/// Admit a binary Cartesian pair to a dimension-specific lane from stored kinds
/// alone, so ineligible pairs pay only this branch before the generic owner.
/// Each lane re-checks its operands and falls back to the exact partition when
/// it cannot answer exactly, so this is admission, not a semantic decision.
macro_rules! cartesian_binary_lane {
    ($left:expr, $right:expr, $operation:expr) => {{
        let left = $left;
        let right = $right;
        let operation = $operation;
        if left.topological_dimension() == Dimension::Point
            || right.topological_dimension() == Dimension::Point
        {
            Some(CartesianBinaryLane::Puntal)
        } else {
            match operation {
                OverlayOp::Intersection
                    if (is_line_operand(left) && is_polygon_operand(right))
                        || (is_line_operand(right) && is_polygon_operand(left)) =>
                {
                    Some(CartesianBinaryLane::LinePolygon)
                },
                OverlayOp::Difference if is_line_operand(left) && is_polygon_operand(right) => {
                    Some(CartesianBinaryLane::LinePolygon)
                },
                _ => None,
            }
        }
    }};
}

/// Evaluate one or more operands in the canonical overlay owner.
///
/// Binary Cartesian puntal and line/polygon pairs use dimension-specific lanes;
/// all other inputs retain exact partition or geographic quotient semantics.
pub(crate) fn overlay_all<S: Borrow<Shape>>(
    geometries: &[S],
    domain: TopologyDomain,
    op: OverlayOp,
    strictness: Strictness,
) -> Result<Shape> {
    if matches!(&domain, TopologyDomain::Cartesian)
        && let [left, right] = geometries
    {
        let left = <S as Borrow<Shape>>::borrow(left);
        let right = <S as Borrow<Shape>>::borrow(right);
        if let Some(lane) = cartesian_binary_lane!(left, right, op) {
            let mut inputs = [left, right];
            return exact_cartesian_binary_overlay(
                &mut inputs,
                lane,
                op,
                strictness,
                true,
                ExactPartitionBudget::Charge,
            );
        }
    }
    overlay_all_with_bounds_certificates(geometries, domain, op, strictness, true)
}

/// Overlay already-charted geographic split parts with exact planar topology
/// while retaining vertices that still define geographic metric edges.
pub(crate) fn overlay_all_geographic_chart<S: Borrow<Shape>>(
    geometries: &[S],
    op: OverlayOp,
    strictness: Strictness,
) -> Result<Shape> {
    if geometries.is_empty() {
        return Err(GeometryErrorKind::EmptyGeometrySequence {
            operation: op.name(),
        }
        .into());
    }
    let mut inputs = Vec::with_capacity(geometries.len());
    inputs.extend(geometries.iter().map(std::borrow::Borrow::borrow));
    exact_partition_overlay_with_bounds_certificates(
        inputs,
        op,
        strictness,
        true,
        true,
        ExactPartitionBudget::Charge,
    )
}

#[cfg(test)]
pub(super) fn overlay_all_without_bounds_certificates<S: Borrow<Shape>>(
    geometries: &[S],
    domain: TopologyDomain,
    op: OverlayOp,
    strictness: Strictness,
) -> Result<Shape> {
    overlay_all_with_bounds_certificates(geometries, domain, op, strictness, false)
}

pub(super) fn overlay_all_with_bounds_certificates<S: Borrow<Shape>>(
    geometries: &[S],
    domain: TopologyDomain,
    op: OverlayOp,
    strictness: Strictness,
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
            exact_cartesian_overlay(
                refs,
                op,
                strictness,
                bounds_certificates,
                ExactPartitionBudget::Charge,
            )
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

#[derive(Clone, Copy)]
enum ExactPartitionBudget {
    Charge,
    Ignore,
}

#[derive(Clone, Copy)]
pub(in crate::geometry) enum VoronoiIntermediateOverlay {
    Intersection,
    Difference,
}

impl VoronoiIntermediateOverlay {
    const fn operation(self) -> OverlayOp {
        match self {
            Self::Intersection => OverlayOp::Intersection,
            Self::Difference => OverlayOp::Difference,
        }
    }
}

/// The Cartesian overlay route. Source inventory, exact incidence, cell
/// selection, dimensional absorption, and checked emission belong to
/// `ExactPartition`; disjoint bounds only partition independent inputs.
fn exact_cartesian_overlay(
    inputs: Vec<&Shape>,
    operation: OverlayOp,
    strictness: Strictness,
    bounds_certificates: bool,
    partition_budget: ExactPartitionBudget,
) -> Result<Shape> {
    if inputs.len() == 1 && operation == OverlayOp::Intersection {
        return Ok(inputs[0].clone());
    }
    exact_partition_overlay_with_bounds_certificates(
        inputs,
        operation,
        strictness,
        bounds_certificates,
        false,
        partition_budget,
    )
}

/// Try dimension-specific lanes only when stored shape metadata admits them.
/// Every other binary input goes directly to the generic Cartesian owner.
fn exact_cartesian_binary_overlay(
    inputs: &mut [&Shape],
    lane: CartesianBinaryLane,
    operation: OverlayOp,
    strictness: Strictness,
    bounds_certificates: bool,
    partition_budget: ExactPartitionBudget,
) -> Result<Shape> {
    sort_overlay_inputs!(inputs, operation);
    let sorted_inputs: &[&Shape] = inputs;
    let fast_result = match lane {
        CartesianBinaryLane::Puntal => {
            exact_cartesian_puntal_overlay(sorted_inputs, operation, strictness)?
        },
        CartesianBinaryLane::LinePolygon => {
            exact_cartesian_line_polygon_overlay(sorted_inputs, operation, strictness)?
        },
    };
    if let Some(shape) = fast_result {
        return Ok(shape);
    }

    exact_partition_overlay_with_bounds_certificates(
        sorted_inputs.to_vec(),
        operation,
        strictness,
        bounds_certificates,
        false,
        partition_budget,
    )
}

fn exact_partition_overlay_with_bounds_certificates(
    mut inputs: Vec<&Shape>,
    operation: OverlayOp,
    strictness: Strictness,
    bounds_certificates: bool,
    preserve_geographic_vertices: bool,
    partition_budget: ExactPartitionBudget,
) -> Result<Shape> {
    sort_overlay_inputs!(inputs, operation);
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
        None => exact_partition_overlay(
            inputs,
            operation,
            declared_axes,
            strict,
            preserve_geographic_vertices,
            partition_budget,
        )?,
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
                    preserve_geographic_vertices,
                    partition_budget,
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

/// Point-set operations do not need an arrangement. A higher-dimensional
/// operand remains its original value when the point result is absorbed or
/// added alongside it.
fn exact_cartesian_puntal_overlay(
    inputs: &[&Shape],
    operation: OverlayOp,
    strictness: Strictness,
) -> Result<Option<Shape>> {
    let [left, right] = inputs else {
        return Ok(None);
    };
    let left_points = puntal_points(left);
    let right_points = puntal_points(right);
    match (left_points, right_points) {
        (Some(left_points), Some(right_points)) => {
            let left_points = unique_xy_points(left_points)?;
            let right_points = unique_xy_points(right_points)?;
            let points = match operation {
                OverlayOp::Intersection | OverlayOp::Difference => {
                    let right_keys = point_keys(&right_points)?;
                    let intersection = operation == OverlayOp::Intersection;
                    left_points
                        .into_iter()
                        .filter(|point| right_keys.contains(&PointKey::new(*point)) == intersection)
                        .collect()
                },
                OverlayOp::Union => {
                    let left_keys = point_keys(&left_points)?;
                    let mut points = left_points;
                    points.extend(
                        right_points
                            .into_iter()
                            .filter(|point| !left_keys.contains(&PointKey::new(*point))),
                    );
                    points
                },
                OverlayOp::SymmetricDifference => {
                    let left_keys = point_keys(&left_points)?;
                    let right_keys = point_keys(&right_points)?;
                    let mut points: Vec<_> = left_points
                        .into_iter()
                        .filter(|point| !right_keys.contains(&PointKey::new(*point)))
                        .collect();
                    points.extend(
                        right_points
                            .into_iter()
                            .filter(|point| !left_keys.contains(&PointKey::new(*point))),
                    );
                    points
                },
            };
            point_overlay_shape(points, inputs, operation, strictness).map(Some)
        },
        (Some(points), None) => {
            one_puntal_overlay(points, right, true, inputs, operation, strictness)
        },
        (None, Some(points)) => {
            one_puntal_overlay(points, left, false, inputs, operation, strictness)
        },
        (None, None) => Ok(None),
    }
}

fn one_puntal_overlay(
    points: Vec<Point>,
    other: &Shape,
    puntal_is_left: bool,
    inputs: &[&Shape],
    operation: OverlayOp,
    strictness: Strictness,
) -> Result<Option<Shape>> {
    let mut points = unique_xy_points(points)?;
    if operation == OverlayOp::Difference && !puntal_is_left {
        return Ok((other.topological_dimension() > Dimension::Point).then(|| other.clone()));
    }

    points.retain(|&point| {
        let covered = cartesian_point_location(other, point) != Loc::Exterior;
        match operation {
            OverlayOp::Intersection => covered,
            OverlayOp::Difference | OverlayOp::Union | OverlayOp::SymmetricDifference => !covered,
        }
    });

    if operation == OverlayOp::Intersection || operation == OverlayOp::Difference {
        return point_overlay_shape(points, inputs, operation, strictness).map(Some);
    }

    if points.is_empty() {
        return Ok(Some(if other.is_empty() {
            empty_nary_overlay_shape(inputs, operation)
        } else {
            other.clone()
        }));
    }
    let point_shape = point_overlay_shape(points, inputs, operation, strictness)?;
    if other.is_empty() {
        return Ok(Some(point_shape));
    }

    let mut parts = Vec::new();
    parts.push(point_shape);
    if let Shape::GeometryCollection(collection) = other
        && other.nesting_depth() == crate::geometry::MAX_NESTING
    {
        // Preserve the collection as a member unless another layer would exceed
        // the admitted depth; then move its existing members up intact.
        parts.extend(collection.iter().cloned());
    } else {
        parts.push(other.clone());
    }
    Shape::geometry_collection(parts).map(Some)
}

fn point_overlay_shape(
    points: Vec<Point>,
    inputs: &[&Shape],
    operation: OverlayOp,
    strictness: Strictness,
) -> Result<Shape> {
    if points.is_empty() {
        return Ok(empty_nary_overlay_shape(inputs, operation));
    }
    let shape = build_certified_overlay_shape(points, Vec::new(), Vec::new())?;
    carry_overlay_ordinates(shape, inputs, operation, strictness.is_strict())
}

fn point_keys(points: &[Point]) -> Result<HashSet<PointKey>> {
    let mut keys = HashSet::with_hasher(crate::collections::collection_hash_state()?.clone());
    keys.reserve(points.len());
    keys.extend(points.iter().copied().map(PointKey::new));
    Ok(keys)
}

fn puntal_points(shape: &Shape) -> Option<Vec<Point>> {
    fn collect(shape: &Shape, points: &mut Vec<Point>, owners: &mut SharedShapeOwners) -> bool {
        match shape {
            Shape::Point(point) => points.push(*point),
            Shape::MultiPoint(multipoint) => points.extend(multipoint.points()),
            Shape::Empty(
                GeometryKind::Point | GeometryKind::MultiPoint | GeometryKind::GeometryCollection,
                _,
            ) => {},
            Shape::GeometryCollection(collection) => {
                if !owners.first_collection(collection.as_slice(), collection.members_are_shared())
                {
                    return true;
                }
                for part in collection {
                    if !collect(part, points, owners) {
                        return false;
                    }
                }
            },
            Shape::LineString(_)
            | Shape::MultiLineString(_)
            | Shape::Polygon(_)
            | Shape::MultiPolygon(_)
            | Shape::Empty(..) => return false,
        }
        true
    }

    if shape.topological_dimension() != Dimension::Point {
        return None;
    }

    let mut points = Vec::new();
    let mut owners = SharedShapeOwners::for_collections();
    collect(shape, &mut points, &mut owners).then_some(points)
}

fn line_segment_midpoint(segment: Segment) -> Option<XY> {
    let midpoint = XY::new(
        f64::midpoint(segment.start.x, segment.end.x),
        f64::midpoint(segment.start.y, segment.end.y),
    );
    (!crate::geometry::same_point(midpoint, segment.start)
        && !crate::geometry::same_point(midpoint, segment.end)
        && point_on_segment(midpoint.point(), segment.start, segment.end))
    .then_some(midpoint)
}

/// Lineal results can be clipped by exact pair-sweep contacts and indexed point
/// location without constructing areal faces. Degenerate lines and isolated
/// point intersections remain on the general partition path.
fn exact_cartesian_line_polygon_overlay(
    inputs: &[&Shape],
    operation: OverlayOp,
    strictness: Strictness,
) -> Result<Option<Shape>> {
    let [left, right] = inputs else {
        return Ok(None);
    };
    let (line_shape, polygon_shape) = match operation {
        OverlayOp::Intersection | OverlayOp::Difference
            if is_line_operand(left) && is_polygon_operand(right) =>
        {
            (left, right)
        },
        OverlayOp::Intersection if is_line_operand(right) && is_polygon_operand(left) => {
            (right, left)
        },
        OverlayOp::Union
        | OverlayOp::Intersection
        | OverlayOp::Difference
        | OverlayOp::SymmetricDifference => return Ok(None),
    };
    let line_sequences = line_sequences(line_shape).expect("line operand was admitted");
    if line_shape.is_empty() {
        return Ok(Some(empty_nary_overlay_shape(inputs, operation)));
    }
    if line_sequences.iter().any(|line| {
        !line
            .segment_pairs()
            .any(|[start, end]| !crate::geometry::same_point(start.xy(), end.xy()))
    }) {
        return Ok(None);
    }

    let first_polygon_chain =
        u32::try_from(line_sequences.len()).map_err(|_| ResourceError::SizeOverflow)?;
    let mut candidate_segments = Vec::new();
    let mut candidate_chains = Vec::new();
    for (chain, line) in line_sequences.iter().enumerate() {
        let chain = u32::try_from(chain).map_err(|_| ResourceError::SizeOverflow)?;
        for [start, end] in line.segment_pairs() {
            candidate_segments.push(Segment {
                start: start.xy(),
                end: end.xy(),
            });
            candidate_chains.push(chain);
        }
    }
    let line_segment_count = candidate_segments.len();
    let Some(polygon_boundaries) = polygon_boundaries(polygon_shape, first_polygon_chain)? else {
        return Ok(None);
    };
    let candidate_capacity = line_segment_count
        .checked_add(polygon_boundaries.segments.len())
        .ok_or(ResourceError::SizeOverflow)?;
    candidate_segments.reserve(candidate_capacity.saturating_sub(candidate_segments.len()));
    candidate_chains.reserve(candidate_capacity.saturating_sub(candidate_chains.len()));
    candidate_segments.extend(polygon_boundaries.segments);
    candidate_chains.extend(polygon_boundaries.chains);
    let strictly_monotone = if let [line] = line_sequences.as_slice() {
        strictly_monotone_column(line.xs()) || strictly_monotone_column(line.ys())
    } else {
        false
    };
    let LineContactEvents {
        mut cut_events,
        deduplicate_line_edges,
        atomicize_line_edges,
    } = line_polygon_contact_events(
        &candidate_segments,
        &candidate_chains,
        line_segment_count,
        strictly_monotone,
    )?;
    cut_events.sort_unstable_by_key(|event| event.source_segment);
    let polygon_bounds = polygon_shape.bounds();
    let polygon_plan = PreparedPlan::transient(polygon_shape);
    let intersection = operation == OverlayOp::Intersection;

    let mut lines = Vec::new();
    let mut polygon_contact_points =
        HashSet::with_hasher(crate::collections::collection_hash_state()?.clone());
    let mut boundary_contacts =
        HashSet::with_hasher(crate::collections::collection_hash_state()?.clone());
    let parameter_zero = ExactDyadic::from_f64(0.0);
    let parameter_one = ExactDyadic::from_f64(1.0);
    let mut cuts = Vec::new();
    let mut unique_cuts = Vec::new();
    let mut cut_events = cut_events.into_iter().peekable();
    let mut source_segment_index = 0_usize;
    for source_line in &line_sequences {
        let mut current = Vec::new();
        for [start, end] in source_line.segment_pairs() {
            let segment = Segment {
                start: start.xy(),
                end: end.xy(),
            };
            let index = source_segment_index;
            source_segment_index = source_segment_index.wrapping_add(1);
            if crate::geometry::same_point(segment.start, segment.end) {
                discard_line_events(index, &mut cut_events);
                continue;
            }
            let segment_bounds = Bounds::new_raw(
                segment.start.x.min(segment.end.x),
                segment.start.y.min(segment.end.y),
                segment.start.x.max(segment.end.x),
                segment.start.y.max(segment.end.y),
            );
            let outside_polygon_bounds =
                polygon_bounds.is_none_or(|bounds| !bounds.intersects(segment_bounds));
            let has_events = cut_events
                .peek()
                .is_some_and(|event| event.source_segment == index);
            if outside_polygon_bounds && intersection {
                finish_line_piece(&mut current, &mut lines)?;
                discard_line_events(index, &mut cut_events);
                continue;
            }
            if outside_polygon_bounds && !has_events {
                emit_line_piece(
                    &mut current,
                    &mut lines,
                    segment.start,
                    segment.end,
                    atomicize_line_edges,
                )?;
                continue;
            }
            if !outside_polygon_bounds && !has_events {
                let Some(midpoint) = line_segment_midpoint(segment) else {
                    return Ok(None);
                };
                let location = polygon_plan.cartesian_point_location(midpoint.point())?;
                let keep = if intersection {
                    location != Loc::Exterior
                } else {
                    location == Loc::Exterior
                };
                if keep {
                    emit_line_piece(
                        &mut current,
                        &mut lines,
                        segment.start,
                        segment.end,
                        atomicize_line_edges,
                    )?;
                } else {
                    finish_line_piece(&mut current, &mut lines)?;
                }
                continue;
            }
            gather_line_cuts(
                index,
                segment,
                &mut cut_events,
                &mut cuts,
                &mut unique_cuts,
                &parameter_zero,
                &parameter_one,
            )?;
            for cut in cuts.iter().filter(|cut| cut.on_polygon_segment) {
                let point = cut.point.point();
                polygon_contact_points.insert(PointKey::new(point));
                if polygon_plan.cartesian_point_location(point)? == Loc::Boundary {
                    boundary_contacts.insert(PointKey::new(point));
                }
            }
            if outside_polygon_bounds {
                for [start, end] in cuts.array_windows::<2>() {
                    if crate::geometry::same_point(start.point, end.point) {
                        return Ok(None);
                    }
                    emit_line_piece(
                        &mut current,
                        &mut lines,
                        start.point,
                        end.point,
                        atomicize_line_edges,
                    )?;
                }
                continue;
            }
            for [start, end] in cuts.array_windows::<2>() {
                if crate::geometry::same_point(start.point, end.point) {
                    return Ok(None);
                }
                let Some(midpoint) = line_segment_midpoint(Segment {
                    start: start.point,
                    end: end.point,
                }) else {
                    return Ok(None);
                };
                let location = polygon_plan.cartesian_point_location(midpoint.point())?;
                let keep = if intersection {
                    location != Loc::Exterior
                } else {
                    location == Loc::Exterior
                };
                if keep {
                    emit_line_piece(
                        &mut current,
                        &mut lines,
                        start.point,
                        end.point,
                        atomicize_line_edges,
                    )?;
                } else {
                    finish_line_piece(&mut current, &mut lines)?;
                }
            }
        }
        finish_line_piece(&mut current, &mut lines)?;
    }

    if operation == OverlayOp::Intersection && !boundary_contacts.is_empty() {
        let mut retained_boundary_contacts =
            HashSet::with_hasher(crate::collections::collection_hash_state()?.clone());
        for line in &lines {
            for point in line {
                let key = PointKey::new(point);
                if boundary_contacts.contains(&key) {
                    retained_boundary_contacts.insert(key);
                }
            }
        }
        if retained_boundary_contacts.len() != boundary_contacts.len() {
            return Ok(None);
        }
    }
    if lines.is_empty() {
        return Ok(Some(empty_nary_overlay_shape(inputs, operation)));
    }
    if deduplicate_line_edges {
        lines = unique_line_edges(&lines)?;
    }
    let mut lines = merge_chains(&lines)?;
    for line in &mut lines {
        *line = dissolve_boundary_line_vertices(line, &polygon_contact_points)?;
    }
    let shape = build_certified_overlay_shape(Vec::new(), lines, Vec::new())?;
    let shape = carry_overlay_ordinates(shape, inputs, operation, strictness.is_strict())?;
    shape.normalize().map(Some)
}

fn strictly_monotone_column(values: &[f64]) -> bool {
    let Some([first, second]) = values.first_chunk::<2>() else {
        return false;
    };
    let Some(first_ordering) = first.partial_cmp(second) else {
        return false;
    };
    let increasing = first_ordering.is_lt();
    if first_ordering.is_eq() {
        return false;
    }
    values.array_windows::<2>().all(|[start, end]| {
        let Some(ordering) = start.partial_cmp(end) else {
            return false;
        };
        if ordering.is_eq() {
            return false;
        }
        if increasing {
            ordering.is_lt()
        } else {
            ordering.is_gt()
        }
    })
}

fn line_sequences(shape: &Shape) -> Option<Vec<&CoordSeq>> {
    match shape {
        Shape::LineString(line) => Some(vec![line.as_coords()]),
        Shape::MultiLineString(lines) => Some(lines.iter().map(|line| line.as_coords()).collect()),
        Shape::Empty(GeometryKind::LineString | GeometryKind::MultiLineString, _) => {
            Some(Vec::new())
        },
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::Polygon(_)
        | Shape::MultiPolygon(_)
        | Shape::GeometryCollection(_)
        | Shape::Empty(
            GeometryKind::Point
            | GeometryKind::MultiPoint
            | GeometryKind::Polygon
            | GeometryKind::MultiPolygon
            | GeometryKind::GeometryCollection,
            _,
        ) => None,
    }
}

const fn is_line_operand(shape: &Shape) -> bool {
    matches!(
        shape,
        Shape::LineString(_)
            | Shape::MultiLineString(_)
            | Shape::Empty(GeometryKind::LineString | GeometryKind::MultiLineString, _)
    )
}

const fn is_polygon_operand(shape: &Shape) -> bool {
    matches!(
        shape,
        Shape::Polygon(_)
            | Shape::MultiPolygon(_)
            | Shape::Empty(GeometryKind::Polygon | GeometryKind::MultiPolygon, _)
    )
}

struct PolygonBoundarySegments {
    segments: Vec<Segment>,
    chains: Vec<u32>,
}

fn polygon_boundaries(shape: &Shape, first_chain: u32) -> Result<Option<PolygonBoundarySegments>> {
    fn append_polygon(
        polygon: &crate::geometry::Polygon,
        segments: &mut Vec<Segment>,
        chains: &mut Vec<u32>,
        next_chain: &mut u32,
    ) -> Result<bool> {
        for ring in polygon.rings() {
            if crate::geometry::ring_kind_columns(ring.xs(), ring.ys())
                == crate::geometry::RingKind::Collapsed
            {
                return Ok(false);
            }
            let chain = *next_chain;
            *next_chain = chain.checked_add(1).ok_or(ResourceError::SizeOverflow)?;
            for [start, end] in ring.segment_pairs() {
                segments.push(Segment {
                    start: start.xy(),
                    end: end.xy(),
                });
                chains.push(chain);
            }
        }
        Ok(true)
    }

    let mut segments = Vec::new();
    let mut chains = Vec::new();
    let mut next_chain = first_chain;
    match shape {
        Shape::Polygon(polygon) => {
            if !append_polygon(polygon, &mut segments, &mut chains, &mut next_chain)? {
                return Ok(None);
            }
        },
        Shape::MultiPolygon(polygons) => {
            for polygon in polygons {
                if !append_polygon(polygon, &mut segments, &mut chains, &mut next_chain)? {
                    return Ok(None);
                }
            }
        },
        Shape::Empty(GeometryKind::Polygon | GeometryKind::MultiPolygon, _) => {},
        Shape::Point(_)
        | Shape::MultiPoint(_)
        | Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::GeometryCollection(_)
        | Shape::Empty(
            GeometryKind::Point
            | GeometryKind::MultiPoint
            | GeometryKind::LineString
            | GeometryKind::MultiLineString
            | GeometryKind::GeometryCollection,
            _,
        ) => return Ok(None),
    }
    Ok(Some(PolygonBoundarySegments { segments, chains }))
}

struct LineCut {
    parameter: SegmentParameter,
    point: XY,
    on_polygon_segment: bool,
}

struct LineCutEvent {
    source_segment: usize,
    cut: LineCut,
}

struct LineContactEvents {
    cut_events: Vec<LineCutEvent>,
    deduplicate_line_edges: bool,
    atomicize_line_edges: bool,
}

fn line_polygon_contact_events(
    candidates: &[Segment],
    chains: &[u32],
    line_segment_count: usize,
    line_is_monotone: bool,
) -> Result<LineContactEvents> {
    let mut cut_events = Vec::new();
    let mut deduplicate_line_edges = false;
    let candidate_flow: ControlFlow<crate::error::Error> = for_each_candidate_pair(
        candidates,
        |index| {
            chains
                .get(index)
                .copied()
                .expect("candidate index addresses the chain column")
        },
        |left, right| {
            let (Some(&left_segment), Some(&right_segment)) =
                (candidates.get(left), candidates.get(right))
            else {
                return ControlFlow::Break(
                    GeometryErrorKind::Invalid(
                        "segment candidate index escaped its source pool".into(),
                    )
                    .into(),
                );
            };
            let left_is_line = left < line_segment_count;
            let right_is_line = right < line_segment_count;
            match (left_is_line, right_is_line) {
                (false, false) => ControlFlow::Continue(()),
                (true, true) if line_is_monotone => ControlFlow::Continue(()),
                (true, true) => {
                    let contact = segment_contact_exact(left_segment, right_segment);
                    if let Contact::Collinear { span } = contact {
                        deduplicate_line_edges |=
                            !crate::geometry::same_point(span.start, span.end);
                    }
                    match push_line_self_contact(
                        left,
                        left_segment,
                        right,
                        right_segment,
                        contact,
                        &mut cut_events,
                    ) {
                        Ok(()) => ControlFlow::Continue(()),
                        Err(error) => ControlFlow::Break(error),
                    }
                },
                _ => {
                    let (source_segment, line, boundary) = if left_is_line {
                        (left, left_segment, right_segment)
                    } else {
                        (right, right_segment, left_segment)
                    };
                    let contact = segment_contact_exact(line, boundary);
                    match push_line_polygon_contact(
                        source_segment,
                        line,
                        boundary,
                        contact,
                        &mut cut_events,
                    ) {
                        Ok(()) => ControlFlow::Continue(()),
                        Err(error) => ControlFlow::Break(error),
                    }
                },
            }
        },
    )?;
    if let ControlFlow::Break(error) = candidate_flow {
        return Err(error);
    }
    Ok(LineContactEvents {
        cut_events,
        deduplicate_line_edges,
        atomicize_line_edges: !line_is_monotone,
    })
}

fn push_line_polygon_contact(
    source_segment: usize,
    line: Segment,
    boundary: Segment,
    contact: Contact,
    cuts: &mut Vec<LineCutEvent>,
) -> Result<()> {
    match contact {
        Contact::Disjoint => {},
        Contact::Cross => {
            let point = crossing_point(line, boundary)?.point;
            push_line_cut(
                cuts,
                source_segment,
                exact_crossing_parameter(line, boundary)?,
                point,
                true,
            );
        },
        Contact::Touch { point } => {
            push_line_cut(
                cuts,
                source_segment,
                clip_segment_parameter(line, point)?,
                point,
                true,
            );
        },
        Contact::Collinear { span } => {
            push_line_cut(
                cuts,
                source_segment,
                clip_segment_parameter(line, span.start)?,
                span.start,
                true,
            );
            push_line_cut(
                cuts,
                source_segment,
                clip_segment_parameter(line, span.end)?,
                span.end,
                true,
            );
        },
    }
    Ok(())
}

fn push_line_self_contact(
    left_source: usize,
    left: Segment,
    right_source: usize,
    right: Segment,
    contact: Contact,
    cuts: &mut Vec<LineCutEvent>,
) -> Result<()> {
    match contact {
        Contact::Disjoint => {},
        Contact::Cross => {
            let point = crossing_point(left, right)?.point;
            push_line_cut(
                cuts,
                left_source,
                exact_crossing_parameter(left, right)?,
                point,
                false,
            );
            push_line_cut(
                cuts,
                right_source,
                exact_crossing_parameter(right, left)?,
                point,
                false,
            );
        },
        Contact::Touch { point } => {
            push_line_cut(
                cuts,
                left_source,
                clip_segment_parameter(left, point)?,
                point,
                false,
            );
            push_line_cut(
                cuts,
                right_source,
                clip_segment_parameter(right, point)?,
                point,
                false,
            );
        },
        Contact::Collinear { span } => {
            push_line_cut(
                cuts,
                left_source,
                clip_segment_parameter(left, span.start)?,
                span.start,
                false,
            );
            push_line_cut(
                cuts,
                left_source,
                clip_segment_parameter(left, span.end)?,
                span.end,
                false,
            );
            push_line_cut(
                cuts,
                right_source,
                clip_segment_parameter(right, span.start)?,
                span.start,
                false,
            );
            push_line_cut(
                cuts,
                right_source,
                clip_segment_parameter(right, span.end)?,
                span.end,
                false,
            );
        },
    }
    Ok(())
}

fn push_line_cut(
    events: &mut Vec<LineCutEvent>,
    source_segment: usize,
    parameter: SegmentParameter,
    point: XY,
    on_polygon_segment: bool,
) {
    events.push(LineCutEvent {
        source_segment,
        cut: LineCut {
            parameter,
            point,
            on_polygon_segment,
        },
    });
}

fn discard_line_events(
    source_segment: usize,
    cuts: &mut std::iter::Peekable<std::vec::IntoIter<LineCutEvent>>,
) {
    while cuts
        .peek()
        .is_some_and(|event| event.source_segment == source_segment)
    {
        cuts.next();
    }
}

fn gather_line_cuts(
    source_segment: usize,
    segment: Segment,
    cut_events: &mut std::iter::Peekable<std::vec::IntoIter<LineCutEvent>>,
    cuts: &mut Vec<LineCut>,
    unique_cuts: &mut Vec<LineCut>,
    zero: &ExactDyadic,
    one: &ExactDyadic,
) -> Result<()> {
    cuts.clear();
    unique_cuts.clear();
    cuts.extend([
        LineCut {
            parameter: SegmentParameter::Start,
            point: segment.start,
            on_polygon_segment: false,
        },
        LineCut {
            parameter: SegmentParameter::End,
            point: segment.end,
            on_polygon_segment: false,
        },
    ]);
    while cut_events
        .peek()
        .is_some_and(|event| event.source_segment == source_segment)
    {
        cuts.push(cut_events.next().expect("peeked line cut event exists").cut);
    }
    sort_line_cuts(cuts, unique_cuts, zero, one)
}

fn sort_line_cuts(
    cuts: &mut Vec<LineCut>,
    unique: &mut Vec<LineCut>,
    zero: &ExactDyadic,
    one: &ExactDyadic,
) -> Result<()> {
    let mut comparison_error = None;
    cuts.sort_unstable_by(|left, right| {
        if comparison_error.is_some() {
            return std::cmp::Ordering::Equal;
        }
        match left.parameter.cmp(&right.parameter, zero, one) {
            Ok(std::cmp::Ordering::Equal) => left
                .on_polygon_segment
                .cmp(&right.on_polygon_segment)
                .then_with(|| left.point.x.total_cmp(&right.point.x))
                .then_with(|| left.point.y.total_cmp(&right.point.y)),
            Ok(ordering) => ordering,
            Err(error) => {
                comparison_error = Some(error);
                std::cmp::Ordering::Equal
            },
        }
    });
    if let Some(error) = comparison_error {
        return Err(error);
    }
    unique.clear();
    unique.reserve(cuts.len());
    for cut in cuts.drain(..) {
        if let Some(previous) = unique.last_mut()
            && previous.parameter.cmp(&cut.parameter, zero, one)?.is_eq()
        {
            previous.on_polygon_segment |= cut.on_polygon_segment;
        } else {
            unique.push(cut);
        }
    }
    std::mem::swap(cuts, unique);
    unique.clear();
    Ok(())
}

fn append_line_piece(
    current: &mut Vec<Point>,
    output: &mut Vec<CoordSeq>,
    start: XY,
    end: XY,
) -> Result<()> {
    if current
        .last()
        .is_some_and(|point| !crate::geometry::same_point(point.xy(), start))
    {
        finish_line_piece(current, output)?;
    }
    if current.is_empty() {
        current.push(start.point());
    }
    if current
        .last()
        .is_none_or(|point| !crate::geometry::same_point(point.xy(), end))
    {
        current.push(end.point());
    }
    Ok(())
}

fn emit_line_piece(
    current: &mut Vec<Point>,
    output: &mut Vec<CoordSeq>,
    start: XY,
    end: XY,
    atomic: bool,
) -> Result<()> {
    if atomic {
        finish_line_piece(current, output)?;
        output.push(CoordSeq::try_from_xy(&[start, end])?);
        Ok(())
    } else {
        append_line_piece(current, output, start, end)
    }
}

fn finish_line_piece(current: &mut Vec<Point>, output: &mut Vec<CoordSeq>) -> Result<()> {
    if current.len() >= 2 {
        output.push(CoordSeq::try_from_points(current)?);
    }
    current.clear();
    Ok(())
}

fn unique_line_edges(lines: &[CoordSeq]) -> Result<Vec<CoordSeq>> {
    let mut seen = HashSet::with_hasher(crate::collections::collection_hash_state()?.clone());
    let mut unique = Vec::new();
    for line in lines {
        for [start, end] in line.segment_pairs() {
            let segment = Segment {
                start: start.xy(),
                end: end.xy(),
            };
            if !seen.insert(undirected_segment_edge_key(segment)) {
                continue;
            }
            u32::try_from(unique.len()).map_err(|_| ResourceError::SizeOverflow)?;
            unique.push(segment);
        }
    }
    let mut edges = Vec::with_capacity(unique.len());
    for segment in unique {
        edges.push(CoordSeq::try_from_xy(&[segment.start, segment.end])?);
    }
    Ok(edges)
}

/// Contact-generated line nodes are dissolved when they are collinear; source
/// vertices away from polygon contacts retain their stored presentation.
fn dissolve_boundary_line_vertices(
    line: &CoordSeq,
    boundary_points: &HashSet<PointKey>,
) -> Result<CoordSeq> {
    let points: Vec<_> = line.iter().collect();
    if let Some((&closing, open)) = points.split_last()
        && open
            .first()
            .is_some_and(|&first| crate::geometry::same_point(first, closing))
    {
        let mut cycle = open.to_vec();
        loop {
            if cycle.len() <= 3 {
                break;
            }
            let first = cycle
                .first()
                .copied()
                .expect("cycle has at least four points");
            let mut previous = cycle
                .last()
                .copied()
                .expect("cycle has at least four points");
            let next_points = cycle.iter().copied().skip(1).chain(std::iter::once(first));
            let mut removable = None;
            for (index, (point, next)) in cycle.iter().copied().zip(next_points).enumerate() {
                if boundary_points.contains(&PointKey::new(point))
                    && point_on_segment(point, previous, next)
                {
                    removable = Some(index);
                    break;
                }
                previous = point;
            }
            match removable {
                Some(index) => {
                    cycle.remove(index);
                },
                None => break,
            }
        }
        if let Some(&first) = cycle.first() {
            cycle.push(first);
        }
        return CoordSeq::try_from_points(&cycle);
    }

    let Some((&first, remaining)) = points.split_first() else {
        return Ok(line.clone());
    };
    let mut kept = Vec::with_capacity(points.len());
    kept.push(first);
    for next in remaining.iter().copied() {
        while let [.., previous, point] = kept.as_slice() {
            if !boundary_points.contains(&PointKey::new(*point))
                || !point_on_segment(*point, *previous, next)
            {
                break;
            }
            kept.pop();
        }
        kept.push(next);
    }
    CoordSeq::try_from_points(&kept)
}

/// Independently partitioned results retain one common ordinate axis per
/// dimension, just as a single exact partition does.
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
        *point = crate::geometry::Point::new_raw_axes(
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

fn append_overlay_parts(
    shape: &Shape,
    points: &mut Vec<crate::geometry::Point>,
    lines: &mut Vec<CoordSeq>,
    polygons: &mut Vec<crate::geometry::Polygon>,
) -> Result<()> {
    shape.try_for_each_component(&mut |component| {
        match component {
            crate::geometry::Component::Point(point) => points.push(point),
            crate::geometry::Component::LineString(line) => {
                lines.push(line.as_coords().clone());
            },
            crate::geometry::Component::Polygon(polygon) => polygons.push(polygon.clone()),
        }
        Ok(())
    })
}

/// One exact Cartesian partition over the already canonical-ordered inputs.
fn exact_partition_overlay(
    inputs: &[&Shape],
    operation: OverlayOp,
    declared_axes: CoordinateAxes,
    strict: bool,
    preserve_geographic_vertices: bool,
    partition_budget: ExactPartitionBudget,
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
    let mut partition = match partition_budget {
        ExactPartitionBudget::Charge => {
            let mut budget =
                crate::geometry::ExpansionBudget::new(operation.name(), "exact partition");
            inventory.partition(&mut budget)?
        },
        ExactPartitionBudget::Ignore => inventory.partition_without_budget()?,
    };
    if preserve_geographic_vertices {
        // Chart-collinear vertices still define separate geographic metric
        // edges, so exact planar simplification must not merge them.
        partition.keep_all_nodes();
    }
    inventory.overlay_shape(&partition, operation, declared_axes, strict)
}

#[cfg(test)]
pub(super) fn full_partition_cartesian_overlay(
    inputs: &[&Shape],
    operation: OverlayOp,
    strictness: Strictness,
) -> Result<Shape> {
    let mut inputs = inputs.to_vec();
    sort_overlay_inputs!(inputs, operation);
    let inputs = inputs.as_slice();
    let shape = exact_partition_overlay(
        inputs,
        operation,
        overlay_declared_axes(inputs, operation),
        strictness.is_strict(),
        false,
        ExactPartitionBudget::Charge,
    )?;
    Ok(if shape.is_empty() {
        empty_nary_overlay_shape(inputs, operation)
    } else {
        shape.normalize()?
    })
}

/// Voronoi owns the published-output budget for these intermediate clips.
pub(in crate::geometry) fn binary_cartesian_overlay_without_partition_budget(
    left: &Shape,
    right: &Shape,
    operation: VoronoiIntermediateOverlay,
    strictness: Strictness,
) -> Result<Shape> {
    let operation = operation.operation();
    let mut inputs = vec![left, right];
    if let Some(lane) = cartesian_binary_lane!(left, right, operation) {
        return exact_cartesian_binary_overlay(
            inputs.as_mut_slice(),
            lane,
            operation,
            strictness,
            true,
            ExactPartitionBudget::Ignore,
        );
    }
    exact_cartesian_overlay(
        inputs,
        operation,
        strictness,
        true,
        ExactPartitionBudget::Ignore,
    )
}

/// Geographic overlays share one chart classifier. Ordinary charts use the
/// exact planar partition; seam and pole cases use one global Q over the
/// original rows so source intervals still own checked Z/M emission.
pub(in crate::geometry) fn overlay_all_geographic_from(
    originals: &GeographicInputs<'_>,
    op: OverlayOp,
    strictness: Strictness,
    bounds_certificates: bool,
) -> Result<Shape> {
    let mut original_refs = Vec::with_capacity(originals.views().len());
    original_refs.extend(originals.views().iter().map(|view| view.shape()));
    if crate::geometry::geographic_topology_route(originals.views().iter().copied())
        == GeographicTopologyRoute::ExactPartition
    {
        return exact_partition_overlay_with_bounds_certificates(
            original_refs,
            op,
            strictness,
            bounds_certificates,
            true,
            ExactPartitionBudget::Charge,
        );
    }
    exact_geographic_q_overlay(
        originals,
        &original_refs,
        op,
        strictness,
        bounds_certificates,
    )
}

#[cfg(test)]
pub(crate) fn overlay_all_geographic_q_from(
    originals: &GeographicInputs<'_>,
    op: OverlayOp,
    strictness: Strictness,
    bounds_certificates: bool,
) -> Result<Shape> {
    let mut original_refs = Vec::with_capacity(originals.views().len());
    original_refs.extend(originals.views().iter().map(|view| view.shape()));
    exact_geographic_q_overlay(
        originals,
        &original_refs,
        op,
        strictness,
        bounds_certificates,
    )
}

fn exact_geographic_q_overlay(
    originals: &GeographicInputs<'_>,
    original_refs: &[&Shape],
    op: OverlayOp,
    strictness: Strictness,
    bounds_certificates: bool,
) -> Result<Shape> {
    let frame = originals.frame();
    let declared_axes = overlay_declared_axes(original_refs, op);

    if bounds_certificates && op == OverlayOp::Intersection && originals.views().len() > 1 {
        let facts = overlay_bounds_facts_geographic(originals);
        if facts.raw_coordinate_bounds_sound && intersection_bounds_empty(&facts.bounds) {
            return Ok(empty_nary_overlay_shape(original_refs, op));
        }
    }

    // Preserve the left row for difference; commutative operations use a
    // canonical row order so Q ownership is independent of input permutation.
    let mut views = originals.views().to_vec();
    if op == OverlayOp::Difference {
        let (_, rest) = views
            .split_first_mut()
            .expect("geographic overlays have at least one operand");
        rest.sort_unstable_by(|left, right| {
            crate::geometry::compare_shapes(left.shape(), right.shape())
        });
    } else {
        views.sort_unstable_by(|left, right| {
            crate::geometry::compare_shapes(left.shape(), right.shape())
        });
    }
    let topology = crate::geometry::ExactGeographicTopology::from_views(frame, views.into_iter())?;
    let output = exact_geographic_overlay(&topology, op, declared_axes, strictness.is_strict())?;
    Ok(if output.is_empty() {
        empty_nary_overlay_shape(original_refs, op)
    } else {
        output
    })
}

pub(crate) fn occupied_geographic_support(
    shape: &Shape,
    frame: crate::geographic::GeographicFrame,
) -> Result<Shape> {
    let originals = GeographicInputs::try_from_shapes(frame, std::iter::once(shape))
        .map_err(|error| overlay_view_error(&error))?;
    overlay_all_geographic_from(&originals, OverlayOp::Union, Strictness::Lenient, false)
}

struct OverlayBoundsFacts {
    bounds: Vec<Option<Bounds>>,
    raw_coordinate_bounds_sound: bool,
}

pub(in crate::geometry) fn overlay_view_error(error: &GeographicViewError) -> crate::error::Error {
    invalid_lonlat_error(error.native.x, error.native.y)
}

fn overlay_bounds_facts_geographic(originals: &GeographicInputs<'_>) -> OverlayBoundsFacts {
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
    OverlayBoundsFacts {
        bounds,
        raw_coordinate_bounds_sound,
    }
}

/// Inputs whose certified bounds never meet cannot interact in the Cartesian
/// lane. Geographic Q instead owns every admitted row in one global face table.
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

#[cfg(test)]
mod geographic_chart_route_tests {
    use super::*;
    use crate::geographic::GeographicFrame;
    use crate::geometry::{Polygon, Ring, graph_relate_shapes};

    fn rectangle(west: f64, south: f64, east: f64, north: f64) -> Shape {
        let points = [
            Point::new_raw_xy(west, south),
            Point::new_raw_xy(east, south),
            Point::new_raw_xy(east, north),
            Point::new_raw_xy(west, north),
            Point::new_raw_xy(west, south),
        ];
        Shape::Polygon(Polygon::new(
            Ring::try_from_closed(CoordSeq::try_from_points(&points).unwrap()).unwrap(),
            Vec::new(),
        ))
    }

    #[test]
    fn ordinary_geographic_overlays_match_the_q_reference() {
        let shell = rectangle(-10.0, -10.0, 10.0, 10.0);
        let overlap = rectangle(0.0, -5.0, 8.0, 5.0);
        let inner = rectangle(-2.0, -2.0, 2.0, 2.0);
        let disconnected = rectangle(20.0, 0.0, 24.0, 4.0);
        let cases = [
            (OverlayOp::Union, vec![&shell, &overlap, &disconnected]),
            (OverlayOp::Intersection, vec![&shell, &overlap]),
            (OverlayOp::Difference, vec![&shell, &inner]),
            (OverlayOp::SymmetricDifference, vec![&shell, &overlap]),
        ];

        for (operation, inputs) in cases {
            let originals =
                GeographicInputs::try_from_shapes(GeographicFrame::crs84(), inputs.iter().copied())
                    .unwrap();
            assert_eq!(
                crate::geometry::geographic_topology_route(originals.views().iter().copied()),
                GeographicTopologyRoute::ExactPartition
            );

            let routed =
                overlay_all_geographic_from(&originals, operation, Strictness::Lenient, true)
                    .unwrap();
            let q = overlay_all_geographic_q_from(&originals, operation, Strictness::Lenient, true)
                .unwrap();
            assert!(
                graph_relate_shapes(&routed, &q).unwrap().is_equal_topo(),
                "{operation:?}: ExactPartition and Q outputs differ"
            );
        }
    }

    #[test]
    fn ordinary_near_ulp_country_overlay_matches_the_q_reference() {
        let argentina = crate::io::parse_wkt(
            "MULTIPOLYGON (((-58.10222180941994 -33.010937543229446, \
             -58.084289 -33.0009615, -58.085477253434156 -32.99, \
             -58.089648910878246 -32.99, -58.0878711 -32.9965279, \
             -58.10222180941994 -33.010937543229446)), \
             ((-58.11 -33.00166081596568, -58.10769305699579 -32.99, \
             -58.11 -32.99, -58.11 -33.00166081596568)))",
        )
        .unwrap();
        let uruguay = crate::io::parse_wkt(
            "POLYGON ((-58.11 -33.02, -58.08 -33.02, -58.08 -32.99, \
             -58.085477253434156 -32.99, -58.084289 -33.0009615, \
             -58.10256757394891 -33.011129892448544, \
             -58.0878711 -32.9965279, -58.089648910878246 -32.99, \
             -58.10765630407339 -32.99, -58.11 -33.001904214459806, \
             -58.11 -33.02))",
        )
        .unwrap();
        let inputs = [&argentina, &uruguay];
        let originals =
            GeographicInputs::try_from_shapes(GeographicFrame::crs84(), inputs.into_iter())
                .unwrap();
        assert_eq!(
            crate::geometry::geographic_topology_route(originals.views().iter().copied()),
            GeographicTopologyRoute::ExactPartition
        );

        let routed =
            overlay_all_geographic_from(&originals, OverlayOp::Union, Strictness::Lenient, true)
                .unwrap();
        let q =
            overlay_all_geographic_q_from(&originals, OverlayOp::Union, Strictness::Lenient, true)
                .unwrap();
        assert!(
            graph_relate_shapes(&routed, &q).unwrap().is_equal_topo(),
            "ExactPartition and Q differ for the near-ULP border union"
        );
    }
}
