use std::borrow::Borrow;
use std::ops::ControlFlow;

use crate::Frame;
use crate::error::Result;
use crate::geographic::lonlat::geographic_admission_error;
use crate::geographic::{GeographicFrame, GeographicInputs, GeographicViewError};
use crate::geometry::ordinates::AxisValue;
use crate::geometry::output::build_certified_shape;
use crate::geometry::overlay::{
    ExactSourceInventory, OverlayOp, OverlayPolygonPartWithSources, Strictness,
    intersect_rectangle, overlay_declared_axes, rectangle_bounds,
};
use crate::geometry::{
    Bounds, CoordSeq, CoordinateAxes, Coordinates as _, Dimension, GeographicTopologyRoute,
    GeometryErrorKind, GeometryKind, HashSet, Loc, Point, PointKey, Shape, SharedShapeOwners,
    SourceArcContext, SourceArcMetricAccess, TopologyDomain, XY, carry_overlay_ordinates,
    cartesian_point_location, unique_xy_points,
};

mod publication;
pub(in crate::geometry) use publication::overlay_all_geographic_from_with_metric;
pub(crate) use publication::overlay_all_geographic_output_with_metric_and_frame;

#[cfg(test)]
thread_local! {
    static NARY_CANDIDATE_WORK: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// Canonical operand order for partition overlay: difference keeps its left
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

/// Admit point unions and parity at the caller, before entering the membership
/// owner's larger frame. Nonpoint overlays retain their ordinary call path.
/// Returns the admitted result from the caller.
macro_rules! puntal_nary_lane {
    ($inputs:expr, $operation:expr, $strictness:expr, $context:expr) => {{
        let inputs: &[&Shape] = $inputs;
        let operation = $operation;
        if matches!(operation, OverlayOp::Union | OverlayOp::SymmetricDifference)
            && inputs
                .iter()
                .all(|shape| shape.topological_dimension() == Dimension::Point)
            && let Some(shape) =
                exact_puntal_nary_overlay(inputs, operation, $strictness, $context)?
        {
            return Ok(shape);
        }
    }};
}

/// Evaluate one or more operands in the canonical overlay owner.
///
/// Binary Cartesian puntal pairs use set membership; all other inputs retain
/// exact partition or geographic quotient semantics.
pub(crate) fn overlay_all<S: Borrow<Shape>>(
    geometries: &[S],
    domain: TopologyDomain,
    op: OverlayOp,
    strictness: Strictness,
) -> Result<Shape> {
    overlay_all_inner::<_, crate::crs::GeodesicMetric>(
        geometries, domain, op, strictness, None, None,
    )
}

pub(crate) fn overlay_all_with_metric<S: Borrow<Shape>, M: crate::geometry::ChartMetric>(
    geometries: &[S],
    domain: TopologyDomain,
    op: OverlayOp,
    strictness: Strictness,
    metric_access: &SourceArcMetricAccess<'_, '_, M>,
) -> Result<Shape> {
    overlay_all_inner::<S, M>(
        geometries,
        domain,
        op,
        strictness,
        None,
        Some(metric_access),
    )
}

fn overlay_all_inner<S: Borrow<Shape>, M: crate::geometry::ChartMetric>(
    geometries: &[S],
    domain: TopologyDomain,
    op: OverlayOp,
    strictness: Strictness,
    known_bounds: Option<&[Option<Bounds>]>,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
) -> Result<Shape> {
    let context = SourceArcContext::new(domain, metric_access);
    if context.domain() == TopologyDomain::Cartesian
        && let [left, right] = geometries
    {
        let left = <S as Borrow<Shape>>::borrow(left);
        let right = <S as Borrow<Shape>>::borrow(right);
        let left_rectangle = rectangle_bounds(left);
        let right_rectangle = rectangle_bounds(right);
        if (left_rectangle.is_some() || right_rectangle.is_some())
            && let Some(shape) = rectangle_operation_result(
                left,
                right,
                op,
                overlay_operand_bounds(known_bounds, 0, left),
                overlay_operand_bounds(known_bounds, 1, right),
                left_rectangle,
                right_rectangle,
            )
        {
            return Ok(shape);
        }
        if op == OverlayOp::Intersection
            && let Some(bounds) = right_rectangle
            && let Shape::Polygon(rectangle) = right
            && let Some(shape) = intersect_rectangle(
                left,
                rectangle,
                bounds,
                crate::geometry::compare_shapes(right, left).is_lt(),
                strictness,
            )?
        {
            return Ok(shape);
        }
        if op == OverlayOp::Intersection
            && let Some(bounds) = left_rectangle
            && let Shape::Polygon(rectangle) = left
            && let Some(shape) = intersect_rectangle(
                right,
                rectangle,
                bounds,
                crate::geometry::compare_shapes(left, right).is_lt(),
                strictness,
            )?
        {
            return Ok(shape);
        }
        if left.topological_dimension() == Dimension::Point
            || right.topological_dimension() == Dimension::Point
        {
            let mut inputs = [left, right];
            return exact_cartesian_binary_puntal_overlay(
                &mut inputs,
                op,
                strictness,
                true,
                ExactPartitionBudget::Charge,
            );
        }
        if let Some(shape) = exact_cartesian_line_polygon_overlay(
            left,
            right,
            op,
            strictness,
            ExactPartitionBudget::Charge,
        )? {
            return Ok(shape);
        }
    }
    overlay_all_with_bounds_certificates_and_metric(geometries, &context, op, strictness, true)
}

fn overlay_operand_bounds(
    known_bounds: Option<&[Option<Bounds>]>,
    index: usize,
    shape: &Shape,
) -> Option<Bounds> {
    known_bounds.map_or_else(
        || shape.bounds(),
        |bounds| {
            bounds
                .get(index)
                .copied()
                .flatten()
                .or_else(|| shape.bounds())
        },
    )
}

fn rectangle_operation_result(
    left: &Shape,
    right: &Shape,
    operation: OverlayOp,
    left_bounds: Option<Bounds>,
    right_bounds: Option<Bounds>,
    left_rectangle: Option<Bounds>,
    right_rectangle: Option<Bounds>,
) -> Option<Shape> {
    match operation {
        OverlayOp::Intersection => {
            if let Some(rectangle) = right_rectangle
                && let Some(shape) = rectangle_intersection_result(left, left_bounds, rectangle)
            {
                return Some(shape);
            }
            if let Some(rectangle) = left_rectangle
                && let Some(shape) = rectangle_intersection_result(right, right_bounds, rectangle)
            {
                return Some(shape);
            }
        },
        OverlayOp::Difference => {
            if right_rectangle.is_some_and(|rectangle| {
                left_bounds.is_some_and(|bounds| bounds_contain(rectangle, bounds))
            }) {
                return Some(empty_rectangle_result(left));
            }
        },
        OverlayOp::Union => {
            if left.axes() == CoordinateAxes::XY
                && right.axes() == CoordinateAxes::XY
                && left_rectangle.is_some_and(|rectangle| {
                    right_bounds.is_some_and(|bounds| bounds_contain(rectangle, bounds))
                })
            {
                return Some(left.clone());
            }
            if left.axes() == CoordinateAxes::XY
                && right.axes() == CoordinateAxes::XY
                && right_rectangle.is_some_and(|rectangle| {
                    left_bounds.is_some_and(|bounds| bounds_contain(rectangle, bounds))
                })
            {
                return Some(right.clone());
            }
        },
        OverlayOp::SymmetricDifference => {},
    }
    None
}

fn rectangle_intersection_result(
    subject: &Shape,
    subject_bounds: Option<Bounds>,
    rectangle: Bounds,
) -> Option<Shape> {
    let bounds = subject_bounds?;
    if bounds_contain(rectangle, bounds)
        && (subject.topological_dimension() != Dimension::Curve
            || (bounds.minx() > rectangle.minx()
                && bounds.maxx() < rectangle.maxx()
                && bounds.miny() > rectangle.miny()
                && bounds.maxy() < rectangle.maxy()))
    {
        Some(subject.clone())
    } else if bounds_disjoint(bounds, rectangle) {
        Some(empty_rectangle_result(subject))
    } else {
        None
    }
}

fn empty_rectangle_result(subject: &Shape) -> Shape {
    empty_shape_for_dimension_with_axes(subject.topological_dimension(), subject.axes())
}

fn bounds_contain(outer: Bounds, inner: Bounds) -> bool {
    inner.minx() >= outer.minx()
        && inner.maxx() <= outer.maxx()
        && inner.miny() >= outer.miny()
        && inner.maxy() <= outer.maxy()
}

fn bounds_disjoint(left: Bounds, right: Bounds) -> bool {
    left.maxx() < right.minx()
        || right.maxx() < left.minx()
        || left.maxy() < right.miny()
        || right.maxy() < left.miny()
}

/// Overlay already-charted geographic split parts with exact planar topology
/// while retaining vertices that still define geographic metric edges.
pub(crate) fn overlay_all_geographic_chart<S: Borrow<Shape>, M: crate::geometry::ChartMetric>(
    geometries: &[S],
    frame: GeographicFrame,
    op: OverlayOp,
    strictness: Strictness,
    metric_access: Option<&SourceArcMetricAccess<'_, '_, M>>,
) -> Result<Shape> {
    if geometries.is_empty() {
        return Err(GeometryErrorKind::EmptyGeometrySequence {
            operation: op.name(),
        }
        .into());
    }
    let mut inputs = Vec::with_capacity(geometries.len());
    inputs.extend(geometries.iter().map(std::borrow::Borrow::borrow));
    exact_partition_overlay_with_bounds_certificates::<M>(
        inputs,
        op,
        strictness,
        true,
        ExactPartitionBudget::Charge,
        &SourceArcContext::new(TopologyDomain::Geographic(frame), metric_access),
    )
}

pub(in crate::geometry) fn overlay_all_geographic_chart_with_sources(
    geometries: &[&Shape],
    operation: OverlayOp,
) -> Result<Vec<OverlayPolygonPartWithSources>> {
    if geometries.is_empty() {
        return Err(GeometryErrorKind::EmptyGeometrySequence {
            operation: operation.name(),
        }
        .into());
    }
    if operation != OverlayOp::Difference || geometries.len() != 2 {
        return Err(GeometryErrorKind::Invalid(
            "lineage-aware chart overlay requires a binary difference".into(),
        )
        .into());
    }
    ExactSourceInventory::from_shapes(geometries)?.overlay_polygon_parts_with_source_roles(
        operation,
        0,
        GeographicFrame::crs84(),
    )
}

pub(crate) fn overlay_all_geographic_with_metric_and_frame<
    S: Borrow<Shape>,
    M: crate::geometry::ChartMetric,
>(
    geometries: &[S],
    frame: GeographicFrame,
    output_frame: &Frame,
    op: OverlayOp,
    strictness: Strictness,
    metric_access: &SourceArcMetricAccess<'_, '_, M>,
) -> Result<Shape> {
    if geometries.is_empty() {
        return Err(GeometryErrorKind::EmptyGeometrySequence {
            operation: op.name(),
        }
        .into());
    }
    let originals = GeographicInputs::try_from_shapes(frame, geometries.iter().map(Borrow::borrow))
        .map_err(|error| overlay_view_error(&error))?;
    let context = SourceArcContext::new(TopologyDomain::Geographic(frame), Some(metric_access));
    publication::overlay_all_geographic_from_with_output_frame(
        &originals,
        op,
        strictness,
        true,
        &context,
        output_frame,
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

#[cfg(test)]
pub(super) fn overlay_all_with_bounds_certificates<S: Borrow<Shape>>(
    geometries: &[S],
    domain: TopologyDomain,
    op: OverlayOp,
    strictness: Strictness,
    bounds_certificates: bool,
) -> Result<Shape> {
    overlay_all_with_bounds_certificates_and_metric::<S, crate::crs::GeodesicMetric>(
        geometries,
        &SourceArcContext::new(domain, None),
        op,
        strictness,
        bounds_certificates,
    )
}

fn overlay_all_with_bounds_certificates_and_metric<
    S: Borrow<Shape>,
    M: crate::geometry::ChartMetric,
>(
    geometries: &[S],
    context: &SourceArcContext<'_, '_, M>,
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

    match context.domain() {
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
            overlay_all_geographic_from_with_metric(
                &originals,
                op,
                strictness,
                bounds_certificates,
                context,
            )
        },
    }
}

#[derive(Clone, Copy)]
enum ExactPartitionBudget {
    Charge,
    Ignore,
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
    if operation == OverlayOp::Intersection
        && let [input] = inputs.as_slice()
    {
        return Ok((**input).clone());
    }
    exact_partition_overlay_with_bounds_certificates::<crate::crs::GeodesicMetric>(
        inputs,
        operation,
        strictness,
        bounds_certificates,
        partition_budget,
        &SourceArcContext::cartesian(),
    )
}

/// Try point-set membership for an admitted binary Cartesian puntal pair.
fn exact_cartesian_binary_puntal_overlay(
    inputs: &mut [&Shape],
    operation: OverlayOp,
    strictness: Strictness,
    bounds_certificates: bool,
    partition_budget: ExactPartitionBudget,
) -> Result<Shape> {
    puntal_nary_lane!(
        inputs,
        operation,
        strictness,
        &SourceArcContext::cartesian()
    );
    sort_overlay_inputs!(inputs, operation);
    let sorted_inputs: &[&Shape] = inputs;
    let fast_result = exact_cartesian_puntal_overlay(sorted_inputs, operation, strictness)?;
    if let Some(shape) = fast_result {
        return Ok(shape);
    }

    exact_partition_overlay_with_bounds_certificates::<crate::crs::GeodesicMetric>(
        sorted_inputs.to_vec(),
        operation,
        strictness,
        bounds_certificates,
        partition_budget,
        &SourceArcContext::cartesian(),
    )
}

fn exact_cartesian_line_polygon_overlay(
    left: &Shape,
    right: &Shape,
    operation: OverlayOp,
    strictness: Strictness,
    partition_budget: ExactPartitionBudget,
) -> Result<Option<Shape>> {
    let polygonal = |shape: &Shape| {
        matches!(
            shape,
            Shape::Polygon(_)
                | Shape::MultiPolygon(_)
                | Shape::Empty(GeometryKind::Polygon | GeometryKind::MultiPolygon, _)
        )
    };
    let (line_shape, line, polygon) = match (operation, left, right) {
        (
            OverlayOp::Intersection | OverlayOp::Difference,
            line_shape @ Shape::LineString(line),
            polygon,
        ) if line.len() <= 3 && polygonal(polygon) => (line_shape, line, polygon),
        (OverlayOp::Intersection, polygon, line_shape @ Shape::LineString(line))
            if line.len() <= 3 && polygonal(polygon) =>
        {
            (line_shape, line, polygon)
        },
        _ => return Ok(None),
    };
    let mut inputs = [left, right];
    sort_overlay_inputs!(inputs, operation);
    let [first, _] = inputs;
    let line_operand = u32::from(!matches!(first, Shape::LineString(_)));
    if operation == OverlayOp::Intersection && polygon.is_empty() {
        return Ok(Some(empty_nary_overlay_shape(&inputs, operation)));
    }
    if line_shape
        .bounds()
        .zip(polygon.bounds())
        .is_some_and(|(line, polygon)| !line.intersects(polygon))
    {
        return Ok((operation == OverlayOp::Intersection)
            .then(|| empty_nary_overlay_shape(&inputs, operation)));
    }
    if let Some(shape) = ExactSourceInventory::contact_free_xy_line(line, polygon, operation) {
        return Ok(Some(if shape.is_empty() {
            empty_nary_overlay_shape(&inputs, operation)
        } else {
            shape.normalize()?
        }));
    }
    let inventory = ExactSourceInventory::from_shapes(&inputs)?;
    let mut budget = match partition_budget {
        ExactPartitionBudget::Charge => Some(crate::geometry::ExpansionBudget::new(
            operation.name(),
            "exact partition",
        )),
        ExactPartitionBudget::Ignore => None,
    };
    let shape = inventory.overlay_line_polygon(
        polygon,
        line_operand,
        operation,
        overlay_declared_axes(&inputs, operation),
        strictness.is_strict(),
        budget.as_mut(),
    )?;
    Ok(Some(if shape.is_empty() {
        empty_nary_overlay_shape(&inputs, operation)
    } else {
        shape.normalize()?
    }))
}

// Point support is a set within each operand. XOR counts those operand sets,
// while all incident occurrences still participate in ordinate reconciliation.
#[expect(
    clippy::indexing_slicing,
    reason = "source ordinals are initialized from 0..inputs.len() and sorting preserves that admitted domain"
)]
fn exact_puntal_nary_overlay<M: crate::geometry::ChartMetric>(
    inputs: &[&Shape],
    operation: OverlayOp,
    strictness: Strictness,
    context: &SourceArcContext<'_, '_, M>,
) -> Result<Option<Shape>> {
    struct Membership {
        xy: XY,
        z: AxisValue,
        m: AxisValue,
        last_operand: usize,
        odd: bool,
    }
    let quotient_frame = match context.domain() {
        TopologyDomain::Cartesian => None,
        TopologyDomain::Geographic(frame) => Some(frame),
    };
    let mut membership =
        crate::geometry::HashMap::with_hasher(crate::collections::collection_hash_state()?.clone());
    let mut points = Vec::new();
    let identity = |point: Point| {
        quotient_frame.map_or_else(
            || (PointKey::new(point), point.xy()),
            |frame| {
                let latitude = crate::geographic::Latitude::from_admitted(frame, point.y);
                let longitude = frame
                    .longitude()
                    .canonicalize(point.x)
                    .expect("admitted geographic points have canonical longitudes")
                    .get();
                let key_longitude =
                    if latitude.identity(frame) == crate::geographic::SeamIdentity::Pole {
                        0.0
                    } else {
                        longitude
                    };
                (
                    PointKey::new(XY::new(key_longitude, latitude.get())),
                    XY::new(point.x, latitude.get()),
                )
            },
        )
    };
    let mut order_sources = false;
    for (operand, shape) in inputs.iter().enumerate() {
        points.clear();
        let mut owners = SharedShapeOwners::for_collections();
        if !collect_puntal_points(shape, &mut points, &mut owners) {
            return Ok(None);
        }
        for &point in &points {
            #[cfg(test)]
            NARY_CANDIDATE_WORK.with(|work| work.set(work.get().saturating_add(1)));
            let (key, xy) = identity(point);
            let value = membership.entry(key).or_insert_with(|| Membership {
                xy,
                z: AxisValue::default(),
                m: AxisValue::default(),
                last_operand: usize::MAX,
                odd: false,
            });
            if value.last_operand != operand {
                order_sources |= xy.x.to_bits() != value.xy.x.to_bits()
                    || xy.y.to_bits() != value.xy.y.to_bits();
                value.last_operand = operand;
                value.odd = !value.odd;
            }
            value.z.observe(point.z());
            value.m.observe(point.m());
        }
    }
    if order_sources {
        // Only signed-zero or geographic aliases need canonical source order
        // to choose a spelling. Ordinary point support never sorts its rows.
        let mut order: Vec<_> = (0..inputs.len()).collect();
        let mut equal_collections =
            HashSet::with_hasher(crate::collections::collection_hash_state()?.clone());
        order.sort_unstable_by(|&left, &right| {
            compare_puntal_sources(inputs[left], inputs[right], &mut equal_collections)
        });
        #[expect(
            clippy::iter_over_hash_type,
            reason = "the idempotent sentinel reset is independent of membership iteration order"
        )]
        for value in membership.values_mut() {
            value.last_operand = usize::MAX;
        }
        for operand in order {
            points.clear();
            let mut owners = SharedShapeOwners::for_collections();
            let _ = collect_puntal_points(inputs[operand], &mut points, &mut owners);
            for &point in &points {
                let (key, xy) = identity(point);
                let value = membership
                    .get_mut(&key)
                    .expect("the admitted source point has a membership entry");
                if value.last_operand == usize::MAX {
                    value.last_operand = operand;
                    value.xy = xy;
                }
            }
        }
    }
    points.clear();
    points.reserve(membership.len());
    for value in membership.into_values() {
        if operation == OverlayOp::Union || value.odd {
            points.push(Point::new_raw_axes(
                value.xy.x,
                value.xy.y,
                crate::geometry::ZOrdinate(value.z.value()),
                crate::geometry::MOrdinate(value.m.value()),
            ));
        }
    }
    if points.is_empty() {
        return Ok(Some(empty_nary_overlay_shape(inputs, operation)));
    }
    let axes = points
        .iter()
        .map(|point| point.axes())
        .reduce(CoordinateAxes::intersection)
        .expect("nonempty point result");
    let declared_axes = overlay_declared_axes(inputs, operation);
    if strictness.is_strict() && axes != declared_axes {
        return Err(GeometryErrorKind::ordinate_dropped(operation.name()));
    }
    for point in &mut points {
        *point = Point::new_raw_axes(
            point.x,
            point.y,
            crate::geometry::ZOrdinate(point.z().filter(|_| axes.has_z())),
            crate::geometry::MOrdinate(point.m().filter(|_| axes.has_m())),
        );
    }
    build_certified_shape(points, Vec::new(), Vec::new()).map(Some)
}

fn compare_puntal_sources(
    left: &Shape,
    right: &Shape,
    equal_collections: &mut HashSet<(usize, usize, usize, usize)>,
) -> std::cmp::Ordering {
    let (Shape::GeometryCollection(left), Shape::GeometryCollection(right)) = (left, right) else {
        return crate::geometry::compare_shapes(left, right);
    };
    let left = left.as_slice();
    let right = right.as_slice();
    let pair = (
        left.as_ptr().addr(),
        left.len(),
        right.as_ptr().addr(),
        right.len(),
    );
    // Literal source order still determines coordinate spelling, but equal
    // shared prefixes cost their owners rather than their expanded occurrences.
    if std::ptr::eq(left, right) || equal_collections.contains(&pair) {
        return std::cmp::Ordering::Equal;
    }
    for (left, right) in left.iter().zip(right) {
        let order = compare_puntal_sources(left, right, equal_collections);
        if !order.is_eq() {
            return order;
        }
    }
    let order = left.len().cmp(&right.len());
    if order.is_eq() {
        equal_collections.insert(pair);
    }
    order
}

fn exact_partition_overlay_with_bounds_certificates<M: crate::geometry::ChartMetric>(
    mut inputs: Vec<&Shape>,
    operation: OverlayOp,
    strictness: Strictness,
    bounds_certificates: bool,
    partition_budget: ExactPartitionBudget,
    context: &SourceArcContext<'_, '_, M>,
) -> Result<Shape> {
    puntal_nary_lane!(&inputs, operation, strictness, context);
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
        interacting_components(&bounds, operation)
    } else {
        None
    };
    let shape = match components {
        None => exact_partition_overlay::<M>(
            inputs,
            operation,
            declared_axes,
            strict,
            partition_budget,
            context,
        )?,
        Some(components) => {
            let mut shapes = Vec::with_capacity(components.len());
            let mut subset = Vec::new();
            for component in components {
                subset.clear();
                subset.reserve(component.len());
                #[expect(
                    clippy::indexing_slicing,
                    reason = "component ordinals are returned from the same input-sized bounds array"
                )]
                subset.extend(component.iter().map(|&index| inputs[index]));
                shapes.push(exact_partition_overlay::<M>(
                    &subset,
                    operation,
                    declared_axes,
                    strict,
                    partition_budget,
                    context,
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
            debug_assert!(matches!(
                operation,
                OverlayOp::Intersection | OverlayOp::Difference
            ));
            let left_points = unique_xy_points(left_points)?;
            let right_points = unique_xy_points(right_points)?;
            let right_keys = point_keys(&right_points)?;
            let intersection = operation == OverlayOp::Intersection;
            let points = left_points
                .into_iter()
                .filter(|point| right_keys.contains(&PointKey::new(*point)) == intersection)
                .collect();
            point_overlay_shape(
                points,
                inputs,
                operation,
                strictness,
                &SourceArcContext::cartesian(),
            )
            .map(Some)
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
        return point_overlay_shape(
            points,
            inputs,
            operation,
            strictness,
            &SourceArcContext::cartesian(),
        )
        .map(Some);
    }

    if points.is_empty() {
        return Ok(Some(if other.is_empty() {
            empty_nary_overlay_shape(inputs, operation)
        } else {
            other.clone()
        }));
    }
    let point_shape = point_overlay_shape(
        points,
        inputs,
        operation,
        strictness,
        &SourceArcContext::cartesian(),
    )?;
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
    context: &SourceArcContext<'_, '_>,
) -> Result<Shape> {
    if points.is_empty() {
        return Ok(empty_nary_overlay_shape(inputs, operation));
    }
    let shape = build_certified_shape(points, Vec::new(), Vec::new())?;
    carry_overlay_ordinates(shape, inputs, operation, strictness.is_strict(), context)
}

fn point_keys(points: &[Point]) -> Result<HashSet<PointKey>> {
    let mut keys = HashSet::with_hasher(crate::collections::collection_hash_state()?.clone());
    keys.reserve(points.len());
    keys.extend(points.iter().copied().map(PointKey::new));
    Ok(keys)
}

fn puntal_points(shape: &Shape) -> Option<Vec<Point>> {
    if shape.topological_dimension() != Dimension::Point {
        return None;
    }

    let mut points = Vec::new();
    let mut owners = SharedShapeOwners::for_collections();
    collect_puntal_points(shape, &mut points, &mut owners).then_some(points)
}

fn collect_puntal_points(
    shape: &Shape,
    points: &mut Vec<Point>,
    owners: &mut SharedShapeOwners,
) -> bool {
    match shape {
        Shape::Point(point) => points.push(*point),
        Shape::MultiPoint(multipoint) => points.extend(multipoint.points()),
        Shape::Empty(
            GeometryKind::Point | GeometryKind::MultiPoint | GeometryKind::GeometryCollection,
            _,
        ) => {},
        Shape::GeometryCollection(collection) => {
            if !owners.first_collection(collection.as_slice(), collection.members_are_shared()) {
                return true;
            }
            for part in collection {
                if !collect_puntal_points(part, points, owners) {
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
            coords.replace_z(None, false)?
        };
        if axes.has_m() {
            Ok(coords)
        } else {
            coords.replace_m(None, false)
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
    build_certified_shape(points, lines, polygons)
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
fn exact_partition_overlay<M: crate::geometry::ChartMetric>(
    inputs: &[&Shape],
    operation: OverlayOp,
    declared_axes: CoordinateAxes,
    strict: bool,
    partition_budget: ExactPartitionBudget,
    context: &SourceArcContext<'_, '_, M>,
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
    if let TopologyDomain::Geographic(frame) = context.domain() {
        // Chart-collinear vertices still define separate geographic metric
        // edges, so exact planar simplification must not merge them.
        partition.set_geographic_frame(frame);
    }
    inventory.overlay_shape(&partition, operation, declared_axes, strict, context)
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
    let shape = exact_partition_overlay::<crate::crs::GeodesicMetric>(
        inputs,
        operation,
        overlay_declared_axes(inputs, operation),
        strictness.is_strict(),
        ExactPartitionBudget::Charge,
        &SourceArcContext::cartesian(),
    )?;
    Ok(if shape.is_empty() {
        empty_nary_overlay_shape(inputs, operation)
    } else {
        shape.normalize()?
    })
}

/// Voronoi owns the published-output budget for these intermediate clips.
pub(in crate::geometry) fn binary_cartesian_difference_without_partition_budget(
    left: &Shape,
    right: &Shape,
    strictness: Strictness,
) -> Result<Shape> {
    let operation = OverlayOp::Difference;
    let mut inputs = vec![left, right];
    if left.topological_dimension() == Dimension::Point
        || right.topological_dimension() == Dimension::Point
    {
        return exact_cartesian_binary_puntal_overlay(
            inputs.as_mut_slice(),
            operation,
            strictness,
            true,
            ExactPartitionBudget::Ignore,
        );
    }
    if let Some(shape) = exact_cartesian_line_polygon_overlay(
        left,
        right,
        operation,
        strictness,
        ExactPartitionBudget::Ignore,
    )? {
        return Ok(shape);
    }
    exact_cartesian_overlay(
        inputs,
        operation,
        strictness,
        true,
        ExactPartitionBudget::Ignore,
    )
}

pub(in crate::geometry) fn binary_cartesian_intersection_with_edge_roles(
    generated: &Shape,
    preserved: &Shape,
) -> Result<Vec<OverlayPolygonPartWithSources>> {
    let inputs = [generated, preserved];
    ExactSourceInventory::from_shapes(&inputs)?
        .overlay_polygon_parts_with_preserved_operand(OverlayOp::Intersection, 1)
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
    overlay_all_geographic_from_with_metric::<crate::crs::GeodesicMetric>(
        originals,
        op,
        strictness,
        bounds_certificates,
        &SourceArcContext::new(TopologyDomain::Geographic(originals.frame()), None),
    )
}

fn geographic_rectangle_operation_result(
    originals: &GeographicInputs<'_>,
    shapes: &[&Shape],
    operation: OverlayOp,
) -> Option<Shape> {
    let [left, right] = shapes else {
        return None;
    };
    let frame = originals.frame();
    let left_rectangle = canonical_geographic_rectangle_bounds(left, frame);
    let right_rectangle = canonical_geographic_rectangle_bounds(right, frame);
    if left_rectangle.is_none() && right_rectangle.is_none() {
        return None;
    }
    let left_bounds = left.bounds();
    let right_bounds = right.bounds();
    if !left_bounds.is_some_and(|bounds| {
        crate::geometry::raw_coordinate_bounds_are_sound_in_frame(Some(left), bounds, frame)
    }) || !right_bounds.is_some_and(|bounds| {
        crate::geometry::raw_coordinate_bounds_are_sound_in_frame(Some(right), bounds, frame)
    }) {
        return None;
    }
    rectangle_operation_result(
        left,
        right,
        operation,
        left_bounds,
        right_bounds,
        left_rectangle,
        right_rectangle,
    )
}

#[expect(
    clippy::float_cmp,
    reason = "rectangle recognition requires exact stored-coordinate equality while preserving signed-zero equality"
)]
fn canonical_geographic_rectangle_bounds(
    shape: &Shape,
    frame: crate::geographic::GeographicFrame,
) -> Option<Bounds> {
    let Shape::Polygon(polygon) = shape else {
        return None;
    };
    if !polygon.holes.is_empty() || polygon.axes() != CoordinateAxes::XY {
        return None;
    }
    let ring = polygon.shell.coords();
    let coordinate_count = ring.coord_count();
    if coordinate_count < 5 || coordinate_count.wrapping_sub(3) % 2 != 0 {
        return None;
    }
    let segments = coordinate_count.wrapping_sub(3) / 2;
    let first = ring.point_at(0);
    let east_south = ring.point_at(segments);
    let east_north = ring.point_at(segments.wrapping_add(1));
    let west = first.x;
    let south = first.y;
    let east = east_south.x;
    let north = east_north.y;
    if !(west < east && south < north && east_south.y == south && east_north.x == east) {
        return None;
    }

    let mut previous_x = west;
    for index in 1..=segments {
        let point = ring.point_at(index);
        if point.y != south || point.x <= previous_x {
            return None;
        }
        previous_x = point.x;
    }
    if previous_x != east {
        return None;
    }

    previous_x = east;
    for index in segments.wrapping_add(2)..=segments.wrapping_mul(2).wrapping_add(1) {
        let point = ring.point_at(index);
        if point.y != north || point.x >= previous_x {
            return None;
        }
        previous_x = point.x;
    }
    let closing = ring.point_at(coordinate_count.wrapping_sub(1));
    if previous_x != west || !crate::geometry::same_point(first, closing) {
        return None;
    }
    let Ok(bounds) = Bounds::new(west, south, east, north) else {
        return None;
    };
    (crate::geometry::raw_coordinate_bounds_are_sound_in_frame(None, bounds, frame)
        && crate::geometry::raw_coordinate_bounds_are_sound_in_frame(Some(shape), bounds, frame))
    .then_some(bounds)
}

#[cfg(test)]
pub(crate) fn overlay_all_geographic_q_from(
    originals: &GeographicInputs<'_>,
    op: OverlayOp,
    strictness: Strictness,
    bounds_certificates: bool,
) -> Result<Shape> {
    publication::exact_geographic_q_from(originals, op, strictness, bounds_certificates)
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
    geographic_admission_error(error)
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
#[expect(
    clippy::indexing_slicing,
    reason = "parent IDs stay in the bounds domain; sweep links stay in the entries domain plus its terminal sentinel"
)]
fn interacting_components(bounds: &[Option<Bounds>], op: OverlayOp) -> Option<Vec<Vec<usize>>> {
    const fn root(parent: &mut [usize], mut node: usize) -> usize {
        while parent[node] != node {
            parent[node] = parent[parent[node]];
            node = parent[node];
        }
        node
    }
    if op == OverlayOp::Intersection {
        return None;
    }
    let count = bounds.len();
    let mut parent = Vec::with_capacity(count);
    parent.extend(0..count);
    let visit_limit = count.saturating_mul(8);
    if count <= 256 {
        // The existing compact sweep costs less on ordinary small overlays.
        // Its pair budget still bounds coincident inputs before partitioning.
        let mut visits = 0_usize;
        let flow =
            crate::geometry::for_each_indexed_overlapping_bounds_pair(bounds, |left, right| {
                visits = visits.wrapping_add(1);
                #[cfg(test)]
                NARY_CANDIDATE_WORK.with(|work| work.set(work.get().saturating_add(1)));
                if visits > visit_limit {
                    return ControlFlow::Break(());
                }
                let (a, b) = (root(&mut parent, left), root(&mut parent, right));
                parent[a.max(b)] = a.min(b);
                ControlFlow::Continue(())
            })
            .expect("at most 256 bounds fit the sweep's u32 ordinal domain");
        if flow.is_break() {
            return None;
        }
    } else {
        let mut entries: Vec<_> = bounds
            .iter()
            .enumerate()
            .filter_map(|(ordinal, bounds)| bounds.map(|bounds| (ordinal, bounds)))
            .collect();
        entries.sort_unstable_by(|&(_, left), &(_, right)| {
            left.minx()
                .total_cmp(&right.minx())
                .then_with(|| right.maxx().total_cmp(&left.maxx()))
                .then_with(|| left.miny().total_cmp(&right.miny()))
                .then_with(|| right.maxy().total_cmp(&left.maxy()))
        });
        // A contained box adds no future connection that its container cannot add.
        // Remove it from the sweep's linked order after joining their components.
        // Larger boxes precede their containers' children even at equal minx.
        let mut next: Vec<_> = (1..=entries.len()).collect();
        let mut left = 0;
        let mut visits = 0_usize;
        while let Some(&(left_ordinal, left_bounds)) = entries.get(left) {
            let mut previous = left;
            let mut right = next[left];
            while let Some(&(right_ordinal, right_bounds)) = entries.get(right) {
                if right_bounds.minx() > left_bounds.maxx() {
                    break;
                }
                visits = visits.wrapping_add(1);
                #[cfg(test)]
                NARY_CANDIDATE_WORK.with(|work| work.set(work.get().saturating_add(1)));
                if visits > visit_limit {
                    // Decomposition is optional. Dense nonnested windows must not
                    // spend quadratic work before the exact partition can start.
                    return None;
                }
                if left_bounds.intersects(right_bounds) {
                    let (a, b) = (
                        root(&mut parent, left_ordinal),
                        root(&mut parent, right_ordinal),
                    );
                    parent[a.max(b)] = a.min(b);
                    if left_bounds.maxx() >= right_bounds.maxx()
                        && left_bounds.miny() <= right_bounds.miny()
                        && left_bounds.maxy() >= right_bounds.maxy()
                    {
                        next[previous] = next[right];
                        right = next[right];
                        continue;
                    }
                }
                previous = right;
                right = next[right];
            }
            left = next[left];
        }
    }
    for node in 0..count {
        parent[node] = root(&mut parent, node);
    }
    let mut components: Vec<Vec<usize>> = Vec::new();
    if op == OverlayOp::Difference {
        let left = parent.iter().filter(|&&root| root == 0).count();
        if left == count {
            return None;
        }
        let mut component = Vec::with_capacity(left);
        component.extend((0..count).filter(|&node| parent[node] == 0));
        components.push(component);
        return Some(components);
    }
    let roots = (0..count).filter(|&node| parent[node] == node).count();
    if roots == 1 {
        return None;
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
    Some(components)
}

#[cfg(test)]
mod nary_work_tests {
    use pyo3::prelude::*;
    use pyo3::types::{PyList, PyTuple};

    use super::NARY_CANDIDATE_WORK;

    #[test]
    #[cfg_attr(miri, ignore = "Miri cannot execute CPython FFI")]
    fn public_nary_overlays_bound_repeated_input_work() {
        crate::test_support::initialize_python();
        Python::attach(|py| {
            let module = PyModule::new(py, "gometry._lib").unwrap();
            crate::lib(&module).unwrap();
            let point = module
                .getattr("Point")
                .unwrap()
                .call1((0.25, 0.25))
                .unwrap();
            let polygon = module.getattr("box").unwrap().call1((0, 0, 1, 1)).unwrap();
            for count in [128_usize, 512] {
                let points = PyList::empty(py);
                let polygons = PyList::empty(py);
                for _ in 0..count {
                    points.append(&point).unwrap();
                    polygons.append(&polygon).unwrap();
                }
                points.append(py.None()).unwrap();
                let array = module
                    .getattr("GeometryArray")
                    .unwrap()
                    .call1((&points,))
                    .unwrap();
                for (rows, expected) in [
                    (&array, "POINT (0.25 0.25)"),
                    (polygons.as_any(), "POLYGON ((0 0, 1 0, 1 1, 0 1, 0 0))"),
                ] {
                    for operation in ["union_all", "symmetric_difference_all"] {
                        NARY_CANDIDATE_WORK.with(|work| work.set(0));
                        let output = module.getattr(operation).unwrap().call1((rows,)).unwrap();
                        let expected = if operation == "symmetric_difference_all" {
                            if rows.is(&array) {
                                "POINT EMPTY"
                            } else {
                                "POLYGON EMPTY"
                            }
                        } else {
                            expected
                        };
                        assert_eq!(
                            output
                                .call_method0("to_wkt")
                                .unwrap()
                                .extract::<String>()
                                .unwrap(),
                            expected,
                        );
                        let work = NARY_CANDIDATE_WORK.with(std::cell::Cell::get);
                        assert!(work > 0, "{operation} did not reach n-ary candidate work");
                        assert!(
                            work < count.saturating_mul(16),
                            "{operation} examined {work} candidates for {count} repeated rows"
                        );
                    }
                }
                let keys = PyList::new(py, std::iter::repeat_n(0, count.wrapping_add(1))).unwrap();
                NARY_CANDIDATE_WORK.with(|work| work.set(0));
                let dissolved = array.call_method1("dissolve", (&keys,)).unwrap();
                let shapes = dissolved.cast::<PyTuple>().unwrap().get_item(0).unwrap();
                let output = shapes.get_item(0).unwrap();
                assert_eq!(
                    output
                        .call_method0("to_wkt")
                        .unwrap()
                        .extract::<String>()
                        .unwrap(),
                    "POINT (0.25 0.25)",
                );
                let work = NARY_CANDIDATE_WORK.with(std::cell::Cell::get);
                assert!(work > 0, "dissolve did not reach n-ary candidate work");
                assert!(
                    work < count.saturating_mul(16),
                    "dissolve examined {work} candidates for {count} repeated rows"
                );
            }
        });
    }
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

#[expect(
    clippy::indexing_slicing,
    reason = "difference preserves the first operand and callers admit a non-empty input slice"
)]
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

#[cfg(test)]
mod extracted_tests;
