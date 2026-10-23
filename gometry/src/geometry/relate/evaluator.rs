use std::cell::{Cell, OnceCell};
use std::cmp::Ordering;
use std::ops::ControlFlow;

use crate::collections::{HashSet, HashSetExt as _, collection_hash_state};
use crate::error::{ResourceError, Result};
use crate::geometry::overlay::{
    ExactPointForest, StagedExactSources, StagedRelationItem, StagedSourceComponent,
    homogeneous_midpoint,
};
use crate::geometry::predicate_spec::Predicate;
use crate::geometry::relate::matrix_predicates::{self, CellMask};
use crate::geometry::relate::{De9im, De9imInterval, Loc, OperandShape};
use crate::geometry::segments::contact::TouchLocation;
use crate::geometry::segments::crossing_point_with;
use crate::geometry::source_support::{
    OccupiedToward, PointMembership, SourcePointLocation, cartesian_row_point_location,
    cartesian_row_point_location_and_area, occupied_toward,
};
#[cfg(test)]
use crate::geometry::source_support::{
    cartesian_row_exact_offset_location_and_area, cartesian_row_exact_point_location_and_area,
};
use crate::geometry::storage::{PackedRow, ShapeRow};
use crate::geometry::tessellation::exact::{ExactDyadic, compare_ratios};
use crate::geometry::{
    Bounds, CompiledPattern, Contact, Coordinates as _, Dimension, GeometryErrorKind, HashMap,
    Point, PointKey, PreparedPlan, PreparedPointQueries, RingKind, Segment, Shape, XY, dimension,
    for_each_candidate_pair, ring_winding, same_point, segment_contact_exact,
};

mod atoms;
mod contacts;
mod exhaustive;
mod model;
mod points;
mod query;
mod rectangle;
mod sides;
mod staging;
mod witnesses;

#[cfg(test)]
mod tests;

use model::{
    AtomPoint, Component, ContactEvent, Coverage, EventKind, EventPoint, EventPointKind, ExactAtom,
    ExactCoordinates, Operand, SourceCertificates, StagedGeometry, by_operand, by_operand_mut,
    opposite_operand, staged_operand,
};
#[cfg(test)]
use query::EndOnly;
use query::{LOCATIONS, MatrixQuery, PatternQuery, PredicateQuery, Query};

pub(crate) fn relate(left: &PreparedPlan, right: &PreparedPlan) -> Result<De9im> {
    let operands = [Operand::from_plan(left), Operand::from_plan(right)];
    evaluate(&operands, &MatrixQuery)
}

pub(crate) fn predicate(
    left: &PreparedPlan,
    right: &PreparedPlan,
    predicate: Predicate,
) -> Result<bool> {
    let operands = [Operand::from_plan(left), Operand::from_plan(right)];
    evaluate(&operands, &PredicateQuery(predicate))
}

pub(crate) fn predicate_rows(
    left: ShapeRow<'_>,
    right: ShapeRow<'_>,
    predicate: Predicate,
) -> Result<bool> {
    let operands = [Operand::from_row(left), Operand::from_row(right)];
    evaluate(&operands, &PredicateQuery(predicate))
}

pub(crate) fn predicate_prepared_row(
    plan: &PreparedPlan,
    row: ShapeRow<'_>,
    plan_is_left: bool,
    predicate: Predicate,
) -> Result<bool> {
    let plan = Operand::from_plan(plan);
    let row = Operand::from_row(row);
    let operands = if plan_is_left {
        [plan, row]
    } else {
        [row, plan]
    };
    evaluate(&operands, &PredicateQuery(predicate))
}

pub(crate) fn pattern(
    left: &PreparedPlan,
    right: &PreparedPlan,
    pattern: CompiledPattern,
) -> Result<bool> {
    let operands = [Operand::from_plan(left), Operand::from_plan(right)];
    evaluate(&operands, &PatternQuery(pattern))
}

fn evaluate<Q: Query>(operands: &[Operand<'_>; 2], query: &Q) -> Result<Q::Output> {
    evaluate_mode::<Q, true>(operands, query)
}

fn evaluate_mode<Q: Query, const SATURATE: bool>(
    operands: &[Operand<'_>; 2],
    query: &Q,
) -> Result<Q::Output> {
    use self::contacts::contact_sweep;
    use self::exhaustive::exhaustive_pass;
    use self::points::{
        operand_shape, point_only_phase, row_dimension, settle_disjoint_interiors,
        settle_empty_operands,
    };
    use self::rectangle::rectangle_relation;
    use self::sides::initialize_clean_ring_sides;
    use self::staging::{
        apply_dimension_certificates, apply_staged_dimension_floors, row_certificates,
        stage_components,
    };
    use self::witnesses::representative_probes;

    if SATURATE {
        for point_operand in [
            matrix_predicates::Operand::Left,
            matrix_predicates::Operand::Right,
        ] {
            let Some(point_query) = query.point_membership(point_operand) else {
                continue;
            };
            let [left, right] = operands;
            let (point_operand, target) = match point_operand {
                matrix_predicates::Operand::Left => (left, right),
                matrix_predicates::Operand::Right => (right, left),
            };
            let Some(point) = point_operand.row.point() else {
                continue;
            };
            let matches = target.point_membership(point, point_query.membership)?;
            return Ok(point_query.result(matches));
        }
    }
    // Intersection observes union support, not repeated line-endpoint parity.
    // Flatten shared collection owners before occurrence-aware contact staging.
    if query.intersection_result(true).is_some() {
        let [left, right] = operands;
        let (left_set, right_set) = (left.compact_set()?, right.compact_set()?);
        if left_set.is_some() || right_set.is_some() {
            let left = left_set
                .as_ref()
                .map_or_else(|| left.reborrow(), |shape| Operand::from_row(shape.into()));
            let right = right_set
                .as_ref()
                .map_or_else(|| right.reborrow(), |shape| Operand::from_row(shape.into()));
            return evaluate_mode::<Q, SATURATE>(&[left, right], query);
        }
    }
    if SATURATE
        && operands[0].row.same_stored_geometry(operands[1].row)
        && let Some(answer) = query.same_stored_geometry(&operands[0])?
    {
        return Ok(answer);
    }
    let bounds = [operands[0].bounds(), operands[1].bounds()];
    if SATURATE && let Some(matrix) = rectangle_relation(operands, &bounds) {
        return Ok(query.decided_matrix(matrix));
    }
    if SATURATE && let Some(answer) = query.intersection_result(true) {
        let [left, right] = operands;
        let [left_bounds, right_bounds] = bounds;
        for (source, target, target_bounds) in
            [(left, right, right_bounds), (right, left, left_bounds)]
        {
            let Some(point) = target_bounds.and_then(|bounds| source.candidate_point(bounds))
            else {
                continue;
            };
            if target.point_membership(point, PointMembership::BoundaryInclusive)?
                // A stored shell vertex can be cancelled by its own winding or holes.
                && (!source.row.has_area_parts()
                    || source
                        .point_membership(point, PointMembership::BoundaryInclusive)?)
            {
                return Ok(answer);
            }
        }
    }
    let dimensions = [
        row_dimension(operands[0].row),
        row_dimension(operands[1].row),
    ];
    let shapes = [
        operand_shape(operands[0].row, dimensions[0]),
        operand_shape(operands[1].row, dimensions[1]),
    ];
    let empty = [dimensions[0].is_none(), dimensions[1].is_none()];
    let mut interval = De9imInterval::open(shapes[0], shapes[1]);
    settle_empty_operands(&mut interval, empty);
    if let [Some(left), Some(right)] = bounds
        && !left.intersects(right)
    {
        settle_disjoint_interiors(&mut interval);
    }
    if let Some(answer) = query.verdict(&interval) {
        return Ok(answer);
    }

    let certificates = if query.needs_dimensions() {
        let certificates = [
            row_certificates(operands[0].row)?,
            row_certificates(operands[1].row)?,
        ];
        apply_dimension_certificates(&mut interval, dimensions, certificates);
        certificates
    } else {
        [SourceCertificates::default(); 2]
    };
    if let Some(answer) = query.verdict(&interval) {
        return Ok(answer);
    }

    if let Some(answer) =
        point_only_phase::<Q, SATURATE>(&mut interval, operands, dimensions, certificates, query)?
    {
        return Ok(answer);
    }
    if let Some(answer) = query.verdict(&interval) {
        return Ok(answer);
    }

    let staged_sources =
        StagedExactSources::from_rows_for_relation([operands[0].row, operands[1].row].into_iter())?;
    let rows = [operands[0].row, operands[1].row];
    let mut staged = stage_components(staged_sources, &rows)?;
    apply_staged_dimension_floors(&mut interval, dimensions, &staged, query.needs_dimensions());
    if let Some(answer) = query.verdict(&interval) {
        return Ok(answer);
    }

    if let Some(answer) =
        representative_probes::<Q, SATURATE>(&mut interval, operands, &mut staged, query)?
    {
        return Ok(answer);
    }
    if let Some(answer) = query.verdict(&interval) {
        return Ok(answer);
    }

    let mut events = Vec::new();
    let mut coordinates = ExactCoordinates::default();
    let mut forest = ExactPointForest::with_capacity(staged.sources.segments(), 0);
    let sources = &staged.sources;
    let components = &mut staged.components;
    let segment_component = &staged.segment_component;
    let operand_has_ring = staged.operand_has_ring;
    if let Some(answer) = contact_sweep::<Q, SATURATE>(
        &mut interval,
        operands,
        query,
        sources,
        components,
        segment_component,
        operand_has_ring,
        &mut events,
        &mut coordinates,
    )? {
        return Ok(answer);
    }
    // Adjacent vertices of a clean winding ring have the same own-operand
    // node and edge labels. Inter-operand contacts retain every exceptional
    // node; dirty and collapsed components retain their full incidence.
    events.retain(|event| {
        let [left, right] = event.components.map(|id| {
            components
                .get(id as usize)
                .expect("contact component was admitted")
        });
        left.side != right.side
            || left.dirty
            || right.dirty
            || left.source.ring_kind != Some(RingKind::Winding)
            || right.source.ring_kind != Some(RingKind::Winding)
    });
    initialize_clean_ring_sides(operands, sources, components, &mut coordinates)?;
    if let Some(answer) = exhaustive_pass::<Q, SATURATE>(
        &mut interval,
        operands,
        query,
        &staged,
        &mut events,
        &mut forest,
        &mut coordinates,
    )? {
        return Ok(answer);
    }
    query.finish(&interval)
}
