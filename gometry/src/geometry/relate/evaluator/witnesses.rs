#[cfg(all(test, debug_assertions))]
use super::model::update_atom_metrics;
use super::points::{point_from_key, witness_point};
use super::{
    Component, ContactEvent, De9imInterval, ExactPointForest, GeometryErrorKind, HashMap, Operand,
    Point, Query, Result, RingKind, Segment, SourcePointLocation, StagedGeometry,
    StagedRelationItem, StagedSourceComponent, XY, by_operand, by_operand_mut, matrix_predicates,
    same_point,
};

pub(super) fn event_node_witnesses<Q: Query>(
    interval: &mut De9imInterval,
    operands: &[Operand<'_>; 2],
    query: &Q,
    events: &[ContactEvent],
    components: &[Component],
    forest: &mut ExactPointForest<'_>,
) -> Result<Option<Q::Output>> {
    let mut nodes = HashMap::<u32, [Option<SourcePointLocation>; 2]>::default();
    for event in events {
        for point in event.points.iter().flatten() {
            let root = forest.find(point.handle.expect("event points were interned"));
            let locations = nodes.entry(root).or_insert([None; 2]);
            for &id in &event.components {
                let component = components
                    .get(id as usize)
                    .expect("contact component was admitted");
                if !component.dirty
                    && component.source.ring_kind == Some(RingKind::Winding)
                    && let Some(seed) = component.seed
                    && seed.area
                {
                    *by_operand_mut(locations, component.side) = Some(seed);
                }
            }
        }
    }
    for event in events {
        for point in event.points.iter().flatten() {
            let Some(handle) = point.handle else {
                return Err(GeometryErrorKind::Invalid(
                    "event node was not interned before the exhaustive pass".into(),
                )
                .into());
            };
            let root = forest.find(handle);
            let Some([left, right]) = nodes.remove(&root) else {
                continue;
            };
            let (left, right) = if let Some(key) = forest.points().stored_key(root) {
                let point = point_from_key(key);
                let locate = |side: matrix_predicates::Operand,
                              known: Option<SourcePointLocation>| {
                    if let Some(known) = known {
                        return Ok(known);
                    }
                    #[cfg(all(test, debug_assertions))]
                    update_atom_metrics(|metrics| {
                        metrics.node_locations = metrics.node_locations.wrapping_add(1);
                    });
                    by_operand(operands, side).locate_stored(point)
                };
                (
                    locate(matrix_predicates::Operand::Left, left)?,
                    locate(matrix_predicates::Operand::Right, right)?,
                )
            } else {
                let exact = forest.points().exact(root)?;
                let probe = crate::geometry::source_support::ExactProbe::new(&exact);
                #[cfg(all(test, debug_assertions))]
                update_atom_metrics(|metrics| {
                    metrics.node_locations = metrics.node_locations.wrapping_add(
                        usize::from(left.is_none()).wrapping_add(usize::from(right.is_none())),
                    );
                });
                (
                    left.map_or_else(|| operands[0].locate_probe(&probe), Ok)?,
                    right.map_or_else(|| operands[1].locate_probe(&probe), Ok)?,
                )
            };
            witness_point(interval, left, right, query.needs_dimensions());
            if let Some(answer) = query.verdict(interval) {
                return Ok(Some(answer));
            }
        }
    }
    Ok(None)
}

pub(super) fn representative_probes<Q: Query, const SATURATE: bool>(
    interval: &mut De9imInterval,
    operands: &[Operand<'_>; 2],
    staged: &mut StagedGeometry,
    query: &Q,
) -> Result<Option<Q::Output>> {
    let source_order = staged
        .sources
        .relation_source_order()
        .expect("relation staging retains source order");
    for order_index in 0..source_order.len() {
        if SATURATE && interval.settled_for(query.cells()) {
            return Ok(query.verdict(interval));
        }
        let item = *source_order
            .get(order_index)
            .expect("source-order index lies within staged entries");
        match item {
            StagedRelationItem::Isolated(index) => {
                let point = *staged
                    .sources
                    .isolated()
                    .get(index)
                    .expect("isolated source ordinal lies within staged points");
                let locations = [
                    operands[0].locate_stored(Point::new_raw_xy(point.x, point.y))?,
                    operands[1].locate_stored(Point::new_raw_xy(point.x, point.y))?,
                ];
                witness_point(
                    interval,
                    locations[0],
                    locations[1],
                    query.needs_dimensions(),
                );
            },
            StagedRelationItem::Component(index) => {
                let staged_component = staged
                    .components
                    .get(index)
                    .expect("staged component ordinal lies within its table");
                let side = staged_component.side;
                let component = staged_component.source.clone();
                let mut representative = None;
                let mut last = None;
                visit_component_representatives(
                    &component,
                    staged.sources.segments(),
                    &mut |point| {
                        representative.get_or_insert(point);
                        if !representative.is_some_and(|first| same_point(first, point)) {
                            last = Some(point);
                        }
                    },
                );
                let Some(first) = representative else {
                    return Err(GeometryErrorKind::Invalid(
                        "a staged component has no representative point".into(),
                    )
                    .into());
                };
                let first_location = by_operand(operands, side)
                    .locate_stored(Point::new_raw_xy(first.x, first.y))?;
                let last_location = if let Some(last) = last {
                    Some(
                        by_operand(operands, side)
                            .locate_stored(Point::new_raw_xy(last.x, last.y))?,
                    )
                } else {
                    None
                };
                let (left_location, right_location) = if side == matrix_predicates::Operand::Left {
                    (
                        first_location,
                        by_operand(operands, matrix_predicates::Operand::Right)
                            .locate_stored(Point::new_raw_xy(first.x, first.y))?,
                    )
                } else {
                    (
                        by_operand(operands, matrix_predicates::Operand::Left)
                            .locate_stored(Point::new_raw_xy(first.x, first.y))?,
                        first_location,
                    )
                };
                witness_point(
                    interval,
                    left_location,
                    right_location,
                    query.needs_dimensions(),
                );
                if let Some(last) = last
                    && let Some(last_location) = last_location
                {
                    let (left, right) = if side == matrix_predicates::Operand::Left {
                        (
                            last_location,
                            by_operand(operands, matrix_predicates::Operand::Right)
                                .locate_stored(Point::new_raw_xy(last.x, last.y))?,
                        )
                    } else {
                        (
                            by_operand(operands, matrix_predicates::Operand::Left)
                                .locate_stored(Point::new_raw_xy(last.x, last.y))?,
                            last_location,
                        )
                    };
                    witness_point(interval, left, right, query.needs_dimensions());
                }

                if component.ring.is_some()
                    && let Some(owner) = staged.components.get_mut(index)
                {
                    owner.seed = Some(first_location);
                } else if component.has_nonzero_segment
                    && let Some(owner) = staged.components.get_mut(index)
                {
                    owner.seed = Some(first_location);
                    owner.left_area = Some(first_location.area);
                }
            },
        }
        if let Some(answer) = query.verdict(interval) {
            return Ok(Some(answer));
        }
    }
    Ok(None)
}

pub(super) fn visit_component_representatives(
    component: &StagedSourceComponent,
    segments: &[Segment],
    mut visit: impl FnMut(XY),
) {
    let Some(pool) = segments.get(component.segments.clone()) else {
        return;
    };
    if component.ring.is_some() {
        if let Some(first) = pool.first() {
            visit(first.start);
        }
        return;
    }
    let mut first = None;
    let mut last = None;
    for segment in pool {
        if !same_point(segment.start, segment.end) {
            first.get_or_insert(segment.start);
            last = Some(segment.end);
        }
    }
    if let Some(first) = first {
        visit(first);
    }
    if let Some(last) = last
        && !first.is_some_and(|first| same_point(first, last))
    {
        visit(last);
    }
}
