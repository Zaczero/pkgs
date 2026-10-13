use super::atoms::{
    build_atom_coverage, cross_event_nodes, intern_event_points, order_event_cuts, same_atom_point,
};
use super::points::settle_all;
use super::sides::{AtomSides, RunCache, atom_owner_label, atom_sides, run_label};
use super::witnesses::event_node_witnesses;
use super::*;

pub(super) fn exhaustive_pass<Q: Query, const SATURATE: bool>(
    interval: &mut De9imInterval,
    operands: &[Operand<'_>; 2],
    query: &Q,
    staged: &StagedGeometry,
    events: &mut [ContactEvent],
    forest: &mut ExactPointForest<'_>,
    coordinates: &mut ExactCoordinates,
) -> Result<Option<Q::Output>> {
    let StagedGeometry {
        sources,
        components,
        segment_component,
        operand_has_area,
        ..
    } = staged;
    let mut stored_points = HashMap::with_hasher(collection_hash_state()?.clone());
    let segments = sources.segments();
    let mut segment_cuts =
        intern_event_points(events, segments, forest, &mut stored_points, coordinates)?;
    let (boundaries, cut_positions) =
        order_event_cuts(segments, &mut segment_cuts, forest, coordinates)?;
    let mut coverage = build_atom_coverage(
        events,
        segments,
        components,
        &boundaries,
        &cut_positions,
        forest,
        coordinates,
    )?;
    let (cross_stored, cross_constructed) = cross_event_nodes(events, components, forest)?;
    if let Some(answer) =
        event_node_witnesses(interval, operands, query, events, components, forest)?
    {
        return Ok(Some(answer));
    }

    let mut run_cache = None;
    for segment_index in 0..segments.len() {
        if SATURATE && interval.settled_for(query.cells()) {
            return Ok(query.verdict(interval));
        }
        let Some(&segment) = segments.get(segment_index) else {
            return Err(
                GeometryErrorKind::Invalid("segment loop exceeded the staged pool".into()).into(),
            );
        };
        if same_point(segment.start, segment.end) {
            continue;
        }
        let component_id = *segment_component
            .get(segment_index)
            .ok_or_else(|| GeometryErrorKind::Invalid("segment has no staged component".into()))?;
        let component_index =
            usize::try_from(component_id).map_err(|_| ResourceError::SizeOverflow)?;
        let component = components.get(component_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("segment component ID is out of range".into())
        })?;
        let side = component.side;
        let target_side = opposite_operand(side);
        let cuts = boundaries.get(segment_index).ok_or_else(|| {
            GeometryErrorKind::Invalid("segment has no ordered boundaries".into())
        })?;
        let atom_cover = coverage
            .get_mut(segment_index)
            .ok_or_else(|| GeometryErrorKind::Invalid("segment has no atom-coverage row".into()))?;
        let direction = OnceCell::new();
        for atom_index in 0..cuts.len().saturating_sub(1) {
            if SATURATE && interval.settled_for(query.cells()) {
                return Ok(query.verdict(interval));
            }
            let start = *cuts
                .get(atom_index)
                .ok_or_else(|| GeometryErrorKind::Invalid("atom start cut is missing".into()))?;
            let end = *cuts
                .get(atom_index.wrapping_add(1))
                .ok_or_else(|| GeometryErrorKind::Invalid("atom end cut is missing".into()))?;
            if same_atom_point(start, end, forest, coordinates)? {
                continue;
            }
            let atom = ExactAtom {
                start,
                end,
                start_exact: OnceCell::new(),
                midpoint: OnceCell::new(),
                segment,
                direction: &direction,
            };
            let own = atom_owner_label(
                by_operand(operands, side),
                component,
                &atom,
                coordinates,
                forest,
            )?;
            let cover = atom_cover.get(atom_index).copied().flatten();
            let (foreign, foreign_sides) = if let Some(cover) = cover {
                let partner_index = usize::try_from(cover.partner_component)
                    .map_err(|_| ResourceError::SizeOverflow)?;
                let partner = components.get(partner_index).ok_or_else(|| {
                    GeometryErrorKind::Invalid("coverage partner component is out of range".into())
                })?;
                let foreign = atom_owner_label(
                    by_operand(operands, target_side),
                    partner,
                    &atom,
                    coordinates,
                    forest,
                )?;
                let mut sides = atom_sides(
                    by_operand(operands, target_side),
                    partner,
                    *by_operand(operand_has_area, target_side),
                    &atom,
                    coordinates,
                    forest,
                )?;
                // Dirty components were probed in this segment's direction;
                // cached clean-ring sides still follow their own carrier.
                if !partner.dirty && !cover.same_direction {
                    sides = sides.swap();
                }
                (foreign, sides)
            } else {
                let foreign = if *by_operand(operand_has_area, target_side) {
                    run_label(
                        by_operand(operands, target_side),
                        component_id,
                        &atom,
                        coordinates,
                        run_cache.as_ref(),
                        forest,
                        &cross_stored,
                        &cross_constructed,
                    )?
                } else {
                    run_cache = None;
                    Loc::Exterior
                };
                let occupied = foreign == Loc::Interior;
                (foreign, AtomSides::new(occupied, occupied))
            };
            let own_sides = atom_sides(
                by_operand(operands, side),
                component,
                *by_operand(operand_has_area, side),
                &atom,
                coordinates,
                forest,
            )?;

            let (left_label, right_label) = match side {
                matrix_predicates::Operand::Left => (own, foreign),
                matrix_predicates::Operand::Right => (foreign, own),
            };
            if left_label != Loc::Exterior || right_label != Loc::Exterior {
                interval.witness(left_label, right_label, Dimension::Curve);
            }
            let side_pairs = match side {
                matrix_predicates::Operand::Left => [
                    (own_sides.left, foreign_sides.left),
                    (own_sides.right, foreign_sides.right),
                ],
                matrix_predicates::Operand::Right => [
                    (foreign_sides.left, own_sides.left),
                    (foreign_sides.right, own_sides.right),
                ],
            };
            for (left_occupied, right_occupied) in side_pairs {
                interval.witness(
                    if left_occupied {
                        Loc::Interior
                    } else {
                        Loc::Exterior
                    },
                    if right_occupied {
                        Loc::Interior
                    } else {
                        Loc::Exterior
                    },
                    Dimension::Surface,
                );
            }
            if cover.is_some() {
                run_cache = None;
            } else if *by_operand(operand_has_area, target_side) {
                run_cache = Some(RunCache {
                    component: component_id,
                    end,
                    label: foreign,
                });
            }
            if let Some(answer) = query.verdict(interval) {
                return Ok(Some(answer));
            }
        }
    }
    settle_all(interval);
    Ok(query.verdict(interval))
}
