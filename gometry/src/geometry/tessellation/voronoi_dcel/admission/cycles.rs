//! Exact contact and cycle validation for Voronoi clips.

#[cfg(test)]
use super::ValidatedExactCycles;
use super::{
    ExactCycleContact, ExactPairTouch, ExactSubdivision, ExactTouchForest, NodedExactCycle,
    NodedSegmentIndex, Ordering, OwnerCycleContainment, OwnerCycleFailure, OwnerShellPosition,
    Polygon, Result, VertexId, exact, polygon_is_valid, try_dedup_by, try_sort_by, voronoi_error,
};

pub(in crate::geometry::tessellation::voronoi_dcel) fn first_owner_cycle_failure(
    facts: &[OwnerCycleContainment],
    shell: usize,
) -> Option<(usize, OwnerCycleFailure)> {
    for (ring, fact) in facts.iter().enumerate() {
        if ring == shell {
            continue;
        }
        match fact.shell_position {
            OwnerShellPosition::Outside => {
                return Some((ring, OwnerCycleFailure::OutsideShell));
            },
            OwnerShellPosition::BoundaryOnly => {
                return Some((ring, OwnerCycleFailure::BoundaryOnly));
            },
            OwnerShellPosition::Inside => {},
        }
        if fact.nested_descendant {
            return Some((ring, OwnerCycleFailure::NestedHole));
        }
    }
    None
}

pub(in crate::geometry::tessellation::voronoi_dcel) fn validate_owner_cycle_facts(
    facts: &[OwnerCycleContainment],
    expected_cycles: usize,
    shell: usize,
) -> Result<()> {
    if facts.len() != expected_cycles || first_owner_cycle_failure(facts, shell).is_some() {
        return Err(exact_cycle_error());
    }
    Ok(())
}

pub(in crate::geometry::tessellation::voronoi_dcel) fn exact_cycle_error() -> crate::error::Error {
    voronoi_error("exact Voronoi cycle set is not a valid polygon boundary")
}

pub(in crate::geometry::tessellation::voronoi_dcel) fn admit_embedded_clip(
    polygon: &Polygon,
) -> Result<()> {
    if polygon_is_valid(polygon)? {
        Ok(())
    } else {
        Err(voronoi_error(
            "embedded Voronoi clip is not a valid polygon",
        ))
    }
}

#[expect(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "each corner index comes from the fixed four-entry range and each next-corner index wraps within that array"
)]
pub(in crate::geometry::tessellation::voronoi_dcel) fn validate_exact_rectangle(
    corners: &[exact::ExactPoint; 4],
) -> Result<()> {
    for left_edge in 0..4 {
        let left_next = (left_edge + 1) % 4;
        let left = [&corners[left_edge], &corners[left_next]];
        for right_edge in left_edge + 1..4 {
            let right_next = (right_edge + 1) % 4;
            let right = [&corners[right_edge], &corners[right_next]];
            let adjacent = right_edge == left_edge + 1 || (left_edge == 0 && right_edge == 3);
            match exact::segment_intersection(left[0], left[1], right[0], right[1])? {
                exact::SegmentIntersection::None if !adjacent => {},
                exact::SegmentIntersection::Point(point) if adjacent => {
                    if position_of(&left, &point)?.is_none()
                        || position_of(&right, &point)?.is_none()
                    {
                        return Err(exact_cycle_error());
                    }
                },
                exact::SegmentIntersection::None
                | exact::SegmentIntersection::Point(_)
                | exact::SegmentIntersection::Overlap { .. } => {
                    return Err(exact_cycle_error());
                },
            }
        }
    }
    Ok(())
}

#[expect(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "binary search maintains low <= middle < high <= points.len(), so both the midpoint read and lower-bound advance remain in this admitted sorted column"
)]
fn exact_point_index(points: &[exact::ExactPoint], point: &exact::ExactPoint) -> Result<usize> {
    let mut low = 0;
    let mut high = points.len();
    while low < high {
        let middle = low.midpoint(high);
        match points[middle].compare_lex(point)? {
            Ordering::Less => {
                low = middle + 1;
            },
            Ordering::Equal => return Ok(middle),
            Ordering::Greater => high = middle,
        }
    }
    Err(exact_cycle_error())
}

fn validate_touch_wedge<'a>(
    point: &exact::ExactPoint,
    first_directions: impl IntoIterator<Item = &'a exact::ExactPoint>,
    second_directions: impl IntoIterator<Item = &'a exact::ExactPoint>,
    rays: &mut Vec<(usize, &'a exact::ExactPoint)>,
) -> Result<()> {
    rays.clear();
    rays.extend(first_directions.into_iter().map(|target| (0, target)));
    rays.extend(second_directions.into_iter().map(|target| (1, target)));
    let angle = |left: &exact::ExactPoint, right: &exact::ExactPoint| {
        exact::angle_ccw_cmp(point, left, right).map_err(|_| exact_cycle_error())
    };
    try_sort_by(rays, |left, right| angle(left.1, right.1))?;
    for [left, right] in rays.array_windows::<2>() {
        angle(left.1, right.1)?;
    }
    if rays.len() > 1 {
        let (Some(first), Some(last)) = (rays.first(), rays.last()) else {
            return Err(exact_cycle_error());
        };
        angle(last.1, first.1)?;
    }
    let changes = rays
        .iter()
        .zip(rays.iter().cycle().skip(1))
        .filter(|(left, right)| left.0 != right.0)
        .count();
    if changes == 2 {
        Ok(())
    } else {
        Err(exact_cycle_error())
    }
}

pub(in crate::geometry::tessellation::voronoi_dcel::admission) fn validate_exact_contact_graph(
    cycles: &[&[exact::ExactPoint]],
    shell: usize,
    pair_touches: &[ExactPairTouch],
) -> Result<()> {
    if cycles.is_empty() || shell >= cycles.len() || cycles.iter().any(|cycle| cycle.len() < 3) {
        return Err(exact_cycle_error());
    }
    let mut rays = Vec::new();
    for touch in pair_touches {
        let [first_directions, second_directions] = &touch.directions;
        let ray_capacity = first_directions
            .len()
            .checked_add(second_directions.len())
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        if rays.capacity() < ray_capacity {
            rays.clear();
            rays.reserve(ray_capacity);
        }
        validate_touch_wedge(
            &touch.point,
            first_directions.iter(),
            second_directions.iter(),
            &mut rays,
        )?;
    }
    let mut touch_points = Vec::with_capacity(pair_touches.len());
    touch_points.extend(pair_touches.iter().map(|touch| touch.point.clone()));
    try_sort_by(&mut touch_points, exact::ExactPoint::compare_lex)?;
    try_dedup_by(&mut touch_points, exact::ExactPoint::same_position)?;
    let mut forest = ExactTouchForest::new(cycles.len(), touch_points.len())?;
    let mut incidences = Vec::new();
    for touch in pair_touches {
        let point = exact_point_index(&touch_points, &touch.point)?;
        for ring in touch.rings {
            incidences.push((point, ring));
        }
    }
    incidences.sort_unstable();
    incidences.dedup();
    for (point, ring) in incidences {
        forest.attach(point, ring)?;
    }
    Ok(())
}

pub(in crate::geometry::tessellation::voronoi_dcel) fn for_each_touch_fan(
    index: &NodedSegmentIndex,
    mut visit: impl FnMut(usize, VertexId, usize, usize, usize) -> Result<()>,
) -> Result<usize> {
    let mut cursor = 0_usize;
    let mut touch_index = 0_usize;
    while cursor < index.incidences.len() {
        let vertex = index
            .incidences
            .get(cursor)
            .ok_or_else(exact_cycle_error)?
            .vertex;
        let start = cursor;
        let mut end = start.wrapping_add(1);
        while end < index.incidences.len()
            && index
                .incidences
                .get(end)
                .ok_or_else(exact_cycle_error)?
                .vertex
                == vertex
        {
            end = end.wrapping_add(1);
        }

        let mut ring_count = 0_usize;
        let mut ring_start = start;
        while ring_start < end {
            let ring = index
                .incidences
                .get(ring_start)
                .ok_or_else(exact_cycle_error)?
                .ring;
            let mut ring_end = ring_start.wrapping_add(1);
            while ring_end < end
                && index
                    .incidences
                    .get(ring_end)
                    .ok_or_else(exact_cycle_error)?
                    .ring
                    == ring
            {
                ring_end = ring_end.wrapping_add(1);
            }
            if ring_end.wrapping_sub(ring_start) != 2 {
                return Err(exact_cycle_error());
            }
            ring_count = ring_count
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            ring_start = ring_end;
        }
        if ring_count > 1 {
            visit(touch_index, vertex, start, end, ring_count)?;
            touch_index = touch_index
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
        }
        cursor = end;
    }
    Ok(touch_index)
}

fn validate_assembled_touch_fans(
    subdivision: &ExactSubdivision,
    cycle_count: usize,
    index: &NodedSegmentIndex,
) -> Result<()> {
    let touch_count = for_each_touch_fan(index, |_, _, _, _, _| Ok(()))?;
    let mut rays = Vec::with_capacity(4);

    for_each_touch_fan(index, |_, vertex, start, end, ring_count| {
        let touch = &subdivision.vertex(vertex).point;
        let mut left = start;
        for left_group in 0..ring_count {
            let left_first = index.incidences.get(left).ok_or_else(exact_cycle_error)?;
            let left_second = index
                .incidences
                .get(left.wrapping_add(1))
                .ok_or_else(exact_cycle_error)?;
            let first_directions = [
                &subdivision.vertex(left_first.neighbor).point,
                &subdivision.vertex(left_second.neighbor).point,
            ];
            let mut right = left.wrapping_add(2);
            for _ in left_group.wrapping_add(1)..ring_count {
                let right_first = index.incidences.get(right).ok_or_else(exact_cycle_error)?;
                let right_second = index
                    .incidences
                    .get(right.wrapping_add(1))
                    .ok_or_else(exact_cycle_error)?;
                let second_directions = [
                    &subdivision.vertex(right_first.neighbor).point,
                    &subdivision.vertex(right_second.neighbor).point,
                ];
                validate_touch_wedge(touch, first_directions, second_directions, &mut rays)?;
                right = right.wrapping_add(2);
            }
            left = left.wrapping_add(2);
        }
        if left != end {
            return Err(exact_cycle_error());
        }
        Ok(())
    })?;

    let mut forest = ExactTouchForest::new(cycle_count, touch_count)?;
    for_each_touch_fan(index, |touch_index, _, start, end, _| {
        let mut cursor = start;
        while cursor < end {
            let ring = index
                .incidences
                .get(cursor)
                .ok_or_else(exact_cycle_error)?
                .ring;
            forest.attach(touch_index, ring)?;
            cursor = cursor.wrapping_add(2);
        }
        Ok(())
    })?;
    Ok(())
}

#[expect(
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects,
    reason = "segment ordinals are admitted by offsets and each cycle's closed next-edge index wraps within its nonzero point slice"
)]
fn classify_exact_segment_pair(
    cycles: &[&[exact::ExactPoint]],
    offsets: &[usize],
    [left_segment, right_segment]: [usize; 2],
    noded_endpoint_pair: bool,
) -> Result<Option<ExactCycleContact>> {
    let left_ring = offsets.partition_point(|offset| *offset <= left_segment) - 1;
    let right_ring = offsets.partition_point(|offset| *offset <= right_segment) - 1;
    let left_edge = left_segment - offsets[left_ring];
    let right_edge = right_segment - offsets[right_ring];
    let left_cycle = cycles[left_ring];
    let left_next = left_edge
        .checked_add(1)
        .filter(|&next| next < left_cycle.len())
        .unwrap_or(0);
    let left = [&left_cycle[left_edge], &left_cycle[left_next]];
    let right_cycle = cycles[right_ring];
    let right_next = right_edge
        .checked_add(1)
        .filter(|&next| next < right_cycle.len())
        .unwrap_or(0);
    let right = [&right_cycle[right_edge], &right_cycle[right_next]];
    let adjacent = left_ring == right_ring
        && (right_edge == left_edge + 1 || (left_edge == 0 && right_edge + 1 == left_cycle.len()));
    match exact::segment_intersection(left[0], left[1], right[0], right[1])? {
        exact::SegmentIntersection::None if noded_endpoint_pair => Err(exact_cycle_error()),
        exact::SegmentIntersection::None => Ok(None),
        exact::SegmentIntersection::Overlap { .. } => Err(exact_cycle_error()),
        exact::SegmentIntersection::Point(point) => {
            let endpoints = [position_of(&left, &point)?, position_of(&right, &point)?];
            if adjacent {
                if endpoints[0].is_none() || endpoints[1].is_none() {
                    return Err(exact_cycle_error());
                }
                return Ok(None);
            }
            if left_ring == right_ring
                || (endpoints[0].is_none() && endpoints[1].is_none())
                || (noded_endpoint_pair && (endpoints[0].is_none() || endpoints[1].is_none()))
            {
                return Err(exact_cycle_error());
            }
            Ok(Some(ExactCycleContact {
                segments: [left_segment, right_segment],
                point,
            }))
        },
    }
}

pub(in crate::geometry::tessellation::voronoi_dcel) fn for_each_assembled_pair(
    cycles: &[&[exact::ExactPoint]],
    index: &NodedSegmentIndex,
    mut visit: impl FnMut([usize; 2], Option<ExactCycleContact>) -> Result<()>,
) -> Result<()> {
    index.for_each_source_pair(|left_segment, right_segment| {
        let contact = classify_exact_segment_pair(
            cycles,
            &index.offsets,
            [left_segment, right_segment],
            true,
        )?;
        visit([left_segment, right_segment], contact)
    })
}

/// The nesting step runs only after noded contact and touch validation succeeds.
pub(in crate::geometry::tessellation::voronoi_dcel) fn validate_assembled_exact_cycles(
    subdivision: &ExactSubdivision,
    cycles: &[NodedExactCycle<'_>],
    shell: usize,
    validate_nesting: impl FnOnce() -> Result<()>,
) -> Result<()> {
    let mut points = Vec::with_capacity(cycles.len());
    points.extend(cycles.iter().map(|cycle| cycle.points));
    validate_exact_cycle_inputs(&points, shell)?;
    validate_exact_cycle_rounding(&points)?;
    let index = NodedSegmentIndex::new(cycles)?;
    for_each_assembled_pair(&points, &index, |_, _| Ok(()))?;
    validate_assembled_touch_fans(subdivision, cycles.len(), &index)?;
    validate_nesting()
}

#[cfg(test)]
pub(in crate::geometry::tessellation::voronoi_dcel) fn validate_exact_cycles(
    cycles: &[&[exact::ExactPoint]],
    shell: usize,
) -> Result<ValidatedExactCycles> {
    validate_exact_cycle_inputs(cycles, shell)?;
    let offsets = exact_cycle_offsets(cycles)?;
    validate_exact_cycle_rounding(cycles)?;

    let segment_count = *offsets
        .last()
        .expect("cycle offsets contain their terminal value");
    let mut contacts = Vec::new();
    let mut pair_touches = Vec::new();
    for left_segment in 0..segment_count {
        let right_start = left_segment.wrapping_add(1);
        for right_segment in right_start..segment_count {
            let Some(contact) = classify_exact_segment_pair(
                cycles,
                &offsets,
                [left_segment, right_segment],
                false,
            )?
            else {
                continue;
            };
            record_generic_exact_touch(cycles, &offsets, &contact, &mut pair_touches)?;
            contacts.push(contact);
        }
    }
    validate_exact_contact_graph(cycles, shell, &pair_touches)?;
    validate_exact_cycle_nesting(cycles, shell)?;
    try_sort_by(&mut contacts, |left, right| {
        Ok(match left.segments.cmp(&right.segments) {
            Ordering::Equal => left.point.compare_lex(&right.point)?,
            Ordering::Less => Ordering::Less,
            Ordering::Greater => Ordering::Greater,
        })
    })?;
    try_dedup_by(&mut contacts, |later, earlier| {
        Ok(later.segments == earlier.segments && later.point.same_position(&earlier.point)?)
    })?;
    Ok(ValidatedExactCycles { contacts })
}

fn validate_exact_cycle_inputs(cycles: &[&[exact::ExactPoint]], shell: usize) -> Result<()> {
    if cycles.is_empty() || shell >= cycles.len() || cycles.iter().any(|cycle| cycle.len() < 3) {
        return Err(exact_cycle_error());
    }

    Ok(())
}

#[cfg(test)]
fn exact_cycle_offsets(cycles: &[&[exact::ExactPoint]]) -> Result<Vec<usize>> {
    let offset_capacity = cycles
        .len()
        .checked_add(1)
        .ok_or(crate::error::ResourceError::SizeOverflow)?;
    let mut offsets = Vec::with_capacity(offset_capacity);
    offsets.push(0_usize);
    for cycle in cycles {
        let offset = offsets
            .last()
            .expect("cycle offsets contain their initial value")
            .checked_add(cycle.len())
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        offsets.push(offset);
    }
    Ok(offsets)
}

fn validate_exact_cycle_rounding(cycles: &[&[exact::ExactPoint]]) -> Result<()> {
    for cycle in cycles {
        for point in *cycle {
            let _ = point.finite_rounding()?;
        }
    }
    Ok(())
}

#[cfg(test)]
pub(in crate::geometry::tessellation::voronoi_dcel) fn validate_exact_cycle_nesting(
    cycles: &[&[exact::ExactPoint]],
    shell: usize,
) -> Result<()> {
    let shell_cycle = cycles.get(shell).copied().ok_or_else(exact_cycle_error)?;
    for (hole, hole_cycle) in cycles.iter().enumerate() {
        if hole == shell {
            continue;
        }
        let mut saw_inside = false;
        for point in *hole_cycle {
            match exact::point_in_cycle(shell_cycle, point)? {
                exact::PointInCycle::Outside => return Err(exact_cycle_error()),
                exact::PointInCycle::Inside => saw_inside = true,
                exact::PointInCycle::Boundary => {},
            }
        }
        if !saw_inside {
            return Err(exact_cycle_error());
        }
        for (other, other_cycle) in cycles.iter().enumerate() {
            if other == shell || other == hole {
                continue;
            }
            for point in *other_cycle {
                if exact::point_in_cycle(hole_cycle, point)? == exact::PointInCycle::Inside {
                    return Err(exact_cycle_error());
                }
            }
        }
    }
    Ok(())
}

/// The index of the first point at `point`'s exact position.
fn position_of<P: std::borrow::Borrow<exact::ExactPoint>>(
    points: &[P],
    point: &exact::ExactPoint,
) -> Result<Option<usize>> {
    for (index, candidate) in points.iter().enumerate() {
        if candidate.borrow().same_position(point)? {
            return Ok(Some(index));
        }
    }
    Ok(None)
}

#[cfg(test)]
fn record_generic_exact_touch(
    cycles: &[&[exact::ExactPoint]],
    offsets: &[usize],
    contact: &ExactCycleContact,
    pair_touches: &mut Vec<ExactPairTouch>,
) -> Result<()> {
    #[expect(
        clippy::arithmetic_side_effects,
        reason = "contact segment ordinals came from the exhaustive pair loop and map through the admitted cycle offsets"
    )]
    fn source_edge<'a>(
        cycles: &'a [&'a [exact::ExactPoint]],
        offsets: &[usize],
        segment: usize,
    ) -> (usize, usize, [&'a exact::ExactPoint; 2]) {
        let ring = offsets.partition_point(|offset| *offset <= segment) - 1;
        let edge = segment - offsets[ring];
        let cycle = cycles[ring];
        let next = edge
            .checked_add(1)
            .filter(|&next| next < cycle.len())
            .unwrap_or(0);
        (ring, edge, [&cycle[edge], &cycle[next]])
    }

    let [left_segment, right_segment] = contact.segments;
    let (left_ring, _, left) = source_edge(cycles, offsets, left_segment);
    let (right_ring, _, right) = source_edge(cycles, offsets, right_segment);
    let endpoints = [
        position_of(&left, &contact.point)?,
        position_of(&right, &contact.point)?,
    ];
    let rings = [left_ring, right_ring];
    let mut known_touch = None;
    for (index, touch) in pair_touches.iter().enumerate() {
        if touch.rings == rings && touch.point.same_position(&contact.point)? {
            known_touch = Some(index);
            break;
        }
    }
    if let Some(index) = known_touch {
        let touch = pair_touches.get_mut(index).ok_or_else(exact_cycle_error)?;
        for (directions, (segment, endpoint)) in touch
            .directions
            .iter_mut()
            .zip([(left, endpoints[0]), (right, endpoints[1])])
        {
            for (index, direction) in segment.into_iter().enumerate() {
                if endpoint == Some(index) || position_of(directions, direction)?.is_some() {
                    continue;
                }
                directions.push(direction.clone());
            }
        }
    } else {
        let mut directions = [Vec::new(), Vec::new()];
        for (ring_directions, (segment, endpoint)) in directions
            .iter_mut()
            .zip([(left, endpoints[0]), (right, endpoints[1])])
        {
            for (index, direction) in segment.into_iter().enumerate() {
                if endpoint.is_none_or(|endpoint| index != endpoint) {
                    ring_directions.push(direction.clone());
                }
            }
        }
        pair_touches.push(ExactPairTouch {
            rings,
            point: contact.point.clone(),
            directions,
        });
    }
    Ok(())
}
