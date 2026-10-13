use super::{Axis, ExactPartition, ExactSign, NodeId, Ordering, Result};
#[cfg(test)]
use super::{
    ControlFlow, ExactSourceInventory, ExpansionBudget, GeometryErrorKind, HashMap, PointKey,
    ResourceError, Segment, emitted_pair_preserves_incidence, for_each_candidate_pair,
    single_chain,
};

/// Certify a rounded embedding whose endpoint identities were decided by a
/// different exact construction (currently Voronoi). ExactPartition owns the
/// final noding/incidence pass; the external owner retains cocircular centers,
/// clipping policy, and fan order.
#[cfg(test)]
pub(in crate::geometry) fn certify_emitted_incidence(items: &[([u32; 2], Segment)]) -> Result<()> {
    let mut coordinates: HashMap<u32, PointKey> =
        HashMap::with_hasher(crate::collections::collection_hash_state()?.clone());
    let mut by_coordinate: HashMap<PointKey, u32> =
        HashMap::with_hasher(crate::collections::collection_hash_state()?.clone());
    let endpoint_capacity = items
        .len()
        .checked_mul(2)
        .ok_or(ResourceError::SizeOverflow)?;
    coordinates.reserve(endpoint_capacity);
    by_coordinate.reserve(endpoint_capacity);
    let mut segments = Vec::with_capacity(items.len());
    for &([start_id, end_id], segment) in items {
        for (id, point) in [(start_id, segment.start), (end_id, segment.end)] {
            let key = PointKey::new(point);
            if let Some(previous) = coordinates.insert(id, key)
                && previous != key
            {
                return Err(GeometryErrorKind::ExactNodeCollision {
                    first: id,
                    second: id,
                }
                .into());
            }
            if let Some(previous) = by_coordinate.insert(key, id)
                && previous != id
            {
                return Err(GeometryErrorKind::ExactNodeCollision {
                    first: previous,
                    second: id,
                }
                .into());
            }
        }
        segments.push(segment);
    }
    let inventory = ExactSourceInventory::from_segments(&segments)?;
    let mut budget = ExpansionBudget::new("exact_partition", "Voronoi incidence");
    let partition = inventory.partition(&mut budget)?;
    drop(partition.local_topology()?);
    if let ControlFlow::Break((first, second)) =
        for_each_candidate_pair(&segments, single_chain, |left, right| {
            let endpoints = |index: usize| {
                let ([start, end], segment) = *items
                    .get(index)
                    .expect("candidate-pair index belongs to the source item table");
                [(NodeId(start), segment.start), (NodeId(end), segment.end)]
            };
            if emitted_pair_preserves_incidence(endpoints(left), endpoints(right)) {
                ControlFlow::Continue(())
            } else {
                ControlFlow::Break((left, right))
            }
        })?
    {
        return Err(GeometryErrorKind::ExactUnexpectedContact { first, second }.into());
    }
    Ok(())
}

pub(super) fn cycle_contains_node(
    partition: &ExactPartition<'_>,
    cycle: &[NodeId],
    query: NodeId,
) -> Result<bool> {
    let mut inside = false;
    for (&a, &b) in cycle
        .iter()
        .zip(cycle.iter().skip(1).chain(cycle.iter().take(1)))
    {
        let y_a = partition.compare_axis(a, query, Axis::Y)?;
        let y_b = partition.compare_axis(b, query, Axis::Y)?;
        let a_is_below = y_a != Ordering::Greater;
        let b_is_below = y_b != Ordering::Greater;
        // The orientation decides only an edge that straddles the ray or
        // whose extent could hold the query.
        let may_hold = y_a != Ordering::Greater
            && y_b != Ordering::Less
            && partition.compare_axis(query, a, Axis::X)? != Ordering::Less
            && partition.compare_axis(query, b, Axis::X)? != Ordering::Greater;
        if a_is_below == b_is_below && !may_hold {
            continue;
        }
        let orientation = partition.orientation(a, b, query)?;
        if may_hold && orientation == ExactSign::Zero {
            return Ok(false);
        }
        if a_is_below == b_is_below {
            continue;
        }
        let crosses_left = (a_is_below && orientation == ExactSign::Negative)
            || (b_is_below && orientation == ExactSign::Positive);
        if crosses_left {
            inside = !inside;
        }
    }
    Ok(inside)
}
