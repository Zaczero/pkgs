use super::ChainOrigins;
use crate::geometry::{CoordSeq, CoordSeqBuilder, HashMap, PointKey};

fn extend_chain(
    edge: &CoordSeq,
    from: PointKey,
    skip: usize,
    edge_index: usize,
    chain_index: usize,
    out: &mut CoordSeqBuilder,
    record_origin: &mut impl FnMut(usize, usize, usize),
) {
    if PointKey::new(edge.first().expect("edge has >= 2 vertices")) == from {
        out.extend_window(edge, skip..edge.len());
        for vertex in skip..edge.len() {
            record_origin(chain_index, edge_index, vertex);
        }
    } else {
        for (offset, point) in edge.iter().rev().enumerate().skip(skip) {
            out.push(point);
            record_origin(
                chain_index,
                edge_index,
                edge.len().wrapping_sub(offset.wrapping_add(1)),
            );
        }
    }
}

/// Dissolve linework edges into maximal chains, split only at genuine
/// junctions (nodes of degree other than two). Every edge is consumed once;
/// output order follows the lowest unused seed, so it is independent of
/// hash iteration. Node incidence is a CSR over the distinct end keys: one
/// map lookup per edge end and no per-node allocation, which is what an
/// overlay of thousands of atomic segments spends its time on otherwise.
pub(crate) fn merge_chains<E: AsRef<CoordSeq>>(edges: &[E]) -> crate::error::Result<Vec<CoordSeq>> {
    merge_chains_impl(edges, false, &mut |_, _, _| {})
}

/// Merge chains while retaining the input vertex occurrence that emitted each
/// output coordinate. The occurrence is needed when source provenance, rather
/// than coordinate equality, owns additional ordinate candidates.
pub(crate) fn merge_chains_with_origins<E: AsRef<CoordSeq>>(
    edges: &[E],
) -> crate::error::Result<(Vec<CoordSeq>, ChainOrigins)> {
    let mut origins = Vec::with_capacity(edges.len());
    let chains = merge_chains_impl(edges, false, &mut |chain, edge, vertex| {
        while origins.len() <= chain {
            origins.push(Vec::new());
        }
        origins
            .get_mut(chain)
            .expect("origin chain row is initialized before appending")
            .push((edge, vertex));
    })?;
    Ok((chains, origins))
}

pub(crate) fn merge_line_chains<E: AsRef<CoordSeq>>(
    edges: &[E],
    directed: bool,
) -> crate::error::Result<Vec<CoordSeq>> {
    merge_chains_impl(edges, directed, &mut |_, _, _| {})
}

#[expect(
    clippy::indexing_slicing,
    reason = "dense endpoint IDs, aligned live/node columns, checked CSR totals, and CSR-row traversal bound every lookup"
)]
fn merge_chains_impl<E: AsRef<CoordSeq>>(
    edges: &[E],
    directed: bool,
    record_origin: &mut impl FnMut(usize, usize, usize),
) -> crate::error::Result<Vec<CoordSeq>> {
    let mut ends: Vec<[PointKey; 2]> = Vec::with_capacity(edges.len());
    let mut live: Vec<usize> = Vec::with_capacity(edges.len());
    for (index, edge) in edges.iter().enumerate() {
        let edge = edge.as_ref();
        let mut points = edge.iter();
        let Some(first) = points.next().map(PointKey::new) else {
            continue;
        };
        if (directed && edge.len() >= 2) || points.any(|point| PointKey::new(point) != first) {
            ends.push([
                first,
                PointKey::new(edge.last().expect("edge has >= 2 vertices")),
            ]);
            live.push(index);
        }
    }
    if live.len() <= 1 {
        let mut result = Vec::with_capacity(live.len());
        for (chain_index, (index, [start, _])) in live.into_iter().zip(ends).enumerate() {
            let edge = edges
                .get(index)
                .expect("live edge ordinal comes from the input edges")
                .as_ref();
            let mut coords = CoordSeqBuilder::with_capacity(edge.axes(), edge.len());
            extend_chain(
                edge,
                start,
                0,
                index,
                chain_index,
                &mut coords,
                record_origin,
            );
            result.push(coords.finish()?);
        }
        return Ok(result);
    }
    let mut node_of: HashMap<PointKey, u32> =
        HashMap::with_hasher(crate::collections::collection_hash_state()?.clone());
    node_of.reserve(ends.len());
    let mut degree: Vec<u32> = Vec::new();
    let node_capacity = ends.len().wrapping_mul(2);
    degree.reserve(node_capacity);
    let mut node_ends: Vec<[u32; 2]> = Vec::with_capacity(ends.len());
    for pair in &ends {
        let mut ids = [0_u32; 2];
        for (key, id_slot) in pair.iter().zip(ids.iter_mut()) {
            let next = u32::try_from(degree.len())
                .map_err(|_| crate::error::ResourceError::SizeOverflow)?;
            let id = *node_of.entry(*key).or_insert(next);
            if id == next {
                degree.push(0);
            }
            let count = degree
                .get_mut(id as usize)
                .expect("interned endpoint has a degree entry");
            *count = count
                .checked_add(1)
                .ok_or(crate::error::ResourceError::SizeOverflow)?;
            *id_slot = id;
        }
        node_ends.push(ids);
    }
    drop(node_of);
    let offset_capacity = degree.len().wrapping_add(1);
    let mut offsets: Vec<u32> = Vec::with_capacity(offset_capacity);
    offsets.push(0);
    for &count in &degree {
        let next = offsets
            .last()
            .expect("CSR offsets start with their zero origin")
            .checked_add(count)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        offsets.push(next);
    }
    let mut cursor = Vec::with_capacity(offsets.len());
    cursor.extend_from_slice(&offsets);
    let slot_count = *offsets.last().expect("CSR offsets have an initial zero") as usize;
    let mut slots = vec![0_u32; slot_count];
    for (edge, ids) in node_ends.iter().enumerate() {
        for &id in ids {
            let position = cursor
                .get_mut(id as usize)
                .expect("each endpoint has a CSR cursor");
            slots[*position as usize] = edge as u32;
            *position = position.wrapping_add(1);
        }
    }
    drop(cursor);
    let incident = |node: u32| {
        let node_index = node as usize;
        let [start, end] = offsets[node_index..node_index.wrapping_add(2)]
            .first_chunk::<2>()
            .expect("each dense endpoint has adjacent CSR offsets");
        &slots[*start as usize..*end as usize]
    };

    let mut used = vec![false; ends.len()];
    let mut chains: Vec<CoordSeq> = Vec::with_capacity(ends.len());
    let mut walk = |seed: usize,
                    mut from: u32,
                    chain_index: usize,
                    used: &mut [bool]|
     -> crate::error::Result<CoordSeq> {
        let input_index = *live
            .get(seed)
            .expect("chain seed belongs to the live edges");
        let first = edges
            .get(input_index)
            .expect("live edge ordinal comes from the input edges")
            .as_ref();
        let mut coords = CoordSeqBuilder::with_capacity(first.axes(), first.len());
        let mut edge = seed;
        let mut previous_end = None;
        loop {
            used[edge] = true;
            let [a, b] = node_ends[edge];
            let [start_key, end_key] = ends[edge];
            let from_key = if a == from { start_key } else { end_key };
            let source = edges[live[edge]].as_ref();
            let skip = usize::from(
                coords.len() != 0
                    && (!directed
                        || previous_end.is_some_and(|previous| {
                            crate::geometry::same_active_position(
                                previous,
                                source.first().expect("live edge has at least two vertices"),
                            )
                        })),
            );
            extend_chain(
                source,
                from_key,
                skip,
                live[edge],
                chain_index,
                &mut coords,
                record_origin,
            );
            if directed {
                previous_end = source.last();
            }
            let far = if a == from { b } else { a };
            if degree[far as usize] != 2 {
                break;
            }
            match incident(far).iter().copied().find(|&next| {
                next as usize != edge
                    && !used[next as usize]
                    && (!directed || node_ends[next as usize][0] == far)
            }) {
                Some(next) => {
                    edge = next as usize;
                    from = far;
                },
                None => break,
            }
        }
        coords.finish()
    };
    // Open chains first: every edge with a barrier end (degree != 2) starts
    // a chain that runs until the next barrier; edges interior to such a
    // chain are consumed mid-walk and skipped when their own seed turn comes.
    for (seed, [a, b]) in node_ends.iter().copied().enumerate() {
        if used[seed] {
            continue;
        }
        let start = if directed {
            (degree[a as usize] != 2
                || !incident(a)
                    .iter()
                    .any(|&edge| edge as usize != seed && node_ends[edge as usize][1] == a))
            .then_some(a)
        } else {
            [a, b].into_iter().find(|&node| degree[node as usize] != 2)
        };
        if let Some(from) = start {
            let chain_index = chains.len();
            chains.push(walk(seed, from, chain_index, &mut used)?);
        }
    }
    // Whatever remains is an all-degree-2 cycle; walk each from its lowest
    // unused edge.
    for (seed, [from, _]) in node_ends.iter().copied().enumerate() {
        if !used[seed] {
            let chain_index = chains.len();
            chains.push(walk(seed, from, chain_index, &mut used)?);
        }
    }
    Ok(chains)
}

#[cfg(test)]
mod extracted_tests;
