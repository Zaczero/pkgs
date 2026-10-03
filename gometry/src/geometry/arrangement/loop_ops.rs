use crate::collections::{HashSet, collection_hash_state};
use crate::error::ResourceError;
use crate::geometry::arrangement::{WindingWeight, compare_departures};
use crate::geometry::overlay::NodingScan;
use crate::geometry::{PointKey, Segment, XY, overlay, same_point, segments};

#[derive(Clone, Copy)]
pub(crate) struct LoopCut {
    pub(crate) ordinal: u32,
    pub(crate) point: XY,
    pub(crate) group: u32,
}

/// Find noding events and crossing groups for multiple positional loops.
/// Bit-level chain closure is checked per loop range, and duplicate starts
/// reject cross-loop vertex coincidence before noding.
pub(crate) fn positional_loop_cuts(
    segments: &[Segment],
    loop_ranges: &[(u32, u32)],
) -> crate::error::Result<Option<(Vec<LoopCut>, u32)>> {
    let n = segments.len();
    if n == 0 || loop_ranges.is_empty() {
        return Ok(None);
    }
    // Bitwise chain closure per loop: positional vertex identity requires
    // every end to be its successor's start AT THE BIT LEVEL — the same
    // identity the general dedup map keys on.
    for &(start, end) in loop_ranges {
        if end <= start {
            return Ok(None);
        }
        if (start..end).any(|index| {
            let next = index.wrapping_add(1);
            let next = if next < end { next } else { start };
            PointKey::new(segments[index as usize].end)
                != PointKey::new(segments[next as usize].start)
        }) {
            return Ok(None);
        }
    }
    // Key-coincident vertices across loops (exact duplicate multiparts,
    // shared endpoints, T-junctions) make positional identity impossible —
    // bail BEFORE the O(E²) noding pass. Previously this check ran after
    // full_noding_events, so stacked multiparts paid a full quadratic node
    // only to fall back to the general path that dedups first.
    {
        let mut seen = HashSet::with_hasher(collection_hash_state()?.clone());
        seen.reserve(n);
        if !segments
            .iter()
            .all(|segment| seen.insert(PointKey::new(segment.start)))
        {
            return Ok(None);
        }
    }
    let NodingScan {
        mut events,
        overlaps_found,
    } = overlay::full_noding_events(segments)?;
    if overlaps_found {
        // Collinear overlaps take the exactness pre-pass — general path.
        return Ok(None);
    }
    // Replay split_by_events' ordering exactly: the same unstable sort on
    // the same event list yields the same within-ordinal permutation the
    // general path's stable directed-coordinate sort starts from.
    events.sort_unstable_by_key(|&(ordinal, _)| ordinal);
    let n_events = events.len();
    // Group the cut events by exact point: one transversal crossing is the
    // SAME bit pattern visited from exactly two distinct ordinals
    // (the crossing is the rounding of one exact point). Anything else —
    // concurrent crossings, ulp-twin placements, one-sided events left by
    // endpoint-filtered T-junctions — bails.
    let event_count = u32::try_from(n_events).map_err(|_| ResourceError::SizeOverflow)?;
    let mut by_key = Vec::with_capacity(n_events);
    by_key.extend(0..event_count);
    by_key.sort_unstable_by_key(|&event| PointKey::new(events[event as usize].1));
    let mut group_of = vec![u32::MAX; n_events];
    let key_count = n
        .checked_add(n_events / 2)
        .ok_or(ResourceError::SizeOverflow)?;
    let mut keys = Vec::with_capacity(key_count);
    keys.extend(segments.iter().map(|segment| PointKey::new(segment.start)));
    let mut group_count = 0_u32;
    let mut run = 0;
    while run < n_events {
        let first = by_key[run] as usize;
        let key = PointKey::new(events[first].1);
        let mut end = run.wrapping_add(1);
        while end < n_events && PointKey::new(events[by_key[end] as usize].1) == key {
            end = end.wrapping_add(1);
        }
        if end.wrapping_sub(run) != 2 {
            return Ok(None);
        }
        let second = by_key[run.wrapping_add(1)] as usize;
        if events[first].0 == events[second].0 {
            return Ok(None);
        }
        group_of[first] = group_count;
        group_of[second] = group_count;
        keys.push(key);
        group_count = group_count.wrapping_add(1);
        run = end;
    }
    // Key-coincident vertices — repeated loop vertices, T-junctions (a cut
    // landing on a vertex), zero-length pieces — all MERGE in the general
    // dedup; positional identity cannot express that. Duplicate detection
    // only — a presized set beats sorting the whole key vector.
    let mut seen = HashSet::with_hasher(collection_hash_state()?.clone());
    seen.reserve(keys.len());
    if !keys.iter().all(|&key| seen.insert(key)) {
        return Ok(None);
    }
    // Unique cuts per ordinal have the same order as split_segment_at.
    let mut cuts = Vec::with_capacity(n_events);
    cuts.extend(
        std::iter::zip(&events, &group_of).map(|(&(ordinal, point), &group)| LoopCut {
            ordinal,
            point,
            group,
        }),
    );
    let mut slice_start = 0;
    while slice_start < n_events {
        let ordinal = events[slice_start].0;
        let mut slice_end = slice_start.wrapping_add(1);
        while slice_end < n_events && events[slice_end].0 == ordinal {
            slice_end = slice_end.wrapping_add(1);
        }
        let segment = segments[ordinal as usize];
        let slice = &mut cuts[slice_start..slice_end];
        slice.sort_unstable_by(|left, right| {
            segments::compare_along_segment(segment, left.point, right.point)
        });
        // Same-coordinate neighbors are impossible here: `same_point` and
        // `PointKey` share one canonicalization, so topologically equal
        // cuts share a key, land in one group, and a group never puts two
        // events on one ordinal — `split_segment_at`'s dedup has nothing
        // left to drop.
        debug_assert!(
            !slice
                .array_windows::<2>()
                .any(|[left, right]| same_point(left.point, right.point)),
            "key-grouped cuts cannot repeat on one ordinal"
        );
        slice_start = slice_end;
    }
    Ok(Some((cuts, group_count)))
}

/// Positional multi-loop decomposition: per-vertex cut flags, atomic
/// pieces as `(from, to, loop_index)`, and each loop's anchor vertex id.
type LoopDecomposition = (Vec<bool>, Vec<(u32, u32, u32)>, Vec<u32>);

/// Materialize positional pieces across loop ranges. Each loop's final
/// segment closes to its own first id, while crossing groups are shared
/// globally so a cross-loop crossing joins the embedding components.
/// Pieces carry their loop index so the CSR fill can apply per-loop
/// winding weights.
pub(crate) fn positional_loop_pieces(
    segments: &[Segment],
    loop_ranges: &[(u32, u32)],
    cuts: &[LoopCut],
    group_count: u32,
    points: &mut Vec<XY>,
) -> crate::error::Result<LoopDecomposition> {
    let vertex_count = segments
        .len()
        .checked_add(group_count as usize)
        .ok_or(ResourceError::SizeOverflow)?;
    let piece_count = segments
        .len()
        .checked_add(cuts.len())
        .ok_or(ResourceError::SizeOverflow)?;
    let mut is_cut = Vec::with_capacity(vertex_count);
    let mut group_vertex = vec![u32::MAX; group_count as usize];
    let mut pieces = Vec::with_capacity(piece_count);
    let mut anchor_ids = Vec::with_capacity(loop_ranges.len());
    let mut cursor = 0;
    for (loop_index, &(start, end)) in loop_ranges.iter().enumerate() {
        let loop_index = loop_index as u32;
        let first_id = points.len() as u32;
        anchor_ids.push(first_id);
        points.push(segments[start as usize].start);
        is_cut.push(false);
        let mut current = first_id;
        for ordinal in start..end {
            while cursor < cuts.len() && cuts[cursor].ordinal == ordinal {
                let cut = cuts[cursor];
                cursor = cursor.wrapping_add(1);
                let slot = &mut group_vertex[cut.group as usize];
                let id = if *slot == u32::MAX {
                    let id = points.len() as u32;
                    points.push(cut.point);
                    is_cut.push(true);
                    *slot = id;
                    id
                } else {
                    *slot
                };
                pieces.push((current, id, loop_index));
                current = id;
            }
            let segment_end = if ordinal.wrapping_add(1) < end {
                let id = points.len() as u32;
                // FIRST-ENCOUNTER raw bits, exactly like the general dedup.
                points.push(segments[ordinal as usize].end);
                is_cut.push(false);
                id
            } else {
                first_id
            };
            pieces.push((current, segment_end, loop_index));
            current = segment_end;
        }
    }
    Ok((is_cut, pieces, anchor_ids))
}

/// Order every CSR row of a single-loop build to match the general path:
/// ascending target for degree 2 (what the global edge sort produces), and
/// the same certified angular comparator as the general path for crossings.
/// Returns `false` on a duplicate target inside any row — a
/// duplicate undirected edge the general path would MERGE (numerically
/// squeezed near-collinear linework).
pub(crate) fn order_single_loop_rows<W: WindingWeight>(
    points: &[XY],
    starts: &[u32],
    targets: &mut [u32],
    multiplicities: &mut [W],
    edge_of_slot: &mut [u32],
) -> bool {
    for (vertex, [start, end]) in starts.array_windows::<2>().enumerate() {
        let row = *start as usize..*end as usize;
        if row.len() == 2 {
            let second = row.start.wrapping_add(1);
            let (a, b) = (targets[row.start], targets[second]);
            if a == b {
                return false;
            }
            if a > b {
                targets.swap(row.start, second);
                multiplicities.swap(row.start, second);
                edge_of_slot.swap(row.start, second);
            }
        } else {
            let origin = points[vertex];
            let mut entries = [(0_u32, W::UNSET, 0_u32); 4];
            for (entry, slot) in std::iter::zip(&mut entries, row.clone()) {
                *entry = (targets[slot], multiplicities[slot], edge_of_slot[slot]);
            }
            let entries = &mut entries[..row.len()];
            entries.sort_unstable_by(|left, right| {
                compare_departures(
                    origin,
                    points[left.0 as usize],
                    left.0,
                    points[right.0 as usize],
                    right.0,
                )
            });
            if entries
                .array_windows::<2>()
                .any(|[left, right]| left.0 == right.0)
            {
                return false;
            }
            for (slot, entry) in std::iter::zip(row, entries) {
                let (target, multiplicity, edge) = *entry;
                targets[slot] = target;
                multiplicities[slot] = multiplicity;
                edge_of_slot[slot] = edge;
            }
        }
    }
    true
}
