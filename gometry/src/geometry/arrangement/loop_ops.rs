#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use ahash::HashSetExt as _;

use crate::geometry::arrangement::{WindingWeight, departure_angle, departure_scales};
use crate::geometry::{PointKey, Segment, XY, same_point};

#[derive(Clone, Copy)]
pub(crate) struct LoopCut {
    pub(crate) ordinal: u32,
    pub(crate) point: XY,
    pub(crate) group: u32,
}

/// Phase 1 of [`Arrangement::from_single_loop`] — pure analysis, no pooled
/// buffers touched: noding events, crossing groups, and every structural
/// bail. Returns the cuts ordered per ordinal exactly like
/// `split_segment_at`, plus the group count.
pub(crate) fn single_loop_cuts(segments: &[Segment]) -> Option<(Vec<LoopCut>, u32)> {
    positional_loop_cuts(segments, &[(0, segments.len() as u32)])
}

/// [`single_loop_cuts`] over MULTIPLE positional loops: the same noding
/// events, crossing groups, and bails, with the bit-level chain closure
/// checked per loop range and the duplicate-key bail covering CROSS-LOOP
/// vertex coincidence for free (all loops' start keys share one set).
pub(crate) fn positional_loop_cuts(
    segments: &[Segment],
    loop_ranges: &[(u32, u32)],
) -> Option<(Vec<LoopCut>, u32)> {
    let n = segments.len();
    if n == 0 || loop_ranges.is_empty() {
        return None;
    }
    // Bitwise chain closure per loop: positional vertex identity requires
    // every end to be its successor's start AT THE BIT LEVEL — the same
    // identity the general dedup map keys on.
    for &(start, end) in loop_ranges {
        if end <= start {
            return None;
        }
        if (start..end).any(|index| {
            let next = if index + 1 < end { index + 1 } else { start };
            PointKey::new(segments[index as usize].end)
                != PointKey::new(segments[next as usize].start)
        }) {
            return None;
        }
    }
    // Key-coincident vertices across loops (exact duplicate multiparts,
    // shared endpoints, T-junctions) make positional identity impossible —
    // bail BEFORE the O(E²) noding pass. Previously this check ran after
    // full_noding_events, so stacked multiparts paid a full quadratic node
    // only to fall back to the general path that dedups first.
    {
        let mut seen = crate::collections::HashSet::with_capacity(n);
        if !segments
            .iter()
            .all(|segment| seen.insert(PointKey::new(segment.start)))
        {
            return None;
        }
    }
    let (mut events, overlaps_found) = crate::geometry::overlay::full_noding_events(segments);
    if overlaps_found {
        // Collinear overlaps take the exactness pre-pass — general path.
        return None;
    }
    // Replay split_by_events' ordering exactly: the same unstable sort on
    // the same event list yields the same within-ordinal permutation the
    // general path's stable distance sort starts from.
    events.sort_unstable_by_key(|&(ordinal, _)| ordinal);
    let n_events = events.len();
    // Group the cut events by exact point: one transversal crossing is the
    // SAME bit pattern visited from exactly two distinct ordinals
    // (`segment_cross_point` canonicalizes operand order). Anything else —
    // concurrent crossings, ulp-twin placements, one-sided events left by
    // endpoint-filtered T-junctions — bails.
    let mut by_key: Vec<u32> = (0..n_events as u32).collect();
    by_key.sort_unstable_by_key(|&event| PointKey::new(events[event as usize].1));
    let mut group_of = vec![u32::MAX; n_events];
    let mut keys: Vec<PointKey> = Vec::with_capacity(n + n_events / 2);
    keys.extend(segments.iter().map(|segment| PointKey::new(segment.start)));
    let mut group_count = 0_u32;
    let mut run = 0;
    while run < n_events {
        let first = by_key[run] as usize;
        let key = PointKey::new(events[first].1);
        let mut end = run + 1;
        while end < n_events && PointKey::new(events[by_key[end] as usize].1) == key {
            end += 1;
        }
        if end - run != 2 {
            return None;
        }
        let second = by_key[run + 1] as usize;
        if events[first].0 == events[second].0 {
            return None;
        }
        group_of[first] = group_count;
        group_of[second] = group_count;
        keys.push(key);
        group_count += 1;
        run = end;
    }
    // Key-coincident vertices — repeated loop vertices, T-junctions (a cut
    // landing on a vertex), zero-length pieces — all MERGE in the general
    // dedup; positional identity cannot express that. Duplicate detection
    // only — a presized set beats sorting the whole key vector.
    let mut seen = crate::collections::HashSet::with_capacity(keys.len());
    if !keys.iter().all(|&key| seen.insert(key)) {
        return None;
    }
    // Cuts per ordinal, ordered exactly like split_segment_at: a STABLE
    // sort by squared distance from the segment start (ordinal and group
    // ride along as payload).
    let mut cuts: Vec<LoopCut> = std::iter::zip(&events, &group_of)
        .map(|(&(ordinal, point), &group)| LoopCut {
            ordinal,
            point,
            group,
        })
        .collect();
    let mut slice_start = 0;
    while slice_start < n_events {
        let ordinal = events[slice_start].0;
        let mut slice_end = slice_start + 1;
        while slice_end < n_events && events[slice_end].0 == ordinal {
            slice_end += 1;
        }
        let origin = segments[ordinal as usize].start;
        let slice = &mut cuts[slice_start..slice_end];
        slice.sort_by(|left, right| {
            crate::geometry::segments::distance_order_from_origin(origin, left.point, right.point)
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
    Some((cuts, group_count))
}

/// The ordinal walk of [`Arrangement::from_single_loop`]: dense ids in
/// first-encounter order — exactly the general dedup's assignment over the
/// atomic chain — filling `points` (pooled) and returning the cut flag per
/// id plus the atomic pieces as id pairs.
pub(crate) fn single_loop_pieces(
    segments: &[Segment],
    cuts: &[LoopCut],
    group_count: u32,
    points: &mut Vec<XY>,
) -> (Vec<bool>, Vec<(u32, u32)>) {
    let n = segments.len();
    let mut is_cut: Vec<bool> = Vec::with_capacity(n + group_count as usize);
    let mut group_vertex = vec![u32::MAX; group_count as usize];
    let mut pieces: Vec<(u32, u32)> = Vec::with_capacity(n + cuts.len());
    points.push(segments[0].start);
    is_cut.push(false);
    let mut cursor = 0;
    let mut current = 0_u32;
    for (ordinal, segment) in segments.iter().enumerate() {
        while cursor < cuts.len() && cuts[cursor].ordinal as usize == ordinal {
            let cut = cuts[cursor];
            cursor += 1;
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
            pieces.push((current, id));
            current = id;
        }
        let end = if ordinal + 1 < n {
            let id = points.len() as u32;
            // FIRST-ENCOUNTER raw bits, exactly like the general dedup: the
            // chain vertex is first seen as this segment's END (its key may
            // match the next start through ±0.0 canonicalization while the
            // bits differ).
            points.push(segment.end);
            is_cut.push(false);
            id
        } else {
            0
        };
        pieces.push((current, end));
        current = end;
    }
    (is_cut, pieces)
}

/// Positional multi-loop decomposition: per-vertex cut flags, atomic
/// pieces as `(from, to, loop_index)`, and each loop's anchor vertex id.
type LoopDecomposition = (Vec<bool>, Vec<(u32, u32, u32)>, Vec<u32>);

/// [`single_loop_pieces`] across loop ranges: dense first-encounter ids
/// per loop (each loop's last segment closes to ITS first id), crossing
/// groups shared globally — a cross-loop crossing merges the loops into
/// one component exactly like a self-crossing merges a figure-eight.
/// Pieces carry their loop index so the CSR fill can apply per-loop
/// winding weights.
pub(crate) fn positional_loop_pieces(
    segments: &[Segment],
    loop_ranges: &[(u32, u32)],
    cuts: &[LoopCut],
    group_count: u32,
    points: &mut Vec<XY>,
) -> LoopDecomposition {
    let mut is_cut: Vec<bool> = Vec::with_capacity(segments.len() + group_count as usize);
    let mut group_vertex = vec![u32::MAX; group_count as usize];
    let mut pieces: Vec<(u32, u32, u32)> = Vec::with_capacity(segments.len() + cuts.len());
    let mut anchor_ids: Vec<u32> = Vec::with_capacity(loop_ranges.len());
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
                cursor += 1;
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
            let segment_end = if ordinal + 1 < end {
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
    (is_cut, pieces, anchor_ids)
}

/// Order every CSR row of a single-loop build to match the general path:
/// ascending target for degree 2 (what the global edge sort produces),
/// pseudo-angle then target for crossings (`sort_rows_counterclockwise`'s
/// comparator — a total order over distinct targets, so fill order is
/// irrelevant). Returns `false` on a duplicate target inside any row — a
/// duplicate undirected edge the general path would MERGE (numerically
/// squeezed near-collinear linework).
pub(crate) fn order_single_loop_rows<W: WindingWeight>(
    points: &[XY],
    starts: &[u32],
    targets: &mut [u32],
    multiplicities: &mut [W],
) -> bool {
    for vertex in 0..starts.len() - 1 {
        let row = starts[vertex] as usize..starts[vertex + 1] as usize;
        if row.len() == 2 {
            let (a, b) = (targets[row.start], targets[row.start + 1]);
            if a == b {
                return false;
            }
            if a > b {
                targets.swap(row.start, row.start + 1);
                multiplicities.swap(row.start, row.start + 1);
            }
        } else {
            let origin = points[vertex];
            let (scale_x, scale_y) =
                departure_scales(points, origin, row.clone().map(|slot| targets[slot]));
            let mut entries = [(0.0_f64, 0_u32, W::UNSET); 4];
            for (entry, slot) in std::iter::zip(&mut entries, row.clone()) {
                let to = points[targets[slot] as usize];
                *entry = (
                    departure_angle(origin, to, scale_x, scale_y),
                    targets[slot],
                    multiplicities[slot],
                );
            }
            entries.sort_unstable_by(|left, right| {
                left.0.total_cmp(&right.0).then(left.1.cmp(&right.1))
            });
            if entries
                .array_windows::<2>()
                .any(|[left, right]| left.1 == right.1)
            {
                return false;
            }
            for (slot, &(_, target, multiplicity)) in std::iter::zip(row, &entries) {
                targets[slot] = target;
                multiplicities[slot] = multiplicity;
            }
        }
    }
    true
}
