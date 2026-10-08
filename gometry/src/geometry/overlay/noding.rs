use std::collections::hash_map::Entry;

use crate::error::{ResourceError, Result};
#[cfg(test)]
use crate::geometry::segments::{
    PairOrientations, point_in_segment_envelope, segment_cross_point_with_orientations,
    segment_envelopes_disjoint,
};
use crate::geometry::{HashMap, PointKey, Segment, same_point, undirected_segment_edge_key};
#[cfg(test)]
use crate::geometry::{
    Orientation, XY, dedup_consecutive_xy, for_each_candidate_pair, segments, single_chain,
};

/// One original source ordinal that owned a unique undirected edge, plus
/// whether that source's direction was the reverse of the representative
/// kept for noding. Callers that need directed weights (binary overlay
/// windings) apply `weight.neg()` when `reversed` is set.
pub(crate) type SegmentSource = (u32, bool);

/// Flat CSR multi-owner provenance: `offsets` has `unique_count + 1` entries,
/// `entries[offsets[i]..offsets[i+1]]` is every `(ordinal, reversed)` owner of
/// unique edge `i`. Always cheap on unique input (one flat vec of E singles,
/// no per-piece `Vec` heap allocs) so noding never needs a size-threshold fork.
#[derive(Clone, Debug, Default)]
pub(crate) struct SegmentProvenance {
    offsets: Vec<u32>,
    entries: Vec<SegmentSource>,
}

impl SegmentProvenance {
    #[expect(
        clippy::indexing_slicing,
        reason = "a valid piece has adjacent offsets and every internal CSR builder keeps the terminal owner range in bounds"
    )]
    pub(crate) fn owners(&self, piece: usize) -> &[SegmentSource] {
        let start = self.offsets[piece] as usize;
        let end = self.offsets[piece.wrapping_add(1)] as usize;
        &self.entries[start..end]
    }

    /// Adopt caller-built CSR columns without copying them.
    pub(crate) fn from_csr(offsets: Vec<u32>, entries: Vec<SegmentSource>) -> Self {
        debug_assert!(!offsets.is_empty());
        Self { offsets, entries }
    }

    pub(crate) const fn len(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "internal CSR builders publish monotone offsets whose terminal value equals the owner-entry length"
    )]
    pub(crate) fn iter(&self) -> impl Iterator<Item = &[SegmentSource]> + '_ {
        self.offsets.array_windows::<2>().map(|&[start, end]| {
            let start = start as usize;
            let end = end as usize;
            &self.entries[start..end]
        })
    }
}

impl std::ops::Index<usize> for SegmentProvenance {
    type Output = [SegmentSource];

    fn index(&self, piece: usize) -> &Self::Output {
        self.owners(piece)
    }
}

/// Sourced undirected dedup into flat CSR: one representative directed segment
/// per exact endpoint pair, plus every original ordinal that owned that edge
/// and whether that ordinal's direction is reversed relative to the
/// representative. Binary overlay winding needs the FULL provenance set — a
/// shared edge keeps both operands' tags after expansion.
///
/// Unique input publishes identity provenance directly. The first repeated edge
/// stages assignments for the unique prefix, then counting placement groups
/// `(ordinal, reversed)` owners into one flat `entries` vector.
#[expect(
    clippy::indexing_slicing,
    reason = "map-issued unique IDs address parallel count cursors, and checked CSR row/total counts bound owner-entry scatter slots"
)]
pub(crate) fn dedup_undirected_segments_sourced(
    segments: impl IntoIterator<Item = Segment>,
) -> Result<(Vec<Segment>, SegmentProvenance)> {
    let segments = segments.into_iter();
    let capacity = segments.size_hint().0;
    let mut index_of: HashMap<(PointKey, PointKey), u32> =
        HashMap::with_hasher(crate::collections::collection_hash_state()?.clone());
    index_of.reserve(capacity);
    let mut unique: Vec<Segment> = Vec::with_capacity(capacity);
    // Per original ordinal: (unique_index, reversed).
    let mut assignment: Vec<(u32, bool)> = Vec::new();
    for segment in segments {
        let key = undirected_segment_edge_key(segment);
        match index_of.entry(key) {
            Entry::Occupied(entry) => {
                if assignment.is_empty() {
                    let prefix_count =
                        u32::try_from(unique.len()).map_err(|_| ResourceError::SizeOverflow)?;
                    assignment.reserve(capacity);
                    assignment.extend((0..prefix_count).map(|index| (index, false)));
                }
                let unique_index = *entry.get();
                let rep = unique[unique_index as usize];
                // Same undirected endpoints ⇒ same direction or exact reverse.
                let reversed = !same_point(rep.start, segment.start);
                debug_assert!(
                    (same_point(rep.start, segment.start) && same_point(rep.end, segment.end))
                        || (same_point(rep.start, segment.end)
                            && same_point(rep.end, segment.start))
                );
                assignment.push((unique_index, reversed));
            },
            Entry::Vacant(entry) => {
                let unique_index =
                    u32::try_from(unique.len()).map_err(|_| ResourceError::SizeOverflow)?;
                entry.insert(unique_index);
                unique.push(segment);
                if !assignment.is_empty() {
                    assignment.push((unique_index, false));
                }
            },
        }
    }
    drop(index_of);
    let unique_count = unique.len();
    if assignment.is_empty() {
        let count = u32::try_from(unique_count).map_err(|_| ResourceError::SizeOverflow)?;
        return Ok((unique, SegmentProvenance {
            offsets: (0..=count).collect(),
            entries: (0..count).map(|ordinal| (ordinal, false)).collect(),
        }));
    }
    let mut counts = vec![0_u32; unique_count];
    for &(unique_index, _) in &assignment {
        counts[unique_index as usize] = counts[unique_index as usize]
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?;
    }
    let offsets = placement_offsets(&mut counts)?;
    let mut entries = vec![(0_u32, false); assignment.len()];
    for (ordinal, &(unique_index, reversed)) in assignment.iter().enumerate() {
        let cursor = &mut counts[unique_index as usize];
        let slot = *cursor as usize;
        entries[slot] = (
            u32::try_from(ordinal).map_err(|_| ResourceError::SizeOverflow)?,
            reversed,
        );
        *cursor = (*cursor).wrapping_add(1);
    }
    Ok((unique, SegmentProvenance { offsets, entries }))
}

/// Build CSR offsets, replacing row counts with starting write cursors.
pub(super) fn placement_offsets(counts: &mut [u32]) -> Result<Vec<u32>> {
    let mut offsets = Vec::with_capacity(counts.len().wrapping_add(1));
    let mut start = 0_u32;
    for count in counts {
        offsets.push(start);
        let end = start
            .checked_add(*count)
            .ok_or(ResourceError::SizeOverflow)?;
        *count = start;
        start = end;
    }
    offsets.push(start);
    Ok(offsets)
}

/// Map per-unique provenance onto per-atomic-piece provenance (each atomic
/// piece inherits the full owner set of its unique parent edge).
#[cfg(test)]
fn expand_provenance_to_atoms(
    unique_sources: &[u32],
    provenance: &SegmentProvenance,
) -> Result<SegmentProvenance> {
    let atom_count = unique_sources.len();
    let mut offsets = Vec::with_capacity(
        atom_count
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?,
    );
    offsets.push(0);
    let mut total = 0_u32;
    for &unique_index in unique_sources {
        let count = u32::try_from(provenance.owners(unique_index as usize).len())
            .map_err(|_| ResourceError::SizeOverflow)?;
        total = total
            .checked_add(count)
            .ok_or(ResourceError::SizeOverflow)?;
        offsets.push(total);
    }
    let mut entries = Vec::with_capacity(total as usize);
    for &unique_index in unique_sources {
        entries.extend_from_slice(provenance.owners(unique_index as usize));
    }
    Ok(SegmentProvenance { offsets, entries })
}

/// Test-only sourced noding, with multi-source provenance retained per atomic
/// piece.
///
/// Exact undirected duplicates always collapse before noding so the pair scan
/// is O(U²) in unique edges, not O(E²) in duplicate multiplicity. Each atomic
/// piece carries the **full** set of original ordinals that owned that
/// undirected edge, including each source direction.
///
/// Atoms stay in the representative direction; each owner retains its source
/// direction in the provenance row.
#[cfg(test)]
pub(crate) fn self_node_segments_sourced(
    segments: impl IntoIterator<Item = Segment>,
) -> Result<(Vec<Segment>, SegmentProvenance)> {
    let (unique, provenance) = dedup_undirected_segments_sourced(segments)?;
    if unique.is_empty() {
        return Ok((unique, provenance));
    }
    let (atomic, unique_sources) = self_node_segments_sourced_unique(&unique)?;
    let multi = expand_provenance_to_atoms(&unique_sources, &provenance)?;
    Ok((atomic, multi))
}

#[cfg(test)]
fn self_node_segments_sourced_unique(segments: &[Segment]) -> Result<(Vec<Segment>, Vec<u32>)> {
    let NodingScan {
        events,
        overlaps_found,
    } = full_noding_events(segments)?;
    if !overlaps_found {
        return split_by_events_sourced(segments, events);
    }
    let (pre_split, pre_sources) =
        split_by_events_sourced(segments, collinear_overlap_events(segments)?)?;
    let NodingScan { events, .. } = full_noding_events(&pre_split)?;
    let (atomic, mut sources) = split_by_events_sourced(&pre_split, events)?;
    for source in &mut sources {
        *source = pre_sources[*source as usize];
    }
    Ok((atomic, sources))
}

/// Endpoint cuts for collinear overlaps in sourced noding.
#[cfg(test)]
pub(crate) fn collinear_overlap_events(segments: &[Segment]) -> Result<Vec<(u32, XY)>> {
    let mut events: Vec<(u32, XY)> = Vec::new();
    let mut failure = Ok(());
    let _ = for_each_candidate_pair(segments, single_chain, |left, right| {
        let (a, b) = (segments[left], segments[right]);
        let orientations = PairOrientations::new(a, b);
        if orientations.right_start == Orientation::Collinear
            && orientations.right_end == Orientation::Collinear
            && let Err(error) =
                push_collinear_overlap_cuts(segments, left, right, orientations, &mut events)
        {
            failure = Err(error);
            return std::ops::ControlFlow::Break(());
        }
        std::ops::ControlFlow::Continue(())
    })?;
    failure?;
    Ok(events)
}

#[cfg(test)]
fn push_collinear_overlap_cuts(
    segments: &[Segment],
    left: usize,
    right: usize,
    orientations: PairOrientations,
    events: &mut Vec<(u32, XY)>,
) -> Result<bool> {
    let mut cut_any = false;
    let (a, b) = (segments[left], segments[right]);
    let mut push_if_interior = |target: usize, point: XY, segment: Segment| -> Result<()> {
        if point_in_segment_envelope(point, segment)
            && !same_point(point, segment.start)
            && !same_point(point, segment.end)
        {
            let target = u32::try_from(target).map_err(|_| ResourceError::SizeOverflow)?;
            events.push((target, point));
            cut_any = true;
        }
        Ok(())
    };

    // Both endpoints of b are already known to be on a's carrier. For a
    // non-degenerate a, that also establishes the common carrier, so closed
    // envelopes decide all four endpoint cuts. A degenerate a has no carrier;
    // its endpoints still need b's precomputed orientation signs.
    for point in [b.start, b.end] {
        push_if_interior(left, point, a)?;
    }
    if same_point(a.start, a.end) {
        for (point, orientation) in [
            (a.start, orientations.left_start),
            (a.end, orientations.left_end),
        ] {
            if orientation == Orientation::Collinear {
                push_if_interior(right, point, b)?;
            }
        }
    } else {
        for point in [a.start, a.end] {
            push_if_interior(right, point, b)?;
        }
    }
    Ok(cut_any)
}

/// What one pass of the candidate sweep learned about a segment pool.
#[cfg(test)]
pub(in crate::geometry) struct NodingScan {
    /// Interior cut points, per segment ordinal.
    pub(in crate::geometry) events: Vec<(u32, XY)>,
    /// A collinear pair shared more than a point — an interior overlap, or two
    /// identical segments.
    pub(in crate::geometry) overlaps_found: bool,
}

/// Pairwise cut events and collinear-overlap observations for sourced noding.
#[cfg(test)]
pub(in crate::geometry) fn full_noding_events(segments: &[Segment]) -> Result<NodingScan> {
    let mut events: Vec<(u32, XY)> = Vec::new();
    let mut overlaps_found = false;
    let mut failure = Ok(());
    // Large pools take the monotone-run path and avoid a flat sweep-entry sort.
    let _ = for_each_candidate_pair(segments, single_chain, |left, right| {
        // Endpoint-sharing pairs (every ADJACENT pair of a chain) cannot
        // cross at an interior point — the only interior cuts they can
        // produce are collinear-overlap endpoints, decided by the robust
        // orientations below. Skipping the exact crossing solver here removes
        // the dominant cost of self-noding closed loops.
        let (a, b) = (segments[left], segments[right]);
        // Envelope reject FIRST: the sweep prunes only its own axis, so
        // cross-axis-disjoint candidates land here — four compares beat the
        // robust orientation work they'd otherwise pay (a
        // disjoint-envelope pair shares no point: no cuts of any kind).
        if segment_envelopes_disjoint(a, b) {
            return std::ops::ControlFlow::Continue(());
        }
        let orientations = PairOrientations::new(a, b);
        let shares_endpoint = same_point(a.start, b.start)
            || same_point(a.start, b.end)
            || same_point(a.end, b.start)
            || same_point(a.end, b.end);
        // A shared endpoint between segments that are not consecutive is a
        // junction whatever else the pair does — collinear pairs meeting end
        // to end included.
        let collinear = orientations.right_start == Orientation::Collinear
            && orientations.right_end == Orientation::Collinear;
        if collinear {
            // An identical pair shares its whole extent and cuts nothing; it is
            // an overlap all the same, and the one a raw (undeduplicated) pool
            // has to report.
            let identical = (same_point(a.start, b.start) && same_point(a.end, b.end))
                || (same_point(a.start, b.end) && same_point(a.end, b.start));
            if identical {
                overlaps_found = true;
            } else {
                match push_collinear_overlap_cuts(segments, left, right, orientations, &mut events)
                {
                    Ok(cut) => overlaps_found |= cut,
                    Err(error) => {
                        failure = Err(error);
                        return std::ops::ControlFlow::Break(());
                    },
                }
            }
            return std::ops::ControlFlow::Continue(());
        }
        if shares_endpoint {
            return std::ops::ControlFlow::Continue(());
        }
        match segment_cross_point_with_orientations(a, b, orientations) {
            Ok(Some(point)) => {
                for (target, segment) in [(left, a), (right, b)] {
                    if !same_point(point, segment.start) && !same_point(point, segment.end) {
                        let result = u32::try_from(target)
                            .map_err(|_| ResourceError::SizeOverflow.into())
                            .map(|target| events.push((target, point)));
                        if let Err(error) = result {
                            failure = Err(error);
                            return std::ops::ControlFlow::Break(());
                        }
                    }
                }
            },
            Ok(None) => {},
            Err(error) => {
                failure = Err(error);
                return std::ops::ControlFlow::Break(());
            },
        }
        std::ops::ControlFlow::Continue(())
    })?;
    failure?;
    Ok(NodingScan {
        events,
        overlaps_found,
    })
}

/// [`split_by_events`] also reporting each piece's source ordinal.
#[cfg(test)]
pub(crate) fn split_by_events_sourced(
    segments: &[Segment],
    mut events: Vec<(u32, XY)>,
) -> Result<(Vec<Segment>, Vec<u32>)> {
    events.sort_unstable_by_key(|&(ordinal, _)| ordinal);
    let capacity = segments
        .len()
        .checked_add(events.len())
        .ok_or(ResourceError::SizeOverflow)?;
    let mut atomic = Vec::new();
    let mut sources: Vec<u32> = Vec::new();
    atomic.reserve(capacity);
    sources.reserve(capacity);
    let mut cuts: Vec<XY> = Vec::new();
    let mut cursor = 0;
    for (index, &segment) in segments.iter().enumerate() {
        cuts.clear();
        while cursor < events.len() && events[cursor].0 as usize == index {
            cuts.push(events[cursor].1);
            cursor = cursor
                .checked_add(1)
                .expect("the event cursor is below its vector length");
        }
        split_segment_at(segment, &mut cuts, &mut atomic)?;
        sources.resize(
            atomic.len(),
            u32::try_from(index).map_err(|_| ResourceError::SizeOverflow)?,
        );
    }
    Ok((atomic, sources))
}

/// Split `segment` at `cuts` (ordered along the segment, deduped) into atomic
/// sub-segments appended to `out`.
#[cfg(test)]
pub(crate) fn split_segment_at(
    segment: Segment,
    cuts: &mut Vec<XY>,
    out: &mut Vec<Segment>,
) -> Result<()> {
    out.reserve(
        cuts.len()
            .checked_add(1)
            .ok_or(ResourceError::SizeOverflow)?,
    );
    if cuts.is_empty() {
        out.push(segment);
        return Ok(());
    }
    crate::geometry::try_sort_by(cuts, |left, right| {
        Ok(segments::compare_along_segment(segment, *left, *right))
    })?;
    dedup_consecutive_xy(cuts);
    let mut previous = segment.start;
    for &cut in cuts.iter() {
        if !same_point(previous, cut) {
            out.push(Segment {
                start: previous,
                end: cut,
            });
            previous = cut;
        }
    }
    if !same_point(previous, segment.end) {
        out.push(Segment {
            start: previous,
            end: segment.end,
        });
    }
    Ok(())
}
