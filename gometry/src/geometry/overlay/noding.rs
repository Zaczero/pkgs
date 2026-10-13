use std::collections::hash_map::Entry;

use crate::error::{ResourceError, Result};
use crate::geometry::{HashMap, PointKey, Segment, same_point, undirected_segment_edge_key};

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
