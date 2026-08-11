#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use ahash::HashSetExt as _;

use crate::geometry::{
    CHAIN_MIN_SEGMENTS, CoordSeq, HashMap, HashMapExt as _, HashSet, Orientation, PointKey,
    RUN_NODING_MIN, Segment, SegmentIndex, XY, dedup_consecutive_xy, for_each_candidate_pair,
    orientation, point_on_segment, same_point, segment_cross_point, segment_envelopes_disjoint,
    single_chain, undirected_segment_edge_key,
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
    fn empty() -> Self {
        Self {
            offsets: vec![0],
            entries: Vec::new(),
        }
    }

    pub(crate) fn owners(&self, piece: usize) -> &[SegmentSource] {
        let start = self.offsets[piece] as usize;
        let end = self.offsets[piece + 1] as usize;
        &self.entries[start..end]
    }

    pub(crate) const fn len(&self) -> usize {
        self.offsets.len().saturating_sub(1)
    }

    /// True when every unique edge has exactly one forward owner (identity
    /// multiplicity — expand is a no-op).
    fn all_single_forward(&self) -> bool {
        self.entries.len() == self.len() && self.entries.iter().all(|&(_, reversed)| !reversed)
    }

    pub(crate) fn iter(&self) -> impl Iterator<Item = &[SegmentSource]> + '_ {
        self.offsets.array_windows::<2>().map(|window| {
            let start = window[0] as usize;
            let end = window[1] as usize;
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
/// Two-pass CSR build: assign unique indices, count multiplicity, place
/// `(ordinal, reversed)` into a single flat `entries` vector. Unique input
/// pays HashMap inserts + two flat vecs — never N tiny heap `Vec`s.
fn dedup_undirected_segments_sourced(segments: &[Segment]) -> (Vec<Segment>, SegmentProvenance) {
    let n = segments.len();
    if n == 0 {
        return (Vec::new(), SegmentProvenance::empty());
    }
    let mut index_of: HashMap<(PointKey, PointKey), u32> = HashMap::with_capacity(n);
    let mut unique: Vec<Segment> = Vec::with_capacity(n);
    // Per original ordinal: (unique_index, reversed).
    let mut assignment: Vec<(u32, bool)> = Vec::with_capacity(n);
    for &segment in segments {
        let key = undirected_segment_edge_key(segment);
        if let Some(&unique_index) = index_of.get(&key) {
            let rep = unique[unique_index as usize];
            // Same undirected endpoints ⇒ same direction or exact reverse.
            let reversed = !same_point(rep.start, segment.start);
            debug_assert!(
                (same_point(rep.start, segment.start) && same_point(rep.end, segment.end))
                    || (same_point(rep.start, segment.end) && same_point(rep.end, segment.start))
            );
            assignment.push((unique_index, reversed));
        } else {
            let unique_index = unique.len() as u32;
            index_of.insert(key, unique_index);
            unique.push(segment);
            assignment.push((unique_index, false));
        }
    }
    let unique_count = unique.len();
    let mut counts = vec![0_u32; unique_count];
    for &(unique_index, _) in &assignment {
        counts[unique_index as usize] += 1;
    }
    let mut offsets = Vec::with_capacity(unique_count + 1);
    offsets.push(0);
    for &count in &counts {
        offsets.push(offsets[offsets.len() - 1] + count);
    }
    let mut entries = vec![(0_u32, false); n];
    let mut cursor = offsets[..unique_count].to_vec();
    for (ordinal, &(unique_index, reversed)) in assignment.iter().enumerate() {
        let slot = cursor[unique_index as usize] as usize;
        entries[slot] = (ordinal as u32, reversed);
        cursor[unique_index as usize] += 1;
    }
    (unique, SegmentProvenance { offsets, entries })
}

/// Expand noded unique pieces once per original directed source so unit-weight
/// consumers ([`Arrangement::new`], even-odd repair) keep directed multiplicity:
/// same-direction stacks accumulate, opposite directions on a shared edge
/// cancel. Noding runs on the unique undirected set (O(U²)); expansion is O(E).
fn expand_directed_atoms(
    atomic: Vec<Segment>,
    unique_sources: &[u32],
    provenance: &SegmentProvenance,
) -> Vec<Segment> {
    if provenance.all_single_forward() {
        return atomic;
    }
    let mut out = Vec::with_capacity(atomic.len().saturating_mul(2));
    for (piece, &unique_index) in atomic.iter().zip(unique_sources) {
        for &(_, reversed) in provenance.owners(unique_index as usize) {
            out.push(if reversed {
                Segment {
                    start: piece.end,
                    end: piece.start,
                }
            } else {
                *piece
            });
        }
    }
    out
}

/// Map per-unique provenance onto per-atomic-piece provenance (each atomic
/// piece inherits the full owner set of its unique parent edge).
fn expand_provenance_to_atoms(
    unique_sources: &[u32],
    provenance: &SegmentProvenance,
) -> SegmentProvenance {
    let atom_count = unique_sources.len();
    let mut offsets = Vec::with_capacity(atom_count + 1);
    offsets.push(0);
    let mut total = 0_u32;
    for &unique_index in unique_sources {
        total += provenance.owners(unique_index as usize).len() as u32;
        offsets.push(total);
    }
    let mut entries = Vec::with_capacity(total as usize);
    for &unique_index in unique_sources {
        entries.extend_from_slice(provenance.owners(unique_index as usize));
    }
    SegmentProvenance { offsets, entries }
}

/// Self-node one segment set: split every segment at its interior contacts
/// with every other, via the pair-once candidate sweep — no index build,
/// the same atomic output as [`node_segments`] against itself (the cut
/// consumer sorts and dedups per segment, so collection order is free).
///
/// Exact undirected XY duplicates always collapse for the pair scan via flat
/// CSR provenance (no size threshold — thresholds are cliffs for cascade
/// callers that re-enter below a floor). Unique input stays linear: dedup is
/// one HashMap + two flat vecs, then noding on U = E. Duplicate-rich input
/// nodes O(U²) and re-expands directed multiplicity for unit-weight consumers.
pub(crate) fn self_node_segments(segments: &[Segment]) -> Vec<Segment> {
    if segments.is_empty() {
        return Vec::new();
    }
    let (unique, provenance) = dedup_undirected_segments_sourced(segments);
    if provenance.all_single_forward() {
        // Unique (or pure forward 1:1 after collapse of identical copies that
        // still leave single forward — actually identical dups make len>1).
        // all_single_forward ⇒ U owners, each single forward ⇒ U == E and no
        // reverse; unique may still be a proper subset only if... never when
        // all single. So unique.len() == segments.len() and we can node unique
        // (equal to original order of first occurrence = original order).
        return self_node_segments_unique(&unique);
    }
    let (atomic, unique_sources) = self_node_segments_sourced_unique(&unique);
    expand_directed_atoms(atomic, &unique_sources, &provenance)
}

/// Node a pre-deduped segment set (collinear-overlap pre-pass + full pass).
fn self_node_segments_unique(segments: &[Segment]) -> Vec<Segment> {
    // Collinear OVERLAPS need a pre-pass: splitting them at each other's
    // endpoints first (exact points) makes coincident strokes
    // BIT-IDENTICAL, so the main pass's canonicalized crossing placement
    // lands on the same point for every copy — a third segment crossing an
    // overlapped run can no longer mint ulp-twin cut points by being
    // solved against two different host extents. Overlaps only exist in
    // folded linework (inverted offsets); the common case detects their
    // absence in the single main sweep and pays nothing extra.
    let (atomic, overlaps_found) = full_noding_pass(segments);
    if !overlaps_found {
        return atomic;
    }
    let pre_split = collinear_overlap_pass(segments);
    full_noding_pass(&pre_split).0
}

/// [`self_node_segments`] with multi-source provenance per atomic piece.
///
/// Exact undirected duplicates always collapse before noding so the pair scan
/// is O(U²) in unique edges, not O(E²) in duplicate multiplicity. Each atomic
/// piece carries the **full** set of original ordinals that owned that
/// undirected edge (with reverse flags) in flat CSR form — binary overlay
/// winding depends on every operand's contribution, not just the first source.
///
/// Atoms stay in the representative direction; consumers fold reverse flags
/// into weights rather than re-expanding E copies of each piece. One uniform
/// path (no size threshold, no probe-vs-no-probe fork).
pub(crate) fn self_node_segments_sourced(
    segments: &[Segment],
) -> (Vec<Segment>, SegmentProvenance) {
    if segments.is_empty() {
        return (Vec::new(), SegmentProvenance::empty());
    }
    let (unique, provenance) = dedup_undirected_segments_sourced(segments);
    let (atomic, unique_sources) = self_node_segments_sourced_unique(&unique);
    let multi = expand_provenance_to_atoms(&unique_sources, &provenance);
    (atomic, multi)
}

fn self_node_segments_sourced_unique(segments: &[Segment]) -> (Vec<Segment>, Vec<u32>) {
    let (events, overlaps_found) = full_noding_events(segments);
    if !overlaps_found {
        return split_by_events_sourced(segments, events);
    }
    let (pre_split, pre_sources) =
        split_by_events_sourced(segments, collinear_overlap_events(segments));
    let (events, _) = full_noding_events(&pre_split);
    let (atomic, mid_sources) = split_by_events_sourced(&pre_split, events);
    let sources = mid_sources
        .into_iter()
        .map(|mid| pre_sources[mid as usize])
        .collect();
    (atomic, sources)
}

/// Pass 1 of [`self_node_segments`]: collinear-overlap endpoint cuts only.
pub(crate) fn collinear_overlap_pass(segments: &[Segment]) -> Vec<Segment> {
    split_by_events(segments, collinear_overlap_events(segments))
}

/// The cut events of [`collinear_overlap_pass`].
pub(crate) fn collinear_overlap_events(segments: &[Segment]) -> Vec<(u32, XY)> {
    let mut events: Vec<(u32, XY)> = Vec::with_capacity(8);
    let _ = for_each_candidate_pair::<CHAIN_MIN_SEGMENTS>(segments, single_chain, |left, right| {
        let (a, b) = (segments[left], segments[right]);
        if orientation(a.start, a.end, b.start) == Orientation::Collinear
            && orientation(a.start, a.end, b.end) == Orientation::Collinear
        {
            push_collinear_overlap_cuts(segments, left, right, &mut events);
        }
        std::ops::ControlFlow::Continue(())
    });
    events
}

fn push_collinear_overlap_cuts(
    segments: &[Segment],
    left: usize,
    right: usize,
    events: &mut Vec<(u32, XY)>,
) -> bool {
    let mut cut_any = false;
    for (target, other) in [(left, right), (right, left)] {
        let segment = segments[target];
        for point in [segments[other].start, segments[other].end] {
            if point_on_segment(point, segment.start, segment.end)
                && !same_point(point, segment.start)
                && !same_point(point, segment.end)
            {
                events.push((target as u32, point));
                cut_any = true;
            }
        }
    }
    cut_any
}

/// The main pass of [`self_node_segments`]: full pairwise interior cuts,
/// also reporting whether any collinear OVERLAP pair was seen (the trigger
/// for the exactness pre-pass).
pub(crate) fn full_noding_pass(segments: &[Segment]) -> (Vec<Segment>, bool) {
    let (events, overlaps_found) = full_noding_events(segments);
    (split_by_events(segments, events), overlaps_found)
}

/// The cut events of [`full_noding_pass`], plus the collinear-overlap flag.
/// Also the noding front end of [`Arrangement::from_single_loop`], which
/// replays [`split_by_events`]' ordering bit-for-bit without materializing
/// the atomic segment soup.
pub(in crate::geometry) fn full_noding_events(segments: &[Segment]) -> (Vec<(u32, XY)>, bool) {
    let mut events: Vec<(u32, XY)> = Vec::with_capacity(8);
    let mut overlaps_found = false;
    // L1-aware sweep: large pools take the monotone-run path, avoiding the flat
    // SweepEntry sort that exceeds L1 at ~1 K segments (the arrangement noding
    // behind union/difference fallback, symmetric_difference, intersection,
    // relate, and polygonize all flow through here).
    let _ = for_each_candidate_pair::<RUN_NODING_MIN>(segments, single_chain, |left, right| {
        // Endpoint-sharing pairs (every ADJACENT pair of a chain) cannot
        // cross at an interior point — the only interior cuts they can
        // produce are collinear-overlap endpoints, decided by two robust
        // orientations. Skipping the exact crossing solver here removes
        // the dominant cost of self-noding closed loops.
        let (a, b) = (segments[left], segments[right]);
        // Envelope reject FIRST: the sweep prunes only its own axis, so
        // cross-axis-disjoint candidates land here — four compares beat
        // the two robust orientations they'd otherwise always pay (a
        // disjoint-envelope pair shares no point: no cuts of any kind).
        if segment_envelopes_disjoint(a, b) {
            return std::ops::ControlFlow::Continue(());
        }
        let collinear = orientation(a.start, a.end, b.start) == Orientation::Collinear
            && orientation(a.start, a.end, b.end) == Orientation::Collinear;
        if collinear {
            overlaps_found |= push_collinear_overlap_cuts(segments, left, right, &mut events);
            return std::ops::ControlFlow::Continue(());
        }
        if same_point(a.start, b.start)
            || same_point(a.start, b.end)
            || same_point(a.end, b.start)
            || same_point(a.end, b.end)
        {
            return std::ops::ControlFlow::Continue(());
        }
        // `for_each_segment_overlap_point` reports the points splitting its
        // FIRST operand, so each candidate pair contributes both ways.
        for (target, other) in [(left, right), (right, left)] {
            let segment = segments[target];
            for_each_segment_overlap_point(segment, segments[other], |point| {
                if !same_point(point, segment.start) && !same_point(point, segment.end) {
                    events.push((target as u32, point));
                }
            });
        }
        std::ops::ControlFlow::Continue(())
    });
    (events, overlaps_found)
}

/// Split every segment at its collected `(ordinal, cut)` events — the
/// shared tail of both noding passes (the splitter sorts and dedups cuts
/// per segment).
pub(crate) fn split_by_events(segments: &[Segment], mut events: Vec<(u32, XY)>) -> Vec<Segment> {
    events.sort_unstable_by_key(|&(ordinal, _)| ordinal);
    let mut atomic = Vec::with_capacity(segments.len() + events.len());
    let mut cuts: Vec<XY> = Vec::new();
    let mut cursor = 0;
    for (index, &segment) in segments.iter().enumerate() {
        cuts.clear();
        while cursor < events.len() && events[cursor].0 as usize == index {
            cuts.push(events[cursor].1);
            cursor += 1;
        }
        split_segment_at(segment, &mut cuts, &mut atomic);
    }
    atomic
}

/// [`split_by_events`] also reporting each piece's source ordinal.
pub(crate) fn split_by_events_sourced(
    segments: &[Segment],
    mut events: Vec<(u32, XY)>,
) -> (Vec<Segment>, Vec<u32>) {
    events.sort_unstable_by_key(|&(ordinal, _)| ordinal);
    let mut atomic = Vec::with_capacity(segments.len() + events.len());
    let mut sources: Vec<u32> = Vec::with_capacity(segments.len() + events.len());
    let mut cuts: Vec<XY> = Vec::new();
    let mut cursor = 0;
    for (index, &segment) in segments.iter().enumerate() {
        cuts.clear();
        while cursor < events.len() && events[cursor].0 as usize == index {
            cuts.push(events[cursor].1);
            cursor += 1;
        }
        split_segment_at(segment, &mut cuts, &mut atomic);
        sources.resize(atomic.len(), index as u32);
    }
    (atomic, sources)
}

/// Split each segment of `segments` at every interior intersection point it has
/// with any segment in `all`, yielding atomic sub-segments.
pub(crate) fn node_segments(segments: &[Segment], all: &SegmentIndex) -> Vec<Segment> {
    // Every input yields at least itself; cuts only add to that.
    let mut atomic = Vec::with_capacity(segments.len());
    // One cuts scratch for the whole batch (cleared per segment by
    // `split_segment_at`'s consumer contract below).
    let mut cuts: Vec<XY> = Vec::new();
    for &segment in segments {
        cuts.clear();
        // Only envelope-overlapping candidates can produce a cut, so the
        // index sweep nodes exactly the same points as the all-pairs scan.
        for entry in all.intersecting_candidates(segment) {
            #[cfg(test)]
            crate::geometry::overlay::test_counters::bump_candidate_pair();
            for_each_segment_overlap_point(segment, entry.segment, |point| {
                if !same_point(point, segment.start) && !same_point(point, segment.end) {
                    cuts.push(point);
                }
            });
        }
        split_segment_at(segment, &mut cuts, &mut atomic);
    }
    atomic
}

/// Visit the points at which `a` is split by `b`: a transversal crossing
/// within both segments, or the interior endpoints of a collinear overlap.
/// At most two points — visited, never allocated.
pub(crate) fn for_each_segment_overlap_point(a: Segment, b: Segment, mut visit: impl FnMut(XY)) {
    let collinear = orientation(a.start, a.end, b.start) == Orientation::Collinear
        && orientation(a.start, a.end, b.end) == Orientation::Collinear;
    if collinear {
        for point in [b.start, b.end] {
            if point_on_segment(point, a.start, a.end) {
                visit(point);
            }
        }
    } else if let Some(point) = segment_cross_point(a, b) {
        visit(point);
    }
}

/// Split `segment` at `cuts` (ordered along the segment, deduped) into atomic
/// sub-segments appended to `out`.
pub(crate) fn split_segment_at(segment: Segment, cuts: &mut Vec<XY>, out: &mut Vec<Segment>) {
    if cuts.is_empty() {
        out.push(segment);
        return;
    }
    cuts.sort_by_cached_key(|cut| {
        crate::geometry::segments::point_distance_squared(segment.start, *cut).to_bits()
    });
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
}

/// Dedup coincident undirected atomic segments into 2-point coordinate
/// sequences.
pub(crate) fn dedup_segments_to_coordseqs(segments: Vec<Segment>) -> Vec<CoordSeq> {
    let mut seen: HashSet<(PointKey, PointKey)> = HashSet::new();
    let mut lines = Vec::new();
    for segment in segments {
        if seen.insert(undirected_segment_edge_key(segment)) {
            lines.push(CoordSeq::from(vec![segment.start, segment.end]));
        }
    }
    lines
}
