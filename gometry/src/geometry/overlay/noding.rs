#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use crate::geometry::*;

/// Self-node one segment set: split every segment at its interior contacts
/// with every other, via the pair-once candidate sweep — no index build,
/// the same atomic output as [`node_segments`] against itself (the cut
/// consumer sorts and dedups per segment, so collection order is free).
pub(crate) fn self_node_segments(segments: &[Segment]) -> Vec<Segment> {
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

/// [`self_node_segments`] also reporting each atomic piece's SOURCE
/// segment index — the operand provenance the binary overlay's
/// per-operand windings ride on.
pub(crate) fn self_node_segments_sourced(segments: &[Segment]) -> (Vec<Segment>, Vec<u32>) {
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
