#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::ops::ControlFlow;

use crate::geometry::{
    Bounds, DistanceParts, PointKey, PreparedLinework, Segment, SegmentContact, Shape, XY,
    segment_contact, segments_intersect,
};

/// Below this many candidate *pairs* the brute-force double loop wins: the
/// index build (sort + tree nodes) costs more than it saves.
pub(crate) const SEGMENT_INDEX_MIN_PAIRS: usize = 4096;

/// The sweep's build is just an envelope `Vec` and one sort — far cheaper
/// than an R-tree — so its brute crossover sits much lower (measured: the
/// sweep wins from ~6-8 segments; below that the allocation dominates).
/// Brute-force when item count is strictly below this threshold (equivalent
/// to the historical `count * count < 32` pair-count floor).
const SWEEP_MIN_ITEMS: usize = 6;

/// Entry count from which the sweep bands its cross axis (below it the
/// plain forward window is already short), and the per-band grain the
/// band count derives from (`count / grain`, capped at 64 bands).
const SWEEP_BAND_MIN: usize = 4096;
const SWEEP_BAND_GRAIN: usize = 2048;

/// Segment count from which the MONOTONE-CHAIN strategy pays: below it
/// the flat per-segment sweep's forward windows are already short and
/// the run-join setup (clip searches, pool-indirect scans) costs more
/// than the within-chain skips save; above it the chain decomposition
/// collapses smooth linework to a few hundred envelopes.
pub(in crate::geometry) const CHAIN_MIN_SEGMENTS: usize = 1024;

/// One envelope in [`for_each_candidate_pair`]'s sweep order: extents along
/// the sweep axis (sorted on `sweep_min`) and the cross axis (overlap
/// filter), plus the input ordinal the visitor receives.
struct SweepEntry {
    sweep_min: f64,
    sweep_max: f64,
    cross_min: f64,
    cross_max: f64,
    ordinal: u32,
}

fn wider_axis_swapped(min_x: f64, max_x: f64, min_y: f64, max_y: f64) -> bool {
    max_y - min_y > max_x - min_x
}

/// Reorient entries to sweep the wider axis, then sort on `sweep_min`.
fn finish_sweep_entries(entries: &mut [SweepEntry], swapped: bool) {
    if swapped {
        for entry in entries.iter_mut() {
            std::mem::swap(&mut entry.sweep_min, &mut entry.cross_min);
            std::mem::swap(&mut entry.sweep_max, &mut entry.cross_max);
        }
    }
    entries.sort_unstable_by(|a, b| a.sweep_min.total_cmp(&b.sweep_min));
}

/// One maximal monotone chain of the input: a run of CHAINED segments
/// (each starts bit-exactly where the previous ends) whose direction
/// signs stay within one quadrant. Within a chain, non-adjacent segments
/// can never intersect (both coordinates are monotone along the run), and
/// the per-segment sweep extents are CONSECUTIVE intervals — so
/// chain-vs-chain candidate scans are linear two-pointer joins.
pub(in crate::geometry) struct MonotoneRun {
    pub(in crate::geometry) start: u32,
    pub(in crate::geometry) end: u32,
    /// Direction signs of the run's quadrant (`0` = degenerate along that
    /// axis) — axis-neutral, so runs can be staged before the sweep axis
    /// is chosen; `ascending` along an axis derives from the sign.
    pub(in crate::geometry) sign_x: i8,
    pub(in crate::geometry) sign_y: i8,
}

impl MonotoneRun {
    const fn ascending(&self, swapped: bool) -> bool {
        (if swapped { self.sign_y } else { self.sign_x }) >= 0
    }
}

/// Visit every unordered pair of axis-aligned bounds whose envelopes
/// overlap, each at most once as `visit(i, j)` with `i < j` in input
/// order. Same crossover shape as [`for_each_candidate_pair`]: brute below
/// [`SWEEP_MIN_ITEMS`], sweep-and-prune above. Used by polygon-hole and
/// multipolygon-member validity so non-overlapping parts never pay an
/// all-pairs bounds compare.
pub(in crate::geometry) fn for_each_overlapping_bounds_pair(
    bounds: &[Bounds],
    mut visit: impl FnMut(usize, usize) -> ControlFlow<()>,
) -> ControlFlow<()> {
    let count = bounds.len();
    if count < 2 {
        return ControlFlow::Continue(());
    }
    if count < SWEEP_MIN_ITEMS {
        for left in 0..count {
            for right in (left + 1)..count {
                if bounds[left].intersects(bounds[right]) {
                    visit(left, right)?;
                }
            }
        }
        return ControlFlow::Continue(());
    }
    thread_local! {
        static BOUNDS_SCRATCH: std::cell::Cell<Vec<SweepEntry>> =
            const { std::cell::Cell::new(Vec::new()) };
    }
    let mut entries = BOUNDS_SCRATCH.take();
    entries.clear();
    entries.reserve(count);
    let (mut min_x, mut max_x) = (f64::INFINITY, f64::NEG_INFINITY);
    let (mut min_y, mut max_y) = (f64::INFINITY, f64::NEG_INFINITY);
    for (ordinal, b) in bounds.iter().enumerate() {
        let (x_lo, x_hi, y_lo, y_hi) = (b.minx(), b.maxx(), b.miny(), b.maxy());
        min_x = min_x.min(x_lo);
        max_x = max_x.max(x_hi);
        min_y = min_y.min(y_lo);
        max_y = max_y.max(y_hi);
        entries.push(SweepEntry {
            sweep_min: x_lo,
            sweep_max: x_hi,
            cross_min: y_lo,
            cross_max: y_hi,
            ordinal: ordinal as u32,
        });
    }
    finish_sweep_entries(&mut entries, wider_axis_swapped(min_x, max_x, min_y, max_y));
    // Dense-enough sets take the banded windows; otherwise the plain
    // forward window (same split the segment sweep uses for large pools).
    let flow = if count >= SWEEP_BAND_MIN {
        let bands = (count / SWEEP_BAND_GRAIN).clamp(2, 64);
        let cross_lo = entries
            .iter()
            .map(|e| e.cross_min)
            .fold(f64::INFINITY, f64::min);
        let cross_hi = entries
            .iter()
            .map(|e| e.cross_max)
            .fold(f64::NEG_INFINITY, f64::max);
        let band_height = ((cross_hi - cross_lo) / bands as f64).max(f64::EPSILON);
        banded_windows(&entries, bands, cross_lo, band_height, &mut visit)
    } else {
        plain_windows(&entries, &mut visit)
    };
    BOUNDS_SCRATCH.set(entries);
    flow
}

/// Visit a superset of the pairs of one segment set that can interact
/// beyond a chained shared vertex, each unordered pair at most once, as
/// `visit(i, j)` with `i < j` in input order. Below the pair-count
/// crossover every pair is visited; above it the segments decompose into
/// MONOTONE CHAINS (the JTS design): within one chain NO pair is visited
/// — non-adjacent in-chain segments cannot touch at all, and adjacent
/// ones share exactly their one chaining vertex (monotonicity rules out
/// backtracks and collinear overlaps, which always BREAK the chain and
/// surface as chain-pair contacts). Chain pairs come from a
/// sweep-and-prune pass over the chain envelopes (banded across the
/// cross axis for dense sets), each expanding into a linear interval
/// join of the chains' consecutive sweep extents. `Break` from the
/// visitor stops the scan.
///
/// Chains follow bit-exact geometric continuity by default ([`single_chain`]);
/// when segments carry SOURCE identities whose pairings matter even at a plain
/// chained vertex (per-line simplicity rules, cross-ring touch records), pass a
/// `chain_of` that breaks runs at source boundaries so those junction pairs
/// reach the visitor.
///
/// `RUN_MIN` is the monotone-run crossover: [`CHAIN_MIN_SEGMENTS`] for the
/// broad default (the run-join setup only pays past it), [`RUN_NODING_MIN`] for
/// the noding hot paths (whose ~40 KB flat sort would otherwise exceed L1).
/// Below it the flat per-segment sweep wins; below [`SWEEP_MIN_ITEMS`] the
/// brute double loop wins. Same candidate set at every threshold — only the
/// work shape differs.
pub(in crate::geometry) fn for_each_candidate_pair<const RUN_MIN: usize>(
    pool: &[Segment],
    chain_of: impl Fn(usize) -> u32,
    mut visit: impl FnMut(usize, usize) -> ControlFlow<()>,
) -> ControlFlow<()> {
    let count = pool.len();
    if count < SWEEP_MIN_ITEMS {
        for left in 0..count {
            for right in (left + 1)..count {
                visit(left, right)?;
            }
        }
        return ControlFlow::Continue(());
    }
    if count < RUN_MIN {
        return flat_segment_sweep(pool, &mut visit);
    }
    let bounds = Bounds::from_xy_iter(pool.iter().flat_map(|segment| [segment.start, segment.end]));
    // Per-call scratch reuse: bulk predicates sweep thousands of small rows, and
    // this run buffer was their only allocation.
    thread_local! {
        static RUNS: std::cell::Cell<Vec<MonotoneRun>> = const { std::cell::Cell::new(Vec::new()) };
    }
    let mut runs = RUNS.take();
    runs.clear();
    monotone_runs(pool, &chain_of, &mut runs);
    let flow = candidate_pairs_over_runs(pool, &runs, bounds, &mut visit);
    RUNS.set(runs);
    flow
}

/// The default `chain_of` for [`for_each_candidate_pair`]: one geometric chain,
/// so runs break only on bit-exact discontinuity, not source identity.
pub(in crate::geometry) const fn single_chain(_: usize) -> u32 {
    0
}

/// The flat per-segment sweep (one envelope per segment, every
/// envelope-overlapping pair visited — within-chain adjacents included):
/// the small-pool strategy where short forward windows beat the chain
/// machinery's setup. ONE pass builds entries and global extents
/// together; the axis swap rewrites the columns in place.
pub(in crate::geometry) fn flat_segment_sweep(
    pool: &[Segment],
    visit: &mut impl FnMut(usize, usize) -> ControlFlow<()>,
) -> ControlFlow<()> {
    thread_local! {
        static FLAT_SCRATCH: std::cell::Cell<Vec<SweepEntry>> =
            const { std::cell::Cell::new(Vec::new()) };
    }
    let mut entries = FLAT_SCRATCH.take();
    entries.clear();
    entries.reserve(pool.len());
    let (mut min_x, mut max_x) = (f64::INFINITY, f64::NEG_INFINITY);
    let (mut min_y, mut max_y) = (f64::INFINITY, f64::NEG_INFINITY);
    for (ordinal, segment) in pool.iter().enumerate() {
        let (x_lo, x_hi) = (
            segment.start.x.min(segment.end.x),
            segment.start.x.max(segment.end.x),
        );
        let (y_lo, y_hi) = (
            segment.start.y.min(segment.end.y),
            segment.start.y.max(segment.end.y),
        );
        min_x = min_x.min(x_lo);
        max_x = max_x.max(x_hi);
        min_y = min_y.min(y_lo);
        max_y = max_y.max(y_hi);
        entries.push(SweepEntry {
            sweep_min: x_lo,
            sweep_max: x_hi,
            cross_min: y_lo,
            cross_max: y_hi,
            ordinal: ordinal as u32,
        });
    }
    finish_sweep_entries(&mut entries, wider_axis_swapped(min_x, max_x, min_y, max_y));
    let flow = plain_windows(&entries, visit);
    FLAT_SCRATCH.set(entries);
    flow
}

/// [`for_each_candidate_pair`] over PRE-STAGED runs and bounds (built
/// during linework staging — see `LineworkChains`): skips the per-call
/// extent and run-decomposition passes entirely. Small inputs should
/// take the brute loop upstream; this always sweeps.
pub(in crate::geometry) fn candidate_pairs_over_runs(
    pool: &[Segment],
    runs: &[MonotoneRun],
    bounds: Bounds,
    visit: &mut impl FnMut(usize, usize) -> ControlFlow<()>,
) -> ControlFlow<()> {
    let (min_x, min_y, max_x, max_y) = (bounds.minx(), bounds.miny(), bounds.maxx(), bounds.maxy());
    let swapped = wider_axis_swapped(min_x, max_x, min_y, max_y);
    thread_local! {
        static SWEEP_SCRATCH: std::cell::Cell<Vec<SweepEntry>> =
            const { std::cell::Cell::new(Vec::new()) };
    }
    let mut entries = SWEEP_SCRATCH.take();
    entries.clear();
    entries.reserve(runs.len());
    for (ordinal, run) in runs.iter().enumerate() {
        // Length-exact run window: first/last via slice pattern (elision).
        let segs = &pool[run.start as usize..run.end as usize];
        let (first, last) = match segs {
            [only] => (only.start, only.end),
            [first, .., last] => (first.start, last.end),
            [] => continue,
        };
        let (x_lo, x_hi) = (first.x.min(last.x), first.x.max(last.x));
        let (y_lo, y_hi) = (first.y.min(last.y), first.y.max(last.y));
        entries.push(SweepEntry {
            sweep_min: x_lo,
            sweep_max: x_hi,
            cross_min: y_lo,
            cross_max: y_hi,
            ordinal: ordinal as u32,
        });
    }
    finish_sweep_entries(&mut entries, swapped);
    // Large sets band the CROSS axis: a pure 1-D sweep's forward window
    // grows linearly with density, while B bands cut it by B. An envelope
    // joins every band its cross extent overlaps, and a pair is visited
    // only in the FIRST band both share.
    let (cross_lo, cross_hi) = if swapped {
        (min_x, max_x)
    } else {
        (min_y, max_y)
    };
    let bands = if runs.len() >= SWEEP_BAND_MIN {
        (runs.len() / SWEEP_BAND_GRAIN).next_power_of_two().min(64)
    } else {
        1
    };
    let band_height = (cross_hi - cross_lo) / bands as f64;
    let mut run_visit = |left: usize, right: usize| -> ControlFlow<()> {
        run_pair_join(pool, &runs[left], &runs[right], swapped, visit)
    };
    let flow = if bands > 1 && band_height > 0.0 {
        banded_windows(&entries, bands, cross_lo, band_height, &mut run_visit)
    } else {
        plain_windows(&entries, &mut run_visit)
    };
    SWEEP_SCRATCH.set(entries);
    flow
}

/// Pool size at or above which the noding hot paths route through the
/// monotone-run sweep rather than the flat `SweepEntry` sort, whose ~40 KB
/// array (at ~1 K segments) exceeds L1 and profiled at ~45% of the clean
/// overlay kernel. Below this the flat sweep's setup-light path wins. Distinct
/// from the generic [`for_each_candidate_pair`] threshold
/// (`CHAIN_MIN_SEGMENTS`, tuned for its other callers) — the noding paths opt
/// into this lower one.
pub(in crate::geometry) const RUN_NODING_MIN: usize = 512;

/// Visit every CROSS-OPERAND candidate segment pair between two operands'
/// linework — the bipartite contact-classification sweep behind
/// `overlaps`/`touches`/`crosses`/`relate`. `fill` pushes the LEFT operand's
/// `split` segments, then the RIGHT operand's, into the combined pool; distinct
/// chain ids (`index >= split`) keep every cross-operand pair visible while
/// same-operand pairs are dropped before the visitor. One combined-pool sweep —
/// no per-call distance BVH or R-tree build.
fn for_each_bipartite_candidate_pair(
    split: usize,
    fill: impl FnOnce(&mut Vec<Segment>),
    mut visit: impl FnMut(Segment, Segment) -> ControlFlow<()>,
) -> ControlFlow<()> {
    thread_local! {
        static POOL: std::cell::Cell<Vec<Segment>> = const { std::cell::Cell::new(Vec::new()) };
    }
    let mut pool = POOL.take();
    pool.clear();
    fill(&mut pool);
    debug_assert!(
        split <= pool.len(),
        "fill must push at least `split` left segments"
    );
    let flow = for_each_candidate_pair::<RUN_NODING_MIN>(
        &pool,
        |index| u32::from(index >= split),
        |left, right| {
            if left < split && right >= split {
                visit(pool[left], pool[right])
            } else {
                ControlFlow::Continue(())
            }
        },
    );
    POOL.set(pool);
    flow
}

/// Like [`for_each_bipartite_candidate_pair`], but yields the operand-local
/// segment INDICES `(a_index, b_index)` of each cross-operand candidate (left =
/// `a`, right = `b`), so callers that key per-segment scratch by index — the
/// lineal/mixed relate coverage tables — get the cheap monotone-run sweep
/// instead of a per-call R-tree build. Same candidate superset as the R-tree
/// `intersecting_candidates` path; the visitor still applies the exact contact
/// kernel.
pub(in crate::geometry) fn for_each_bipartite_index_pair(
    a: &[Segment],
    b: &[Segment],
    mut visit: impl FnMut(usize, usize),
) {
    let split = a.len();
    if split == 0 || b.is_empty() {
        return;
    }
    thread_local! {
        static POOL: std::cell::Cell<Vec<Segment>> = const { std::cell::Cell::new(Vec::new()) };
    }
    let mut pool = POOL.take();
    pool.clear();
    pool.extend_from_slice(a);
    pool.extend_from_slice(b);
    let _ = for_each_candidate_pair::<RUN_NODING_MIN>(
        &pool,
        |index| u32::from(index >= split),
        |left, right| {
            // The sweep visits unordered pairs `(i, j)` with `i < j`; the
            // bipartite filter keeps only cross-operand pairs. A cross pair has
            // exactly one endpoint below `split`, which is the `a` index.
            let (a_index, b_index) = if left < split && right >= split {
                (left, right - split)
            } else if right < split && left >= split {
                (right, left - split)
            } else {
                return ControlFlow::Continue(());
            };
            visit(a_index, b_index);
            ControlFlow::Continue(())
        },
    );
    POOL.set(pool);
}

/// A borrowed source of linework segments for the bipartite contact/cross
/// queries — the ONE abstraction over the forms segments arrive in (`Shape`
/// nested rings, `PreparedLinework`/`DistanceParts` packed columns, a raw
/// `&[Segment]`). `Copy` (always a borrow): monomorphizes per source, no `dyn`.
pub(in crate::geometry) trait SegmentSource: Copy {
    fn segment_count(self) -> usize;
    #[expect(
        clippy::impl_trait_in_params,
        reason = "visitor type is intentionally opaque at this one-pass traversal boundary"
    )]
    fn for_each_segment(self, visit: impl FnMut(Segment));
    /// Push every segment into the combined sweep pool (slices bulk-copy).
    fn fill_segments(self, pool: &mut Vec<Segment>) {
        self.for_each_segment(|segment| pool.push(segment));
    }
}

impl SegmentSource for &Shape {
    fn segment_count(self) -> usize {
        Shape::segment_count(self)
    }
    fn for_each_segment(self, visit: impl FnMut(Segment)) {
        Shape::for_each_segment(self, visit);
    }
}

impl SegmentSource for &PreparedLinework {
    fn segment_count(self) -> usize {
        PreparedLinework::segment_count(self)
    }
    fn for_each_segment(self, visit: impl FnMut(Segment)) {
        PreparedLinework::for_each_segment(self, visit);
    }
}

impl SegmentSource for &DistanceParts {
    fn segment_count(self) -> usize {
        self.linework.segment_count()
    }
    fn for_each_segment(self, visit: impl FnMut(Segment)) {
        self.linework.for_each_segment(visit);
    }
}

impl SegmentSource for &[Segment] {
    fn segment_count(self) -> usize {
        self.len()
    }
    fn for_each_segment(self, visit: impl FnMut(Segment)) {
        self.iter().copied().for_each(visit);
    }
    fn fill_segments(self, pool: &mut Vec<Segment>) {
        pool.extend_from_slice(self);
    }
}

/// Fold every cross-operand candidate segment pair through `step` — the shared
/// engine behind [`linework_contact`] and [`segments_cross`]. Both sources fill
/// the combined bipartite pool (left then right) and it is swept once.
fn segment_interaction<L, R, A>(
    left: L,
    right: R,
    mut state: A,
    mut step: impl FnMut(&mut A, Segment, Segment) -> ControlFlow<()>,
) -> A
where
    L: SegmentSource,
    R: SegmentSource,
{
    let split = left.segment_count();
    if split == 0 || right.segment_count() == 0 {
        return state;
    }
    let _ = for_each_bipartite_candidate_pair(
        split,
        |pool| {
            left.fill_segments(pool);
            right.fill_segments(pool);
        },
        |a, b| step(&mut state, a, b),
    );
    state
}

/// The STRONGEST linework contact between two operands (`Cross` > `Touch` >
/// `None`) — the one engine for the scalar `Shape` path AND the cached
/// `PreparedLinework`/`DistanceParts` path. Early-exits on the first
/// transversal `Cross`. Drives the relate-class predicate lanes.
pub(in crate::geometry) fn linework_contact<L: SegmentSource, R: SegmentSource>(
    left: L,
    right: R,
) -> SegmentContact {
    segment_interaction(left, right, SegmentContact::None, |contact, a, b| {
        *contact = (*contact).max(segment_contact(a, b));
        if *contact == SegmentContact::Cross {
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    })
}

/// Whether ANY cross-operand segment pair touches or crosses — the boolean
/// sibling of [`linework_contact`] on the SAME bipartite monotone-run sweep
/// (no per-call R-tree). `intersects` needs only this weaker fact and exits a
/// whole relate class earlier than the full contact classification.
pub(in crate::geometry) fn segments_cross<L: SegmentSource, R: SegmentSource>(
    left: L,
    right: R,
) -> bool {
    segment_interaction(left, right, false, |hit, a, b| {
        if segments_intersect(a, b) {
            *hit = true;
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    })
}

/// Decompose the input into maximal monotone runs: a run extends while
/// the next segment CHAINS (starts bit-exactly at the previous end) and
/// keeps both direction signs within the run's quadrant (zero components
/// are wildcards). Zero-length segments break out into their own runs so
/// their point-touch pairs still reach the visitor through run pairs.
pub(in crate::geometry) fn monotone_runs(
    pool: &[Segment],
    chain_of: &impl Fn(usize) -> u32,
    runs: &mut Vec<MonotoneRun>,
) {
    let mut start = 0_u32;
    let (mut sign_x, mut sign_y) = (0_i8, 0_i8);
    let close = |runs: &mut Vec<MonotoneRun>, start: u32, end: u32, sign_x: i8, sign_y: i8| {
        if end > start {
            runs.push(MonotoneRun {
                start,
                end,
                sign_x,
                sign_y,
            });
        }
    };
    for (index, segment) in pool.iter().enumerate() {
        let dx = segment.end.x - segment.start.x;
        let dy = segment.end.y - segment.start.y;
        let (sx, sy) = (sign_of(dx), sign_of(dy));
        let degenerate = sx == 0 && sy == 0;
        let chained = index as u32 > start
            && !degenerate
            && chain_of(index - 1) == chain_of(index)
            && PointKey::new(pool[index - 1].end) == PointKey::new(segment.start)
            && (sx == 0 || sign_x == 0 || sx == sign_x)
            && (sy == 0 || sign_y == 0 || sy == sign_y);
        if chained {
            if sign_x == 0 {
                sign_x = sx;
            }
            if sign_y == 0 {
                sign_y = sy;
            }
            continue;
        }
        close(runs, start, index as u32, sign_x, sign_y);
        start = index as u32;
        (sign_x, sign_y) = (sx, sy);
        if degenerate {
            close(runs, start, index as u32 + 1, 0, 0);
            start = index as u32 + 1;
            (sign_x, sign_y) = (0, 0);
        }
    }
    close(runs, start, pool.len() as u32, sign_x, sign_y);
}

pub(in crate::geometry) fn sign_of(value: f64) -> i8 {
    if value > 0.0 {
        1
    } else if value < 0.0 {
        -1
    } else {
        0
    }
}

/// Candidate pairs between two monotone runs: both runs' per-segment
/// sweep extents are consecutive sorted intervals, so the join is a
/// two-pointer interval scan — linear in the run lengths plus the
/// matches. Within the window, the cross-axis filter prunes as usual.
fn run_pair_join(
    pool: &[Segment],
    a: &MonotoneRun,
    b: &MonotoneRun,
    swapped: bool,
    visit: &mut impl FnMut(usize, usize) -> ControlFlow<()>,
) -> ControlFlow<()> {
    // Single-segment runs are the noding pools' common case (split
    // pieces interleave their sources, so chains rarely form there) —
    // and for them the entry sweep has ALREADY proven exact envelope
    // overlap on both axes, so the pair goes straight to the visitor.
    if a.end - a.start == 1 && b.end - b.start == 1 {
        let (low, high) = if a.start < b.start {
            (a.start, b.start)
        } else {
            (b.start, a.start)
        };
        return visit(low as usize, high as usize);
    }
    let sweep = |point: XY| if swapped { point.y } else { point.x };
    let cross = |point: XY| if swapped { point.x } else { point.y };
    let index_of = |run: &MonotoneRun, position: u32| -> usize {
        if run.ascending(swapped) {
            (run.start + position) as usize
        } else {
            (run.end - 1 - position) as usize
        }
    };
    // Chain-vertex sweep coordinate at ascending position `p` (0..=len):
    // a run's segment extents are the consecutive intervals [v(p), v(p+1)].
    let vertex = |run: &MonotoneRun, position: u32| -> f64 {
        let len = run.end - run.start;
        if run.ascending(swapped) {
            if position < len {
                sweep(pool[(run.start + position) as usize].start)
            } else {
                sweep(pool[run.end as usize - 1].end)
            }
        } else if position < len {
            sweep(pool[(run.end - 1 - position) as usize].end)
        } else {
            sweep(pool[run.start as usize].start)
        }
    };
    // Clip each run's scan range to the OTHER's sweep envelope by binary
    // search over the sorted chain vertices — neighbor runs that barely
    // touch cost O(log n) instead of a full linear walk.
    let clip = |run: &MonotoneRun, lo: f64, hi: f64| -> (u32, u32) {
        let len = run.end - run.start;
        let mut first = 0_u32;
        let mut count = len;
        while count > 0 {
            let half = count / 2;
            let probe = first + half;
            if vertex(run, probe + 1) < lo {
                first = probe + 1;
                count -= half + 1;
            } else {
                count = half;
            }
        }
        let mut last = first;
        let mut count = len - first;
        while count > 0 {
            let half = count / 2;
            let probe = last + half;
            if vertex(run, probe) <= hi {
                last = probe + 1;
                count -= half + 1;
            } else {
                count = half;
            }
        }
        (first, last)
    };
    let (b_env_lo, b_env_hi) = (vertex(b, 0), vertex(b, b.end - b.start));
    let (a_lo, a_hi) = clip(a, b_env_lo, b_env_hi);
    let (a_env_lo, a_env_hi) = (vertex(a, 0), vertex(a, a.end - a.start));
    let (b_lo, b_hi) = clip(b, a_env_lo, a_env_hi);
    // Both runs are ALSO cross-monotone (same quadrant), so the candidates lie in
    // the CROSS overlap too — clip each x-overlap range to it, turning the O(n)
    // band walk into O(log n + matches) when two large smooth boundaries cross at
    // few points. Conservative: the per-segment cross filter below still decides
    // exactness, so a loose clip only costs a few extra scanned segments.
    let ((a_lo, a_hi), (b_lo, b_hi)) =
        cross_clip_runs(pool, a, b, swapped, (a_lo, a_hi), (b_lo, b_hi));
    let mut b_low = b_lo;
    for a_pos in a_lo..a_hi {
        let a_idx = index_of(a, a_pos);
        let a_seg = pool[a_idx];
        let (a_min, a_max) = ordered(sweep(a_seg.start), sweep(a_seg.end));
        // Advance the window start: b extents are sorted by BOTH ends.
        while b_low < b_hi {
            let b_seg = pool[index_of(b, b_low)];
            if sweep(b_seg.start).max(sweep(b_seg.end)) < a_min {
                b_low += 1;
            } else {
                break;
            }
        }
        let (a_cross_min, a_cross_max) = ordered(cross(a_seg.start), cross(a_seg.end));
        let mut b_pos = b_low;
        while b_pos < b_hi {
            let b_idx = index_of(b, b_pos);
            let b_seg = pool[b_idx];
            if sweep(b_seg.start).min(sweep(b_seg.end)) > a_max {
                break;
            }
            let (b_cross_min, b_cross_max) = ordered(cross(b_seg.start), cross(b_seg.end));
            if b_cross_min <= a_cross_max && b_cross_max >= a_cross_min {
                let (low, high) = if a_idx < b_idx {
                    (a_idx, b_idx)
                } else {
                    (b_idx, a_idx)
                };
                visit(low, high)?;
            }
            b_pos += 1;
        }
    }
    ControlFlow::Continue(())
}

/// Tighten two cross-monotone runs' already x-clipped position ranges to their
/// mutual CROSS overlap (binary search over the sorted cross vertices). Cross
/// is monotone over each range in either direction; the key is normalized to
/// ascending. Conservative — segments whose cross-extent merely touches the
/// other's range are kept, leaving exactness to `run_pair_join`'s cross filter.
fn cross_clip_runs(
    pool: &[Segment],
    a: &MonotoneRun,
    b: &MonotoneRun,
    swapped: bool,
    (a_lo, a_hi): (u32, u32),
    (b_lo, b_hi): (u32, u32),
) -> ((u32, u32), (u32, u32)) {
    let cross = |point: XY| if swapped { point.x } else { point.y };
    let cross_vertex = |run: &MonotoneRun, position: u32| -> f64 {
        let len = run.end - run.start;
        if run.ascending(swapped) {
            if position < len {
                cross(pool[(run.start + position) as usize].start)
            } else {
                cross(pool[run.end as usize - 1].end)
            }
        } else if position < len {
            cross(pool[(run.end - 1 - position) as usize].end)
        } else {
            cross(pool[run.start as usize].start)
        }
    };
    let cross_clip = |run: &MonotoneRun, p_lo: u32, p_hi: u32, lo: f64, hi: f64| -> (u32, u32) {
        if p_hi <= p_lo + 1 {
            return (p_lo, p_hi);
        }
        let ascending = cross_vertex(run, p_hi) >= cross_vertex(run, p_lo);
        let key = |position: u32| {
            let value = cross_vertex(run, position);
            if ascending { value } else { -value }
        };
        let (klo, khi) = if ascending { (lo, hi) } else { (-hi, -lo) };
        // first = smallest p whose far cross vertex reaches `klo`.
        let mut first = p_lo;
        let mut count = p_hi - p_lo;
        while count > 0 {
            let half = count / 2;
            let probe = first + half;
            if key(probe + 1) < klo {
                first = probe + 1;
                count -= half + 1;
            } else {
                count = half;
            }
        }
        // last = one past the largest p whose near cross vertex is within `khi`.
        let mut last = first;
        let mut count = p_hi - first;
        while count > 0 {
            let half = count / 2;
            let probe = last + half;
            if key(probe) <= khi {
                last = probe + 1;
                count -= half + 1;
            } else {
                count = half;
            }
        }
        (first, last)
    };
    let (bc_lo, bc_hi) = {
        let (c0, c1) = (cross_vertex(b, b_lo), cross_vertex(b, b_hi));
        (c0.min(c1), c0.max(c1))
    };
    let (a_lo, a_hi) = cross_clip(a, a_lo, a_hi, bc_lo, bc_hi);
    let (ac_lo, ac_hi) = {
        let (c0, c1) = (cross_vertex(a, a_lo), cross_vertex(a, a_hi));
        (c0.min(c1), c0.max(c1))
    };
    let (b_lo, b_hi) = cross_clip(b, b_lo, b_hi, ac_lo, ac_hi);
    ((a_lo, a_hi), (b_lo, b_hi))
}

fn ordered(a: f64, b: f64) -> (f64, f64) {
    if a <= b { (a, b) } else { (b, a) }
}

/// The plain forward-window scan over sweep-sorted entries.
fn plain_windows(
    entries: &[SweepEntry],
    visit: &mut impl FnMut(usize, usize) -> ControlFlow<()>,
) -> ControlFlow<()> {
    for (position, left) in entries.iter().enumerate() {
        for right in &entries[(position + 1)..] {
            if right.sweep_min > left.sweep_max {
                break;
            }
            if right.cross_min <= left.cross_max && right.cross_max >= left.cross_min {
                visit_ordered(left, right, visit)?;
            }
        }
    }
    ControlFlow::Continue(())
}

/// The banded forward-window scan (see [`for_each_candidate_pair`]): CSR
/// band membership over the cross axis, a window per band, each pair
/// visited only in the first band both envelopes share.
fn banded_windows(
    entries: &[SweepEntry],
    bands: usize,
    cross_lo: f64,
    band_height: f64,
    visit: &mut impl FnMut(usize, usize) -> ControlFlow<()>,
) -> ControlFlow<()> {
    let band_of =
        |cross: f64| -> usize { (((cross - cross_lo) / band_height) as usize).min(bands - 1) };
    // Entries are already in sweep order, so every band row is too.
    let mut starts = vec![0_u32; bands + 1];
    for entry in entries {
        for band in band_of(entry.cross_min)..=band_of(entry.cross_max) {
            starts[band + 1] += 1;
        }
    }
    for band in 0..bands {
        starts[band + 1] += starts[band];
    }
    let mut cursor = starts.clone();
    let mut slots = vec![0_u32; starts[bands] as usize];
    for (position, entry) in entries.iter().enumerate() {
        for band in band_of(entry.cross_min)..=band_of(entry.cross_max) {
            slots[cursor[band] as usize] = position as u32;
            cursor[band] += 1;
        }
    }
    for band in 0..bands {
        let row = &slots[starts[band] as usize..starts[band + 1] as usize];
        for (offset, &left_slot) in row.iter().enumerate() {
            let left = &entries[left_slot as usize];
            for &right_slot in &row[(offset + 1)..] {
                let right = &entries[right_slot as usize];
                if right.sweep_min > left.sweep_max {
                    break;
                }
                if right.cross_min <= left.cross_max
                    && right.cross_max >= left.cross_min
                    && band_of(left.cross_min).max(band_of(right.cross_min)) == band
                {
                    visit_ordered(left, right, visit)?;
                }
            }
        }
    }
    ControlFlow::Continue(())
}

/// Forward one candidate pair in input-ordinal order (`low < high`).
fn visit_ordered(
    left: &SweepEntry,
    right: &SweepEntry,
    visit: &mut impl FnMut(usize, usize) -> ControlFlow<()>,
) -> ControlFlow<()> {
    let (low, high) = if left.ordinal < right.ordinal {
        (left.ordinal, right.ordinal)
    } else {
        (right.ordinal, left.ordinal)
    };
    visit(low as usize, high as usize)
}
