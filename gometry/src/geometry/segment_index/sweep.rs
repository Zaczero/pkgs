#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use std::ops::ControlFlow;

use rstar::{AABB, RTreeObject};

use crate::geometry::{Bounds, PointKey, Segment, XY};

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

struct IndexedBounds {
    ordinal: usize,
    envelope: AABB<[f64; 2]>,
}

impl RTreeObject for IndexedBounds {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        self.envelope
    }
}

fn bounds_envelope(bounds: Bounds) -> AABB<[f64; 2]> {
    AABB::from_bounds([bounds.minx(), bounds.miny()], [
        bounds.maxx(),
        bounds.maxy(),
    ])
}

/// Visit every unordered pair of inclusive-overlapping row bounds once. The
/// R-tree query is restricted by ordinal, so separated rows cost O(n log n)
/// and a dense set can stop on its first touching pair.
pub(in crate::geometry) fn for_each_indexed_overlapping_bounds_pair(
    bounds: &[Option<Bounds>],
    mut visit: impl FnMut(usize, usize) -> ControlFlow<()>,
) -> ControlFlow<()> {
    if bounds.len() < 2 {
        return ControlFlow::Continue(());
    }
    let entries = bounds
        .iter()
        .enumerate()
        .filter_map(|(ordinal, bounds)| {
            bounds.map(|bounds| IndexedBounds {
                ordinal,
                envelope: bounds_envelope(bounds),
            })
        })
        .collect::<Vec<_>>();
    let tree = crate::geometry::BulkRTree::bulk_load_with_params(entries);
    for (left, bounds) in bounds
        .iter()
        .enumerate()
        .filter_map(|(left, bounds)| bounds.map(|bounds| (left, bounds)))
    {
        for right in tree
            .locate_in_envelope_intersecting(bounds_envelope(bounds))
            .filter(|right| right.ordinal > left)
        {
            visit(left, right.ordinal)?;
        }
    }
    ControlFlow::Continue(())
}

/// One envelope in [`for_each_candidate_pair`]'s sweep order: extents along
/// the sweep axis (sorted on `sweep_min`) and the cross axis (overlap
/// filter), plus the input ordinal the visitor receives.
struct SweepEntry {
    sweep_min: f64,
    sweep_max: f64,
    cross_min: f64,
    cross_max: f64,
    ordinal: u32,
    /// Bipartite sweeps tag the two operands `1` and `2`, so a pair from one
    /// operand is `left.side & right.side != 0` and skips before any extent
    /// compare; a self-join leaves every entry at `0`.
    side: u8,
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
        static BOUNDS_SCRATCH: crate::scratch_pool::ScratchPool<Vec<SweepEntry>> = const {
            crate::scratch_pool::ScratchPool::new(Vec::new())
        };
    }
    let mut entries = crate::scratch_pool::ScratchPool::checkout(&BOUNDS_SCRATCH);
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
            side: 0,
        });
    }
    finish_sweep_entries(&mut entries, wider_axis_swapped(min_x, max_x, min_y, max_y));
    // Dense-enough sets take the banded windows; otherwise the plain
    // forward window (same split the segment sweep uses for large pools).
    if count >= SWEEP_BAND_MIN {
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
    }
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
/// The run decomposition is what prunes, so the strategy follows how far it
/// COLLAPSED the pool rather than how big the pool is: smooth linework folds
/// hundreds of segments into a couple of runs and skips every within-run pair,
/// while linework that alternates direction every segment yields one run each
/// and the two paths visit an identical candidate set. Deciding on the run
/// count therefore takes the pruning wherever it exists — a straight 254-segment
/// pair drops from 252 candidates to 0 — and pays the run machinery only when it
/// buys something. Below [`SWEEP_MIN_ITEMS`] the brute double loop wins.
pub(in crate::geometry) fn for_each_candidate_pair(
    pool: &[Segment],
    chain_of: impl Fn(usize) -> u32,
    mut visit: impl FnMut(usize, usize) -> ControlFlow<()>,
) -> ControlFlow<()> {
    // Per-call scratch reuse: bulk predicates sweep thousands of small rows, and
    // this run buffer was their only allocation.
    thread_local! {
        static RUNS: crate::scratch_pool::ScratchPool<Vec<MonotoneRun>> = const {
            crate::scratch_pool::ScratchPool::new(Vec::new())
        };
    }
    let mut runs = crate::scratch_pool::ScratchPool::checkout(&RUNS);
    monotone_runs(pool, &chain_of, &mut runs);
    candidate_pairs_over_staged_runs(
        pool,
        &runs,
        || Bounds::from_xy_iter(pool.iter().flat_map(|segment| [segment.start, segment.end])),
        &mut visit,
    )
}

/// The shared entry point behind [`for_each_candidate_pair`] and the
/// prepared-linework sweep, over runs that are already decomposed. `bounds` is
/// consulted only on the sweep path, so a caller that has to compute them can
/// defer the pass.
pub(in crate::geometry) fn candidate_pairs_over_staged_runs(
    pool: &[Segment],
    runs: &[MonotoneRun],
    bounds: impl FnOnce() -> Bounds,
    visit: &mut impl FnMut(usize, usize) -> ControlFlow<()>,
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
    candidate_pairs_over_runs(pool, runs, bounds(), visit)
}

/// The default `chain_of` for [`for_each_candidate_pair`]: one geometric chain,
/// so runs break only on bit-exact discontinuity, not source identity.
pub(in crate::geometry) const fn single_chain(_: usize) -> u32 {
    0
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
        static SWEEP_SCRATCH: crate::scratch_pool::ScratchPool<Vec<SweepEntry>> = const {
            crate::scratch_pool::ScratchPool::new(Vec::new())
        };
    }
    let mut entries = crate::scratch_pool::ScratchPool::checkout(&SWEEP_SCRATCH);
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
            side: 0,
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
    if bands > 1 && band_height > 0.0 {
        banded_windows(&entries, bands, cross_lo, band_height, &mut run_visit)
    } else {
        plain_windows(&entries, &mut run_visit)
    }
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

/// Candidate pairs between two SEPARATELY staged pools: every run of `a`
/// against every run of `b` whose envelopes overlap, `visit(a_ordinal,
/// b_ordinal)` each once, in the two pools' own index spaces. One sweep over
/// both run sets, tagged by side so same-pool pairs never reach the scan,
/// and the run join bisects across the two pools. This is what a prepared
/// operand's staged runs are swept against each row with: nothing about
/// the prepared side is rebuilt per call.
pub(in crate::geometry) fn candidate_pairs_between(
    a: (&[Segment], &[MonotoneRun]),
    b: (&[Segment], &[MonotoneRun]),
    bounds: Bounds,
    visit: &mut impl FnMut(usize, usize) -> ControlFlow<()>,
) -> ControlFlow<()> {
    let (min_x, min_y, max_x, max_y) = (bounds.minx(), bounds.miny(), bounds.maxx(), bounds.maxy());
    let swapped = wider_axis_swapped(min_x, max_x, min_y, max_y);
    thread_local! {
        static SWEEP_SCRATCH: crate::scratch_pool::ScratchPool<Vec<SweepEntry>> = const {
            crate::scratch_pool::ScratchPool::new(Vec::new())
        };
    }
    let mut entries = crate::scratch_pool::ScratchPool::checkout(&SWEEP_SCRATCH);
    entries.reserve(a.1.len() + b.1.len());
    for (side, (pool, runs)) in [(1_u8, a), (2_u8, b)] {
        for (index, run) in runs.iter().enumerate() {
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
                // Ordinal space: `a`'s runs first, then `b`'s.
                ordinal: (index + if side == 1 { 0 } else { a.1.len() }) as u32,
                side,
            });
        }
    }
    finish_sweep_entries(&mut entries, swapped);
    let (cross_lo, cross_hi) = if swapped {
        (min_x, max_x)
    } else {
        (min_y, max_y)
    };
    let total = a.1.len() + b.1.len();
    let bands = if total >= SWEEP_BAND_MIN {
        (total / SWEEP_BAND_GRAIN).next_power_of_two().min(64)
    } else {
        1
    };
    let band_height = (cross_hi - cross_lo) / bands as f64;
    let mut run_visit = |left: usize, right: usize| -> ControlFlow<()> {
        // `left < right` in ordinal space, and the sides differ, so `left` is
        // an `a` run and `right` a `b` run.
        let (a_run, b_run) = (&a.1[left], &b.1[right - a.1.len()]);
        let (a_chain, b_chain) = (
            RunChain {
                pool: a.0,
                run: a_run,
                swapped,
            },
            RunChain {
                pool: b.0,
                run: b_run,
                swapped,
            },
        );
        bisect_runs(&a_chain, 0, a_chain.len(), &b_chain, 0, b_chain.len(), visit)
    };
    if bands > 1 && band_height > 0.0 {
        banded_windows(&entries, bands, cross_lo, band_height, &mut run_visit)
    } else {
        plain_windows(&entries, &mut run_visit)
    }
}

/// Candidate pairs between two monotone runs by envelope bisection: the
/// envelope of any position range of a monotone run is the box spanned by
/// its two end vertices, so a range pair whose boxes are apart is dropped
/// whole and the longer range is halved otherwise. Two smooth chains that
/// meet at a few points cost O(k log n) — the first crossing of two
/// 1,200-vertex rings is found after a handful of halvings — where a linear
/// interval merge walked every segment in the sweep overlap. Same candidate
/// set: every pair whose envelopes overlap on both axes, each once.
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
    let a = RunChain {
        pool,
        run: a,
        swapped,
    };
    let b = RunChain {
        pool,
        run: b,
        swapped,
    };
    bisect_runs(&a, 0, a.len(), &b, 0, b.len(), visit)
}

/// One monotone run viewed by ascending sweep position, whichever way its
/// segments were stored.
struct RunChain<'a> {
    pool: &'a [Segment],
    run: &'a MonotoneRun,
    swapped: bool,
}

impl RunChain<'_> {
    fn len(&self) -> u32 {
        self.run.end - self.run.start
    }

    /// Pool ordinal of the segment at ascending position `position`.
    fn ordinal(&self, position: u32) -> usize {
        if self.run.ascending(self.swapped) {
            (self.run.start + position) as usize
        } else {
            (self.run.end - 1 - position) as usize
        }
    }

    /// Chain vertex at ascending position `position` (`0..=len`).
    fn vertex(&self, position: u32) -> XY {
        if self.run.ascending(self.swapped) {
            if position < self.len() {
                self.pool[(self.run.start + position) as usize].start
            } else {
                self.pool[self.run.end as usize - 1].end
            }
        } else if position < self.len() {
            self.pool[(self.run.end - 1 - position) as usize].end
        } else {
            self.pool[self.run.start as usize].start
        }
    }

    /// The envelope of positions `lo..hi`, from its two end vertices: a
    /// monotone run's sweep and cross coordinates both move one way.
    fn envelope(&self, lo: u32, hi: u32) -> (f64, f64, f64, f64) {
        let (first, last) = (self.vertex(lo), self.vertex(hi));
        let (sweep, cross) = if self.swapped {
            ((first.y, last.y), (first.x, last.x))
        } else {
            ((first.x, last.x), (first.y, last.y))
        };
        let (s_lo, s_hi) = ordered(sweep.0, sweep.1);
        let (c_lo, c_hi) = ordered(cross.0, cross.1);
        (s_lo, s_hi, c_lo, c_hi)
    }
}

fn ordered(a: f64, b: f64) -> (f64, f64) {
    if a <= b { (a, b) } else { (b, a) }
}

fn bisect_runs(
    a: &RunChain<'_>,
    a_lo: u32,
    a_hi: u32,
    b: &RunChain<'_>,
    b_lo: u32,
    b_hi: u32,
    visit: &mut impl FnMut(usize, usize) -> ControlFlow<()>,
) -> ControlFlow<()> {
    let (as_lo, as_hi, ac_lo, ac_hi) = a.envelope(a_lo, a_hi);
    let (bs_lo, bs_hi, bc_lo, bc_hi) = b.envelope(b_lo, b_hi);
    if as_hi < bs_lo || bs_hi < as_lo || ac_hi < bc_lo || bc_hi < ac_lo {
        return ControlFlow::Continue(());
    }
    let (a_len, b_len) = (a_hi - a_lo, b_hi - b_lo);
    if a_len == 1 && b_len == 1 {
        let (x, y) = (a.ordinal(a_lo), b.ordinal(b_lo));
        // One pool: input-ordinal order. Two pools: `a` then `b`, whatever
        // their ordinals.
        let (low, high) = if std::ptr::eq(a.pool, b.pool) && x > y {
            (y, x)
        } else {
            (x, y)
        };
        return visit(low, high);
    }
    if a_len >= b_len {
        let mid = a_lo + a_len / 2;
        bisect_runs(a, a_lo, mid, b, b_lo, b_hi, visit)?;
        bisect_runs(a, mid, a_hi, b, b_lo, b_hi, visit)
    } else {
        let mid = b_lo + b_len / 2;
        bisect_runs(a, a_lo, a_hi, b, b_lo, mid, visit)?;
        bisect_runs(a, a_lo, a_hi, b, mid, b_hi, visit)
    }
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
            if left.side & right.side == 0
                && right.cross_min <= left.cross_max
                && right.cross_max >= left.cross_min
            {
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
                if left.side & right.side == 0
                    && right.cross_min <= left.cross_max
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
