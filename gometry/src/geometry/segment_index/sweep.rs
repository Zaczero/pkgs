#![expect(
    clippy::similar_names,
    reason = "axis and formula symbols name paired quantities here (minx/miny, xs/ys, lat_rad/lam_rad, step/stop), which this lint reads as typos"
)]
use std::ops::ControlFlow;
#[cfg(test)]
use std::{cell::Cell, thread_local};

use crate::error::{ResourceError, Result};
use crate::geometry::segments::ordering::try_sort_by_with_work;
use crate::geometry::{Bounds, PointKey, Segment, XY, segment_envelopes_disjoint};
use crate::work::{Unmetered, Work};

fn u32_ordinal(value: usize) -> Result<u32> {
    u32::try_from(value).map_err(|_| ResourceError::SizeOverflow.into())
}

/// Below this many candidate *pairs* the brute-force double loop wins: the
/// index build (sort + tree nodes) costs more than it saves.
pub(crate) const SEGMENT_INDEX_MIN_PAIRS: usize = 4096;

/// The sweep's build is just an envelope `Vec` and one sort — far cheaper
/// than an R-tree — so its brute crossover sits much lower (measured: the
/// sweep wins from ~6-8 segments; below that the allocation dominates).
/// Brute-force when item count is strictly below this threshold (equivalent
/// to the historical `count * count < 32` pair-count floor).
const SWEEP_MIN_ITEMS: usize = 6;
const CROSS_LEFT_SIDE: u8 = 1;
const CROSS_RIGHT_SIDE: u8 = 2;

#[cfg(test)]
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::geometry) struct CrossPairProbeProgress {
    pub(in crate::geometry) attempted: usize,
    pub(in crate::geometry) completed: usize,
}

#[cfg(test)]
thread_local! {
    static CROSS_PAIR_PROBE_PROGRESS: Cell<CrossPairProbeProgress> =
        const { Cell::new(CrossPairProbeProgress { attempted: 0, completed: 0 }) };
}

#[cfg(test)]
pub(in crate::geometry) fn reset_cross_pair_probe_progress() {
    CROSS_PAIR_PROBE_PROGRESS.with(|progress| {
        progress.set(CrossPairProbeProgress::default());
    });
}

#[cfg(test)]
pub(in crate::geometry) fn cross_pair_probe_progress() -> CrossPairProbeProgress {
    CROSS_PAIR_PROBE_PROGRESS.with(Cell::get)
}

#[cfg(test)]
fn record_cross_pair_probe_attempt() {
    CROSS_PAIR_PROBE_PROGRESS.with(|progress| {
        let mut value = progress.get();
        value.attempted = value.attempted.wrapping_add(1);
        progress.set(value);
    });
}

#[cfg(test)]
fn record_cross_pair_probe_complete() {
    CROSS_PAIR_PROBE_PROGRESS.with(|progress| {
        let mut value = progress.get();
        value.completed = value.completed.wrapping_add(1);
        progress.set(value);
    });
}

/// Entry count from which the sweep bands its cross axis (below it the
/// plain forward window is already short), and the per-band grain the
/// band count derives from (`count / grain`, capped at 64 bands).
const SWEEP_BAND_MIN: usize = 4096;
const SWEEP_BAND_GRAIN: usize = 2048;
// A projection is a geometry-wide discriminator only for a prevalent run
// direction, not for one incidental diagonal.
const OBLIQUE_PROJECTION_MIN_RUNS: usize = 64;
const OBLIQUE_PROJECTION_MIN_COHERENCE: f64 = 0.9;

/// [`for_each_overlapping_bounds_pair`] over rows that may lack bounds: a row
/// without bounds meets nothing, and the visitor receives row ordinals.
pub(in crate::geometry) fn for_each_indexed_overlapping_bounds_pair<B>(
    bounds: &[Option<Bounds>],
    mut visit: impl FnMut(usize, usize) -> ControlFlow<B>,
) -> Result<ControlFlow<B>> {
    for_each_indexed_overlapping_bounds_pair_with_work(
        bounds,
        &mut Unmetered,
        |left, right, _work| Ok(visit(left, right)),
    )
}

#[expect(
    clippy::indexing_slicing,
    reason = "the compact bounds slice and ordinal column are built one-for-one, and the sweep only yields in-range compact positions"
)]
pub(in crate::geometry) fn for_each_indexed_overlapping_bounds_pair_with_work<B, W: Work>(
    bounds: &[Option<Bounds>],
    work: &mut W,
    mut visit: impl FnMut(usize, usize, &mut W) -> Result<ControlFlow<B>>,
) -> Result<ControlFlow<B>> {
    // The compact ordinal and bounds columns each request one slot per input.
    work.charge_product_into_error(bounds.len(), 2)?;
    let mut ordinals = Vec::with_capacity(bounds.len());
    let mut present = Vec::with_capacity(bounds.len());
    for (ordinal, bounds) in bounds.iter().enumerate() {
        work.charge_into_error(1)?;
        if let Some(bounds) = bounds {
            // Publish one ordinal and one copied bounds record.
            work.charge_into_error(2)?;
            ordinals.push(ordinal);
            present.push(*bounds);
        }
    }
    for_each_overlapping_bounds_pair_with_work(&present, work, |left, right, work| {
        visit(ordinals[left], ordinals[right], work)
    })
}

/// One envelope in [`for_each_candidate_pair`]'s sweep order: extents along
/// the primary sweep axis (sorted on `sweep_min`) and the secondary interval
/// (the XY cross axis or an oblique projection), plus the input ordinal.
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

#[derive(Clone, Copy)]
struct ObliqueProjection {
    x: f64,
    y: f64,
}

thread_local! {
    static SWEEP_ENTRY_SCRATCH: crate::scratch_pool::ScratchPool<Vec<SweepEntry>> = const {
        crate::scratch_pool::ScratchPool::new(Vec::new())
    };
}

fn wider_axis_swapped(min_x: f64, max_x: f64, min_y: f64, max_y: f64) -> bool {
    max_y - min_y > max_x - min_x
}

fn projected_point_interval(point: XY, projection: ObliqueProjection) -> Option<(f64, f64)> {
    if !point.x.is_finite() || !point.y.is_finite() {
        return None;
    }
    let (x, y) = (projection.x * point.x, projection.y * point.y);
    if !x.is_finite() || !y.is_finite() {
        return None;
    }
    let lower = x.next_down() + y.next_down();
    let upper = x.next_up() + y.next_up();
    if lower.is_nan() || upper.is_nan() {
        return None;
    }
    let (lower, upper) = (lower.next_down(), upper.next_up());
    (lower <= upper).then_some((lower, upper))
}

fn run_oblique_bounds_with_work<W: Work>(
    pool: &[Segment],
    run: &MonotoneRun,
    projection: ObliqueProjection,
    work: &mut W,
) -> Result<(f64, f64)> {
    let full = (f64::NEG_INFINITY, f64::INFINITY);
    let Some(segments) = pool.get(run.start as usize..run.end as usize) else {
        return Ok(full);
    };
    let (mut min, mut max) = (f64::INFINITY, f64::NEG_INFINITY);
    for segment in segments {
        work.charge_into_error(1)?;
        let Some((lo, hi)) = projected_point_interval(segment.start, projection) else {
            return Ok(full);
        };
        min = min.min(lo);
        max = max.max(hi);
    }
    let Some(last) = segments.last() else {
        return Ok(full);
    };
    work.charge_into_error(1)?;
    let Some((lo, hi)) = projected_point_interval(last.end, projection) else {
        return Ok(full);
    };
    Ok((min.min(lo), max.max(hi)))
}

fn monotone_runs_oblique_projection(
    runs: &[MonotoneRun],
    double_angle_sum: (f64, f64),
    oblique_count: usize,
) -> Option<ObliqueProjection> {
    if runs.len() < OBLIQUE_PROJECTION_MIN_RUNS
        || oblique_count < OBLIQUE_PROJECTION_MIN_RUNS
        || (double_angle_sum.0 == 0.0 && double_angle_sum.1 == 0.0)
    {
        return None;
    }
    let strength =
        (double_angle_sum.0 * double_angle_sum.0 + double_angle_sum.1 * double_angle_sum.1).sqrt();
    let coherence = strength / oblique_count as f64;
    if coherence < OBLIQUE_PROJECTION_MIN_COHERENCE {
        return None;
    }
    let cos_double = (double_angle_sum.0 / strength).clamp(-1.0, 1.0);
    let direction_x = f64::midpoint(1.0, cos_double).sqrt();
    let direction_y = f64::midpoint(1.0, -cos_double)
        .sqrt()
        .copysign(double_angle_sum.1);
    let projection = ObliqueProjection {
        x: direction_y,
        y: -direction_x,
    };
    // An axis-aligned normal adds no information beyond the XY envelope.
    if projection.x == 0.0 || projection.y == 0.0 {
        return None;
    }
    Some(projection)
}

/// Reorient entries to sweep the wider axis, then sort on `sweep_min`.
fn finish_sweep_entries_with_work<W: Work>(
    entries: &mut [SweepEntry],
    swapped: bool,
    work: &mut W,
) -> Result<()> {
    if swapped {
        work.charge_product_into_error(entries.len(), 1)?;
        for entry in entries.iter_mut() {
            std::mem::swap(&mut entry.sweep_min, &mut entry.cross_min);
            std::mem::swap(&mut entry.sweep_max, &mut entry.cross_max);
        }
    }
    // Equal primary keys retain staging order: source ordinals for self joins,
    // and left rows followed by right rows for cross joins.
    try_sort_by_with_work(entries, work, |a, b, _work| {
        Ok(a.sweep_min.total_cmp(&b.sweep_min))
    })
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

    const fn len(&self) -> u32 {
        self.end.wrapping_sub(self.start)
    }
}

/// Visit every unordered pair of axis-aligned bounds whose envelopes
/// overlap, each at most once as `visit(i, j)` with `i < j` in input
/// order. Same crossover shape as [`for_each_candidate_pair`]: brute below
/// [`SWEEP_MIN_ITEMS`], sweep-and-prune above. Used by polygon-hole and
/// multipolygon-member validity so non-overlapping parts never pay an
/// all-pairs bounds compare.
pub(in crate::geometry) fn for_each_overlapping_bounds_pair<B>(
    bounds: &[Bounds],
    mut visit: impl FnMut(usize, usize) -> ControlFlow<B>,
) -> Result<ControlFlow<B>> {
    for_each_overlapping_bounds_pair_with_work(bounds, &mut Unmetered, |left, right, _work| {
        Ok(visit(left, right))
    })
}

pub(in crate::geometry) fn for_each_overlapping_bounds_pair_with_work<B, W: Work>(
    bounds: &[Bounds],
    work: &mut W,
    mut visit: impl FnMut(usize, usize, &mut W) -> Result<ControlFlow<B>>,
) -> Result<ControlFlow<B>> {
    let count = bounds.len();
    if count < 2 {
        return Ok(ControlFlow::Continue(()));
    }
    if count < SWEEP_MIN_ITEMS {
        let mut remaining = bounds;
        let mut left_index = 0_usize;
        while let Some((left, right_bounds)) = remaining.split_first() {
            let mut right_index = left_index.wrapping_add(1);
            for right in right_bounds {
                work.charge_into_error(1)?;
                if left.intersects(*right) {
                    match visit(left_index, right_index, work)? {
                        ControlFlow::Continue(()) => {},
                        ControlFlow::Break(value) => return Ok(ControlFlow::Break(value)),
                    }
                }
                right_index = right_index.wrapping_add(1);
            }
            left_index = left_index.wrapping_add(1);
            remaining = right_bounds;
        }
        return Ok(ControlFlow::Continue(()));
    }
    try_overlapping_bounds_sweep(bounds, work, &mut visit)
}

/// Visit every overlapping pair across two bounds sets, preserving each
/// operand's own ordinal even when the sweep interleaves their entries.
pub(in crate::geometry) fn for_each_overlapping_bounds_cross_pair<B>(
    left: &[Bounds],
    right: &[Bounds],
    mut visit: impl FnMut(usize, usize) -> ControlFlow<B>,
) -> Result<ControlFlow<B>> {
    for_each_overlapping_bounds_cross_pair_with_work(
        left,
        right,
        &mut Unmetered,
        |left, right, _work| Ok(visit(left, right)),
    )
}

pub(in crate::geometry) fn for_each_overlapping_bounds_cross_pair_with_work<B, W: Work>(
    left: &[Bounds],
    right: &[Bounds],
    work: &mut W,
    mut visit: impl FnMut(usize, usize, &mut W) -> Result<ControlFlow<B>>,
) -> Result<ControlFlow<B>> {
    let (Some(left_last), Some(right_last)) =
        (left.len().checked_sub(1), right.len().checked_sub(1))
    else {
        return Ok(ControlFlow::Continue(()));
    };
    u32_ordinal(left_last)?;
    u32_ordinal(right_last)?;
    let count = left
        .len()
        .checked_add(right.len())
        .ok_or(ResourceError::SizeOverflow)?;
    if count < SWEEP_MIN_ITEMS {
        for (left_index, left_bounds) in left.iter().enumerate() {
            for (right_index, right_bounds) in right.iter().enumerate() {
                #[cfg(test)]
                record_cross_pair_probe_attempt();
                work.charge_into_error(1)?;
                let intersects = left_bounds.intersects(*right_bounds);
                #[cfg(test)]
                record_cross_pair_probe_complete();
                if intersects {
                    match visit(left_index, right_index, work)? {
                        ControlFlow::Continue(()) => {},
                        ControlFlow::Break(value) => return Ok(ControlFlow::Break(value)),
                    }
                }
            }
        }
        return Ok(ControlFlow::Continue(()));
    }
    try_overlapping_bounds_cross_sweep(left, right, count, work, &mut visit)
}

fn try_overlapping_bounds_sweep<B, W: Work>(
    bounds: &[Bounds],
    work: &mut W,
    visit: &mut impl FnMut(usize, usize, &mut W) -> Result<ControlFlow<B>>,
) -> Result<ControlFlow<B>> {
    let count = bounds.len();
    if let Some(last) = count.checked_sub(1) {
        u32_ordinal(last)?;
    }
    let mut entries = crate::scratch_pool::ScratchPool::checkout(&SWEEP_ENTRY_SCRATCH);
    // Reserve the logical entry row before touching retained scratch storage.
    work.charge_product_into_error(count, 1)?;
    entries.reserve(count);
    let (mut min_x, mut max_x) = (f64::INFINITY, f64::NEG_INFINITY);
    let (mut min_y, mut max_y) = (f64::INFINITY, f64::NEG_INFINITY);
    for (ordinal, b) in bounds.iter().enumerate() {
        // Read the bounds, fold their extents, and publish one entry.
        work.charge_into_error(3)?;
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
    finish_sweep_entries_with_work(
        &mut entries,
        wider_axis_swapped(min_x, max_x, min_y, max_y),
        work,
    )?;
    // Dense-enough sets take the banded windows; otherwise the plain
    // forward window (same split the segment sweep uses for large pools).
    if count >= SWEEP_BAND_MIN {
        let bands = (count / SWEEP_BAND_GRAIN).clamp(2, 64);
        work.charge_product_into_error(entries.len(), 2)?;
        let cross_lo = entries
            .iter()
            .map(|e| e.cross_min)
            .fold(f64::INFINITY, f64::min);
        let cross_hi = entries
            .iter()
            .map(|e| e.cross_max)
            .fold(f64::NEG_INFINITY, f64::max);
        let band_height = ((cross_hi - cross_lo) / bands as f64).max(f64::EPSILON);
        banded_windows(&entries, bands, cross_lo, band_height, work, visit)
    } else {
        plain_windows(&entries, work, visit)
    }
}

fn try_overlapping_bounds_cross_sweep<B, W: Work>(
    left: &[Bounds],
    right: &[Bounds],
    count: usize,
    work: &mut W,
    visit: &mut impl FnMut(usize, usize, &mut W) -> Result<ControlFlow<B>>,
) -> Result<ControlFlow<B>> {
    let mut entries = crate::scratch_pool::ScratchPool::checkout(&SWEEP_ENTRY_SCRATCH);
    // Reserve the combined logical entry rows before touching retained scratch.
    work.charge_product_into_error(count, 1)?;
    entries.reserve(count);
    let (mut min_x, mut max_x) = (f64::INFINITY, f64::NEG_INFINITY);
    let (mut min_y, mut max_y) = (f64::INFINITY, f64::NEG_INFINITY);
    for (side, bounds) in [(CROSS_LEFT_SIDE, left), (CROSS_RIGHT_SIDE, right)] {
        for (ordinal, bound) in bounds.iter().enumerate() {
            // Read the bounds, fold their extents, and publish one entry.
            work.charge_into_error(3)?;
            let (x_lo, x_hi, y_lo, y_hi) = (bound.minx(), bound.maxx(), bound.miny(), bound.maxy());
            min_x = min_x.min(x_lo);
            max_x = max_x.max(x_hi);
            min_y = min_y.min(y_lo);
            max_y = max_y.max(y_hi);
            entries.push(SweepEntry {
                sweep_min: x_lo,
                sweep_max: x_hi,
                cross_min: y_lo,
                cross_max: y_hi,
                ordinal: u32_ordinal(ordinal)?,
                side,
            });
        }
    }
    finish_sweep_entries_with_work(
        &mut entries,
        wider_axis_swapped(min_x, max_x, min_y, max_y),
        work,
    )?;
    let mut cross_visit = |left: usize, right: usize, work: &mut W| visit(left, right, work);
    if count >= SWEEP_BAND_MIN {
        let bands = (count / SWEEP_BAND_GRAIN).clamp(2, 64);
        work.charge_product_into_error(entries.len(), 2)?;
        let cross_lo = entries
            .iter()
            .map(|entry| entry.cross_min)
            .fold(f64::INFINITY, f64::min);
        let cross_hi = entries
            .iter()
            .map(|entry| entry.cross_max)
            .fold(f64::NEG_INFINITY, f64::max);
        let band_height = ((cross_hi - cross_lo) / bands as f64).max(f64::EPSILON);
        banded_windows(
            &entries,
            bands,
            cross_lo,
            band_height,
            work,
            &mut cross_visit,
        )
    } else {
        plain_windows(&entries, work, &mut cross_visit)
    }
}

/// Visit a superset of the pairs of one segment set that can interact
/// beyond a chained shared vertex, each unordered pair at most once, as
/// `visit(i, j)` with `i < j` in input order. Below the pair-count
/// crossover every pair is visited; above it the segments decompose into
/// Monotone chains: within one chain NO pair is visited — non-adjacent
/// in-chain segments cannot touch at all, and adjacent ones share exactly
/// their one chaining vertex (monotonicity rules out
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
/// Unrepresentable ordinals and band-CSR sizes are [`ResourceError`]; visit
/// `Break` is `Ok(ControlFlow::Break(value))`.
pub(in crate::geometry) fn for_each_candidate_pair<B>(
    pool: &[Segment],
    chain_of: impl Fn(usize) -> u32,
    mut visit: impl FnMut(usize, usize) -> ControlFlow<B>,
) -> Result<ControlFlow<B>> {
    for_each_candidate_pair_with_work(pool, chain_of, &mut Unmetered, |left, right, _work| {
        Ok(visit(left, right))
    })
}

pub(in crate::geometry) fn for_each_candidate_pair_with_work<B, W: Work>(
    pool: &[Segment],
    chain_of: impl Fn(usize) -> u32,
    work: &mut W,
    mut visit: impl FnMut(usize, usize, &mut W) -> Result<ControlFlow<B>>,
) -> Result<ControlFlow<B>> {
    // Per-call scratch reuse: bulk predicates sweep thousands of small rows, and
    // this run buffer was their only allocation.
    thread_local! {
        static RUNS: crate::scratch_pool::ScratchPool<Vec<MonotoneRun>> = const {
            crate::scratch_pool::ScratchPool::new(Vec::new())
        };
    }
    let mut runs = crate::scratch_pool::ScratchPool::checkout(&RUNS);
    monotone_runs_with_work(pool, &chain_of, &mut runs, work)?;
    candidate_pairs_over_staged_runs_with_work::<false, _, _>(pool, &runs, work, &mut visit)
}

/// The shared entry point behind [`for_each_candidate_pair`] and the
/// prepared-linework sweep, over runs that are already decomposed.
pub(in crate::geometry) fn candidate_pairs_over_staged_runs<const OBLIQUE: bool, B>(
    pool: &[Segment],
    runs: &[MonotoneRun],
    mut visit: impl FnMut(usize, usize) -> ControlFlow<B>,
) -> Result<ControlFlow<B>> {
    candidate_pairs_over_staged_runs_with_work::<OBLIQUE, _, _>(
        pool,
        runs,
        &mut Unmetered,
        &mut |left, right, _work| Ok(visit(left, right)),
    )
}

pub(in crate::geometry) fn candidate_pairs_over_staged_runs_with_work<
    const OBLIQUE: bool,
    B,
    W: Work,
>(
    pool: &[Segment],
    runs: &[MonotoneRun],
    work: &mut W,
    visit: &mut impl FnMut(usize, usize, &mut W) -> Result<ControlFlow<B>>,
) -> Result<ControlFlow<B>> {
    let count = pool.len();
    if count < SWEEP_MIN_ITEMS {
        let mut remaining = pool;
        let mut left = 0_usize;
        while let Some((_segment, right_segments)) = remaining.split_first() {
            let mut right = left.wrapping_add(1);
            for _ in right_segments {
                work.charge_into_error(1)?;
                match visit(left, right, work)? {
                    ControlFlow::Continue(()) => {},
                    ControlFlow::Break(value) => return Ok(ControlFlow::Break(value)),
                }
                right = right.wrapping_add(1);
            }
            left = left.wrapping_add(1);
            remaining = right_segments;
        }
        return Ok(ControlFlow::Continue(()));
    }
    try_candidate_pairs_over_runs::<OBLIQUE, _, _>(pool, runs, work, visit)
}

/// The default `chain_of` for [`for_each_candidate_pair`]: one geometric chain,
/// so runs break only on bit-exact discontinuity, not source identity.
pub(in crate::geometry) const fn single_chain(_: usize) -> u32 {
    0
}

#[expect(
    clippy::indexing_slicing,
    reason = "run windows and entry ordinals are generated from these exact u32-admitted slices"
)]
fn try_candidate_pairs_over_runs<const OBLIQUE: bool, B, W: Work>(
    pool: &[Segment],
    runs: &[MonotoneRun],
    work: &mut W,
    visit: &mut impl FnMut(usize, usize, &mut W) -> Result<ControlFlow<B>>,
) -> Result<ControlFlow<B>> {
    let mut entries = crate::scratch_pool::ScratchPool::checkout(&SWEEP_ENTRY_SCRATCH);
    // Reserve one logical envelope row per staged run.
    work.charge_product_into_error(runs.len(), 1)?;
    entries.reserve(runs.len());
    let (mut min_x, mut min_y) = (f64::INFINITY, f64::INFINITY);
    let (mut max_x, mut max_y) = (f64::NEG_INFINITY, f64::NEG_INFINITY);
    let (mut double_angle_x, mut double_angle_y) = (0.0_f64, 0.0_f64);
    let mut oblique_count = 0_usize;
    for (ordinal, run) in runs.iter().enumerate() {
        work.charge_into_error(1)?;
        // Length-exact run window: first/last via slice pattern (elision).
        let segs = &pool[run.start as usize..run.end as usize];
        let (first, last) = match segs {
            [only] => (only.start, only.end),
            [first, .., last] => (first.start, last.end),
            [] => continue,
        };
        let (x_lo, x_hi) = (first.x.min(last.x), first.x.max(last.x));
        let (y_lo, y_hi) = (first.y.min(last.y), first.y.max(last.y));
        work.charge_into_error(2)?;
        min_x = min_x.min(x_lo);
        min_y = min_y.min(y_lo);
        max_x = max_x.max(x_hi);
        max_y = max_y.max(y_hi);
        if OBLIQUE {
            work.charge_into_error(1)?;
            let (dx, dy) = (last.x - first.x, last.y - first.y);
            if run.sign_x != 0
                && run.sign_y != 0
                && dx.is_finite()
                && dy.is_finite()
                && let scale = dx.abs().max(dy.abs())
                && scale > 0.0
            {
                work.charge_into_error(1)?;
                let (dx, dy) = (dx / scale, dy / scale);
                let norm_squared = dx * dx + dy * dy;
                double_angle_x += (dx * dx - dy * dy) / norm_squared;
                double_angle_y += (2.0 * dx * dy) / norm_squared;
                oblique_count = oblique_count.wrapping_add(1);
            }
        }
        entries.push(SweepEntry {
            sweep_min: x_lo,
            sweep_max: x_hi,
            cross_min: y_lo,
            cross_max: y_hi,
            ordinal: u32_ordinal(ordinal)?,
            side: 0,
        });
    }
    let projection = if OBLIQUE && runs.len() < SWEEP_BAND_MIN {
        monotone_runs_oblique_projection(runs, (double_angle_x, double_angle_y), oblique_count)
    } else {
        None
    };
    // Keep the XY primary axis and reuse the plain sweep's cross interval for
    // the oblique coordinate. Run refinement still checks both XY envelopes.
    let swapped = wider_axis_swapped(min_x, max_x, min_y, max_y);
    finish_sweep_entries_with_work(&mut entries, swapped, work)?;
    if let Some(projection) = projection {
        // An uncertain projection becomes an unbounded interval, so only a
        // certified gap between run envelopes can prune the XY sweep.
        for entry in entries.iter_mut() {
            work.charge_into_error(1)?;
            let run = &runs[entry.ordinal as usize];
            (entry.cross_min, entry.cross_max) =
                run_oblique_bounds_with_work(pool, run, projection, work)?;
        }
    }
    // Each run is monotone on both axes, so its endpoint envelope contains
    // every staged vertex. Fold those same envelopes for the global sweep
    // extents instead of rescanning all source coordinates.
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
    let projection_active = projection.is_some();
    let mut run_visit = |left: usize, right: usize, work: &mut W| {
        run_pair_join::<OBLIQUE, _, _>(
            pool,
            &runs[left],
            &runs[right],
            swapped,
            projection_active,
            work,
            visit,
        )
    };
    if projection.is_some() {
        plain_windows(&entries, work, &mut run_visit)
    } else if bands > 1 && band_height > 0.0 {
        banded_windows(&entries, bands, cross_lo, band_height, work, &mut run_visit)
    } else {
        plain_windows(&entries, work, &mut run_visit)
    }
}

/// Decompose the input into maximal monotone runs: a run extends while
/// the next segment CHAINS (starts bit-exactly at the previous end) and
/// keeps both direction signs within the run's quadrant (zero components
/// are wildcards). Zero-length segments break out into their own runs so
/// their point-touch pairs still reach the visitor through run pairs.
pub(in crate::geometry) fn monotone_runs_with_work<W: Work>(
    pool: &[Segment],
    chain_of: &impl Fn(usize) -> u32,
    runs: &mut Vec<MonotoneRun>,
    work: &mut W,
) -> Result<()> {
    let len = u32_ordinal(pool.len())?;
    let mut start = 0_u32;
    let (mut sign_x, mut sign_y) = (0_i8, 0_i8);
    let mut previous_segment = None;
    for (index, segment) in pool.iter().enumerate() {
        work.charge_into_error(1)?;
        let index_u32 = index as u32;
        let dx = segment.end.x - segment.start.x;
        let dy = segment.end.y - segment.start.y;
        let (sx, sy) = (sign_of(dx), sign_of(dy));
        let degenerate = sx == 0 && sy == 0;
        let previous = previous_segment;
        previous_segment = Some((index, segment));
        let chained = (if index_u32 > start && !degenerate {
            if let Some((previous_index, previous_segment)) = previous {
                work.charge_into_error(1)?;
                let previous_chain = chain_of(previous_index);
                work.charge_into_error(1)?;
                previous_chain == chain_of(index)
                    && PointKey::new(previous_segment.end) == PointKey::new(segment.start)
            } else {
                false
            }
        } else {
            false
        }) && (sx == 0 || sign_x == 0 || sx == sign_x)
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
        push_run_with_work(runs, start, index_u32, sign_x, sign_y, work)?;
        start = index_u32;
        (sign_x, sign_y) = (sx, sy);
        if degenerate {
            let end = index_u32.wrapping_add(1);
            push_run_with_work(runs, start, end, 0, 0, work)?;
            start = end;
            (sign_x, sign_y) = (0, 0);
        }
    }
    push_run_with_work(runs, start, len, sign_x, sign_y, work)?;
    Ok(())
}

fn push_run_with_work<W: Work>(
    runs: &mut Vec<MonotoneRun>,
    start: u32,
    end: u32,
    sign_x: i8,
    sign_y: i8,
    work: &mut W,
) -> Result<()> {
    if end > start {
        if runs.len() == runs.capacity() {
            let requested = runs.len().wrapping_add(1);
            work.charge_product_into_error(requested, 1)?;
            work.charge_product_into_error(runs.len(), 1)?;
        }
        runs.reserve(1);
        work.charge_into_error(1)?;
        runs.push(MonotoneRun {
            start,
            end,
            sign_x,
            sign_y,
        });
    }
    Ok(())
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

/// Candidate pairs between two monotone runs by envelope bisection: the
/// envelope of any position range of a monotone run is the box spanned by
/// its two end vertices, so a range pair whose boxes are apart is dropped
/// whole and the longer range is halved otherwise. Two smooth chains that
/// meet at a few points cost O(k log n) — the first crossing of two
/// 1,200-vertex rings is found after a handful of halvings — where a linear
/// interval merge walked every segment in the sweep overlap. Same candidate
/// set: every pair whose envelopes overlap on both axes, each once.
fn run_pair_join<const OBLIQUE: bool, B, W: Work>(
    pool: &[Segment],
    a: &MonotoneRun,
    b: &MonotoneRun,
    swapped: bool,
    projection_active: bool,
    work: &mut W,
    visit: &mut impl FnMut(usize, usize, &mut W) -> Result<ControlFlow<B>>,
) -> Result<ControlFlow<B>> {
    // Single-segment runs are the noding pools' common case (split
    // pieces interleave their sources, so chains rarely form there) —
    // For an axis-aligned sweep the entry window already proves envelope
    // overlap on both axes. With a projected secondary interval, only the
    // primary axis was checked, so verify the XY envelopes below.
    if a.len() == 1 && b.len() == 1 {
        work.charge_into_error(1)?;
        if OBLIQUE
            && projection_active
            && pool
                .get(a.start as usize)
                .zip(pool.get(b.start as usize))
                .is_some_and(|(left, right)| segment_envelopes_disjoint(*left, *right))
        {
            return Ok(ControlFlow::Continue(()));
        }
        let (low, high) = if a.start < b.start {
            (a.start, b.start)
        } else {
            (b.start, a.start)
        };
        return visit(low as usize, high as usize, work);
    }
    let (a, b) = (
        RunChain {
            pool,
            run: a,
            swapped,
        },
        RunChain {
            pool,
            run: b,
            swapped,
        },
    );
    bisect_runs(&a, 0, a.len(), &b, 0, b.len(), work, visit)
}

/// One monotone run viewed by ascending sweep position, whichever way its
/// segments were stored.
struct RunChain<'a> {
    pool: &'a [Segment],
    run: &'a MonotoneRun,
    swapped: bool,
}

impl RunChain<'_> {
    const fn len(&self) -> u32 {
        self.run.len()
    }

    /// Pool ordinal of the segment at ascending position `position`.
    const fn ordinal(&self, position: u32) -> usize {
        if self.run.ascending(self.swapped) {
            self.run.start.wrapping_add(position) as usize
        } else {
            self.run.end.wrapping_sub(1).wrapping_sub(position) as usize
        }
    }

    /// Chain vertex at ascending position `position` (`0..=len`).
    #[expect(
        clippy::indexing_slicing,
        reason = "the run and bisected vertex position are bounded by this exact pool slice"
    )]
    const fn vertex(&self, position: u32) -> XY {
        if self.run.ascending(self.swapped) {
            if position < self.len() {
                self.pool[self.run.start.wrapping_add(position) as usize].start
            } else {
                self.pool[self.run.end.wrapping_sub(1) as usize].end
            }
        } else if position < self.len() {
            self.pool[self.run.end.wrapping_sub(1).wrapping_sub(position) as usize].end
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

    /// After the parent envelopes overlap, a two-segment range can still
    /// miss both child boxes. Their remaining bounds are the shared vertex;
    /// the parent comparison already settled the outer bounds.
    fn two_segment_gap(&self, lo: u32, (s_lo, s_hi, c_lo, c_hi): (f64, f64, f64, f64)) -> bool {
        let middle = self.vertex(lo.wrapping_add(1));
        let (s_middle, c_middle) = if self.swapped {
            (middle.y, middle.x)
        } else {
            (middle.x, middle.y)
        };
        if (self.run.sign_x < 0) == (self.run.sign_y < 0) {
            (s_middle < s_lo || c_middle < c_lo) && (s_hi < s_middle || c_hi < c_middle)
        } else {
            (s_middle < s_lo || c_hi < c_middle) && (s_hi < s_middle || c_middle < c_lo)
        }
    }
}

fn ordered(a: f64, b: f64) -> (f64, f64) {
    if a <= b { (a, b) } else { (b, a) }
}

fn bisect_runs<B, W: Work>(
    a: &RunChain<'_>,
    a_lo: u32,
    a_hi: u32,
    b: &RunChain<'_>,
    b_lo: u32,
    b_hi: u32,
    work: &mut W,
    visit: &mut impl FnMut(usize, usize, &mut W) -> Result<ControlFlow<B>>,
) -> Result<ControlFlow<B>> {
    work.charge_into_error(1)?;
    let (as_lo, as_hi, ac_lo, ac_hi) = a.envelope(a_lo, a_hi);
    let (bs_lo, bs_hi, bc_lo, bc_hi) = b.envelope(b_lo, b_hi);
    if as_hi < bs_lo || bs_hi < as_lo || ac_hi < bc_lo || bc_hi < ac_lo {
        return Ok(ControlFlow::Continue(()));
    }
    let (a_len, b_len) = (a_hi.wrapping_sub(a_lo), b_hi.wrapping_sub(b_lo));
    let children_miss = if a_len == 2 {
        a.two_segment_gap(a_lo, (bs_lo, bs_hi, bc_lo, bc_hi))
    } else if b_len == 2 {
        b.two_segment_gap(b_lo, (as_lo, as_hi, ac_lo, ac_hi))
    } else {
        false
    };
    if children_miss {
        return Ok(ControlFlow::Continue(()));
    }
    if a_len == 1 && b_len == 1 {
        let (x, y) = (a.ordinal(a_lo), b.ordinal(b_lo));
        // One pool: input-ordinal order. Two pools: `a` then `b`, whatever
        // their ordinals.
        let (low, high) = if std::ptr::eq(a.pool, b.pool) && x > y {
            (y, x)
        } else {
            (x, y)
        };
        return visit(low, high, work);
    }
    if a_len >= b_len {
        let mid = a_lo.wrapping_add(a_len / 2);
        match bisect_runs(a, a_lo, mid, b, b_lo, b_hi, work, visit)? {
            ControlFlow::Continue(()) => {},
            ControlFlow::Break(value) => return Ok(ControlFlow::Break(value)),
        }
        bisect_runs(a, mid, a_hi, b, b_lo, b_hi, work, visit)
    } else {
        let mid = b_lo.wrapping_add(b_len / 2);
        match bisect_runs(a, a_lo, a_hi, b, b_lo, mid, work, visit)? {
            ControlFlow::Continue(()) => {},
            ControlFlow::Break(value) => return Ok(ControlFlow::Break(value)),
        }
        bisect_runs(a, a_lo, a_hi, b, mid, b_hi, work, visit)
    }
}

/// The plain forward-window scan over sweep-sorted entries.
fn plain_windows<B, W: Work>(
    entries: &[SweepEntry],
    work: &mut W,
    visit: &mut impl FnMut(usize, usize, &mut W) -> Result<ControlFlow<B>>,
) -> Result<ControlFlow<B>> {
    let mut remaining = entries;
    while let Some((left, right_entries)) = remaining.split_first() {
        work.charge_into_error(1)?;
        for right in right_entries {
            work.charge_into_error(1)?;
            if right.sweep_min > left.sweep_max {
                break;
            }
            if left.side & right.side == 0
                && right.cross_min <= left.cross_max
                && right.cross_max >= left.cross_min
            {
                match visit_pair(left, right, work, visit)? {
                    ControlFlow::Continue(()) => {},
                    ControlFlow::Break(value) => return Ok(ControlFlow::Break(value)),
                }
            }
        }
        remaining = right_entries;
    }
    Ok(ControlFlow::Continue(()))
}

/// The banded forward-window scan (see [`for_each_candidate_pair`]): CSR
/// band membership over the cross axis, a window per band, each pair
/// visited only in the first band both envelopes share.
#[expect(
    clippy::indexing_slicing,
    reason = "at most 64 bands of u32-admitted entries keep every usize CSR offset representable on the supported 64-bit target"
)]
fn banded_windows<B, W: Work>(
    entries: &[SweepEntry],
    bands: usize,
    cross_lo: f64,
    band_height: f64,
    work: &mut W,
    visit: &mut impl FnMut(usize, usize, &mut W) -> Result<ControlFlow<B>>,
) -> Result<ControlFlow<B>> {
    let band_of = |cross: f64| -> usize {
        (((cross - cross_lo) / band_height) as usize).min(bands.wrapping_sub(1))
    };
    // Entries are already in sweep order, so every band row is too.
    let start_len = bands.wrapping_add(1);
    // Allocate and zero the CSR row starts before counting memberships.
    work.charge_product_into_error(start_len, 2)?;
    let mut starts = vec![0_usize; start_len];
    for entry in entries {
        work.charge_into_error(1)?;
        for band in band_of(entry.cross_min)..=band_of(entry.cross_max) {
            work.charge_into_error(1)?;
            let next = band.wrapping_add(1);
            starts[next] = starts[next].wrapping_add(1);
        }
    }
    // Prefix sums visit one row per band.
    work.charge_product_into_error(bands, 1)?;
    for band in 0..bands {
        let next = band.wrapping_add(1);
        starts[next] = starts[next].wrapping_add(starts[band]);
    }
    // The cursor is a copied starts column.
    work.charge_product_into_error(start_len, 2)?;
    let mut cursor = Vec::with_capacity(start_len);
    cursor.extend_from_slice(&starts);
    // Allocate and zero one slot per actual band membership.
    work.charge_product_into_error(starts[bands], 2)?;
    let mut slots = vec![0_usize; starts[bands]];
    for (position, entry) in entries.iter().enumerate() {
        work.charge_into_error(1)?;
        for band in band_of(entry.cross_min)..=band_of(entry.cross_max) {
            work.charge_into_error(1)?;
            slots[cursor[band]] = position;
            cursor[band] = cursor[band].wrapping_add(1);
        }
    }
    for band in 0..bands {
        work.charge_into_error(1)?;
        let next = band.wrapping_add(1);
        let row = &slots[starts[band]..starts[next]];
        let mut remaining = row;
        while let Some((&left_slot, right_slots)) = remaining.split_first() {
            work.charge_into_error(1)?;
            let left = &entries[left_slot];
            for &right_slot in right_slots {
                work.charge_into_error(1)?;
                let right = &entries[right_slot];
                if right.sweep_min > left.sweep_max {
                    break;
                }
                if left.side & right.side == 0
                    && right.cross_min <= left.cross_max
                    && right.cross_max >= left.cross_min
                    && band_of(left.cross_min).max(band_of(right.cross_min)) == band
                {
                    match visit_pair(left, right, work, visit)? {
                        ControlFlow::Continue(()) => {},
                        ControlFlow::Break(value) => return Ok(ControlFlow::Break(value)),
                    }
                }
            }
            remaining = right_slots;
        }
    }
    Ok(ControlFlow::Continue(()))
}

/// Preserve operand order for cross joins and ascending ordinals for self joins.
fn visit_pair<B, W: Work>(
    left: &SweepEntry,
    right: &SweepEntry,
    work: &mut W,
    visit: &mut impl FnMut(usize, usize, &mut W) -> Result<ControlFlow<B>>,
) -> Result<ControlFlow<B>> {
    match (left.side, right.side) {
        (CROSS_LEFT_SIDE, CROSS_RIGHT_SIDE) => {
            return visit(left.ordinal as usize, right.ordinal as usize, work);
        },
        (CROSS_RIGHT_SIDE, CROSS_LEFT_SIDE) => {
            return visit(right.ordinal as usize, left.ordinal as usize, work);
        },
        _ => {},
    }
    let (low, high) = if left.ordinal < right.ordinal {
        (left.ordinal, right.ordinal)
    } else {
        (right.ordinal, left.ordinal)
    };
    visit(low as usize, high as usize, work)
}

#[cfg(test)]
mod oblique_projection_tests {
    use std::cmp::Ordering;

    use super::*;
    use crate::geometry::tessellation::exact::ExactDyadic;

    #[test]
    fn oblique_projection_intervals_contain_exact_stored_values() {
        let tiny = f64::from_bits(1);
        let axes = [
            ObliqueProjection { x: 0.125, y: -1.0 },
            ObliqueProjection { x: 1.0, y: -1.0 },
            ObliqueProjection {
                x: tiny,
                y: -3.0 * tiny,
            },
        ];
        let points = [
            XY::new(0.0, -0.0),
            XY::new(tiny, -tiny),
            XY::new(1.0e200, 1.0e200),
            XY::new(1.0e200, 1.0e200_f64.next_up()),
            XY::new(f64::MAX, -f64::MAX),
        ];
        for projection in axes {
            for point in points {
                let Some((lower, upper)) = projected_point_interval(point, projection) else {
                    continue;
                };
                let x = ExactDyadic::from_f64(projection.x)
                    .try_product(&ExactDyadic::from_f64(point.x))
                    .unwrap();
                let y = ExactDyadic::from_f64(projection.y)
                    .try_product(&ExactDyadic::from_f64(point.y))
                    .unwrap();
                let exact = x.try_add(y).unwrap();
                if !(lower.is_infinite() && lower.is_sign_negative()) {
                    assert_ne!(ExactDyadic::from_f64(lower).cmp(&exact), Ordering::Greater);
                }
                if !(upper.is_infinite() && upper.is_sign_positive()) {
                    assert_ne!(ExactDyadic::from_f64(upper).cmp(&exact), Ordering::Less);
                }
            }
        }
        assert!(
            projected_point_interval(XY::new(f64::NAN, 0.0), ObliqueProjection {
                x: 0.125,
                y: -1.0
            },)
            .is_none()
        );
    }
}
