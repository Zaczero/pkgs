#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use crate::geometry::{
    CoordSeq, Point, axis_extent, axis_pow2_scale, packed_line_metrics, point_distance,
    same_topological_coordinate,
};
pub(crate) fn frechet_distance_line_columns(
    left_xs: &[f64],
    left_ys: &[f64],
    right_xs: &[f64],
    right_ys: &[f64],
) -> f64 {
    // Empty is total for Fréchet, so one empty row cannot abort a batch.
    if left_xs.is_empty() || right_xs.is_empty() {
        return f64::INFINITY;
    }
    if left_xs.len() <= right_xs.len() {
        frechet_dp_columns(left_xs, left_ys, right_xs, right_ys)
    } else {
        frechet_dp_columns(right_xs, right_ys, left_xs, left_ys)
    }
}

/// Element-wise planar Fréchet over packed line CSR windows (one column
/// kernel per pair, identity shortcuts for identical windows).
pub(crate) fn frechet_distance_line_columns_batch(
    left: crate::geometry::packed_line_metrics::PackedLineColumnView<'_>,
    right: crate::geometry::packed_line_metrics::PackedLineColumnView<'_>,
    out: &mut [f64],
) {
    let len = left.logical_len();
    debug_assert_eq!(len, right.logical_len());
    debug_assert_eq!(out.len(), len);
    let mut scratch = FrechetDpScratch::default();
    for (index, slot) in out.iter_mut().enumerate() {
        let (left_xs, left_ys) = left.row_xy(index);
        let (right_xs, right_ys) = right.row_xy(index);
        *slot = if let Some(value) = packed_line_metrics::packed_line_same_window_value(
            left_xs,
            left_ys,
            right_xs,
            right_ys,
            f64::INFINITY,
        ) {
            value
        } else if left_xs.is_empty() || right_xs.is_empty() {
            f64::INFINITY
        } else if left_xs.len() <= right_xs.len() {
            let width = left_xs.len();
            scratch.ensure_capacity(width);
            frechet_dp_columns_with_scratch(
                left_xs,
                left_ys,
                right_xs,
                right_ys,
                &mut scratch.previous[..width],
                &mut scratch.current[..width],
            )
        } else {
            let width = right_xs.len();
            scratch.ensure_capacity(width);
            frechet_dp_columns_with_scratch(
                right_xs,
                right_ys,
                left_xs,
                left_ys,
                &mut scratch.previous[..width],
                &mut scratch.current[..width],
            )
        };
    }
}

pub(crate) fn discrete_frechet_distance(left: &CoordSeq, right: &CoordSeq) -> f64 {
    let mut scratch = FrechetDpScratch::default();
    discrete_frechet_distance_with_scratch(left, right, &mut scratch)
}

pub(crate) fn discrete_frechet_distance_with_scratch(
    left: &CoordSeq,
    right: &CoordSeq,
    scratch: &mut FrechetDpScratch,
) -> f64 {
    if left.len() <= right.len() {
        frechet_dp_columns_with_dp_scratch(left.xs(), left.ys(), right.xs(), right.ys(), scratch)
    } else {
        frechet_dp_columns_with_dp_scratch(right.xs(), right.ys(), left.xs(), left.ys(), scratch)
    }
}

/// A valid UPPER BOUND on the discrete Fréchet² — the squared bottleneck of
/// ONE achievable monotone coupling (every coupling's bottleneck ≥ the
/// optimum). A greedy diagonal walk from `(0, 0)` to `(L-1, W-1)`: at each
/// step take whichever forward move (down / right / diagonal) pairs the
/// nearest next vertices, tracking the worst pairing. `O(L + W)`, and tight
/// when the curves run close — which is exactly when the band pays off.
pub(crate) fn frechet_greedy_ub_squared(
    short_xs: &[f64],
    short_ys: &[f64],
    long_xs: &[f64],
    long_ys: &[f64],
) -> f64 {
    let (width, length) = (short_xs.len(), long_xs.len());
    // Length-exact reborrows so the walk's j/i indexing is provably in range.
    let short_ys = &short_ys[..width];
    let long_ys = &long_ys[..length];
    let d2 = |i: usize, j: usize| {
        let (dx, dy) = (short_xs[j] - long_xs[i], short_ys[j] - long_ys[i]);
        dx * dx + dy * dy
    };
    let (mut i, mut j) = (0_usize, 0_usize);
    let mut bottleneck = d2(0, 0);
    while i + 1 < length || j + 1 < width {
        let (down, right) = (i + 1 < length, j + 1 < width);
        (i, j) = if down && right {
            let (diag, dn, dr) = (d2(i + 1, j + 1), d2(i + 1, j), d2(i, j + 1));
            if diag <= dn && diag <= dr {
                (i + 1, j + 1)
            } else if dn <= dr {
                (i + 1, j)
            } else {
                (i, j + 1)
            }
        } else if down {
            (i + 1, j)
        } else {
            (i, j + 1)
        };
        bottleneck = bottleneck.max(d2(i, j));
    }
    bottleneck
}

#[derive(Default)]
pub(crate) struct FrechetDpScratch {
    previous: Vec<f64>,
    current: Vec<f64>,
}

impl FrechetDpScratch {
    const fn new() -> Self {
        Self {
            previous: Vec::new(),
            current: Vec::new(),
        }
    }

    fn ensure_capacity(&mut self, width: usize) {
        if self.previous.len() < width {
            self.previous.resize(width, f64::INFINITY);
            self.current.resize(width, f64::INFINITY);
        }
    }
}

impl crate::scratch_pool::RetainScratch for FrechetDpScratch {
    fn clear_for_reuse(&mut self) {
        self.previous.clear();
        self.current.clear();
    }

    fn retained_bytes(&self) -> usize {
        crate::scratch_pool::flat_vec_retained_bytes(&self.previous)
            .saturating_add(crate::scratch_pool::flat_vec_retained_bytes(&self.current))
    }
}

thread_local! {
    static FRECHET_DP_SCRATCH: crate::scratch_pool::ScratchPool<FrechetDpScratch> =
        const { crate::scratch_pool::ScratchPool::new(FrechetDpScratch::new()) };
}

/// Planar discrete Fréchet over raw `SoA` columns, with EXACT band pruning. The full
/// `O(n·m)` table is unnecessary: with an achievable upper bound `ub²`
/// ([`frechet_greedy_ub_squared`]), every cell whose reachability value
/// exceeds `ub²` is provably off any optimal coupling (its bottleneck would be
/// ≥ `ub²` ≥ the optimum), so we treat it as `+∞` and never compute it. The
/// live cells of each row form a contiguous interval `[lo, hi]` whose left edge
/// `lo` is non-decreasing across rows, so the sweep visits only the diagonal
/// band — `O(n + band·area)`, collapsing toward linear when the curves run
/// close, while staying `O(n·m)` in the worst case (far curves, loose bound).
///
/// Bit-identical to the full table: a live cell's value is `max(d², min(three
/// neighbours))`; being ≤ `ub²` forces that min ≤ `ub²`, so the deciding
/// neighbour is itself live and carries the SAME finite value in both the
/// banded and the full DP. The corner is live (the greedy coupling reaches it
/// at bottleneck ≤ `ub²`), so its value is the exact discrete Fréchet².
pub(crate) fn frechet_dp_columns(
    short_xs: &[f64],
    short_ys: &[f64],
    long_xs: &[f64],
    long_ys: &[f64],
) -> f64 {
    let mut scratch = crate::scratch_pool::ScratchPool::checkout(&FRECHET_DP_SCRATCH);
    frechet_dp_columns_with_dp_scratch(short_xs, short_ys, long_xs, long_ys, &mut scratch)
}

pub(crate) fn frechet_dp_columns_with_dp_scratch(
    short_xs: &[f64],
    short_ys: &[f64],
    long_xs: &[f64],
    long_ys: &[f64],
    scratch: &mut FrechetDpScratch,
) -> f64 {
    let width = short_xs.len();
    scratch.ensure_capacity(width);
    let FrechetDpScratch { previous, current } = scratch;
    frechet_dp_columns_with_scratch(
        short_xs,
        short_ys,
        long_xs,
        long_ys,
        &mut previous[..width],
        &mut current[..width],
    )
}

/// Column Fréchet DP using caller-provided row workspaces (`previous` /
/// `current`, each at least `short_xs.len()`). Buffers are reset to `+∞` on
/// entry; stale slots outside the live band are never read.
pub(crate) fn frechet_dp_columns_with_scratch<'a>(
    short_xs: &[f64],
    short_ys: &[f64],
    long_xs: &[f64],
    long_ys: &[f64],
    previous: &'a mut [f64],
    current: &'a mut [f64],
) -> f64 {
    const INF: f64 = f64::INFINITY;
    let width = short_xs.len();
    let ub2 = frechet_greedy_ub_squared(short_xs, short_ys, long_xs, long_ys);
    // Width-exact reborrows: every row access below indexes a slice whose
    // length IS the loop bound, so the optimizer proves them in range and the
    // DP inner loop carries no bounds checks (asm-verified).
    let short_ys = &short_ys[..width];
    let mut previous = &mut previous[..width];
    let mut current = &mut current[..width];
    previous.fill(INF);
    current.fill(INF);

    // Row 0 (long[0] vs every short vertex): the bottleneck of coupling long[0]
    // with short[0..=j] is the running max of d² — non-decreasing in j, so the
    // live prefix is exactly [0, prev_hi] (d(0,0)² ≤ ub² always: the endpoints
    // are forced together, so ub² ≥ optimum ≥ d(0,0)²).
    let (first_x, first_y) = (long_xs[0], long_ys[0]);
    let mut running = 0.0_f64;
    let (mut prev_lo, mut prev_hi) = (0_usize, 0_usize);
    for index in 0..width {
        let (dx, dy) = (short_xs[index] - first_x, short_ys[index] - first_y);
        running = running.max(dx * dx + dy * dy);
        if running <= ub2 {
            previous[index] = running;
            prev_hi = index;
        } else {
            break;
        }
    }

    for (&long_x, &long_y) in std::iter::zip(&long_xs[1..], &long_ys[1..]) {
        // `lo` is non-decreasing: no live cell sits left of the previous row's
        // leftmost (every move into a row keeps or increases the column).
        let lo = prev_lo;
        let (mut cur_lo, mut cur_hi) = (usize::MAX, 0_usize);
        let mut left = INF; // current[j-1], +∞ outside the band
        for index in lo..width {
            let prev_j = if index <= prev_hi {
                previous[index]
            } else {
                INF
            };
            let prev_j1 = if index > lo && index - 1 <= prev_hi {
                previous[index - 1]
            } else {
                INF
            };
            let reach = prev_j.min(prev_j1).min(left);
            let value = if reach.is_finite() {
                let (dx, dy) = (short_xs[index] - long_x, short_ys[index] - long_y);
                (dx * dx + dy * dy).max(reach)
            } else {
                INF
            };
            if value <= ub2 {
                current[index] = value;
                if cur_lo == usize::MAX {
                    cur_lo = index;
                }
                cur_hi = index;
                left = value;
            } else {
                // A dead cell inside the swept span MUST be written `+∞`: the
                // row buffer is reused, so an unwritten slot would expose a
                // stale finite value to the next row's band-bounded reads and
                // silently understate a reachability minimum.
                current[index] = INF;
                left = INF;
                // Past the previous row's live span with a dead running cell:
                // no surviving entry from above or the left remains, so every
                // further column is `+∞` too — stop the row.
                if index > prev_hi {
                    break;
                }
            }
        }
        debug_assert_ne!(
            cur_lo,
            usize::MAX,
            "the optimal coupling keeps each row live"
        );
        (previous, current) = (current, previous);
        (prev_lo, prev_hi) = (cur_lo, cur_hi);
    }
    // The corner is live, so `previous[width - 1]` is the exact Fréchet²; the
    // `ub2` arm is unreachable defence against a degenerate empty final band.
    let answer2 = if prev_hi == width - 1 {
        previous[width - 1]
    } else {
        ub2
    };
    finish_frechet_squared(answer2, short_xs, short_ys, long_xs, long_ys)
}

/// `sqrt` of Fréchet², with a distance-space DP rescue when squared space
/// underflows to 0 for non-identical curves (e.g. parallel lines 1e-200 apart).
fn finish_frechet_squared(
    answer2: f64,
    short_xs: &[f64],
    short_ys: &[f64],
    long_xs: &[f64],
    long_ys: &[f64],
) -> f64 {
    // Shared squared-norm trust: normal → sqrt; zero/subnormal/non-finite →
    // distance-space DP, optionally in an origin-shifted power-of-two frame
    // so huge parallel lines do not report +inf.
    if answer2.is_normal() {
        return answer2.sqrt();
    }
    if frechet_columns_vertexwise_equal(short_xs, short_ys, long_xs, long_ys) {
        return 0.0;
    }
    let dist = frechet_dp_columns_distance(short_xs, short_ys, long_xs, long_ys);
    if dist.is_finite() {
        return dist;
    }
    frechet_dp_columns_distance_framed(short_xs, short_ys, long_xs, long_ys)
}

fn frechet_dp_columns_distance_framed(
    short_xs: &[f64],
    short_ys: &[f64],
    long_xs: &[f64],
    long_ys: &[f64],
) -> f64 {
    let origin_x = short_xs
        .first()
        .copied()
        .or_else(|| long_xs.first().copied())
        .unwrap_or(0.0);
    let origin_y = short_ys
        .first()
        .copied()
        .or_else(|| long_ys.first().copied())
        .unwrap_or(0.0);
    let max_abs_x = axis_extent(
        short_xs.iter().copied().chain(long_xs.iter().copied()),
        origin_x,
    );
    let max_abs_y = axis_extent(
        short_ys.iter().copied().chain(long_ys.iter().copied()),
        origin_y,
    );
    let Some(max_abs) = max_abs_x.zip(max_abs_y).map(|(x, y)| x.max(y)) else {
        return f64::INFINITY;
    };
    if max_abs == 0.0 {
        return 0.0;
    }
    let scale = axis_pow2_scale(max_abs);
    let map = |xs: &[f64], ys: &[f64]| -> (Vec<f64>, Vec<f64>) {
        (
            xs.iter()
                .map(|&x| crate::geometry::scaled_residual(x, origin_x, scale))
                .collect(),
            ys.iter()
                .map(|&y| crate::geometry::scaled_residual(y, origin_y, scale))
                .collect(),
        )
    };
    let (sxs, sys) = map(short_xs, short_ys);
    let (lxs, lys) = map(long_xs, long_ys);
    frechet_dp_columns_distance(&sxs, &sys, &lxs, &lys) / scale
}

fn frechet_columns_vertexwise_equal(
    short_xs: &[f64],
    short_ys: &[f64],
    long_xs: &[f64],
    long_ys: &[f64],
) -> bool {
    short_xs.len() == long_xs.len()
        && short_xs
            .iter()
            .zip(long_xs)
            .zip(short_ys.iter().zip(long_ys))
            .all(|((&sx, &lx), (&sy, &ly))| {
                same_topological_coordinate(sx, lx) && same_topological_coordinate(sy, ly)
            })
}

/// Distance-space discrete Fréchet DP (rescue for zero, subnormal, or
/// non-finite squared answers).
fn frechet_dp_columns_distance(
    short_xs: &[f64],
    short_ys: &[f64],
    long_xs: &[f64],
    long_ys: &[f64],
) -> f64 {
    let width = short_xs.len();
    let mut previous = vec![0.0_f64; width];
    let mut current = vec![0.0_f64; width];
    let (first_x, first_y) = (long_xs[0], long_ys[0]);
    let mut running = 0.0_f64;
    for (cell, (&sx, &sy)) in std::iter::zip(&mut previous, std::iter::zip(short_xs, short_ys)) {
        running = running.max(point_distance(
            Point::new_raw_xy(first_x, first_y),
            Point::new_raw_xy(sx, sy),
        ));
        *cell = running;
    }
    for (&long_x, &long_y) in long_xs[1..].iter().zip(&long_ys[1..]) {
        let mut first = true;
        let mut left_value = 0.0;
        for (short_index, (&sx, &sy)) in short_xs.iter().zip(short_ys).enumerate() {
            let distance =
                point_distance(Point::new_raw_xy(long_x, long_y), Point::new_raw_xy(sx, sy));
            let reach = if first {
                first = false;
                previous[0]
            } else {
                previous[short_index]
                    .min(previous[short_index - 1])
                    .min(left_value)
            };
            left_value = distance.max(reach);
            current[short_index] = left_value;
        }
        std::mem::swap(&mut previous, &mut current);
    }
    previous[width - 1]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn framed_distance_rescues_finite_opposite_extreme_operands() {
        let magnitude = f64::MAX;
        let short_x = [-magnitude, magnitude];
        let short_y = [0.0, 0.0];
        let long_x = [-magnitude, 0.0];
        let long_y = [0.0, 0.0];

        assert_eq!(
            frechet_dp_columns_distance_framed(&short_x, &short_y, &long_x, &long_y).to_bits(),
            magnitude.to_bits()
        );
    }
}
