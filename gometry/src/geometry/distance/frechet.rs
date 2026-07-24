#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
#![allow(
    clippy::absolute_paths,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use super::*;
pub(crate) fn frechet_distance_line_columns(
    left_xs: &[f64],
    left_ys: &[f64],
    right_xs: &[f64],
    right_ys: &[f64],
) -> Result<f64> {
    if left_xs.is_empty() || right_xs.is_empty() {
        return Err(GeometryErrorKind::EmptyLinework.into());
    }
    Ok(if left_xs.len() <= right_xs.len() {
        frechet_dp_columns(left_xs, left_ys, right_xs, right_ys)
    } else {
        frechet_dp_columns(right_xs, right_ys, left_xs, left_ys)
    })
}

/// Element-wise planar Fréchet over packed line CSR windows (one column
/// kernel per pair, identity shortcuts for identical windows).
pub(crate) fn frechet_distance_line_columns_batch(
    left: crate::geometry::packed_line_metrics::PackedLineColumnView<'_>,
    right: crate::geometry::packed_line_metrics::PackedLineColumnView<'_>,
    out: &mut [f64],
) -> Result<(), (usize, crate::error::Error)> {
    let len = left.logical_len();
    debug_assert_eq!(len, right.logical_len());
    debug_assert_eq!(out.len(), len);
    let mut scratch = FrechetDpScratch::default();
    for (index, slot) in out.iter_mut().enumerate() {
        let (left_xs, left_ys) = left.row_xy(index);
        let (right_xs, right_ys) = right.row_xy(index);
        *slot = if let Some(value) = super::packed_line_metrics::packed_line_same_window_value(
            left_xs,
            left_ys,
            right_xs,
            right_ys,
            f64::INFINITY,
        ) {
            value
        } else if left_xs.is_empty() || right_xs.is_empty() {
            return Err((index, GeometryErrorKind::EmptyLinework.into()));
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
    Ok(())
}

pub(crate) fn discrete_frechet_distance<A: Coordinates + ?Sized, B: Coordinates + ?Sized>(
    left: &A,
    right: &B,
) -> f64 {
    if left.coord_count() <= right.coord_count() {
        frechet_dp(left, right)
    } else {
        frechet_dp(right, left)
    }
}

/// The discrete-Fréchet DP with `short` as the row dimension.
pub(crate) fn frechet_dp<S: Coordinates + ?Sized, L: Coordinates + ?Sized>(
    short: &S,
    long: &L,
) -> f64 {
    debug_assert!(short.coord_count() > 0 && long.coord_count() > 0);
    // SoA lane: the O(n·m) inner loop reads raw coordinate columns —
    // no per-cell `Point` gather and no first-column branch.
    if let (Some((short_xs, short_ys)), Some((long_xs, long_ys))) =
        (short.xy_columns(), long.xy_columns())
    {
        return frechet_dp_columns(short_xs, short_ys, long_xs, long_ys);
    }

    // First DP row initialized separately, then a branch-free inner loop:
    // no per-cell (row, column) match and no `Vec<Point>` staging.
    let width = short.coord_count();
    let mut previous = vec![0.0_f64; width];
    let mut current = vec![0.0_f64; width];
    let mut long_points = long.iter_coords();
    let first_long = long_points.next().expect("non-empty linework");
    let mut running = 0.0_f64;
    for (cell, short_point) in std::iter::zip(&mut previous, short.iter_coords()) {
        running = running.max(point_distance_squared(first_long, short_point));
        *cell = running;
    }
    for long_point in long_points {
        let mut first = true;
        let mut left_value = 0.0;
        for (short_index, short_point) in short.iter_coords().enumerate() {
            let distance_squared = point_distance_squared(long_point, short_point);
            let reach = if first {
                first = false;
                previous[0]
            } else {
                previous[short_index]
                    .min(previous[short_index - 1])
                    .min(left_value)
            };
            left_value = distance_squared.max(reach);
            current[short_index] = left_value;
        }
        std::mem::swap(&mut previous, &mut current);
    }
    previous[width - 1].sqrt()
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

thread_local! {
    static FRECHET_DP_SCRATCH: std::cell::RefCell<FrechetDpScratch> =
        const { std::cell::RefCell::new(FrechetDpScratch::new()) };
}

pub(crate) struct FrechetDpScratchGuard {
    scratch: FrechetDpScratch,
}

impl FrechetDpScratchGuard {
    fn take() -> Self {
        Self {
            scratch: FRECHET_DP_SCRATCH.with(|scratch| std::mem::take(&mut *scratch.borrow_mut())),
        }
    }
}

impl Drop for FrechetDpScratchGuard {
    fn drop(&mut self) {
        self.scratch.previous.clear();
        self.scratch.current.clear();
        FRECHET_DP_SCRATCH
            .with(|scratch| *scratch.borrow_mut() = std::mem::take(&mut self.scratch));
    }
}

/// [`frechet_dp`] over raw `SoA` columns, with EXACT band pruning. The full
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
    let width = short_xs.len();
    let mut scratch_guard = FrechetDpScratchGuard::take();
    scratch_guard.scratch.ensure_capacity(width);
    frechet_dp_columns_with_scratch(
        short_xs,
        short_ys,
        long_xs,
        long_ys,
        &mut scratch_guard.scratch.previous[..width],
        &mut scratch_guard.scratch.current[..width],
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
    answer2.sqrt()
}
