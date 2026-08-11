use std::simd::cmp::{SimdPartialEq as _, SimdPartialOrd as _};
use std::simd::num::SimdFloat as _;
use std::simd::{Select as _, StdFloat as _};

use crate::geometry::{
    AxisFrame, Coordinates as _, Point, Polygon, REDUCE_LANES, REDUCE_SIMD_MIN, ReduceSimd, Ring,
    Shape, XY, axis_pow2_scale, canonicalize_zero, centroid_ring_sums, centroid_ring_sums_local,
    closed_columns_winding, column_mean2, exact_ring_area_centroid_sums_local, point_distance,
    scaled_residual, simd_reduce_f64,
};
fn accumulate_ring_centroid(
    xs: &[f64],
    ys: &[f64],
    hole: bool,
    base: &mut Option<(f64, f64)>,
    area_sum2: &mut f64,
    cg3: &mut (f64, f64),
) {
    let pairs = xs.len().saturating_sub(1);
    if pairs == 0 {
        return;
    }
    // One GLOBAL base point (the first ring vertex seen) keeps every
    // triangle small. The SIMD `centroid_ring_sums` twin of the shoelace
    // fold sums the ring's SIGNED triangle-fan contributions (cross-product
    // areas about the anchor); the JTS sign rule (a shell contributes
    // positively when CW, a hole when CCW) is applied to the whole-ring
    // total here — exact since `sign` is ±1.0.
    let anchor = *base.get_or_insert((xs[0], ys[0]));
    let ccw = closed_columns_winding(xs, ys, pairs).is_ccw();
    let sign = if hole == ccw { 1.0 } else { -1.0 };
    let (area2, cgx, cgy) = centroid_ring_sums(xs, ys, pairs, anchor);
    *area_sum2 += sign * area2;
    cg3.0 += sign * cgx;
    cg3.1 += sign * cgy;
}

fn finish_areal_centroid(area_sum2: f64, cg3: (f64, f64)) -> Option<(f64, f64)> {
    // Signed triangle-fan sums can cancel to -0.0 on a symmetric polygon; the
    // lineal/puntal paths happen to land on +0.0, so normalize here to keep the
    // +0.0 invariant uniform across centroid of every geometry type.
    // Reject non-finite so a moment overflow cannot reach `Point::new`.
    // Reject subnormal |area|: cg/area collapses toward the origin even when
    // the ring is a positive square (e.g. [0,2e-162]²) — leave None for the
    // shared-scale / lineal cascade.
    let area_abs = area_sum2.abs();
    if area_abs > 0.0 && !area_sum2.is_normal() {
        return None;
    }
    match (area_abs > 0.0).then(|| {
        (
            canonicalize_zero(cg3.0 / 3.0 / area_sum2),
            canonicalize_zero(cg3.1 / 3.0 / area_sum2),
        )
    }) {
        Some((x, y)) if x.is_finite() && y.is_finite() => Some((x, y)),
        Some(_) | None => None,
    }
}

/// Shared-extent areal centroid: bbox-midpoint anchor (finite `f64::midpoint`),
/// per-axis power-of-two scales, local triangle-fan sums as `x*s - origin*s`,
/// then fused map-back `(anchor*s + local) / s`. The midpoint anchor keeps the
/// residual centroid near zero for centered shapes so densified extreme
/// rectangles do not amplify ULP noise by `1/s`; per-axis scales keep a
/// huge-X/tiny-Y ring from underflowing moment products.
fn exact_shared_centroid_sums(
    visit: &impl Fn(&mut dyn FnMut(&[f64], &[f64], bool)),
    anchor_x: f64,
    anchor_y: f64,
    scale_x: f64,
    scale_y: f64,
) -> (f64, f64, f64) {
    let mut sums = (0.0_f64, 0.0_f64, 0.0_f64);
    visit(&mut |xs, ys, hole| {
        let pairs = xs.len().saturating_sub(1);
        if pairs == 0 {
            return;
        }
        let (area, x, y) = exact_ring_area_centroid_sums_local(
            xs, ys, pairs, anchor_x, anchor_y, scale_x, scale_y,
        );
        let ccw = closed_columns_winding(xs, ys, pairs).is_ccw();
        let sign = if hole == ccw { 1.0 } else { -1.0 };
        sums.0 += sign * area;
        sums.1 += sign * x;
        sums.2 += sign * y;
    });
    sums
}

fn finish_shared_areal_centroid(
    area: f64,
    moment_x: f64,
    moment_y: f64,
    anchor_x: f64,
    anchor_y: f64,
    scale_x: f64,
    scale_y: f64,
    driver_x: f64,
    driver_y: f64,
) -> Option<(f64, f64)> {
    if !(area.abs() > 0.0 && area.is_finite()) {
        return None;
    }
    // Local residual centroid in the scaled frame (O(1) extents). Densified
    // extreme rings leave O(eps) accumulation noise in the moments; zero it
    // before unscaling so `noise / s` cannot become world-scale error.
    let local_extent_x = (driver_x * scale_x).max(1.0);
    let local_extent_y = (driver_y * scale_y).max(1.0);
    let mut local_x = moment_x / (3.0 * area);
    let mut local_y = moment_y / (3.0 * area);
    if local_x.abs() <= local_extent_x * f64::EPSILON * 64.0 {
        local_x = 0.0;
    }
    if local_y.abs() <= local_extent_y * f64::EPSILON * 64.0 {
        local_y = 0.0;
    }
    // Fused map-back avoids anchor + huge cancellation at extreme corners.
    Some((
        canonicalize_zero((anchor_x * scale_x + local_x) / scale_x),
        canonicalize_zero((anchor_y * scale_y + local_y) / scale_y),
    ))
}

fn areal_centroid_shared_scale(
    visit: impl Fn(&mut dyn FnMut(&[f64], &[f64], bool)),
) -> Option<(f64, f64)> {
    let mut min_x = f64::INFINITY;
    let mut max_x = f64::NEG_INFINITY;
    let mut min_y = f64::INFINITY;
    let mut max_y = f64::NEG_INFINITY;
    let mut any = false;
    visit(&mut |xs, ys, _hole| {
        for (&x, &y) in xs.iter().zip(ys.iter()) {
            any = true;
            min_x = min_x.min(x);
            max_x = max_x.max(x);
            min_y = min_y.min(y);
            max_y = max_y.max(y);
        }
    });
    if !any || !min_x.is_finite() {
        return None;
    }
    // Midpoint is exact for opposite extremes (`±1e308` → 0) and a much better
    // fan origin than the first vertex for densified extreme rings.
    let ax = f64::midpoint(min_x, max_x);
    let ay = f64::midpoint(min_y, max_y);
    let mut max_abs_x = ax.abs().max(min_x.abs()).max(max_x.abs());
    let mut max_abs_y = ay.abs().max(min_y.abs()).max(max_y.abs());
    let mut max_dx = 0.0_f64;
    let mut max_dy = 0.0_f64;
    let mut overflow_x = false;
    let mut overflow_y = false;
    visit(&mut |xs, ys, _hole| {
        for (&x, &y) in xs.iter().zip(ys.iter()) {
            max_abs_x = max_abs_x.max(x.abs());
            max_abs_y = max_abs_y.max(y.abs());
            let dx = (x - ax).abs();
            let dy = (y - ay).abs();
            if dx.is_finite() {
                max_dx = max_dx.max(dx);
            } else {
                overflow_x = true;
            }
            if dy.is_finite() {
                max_dy = max_dy.max(dy);
            } else {
                overflow_y = true;
            }
        }
    });
    // Prefer finite residual extent; on overflow scale from absolute coords so
    // `x*s - origin*s` stays representable (e.g. ±1e308 rectangle).
    let driver_x = if overflow_x || !max_dx.is_finite() {
        max_abs_x
    } else if max_dx > 0.0 {
        max_dx
    } else {
        max_abs_x
    };
    let driver_y = if overflow_y || !max_dy.is_finite() {
        max_abs_y
    } else if max_dy > 0.0 {
        max_dy
    } else {
        max_abs_y
    };
    if driver_x == 0.0 && driver_y == 0.0 {
        return None;
    }
    let scale_x = axis_pow2_scale(driver_x.max(f64::MIN_POSITIVE));
    let scale_y = axis_pow2_scale(driver_y.max(f64::MIN_POSITIVE));
    let mut sa = 0.0_f64;
    let mut scx = 0.0_f64;
    let mut scy = 0.0_f64;
    visit(&mut |xs, ys, hole| {
        let pairs = xs.len().saturating_sub(1);
        if pairs == 0 {
            return;
        }
        let ccw = closed_columns_winding(xs, ys, pairs).is_ccw();
        let sign = if hole == ccw { 1.0 } else { -1.0 };
        let (sa_l, sx_l, sy_l) =
            centroid_ring_sums_local(xs, ys, pairs, (ax, ay), scale_x, scale_y);
        sa += sign * sa_l;
        scx += sign * sx_l;
        scy += sign * sy_l;
    });
    // The ordinary shared-frame fold is deliberately the hot path.  A rounded
    // zero is not a geometrical zero, though: the exact shoelace tails can
    // still carry a representable area and its centroid.  Recompute all three
    // coupled sums in the SAME shared frame so area cannot select an areal
    // answer while centroid falls through to its lineal fallback.
    if sa == 0.0 {
        (sa, scx, scy) = exact_shared_centroid_sums(&visit, ax, ay, scale_x, scale_y);
    }
    finish_shared_areal_centroid(sa, scx, scy, ax, ay, scale_x, scale_y, driver_x, driver_y)
}

/// Column kernel for polygonal centroid (dimension-2 pass) over one polygon's
/// ring window range.
/// Prefer the local-frame areal centroid when world-space moments lose digits:
/// tiny extents (subnormal / near-subnormal area) or huge coordinates.
const AREAL_SHARED_SCALE_MIN_EXTENT: f64 = 1e-8;
const AREAL_SHARED_SCALE_MAX_COORD: f64 = 1e12;

fn centroid_polygon_columns(
    xs: &[f64],
    ys: &[f64],
    ring_offsets: &[i32],
    rings: std::ops::Range<usize>,
) -> Option<(f64, f64)> {
    let shared = || {
        areal_centroid_shared_scale(|visit| {
            for ring_index in rings.clone() {
                let start = ring_offsets[ring_index] as usize;
                let end = ring_offsets[ring_index + 1] as usize;
                visit(&xs[start..end], &ys[start..end], ring_index > rings.start);
            }
        })
    };
    // Classical fan + extent in one ring walk (no separate pre-scan).
    let mut base = None;
    let mut area_sum2 = 0.0_f64;
    let mut cg3 = (0.0_f64, 0.0_f64);
    let mut min_x = f64::INFINITY;
    let mut max_x = f64::NEG_INFINITY;
    let mut min_y = f64::INFINITY;
    let mut max_y = f64::NEG_INFINITY;
    for ring_index in rings.clone() {
        let start = ring_offsets[ring_index] as usize;
        let end = ring_offsets[ring_index + 1] as usize;
        let rxs = &xs[start..end];
        let rys = &ys[start..end];
        for (&x, &y) in rxs.iter().zip(rys.iter()) {
            min_x = min_x.min(x);
            max_x = max_x.max(x);
            min_y = min_y.min(y);
            max_y = max_y.max(y);
        }
        accumulate_ring_centroid(
            rxs,
            rys,
            ring_index > rings.start,
            &mut base,
            &mut area_sum2,
            &mut cg3,
        );
    }
    let extreme = if min_x.is_finite() {
        let extent = (max_x - min_x).max(max_y - min_y);
        let max_abs = max_x
            .abs()
            .max(min_x.abs())
            .max(max_y.abs())
            .max(min_y.abs());
        extent < AREAL_SHARED_SCALE_MIN_EXTENT || max_abs > AREAL_SHARED_SCALE_MAX_COORD
    } else {
        false
    };
    if extreme {
        return shared().or_else(|| finish_areal_centroid(area_sum2, cg3));
    }
    // Ordinary path: classical JTS fan (kernel-parity snapshot bit-identity).
    finish_areal_centroid(area_sum2, cg3).or_else(shared)
}

fn lineal_centroid_segment_contrib(xs: &[f64], ys: &[f64], segment: usize) -> (f64, f64, f64) {
    let length = point_distance(
        Point::new_unchecked_xy(xs[segment], ys[segment]),
        Point::new_unchecked_xy(xs[segment + 1], ys[segment + 1]),
    );
    (
        length,
        length.algebraic_mul(f64::midpoint(xs[segment], xs[segment + 1])),
        length.algebraic_mul(f64::midpoint(ys[segment], ys[segment + 1])),
    )
}

fn lineal_centroid_scalar_plain(
    xs: &[f64],
    ys: &[f64],
    range: std::ops::Range<usize>,
) -> (f64, f64, f64) {
    // Use the shared point-distance (hypot rescue on untrusted squares) so
    // length matches `.length` / `point_distance` at subnormal scales. The
    // length·midpoint product may still underfloor — finish rescues via online.
    range.fold((0.0_f64, 0.0_f64, 0.0_f64), |(tl, lx, ly), segment| {
        let (length, cx, cy) = lineal_centroid_segment_contrib(xs, ys, segment);
        (tl + length, lx + cx, ly + cy)
    })
}

fn lineal_centroid_scalar_algebraic(
    xs: &[f64],
    ys: &[f64],
    range: std::ops::Range<usize>,
) -> (f64, f64, f64) {
    range.fold((0.0_f64, 0.0_f64, 0.0_f64), |(tl, lx, ly), segment| {
        let (length, cx, cy) = lineal_centroid_segment_contrib(xs, ys, segment);
        (
            tl.algebraic_add(length),
            lx.algebraic_add(cx),
            ly.algebraic_add(cy),
        )
    })
}

fn lineal_centroid_column_sums(xs: &[f64], ys: &[f64]) -> (f64, f64, f64) {
    let segments = xs.len().saturating_sub(1);
    if segments < REDUCE_SIMD_MIN {
        return lineal_centroid_scalar_plain(xs, ys, 0..segments);
    }
    let (x0, _) = xs[..segments].as_chunks::<REDUCE_LANES>();
    let (x1, _) = xs[1..=segments].as_chunks::<REDUCE_LANES>();
    let (y0, _) = ys[..segments].as_chunks::<REDUCE_LANES>();
    let (y1, _) = ys[1..=segments].as_chunks::<REDUCE_LANES>();
    let simd_fold = |mut acc: (ReduceSimd, ReduceSimd, ReduceSimd),
                     mut scalar: (f64, f64, f64),
                     start: usize| {
        let index = start / REDUCE_LANES;
        let dx = ReduceSimd::from_array(x1[index]) - ReduceSimd::from_array(x0[index]);
        let dy = ReduceSimd::from_array(y1[index]) - ReduceSimd::from_array(y0[index]);
        let squared = dx * dx + dy * dy;
        let length = squared.sqrt();
        let zero_delta = dx.simd_eq(ReduceSimd::splat(0.0)) & dy.simd_eq(ReduceSimd::splat(0.0));
        let underflow = squared.simd_eq(ReduceSimd::splat(0.0)) & !zero_delta;
        if length.is_finite().all() && !underflow.any() {
            let half = ReduceSimd::splat(0.5);
            let mid_x =
                (ReduceSimd::from_array(x0[index]) + ReduceSimd::from_array(x1[index])) * half;
            let mid_y =
                (ReduceSimd::from_array(y0[index]) + ReduceSimd::from_array(y1[index])) * half;
            acc.0 += length;
            acc.1 += length * mid_x;
            acc.2 += length * mid_y;
        } else {
            for segment in start..start + REDUCE_LANES {
                if segment < segments {
                    let (length, cx, cy) = lineal_centroid_segment_contrib(xs, ys, segment);
                    scalar.0 = scalar.0.algebraic_add(length);
                    scalar.1 = scalar.1.algebraic_add(cx);
                    scalar.2 = scalar.2.algebraic_add(cy);
                }
            }
        }
        (acc, scalar)
    };
    let lanes = x0.len() * REDUCE_LANES;
    simd_reduce_f64(
        segments,
        (
            ReduceSimd::splat(0.0),
            ReduceSimd::splat(0.0),
            ReduceSimd::splat(0.0),
        ),
        (0.0_f64, 0.0_f64, 0.0_f64),
        simd_fold,
        |scalar, range| {
            if range.start == 0 {
                return lineal_centroid_scalar_algebraic(xs, ys, range);
            }
            let start = segments - REDUCE_LANES;
            let dx =
                ReduceSimd::from_slice(&xs[start + 1..]) - ReduceSimd::from_slice(&xs[start..]);
            let dy =
                ReduceSimd::from_slice(&ys[start + 1..]) - ReduceSimd::from_slice(&ys[start..]);
            let lane_index = ReduceSimd::from_array(std::array::from_fn(|lane| lane as f64));
            let fresh = lane_index.simd_ge(ReduceSimd::splat(f64::from((lanes - start) as u32)));
            let squared = dx * dx + dy * dy;
            let length = fresh.select(squared.sqrt(), ReduceSimd::splat(0.0));
            let zero_delta =
                dx.simd_eq(ReduceSimd::splat(0.0)) & dy.simd_eq(ReduceSimd::splat(0.0));
            let underflow = fresh & squared.simd_eq(ReduceSimd::splat(0.0)) & !zero_delta;
            if length.is_finite().all() && !underflow.any() {
                let half = ReduceSimd::splat(0.5);
                let mid_x = (ReduceSimd::from_slice(&xs[start..])
                    + ReduceSimd::from_slice(&xs[start + 1..]))
                    * half;
                let mid_y = (ReduceSimd::from_slice(&ys[start..])
                    + ReduceSimd::from_slice(&ys[start + 1..]))
                    * half;
                let contrib_x = fresh.select(length * mid_x, ReduceSimd::splat(0.0));
                let contrib_y = fresh.select(length * mid_y, ReduceSimd::splat(0.0));
                let contrib_len = fresh.select(length, ReduceSimd::splat(0.0));
                (
                    scalar.0.algebraic_add(contrib_len.reduce_sum()),
                    scalar.1.algebraic_add(contrib_x.reduce_sum()),
                    scalar.2.algebraic_add(contrib_y.reduce_sum()),
                )
            } else {
                let (added_len, added_x, added_y) =
                    lineal_centroid_scalar_algebraic(xs, ys, lanes..segments);
                (
                    scalar.0.algebraic_add(added_len),
                    scalar.1.algebraic_add(added_x),
                    scalar.2.algebraic_add(added_y),
                )
            }
        },
        |(len_acc, x_acc, y_acc), scalar| {
            (
                len_acc.reduce_sum().algebraic_add(scalar.0),
                x_acc.reduce_sum().algebraic_add(scalar.1),
                y_acc.reduce_sum().algebraic_add(scalar.2),
            )
        },
    )
}

fn accumulate_lineal_centroid(
    xs: &[f64],
    ys: &[f64],
    total_length: &mut f64,
    line_sum: &mut (f64, f64),
) {
    if xs.len() < 2 {
        return;
    }
    let (len, sum_x, sum_y) = lineal_centroid_column_sums(xs, ys);
    *total_length += len;
    line_sum.0 += sum_x;
    line_sum.1 += sum_y;
}

fn finish_lineal_centroid(total_length: f64, line_sum: (f64, f64)) -> Option<(f64, f64)> {
    (total_length > 0.0).then(|| (line_sum.0 / total_length, line_sum.1 / total_length))
}

/// [`finish_lineal_centroid`] with an overflow rescue. The `Σ length·midpoint`
/// fast-path intermediates overflow f64 range for a huge-coordinate run (e.g. a
/// vertical line at `x=1e308`) even though the finite vertices have a finite
/// centroid, committing a non-finite value that a downstream `Point::new` would
/// reject as "coordinates must be finite". When the fast result is non-finite,
/// replay the same coordinate runs (via `revisit`) through the overflow-safe
/// weighted online mean, which never forms the oversized product.
fn finish_lineal_centroid_rescued(
    total_length: f64,
    line_sum: (f64, f64),
    revisit: impl Fn(&mut dyn FnMut(&[f64], &[f64])),
) -> Option<(f64, f64)> {
    match finish_lineal_centroid(total_length, line_sum) {
        Some((x, y)) if x.is_finite() && y.is_finite() => {
            // A subnormal total or weighted sum has already rounded away
            // centroid bits even when the quotient is finite and nonzero
            // (e.g. the 2e-162 diagonal). The online mean works in a centered
            // power-of-two frame and never forms length·midpoint. Zero remains
            // a symptom worth replaying only when both components vanish.
            let underresolved = !total_length.is_normal()
                || (line_sum.0 != 0.0 && !line_sum.0.is_normal())
                || (line_sum.1 != 0.0 && !line_sum.1.is_normal());
            if underresolved || (total_length > 0.0 && x == 0.0 && y == 0.0) {
                return lineal_centroid_online(revisit).or(Some((x, y)));
            }
            Some((x, y))
        },
        Some(_) => lineal_centroid_online(revisit),
        None => None,
    }
}

/// Overflow-safe weighted online mean of edge midpoints (weight = edge length):
/// `mean += (mid - mean) · (length / Σlength)` per segment, so no oversized
/// `length·midpoint` product is ever formed. The correctness rescue for the
/// lineal centroid; `revisit` replays the same coordinate runs the fast path
/// summed. It first chooses one power-of-two frame for every replayed segment,
/// so an unrepresentable world-space length remains a finite relative weight
/// rather than becoming `inf` and poisoning the online update.
fn lineal_centroid_online(revisit: impl Fn(&mut dyn FnMut(&[f64], &[f64]))) -> Option<(f64, f64)> {
    let mut max_abs_x = 0.0_f64;
    let mut max_abs_y = 0.0_f64;
    let mut min_x = f64::INFINITY;
    let mut max_x = f64::NEG_INFINITY;
    let mut min_y = f64::INFINITY;
    let mut max_y = f64::NEG_INFINITY;
    revisit(&mut |xs, ys| {
        for (&x, &y) in xs.iter().zip(ys.iter()) {
            max_abs_x = max_abs_x.max(x.abs());
            max_abs_y = max_abs_y.max(y.abs());
            min_x = min_x.min(x);
            max_x = max_x.max(x);
            min_y = min_y.min(y);
            max_y = max_y.max(y);
        }
    });
    if !max_abs_x.is_finite() || !max_abs_y.is_finite() || !min_x.is_finite() || !min_y.is_finite()
    {
        return None;
    }
    // Update the online mean in one centered, power-of-two frame.  Scaling
    // only edge lengths still leaves `(-1e308 - 1e308)` in the world-space
    // midpoint update, so two symmetric infinite-length components poison a
    // perfectly finite centroid with an intermediate infinity.
    let anchor_x = f64::midpoint(min_x, max_x);
    let anchor_y = f64::midpoint(min_y, max_y);
    // `max - min` may overflow for perfectly finite opposite extremes.  The
    // frame needs a scale that keeps the subtraction safe, not the exact span:
    // the largest endpoint magnitude provides that without turning a valid
    // centroid rescue into `None`.
    let extent_x = max_abs_x;
    let extent_y = max_abs_y;
    let frame = AxisFrame::from_origin_extents(
        Point::new_unchecked_xy(anchor_x, anchor_y),
        extent_x,
        extent_y,
    )?;
    let mut total_length = 0.0_f64;
    let mut mean = (0.0_f64, 0.0_f64);
    // Coordinate axes may use independent scales for their midpoints, but an
    // edge length is Euclidean and therefore needs one common factor.  Scaling
    // its axes independently changes the relative weights of horizontal and
    // vertical components (the 2e308 vs 1.8e308 regression).
    let length_scale = axis_pow2_scale(max_abs_x.max(max_abs_y));
    revisit(&mut |xs, ys| {
        for segment in 0..xs.len().saturating_sub(1) {
            let dx = scaled_residual(xs[segment + 1], xs[segment], length_scale);
            let dy = scaled_residual(ys[segment + 1], ys[segment], length_scale);
            let length = point_distance(
                Point::new_unchecked_xy(0.0, 0.0),
                Point::new_unchecked_xy(dx, dy),
            );
            if !(length > 0.0 && length.is_finite()) {
                continue;
            }
            total_length += length;
            let weight = length / total_length;
            let midpoint_x = f64::midpoint(
                scaled_residual(xs[segment], anchor_x, frame.scale_x()),
                scaled_residual(xs[segment + 1], anchor_x, frame.scale_x()),
            );
            let midpoint_y = f64::midpoint(
                scaled_residual(ys[segment], anchor_y, frame.scale_y()),
                scaled_residual(ys[segment + 1], anchor_y, frame.scale_y()),
            );
            mean.0 += (midpoint_x - mean.0) * weight;
            mean.1 += (midpoint_y - mean.1) * weight;
        }
    });
    (total_length > 0.0).then(|| {
        let world = frame.unframe_xy(XY::new(mean.0, mean.1));
        (canonicalize_zero(world.x), canonicalize_zero(world.y))
    })
}

/// Column kernel for lineal centroid (dimension-1 pass) over one coordinate
/// run.
fn centroid_line_columns(xs: &[f64], ys: &[f64]) -> Option<(f64, f64)> {
    let mut total_length = 0.0_f64;
    let mut line_sum = (0.0_f64, 0.0_f64);
    accumulate_lineal_centroid(xs, ys, &mut total_length, &mut line_sum);
    finish_lineal_centroid_rescued(total_length, line_sum, |visit| visit(xs, ys))
}

/// Zero-area polygon fallback: length-weighted edge midpoints over all rings.
fn centroid_polygon_lineal_columns(
    xs: &[f64],
    ys: &[f64],
    ring_offsets: &[i32],
    rings: std::ops::Range<usize>,
) -> Option<(f64, f64)> {
    let mut total_length = 0.0_f64;
    let mut line_sum = (0.0_f64, 0.0_f64);
    for ring_index in rings.clone() {
        let start = ring_offsets[ring_index] as usize;
        let end = ring_offsets[ring_index + 1] as usize;
        accumulate_lineal_centroid(
            &xs[start..end],
            &ys[start..end],
            &mut total_length,
            &mut line_sum,
        );
    }
    finish_lineal_centroid_rescued(total_length, line_sum, |visit| {
        for ring_index in rings.clone() {
            let start = ring_offsets[ring_index] as usize;
            let end = ring_offsets[ring_index + 1] as usize;
            visit(&xs[start..end], &ys[start..end]);
        }
    })
}

/// Packed-line row kernel: lineal centroid, then the first-vertex puntal
/// anchor.
pub(crate) fn centroid_line_row_columns(xs: &[f64], ys: &[f64]) -> Option<(f64, f64)> {
    centroid_line_columns(xs, ys).or_else(|| xs.first().zip(ys.first()).map(|(&x, &y)| (x, y)))
}

/// Packed-polygon row kernel: areal → lineal boundary → first-shell-vertex
/// cascade.
pub(crate) fn centroid_polygon_row_columns(
    xs: &[f64],
    ys: &[f64],
    ring_offsets: &[i32],
    rings: std::ops::Range<usize>,
) -> Option<(f64, f64)> {
    centroid_polygon_columns(xs, ys, ring_offsets, rings.clone()).or_else(|| {
        centroid_polygon_lineal_columns(xs, ys, ring_offsets, rings.clone()).or_else(|| {
            let start = ring_offsets[rings.start] as usize;
            (start < xs.len()).then(|| (xs[start], ys[start]))
        })
    })
}

/// Single-polygon areal centroid (shell + holes). One classical JTS fan that
/// also tracks extent; shared-scale only when the fan rejects or extent is
/// extreme. Ordinary mid-range polygons pay a single vertex pass (no separate
/// pre-scan).
fn areal_centroid_polygon(polygon: &Polygon) -> Option<(f64, f64)> {
    let shared = || {
        areal_centroid_shared_scale(|visit| {
            polygon_rings(polygon, &mut |ring, hole| {
                let Some((xs, ys)) = ring.xy_columns() else {
                    return;
                };
                visit(xs, ys, hole);
            });
        })
    };
    let mut base = None;
    let mut area_sum2 = 0.0_f64;
    let mut cg3 = (0.0_f64, 0.0_f64);
    let mut min_x = f64::INFINITY;
    let mut max_x = f64::NEG_INFINITY;
    let mut min_y = f64::INFINITY;
    let mut max_y = f64::NEG_INFINITY;
    polygon_rings(polygon, &mut |ring, hole| {
        let Some((xs, ys)) = ring.xy_columns() else {
            return;
        };
        for (&x, &y) in xs.iter().zip(ys.iter()) {
            min_x = min_x.min(x);
            max_x = max_x.max(x);
            min_y = min_y.min(y);
            max_y = max_y.max(y);
        }
        accumulate_ring_centroid(xs, ys, hole, &mut base, &mut area_sum2, &mut cg3);
    });
    let extreme = if min_x.is_finite() {
        let extent = (max_x - min_x).max(max_y - min_y);
        let max_abs = max_x
            .abs()
            .max(min_x.abs())
            .max(max_y.abs())
            .max(min_y.abs());
        extent < AREAL_SHARED_SCALE_MIN_EXTENT || max_abs > AREAL_SHARED_SCALE_MAX_COORD
    } else {
        false
    };
    // Extreme extents: prefer shared-scale (classical moments overflow / lose digits).
    if extreme {
        return shared().or_else(|| finish_areal_centroid(area_sum2, cg3));
    }
    finish_areal_centroid(area_sum2, cg3).or_else(shared)
}

/// Neumaier compensated sum — used only on the cold stable-merge path when
/// opposite-sign extremes cancel. Not the common-path accumulator (measured
/// blanket Neumaier was +58–175% and was rejected).
fn neumaier_sum(values: impl IntoIterator<Item = f64>) -> f64 {
    let mut sum = 0.0_f64;
    let mut correction = 0.0_f64;
    for x in values {
        let t = sum + x;
        if sum.abs() >= x.abs() {
            correction += (sum - t) + x;
        } else {
            correction += (x - t) + sum;
        }
        sum = t;
    }
    sum + correction
}

/// Area/length-weighted merge of component centroids. Ordinary mid-range
/// inputs use a flat weighted sum (bit-stable). Large absolute coordinates or
/// detected cancellation scale by a power-of-two first and Neumaier-sum the
/// scaled weighted coordinates so order cannot move the result (N6).
///
/// Finite weights are normalized by their finite maximum before *both* the
/// denominator and coordinate moments. Infinite `f64` weights are never a
/// mathematical identity: callers replay those components through one shared
/// normalized frame before reaching this finite-only merge.
fn weighted_centroid_merge(parts: &[(f64, f64, f64)]) -> Option<(f64, f64)> {
    if parts.is_empty() {
        return None;
    }
    let mut max_weight = 0.0_f64;
    let mut max_abs_x = 0.0_f64;
    let mut max_abs_y = 0.0_f64;
    for &(w, cx, cy) in parts {
        if !(w > 0.0 && cx.is_finite() && cy.is_finite()) {
            continue;
        }
        if !w.is_finite() {
            return None;
        }
        max_weight = max_weight.max(w);
        max_abs_x = max_abs_x.max(cx.abs());
        max_abs_y = max_abs_y.max(cy.abs());
    }
    if !(max_weight > 0.0 && max_weight.is_finite()) {
        return None;
    }
    // Fast path: unscaled weighted sum when coordinates are mid-range.
    if max_abs_x.max(max_abs_y) <= AREAL_SHARED_SCALE_MAX_COORD {
        let mut sx = 0.0_f64;
        let mut sy = 0.0_f64;
        let mut w_sum = 0.0_f64;
        for &(w, cx, cy) in parts {
            if w > 0.0 && w.is_finite() && cx.is_finite() && cy.is_finite() {
                let normalized = w / max_weight;
                w_sum += normalized;
                sx += normalized * cx;
                sy += normalized * cy;
            }
        }
        let (mx, my) = (sx / w_sum, sy / w_sum);
        if mx.is_finite() && my.is_finite() {
            // Cancellation guard: if the mean is tiny vs max |c|, recompute.
            if max_abs_x.max(max_abs_y) == 0.0
                || mx.abs().max(my.abs()) >= max_abs_x.max(max_abs_y) * f64::EPSILON * 32.0
            {
                return Some((canonicalize_zero(mx), canonicalize_zero(my)));
            }
        }
    }
    // Stable path: scale each coordinate axis independently, then
    // Neumaier-sum the normalized-weight moments. A single scale chosen from
    // huge X makes a finite subnormal Y round to zero before it is merged.
    // The normalization is load-bearing: doing it only in the coordinate sums
    // still lets the denominator overflow.
    let scale_x = axis_pow2_scale(max_abs_x);
    let scale_y = axis_pow2_scale(max_abs_y);
    let inv_scale_x = 1.0 / scale_x;
    let inv_scale_y = 1.0 / scale_y;
    let mut w_sum = 0.0_f64;
    let mut xs = Vec::with_capacity(parts.len());
    let mut ys = Vec::with_capacity(parts.len());
    for &(w, cx, cy) in parts {
        if w > 0.0 && w.is_finite() && cx.is_finite() && cy.is_finite() {
            let normalized = w / max_weight;
            w_sum += normalized;
            xs.push(normalized * (cx * scale_x));
            ys.push(normalized * (cy * scale_y));
        }
    }
    if w_sum <= 0.0 {
        return None;
    }
    Some((
        canonicalize_zero((neumaier_sum(xs) / w_sum) * inv_scale_x),
        canonicalize_zero((neumaier_sum(ys) / w_sum) * inv_scale_y),
    ))
}

fn visit_areal_polygons(shape: &Shape, visit: &mut dyn FnMut(&Polygon)) {
    match shape {
        Shape::Polygon(polygon) => visit(polygon),
        Shape::MultiPolygon(polygons) => {
            for polygon in polygons {
                visit(polygon);
            }
        },
        Shape::GeometryCollection(geometries) => {
            for geometry in geometries {
                visit_areal_polygons(geometry, visit);
            }
        },
        _ => {},
    }
}

fn areal_centroid_shared_scale_shape(shape: &Shape) -> Option<(f64, f64)> {
    areal_centroid_shared_scale(|visit| {
        visit_areal_polygons(shape, &mut |polygon| {
            polygon_rings(polygon, &mut |ring, hole| {
                if let Some((xs, ys)) = ring.xy_columns() {
                    visit(xs, ys, hole);
                }
            });
        });
    })
}

fn areal_has_unrepresentable_component(shape: &Shape) -> bool {
    let mut found = false;
    visit_areal_polygons(shape, &mut |polygon| {
        found |= polygon.area().abs().is_infinite();
    });
    found
}

/// Dimension-2 centroid. Multi-polygon / multi-area collections compute each
/// component's centroid in its own local frame, then area-weight merge —
/// a single global triangle fan cancels across distant equal parts (N6).
pub(crate) fn areal_centroid(shape: &Shape) -> Option<(f64, f64)> {
    match shape {
        Shape::Polygon(polygon) => areal_centroid_polygon(polygon),
        Shape::MultiPolygon(polygons) => {
            if polygons.len() == 1 {
                return areal_centroid_polygon(&polygons[0]);
            }
            if areal_has_unrepresentable_component(shape) {
                return areal_centroid_shared_scale_shape(shape);
            }
            let mut parts = Vec::with_capacity(polygons.len());
            for polygon in polygons {
                let area = polygon.area().abs();
                if area > 0.0
                    && area.is_finite()
                    && let Some((cx, cy)) = areal_centroid_polygon(polygon)
                {
                    parts.push((area, cx, cy));
                }
            }
            weighted_centroid_merge(&parts)
        },
        Shape::GeometryCollection(geometries) => {
            if areal_has_unrepresentable_component(shape) {
                return areal_centroid_shared_scale_shape(shape);
            }
            let mut parts = Vec::new();
            for geometry in geometries {
                if let Some((cx, cy)) = areal_centroid(geometry) {
                    let area = geometry.area().abs();
                    if area > 0.0 && area.is_finite() {
                        parts.push((area, cx, cy));
                    }
                }
            }
            if parts.is_empty() {
                None
            } else if parts.len() == 1 {
                Some((parts[0].1, parts[0].2))
            } else {
                weighted_centroid_merge(&parts)
            }
        },
        _ => None,
    }
}

/// Per-component lineal centroid for a single coordinate run, returned with
/// its length weight for a later stable merge.
fn lineal_component_centroid(xs: &[f64], ys: &[f64]) -> Option<(f64, f64, f64)> {
    let mut total_length = 0.0_f64;
    let mut line_sum = (0.0_f64, 0.0_f64);
    accumulate_lineal_centroid(xs, ys, &mut total_length, &mut line_sum);
    let (cx, cy) = finish_lineal_centroid_rescued(total_length, line_sum, |visit| visit(xs, ys))?;
    (total_length > 0.0).then_some((total_length, cx, cy))
}

/// Dimension-1 centroid: length-weighted edge-midpoint sums over every lineal
/// run — standalone lines AND polygon-ring boundaries (the zero-area fallback,
/// per JTS, where a ring's edges feed the lineal centroid). `None` when the
/// total length is zero. Multi-part lines use component-local centroids then a
/// length-weighted stable merge so order and cancellation cannot move the mean.
pub(crate) fn lineal_centroid(shape: &Shape) -> Option<(f64, f64)> {
    match shape {
        Shape::MultiLineString(lines) if lines.len() > 1 => {
            let mut parts = Vec::with_capacity(lines.len());
            for line in lines {
                if let Some((w, cx, cy)) = lineal_component_centroid(line.xs(), line.ys()) {
                    parts.push((w, cx, cy));
                }
            }
            if parts.iter().any(|(weight, ..)| weight.is_infinite()) {
                lineal_centroid_online(|visit| {
                    for line in lines {
                        visit(line.xs(), line.ys());
                    }
                })
            } else {
                weighted_centroid_merge(&parts)
            }
        },
        Shape::GeometryCollection(_) => {
            // Mixed collections: accumulate per lineal leaf, then merge.
            let mut parts = Vec::new();
            for_each_lineal(shape, &mut |xs, ys| {
                if let Some((w, cx, cy)) = lineal_component_centroid(xs, ys) {
                    parts.push((w, cx, cy));
                }
            });
            if parts.iter().any(|(weight, ..)| weight.is_infinite()) {
                lineal_centroid_online(|visit| for_each_lineal(shape, visit))
            } else if parts.len() > 1 {
                weighted_centroid_merge(&parts)
            } else if let Some((w, cx, cy)) = parts.first() {
                (*w > 0.0).then_some((*cx, *cy))
            } else {
                None
            }
        },
        _ => {
            let mut total_length = 0.0_f64;
            let mut line_sum = (0.0_f64, 0.0_f64);
            for_each_lineal(shape, &mut |xs, ys| {
                accumulate_lineal_centroid(xs, ys, &mut total_length, &mut line_sum);
            });
            finish_lineal_centroid_rescued(total_length, line_sum, |visit| {
                for_each_lineal(shape, &mut |xs, ys| visit(xs, ys));
            })
        },
    }
}

/// Equal-weight stable mean of 2-D points. Ordinary mid-range inputs keep the
/// flat `column_mean2` bit path. Large absolute coordinates or detected
/// cancellation scale first then Neumaier-sum so every permutation of
/// `[1e16, 1, -1e16]` yields exact `1/3`.
fn stable_point_mean(xs: &[f64], ys: &[f64]) -> Option<(f64, f64)> {
    let n = xs.len();
    if n == 0 {
        return None;
    }
    debug_assert_eq!(xs.len(), ys.len());
    let mut max_abs_x = 0.0_f64;
    let mut max_abs_y = 0.0_f64;
    for (&x, &y) in xs.iter().zip(ys.iter()) {
        max_abs_x = max_abs_x.max(x.abs());
        max_abs_y = max_abs_y.max(y.abs());
    }
    let max_abs = max_abs_x.max(max_abs_y);
    if max_abs <= AREAL_SHARED_SCALE_MAX_COORD
        && let Some((mx, my)) = column_mean2(xs, ys)
        && mx.is_finite()
        && my.is_finite()
        && (max_abs == 0.0 || mx.abs().max(my.abs()) >= max_abs * f64::EPSILON * 32.0)
    {
        return Some((mx, my));
    }
    let scale_x = axis_pow2_scale(max_abs_x.max(1.0));
    let scale_y = axis_pow2_scale(max_abs_y.max(1.0));
    let n_f = n as f64;
    let sx = neumaier_sum(xs.iter().map(|&x| x * scale_x));
    let sy = neumaier_sum(ys.iter().map(|&y| y * scale_y));
    Some((
        canonicalize_zero((sx / n_f) / scale_x),
        canonicalize_zero((sy / n_f) / scale_y),
    ))
}

/// Dimension-0 centroid: the average of the geometry's points — actual
/// `Point`/`MultiPoint` vertices plus the first vertex of each (necessarily
/// zero-length, since we only get here when total length is zero) lineal run,
/// matching JTS's zero-length-chain anchor. `None` for a fully empty geometry.
pub(crate) fn point_centroid(shape: &Shape) -> Option<(f64, f64)> {
    if let Shape::MultiPoint(seq) = shape {
        return stable_point_mean(seq.xs(), seq.ys());
    }
    let mut xs = Vec::new();
    let mut ys = Vec::new();
    for_each_fallback_point(shape, &mut |x, y| {
        xs.push(x);
        ys.push(y);
    });
    stable_point_mean(&xs, &ys)
}

fn polygon_rings(polygon: &Polygon, visit: &mut impl FnMut(&Ring, bool)) {
    visit(&polygon.shell, false);
    for hole in polygon.holes.iter() {
        visit(hole, true);
    }
}

/// Visit every lineal coordinate run — standalone lines AND polygon-ring
/// boundaries (shell + holes), recursing collections — the dimension-1
/// traversal. Order matches the single-pass JTS accumulation (shell before
/// holes, per polygon) so the fallback float stream is identical.
fn for_each_lineal(shape: &Shape, visit: &mut (impl FnMut(&[f64], &[f64]) + ?Sized)) {
    match shape {
        Shape::LineString(seq) => visit(seq.xs(), seq.ys()),
        Shape::MultiLineString(lines) => {
            for line in lines {
                visit(line.xs(), line.ys());
            }
        },
        Shape::Polygon(polygon) => polygon_ring_columns(polygon, visit),
        Shape::MultiPolygon(polygons) => {
            for polygon in polygons {
                polygon_ring_columns(polygon, visit);
            }
        },
        Shape::GeometryCollection(geometries) => {
            for geometry in geometries {
                for_each_lineal(geometry, visit);
            }
        },
        _ => {},
    }
}

fn polygon_ring_columns(polygon: &Polygon, visit: &mut (impl FnMut(&[f64], &[f64]) + ?Sized)) {
    if let Some((xs, ys)) = polygon.shell.xy_columns() {
        visit(xs, ys);
    }
    for hole in polygon.holes.iter() {
        if let Some((xs, ys)) = hole.xy_columns() {
            visit(xs, ys);
        }
    }
}

/// Visit the dimension-0 fallback points in JTS order: every `Point`/
/// `MultiPoint` vertex, plus the first vertex of each non-empty lineal run
/// (its zero-length-chain anchor).
fn for_each_fallback_point(shape: &Shape, visit: &mut impl FnMut(f64, f64)) {
    match shape {
        Shape::Point(point) => visit(point.x, point.y),
        Shape::MultiPoint(seq) => {
            for (&x, &y) in std::iter::zip(seq.xs(), seq.ys()) {
                visit(x, y);
            }
        },
        Shape::GeometryCollection(geometries) => {
            for geometry in geometries {
                for_each_fallback_point(geometry, visit);
            }
        },
        // Lineal runs (lines + polygon rings): only reached when total length is
        // zero, so each non-empty run anchors its first vertex (JTS).
        Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Polygon(_)
        | Shape::MultiPolygon(_) => for_each_lineal(shape, &mut |xs, ys| {
            if let Some((&x, &y)) = xs.first().zip(ys.first()) {
                visit(x, y);
            }
        }),
        Shape::Empty(..) => {},
    }
}
