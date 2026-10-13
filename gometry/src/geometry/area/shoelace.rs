use super::*;

/// Ring vertex-pair counts below this use the scalar fold — LLVM auto-vectorizes
/// it to packed `mulpd`/`addpd` (asm-verified) with no per-ring horizontal
/// reduce; at or above it the 512-bit `simd_reduce_f64` path's extra ILP wins
/// (measured: scalar ties to ~64 verts, SIMD wins 15–25% at 256+). Centralized
/// in [`crossover`].
pub(super) const SHOELACE_SIMD_MIN: usize = crossover::OFFSET_PAIR_MEASURE;

/// Scalar shoelace half-sum for the short-ring path. Unit scales retain the
/// existing operation order; non-unit scales form `x*s - origin*s`.
#[expect(
    clippy::indexing_slicing,
    reason = "pairs counts adjacent entries in equal closed-ring columns, including the terminal vertex"
)]
pub(super) fn shoelace_shifted(
    xs: &[f64],
    ys: &[f64],
    pairs: usize,
    ox: f64,
    oy: f64,
    sx: f64,
    sy: f64,
) -> f64 {
    let identity = sx.to_bits() == 1.0_f64.to_bits() && sy.to_bits() == 1.0_f64.to_bits();
    xs[..pairs]
        .iter()
        .zip(&xs[1..=pairs])
        .zip(ys[..pairs].iter().zip(&ys[1..=pairs]))
        .fold(0.0_f64, |area, ((&x0, &x1), (&y0, &y1))| {
            let (sx0, sy0) = if identity {
                (x0 - ox, y0 - oy)
            } else {
                (scaled_residual(x0, ox, sx), scaled_residual(y0, oy, sy))
            };
            let (sx1, sy1) = if identity {
                (x1 - ox, y1 - oy)
            } else {
                (scaled_residual(x1, ox, sx), scaled_residual(y1, oy, sy))
            };
            area.algebraic_add(sx0.algebraic_mul(sy1).algebraic_sub(sx1.algebraic_mul(sy0)))
        })
        / 2.0
}

/// Shifted shoelace measure and its absolute error bound. Long rings compute
/// both in one traversal, reusing the SIMD residuals for the bound without
/// changing the cross-sum reduction.
#[expect(
    clippy::indexing_slicing,
    reason = "pairs and SIMD reduction ranges are bounded by the admitted closed-ring columns"
)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "The binary64 cross fold and product scale form the certified measure and roundoff bound"
)]
pub(super) fn shoelace_shifted_with_roundoff(
    xs: &[f64],
    ys: &[f64],
    pairs: usize,
    ox: f64,
    oy: f64,
    sx: f64,
    sy: f64,
) -> (f64, f64) {
    if pairs < SHOELACE_SIMD_MIN {
        return (
            shoelace_shifted(xs, ys, pairs, ox, oy, sx, sy),
            shoelace_shifted_roundoff(xs, ys, pairs, ox, oy, sx, sy),
        );
    }

    let identity = sx.to_bits() == 1.0_f64.to_bits() && sy.to_bits() == 1.0_f64.to_bits();
    let (x0, _) = xs[..pairs].as_chunks::<REDUCE_LANES>();
    let (x1, _) = xs[1..=pairs].as_chunks::<REDUCE_LANES>();
    let (y0, _) = ys[..pairs].as_chunks::<REDUCE_LANES>();
    let (y1, _) = ys[1..=pairs].as_chunks::<REDUCE_LANES>();
    let (ovx, ovy) = (ReduceSimd::splat(ox), ReduceSimd::splat(oy));
    let sx_v = ReduceSimd::splat(sx);
    let sy_v = ReduceSimd::splat(sy);
    let residual_x = |v: ReduceSimd, o: ReduceSimd| {
        if identity {
            v - o
        } else {
            // x*s - origin*s (scale before subtraction).
            v * sx_v - o * sx_v
        }
    };
    let residual_y = |v: ReduceSimd, o: ReduceSimd| {
        if identity { v - o } else { v * sy_v - o * sy_v }
    };
    let simd_fold = |mut acc: ReduceSimd, mut product_scale: f64, start: usize| {
        let chunk = start / REDUCE_LANES;
        let (rx0, rx1) = (
            residual_x(ReduceSimd::from_array(x0[chunk]), ovx),
            residual_x(ReduceSimd::from_array(x1[chunk]), ovx),
        );
        let (ry0, ry1) = (
            residual_y(ReduceSimd::from_array(y0[chunk]), ovy),
            residual_y(ReduceSimd::from_array(y1[chunk]), ovy),
        );
        acc += rx0 * ry1 - rx1 * ry0;
        for (((x0, x1), y0), y1) in rx0
            .to_array()
            .into_iter()
            .zip(rx1.to_array())
            .zip(ry0.to_array())
            .zip(ry1.to_array())
        {
            product_scale += x0.abs() * y1.abs() + x1.abs() * y0.abs();
        }
        (acc, product_scale)
    };
    let lanes = x0.len() * REDUCE_LANES;
    simd_reduce_f64(
        pairs,
        ReduceSimd::splat(0.0),
        0.0_f64,
        simd_fold,
        |mut product_scale, range| {
            if range.start != 0 {
                debug_assert_eq!(range.start, lanes);
            }
            let start = range.start;
            let end = range.end;
            let x_start = &xs[range.clone()];
            let y_start = &ys[range];
            for ((&x0, &x1), (&y0, &y1)) in x_start
                .iter()
                .zip(xs[start + 1..=end].iter())
                .zip(y_start.iter().zip(ys[start + 1..=end].iter()))
            {
                let (x0, y0) = if identity {
                    (x0 - ox, y0 - oy)
                } else {
                    (scaled_residual(x0, ox, sx), scaled_residual(y0, oy, sy))
                };
                let (x1, y1) = if identity {
                    (x1 - ox, y1 - oy)
                } else {
                    (scaled_residual(x1, ox, sx), scaled_residual(y1, oy, sy))
                };
                product_scale += x0.abs() * y1.abs() + x1.abs() * y0.abs();
            }
            product_scale
        },
        |mut acc, product_scale| {
            if lanes < pairs {
                let (tx0, tx1) = (
                    residual_x(ReduceSimd::load_or_default(&xs[lanes..pairs]), ovx),
                    residual_x(ReduceSimd::load_or_default(&xs[lanes + 1..=pairs]), ovx),
                );
                let (ty0, ty1) = (
                    residual_y(ReduceSimd::load_or_default(&ys[lanes..pairs]), ovy),
                    residual_y(ReduceSimd::load_or_default(&ys[lanes + 1..=pairs]), ovy),
                );
                acc += tx0 * ty1 - tx1 * ty0;
            }
            let area = acc.reduce_sum() / 2.0;
            let operations = pairs.saturating_mul(4).saturating_add(1) as f64;
            (area, product_scale * EPSILON * operations)
        },
    )
}

/// Absolute roundoff bound for the scalar shoelace fold on one ring. The
/// decisive scale is the sum of the two products *before* each subtraction: a
/// finite nonzero cross term can already have lost its answer to product
/// cancellation, so bounding only the result misses the `2^-27` rotated
/// triangle.
pub(super) fn shoelace_shifted_roundoff(
    xs: &[f64],
    ys: &[f64],
    pairs: usize,
    ox: f64,
    oy: f64,
    sx: f64,
    sy: f64,
) -> f64 {
    let identity = sx.to_bits() == 1.0_f64.to_bits() && sy.to_bits() == 1.0_f64.to_bits();
    let mut product_scale = 0.0_f64;
    for (&[x0, x1], &[y0, y1]) in xs
        .array_windows::<2>()
        .zip(ys.array_windows::<2>())
        .take(pairs)
    {
        let (x0, y0) = if identity {
            (x0 - ox, y0 - oy)
        } else {
            (scaled_residual(x0, ox, sx), scaled_residual(y0, oy, sy))
        };
        let (x1, y1) = if identity {
            (x1 - ox, y1 - oy)
        } else {
            (scaled_residual(x1, ox, sx), scaled_residual(y1, oy, sy))
        };
        product_scale += x0.abs() * y1.abs() + x1.abs() * y0.abs();
    }
    // Each edge has two rounded products, one rounded subtraction, and one
    // contribution to the fold.  This conservative IEEE-754 roundoff bound
    // is a property of the performed arithmetic, not a geometry-size knob.
    let operations = pairs.saturating_mul(4).saturating_add(1) as f64;
    product_scale * EPSILON * operations
}

pub(super) fn open_cycle_sum_columns(xs: &[f64], ys: &[f64]) -> ExactSum {
    let mut sum = ExactSum::zero();
    let count = xs.len();
    if count < 2 {
        return sum;
    }
    for (&[x0, x1], &[y0, y1]) in xs.array_windows::<2>().zip(ys.array_windows::<2>()) {
        sum.add_cross(x0, y0, x1, y1);
    }
    let (first_x, first_y) = xs
        .first()
        .zip(ys.first())
        .map(|(&x, &y)| (x, y))
        .expect("a cycle with at least two coordinates has a first coordinate");
    let (last_x, last_y) = xs
        .last()
        .zip(ys.last())
        .map(|(&x, &y)| (x, y))
        .expect("a cycle with at least two coordinates has a last coordinate");
    sum.add_cross(last_x, last_y, first_x, first_y);
    sum
}

pub(super) fn open_cycle_sum<C: Coordinates + ?Sized>(points: &C) -> ExactSum {
    if let Some((xs, ys)) = points.xy_columns() {
        return open_cycle_sum_columns(xs, ys);
    }
    let mut coords = points.iter_coords();
    let Some(first) = coords.next() else {
        return ExactSum::zero();
    };
    let Some(second) = coords.next() else {
        return ExactSum::zero();
    };
    let mut sum = ExactSum::zero();
    sum.add_cross(first.x, first.y, second.x, second.y);
    let mut prev = second;
    for next in coords {
        sum.add_cross(prev.x, prev.y, next.x, next.y);
        prev = next;
    }
    sum.add_cross(prev.x, prev.y, first.x, first.y);
    sum
}

pub(super) fn unsigned_area(sum: &ExactSum) -> AreaMeasure {
    AreaMeasure(sum.round_to_area().abs())
}
