use std::simd::cmp::{SimdPartialEq, SimdPartialOrd};
use std::simd::num::SimdFloat;

use crate::geometry::*;
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
    // fold sums the ring's UNSIGNED triangle-fan contributions; the JTS
    // sign rule (a shell contributes positively when CW, a hole when CCW)
    // is applied to the whole-ring total here — exact since `sign` is ±1.0.
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
    (area_sum2.abs() > 0.0).then(|| {
        (
            canonicalize_zero(cg3.0 / 3.0 / area_sum2),
            canonicalize_zero(cg3.1 / 3.0 / area_sum2),
        )
    })
}

/// Column kernel for polygonal centroid (dimension-2 pass) over one polygon's
/// ring window range.
fn centroid_polygon_columns(
    xs: &[f64],
    ys: &[f64],
    ring_offsets: &[i32],
    rings: std::ops::Range<usize>,
) -> Option<(f64, f64)> {
    let mut base = None;
    let mut area_sum2 = 0.0_f64;
    let mut cg3 = (0.0_f64, 0.0_f64);
    for ring_index in rings.clone() {
        let start = ring_offsets[ring_index] as usize;
        let end = ring_offsets[ring_index + 1] as usize;
        accumulate_ring_centroid(
            &xs[start..end],
            &ys[start..end],
            ring_index > rings.start,
            &mut base,
            &mut area_sum2,
            &mut cg3,
        );
    }
    finish_areal_centroid(area_sum2, cg3)
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
    range.fold((0.0_f64, 0.0_f64, 0.0_f64), |(tl, lx, ly), segment| {
        let (dx, dy) = (xs[segment + 1] - xs[segment], ys[segment + 1] - ys[segment]);
        let length = (dx * dx + dy * dy).sqrt();
        (
            tl + length,
            lx + length * f64::midpoint(xs[segment], xs[segment + 1]),
            ly + length * f64::midpoint(ys[segment], ys[segment + 1]),
        )
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
    revisit: impl FnOnce(&mut dyn FnMut(&[f64], &[f64])),
) -> Option<(f64, f64)> {
    match finish_lineal_centroid(total_length, line_sum) {
        Some((x, y)) if x.is_finite() && y.is_finite() => Some((x, y)),
        Some(_) => lineal_centroid_online(revisit),
        None => None,
    }
}

/// Overflow-safe weighted online mean of edge midpoints (weight = edge length):
/// `mean += (mid - mean) · (length / Σlength)` per segment, so no oversized
/// `length·midpoint` product is ever formed. The correctness rescue for the
/// lineal centroid; `revisit` replays the same coordinate runs the fast path
/// summed. Plain arithmetic — a correctness fallback, never a measurement fold.
fn lineal_centroid_online(
    revisit: impl FnOnce(&mut dyn FnMut(&[f64], &[f64])),
) -> Option<(f64, f64)> {
    let mut total_length = 0.0_f64;
    let mut mean = (0.0_f64, 0.0_f64);
    revisit(&mut |xs, ys| {
        for segment in 0..xs.len().saturating_sub(1) {
            let length = point_distance(
                Point::new_unchecked_xy(xs[segment], ys[segment]),
                Point::new_unchecked_xy(xs[segment + 1], ys[segment + 1]),
            );
            if length == 0.0 {
                continue;
            }
            total_length += length;
            let weight = length / total_length;
            mean.0 += (f64::midpoint(xs[segment], xs[segment + 1]) - mean.0) * weight;
            mean.1 += (f64::midpoint(ys[segment], ys[segment + 1]) - mean.1) * weight;
        }
    });
    (total_length > 0.0).then_some(mean)
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
        for ring_index in rings {
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

/// Dimension-2 centroid: the JTS triangle-fan area sums about one global base
/// point (the first ring vertex seen). `None` when the total signed area is
/// zero — the caller then falls to [`lineal_centroid`]. This is the ONLY pass a
/// normal nonzero-area polygon needs; mirrors JTS `Centroid` arithmetic exactly
/// (operation order, sign rule) so results stay WKT-identical to GEOS.
pub(crate) fn areal_centroid(shape: &Shape) -> Option<(f64, f64)> {
    let mut base = None;
    let mut area_sum2 = 0.0_f64;
    let mut cg3 = (0.0_f64, 0.0_f64);
    for_each_ring(shape, &mut |ring, hole| {
        let Some((xs, ys)) = ring.xy_columns() else {
            return;
        };
        accumulate_ring_centroid(xs, ys, hole, &mut base, &mut area_sum2, &mut cg3);
    });
    finish_areal_centroid(area_sum2, cg3)
}

/// Dimension-1 centroid: length-weighted edge-midpoint sums over every lineal
/// run — standalone lines AND polygon-ring boundaries (the zero-area fallback,
/// per JTS, where a ring's edges feed the lineal centroid). `None` when the
/// total length is zero. The per-edge `sqrt` lives here, reached only when
/// there is no areal component.
pub(crate) fn lineal_centroid(shape: &Shape) -> Option<(f64, f64)> {
    let mut total_length = 0.0_f64;
    let mut line_sum = (0.0_f64, 0.0_f64);
    for_each_lineal(shape, &mut |xs, ys| {
        accumulate_lineal_centroid(xs, ys, &mut total_length, &mut line_sum);
    });
    finish_lineal_centroid_rescued(total_length, line_sum, |visit| {
        for_each_lineal(shape, &mut |xs, ys| visit(xs, ys));
    })
}

/// Dimension-0 centroid: the average of the geometry's points — actual
/// `Point`/`MultiPoint` vertices plus the first vertex of each (necessarily
/// zero-length, since we only get here when total length is zero) lineal run,
/// matching JTS's zero-length-chain anchor. `None` for a fully empty geometry.
pub(crate) fn point_centroid(shape: &Shape) -> Option<(f64, f64)> {
    if let Shape::MultiPoint(seq) = shape {
        return column_mean2(seq.xs(), seq.ys());
    }
    let mut count = 0_usize;
    let mut sum = (0.0_f64, 0.0_f64);
    for_each_fallback_point(shape, &mut |x, y| {
        count += 1;
        sum.0 += x;
        sum.1 += y;
    });
    (count > 0).then(|| {
        let count = count as f64;
        (sum.0 / count, sum.1 / count)
    })
}

/// Visit every polygon ring (shell then holes, recursing collections) with a
/// `hole` flag — the dimension-2 traversal.
fn for_each_ring(shape: &Shape, visit: &mut impl FnMut(&Ring, bool)) {
    match shape {
        Shape::Polygon(polygon) => polygon_rings(polygon, visit),
        Shape::MultiPolygon(polygons) => {
            for polygon in polygons {
                polygon_rings(polygon, visit);
            }
        },
        Shape::GeometryCollection(geometries) => {
            for geometry in geometries {
                for_each_ring(geometry, visit);
            }
        },
        _ => {},
    }
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
fn for_each_lineal(shape: &Shape, visit: &mut impl FnMut(&[f64], &[f64])) {
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

fn polygon_ring_columns(polygon: &Polygon, visit: &mut impl FnMut(&[f64], &[f64])) {
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
