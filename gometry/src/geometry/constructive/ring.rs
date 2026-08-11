use std::simd::cmp::{SimdPartialEq as _, SimdPartialOrd as _};
use std::simd::num::SimdFloat as _;
use std::simd::{Select as _, StdFloat as _};

use crate::error::Result;
use crate::geometry::constructive::reversed_points;
use crate::geometry::{
    AxisFrame, BinaryHeap, CoordSeq, Coordinates, GeometryErrorKind, Ordering, Point, REDUCE_LANES,
    ReduceSimd, compare_f64, compare_point_slices, compare_points, power_of_two_exponent,
    ring_winding, same_point, same_topological_coordinate, scale_by_power_of_two, simd_map_f64,
    topology_coordinate_bits_simd, try_simd_map_f64, wrap_index,
};
pub(crate) fn normalized_line<C: Coordinates + ?Sized>(points: &C) -> Vec<Point> {
    let forward: Vec<Point> = points.iter_coords().collect();
    // A CLOSED line canonicalizes to the lexicographically smallest of all
    // its presentations (every rotation, both directions) — a pure minimum
    // over the orbit: total for self-intersecting rings, no orientation
    // predicate. Open lines pick the smaller of their two directions; the
    // same "smallest presentation" principle, one comparator.
    if forward.len() > 2 && same_point(forward[0], forward[forward.len() - 1]) {
        let backward = rotate_ring_min_first(reversed_points(&forward));
        let forward = rotate_ring_min_first(forward);
        return if compare_point_slices(&backward, &forward) == Ordering::Less {
            backward
        } else {
            forward
        };
    }
    let backward = reversed_points(&forward);
    if compare_point_slices(&backward, &forward) == Ordering::Less {
        backward
    } else {
        forward
    }
}

pub(crate) fn orient_ring<C: Coordinates + ?Sized>(points: &C, clockwise: bool) -> Vec<Point> {
    if ring_winding(points).reverse_for_clockwise(clockwise) {
        reversed_points(points)
    } else {
        points.iter_coords().collect()
    }
}

pub(crate) fn canonical_ring<C: Coordinates + ?Sized>(points: &C, clockwise: bool) -> Vec<Point> {
    rotate_ring_min_first(orient_ring(points, clockwise))
}

/// Column-level [`canonical_ring`]: orientation by the shoelace sign,
/// the min-vertex start by one allocation-free scan (stack `Point`s per
/// candidate; DUPLICATE minima fall back to the exact rotation-compare
/// path), and the output written as two slice copies per ordinate
/// column — no per-vertex `Vec<Point>` round-trips. Equality with the
/// `Vec<Point>` form is preserved bit-for-bit.
pub(in crate::geometry) fn canonical_ring_seq(ring: &CoordSeq, clockwise: bool) -> CoordSeq {
    let count = ring.coord_count();
    let closed = count > 1 && same_point(ring.point_at(0), ring.point_at(count - 1));
    let unique = if closed { count - 1 } else { count };
    if unique <= 1 {
        return ring.clone();
    }
    let reverse = ring_winding(ring).reverse_for_clockwise(clockwise);
    let mut start = 0_usize;
    let mut duplicate_min = false;
    if ring.zs().is_none() && ring.ms().is_none() {
        // XY rings (the common case) compare straight off the columns —
        // no per-candidate `Point` construction.
        let (xs, ys) = (ring.xs(), ring.ys());
        for index in 1..unique {
            match compare_f64(xs[index], xs[start]).then_with(|| compare_f64(ys[index], ys[start]))
            {
                Ordering::Less => {
                    start = index;
                    duplicate_min = false;
                },
                Ordering::Equal => duplicate_min = true,
                Ordering::Greater => {},
            }
        }
    } else {
        for index in 1..unique {
            match compare_points(&ring.point_at(index), &ring.point_at(start)) {
                Ordering::Less => {
                    start = index;
                    duplicate_min = false;
                },
                Ordering::Equal => duplicate_min = true,
                Ordering::Greater => {},
            }
        }
    }
    if duplicate_min {
        return CoordSeq::from(canonical_ring(ring, clockwise));
    }
    // After reversal the ring still starts at the min vertex; the walk
    // direction flips. Emit per ordinate column in two contiguous chunks.
    let rotate = |column: &[f64]| -> Box<[f64]> {
        let mut output = Vec::with_capacity(count);
        if reverse {
            output.extend(column[..=start].iter().rev());
            output.extend(column[start + 1..unique].iter().rev());
        } else {
            output.extend_from_slice(&column[start..unique]);
            output.extend_from_slice(&column[..start]);
        }
        if closed {
            output.push(output[0]);
        }
        output.into_boxed_slice()
    };
    CoordSeq::from_columns(
        rotate(ring.xs()).into(),
        rotate(ring.ys()).into(),
        ring.zs().map(rotate).map(Into::into),
        ring.ms().map(rotate).map(Into::into),
    )
}

/// Rotate a (closed or open) ring's unique vertices so the minimum one
/// leads, preserving the closure point — the rotation half of
/// [`canonical_ring`], shared by closed-`LineString` normalization.
pub(crate) fn rotate_ring_min_first(points: Vec<Point>) -> Vec<Point> {
    let closed = points.len() > 1 && same_point(points[0], points[points.len() - 1]);
    let unique_len = if closed {
        points.len() - 1
    } else {
        points.len()
    };
    if unique_len <= 1 {
        return points;
    }

    let start = (1..unique_len).fold(0, |best, candidate| {
        if compare_ring_rotation(&points, candidate, best, unique_len) == Ordering::Less {
            candidate
        } else {
            best
        }
    });
    let mut output = (0..unique_len)
        .map(|offset| points[wrap_index(start + offset, unique_len)])
        .collect::<Vec<_>>();
    if closed {
        output.push(output[0]);
    }
    output
}

pub(crate) fn compare_ring_rotation(
    points: &[Point],
    left_start: usize,
    right_start: usize,
    len: usize,
) -> Ordering {
    (0..len)
        .map(|offset| {
            compare_points(
                &points[wrap_index(left_start + offset, len)],
                &points[wrap_index(right_start + offset, len)],
            )
        })
        .find(|order| *order != Ordering::Equal)
        .unwrap_or(Ordering::Equal)
}

/// Build the 6-tuple `(a, b, d, e, xoff, yoff)` for a linear map `[a, b; d, e]`
/// applied about `origin`: translate(-origin), apply linear,
/// translate(+origin).
pub(crate) fn affine_about(a: f64, b: f64, d: f64, e: f64, origin: (f64, f64)) -> [f64; 6] {
    let (ox, oy) = origin;
    let xoff = ox - (a * ox + b * oy);
    let yoff = oy - (d * ox + e * oy);
    [a, b, d, e, xoff, yoff]
}

/// Decimal scales for the boundary-validated quantize precisions (0..=15,
/// enforced at the Python boundary) — a table lookup instead of a runtime
/// `powi` loop per quantize call.
pub(crate) const DECIMAL_SCALES: [f64; 16] = [
    1e0, 1e1, 1e2, 1e3, 1e4, 1e5, 1e6, 1e7, 1e8, 1e9, 1e10, 1e11, 1e12, 1e13, 1e14, 1e15,
];

pub(crate) fn decimal_scale(precision: i32) -> f64 {
    debug_assert!((0..=15).contains(&precision));
    DECIMAL_SCALES[precision as usize]
}

pub(crate) fn quantize_to_scale(value: f64, scale: f64) -> f64 {
    let quantized = (value * scale).round() / scale;
    // The grid falls outside f64's representable resolution when `scale`
    // over/underflows (|precision| past ~308) or when `value * scale` overflows
    // for a large finite coordinate. In every such case rounding is a no-op the
    // type cannot express, so keep the original value rather than yielding the
    // inf/NaN that would otherwise be stored unvalidated as a coordinate.
    if quantized.is_finite() && !same_topological_coordinate(value, quantized) {
        quantized
    } else {
        value
    }
}

/// SIMD quantize of one ordinate column: `(v*scale).round()/scale`, keeping the
/// original value wherever the result is non-finite (the [`quantize_to_scale`]
/// overflow fallback, lane-selected). 8-wide; `StdFloat::round` rounds
/// half-away-from-zero, matching `f64::round`, so this is bit-identical to the
/// scalar per-element map (which compiled to a scalar loop through the
/// closure).
pub(crate) fn quantize_column_simd(column: &[f64], scale: f64) -> Box<[f64]> {
    let n = column.len();
    let mut out = vec![0.0_f64; n].into_boxed_slice();
    let vscale = ReduceSimd::splat(scale);
    let infinity = ReduceSimd::splat(f64::INFINITY);
    simd_map_f64(
        column,
        &mut out,
        |value| quantize_to_scale(value, scale),
        |value| {
            let quantized = (value * vscale).round() / vscale;
            // `abs() < INFINITY` is the per-lane finiteness test (±inf and NaN both fail).
            let finite = quantized.abs().simd_lt(infinity);
            let changed = topology_coordinate_bits_simd(value)
                .simd_ne(topology_coordinate_bits_simd(quantized));
            (finite & changed).select(quantized, value)
        },
    );
    out
}

/// SIMD snap of one ordinate column onto `origin + k * size`:
/// `((v - origin) / size).round() * size + origin`. 8-wide, same op order as
/// the scalar form (bit-identical; `StdFloat::round` is half-away). Errors with
/// `SnapGridTooFine` if any snapped value is non-finite (the grid is finer than
/// the coordinate's ULP) — the same condition the scalar path rejects.
pub(crate) fn snap_column_simd(
    column: &[f64],
    origin: f64,
    size: f64,
) -> Result<Box<[f64]>, crate::error::Error> {
    let n = column.len();
    let mut out = vec![0.0_f64; n].into_boxed_slice();
    let (vorigin, vsize) = (ReduceSimd::splat(origin), ReduceSimd::splat(size));
    let infinity = ReduceSimd::splat(f64::INFINITY);
    try_simd_map_f64::<crate::error::Error>(
        column,
        &mut out,
        |value| {
            let snapped = stable_snap_ordinate(value, origin, size);
            if !snapped.is_finite() {
                return Err(GeometryErrorKind::SnapGridTooFine.into());
            }
            if same_topological_coordinate(value, snapped) {
                Ok(value)
            } else {
                Ok(snapped)
            }
        },
        |value| {
            let snapped = ((value - vorigin) / vsize).round() * vsize + vorigin;
            // SIMD path may overflow extreme value/origin pairs; fall back
            // per-lane via the scalar stable form when any lane is non-finite.
            if !snapped.abs().simd_lt(infinity).all() {
                let mut out_lane = snapped.to_array();
                let values = value.to_array();
                for lane in 0..REDUCE_LANES {
                    if !out_lane[lane].is_finite() {
                        out_lane[lane] = stable_snap_ordinate(values[lane], origin, size);
                        if !out_lane[lane].is_finite() {
                            return Err(GeometryErrorKind::SnapGridTooFine.into());
                        }
                    }
                }
                let snapped = ReduceSimd::from_array(out_lane);
                let changed = topology_coordinate_bits_simd(value)
                    .simd_ne(topology_coordinate_bits_simd(snapped));
                return Ok(changed.select(snapped, value));
            }
            let changed = topology_coordinate_bits_simd(value)
                .simd_ne(topology_coordinate_bits_simd(snapped));
            Ok(changed.select(snapped, value))
        },
    )?;
    Ok(out)
}

/// `((v - origin) / size).round() * size + origin` with power-of-two
/// pre-scale so `v=1e308, origin=-1e308, size=1e308` stays finite.
///
/// Algebra: `k = round((v - origin) / size)`, result = `origin + k * size`.
/// Scale `v` and `origin` together before the subtraction so the difference
/// does not overflow; `size` stays in world units for the final multiply.
fn stable_snap_ordinate(value: f64, origin: f64, size: f64) -> f64 {
    let classic = ((value - origin) / size).round() * size + origin;
    if classic.is_finite() {
        return classic;
    }
    if size == 0.0 || !size.is_finite() {
        return classic;
    }
    // Avoid (value - origin) overflow: form k = round(value/size - origin/size),
    // then result = size * (origin/size + k) so k*size never materializes alone
    // (2 * 1e308 overflows, but 1e308 * ( -1 + 2) is fine).
    let v_over = value / size;
    let o_over = origin / size;
    if v_over.is_finite() && o_over.is_finite() {
        let k = (v_over - o_over).round();
        let result = size * (o_over + k);
        if result.is_finite() {
            return result;
        }
    }
    // Last resort: scale value and origin before subtraction.
    let max_abs = value.abs().max(origin.abs());
    if max_abs == 0.0 || !max_abs.is_finite() {
        return classic;
    }
    let exp = max_abs.log2().floor();
    let scale_exp = (-exp).clamp(-1022.0, 1023.0) as i32;
    let scale = f64::from_bits(((scale_exp + 1023) as u64) << 52);
    let delta = value * scale - origin * scale;
    let sized = size * scale;
    if sized == 0.0 || !sized.is_finite() || !delta.is_finite() {
        return classic;
    }
    let k = (delta / sized).round();
    let result = (origin * scale + k * sized) / scale;
    if result.is_finite() { result } else { classic }
}

/// Chain-wide per-axis power-of-two frame for VW scoring, returning framed
/// columns (when needed) and the area threshold in that frame.
fn vw_frame_columns(
    xs: &[f64],
    ys: &[f64],
    distance_tolerance: f64,
) -> (Option<Vec<f64>>, Option<Vec<f64>>, f64) {
    let ox = xs[0];
    let oy = ys[0];
    let mut max_abs_x = ox.abs();
    let mut max_abs_y = oy.abs();
    for index in 0..xs.len() {
        max_abs_x = max_abs_x.max(xs[index].abs());
        max_abs_y = max_abs_y.max(ys[index].abs());
    }
    let world_area_tol = crate::geometry::vw_area_tolerance(distance_tolerance);
    let max_abs = max_abs_x.max(max_abs_y);
    let use_frame = max_abs > 0.0
        && max_abs.is_finite()
        && (max_abs_x < 1e-8
            || max_abs_y < 1e-8
            || max_abs_x > 1e8
            || max_abs_y > 1e8
            || !world_area_tol.is_normal());
    if !use_frame {
        return (None, None, world_area_tol);
    }
    let Some(frame) =
        AxisFrame::from_origin_extents(Point::new_unchecked_xy(ox, oy), max_abs_x, max_abs_y)
    else {
        return (None, None, world_area_tol);
    };
    let fxs: Vec<f64> = xs.iter().map(|&x| frame.frame_xy(x, oy).x).collect();
    let fys: Vec<f64> = ys.iter().map(|&y| frame.frame_xy(ox, y).y).collect();
    // A VW score is an area, so map its distance-square threshold through both
    // exact scale exponents together.  Forming the world threshold first
    // would make a valid huge tolerance `+inf` before the rescue could scale
    // it back down.
    let exponent = power_of_two_exponent(frame.scale_x()) + power_of_two_exponent(frame.scale_y());
    let framed_area =
        0.5 * distance_tolerance * scale_by_power_of_two(distance_tolerance, exponent);
    (Some(fxs), Some(fys), framed_area)
}

#[doc(hidden)]
/// Visvalingam-Whyatt keep mask. `distance_tolerance` is the public
/// distance-scale threshold (same units as `simplify`/`coverage_simplify`);
/// the area threshold is derived **after** any chain-wide power-of-two frame
/// so huge inputs (tol² → +inf in world units) still compare correctly.
pub(crate) fn vw_keep(
    xs: &[f64],
    ys: &[f64],
    distance_tolerance: f64,
    keep: &mut Vec<bool>,
) -> Option<usize> {
    let count = xs.len();
    if count < 3 {
        return None;
    }
    let (framed_xs, framed_ys, area_tol) = vw_frame_columns(xs, ys, distance_tolerance);
    let xs = framed_xs.as_deref().unwrap_or(xs);
    let ys = framed_ys.as_deref().unwrap_or(ys);
    let area_tolerance = area_tol;
    let area = |a: usize, b: usize, c: usize| -> f64 {
        0.5 * ((xs[b] - xs[a]) * (ys[c] - ys[a]) - (ys[b] - ys[a]) * (xs[c] - xs[a])).abs()
    };
    keep.clear();
    keep.resize(count, true);
    let mut prev: Vec<u32> = (0..count as u32).map(|i| i.saturating_sub(1)).collect();
    let mut next: Vec<u32> = (1..=count as u32).collect();
    let mut current: Vec<u64> = vec![0; count];
    let mut heap: BinaryHeap<std::cmp::Reverse<(u64, u32)>> = BinaryHeap::with_capacity(count);
    // Interior indices 1..count-1: batch the initial heap fill with f64x8
    // shoelace crosses; neighbor refresh stays scalar (irregular indices)
    // and only re-queues vertices still below tolerance (cuts heap churn).
    let interior_end = count - 1;
    let interior_count = count.saturating_sub(2);
    let simd_lanes = interior_count - interior_count % REDUCE_LANES;
    let half = ReduceSimd::splat(0.5);
    let mut index = 1_usize;
    while index < 1 + simd_lanes {
        let xa = ReduceSimd::from_slice(&xs[index - 1..index - 1 + REDUCE_LANES]);
        let xb = ReduceSimd::from_slice(&xs[index..index + REDUCE_LANES]);
        let xc = ReduceSimd::from_slice(&xs[index + 1..index + 1 + REDUCE_LANES]);
        let ya = ReduceSimd::from_slice(&ys[index - 1..index - 1 + REDUCE_LANES]);
        let yb = ReduceSimd::from_slice(&ys[index..index + REDUCE_LANES]);
        let yc = ReduceSimd::from_slice(&ys[index + 1..index + 1 + REDUCE_LANES]);
        let cross = (xb - xa) * (yc - ya) - (yb - ya) * (xc - xa);
        let areas = (cross * half).abs();
        for (lane, area_value) in areas.to_array().into_iter().enumerate() {
            let vertex = index + lane;
            let bits = area_value.to_bits();
            current[vertex] = bits;
            heap.push(std::cmp::Reverse((bits, vertex as u32)));
        }
        index += REDUCE_LANES;
    }
    for (index, slot) in current
        .iter_mut()
        .enumerate()
        .take(interior_end)
        .skip(1 + simd_lanes)
    {
        let bits = area(index - 1, index, index + 1).to_bits();
        *slot = bits;
        heap.push(std::cmp::Reverse((bits, index as u32)));
    }
    let tolerance_bits = area_tolerance.to_bits();
    while let Some(std::cmp::Reverse((bits, index))) = heap.pop() {
        let index = index as usize;
        if !keep[index] || bits != current[index] {
            continue; // stale entry
        }
        if bits >= tolerance_bits {
            break;
        }
        keep[index] = false;
        let (before, after) = (prev[index] as usize, next[index] as usize);
        next[before] = after as u32;
        prev[after] = before as u32;
        for neighbor in [before, after] {
            if neighbor == 0 || neighbor == count - 1 || !keep[neighbor] {
                continue;
            }
            let bits = area(prev[neighbor] as usize, neighbor, next[neighbor] as usize).to_bits();
            current[neighbor] = bits;
            if bits < tolerance_bits {
                heap.push(std::cmp::Reverse((bits, neighbor as u32)));
            }
        }
    }
    Some(keep.iter().filter(|&&kept| kept).count())
}

/// One Visvalingam-Whyatt pass over a coordinate sequence: interior
/// vertices drop in ascending effective-area order (binary heap over the
/// non-negative area BITS — bit order IS numeric order — with lazy
/// invalidation over doubly-linked neighbors) until every survivor's
/// triangle area reaches the distance-scale threshold. Endpoints are pinned,
/// so a closed ring keeps its closure. Survivors keep their Z/M.
pub(crate) fn vw_filter(points: &CoordSeq, distance_tolerance: f64) -> CoordSeq {
    let count = points.coord_count();
    if count < 3 {
        return points.clone();
    }
    let (xs, ys) = (points.xs(), points.ys());
    let mut keep = Vec::new();
    if vw_keep(xs, ys, distance_tolerance, &mut keep).is_none() {
        return points.clone();
    }
    points.select(
        keep.iter()
            .enumerate()
            .filter_map(|(index, &kept)| kept.then_some(index)),
    )
}

#[cfg(test)]
mod vw_frame_tests {
    use super::*;

    #[test]
    fn opposite_sign_extremes_scale_each_operand_before_the_frame_subtraction() {
        let xs = [-1e308, 0.0, 1e308];
        let ys = [0.0, 1e307, 0.0];
        // This calls the exact VW frame used by simplify. Its finite output
        // proves the residual is `x*s - origin*s`; `(x-origin)*s` overflows
        // before it can be scaled.
        let (Some(fxs), Some(fys), _) = vw_frame_columns(&xs, &ys, 5e307) else {
            panic!("opposite-sign extreme coordinates require a VW frame");
        };
        assert!(fxs.iter().chain(&fys).all(|value| value.is_finite()));
        assert_eq!(fxs[0].to_bits(), 0.0_f64.to_bits());
        assert!(fxs[2] > fxs[1] && fxs[1] > fxs[0]);
    }
}
