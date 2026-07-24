#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use super::*;
use crate::error::Result;
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
            let snapped = ((value - origin) / size).round() * size + origin;
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
            if !snapped.abs().simd_lt(infinity).all() {
                return Err(GeometryErrorKind::SnapGridTooFine.into());
            }
            let changed = topology_coordinate_bits_simd(value)
                .simd_ne(topology_coordinate_bits_simd(snapped));
            Ok(changed.select(snapped, value))
        },
    )?;
    Ok(out)
}

#[doc(hidden)]
/// The Visvalingam-Whyatt keep mask over raw columns: `keep` is cleared and
/// refilled (reusable across rows); `Some(kept_count)` unless the chain is
/// too short to simplify. The packed-array lane appends kept vertices
/// straight into new CSR columns from this mask.
pub(crate) fn vw_keep(
    xs: &[f64],
    ys: &[f64],
    area_tolerance: f64,
    keep: &mut Vec<bool>,
) -> Option<usize> {
    let count = xs.len();
    if count < 3 {
        return None;
    }
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
/// triangle area reaches `area_tolerance`. Endpoints are pinned, so a
/// closed ring keeps its closure. Survivors keep their Z/M.
pub(crate) fn vw_filter(points: &CoordSeq, area_tolerance: f64) -> CoordSeq {
    let count = points.coord_count();
    if count < 3 {
        return points.clone();
    }
    let (xs, ys) = (points.xs(), points.ys());
    let mut keep = Vec::new();
    if vw_keep(xs, ys, area_tolerance, &mut keep).is_none() {
        return points.clone();
    }
    points.select(
        keep.iter()
            .enumerate()
            .filter_map(|(index, &kept)| kept.then_some(index)),
    )
}
