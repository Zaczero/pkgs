use std::simd::cmp::{SimdPartialEq as _, SimdPartialOrd as _};
use std::simd::num::SimdFloat as _;
use std::simd::{Select as _, StdFloat as _};

use crate::error::Result;
use crate::geometry::constructive::reversed_points;
use crate::geometry::{
    AxisFrame, BinaryHeap, CoordSeq, Coordinates, GeometryErrorKind, Ordering, Point, REDUCE_LANES,
    ReduceSimd, Ring, axis_pow2_scale, compare_f64, compare_points, power_of_two_exponent,
    ring_winding, same_point, same_topological_coordinate, scale_by_power_of_two, simd_map_f64,
    topology_coordinate_bits_simd, try_simd_map_f64, wrap_index,
};

pub(crate) fn normalized_line(points: &CoordSeq) -> Result<CoordSeq> {
    let count = points.len();
    // A CLOSED line canonicalizes to the lexicographically smallest of all
    // its presentations (every rotation, both directions) — a pure minimum
    // over the orbit: total for self-intersecting rings, no orientation
    // predicate. Open lines pick the smaller of their two directions; the
    // same "smallest presentation" principle, one comparator.
    if count > 2 && same_point(points.point_at(0), points.point_at(count - 1)) {
        let unique = count - 1;
        let forward = ring_rotation_start(points, 0..unique, false);
        // Reversal retains the stored closing point, which may differ in Z/M.
        let backward = ring_rotation_start(points, 1..count, true);
        let backward_offset = count - 1 - backward;
        let reverse = (0..unique)
            .map(|offset| {
                compare_points(
                    &points.point_at(count - 1 - wrap_index(backward_offset + offset, unique)),
                    &points.point_at(wrap_index(forward + offset, unique)),
                )
            })
            .find(|order| *order != Ordering::Equal)
            == Some(Ordering::Less);
        return if reverse {
            rotated_ring_columns(points, 1..count, backward, true, true)
        } else {
            rotated_ring_columns(points, 0..unique, forward, false, true)
        };
    }
    let reverse = points
        .iter_coords()
        .rev()
        .zip(points.iter_coords())
        .map(|(left, right)| compare_points(&left, &right))
        .find(|order| *order != Ordering::Equal)
        == Some(Ordering::Less);
    if reverse {
        points.reversed()
    } else {
        Ok(points.clone())
    }
}

pub(crate) fn orient_ring<C: Coordinates + ?Sized>(points: &C, clockwise: bool) -> Vec<Point> {
    if ring_winding(points).reverse_for_clockwise(clockwise) {
        reversed_points(points)
    } else {
        points.iter_coords().collect()
    }
}

/// Orient an admitted ring without copying its coordinate columns when the
/// requested winding already matches.
pub(in crate::geometry) fn oriented_ring(ring: &Ring, clockwise: bool) -> Result<Ring> {
    if ring_winding(ring).reverse_for_clockwise(clockwise) {
        ring.reversed()
    } else {
        Ok(ring.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::{MOrdinate, Polygon, Shape, XY, ZOrdinate};
    use crate::test_support::allocation_count;

    fn xyzm_ring(points: &[(f64, f64, f64, f64)]) -> Ring {
        Ring::closed(
            points
                .iter()
                .map(|&(x, y, z, m)| {
                    Point::new_axes(x, y, ZOrdinate(Some(z)), MOrdinate(Some(m)))
                        .expect("finite XYZM test coordinate")
                })
                .collect(),
        )
        .expect("closed XYZM test ring")
    }

    #[test]
    fn least_rotation_matches_exhaustive_orbits_and_bounds_repeated_work() {
        for len in 1..=8 {
            for mut code in 0..3_usize.pow(len as u32) {
                let values: Vec<_> = std::iter::repeat_with(|| {
                    let value = code % 3;
                    code /= 3;
                    value
                })
                .take(len)
                .collect();
                let expected = (0..len)
                    .min_by_key(|&start| {
                        (0..len)
                            .map(|offset| values[(start + offset) % len])
                            .collect::<Vec<_>>()
                    })
                    .unwrap();
                assert_eq!(
                    least_rotation_by(len, |left, right| values[left].cmp(&values[right])),
                    expected,
                    "{values:?}"
                );
            }
        }
        let len = 10_000;
        let comparisons = std::cell::Cell::new(0);
        assert_eq!(
            least_rotation_by(len, |_, _| {
                comparisons.set(comparisons.get() + 1);
                Ordering::Equal
            }),
            0
        );
        assert!(comparisons.get() <= 2 * len);
    }

    #[test]
    fn column_normalization_matches_full_ordinate_presentation_orbit() {
        let point = |x, z| Point::new_axes(x, x, ZOrdinate(Some(z)), MOrdinate(Some(-z))).unwrap();
        for input in [
            vec![],
            vec![point(2., 3.), point(1., 4.)],
            vec![point(0., 9.), point(1., 3.), point(0., 1.)],
            vec![
                point(0., 1.),
                point(2., 2.),
                point(0., 1.),
                point(1., 3.),
                point(0., 1.),
            ],
            vec![
                point(-0., 1.),
                point(2., 2.),
                point(0., 1.),
                point(1., 3.),
                point(-0., 1.),
            ],
        ] {
            let mut presentations = Vec::new();
            for reverse in [false, true] {
                let mut sequence = input.clone();
                if reverse {
                    sequence.reverse();
                }
                if sequence.len() > 2 && same_point(sequence[0], sequence[sequence.len() - 1]) {
                    sequence.pop();
                    for start in 0..sequence.len() {
                        let mut rotated = sequence.clone();
                        rotated.rotate_left(start);
                        rotated.push(rotated[0]);
                        presentations.push(rotated);
                    }
                } else {
                    presentations.push(sequence);
                }
            }
            let expected = presentations
                .into_iter()
                .min_by(crate::geometry::compare_point_slices)
                .unwrap();
            let source = CoordSeq::try_from_points(&input).unwrap();
            let actual = normalized_line(&source).unwrap();
            let bits = |point: Point| {
                (
                    point.x.to_bits(),
                    point.y.to_bits(),
                    point.z().map(f64::to_bits),
                    point.m().map(f64::to_bits),
                )
            };
            assert_eq!(
                actual.iter_coords().map(bits).collect::<Vec<_>>(),
                expected.into_iter().map(bits).collect::<Vec<_>>()
            );
        }
    }

    #[test]
    fn reversed_canonical_ring_retains_the_oriented_closing_endpoint() {
        let ring = CoordSeq::try_from_xy(&[
            XY::new(0.0, 0.0),
            XY::new(0.0, 1.0),
            XY::new(1.0, 0.0),
            XY::new(-0.0, 0.0),
        ])
        .unwrap();
        let result = canonical_ring_seq(&ring, false).unwrap();
        assert_eq!(
            result.xs().iter().map(|x| x.to_bits()).collect::<Vec<_>>(),
            [-0.0_f64, 1.0, 0.0, -0.0].map(f64::to_bits)
        );
        assert_eq!(result.ys(), [0.0, 0.0, 1.0, 0.0]);
    }

    #[test]
    fn matching_winding_reuses_ring_columns() {
        let ring = xyzm_ring(&[
            (0.0, 0.0, 1.0, 11.0),
            (1.0, 0.0, 2.0, 22.0),
            (1.0, 1.0, 3.0, 33.0),
            (0.0, 1.0, 4.0, 44.0),
            (0.0, 0.0, 1.0, 11.0),
        ]);
        let (oriented, allocations) = allocation_count(|| oriented_ring(&ring, false).unwrap());

        assert_eq!(oriented, ring);
        assert_eq!(allocations, 0);
    }

    #[test]
    fn polygon_orientation_reverses_wrong_xyzm_shell_and_keeps_hole() {
        let shell = xyzm_ring(&[
            (0.0, 0.0, 1.0, 11.0),
            (0.0, 4.0, 2.0, 22.0),
            (4.0, 4.0, 3.0, 33.0),
            (4.0, 0.0, 4.0, 44.0),
            (0.0, 0.0, 1.0, 11.0),
        ]);
        let hole = xyzm_ring(&[
            (1.0, 1.0, 5.0, 55.0),
            (1.0, 2.0, 6.0, 66.0),
            (2.0, 2.0, 7.0, 77.0),
            (2.0, 1.0, 8.0, 88.0),
            (1.0, 1.0, 5.0, 55.0),
        ]);
        let shape = Shape::Polygon(Polygon::new(shell, vec![hole]));
        let oriented = shape.orient_polygons(false).expect("orientation succeeds");

        let expected = Shape::Polygon(Polygon::new(
            xyzm_ring(&[
                (0.0, 0.0, 1.0, 11.0),
                (4.0, 0.0, 4.0, 44.0),
                (4.0, 4.0, 3.0, 33.0),
                (0.0, 4.0, 2.0, 22.0),
                (0.0, 0.0, 1.0, 11.0),
            ]),
            vec![xyzm_ring(&[
                (1.0, 1.0, 5.0, 55.0),
                (1.0, 2.0, 6.0, 66.0),
                (2.0, 2.0, 7.0, 77.0),
                (2.0, 1.0, 8.0, 88.0),
                (1.0, 1.0, 5.0, 55.0),
            ])],
        ));
        assert_eq!(oriented, expected);
    }

    #[test]
    fn polygon_orientation_reverses_wrong_xyzm_hole() {
        let shell = xyzm_ring(&[
            (0.0, 0.0, 1.0, 11.0),
            (4.0, 0.0, 2.0, 22.0),
            (4.0, 4.0, 3.0, 33.0),
            (0.0, 4.0, 4.0, 44.0),
            (0.0, 0.0, 1.0, 11.0),
        ]);
        let hole = xyzm_ring(&[
            (1.0, 1.0, 5.0, 55.0),
            (2.0, 1.0, 6.0, 66.0),
            (2.0, 2.0, 7.0, 77.0),
            (1.0, 2.0, 8.0, 88.0),
            (1.0, 1.0, 5.0, 55.0),
        ]);
        let shape = Shape::Polygon(Polygon::new(shell, vec![hole]));
        let oriented = shape.orient_polygons(false).expect("orientation succeeds");

        let expected = Shape::Polygon(Polygon::new(
            xyzm_ring(&[
                (0.0, 0.0, 1.0, 11.0),
                (4.0, 0.0, 2.0, 22.0),
                (4.0, 4.0, 3.0, 33.0),
                (0.0, 4.0, 4.0, 44.0),
                (0.0, 0.0, 1.0, 11.0),
            ]),
            vec![xyzm_ring(&[
                (1.0, 1.0, 5.0, 55.0),
                (1.0, 2.0, 8.0, 88.0),
                (2.0, 2.0, 7.0, 77.0),
                (2.0, 1.0, 6.0, 66.0),
                (1.0, 1.0, 5.0, 55.0),
            ])],
        ));
        assert_eq!(oriented, expected);
    }

    #[test]
    fn degenerate_winding_does_not_reverse_or_rotate_the_ring() {
        let ring = xyzm_ring(&[
            (0.0, 0.0, 1.0, 11.0),
            (1.0, 0.0, 2.0, 22.0),
            (2.0, 0.0, 3.0, 33.0),
            (0.0, 0.0, 1.0, 11.0),
        ]);
        let (oriented, allocations) = allocation_count(|| oriented_ring(&ring, true).unwrap());

        assert_eq!(oriented, ring);
        assert_eq!(allocations, 0);
    }
}

/// Canonical winding and start vertex, written directly into coordinate columns.
/// Duplicate minimum vertices use a linear least-rotation comparison.
pub(in crate::geometry) fn canonical_ring_seq(
    ring: &CoordSeq,
    clockwise: bool,
) -> Result<CoordSeq> {
    let count = ring.coord_count();
    let closed = count > 1 && same_point(ring.point_at(0), ring.point_at(count - 1));
    let unique = if closed { count - 1 } else { count };
    if unique <= 1 {
        return Ok(ring.clone());
    }
    let reverse = ring_winding(ring).reverse_for_clockwise(clockwise);
    let range = if reverse && closed {
        1..count
    } else {
        0..unique
    };
    let start = ring_rotation_start(ring, range.clone(), reverse);
    rotated_ring_columns(ring, range, start, reverse, closed)
}

fn rotated_ring_columns(
    ring: &CoordSeq,
    range: std::ops::Range<usize>,
    start: usize,
    reverse: bool,
    closed: bool,
) -> Result<CoordSeq> {
    let count = range.len() + usize::from(closed);
    // After reversal the ring still starts at the min vertex; the walk
    // direction flips. Emit per ordinate column in two contiguous chunks.
    let rotate = |column: &[f64]| {
        let mut output = Vec::with_capacity(count);
        if reverse {
            output.extend(column[range.start..=start].iter().rev().copied());
            output.extend(column[start + 1..range.end].iter().rev().copied());
        } else {
            output.extend_from_slice(&column[start..range.end]);
            output.extend_from_slice(&column[range.start..start]);
        }
        if closed {
            let first = output[0];
            output.push(first);
        }
        output
    };
    CoordSeq::try_from_vecs(
        rotate(ring.xs()),
        rotate(ring.ys()),
        ring.zs().map(rotate),
        ring.ms().map(rotate),
    )
}

fn ring_rotation_start(ring: &CoordSeq, range: std::ops::Range<usize>, reverse: bool) -> usize {
    let mut minimum = range.start;
    let mut repeated = false;
    if ring.zs().is_none() && ring.ms().is_none() {
        let (xs, ys) = (ring.xs(), ring.ys());
        for candidate in range.start + 1..range.end {
            match compare_f64(xs[candidate], xs[minimum])
                .then_with(|| compare_f64(ys[candidate], ys[minimum]))
            {
                Ordering::Less => {
                    minimum = candidate;
                    repeated = false;
                },
                Ordering::Equal => repeated = true,
                Ordering::Greater => {},
            }
        }
    } else {
        for candidate in range.start + 1..range.end {
            match compare_points(&ring.point_at(candidate), &ring.point_at(minimum)) {
                Ordering::Less => {
                    minimum = candidate;
                    repeated = false;
                },
                Ordering::Equal => repeated = true,
                Ordering::Greater => {},
            }
        }
    }
    if !repeated {
        return minimum;
    }
    let source = |index| {
        if reverse {
            range.end - 1 - index
        } else {
            range.start + index
        }
    };
    let start = if ring.zs().is_none() && ring.ms().is_none() {
        let (xs, ys) = (ring.xs(), ring.ys());
        least_rotation_by(range.len(), |left, right| {
            let (left, right) = (source(left), source(right));
            compare_f64(xs[left], xs[right]).then_with(|| compare_f64(ys[left], ys[right]))
        })
    } else {
        least_rotation_by(range.len(), |left, right| {
            compare_points(&ring.point_at(source(left)), &ring.point_at(source(right)))
        })
    };
    source(start)
}

fn least_rotation_by(len: usize, compare: impl Fn(usize, usize) -> Ordering) -> usize {
    let (mut left, mut right, mut offset) = (0, 1, 0);
    // A mismatch rules out the losing start and every start in its equal prefix.
    while left < len && right < len && offset < len {
        match compare(
            wrap_index(left + offset, len),
            wrap_index(right + offset, len),
        ) {
            Ordering::Equal => offset += 1,
            Ordering::Less => {
                right += offset + 1;
                if right == left {
                    right += 1;
                }
                offset = 0;
            },
            Ordering::Greater => {
                left += offset + 1;
                if left == right {
                    left += 1;
                }
                offset = 0;
            },
        }
    }
    left.min(right)
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
    let mut out = vec![0.0_f64; n];
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
    Ok(out.into_boxed_slice())
}

#[expect(
    clippy::float_cmp,
    reason = "an exact predicate's degeneracy test: these operands are compared for identity, not proximity, and a tolerance would make the predicate inexact"
)]
/// `((v - origin) / size).round() * size + origin` with power-of-two
/// pre-scale so `v=1e308, origin=-1e308, size=1e308` stays finite.
///
/// Algebra: `k = round((v - origin) / size)`, result = `origin + k * size`.
/// Scale `v` and `origin` together before the subtraction so the difference
/// does not overflow; `size` stays in world units for the final multiply.
pub(crate) fn stable_snap_ordinate(value: f64, origin: f64, size: f64) -> f64 {
    if value == origin {
        return value;
    }
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
    let scale = axis_pow2_scale(max_abs);
    let delta = value * scale - origin * scale;
    let sized = size * scale;
    if sized == 0.0 || !sized.is_finite() || !delta.is_finite() {
        return classic;
    }
    let k = (delta / sized).round();
    let result = (origin * scale + k * sized) / scale;
    if result.is_finite() { result } else { classic }
}

/// Chain-wide per-axis power-of-two frame for VW scoring: optional framed
/// columns and the area threshold in that frame.
struct VwScoringFrame {
    xs: Option<Vec<f64>>,
    ys: Option<Vec<f64>>,
    area_tolerance: f64,
}

fn vw_frame_columns(xs: &[f64], ys: &[f64], distance_tolerance: f64) -> VwScoringFrame {
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
        return VwScoringFrame {
            xs: None,
            ys: None,
            area_tolerance: world_area_tol,
        };
    }
    let Some(frame) =
        AxisFrame::from_origin_extents(Point::new_raw_xy(ox, oy), max_abs_x, max_abs_y)
    else {
        return VwScoringFrame {
            xs: None,
            ys: None,
            area_tolerance: world_area_tol,
        };
    };
    let mut fxs = Vec::with_capacity(xs.len());
    let mut fys = Vec::with_capacity(xs.len());
    for index in 0..xs.len() {
        fxs.push(frame.frame_xy(xs[index], oy).x);
        fys.push(frame.frame_xy(ox, ys[index]).y);
    }
    // A VW score is an area, so map its distance-square threshold through both
    // exact scale exponents together.  Forming the world threshold first
    // would make a valid huge tolerance `+inf` before the rescue could scale
    // it back down.
    let exponent = power_of_two_exponent(frame.scale_x()) + power_of_two_exponent(frame.scale_y());
    let framed_area =
        0.5 * distance_tolerance * scale_by_power_of_two(distance_tolerance, exponent);
    VwScoringFrame {
        xs: Some(fxs),
        ys: Some(fys),
        area_tolerance: framed_area,
    }
}

/// Visvalingam-Whyatt keep mask. `distance_tolerance` is the public
/// distance-scale threshold (same units as `simplify`/`coverage_simplify`);
/// the area threshold is derived **after** any chain-wide power-of-two frame
/// so huge inputs (tol² → +inf in world units) still compare correctly.
/// Input columns are admitted coordinate windows whose length fits `i32`.
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
    let framed = vw_frame_columns(xs, ys, distance_tolerance);
    let xs = framed.xs.as_deref().unwrap_or(xs);
    let ys = framed.ys.as_deref().unwrap_or(ys);
    let area_tolerance = framed.area_tolerance;
    let area = |a: usize, b: usize, c: usize| -> f64 {
        0.5 * ((xs[b] - xs[a]) * (ys[c] - ys[a]) - (ys[b] - ys[a]) * (xs[c] - xs[a])).abs()
    };
    keep.clear();
    keep.reserve(count);
    keep.resize(count, true);
    let mut prev: Vec<_> = (0..count as u32)
        .map(|index| index.saturating_sub(1))
        .collect();
    let mut next: Vec<_> = (1..=count as u32).collect();
    let mut current = vec![0_u64; count];
    let mut heap = BinaryHeap::with_capacity(count);
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
        let framed = vw_frame_columns(&xs, &ys, 5e307);
        let (Some(fxs), Some(fys)) = (framed.xs.as_deref(), framed.ys.as_deref()) else {
            panic!("opposite-sign extreme coordinates require a VW frame");
        };
        assert!(fxs.iter().chain(fys).all(|value| value.is_finite()));
        let Some([first, second, third]) = fxs.first_chunk::<3>() else {
            panic!("the frame keeps one column per input coordinate");
        };
        assert_eq!(first.to_bits(), 0.0_f64.to_bits());
        assert!(third > second && second > first);
    }
}
