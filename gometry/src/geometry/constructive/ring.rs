use std::simd::cmp::{SimdPartialEq as _, SimdPartialOrd as _};
use std::simd::num::SimdFloat as _;
use std::simd::{Select as _, StdFloat as _};

use crate::error::Result;
use crate::geographic::PeriodicAxis;
use crate::geometry::constructive::reversed_points;
use crate::geometry::{
    AxisFrame, BinaryHeap, CoordSeq, Coordinates, GeometryErrorKind, Ordering, Point, REDUCE_LANES,
    ReduceSimd, Ring, axis_pow2_scale, compare_f64, compare_points, power_of_two_exponent,
    ring_winding, same_point, same_topological_coordinate, scale_by_power_of_two,
    topology_coordinate_bits_simd, try_simd_map_f64, wrap_index,
};

pub(crate) fn normalized_line(points: &CoordSeq) -> Result<CoordSeq> {
    let count = points.len();
    // A CLOSED line canonicalizes to the lexicographically smallest of all
    // its presentations (every rotation, both directions) — a pure minimum
    // over the orbit: total for self-intersecting rings, no orientation
    // predicate. Open lines pick the smaller of their two directions; the
    // same "smallest presentation" principle, one comparator.
    if count > 2 && same_point(points.point_at(0), points.point_at(count.wrapping_sub(1))) {
        let unique = count.wrapping_sub(1);
        let forward = ring_rotation_start(points, 0..unique, false);
        // Reversal retains the stored closing point, which may differ in Z/M.
        let backward = ring_rotation_start(points, 1..count, true);
        let backward_offset = count.wrapping_sub(1).wrapping_sub(backward);
        let reverse = (0..unique)
            .map(|offset| {
                compare_points(
                    &points.point_at(
                        count
                            .wrapping_sub(1)
                            .wrapping_sub(wrap_index(backward_offset.wrapping_add(offset), unique)),
                    ),
                    &points.point_at(wrap_index(forward.wrapping_add(offset), unique)),
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
    fn least_rotation_matches_presentations_and_bounds_repeated_work() {
        for source in [
            &[0][..],
            &[1, 0],
            &[2, 1, 0],
            &[1, 1, 1, 1],
            &[0, 1, 0, 1],
            &[0, 1, 0, 1, 0, 1, 0, 0],
            &[2, 1, 2, 1, 0, 1, 2],
            &[0, 1, 0, 2, 0, 1, 0, 1, 0],
        ] {
            let len = source.len();
            for shift in 0..len {
                let values: Vec<_> = source[shift..]
                    .iter()
                    .chain(&source[..shift])
                    .copied()
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
        let len = 257;
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

    #[rstest::rstest]
    #[case::empty(vec![])]
    #[case::open({
        let point = |x, z| Point::new_axes(x, x, ZOrdinate(Some(z)), MOrdinate(Some(-z))).unwrap();
        vec![point(2., 3.), point(1., 4.)]
    })]
    #[case::closed({
        let point = |x, z| Point::new_axes(x, x, ZOrdinate(Some(z)), MOrdinate(Some(-z))).unwrap();
        vec![point(0., 9.), point(1., 3.), point(0., 1.)]
    })]
    #[case::repeated_minimum({
        let point = |x, z| Point::new_axes(x, x, ZOrdinate(Some(z)), MOrdinate(Some(-z))).unwrap();
        vec![
            point(0., 1.),
            point(2., 2.),
            point(0., 1.),
            point(1., 3.),
            point(0., 1.),
        ]
    })]
    #[case::signed_zero({
        let point = |x, z| Point::new_axes(x, x, ZOrdinate(Some(z)), MOrdinate(Some(-z))).unwrap();
        vec![
            point(-0., 1.),
            point(2., 2.),
            point(0., 1.),
            point(1., 3.),
            point(-0., 1.),
        ]
    })]
    fn column_normalization_matches_full_ordinate_presentation_orbit(#[case] input: Vec<Point>) {
        let mut presentations = Vec::new();
        for reverse in [false, true] {
            let mut sequence = input.clone();
            if reverse {
                sequence.reverse();
            }
            if sequence.len() > 2
                && same_point(sequence[0], sequence[sequence.len().wrapping_sub(1)])
            {
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
    let closed = count > 1 && same_point(ring.point_at(0), ring.point_at(count.wrapping_sub(1)));
    let unique = if closed { count.wrapping_sub(1) } else { count };
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
    let count = range.len().wrapping_add(usize::from(closed));
    // After reversal the ring still starts at the min vertex; the walk
    // direction flips. Emit per ordinate column in two contiguous chunks.
    let rotate = |column: &[f64]| {
        let mut output = Vec::with_capacity(count);
        let column = column
            .get(range.clone())
            .expect("ring rotation range belongs to its coordinate column");
        if reverse {
            let split = start.wrapping_sub(range.start).wrapping_add(1);
            let (prefix, suffix) = column
                .split_at_checked(split)
                .expect("ring rotation start belongs to its coordinate range");
            output.extend(prefix.iter().rev().copied());
            output.extend(suffix.iter().rev().copied());
        } else {
            let split = start.wrapping_sub(range.start);
            let (prefix, suffix) = column
                .split_at_checked(split)
                .expect("ring rotation start belongs to its coordinate range");
            output.extend_from_slice(suffix);
            output.extend_from_slice(prefix);
        }
        if closed {
            let first = *output
                .first()
                .expect("a closed ring rotation has a first coordinate");
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
        let (xs, ys) = (
            ring.xs()
                .get(range.clone())
                .expect("ring rotation range belongs to its X column"),
            ring.ys()
                .get(range.clone())
                .expect("ring rotation range belongs to its Y column"),
        );
        let mut coordinates = xs.iter().copied().zip(ys.iter().copied()).enumerate();
        let (_, (mut minimum_x, mut minimum_y)) = coordinates
            .next()
            .expect("nontrivial ring range has a minimum coordinate");
        for (offset, (x, y)) in coordinates {
            match compare_f64(x, minimum_x).then_with(|| compare_f64(y, minimum_y)) {
                Ordering::Less => {
                    minimum = range.start.wrapping_add(offset);
                    minimum_x = x;
                    minimum_y = y;
                    repeated = false;
                },
                Ordering::Equal => repeated = true,
                Ordering::Greater => {},
            }
        }
    } else {
        let mut points = ring
            .iter_coords()
            .skip(range.start)
            .take(range.len())
            .enumerate();
        let (_, mut minimum_point) = points
            .next()
            .expect("nontrivial ring range has a minimum point");
        for (offset, candidate) in points {
            match compare_points(&candidate, &minimum_point) {
                Ordering::Less => {
                    minimum = range.start.wrapping_add(offset);
                    minimum_point = candidate;
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
            range.end.wrapping_sub(1).wrapping_sub(index)
        } else {
            range.start.wrapping_add(index)
        }
    };
    let start = if ring.zs().is_none() && ring.ms().is_none() {
        let (xs, ys) = (ring.xs(), ring.ys());
        least_rotation_by(range.len(), |left, right| {
            let (left, right) = (source(left), source(right));
            compare_f64(
                *xs.get(left)
                    .expect("ring rotation source belongs to its X column"),
                *xs.get(right)
                    .expect("ring rotation source belongs to its X column"),
            )
            .then_with(|| {
                compare_f64(
                    *ys.get(left)
                        .expect("ring rotation source belongs to its Y column"),
                    *ys.get(right)
                        .expect("ring rotation source belongs to its Y column"),
                )
            })
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
            wrap_index(left.wrapping_add(offset), len),
            wrap_index(right.wrapping_add(offset), len),
        ) {
            Ordering::Equal => offset = offset.wrapping_add(1),
            Ordering::Less => {
                right = right.wrapping_add(offset).wrapping_add(1);
                if right == left {
                    right = right.wrapping_add(1);
                }
                offset = 0;
            },
            Ordering::Greater => {
                left = left.wrapping_add(offset).wrapping_add(1);
                if left == right {
                    left = left.wrapping_add(1);
                }
                offset = 0;
            },
        }
    }
    left.min(right)
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
            #[expect(
                clippy::arithmetic_side_effects,
                reason = "the SIMD snap lane preserves the scalar binary64 operation order"
            )]
            let snapped = ((value - vorigin) / vsize).round() * vsize + vorigin;
            // SIMD path may overflow extreme value/origin pairs; fall back
            // per-lane via the scalar stable form when any lane is non-finite.
            if !snapped.abs().simd_lt(infinity).all() {
                let mut out_lane = snapped.to_array();
                let values = value.to_array();
                for (output, value) in out_lane.iter_mut().zip(values) {
                    if !output.is_finite() {
                        *output = stable_snap_ordinate(value, origin, size);
                        if !output.is_finite() {
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

macro_rules! vw_frame_columns_body {
    ($xs:expr, $ys:expr, $distance_tolerance:expr) => {{
        let (xs, ys, distance_tolerance) = ($xs, $ys, $distance_tolerance);
        let (&ox, &oy) = xs
            .first()
            .zip(ys.first())
            .expect("VW frame receives admitted coordinate columns");
        let mut max_abs_x = ox.abs();
        let mut max_abs_y = oy.abs();
        for (&x, &y) in xs.iter().zip(ys) {
            max_abs_x = max_abs_x.max(x.abs());
            max_abs_y = max_abs_y.max(y.abs());
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
        for (&x, &y) in xs.iter().zip(ys) {
            fxs.push(frame.frame_xy(x, oy).x);
            fys.push(frame.frame_xy(ox, y).y);
        }
        // A VW score is an area, so map its distance-square threshold through both
        // exact scale exponents together.  Forming the world threshold first
        // would make a valid huge tolerance `+inf` before the rescue could scale
        // it back down.
        let exponent = power_of_two_exponent(frame.scale_x())
            .wrapping_add(power_of_two_exponent(frame.scale_y()));
        let framed_area =
            0.5 * distance_tolerance * scale_by_power_of_two(distance_tolerance, exponent);
        VwScoringFrame {
            xs: Some(fxs),
            ys: Some(fys),
            area_tolerance: framed_area,
        }
    }};
}

fn vw_frame_columns(xs: &[f64], ys: &[f64], distance_tolerance: f64) -> VwScoringFrame {
    vw_frame_columns_body!(xs, ys, distance_tolerance)
}

// Keep geographic frame work out of the planar VW kernel's inlined path.
fn vw_frame_columns_geographic(xs: &[f64], ys: &[f64], distance_tolerance: f64) -> VwScoringFrame {
    vw_frame_columns_body!(xs, ys, distance_tolerance)
}

/// Visvalingam-Whyatt keep mask. `distance_tolerance` is the public
/// distance-scale threshold (same units as `simplify`/`coverage_simplify`);
/// the area threshold is derived **after** any chain-wide power-of-two frame
/// so huge inputs (tol² → +inf in world units) still compare correctly.
/// Input columns are admitted coordinate windows whose length fits `i32`.
///
/// Keep the planar kernel's heap-pop path direct; the geographic variant must
/// inspect the proposed chord before mutating the keep mask.
macro_rules! vw_keep_span_exceeds {
    (planar, $guard:expr, $before:ident, $after:ident) => {
        false
    };
    (geographic, $guard:expr, $before:ident, $after:ident) => {
        $guard.rejects_span($before, $after)
    };
}

macro_rules! vw_keep_body {
    ($xs:expr, $ys:expr, $distance_tolerance:expr, $keep:expr, $frame_fn:ident, $policy:ident, $axis:expr) => {{
        let (xs, ys, distance_tolerance, keep) = ($xs, $ys, $distance_tolerance, $keep);

        let count = xs.len();
        if count < 3 {
            return None;
        }
        let framed = $frame_fn(xs, ys, distance_tolerance);
        let xs = framed.xs.as_deref().unwrap_or(xs);
        let ys = framed.ys.as_deref().unwrap_or(ys);
        let area_tolerance = framed.area_tolerance;
        let area = |a: usize, b: usize, c: usize| -> f64 {
            let x_at = |index| {
                *xs.get(index)
                    .expect("VW vertex index belongs to the admitted X column")
            };
            let y_at = |index| {
                *ys.get(index)
                    .expect("VW vertex index belongs to the admitted Y column")
            };
            0.5 * ((x_at(b) - x_at(a)) * (y_at(c) - y_at(a))
                - (y_at(b) - y_at(a)) * (x_at(c) - x_at(a)))
            .abs()
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
        let interior_end = count.wrapping_sub(1);
        let interior_count = count.wrapping_sub(2);
        let simd_lanes = interior_count.wrapping_sub(interior_count % REDUCE_LANES);
        let half = ReduceSimd::splat(0.5);
        let mut index = 1_usize;
        if simd_lanes != 0 {
            let middle_end = 1_usize.wrapping_add(simd_lanes);
            let right_end = 2_usize.wrapping_add(simd_lanes);
            let left_xs = xs
                .get(..simd_lanes)
                .expect("VW left SIMD span belongs to the input column");
            let middle_xs = xs
                .get(1..middle_end)
                .expect("VW middle SIMD span belongs to the input column");
            let right_xs = xs
                .get(2..right_end)
                .expect("VW right SIMD span belongs to the input column");
            let left_ys = ys
                .get(..simd_lanes)
                .expect("VW left SIMD span belongs to the input column");
            let middle_ys = ys
                .get(1..middle_end)
                .expect("VW middle SIMD span belongs to the input column");
            let right_ys = ys
                .get(2..right_end)
                .expect("VW right SIMD span belongs to the input column");
            let x_batches = left_xs
                .chunks_exact(REDUCE_LANES)
                .zip(middle_xs.chunks_exact(REDUCE_LANES))
                .zip(right_xs.chunks_exact(REDUCE_LANES));
            let y_batches = left_ys
                .chunks_exact(REDUCE_LANES)
                .zip(middle_ys.chunks_exact(REDUCE_LANES))
                .zip(right_ys.chunks_exact(REDUCE_LANES));
            for (((xa_values, xb_values), xc_values), ((ya_values, yb_values), yc_values)) in
                x_batches.zip(y_batches)
            {
                let xa = ReduceSimd::from_slice(xa_values);
                let xb = ReduceSimd::from_slice(xb_values);
                let xc = ReduceSimd::from_slice(xc_values);
                let ya = ReduceSimd::from_slice(ya_values);
                let yb = ReduceSimd::from_slice(yb_values);
                let yc = ReduceSimd::from_slice(yc_values);
                #[expect(
                    clippy::arithmetic_side_effects,
                    reason = "VW's lane-wise shoelace cross preserves its binary64 score"
                )]
                let cross = (xb - xa) * (yc - ya) - (yb - ya) * (xc - xa);
                #[expect(
                    clippy::arithmetic_side_effects,
                    reason = "VW's lane-wise half-area preserves the public area threshold"
                )]
                let areas = (cross * half).abs();
                for (lane, area_value) in areas.to_array().into_iter().enumerate() {
                    let vertex = index.wrapping_add(lane);
                    let bits = area_value.to_bits();
                    *current
                        .get_mut(vertex)
                        .expect("VW SIMD vertex belongs to the keep columns") = bits;
                    heap.push(std::cmp::Reverse((bits, vertex as u32)));
                }
                index = index.wrapping_add(REDUCE_LANES);
            }
        }
        for (index, slot) in current
            .iter_mut()
            .enumerate()
            .take(interior_end)
            .skip(1_usize.wrapping_add(simd_lanes))
        {
            let bits = area(index.wrapping_sub(1), index, index.wrapping_add(1)).to_bits();
            *slot = bits;
            heap.push(std::cmp::Reverse((bits, index as u32)));
        }
        let tolerance_bits = area_tolerance.to_bits();
        while let Some(std::cmp::Reverse((bits, index))) = heap.pop() {
            let index = index as usize;
            if !*keep
                .get(index)
                .expect("VW heap vertex belongs to the keep columns")
                || bits
                    != *current
                        .get(index)
                        .expect("VW heap vertex belongs to the score columns")
            {
                continue; // stale entry
            }
            if bits >= tolerance_bits {
                break;
            }
            let (before, after) = (
                *prev
                    .get(index)
                    .expect("VW heap vertex belongs to previous links") as usize,
                *next
                    .get(index)
                    .expect("VW heap vertex belongs to next links") as usize,
            );
            if vw_keep_span_exceeds!($policy, $axis, before, after) {
                continue;
            }
            *keep
                .get_mut(index)
                .expect("VW heap vertex belongs to the keep columns") = false;
            *next
                .get_mut(before)
                .expect("VW previous link belongs to next links") = after as u32;
            *prev
                .get_mut(after)
                .expect("VW next link belongs to previous links") = before as u32;
            for neighbor in [before, after] {
                if neighbor == 0
                    || neighbor == count.wrapping_sub(1)
                    || !*keep
                        .get(neighbor)
                        .expect("VW neighbor belongs to the keep columns")
                {
                    continue;
                }
                let bits = area(
                    *prev
                        .get(neighbor)
                        .expect("VW neighbor belongs to previous links")
                        as usize,
                    neighbor,
                    *next
                        .get(neighbor)
                        .expect("VW neighbor belongs to next links") as usize,
                )
                .to_bits();
                *current
                    .get_mut(neighbor)
                    .expect("VW neighbor belongs to score columns") = bits;
                if bits < tolerance_bits {
                    heap.push(std::cmp::Reverse((bits, neighbor as u32)));
                }
            }
        }
        Some(keep.iter().filter(|&&kept| kept).count())
    }};
}

pub(crate) fn vw_keep(
    xs: &[f64],
    ys: &[f64],
    distance_tolerance: f64,
    keep: &mut Vec<bool>,
) -> Option<usize> {
    vw_keep_body!(
        xs,
        ys,
        distance_tolerance,
        keep,
        vw_frame_columns,
        planar,
        ()
    )
}

pub(crate) fn vw_keep_geographic(
    xs: &[f64],
    ys: &[f64],
    distance_tolerance: f64,
    axis: PeriodicAxis,
    keep: &mut Vec<bool>,
) -> Option<usize> {
    let span_guard = crate::geographic::ChartSpanGuard::new(xs, axis);
    vw_keep_body!(
        xs,
        ys,
        distance_tolerance,
        keep,
        vw_frame_columns_geographic,
        geographic,
        span_guard
    )
}

#[cfg(test)]
mod vw_frame_tests {
    use super::*;

    #[test]
    fn opposite_sign_extremes_scale_each_operand_before_the_frame_subtraction() {
        let xs = [-1e308, 0.0, 1e308];
        let ys = [0.0, 1e307, 0.0];
        // Finite ordered coordinates prove the residual is `x*s - origin*s`;
        // `(x-origin)*s` overflows before it can be scaled.
        for framed in [
            vw_frame_columns(&xs, &ys, 5e307),
            vw_frame_columns_geographic(&xs, &ys, 5e307),
        ] {
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
}
