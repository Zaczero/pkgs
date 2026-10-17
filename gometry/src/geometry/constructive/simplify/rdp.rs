//! Exact Douglas–Peucker decisions over stored coordinates.

mod farthest;
mod index;

pub(super) use index::Index;

use crate::error::Result;
use crate::geometry::{Segment, XY, point_pair_distance_key, point_segment_exact_distance_key};

#[cfg(test)]
std::thread_local! {
    static SOURCE_VISITS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
pub(super) fn record_source_visits(count: usize) {
    SOURCE_VISITS.with(|visits| visits.set(visits.get().wrapping_add(count)));
}

#[cfg(test)]
pub(super) fn take_source_visits() -> usize {
    SOURCE_VISITS.with(|visits| visits.replace(0))
}

pub(super) fn point(xs: &[f64], ys: &[f64], index: usize) -> XY {
    #[cfg(test)]
    record_source_visits(1);
    XY::new(
        *xs.get(index).expect("RDP ordinal belongs to its source"),
        *ys.get(index).expect("RDP ordinal belongs to its source"),
    )
}

fn score(point: XY, chord: Segment, length_squared: f64) -> (f64, f64) {
    let (dx, dy) = (chord.end.x - chord.start.x, chord.end.y - chord.start.y);
    let (qx, qy) = (point.x - chord.start.x, point.y - chord.start.y);
    let along = qx * dx + qy * dy;
    let origin_norm = qx * qx + qy * qy;
    let value = if along <= 0.0 {
        origin_norm * length_squared
    } else if along >= length_squared {
        let (rx, ry) = (point.x - chord.end.x, point.y - chord.end.y);
        (rx * rx + ry * ry) * length_squared
    } else {
        let cross = qx * dy - qy * dx;
        cross * cross
    };
    (value, origin_norm)
}

macro_rules! approximate_maximum_inline {
    ($point:literal, $xs:expr, $ys:expr, $chord:expr, $length:expr $(,)?) => {{
    use std::simd::Select as _;
    use std::simd::cmp::SimdPartialOrd as _;
    use std::simd::num::SimdFloat as _;
    use $crate::geometry::{REDUCE_LANES, ReduceSimd};
    const POINT: bool = $point;
    let (xs, ys, chord, length_squared) = ($xs, $ys, $chord, $length);
    (|| {
    if xs.len() < REDUCE_LANES {
        return $crate::geometry::constructive::simplify::rdp::scalar_maximum::<POINT>(xs, ys, chord, length_squared, 0, (0, 0.0, 0.0, 0.0));
    }
    let (dx, dy) = (chord.end.x - chord.start.x, chord.end.y - chord.start.y);
    let (ax, ay) = (
        ReduceSimd::splat(chord.start.x),
        ReduceSimd::splat(chord.start.y),
    );
    let (bx, by) = (
        ReduceSimd::splat(chord.end.x),
        ReduceSimd::splat(chord.end.y),
    );
    let (dx, dy) = (ReduceSimd::splat(dx), ReduceSimd::splat(dy));
    let length = ReduceSimd::splat(length_squared);
    let mut maximum = ReduceSimd::splat(0.0);
    let mut runner = ReduceSimd::splat(0.0);
    let mut scale = ReduceSimd::splat(0.0);
    let mut ordinals = std::simd::Simd::<u64, REDUCE_LANES>::splat(0);
    let offsets =
        std::simd::Simd::<u64, REDUCE_LANES>::from_array(std::array::from_fn(|lane| lane as u64));
    let mut index = 0_usize;
    while index.wrapping_add(REDUCE_LANES) <= xs.len() {
        let end = index.wrapping_add(REDUCE_LANES);
        let x = ReduceSimd::from_slice(xs.get(index..end).expect("RDP SIMD source window"));
        let y = ReduceSimd::from_slice(ys.get(index..end).expect("RDP SIMD source window"));
        #[expect(
            clippy::arithmetic_side_effects,
            reason = "SIMD polynomial values are bounded by the decision certificate or declined"
        )]
        let (value, origin_score) = {
            let (qx, qy) = (x - ax, y - ay);
            let origin_score = qx * qx + qy * qy;
            let value = if POINT {
                origin_score
            } else {
                let along = qx * dx + qy * dy;
                let cross = qx * dy - qy * dx;
                let before = along.simd_le(ReduceSimd::splat(0.0));
                let after = along.simd_ge(length);
                if (before | after).any() {
                    let (rx, ry) = (x - bx, y - by);
                    before.select(
                        origin_score * length,
                        after.select((rx * rx + ry * ry) * length, cross * cross),
                    )
                } else {
                    cross * cross
                }
            };
            (value, origin_score)
        };
        let contender = maximum.simd_lt(value).select(maximum, value);
        runner = contender.simd_gt(runner).select(contender, runner);
        if !POINT {
            scale = origin_score.simd_gt(scale).select(origin_score, scale);
        }
        let improved = value.simd_gt(maximum);
        maximum = improved.select(value, maximum);
        #[expect(
            clippy::arithmetic_side_effects,
            reason = "SIMD source ordinals fit the admitted source window"
        )]
        let cursor = std::simd::Simd::splat(index as u64) + offsets;
        ordinals = improved.cast::<i64>().select(cursor, ordinals);
        index = end;
    }
    let (mut best, mut second, mut farthest) = (0.0_f64, 0.0_f64, 0_usize);
    for ((&value, &previous), &ordinal) in maximum
        .as_array()
        .iter()
        .zip(runner.as_array())
        .zip(ordinals.as_array())
    {
        if previous > second {
            second = previous;
        }
        if value > best {
            if best > second {
                second = best;
            }
            best = value;
            farthest = ordinal as usize;
        } else if value > second {
            second = value;
        }
    }
    $crate::geometry::constructive::simplify::rdp::scalar_maximum::<POINT>(
        xs.get(index..).expect("RDP scalar source suffix"),
        ys.get(index..).expect("RDP scalar source suffix"),
        chord,
        length_squared,
        index,
        (farthest, best, second, scale.reduce_max()),
    )
    })()
    }};
}

pub(super) use approximate_maximum_inline;

pub(super) fn scalar_maximum<const POINT: bool>(
    xs: &[f64],
    ys: &[f64],
    chord: Segment,
    length_squared: f64,
    index: usize,
    (mut farthest, mut best, mut second, mut scale): (usize, f64, f64, f64),
) -> (usize, f64, f64, f64) {
    for (offset, (&x, &y)) in xs.iter().zip(ys).enumerate() {
        let (value, origin_score) = if POINT {
            let (qx, qy) = (x - chord.start.x, y - chord.start.y);
            let value = qx * qx + qy * qy;
            (value, value)
        } else {
            score(XY::new(x, y), chord, length_squared)
        };
        if origin_score > scale {
            scale = origin_score;
        }
        if value > best {
            second = best;
            best = value;
            farthest = index.wrapping_add(offset);
        } else if value > second {
            second = value;
        }
    }
    (farthest, best, second, scale)
}

// Expand the certified scan into each keep loop; exact resolution stays at
// its shared owner so short ranges do not carry its comparison-key frame.
macro_rules! split_inline {
    ($xs:expr, $ys:expr, $start:expr, $end:expr, $tolerance:expr, $squared:expr) => {{
        (|| -> $crate::error::Result<Option<usize>> {
            let (xs, ys, start, end, tolerance, tolerance_squared) =
                ($xs, $ys, $start, $end, $tolerance, $squared);
            let chord = Segment {
                start: $crate::geometry::constructive::simplify::rdp::point(xs, ys, start),
                end: $crate::geometry::constructive::simplify::rdp::point(xs, ys, end),
            };
            let interior = start.wrapping_add(1)..end;
            let xs = xs.get(interior.clone()).expect("RDP interior source range");
            let ys = ys.get(interior.clone()).expect("RDP interior source range");
            #[cfg(test)]
            $crate::geometry::constructive::simplify::rdp::record_source_visits(xs.len());
            let (dx, dy) = (chord.end.x - chord.start.x, chord.end.y - chord.start.y);
            let collapsed = dx == 0.0 && dy == 0.0;
            let length_squared = if collapsed { 1.0 } else { dx * dx + dy * dy };
            let threshold = tolerance_squared * length_squared;
            let (farthest, maximum, runner, scale) = if xs.len() < $crate::geometry::REDUCE_LANES {
                if collapsed {
                    $crate::geometry::constructive::simplify::rdp::scalar_maximum::<true>(
                        xs,
                        ys,
                        chord,
                        length_squared,
                        0,
                        (0, 0.0, 0.0, 0.0),
                    )
                } else {
                    $crate::geometry::constructive::simplify::rdp::scalar_maximum::<false>(
                        xs,
                        ys,
                        chord,
                        length_squared,
                        0,
                        (0, 0.0, 0.0, 0.0),
                    )
                }
            } else if collapsed {
                $crate::geometry::constructive::simplify::rdp::approximate_maximum_inline!(
                    true,
                    xs,
                    ys,
                    chord,
                    length_squared,
                )
            } else {
                $crate::geometry::constructive::simplify::rdp::approximate_maximum_inline!(
                    false,
                    xs,
                    ys,
                    chord,
                    length_squared,
                )
            };

            // All endpoint scores are bounded by 2|p-a|²|b-a|² + 2|b-a|⁴.
            // The extra factor absorbs bound rounding. Sixty-four epsilons covers the
            // degree-four score stages and the continuous clamped-parameter boundary.
            // The absolute floor covers underflowed score stages. A subnormal
            // chord norm declines because its relative error can be unbounded.
            // Non-finite bounds fail both certificates and go to exact resolution.
            let scale = 4.0 * (scale * length_squared + length_squared * length_squared);
            let error = if collapsed {
                8.0 * f64::EPSILON * maximum + f64::MIN_POSITIVE
            } else if length_squared >= f64::MIN_POSITIVE {
                64.0 * f64::EPSILON * scale + f64::MIN_POSITIVE
            } else {
                f64::INFINITY
            };
            let threshold_error = if tolerance_squared >= f64::MIN_POSITIVE || tolerance == 0.0 {
                16.0 * f64::EPSILON * threshold + f64::MIN_POSITIVE
            } else {
                f64::INFINITY
            };
            if maximum + error < threshold - threshold_error {
                return Ok(None);
            }
            if maximum - error > threshold + threshold_error && maximum - runner > 2.0 * error {
                return Ok(Some(interior.start.wrapping_add(farthest)));
            }
            $crate::geometry::constructive::simplify::rdp::resolve(
                xs,
                ys,
                chord,
                tolerance,
                interior.start,
                (length_squared, maximum, error, collapsed),
            )
        })()
    }};
}

pub(super) use split_inline;

pub(super) fn split(
    xs: &[f64],
    ys: &[f64],
    start: usize,
    end: usize,
    tolerance: f64,
) -> Result<Option<usize>> {
    if !tolerance.is_finite() {
        return Ok(None);
    }
    split_inline!(xs, ys, start, end, tolerance, tolerance * tolerance)
}

pub(super) fn resolve(
    xs: &[f64],
    ys: &[f64],
    chord: Segment,
    tolerance: f64,
    start: usize,
    (length_squared, maximum, error, collapsed): (f64, f64, f64, bool),
) -> Result<Option<usize>> {
    let mut best = None;
    #[cfg(test)]
    record_source_visits(xs.len());
    for (offset, (&x, &y)) in xs.iter().zip(ys).enumerate() {
        let vertex = XY::new(x, y);
        if error.is_finite() && score(vertex, chord, length_squared).0 < maximum - 2.0 * error {
            continue;
        }
        let key = if collapsed {
            point_pair_distance_key(vertex, chord.start)
        } else {
            point_segment_exact_distance_key(vertex, chord)
        };
        if best.as_ref().map_or(Ok(true), |(_, incumbent)| {
            key.cmp_distance(incumbent).map(std::cmp::Ordering::is_gt)
        })? {
            best = Some((offset, key));
        }
    }
    let (offset, best) = best.expect("an uncertain maximum has an interior candidate");
    let threshold = point_pair_distance_key(XY::new(0.0, 0.0), XY::new(tolerance, 0.0));
    Ok(best
        .cmp_distance(&threshold)?
        .is_gt()
        .then_some(start.wrapping_add(offset)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::segments::ExactDistanceWitness;
    use crate::geometry::segments::distance::exact_distance_witness_cmp;

    fn exact_split(xs: &[f64], ys: &[f64], tolerance: f64) -> Option<usize> {
        let end = xs.len().wrapping_sub(1);
        let chord = Segment {
            start: point(xs, ys, 0),
            end: point(xs, ys, end),
        };
        let interior_xs = xs
            .get(1..end)
            .expect("exact RDP interior belongs to its source range");
        let interior_ys = ys
            .get(1..end)
            .expect("exact RDP interior belongs to its source range");
        let mut interior = interior_xs.iter().zip(interior_ys).enumerate();
        let (first_offset, (&first_x, &first_y)) =
            interior.next().expect("exact RDP source has an interior");
        let mut farthest = 1_usize.wrapping_add(first_offset);
        let mut best = ExactDistanceWitness::Clamped(XY::new(first_x, first_y), chord);
        for (offset, (&x, &y)) in interior {
            let index = 1_usize.wrapping_add(offset);
            let candidate = ExactDistanceWitness::Clamped(XY::new(x, y), chord);
            if exact_distance_witness_cmp(candidate, best).unwrap().is_gt() {
                best = candidate;
                farthest = index;
            }
        }
        exact_distance_witness_cmp(
            best,
            ExactDistanceWitness::Points(XY::new(0.0, 0.0), XY::new(tolerance, 0.0)),
        )
        .unwrap()
        .is_gt()
        .then_some(farthest)
    }

    #[test]
    fn certificates_agree_with_unfiltered_distances_at_scale_and_projection_boundaries() {
        use crate::geometry::numerics::product::exact_power_of_two;
        let cases: &[(&[f64], &[f64])] = &[
            (
                &[0.0, -4.0, 1.0, 2.0, 3.0, 4.0, 6.0, 7.0, 8.0, 10.0, 9.0, 8.0],
                &[
                    0.0, 1.0, 4.0, 3.0, 0.0, -1.0, 2.0, -3.0, 5.0, 1.0, -2.0, 2.0,
                ],
            ),
            (
                &[
                    0.0, -1.0, 2.0, -2.0, 1.0, 3.0, -3.0, 4.0, -4.0, 5.0, -5.0, 0.0,
                ],
                &[
                    0.0, 1.0, 0.0, 1.0, -1.0, 0.0, 1.0, -1.0, 0.0, 1.0, -1.0, 0.0,
                ],
            ),
            (&[0.0, -2.0, 0.0, 2.0, 4.0, 6.0, 4.0], &[
                0.0, 1.0, 1.0, 2.0, 1.0, 1.0, 0.0,
            ]),
            (&[0.0, 1.0, 1.0, 1.0, 0.0], &[0.0, 1.0, -1.0, 0.0, 0.0]),
            (&[0.0, 1.0, 2.0, 3.0, 4.0], &[
                0.0,
                1.0,
                2.0_f64.next_up(),
                3.0,
                5.0,
            ]),
            (
                &[
                    0.0,
                    134_217_728.0,
                    134_217_732.0,
                    134_217_736.0,
                    268_435_457.0,
                ],
                &[
                    0.0,
                    134_217_729.0,
                    134_217_733.0,
                    134_217_737.0,
                    268_435_457.0,
                ],
            ),
            (&[0.0, -1.0, 1.0, 3.0, 5.0, 7.0, 9.0, 11.0, 4.0], &[
                0.0, 0.0, 1.0, -1.0, 1.0, -1.0, 1.0, 0.0, 0.0,
            ]),
        ];
        for &(xs, ys) in cases {
            for exponent in [-500, -100, 0, 100, 500] {
                let scale = exact_power_of_two(exponent);
                let xs: Vec<_> = xs.iter().map(|x| x * scale).collect();
                let ys: Vec<_> = ys.iter().map(|y| y * scale).collect();
                for tolerance in [0.0, 0.7, 1.0_f64.next_down(), 1.0, 1.0_f64.next_up(), 3.0] {
                    let tolerance = tolerance * scale;
                    assert_eq!(
                        split(&xs, &ys, 0, xs.len() - 1, tolerance).unwrap(),
                        exact_split(&xs, &ys, tolerance),
                        "coordinates={xs:?}/{ys:?}, tolerance={tolerance:?}",
                    );
                }
            }
        }
    }

    #[test]
    fn exact_distance_rejects_the_rounded_hull_counterexample() {
        let xs = [
            0.0,
            134_217_728.0,
            134_217_732.0,
            134_217_736.0,
            268_435_457.0,
        ];
        let ys = [
            0.0,
            134_217_729.0,
            134_217_733.0,
            134_217_737.0,
            268_435_457.0,
        ];
        assert_eq!(split(&xs, &ys, 0, 4, 0.707_106_79).unwrap(), None);
        assert_eq!(split(&xs, &ys, 0, 4, 0.7).unwrap(), Some(1));
    }

    #[test]
    fn exact_distance_breaks_simd_ties_by_source_index() {
        let xs: [f64; 32] = std::array::from_fn(|index| index as f64);
        let ys: [f64; 32] =
            std::array::from_fn(|index| if index == 2 || index == 9 { 1.0 } else { 0.0 });
        assert_eq!(split(&xs, &ys, 0, 31, 0.5).unwrap(), Some(2));
    }

    #[test]
    fn exact_distance_retains_clamped_and_extreme_decisions() {
        assert_eq!(
            split(&[0.0, -2.0, 1.0], &[0.0, 0.0, 0.0], 0, 2, 1.0).unwrap(),
            Some(1)
        );
        assert_eq!(
            split(&[0.0, 2.0, 0.0], &[0.0, 0.0, 0.0], 0, 2, 1.0).unwrap(),
            Some(1)
        );
        assert_eq!(
            split(&[-1e308, 0.0, 1e308], &[0.0, 1e307, 0.0], 0, 2, 1e308).unwrap(),
            None
        );
        assert_eq!(
            split(&[0.0, f64::from_bits(1), 0.0], &[0.0, 0.0, 0.0], 0, 2, 0.0).unwrap(),
            Some(1)
        );
    }

    #[test]
    fn exact_distance_resolves_tolerance_boundaries() {
        let xs = [0.0, 1.0, 2.0];
        let ys = [0.0, 1.0, 0.0];
        assert_eq!(split(&xs, &ys, 0, 2, 1.0).unwrap(), None);
        assert_eq!(split(&xs, &ys, 0, 2, 1.0_f64.next_up()).unwrap(), None);
        assert_eq!(split(&xs, &ys, 0, 2, 1.0_f64.next_down()).unwrap(), Some(1));
        assert_eq!(
            split(
                &[0.0, f64::from_bits(1), 0.0],
                &[0.0, 0.0, 0.0],
                0,
                2,
                f64::from_bits(1)
            )
            .unwrap(),
            None
        );
        assert_eq!(
            split(&[0.0, -1.0, 0.5, 1.0], &[0.0, 0.0, 1.0, 0.0], 0, 3, 0.5).unwrap(),
            Some(1)
        );
        assert_eq!(
            split(&[1e308, -1e308, 1e308], &[0.0, 0.0, 0.0], 0, 2, 1.0).unwrap(),
            Some(1)
        );
    }
}
