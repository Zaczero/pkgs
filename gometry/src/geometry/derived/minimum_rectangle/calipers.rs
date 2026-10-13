//! Cartesian and retained-chart support advancement.

use super::score::candidate_area_order;
use super::{
    Basis, Candidate, ExactSum, Interval, LIMBS, Ordering, PRODUCT_MIN_EXP, PROJECTION_ERRBOUND,
    Point, RankedCandidate, Supports,
};

/// Adjacent binary64 values bracketing an exact positive fixed-limb value.
#[expect(
    clippy::indexing_slicing,
    reason = "high_bit is located by this fixed LIMBS range before its limb is read"
)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "the fixed limb count bounds every bit, exponent, and mantissa operation"
)]
pub(super) fn positive_interval(magnitude: &[u64; LIMBS]) -> Interval {
    let high_index = (0..LIMBS)
        .rev()
        .find(|&index| magnitude[index] != 0)
        .expect("positive magnitude has a high limb");
    let high_bit = high_index * 64 + (63 - magnitude[high_index].leading_zeros() as usize);
    let exponent = PRODUCT_MIN_EXP + high_bit as i32;
    if exponent > 1023 {
        return Interval {
            lo: f64::MAX,
            hi: f64::INFINITY,
        };
    }
    let discard = if exponent >= -1022 {
        high_bit - 52
    } else {
        usize::try_from(-1074 - PRODUCT_MIN_EXP).expect("fixed product exponent")
    };
    let mantissa = shifted_low_u64(magnitude, discard);
    let lo = if exponent >= -1022 {
        let stored = u64::try_from(exponent + 1023).expect("finite stored exponent");
        f64::from_bits((stored << 52) | (mantissa - (1_u64 << 52)))
    } else {
        f64::from_bits(mantissa)
    };
    Interval {
        lo,
        hi: if any_bits_below(magnitude, discard) {
            lo.next_up()
        } else {
            lo
        },
    }
}

#[expect(
    clippy::indexing_slicing,
    reason = "shift is bounded by the fixed exact-limb exponent range and the adjacent word is checked"
)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "word and bits come from shift division/remainder by 64 and the fixed limb range"
)]
pub(super) const fn shifted_low_u64(words: &[u64; LIMBS], shift: usize) -> u64 {
    let word = shift / 64;
    let bits = shift % 64;
    let mut value = words[word] >> bits;
    if bits != 0 && word + 1 < LIMBS {
        value |= words[word + 1] << (64 - bits);
    }
    value
}

#[expect(
    clippy::indexing_slicing,
    reason = "bit is bounded by the fixed exact-limb exponent range"
)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "bits is the remainder of division by 64 and remains below the word width"
)]
pub(super) fn any_bits_below(words: &[u64; LIMBS], bit: usize) -> bool {
    let word = bit / 64;
    let bits = bit % 64;
    words[..word].iter().any(|&value| value != 0)
        || (bits != 0 && words[word] & ((1_u64 << bits) - 1) != 0)
}

pub(super) fn exact_dot_delta(direction: (f64, f64), current: Point, next: Point) -> ExactSum {
    let mut sum = ExactSum::zero();
    sum.add_product(direction.0, next.x);
    sum.subtract_product(direction.0, current.x);
    sum.add_product(direction.1, next.y);
    sum.subtract_product(direction.1, current.y);
    sum
}

pub(super) fn projection_delta_order(
    direction: (f64, f64),
    current: Point,
    next: Point,
) -> Ordering {
    projection_delta_filter(direction, current, next)
        .unwrap_or_else(|| exact_dot_delta(direction, current, next).ordering())
}

pub(super) fn projection_delta_filter(
    direction: (f64, f64),
    current: Point,
    next: Point,
) -> Option<Ordering> {
    let dx = next.x - current.x;
    let dy = next.y - current.y;
    let first = direction.0 * dx;
    let second = direction.1 * dy;
    let estimate = first + second;
    let magnitude = first.abs() + second.abs();
    let first_ordinary = first.is_normal() || (first == 0.0 && (direction.0 == 0.0 || dx == 0.0));
    let second_ordinary =
        second.is_normal() || (second == 0.0 && (direction.1 == 0.0 || dy == 0.0));
    if first_ordinary
        && second_ordinary
        && magnitude.is_normal()
        && estimate.abs() > magnitude * PROJECTION_ERRBOUND
    {
        return Some(estimate.total_cmp(&0.0));
    }
    None
}

#[expect(
    clippy::indexing_slicing,
    reason = "caliper edge and support ordinals are bounded by the admitted convex hull"
)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "the hull has at least three vertices and edge/support ordinals remain in its bounded cycle"
)]
pub(super) fn select_minimum_candidate(hull: &[Point]) -> Candidate {
    let count = hull.len();
    let first_basis = (0..count)
        .find_map(|edge| Basis::from_edge(hull[edge], hull[(edge + 1) % count]))
        .expect("strict convex hull has a finite nonzero edge basis");
    // One exact global support establishes the first caliper.  In CCW support
    // order the remaining directions follow `-v, u, v, -u`; advancing those
    // unwrapped pointers from their predecessor crosses each first-edge
    // normal cone once, rather than paying four independent full scans.
    let outward_direction = (-first_basis.outward.0, -first_basis.outward.1);
    let initial_outward_min = exact_argmax(hull, outward_direction);
    let mut outward_min = advance_max(hull, outward_direction, initial_outward_min);
    let mut along_max = advance_max(hull, first_basis.along, outward_min);
    let mut outward_max = advance_max(hull, first_basis.outward, along_max);
    let mut along_min = advance_max(
        hull,
        (-first_basis.along.0, -first_basis.along.1),
        outward_max,
    );
    // The advanced pointer is the strict certificate and progression state.
    // Keep its exact-argmax representative for the first candidate: equal
    // support points are geometrically interchangeable, but their dot-product
    // rounding is part of the established byte-level output.
    let mut candidate_outward_min = initial_outward_min;
    let mut best: Option<RankedCandidate> = None;
    for edge in 0..count {
        let basis = Basis::from_edge(hull[edge], hull[(edge + 1) % count])
            .expect("strict convex hull edge has a basis");
        if edge != 0 {
            along_max = advance_max(hull, basis.along, along_max);
            outward_min = advance_max(hull, (-basis.outward.0, -basis.outward.1), outward_min);
            candidate_outward_min = outward_min;
            outward_max = advance_max(hull, basis.outward, outward_max);
            along_min = advance_max(hull, (-basis.along.0, -basis.along.1), along_min);
        }
        debug_assert!(support_stops_here(hull, basis.along, along_max));
        debug_assert!(support_stops_here(
            hull,
            (-basis.outward.0, -basis.outward.1),
            outward_min
        ));
        debug_assert!(support_stops_here(hull, basis.outward, outward_max));
        debug_assert!(support_stops_here(
            hull,
            (-basis.along.0, -basis.along.1),
            along_min
        ));
        let mut candidate = RankedCandidate::new(hull, Candidate {
            basis,
            supports: Supports {
                along_min: along_min % count,
                along_max: along_max % count,
                outward_min: candidate_outward_min % count,
                outward_max: outward_max % count,
            },
        });
        if best
            .as_mut()
            .is_none_or(|current| candidate_area_order(hull, &mut candidate, current).is_lt())
        {
            best = Some(candidate);
        }
    }
    best.expect("strict convex hull has at least one edge")
        .candidate
}

#[expect(
    clippy::indexing_slicing,
    reason = "the support ordinal and its successor are positions in the nonempty convex hull"
)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "the cyclic successor is bounded by the admitted hull length"
)]
pub(super) fn support_stops_here(hull: &[Point], direction: (f64, f64), index: usize) -> bool {
    let current = index % hull.len();
    let next = (current + 1) % hull.len();
    projection_delta_order(direction, hull[current], hull[next]).is_lt()
}

#[expect(
    clippy::indexing_slicing,
    reason = "the fold retains source ordinals from this hull slice"
)]
pub(super) fn exact_argmax(hull: &[Point], direction: (f64, f64)) -> usize {
    (1..hull.len()).fold(0, |best, index| {
        if projection_delta_order(direction, hull[best], hull[index]).is_gt() {
            index
        } else {
            best
        }
    })
}

#[expect(
    clippy::indexing_slicing,
    reason = "support and successor ordinals remain in the admitted convex hull"
)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "the unwrapped support pointer advances at most one hull length from an admitted ordinal"
)]
pub(super) fn advance_max(hull: &[Point], direction: (f64, f64), mut index: usize) -> usize {
    let count = hull.len();
    for _ in 0..count {
        let current = index % count;
        let next = (index + 1) % count;
        // Exact equality can cross a support plateau safely.  Stopping on a
        // rounded equality is the counterexample in `mrr_counterexamples.py`:
        // it can strand the pointer before a later strict increase.
        if projection_delta_order(direction, hull[current], hull[next]).is_lt() {
            break;
        }
        index += 1;
    }
    index
}

pub(super) fn axis_aligned_hull_rectangle(hull: &[Point]) -> Option<[Point; 5]> {
    (hull.len() == 4
        && hull
            .iter()
            .zip(hull.iter().cycle().skip(1))
            .all(|(point, next)| {
                point.x.to_bits() == next.x.to_bits() || point.y.to_bits() == next.y.to_bits()
            }))
    .then(|| {
        let (min_x, max_x, min_y, max_y) = hull.iter().fold(
            (
                f64::INFINITY,
                f64::NEG_INFINITY,
                f64::INFINITY,
                f64::NEG_INFINITY,
            ),
            |(min_x, max_x, min_y, max_y), point| {
                (
                    min_x.min(point.x),
                    max_x.max(point.x),
                    min_y.min(point.y),
                    max_y.max(point.y),
                )
            },
        );
        [
            Point::new_raw_xy(min_x, min_y),
            Point::new_raw_xy(max_x, min_y),
            Point::new_raw_xy(max_x, max_y),
            Point::new_raw_xy(min_x, max_y),
            Point::new_raw_xy(min_x, min_y),
        ]
    })
}

pub(super) fn exact_width(direction: (f64, f64), minimum: Point, maximum: Point) -> [u64; LIMBS] {
    let exact = exact_dot_delta(direction, minimum, maximum);
    let (sign, magnitude) = exact.signed_magnitude();
    debug_assert_eq!(sign, Ordering::Greater);
    magnitude
}
