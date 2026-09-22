//! Full-binary64 orientation sign.
//!
//! One ladder serves every caller: Shewchuk's A-stage filter settles
//! ordinary determinants, an identity tie settles axis-aligned collinearity
//! and repeated points, an outward interval settles most of the rest, and six
//! exact products of the original coordinates decide what remains.

use super::CCW_ERRBOUND_A;
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::exact_arith::product_sum::ExactSum;
use crate::geometry::tessellation::exact::Interval;
use crate::geometry::{Orientation, XY};

/// Exact sign of the orientation of `c` relative to `a -> b` over the stored
/// binary64 coordinates.
#[expect(
    clippy::float_cmp,
    reason = "an exact predicate's degeneracy test: these operands are compared for identity, not proximity, and a tolerance would make the predicate inexact"
)]
pub(in crate::geometry) fn stored_orientation_sign(
    ax: f64,
    ay: f64,
    bx: f64,
    by: f64,
    cx: f64,
    cy: f64,
) -> ExactSign {
    // The minimum-normal term covers underflow; an overflowed or NaN
    // determinant fails the comparison and continues down the ladder.
    let left = (bx - ax) * (cy - ay);
    let right = (by - ay) * (cx - ax);
    let det = left - right;
    if det.abs() > ((left.abs() + right.abs()) + f64::MIN_POSITIVE) * CCW_ERRBOUND_A {
        return if det > 0.0 {
            ExactSign::Positive
        } else {
            ExactSign::Negative
        };
    }
    if (ax == bx && ax == cx)
        || (ay == by && ay == cy)
        || (cx == ax && cy == ay)
        || (cx == bx && cy == by)
        || (ax == bx && ay == by)
    {
        return ExactSign::Zero;
    }
    let exact = Interval::exact;
    let (bax, bay) = (exact(bx).sub(exact(ax)), exact(by).sub(exact(ay)));
    let (cax, cay) = (exact(cx).sub(exact(ax)), exact(cy).sub(exact(ay)));
    if bax.is_finite()
        && bay.is_finite()
        && cax.is_finite()
        && cay.is_finite()
        && let Some(sign) = bax.mul(cay).sub(bay.mul(cax)).sign()
    {
        return sign;
    }
    let mut sum = ExactSum::zero();
    sum.add_product(bx, cy);
    sum.subtract_product(bx, ay);
    sum.subtract_product(ax, cy);
    sum.subtract_product(by, cx);
    sum.add_product(by, ax);
    sum.add_product(ay, cx);
    match sum.ordering() {
        std::cmp::Ordering::Less => ExactSign::Negative,
        std::cmp::Ordering::Equal => ExactSign::Zero,
        std::cmp::Ordering::Greater => ExactSign::Positive,
    }
}

/// Exact sign of `(b - a) x (d - c)` over stored binary64 coordinates: the
/// turn from one segment's direction to another's. The determinant has
/// `orient2d`'s shape — two products of rounded differences, one subtraction
/// — so the same A-stage bound certifies it; eight exact products of the
/// original coordinates decide the rest.
pub(in crate::geometry) fn stored_cross_sign(a: XY, b: XY, c: XY, d: XY) -> ExactSign {
    let left = (b.x - a.x) * (d.y - c.y);
    let right = (b.y - a.y) * (d.x - c.x);
    let det = left - right;
    if det.abs() > ((left.abs() + right.abs()) + f64::MIN_POSITIVE) * CCW_ERRBOUND_A {
        return if det > 0.0 {
            ExactSign::Positive
        } else {
            ExactSign::Negative
        };
    }
    let mut sum = ExactSum::zero();
    sum.add_product(b.x, d.y);
    sum.subtract_product(b.x, c.y);
    sum.subtract_product(a.x, d.y);
    sum.add_product(a.x, c.y);
    sum.subtract_product(b.y, d.x);
    sum.add_product(b.y, c.x);
    sum.add_product(a.y, d.x);
    sum.subtract_product(a.y, c.x);
    match sum.ordering() {
        std::cmp::Ordering::Less => ExactSign::Negative,
        std::cmp::Ordering::Equal => ExactSign::Zero,
        std::cmp::Ordering::Greater => ExactSign::Positive,
    }
}

pub(super) fn orientation_xy(ax: f64, ay: f64, bx: f64, by: f64, cx: f64, cy: f64) -> Orientation {
    match stored_orientation_sign(ax, ay, bx, by, cx, cy) {
        ExactSign::Negative => Orientation::Clockwise,
        ExactSign::Zero => Orientation::Collinear,
        ExactSign::Positive => Orientation::CounterClockwise,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::tessellation::exact::ExactDyadic;

    fn dyadic_difference(left: f64, right: f64) -> ExactDyadic {
        ExactDyadic::from_f64(left)
            .try_subtract(ExactDyadic::from_f64(right))
            .unwrap()
    }

    fn dyadic_cross_sign(left: &[ExactDyadic; 2], right: &[ExactDyadic; 2]) -> ExactSign {
        left[0]
            .try_product(&right[1])
            .unwrap()
            .try_subtract(left[1].try_product(&right[0]).unwrap())
            .unwrap()
            .sign()
    }

    fn dyadic_product_sign(ax: f64, ay: f64, bx: f64, by: f64, cx: f64, cy: f64) -> ExactSign {
        dyadic_cross_sign(&[dyadic_difference(bx, ax), dyadic_difference(by, ay)], &[
            dyadic_difference(cx, ax),
            dyadic_difference(cy, ay),
        ])
    }

    #[test]
    fn cross_sign_matches_exact_dyadics() {
        let tiny = f64::from_bits(1);
        let max = f64::MAX;
        let xy = XY::new;
        let cases = [
            (xy(0.0, 0.0), xy(1.0, 0.0), xy(5.0, 5.0), xy(7.0, 5.0)),
            (
                xy(0.0, 0.0),
                xy(1.0, 1.0),
                xy(0.0, 0.0),
                xy(1.0, 1.0_f64.next_up()),
            ),
            (xy(0.1, 0.2), xy(0.3, 0.7), xy(-4.0, 1.0), xy(-3.8, 1.5)),
            (xy(-max, -max), xy(max, max), xy(max, -max), xy(-max, max)),
            (
                xy(tiny, 0.0),
                xy(3.0 * tiny, tiny),
                xy(0.0, 0.0),
                xy(2.0 * tiny, tiny),
            ),
            (xy(1.0, 2.0), xy(3.0, 4.0), xy(3.0, 4.0), xy(1.0, 2.0)),
            (xy(0.0, 0.0), xy(-1.0, 3.0), xy(2.0, 2.0), xy(5.0, -1.0)),
        ];
        for (a, b, c, d) in cases {
            let expected = dyadic_cross_sign(
                &[dyadic_difference(b.x, a.x), dyadic_difference(b.y, a.y)],
                &[dyadic_difference(d.x, c.x), dyadic_difference(d.y, c.y)],
            );
            assert_eq!(
                stored_cross_sign(a, b, c, d),
                expected,
                "{a:?} {b:?} {c:?} {d:?}"
            );
        }
    }

    #[test]
    fn mixed_scale_diagonal_reaches_exact_fallback() {
        let mu = f64::from_bits(1);
        let length = 2.0_f64.powi(-20);
        let exact = Interval::exact;
        let bax = exact(length).sub(exact(0.0));
        let bay = exact(length).sub(exact(0.0));
        let cax = exact(mu).sub(exact(0.0));
        let cay = exact(2.0 * mu).sub(exact(0.0));
        assert_eq!(bax.mul(cay).sub(bay.mul(cax)).sign(), None);
        assert_eq!(
            orientation_xy(0.0, 0.0, length, length, mu, 2.0 * mu),
            Orientation::CounterClockwise
        );
    }

    #[test]
    fn overflowing_differences_reach_exact_fallback() {
        let max = f64::MAX;
        assert_eq!(
            orientation_xy(-max, -max, max, max, max, -max),
            Orientation::Clockwise
        );
    }

    #[test]
    fn axis_aligned_ties_are_collinear() {
        let subnormal = f64::from_bits(1);
        let max = f64::MAX;
        for (a, b, c) in [
            ((-0.0, -0.0), (1.0, -0.0), (-1.0, 0.0)),
            ((-0.0, -1.0), (0.0, 0.0), (0.0, 1.0)),
            (
                (subnormal, subnormal),
                (2.0 * subnormal, subnormal),
                (3.0 * subnormal, subnormal),
            ),
            ((max, -max), (max, 0.0), (max, max)),
        ] {
            assert_eq!(
                orientation_xy(a.0, a.1, b.0, b.1, c.0, c.1),
                Orientation::Collinear
            );
        }
    }

    #[test]
    fn one_ulp_axis_near_tie_remains_noncollinear() {
        let ay = 1.0_f64;
        let by = f64::from_bits(ay.to_bits() + 1);
        assert_eq!(
            orientation_xy(0.0, ay, 1.0, by, 2.0, ay),
            Orientation::Clockwise
        );
    }

    #[test]
    fn stored_orientation_preserves_all_underflow_ray_permutations() {
        let mu = f64::from_bits(1);
        let a = (1.0, -4096.0 * mu);
        let b = (-(2.0_f64.powi(-12)), 6144.0 * mu);
        let p = (f64::from_bits(0x3FD9_9733_3333_3333), 2048.0 * mu);
        for ((left, middle, right), expected) in [
            ((a, b, p), ExactSign::Positive),
            ((a, p, b), ExactSign::Negative),
            ((b, a, p), ExactSign::Negative),
            ((b, p, a), ExactSign::Positive),
            ((p, a, b), ExactSign::Positive),
            ((p, b, a), ExactSign::Negative),
        ] {
            assert_eq!(
                stored_orientation_sign(left.0, left.1, middle.0, middle.1, right.0, right.1),
                expected
            );
        }
    }

    #[test]
    fn raw_product_orientation_matches_dyadics_across_binary64_range() {
        let tiny = f64::from_bits(1);
        let max = f64::MAX;
        let triples = [
            ((0.0, 0.0), (max, 0.0), (0.0, tiny)),
            ((-max, -max), (max, max), (max, -max)),
            ((tiny, -tiny), (2.0 * tiny, tiny), (3.0 * tiny, -2.0 * tiny)),
            (
                (f64::MIN_POSITIVE, -max),
                (max, f64::MIN_POSITIVE),
                (tiny, 1.0),
            ),
            (
                (1.0, -4096.0 * tiny),
                (-(2.0_f64.powi(-12)), 6144.0 * tiny),
                (f64::from_bits(0x3FD9_9733_3333_3333), 2048.0 * tiny),
            ),
        ];
        assert!((triples[1].1.0 - triples[1].0.0).is_infinite());
        for (a, b, c) in triples {
            let expected = dyadic_product_sign(a.0, a.1, b.0, b.1, c.0, c.1);
            assert_eq!(
                stored_orientation_sign(a.0, a.1, b.0, b.1, c.0, c.1),
                expected
            );
        }
    }
}
