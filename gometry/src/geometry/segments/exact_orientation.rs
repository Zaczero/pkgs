//! Full-binary64 orientation sign.
//!
//! The fast filter first applies the standard determinant error bound, then
//! rounds an outward interval for ambiguous lanes. Unresolved cases fall back
//! to an exact sum of products of the original coordinates.

use super::CCW_ERRBOUND_A;
use crate::geometry::Orientation;
use crate::geometry::exact_arith::ExactSign;
use crate::geometry::exact_arith::product_sum::ExactSum;
use crate::geometry::tessellation::exact::Interval;

/// Exact sign of the stored-binary64 orientation after the caller's fast path
/// has declined. The outward interval proves ordinary cases; six exact raw
/// coordinate products resolve its remaining full-range cases.
pub(in crate::geometry) fn stored_orientation_sign(
    ax: f64,
    ay: f64,
    bx: f64,
    by: f64,
    cx: f64,
    cy: f64,
) -> ExactSign {
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
    sum.add_product(1, bx, cy);
    sum.add_product(-1, bx, ay);
    sum.add_product(-1, ax, cy);
    sum.add_product(-1, by, cx);
    sum.add_product(1, by, ax);
    sum.add_product(1, ay, cx);
    match sum.ordering() {
        std::cmp::Ordering::Less => ExactSign::Negative,
        std::cmp::Ordering::Equal => ExactSign::Zero,
        std::cmp::Ordering::Greater => ExactSign::Positive,
    }
}

#[expect(
    clippy::float_cmp,
    reason = "an exact predicate's degeneracy test: these operands are compared for identity, not proximity, and a tolerance would make the predicate inexact"
)]
pub(super) fn orientation_xy(ax: f64, ay: f64, bx: f64, by: f64, cx: f64, cy: f64) -> Orientation {
    // Certify ordinary finite coordinates with the first Shewchuk filter before
    // constructing outward intervals. Overflow, underflow, and near-collinear
    // cases continue through the complete binary64 path below.
    let (bax, bay) = (bx - ax, by - ay);
    let (cax, cay) = (cx - ax, cy - ay);
    let left = bax * cay;
    let right = bay * cax;
    let det = left - right;
    let magnitude = left.abs() + right.abs();
    if det.is_finite()
        && magnitude.is_finite()
        && det.abs() > (magnitude + f64::MIN_POSITIVE) * CCW_ERRBOUND_A
    {
        return if det > 0.0 {
            Orientation::CounterClockwise
        } else {
            Orientation::Clockwise
        };
    }
    if (ax == bx && ax == cx) || (ay == by && ay == cy) {
        return Orientation::Collinear;
    }
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
        ExactDyadic::from_f64(left).subtract(ExactDyadic::from_f64(right))
    }

    fn dyadic_product_sign(ax: f64, ay: f64, bx: f64, by: f64, cx: f64, cy: f64) -> ExactSign {
        dyadic_difference(bx, ax)
            .product(&dyadic_difference(cy, ay))
            .subtract(dyadic_difference(by, ay).product(&dyadic_difference(cx, ax)))
            .sign()
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
        let b = (-2.0_f64.powi(-12), 6144.0 * mu);
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
                (-2.0_f64.powi(-12), 6144.0 * tiny),
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
