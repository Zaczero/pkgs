//! Double-double values carrying a rigorous absolute error bound: the cheap
//! certificate for a construction whose exact value is needed as its
//! nearest-even binary64. A few dozen flops, no heap, no multi-limb integer;
//! a possible tie or a magnitude outside the window declines to the exact
//! construction instead of rounding wrong.

use crate::geometry::exact_arith::{fast_two_sum, two_product_exact, two_sum};

/// The exact quantity lies within `err` of `hi + lo`.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Bounded {
    hi: f64,
    lo: f64,
    err: f64,
}

/// The per-operation error constant, 2^-98: every double-double operation
/// below is accurate to a few units of 2^-106 relative to its operands, and
/// the slack absorbs the rounding of the bound arithmetic itself.
const KAPPA: f64 = 3.155_443_620_884_047e-30;

/// The magnitude window every nonzero stored coordinate and coordinate
/// difference must lie in, so that no intermediate leaves the normal range
/// where the error-free transformations and the bounds hold.
const WINDOW: f64 = 1.606_938_044_258_990_3e60; // 2^200

/// Whether `value` is zero or inside the certificate's magnitude window.
pub(crate) fn inside_window(value: f64) -> bool {
    value == 0.0 || (value.abs() >= 1.0 / WINDOW && value.abs() <= WINDOW)
}

impl Bounded {
    /// A stored binary64, exactly.
    pub(crate) const fn exact(value: f64) -> Self {
        Self {
            hi: value,
            lo: 0.0,
            err: 0.0,
        }
    }

    /// `left - right`, exactly.
    pub(crate) fn exact_difference(left: f64, right: f64) -> Self {
        let (hi, lo) = two_sum(left, -right);
        Self { hi, lo, err: 0.0 }
    }

    /// The leading binary64 of the double-double value.
    pub(crate) const fn head(self) -> f64 {
        self.hi
    }

    pub(crate) const fn is_finite(self) -> bool {
        self.hi.is_finite() && self.lo.is_finite() && self.err.is_finite()
    }

    /// Whether the value is certainly zero: an exact difference of equal
    /// doubles, never a rounded one.
    pub(crate) fn is_exact_zero(self) -> bool {
        self.hi == 0.0 && self.lo == 0.0 && self.err == 0.0
    }

    fn magnitude(self) -> f64 {
        (self.hi.abs() + self.lo.abs()).next_up()
    }

    pub(crate) fn mul(self, other: Self) -> Option<Self> {
        let (p, e) = two_product_exact(self.hi, other.hi)?;
        let cross = self.hi.mul_add(other.lo, self.lo * other.hi);
        let (hi, lo) = fast_two_sum(p, e + cross);
        let product = self.magnitude() * other.magnitude();
        let err = (self.err * other.magnitude() + other.err * self.magnitude() + KAPPA * product)
            .next_up();
        (hi.is_finite() && lo.is_finite() && err.is_finite()).then_some(Self { hi, lo, err })
    }

    pub(crate) fn add(self, other: Self) -> Self {
        let (s, e) = two_sum(self.hi, other.hi);
        let (hi, lo) = fast_two_sum(s, e + self.lo + other.lo);
        let err = (self.err + other.err + KAPPA * (self.magnitude() + other.magnitude())).next_up();
        Self { hi, lo, err }
    }

    pub(crate) const fn neg(self) -> Self {
        Self {
            hi: -self.hi,
            lo: -self.lo,
            err: self.err,
        }
    }

    pub(crate) const fn twice(self) -> Self {
        Self {
            hi: 2.0 * self.hi,
            lo: 2.0 * self.lo,
            err: 2.0 * self.err,
        }
    }

    /// `self / other`, with `other` bounded away from zero by its own error.
    pub(crate) fn div(self, other: Self) -> Option<Self> {
        let divisor_low = (other.hi.abs() - other.lo.abs() - other.err).next_down();
        // A NaN divisor must decline too, which the partial ordering spells out.
        if divisor_low.partial_cmp(&0.0) != Some(std::cmp::Ordering::Greater) {
            return None;
        }
        let hi = self.hi / other.hi;
        // Remainder r = self - hi * other, in double-double.
        let (p, e) = two_product_exact(hi, other.hi)?;
        let (r_hi, r_lo) = {
            let (s, t) = two_sum(self.hi, -p);
            fast_two_sum(s, t - e + self.lo - hi * other.lo)
        };
        let lo = (r_hi + r_lo) / other.hi;
        let (hi, lo) = fast_two_sum(hi, lo);
        let quotient = Self { hi, lo, err: 0.0 }.magnitude();
        let err = (self.err / divisor_low + quotient * other.err / divisor_low + KAPPA * quotient)
            .next_up();
        Some(Self { hi, lo, err })
    }

    /// The nearest-even binary64 of the exact value, when the error interval
    /// lies strictly inside one rounding cell; `None` on a possible tie.
    pub(crate) fn certified_rounding(self) -> Option<f64> {
        if !self.err.is_finite() || self.err < 0.0 {
            return None;
        }
        let candidate = self.hi + self.lo;
        if !candidate.is_finite() || !candidate.is_normal() {
            return None;
        }
        // The distance from the candidate to hi + lo, with its own rounding.
        let (d, d_err) = two_sum(self.hi - candidate, self.lo);
        let offset = (d.abs() + d_err.abs()).next_up();
        let predecessor = candidate.next_down();
        let successor = candidate.next_up();
        if !(predecessor.is_finite() && successor.is_finite()) {
            return None;
        }
        // The cells are asymmetric at every binade boundary.  Distances are
        // formed before halving so the midpoint itself need not be a binary64.
        let lower_midpoint_distance = 0.5 * (candidate - predecessor);
        let upper_midpoint_distance = 0.5 * (successor - candidate);
        let reach = (offset + self.err).next_up();
        (reach < lower_midpoint_distance && reach < upper_midpoint_distance).then_some(candidate)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rounding_certificate_uses_signed_asymmetric_cells() {
        let one = Bounded::exact(1.0);
        let lower = 0.5 * (1.0 - 1.0_f64.next_down());
        let upper = 0.5 * (1.0_f64.next_up() - 1.0);
        assert!(lower < upper);
        assert_eq!(one.certified_rounding(), Some(1.0));
        assert_eq!(Bounded { err: lower, ..one }.certified_rounding(), None);
        assert_eq!(Bounded { err: upper, ..one }.certified_rounding(), None);

        // A symmetric upward cell would admit this interval even though its
        // lower end crosses the predecessor midpoint.
        assert_eq!(
            Bounded {
                err: 3.0 * 2.0_f64.powi(-55),
                ..one
            }
            .certified_rounding(),
            None
        );
        assert_eq!(
            Bounded {
                err: 2.0_f64.powi(-56),
                ..one
            }
            .certified_rounding(),
            Some(1.0)
        );

        let negative = Bounded {
            hi: -1.0,
            lo: 0.0,
            err: 2.0_f64.powi(-56),
        };
        assert_eq!(negative.certified_rounding(), Some(-1.0));
        assert_eq!(Bounded::exact(0.0).certified_rounding(), None);
        assert_eq!(Bounded::exact(f64::from_bits(1)).certified_rounding(), None);
        assert_eq!(Bounded::exact(f64::MAX).certified_rounding(), None);
    }
}
