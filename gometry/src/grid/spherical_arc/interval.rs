//! Certified interval arithmetic on the sphere.
//!
//! Every bound here encloses its true value: an operation that cannot prove
//! an enclosure returns `None` rather than a number, so a caller never
//! mistakes a rounded answer for a certified one.

use super::{Midpoint as _, down, finite_minmax, outward_half_width, ulp, up, upward_sum};

/// An outward binary64 interval.  `None` is deliberately used for operations
/// which cannot certify a finite result; callers map it to their fail-open
/// state rather than inventing an epsilon.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct Bound {
    pub(crate) lo: f64,
    pub(crate) hi: f64,
}

impl Bound {
    pub(crate) const fn exact(value: f64) -> Option<Self> {
        if value.is_finite() {
            Some(Self {
                lo: value,
                hi: value,
            })
        } else {
            None
        }
    }

    pub(crate) const fn new(lo: f64, hi: f64) -> Option<Self> {
        if lo.is_finite() && hi.is_finite() && lo <= hi {
            Some(Self { lo, hi })
        } else {
            None
        }
    }

    pub(super) fn from_i128(value: i128) -> Option<Self> {
        // Every integer through 2^53 has an exact binary64 representation.
        // Preserve that structural fact: widening an exact native zero below
        // would make a squared-norm enclosure spuriously cross zero.
        const MAX_EXACT_BINARY64_INTEGER: i128 = 1_i128 << 53;
        if (-MAX_EXACT_BINARY64_INTEGER..=MAX_EXACT_BINARY64_INTEGER).contains(&value) {
            return Self::exact(value as f64);
        }
        let rounded = value as f64;
        rounded
            .is_finite()
            .then(|| Self::new(rounded.next_down(), rounded.next_up()))?
    }

    pub(crate) const fn contains_zero(self) -> bool {
        self.lo <= 0.0 && self.hi >= 0.0
    }

    pub(crate) const fn strictly_positive(self) -> bool {
        self.lo > 0.0
    }

    pub(crate) const fn strictly_negative(self) -> bool {
        self.hi < 0.0
    }

    pub(crate) const fn width(self) -> f64 {
        self.hi - self.lo
    }

    pub(super) const fn is_exact_zero(self) -> bool {
        self.lo == 0.0 && self.hi == 0.0
    }

    /// Structural endpoint identity is intentionally bit-exact.  It never
    /// turns an enclosure into a numeric tolerance test.
    pub(super) const fn is_exact_value(self, value: f64) -> bool {
        self.lo.to_bits() == value.to_bits() && self.hi.to_bits() == value.to_bits()
    }

    pub(crate) fn add(self, rhs: Self) -> Option<Self> {
        if self.is_exact_zero() {
            return Some(rhs);
        }
        if rhs.is_exact_zero() {
            return Some(self);
        }
        Self::new(down(self.lo + rhs.lo)?, up(self.hi + rhs.hi)?)
    }

    /// Adds quantities already proved non-negative without fabricating a
    /// negative lower endpoint by rounding `0 + 0` downward.
    pub(crate) fn add_nonnegative(self, rhs: Self) -> Option<Self> {
        if self.lo < 0.0 || rhs.lo < 0.0 {
            return None;
        }
        if self.is_exact_zero() {
            return Some(rhs);
        }
        if rhs.is_exact_zero() {
            return Some(self);
        }
        Self::new(down(self.lo + rhs.lo)?.max(0.0), up(self.hi + rhs.hi)?)
    }

    /// Multiplies quantities already proved non-negative while retaining the
    /// exact lower fact at zero.
    pub(crate) fn mul_nonnegative(self, rhs: Self) -> Option<Self> {
        if self.lo < 0.0 || rhs.lo < 0.0 {
            return None;
        }
        Self::new(down(self.lo * rhs.lo)?.max(0.0), up(self.hi * rhs.hi)?)
    }

    /// Divides a proved non-negative quantity by a proved positive one while
    /// retaining the exact lower fact at zero.
    pub(crate) fn div_nonnegative(self, rhs: Self) -> Option<Self> {
        if self.lo < 0.0 || !rhs.strictly_positive() {
            return None;
        }
        Self::new(down(self.lo / rhs.hi)?.max(0.0), up(self.hi / rhs.lo)?)
    }

    pub(crate) fn sub(self, rhs: Self) -> Option<Self> {
        if rhs.is_exact_zero() {
            return Some(self);
        }
        if self.is_exact_zero() {
            return Some(rhs.neg());
        }
        Self::new(down(self.lo - rhs.hi)?, up(self.hi - rhs.lo)?)
    }

    pub(crate) fn neg(self) -> Self {
        // Endpoints are finite by construction and negation is exact.
        Self {
            lo: -self.hi,
            hi: -self.lo,
        }
    }

    pub(crate) fn abs(self) -> Self {
        if self.contains_zero() {
            Self {
                lo: 0.0,
                hi: self.lo.abs().max(self.hi.abs()),
            }
        } else if self.strictly_negative() {
            self.neg()
        } else {
            self
        }
    }

    pub(crate) fn mul(self, rhs: Self) -> Option<Self> {
        if self.is_exact_zero() || rhs.is_exact_zero() {
            return Self::exact(0.0);
        }
        let products = [
            self.lo * rhs.lo,
            self.lo * rhs.hi,
            self.hi * rhs.lo,
            self.hi * rhs.hi,
        ];
        let (min, max) = finite_minmax(products)?;
        Self::new(down(min)?, up(max)?)
    }

    /// Outward square with the mathematical non-negativity fact retained when
    /// an enclosure crosses zero.  Generic interval multiplication must not
    /// use a negative lower endpoint as evidence against `x * x >= 0`.
    pub(crate) fn square(self) -> Option<Self> {
        if self.is_exact_zero() {
            return Self::exact(0.0);
        }
        if self.contains_zero() {
            let maximum = self.lo.abs().max(self.hi.abs());
            return Self::new(0.0, up(maximum * maximum)?);
        }
        self.mul(self)
    }

    pub(crate) fn div(self, rhs: Self) -> Option<Self> {
        if rhs.contains_zero() {
            return None;
        }
        if self.is_exact_zero() {
            return Self::exact(0.0);
        }
        let quotients = [
            self.lo / rhs.lo,
            self.lo / rhs.hi,
            self.hi / rhs.lo,
            self.hi / rhs.hi,
        ];
        let (min, max) = finite_minmax(quotients)?;
        Self::new(down(min)?, up(max)?)
    }

    pub(crate) fn sqrt(self) -> Option<Self> {
        if self.lo < 0.0 {
            return None;
        }
        let proposed_lo = self.lo.sqrt();
        let proposed_hi = self.hi.sqrt();
        // Hardware square root only proposes candidates.  Round them outward
        // *before* checking their squares: the lower candidate must be no
        // greater than sqrt(lo), and the upper candidate no smaller than
        // sqrt(hi).  Mere interval overlap would accept an inward bracket.
        let mut lo = if self.lo == 0.0 {
            0.0
        } else {
            down(proposed_lo)?
        };
        let mut hi = if self.hi == 0.0 {
            0.0
        } else {
            up(proposed_hi)?
        };
        // A one-ULP proposal can still be too narrow after the independently
        // outward multiplication in `verified_sqrt_enclosure`.  Expand only
        // adjacent binary64 candidates and accept the first interval whose
        // two square inequalities are proved; there is no tolerance branch.
        for _ in 0..=64 {
            if let Some(root) = verified_sqrt_enclosure(self, lo, hi) {
                return Some(root);
            }
            if lo > 0.0 {
                lo = lo.next_down();
            }
            hi = up(hi)?;
        }
        None
    }

    pub(super) const fn intersection(self, rhs: Self) -> Option<Self> {
        Self::new(self.lo.max(rhs.lo), self.hi.min(rhs.hi))
    }
}

/// Accepts a proposed root enclosure only after multiplication proves it
/// outward.  Keeping the check separate makes the two inequality directions
/// testable: interval overlap is not a square-root certificate.
pub(super) fn verified_sqrt_enclosure(input: Bound, lo: f64, hi: f64) -> Option<Bound> {
    if input.lo < 0.0 || !lo.is_finite() || !hi.is_finite() || lo < 0.0 || lo > hi {
        return None;
    }
    let lo_sq = Bound::exact(lo)?.mul(Bound::exact(lo)?)?;
    let hi_sq = Bound::exact(hi)?.mul(Bound::exact(hi)?)?;
    if lo_sq.hi > input.lo || hi_sq.lo < input.hi {
        return None;
    }
    Bound::new(lo, hi)
}

/// A scalar centre plus an outward radius.  The representation is compact for
/// the common scalar filter and converts to [`Bound`] before any branch.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct Ball {
    pub(crate) mid: f64,
    pub(crate) rad: f64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CertSign {
    Negative,
    Positive,
    Uncertain,
}

impl Ball {
    pub(crate) const fn exact(value: f64) -> Option<Self> {
        if value.is_finite() {
            Some(Self {
                mid: value,
                rad: 0.0,
            })
        } else {
            None
        }
    }

    pub(crate) fn new(mid: f64, rad: f64) -> Option<Self> {
        if !mid.is_finite() || !rad.is_finite() || rad < 0.0 {
            return None;
        }
        let result = Self { mid, rad };
        result.bound().map(|_| result)
    }

    pub(super) fn from_bound(value: Bound) -> Option<Self> {
        let mid = value.lo.certified_midpoint(value.hi)?;
        Self::new(mid, outward_half_width(value)?)
    }

    pub(crate) fn bound(self) -> Option<Bound> {
        Bound::new(down(self.mid - self.rad)?, up(self.mid + self.rad)?)
    }

    pub(crate) fn sign(self) -> CertSign {
        match self.bound() {
            Some(bound) if bound.strictly_positive() => CertSign::Positive,
            Some(bound) if bound.strictly_negative() => CertSign::Negative,
            _ => CertSign::Uncertain,
        }
    }

    pub(crate) fn add(self, rhs: Self) -> Option<Self> {
        let mid = self.mid + rhs.mid;
        let rad = upward_sum([self.rad, rhs.rad, ulp(mid)?])?;
        Self::new(mid, rad)
    }

    pub(super) fn neg(self) -> Option<Self> {
        Self::new(-self.mid, self.rad)
    }

    pub(super) fn scale_sign(self, sign: i8) -> Option<Self> {
        match sign {
            1 => Some(self),
            -1 => self.neg(),
            _ => None,
        }
    }

    pub(crate) fn mul(self, rhs: Self) -> Option<Self> {
        let mid = self.mid * rhs.mid;
        let rad = upward_sum([
            self.mid.abs() * rhs.rad,
            rhs.mid.abs() * self.rad,
            self.rad * rhs.rad,
            ulp(mid)?,
        ])?;
        Self::new(mid, rad)
    }
}
