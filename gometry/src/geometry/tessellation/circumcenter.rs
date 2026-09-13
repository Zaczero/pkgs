//! The circumcenter of three sites, rounded to nearest-even and certified,
//! in floating-point expansions.
//!
//! The dual Voronoi lane needs every circumcenter twice: as a binary64 point
//! for the embedded diagram, and as an exact point for the comparisons that
//! decide topology. Building the exact homogeneous point first and rounding
//! it afterwards paid a multi-limb product and division per coordinate for
//! every face. Here the center is evaluated in double-double arithmetic with
//! a rigorous absolute error bound carried beside it, and the rounding is
//! certified when the whole error interval lies inside one rounding cell of
//! the candidate. A possible tie, a collinear triple, or a magnitude outside
//! the window where the bounds hold declines to the exact construction. A
//! few dozen flops, no heap, no multi-limb integer.

use crate::geometry::XY;
use crate::geometry::exact_arith::{two_product_exact, two_sum};

/// Certified outward bounds around a certified rounding: the exact value lies
/// inside `rounded`'s nearest-even cell, so it is between the two binary64
/// neighbours used by the comparison filter.
pub(super) struct CertifiedCenter {
    pub(super) rounded: XY,
    pub(super) bounds: [(f64, f64); 2],
}

/// A double-double value with a rigorous absolute error bound on it: the
/// exact quantity it stands for lies within `err` of `hi + lo`.
#[derive(Clone, Copy)]
struct Bounded {
    hi: f64,
    lo: f64,
    err: f64,
}

/// The per-operation error constant, 2^-98: every double-double operation
/// below is accurate to a few units of 2^-106 relative to its operands, and
/// the slack absorbs the rounding of the bound arithmetic itself.
const KAPPA: f64 = 3.155_443_620_884_047e-30;

impl Bounded {
    fn magnitude(self) -> f64 {
        (self.hi.abs() + self.lo.abs()).next_up()
    }

    fn exact_difference(left: f64, right: f64) -> Self {
        let (hi, lo) = two_sum(left, -right);
        Self { hi, lo, err: 0.0 }
    }

    fn mul(self, other: Self) -> Option<Self> {
        let (p, e) = two_product_exact(self.hi, other.hi)?;
        let cross = self.hi.mul_add(other.lo, self.lo * other.hi);
        let (hi, lo) = fast_two_sum(p, e + cross);
        let product = self.magnitude() * other.magnitude();
        let err = (self.err * other.magnitude() + other.err * self.magnitude() + KAPPA * product)
            .next_up();
        (hi.is_finite() && lo.is_finite() && err.is_finite()).then_some(Self { hi, lo, err })
    }

    fn add(self, other: Self) -> Self {
        let (s, e) = two_sum(self.hi, other.hi);
        let (hi, lo) = fast_two_sum(s, e + self.lo + other.lo);
        let err = (self.err + other.err + KAPPA * (self.magnitude() + other.magnitude())).next_up();
        Self { hi, lo, err }
    }

    fn neg(self) -> Self {
        Self {
            hi: -self.hi,
            lo: -self.lo,
            err: self.err,
        }
    }

    fn twice(self) -> Self {
        Self {
            hi: 2.0 * self.hi,
            lo: 2.0 * self.lo,
            err: 2.0 * self.err,
        }
    }

    /// `self / other`, with `other` bounded away from zero by its own error.
    fn div(self, other: Self) -> Option<Self> {
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
    fn certified_rounding(self) -> Option<f64> {
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

fn fast_two_sum(a: f64, b: f64) -> (f64, f64) {
    let s = a + b;
    (s, b - (s - a))
}

/// The magnitude window every nonzero coordinate and coordinate difference
/// must lie in, so that no intermediate leaves the normal range where the
/// error-free transformations and the bounds above hold.
const WINDOW: f64 = 1.606_938_044_258_990_3e60; // 2^200

fn inside_window(value: f64) -> bool {
    value == 0.0 || (value.abs() >= 1.0 / WINDOW && value.abs() <= WINDOW)
}

/// The circumcenter of `a`, `b`, `c` rounded to nearest-even and certified.
#[expect(
    clippy::many_single_char_names,
    reason = "a, b, c are the sites and d the doubled determinant, as in the formula"
)]
pub(super) fn certified_circumcenter(a: XY, b: XY, c: XY) -> Option<CertifiedCenter> {
    let ux = Bounded::exact_difference(b.x, a.x);
    let uy = Bounded::exact_difference(b.y, a.y);
    let vx = Bounded::exact_difference(c.x, a.x);
    let vy = Bounded::exact_difference(c.y, a.y);
    // This is the ingress proof for stored sites and rounded difference heads.
    // Cancellation in a derived numerator or quotient can leave the window,
    // so every later FMA decomposition remains fallible.
    let admitted = [a.x, a.y, b.x, b.y, c.x, c.y]
        .into_iter()
        .all(inside_window)
        && [ux, uy, vx, vy].into_iter().all(|d| inside_window(d.hi));
    if !admitted {
        return None;
    }
    // d = 2 (ux vy - vx uy); bb = |u|²; cc = |v|².
    let d = ux.mul(vy)?.add(vx.mul(uy)?.neg()).twice();
    let bb = ux.mul(ux)?.add(uy.mul(uy)?);
    let cc = vx.mul(vx)?.add(vy.mul(vy)?);
    // The center relative to a: (vy bb - uy cc, ux cc - vx bb) / d.
    let nx = vy.mul(bb)?.add(uy.mul(cc)?.neg());
    let ny = ux.mul(cc)?.add(vx.mul(bb)?.neg());
    let origin = |value: f64| Bounded {
        hi: value,
        lo: 0.0,
        err: 0.0,
    };
    let x = nx.div(d)?.add(origin(a.x));
    let y = ny.div(d)?.add(origin(a.y));
    if !(x.hi.is_finite() && y.hi.is_finite() && x.err.is_finite() && y.err.is_finite()) {
        return None;
    }
    let (x, y) = (x.certified_rounding()?, y.certified_rounding()?);
    Some(CertifiedCenter {
        rounded: XY::new(x, y),
        bounds: [(x.next_down(), x.next_up()), (y.next_down(), y.next_up())],
    })
}

/// Which frame side a bisector is met on: a vertical side fixes `x`, a
/// horizontal side fixes `y`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum SideAxis {
    FixedX,
    FixedY,
}

/// What the certificate says about a bisector meeting a frame side.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(super) enum BisectorHit {
    /// Outside the window or a possible tie: the exact construction decides.
    Declined,
    /// The bisector is exactly parallel to the side.
    Parallel,
    /// The certified nearest-even rounding of the free coordinate.
    Free(f64),
}

/// Where the perpendicular bisector of `a` and `b` meets the line on which
/// one coordinate equals `value`.
pub(super) fn certified_bisector_hit(a: XY, b: XY, axis: SideAxis, value: f64) -> BisectorHit {
    if ![a.x, a.y, b.x, b.y, value].into_iter().all(inside_window) {
        return BisectorHit::Declined;
    }
    let ux = Bounded::exact_difference(b.x, a.x);
    let uy = Bounded::exact_difference(b.y, a.y);
    if !(inside_window(ux.hi) && inside_window(uy.hi)) {
        return BisectorHit::Declined;
    }
    let exact = |v: f64| Bounded {
        hi: v,
        lo: 0.0,
        err: 0.0,
    };
    // 2 ux x + 2 uy y = |b|² - |a|².
    let (fixed_slope, free_slope) = match axis {
        SideAxis::FixedX => (ux, uy),
        SideAxis::FixedY => (uy, ux),
    };
    if free_slope.hi == 0.0 && free_slope.lo == 0.0 {
        return BisectorHit::Parallel;
    }
    let Some(bx_squared) = exact(b.x).mul(exact(b.x)) else {
        return BisectorHit::Declined;
    };
    let Some(by_squared) = exact(b.y).mul(exact(b.y)) else {
        return BisectorHit::Declined;
    };
    let Some(ax_squared) = exact(a.x).mul(exact(a.x)) else {
        return BisectorHit::Declined;
    };
    let Some(ay_squared) = exact(a.y).mul(exact(a.y)) else {
        return BisectorHit::Declined;
    };
    let squares = bx_squared
        .add(by_squared)
        .add(ax_squared.neg())
        .add(ay_squared.neg());
    let Some(fixed_term) = fixed_slope.mul(exact(value)) else {
        return BisectorHit::Declined;
    };
    let numerator = squares.add(fixed_term.twice().neg());
    let Some(free) = numerator.div(free_slope.twice()) else {
        return BisectorHit::Declined;
    };
    if !(free.hi.is_finite() && free.err.is_finite()) {
        return BisectorHit::Declined;
    }
    free.certified_rounding()
        .map_or(BisectorHit::Declined, BisectorHit::Free)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::tessellation::exact;

    fn oracle(a: XY, b: XY, c: XY) -> Option<XY> {
        exact::circumcenter(a, b, c).ok()?.round_nearest_even().ok()
    }

    fn agree(a: XY, b: XY, c: XY) -> bool {
        match (certified_circumcenter(a, b, c), oracle(a, b, c)) {
            (Some(center), Some(expected)) => {
                assert_eq!(
                    center.rounded.x.to_bits(),
                    expected.x.to_bits(),
                    "x for {a:?} {b:?} {c:?}"
                );
                assert_eq!(
                    center.rounded.y.to_bits(),
                    expected.y.to_bits(),
                    "y for {a:?} {b:?} {c:?}"
                );
                true
            },
            (None, _) => false,
            (Some(center), None) => panic!(
                "certified {center:?} where the exact construction failed",
                center = center.rounded
            ),
        }
    }

    #[test]
    fn rounding_certificate_uses_signed_asymmetric_cells() {
        let one = Bounded {
            hi: 1.0,
            lo: 0.0,
            err: 0.0,
        };
        let lower = 0.5 * (1.0 - 1.0_f64.next_down());
        let upper = 0.5 * (1.0_f64.next_up() - 1.0);
        assert!(lower < upper);
        assert_eq!(one.certified_rounding(), Some(1.0));
        assert_eq!(Bounded { err: lower, ..one }.certified_rounding(), None);
        assert_eq!(Bounded { err: upper, ..one }.certified_rounding(), None);

        // The old symmetric upward cell admitted this interval even though
        // its lower end crosses the predecessor midpoint.
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
        assert_eq!(
            Bounded {
                hi: 0.0,
                lo: 0.0,
                err: 0.0,
            }
            .certified_rounding(),
            None
        );
        assert_eq!(
            Bounded {
                hi: f64::from_bits(1),
                lo: 0.0,
                err: 0.0,
            }
            .certified_rounding(),
            None
        );
        assert_eq!(
            Bounded {
                hi: f64::MAX,
                lo: 0.0,
                err: 0.0,
            }
            .certified_rounding(),
            None
        );
    }

    #[test]
    fn known_bounded_triangle_keeps_a_positive_certificate() {
        let center =
            certified_circumcenter(XY::new(0.0, 0.0), XY::new(2.0, 0.0), XY::new(0.0, 2.0))
                .expect("a unit circumcenter is inside the proven fast domain");
        assert_eq!(center.rounded, XY::new(1.0, 1.0));
    }

    #[test]
    fn certified_centers_are_the_exact_nearest_even_rounding() {
        let mut state = 0x2545_F491_4F6C_DD1D_u64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 11) as f64 / (1_u64 << 53) as f64
        };
        let mut certified = 0_usize;
        let mut declined = 0_usize;
        for case in 0..200_000 {
            // Two of the five scales sit outside the certificate's window and
            // must decline to the exact construction rather than round wrong.
            let scale = match case % 5 {
                0 => 1.0,
                1 => 1e6,
                2 => 1e-6,
                3 => 1e150,
                _ => 1e-150,
            };
            let p = |next: &mut dyn FnMut() -> f64| {
                XY::new((next() - 0.5) * scale, (next() - 0.5) * scale)
            };
            let (a, b, c) = (p(&mut next), p(&mut next), p(&mut next));
            if agree(a, b, c) {
                certified += 1;
            } else {
                declined += 1;
            }
        }
        assert!(
            certified > 110_000 && declined >= 80_000,
            "certified {certified}, declined {declined}"
        );
    }

    #[test]
    fn integer_lattice_centers_and_ties_round_exactly() {
        // Right triangles on the lattice have circumcenters at half-integers,
        // which are exact; symmetric triples put the center on an axis.
        for (a, b, c) in [
            ((0.0, 0.0), (2.0, 0.0), (0.0, 2.0)),
            ((0.0, 0.0), (1.0, 0.0), (0.0, 1.0)),
            ((-1.0, 0.0), (1.0, 0.0), (0.0, 1.0)),
            ((3.0, 7.0), (11.0, 7.0), (3.0, 19.0)),
            ((0.0, 0.0), (1e15, 0.0), (0.0, 1e15)),
            (
                (1e300, 1e300),
                (1e300 + 1e284, 1e300),
                (1e300, 1e300 + 1e284),
            ),
            // A true tie: the center's x is 2^52 + 1/2, exactly halfway between
            // two doubles, so the certificate must decline and the exact
            // construction round to even.
            (
                (4_503_599_627_370_496.0, 0.0),
                (4_503_599_627_370_497.0, 0.0),
                (4_503_599_627_370_496.0, 4.0),
            ),
        ] {
            let (a, b, c) = (XY::new(a.0, a.1), XY::new(b.0, b.1), XY::new(c.0, c.1));
            agree(a, b, c);
        }
    }

    #[test]
    fn certified_bisector_hits_are_the_exact_nearest_even_rounding() {
        let mut state = 0x9E37_79B9_7F4A_7C15_u64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            (state >> 11) as f64 / (1_u64 << 53) as f64
        };
        let mut certified = 0_usize;
        for case in 0..100_000 {
            let scale = if case % 2 == 0 { 1.0 } else { 1e5 };
            let a = XY::new((next() - 0.5) * scale, (next() - 0.5) * scale);
            let b = XY::new((next() - 0.5) * scale, (next() - 0.5) * scale);
            let value = (next() - 0.5) * scale * 4.0;
            let axis = if case % 3 == 0 {
                SideAxis::FixedX
            } else {
                SideAxis::FixedY
            };
            let BisectorHit::Free(free) = certified_bisector_hit(a, b, axis, value) else {
                continue;
            };
            let side = match axis {
                SideAxis::FixedX => (XY::new(value, -1.0e9), XY::new(value, 1.0e9)),
                SideAxis::FixedY => (XY::new(-1.0e9, value), XY::new(1.0e9, value)),
            };
            let hit = exact::line_intersection(
                &exact::ExactLine::perpendicular_bisector(a, b),
                &exact::ExactLine::through_points(
                    &exact::ExactPoint::from_xy(side.0),
                    &exact::ExactPoint::from_xy(side.1),
                )
                .unwrap(),
            )
            .round_nearest_even()
            .unwrap();
            let expected = match axis {
                SideAxis::FixedX => hit.y,
                SideAxis::FixedY => hit.x,
            };
            assert_eq!(
                free.to_bits(),
                expected.to_bits(),
                "{a:?} {b:?} {axis:?} {value}"
            );
            certified += 1;
        }
        assert!(certified > 95_000, "certified {certified}");
        // Sites on a horizontal line have a vertical bisector: parallel to a
        // vertical side, and meeting a horizontal side at x = 1.
        assert_eq!(
            certified_bisector_hit(XY::new(0.0, 0.0), XY::new(2.0, 0.0), SideAxis::FixedX, 5.0),
            BisectorHit::Parallel
        );
        assert_eq!(
            certified_bisector_hit(XY::new(0.0, 0.0), XY::new(2.0, 0.0), SideAxis::FixedY, 5.0),
            BisectorHit::Free(1.0)
        );
        // A true tie at 2^52 + 1/2 must decline rather than round either way.
        let two52 = 4_503_599_627_370_496.0;
        assert_eq!(
            certified_bisector_hit(
                XY::new(two52, 0.0),
                XY::new(two52 + 1.0, 0.0),
                SideAxis::FixedY,
                7.0
            ),
            BisectorHit::Declined
        );
        let tie = certified_circumcenter(
            XY::new(two52, 0.0),
            XY::new(two52 + 1.0, 0.0),
            XY::new(two52, 4.0),
        );
        assert!(
            tie.is_none(),
            "a midpoint tie must decline to the exact construction"
        );
    }

    #[test]
    fn collinear_and_overflowing_triples_decline() {
        assert!(
            certified_circumcenter(XY::new(0.0, 0.0), XY::new(1.0, 1.0), XY::new(2.0, 2.0))
                .is_none()
        );
        assert!(
            certified_circumcenter(XY::new(0.0, 0.0), XY::new(1e200, 0.0), XY::new(0.0, 1e200))
                .is_none()
        );
    }
}
