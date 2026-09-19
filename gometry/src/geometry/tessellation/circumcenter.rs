//! The circumcenter of three sites, rounded to nearest-even and certified.
//!
//! The dual Voronoi lane needs every circumcenter twice: as a binary64 point
//! for the embedded diagram, and as an exact point for the comparisons that
//! decide topology. The center is evaluated in [`Bounded`] double-double
//! arithmetic, so its rounding is certified without building the exact
//! homogeneous point; a collinear triple, a possible tie, or a magnitude
//! outside the window declines to the exact construction.

use crate::geometry::XY;
use crate::geometry::exact_arith::bounded::{Bounded, inside_window};

/// Certified outward bounds around a certified rounding: the exact value lies
/// inside `rounded`'s nearest-even cell, so it is between the two binary64
/// neighbours used by the comparison filter.
pub(super) struct CertifiedCenter {
    pub(super) rounded: XY,
    pub(super) bounds: [(f64, f64); 2],
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
        && [ux, uy, vx, vy]
            .into_iter()
            .all(|d| inside_window(d.head()));
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
    let x = nx.div(d)?.add(Bounded::exact(a.x));
    let y = ny.div(d)?.add(Bounded::exact(a.y));
    if !(x.is_finite() && y.is_finite()) {
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
    if !(inside_window(ux.head()) && inside_window(uy.head())) {
        return BisectorHit::Declined;
    }
    let exact = Bounded::exact;
    // 2 ux x + 2 uy y = |b|² - |a|².
    let (fixed_slope, free_slope) = match axis {
        SideAxis::FixedX => (ux, uy),
        SideAxis::FixedY => (uy, ux),
    };
    if free_slope.is_exact_zero() {
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
    if !free.is_finite() {
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
        for case in 0..20_000 {
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
            certified > 11_000 && declined >= 8_000,
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
        for case in 0..20_000 {
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
                &exact::ExactLine::perpendicular_bisector(a, b).unwrap(),
                &exact::ExactLine::through_points(
                    &exact::ExactPoint::from_xy(side.0),
                    &exact::ExactPoint::from_xy(side.1),
                )
                .unwrap(),
            )
            .unwrap()
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
        assert!(certified > 19_000, "certified {certified}");
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
