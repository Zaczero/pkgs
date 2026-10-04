//! Certified orientation and double-double arithmetic for segments.
//! Orientation uses an outward interval and exact stored-dyadic fallback over
//! the complete finite binary64 domain.

use crate::error::Result;
use crate::geometry::Ordering;
use crate::geometry::exact_arith::bounded::{Bounded, inside_window};
use crate::geometry::exact_arith::{binary_zero, dd_add, dd_mul, two_product_exact, two_sum};
use crate::geometry::segments::{
    Orientation, Segment, XY, interpolate_f64, interpolate_segment_point,
};
use crate::geometry::tessellation::exact::{ExactDyadic, Interval, round_ratio};

#[path = "exact_orientation.rs"]
mod exact_orientation;
use exact_orientation::orientation_xy as certified_orientation_xy;
pub(in crate::geometry) use exact_orientation::{stored_cross_sign, stored_orientation_sign};

/// The exact homogeneous crossing of the carriers of `a` and `b`,
/// `[p d + r t, d]` for `p + r t / d` along `a`, with `r` and `s` the segment
/// directions, `d = r x s` and `t = (q - p) x s`. `d` keeps its sign.
pub(in crate::geometry) fn crossing_homogeneous(
    a: Segment,
    b: Segment,
) -> Result<[ExactDyadic; 3]> {
    let point = |point: XY| {
        [
            ExactDyadic::from_f64(point.x),
            ExactDyadic::from_f64(point.y),
            ExactDyadic::one(),
        ]
    };
    crossing_homogeneous_stored(
        &point(a.start),
        &point(a.end),
        &point(b.start),
        &point(b.end),
    )
}

pub(in crate::geometry) fn crossing_homogeneous_stored(
    a_start: &[ExactDyadic; 3],
    a_end: &[ExactDyadic; 3],
    b_start: &[ExactDyadic; 3],
    b_end: &[ExactDyadic; 3],
) -> Result<[ExactDyadic; 3]> {
    let (rx, ry) = (
        a_end[0].try_subtract_ref(&a_start[0])?,
        a_end[1].try_subtract_ref(&a_start[1])?,
    );
    let (sx, sy) = (
        b_end[0].try_subtract_ref(&b_start[0])?,
        b_end[1].try_subtract_ref(&b_start[1])?,
    );
    let (cx, cy) = (
        b_start[0].try_subtract_ref(&a_start[0])?,
        b_start[1].try_subtract_ref(&a_start[1])?,
    );
    let den = rx.try_product(&sy)?.try_subtract(ry.try_product(&sx)?)?;
    let t = cx.try_product(&sy)?.try_subtract(cy.try_product(&sx)?)?;
    let coordinate = |start: &ExactDyadic, direction: &ExactDyadic| -> Result<ExactDyadic> {
        den.try_product(start)?.try_add(t.try_product(direction)?)
    };
    Ok([
        coordinate(&a_start[0], &rx)?,
        coordinate(&a_start[1], &ry)?,
        den.clone(),
    ])
}

/// The nearest-even rounding of the crossing of the carriers of `a` and `b`,
/// certified in [`Bounded`] double-double arithmetic. `None` declines to
/// [`crossing_homogeneous`]: a possible tie, a rounded coordinate that is zero
/// or subnormal, a magnitude outside the certificate window, or carriers too
/// close to parallel for the bound.
pub(in crate::geometry) fn certified_crossing(a: Segment, b: Segment) -> Option<XY> {
    let stored = [
        a.start.x, a.start.y, a.end.x, a.end.y, b.start.x, b.start.y, b.end.x, b.end.y,
    ];
    if !stored.into_iter().all(inside_window) {
        return None;
    }
    let rx = Bounded::exact_difference(a.end.x, a.start.x);
    let ry = Bounded::exact_difference(a.end.y, a.start.y);
    let sx = Bounded::exact_difference(b.end.x, b.start.x);
    let sy = Bounded::exact_difference(b.end.y, b.start.y);
    let cx = Bounded::exact_difference(b.start.x, a.start.x);
    let cy = Bounded::exact_difference(b.start.y, a.start.y);
    if ![rx, ry, sx, sy, cx, cy]
        .into_iter()
        .all(|value| inside_window(value.head()))
    {
        return None;
    }
    let den = rx.mul(sy)?.add(ry.mul(sx)?.neg());
    let along = cx.mul(sy)?.add(cy.mul(sx)?.neg()).div(den)?;
    let crossing_x = rx.mul(along)?.add(Bounded::exact(a.start.x));
    let crossing_y = ry.mul(along)?.add(Bounded::exact(a.start.y));
    Some(XY::new(
        crossing_x.certified_rounding()?,
        crossing_y.certified_rounding()?,
    ))
}

/// A proper crossing rounded to nearest-even, with the exact homogeneous
/// point when the certificate declined and the rounding had to build it.
pub(in crate::geometry) struct RoundedCrossing {
    pub(in crate::geometry) point: XY,
    pub(in crate::geometry) exact: Option<[ExactDyadic; 3]>,
}

/// The nearest-even rounding of the crossing of the carriers of `a` and `b`:
/// the double-double certificate, or the exact construction where it
/// declines. Rounding is symmetric in the pair and monotone, so every caller
/// that meets this crossing agrees on its point, and a crossing inside both
/// segments rounds inside both envelopes.
pub(in crate::geometry) fn crossing_point(a: Segment, b: Segment) -> Result<RoundedCrossing> {
    crossing_point_with(a, b, || crossing_homogeneous(a, b))
}

pub(in crate::geometry) fn crossing_point_with(
    a: Segment,
    b: Segment,
    exact: impl FnOnce() -> Result<[ExactDyadic; 3]>,
) -> Result<RoundedCrossing> {
    if let Some(point) = certified_crossing(a, b) {
        return Ok(RoundedCrossing { point, exact: None });
    }
    let exact = exact()?;
    let unsigned = |value: f64| if value == 0.0 { 0.0 } else { value };
    Ok(RoundedCrossing {
        point: XY::new(
            unsigned(round_ratio(&exact[0], &exact[2])?),
            unsigned(round_ratio(&exact[1], &exact[2])?),
        ),
        exact: Some(exact),
    })
}

/// Quotient carrying its residual. A caller that keeps computing with the
/// ratio needs the tail: rounding it to binary64 first is what pushes an
/// exactly representable crossing an ulp off itself.
pub(super) fn dd_quotient(numerator: (f64, f64), denominator: (f64, f64)) -> (f64, f64) {
    let estimate = numerator.0 / denominator.0;
    let product = dd_mul((estimate, 0.0), denominator);
    let (residual, residual_err) = two_sum(numerator.0, -product.0);
    let residual = residual + (residual_err + (numerator.1 - product.1));
    two_sum(estimate, residual / denominator.0)
}

fn dd_div(numerator: (f64, f64), denominator: (f64, f64)) -> f64 {
    dd_quotient(numerator, denominator).0
}

/// [`interpolate_f64`] with a double-double fraction, rounded exactly once.
///
/// The convex form keeps every product bounded by `max(|start|, |end|)` for a
/// fraction in `[0, 1]`, so it inherits that function's overflow behaviour and
/// its endpoint exactness; what it removes is the two intermediate roundings
/// (the fraction, then the product) that placed a representable crossing such
/// as `(0, 1)` one ulp away from itself.
pub(super) fn dd_interpolate(start: f64, end: f64, fraction: (f64, f64)) -> f64 {
    let complement = dd_add((1.0, 0.0), (-fraction.0, -fraction.1));
    let value = dd_add(
        dd_mul((start, 0.0), complement),
        dd_mul((end, 0.0), fraction),
    );
    if value.0.is_finite() {
        value.0
    } else {
        interpolate_f64(start, end, fraction.0)
    }
}

#[derive(Clone, Copy)]
pub(in crate::geometry) struct DdInterval {
    head: f64,
    tail: Interval,
}

impl DdInterval {
    pub(in crate::geometry) const fn exact(value: f64) -> Self {
        Self {
            head: value,
            tail: Interval::exact(0.0),
        }
    }

    pub(in crate::geometry) fn difference(left: f64, right: f64) -> Option<Self> {
        let (head, tail) = two_sum(left, -right);
        head.is_finite().then_some(Self {
            head,
            tail: Interval::exact(tail),
        })
    }

    pub(in crate::geometry) fn add(self, other: Self) -> Option<Self> {
        let (head, tail) = two_sum(self.head, other.head);
        if !head.is_finite() {
            return None;
        }
        let tail = interval_add_nonzero(Interval::exact(tail), self.tail);
        let tail = interval_add_nonzero(tail, other.tail);
        tail.is_finite().then_some(Self { head, tail })
    }

    pub(in crate::geometry) fn sub(self, other: Self) -> Option<Self> {
        let (head, tail) = two_sum(self.head, -other.head);
        if !head.is_finite() {
            return None;
        }
        let tail = interval_add_nonzero(Interval::exact(tail), self.tail);
        let tail = if interval_is_exact_zero(other.tail) {
            tail
        } else {
            tail.sub(other.tail)
        };
        tail.is_finite().then_some(Self { head, tail })
    }

    /// Outward enclosure of the exact product of the two represented values.
    /// The high-high product is error-free only when its residual is
    /// representable (or the product is algebraically zero); every cross term,
    /// including low-low, is then accumulated through the shared outward
    /// interval.
    pub(in crate::geometry) fn mul(self, other: Self) -> Option<Self> {
        let head_is_zero = binary_zero(self.head) || binary_zero(other.head);
        let high_product = self.head * other.head;
        if !head_is_zero && !high_product.is_normal() {
            return None;
        }
        if !high_product.is_finite() {
            return None;
        }
        let (head, product_tail) = if head_is_zero {
            (high_product, 0.0)
        } else {
            two_product_exact(self.head, other.head)?
        };
        let mut tail = Interval::exact(product_tail);
        tail = interval_add_nonzero(
            tail,
            interval_mul_nonzero(Interval::exact(self.head), other.tail),
        );
        tail = interval_add_nonzero(
            tail,
            interval_mul_nonzero(Interval::exact(other.head), self.tail),
        );
        tail = interval_add_nonzero(tail, interval_mul_nonzero(self.tail, other.tail));
        tail.is_finite().then_some(Self { head, tail })
    }

    pub(in crate::geometry) fn square(self) -> Option<Self> {
        let head_is_zero = binary_zero(self.head);
        let high_product = self.head * self.head;
        if !head_is_zero && !high_product.is_normal() {
            return None;
        }
        if !high_product.is_finite() {
            return None;
        }
        let (head, product_tail) = if head_is_zero {
            (high_product, 0.0)
        } else {
            two_product_exact(self.head, self.head)?
        };
        let doubled_head = self.head * 2.0;
        if !doubled_head.is_finite() {
            return None;
        }
        let mut tail = Interval::exact(product_tail);
        tail = interval_add_nonzero(
            tail,
            interval_mul_nonzero(Interval::exact(doubled_head), self.tail),
        );
        tail = interval_add_nonzero(tail, interval_mul_nonzero(self.tail, self.tail));
        tail.is_finite().then_some(Self { head, tail })
    }

    pub(in crate::geometry) fn scale(self, value: f64) -> Option<Self> {
        self.mul(Self::exact(value))
    }

    fn bounds(self) -> Interval {
        if interval_is_exact_zero(self.tail) {
            Interval::exact(self.head)
        } else {
            Interval::exact(self.head).add(self.tail)
        }
    }

    pub(in crate::geometry) fn outward_bounds(self) -> (f64, f64) {
        let bounds = self.bounds();
        (bounds.lo, bounds.hi)
    }

    pub(in crate::geometry) fn approximate_pair(self) -> (f64, f64) {
        let midpoint = self.tail.lo * 0.5 + self.tail.hi * 0.5;
        (self.head, midpoint)
    }

    /// Round the enclosed value only when both outward endpoints land in the
    /// same binary64 cell. The endpoints stay in double-double form until
    /// this comparison; flattening the tail first would make the certificate
    /// circular.
    pub(in crate::geometry) fn rounded_common(self) -> Option<f64> {
        if !self.head.is_finite() || !self.tail.is_finite() {
            return None;
        }
        let lower = self.head + self.tail.lo;
        let upper = self.head + self.tail.hi;
        (lower.is_finite() && upper.is_finite() && lower.to_bits() == upper.to_bits())
            .then_some(lower)
    }

    pub(in crate::geometry) const fn is_exact_zero(self) -> bool {
        binary_zero(self.head) && interval_is_exact_zero(self.tail)
    }
}

fn signed_sum_order(left: f64, right: f64) -> Ordering {
    if binary_zero(left) {
        return if binary_zero(right) {
            Ordering::Equal
        } else {
            right.total_cmp(&0.0)
        };
    }
    if binary_zero(right) {
        return left.total_cmp(&0.0);
    }
    if left.is_sign_positive() == right.is_sign_positive() {
        return left.total_cmp(&0.0);
    }
    match left.abs().total_cmp(&right.abs()) {
        Ordering::Greater => left.total_cmp(&0.0),
        Ordering::Less => right.total_cmp(&0.0),
        Ordering::Equal => Ordering::Equal,
    }
}

/// Strictly compare two outward DD enclosures without rounding `head + tail`
/// through a single `f64`.
pub(in crate::geometry) fn dd_strict_cmp(left: DdInterval, right: DdInterval) -> Option<Ordering> {
    let difference = left.sub(right)?;
    if !difference.head.is_finite() || !difference.tail.is_finite() {
        return None;
    }
    let lo = signed_sum_order(difference.head, difference.tail.lo);
    let hi = signed_sum_order(difference.head, difference.tail.hi);
    (lo == hi && lo != Ordering::Equal).then_some(lo)
}

const fn interval_is_exact_zero(value: Interval) -> bool {
    binary_zero(value.lo) && binary_zero(value.hi)
}

fn interval_add_nonzero(left: Interval, right: Interval) -> Interval {
    if interval_is_exact_zero(left) {
        right
    } else if interval_is_exact_zero(right) {
        left
    } else {
        left.add(right)
    }
}

fn interval_mul_nonzero(left: Interval, right: Interval) -> Interval {
    if interval_is_exact_zero(left) || interval_is_exact_zero(right) {
        Interval::exact(0.0)
    } else if left.lo.to_bits() == left.hi.to_bits() && right.lo.to_bits() == right.hi.to_bits() {
        let product = left.lo * right.lo;
        Interval {
            lo: product.next_down(),
            hi: product.next_up(),
        }
    } else {
        left.mul(right)
    }
}

fn dd_interval_dot(
    left_a: DdInterval,
    left_b: DdInterval,
    right_a: DdInterval,
    right_b: DdInterval,
) -> Option<DdInterval> {
    left_a.mul(left_b)?.add(right_a.mul(right_b)?)
}

fn dd_interval_squared_norm(x: DdInterval, y: DdInterval) -> Option<DdInterval> {
    x.square()?.add(y.square()?)
}

fn half_ulp_margin(value: f64) -> Option<f64> {
    if !value.is_normal() {
        return None;
    }
    let below = (value - value.next_down()) * 0.5;
    let above = (value.next_up() - value) * 0.5;
    let margin = below.min(above);
    (margin > 0.0 && margin.is_finite()).then_some(margin)
}

fn residual_inside_rounding_cell(
    residual: DdInterval,
    denominator: DdInterval,
    rounded: f64,
) -> bool {
    let Some(margin) = half_ulp_margin(rounded) else {
        return false;
    };
    let residual = residual.bounds();
    let denominator = denominator.bounds();
    if !residual.is_finite() || !denominator.is_finite() || denominator.lo <= 0.0 {
        return false;
    }
    let residual_magnitude = residual.lo.abs().max(residual.hi.abs());
    let rounding_radius = Interval::exact(margin).mul(denominator).lo;
    residual_magnitude < rounding_radius
}

/// Prove that `fraction` is the correctly rounded exact ratio and return the
/// correctly rounded exact foot on both axes. The ordinary interpolation is
/// retained when it already occupies those cells; otherwise a compensated
/// coordinate proposal is admitted only after the same proof. The comparison
/// is strict, so midpoint ties and every range-error ambiguity decline.
fn certified_projection_coordinate(
    start: f64,
    delta: DdInterval,
    initial: f64,
    fraction: f64,
    ratio_residual: DdInterval,
    denominator: DdInterval,
) -> Option<f64> {
    let coordinate_residual = |rounded| {
        let scaled_delta = delta.scale(fraction)?;
        let affine = DdInterval::exact(start)
            .add(scaled_delta)?
            .sub(DdInterval::exact(rounded))?;
        // D * (exact_foot - rounded) =
        // D * (start + fraction * delta - rounded)
        //   + delta * (N - fraction * D).
        affine.mul(denominator)?.add(delta.mul(ratio_residual)?)
    };

    let initial_residual = coordinate_residual(initial)?;
    if residual_inside_rounding_cell(initial_residual, denominator, initial) {
        return Some(initial);
    }
    let correction = dd_div(
        initial_residual.approximate_pair(),
        denominator.approximate_pair(),
    );
    let corrected = initial + correction;
    let corrected_residual = coordinate_residual(corrected)?;
    residual_inside_rounding_cell(corrected_residual, denominator, corrected).then_some(corrected)
}

fn certified_projection_fraction(
    fraction: f64,
    segment: Segment,
    dx: DdInterval,
    dy: DdInterval,
    numerator: DdInterval,
    denominator: DdInterval,
) -> Option<XY> {
    let ratio_residual = certified_projection_ratio(fraction, numerator, denominator)?;

    let foot = interpolate_segment_point(segment.start, segment.end, fraction);
    Some(XY::new(
        certified_projection_coordinate(
            segment.start.x,
            dx,
            foot.x,
            fraction,
            ratio_residual,
            denominator,
        )?,
        certified_projection_coordinate(
            segment.start.y,
            dy,
            foot.y,
            fraction,
            ratio_residual,
            denominator,
        )?,
    ))
}

fn certified_projection_ratio(
    fraction: f64,
    numerator: DdInterval,
    denominator: DdInterval,
) -> Option<DdInterval> {
    if !fraction.is_normal() || !(0.0..1.0).contains(&fraction) {
        return None;
    }
    let scaled_denominator = denominator.scale(fraction)?;
    let ratio_residual = numerator.sub(scaled_denominator)?;
    if !residual_inside_rounding_cell(ratio_residual, denominator, fraction) {
        return None;
    }
    Some(ratio_residual)
}

/// Certified B-stage for opposing projection products. The raw quotient is
/// tried first to retain ordinary ratio and witness bits. A compensated
/// quotient is merely a second candidate, and its reconstructed coordinates
/// cannot leave this function until the shared outward interval proves every
/// affected rounding cell.
pub(super) fn certified_projection(
    point: XY,
    segment: Segment,
    raw_fraction: f64,
) -> Option<(f64, XY)> {
    let dx = DdInterval::difference(segment.end.x, segment.start.x)?;
    let dy = DdInterval::difference(segment.end.y, segment.start.y)?;
    let qx = DdInterval::difference(point.x, segment.start.x)?;
    let qy = DdInterval::difference(point.y, segment.start.y)?;
    let numerator = dd_interval_dot(qx, dx, qy, dy)?;
    let denominator = dd_interval_squared_norm(dx, dy)?;

    if certified_projection_ratio(raw_fraction, numerator, denominator).is_some() {
        return Some((
            raw_fraction,
            interpolate_segment_point(segment.start, segment.end, raw_fraction),
        ));
    }
    let compensated = dd_div(numerator.approximate_pair(), denominator.approximate_pair());
    if compensated.to_bits() == raw_fraction.to_bits() {
        return None;
    }
    certified_projection_fraction(compensated, segment, dx, dy, numerator, denominator)
        .map(|foot| (compensated, foot))
}

/// Exact orientation of `c` against the directed line `a -> b`.
///
/// A vertex identity (`c` is `a` or `b`, or the line is a point) is a zero
/// determinant no floating filter can certify, and callers produce it
/// constantly: a node against its own incident edges, adjacent ring edges as
/// a candidate pair. Settling it by comparison keeps the exact path for
/// genuine near-collinearity.
#[expect(
    clippy::float_cmp,
    reason = "vertex identity, not proximity: the operands either are the same stored coordinate or they are not"
)]
pub(in crate::geometry) fn orientation_xy(
    ax: f64,
    ay: f64,
    bx: f64,
    by: f64,
    cx: f64,
    cy: f64,
) -> Orientation {
    if (cx == ax && cy == ay) || (cx == bx && cy == by) || (ax == bx && ay == by) {
        return Orientation::Collinear;
    }
    certified_orientation_xy(ax, ay, bx, by, cx, cy)
}

pub(crate) fn orientation(a: impl Into<XY>, b: impl Into<XY>, c: impl Into<XY>) -> Orientation {
    let (a, b, c) = (a.into(), b.into(), c.into());
    orientation_xy(a.x, a.y, b.x, b.y, c.x, c.y)
}

/// Shewchuk's A-stage `orient2d` filter constant: a float determinant whose
/// magnitude exceeds this multiple of the two term magnitudes summed has the
/// exact sign (`(3 + 16eps) * eps` with `eps = 2^-53`).
pub(in crate::geometry) const CCW_ERRBOUND_A: f64 =
    (3.0 + 16.0 * (f64::EPSILON / 2.0)) * (f64::EPSILON / 2.0);

/// Exact ray-crossing decision for one straddling ring edge: does `a -> b`
/// cross the horizontal ray through `p` strictly RIGHT of `p`?
///
/// Callers must have established the straddle precondition
/// `(ay > py) != (by > py)` (which also guarantees `by != ay`). Equivalent to
/// the division form `px < (bx - ax) * ((py - ay) / (by - ay)) + ax`, but
/// decided from the robust orientation sign: an upward edge (`by > ay`)
/// crosses right of `p` iff `p` lies strictly counter-clockwise of `a -> b`;
/// a downward edge flips the reading. The division form materializes the
/// intersection X, so opposite-sign huge coordinates overflow `bx - ax` to
/// infinity and corrupt the crossing parity (wrong `contains` near
/// |x| ~ 1e308); the orientation route inherits `orientation_xy`'s
/// exponent-normalized exact fallback instead.
pub(in crate::geometry) fn ray_crossing_is_right(
    ax: f64,
    ay: f64,
    bx: f64,
    by: f64,
    px: f64,
    py: f64,
) -> bool {
    debug_assert!((ay > py) != (by > py), "ray-crossing straddle precondition");
    // All-input Ozaki filter first, textually matching the SIMD kernel. The
    // minimum-normal term covers underflow; uncertainty reaches the exact
    // stored-dyadic predicate.
    // Every escape hatch falls through the same comparison: a NaN or +-inf
    // `det` (term overflow) and a zero/near-tie `det` all fail `>`, taking
    // the exponent-normalized exact route below.
    let t1 = (bx - ax) * (py - ay);
    let t2 = (px - ax) * (by - ay);
    let det = t1 - t2;
    if det.abs() > ((t1.abs() + t2.abs()) + f64::MIN_POSITIVE) * CCW_ERRBOUND_A {
        return (det > 0.0) == (by > ay);
    }
    match orientation_xy(ax, ay, bx, by, px, py) {
        Orientation::Collinear => false,
        orientation @ (Orientation::Clockwise | Orientation::CounterClockwise) => {
            (orientation == Orientation::CounterClockwise) == (by > ay)
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ray_filter_declines_wrong_nonzero_underflow_sign() {
        let mu = f64::from_bits(1);
        let a = (1.0, -4096.0 * mu);
        let b = (-(2.0_f64.powi(-12)), 6144.0 * mu);
        let p = (f64::from_bits(0x3FD9_9733_3333_3333), 2048.0 * mu);
        assert_ne!(a.1 > p.1, b.1 > p.1);
        let t1 = (b.0 - a.0) * (p.1 - a.1);
        let t2 = (p.0 - a.0) * (b.1 - a.1);
        let determinant = t1 - t2;
        let bound = ((t1.abs() + t2.abs()) + f64::MIN_POSITIVE) * CCW_ERRBOUND_A;
        assert_eq!(determinant.to_bits(), (-mu).to_bits());
        assert!(determinant.abs() <= bound);
        assert!(ray_crossing_is_right(a.0, a.1, b.0, b.1, p.0, p.1));
    }

    #[test]
    fn dd_interval_bounds_contain_a_nonrepresentable_product() {
        let left = 0.1;
        let right = 0.2;
        let value = DdInterval::exact(left)
            .mul(DdInterval::exact(right))
            .expect("normal product");
        let (lower, upper) = value.outward_bounds();
        let exact = ExactDyadic::from_f64(left)
            .try_product(&ExactDyadic::from_f64(right))
            .unwrap();
        assert_ne!(exact.cmp(&ExactDyadic::from_f64(lower)), Ordering::Less);
        assert_ne!(exact.cmp(&ExactDyadic::from_f64(upper)), Ordering::Greater);
    }
}
