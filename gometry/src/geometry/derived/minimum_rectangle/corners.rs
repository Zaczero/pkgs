//! Cartesian rectangle emission and finite-corner repair.

use super::calipers::{exact_dot_delta, positive_interval, projection_delta_order};
use super::score::{add_triple_magnitude, cmp_wide, wide_product};
use super::{
    Axis, Basis, Candidate, DOUBLE_LIMBS, ExactSum, GeometryErrorKind, Interval, LIMBS, Ordering,
    Orientation, PROJECTION_ERRBOUND, Point, PreferredCorner, Result, SupportHalfplane,
    TRIPLE_LIMBS, axis_pow2_scale, cmp_magnitude, orientation, two_sum,
};

impl Candidate {
    #[expect(
        clippy::indexing_slicing,
        reason = "caliper support IDs are source ordinals into this same convex hull"
    )]
    pub(super) fn emit(self, hull: &[Point]) -> Result<[Point; 5]> {
        // The legacy formula intentionally omits /N.  It is retained only when
        // the rounded quadrilateral itself carries the constant-time actual
        // edge certificate below; this is the compatibility fast path, never
        // an enclosure test over the hull.
        if let Some([legacy_a, legacy_b, legacy_c, legacy_d]) = self.legacy_corners(hull)
            && legacy_rectangle_is_certified(hull, &[legacy_a, legacy_b, legacy_c, legacy_d], self)
        {
            return Ok([legacy_a, legacy_b, legacy_c, legacy_d, legacy_a]);
        }

        let supports = self.supports;
        let [preferred_a, preferred_b, preferred_c, preferred_d] = self
            .correct_corners(hull)
            .ok_or_else(minimum_rectangle_not_representable)?;
        let [corner_a, corner_b, corner_c, corner_d] = [
            emit_corner(
                self.basis,
                SupportHalfplane {
                    gradient: (-self.basis.along.0, -self.basis.along.1),
                    support: hull[supports.along_min],
                },
                SupportHalfplane {
                    gradient: (-self.basis.outward.0, -self.basis.outward.1),
                    support: hull[supports.outward_min],
                },
                &preferred_a,
            ),
            emit_corner(
                self.basis,
                SupportHalfplane {
                    gradient: self.basis.along,
                    support: hull[supports.along_max],
                },
                SupportHalfplane {
                    gradient: (-self.basis.outward.0, -self.basis.outward.1),
                    support: hull[supports.outward_min],
                },
                &preferred_b,
            ),
            emit_corner(
                self.basis,
                SupportHalfplane {
                    gradient: self.basis.along,
                    support: hull[supports.along_max],
                },
                SupportHalfplane {
                    gradient: self.basis.outward,
                    support: hull[supports.outward_max],
                },
                &preferred_c,
            ),
            emit_corner(
                self.basis,
                SupportHalfplane {
                    gradient: (-self.basis.along.0, -self.basis.along.1),
                    support: hull[supports.along_min],
                },
                SupportHalfplane {
                    gradient: self.basis.outward,
                    support: hull[supports.outward_max],
                },
                &preferred_d,
            ),
        ];
        let corner_a = corner_a?;
        let corner_b = corner_b?;
        let corner_c = corner_c?;
        let corner_d = corner_d?;
        Ok([corner_a, corner_b, corner_c, corner_d, corner_a])
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "candidate support ordinals are generated from this same convex hull"
    )]
    pub(super) fn projection_scalars(self, hull: &[Point]) -> Option<(f64, f64, f64, f64)> {
        let supports = self.supports;
        let dot =
            |direction: (f64, f64), point: Point| direction.0 * point.x + direction.1 * point.y;
        let min_along = dot(self.basis.along, hull[supports.along_min]);
        let max_along = dot(self.basis.along, hull[supports.along_max]);
        let min_outward = dot(self.basis.outward, hull[supports.outward_min]);
        // Keep the legacy evaluation order: its raw output bytes are retained
        // whenever the actual-edge certificate succeeds.
        let max_outward =
            min_outward + (dot(self.basis.outward, hull[supports.outward_max]) - min_outward);
        (min_along.is_finite()
            && max_along.is_finite()
            && min_outward.is_finite()
            && max_outward.is_finite())
        .then_some((min_along, max_along, min_outward, max_outward))
    }

    pub(super) fn legacy_corners(self, hull: &[Point]) -> Option<[Point; 4]> {
        let (minimum, maximum, base, ceiling) = self.projection_scalars(hull)?;
        let corner = |along: f64, outward: f64| {
            Point::new_raw_xy(
                self.basis.along.0 * along + self.basis.outward.0 * outward,
                self.basis.along.1 * along + self.basis.outward.1 * outward,
            )
        };
        let corners = [
            corner(minimum, base),
            corner(maximum, base),
            corner(maximum, ceiling),
            corner(minimum, ceiling),
        ];
        corners
            .iter()
            .all(|point| point.x.is_finite() && point.y.is_finite())
            .then_some(corners)
    }

    #[expect(
        clippy::indexing_slicing,
        reason = "candidate support ordinals are generated from this same convex hull"
    )]
    pub(super) fn correct_corners(self, hull: &[Point]) -> Option<[PreferredCorner; 4]> {
        let supports = self.supports;
        let origin = hull[supports.along_min];
        Some([
            self.unframe_corner(origin, hull[supports.along_min], hull[supports.outward_min])?,
            self.unframe_corner(origin, hull[supports.along_max], hull[supports.outward_min])?,
            self.unframe_corner(origin, hull[supports.along_max], hull[supports.outward_max])?,
            self.unframe_corner(origin, hull[supports.along_min], hull[supports.outward_max])?,
        ])
    }

    /// Intersect selected support lines in an anisotropically scaled frame.
    /// X/Y scales are independent, and gradients transform contravariantly,
    /// so the original world-coordinate support lines remain the construction
    /// contract. This is four O(1) support-pair intersections, never a local
    /// hull or a later full-hull enclosure pass.
    pub(super) fn unframe_corner(
        self,
        origin: Point,
        along_support: Point,
        outward_support: Point,
    ) -> Option<PreferredCorner> {
        let max_x = origin
            .x
            .abs()
            .max(along_support.x.abs())
            .max(outward_support.x.abs());
        let max_y = origin
            .y
            .abs()
            .max(along_support.y.abs())
            .max(outward_support.y.abs());
        let sx = axis_pow2_scale(max_x);
        let sy = axis_pow2_scale(max_y);
        if !sx.is_finite() || sx <= 0.0 || !sy.is_finite() || sy <= 0.0 {
            return None;
        }
        let equal_scales = sx.to_bits() == sy.to_bits();
        let (origin_x, origin_y, along_x, along_y, outward_x, outward_y) = if sx.to_bits()
            == 1.0_f64.to_bits()
            && sy.to_bits() == 1.0_f64.to_bits()
            && origin.x.to_bits() == 0.0_f64.to_bits()
            && origin.y.to_bits() == 0.0_f64.to_bits()
        {
            (
                0.0,
                0.0,
                along_support.x,
                along_support.y,
                outward_support.x,
                outward_support.y,
            )
        } else {
            (
                origin.x * sx,
                origin.y * sy,
                along_support.x * sx,
                along_support.y * sy,
                outward_support.x * sx,
                outward_support.y * sy,
            )
        };
        // Equal axes are the isotropic degeneracy of this frame. Preserve its
        // established operation order exactly: that is the ordinary-path
        // byte-compatibility contract. Only a genuinely unequal frame needs
        // the contravariant gradient transform.
        let (along, outward) = if equal_scales {
            (self.basis.along, self.basis.outward)
        } else {
            (
                (self.basis.along.0 / sx, self.basis.along.1 / sy),
                (self.basis.outward.0 / sx, self.basis.outward.1 / sy),
            )
        };
        if ![
            origin_x, origin_y, along_x, along_y, outward_x, outward_y, along.0, along.1,
            outward.0, outward.1,
        ]
        .into_iter()
        .all(|value| value.is_finite())
        {
            return None;
        }
        let along_residual = along.0 * (along_x - origin_x) + along.1 * (along_y - origin_y);
        let outward_residual =
            outward.0 * (outward_x - origin_x) + outward.1 * (outward_y - origin_y);
        let determinant = along.0 * outward.1 - along.1 * outward.0;
        if !along_residual.is_finite()
            || !outward_residual.is_finite()
            || !determinant.is_finite()
            || determinant <= 0.0
        {
            return None;
        }
        let denominator = [determinant * sx, determinant * sy];
        if !denominator
            .into_iter()
            .all(|value| value.is_finite() && value > 0.0)
        {
            return None;
        }
        let x_value = (origin_x * determinant + along_residual * outward.1
            - along.1 * outward_residual)
            / determinant
            / sx;
        let y_value = (origin_y * determinant + along.0 * outward_residual
            - along_residual * outward.0)
            / determinant
            / sy;
        let mut x_numerator = ExactSum::zero();
        x_numerator.add_product(origin_x, determinant);
        x_numerator.add_product(along_residual, outward.1);
        x_numerator.subtract_product(along.1, outward_residual);
        let mut y_numerator = ExactSum::zero();
        y_numerator.add_product(origin_y, determinant);
        y_numerator.add_product(along.0, outward_residual);
        y_numerator.subtract_product(along_residual, outward.0);
        Some(PreferredCorner {
            point: Point::new_raw_xy(x_value, y_value),
            numerator: [x_numerator, y_numerator],
            denominator,
            anisotropic_frame: !equal_scales,
        })
    }
}

pub(super) fn exact_cross_delta(start: Point, end: Point, current: Point, next: Point) -> ExactSum {
    // cross(end-start, next-current), expanded before any subtraction so this
    // remains exact even when an intermediate world-coordinate difference
    // overflows.
    let mut sum = ExactSum::zero();
    sum.add_product(end.x, next.y);
    sum.subtract_product(start.x, next.y);
    sum.subtract_product(end.x, current.y);
    sum.add_product(start.x, current.y);
    sum.subtract_product(end.y, next.x);
    sum.add_product(start.y, next.x);
    sum.add_product(end.y, current.x);
    sum.subtract_product(start.y, current.x);
    sum
}

/// Adaptive sign of `cross(end - start, next - current)`.  The fast branch is
/// the standard first-stage determinant filter: every subtraction and product
/// is normal (or an exact zero), and the determinant clears a deliberately
/// conservative rounding bound.  All cancellation, subnormal, and overflow
/// cases retain the fixed-limb dyadic expansion below.
///
/// This is the certificate's ordinary path.  It is not a weaker local test:
/// the returned sign is exact whenever it returns from the filter.
pub(super) fn cross_delta_order(start: Point, end: Point, current: Point, next: Point) -> Ordering {
    let dx = end.x - start.x;
    let dy = end.y - start.y;
    let px = next.x - current.x;
    let py = next.y - current.y;
    let left = dx * py;
    let right = dy * px;
    let estimate = left - right;
    let magnitude = left.abs() + right.abs();
    let ordinary = [dx, dy, px, py, left, right]
        .into_iter()
        .all(|value| value.is_normal() || value == 0.0)
        && magnitude.is_normal();
    if ordinary && estimate.abs() > magnitude * PROJECTION_ERRBOUND {
        return estimate.total_cmp(&0.0);
    }
    exact_cross_delta(start, end, current, next).ordering()
}

/// Sound O(1) certificate for preserving a legacy unnormalised rectangle.
/// Each retained support must be locally minimal for its *actual emitted
/// edge*.  Convexity turns that local statement into global support truth.
#[expect(
    clippy::indexing_slicing,
    reason = "candidate support ordinals and their cyclic neighbors are within this convex hull"
)]
#[expect(
    clippy::arithmetic_side_effects,
    reason = "support ordinals are below count and Vec lengths bound the cyclic neighbor arithmetic"
)]
pub(super) fn legacy_rectangle_is_certified(
    hull: &[Point],
    corners: &[Point; 4],
    candidate: Candidate,
) -> bool {
    if !corners
        .iter()
        .all(|point| point.x.is_finite() && point.y.is_finite())
    {
        return false;
    }
    let count = hull.len();
    let support = [
        candidate.supports.outward_min,
        candidate.supports.along_max,
        candidate.supports.outward_max,
        candidate.supports.along_min,
    ];
    for ((start, end), index) in corners
        .iter()
        .copied()
        .zip(corners.iter().copied().cycle().skip(1))
        .zip(support)
    {
        let previous = hull[(index + count - 1) % count];
        let current = hull[index];
        let next = hull[(index + 1) % count];
        if cross_delta_order(start, end, current, previous).is_lt()
            || cross_delta_order(start, end, current, next).is_lt()
        {
            return false;
        }
        // The support must lie on the inside of its actual edge.  The edge
        // orientation is CCW, so a negative cross is the outside halfplane.
        let mut inside = ExactSum::zero();
        inside.add_product(end.x, current.y);
        inside.subtract_product(start.x, current.y);
        inside.subtract_product(end.y, current.x);
        inside.add_product(start.y, current.x);
        inside.subtract_product(end.x, start.y);
        inside.add_product(start.x, end.y);
        if inside.ordering().is_lt() {
            return false;
        }
    }
    corners
        .iter()
        .copied()
        .zip(corners.iter().copied().cycle().skip(1))
        .zip(corners.iter().copied().cycle().skip(2))
        .all(|((first, second), third)| {
            orientation(first, second, third) == Orientation::CounterClockwise
        })
}

pub(super) fn support_margin(halfplane: SupportHalfplane, candidate: Point) -> ExactSum {
    exact_dot_delta(halfplane.gradient, halfplane.support, candidate)
}

pub(super) fn minimum_rectangle_not_representable() -> crate::error::Error {
    GeometryErrorKind::message(
        "minimum_rotated_rectangle result is not representable with finite coordinates",
    )
}

/// Emit a finite point in the outward intersection of two support halfplanes.
/// The preferred inverse is accepted only after its two exact residuals prove
/// it lies in the wedge.  The repair shifts one coordinate in a shared outward
/// direction; every direction and rounding choice is determined by the exact
/// residual, not by a later hull predicate.
pub(super) fn emit_corner(
    _basis: Basis,
    first: SupportHalfplane,
    second: SupportHalfplane,
    preferred: &PreferredCorner,
) -> Result<Point> {
    if (first.gradient.0 == 0.0 || first.gradient.1 == 0.0)
        && let Some(axis_corner) = axis_corner(first, second)
    {
        return Ok(axis_corner);
    }
    if preferred.point.x.is_finite()
        && preferred.point.y.is_finite()
        && !projection_delta_order(first.gradient, first.support, preferred.point).is_lt()
        && !projection_delta_order(second.gradient, second.support, preferred.point).is_lt()
    {
        // The adaptive sign filter above is an exact dyadic predicate when it
        // returns.  In ordinary cases it proves both support inequalities
        // without materialising the 68-limb margins used by the repair lane.
        return Ok(preferred.point);
    }

    // The anisotropic inverse can land just beyond the finite range even
    // though a nearby coordinate on one of its two exact support lines is
    // representable.  Search the two preferred coordinate boundaries only:
    // this is four support-pair intersections, not a hull predicate.  Each
    // candidate is accepted by the same exact two-halfplane certificate used
    // below, so construction—not a scan over source vertices—establishes
    // enclosure and minimality.
    if preferred.point.x.is_finite()
        && preferred.point.y.is_finite()
        && (preferred.anisotropic_frame
            || preferred.point.x.to_bits() & !(1_u64 << 63) == f64::MAX.to_bits()
            || preferred.point.y.to_bits() & !(1_u64 << 63) == f64::MAX.to_bits())
    {
        for (axis, fixed) in [(Axis::X, preferred.point.x), (Axis::Y, preferred.point.y)] {
            if let Some(candidate) = finite_point_on_edge(first, second, axis, fixed, preferred) {
                return Ok(candidate);
            }
        }
    }

    if preferred.point.x.is_finite() && preferred.point.y.is_finite() {
        let first_margin = support_margin(first, preferred.point);
        let second_margin = support_margin(second, preferred.point);
        if let Some(candidate) =
            directed_axis_repair(first, second, preferred, &first_margin, &second_margin)
        {
            return Ok(candidate);
        }
    }
    finite_support_wedge_point(first, second, preferred)
        .ok_or_else(minimum_rectangle_not_representable)
}

pub(super) fn axis_corner(first: SupportHalfplane, second: SupportHalfplane) -> Option<Point> {
    let (along, outward) = if first.gradient.0 == 0.0 || first.gradient.1 == 0.0 {
        (first, second)
    } else {
        (second, first)
    };
    if along.gradient.1 == 0.0 && outward.gradient.0 == 0.0 {
        Some(Point::new_raw_xy(along.support.x, outward.support.y))
    } else if along.gradient.0 == 0.0 && outward.gradient.1 == 0.0 {
        Some(Point::new_raw_xy(outward.support.x, along.support.y))
    } else {
        None
    }
}

pub(super) fn directed_axis_repair(
    first: SupportHalfplane,
    second: SupportHalfplane,
    preferred: &PreferredCorner,
    first_margin: &ExactSum,
    second_margin: &ExactSum,
) -> Option<Point> {
    // A perpendicular pair always has a Cartesian component with identical
    // nonzero signs.  Moving in that signed direction improves both margins.
    let mut best = None;
    for (axis, first_coefficient, second_coefficient) in [
        (Axis::X, first.gradient.0, second.gradient.0),
        (Axis::Y, first.gradient.1, second.gradient.1),
    ] {
        if first_coefficient == 0.0
            || second_coefficient == 0.0
            || first_coefficient.is_sign_negative() != second_coefficient.is_sign_negative()
        {
            continue;
        }
        let sign = if first_coefficient.is_sign_negative() {
            -1.0
        } else {
            1.0
        };
        let first_deficit = quotient_for_outward_deficit(first_margin, first_coefficient * sign)?;
        let second_deficit =
            quotient_for_outward_deficit(second_margin, second_coefficient * sign)?;
        let distance = first_deficit.max(second_deficit).max(0.0);
        let old = match axis {
            Axis::X => preferred.point.x,
            Axis::Y => preferred.point.y,
        };
        let (sum, residual) = two_sum(old, sign * distance);
        // `two_sum` gives `old + signed_distance == sum + residual` exactly.
        // Round only when that residual leaves `sum` short in the selected
        // outward direction; an unconditional ULP would over-pad exact sums.
        let shifted = if sign.is_sign_positive() && residual > 0.0 {
            sum.next_up()
        } else if sign.is_sign_negative() && residual < 0.0 {
            sum.next_down()
        } else {
            sum
        };
        if !shifted.is_finite() {
            continue;
        }
        let candidate = match axis {
            Axis::X => Point::new_raw_xy(shifted, preferred.point.y),
            Axis::Y => Point::new_raw_xy(preferred.point.x, shifted),
        };
        if !support_margin(first, candidate).ordering().is_lt()
            && !support_margin(second, candidate).ordering().is_lt()
            && best.is_none_or(|current| displacement_order(candidate, current, preferred).is_lt())
        {
            best = Some(candidate);
        }
    }
    best
}

pub(super) fn quotient_for_outward_deficit(
    margin: &ExactSum,
    positive_coefficient: f64,
) -> Option<f64> {
    debug_assert!(positive_coefficient > 0.0);
    if !margin.ordering().is_lt() {
        return Some(0.0);
    }
    let mut deficit = margin.clone();
    deficit.negate();
    let interval = exact_quotient_interval(&deficit, positive_coefficient);
    interval.hi.is_finite().then_some(interval.hi)
}

/// Adjacent binary64 values bracketing `numerator / denominator` exactly.
/// This fixed 63-step search is used only on an overflowing preferred corner.
pub(super) fn exact_quotient_interval(numerator: &ExactSum, denominator: f64) -> Interval {
    debug_assert!(denominator.is_finite() && denominator != 0.0);
    let (numerator_sign, magnitude) = numerator.signed_magnitude();
    if numerator_sign == Ordering::Equal {
        return Interval { lo: 0.0, hi: 0.0 };
    }
    let negative = (numerator_sign == Ordering::Less) ^ denominator.is_sign_negative();
    let positive = positive_quotient_interval(&magnitude, denominator.abs());
    if negative {
        Interval {
            lo: -positive.hi,
            hi: -positive.lo,
        }
    } else {
        positive
    }
}

pub(super) fn positive_quotient_interval(magnitude: &[u64; LIMBS], denominator: f64) -> Interval {
    match magnitude_product_order(magnitude, denominator, f64::MAX) {
        Ordering::Greater => {
            return Interval {
                lo: f64::MAX,
                hi: f64::INFINITY,
            };
        },
        Ordering::Equal => {
            return Interval {
                lo: f64::MAX,
                hi: f64::MAX,
            };
        },
        Ordering::Less => {},
    }
    // The exact numerator has a two-float bracket.  One directed hardware
    // division of its upper endpoint therefore gives a valid ceiling, and
    // in the ordinary case it is at most three representable values above the
    // exact quotient.  Walk that tiny neighbourhood with exact products;
    // the 63-step format-wide search remains the finite, constant fallback
    // for subnormal and unusually wide cases.
    let numerator = positive_interval(magnitude);
    let mut upper = (numerator.hi / denominator).next_up();
    if upper.is_finite() && !magnitude_product_order(magnitude, denominator, upper).is_gt() {
        for _ in 0..3 {
            let lower = upper.next_down();
            match magnitude_product_order(magnitude, denominator, lower) {
                Ordering::Less => upper = lower,
                Ordering::Equal => {
                    return Interval {
                        lo: lower,
                        hi: lower,
                    };
                },
                Ordering::Greater => {
                    return Interval {
                        lo: lower,
                        hi: upper,
                    };
                },
            }
        }
    }
    positive_quotient_interval_binary(magnitude, denominator)
}

#[expect(
    clippy::arithmetic_side_effects,
    reason = "low and high are ordered nonnegative f64 total-order encodings and the midpoint stays between them"
)]
pub(super) fn positive_quotient_interval_binary(
    magnitude: &[u64; LIMBS],
    denominator: f64,
) -> Interval {
    const MAX_BITS: u64 = f64::MAX.to_bits();
    let mut low = 0_u64;
    let mut high = MAX_BITS;
    while high - low > 1 {
        let middle = low + (high - low) / 2;
        let candidate = f64::from_bits(middle);
        if magnitude_product_order(magnitude, denominator, candidate) == Ordering::Less {
            high = middle;
        } else {
            low = middle;
        }
    }
    let floor = f64::from_bits(low);
    if magnitude_product_order(magnitude, denominator, floor) == Ordering::Equal {
        Interval {
            lo: floor,
            hi: floor,
        }
    } else {
        Interval {
            lo: floor,
            hi: f64::from_bits(high),
        }
    }
}

pub(super) fn magnitude_product_order(
    magnitude: &[u64; LIMBS],
    denominator: f64,
    candidate: f64,
) -> Ordering {
    let mut product = ExactSum::zero();
    product.add_product(denominator, candidate);
    product.compare_positive_magnitude(magnitude)
}

/// Finite feasibility is decided on the four square boundaries.  We enumerate
/// all boundary candidates and choose the exact minimum squared world-coordinate
/// Euclidean displacement from the preferred corner; equal exact distances break by
/// total-order x/y bits.  This is deterministic, constant work, and unlike a
/// fixed boundary order is an actual minimum-displacement rule.
pub(super) fn finite_support_wedge_point(
    first: SupportHalfplane,
    second: SupportHalfplane,
    preferred: &PreferredCorner,
) -> Option<Point> {
    let mut best: Option<Point> = None;
    for (axis, fixed) in [
        (Axis::X, f64::MAX),
        (Axis::X, -f64::MAX),
        (Axis::Y, f64::MAX),
        (Axis::Y, -f64::MAX),
    ] {
        if let Some(candidate) = finite_point_on_edge(first, second, axis, fixed, preferred)
            && best.is_none_or(|current| displacement_order(candidate, current, preferred).is_lt())
        {
            best = Some(candidate);
        }
    }
    best
}

pub(super) fn finite_point_on_edge(
    first: SupportHalfplane,
    second: SupportHalfplane,
    fixed_axis: Axis,
    fixed: f64,
    preferred: &PreferredCorner,
) -> Option<Point> {
    let mut lower = -f64::MAX;
    let mut upper = f64::MAX;
    for halfplane in [first, second] {
        let (fixed_coefficient, free_coefficient, fixed_support, free_support) = match fixed_axis {
            Axis::X => (
                halfplane.gradient.0,
                halfplane.gradient.1,
                halfplane.support.x,
                halfplane.support.y,
            ),
            Axis::Y => (
                halfplane.gradient.1,
                halfplane.gradient.0,
                halfplane.support.y,
                halfplane.support.x,
            ),
        };
        if free_coefficient == 0.0 {
            let satisfies = match fixed_coefficient.total_cmp(&0.0) {
                Ordering::Greater => fixed >= fixed_support,
                Ordering::Less => fixed <= fixed_support,
                Ordering::Equal => true,
            };
            if !satisfies {
                return None;
            }
            continue;
        }
        // Construct the threshold as an anchored exact dyadic sum.  In
        // particular never evaluate `(fixed - support)` in f64.
        let mut numerator = ExactSum::zero();
        numerator.add_product(free_coefficient, free_support);
        numerator.subtract_product(fixed_coefficient, fixed);
        numerator.add_product(fixed_coefficient, fixed_support);
        let threshold = exact_quotient_interval(&numerator, free_coefficient);
        if free_coefficient > 0.0 {
            lower = lower.max(threshold.hi);
        } else {
            upper = upper.min(threshold.lo);
        }
        if !lower.is_finite() || !upper.is_finite() || lower > upper {
            return None;
        }
    }
    let free = nearest_feasible_coordinate(
        preferred,
        match fixed_axis {
            Axis::X => Axis::Y,
            Axis::Y => Axis::X,
        },
        lower,
        upper,
    );
    let candidate = match fixed_axis {
        Axis::X => Point::new_raw_xy(fixed, free),
        Axis::Y => Point::new_raw_xy(free, fixed),
    };
    (!support_margin(first, candidate).ordering().is_lt()
        && !support_margin(second, candidate).ordering().is_lt())
    .then_some(candidate)
}

#[expect(
    clippy::arithmetic_side_effects,
    reason = "low and high are ordered total-order encodings, so binary-search subtraction and midpoint stay in range"
)]
pub(super) fn nearest_feasible_coordinate(
    preferred: &PreferredCorner,
    axis: Axis,
    lower: f64,
    upper: f64,
) -> f64 {
    debug_assert!(lower.is_finite() && upper.is_finite() && lower <= upper);
    if !preferred.coordinate_order(axis, lower).is_lt() {
        return lower;
    }
    if !preferred.coordinate_order(axis, upper).is_gt() {
        return upper;
    }
    let mut low = total_order_bits(lower);
    let mut high = total_order_bits(upper);
    while high - low > 1 {
        let middle = low + (high - low) / 2;
        let candidate = f64_from_total_order_bits(middle);
        if preferred.coordinate_order(axis, candidate).is_lt() {
            low = middle;
        } else {
            high = middle;
        }
    }
    let floor = f64_from_total_order_bits(low);
    let ceiling = f64_from_total_order_bits(high);
    let floor_delta = preferred
        .coordinate_difference(axis, floor)
        .signed_magnitude()
        .1;
    let ceiling_delta = preferred
        .coordinate_difference(axis, ceiling)
        .signed_magnitude()
        .1;
    match cmp_magnitude(&floor_delta, &ceiling_delta) {
        Ordering::Greater => ceiling,
        Ordering::Equal if total_order_bits(ceiling) < total_order_bits(floor) => ceiling,
        Ordering::Equal | Ordering::Less => floor,
    }
}

pub(super) fn displacement_order(
    left: Point,
    right: Point,
    preferred: &PreferredCorner,
) -> Ordering {
    let left_distance = exact_squared_distance(left, preferred);
    let right_distance = exact_squared_distance(right, preferred);
    match cmp_wide(&left_distance, &right_distance) {
        Ordering::Equal => total_order_bits(left.x)
            .cmp(&total_order_bits(right.x))
            .then_with(|| total_order_bits(left.y).cmp(&total_order_bits(right.y))),
        Ordering::Less => Ordering::Less,
        Ordering::Greater => Ordering::Greater,
    }
}

pub(super) fn exact_squared_distance(
    point: Point,
    preferred: &PreferredCorner,
) -> [u64; TRIPLE_LIMBS] {
    let x = preferred
        .coordinate_difference(Axis::X, point.x)
        .signed_magnitude()
        .1;
    let y = preferred
        .coordinate_difference(Axis::Y, point.y)
        .signed_magnitude()
        .1;
    let denominator_square = |axis: Axis| {
        let denominator = *axis.select(&preferred.denominator);
        let mut square = ExactSum::zero();
        square.add_product(denominator, denominator);
        square.signed_magnitude().1
    };
    let x_denominator_square = denominator_square(Axis::X);
    let y_denominator_square = denominator_square(Axis::Y);
    // `(Rx / Dx)^2 + (Ry / Dy)^2` has common positive denominator
    // `Dx^2 * Dy^2`, so compare its exact numerator instead.
    let x_square: [u64; DOUBLE_LIMBS] = wide_product(&x, &x);
    let y_square: [u64; DOUBLE_LIMBS] = wide_product(&y, &y);
    add_triple_magnitude(
        &wide_product(&x_square, &y_denominator_square),
        &wide_product(&y_square, &x_denominator_square),
    )
}

pub(super) const fn total_order_bits(value: f64) -> u64 {
    let bits = value.to_bits();
    if bits >> 63 == 0 {
        bits | (1_u64 << 63)
    } else {
        !bits
    }
}

pub(super) const fn f64_from_total_order_bits(bits: u64) -> f64 {
    if bits >> 63 == 0 {
        f64::from_bits(!bits)
    } else {
        f64::from_bits(bits & !(1_u64 << 63))
    }
}
