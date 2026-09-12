use crate::geometry::distance::hausdorff::{
    EquidistantRootSink, HausdorffFeature, hausdorff_feature_distance_squared_at,
};
use crate::geometry::exact_arith::{floor_binary_exponent, two_product, two_sum};
use crate::geometry::{Segment, scale_by_power_of_two};

/// Squared-distance quadratic `Q(t) = a·t² + b·t + c` on a fixed regime
/// interval.
#[derive(Clone, Copy)]
pub(crate) struct HausdorffQuadratic {
    a: f64,
    b: f64,
    c: f64,
}

impl HausdorffQuadratic {
    pub(crate) const fn coefficients(self) -> (f64, f64, f64) {
        (self.a, self.b, self.c)
    }

    const fn is_finite(self) -> bool {
        self.a.is_finite() && self.b.is_finite() && self.c.is_finite()
    }

    fn from_point_target(ax: f64, ay: f64, dx: f64, dy: f64, px: f64, py: f64) -> Self {
        let vx0 = ax - px;
        let vy0 = ay - py;
        Self {
            a: dx * dx + dy * dy,
            b: 2.0 * (vx0 * dx + vy0 * dy),
            c: vx0 * vx0 + vy0 * vy0,
        }
    }

    fn from_segment(ax: f64, ay: f64, dx: f64, dy: f64, segment: Segment, t_ref: f64) -> Self {
        let ex = segment.end.x - segment.start.x;
        let ey = segment.end.y - segment.start.y;
        let length2 = ex * ex + ey * ey;
        if length2 == 0.0 {
            return Self::from_point_target(ax, ay, dx, dy, segment.start.x, segment.start.y);
        }
        let qx0 = ax - segment.start.x;
        let qy0 = ay - segment.start.y;
        let dot_de = dx * ex + dy * ey;
        let s_ref = (qx0 * ex + qy0 * ey + t_ref * dot_de) / length2;
        if s_ref <= 0.0 {
            Self::from_point_target(ax, ay, dx, dy, segment.start.x, segment.start.y)
        } else if s_ref >= 1.0 {
            Self::from_point_target(ax, ay, dx, dy, segment.end.x, segment.end.y)
        } else {
            let s0 = (qx0 * ex + qy0 * ey) / length2;
            let sr = dot_de / length2;
            let wx0 = qx0 - s0 * ex;
            let wy0 = qy0 - s0 * ey;
            let wx1 = dx - sr * ex;
            let wy1 = dy - sr * ey;
            Self {
                a: wx1 * wx1 + wy1 * wy1,
                b: 2.0 * (wx0 * wx1 + wy0 * wy1),
                c: wx0 * wx0 + wy0 * wy0,
            }
        }
    }

    pub(crate) fn from_feature(
        ax: f64,
        ay: f64,
        dx: f64,
        dy: f64,
        feature: HausdorffFeature,
        t_ref: f64,
    ) -> Self {
        match feature {
            HausdorffFeature::Point(point) => {
                Self::from_point_target(ax, ay, dx, dy, point.x, point.y)
            },
            HausdorffFeature::Segment(segment) => {
                Self::from_segment(ax, ay, dx, dy, segment, t_ref)
            },
        }
    }
}

pub(crate) fn push_equidistant_roots_quadratic(
    left: HausdorffQuadratic,
    right: HausdorffQuadratic,
    ta: f64,
    tb: f64,
    roots: &mut impl EquidistantRootSink,
) {
    for root in quadratic_roots(left.a - right.a, left.b - right.b, left.c - right.c)
        .into_iter()
        .flatten()
    {
        if root.is_finite() && root >= ta && root <= tb {
            roots.push_root(root);
        }
    }
}

#[expect(
    clippy::many_single_char_names,
    reason = "quadratic coefficients and exponent-frame notation"
)]
pub(crate) fn quadratic_roots(a: f64, b: f64, c: f64) -> [Option<f64>; 2] {
    if !a.is_finite() || !b.is_finite() || !c.is_finite() {
        return [None, None];
    }
    // The rounded coefficient polynomial is degenerate only at exact zero;
    // a proximity band would change its roots.
    if a == 0.0 {
        return if b == 0.0 {
            [None, None]
        } else {
            [Some(-c / b), None]
        };
    }

    if c == 0.0 {
        if b == 0.0 {
            return [Some(0.0), None];
        }
        let other = -b / a;
        return if other == 0.0 {
            [Some(0.0), None]
        } else {
            [Some(0.0), Some(other)]
        };
    }

    let (ma, ea) = coefficient_parts(a);
    let (mc, ec) = coefficient_parts(c);
    let (mb, eb) = if b == 0.0 {
        (0.0, None)
    } else {
        let (mantissa, exponent) = coefficient_parts(b);
        (mantissa, Some(exponent))
    };
    let mut k = ceil_divide_by_two(ea + ec) + 1;
    if let Some(eb) = eb {
        k = k.max(eb);
    }
    let bs = eb.map_or(0.0, |eb| scale_by_power_of_two(mb, eb - k));
    let delta = ea + ec + 2 - 2 * k;
    debug_assert!(delta <= 0);
    let first_delta = delta.div_euclid(2);
    let second_delta = delta - first_delta;
    let z = scale_by_power_of_two(ma, first_delta);
    let w = scale_by_power_of_two(mc, second_delta);
    let discriminant = compensated_product_difference(bs, bs, z, w);
    if !discriminant.is_finite() || discriminant < 0.0 {
        return [None, None];
    }
    if discriminant == 0.0 {
        let qscaled = -0.5 * bs;
        if qscaled == 0.0 {
            return [Some(0.0), None];
        }
        return [Some(scale_by_power_of_two(qscaled / ma, k - ea)), None];
    }
    let sqrt_discriminant = discriminant.sqrt();
    // q/a and c/q avoid the cancellation in (-b + sqrt(discriminant)) / 2a.
    let qscaled = -0.5 * (bs + sqrt_discriminant.copysign(b));
    if qscaled == 0.0 {
        [None, None]
    } else {
        [
            Some(scale_by_power_of_two(qscaled / ma, k - ea)),
            Some(scale_by_power_of_two(mc / qscaled, ec - k)),
        ]
    }
}

fn coefficient_parts(value: f64) -> (f64, i32) {
    let exponent = floor_binary_exponent(value.abs());
    (scale_by_power_of_two(value, -exponent), exponent)
}

fn ceil_divide_by_two(value: i32) -> i32 {
    value.div_euclid(2) + i32::from(value.rem_euclid(2) != 0)
}

fn compensated_product_difference(x: f64, y: f64, z: f64, w: f64) -> f64 {
    // This is the no-FMA error-free-transform form of the compensated
    // x*y - z*w expression; scalar mul_add is a libm call on x86-64-v2.
    let (xy, xy_error) = two_product(x, y);
    let (zw, zw_error) = two_product(z, w);
    let (difference, difference_error) = two_sum(xy, -zw);
    difference + (difference_error + (xy_error - zw_error))
}

pub(crate) fn push_equidistant_roots_on_interval(
    ax: f64,
    ay: f64,
    dx: f64,
    dy: f64,
    ta: f64,
    tb: f64,
    left: HausdorffFeature,
    right: HausdorffFeature,
    roots: &mut impl EquidistantRootSink,
) -> crate::error::Result<()> {
    let t_ref = f64::midpoint(ta, tb);
    let q_left = HausdorffQuadratic::from_feature(ax, ay, dx, dy, left, t_ref);
    let q_right = HausdorffQuadratic::from_feature(ax, ay, dx, dy, right, t_ref);
    if !q_left.is_finite() || !q_right.is_finite() {
        return push_equidistant_root_bisect(ax, ay, dx, dy, ta, tb, left, right, roots);
    }
    let before = roots.roots().len();
    push_equidistant_roots_quadratic(q_left, q_right, ta, tb, roots);
    if roots.roots().len() == before {
        push_equidistant_root_bisect(ax, ay, dx, dy, ta, tb, left, right, roots)?;
    }
    Ok(())
}

pub(crate) fn push_equidistant_root_bisect(
    ax: f64,
    ay: f64,
    dx: f64,
    dy: f64,
    ta: f64,
    tb: f64,
    left: HausdorffFeature,
    right: HausdorffFeature,
    roots: &mut impl EquidistantRootSink,
) -> crate::error::Result<()> {
    let mut fa = hausdorff_feature_distance_squared_at(ax, ay, dx, dy, ta, left)?
        - hausdorff_feature_distance_squared_at(ax, ay, dx, dy, ta, right)?;
    let mut fb = hausdorff_feature_distance_squared_at(ax, ay, dx, dy, tb, left)?
        - hausdorff_feature_distance_squared_at(ax, ay, dx, dy, tb, right)?;
    if !fa.is_finite() || !fb.is_finite() {
        return Ok(());
    }
    if fa == 0.0 {
        roots.push_root(ta);
        return Ok(());
    }
    if fb == 0.0 {
        roots.push_root(tb);
        return Ok(());
    }
    if fa.signum() == fb.signum() {
        return Ok(());
    }
    let mut lo = ta;
    let mut hi = tb;
    while can_subdivide(lo, hi) {
        let mid = f64::midpoint(lo, hi);
        let fm = hausdorff_feature_distance_squared_at(ax, ay, dx, dy, mid, left)?
            - hausdorff_feature_distance_squared_at(ax, ay, dx, dy, mid, right)?;
        if !fm.is_finite() {
            return Ok(());
        }
        if fm == 0.0 {
            roots.push_root(mid);
            return Ok(());
        }
        if fa.signum() == fm.signum() {
            lo = mid;
            fa = fm;
        } else {
            hi = mid;
            fb = fm;
        }
    }
    // No representable parameter remains between the bracket endpoints.
    // Keep the endpoint with the smaller evaluated distance difference.
    roots.push_root(if fa.abs() <= fb.abs() { lo } else { hi });
    Ok(())
}

fn can_subdivide(left: f64, right: f64) -> bool {
    let middle = f64::midpoint(left, right);
    middle > left && middle < right
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::geometry::XY;

    fn zero_quadratic() -> HausdorffQuadratic {
        HausdorffQuadratic {
            a: 0.0,
            b: 0.0,
            c: 0.0,
        }
    }

    fn quadratic_residual((a, b, c): (f64, f64, f64), root: f64) -> f64 {
        a * root * root + b * root + c
    }

    #[test]
    fn stable_formula_retains_both_ill_conditioned_roots() {
        let left = HausdorffQuadratic {
            a: 1.0,
            b: -0.5,
            c: 1.0e-20,
        };
        let mut roots = Vec::new();
        push_equidistant_roots_quadratic(left, zero_quadratic(), 0.0, 1.0, &mut roots);

        assert_eq!(roots.len(), 2);
        assert!(
            roots
                .iter()
                .any(|root| root.to_bits() == 2.0e-20_f64.to_bits()),
            "small root missing: {roots:?}"
        );
        assert!(
            roots.iter().any(|root| root.to_bits() == 0.5_f64.to_bits()),
            "large root missing: {roots:?}"
        );
        for root in roots {
            let residual = quadratic_residual(left.coefficients(), root).abs();
            let scale = (left.a * root * root).abs() + (left.b * root).abs() + left.c.abs();
            // The independent polynomial evaluation has three products and
            // two additions; allow their accumulated machine-relative error.
            assert!(
                residual <= 32.0 * f64::EPSILON * scale,
                "root={root:?}, residual={residual:?}, scale={scale:?}"
            );
        }
    }

    #[test]
    fn small_scale_quadratic_is_not_treated_as_constant() {
        let left = HausdorffQuadratic {
            a: 5.0e-16,
            b: 0.0,
            c: -1.0e-16,
        };
        let mut roots = Vec::new();
        push_equidistant_roots_quadratic(left, zero_quadratic(), 0.0, 1.0, &mut roots);

        assert_eq!(roots.len(), 1);
        let root = roots[0];
        assert!(root > 0.0 && root < 1.0);
        let scale = (left.a * root * root).abs() + left.c.abs();
        // Use the same machine-relative allowance as the independent
        // polynomial evaluation, rather than an absolute coordinate scale.
        assert!(quadratic_residual(left.coefficients(), root).abs() <= 32.0 * f64::EPSILON * scale);
    }

    #[test]
    fn zero_constant_recovers_zero_and_the_linear_root() {
        let roots = quadratic_roots(1.0, -1.0e-200, 0.0);
        let root_bits = roots
            .into_iter()
            .flatten()
            .map(|root| root.to_bits())
            .collect::<Vec<_>>();

        assert_eq!(root_bits.len(), 2);
        assert!(root_bits.contains(&0.0_f64.to_bits()));
        assert!(root_bits.contains(&1.0e-200_f64.to_bits()));
    }

    #[test]
    fn exponent_carriage_preserves_a_negative_subnormal_discriminant() {
        let b = -f64::from_bits(2.0_f64.powi(-536).to_bits() - 1);
        let roots = quadratic_roots(1.0, b, f64::from_bits(1));

        assert_eq!(roots, [None, None]);
    }

    #[test]
    fn exponent_carriage_keeps_two_positive_subnormal_discriminant_roots() {
        let b = -f64::from_bits(2.0_f64.powi(-536).to_bits() + 1);
        let roots = quadratic_roots(1.0, b, f64::from_bits(1));
        let root_bits = roots
            .into_iter()
            .flatten()
            .map(|root| root.to_bits())
            .collect::<Vec<_>>();

        assert_eq!(root_bits.len(), 2);
        assert_ne!(root_bits[0], root_bits[1]);
    }

    #[test]
    fn negative_discriminant_is_not_snapped_to_a_root() {
        let left = HausdorffQuadratic {
            a: 1.0e-10,
            b: 2.0e-10,
            c: 1.0e-10 * (1.0 + f64::EPSILON),
        };
        let mut roots = Vec::new();
        push_equidistant_roots_quadratic(left, zero_quadratic(), -2.0, 0.0, &mut roots);

        assert!(roots.is_empty());
    }

    #[test]
    fn near_double_positive_discriminant_keeps_both_roots() {
        // These stored coefficients have exact D = 2^-106, with roots 0.5
        // and 0.4999999999999999.
        let left = HausdorffQuadratic {
            a: 1.0,
            b: -0.999_999_999_999_999_9,
            c: 0.249_999_999_999_999_94,
        };
        let mut roots = Vec::new();
        push_equidistant_roots_quadratic(left, zero_quadratic(), 0.0, 1.0, &mut roots);
        let root_bits = roots
            .into_iter()
            .map(|root| root.to_bits())
            .collect::<Vec<_>>();

        assert_eq!(root_bits.len(), 2);
        assert!(root_bits.contains(&0.5_f64.to_bits()));
        assert!(root_bits.contains(&0.499_999_999_999_999_9_f64.to_bits()));
    }

    #[test]
    fn near_double_negative_discriminant_does_not_invent_a_root() {
        // The exact discriminant is
        // -4503599627370495/40564819207303340847894502572032.
        let left = HausdorffQuadratic {
            a: 0.999_999_999_999_999_9,
            b: -1.0,
            c: 0.250_000_000_000_000_06,
        };
        let mut roots = Vec::new();
        push_equidistant_roots_quadratic(left, zero_quadratic(), 0.0, 1.0, &mut roots);

        assert!(roots.is_empty());
    }

    #[test]
    fn interval_membership_does_not_clamp_an_outside_root() {
        let outside = 1.0_f64.next_up();
        let left = HausdorffQuadratic {
            a: 0.0,
            b: 1.0,
            c: -outside,
        };
        let mut roots = Vec::new();
        push_equidistant_roots_quadratic(left, zero_quadratic(), 0.0, 1.0, &mut roots);

        assert!(roots.is_empty());
    }

    #[test]
    fn bisection_refines_until_adjacent_parameters() {
        let left = HausdorffFeature::Point(XY::new(0.0, 0.0));
        let right = HausdorffFeature::Point(XY::new(1.0, 0.3));
        let mut roots = Vec::new();
        push_equidistant_root_bisect(0.0, 0.0, 1.0, 0.0, 0.0, 1.0, left, right, &mut roots)
            .expect("point distances are finite");

        assert_eq!(roots.len(), 1);
        let root = roots[0];
        // For stored y = 0.3 = 5404319552844595 / 2^54, exact real
        // arithmetic gives x = (1 + y^2) / 2; its nearest binary64 value
        // for this fixed case is 0.545. This does not assert a universal
        // claim about which adjacent endpoint a bisection should return.
        assert_eq!(root.to_bits(), 0.545_f64.to_bits());
        let difference = |t| {
            hausdorff_feature_distance_squared_at(0.0, 0.0, 1.0, 0.0, t, left).unwrap()
                - hausdorff_feature_distance_squared_at(0.0, 0.0, 1.0, 0.0, t, right).unwrap()
        };
        let left_distance =
            hausdorff_feature_distance_squared_at(0.0, 0.0, 1.0, 0.0, root, left).unwrap();
        let right_distance =
            hausdorff_feature_distance_squared_at(0.0, 0.0, 1.0, 0.0, root, right).unwrap();
        let distance_scale = left_distance.max(right_distance);
        // Two squared-distance evaluations and their subtraction contribute
        // only machine-relative error to this equidistance oracle.
        assert!(
            (left_distance - right_distance).abs() <= 4.0 * f64::EPSILON * distance_scale,
            "root={root:?}, left={left_distance:?}, right={right_distance:?}"
        );
        assert!(
            difference(root.next_down()).is_sign_negative()
                && difference(root.next_up()).is_sign_positive(),
            "root={root:?}, below={}, above={}",
            difference(root.next_down()),
            difference(root.next_up())
        );
    }
}
