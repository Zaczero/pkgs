use crate::geometry::Segment;
use crate::geometry::distance::hausdorff::{
    EquidistantRootSink, HausdorffFeature, hausdorff_feature_distance_squared_at,
};

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
    let a = left.a - right.a;
    let b = left.b - right.b;
    let c = left.c - right.c;
    if !a.is_finite() || !b.is_finite() || !c.is_finite() {
        return;
    }
    // The rounded coefficient polynomial is degenerate only at exact zero;
    // a proximity band would change its roots.
    if a == 0.0 {
        if b == 0.0 {
            return;
        }
        let t = -c / b;
        if t.is_finite() && t >= ta && t <= tb {
            roots.push_root(t);
        }
        return;
    }
    // A positive common scale preserves the roots and discriminant sign while
    // keeping the fourth-power products in the binary64 range.
    let coefficient_scale = a.abs().max(b.abs()).max(c.abs());
    let (a, b, c) = (
        a / coefficient_scale,
        b / coefficient_scale,
        c / coefficient_scale,
    );
    let disc = b * b - 4.0 * a * c;
    if !disc.is_finite() || disc < 0.0 {
        return;
    }
    let sqrt_disc = disc.sqrt();
    let roots_to_check = if disc == 0.0 {
        [Some(-0.5 * (b / a)), None]
    } else {
        // Both roots use the non-cancelling q form.  Splitting the halves
        // avoids overflowing the sum when b and sqrt_disc are both near
        // the largest finite binary64 value.
        let q = -0.5 * b - 0.5 * sqrt_disc.copysign(b);
        if q == 0.0 {
            [None, None]
        } else {
            [Some(q / a), Some(c / q)]
        }
    };
    for root in roots_to_check.into_iter().flatten() {
        if root.is_finite() && root >= ta && root <= tb {
            roots.push_root(root);
        }
    }
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
            b: 1.0e16,
            c: 1.0,
        };
        let mut roots = Vec::new();
        push_equidistant_roots_quadratic(left, zero_quadratic(), -1.0e17, 0.0, &mut roots);

        assert_eq!(roots.len(), 2);
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
