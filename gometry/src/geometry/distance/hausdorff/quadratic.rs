#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use super::*;

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

pub(crate) const HAUSDORFF_QUADRATIC_EPS: f64 = 1e-15;

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
    if a.abs() < HAUSDORFF_QUADRATIC_EPS {
        if b.abs() < HAUSDORFF_QUADRATIC_EPS {
            return;
        }
        let t = -c / b;
        if t.is_finite() && t >= ta - HAUSDORFF_QUADRATIC_EPS && t <= tb + HAUSDORFF_QUADRATIC_EPS {
            roots.push_root(t.clamp(ta, tb));
        }
        return;
    }
    let disc = b * b - 4.0 * a * c;
    if disc < -1e-20 {
        return;
    }
    let sqrt_disc = if disc <= 0.0 { 0.0 } else { disc.sqrt() };
    let inv_2a = 0.5 / a;
    for root in [(-b - sqrt_disc) * inv_2a, (-b + sqrt_disc) * inv_2a] {
        if root.is_finite()
            && root >= ta - HAUSDORFF_QUADRATIC_EPS
            && root <= tb + HAUSDORFF_QUADRATIC_EPS
        {
            roots.push_root(root.clamp(ta, tb));
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
) {
    let t_ref = f64::midpoint(ta, tb);
    let q_left = HausdorffQuadratic::from_feature(ax, ay, dx, dy, left, t_ref);
    let q_right = HausdorffQuadratic::from_feature(ax, ay, dx, dy, right, t_ref);
    if !q_left.is_finite() || !q_right.is_finite() {
        push_equidistant_root_bisect(ax, ay, dx, dy, ta, tb, left, right, roots);
        return;
    }
    let before = roots.roots().len();
    push_equidistant_roots_quadratic(q_left, q_right, ta, tb, roots);
    if roots.roots().len() == before {
        push_equidistant_root_bisect(ax, ay, dx, dy, ta, tb, left, right, roots);
    }
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
) {
    let mut fa = hausdorff_feature_distance_squared_at(ax, ay, dx, dy, ta, left)
        - hausdorff_feature_distance_squared_at(ax, ay, dx, dy, ta, right);
    let fb = hausdorff_feature_distance_squared_at(ax, ay, dx, dy, tb, left)
        - hausdorff_feature_distance_squared_at(ax, ay, dx, dy, tb, right);
    if !fa.is_finite() || !fb.is_finite() {
        return;
    }
    if fa == 0.0 {
        roots.push_root(ta);
        return;
    }
    if fb == 0.0 {
        roots.push_root(tb);
        return;
    }
    if fa.signum() == fb.signum() {
        return;
    }
    let mut lo = ta;
    let mut hi = tb;
    for _ in 0..64 {
        let mid = f64::midpoint(lo, hi);
        let fm = hausdorff_feature_distance_squared_at(ax, ay, dx, dy, mid, left)
            - hausdorff_feature_distance_squared_at(ax, ay, dx, dy, mid, right);
        if fm == 0.0 || hi - lo <= 1e-15 {
            roots.push_root(mid);
            return;
        }
        if fa.signum() == fm.signum() {
            lo = mid;
            fa = fm;
        } else {
            hi = mid;
        }
    }
    roots.push_root(f64::midpoint(lo, hi));
}
