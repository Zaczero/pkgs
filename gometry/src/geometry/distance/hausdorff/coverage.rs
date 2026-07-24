use super::*;

/// Merge intervals on [0, 1] and test full coverage (tolerance-safe).
const UNIT_INTERVAL_EPS: f64 = 1e-15;

pub(crate) fn unit_interval_covered(mut intervals: Vec<(f64, f64)>) -> bool {
    if intervals.is_empty() {
        return false;
    }
    for window in &mut intervals {
        window.0 = window.0.clamp(0.0, 1.0);
        window.1 = window.1.clamp(0.0, 1.0);
        if window.0 > window.1 {
            std::mem::swap(&mut window.0, &mut window.1);
        }
    }
    intervals.sort_by(|left, right| left.0.total_cmp(&right.0));
    let mut end = intervals[0].1;
    if intervals[0].0 > UNIT_INTERVAL_EPS {
        return false;
    }
    for &(start, stop) in &intervals[1..] {
        if start > end + UNIT_INTERVAL_EPS {
            return false;
        }
        end = end.max(stop);
    }
    end >= 1.0 - UNIT_INTERVAL_EPS
}

/// Parameter intervals on [0, 1] where dist²(p(t), point) ≤ cmax.
pub(crate) fn point_radius_coverage_intervals(
    ax: f64,
    ay: f64,
    dx: f64,
    dy: f64,
    px: f64,
    py: f64,
    cmax: f64,
) -> Vec<(f64, f64)> {
    let vx0 = ax - px;
    let vy0 = ay - py;
    quadratic_le_cmax_intervals(
        dx * dx + dy * dy,
        2.0 * (vx0 * dx + vy0 * dy),
        vx0 * vx0 + vy0 * vy0,
        cmax,
    )
}

#[expect(clippy::many_single_char_names, reason = "quadratic coefficient names")]
pub(crate) fn quadratic_le_cmax_intervals(a: f64, b: f64, c: f64, cmax: f64) -> Vec<(f64, f64)> {
    let d = c - cmax;
    if a.abs() < HAUSDORFF_QUADRATIC_EPS {
        if b.abs() < HAUSDORFF_QUADRATIC_EPS {
            return if d <= 0.0 {
                vec![(0.0, 1.0)]
            } else {
                Vec::new()
            };
        }
        if b > 0.0 {
            if d <= 0.0 && b + d <= 0.0 {
                return vec![(0.0, 1.0)];
            }
            let t = (-d / b).clamp(0.0, 1.0);
            return if d <= 0.0 { vec![(0.0, t)] } else { Vec::new() };
        }
        if d <= 0.0 && -b + d <= 0.0 {
            return vec![(0.0, 1.0)];
        }
        let t = (-d / b).clamp(0.0, 1.0);
        return if -b + d <= 0.0 {
            vec![(t, 1.0)]
        } else {
            Vec::new()
        };
    }
    let disc = b * b - 4.0 * a * d;
    if disc < -1e-20 {
        return if a < 0.0 && d <= 0.0 {
            vec![(0.0, 1.0)]
        } else {
            Vec::new()
        };
    }
    let sqrt_disc = if disc <= 0.0 { 0.0 } else { disc.sqrt() };
    let inv_2a = 0.5 / a;
    let mut t0 = (-b - sqrt_disc) * inv_2a;
    let mut t1 = (-b + sqrt_disc) * inv_2a;
    if t0 > t1 {
        std::mem::swap(&mut t0, &mut t1);
    }
    if a > 0.0 {
        if t1 <= 0.0 || t0 >= 1.0 {
            return Vec::new();
        }
        vec![(t0.max(0.0), t1.min(1.0))]
    } else if d <= 0.0 {
        let mut out = Vec::new();
        if t0 > 0.0 {
            out.push((0.0, t0.min(1.0)));
        }
        if t1 < 1.0 {
            out.push((t1.max(0.0), 1.0));
        }
        if out.is_empty() {
            out.push((0.0, 1.0));
        }
        out
    } else {
        Vec::new()
    }
}

/// Radius-coverage certificate: max_t dist(p(t), B) ≤ sqrt(cmax).
pub(crate) fn segment_radius_coverage_certified<T: HausdorffTargetLike>(
    ax: f64,
    ay: f64,
    dx: f64,
    dy: f64,
    target: &T,
    cmax_sq: f64,
) -> bool {
    if cmax_sq <= 0.0 || !cmax_sq.is_finite() {
        return true;
    }
    let radius = sqrt_distance_squared(cmax_sq);
    let prune_margin = radius;
    let mut intervals = Vec::new();
    for index in 0..target.segments_len() {
        let segment = target.segment_at(index);
        if hausdorff_feature_bbox_disjoint_from_expanded_source(
            ax,
            ay,
            dx,
            dy,
            HausdorffFeature::Segment(segment),
            prune_margin,
        ) {
            continue;
        }
        let feature = HausdorffFeature::Segment(segment);
        intervals.extend(segment_feature_radius_intervals(
            ax, ay, dx, dy, feature, cmax_sq,
        ));
    }
    for index in 0..target.points_len() {
        let point = target.point_xy_at(index);
        if hausdorff_feature_bbox_disjoint_from_expanded_source(
            ax,
            ay,
            dx,
            dy,
            HausdorffFeature::Point(point),
            prune_margin,
        ) {
            continue;
        }
        intervals.extend(point_radius_coverage_intervals(
            ax, ay, dx, dy, point.x, point.y, cmax_sq,
        ));
    }
    unit_interval_covered(intervals)
}

fn segment_feature_radius_intervals(
    ax: f64,
    ay: f64,
    dx: f64,
    dy: f64,
    feature: HausdorffFeature,
    cmax: f64,
) -> Vec<(f64, f64)> {
    let mut params = SmallHausdorffParams::new();
    params.push_param(0.0);
    params.push_param(1.0);
    match feature {
        HausdorffFeature::Segment(segment) => {
            push_segment_projection_breakpoints(ax, ay, dx, dy, segment, &mut params);
        },
        HausdorffFeature::Point(point) => {
            push_point_on_line_breakpoint(ax, ay, dx, dy, point, &mut params);
        },
    }
    let slice = params.as_mut_slice();
    let param_count = compact_hausdorff_params(slice);
    let params = &slice[..param_count];
    let mut out = Vec::new();
    for &[ta, tb] in params.array_windows::<2>() {
        if tb <= ta {
            continue;
        }
        let t_ref = f64::midpoint(ta, tb);
        let q = HausdorffQuadratic::from_feature(ax, ay, dx, dy, feature, t_ref);
        let (qa, qb, qc) = q.coefficients();
        for (left, right) in quadratic_le_cmax_intervals(qa, qb, qc, cmax) {
            let start = left.max(ta);
            let end = right.min(tb);
            if end > start {
                out.push((start, end));
            }
        }
    }
    out
}
