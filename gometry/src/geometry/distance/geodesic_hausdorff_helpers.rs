use crate::geometry::{GeodesicMetric, GeodesicSegment, Point};
pub(crate) const GEODESIC_HAUSDORFF_GOLDEN_TOLERANCE: f64 = 0.01;
pub(crate) const GEODESIC_HAUSDORFF_GOLDEN_PHI: f64 = std::f64::consts::GOLDEN_RATIO;

pub(crate) fn geodesic_min_distance_to_target(
    point: Point,
    target_edges: &[GeodesicSegment],
    target_points: &[Point],
    metric: &impl GeodesicMetric,
    prune_at: f64,
) -> f64 {
    let mut cmin = f64::INFINITY;
    for &target in target_points {
        let bound = metric.point_distance_lower_bound(point, target);
        if bound >= cmin {
            continue;
        }
        let distance = metric.segment_length(point, target);
        if distance < cmin {
            cmin = distance;
        }
        if cmin <= prune_at {
            return cmin;
        }
    }
    for &segment in target_edges {
        let distance = metric.point_to_segment(point, segment, cmin);
        if distance < cmin {
            cmin = distance;
        }
        if cmin <= prune_at {
            return cmin;
        }
    }
    cmin
}

#[expect(
    clippy::large_types_passed_by_value,
    reason = "the owned Copy aggregate is a hot kernel snapshot; a borrow adds pointer and lifetime plumbing without changing its data flow"
)]
pub(crate) fn geodesic_max_min_on_source_segment(
    segment: GeodesicSegment,
    target_edges: &[GeodesicSegment],
    target_points: &[Point],
    metric: &impl GeodesicMetric,
    cmax: f64,
) -> f64 {
    if !segment.length.is_finite() || segment.length == 0.0 {
        return geodesic_min_distance_to_target(
            segment.start,
            target_edges,
            target_points,
            metric,
            cmax,
        );
    }
    let min_at = |along: f64| -> f64 {
        let fraction = (along / segment.length).clamp(0.0, 1.0);
        let point = metric.interpolate(segment.start, segment.end, fraction);
        geodesic_min_distance_to_target(point, target_edges, target_points, metric, cmax)
    };
    let mut best = min_at(0.0).max(min_at(segment.length));
    let (mut lo, mut hi) = (0.0, segment.length);
    let mut c = hi - (hi - lo) / GEODESIC_HAUSDORFF_GOLDEN_PHI;
    let mut d = lo + (hi - lo) / GEODESIC_HAUSDORFF_GOLDEN_PHI;
    let mut fc = min_at(c);
    let mut fd = min_at(d);
    loop {
        if hi - lo <= GEODESIC_HAUSDORFF_GOLDEN_TOLERANCE {
            break;
        }
        if fc > fd {
            lo = d;
            d = c;
            fd = fc;
            c = hi - (hi - lo) / GEODESIC_HAUSDORFF_GOLDEN_PHI;
            fc = min_at(c);
        } else {
            hi = c;
            c = d;
            fc = fd;
            d = lo + (hi - lo) / GEODESIC_HAUSDORFF_GOLDEN_PHI;
            fd = min_at(d);
        }
        best = best.max(fc).max(fd);
    }
    best
}
