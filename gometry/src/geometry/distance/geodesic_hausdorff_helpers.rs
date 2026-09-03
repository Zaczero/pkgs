use std::cmp::Ordering;
use std::collections::BinaryHeap;

use crate::error::Result;
use crate::geometry::{GeodesicMetric, GeodesicSegment, Point};
pub(crate) const GEODESIC_HAUSDORFF_TOLERANCE: f64 = 0.01;

pub(crate) fn geodesic_min_distance_to_target(
    point: Point,
    target_edges: &[GeodesicSegment],
    target_points: &[Point],
    metric: &impl GeodesicMetric,
    prune_at: f64,
) -> Result<f64> {
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
            return Ok(cmin);
        }
    }
    for &segment in target_edges {
        let distance = metric.point_to_segment(point, segment, cmin)?;
        if distance < cmin {
            cmin = distance;
        }
        if cmin <= prune_at {
            return Ok(cmin);
        }
    }
    Ok(cmin)
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
) -> Result<f64> {
    if target_edges.is_empty() && target_points.is_empty() {
        return Ok(f64::INFINITY);
    }
    if !segment.length.is_finite() || segment.length == 0.0 {
        return geodesic_min_distance_to_target(
            segment.start,
            target_edges,
            target_points,
            metric,
            cmax,
        );
    }
    // The prune bound is the standing maximum, not the caller's alone: a
    // sample whose nearest-target distance is already below a maximum found
    // on this segment cannot raise it either, and the nearest-target search
    // is what dominates the sweep.
    let min_at = |along: f64, prune_at: f64| -> Result<f64> {
        let fraction = (along / segment.length).clamp(0.0, 1.0);
        let point = metric.interpolate(segment.start, segment.end, fraction);
        geodesic_min_distance_to_target(point, target_edges, target_points, metric, prune_at)
    };
    let left_value = min_at(0.0, cmax)?;
    let right_value = min_at(segment.length, cmax.max(left_value))?;
    let mut best = left_value.max(right_value);
    let mut live = BinaryHeap::from([HausdorffInterval {
        upper: hausdorff_upper_bound(left_value, right_value, segment.length),
        left: 0.0,
        right: segment.length,
        left_value,
        right_value,
    }]);
    while let Some(interval) = live.pop() {
        if interval.upper - best <= GEODESIC_HAUSDORFF_TOLERANCE {
            break;
        }
        let middle = f64::midpoint(interval.left, interval.right);
        if !(middle > interval.left && middle < interval.right) {
            return Err(crate::crs::CrsError::message(
                "geodesic Hausdorff proof interval cannot be subdivided",
            ));
        }
        let middle_value = min_at(middle, cmax.max(best))?;
        best = best.max(middle_value);
        let left = HausdorffInterval {
            upper: hausdorff_upper_bound(interval.left_value, middle_value, middle - interval.left),
            left: interval.left,
            right: middle,
            left_value: interval.left_value,
            right_value: middle_value,
        };
        let right = HausdorffInterval {
            upper: hausdorff_upper_bound(
                middle_value,
                interval.right_value,
                interval.right - middle,
            ),
            left: middle,
            right: interval.right,
            left_value: middle_value,
            right_value: interval.right_value,
        };
        if left.upper > best + GEODESIC_HAUSDORFF_TOLERANCE {
            live.push(left);
        }
        if right.upper > best + GEODESIC_HAUSDORFF_TOLERANCE {
            live.push(right);
        }
    }
    Ok(best)
}

#[derive(Clone, Copy, Debug)]
struct HausdorffInterval {
    upper: f64,
    left: f64,
    right: f64,
    left_value: f64,
    right_value: f64,
}

impl PartialEq for HausdorffInterval {
    fn eq(&self, other: &Self) -> bool {
        self.upper.to_bits() == other.upper.to_bits()
            && self.left.to_bits() == other.left.to_bits()
            && self.right.to_bits() == other.right.to_bits()
    }
}

impl Eq for HausdorffInterval {}

impl PartialOrd for HausdorffInterval {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HausdorffInterval {
    fn cmp(&self, other: &Self) -> Ordering {
        self.upper
            .total_cmp(&other.upper)
            .then_with(|| other.left.total_cmp(&self.left))
            .then_with(|| other.right.total_cmp(&self.right))
    }
}

fn hausdorff_upper_bound(left: f64, right: f64, width: f64) -> f64 {
    left.max(right).max((left + right + width) / 2.0)
}
