#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use super::*;
pub(crate) fn geodesic_pair_spans_antimeridian(start: Point, end: Point) -> bool {
    super::predicates::spans_antimeridian(Segment {
        start: XY::new(start.x, start.y),
        end: XY::new(end.x, end.y),
    })
}

pub(crate) fn geodesic_cap_streaming(
    shape: &Shape,
    metric: &impl GeodesicMetric,
) -> Option<(Point, f64)> {
    let mut anchor = None;
    shape.for_each_point(|point| {
        if anchor.is_none() {
            anchor = Some(point);
        }
    });
    let anchor = anchor?;
    let mut reach = 0.0_f64;
    shape.for_each_vertex_pair(|start, end| {
        let length = metric.segment_length(start, end);
        let from_anchor = metric.segment_length(anchor, start);
        reach = reach.max(from_anchor + length);
    });
    let mut point_only = Vec::new();
    collect_point_only_into(shape, &mut point_only);
    for point in point_only {
        reach = reach.max(metric.segment_length(anchor, point));
    }
    Some((anchor, reach))
}

pub(crate) fn geodesic_cap_from_parts(
    parts: &GeodesicParts,
    metric: &impl GeodesicMetric,
) -> Option<(Point, f64)> {
    let anchor = *parts.points.first()?;
    let mut reach = 0.0_f64;
    for segment in &parts.segments {
        let from_anchor = metric.segment_length(anchor, segment.start);
        reach = reach.max(from_anchor + segment.length);
    }
    if !parts.point_only.is_empty() {
        for &point in &parts.point_only {
            reach = reach.max(metric.segment_length(anchor, point));
        }
    }
    Some((anchor, reach))
}

impl ShapeData {
    /// Auxiliary-sphere cap reusing a cached [`GeodesicParts`] when present.
    pub(crate) fn geodesic_cap_cached(
        &self,
        frame_cache: &FrameDependentCaches,
        crs: &str,
        semi_major: f64,
        flattening: f64,
        metric: &impl GeodesicMetric,
    ) -> Option<(Point, f64)> {
        let key = GeodesicPartsKey::new(crs, semi_major, flattening);
        if let Some(parts) = self.cached_geodesic_parts(frame_cache, &key) {
            return geodesic_cap_from_parts(&parts, metric);
        }
        self.shape().geodesic_cap(metric)
    }
}

pub(crate) fn geodesic_segments_cross_streaming(
    left: &Shape,
    right: &Shape,
    metric: &impl GeodesicMetric,
) -> bool {
    let mut cross = false;
    left.for_each_vertex_pair(|l_start, l_end| {
        if cross || !geodesic_pair_spans_antimeridian(l_start, l_end) {
            return;
        }
        right.for_each_vertex_pair(|r_start, r_end| {
            if cross {
                return;
            }
            if metric.segments_cross(l_start, l_end, r_start, r_end) {
                cross = true;
            }
        });
    });
    if cross {
        return true;
    }
    right.for_each_vertex_pair(|r_start, r_end| {
        if cross || !geodesic_pair_spans_antimeridian(r_start, r_end) {
            return;
        }
        left.for_each_vertex_pair(|l_start, l_end| {
            if cross {
                return;
            }
            if metric.segments_cross(l_start, l_end, r_start, r_end) {
                cross = true;
            }
        });
    });
    cross
}

pub(crate) fn geodesic_segments_cross_parts(
    left: &GeodesicParts,
    right: &GeodesicParts,
    metric: &impl GeodesicMetric,
) -> bool {
    if left.antimeridian_segments.is_empty() && right.antimeridian_segments.is_empty() {
        return false;
    }

    // Preserve the shape-level predicate exactly: a candidate pair is tested
    // when EITHER segment spans the antimeridian.
    left.antimeridian_segments.iter().any(|segment| {
        right
            .segments
            .iter()
            .any(|other| metric.segments_cross(segment.start, segment.end, other.start, other.end))
    }) || right.antimeridian_segments.iter().any(|segment| {
        left.segments
            .iter()
            .any(|other| metric.segments_cross(other.start, other.end, segment.start, segment.end))
    })
}

pub(crate) fn geodesic_distance_with_parts(
    left: &Shape,
    left_parts: &GeodesicParts,
    right: &Shape,
    right_parts: &GeodesicParts,
    metric: &impl GeodesicMetric,
) -> f64 {
    if left.intersects(right) {
        return 0.0;
    }
    if geodesic_segments_cross_parts(left_parts, right_parts, metric) {
        return 0.0;
    }
    if matches!(left, Shape::Point(_)) {
        return geodesic_point_distance_with_parts(
            right,
            left_parts.points[0],
            right_parts,
            metric,
            &mut Vec::new(),
        );
    }
    if matches!(right, Shape::Point(_)) {
        return geodesic_point_distance_with_parts(
            left,
            right_parts.points[0],
            left_parts,
            metric,
            &mut Vec::new(),
        );
    }
    let mut best = f64::INFINITY;
    best = geodesic_sweep_with_parts(right, &left_parts.points, right_parts, metric, best);
    geodesic_sweep_with_parts(left, &right_parts.points, left_parts, metric, best)
}

pub(crate) fn geodesic_dwithin_with_parts(
    left: &Shape,
    left_parts: &GeodesicParts,
    right: &Shape,
    right_parts: &GeodesicParts,
    metric: &impl GeodesicMetric,
    distance: f64,
) -> bool {
    if left.intersects(right) {
        return true;
    }
    if geodesic_segments_cross_parts(left_parts, right_parts, metric) {
        return true;
    }
    if matches!(left, Shape::Point(_)) {
        return geodesic_point_dwithin_with_parts(
            right,
            left_parts.points[0],
            right_parts,
            metric,
            distance,
            &mut Vec::new(),
        );
    }
    if matches!(right, Shape::Point(_)) {
        return geodesic_point_dwithin_with_parts(
            left,
            right_parts.points[0],
            left_parts,
            metric,
            distance,
            &mut Vec::new(),
        );
    }
    geodesic_dwithin_sweep_with_parts(right, &left_parts.points, right_parts, metric, distance)
        || geodesic_dwithin_sweep_with_parts(
            left,
            &right_parts.points,
            left_parts,
            metric,
            distance,
        )
}

pub(crate) fn geodesic_nearest_points_with_parts(
    left: &Shape,
    left_parts: &GeodesicParts,
    right: &Shape,
    right_parts: &GeodesicParts,
    metric: &impl GeodesicMetric,
) -> Option<(Point, Point)> {
    if left_parts.points.is_empty() || right_parts.points.is_empty() {
        return None;
    }
    if left.intersects(right) {
        return left.nearest_points(right);
    }
    if matches!(left, Shape::Point(_)) {
        return geodesic_witness_sweep_with_parts(
            &left_parts.points,
            right_parts,
            metric,
            None,
            0,
            false,
        )
        .map(|candidate| (candidate.probe, candidate.target));
    }
    if matches!(right, Shape::Point(_)) {
        return geodesic_witness_sweep_with_parts(
            &right_parts.points,
            left_parts,
            metric,
            None,
            0,
            true,
        )
        .map(|candidate| (candidate.target, candidate.probe));
    }
    let reverse_order_offset =
        left_parts.points.len() * (right_parts.segments.len() + right_parts.point_only.len());
    let best =
        geodesic_witness_sweep_with_parts(&left_parts.points, right_parts, metric, None, 0, false);
    let best = geodesic_witness_sweep_with_parts(
        &right_parts.points,
        left_parts,
        metric,
        best,
        reverse_order_offset,
        true,
    );
    best.map(|candidate| {
        if candidate.swapped {
            (candidate.target, candidate.probe)
        } else {
            (candidate.probe, candidate.target)
        }
    })
}

/// One directed geodesic sweep `vertices -> (segments + isolated points)`
/// under auxiliary-sphere cap bounds, tiered:
///
/// 1. Per target, TWO Karney inverses (once): its length and its distance from
///    its GROUP anchor — `σ ≤ s/b` makes `s(anchor, start) + length` a proven
///    upper bound on any target point's aux-sphere reach, so groups of
///    [`CAP_GROUP`] targets carry one conservative cap each (a two-level cap
///    hierarchy, flat arrays).
/// 2. Per vertex, one trigonometric bound against the global cap; rows process
///    in ascending order and the first row past the running best retires every
///    remaining vertex.
/// 3. Inside a surviving row, each GROUP bound can retire [`CAP_GROUP`] targets
///    with one trigonometric evaluation; surviving members gate individually
///    (`s(v, x) ≥ bound(v, start) − length`) before any exact Karney work.
///
/// `target` keeps the historical per-vertex containment short-circuit
/// (evaluated lazily — pruned vertices never pay it).
pub(crate) fn geodesic_capped_sweep(
    target: &Shape,
    vertices: &[Point],
    segments: &[GeodesicSegment],
    point_only: &[Point],
    metric: &impl GeodesicMetric,
    mut best: f64,
    cap_lengths: &mut Vec<f64>,
    cap_groups: &mut Vec<CapGroup>,
    rows: &mut Vec<RowProbe>,
) -> f64 {
    let Some(caps) =
        geodesic_sweep_caps_into(segments, point_only, metric, cap_lengths, cap_groups)
    else {
        return best;
    };
    geodesic_ordered_rows_into(vertices, caps, metric, false, rows);
    for &RowProbe {
        bound: row_bound,
        vertex: vertex_index,
    } in rows.iter()
    {
        if row_bound > best {
            break;
        }
        let vertex = vertices[vertex_index as usize];
        if target.contains_point(vertex) {
            return 0.0;
        }
        // Tier 3: group caps, then member caps, then exact.
        for (
            group_index,
            &CapGroup {
                anchor: group_anchor,
                reach,
            },
        ) in caps.groups.iter().enumerate()
        {
            if metric.point_distance_lower_bound(vertex, group_anchor) - reach > best {
                continue;
            }
            let first = group_index * GEODESIC_CAP_GROUP;
            let chunk = &segments[first..segments.len().min(first + GEODESIC_CAP_GROUP)];
            for (offset, segment) in chunk.iter().enumerate() {
                let bound = metric.point_distance_lower_bound(vertex, segment.start)
                    - caps.lengths[first + offset];
                if bound > best {
                    continue;
                }
                best = best.min(metric.point_to_segment(vertex, *segment, best));
            }
        }
        for &point in point_only {
            if metric.point_distance_lower_bound(vertex, point) > best {
                continue;
            }
            best = best.min(metric.segment_length(vertex, point));
        }
    }
    best
}

pub(crate) fn geodesic_capped_sweep_with_parts(
    target: &Shape,
    vertices: &[Point],
    target_parts: &GeodesicParts,
    metric: &impl GeodesicMetric,
    mut best: f64,
    rows: &mut Vec<RowProbe>,
) -> f64 {
    let Some(caps) = target_parts.caps.as_ref() else {
        return best;
    };
    geodesic_ordered_rows_into(vertices, caps.view(), metric, false, rows);
    for &RowProbe {
        bound: row_bound,
        vertex: vertex_index,
    } in rows.iter()
    {
        if row_bound > best {
            break;
        }
        let vertex = vertices[vertex_index as usize];
        if target.contains_point(vertex) {
            return 0.0;
        }
        for (
            group_index,
            &CapGroup {
                anchor: group_anchor,
                reach,
            },
        ) in caps.groups.iter().enumerate()
        {
            if metric.point_distance_lower_bound(vertex, group_anchor) - reach > best {
                continue;
            }
            let first = group_index * GEODESIC_CAP_GROUP;
            let chunk = &target_parts.segments
                [first..target_parts.segments.len().min(first + GEODESIC_CAP_GROUP)];
            for (offset, &segment) in chunk.iter().enumerate() {
                let bound = metric.point_distance_lower_bound(vertex, segment.start)
                    - caps.lengths[first + offset];
                if bound > best {
                    continue;
                }
                best = best.min(metric.point_to_segment(vertex, segment, best));
            }
        }
        if !target_parts.point_only.is_empty() {
            for &point in &target_parts.point_only {
                if metric.point_distance_lower_bound(vertex, point) > best {
                    continue;
                }
                best = best.min(metric.segment_length(vertex, point));
            }
        }
    }
    best
}

pub(crate) fn geodesic_sweep_with_parts(
    target: &Shape,
    vertices: &[Point],
    target_parts: &GeodesicParts,
    metric: &impl GeodesicMetric,
    mut best: f64,
) -> f64 {
    let mut scratch_guard = GeodesicScratchGuard::take();
    let scratch = &mut scratch_guard.scratch;
    if let Some(bvh) = target_parts.geodesic_bvh(metric) {
        for &vertex in vertices {
            if target.contains_point(vertex) {
                return 0.0;
            }
            best = bvh
                .min_point_distance(vertex, target_parts, metric, &mut scratch.stack, best)
                .unwrap_or_else(|| {
                    geodesic_capped_sweep_with_parts(
                        target,
                        std::slice::from_ref(&vertex),
                        target_parts,
                        metric,
                        best,
                        &mut scratch.rows,
                    )
                });
        }
        best
    } else {
        geodesic_capped_sweep_with_parts(
            target,
            vertices,
            target_parts,
            metric,
            best,
            &mut scratch.rows,
        )
    }
}

pub(crate) fn geodesic_point_distance_with_parts(
    target: &Shape,
    point: Point,
    target_parts: &GeodesicParts,
    metric: &impl GeodesicMetric,
    stack: &mut Vec<u32>,
) -> f64 {
    let mut scratch_guard = GeodesicScratchGuard::take();
    let scratch = &mut scratch_guard.scratch;
    if target.contains_point(point) || target.intersects(&Shape::Point(point)) {
        return 0.0;
    }
    if let Some(bvh) = target_parts.geodesic_bvh(metric) {
        bvh.min_point_distance(point, target_parts, metric, stack, f64::INFINITY)
            .unwrap_or_else(|| {
                geodesic_capped_sweep_with_parts(
                    target,
                    std::slice::from_ref(&point),
                    target_parts,
                    metric,
                    f64::INFINITY,
                    &mut scratch.rows,
                )
            })
    } else {
        geodesic_capped_sweep_with_parts(
            target,
            std::slice::from_ref(&point),
            target_parts,
            metric,
            f64::INFINITY,
            &mut scratch.rows,
        )
    }
}

pub(crate) fn geodesic_capped_dwithin_with_parts(
    target: &Shape,
    vertices: &[Point],
    target_parts: &GeodesicParts,
    metric: &impl GeodesicMetric,
    distance: f64,
    rows: &mut Vec<RowProbe>,
) -> bool {
    let Some(caps) = target_parts.caps.as_ref() else {
        return false;
    };
    geodesic_ordered_rows_into(vertices, caps.view(), metric, false, rows);
    for &RowProbe {
        bound: row_bound,
        vertex: vertex_index,
    } in rows.iter()
    {
        if row_bound > distance {
            break;
        }
        let vertex = vertices[vertex_index as usize];
        if target.contains_point(vertex) {
            return true;
        }
        for (
            group_index,
            &CapGroup {
                anchor: group_anchor,
                reach,
            },
        ) in caps.groups.iter().enumerate()
        {
            if metric.point_distance_lower_bound(vertex, group_anchor) - reach > distance {
                continue;
            }
            let first = group_index * GEODESIC_CAP_GROUP;
            let chunk = &target_parts.segments
                [first..target_parts.segments.len().min(first + GEODESIC_CAP_GROUP)];
            for (offset, &segment) in chunk.iter().enumerate() {
                let bound = metric.point_distance_lower_bound(vertex, segment.start)
                    - caps.lengths[first + offset];
                if bound > distance {
                    continue;
                }
                if metric.point_to_segment(vertex, segment, distance) <= distance {
                    return true;
                }
            }
        }
        if !target_parts.point_only.is_empty() {
            for &point in &target_parts.point_only {
                if metric.point_distance_lower_bound(vertex, point) > distance {
                    continue;
                }
                if metric.segment_length(vertex, point) <= distance {
                    return true;
                }
            }
        }
    }
    false
}

pub(crate) fn geodesic_dwithin_sweep_with_parts(
    target: &Shape,
    vertices: &[Point],
    target_parts: &GeodesicParts,
    metric: &impl GeodesicMetric,
    distance: f64,
) -> bool {
    let mut scratch_guard = GeodesicScratchGuard::take();
    let scratch = &mut scratch_guard.scratch;
    if let Some(bvh) = target_parts.geodesic_bvh(metric) {
        vertices.iter().any(|&vertex| {
            target.contains_point(vertex)
                || bvh
                    .dwithin_point(vertex, target_parts, metric, &mut scratch.stack, distance)
                    .unwrap_or_else(|| {
                        geodesic_capped_dwithin_with_parts(
                            target,
                            std::slice::from_ref(&vertex),
                            target_parts,
                            metric,
                            distance,
                            &mut scratch.rows,
                        )
                    })
        })
    } else {
        geodesic_capped_dwithin_with_parts(
            target,
            vertices,
            target_parts,
            metric,
            distance,
            &mut scratch.rows,
        )
    }
}

pub(crate) fn geodesic_point_dwithin_with_parts(
    target: &Shape,
    point: Point,
    target_parts: &GeodesicParts,
    metric: &impl GeodesicMetric,
    distance: f64,
    stack: &mut Vec<u32>,
) -> bool {
    let mut scratch_guard = GeodesicScratchGuard::take();
    let scratch = &mut scratch_guard.scratch;
    if target.contains_point(point) || target.intersects(&Shape::Point(point)) {
        return true;
    }
    if let Some(bvh) = target_parts.geodesic_bvh(metric) {
        bvh.dwithin_point(point, target_parts, metric, stack, distance)
            .unwrap_or_else(|| {
                geodesic_capped_dwithin_with_parts(
                    target,
                    std::slice::from_ref(&point),
                    target_parts,
                    metric,
                    distance,
                    &mut scratch.rows,
                )
            })
    } else {
        geodesic_capped_dwithin_with_parts(
            target,
            std::slice::from_ref(&point),
            target_parts,
            metric,
            distance,
            &mut scratch.rows,
        )
    }
}
