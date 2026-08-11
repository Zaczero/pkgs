use crate::geometry::{
    DistanceParts, Point, Segment, Shape, point_distance, point_distance_squared,
    point_segment_distance, point_segment_distance_squared, points_dwithin, same_point,
    segment_envelopes_disjoint, segments_intersect,
};
pub(crate) fn parts_covers_point(shape: &Shape, parts: &DistanceParts, point: Point) -> bool {
    parts
        .point_only
        .iter()
        .any(|&other| same_point(other, point))
        || parts.bvh().map_or_else(
            || parts.linework.covers_point(point),
            |bvh| bvh.covers_point(&parts.linework, point),
        )
        || (shape.has_area_parts() && shape.area_covers_point(point))
}

/// Whether any segment pair between the two parts touches or crosses —
/// dual-tree descent, one-sided probes, or the brute pair scan, depending
/// on which sides cleared the tree crossover.
pub(crate) fn parts_segments_cross(left: &DistanceParts, right: &DistanceParts) -> bool {
    if left.linework.segment_count() == 0 || right.linework.segment_count() == 0 {
        return false;
    }
    match (left.bvh(), right.bvh()) {
        (Some(left_bvh), Some(right_bvh)) => {
            left_bvh.any_segments_intersect(&left.linework, right_bvh, &right.linework)
        },
        (Some(left_bvh), None) => {
            // One stack for every probe segment — was a fresh allocation per
            // `any_segment_intersecting` call.
            let mut stack = Vec::new();
            right.linework.any_segment(|probe| {
                left_bvh.any_segment_intersecting_with_stack(&left.linework, probe, &mut stack)
            })
        },
        (None, Some(right_bvh)) => {
            let mut stack = Vec::new();
            left.linework.any_segment(|probe| {
                right_bvh.any_segment_intersecting_with_stack(&right.linework, probe, &mut stack)
            })
        },
        (None, None) => left.linework.any_segment(|left_segment| {
            right.linework.any_segment(|right_segment| {
                !segment_envelopes_disjoint(left_segment, right_segment)
                    && segments_intersect(left_segment, right_segment)
            })
        }),
    }
}

/// Minimum distance from every vertex of `probe` (linework columns +
/// isolated points, streamed — no `Vec<Point>`) to `target`'s linework and
/// isolated points. `SQUARED` selects squared space + the SIMD facet kernel
/// (callers gate on squared-space-safe operands); `false` runs overflow-safe
/// `hypot` kernels. Seeds from (and never exceeds) `best`.
pub(crate) fn min_parts_to_parts<const SQUARED: bool>(
    probe: &DistanceParts,
    target: &DistanceParts,
    mut best: f64,
) -> f64 {
    best = target.bvh().map_or_else(
        || {
            target
                .linework
                .min_points_distance::<SQUARED>(probe.probe_coords(), best)
        },
        |bvh| bvh.min_points_distance::<SQUARED>(&target.linework, probe.probe_coords(), best),
    );
    if !target.point_only.is_empty() {
        for (x, y) in probe.probe_coords() {
            for &other in &target.point_only {
                let candidate = if SQUARED {
                    point_distance_squared(Point::new_unchecked_xy(x, y), other)
                } else {
                    point_distance(Point::new_unchecked_xy(x, y), other)
                };
                best = best.min(candidate);
            }
        }
    }
    best
}

/// Whether any vertex of `probe` sits within `distance` (inclusive) of
/// `target`'s linework or isolated points — the dwithin sweep.
/// `limit_sq` is `distance²` when honest; callers pass both so underflow of
/// either side can fall back to distance space.
pub(crate) fn any_parts_within(
    probe: &DistanceParts,
    target: &DistanceParts,
    distance: f64,
    limit_sq: f64,
    simd: bool,
) -> bool {
    // Squared path only when limit² is honest (positive finite, non-false-zero).
    let use_sq = limit_sq.is_finite() && limit_sq != 0.0 && distance > 0.0;
    let segment_hit = if use_sq {
        target.bvh().map_or_else(
            || {
                target
                    .linework
                    .any_points_within(probe.probe_coords(), limit_sq, simd)
            },
            |bvh| bvh.any_points_within(&target.linework, probe.probe_coords(), limit_sq, simd),
        )
    } else {
        // Tiny / zero radius: distance-space against linework.
        probe.probe_coords().any(|(x, y)| {
            let point = Point::new_unchecked_xy(x, y);
            target.linework.facets.iter().any(|&facet| {
                (0..facet.segment_count as usize).any(|index| {
                    let segment = target.linework.segment(facet, index);
                    point_segment_distance(point, segment) <= distance
                })
            })
        })
    };
    segment_hit
        || (!target.point_only.is_empty()
            && probe.probe_coords().any(|(x, y)| {
                target
                    .point_only
                    .iter()
                    .any(|&other| points_dwithin(Point::new_unchecked_xy(x, y), other, distance))
            }))
}

/// Pre-parts zero oracle: a representative vertex of one operand inside
/// the other's AREA parts proves intersection for one raycast, before any
/// `PreparedLinework` staging — the common area-overlap case (a polygon
/// covering the other operand) answers without building parts at all.
/// Callers gate on overlapping bounds. Returns the witnessing vertex.
pub(crate) fn quick_area_overlap(left: &Shape, right: &Shape) -> Option<Point> {
    let mut witness = None;
    if right.has_area_parts() {
        left.any_component_representative(&mut |point| {
            let covered = right.area_covers_point(point);
            if covered {
                witness = Some(point);
            }
            covered
        });
    }
    if witness.is_none() && left.has_area_parts() {
        right.any_component_representative(&mut |point| {
            let covered = left.area_covers_point(point);
            if covered {
                witness = Some(point);
            }
            covered
        });
    }
    witness
}

/// Segment-count crossover for [`puntal_brute_distance`]. Measured against the
/// prepared-sweep path (point vs regular n-gon, point outside): the in-place
/// walk wins up to ~24 edges — past that the sweep's prepared-linework +
/// axis-sorted vertex pruning beats an unpruned scan, even below the BVH index
/// threshold (64), so the walk defers. Boxes/tiles (4 edges) — the common
/// distance-query shape — land squarely in the win zone.
pub(crate) const PUNTAL_BRUTE_MAX_SEGMENTS: usize = 24;

/// Whether the puntal-vs-few-segment fast path applies; shared by squared
/// and distance-space variants.
fn puntal_brute_operands<'a>(a: &'a Shape, b: &'a Shape) -> Option<(&'a Shape, &'a Shape)> {
    let puntal = |shape: &Shape| matches!(shape, Shape::Point(_) | Shape::MultiPoint(_));
    let lineal_or_areal = |shape: &Shape| {
        matches!(
            shape,
            Shape::LineString(_)
                | Shape::MultiLineString(_)
                | Shape::Polygon(_)
                | Shape::MultiPolygon(_)
        )
    };
    let (points, other) = match (puntal(a), puntal(b)) {
        (true, false) => (a, b),
        (false, true) => (b, a),
        _ => return None,
    };
    if !lineal_or_areal(other) {
        return None;
    }
    let segments = other.segment_count();
    if segments == 0 || segments > PUNTAL_BRUTE_MAX_SEGMENTS {
        return None;
    }
    Some((points, other))
}

/// SQUARED min point-to-edge for puntal vs few-segment lineal/areal.
/// A returned `0.0` is **not** a contact certificate — callers finish via
/// [`finish_planar_squared_min`] with [`puntal_brute_distance`] as recompute.
/// Returns `None` when the shape pair is out of this fast path.
pub(crate) fn puntal_brute_distance_squared(a: &Shape, b: &Shape) -> Option<f64> {
    let (points, other) = puntal_brute_operands(a, b)?;
    let mut best_squared = f64::INFINITY;
    let mut probe = |point: Point| {
        other.for_each_vertex_pair(|start, end| {
            best_squared = best_squared.min(point_segment_distance_squared(point, Segment {
                start: start.into(),
                end: end.into(),
            }));
        });
    };
    match points {
        Shape::Point(point) => probe(*point),
        Shape::MultiPoint(coords) => coords.points().for_each(probe),
        _ => unreachable!("puntal guard restricts to Point/MultiPoint"),
    }
    Some(best_squared)
}

/// Distance-space min point-to-edge for the same puntal-vs-few-segment class —
/// the recompute half of [`finish_planar_squared_min`] for this lane.
pub(crate) fn puntal_brute_distance(a: &Shape, b: &Shape) -> Option<f64> {
    let (points, other) = puntal_brute_operands(a, b)?;
    let mut best = f64::INFINITY;
    let mut probe = |point: Point| {
        other.for_each_vertex_pair(|start, end| {
            best = best.min(point_segment_distance(point, Segment {
                start: start.into(),
                end: end.into(),
            }));
        });
    };
    match points {
        Shape::Point(point) => probe(*point),
        Shape::MultiPoint(coords) => coords.points().for_each(probe),
        _ => unreachable!("puntal guard restricts to Point/MultiPoint"),
    }
    Some(best)
}
