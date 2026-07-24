#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use super::*;
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

/// Whether any vertex of `probe` sits within `limit` (squared, inclusive) of
/// `target`'s linework or isolated points — the dwithin sweep. `simd` gates
/// the vector facet kernel (squared-space-safe callers only).
pub(crate) fn any_parts_within(
    probe: &DistanceParts,
    target: &DistanceParts,
    limit: f64,
    simd: bool,
) -> bool {
    let segment_hit = target.bvh().map_or_else(
        || {
            target
                .linework
                .any_points_within(probe.probe_coords(), limit, simd)
        },
        |bvh| bvh.any_points_within(&target.linework, probe.probe_coords(), limit, simd),
    );
    segment_hit
        || (!target.point_only.is_empty()
            && probe.probe_coords().any(|(x, y)| {
                target.point_only.iter().any(|&other| {
                    point_distance_squared(Point::new_unchecked_xy(x, y), other) <= limit
                })
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

/// SQUARED planar distance for a pure point set vs a few-segment lineal/areal
/// shape: the minimum squared point-to-edge distance, walked in place over the
/// edges. Squared (not rooted) so `distance` and `dwithin` share ONE kernel —
/// `distance` roots it, `dwithin` compares it to the squared limit — keeping
/// the two within one ulp at the boundary (the GEOS/PostGIS trade). Returns
/// `None` (defer to the indexed sweep) unless exactly one operand is puntal
/// (`Point`/`MultiPoint`), the other is a single lineal/areal type
/// (`LineString`/`MultiLineString`/`Polygon`/`MultiPolygon`), and that other
/// is small enough that the prepared-linework allocation dominates. The caller
/// has already returned 0 for intersecting/contained/boundary cases, so the
/// remainder is disjoint and the boundary distance is exact — including a point
/// inside a hole, whose nearest edge is reached by walking every ring.
pub(crate) fn puntal_brute_distance_squared(a: &Shape, b: &Shape) -> Option<f64> {
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
    // Accumulate in squared space — one `sqrt` at the end, not per edge (the
    // squared kernel is exactly what the indexed sweep uses).
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
