use crate::geometry::{
    DistanceParts, MultiPoint, Point, Segment, Shape, ShapeCountQuery, cartesian_point_covers,
    point_distance, point_segment_distance, point_segment_distance_squared, points_dwithin,
    same_point, segment_envelopes_disjoint, segments_intersect,
};
pub(crate) fn parts_covers_point(shape: &Shape, parts: &DistanceParts, point: Point) -> bool {
    if shape.has_area_parts() {
        return cartesian_point_covers(shape, point);
    }
    parts
        .point_only
        .iter()
        .any(|&other| same_point(other, point))
        || parts.bvh().map_or_else(
            || parts.linework.covers_point(point),
            |bvh| bvh.covers_point(&parts.linework, point),
        )
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
) -> crate::error::Result<f64> {
    best = match target.bvh() {
        None => target
            .linework
            .min_points_distance::<SQUARED>(probe.probe_coords(), best)?,
        Some(bvh) => {
            bvh.min_points_distance::<SQUARED>(&target.linework, probe.probe_coords(), best)?
        },
    };
    if !target.point_only.is_empty() {
        probe.linework.for_each_chain_xy_columns(|xs, ys| {
            best = min_point_pairs::<SQUARED>(
                &xs.iter().copied().zip(ys.iter().copied()),
                &target.point_only,
                best,
            );
        });
        best = min_point_pairs::<SQUARED>(
            &probe.point_only.iter().map(|point| (point.x, point.y)),
            &target.point_only,
            best,
        );
    }
    Ok(best)
}

fn min_point_pairs<const SQUARED: bool>(
    probes: &(impl ExactSizeIterator<Item = (f64, f64)> + Clone),
    targets: &[Point],
    mut best: f64,
) -> f64 {
    if probes.len() == 0 {
        return best;
    }
    for other in targets {
        for (x, y) in (*probes).clone() {
            let candidate = if SQUARED {
                // Squared-space callers bound the coordinates against overflow
                // and rescue a subnormal/false-zero minimum in distance space.
                let dx = x - other.x;
                let dy = y - other.y;
                dx * dx + dy * dy
            } else {
                point_distance(Point::new_raw_xy(x, y), *other)
            };
            best = best.min(candidate);
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
) -> crate::error::Result<bool> {
    // Squared path only when limit² is a normal, faithful f64.
    let use_sq = limit_sq.is_normal() && distance > 0.0;
    let segment_hit = if use_sq {
        match target.bvh() {
            None => target
                .linework
                .any_points_within(probe.probe_coords(), limit_sq, simd)?,
            Some(bvh) => {
                bvh.any_points_within(&target.linework, probe.probe_coords(), limit_sq, simd)?
            },
        }
    } else {
        // Tiny / zero radius: distance-space against linework.
        {
            let mut hit = false;
            for (x, y) in probe.probe_coords() {
                let point = Point::new_raw_xy(x, y);
                for &facet in &target.linework.facets {
                    for index in 0..facet.segment_count as usize {
                        let segment = target.linework.segment(facet, index);
                        if point_segment_distance(point, segment)? <= distance {
                            hit = true;
                            break;
                        }
                    }
                    if hit {
                        break;
                    }
                }
                if hit {
                    break;
                }
            }
            hit
        }
    };
    Ok(segment_hit
        || (!target.point_only.is_empty()
            && probe.probe_coords().any(|(x, y)| {
                target
                    .point_only
                    .iter()
                    .any(|&other| points_dwithin(Point::new_raw_xy(x, y), other, distance))
            })))
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

/// Whether the puntal-vs-few-segment fast path applies: the puntal operand
/// and the lineal/areal operand whose segments it probes.
fn puntal_brute_operands<'a>(a: &'a Shape, b: &'a Shape) -> Option<(PuntalProbe<'a>, &'a Shape)> {
    let puntal = |shape: &'a Shape| match shape {
        Shape::Point(point) => Some(PuntalProbe::One(*point)),
        Shape::MultiPoint(points) => Some(PuntalProbe::Many(points)),
        Shape::LineString(_)
        | Shape::MultiLineString(_)
        | Shape::Polygon(_)
        | Shape::MultiPolygon(_)
        | Shape::GeometryCollection(_)
        | Shape::Empty(..) => None,
    };
    let (points, other) = match (puntal(a), puntal(b)) {
        (Some(points), None) => (points, b),
        (None, Some(points)) => (points, a),
        _ => return None,
    };
    if !matches!(
        other,
        Shape::LineString(_)
            | Shape::MultiLineString(_)
            | Shape::Polygon(_)
            | Shape::MultiPolygon(_)
    ) {
        return None;
    }
    let segments = ShapeCountQuery::default().segments(other).ok()?;
    if segments == 0 || segments > PUNTAL_BRUTE_MAX_SEGMENTS {
        return None;
    }
    Some((points, other))
}

enum PuntalProbe<'a> {
    One(Point),
    Many(&'a MultiPoint),
}

/// Minimum of `metric` over every probe point and every segment of the other
/// operand, or `None` when the shape pair is out of this fast path.
fn puntal_brute_minimum(
    a: &Shape,
    b: &Shape,
    metric: fn(Point, Segment) -> crate::error::Result<f64>,
) -> crate::error::Result<Option<f64>> {
    let Some((points, other)) = puntal_brute_operands(a, b) else {
        return Ok(None);
    };
    let mut best = f64::INFINITY;
    let mut probe = |point: Point| -> crate::error::Result<()> {
        other.try_for_each_vertex_pair(|start, end| {
            best = best.min(metric(point, Segment {
                start: start.into(),
                end: end.into(),
            })?);
            Ok(())
        })
    };
    match points {
        PuntalProbe::One(point) => probe(point)?,
        PuntalProbe::Many(points) => {
            for point in points.points() {
                probe(point)?;
            }
        },
    }
    Ok(Some(best))
}

/// SQUARED min point-to-edge for puntal vs few-segment lineal/areal.
/// A returned `0.0` is **not** a contact certificate — callers finish via
/// [`finish_planar_squared_min`] with [`puntal_brute_distance`] as recompute.
pub(crate) fn puntal_brute_distance_squared(
    a: &Shape,
    b: &Shape,
) -> crate::error::Result<Option<f64>> {
    puntal_brute_minimum(a, b, point_segment_distance_squared)
}

/// Distance-space min point-to-edge for the same puntal-vs-few-segment class —
/// the recompute half of [`finish_planar_squared_min`] for this lane.
pub(crate) fn puntal_brute_distance(a: &Shape, b: &Shape) -> crate::error::Result<Option<f64>> {
    puntal_brute_minimum(a, b, point_segment_distance)
}
