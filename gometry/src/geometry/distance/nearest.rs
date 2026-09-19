use crate::geometry::distance::parts_covers_point;
use crate::geometry::{
    BVH_MIN_INDEXED_SEGMENTS, DistanceParts, FacetBvh, NearestCandidate, Point, PointSetIndex,
    Segment, Shape, point_distance_squared, point_pair_distance_key, same_point,
    segment_contact_point,
};
pub(crate) fn nearest_probe_to_parts(
    probe: &DistanceParts,
    target: &DistanceParts,
    best: Option<OrientedCandidate>,
    swapped: bool,
) -> crate::error::Result<Option<OrientedCandidate>> {
    // The incoming best may belong to the OTHER orientation; keep its tag
    // unless this sweep improves on it.
    let incoming = best;
    let mut best = incoming.as_ref().map(|oriented| NearestCandidate {
        probe: oriented.candidate.probe,
        target: oriented.candidate.target,
        distance_squared: oriented.candidate.distance_squared,
        distance_key: oriented.candidate.distance_key,
    });
    let incoming_distance = best.as_ref().map(|b| b.distance_squared);
    best = match target.bvh() {
        Some(bvh) => bvh.nearest_to_points(&target.linework, probe.probe_points(), best)?,
        None => target
            .linework
            .nearest_to_points(probe.probe_points(), best)?,
    };
    if !target.point_only.is_empty() {
        if let Some(index) = target.point_index()? {
            // Indexed: each probe point's nearest target point in O(log points).
            for probe_point in probe.probe_points() {
                if let Some((target_point, distance_squared)) = index.nearest(probe_point) {
                    let distance_key = point_pair_distance_key(probe_point.xy(), target_point.xy());
                    if let Some(candidate) = best.as_ref()
                        && !distance_key.cmp(&candidate.distance_key)?.is_lt()
                    {
                        continue;
                    }
                    best = Some(NearestCandidate {
                        probe: probe_point,
                        target: target_point,
                        distance_squared,
                        distance_key,
                    });
                }
            }
        } else {
            for probe_point in probe.probe_points() {
                for &target_point in &target.point_only {
                    let distance_squared = point_distance_squared(probe_point, target_point);
                    let distance_key = point_pair_distance_key(probe_point.xy(), target_point.xy());
                    let improves = match best.as_ref() {
                        Some(candidate) => distance_key.cmp(&candidate.distance_key)?.is_lt(),
                        None => true,
                    };
                    if improves {
                        best = Some(NearestCandidate {
                            probe: probe_point,
                            target: target_point,
                            distance_squared,
                            distance_key,
                        });
                    }
                }
            }
        }
    }
    let improved = match incoming.as_ref() {
        None => true,
        Some(previous) => match best.as_ref() {
            None => false,
            Some(candidate) => {
                Some(candidate.distance_squared) != incoming_distance
                    || !same_point(candidate.probe, previous.candidate.probe)
                    || !same_point(candidate.target, previous.candidate.target)
                    || candidate
                        .distance_key
                        .cmp(&previous.candidate.distance_key)?
                        .is_lt()
            },
        },
    };
    Ok(match (best, incoming, improved) {
        (Some(candidate), _, true) | (Some(candidate), None, _) => {
            Some(OrientedCandidate { candidate, swapped })
        },
        (_, incoming, _) => incoming,
    })
}

/// A [`NearestCandidate`] remembering which operand was the probe side.
pub(crate) struct OrientedCandidate {
    pub(crate) candidate: NearestCandidate,
    pub(crate) swapped: bool,
}

/// Boundary/isolated-point intersection witness after the caller has already
/// run [`quick_area_overlap`]. Keeping the area probe out of this helper avoids
/// repeating both representative-containment scans on the negative path.
pub(crate) fn parts_boundary_witness(
    left: &Shape,
    left_parts: &DistanceParts,
    right: &Shape,
    right_parts: &DistanceParts,
) -> crate::error::Result<Option<Point>> {
    if left.has_area_parts() || right.has_area_parts() {
        let intersection = crate::geometry::overlay_all(
            &[left, right],
            crate::geometry::TopologyDomain::Cartesian,
            crate::geometry::OverlayOp::Intersection,
            crate::geometry::Strictness::Lenient,
        )?;
        let mut witness = None;
        intersection.for_each_point(|point| {
            witness.get_or_insert(point);
        });
        return Ok(witness);
    }
    if let Some((left_segment, right_segment)) = parts_crossing_pair(left_parts, right_parts)
        && let Some(point) = segment_contact_point(left_segment, right_segment)?
    {
        return Ok(Some(point.point()));
    }
    for &point in &left_parts.point_only {
        if parts_covers_point(right, right_parts, point)? {
            return Ok(Some(point));
        }
    }
    for &point in &right_parts.point_only {
        if parts_covers_point(left, left_parts, point)? {
            return Ok(Some(point));
        }
    }
    Ok(None)
}

/// First touching/crossing segment pair (full ordinates), mirroring
/// [`parts_segments_cross`]'s dispatch.
pub(crate) fn parts_crossing_pair(
    left: &DistanceParts,
    right: &DistanceParts,
) -> Option<(Segment, Segment)> {
    if left.linework.segment_count() == 0 || right.linework.segment_count() == 0 {
        return None;
    }
    match (left.bvh(), right.bvh()) {
        (Some(left_bvh), Some(right_bvh)) => {
            left_bvh.find_intersecting_pair(&left.linework, right_bvh, &right.linework)
        },
        (Some(left_bvh), None) => {
            let mut hit = None;
            right.linework.for_each_segment(|probe| {
                if hit.is_none()
                    && let Some(target) = left_bvh.find_intersecting_segment(&left.linework, probe)
                {
                    hit = Some((target, probe));
                }
            });
            hit
        },
        (None, Some(right_bvh)) => {
            let mut hit = None;
            left.linework.for_each_segment(|probe| {
                if hit.is_none()
                    && let Some(target) =
                        right_bvh.find_intersecting_segment(&right.linework, probe)
                {
                    hit = Some((probe, target));
                }
            });
            hit
        },
        (None, None) => left.linework.find_intersecting_pair_flat(&right.linework),
    }
}

impl DistanceParts {
    /// Every vertex of this side — the linework columns (which contain all
    /// chain vertices) chained with the isolated points — as a streamed
    /// coordinate probe source.
    pub(crate) fn probe_coords(&self) -> impl Iterator<Item = (f64, f64)> + '_ {
        self.linework
            .vertex_coords()
            .chain(self.point_only.iter().map(|point| (point.x, point.y)))
    }

    /// Full-point sibling of [`probe_coords`](Self::probe_coords) for the
    /// argmin sweeps (Z/M ride along for the result pair).
    pub(crate) fn probe_points(&self) -> impl Iterator<Item = Point> + '_ {
        self.linework
            .vertex_points()
            .chain(self.point_only.iter().copied())
    }

    /// The cached facet BVH over this side's linework, built on first use
    /// once the segment count clears the crossover (`None` below it).
    pub(crate) fn bvh(&self) -> Option<&FacetBvh> {
        self.facet_bvh
            .get_or_init(|| {
                (self.linework.segment_count() >= BVH_MIN_INDEXED_SEGMENTS)
                    .then(|| FacetBvh::build(&self.linework))
                    .flatten()
            })
            .as_ref()
    }

    /// The cached R-tree over this side's isolated points, built on first use
    /// once the point count clears the crossover (`None` below it — a short
    /// brute scan wins under the rstar bulk-load setup cost).
    pub(crate) fn point_index(&self) -> crate::error::Result<Option<&PointSetIndex>> {
        if let Some(index) = self.point_index.get() {
            return Ok(index.as_ref());
        }
        let index = if self.point_only.len() >= POINT_INDEX_MIN_POINTS {
            Some(PointSetIndex::build(self.point_only.iter().copied())?)
        } else {
            None
        };
        let _ = self.point_index.set(index);
        Ok(self
            .point_index
            .get()
            .expect("point index was published")
            .as_ref())
    }
}

/// Point-count crossover for indexing the isolated-point set in nearest-points:
/// below it the brute point-vs-point scan beats the rstar bulk-load + query
/// setup; above it the O(probe·log points) query wins (multipoint operands).
pub(crate) const POINT_INDEX_MIN_POINTS: usize = 32;
