use super::RequiredPointTester;
use crate::geometry::distance::{
    Result, area_overlap_probe, bounds_distance_squared, bounds_squared_safe,
    coordinate_squared_safe, ensure_geodesic_linework, geodesic_distance_with_parts,
    geodesic_dwithin_with_parts, geodesic_point_distance_with_parts_and_scratch,
    geodesic_point_dwithin_with_parts_and_scratch, parts_boundary_contact, parts_covers_point,
    puntal_brute_distance, puntal_brute_distance_squared, quick_area_overlap, squared_space_safe,
};
use crate::geometry::{
    DistanceParts, FacetBvh, FrameDependentCaches, GeodesicMetric, GeodesicPartsKey, Point,
    PointBatchTester, PreparedPlan, Shape, distance, finish_planar_squared_min, point_distance,
    points_dwithin,
};
impl PreparedPlan {
    /// Planar distance with both sides' prepared state cached on the handle —
    /// shadowing [`Shape::distance`] (which builds throwaway parts) so any
    /// caller holding the handle gets amortized indexes automatically.
    pub fn distance(&self, other: &Self) -> Result<f64> {
        // Same point-pair shortcut as `Shape::distance`: a pair of points
        // needs no parts machinery at all.
        if let (Shape::Point(a), Shape::Point(b)) = (self.shape(), other.shape()) {
            return Ok(point_distance(*a, *b));
        }
        // Disjoint boxes cannot intersect: the zero-distance oracles only
        // run when the bounds overlap (they answer `false` for everything
        // else by construction).
        let bounds = self.bounds().zip(other.bounds());
        let overlapping = bounds.is_some_and(|(left, right)| left.intersects(right));
        if overlapping && quick_area_overlap(self.shape(), other.shape())?.is_some() {
            return Ok(0.0);
        }
        // Puntal-vs-(few-segment lineal/areal): the disjoint distance is the min
        // point-to-edge distance, walked in place. Below the BVH crossover the
        // sweep is brute anyway, so this just skips the per-operand prepared
        // linework allocation that dominates point-vs-small-polygon distance.
        // The 0-distance (containment/boundary) cases returned above, so the
        // remainder is disjoint and the boundary distance is exact. Skip
        // extreme-coordinate operands, where the squared distance overflows
        // and only the hypot-space sweep stays exact.
        if let Some((left, right)) = bounds
            && bounds_squared_safe(left)
            && bounds_squared_safe(right)
            && let Some(squared) = puntal_brute_distance_squared(self.shape(), other.shape())?
        {
            // Always finish through the shared rescue — bare sqrt false-zeros
            // at 1e-200 point×line / multipoint×poly separations.
            return finish_planar_squared_min(squared, || {
                Ok(puntal_brute_distance(self.shape(), other.shape())?.unwrap_or(0.0))
            });
        }
        let self_parts = self.distance_parts()?;
        let other_parts = other.distance_parts()?;
        if overlapping
            && parts_boundary_contact(self.shape(), self_parts, other.shape(), other_parts)?
        {
            return Ok(0.0);
        }
        self.shape()
            .distance_disjoint_with_parts(self_parts, other_parts)
    }

    /// Planar `dwithin` on cached prepared state — see [`Self::distance`].
    pub fn dwithin(&self, other: &Self, distance: f64) -> Result<bool> {
        if distance == f64::INFINITY {
            return Ok(!self.shape().is_empty() && !other.shape().is_empty());
        }
        // Point pairs answer directly (mirrors `Shape::dwithin`).
        if let (Shape::Point(a), Shape::Point(b)) = (self.shape(), other.shape()) {
            return Ok(points_dwithin(*a, *b, distance));
        }
        let bounds = self.bounds().zip(other.bounds());
        let limit = distance * distance;
        // The separation gate runs before any parts work — boxes farther
        // apart than the limit answer immediately.
        if limit.is_finite()
            && let Some((left, right)) = bounds
            && bounds_distance_squared(left, right) > limit
        {
            return Ok(false);
        }
        // Disjoint boxes cannot intersect — see `distance`.
        let overlapping = bounds.is_some_and(|(left, right)| left.intersects(right));
        if overlapping && quick_area_overlap(self.shape(), other.shape())?.is_some() {
            return Ok(true);
        }
        // Puntal brute finished through the same helper as distance.
        let use_sq = limit.is_normal() && distance > 0.0;
        if use_sq
            && let Some((left_b, right_b)) = bounds
            && bounds_squared_safe(left_b)
            && bounds_squared_safe(right_b)
            && let Some(squared) = puntal_brute_distance_squared(self.shape(), other.shape())?
        {
            let d = finish_planar_squared_min(squared, || {
                Ok(puntal_brute_distance(self.shape(), other.shape())?.unwrap_or(0.0))
            })?;
            return Ok(d <= distance);
        }
        let self_parts = self.distance_parts()?;
        let other_parts = other.distance_parts()?;
        if overlapping
            && parts_boundary_contact(self.shape(), self_parts, other.shape(), other_parts)?
        {
            return Ok(true);
        }
        if !limit.is_finite() || !use_sq {
            return Ok(self
                .shape()
                .distance_disjoint_with_parts(self_parts, other_parts)?
                <= distance);
        }
        self.shape()
            .dwithin_disjoint_with_parts(self_parts, other_parts, distance)
    }

    /// Geodesic distance with both sides' geodesic working state cached on the
    /// handles for one normalized CRS and ellipsoid.
    pub(crate) fn geodesic_distance_cached(
        &self,
        self_cache: &FrameDependentCaches,
        other: &Self,
        other_cache: &FrameDependentCaches,
        crs: &str,
        semi_major: f64,
        flattening: f64,
        metric: &impl GeodesicMetric,
    ) -> Result<f64> {
        if let (Shape::Point(a), Shape::Point(b)) = (self.shape(), other.shape()) {
            metric.ensure_shape(self.shape())?;
            metric.ensure_shape(other.shape())?;
            return Ok(metric.segment_length(*a, *b));
        }
        let self_key = GeodesicPartsKey::new_at_generation(
            crs,
            semi_major,
            flattening,
            metric.runtime_generation(),
        )?;
        let other_key = GeodesicPartsKey::new_at_generation(
            crs,
            semi_major,
            flattening,
            metric.runtime_generation(),
        )?;
        let self_parts = self.geodesic_parts(self_cache, self_key, metric)?;
        let other_parts = other.geodesic_parts(other_cache, other_key, metric)?;
        geodesic_distance_with_parts(
            self.shape(),
            &self_parts,
            other.shape(),
            &other_parts,
            metric,
        )
    }

    /// Threshold-aware geodesic `dwithin` with cached per-shape geodesic
    /// working state. Exact hits at or below `distance` return immediately.
    pub(crate) fn geodesic_dwithin_cached(
        &self,
        self_cache: &FrameDependentCaches,
        other: &Self,
        other_cache: &FrameDependentCaches,
        crs: &str,
        semi_major: f64,
        flattening: f64,
        metric: &impl GeodesicMetric,
        distance: f64,
    ) -> Result<bool> {
        if distance.is_infinite() && distance.is_sign_positive() {
            ensure_geodesic_linework(self.shape(), metric)?;
            ensure_geodesic_linework(other.shape(), metric)?;
            return Ok(!self.shape().is_empty() && !other.shape().is_empty());
        }
        if let (Shape::Point(a), Shape::Point(b)) = (self.shape(), other.shape()) {
            metric.ensure_shape(self.shape())?;
            metric.ensure_shape(other.shape())?;
            return Ok(metric.segment_length(*a, *b) <= distance);
        }
        let self_key = GeodesicPartsKey::new_at_generation(
            crs,
            semi_major,
            flattening,
            metric.runtime_generation(),
        )?;
        let other_key = GeodesicPartsKey::new_at_generation(
            crs,
            semi_major,
            flattening,
            metric.runtime_generation(),
        )?;
        let self_parts = self.geodesic_parts(self_cache, self_key, metric)?;
        let other_parts = other.geodesic_parts(other_cache, other_key, metric)?;
        geodesic_dwithin_with_parts(
            self.shape(),
            &self_parts,
            other.shape(),
            &other_parts,
            metric,
            distance,
        )
    }

    /// Per-row planar distances from an XY probe stream, on cached prepared
    /// state — the packed point-array lane: parts resolve once and every
    /// probe's tree descent reuses one traversal stack (no per-row handle,
    /// no per-row allocation).
    #[expect(
        clippy::impl_trait_in_params,
        reason = "the probe stream is consumed once and its concrete iterator type is not part of the API"
    )]
    pub fn distance_points(&self, probes: impl Iterator<Item = (f64, f64)>) -> Result<Vec<f64>> {
        let tester = self.point_tester_required(RequiredPointTester(()))?;
        let parts = self.distance_parts()?;
        let parts_squared_safe = squared_space_safe(parts);
        let bvh = parts.bvh();
        let mut stack = Vec::new();
        let mut distances = Vec::new();
        for (x, y) in probes {
            let distance = Self::distance_point_with_resolved(
                self.shape(),
                Point::new_raw_xy(x, y),
                tester,
                parts,
                parts_squared_safe,
                bvh,
                &mut stack,
            )?;
            crate::try_push_vec(&mut distances, distance)?;
        }
        Ok(distances)
    }

    fn distance_point_with_resolved(
        shape: &Shape,
        point: Point,
        tester: Option<&PointBatchTester>,
        parts: &DistanceParts,
        parts_squared_safe: bool,
        bvh: Option<&FacetBvh>,
        stack: &mut Vec<u32>,
    ) -> Result<f64> {
        let covered = match tester {
            None => parts_covers_point(shape, parts, point)?,
            Some(tester) => tester.covers_point(point)?,
        };
        if covered {
            return Ok(0.0);
        }
        let safe = parts_squared_safe && coordinate_squared_safe(point);
        // Local min to linework: squared (SQUARED=true) or distance space.
        let mut min_linework = |squared: bool| -> Result<f64> {
            if squared {
                bvh.map_or_else(
                    || {
                        parts.linework.min_points_distance::<true>(
                            std::iter::once((point.x, point.y)),
                            f64::INFINITY,
                        )
                    },
                    |bvh| {
                        bvh.min_point_distance_with_stack::<true>(
                            &parts.linework,
                            point.x,
                            point.y,
                            stack,
                            f64::INFINITY,
                        )
                    },
                )
            } else {
                bvh.map_or_else(
                    || {
                        parts.linework.min_points_distance::<false>(
                            std::iter::once((point.x, point.y)),
                            f64::INFINITY,
                        )
                    },
                    |bvh| {
                        bvh.min_point_distance_with_stack::<false>(
                            &parts.linework,
                            point.x,
                            point.y,
                            stack,
                            f64::INFINITY,
                        )
                    },
                )
            }
        };
        let mut best = if safe {
            finish_planar_squared_min(min_linework(true)?, || min_linework(false))?
        } else {
            min_linework(false)?
        };
        for &other in &parts.point_only {
            best = best.min(point_distance(point, other));
        }
        Ok(best)
    }

    /// Planar `dwithin` from one point probe, on cached prepared state —
    /// boundary-inclusive, area-interior aware (a point inside a polygon is
    /// at distance 0 even though every ring edge is farther). See
    /// [`Self::dwithin_points`] for the batch lane.
    pub fn dwithin_point(&self, point: Point, distance: f64) -> Result<bool> {
        if distance == f64::INFINITY {
            return Ok(!self.shape().is_empty());
        }
        self.dwithin_point_with(point, distance, &mut Vec::new())
    }

    /// Per-row planar `dwithin` from an XY probe stream — the batch sibling
    /// of [`Self::dwithin_point`], one shared traversal stack across rows.
    #[expect(
        clippy::impl_trait_in_params,
        reason = "the probe stream is consumed once and its concrete iterator type is not part of the API"
    )]
    pub fn dwithin_points(
        &self,
        probes: impl Iterator<Item = (f64, f64)>,
        distance: f64,
    ) -> Result<Vec<bool>> {
        if distance == f64::INFINITY {
            let present = !self.shape().is_empty();
            let mut matches = Vec::new();
            for _ in probes {
                crate::try_push_vec(&mut matches, present)?;
            }
            return Ok(matches);
        }
        let tester = self.point_tester_required(RequiredPointTester(()))?;
        let parts = self.distance_parts()?;
        let parts_squared_safe = squared_space_safe(parts);
        let bvh = parts.bvh();
        let mut stack = Vec::new();
        let mut matches = Vec::new();
        for (x, y) in probes {
            let matched = Self::dwithin_point_with_resolved(
                self.shape(),
                Point::new_raw_xy(x, y),
                distance,
                tester,
                parts,
                parts_squared_safe,
                bvh,
                &mut stack,
            )?;
            crate::try_push_vec(&mut matches, matched)?;
        }
        Ok(matches)
    }

    /// One probe's `dwithin` over a caller-provided (reused) traversal stack.
    fn dwithin_point_with(
        &self,
        point: Point,
        distance: f64,
        stack: &mut Vec<u32>,
    ) -> Result<bool> {
        let tester = self.point_tester_required(RequiredPointTester(()))?;
        let parts = self.distance_parts()?;
        Self::dwithin_point_with_resolved(
            self.shape(),
            point,
            distance,
            tester,
            parts,
            squared_space_safe(parts),
            parts.bvh(),
            stack,
        )
    }

    fn dwithin_point_with_resolved(
        shape: &Shape,
        point: Point,
        distance: f64,
        tester: Option<&PointBatchTester>,
        parts: &DistanceParts,
        parts_squared_safe: bool,
        bvh: Option<&FacetBvh>,
        stack: &mut Vec<u32>,
    ) -> Result<bool> {
        let covered = match tester {
            None => parts_covers_point(shape, parts, point)?,
            Some(tester) => tester.covers_point(point)?,
        };
        if covered {
            return Ok(true);
        }
        let limit_sq = distance * distance;
        let use_sq = limit_sq.is_normal() && distance > 0.0;
        if !use_sq {
            return Ok(Self::distance_point_with_resolved(
                shape,
                point,
                tester,
                parts,
                parts_squared_safe,
                bvh,
                stack,
            )? <= distance);
        }
        let simd = parts_squared_safe && coordinate_squared_safe(point);
        let linework_hit = match bvh {
            None => parts.linework.any_points_within(
                std::iter::once((point.x, point.y)),
                limit_sq,
                simd,
            )?,
            Some(bvh) => bvh.point_within_with_stack(
                &parts.linework,
                point.x,
                point.y,
                limit_sq,
                simd,
                stack,
            )?,
        };
        Ok(linework_hit
            || parts
                .point_only
                .iter()
                .any(|&other| points_dwithin(point, other, distance)))
    }

    /// Per-row geodesic distances from lon/lat point probes to this fixed
    /// shape, reusing one ellipsoid-specific working set and one BVH stack.
    pub(crate) fn geodesic_distance_points(
        &self,
        frame_cache: &FrameDependentCaches,
        probes: impl ExactSizeIterator<Item = (f64, f64)> + Clone,
        crs: &str,
        semi_major: f64,
        flattening: f64,
        metric: &impl GeodesicMetric,
        frame: crate::geographic::GeographicFrame,
    ) -> Result<Vec<f64>> {
        let probe_count = probes.len();
        for (x, y) in probes.clone() {
            crate::crs::ensure_geographic_lonlat_in_frame(frame, x, y)?;
        }
        let key = GeodesicPartsKey::new_at_generation(
            crs,
            semi_major,
            flattening,
            metric.runtime_generation(),
        )?;
        let parts = self.geodesic_parts(frame_cache, key, metric)?;
        let bvh = parts.geodesic_bvh(metric)?;
        let mut scratch = distance::geodesic_scratch();
        let mut stack = Vec::new();
        let mut values = Vec::new();
        crate::boundary::try_reserve_vec(&mut values, probe_count)?;
        for (x, y) in probes {
            let value = geodesic_point_distance_with_parts_and_scratch(
                Point::new_raw_xy(x, y),
                &parts,
                metric,
                bvh,
                &mut scratch,
                Some(&mut stack),
            )?;
            crate::try_push_vec(&mut values, value)?;
        }
        Ok(values)
    }

    /// Per-row geodesic `dwithin` from lon/lat point probes to this fixed
    /// shape, sharing the fixed-shape BVH and traversal stack.
    pub(crate) fn geodesic_dwithin_points(
        &self,
        frame_cache: &FrameDependentCaches,
        probes: impl ExactSizeIterator<Item = (f64, f64)> + Clone,
        crs: &str,
        semi_major: f64,
        flattening: f64,
        metric: &impl GeodesicMetric,
        distance: f64,
        frame: crate::geographic::GeographicFrame,
    ) -> Result<Vec<bool>> {
        let probe_count = probes.len();
        for (x, y) in probes.clone() {
            crate::crs::ensure_geographic_lonlat_in_frame(frame, x, y)?;
        }
        if distance.is_infinite() && distance.is_sign_positive() {
            ensure_geodesic_linework(self.shape(), metric)?;
            let present = !self.shape().is_empty();
            let mut values = Vec::new();
            crate::boundary::try_reserve_vec(&mut values, probe_count)?;
            for _ in 0..probe_count {
                crate::try_push_vec(&mut values, present)?;
            }
            return Ok(values);
        }
        let key = GeodesicPartsKey::new_at_generation(
            crs,
            semi_major,
            flattening,
            metric.runtime_generation(),
        )?;
        let parts = self.geodesic_parts(frame_cache, key, metric)?;
        let bvh = parts.geodesic_bvh(metric)?;
        let mut scratch = distance::geodesic_scratch();
        let mut stack = Vec::new();
        let mut values = Vec::new();
        crate::boundary::try_reserve_vec(&mut values, probe_count)?;
        for (x, y) in probes {
            let value = geodesic_point_dwithin_with_parts_and_scratch(
                Point::new_raw_xy(x, y),
                &parts,
                metric,
                distance,
                bvh,
                &mut scratch,
                Some(&mut stack),
            )?;
            crate::try_push_vec(&mut values, value)?;
        }
        Ok(values)
    }

    /// Planar `intersects` on cached prepared state — the facet trees built
    /// by any earlier distance/dwithin call drive the crossing test, so
    /// repeated predicates on the same operands never rebuild an index.
    pub fn intersects(&self, other: &Self) -> Result<bool> {
        let (Some(left_bounds), Some(right_bounds)) = (self.bounds(), other.bounds()) else {
            return Ok(false);
        };
        if !left_bounds.intersects(right_bounds) {
            return Ok(false);
        }
        let (left, right) = (self.shape(), other.shape());
        // Two axis-aligned rectangles each equal their bounding box, so
        // overlapping bounds is already the exact answer — the box/tile/
        // envelope fast path.
        if crate::geometry::is_axis_aligned_rectangle(left, left_bounds)
            && crate::geometry::is_axis_aligned_rectangle(right, right_bounds)
        {
            return Ok(true);
        }
        // Containment first (raycast over the rings, no prepared linework) so
        // the common area-overlap case never builds `distance_parts`. The
        // boundary-crossing test — and ONLY it — pays to build the prepared
        // linework, and only when containment is negative.
        if area_overlap_probe(left, right)? {
            return Ok(true);
        }
        parts_boundary_contact(left, self.distance_parts()?, right, other.distance_parts()?)
    }
}
