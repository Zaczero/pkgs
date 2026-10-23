use crate::crs::PointEdgeScratch;
use crate::geometry::distance::{
    Result, bounds_distance_squared, bounds_squared_safe, cached_chart_distance,
    cached_chart_dwithin, coordinate_squared_safe, parts_boundary_contact, point_chart_distance,
    point_chart_dwithin, puntal_brute_distance, puntal_brute_distance_squared, quick_area_overlap,
    squared_space_safe,
};
use crate::geometry::{
    ChartMetric, DistanceParts, DwithinOutcome, FacetBvh, FrameDependentCaches, GeodesicMetric,
    GeographicMetricKey, GeographicPrepared, Point, PreparedPlan, Shape, distance,
    finish_planar_squared_min, point_distance, points_dwithin, same_point,
};

/// Select coverage once per batch while keeping each metric loop monomorphic.
/// Coverage errors propagate from the enclosing function through `?`.
macro_rules! point_batch_coverage {
    ($probes:expr, $queries:expr, $parts:expr, $bvh:expr, $stack:ident,
     |$point:ident, $covered:ident| $body:block) => {{
        if let Some(queries) = $queries {
            for (x, y) in $probes {
                let $point = Point::new_raw_xy(x, y);
                let $covered = queries.covers($point)?;
                $body
            }
        } else {
            for (x, y) in $probes {
                let $point = Point::new_raw_xy(x, y);
                let $covered = $parts
                    .point_only
                    .iter()
                    .any(|&other| same_point(other, $point))
                    || $bvh.map_or_else(
                        || $parts.linework.covers_point($point),
                        |bvh| bvh.covers_point_with_stack(&$parts.linework, $point, &mut $stack),
                    );
                $body
            }
        }
    }};
}
impl PreparedPlan {
    /// Planar distance with both sides' prepared state cached on the handle —
    /// shadowing [`Shape::distance`] (which builds throwaway parts) so any
    /// caller holding the handle gets amortized indexes automatically.
    pub(crate) fn distance(&self, other: &Self) -> Result<f64> {
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
        if overlapping && quick_area_overlap(self.shape(), other.shape()).is_some() {
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
            && parts_boundary_contact(self.shape(), self_parts, other.shape(), other_parts)
        {
            return Ok(0.0);
        }
        self.shape()
            .distance_disjoint_with_parts(self_parts, other_parts)
    }

    /// Planar `dwithin` on cached prepared state — see [`Self::distance`].
    pub(crate) fn dwithin(&self, other: &Self, distance: f64) -> Result<bool> {
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
        if overlapping && quick_area_overlap(self.shape(), other.shape()).is_some() {
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
            && parts_boundary_contact(self.shape(), self_parts, other.shape(), other_parts)
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
    pub(crate) fn geodesic_distance_cached<M>(
        &self,
        self_cache: &FrameDependentCaches,
        other: &Self,
        other_cache: &FrameDependentCaches,
        crs: &str,
        semi_major: f64,
        flattening: f64,
        metric: &M,
    ) -> Result<f64>
    where
        M: GeodesicMetric + ChartMetric<PointEdgeScratch = PointEdgeScratch>,
    {
        if let (Shape::Point(a), Shape::Point(b)) = (self.shape(), other.shape()) {
            metric.ensure_shape(self.shape())?;
            metric.ensure_shape(other.shape())?;
            return Ok(metric.segment_length(*a, *b));
        }
        cached_chart_distance(
            self,
            self_cache,
            other,
            other_cache,
            crs,
            semi_major,
            flattening,
            metric,
        )
    }

    /// Threshold-aware geodesic `dwithin` with cached per-shape geodesic
    /// working state. Exact hits at or below `distance` return immediately.
    pub(crate) fn geodesic_dwithin_cached<M>(
        &self,
        self_cache: &FrameDependentCaches,
        other: &Self,
        other_cache: &FrameDependentCaches,
        crs: &str,
        semi_major: f64,
        flattening: f64,
        metric: &M,
        distance: f64,
    ) -> Result<bool>
    where
        M: GeodesicMetric + ChartMetric<PointEdgeScratch = PointEdgeScratch>,
    {
        if distance.is_infinite() && distance.is_sign_positive() {
            metric.ensure_shape(self.shape())?;
            metric.ensure_shape(other.shape())?;
            return Ok(!self.shape().is_empty() && !other.shape().is_empty());
        }
        if let (Shape::Point(a), Shape::Point(b)) = (self.shape(), other.shape()) {
            metric.ensure_shape(self.shape())?;
            metric.ensure_shape(other.shape())?;
            return Ok(metric.segment_length(*a, *b) <= distance);
        }
        cached_chart_dwithin(
            self,
            self_cache,
            other,
            other_cache,
            crs,
            semi_major,
            flattening,
            metric,
            distance,
        )
    }

    /// Per-row planar distances from an XY probe stream, on cached prepared
    /// state — the packed point-array lane: parts resolve once and every
    /// probe's tree descent reuses one traversal stack (no per-row handle,
    /// no per-row allocation).
    pub(crate) fn distance_points(
        &self,
        probes: impl Iterator<Item = (f64, f64)>,
    ) -> Result<Vec<f64>> {
        let probe_count = probes.size_hint().0;
        let tester = self
            .shape()
            .has_area_parts()
            .then(|| self.prepare_point_queries(probe_count))
            .transpose()?;
        let parts = self.distance_parts()?;
        let parts_squared_safe = squared_space_safe(parts);
        let bvh = parts.bvh();
        let mut stack = Vec::new();
        let mut distances = Vec::with_capacity(probe_count);
        point_batch_coverage!(probes, tester, parts, bvh, stack, |point, covered| {
            let distance = if covered {
                0.0
            } else {
                Self::distance_point_with_resolved(
                    point,
                    parts,
                    parts_squared_safe,
                    bvh,
                    &mut stack,
                )?
            };
            distances.push(distance);
        });
        Ok(distances)
    }

    fn distance_point_with_resolved(
        point: Point,
        parts: &DistanceParts,
        parts_squared_safe: bool,
        bvh: Option<&FacetBvh>,
        stack: &mut Vec<u32>,
    ) -> Result<f64> {
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

    /// Per-row planar `dwithin` from an XY probe stream, with one shared
    /// traversal stack across rows.
    pub(crate) fn dwithin_points(
        &self,
        probes: impl Iterator<Item = (f64, f64)>,
        distance: f64,
    ) -> Result<Vec<bool>> {
        let probe_count = probes.size_hint().0;
        if distance == f64::INFINITY {
            let present = !self.shape().is_empty();
            let mut matches = Vec::with_capacity(probe_count);
            for _ in probes {
                matches.push(present);
            }
            return Ok(matches);
        }
        let tester = self
            .shape()
            .has_area_parts()
            .then(|| self.prepare_point_queries(probes.size_hint().0))
            .transpose()?;
        let parts = self.distance_parts()?;
        let parts_squared_safe = squared_space_safe(parts);
        let bvh = parts.bvh();
        let mut stack = Vec::new();
        let mut matches = Vec::with_capacity(probe_count);
        point_batch_coverage!(probes, tester, parts, bvh, stack, |point, covered| {
            let matched = covered
                || Self::dwithin_point_with_resolved(
                    point,
                    distance,
                    parts,
                    parts_squared_safe,
                    bvh,
                    &mut stack,
                )?;
            matches.push(matched);
        });
        Ok(matches)
    }

    fn dwithin_point_with_resolved(
        point: Point,
        distance: f64,
        parts: &DistanceParts,
        parts_squared_safe: bool,
        bvh: Option<&FacetBvh>,
        stack: &mut Vec<u32>,
    ) -> Result<bool> {
        let limit_sq = distance * distance;
        let use_sq = limit_sq.is_normal() && distance > 0.0;
        if !use_sq {
            return Ok(Self::distance_point_with_resolved(
                point,
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
        metric: &(impl GeodesicMetric + ChartMetric<PointEdgeScratch = PointEdgeScratch>),
        frame: crate::geographic::GeographicFrame,
    ) -> Result<Vec<f64>> {
        let probe_count = probes.len();
        for (x, y) in probes.clone() {
            crate::crs::ensure_geographic_lonlat_in_frame(frame, x, y)?;
        }
        let prepared = GeographicPrepared::try_new(self, frame_cache, frame)?;
        let key = GeographicMetricKey::new_at_generation(
            crs,
            semi_major,
            flattening,
            metric.runtime_generation(),
        );
        let parts = prepared.cached_chart_parts(frame_cache, &key, metric)?;
        let mut scratch = distance::chart_scratch();
        let scratch = &mut *scratch;
        let (point_edge, stack) = (&mut scratch.point_edge, &mut scratch.stack);
        let mut values = Vec::with_capacity(probe_count);
        for (x, y) in probes {
            let point = Point::new_raw_xy(x, y);
            let value = if prepared.point_covers(point)? {
                0.0
            } else {
                point_chart_distance(point, &parts, metric, point_edge, stack)?
            };
            values.push(value);
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
        metric: &(impl GeodesicMetric + ChartMetric<PointEdgeScratch = PointEdgeScratch>),
        distance: f64,
        frame: crate::geographic::GeographicFrame,
    ) -> Result<Vec<bool>> {
        let probe_count = probes.len();
        for (x, y) in probes.clone() {
            crate::crs::ensure_geographic_lonlat_in_frame(frame, x, y)?;
        }
        metric.ensure_shape(self.shape())?;
        if distance.is_infinite() && distance.is_sign_positive() {
            let present = !self.shape().is_empty();
            return Ok(vec![present; probe_count]);
        }
        let prepared = GeographicPrepared::try_new(self, frame_cache, frame)?;
        let key = GeographicMetricKey::new_at_generation(
            crs,
            semi_major,
            flattening,
            metric.runtime_generation(),
        );
        let parts = prepared.cached_chart_parts(frame_cache, &key, metric)?;
        let mut scratch = distance::chart_scratch();
        let scratch = &mut *scratch;
        let (point_edge, stack) = (&mut scratch.point_edge, &mut scratch.stack);
        let mut values = Vec::with_capacity(probe_count);
        for (x, y) in probes {
            if distance.is_nan() {
                return Err(crate::crs::CrsError::message(
                    "geodesic dwithin limit is not finite",
                ));
            }
            let point = Point::new_raw_xy(x, y);
            let outcome = if distance < 0.0 || distance.is_infinite() && distance.is_sign_negative()
            {
                DwithinOutcome::Outside
            } else if prepared.point_covers(point)? {
                DwithinOutcome::Within
            } else {
                point_chart_dwithin(point, &parts, metric, distance, point_edge, stack)?
            };
            values.push(outcome.into_bool()?);
        }
        Ok(values)
    }
}
