use super::*;
impl ShapeData {
    /// Planar distance with both sides' prepared state cached on the handle —
    /// shadowing [`Shape::distance`] (which builds throwaway parts) so any
    /// caller holding the handle gets amortized indexes automatically.
    pub fn distance(&self, other: &Self) -> f64 {
        // Same point-pair shortcut as `Shape::distance`: a pair of points
        // needs no parts machinery at all.
        if let (Shape::Point(a), Shape::Point(b)) = (self.shape(), other.shape()) {
            return point_distance(*a, *b);
        }
        // Disjoint boxes cannot intersect: the zero-distance oracles only
        // run when the bounds overlap (they answer `false` for everything
        // else by construction).
        let bounds = self.bounds().zip(other.bounds());
        let overlapping = bounds.is_some_and(|(left, right)| left.intersects(right));
        if overlapping && quick_area_overlap(self.shape(), other.shape()).is_some() {
            return 0.0;
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
            && let Some(squared) = puntal_brute_distance_squared(self.shape(), other.shape())
        {
            return squared.sqrt();
        }
        let self_parts = self.distance_parts();
        let other_parts = other.distance_parts();
        if overlapping
            && parts_boundary_contact(self.shape(), self_parts, other.shape(), other_parts)
        {
            return 0.0;
        }
        self.shape()
            .distance_disjoint_with_parts(self_parts, other_parts)
    }

    /// Planar `dwithin` on cached prepared state — see [`Self::distance`].
    pub fn dwithin(&self, other: &Self, distance: f64) -> bool {
        // Point pairs answer directly (mirrors `Shape::dwithin`).
        if let (Shape::Point(a), Shape::Point(b)) = (self.shape(), other.shape()) {
            return point_distance_squared(*a, *b) <= distance * distance;
        }
        let bounds = self.bounds().zip(other.bounds());
        let limit = distance * distance;
        // The separation gate runs before any parts work — boxes farther
        // apart than the limit answer immediately.
        if limit.is_finite()
            && let Some((left, right)) = bounds
            && bounds_distance_squared(left, right) > limit
        {
            return false;
        }
        // Disjoint boxes cannot intersect — see `distance`.
        let overlapping = bounds.is_some_and(|(left, right)| left.intersects(right));
        if overlapping && quick_area_overlap(self.shape(), other.shape()).is_some() {
            return true;
        }
        // Same in-place puntal kernel as `distance` (squared, so the two stay
        // consistent to one ulp at the boundary): compare the min squared
        // point-to-edge distance to the squared limit (`limit` is +inf for a
        // non-finite distance, so a within-range geometry still answers
        // correctly). Extreme-coordinate operands defer to the hypot sweep.
        if let Some((left_b, right_b)) = bounds
            && bounds_squared_safe(left_b)
            && bounds_squared_safe(right_b)
            && let Some(squared) = puntal_brute_distance_squared(self.shape(), other.shape())
        {
            return squared <= limit;
        }
        let self_parts = self.distance_parts();
        let other_parts = other.distance_parts();
        if overlapping
            && parts_boundary_contact(self.shape(), self_parts, other.shape(), other_parts)
        {
            return true;
        }
        if !limit.is_finite() {
            return self
                .shape()
                .distance_disjoint_with_parts(self_parts, other_parts)
                <= distance;
        }
        self.shape()
            .dwithin_disjoint_with_parts(self_parts, other_parts, limit)
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
            crate::crs::ensure_geographic_lonlat(a.x, a.y)?;
            crate::crs::ensure_geographic_lonlat(b.x, b.y)?;
            return Ok(metric.segment_length(*a, *b));
        }
        let key = GeodesicPartsKey::new(crs, semi_major, flattening);
        let self_parts = self.geodesic_parts(self_cache, key.clone(), metric)?;
        let other_parts = other.geodesic_parts(other_cache, key, metric)?;
        Ok(geodesic_distance_with_parts(
            self.shape(),
            &self_parts,
            other.shape(),
            &other_parts,
            metric,
        ))
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
        if let (Shape::Point(a), Shape::Point(b)) = (self.shape(), other.shape()) {
            crate::crs::ensure_geographic_lonlat(a.x, a.y)?;
            crate::crs::ensure_geographic_lonlat(b.x, b.y)?;
            return Ok(metric.segment_length(*a, *b) <= distance);
        }
        let key = GeodesicPartsKey::new(crs, semi_major, flattening);
        let self_parts = self.geodesic_parts(self_cache, key.clone(), metric)?;
        let other_parts = other.geodesic_parts(other_cache, key, metric)?;
        Ok(geodesic_dwithin_with_parts(
            self.shape(),
            &self_parts,
            other.shape(),
            &other_parts,
            metric,
            distance,
        ))
    }

    /// Boundary-inclusive `covers_point` on cached prepared state — the
    /// exact zero test for the point lanes. Bare (multi)polygons answer
    /// through the banded raycaster (interior + ring boundary; they carry no
    /// other parts); everything else through the prepared parts (isolated
    /// identity, robust on-segment via the facet tree, recursive area parts).
    pub(crate) fn covers_point_cached(&self, point: Point) -> bool {
        self.point_tester().map_or_else(
            || parts_covers_point(self.shape(), self.distance_parts(), point),
            |tester| tester.covers_point(point),
        )
    }

    /// Planar distance from one point probe to this geometry, on cached
    /// prepared state — see [`Self::distance_points`] for the batch lane.
    pub fn distance_point(&self, point: Point) -> f64 {
        self.distance_point_with(point, &mut Vec::new())
    }

    /// Per-row planar distances from an XY probe stream, on cached prepared
    /// state — the packed point-array lane: parts resolve once and every
    /// probe's tree descent reuses one traversal stack (no per-row handle,
    /// no per-row allocation).
    pub fn distance_points(&self, probes: impl Iterator<Item = (f64, f64)>) -> Vec<f64> {
        let mut stack = Vec::new();
        probes
            .map(|(x, y)| self.distance_point_with(Point::new_unchecked_xy(x, y), &mut stack))
            .collect()
    }

    /// One probe's distance over a caller-provided (reused) traversal stack.
    fn distance_point_with(&self, point: Point, stack: &mut Vec<u32>) -> f64 {
        if self.covers_point_cached(point) {
            return 0.0;
        }
        let parts = self.distance_parts();
        let safe = squared_space_safe(parts) && coordinate_squared_safe(point);
        let probe = std::iter::once((point.x, point.y));
        let mut best = match (parts.bvh(), safe) {
            (Some(bvh), true) => bvh
                .min_point_distance_with_stack::<true>(
                    &parts.linework,
                    point.x,
                    point.y,
                    stack,
                    f64::INFINITY,
                )
                .sqrt(),
            (Some(bvh), false) => bvh.min_point_distance_with_stack::<false>(
                &parts.linework,
                point.x,
                point.y,
                stack,
                f64::INFINITY,
            ),
            (None, true) => parts
                .linework
                .min_points_distance::<true>(probe, f64::INFINITY)
                .sqrt(),
            (None, false) => parts
                .linework
                .min_points_distance::<false>(probe, f64::INFINITY),
        };
        for &other in &parts.point_only {
            best = best.min(point_distance(point, other));
        }
        best
    }

    /// Planar `dwithin` from one point probe, on cached prepared state —
    /// boundary-inclusive, area-interior aware (a point inside a polygon is
    /// at distance 0 even though every ring edge is farther). See
    /// [`Self::dwithin_points`] for the batch lane.
    pub fn dwithin_point(&self, point: Point, distance: f64) -> bool {
        self.dwithin_point_with(point, distance, &mut Vec::new())
    }

    /// Per-row planar `dwithin` from an XY probe stream — the batch sibling
    /// of [`Self::dwithin_point`], one shared traversal stack across rows.
    pub fn dwithin_points(
        &self,
        probes: impl Iterator<Item = (f64, f64)>,
        distance: f64,
    ) -> Vec<bool> {
        let mut stack = Vec::new();
        probes
            .map(|(x, y)| {
                self.dwithin_point_with(Point::new_unchecked_xy(x, y), distance, &mut stack)
            })
            .collect()
    }

    /// One probe's `dwithin` over a caller-provided (reused) traversal stack.
    fn dwithin_point_with(&self, point: Point, distance: f64, stack: &mut Vec<u32>) -> bool {
        if self.covers_point_cached(point) {
            return true;
        }
        let parts = self.distance_parts();
        let limit = distance * distance;
        if !limit.is_finite() {
            return self.distance_point_with(point, stack) <= distance;
        }
        let simd = squared_space_safe(parts) && coordinate_squared_safe(point);
        let linework_hit = parts.bvh().map_or_else(
            || {
                parts
                    .linework
                    .any_points_within(std::iter::once((point.x, point.y)), limit, simd)
            },
            |bvh| {
                bvh.point_within_with_stack(&parts.linework, point.x, point.y, limit, simd, stack)
            },
        );
        linework_hit
            || parts
                .point_only
                .iter()
                .any(|&other| point_distance_squared(point, other) <= limit)
    }

    /// Per-row geodesic distances from lon/lat point probes to this fixed
    /// shape, reusing one ellipsoid-specific working set and one BVH stack.
    pub(crate) fn geodesic_distance_points(
        &self,
        frame_cache: &FrameDependentCaches,
        probes: impl Iterator<Item = (f64, f64)>,
        crs: &str,
        semi_major: f64,
        flattening: f64,
        metric: &impl GeodesicMetric,
    ) -> Result<Vec<f64>> {
        let key = GeodesicPartsKey::new(crs, semi_major, flattening);
        let parts = self.geodesic_parts(frame_cache, key, metric)?;
        let mut stack = Vec::new();
        probes
            .map(|(x, y)| {
                crate::crs::ensure_geographic_lonlat(x, y)?;
                Ok(geodesic_point_distance_with_parts(
                    self.shape(),
                    Point::new_unchecked_xy(x, y),
                    &parts,
                    metric,
                    &mut stack,
                ))
            })
            .collect()
    }

    /// Per-row geodesic `dwithin` from lon/lat point probes to this fixed
    /// shape, sharing the fixed-shape BVH and traversal stack.
    pub(crate) fn geodesic_dwithin_points(
        &self,
        frame_cache: &FrameDependentCaches,
        probes: impl Iterator<Item = (f64, f64)>,
        crs: &str,
        semi_major: f64,
        flattening: f64,
        metric: &impl GeodesicMetric,
        distance: f64,
    ) -> Result<Vec<bool>> {
        let key = GeodesicPartsKey::new(crs, semi_major, flattening);
        let parts = self.geodesic_parts(frame_cache, key, metric)?;
        let mut stack = Vec::new();
        probes
            .map(|(x, y)| {
                crate::crs::ensure_geographic_lonlat(x, y)?;
                Ok(geodesic_point_dwithin_with_parts(
                    self.shape(),
                    Point::new_unchecked_xy(x, y),
                    &parts,
                    metric,
                    distance,
                    &mut stack,
                ))
            })
            .collect()
    }

    /// Planar `intersects` on cached prepared state — the facet trees built
    /// by any earlier distance/dwithin call drive the crossing test, so
    /// repeated predicates on the same operands never rebuild an index.
    pub fn intersects(&self, other: &Self) -> bool {
        let (Some(left_bounds), Some(right_bounds)) = (self.bounds(), other.bounds()) else {
            return false;
        };
        if !left_bounds.intersects(right_bounds) {
            return false;
        }
        let (left, right) = (self.shape(), other.shape());
        // Two axis-aligned rectangles each equal their bounding box, so
        // overlapping bounds is already the exact answer — the box/tile/
        // envelope fast path (GEOS special-cases rectangles the same way).
        if left.is_axis_aligned_rectangle(left_bounds)
            && right.is_axis_aligned_rectangle(right_bounds)
        {
            return true;
        }
        // Containment first (raycast over the rings, no prepared linework) so
        // the common area-overlap case never builds `distance_parts`. `||`
        // short-circuits: the boundary-crossing test — and ONLY it — pays to
        // build the prepared linework, and only when containment is negative.
        area_overlap_probe(left, right)
            || parts_boundary_contact(left, self.distance_parts(), right, other.distance_parts())
    }
}
