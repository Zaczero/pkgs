#[cfg(test)]
use crate::geometry::ShapeData;
use crate::geometry::distance::{
    ChartParts, Result, any_parts_within, chart_directed_hausdorff_distance, chart_scratch,
    discrete_frechet_distance_with_scratch, ensure_geographic_hausdorff_linework,
    min_parts_to_parts, squared_space_safe,
};
use crate::geometry::{
    ChartMetric, DistanceParts, GeodesicMetric, Point, PreparedLinework, Shape, distance,
    finish_planar_squared_min,
};
impl Shape {
    /// Build this shape's [`DistanceParts`] (vertices, packed linework,
    /// isolated points) for reuse across a scalar-vs-array distance/dwithin
    /// sweep.
    pub(crate) fn distance_parts(&self) -> Result<DistanceParts> {
        Ok(DistanceParts {
            point_only: self.point_only_points(),
            linework: PreparedLinework::build(self)?,
            // Deferred: the two column scans are paid on first DISTANCE use, not
            // at build — the CONTACT path (overlaps/touches/…) never reads it.
            squared_safe: std::sync::OnceLock::new(),
            facet_bvh: std::sync::OnceLock::new(),
            point_index: std::sync::OnceLock::new(),
        })
    }

    #[cfg(test)]
    pub fn distance(&self, other: &Self) -> Result<f64> {
        crate::geometry::PreparedPlan::new(ShapeData::new(self.clone())).distance(
            &crate::geometry::PreparedPlan::new(ShapeData::new(other.clone())),
        )
    }

    /// Planar distance reusing a pre-built [`DistanceParts`] for each operand.
    /// The scalar-vs-array path builds the fixed operand's parts once and calls
    /// this per row instead of rebuilding both sides every pair. Caller is
    /// responsible for the `intersects` short-circuit (containment → 0).
    pub(crate) fn distance_disjoint_with_parts(
        &self,
        self_parts: &DistanceParts,
        other_parts: &DistanceParts,
    ) -> Result<f64> {
        // Callers have already short-circuited intersecting operands to 0.
        // For disjoint operands the two vertex sweeps are COMPLETE: squared
        // segment-pair distance is convex over the parameter square; a
        // non-parallel pair's only interior stationary point is the line
        // crossing (excluded by disjointness), and a parallel pair's flat
        // minimum always extends to the parameter boundary. Either way the
        // pair minimum sits on the boundary — one of the four
        // vertex-onto-segment projections, every one of which the sweeps
        // visit. No segment×segment phase exists.
        // Squared-space kernels when every coordinate magnitude keeps the
        // squares finite (the overwhelmingly common case — one sqrt at the
        // end, SIMD facet kernel); `hypot` space for extreme coordinates.
        // Both spaces fold the same minima.
        if squared_space_safe(self_parts) && squared_space_safe(other_parts) {
            let mut best = f64::INFINITY;
            best = min_parts_to_parts::<true>(self_parts, other_parts, best)?;
            best = min_parts_to_parts::<true>(other_parts, self_parts, best)?;
            return finish_planar_squared_min(best, || {
                let mut d = f64::INFINITY;
                d = min_parts_to_parts::<false>(self_parts, other_parts, d)?;
                min_parts_to_parts::<false>(other_parts, self_parts, d)
            });
        }
        let mut best = f64::INFINITY;
        best = min_parts_to_parts::<false>(self_parts, other_parts, best)?;
        min_parts_to_parts::<false>(other_parts, self_parts, best)
    }

    /// Directed and symmetric geographic Hausdorff distance in ellipsoidal
    /// metres over the shapes' straight longitude/latitude edges.
    pub(crate) fn chart_hausdorff_distance<M>(&self, other: &Self, metric: &M) -> Result<f64>
    where
        M: GeodesicMetric + ChartMetric<PointEdgeScratch = crate::crs::PointEdgeScratch>,
    {
        metric.ensure_shape(self)?;
        metric.ensure_shape(other)?;
        ensure_geographic_hausdorff_linework(self)?;
        ensure_geographic_hausdorff_linework(other)?;
        if self.is_empty() || other.is_empty() {
            return Ok(f64::INFINITY);
        }
        let frame = metric.chart_frame();
        let latitude_overlap = self
            .bounds()
            .zip(other.bounds())
            .is_none_or(|(left, right)| {
                let latitude =
                    |value| crate::geographic::Latitude::from_admitted(frame, value).get();
                latitude(left.maxy()) >= latitude(right.miny())
                    && latitude(right.maxy()) >= latitude(left.miny())
            });
        if latitude_overlap {
            let left_plan = crate::geometry::PreparedPlan::transient(self);
            let right_plan = crate::geometry::PreparedPlan::transient(other);
            let left_cache = crate::geometry::FrameDependentCaches::default();
            let right_cache = crate::geometry::FrameDependentCaches::default();
            let left =
                crate::geometry::GeographicPrepared::try_new(&left_plan, &left_cache, frame)?;
            let right =
                crate::geometry::GeographicPrepared::try_new(&right_plan, &right_cache, frame)?;
            let bounds_may_match = !left.bounds_sound()
                || !right.bounds_sound()
                || left.plan().bounds() == right.plan().bounds();
            if bounds_may_match
                && crate::geometry::graph_relate_data_geographic(left, right)?.is_equal_topo()
            {
                return Ok(0.0);
            }
        }
        let source_parts = ChartParts::build(self, metric)?;
        let target_parts = ChartParts::build(other, metric)?;
        let mut scratch = chart_scratch();
        let scratch = &mut *scratch;
        let forward =
            chart_directed_hausdorff_distance(&source_parts, &target_parts, metric, scratch)?;
        if target_parts.edges.is_empty()
            && let Some((first, last)) = source_parts
                .vertices
                .first()
                .zip(source_parts.vertices.last())
        {
            // Each source witness bounds a reverse point minimum. Keep the
            // minimum-search and sample allowances before discarding the query.
            let policy = metric.geodesic_numerical_policy();
            let allowance = policy.minimum_gap()
                + policy.evaluation_allowance(4.0)
                + 128.0 * f64::EPSILON * forward.distance.max(1.0);
            let cutoff = forward.distance - allowance;
            if cutoff > 0.0
                && target_parts.point_only.iter().all(|&point| {
                    metric.segment_length(point, *first) < cutoff
                        || metric.segment_length(point, *last) < cutoff
                })
            {
                return Ok(forward.distance);
            }
        }
        let reverse =
            chart_directed_hausdorff_distance(&target_parts, &source_parts, metric, scratch)?;
        Ok(forward.distance.max(reverse.distance))
    }

    #[cfg(test)]
    pub fn dwithin(&self, other: &Self, distance: f64) -> Result<bool> {
        crate::geometry::PreparedPlan::new(ShapeData::new(self.clone())).dwithin(
            &crate::geometry::PreparedPlan::new(ShapeData::new(other.clone())),
            distance,
        )
    }

    /// `dwithin` reusing a pre-built [`DistanceParts`] per operand. Caller owns
    /// the `intersects`/bounds short-circuits; `distance` is the real radius.
    pub(crate) fn dwithin_disjoint_with_parts(
        &self,
        self_parts: &DistanceParts,
        other_parts: &DistanceParts,
        distance: f64,
    ) -> Result<bool> {
        // Same disjointness guarantee as `distance_disjoint_with_parts`.
        // Boundary-inclusive. Underflow-safe via any_parts_within.
        let limit_sq = distance * distance;
        let simd = squared_space_safe(self_parts)
            && squared_space_safe(other_parts)
            && limit_sq.is_normal()
            && distance > 0.0;
        Ok(
            any_parts_within(self_parts, other_parts, distance, limit_sq, simd)?
                || any_parts_within(other_parts, self_parts, distance, limit_sq, simd)?,
        )
    }

    /// The closest pair of points, one on each geometry; `None` only when an
    /// operand is empty (the surfaces map it to `(POINT EMPTY, POINT EMPTY)`).
    pub fn nearest_points(&self, other: &Self) -> Result<Option<(Point, Point)>> {
        crate::geometry::PreparedPlan::new(self.clone())
            .nearest_points(&crate::geometry::PreparedPlan::new(other.clone()))
    }

    pub(crate) fn frechet_distance_with_scratch(
        &self,
        other: &Self,
        scratch: &mut distance::FrechetDpScratch,
    ) -> Result<f64> {
        // Empty Fréchet operands have infinite distance; wrong geometry kinds
        // still raise through single_linework.
        if self.is_empty() || other.is_empty() {
            return Ok(f64::INFINITY);
        }
        Ok(discrete_frechet_distance_with_scratch(
            self.single_linework()?,
            other.single_linework()?,
            scratch,
        ))
    }
}
