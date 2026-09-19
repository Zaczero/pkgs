#[cfg(test)]
use crate::geometry::ShapeData;
use crate::geometry::distance::{
    GeodesicSweepCapsAccum, Result, any_parts_within, collect_point_only_into,
    discrete_frechet_distance_with_scratch, ensure_geodesic_linework, geodesic_cap_streaming,
    geodesic_directed_hausdorff_distance, geodesic_scratch, min_parts_to_parts, squared_space_safe,
};
use crate::geometry::{
    DistanceParts, GeodesicMetric, GeodesicParts, Point, PreparedLinework, Shape, distance,
    finish_planar_squared_min, nearest_points,
};
impl Shape {
    /// Build this shape's [`DistanceParts`] (vertices, packed linework,
    /// isolated points) for reuse across a scalar-vs-array distance/dwithin
    /// sweep.
    pub(crate) fn distance_parts(&self) -> Result<DistanceParts> {
        Ok(DistanceParts {
            point_only: self.point_only_points()?,
            linework: PreparedLinework::build(self)?,
            // Deferred: the two column scans are paid on first DISTANCE use, not
            // at build — the CONTACT path (overlaps/touches/…) never reads it.
            squared_safe: std::sync::OnceLock::new(),
            facet_bvh: std::sync::OnceLock::new(),
            point_index: std::sync::OnceLock::new(),
        })
    }

    /// Build this shape's geodesic working set for one ellipsoid. Domain
    /// validation happens before any allocation is cached by `ShapeData`.
    pub(crate) fn geodesic_parts(&self, metric: &impl GeodesicMetric) -> Result<GeodesicParts> {
        ensure_geodesic_linework(self, metric)?;
        let mut points = Vec::new();
        crate::boundary::try_reserve_vec(&mut points, self.coord_count())?;
        self.collect_points_into(&mut points)?;
        let mut point_only = Vec::new();
        collect_point_only_into(self, &mut point_only)?;
        let segment_count = self.segment_count();
        crate::boundary::try_reserve_vec(&mut point_only, segment_count)?;
        let mut segments = Vec::new();
        crate::boundary::try_reserve_vec(&mut segments, segment_count)?;
        let mut caps_accum = GeodesicSweepCapsAccum::try_new(segment_count)?;
        self.try_for_each_vertex_pair(|start, end| {
            let segment = metric.make_segment(start, end)?;
            if segment.length == 0.0 {
                if points.last().is_some_and(|point| *point == end) {
                    points.pop();
                }
                crate::try_push_vec(&mut point_only, start)?;
                return Ok::<_, crate::error::Error>(());
            }
            caps_accum.push_segment(&segment, metric);
            segments.push(segment);
            Ok::<_, crate::error::Error>(())
        })?;
        let caps = caps_accum.finish(&point_only, metric);
        Ok(GeodesicParts {
            points,
            segments,
            point_only,
            caps,
            facet_bvh: std::sync::OnceLock::new(),
        })
    }

    #[cfg(test)]
    pub fn distance(&self, other: &Self) -> Result<f64> {
        crate::geometry::PreparedPlan::new(ShapeData::new(self.clone()))?.distance(
            &crate::geometry::PreparedPlan::new(ShapeData::new(other.clone()))?,
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

    /// Build the shape's auxiliary-sphere cap under `metric`: its first
    /// vertex (the anchor) and a conservative upper bound (meters) on any
    /// shape point's geodesic distance from it — `max(s(anchor, start) +
    /// length)` over segments plus isolated points (`σ ≤ s/b` covers
    /// geodesic edges that bow outside their lon/lat envelope). `None` for
    /// empty shapes. The spatial index's geodesic row caps and capped
    /// sweeps share this bound.
    pub(crate) fn geodesic_cap(
        &self,
        metric: &impl GeodesicMetric,
    ) -> Result<Option<(Point, f64)>> {
        geodesic_cap_streaming(self, metric)
    }

    /// Directed and symmetric geodesic Hausdorff distance (meters) — the
    /// segment-aware sibling of [`Shape::hausdorff_distance`]. Each directed
    /// sweep evaluates source vertices and a global along-track frontier over
    /// source segment interiors. Complete target witnesses provide the
    /// returned values, while conservative interval supports close regular,
    /// near-coincident, and exact-family cells.
    pub(crate) fn geodesic_hausdorff_distance(
        &self,
        other: &Self,
        metric: &impl GeodesicMetric,
    ) -> Result<f64> {
        ensure_geodesic_linework(self, metric)?;
        ensure_geodesic_linework(other, metric)?;
        if self.is_empty() || other.is_empty() {
            return Ok(f64::INFINITY);
        }
        let source_parts = self.geodesic_parts(metric)?;
        let target_parts = other.geodesic_parts(metric)?;
        let mut scratch = geodesic_scratch();
        let scratch = &mut *scratch;
        let forward =
            geodesic_directed_hausdorff_distance(&source_parts, &target_parts, metric, scratch)?;
        let reverse =
            geodesic_directed_hausdorff_distance(&target_parts, &source_parts, metric, scratch)?;
        Ok(forward.distance.max(reverse.distance))
    }

    #[cfg(test)]
    pub fn dwithin(&self, other: &Self, distance: f64) -> Result<bool> {
        crate::geometry::PreparedPlan::new(ShapeData::new(self.clone()))?.dwithin(
            &crate::geometry::PreparedPlan::new(ShapeData::new(other.clone()))?,
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
        nearest_points(self, other)
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
