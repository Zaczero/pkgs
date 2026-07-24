use super::*;
impl Shape {
    /// Build this shape's [`DistanceParts`] (vertices, packed linework,
    /// isolated points) for reuse across a scalar-vs-array distance/dwithin
    /// sweep.
    pub(crate) fn distance_parts(&self) -> DistanceParts {
        DistanceParts {
            point_only: self.point_only_points(),
            linework: PreparedLinework::build(self),
            // Deferred: the two column scans are paid on first DISTANCE use, not
            // at build — the CONTACT path (overlaps/touches/…) never reads it.
            squared_safe: std::sync::OnceLock::new(),
            facet_bvh: std::sync::OnceLock::new(),
            point_index: std::sync::OnceLock::new(),
        }
    }

    /// Build this shape's geodesic working set for one ellipsoid. Domain
    /// validation happens before any allocation is cached by `ShapeData`.
    pub(crate) fn geodesic_parts(&self, metric: &impl GeodesicMetric) -> Result<GeodesicParts> {
        let mut points = Vec::with_capacity(self.coord_count());
        self.try_for_each_point(|point| -> Result<()> {
            crate::crs::ensure_geographic_lonlat(point.x, point.y)?;
            points.push(point);
            Ok(())
        })?;
        let mut point_only = Vec::new();
        collect_point_only_into(self, &mut point_only);
        let segment_count = self.segment_count();
        let mut segments = Vec::with_capacity(segment_count);
        let mut antimeridian_segments = Vec::new();
        let mut caps_accum = GeodesicSweepCapsAccum::new(segment_count);
        self.for_each_vertex_pair(|start, end| {
            let segment = metric.make_segment(start, end);
            if geodesic_pair_spans_antimeridian(start, end) {
                antimeridian_segments.push(segment);
            }
            caps_accum.push_segment(segment, metric);
            segments.push(segment);
        });
        let caps = caps_accum.finish(&point_only, metric);
        Ok(GeodesicParts {
            points: points.into_boxed_slice(),
            segments: segments.into_boxed_slice(),
            point_only: point_only.into_boxed_slice(),
            antimeridian_segments: antimeridian_segments.into_boxed_slice(),
            caps,
            facet_bvh: std::sync::OnceLock::new(),
        })
    }

    pub fn distance(&self, other: &Self) -> f64 {
        ShapeData::new(self.clone()).distance(&ShapeData::new(other.clone()))
    }

    /// Planar distance reusing a pre-built [`DistanceParts`] for each operand.
    /// The scalar-vs-array path builds the fixed operand's parts once and calls
    /// this per row instead of rebuilding both sides every pair. Caller is
    /// responsible for the `intersects` short-circuit (containment → 0).
    pub(crate) fn distance_disjoint_with_parts(
        &self,
        self_parts: &DistanceParts,
        other_parts: &DistanceParts,
    ) -> f64 {
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
            best = min_parts_to_parts::<true>(self_parts, other_parts, best);
            best = min_parts_to_parts::<true>(other_parts, self_parts, best);
            return best.sqrt();
        }
        let mut best = f64::INFINITY;
        best = min_parts_to_parts::<false>(self_parts, other_parts, best);
        min_parts_to_parts::<false>(other_parts, self_parts, best)
    }

    /// Minimum distance between two geometries whose edges are geodesics, in
    /// the meters returned by `metric`. Mirrors [`Self::distance`] but
    /// measures every vertex-to-edge pair ellipsoidally instead of in the
    /// plane. Crossing and containment cases short-circuit to `0` via the
    /// planar `intersects`/ `contains_point` tests (exact except for
    /// antimeridian-spanning polygon interiors). For two disjoint
    /// geometries the closest pair always lies at a vertex of one, so
    /// vertex-to-segment from both sides is sufficient.
    /// The shape's auxiliary-sphere cap under `metric`: its first vertex
    /// (the anchor) and a PROVEN upper bound (meters) on any shape point's
    /// geodesic distance from it — `max(s(anchor, start) + length)` over
    /// segments plus isolated points (`σ ≤ s/b` covers geodesic edges that
    /// bow outside their lon/lat envelope). `None` for empty shapes. The
    /// spatial index's geodesic row caps and the capped sweeps below share
    /// this bound.
    pub fn geodesic_cap(&self, metric: &impl GeodesicMetric) -> Option<(Point, f64)> {
        geodesic_cap_streaming(self, metric)
    }

    pub fn geodesic_distance(&self, other: &Self, metric: &impl GeodesicMetric) -> f64 {
        // Point pairs reduce to ONE Karney inverse (`segment_length` IS the
        // point distance; the degenerate-segment spelling would burn three).
        if let (Self::Point(a), Self::Point(b)) = (self, other) {
            return metric.segment_length(*a, *b);
        }
        if self.intersects(other) {
            return 0.0;
        }
        // The planar `intersects` above is exact except where a segment's geodesic
        // path diverges from its lon/lat path — i.e. across the antimeridian. Test
        // those spanning pairs geodesically so a true crossing reads as distance 0.
        if self.geodesic_segments_cross(other, metric) {
            return 0.0;
        }
        let mut scratch_guard = GeodesicScratchGuard::take();
        let scratch = &mut scratch_guard.scratch;
        scratch.left_points.clear();
        scratch.left_points.reserve(self.coord_count());
        self.collect_points_into(&mut scratch.left_points);
        scratch.right_points.clear();
        scratch.right_points.reserve(other.coord_count());
        other.collect_points_into(&mut scratch.right_points);
        collect_geodesic_segments_into(self, metric, &mut scratch.left_edges);
        collect_geodesic_segments_into(other, metric, &mut scratch.right_edges);
        collect_point_only_into(self, &mut scratch.left_point_only);
        collect_point_only_into(other, &mut scratch.right_point_only);
        // Cap-bounded best-first sweeps: ONE inverse per target segment
        // turns every vertex x target pair into a no-Karney lower bound
        // (auxiliary-sphere cap separation); pairs evaluate in ascending
        // bound order, so the first bound past the running best prunes the
        // sweep's whole remainder. Far pairs never pay a Karney call.
        let mut best = f64::INFINITY;
        best = geodesic_capped_sweep(
            other,
            &scratch.left_points,
            &scratch.right_edges,
            &scratch.right_point_only,
            metric,
            best,
            &mut scratch.cap_lengths,
            &mut scratch.cap_groups,
            &mut scratch.rows,
        );
        best = geodesic_capped_sweep(
            self,
            &scratch.right_points,
            &scratch.left_edges,
            &scratch.left_point_only,
            metric,
            best,
            &mut scratch.cap_lengths,
            &mut scratch.cap_groups,
            &mut scratch.rows,
        );
        best
    }

    /// Directed and symmetric geodesic Hausdorff distance (meters) — the
    /// segment-aware sibling of [`Shape::hausdorff_distance`]. Each directed
    /// sweep takes the linework max-min over source vertices AND source
    /// segment interiors (golden-section on along-track position), with
    /// target pruning via auxiliary-sphere bounds and
    /// [`GeodesicMetric::point_to_segment`].
    pub fn geodesic_hausdorff_distance(&self, other: &Self, metric: &impl GeodesicMetric) -> f64 {
        if self.is_empty() || other.is_empty() {
            return f64::INFINITY;
        }
        let mut scratch_guard = GeodesicScratchGuard::take();
        let scratch = &mut scratch_guard.scratch;
        collect_geodesic_segments_into(other, metric, &mut scratch.right_edges);
        other.collect_points_into(&mut scratch.right_points);
        collect_geodesic_segments_into(self, metric, &mut scratch.left_edges);
        self.collect_points_into(&mut scratch.left_points);
        let directed = |source: &Self,
                        source_edges: &[GeodesicSegment],
                        target_edges: &[GeodesicSegment],
                        target_points: &[Point]| {
            let mut cmax = 0.0_f64;
            source.for_each_point(|point| {
                let cmin = geodesic_min_distance_to_target(
                    point,
                    target_edges,
                    target_points,
                    metric,
                    cmax,
                );
                if cmin > cmax {
                    cmax = cmin;
                }
            });
            for &segment in source_edges {
                let value = geodesic_max_min_on_source_segment(
                    segment,
                    target_edges,
                    target_points,
                    metric,
                    cmax,
                );
                if value > cmax {
                    cmax = value;
                }
            }
            cmax
        };
        directed(
            self,
            &scratch.left_edges,
            &scratch.right_edges,
            &scratch.right_points,
        )
        .max(directed(
            other,
            &scratch.right_edges,
            &scratch.left_edges,
            &scratch.left_points,
        ))
    }

    /// The closest pair of points (one on each geometry) under the geodesic
    /// metric — the geodesic sibling of `Shape::nearest_points`. Touching
    /// geometries return a shared contact point (the planar witness, which is
    /// metric-independent at distance zero); otherwise both directed
    /// vertex-to-segment witness sweeps run, carrying the running best so the
    /// metric can prune segments that cannot improve it. `None` only when an
    /// operand is empty.
    pub fn geodesic_nearest_points(
        &self,
        other: &Self,
        metric: &impl GeodesicMetric,
    ) -> Option<(Point, Point)> {
        let mut scratch_guard = GeodesicScratchGuard::take();
        let scratch = &mut scratch_guard.scratch;
        scratch.left_points.clear();
        scratch.left_points.reserve(self.coord_count());
        self.collect_points_into(&mut scratch.left_points);
        scratch.right_points.clear();
        scratch.right_points.reserve(other.coord_count());
        other.collect_points_into(&mut scratch.right_points);
        if scratch.left_points.is_empty() || scratch.right_points.is_empty() {
            return None;
        }
        // A planar intersection is a true contact at distance zero, and the
        // planar witness IS that shared point. Geodesic-only antimeridian
        // crossings are deliberately NOT shortcut here: the planar witness would
        // sit at the wrong location (the operands are planar-disjoint), so the
        // directed witness sweeps below return the correct near-contact pair.
        if self.intersects(other) {
            return self.nearest_points(other);
        }
        collect_geodesic_segments_into(self, metric, &mut scratch.left_edges);
        collect_geodesic_segments_into(other, metric, &mut scratch.right_edges);
        collect_point_only_into(self, &mut scratch.left_point_only);
        collect_point_only_into(other, &mut scratch.right_point_only);
        let reverse_order_offset = scratch.left_points.len()
            * (scratch.right_edges.len() + scratch.right_point_only.len());
        let best = geodesic_capped_witness_sweep(
            &scratch.left_points,
            &scratch.right_edges,
            &scratch.right_point_only,
            metric,
            None,
            0,
            false,
            &mut scratch.cap_lengths,
            &mut scratch.cap_groups,
            &mut scratch.rows,
        );
        let best = geodesic_capped_witness_sweep(
            &scratch.right_points,
            &scratch.left_edges,
            &scratch.left_point_only,
            metric,
            best,
            reverse_order_offset,
            true,
            &mut scratch.cap_lengths,
            &mut scratch.cap_groups,
            &mut scratch.rows,
        );
        best.map(|candidate| {
            if candidate.swapped {
                (candidate.target, candidate.probe)
            } else {
                (candidate.probe, candidate.target)
            }
        })
    }

    /// Whether any antimeridian-spanning segment of one geometry geodesically
    /// crosses a segment of the other. Only spanning pairs (`|Δlon| > 180`) are
    /// tested — every other crossing is already caught by the planar
    /// `intersects` shortcut, so this stays cheap for ordinary geometries.
    fn geodesic_segments_cross(&self, other: &Self, metric: &impl GeodesicMetric) -> bool {
        geodesic_segments_cross_streaming(self, other, metric)
    }

    pub fn dwithin(&self, other: &Self, distance: f64) -> bool {
        ShapeData::new(self.clone()).dwithin(&ShapeData::new(other.clone()), distance)
    }

    /// `dwithin` reusing a pre-built [`DistanceParts`] per operand. Caller owns
    /// the `intersects`/bounds short-circuits; `limit` is the squared distance.
    pub(crate) fn dwithin_disjoint_with_parts(
        &self,
        self_parts: &DistanceParts,
        other_parts: &DistanceParts,
        limit: f64,
    ) -> bool {
        // Same disjointness guarantee as `distance_disjoint_with_parts`, so the two
        // vertex sweeps are complete here too (`min ≤ limit` ⟺ some vertex
        // projection `≤ limit`). Every phase is boundary-INCLUSIVE
        // (`distance² <= limit`), matching the dwithin contract. The SIMD
        // point kernel has no extreme-coordinate rescue, so it is gated on
        // squared-space-safe operands (the scalar kernels rescue internally).
        let simd = squared_space_safe(self_parts) && squared_space_safe(other_parts);
        any_parts_within(self_parts, other_parts, limit, simd)
            || any_parts_within(other_parts, self_parts, limit, simd)
    }

    /// The closest pair of points, one on each geometry; `None` only when an
    /// operand is empty (the surfaces map it to `(POINT EMPTY, POINT EMPTY)`).
    pub fn nearest_points(&self, other: &Self) -> Option<(Point, Point)> {
        nearest_points(self, other)
    }

    /// The shortest connecting line between two geometries — `nearest_points`
    /// as a `LineString` (degenerate when the geometries touch), or
    /// `LINESTRING EMPTY` when an operand is empty.
    pub fn shortest_line(&self, other: &Self) -> Self {
        crate::geometry::nearest_line(
            self.nearest_points(other),
            crate::geometry::common_axes(self, other),
        )
    }

    pub fn hausdorff_distance(&self, other: &Self) -> f64 {
        if self.is_empty() || other.is_empty() {
            return f64::INFINITY;
        }
        directed_hausdorff_distance_squared_shapes(self, other)
            .max(directed_hausdorff_distance_squared_shapes(other, self))
            .sqrt()
    }

    pub fn frechet_distance(&self, other: &Self) -> Result<f64> {
        Ok(discrete_frechet_distance(
            self.single_linework()?,
            other.single_linework()?,
        ))
    }
}
