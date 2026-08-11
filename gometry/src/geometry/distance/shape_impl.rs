use crate::geometry::distance::{
    GeodesicScratchGuard, GeodesicSweepCapsAccum, HausdorffFeature, HausdorffTarget, Result,
    any_parts_within, collect_geodesic_segments_into, collect_point_only_into,
    compact_hausdorff_params, directed_hausdorff_distance_squared_shapes,
    discrete_frechet_distance, geodesic_cap_streaming, geodesic_capped_sweep,
    geodesic_capped_witness_sweep, geodesic_max_min_on_source_segment,
    geodesic_min_distance_to_target, geodesic_pair_spans_antimeridian,
    geodesic_segments_cross_streaming, min_parts_to_parts, push_equidistant_roots_on_interval,
    push_point_on_line_breakpoint, push_segment_projection_breakpoints, squared_space_safe,
};
use crate::geometry::frame::SimilarityFrame;
use crate::geometry::{
    Coordinates, DistanceParts, GeodesicMetric, GeodesicParts, GeodesicSegment, Orientation, Point,
    Polygon, PreparedLinework, Segment, Shape, ShapeData, XY, axis_pow2_scale,
    finish_planar_squared_min, nearest_points, orientation, point_distance, point_segment_distance,
    power_of_two_exponent, same_point, scale_by_power_of_two, scaled_residual, segment_projection,
};
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
            return finish_planar_squared_min(best, || {
                let mut d = f64::INFINITY;
                d = min_parts_to_parts::<false>(self_parts, other_parts, d);
                min_parts_to_parts::<false>(other_parts, self_parts, d)
            });
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
    #[expect(
        clippy::impl_trait_in_params,
        reason = "the metric is used only through its trait contract; a named generic adds no API signal"
    )]
    pub fn geodesic_cap(&self, metric: &impl GeodesicMetric) -> Option<(Point, f64)> {
        geodesic_cap_streaming(self, metric)
    }

    #[expect(
        clippy::impl_trait_in_params,
        reason = "the metric is used only through its trait contract; a named generic adds no API signal"
    )]
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
    #[expect(
        clippy::impl_trait_in_params,
        reason = "the metric is used only through its trait contract; a named generic adds no API signal"
    )]
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
    #[expect(
        clippy::impl_trait_in_params,
        reason = "the metric is used only through its trait contract; a named generic adds no API signal"
    )]
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
    /// the `intersects`/bounds short-circuits; `distance` is the real radius.
    pub(crate) fn dwithin_disjoint_with_parts(
        &self,
        self_parts: &DistanceParts,
        other_parts: &DistanceParts,
        distance: f64,
    ) -> bool {
        // Same disjointness guarantee as `distance_disjoint_with_parts`.
        // Boundary-inclusive. Underflow-safe via any_parts_within.
        let limit_sq = distance * distance;
        let simd = squared_space_safe(self_parts)
            && squared_space_safe(other_parts)
            && limit_sq.is_finite()
            && limit_sq != 0.0
            && distance > 0.0;
        any_parts_within(self_parts, other_parts, distance, limit_sq, simd)
            || any_parts_within(other_parts, self_parts, distance, limit_sq, simd)
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
        if let Some(part) = singleton_support(self) {
            return part.hausdorff_distance(other);
        }
        if let Some(part) = singleton_support(other) {
            return self.hausdorff_distance(&part);
        }
        if self.is_empty() || other.is_empty() {
            return f64::INFINITY;
        }
        // A point pair is already the complete continuous Hausdorff problem.
        // Resolve it in source metric space before the squared kernel: a
        // subnormal squared separation underflows to zero, but the stored
        // separation itself remains an exact finite distance.
        if let (Self::Point(left), Self::Point(right)) = (self, other) {
            return point_distance(left.xy(), right.xy());
        }
        // A point-only carrier has no continuous interior.  Resolve the
        // finite directed nearest-neighbour maxima directly with the one
        // overflow-safe distance kernel, rather than sending MultiPoint or a
        // point-only collection through squared space.  This is exact for
        // arbitrary finite point sets, not merely translations.
        if let Some(distance) = point_only_hausdorff_distance(self, other) {
            return distance;
        }
        // A singleton target is another complete source-metric reduction for
        // every public shape kind.  Along every linear edge the distance to a
        // fixed point is convex, so its maximum occurs at a stored vertex;
        // the reverse directed distance is bounded by that same maximum.
        // This covers reciprocal-scale lines, polygons, and collections
        // without letting the squared continuous kernel manufacture `inf`.
        if let Self::Point(point) = other {
            return maximum_distance_to_point(self, point.xy());
        }
        if let Self::Point(point) = self {
            return maximum_distance_to_point(other, point.xy());
        }
        // A segment against points on one parallel axis has a complete
        // source-metric reduction: endpoint gaps are taken whole, interior
        // point gaps are halved, and the common perpendicular offset combines
        // with that largest along-axis radius.  Work from stored coordinates
        // directly so the cold similarity finisher cannot double-round the
        // Voronoi switch reconstructed from a normalized parameter.
        if let Some(distance) = hausdorff_axis_segment_parallel_points(self, other) {
            return distance;
        }
        // A straight baseline and a line sharing its stored endpoints have a
        // second complete reduction. Orthogonal projection of the continuous
        // line visits every point of the baseline, so the bidirectional
        // Hausdorff maximum is exactly the largest line-vertex distance to
        // that baseline. This keeps a reciprocal-scale bend in source metric
        // space instead of erasing its live minor ordinate in a similarity.
        if let Some(distance) = hausdorff_shared_baseline(self, other) {
            return distance;
        }
        // Every public shape kind reaches this one exact translation
        // finisher. It is deliberately before the squared continuous kernel:
        // a finite stored offset can have an infinite square, and Multi*/area
        // carriers must not fall through merely because they are not a line.
        if let Some(translation) = matching_shape_translation(self, other) {
            return point_distance(XY::new(0.0, 0.0), translation);
        }
        // This complete convex reduction belongs before the squared-space
        // fast path: a reciprocal-axis polyline can leave a *normal* but
        // unrelated squared witness there.  It is exact in source distance
        // space and therefore cannot be pre-empted by that witness.
        if let (Some(left), Some(right)) = (linework_as_segment(self), linework_as_segment(other)) {
            return hausdorff_between_segments(left, right);
        }
        if let Some(distance) = hausdorff_segment_graph(self, other) {
            return distance;
        }
        if let Some(distance) = hausdorff_segment_graph(other, self) {
            return distance;
        }
        let source_distance = robust_continuous_hausdorff(self, other);
        let framed_distance = hausdorff_continuous_framed(self, other);
        if framed_distance.is_finite()
            && framed_distance
                .to_bits()
                .abs_diff(source_distance.to_bits())
                <= 1
        {
            framed_distance
        } else {
            source_distance
        }
    }

    pub fn frechet_distance(&self, other: &Self) -> Result<f64> {
        // An empty operand is TOTAL, not an error — the same output-type
        // sentinel `distance` and `hausdorff_distance` already return, so one
        // empty row never aborts a whole batch. Without this gate
        // `single_linework()` raises `EmptyLinework`, which made the scalar
        // form disagree with both siblings and the array form abort on the
        // exact condition the degrade policy names.
        //
        // Emptiness only: a wrong KIND still raises on both paths, per the
        // deliberate wrong-kind-vs-bad-data split.
        if self.is_empty() || other.is_empty() {
            return Ok(f64::INFINITY);
        }
        Ok(discrete_frechet_distance(
            self.single_linework()?,
            other.single_linework()?,
        ))
    }
}

/// A one-member collection or multipart line has exactly the member's
/// support.  Unwrap this structural wrapper before continuous distance
/// kernels so a shared huge origin reaches the finite-delta reductions.
fn singleton_support(shape: &Shape) -> Option<Shape> {
    match shape {
        Shape::GeometryCollection(parts) if parts.len() == 1 => parts.first().cloned(),
        Shape::MultiLineString(parts) if parts.len() == 1 => {
            parts.first().cloned().map(Shape::LineString)
        },
        _ => None,
    }
}

/// Continuous Hausdorff in a cold origin-shifted power-of-two frame, then
/// unscale the distance. The continuous breakpoint kernel runs unchanged on
/// the framed shapes.
fn hausdorff_continuous_framed(left: &Shape, right: &Shape) -> f64 {
    // Equal-vertex line strings separated by one stored translation have an
    // analytic Hausdorff distance: the translation norm.  Detect it before
    // framing so a large shared ordinate cannot make the small translated
    // axis look like one local unit (the packed four-vertex exit shares this
    // shape finisher).
    if let Some(translation) = matching_line_translation(left, right) {
        return point_distance(XY::new(0.0, 0.0), translation);
    }
    // A pair of individual segments is a complete convex case: the maximum
    // distance from one segment to the other occurs at an endpoint.  Resolve
    // it in world distance space before any coordinate conditioning.  This
    // keeps both very long parallel segments and 1e-300 separations exact.
    if let (Some(left_segment), Some(right_segment)) =
        (linework_as_segment(left), linework_as_segment(right))
    {
        return hausdorff_between_segments(left_segment, right_segment);
    }
    // Likewise, the maximum nearest-endpoint distance of a segment whose two
    // endpoints are the target is its true midpoint radius. This is a closed
    // analytic case, not a rounded midpoint probe used as a negative oracle.
    if let Some(segment) = single_line_segment(left)
        && is_its_endpoint_pair(right, segment)
    {
        return point_distance(segment.start, segment.end) / 2.0;
    }
    if let Some(segment) = single_line_segment(right)
        && is_its_endpoint_pair(left, segment)
    {
        return point_distance(segment.start, segment.end) / 2.0;
    }
    // A uniform power-of-two similarity preserves the Euclidean diagram
    // exactly while making its squared arithmetic finite.  It covers every
    // finite extreme whose live coordinate ranges fit one metric scale (the
    // normal huge-coordinate case); the reciprocal-axis cases above keep
    // their source-metric analytic reductions because no similarity can keep
    // both live axes representable.
    similarity_framed_hausdorff(left, right)
        .unwrap_or_else(|| robust_continuous_hausdorff(left, right))
}

/// Retry the complete continuous kernel in one origin-shifted power-of-two
/// *similarity* frame.  Unlike the shared anisotropic frame, this is an exact
/// coordinate change for Euclidean distance: a finite framed result may be
/// unscaled without choosing an axis or changing a Voronoi/Hausdorff witness.
fn similarity_framed_hausdorff(left: &Shape, right: &Shape) -> Option<f64> {
    let mut points = Vec::with_capacity(left.coord_count().checked_add(right.coord_count())?);
    left.for_each_point(|point| points.push(point));
    right.for_each_point(|point| points.push(point));
    let frame = SimilarityFrame::from_points(&points)?;
    let map = |point: Point| {
        Ok(point.with_xy_unchecked(
            frame.frame_xy(point.x, point.y).x,
            frame.frame_xy(point.x, point.y).y,
        ))
    };
    let framed_left = left.map_points(&map).ok()?;
    let framed_right = right.map_points(&map).ok()?;
    let answer2 = directed_hausdorff_distance_squared_shapes(&framed_left, &framed_right).max(
        directed_hausdorff_distance_squared_shapes(&framed_right, &framed_left),
    );
    answer2
        .is_normal()
        .then(|| scale_by_power_of_two(answer2.sqrt(), -power_of_two_exponent(frame.scale())))
}

/// Continuous Hausdorff finisher in source Euclidean distance space.
///
/// The ordinary kernel discovers the complete breakpoint set, but its squared
/// distance evaluation can overflow or underflow. Re-evaluate those same
/// witnesses with the distance family's guarded hypot kernels, so finite
/// coordinates have a finite result without changing the metric by an
/// anisotropic coordinate transform.
fn robust_continuous_hausdorff(left: &Shape, right: &Shape) -> f64 {
    robust_directed_hausdorff(left, right).max(robust_directed_hausdorff(right, left))
}

pub(super) fn robust_directed_hausdorff(source: &Shape, target: &Shape) -> f64 {
    let target = HausdorffTarget::build(target);
    let distance = |point: XY| {
        target
            .features
            .iter()
            .map(|feature| match *feature {
                HausdorffFeature::Point(other) => point_distance(point, other),
                HausdorffFeature::Segment(segment) => robust_point_segment_distance(point, segment),
            })
            .fold(f64::INFINITY, f64::min)
    };
    let mut best = 0.0_f64;
    source.for_each_point(|point| best = best.max(distance(point.xy())));
    source.for_each_segment(|segment| {
        let mut params = robust_hausdorff_segment_params(segment, &target.features);
        let count = compact_hausdorff_params(&mut params);
        for &t in &params[..count] {
            best = best.max(distance(robust_segment_point(segment, t)));
        }
    });
    best
}

pub(super) fn robust_point_segment_distance(point: XY, segment: Segment) -> f64 {
    let projection = segment_projection(point, segment);
    if projection.is_start() {
        return point_distance(point, segment.start);
    }
    if projection.is_end() {
        return point_distance(point, segment.end);
    }
    if segment.start.y.to_bits() == segment.end.y.to_bits() {
        return (point.y - segment.start.y).abs();
    }
    if segment.start.x.to_bits() == segment.end.x.to_bits() {
        return (point.x - segment.start.x).abs();
    }
    let max_delta = (segment.end.x - segment.start.x)
        .abs()
        .max((segment.end.y - segment.start.y).abs())
        .max((point.x - segment.start.x).abs())
        .max((point.y - segment.start.y).abs());
    let scale = axis_pow2_scale(max_delta);
    if scale == 0.0 {
        return 0.0;
    }
    let local_end = XY::new(
        scaled_residual(segment.end.x, segment.start.x, scale),
        scaled_residual(segment.end.y, segment.start.y, scale),
    );
    let local_point = XY::new(
        scaled_residual(point.x, segment.start.x, scale),
        scaled_residual(point.y, segment.start.y, scale),
    );
    let t1 = local_end.x * local_point.y;
    let t2 = local_end.y * local_point.x;
    let determinant = t1 - t2;
    // The adaptive predicate's arithmetic model excludes range errors. Use
    // its accurate determinant magnitude only when both products and their
    // difference are normal; underflow, overflow, and cancellation outside
    // that model decline to the independently scaled distance owner.
    if t1.is_normal() && t2.is_normal() && determinant.is_normal() {
        let local_area = robust::orient2d(
            robust::Coord { x: 0.0, y: 0.0 },
            robust::Coord {
                x: local_end.x,
                y: local_end.y,
            },
            robust::Coord {
                x: local_point.x,
                y: local_point.y,
            },
        )
        .abs();
        let local_distance = local_area / local_end.x.hypot(local_end.y);
        return scale_by_power_of_two(local_distance, -power_of_two_exponent(scale));
    }
    point_segment_distance(point, segment)
}

fn robust_hausdorff_segment_params(source: Segment, features: &[HausdorffFeature]) -> Vec<f64> {
    let mut params = vec![0.0, 1.0];
    for &feature in features {
        let (source, feature) = normalized_source_and_features(source, feature, feature);
        match feature {
            HausdorffFeature::Point(point) => push_point_on_line_breakpoint(
                source.start.x,
                source.start.y,
                source.end.x - source.start.x,
                source.end.y - source.start.y,
                point,
                &mut params,
            ),
            HausdorffFeature::Segment(segment) => push_segment_projection_breakpoints(
                source.start.x,
                source.start.y,
                source.end.x - source.start.x,
                source.end.y - source.start.y,
                segment,
                &mut params,
            ),
        }
    }
    let base_count = compact_hausdorff_params(&mut params);
    let intervals = params[..base_count].to_vec();
    let mut roots = Vec::new();
    for &[ta, tb] in intervals.array_windows::<2>() {
        for (left_index, &left) in features.iter().enumerate() {
            for &right in &features[(left_index + 1)..] {
                let (normalized_source, normalized_left) =
                    normalized_source_and_features(source, left, right);
                let (_, normalized_right) = normalized_source_and_features(source, right, left);
                push_equidistant_roots_on_interval(
                    normalized_source.start.x,
                    normalized_source.start.y,
                    normalized_source.end.x - normalized_source.start.x,
                    normalized_source.end.y - normalized_source.start.y,
                    ta,
                    tb,
                    normalized_left,
                    normalized_right,
                    &mut roots,
                );
            }
        }
    }
    params.truncate(base_count);
    params.extend(roots);
    params
}

fn normalized_source_and_features(
    source: Segment,
    first: HausdorffFeature,
    second: HausdorffFeature,
) -> (Segment, HausdorffFeature) {
    let mut max_abs = source
        .start
        .x
        .abs()
        .max(source.start.y.abs())
        .max(source.end.x.abs())
        .max(source.end.y.abs());
    for feature in [first, second] {
        match feature {
            HausdorffFeature::Point(point) => {
                max_abs = max_abs.max(point.x.abs()).max(point.y.abs());
            },
            HausdorffFeature::Segment(segment) => {
                max_abs = max_abs
                    .max(segment.start.x.abs())
                    .max(segment.start.y.abs())
                    .max(segment.end.x.abs())
                    .max(segment.end.y.abs());
            },
        }
    }
    let scale = axis_pow2_scale(max_abs);
    let point = |point: XY| {
        XY::new(
            scaled_residual(point.x, source.start.x, scale),
            scaled_residual(point.y, source.start.y, scale),
        )
    };
    let normalized_source = Segment {
        start: XY::new(0.0, 0.0),
        end: point(source.end),
    };
    let normalized_first = match first {
        HausdorffFeature::Point(target) => HausdorffFeature::Point(point(target)),
        HausdorffFeature::Segment(segment) => HausdorffFeature::Segment(Segment {
            start: point(segment.start),
            end: point(segment.end),
        }),
    };
    (normalized_source, normalized_first)
}

fn robust_segment_point(segment: Segment, t: f64) -> XY {
    let interpolate = |start: f64, end: f64| {
        if t == 0.0 {
            start
        } else if t.to_bits() == 1.0_f64.to_bits() {
            end
        } else if t.to_bits() == 0.5_f64.to_bits() {
            f64::midpoint(start, end)
        } else {
            t.mul_add(end, (1.0 - t) * start)
        }
    };
    XY::new(
        interpolate(segment.start.x, segment.end.x),
        interpolate(segment.start.y, segment.end.y),
    )
}

fn hausdorff_between_segments(left: Segment, right: Segment) -> f64 {
    point_segment_distance(left.start, right)
        .max(point_segment_distance(left.end, right))
        .max(point_segment_distance(right.start, left))
        .max(point_segment_distance(right.end, left))
}

/// Exact Hausdorff reduction for a line that is a single-valued graph over a
/// complete baseline segment. Every baseline point has a perpendicular graph
/// witness, while distance to the baseline is convex on each graph edge, so
/// the symmetric maximum occurs at a stored graph vertex.
fn hausdorff_segment_graph(baseline: &Shape, graph: &Shape) -> Option<f64> {
    let baseline = linework_as_segment(baseline)?;
    let Shape::LineString(graph) = graph else {
        return None;
    };
    let mut projections = graph
        .iter()
        .map(|point| segment_projection(point.xy(), baseline));
    let first = projections.next()?;
    if !first.is_start() {
        return None;
    }
    let mut previous = first.fraction();
    let mut ended = false;
    for projection in projections {
        // This is only a shortcut certificate. A parameter whose interval
        // declined must retain its exact ratio for ordering; the general
        // Hausdorff kernel is the simpler owner of that uncommon case.
        if projection.uses_exact_ratio() {
            return None;
        }
        let fraction = projection.fraction();
        if fraction < previous {
            return None;
        }
        previous = fraction;
        ended = projection.is_end();
    }
    if !ended {
        return None;
    }
    Some(
        graph
            .iter()
            .map(|point| robust_point_segment_distance(point.xy(), baseline))
            .fold(0.0_f64, f64::max),
    )
}

/// A LineString whose every stored vertex lies on its endpoint segment.
///
/// Continuous distance-to-a-convex-segment is convex, hence a polyline that
/// only subdivides one segment has the same Hausdorff extrema as that one
/// segment.  This is a source-metric reduction, not a framed approximation:
/// the robust orientation predicate decides collinearity from the original
/// stored doubles. Collinearity alone is deliberately insufficient: a
/// backtracking line can visit a point beyond either endpoint and has a
/// different continuous Hausdorff extremum.
fn linework_as_segment(shape: &Shape) -> Option<Segment> {
    let Shape::LineString(line) = shape else {
        return single_line_segment(shape);
    };
    let start = line.nth_coord(0).xy();
    let end = line.nth_coord(line.coord_count().checked_sub(1)?).xy();
    if same_point(start, end)
        || !line.iter().all(|point| {
            let point = point.xy();
            orientation(start, end, point) == Orientation::Collinear
                && point.x >= start.x.min(end.x)
                && point.x <= start.x.max(end.x)
                && point.y >= start.y.min(end.y)
                && point.y <= start.y.max(end.y)
        })
    {
        return None;
    }
    Some(Segment { start, end })
}

fn single_line_segment(shape: &Shape) -> Option<Segment> {
    let Shape::LineString(line) = shape else {
        return None;
    };
    (line.len() == 2).then(|| Segment {
        start: line.point_at(0).xy(),
        end: line.point_at(1).xy(),
    })
}

fn matching_line_translation(left: &Shape, right: &Shape) -> Option<XY> {
    let (Shape::LineString(left), Shape::LineString(right)) = (left, right) else {
        return None;
    };
    if left.is_empty() || left.len() != right.len() {
        return None;
    }
    let translation = XY::new(right.xs()[0] - left.xs()[0], right.ys()[0] - left.ys()[0]);
    (translation.x.is_finite()
        && translation.y.is_finite()
        && left
            .xs()
            .iter()
            .zip(left.ys())
            .zip(right.xs().iter().zip(right.ys()))
            .all(|((&left_x, &left_y), (&right_x, &right_y))| {
                (right_x - left_x).to_bits() == translation.x.to_bits()
                    && (right_y - left_y).to_bits() == translation.y.to_bits()
            }))
    .then_some(translation)
}

/// One stored translation shared by recursively identical shape structure.
///
/// Equal kind keeps this an actual translation of one geometry, not a loose
/// coordinate coincidence between different topological carriers. Every
/// subtraction is checked at the stored-double boundary; `point_distance`
/// then performs the overflow-safe Euclidean norm in source metric space.
#[expect(
    clippy::too_many_lines,
    reason = "the recursive matcher keeps every geometry kind and its shared translation state together"
)]
fn matching_shape_translation(left: &Shape, right: &Shape) -> Option<XY> {
    fn points_match(left: Point, right: Point, translation: &mut Option<XY>) -> bool {
        let delta = XY::new(right.x - left.x, right.y - left.y);
        if !delta.x.is_finite() || !delta.y.is_finite() {
            return false;
        }
        (*translation).map_or_else(
            || {
                *translation = Some(delta);
                true
            },
            |expected| {
                delta.x.to_bits() == expected.x.to_bits()
                    && delta.y.to_bits() == expected.y.to_bits()
            },
        )
    }

    fn sequences_match(
        left: &impl Coordinates,
        right: &impl Coordinates,
        translation: &mut Option<XY>,
    ) -> bool {
        left.coord_count() == right.coord_count()
            && (0..left.coord_count()).all(|index| {
                points_match(left.nth_coord(index), right.nth_coord(index), translation)
            })
    }

    fn line_sequences_match(
        left: &impl Coordinates,
        right: &impl Coordinates,
        translation: &mut Option<XY>,
    ) -> bool {
        let mut left_index = 0;
        let mut right_index = 0;
        while left_index < left.coord_count() && right_index < right.coord_count() {
            let left_point = left.nth_coord(left_index);
            let right_point = right.nth_coord(right_index);
            if !points_match(left_point, right_point, translation) {
                return false;
            }
            left_index += 1;
            while left_index < left.coord_count()
                && left.nth_coord(left_index).xy() == left_point.xy()
            {
                left_index += 1;
            }
            right_index += 1;
            while right_index < right.coord_count()
                && right.nth_coord(right_index).xy() == right_point.xy()
            {
                right_index += 1;
            }
        }
        left_index == left.coord_count() && right_index == right.coord_count()
    }

    fn polygons_match(left: &Polygon, right: &Polygon, translation: &mut Option<XY>) -> bool {
        left.holes.len() == right.holes.len()
            && sequences_match(left.shell.coords(), right.shell.coords(), translation)
            && left
                .holes
                .iter()
                .zip(right.holes.iter())
                .all(|(left, right)| sequences_match(left.coords(), right.coords(), translation))
    }

    fn shapes_match(left: &Shape, right: &Shape, translation: &mut Option<XY>) -> bool {
        match (left, right) {
            (Shape::Point(left), Shape::Point(right)) => points_match(*left, *right, translation),
            (Shape::MultiPoint(left), Shape::MultiPoint(right)) => {
                sequences_match(left, right, translation)
            },
            (Shape::LineString(left), Shape::LineString(right)) => {
                line_sequences_match(left, right, translation)
            },
            (Shape::MultiLineString(left), Shape::MultiLineString(right)) => {
                left.len() == right.len()
                    && left
                        .iter()
                        .zip(right)
                        .all(|(left, right)| line_sequences_match(left, right, translation))
            },
            (Shape::Polygon(left), Shape::Polygon(right)) => {
                polygons_match(left, right, translation)
            },
            (Shape::MultiPolygon(left), Shape::MultiPolygon(right)) => {
                left.len() == right.len()
                    && left
                        .iter()
                        .zip(right)
                        .all(|(left, right)| polygons_match(left, right, translation))
            },
            (Shape::GeometryCollection(left), Shape::GeometryCollection(right)) => {
                left.len() == right.len()
                    && left
                        .iter()
                        .zip(right)
                        .all(|(left, right)| shapes_match(left, right, translation))
            },
            (Shape::Empty(left_kind, left_axes), Shape::Empty(right_kind, right_axes)) => {
                left_kind == right_kind && left_axes == right_axes
            },
            _ => false,
        }
    }

    let mut translation = None;
    shapes_match(left, right, &mut translation)
        .then_some(translation)
        .flatten()
}

/// Exact Hausdorff distance for shapes whose whole support is finite points.
///
/// Unlike a vertex rescue for linework, this is the complete problem: there is
/// no segment interior that could contain a larger nearest-target distance.
/// `point_distance` deliberately owns overflow/underflow recovery, so a
/// finite 1e159-scale MultiPoint result cannot become infinity by squaring.
fn point_only_hausdorff_distance(left: &Shape, right: &Shape) -> Option<f64> {
    if left.segment_count() != 0 || right.segment_count() != 0 {
        return None;
    }
    let left_points = left.point_only_points();
    let right_points = right.point_only_points();
    if left_points.is_empty() || right_points.is_empty() {
        return None;
    }
    let directed = |source: &[Point], target: &[Point]| {
        source
            .iter()
            .map(|point| {
                target
                    .iter()
                    .map(|other| point_distance(point.xy(), other.xy()))
                    .fold(f64::INFINITY, f64::min)
            })
            .fold(0.0, f64::max)
    };
    Some(directed(&left_points, &right_points).max(directed(&right_points, &left_points)))
}

/// Exact continuous Hausdorff distance where one support is a singleton.
///
/// Squared distance to a fixed point is convex on each linear edge, therefore
/// its maximum is attained at an endpoint.  The opposite directed distance is
/// no larger because every source vertex is a candidate point of the source.
fn maximum_distance_to_point(shape: &Shape, target: XY) -> f64 {
    let mut maximum = 0.0_f64;
    shape.for_each_point(|point| {
        maximum = maximum.max(point_distance(point.xy(), target));
    });
    maximum
}

/// Exact Hausdorff support reduction for an axis-aligned segment and a
/// non-empty point row sharing one perpendicular coordinate.
///
/// One-member collections are the same support as their member and therefore
/// enter the same reduction. Along the segment axis, nearest-point ownership
/// switches at the midpoint of each adjacent stored point pair. The maximum
/// is consequently one of the two endpoint gaps or half an interior gap.
fn hausdorff_axis_segment_parallel_points(left: &Shape, right: &Shape) -> Option<f64> {
    const fn support(shape: &Shape) -> &Shape {
        let mut shape = shape;
        while let Shape::GeometryCollection(members) = shape
            && let [member] = members.as_slice()
        {
            shape = member;
        }
        shape
    }

    let left = support(left);
    let right = support(right);
    let (segment, points) = match (single_line_segment(left), right) {
        (Some(segment), Shape::MultiPoint(points)) if !points.is_empty() => (segment, points),
        _ => match (single_line_segment(right), left) {
            (Some(segment), Shape::MultiPoint(points)) if !points.is_empty() => (segment, points),
            _ => return None,
        },
    };
    let horizontal = segment.start.y.to_bits() == segment.end.y.to_bits()
        && segment.start.x.to_bits() != segment.end.x.to_bits();
    let vertical = segment.start.x.to_bits() == segment.end.x.to_bits()
        && segment.start.y.to_bits() != segment.end.y.to_bits();
    if !horizontal && !vertical {
        return None;
    }

    let (start, end, fixed) = if horizontal {
        (segment.start.x, segment.end.x, segment.start.y)
    } else {
        (segment.start.y, segment.end.y, segment.start.x)
    };
    let lower = start.min(end);
    let upper = start.max(end);
    let mut projected = Vec::with_capacity(points.len());
    let mut offset = None;
    for point in points {
        let (axis, perpendicular) = if horizontal {
            (point.x, point.y)
        } else {
            (point.y, point.x)
        };
        if axis < lower || axis > upper {
            return None;
        }
        let delta = perpendicular - fixed;
        if !delta.is_finite()
            || offset.is_some_and(|expected: f64| expected.to_bits() != delta.to_bits())
        {
            return None;
        }
        offset = Some(delta);
        projected.push(axis);
    }
    projected.sort_unstable_by(f64::total_cmp);
    projected.dedup_by(|left, right| left.to_bits() == right.to_bits());

    let mut radius = (projected[0] - lower)
        .abs()
        .max((upper - projected[projected.len() - 1]).abs());
    for pair in projected.array_windows::<2>() {
        radius = radius.max((pair[1] - pair[0]).abs() / 2.0);
    }
    Some(radius.hypot(offset?.abs()))
}

/// Exact Hausdorff distance for a segment and a continuous line with the same
/// stored endpoints (in either direction).
///
/// Every line segment's squared distance to the baseline is convex, so the
/// largest line-to-baseline distance occurs at a stored vertex. Conversely,
/// the scalar projection from the line's shared first endpoint to its shared
/// last endpoint is continuous and spans the complete baseline, placing every
/// baseline point below a line point at no larger perpendicular distance.
fn hausdorff_shared_baseline(left: &Shape, right: &Shape) -> Option<f64> {
    let (baseline, line) = match (single_line_segment(left), right) {
        (Some(baseline), Shape::LineString(line)) => (baseline, line),
        _ => match (single_line_segment(right), left) {
            (Some(baseline), Shape::LineString(line)) => (baseline, line),
            _ => return None,
        },
    };
    let first = line.nth_coord(0).xy();
    let last = line.nth_coord(line.coord_count().checked_sub(1)?).xy();
    if !((same_point(first, baseline.start) && same_point(last, baseline.end))
        || (same_point(first, baseline.end) && same_point(last, baseline.start)))
    {
        return None;
    }
    Some(
        line.iter()
            .map(|point| point_segment_distance(point.xy(), baseline))
            .fold(0.0_f64, f64::max),
    )
}

fn is_its_endpoint_pair(shape: &Shape, segment: Segment) -> bool {
    let Shape::MultiPoint(points) = shape else {
        return false;
    };
    if points.len() != 2 {
        return false;
    }
    let first = points.point_at(0).xy();
    let second = points.point_at(1).xy();
    (same_point(first, segment.start) && same_point(second, segment.end))
        || (same_point(first, segment.end) && same_point(second, segment.start))
}
