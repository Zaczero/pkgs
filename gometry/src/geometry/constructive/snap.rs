use crate::NonNegative;
use crate::geographic::GeographicFrame;
use crate::geometry::constructive::Result;
use crate::geometry::{
    Bounds, BulkRTree, CoordSeq, Coordinates, ExpansionBudget, GENERATED_ITEM_LIMIT,
    GeometryErrorKind, GeometryKind, LineSeq, MultiLineString, MultiMember, MultiPolygon, Point,
    PointKey, Polygon, RepairMethod, Ring, Segment, SegmentProjection, Shape, TopologyDomain, XY,
    axis_pow2_scale, bounds_distance_squared, dedup_consecutive_points,
    interpolate_subdivision_ordinate, line_segments, point_distance,
    point_segment_distance_and_projection, points_dwithin, repair_shape_in_frame, same_point,
    same_topological_coordinate, scaled_residual, validate_shape_in_frame,
};

const SNAP_INDEX_MIN_POINTS: usize = 32;

type SnapTree = BulkRTree<rstar::primitives::GeomWithData<[f64; 2], usize>>;

enum SnapReferenceStorage {
    Linear(Vec<XY>),
    Indexed(SnapTree),
}

pub(crate) struct SnapReference {
    /// One reference representation: either build-ordered XY values or the
    /// same values in the spatial index, whose data is the source ordinal.
    storage: SnapReferenceStorage,
    tolerance: f64,
    /// `tolerance²`; only a normal value enables the spatial candidate filter.
    /// Underflowed and overflowing squares fall back to a full scan.
    limit: Option<f64>,
}

impl SnapReference {
    pub(crate) fn build(reference_points: &[XY], tolerance: f64) -> Self {
        let limit = tolerance * tolerance;
        let storage = if reference_points.len() >= SNAP_INDEX_MIN_POINTS {
            let mut entries = Vec::with_capacity(reference_points.len());
            for (ordinal, point) in reference_points.iter().enumerate() {
                entries.push(rstar::primitives::GeomWithData::new(
                    [point.x, point.y],
                    ordinal,
                ));
            }
            SnapReferenceStorage::Indexed(BulkRTree::bulk_load_with_params(entries))
        } else {
            SnapReferenceStorage::Linear(reference_points.to_vec())
        };
        Self {
            storage,
            tolerance,
            limit: limit.is_finite().then_some(limit),
        }
    }

    /// Snap one vertex: XY moves to the nearest in-tolerance reference
    /// vertex (Z/M kept), or stays put.
    pub(crate) fn snap_point(&self, point: Point) -> Point {
        let Some(reference) = self.nearest(point) else {
            return point;
        };
        point
            .with_xy(reference.x, reference.y)
            .expect("reference XY and source ordinates are already finite")
    }

    /// The reference vertex `point` snaps onto: smallest distance within
    /// tolerance wins, and the LATEST reference (build order) wins exact
    /// ties — the linear scan's `<=` update rule, preserved exactly.
    fn nearest(&self, point: Point) -> Option<XY> {
        let mut best: Option<(f64, usize, XY)> = None;
        let mut consider = |distance: f64, ordinal: usize, position: XY| {
            if best.is_none_or(|(leader, at, _)| {
                distance < leader || (distance.to_bits() == leader.to_bits() && ordinal > at)
            }) {
                best = Some((distance, ordinal, position));
            }
        };
        // The metric (squared when the limit is representable, else distance)
        // is uniform across candidates, so `best` is a valid argmin either way.
        let mut test = |reference: XY, ordinal: usize| {
            // Honest dwithin — squared false-zero must not treat distinct
            // subnormals as within a zero (or tiny) tolerance.
            if points_dwithin(point, reference, self.tolerance) {
                let distance = point_distance(point, reference);
                consider(distance, ordinal, reference);
            }
        };
        match (&self.storage, self.limit) {
            // A normal squared tolerance selects the conservative AABB
            // candidate filter; underflowed/overflowed squares full-scan.
            (SnapReferenceStorage::Indexed(tree), Some(limit)) if limit.is_normal() => {
                if let Some(window) =
                    outward_padded_bounds(point.x, point.x, point.y, point.y, self.tolerance)
                {
                    for candidate in tree.locate_in_envelope_intersecting(window) {
                        test(
                            XY::new(candidate.geom()[0], candidate.geom()[1]),
                            candidate.data,
                        );
                    }
                } else {
                    for candidate in tree {
                        test(
                            XY::new(candidate.geom()[0], candidate.geom()[1]),
                            candidate.data,
                        );
                    }
                }
            },
            // Zero/underflowed limit or non-finite tolerance²: full scan +
            // honest points_dwithin (cannot trust squared-space radius).
            (SnapReferenceStorage::Indexed(tree), _) => {
                for candidate in tree {
                    test(
                        XY::new(candidate.geom()[0], candidate.geom()[1]),
                        candidate.data,
                    );
                }
            },
            // Brute: scan every reference vertex (small reference).
            (SnapReferenceStorage::Linear(points), _) => {
                for (ordinal, &reference) in points.iter().enumerate() {
                    test(reference, ordinal);
                }
            },
        }
        best.map(|(_, _, position)| position)
    }

    /// Reference vertices to drop into the open segment `start -> end`, in
    /// ascending order along it: every reference within tolerance of the
    /// segment but of neither endpoint, placed at its projection (XY from
    /// the reference, Z/M interpolated), with only references at an
    /// identical projected position deduplicated.
    pub(crate) fn insertions(&self, start: Point, end: Point) -> crate::error::Result<Vec<Point>> {
        if same_point(start, end) {
            return Ok(Vec::new());
        }
        let segment = Segment {
            start: start.into(),
            end: end.into(),
        };
        let tolerance = self.tolerance;
        let limit = self.limit;
        // Grow to the surviving candidates rather than the reference count:
        // this runs once per source segment, and a tolerance window admits a
        // handful of references out of the whole set.
        let mut candidates: Vec<(SegmentProjection, usize, Point)> = Vec::new();
        let mut consider = |reference: XY, ordinal: usize| -> crate::error::Result<()> {
            // Skip if within tolerance of either endpoint, or farther than
            // tolerance from the open segment (honest distance, not squared).
            if points_dwithin(reference, start, tolerance)
                || points_dwithin(reference, end, tolerance)
            {
                return Ok(());
            }
            let (distance, projection) = point_segment_distance_and_projection(reference, segment)?;
            if distance > tolerance {
                return Ok(());
            }
            if !projection.is_start() && !projection.is_end() {
                let mut point = projection.interpolate_point(start, end)?;
                point.x = reference.x;
                point.y = reference.y;
                candidates.push((projection, ordinal, point));
            }
            Ok(())
        };
        match (&self.storage, limit) {
            // Indexed + a normal limit: only vertices in the segment's
            // tolerance-padded bbox can land inside (max-norm superset of the
            // euclidean ball). A non-finite limit still scans the whole tree.
            (SnapReferenceStorage::Indexed(tree), Some(limit)) if limit.is_normal() => {
                let (lo_x, hi_x) = (start.x.min(end.x), start.x.max(end.x));
                let (lo_y, hi_y) = (start.y.min(end.y), start.y.max(end.y));
                if let Some(window) = outward_padded_bounds(lo_x, hi_x, lo_y, hi_y, tolerance) {
                    for candidate in tree.locate_in_envelope_intersecting(window) {
                        consider(
                            XY::new(candidate.geom()[0], candidate.geom()[1]),
                            candidate.data,
                        )?;
                    }
                } else {
                    for candidate in tree {
                        consider(
                            XY::new(candidate.geom()[0], candidate.geom()[1]),
                            candidate.data,
                        )?;
                    }
                }
            },
            (SnapReferenceStorage::Indexed(tree), _) => {
                for candidate in tree {
                    consider(
                        XY::new(candidate.geom()[0], candidate.geom()[1]),
                        candidate.data,
                    )?;
                }
            },
            // Brute: scan every reference vertex (small reference); `consider`
            // applies the exact tolerance/projection filter.
            (SnapReferenceStorage::Linear(points), _) => {
                for (ordinal, &reference) in points.iter().enumerate() {
                    consider(reference, ordinal)?;
                }
            },
        }
        // Fraction order with build-order ties: identical to the linear
        // scan's input-order stable sort.
        crate::geometry::try_sort_by(&mut candidates, |a, b| {
            Ok(a.0.cmp_along(&b.0)?.then(a.1.cmp(&b.1)))
        })?;
        // Collapse only genuine duplicates — references at an identical
        // projected position. A fixed parametric epsilon would merge distinct
        // reference points (e.g. 0.5 and 0.5000000000005) even at tolerance 0.
        #[expect(clippy::float_cmp, reason = "exact projected-position identity")]
        candidates.dedup_by(|right, left| right.2.x == left.2.x && right.2.y == left.2.y);
        let mut output = Vec::with_capacity(candidates.len());
        for (_, _, point) in candidates {
            output.push(point);
        }
        Ok(output)
    }
}

fn outward_padded_bounds(
    min_x: f64,
    max_x: f64,
    min_y: f64,
    max_y: f64,
    tolerance: f64,
) -> Option<rstar::AABB<[f64; 2]>> {
    let min_x = (min_x - tolerance).next_down();
    let max_x = (max_x + tolerance).next_up();
    let min_y = (min_y - tolerance).next_down();
    let max_y = (max_y + tolerance).next_up();
    [min_x, max_x, min_y, max_y]
        .into_iter()
        .all(f64::is_finite)
        .then(|| rstar::AABB::from_corners([min_x, min_y], [max_x, max_y]))
}

fn try_unique_xy_points(shape: &Shape) -> crate::error::Result<Vec<XY>> {
    let mut seen = crate::collections::try_hash_set_with_capacity(shape.coord_count())?;
    let mut points = Vec::with_capacity(shape.coord_count());
    shape.for_each_point(|point| {
        let point = point.xy();
        if seen.insert(PointKey::new(point)) {
            points.push(point);
        }
    });
    Ok(points)
}

pub(crate) fn snap_points_to_reference<C: Coordinates + ?Sized>(
    points: &C,
    reference: &SnapReference,
) -> Vec<Point> {
    let mut output = Vec::with_capacity(points.coord_count());
    for point in points.iter_coords() {
        output.push(reference.snap_point(point));
    }
    output
}

pub(crate) fn snap_line_to_reference<C: Coordinates + ?Sized>(
    points: &C,
    reference: &SnapReference,
) -> crate::error::Result<Vec<Point>> {
    if points.coord_count() < 2 {
        return Ok(snap_points_to_reference(points, reference));
    }

    let mut result = Vec::with_capacity(points.coord_count());
    push_distinct_point(&mut result, reference.snap_point(points.nth_coord(0)));

    for [start, end] in points.segment_pairs() {
        for point in reference.insertions(start, end)? {
            push_distinct_point(&mut result, point);
        }
        push_distinct_point(&mut result, reference.snap_point(end));
    }
    if result.len() == 1 {
        result.clear();
        result.extend([
            reference.snap_point(points.nth_coord(0)),
            reference.snap_point(points.nth_coord(points.coord_count() - 1)),
        ]);
    }
    Ok(result)
}

fn push_distinct_point(points: &mut Vec<Point>, point: Point) {
    if points.last().is_none_or(|last| !same_point(*last, point)) {
        points.push(point);
    }
}

pub(crate) fn snap_ring_to_reference<C: Coordinates + ?Sized>(
    points: &C,
    reference: &SnapReference,
) -> crate::error::Result<Vec<Point>> {
    let len = points.coord_count();
    let mut result = snap_line_to_reference(points, reference)?;
    if result.len() > 1 && len > 1 && same_point(points.nth_coord(0), points.nth_coord(len - 1)) {
        let first = result[0];
        if let Some(last) = result.last_mut() {
            *last = first;
        }
    }
    Ok(result)
}

/// Consecutive-duplicate removal against the LAST KEPT vertex (the GEOS
/// rule): a vertex survives when it is farther than `tolerance` from the
/// previously retained one. Columnar: the keep scan reads only the XY
/// columns; survivors gather every present lane by index, so Z/M ride
/// along untouched — and an input with nothing to drop is returned as a
/// zero-copy share.
pub(crate) fn remove_repeated_points(points: &CoordSeq, tolerance: f64) -> Result<CoordSeq> {
    let (xs, ys) = (points.xs(), points.ys());
    if xs.is_empty() {
        return Ok(points.clone());
    }
    let mut keep: Vec<usize> = Vec::with_capacity(xs.len());
    keep.push(0);
    let (mut anchor_x, mut anchor_y) = (xs[0], ys[0]);
    // Honest distance compare: squared false-zero must not collapse distinct
    // subnormals under a zero (or tiny) tolerance.
    for index in 1..xs.len() {
        let anchor = XY::new(anchor_x, anchor_y);
        let candidate = XY::new(xs[index], ys[index]);
        if !points_dwithin(anchor, candidate, tolerance) {
            keep.push(index);
            (anchor_x, anchor_y) = (xs[index], ys[index]);
        }
    }
    if keep.len() == xs.len() {
        return Ok(points.clone());
    }
    points.select(keep.iter().copied())
}

/// Repeated-point removal on a closed ring: the closing vertex always stays,
/// and a kept vertex within `tolerance` of it gives way, so the ring stays
/// closed. `None` when fewer than four coordinates survive.
pub(crate) fn remove_repeated_ring_points(
    points: &CoordSeq,
    tolerance: f64,
) -> Result<Option<CoordSeq>> {
    let (xs, ys) = (points.xs(), points.ys());
    let Some(closing) = xs.len().checked_sub(1) else {
        return Ok(None);
    };
    let mut keep: Vec<usize> = Vec::with_capacity(xs.len());
    keep.push(0);
    let (mut anchor_x, mut anchor_y) = (xs[0], ys[0]);
    for index in 1..closing {
        if !points_dwithin(
            XY::new(anchor_x, anchor_y),
            XY::new(xs[index], ys[index]),
            tolerance,
        ) {
            keep.push(index);
            (anchor_x, anchor_y) = (xs[index], ys[index]);
        }
    }
    let close = XY::new(xs[closing], ys[closing]);
    while let [_, .., last] = keep[..]
        && points_dwithin(XY::new(xs[last], ys[last]), close, tolerance)
    {
        keep.pop();
    }
    keep.push(closing);
    if keep.len() < Ring::MIN_VERTICES_CLOSED {
        return Ok(None);
    }
    if keep.len() == xs.len() {
        return Ok(Some(points.clone()));
    }
    points.select(keep.iter().copied()).map(Some)
}

pub(crate) fn remove_repeated_line_points(points: &CoordSeq, tolerance: f64) -> Result<CoordSeq> {
    let cleaned = remove_repeated_points(points, tolerance)?;
    if points.len() >= 2 && cleaned.len() == 1 {
        return points.select([0, points.len() - 1].into_iter());
    }
    Ok(cleaned)
}

pub(crate) fn segmentize_points_budgeted(
    points: &CoordSeq,
    max_segment_length: f64,
    placement: SegmentPlacement<'_>,
    budget: &mut ExpansionBudget,
) -> Result<CoordSeq> {
    if points.len() < 2 {
        return Ok(points.clone());
    }
    if let SegmentPlacement::Geodesic(geodesic, frame) = placement {
        // Keep the inverse solution only for segments whose admitted output
        // contains an interior vertex. The empty vector preserves the
        // zero-generated path's existing storage behavior.
        let mut solutions = Vec::new();
        let steps = collect_steps(points, |segment_index, _segment| {
            let plan = geodesic_segment_plan(geodesic, frame, points, segment_index)?;
            let steps = segmentize_steps(plan.solution.3, max_segment_length);
            if steps.max(1) > 1 {
                solutions.push(plan);
            }
            Ok(steps)
        })?;
        return subdivide_columns_from_steps(
            points,
            SubdivisionEmission::Geodesic(GeodesicEmitter::retained(
                geodesic,
                frame,
                solutions.as_slice(),
            )),
            &steps,
            budget,
        );
    }
    segmentize_planar_candidate(points, max_segment_length, budget)
}

fn segmentize_planar_candidate(
    points: &CoordSeq,
    max_segment_length: f64,
    budget: &mut ExpansionBudget,
) -> Result<CoordSeq> {
    // One extra piece supplies slack for a count that landed exactly on the
    // requested limit. If that candidate still cannot satisfy the strict
    // bound, the owning post-pass preserves its unrepresentable error.
    const MAX_REPRESENTABILITY_RETRIES: usize = 1;
    let mut steps = collect_steps(points, |_, segment| {
        Ok(planar_segmentize_steps(segment, max_segment_length))
    })?;
    let mut last = None;
    for retry in 0..=MAX_REPRESENTABILITY_RETRIES {
        let (output, generated) = subdivide_columns_from_steps_uncommitted(
            points,
            SubdivisionEmission::Planar,
            &steps,
            budget,
        )?;
        match verify_planar_segmentize_output(&output, max_segment_length) {
            Ok(()) => {
                budget.add(generated)?;
                return Ok(output);
            },
            Err(error)
                if matches!(
                    error.kind(),
                    crate::error::ErrorKind::Geometry(GeometryErrorKind::UnrepresentableOutput {
                        operation: "segmentize"
                    })
                ) =>
            {
                last = Some((output, generated));
            },
            Err(error) => return Err(error),
        }
        if retry == MAX_REPRESENTABILITY_RETRIES {
            break;
        }
        for (step, segment) in steps.iter_mut().zip(line_segments(points)) {
            if !same_point(segment.start, segment.end) {
                *step = step
                    .checked_add(1)
                    .ok_or(crate::error::ResourceError::SizeOverflow)?;
            }
        }
    }
    let (output, generated) = last.expect("segmentize always emits a planar candidate");
    budget.add(generated)?;
    Ok(output)
}

fn segmentize_steps(length: f64, max_segment_length: f64) -> usize {
    if length == 0.0 || !length.is_finite() || max_segment_length <= 0.0 {
        return 1;
    }
    // A huge quotient must not wrap to a small step count, which would admit
    // output beyond the shared 16M generated-work budget.
    let pieces = (length / max_segment_length).ceil();
    if !pieces.is_finite() || pieces > (GENERATED_ITEM_LIMIT + 1) as f64 {
        // The sink charges inserted vertices (`pieces - 1`), so its sentinel
        // must remain one past that limit rather than merely one past pieces.
        GENERATED_ITEM_LIMIT + 2
    } else {
        pieces as usize
    }
}

/// Plan the ideal planar piece count without allowing an overflowing endpoint
/// difference to masquerade as an already-complete segment. The common path
/// keeps the established distance arithmetic; only a non-finite distance uses
/// a common power-of-two frame, so the endpoint residual and the limit are
/// compared in the same scaled units without forming an overflowing length.
fn planar_segmentize_steps(segment: Segment, max_segment_length: f64) -> usize {
    if !max_segment_length.is_finite() || max_segment_length <= 0.0 {
        return 1;
    }
    let length = point_distance(segment.start, segment.end);
    if length.is_finite() {
        return segmentize_steps(length, max_segment_length);
    }

    let maximum = segment
        .start
        .x
        .abs()
        .max(segment.start.y.abs())
        .max(segment.end.x.abs())
        .max(segment.end.y.abs());
    let scale = axis_pow2_scale(maximum);
    let dx = scaled_residual(segment.end.x, segment.start.x, scale);
    let dy = scaled_residual(segment.end.y, segment.start.y, scale);
    let scaled_limit = max_segment_length * scale;
    let ratio = if scaled_limit > 0.0 {
        dx.hypot(dy) / scaled_limit
    } else {
        f64::INFINITY
    };
    let pieces = ratio.ceil();
    // A huge quotient must not wrap to a small step count, which would admit
    // output beyond the shared 16M generated-work budget.
    if !pieces.is_finite() || pieces > (GENERATED_ITEM_LIMIT + 1) as f64 {
        // The sink charges inserted vertices (`pieces - 1`), so its sentinel
        // must remain one past that limit rather than merely one past pieces.
        GENERATED_ITEM_LIMIT + 2
    } else {
        pieces.max(1.0) as usize
    }
}

/// The ideal count describes the continuous subdivision, not the binary64
/// coordinates that the emitter actually stores. Audit the resulting chain so
/// the public maximum-length promise is about every emitted chord, including
/// source-to-source chords that can survive as repeated interior points.
pub(crate) fn verify_planar_segmentize_output(
    points: &CoordSeq,
    max_segment_length: f64,
) -> Result<()> {
    if !max_segment_length.is_finite() || max_segment_length <= 0.0 {
        return Ok(());
    }
    for segment in line_segments(points) {
        if point_distance(segment.start, segment.end)
            .partial_cmp(&max_segment_length)
            .is_none_or(std::cmp::Ordering::is_gt)
        {
            return Err(GeometryErrorKind::unrepresentable_output("segmentize"));
        }
    }
    Ok(())
}

pub(crate) fn verify_planar_segmentize_shape(shape: &Shape, max_segment_length: f64) -> Result<()> {
    match shape {
        Shape::Point(_) | Shape::MultiPoint(_) | Shape::Empty(..) => Ok(()),
        Shape::LineString(line) => verify_planar_segmentize_output(line, max_segment_length),
        Shape::MultiLineString(lines) => lines
            .iter()
            .try_for_each(|line| verify_planar_segmentize_output(line, max_segment_length)),
        Shape::Polygon(polygon) => polygon.rings().try_for_each(|ring| {
            verify_planar_segmentize_output(ring.coords(), max_segment_length)
        }),
        Shape::MultiPolygon(polygons) => polygons.iter().try_for_each(|polygon| {
            polygon.rings().try_for_each(|ring| {
                verify_planar_segmentize_output(ring.coords(), max_segment_length)
            })
        }),
        Shape::GeometryCollection(geometries) => geometries
            .iter()
            .try_for_each(|geometry| verify_planar_segmentize_shape(geometry, max_segment_length)),
    }
}

/// Insert `ceil(1 / fraction) - 1` evenly spaced vertices into every segment
/// (the GEOS densify semantics): each segment is split into equal pieces no
/// longer than `fraction` of its own length, re-segmenting continuous
/// Hausdorff and refining the vertex sequence for discrete Fréchet. `fraction` is in
/// `(0, 1]`; `1` keeps the vertices unchanged.
#[cfg(test)]
pub(crate) fn densify_points_budgeted(
    points: &CoordSeq,
    fraction: f64,
    budget: &mut ExpansionBudget,
) -> Result<CoordSeq> {
    densify_points_budgeted_with_placement(points, fraction, SegmentPlacement::Planar, budget)
}

pub(crate) fn densify_points_budgeted_with_placement(
    points: &CoordSeq,
    fraction: f64,
    placement: SegmentPlacement<'_>,
    budget: &mut ExpansionBudget,
) -> Result<CoordSeq> {
    // One segment's steps already past the budget stop here, before a
    // saturated count (a subnormal `fraction`) reaches the step arithmetic.
    let steps = budget.check_additional((1.0 / fraction).ceil() as usize)?;
    subdivide_columns(
        points,
        placement,
        |segment| Ok(densify_steps(segment, steps)),
        budget,
    )
}

fn densify_steps(segment: Segment, steps: usize) -> usize {
    if same_point(segment.start, segment.end) {
        1
    } else {
        steps
    }
}

#[derive(Clone, Copy)]
struct GeodesicSegmentPlan {
    solution: (f64, f64, f64, f64),
    canonical_end: XY,
}

fn geodesic_segment_plan(
    geodesic: &geographiclib_rs::Geodesic,
    frame: GeographicFrame,
    points: &CoordSeq,
    segment_index: usize,
) -> Result<GeodesicSegmentPlan> {
    let start = frame
        .to_geodesic_point(Point::new_raw_xy(
            points.xs()[segment_index],
            points.ys()[segment_index],
        ))
        .expect("geographic subdivision received admitted coordinates");
    let end = frame
        .to_geodesic_point(Point::new_raw_xy(
            points.xs()[segment_index + 1],
            points.ys()[segment_index + 1],
        ))
        .expect("geographic subdivision received admitted coordinates");
    let solution = crate::crs::geodesic_line_solution(geodesic, start.x, start.y, end.x, end.y)?;
    Ok(GeodesicSegmentPlan {
        solution,
        canonical_end: end.xy(),
    })
}

enum GeodesicPlanSource<'a> {
    Recompute,
    Retained(std::slice::Iter<'a, GeodesicSegmentPlan>),
}

struct GeodesicEmitter<'a> {
    geodesic: &'a geographiclib_rs::Geodesic,
    frame: GeographicFrame,
    plans: GeodesicPlanSource<'a>,
}

impl<'a> GeodesicEmitter<'a> {
    const fn recompute(geodesic: &'a geographiclib_rs::Geodesic, frame: GeographicFrame) -> Self {
        Self {
            geodesic,
            frame,
            plans: GeodesicPlanSource::Recompute,
        }
    }

    fn retained(
        geodesic: &'a geographiclib_rs::Geodesic,
        frame: GeographicFrame,
        plans: &'a [GeodesicSegmentPlan],
    ) -> Self {
        Self {
            geodesic,
            frame,
            plans: GeodesicPlanSource::Retained(plans.iter()),
        }
    }

    fn emit(
        &mut self,
        points: &CoordSeq,
        xs: &mut Vec<f64>,
        ys: &mut Vec<f64>,
        segment_index: usize,
        steps: usize,
    ) -> Result<()> {
        let plan = match &mut self.plans {
            GeodesicPlanSource::Recompute => {
                if steps == 1 {
                    // Densify has no planning solve to reuse. Preserve the
                    // existing inverse-error boundary even when no vertex is
                    // emitted for this segment.
                    geodesic_segment_plan(self.geodesic, self.frame, points, segment_index)?;
                    return Ok(());
                }
                geodesic_segment_plan(self.geodesic, self.frame, points, segment_index)?
            },
            GeodesicPlanSource::Retained(plans) => {
                if steps == 1 {
                    // The same admitted endpoint conversion and inverse have
                    // already succeeded during planning. Geodesic and frame
                    // are immutable here, so this materializer has no work.
                    return Ok(());
                }
                *plans
                    .next()
                    .expect("geodesic subdivision plan matches segment steps")
            },
        };
        emit_geodesic_plan(self.geodesic, self.frame, plan, steps, xs, ys)
    }

    fn assert_exhausted(&self) {
        if let GeodesicPlanSource::Retained(plans) = &self.plans {
            debug_assert!(plans.as_slice().is_empty());
        }
    }
}

enum SubdivisionEmission<'a> {
    Planar,
    Geodesic(GeodesicEmitter<'a>),
}

impl<'a> SubdivisionEmission<'a> {
    const fn recompute(placement: SegmentPlacement<'a>) -> Self {
        match placement {
            SegmentPlacement::Planar => Self::Planar,
            SegmentPlacement::Geodesic(geodesic, frame) => {
                Self::Geodesic(GeodesicEmitter::recompute(geodesic, frame))
            },
        }
    }

    fn assert_exhausted(&self) {
        if let Self::Geodesic(emitter) = self {
            emitter.assert_exhausted();
        }
    }
}

trait SubdivisionSink {
    fn source(&mut self, source_index: usize) -> Result<()>;
    fn interior(&mut self, segment_index: usize, steps: usize) -> Result<()>;
}

/// Run the exact same segment plan through a counting sink and a materializing
/// sink.  Keeping this as one traversal shape makes the budget admission
/// mechanically match the coordinates subsequently emitted.
fn emit_subdivision(steps: &[usize], sink: &mut impl SubdivisionSink) -> Result<()> {
    sink.source(0)?;
    for (segment_index, &segment_steps) in steps.iter().enumerate() {
        sink.interior(segment_index, segment_steps.max(1))?;
        sink.source(segment_index + 1)?;
    }
    Ok(())
}

struct SubdivisionCount {
    emitted: usize,
}

impl SubdivisionCount {
    fn generated(&self, input_len: usize) -> Result<usize> {
        self.emitted
            .checked_sub(input_len)
            .ok_or_else(|| crate::error::ResourceError::SizeOverflow.into())
    }
}

impl SubdivisionSink for SubdivisionCount {
    fn source(&mut self, _: usize) -> Result<()> {
        self.emitted = self
            .emitted
            .checked_add(1)
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        Ok(())
    }

    fn interior(&mut self, _: usize, steps: usize) -> Result<()> {
        self.emitted = self
            .emitted
            .checked_add(
                steps
                    .checked_sub(1)
                    .ok_or(crate::error::ResourceError::SizeOverflow)?,
            )
            .ok_or(crate::error::ResourceError::SizeOverflow)?;
        Ok(())
    }
}

/// How interior vertices are placed along a segment.
///
/// X and Y are a PAIR under a geodesic: the point at a fraction of a geodesic
/// is not the per-axis interpolation of the endpoints. Z and M stay per-column
/// under both placements — they are attribute ramps, not positions.
#[derive(Clone, Copy)]
pub(crate) enum SegmentPlacement<'a> {
    /// Straight line in coordinate space (projected or CRS-free input).
    Planar,
    /// Along the ellipsoid for coordinates stored in a typed native angular
    /// frame; the inserted points are emitted back in that same frame.
    Geodesic(&'a geographiclib_rs::Geodesic, GeographicFrame),
}

struct SubdivisionColumns<'a> {
    points: &'a CoordSeq,
    emission: SubdivisionEmission<'a>,
    xs: Vec<f64>,
    ys: Vec<f64>,
    zs: Option<Vec<f64>>,
    ms: Option<Vec<f64>>,
}

impl<'a> SubdivisionColumns<'a> {
    fn new(points: &'a CoordSeq, emission: SubdivisionEmission<'a>, total: usize) -> Self {
        Self {
            points,
            emission,
            xs: Vec::with_capacity(total),
            ys: Vec::with_capacity(total),
            zs: points.zs().map(|_| Vec::with_capacity(total)),
            ms: points.ms().map(|_| Vec::with_capacity(total)),
        }
    }

    fn finish(self) -> Result<CoordSeq> {
        CoordSeq::try_from_vecs(self.xs, self.ys, self.zs, self.ms)
    }

    fn push_interpolated(
        output: &mut Vec<f64>,
        column: &[f64],
        segment_index: usize,
        steps: usize,
    ) {
        let (start, end) = (column[segment_index], column[segment_index + 1]);
        for step in 1..steps {
            let value = interpolate_subdivision_ordinate(start, end, step as f64 / steps as f64);
            output.push(if value.is_finite() {
                value
            } else if step * 2 <= steps {
                start
            } else {
                end
            });
        }
    }
}

fn emit_geodesic_plan(
    geodesic: &geographiclib_rs::Geodesic,
    frame: GeographicFrame,
    plan: GeodesicSegmentPlan,
    steps: usize,
    xs: &mut Vec<f64>,
    ys: &mut Vec<f64>,
) -> Result<()> {
    // `plan` contains canonical degrees; restore the caller's angular frame
    // only for inserted coordinates. Source vertices and Z/M stay in their
    // original columns in `SubdivisionColumns`.
    let (x0, y0, ..) = plan.solution;
    let (x1, y1) = (plan.canonical_end.x, plan.canonical_end.y);
    let equatorial = y0 == 0.0 && y1 == 0.0 && (x1 - x0).abs() < 180.0;
    for step in 1..steps {
        let fraction = step as f64 / steps as f64;
        let (mut longitude, mut latitude) = if equatorial {
            (interpolate_subdivision_ordinate(x0, x1, fraction), 0.0)
        } else {
            let info = crate::crs::geodesic_interpolate_on_line(geodesic, plan.solution, fraction)?;
            (info.longitude, info.latitude)
        };
        let point = frame
            .point_from_geodesic(Point::new_raw_xy(longitude, latitude))
            .expect("geographic subdivision produced a finite coordinate");
        (longitude, latitude) = (point.x, point.y);
        if !(longitude.is_finite() && latitude.is_finite()) {
            return Err(GeometryErrorKind::message(
                "geodesic subdivision calculation produced a non-finite coordinate",
            ));
        }
        xs.push(longitude);
        ys.push(latitude);
    }
    Ok(())
}

impl SubdivisionSink for SubdivisionColumns<'_> {
    fn source(&mut self, source_index: usize) -> Result<()> {
        self.xs.push(self.points.xs()[source_index]);
        self.ys.push(self.points.ys()[source_index]);
        if let (Some(output), Some(column)) = (&mut self.zs, self.points.zs()) {
            output.push(column[source_index]);
        }
        if let (Some(output), Some(column)) = (&mut self.ms, self.points.ms()) {
            output.push(column[source_index]);
        }
        Ok(())
    }

    fn interior(&mut self, segment_index: usize, steps: usize) -> Result<()> {
        match &mut self.emission {
            SubdivisionEmission::Planar => {
                Self::push_interpolated(&mut self.xs, self.points.xs(), segment_index, steps);
                Self::push_interpolated(&mut self.ys, self.points.ys(), segment_index, steps);
            },
            SubdivisionEmission::Geodesic(emitter) => {
                emitter.emit(
                    self.points,
                    &mut self.xs,
                    &mut self.ys,
                    segment_index,
                    steps,
                )?;
            },
        }
        if let (Some(output), Some(column)) = (&mut self.zs, self.points.zs()) {
            Self::push_interpolated(output, column, segment_index, steps);
        }
        if let (Some(output), Some(column)) = (&mut self.ms, self.points.ms()) {
            Self::push_interpolated(output, column, segment_index, steps);
        }
        Ok(())
    }
}

fn collect_steps(
    points: &CoordSeq,
    mut steps_for: impl FnMut(usize, Segment) -> Result<usize>,
) -> Result<Vec<usize>> {
    let mut steps = Vec::with_capacity(points.len().saturating_sub(1));
    for (segment_index, segment) in line_segments(points).enumerate() {
        let step_count = steps_for(segment_index, segment)?;
        steps.push(step_count);
    }
    Ok(steps)
}

/// Split every segment of a chain into `steps_for(segment)` equal pieces,
/// keeping every original vertex — the shared engine behind `segmentize`
/// (length-driven) and `densify` (fraction-driven). Columnar: every
/// Z/M ordinate interpolates straight from its column into the output column;
/// XY follows `placement` — no 40-byte `Point` staging, no transpose.
pub(crate) fn subdivide_columns(
    points: &CoordSeq,
    placement: SegmentPlacement<'_>,
    mut steps_for: impl FnMut(Segment) -> Result<usize>,
    budget: &mut ExpansionBudget,
) -> Result<CoordSeq> {
    let count = points.len();
    if count < 2 {
        return Ok(points.clone());
    }
    // `steps_for` runs once per segment (not per output vertex); the exact
    // output size keeps every column a single allocation.
    let steps = collect_steps(points, |_, segment| steps_for(segment))?;
    subdivide_columns_from_steps(
        points,
        SubdivisionEmission::recompute(placement),
        &steps,
        budget,
    )
}

fn subdivide_columns_from_steps<'a>(
    points: &'a CoordSeq,
    emission: SubdivisionEmission<'a>,
    steps: &[usize],
    budget: &mut ExpansionBudget,
) -> Result<CoordSeq> {
    let (output, generated) =
        subdivide_columns_from_steps_uncommitted(points, emission, steps, budget)?;
    budget.add(generated)?;
    Ok(output)
}

fn subdivide_columns_from_steps_uncommitted<'a>(
    points: &'a CoordSeq,
    emission: SubdivisionEmission<'a>,
    steps: &[usize],
    budget: &ExpansionBudget,
) -> Result<(CoordSeq, usize)> {
    let count = points.len();
    let mut count_sink = SubdivisionCount { emitted: 0 };
    emit_subdivision(steps, &mut count_sink)?;
    let total = count_sink.emitted;
    // Existing source vertices are input-sized work. Charge only the vertices
    // this operation creates, once for the whole caller-owned budget.
    let generated = count_sink.generated(count)?;
    budget.check_additional(generated)?;
    if generated == 0 {
        // Nothing subdivides (every segment already satisfies the bound —
        // the common case once data is denser than the requested length):
        // the input IS the output, share it.
        return Ok((points.clone(), 0));
    }
    let mut output = SubdivisionColumns::new(points, emission, total);
    emit_subdivision(steps, &mut output)?;
    output.emission.assert_exhausted();
    let output = output.finish()?;
    Ok((output, generated))
}

impl Shape {
    pub fn snap(&self, reference: &Self, tolerance: f64) -> Result<Self> {
        let tolerance = NonNegative::try_new("tolerance", tolerance)?.get();
        let reference_bounds = reference.bounds();
        // Boxes farther apart than the tolerance snap to the identity: no
        // reference vertex can be within tolerance of any vertex OR segment
        // (both sit inside their bounds), so the scans cannot change a thing.
        if self.outside_snap_range(reference_bounds, tolerance) {
            return Ok(self.clone());
        }
        let reference_points = try_unique_xy_points(reference)?;
        if reference_points.is_empty() {
            return Ok(self.clone());
        }
        let engine = SnapReference::build(&reference_points, tolerance);
        drop(reference_points);
        self.snap_using_reference(reference_bounds, &engine)
    }

    fn outside_snap_range(&self, reference_bounds: Option<Bounds>, tolerance: f64) -> bool {
        self.bounds()
            .zip(reference_bounds)
            .is_some_and(|(left, right)| {
                bounds_distance_squared(left, right) > tolerance * tolerance
            })
    }

    fn snap_using_reference(
        &self,
        reference_bounds: Option<Bounds>,
        engine: &SnapReference,
    ) -> Result<Self> {
        Ok(match self {
            Self::Point(point) => Self::Point(engine.snap_point(*point)),
            Self::MultiPoint(points) => {
                Self::MultiPoint(points.map_values(|point| engine.snap_point(point))?)
            },
            Self::LineString(points) => Self::LineString(LineSeq::try_new(
                CoordSeq::try_from_points(&snap_line_to_reference(points, engine)?)?,
            )?),
            Self::MultiLineString(lines) => {
                Self::MultiLineString(lines.try_map_values(lines.axes(), |line| {
                    LineSeq::try_new(CoordSeq::try_from_points(&snap_line_to_reference(
                        line, engine,
                    )?)?)
                })?)
            },
            Self::Polygon(polygon) => polygon.snap_to_reference(engine)?.map_or_else(
                || Self::typed_empty(GeometryKind::Polygon, polygon.axes()),
                Self::Polygon,
            ),
            Self::MultiPolygon(polygons) => {
                let members = polygons
                    .members()
                    .map(|member| match member {
                        MultiMember::Empty => Ok(MultiMember::Empty),
                        MultiMember::Value(index) => Ok(polygons.as_slice()[index]
                            .snap_to_reference(engine)?
                            .map_or(MultiMember::Empty, MultiMember::Value)),
                    })
                    .collect::<Result<Vec<_>>>()?;
                Self::MultiPolygon(MultiPolygon::from_values(polygons.axes(), members)?)
            },
            Self::GeometryCollection(geometries) => {
                Self::GeometryCollection(geometries.try_map_members(|geometry| {
                    if geometry.outside_snap_range(reference_bounds, engine.tolerance) {
                        Ok(geometry.clone())
                    } else {
                        geometry.snap_using_reference(reference_bounds, engine)
                    }
                })?)
            },
            Self::Empty(..) => self.clone(),
        })
    }

    /// Snap every X/Y onto the grid `origin + k * size`, then strip
    /// consecutive duplicate snapped vertices and collapse degenerate
    /// parts (the `PostGIS` ``ST_SnapToGrid`` shape — output may be
    /// non-simple). Z/M ride on surviving vertices. `quantize` is the
    /// decimal-rounding, vertex-preserving sibling.
    pub fn snap_to_grid(
        &self,
        size: (f64, f64),
        origin: (f64, f64),
    ) -> Result<Self, crate::error::Error> {
        // The quotient overflows when the grid is finer than the
        // coordinate's ULP (or the origin offset overflows) — snapping is
        // then meaningless, and silently emitting infinity would break the
        // finite-coordinate invariant.
        self.snapped_to_grid(size, origin)?.collapse_snapped()
    }

    /// The single columnar recursion behind every coordinate-rewriting
    /// transform ([`affine`](Self::affine),
    /// [`snapped_to_grid`](Self::snapped_to_grid),
    /// [`swap_xy`](Self::swap_xy)): apply `point_fn` to a bare point and
    /// `seq_fn` to every coordinate sequence (multipoint, line, every
    /// ring), recursing into collections; `Empty` passes through. Both
    /// callbacks are fallible and share one error type — for an infallible
    /// transform pass [`Infallible`] closures (`Ok(...)`), whose `?`/`Ok`
    /// wrapping collapses away in codegen.
    pub(crate) fn try_map_coordseqs<E>(
        &self,
        seq_fn: impl Fn(&CoordSeq) -> Result<CoordSeq, E> + Copy,
        point_fn: impl Fn(&Point) -> Result<Point, E> + Copy,
    ) -> Result<Self, E>
    where
        E: From<crate::error::Error>,
    {
        Ok(match self {
            Self::Point(point) => Self::Point(point_fn(point)?),
            Self::MultiPoint(points) => Self::MultiPoint(points.with_coords(seq_fn(points)?)),
            Self::LineString(points) => {
                Self::LineString(LineSeq::try_new(seq_fn(points)?).map_err(E::from)?)
            },
            Self::MultiLineString(lines) => {
                let mut members = Vec::with_capacity(lines.member_count());
                for member in lines.members() {
                    let member = match member {
                        MultiMember::Empty => MultiMember::Empty,
                        MultiMember::Value(index) => MultiMember::Value(
                            LineSeq::try_new(seq_fn(&lines.as_slice()[index])?).map_err(E::from)?,
                        ),
                    };
                    members.push(member);
                }
                let axes = members
                    .iter()
                    .find_map(|member| match member {
                        MultiMember::Value(line) => Some(line.axes()),
                        MultiMember::Empty => None,
                    })
                    .unwrap_or_else(|| lines.axes());
                Self::MultiLineString(MultiLineString::from_values(axes, members).map_err(E::from)?)
            },
            Self::Polygon(polygon) => Self::Polygon(polygon.try_map_ring_seqs(seq_fn)?),
            Self::MultiPolygon(polygons) => {
                let mut members = Vec::with_capacity(polygons.member_count());
                for member in polygons.members() {
                    let member = match member {
                        MultiMember::Empty => MultiMember::Empty,
                        MultiMember::Value(index) => MultiMember::Value(
                            polygons.as_slice()[index].try_map_ring_seqs(seq_fn)?,
                        ),
                    };
                    members.push(member);
                }
                let axes = members
                    .iter()
                    .find_map(|member| match member {
                        MultiMember::Value(polygon) => Some(polygon.axes()),
                        MultiMember::Empty => None,
                    })
                    .unwrap_or_else(|| polygons.axes());
                Self::MultiPolygon(MultiPolygon::from_values(axes, members).map_err(E::from)?)
            },
            Self::GeometryCollection(geometries) => Self::GeometryCollection(
                geometries
                    .try_map_members(|geometry| geometry.try_map_coordseqs(seq_fn, point_fn))?,
            ),
            Self::Empty(..) => self.clone(),
        })
    }

    /// The grid snap WITHOUT the trailing duplicate-collapse — routes every
    /// coordinate sequence through the SIMD [`CoordSeq::try_snap_to_grid`]
    /// column kernel (the closure-based `map_xy_with` it replaced was scalar).
    fn snapped_to_grid(
        &self,
        size: (f64, f64),
        origin: (f64, f64),
    ) -> Result<Self, crate::error::Error> {
        let snap = |value: f64, origin: f64, size: f64| -> Result<f64, crate::error::Error> {
            // Same stable form as `snap_column_simd` / `stable_snap_ordinate`.
            let classic = ((value - origin) / size).round() * size + origin;
            let snapped = if classic.is_finite() {
                classic
            } else if size != 0.0 && size.is_finite() {
                let v_over = value / size;
                let o_over = origin / size;
                if v_over.is_finite() && o_over.is_finite() {
                    let k = (v_over - o_over).round();
                    let result = size * (o_over + k);
                    if result.is_finite() { result } else { classic }
                } else {
                    classic
                }
            } else {
                classic
            };
            if snapped.is_finite() {
                if same_topological_coordinate(value, snapped) {
                    Ok(value)
                } else {
                    Ok(snapped)
                }
            } else {
                Err(GeometryErrorKind::SnapGridTooFine.into())
            }
        };
        self.try_map_coordseqs(
            |seq| seq.try_snap_to_grid(size, origin),
            |point| {
                point.with_xy(
                    snap(point.x, origin.0, size.0)?,
                    snap(point.y, origin.1, size.1)?,
                )
            },
        )
    }

    /// [`snap_to_grid`](Self::snap_to_grid) with a validity guarantee:
    /// snap, linework-repair, and re-snap until the result is both
    /// grid-aligned and valid. Snap rounding converges (each pass only moves
    /// vertices onto grid nodes); a small iteration cap turns pathological
    /// non-convergence into an honest error rather than a hang. Repair may
    /// need to invent vertices (noding intersections), so Z/M-carrying
    pub(crate) fn snap_to_grid_repaired(
        &self,
        size: (f64, f64),
        origin: (f64, f64),
        domain: TopologyDomain,
    ) -> Result<Self, crate::error::Error> {
        const MAX_PASSES: usize = 8;
        let mut shape = self.snap_to_grid(size, origin)?;
        for _ in 0..MAX_PASSES {
            if validate_shape_in_frame(&shape, domain)?.is_none() {
                return Ok(shape);
            }
            shape = repair_shape_in_frame(&shape, domain, RepairMethod::Linework)?
                .expect("invalid geometry produces a repaired shape")
                .snap_to_grid(size, origin)?;
        }
        Err(GeometryErrorKind::repair_failed(
            "snap_to_grid(repair=True) did not converge onto a valid grid-aligned result",
        ))
    }

    /// Post-snap structural cleanup: strip consecutive duplicate vertices,
    /// drop collapsed parts, and degrade below-minimum linework/rings to
    /// the representable empty.
    fn collapse_snapped(self) -> Result<Self> {
        fn dedup_line<C: Coordinates + ?Sized>(points: &C) -> Vec<Point> {
            let mut vertices = Vec::with_capacity(points.coord_count());
            vertices.extend(points.iter_coords());
            dedup_consecutive_points(&mut vertices);
            vertices
        }
        fn dedup_ring(ring: &Ring) -> Result<Option<Ring>> {
            let mut vertices = dedup_line(ring);
            // Re-close: the closure duplicate just got deduped away.
            if let Some(first) = vertices.first().copied()
                && (!same_point(first, *vertices.last().expect("non-empty")) || vertices.len() == 1)
            {
                vertices.push(first);
            }
            // A ring needs three distinct vertices plus the closure.
            Ok(Ring::from_closed(CoordSeq::try_from_points(&vertices)?))
        }
        fn dedup_holes(polygon: &Polygon) -> Result<Vec<Ring>> {
            let mut holes = Vec::with_capacity(polygon.holes.len());
            for hole in polygon.holes.iter() {
                if let Some(hole) = dedup_ring(hole)? {
                    holes.push(hole);
                }
            }
            Ok(holes)
        }
        Ok(match self {
            Self::LineString(points) => {
                let vertices = dedup_line(&points);
                if vertices.len() >= 2 {
                    Self::LineString(LineSeq::try_new(CoordSeq::try_from_points(&vertices)?)?)
                } else {
                    Self::typed_empty(GeometryKind::LineString, points.axes())
                }
            },
            Self::MultiLineString(lines) => {
                let mut members = Vec::with_capacity(lines.member_count());
                for member in lines.members() {
                    let member = match member {
                        MultiMember::Empty => MultiMember::Empty,
                        MultiMember::Value(index) => {
                            let vertices = dedup_line(&lines.as_slice()[index]);
                            if vertices.len() >= 2 {
                                MultiMember::Value(LineSeq::try_new(CoordSeq::try_from_points(
                                    &vertices,
                                )?)?)
                            } else {
                                MultiMember::Empty
                            }
                        },
                    };
                    members.push(member);
                }
                Self::MultiLineString(MultiLineString::from_values(lines.axes(), members)?)
            },
            Self::Polygon(polygon) => match dedup_ring(&polygon.shell)? {
                Some(shell) => Self::Polygon(Polygon::new(shell, dedup_holes(&polygon)?)),
                None => Self::typed_empty(GeometryKind::Polygon, polygon.axes()),
            },
            Self::MultiPolygon(polygons) => {
                let mut members = Vec::with_capacity(polygons.member_count());
                for member in polygons.members() {
                    let member = match member {
                        MultiMember::Empty => MultiMember::Empty,
                        MultiMember::Value(index) => {
                            let polygon = &polygons.as_slice()[index];
                            match dedup_ring(&polygon.shell)? {
                                Some(shell) => {
                                    MultiMember::Value(Polygon::new(shell, dedup_holes(polygon)?))
                                },
                                None => MultiMember::Empty,
                            }
                        },
                    };
                    members.push(member);
                }
                Self::MultiPolygon(MultiPolygon::from_values(polygons.axes(), members)?)
            },
            Self::GeometryCollection(geometries) => {
                return geometries.try_filter_map_owned_members(|geometry| {
                    let geometry = geometry.collapse_snapped()?;
                    Ok((!geometry.is_empty()).then_some(geometry))
                });
            },
            other => other,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn subdivision_admits_exact_generated_limit_without_charging_input_vertices() {
        let steps = [GENERATED_ITEM_LIMIT + 1];
        let mut count = SubdivisionCount { emitted: 0 };
        emit_subdivision(&steps, &mut count).unwrap();
        let generated = count.generated(2).unwrap();
        assert_eq!(generated, GENERATED_ITEM_LIMIT);

        let mut budget = ExpansionBudget::new("segmentize", "max_length");
        budget.check_additional(generated).unwrap();
        budget.add(generated).unwrap();
        assert_eq!(budget.used(), GENERATED_ITEM_LIMIT);

        let mut count = SubdivisionCount { emitted: 0 };
        emit_subdivision(&[GENERATED_ITEM_LIMIT + 2], &mut count).unwrap();
        let generated = count.generated(2).unwrap();
        let over_budget = ExpansionBudget::new("segmentize", "max_length");
        let error = over_budget.check_additional(generated).unwrap_err();
        assert!(matches!(
            error.kind(),
            crate::error::ErrorKind::Geometry(
                crate::geometry::GeometryErrorKind::GeneratedOutputTooLarge {
                    operation: "segmentize",
                    parameter: "max_length",
                    ..
                }
            )
        ));
    }

    #[test]
    fn segmentize_planning_sentinel_is_reserved_for_more_than_the_limit() {
        assert_eq!(
            segmentize_steps((GENERATED_ITEM_LIMIT + 1) as f64, 1.0),
            GENERATED_ITEM_LIMIT + 1
        );
        assert_eq!(
            segmentize_steps((GENERATED_ITEM_LIMIT + 2) as f64, 1.0),
            GENERATED_ITEM_LIMIT + 2
        );
    }

    #[test]
    fn planar_segmentize_planner_keeps_the_exact_generated_boundary() {
        let segment = Segment {
            start: XY::new(0.0, 0.0),
            end: XY::new((GENERATED_ITEM_LIMIT + 1) as f64, 0.0),
        };
        assert_eq!(
            planar_segmentize_steps(segment, 1.0),
            GENERATED_ITEM_LIMIT + 1
        );
        let segment = Segment {
            start: XY::new(0.0, 0.0),
            end: XY::new((GENERATED_ITEM_LIMIT + 2) as f64, 0.0),
        };
        assert_eq!(
            planar_segmentize_steps(segment, 1.0),
            GENERATED_ITEM_LIMIT + 2
        );
    }
}

#[cfg(test)]
mod snap_reference_tests {
    use super::*;
    use crate::geometry::{MOrdinate, ZOrdinate};

    #[test]
    fn collection_children_share_reference_without_moving_disjoint_members() {
        let reference = Shape::geometry_collection(
            (0..=SNAP_INDEX_MIN_POINTS)
                .map(|index| Shape::Point(Point::new_raw_xy(index as f64, 0.0)))
                .collect(),
        )
        .unwrap();
        let far = Shape::Point(Point::new_raw_xy(-1000.0, -1000.0));
        let near = Shape::Point(Point::new_raw_xy(0.25, 0.0));
        let source = Shape::geometry_collection(vec![
            near.clone(),
            Shape::geometry_collection(vec![far.clone(), near]).unwrap(),
        ])
        .unwrap();
        let expected = Shape::geometry_collection(vec![
            Shape::Point(Point::new_raw_xy(0.0, 0.0)),
            Shape::geometry_collection(vec![far, Shape::Point(Point::new_raw_xy(0.0, 0.0))])
                .unwrap(),
        ])
        .unwrap();
        assert_eq!(source.snap(&reference, 0.5).unwrap(), expected);
    }

    fn line_reference_points(count: usize) -> Vec<XY> {
        (0..count).map(|index| XY::new(index as f64, 0.0)).collect()
    }

    #[test]
    fn storage_threshold_keeps_linear_and_indexed_insertions_identical() {
        let linear_points = line_reference_points(SNAP_INDEX_MIN_POINTS - 1);
        let mut indexed_points = linear_points.clone();
        indexed_points.push(XY::new(30.0, 10.0));
        let linear = SnapReference::build(&linear_points, 0.5);
        let indexed = SnapReference::build(&indexed_points, 0.5);
        assert!(matches!(
            &linear.storage,
            SnapReferenceStorage::Linear(points) if points.len() == SNAP_INDEX_MIN_POINTS - 1
        ));
        assert!(matches!(
            &indexed.storage,
            SnapReferenceStorage::Indexed(tree) if tree.size() == SNAP_INDEX_MIN_POINTS
        ));

        let start = Point::new_raw_xy(0.0, 0.0);
        let end = Point::new_raw_xy(30.0, 0.0);
        assert_eq!(
            linear.insertions(start, end).unwrap(),
            indexed.insertions(start, end).unwrap()
        );
    }

    #[test]
    fn indexed_nearest_keeps_the_latest_source_ordinal_on_a_distance_tie() {
        let mut points = vec![XY::new(-1.0, 0.0), XY::new(1.0, 0.0)];
        points.extend(
            (0..(SNAP_INDEX_MIN_POINTS - points.len()))
                .map(|index| XY::new(10.0 + index as f64, 10.0)),
        );
        let reference = SnapReference::build(&points, 1.0);
        let source = Point::new_raw_axes(0.0, 0.0, ZOrdinate(Some(7.0)), MOrdinate(Some(11.0)));
        let snapped = reference.snap_point(source);
        assert_eq!(snapped.x.to_bits(), 1.0_f64.to_bits());
        assert_eq!(snapped.y.to_bits(), 0.0_f64.to_bits());
        assert_eq!(snapped.z().unwrap().to_bits(), 7.0_f64.to_bits());
        assert_eq!(snapped.m().unwrap().to_bits(), 11.0_f64.to_bits());
    }

    #[test]
    fn indexed_insertion_keeps_a_boundary_candidate_after_outward_padding() {
        let mut points = vec![XY::new(1.0, 1.0)];
        points.extend(
            (0..(SNAP_INDEX_MIN_POINTS - 1)).map(|index| XY::new(10.0 + index as f64, 10.0)),
        );
        let reference = SnapReference::build(&points, 1.0);
        let insertions = reference
            .insertions(Point::new_raw_xy(0.0, 0.0), Point::new_raw_xy(2.0, 0.0))
            .unwrap();
        assert_eq!(insertions.len(), 1);
        assert_eq!(insertions[0].x.to_bits(), 1.0_f64.to_bits());
        assert_eq!(insertions[0].y.to_bits(), 1.0_f64.to_bits());
    }

    #[test]
    fn indexed_nearest_full_scans_underflowed_and_overflowed_tolerance_squares() {
        for tolerance in [f64::from_bits(1), 2.0_f64.powi(-600)] {
            let mut tiny_points = vec![XY::new(tolerance, 0.0)];
            tiny_points.extend(
                (0..(SNAP_INDEX_MIN_POINTS - 1)).map(|index| XY::new(10.0 + index as f64, 10.0)),
            );
            let tiny_reference = SnapReference::build(&tiny_points, tolerance);
            assert_eq!(
                tiny_reference
                    .snap_point(Point::new_raw_xy(0.0, 0.0))
                    .x
                    .to_bits(),
                tolerance.to_bits()
            );
        }

        let mut huge_points = vec![XY::new(1.0e307, 0.0)];
        huge_points.extend(
            (1..SNAP_INDEX_MIN_POINTS).map(|index| XY::new(1.0e307 + index as f64 * 1.0e306, 0.0)),
        );
        let huge_reference = SnapReference::build(&huge_points, 1.0e308);
        assert_eq!(
            huge_reference
                .snap_point(Point::new_raw_xy(0.0, 0.0))
                .x
                .to_bits(),
            1.0e307_f64.to_bits()
        );
    }

    #[test]
    fn inserted_source_ordinates_follow_zero_and_one_rounded_exact_ratios() {
        let zero_t = 2.0_f64.powi(-100);
        let zero_tolerance = 2.0_f64.powi(-101);
        let extent = 2.0_f64.powi(1023);
        let zero_source_start =
            Point::new_raw_axes(0.0, 0.0, ZOrdinate(Some(0.0)), MOrdinate(Some(0.0)));
        let zero_source_end = Point::new_raw_axes(
            extent,
            0.0,
            ZOrdinate(Some(extent)),
            MOrdinate(Some(extent)),
        );
        let zero_reference =
            SnapReference::build(&[XY::new(zero_t, zero_tolerance)], zero_tolerance);
        let zero_inserted = zero_reference
            .insertions(zero_source_start, zero_source_end)
            .unwrap();
        assert_eq!(zero_inserted.len(), 1);
        assert_eq!(zero_inserted[0].x.to_bits(), zero_t.to_bits());
        assert_eq!(zero_inserted[0].y.to_bits(), zero_tolerance.to_bits());
        assert_eq!(zero_inserted[0].z().unwrap().to_bits(), zero_t.to_bits());
        assert_eq!(zero_inserted[0].m().unwrap().to_bits(), zero_t.to_bits());

        let previous = 1.0_f64.next_down();
        let large = 2.0_f64.powi(1023);
        let one_source_start =
            Point::new_raw_axes(0.0, 0.0, ZOrdinate(Some(-large)), MOrdinate(Some(-large)));
        let one_source_end =
            Point::new_raw_axes(1.0, 1.0, ZOrdinate(Some(large)), MOrdinate(Some(large)));
        let one_reference = SnapReference::build(&[XY::new(1.0, previous)], 1.0e-16);
        let one_inserted = one_reference
            .insertions(one_source_start, one_source_end)
            .unwrap();
        assert_eq!(one_inserted.len(), 1);
        let expected = large.next_down().to_bits();
        assert_eq!(one_inserted[0].z().unwrap().to_bits(), expected);
        assert_eq!(one_inserted[0].m().unwrap().to_bits(), expected);
    }
}
