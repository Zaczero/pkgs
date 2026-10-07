use std::convert::Infallible;
use std::ops::ControlFlow;

use crate::NonNegative;
use crate::geometry::constructive::Result;
use crate::geometry::{
    Bounds, BulkRTree, CoordSeq, Coordinates, GeometryErrorKind, GeometryKind, LineSeq,
    MultiLineString, MultiMember, MultiPolygon, Point, Polygon, RepairMethod, Ring, RunIdentity,
    Segment, SegmentProjection, Shape, SourceArcMetricAccess, TopologyDomain, XY,
    bounds_distance_squared, dedup_consecutive_points, point_distance,
    point_segment_distance_and_projection, points_dwithin, push_distinct_xy_position,
    repair_shape_in_frame, same_point, same_topological_coordinate, validate_shape_in_frame,
};

const SNAP_INDEX_MIN_POINTS: usize = 32;

type SnapTree = BulkRTree<[f64; 2]>;

enum SnapReferenceStorage {
    Linear(Vec<XY>),
    Indexed(SnapTree),
}

pub(crate) struct SnapReference {
    /// Reference XY values stay in build order when linear and become point
    /// primitives in the spatial index for larger sets.
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
            for point in reference_points {
                entries.push([point.x, point.y]);
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
    /// tolerance wins, then lexicographically smallest X/Y breaks exact ties.
    fn nearest(&self, point: Point) -> Option<XY> {
        let mut best: Option<(f64, XY)> = None;
        let mut consider = |distance: f64, position: XY| {
            if best.is_none_or(|(leader, leader_position)| {
                distance < leader
                    || (distance.to_bits() == leader.to_bits()
                        && compare_xy(position, leader_position).is_lt())
            }) {
                best = Some((distance, position));
            }
        };
        // The metric (squared when the limit is representable, else distance)
        // is uniform across candidates, so `best` is a valid argmin either way.
        let mut test = |reference: XY| {
            // Honest dwithin — squared false-zero must not treat distinct
            // subnormals as within a zero (or tiny) tolerance.
            if points_dwithin(point, reference, self.tolerance) {
                let distance = point_distance(point, reference);
                consider(distance, reference);
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
                        test(XY::new(candidate[0], candidate[1]));
                    }
                } else {
                    for candidate in tree {
                        test(XY::new(candidate[0], candidate[1]));
                    }
                }
            },
            // Zero/underflowed limit or non-finite tolerance²: full scan +
            // honest points_dwithin (cannot trust squared-space radius).
            (SnapReferenceStorage::Indexed(tree), _) => {
                for candidate in tree {
                    test(XY::new(candidate[0], candidate[1]));
                }
            },
            // Brute: scan every reference vertex (small reference).
            (SnapReferenceStorage::Linear(points), _) => {
                for &reference in points {
                    test(reference);
                }
            },
        }
        best.map(|(_, position)| position)
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
        let mut candidates: Vec<(SegmentProjection, Point)> = Vec::new();
        let mut consider = |reference: XY| -> crate::error::Result<()> {
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
                candidates.push((projection, point));
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
                        consider(XY::new(candidate[0], candidate[1]))?;
                    }
                } else {
                    for candidate in tree {
                        consider(XY::new(candidate[0], candidate[1]))?;
                    }
                }
            },
            (SnapReferenceStorage::Indexed(tree), _) => {
                for candidate in tree {
                    consider(XY::new(candidate[0], candidate[1]))?;
                }
            },
            // Brute: scan every reference vertex (small reference); `consider`
            // applies the exact tolerance/projection filter.
            (SnapReferenceStorage::Linear(points), _) => {
                for &reference in points {
                    consider(reference)?;
                }
            },
        }
        // Equal projections use the reference XY set rather than its input order.
        crate::geometry::try_sort_by(&mut candidates, |a, b| {
            Ok(a.0
                .cmp_along(&b.0)?
                .then_with(|| compare_xy(XY::new(a.1.x, a.1.y), XY::new(b.1.x, b.1.y))))
        })?;
        // Collapse only genuine duplicates — references at an identical
        // projected position. A fixed parametric epsilon would merge distinct
        // reference points (e.g. 0.5 and 0.5000000000005) even at tolerance 0.
        #[expect(clippy::float_cmp, reason = "exact projected-position identity")]
        candidates.dedup_by(|right, left| right.1.x == left.1.x && right.1.y == left.1.y);
        let mut output = Vec::with_capacity(candidates.len());
        for (_, point) in candidates {
            output.push(point);
        }
        Ok(output)
    }
}

fn compare_xy(left: XY, right: XY) -> std::cmp::Ordering {
    left.x
        .total_cmp(&right.x)
        .then_with(|| left.y.total_cmp(&right.y))
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
    push_distinct_xy_position(&mut result, reference.snap_point(points.nth_coord(0)));

    for [start, end] in points.segment_pairs() {
        for point in reference.insertions(start, end)? {
            push_distinct_xy_position(&mut result, point);
        }
        push_distinct_xy_position(&mut result, reference.snap_point(end));
    }
    if result.len() == 1 {
        result.clear();
        result.extend([
            reference.snap_point(points.nth_coord(0)),
            reference.snap_point(points.nth_coord(points.coord_count().wrapping_sub(1))),
        ]);
    }
    Ok(result)
}

pub(crate) fn snap_ring_to_reference<C: Coordinates + ?Sized>(
    points: &C,
    reference: &SnapReference,
) -> crate::error::Result<Vec<Point>> {
    let len = points.coord_count();
    let mut result = snap_line_to_reference(points, reference)?;
    if result.len() > 1
        && len > 1
        && same_point(points.nth_coord(0), points.nth_coord(len.wrapping_sub(1)))
    {
        let first = *result
            .first()
            .expect("a multi-coordinate snapped ring has a first point");
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
    let (mut anchor_x, mut anchor_y) = (
        *xs.first().expect("nonempty coordinate column"),
        *ys.first().expect("nonempty coordinate column"),
    );
    // Honest distance compare: squared false-zero must not collapse distinct
    // subnormals under a zero (or tiny) tolerance.
    for (index, (&x, &y)) in xs.iter().zip(ys).enumerate().skip(1) {
        let anchor = XY::new(anchor_x, anchor_y);
        let candidate = XY::new(x, y);
        if !points_dwithin(anchor, candidate, tolerance) {
            keep.push(index);
            (anchor_x, anchor_y) = (x, y);
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
    let (mut anchor_x, mut anchor_y) = (
        *xs.first().expect("nonempty ring X column"),
        *ys.first().expect("nonempty ring Y column"),
    );
    for (index, (&x, &y)) in xs.iter().zip(ys).enumerate().take(closing).skip(1) {
        if !points_dwithin(XY::new(anchor_x, anchor_y), XY::new(x, y), tolerance) {
            keep.push(index);
            (anchor_x, anchor_y) = (x, y);
        }
    }
    let close = XY::new(
        *xs.last().expect("nonempty ring X column"),
        *ys.last().expect("nonempty ring Y column"),
    );
    while keep.len() > 1
        && keep.last().is_some_and(|&last| {
            points_dwithin(
                XY::new(
                    *xs.get(last)
                        .expect("kept ring index belongs to its X column"),
                    *ys.get(last)
                        .expect("kept ring index belongs to its Y column"),
                ),
                close,
                tolerance,
            )
        })
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
        return points.select([0, points.len().wrapping_sub(1)].into_iter());
    }
    Ok(cleaned)
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
        let mut reference_points = Vec::new();
        if let Some(capacity) = reference.point_set_capacity_hint(RunIdentity::XY) {
            let _reservation = reference_points.try_reserve(capacity);
        }
        let ControlFlow::Continue(()): ControlFlow<Infallible> = reference
            .try_for_each_distinct_xy_point(|point| {
                reference_points.push(point.xy());
                ControlFlow::Continue(())
            })?;
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
                    .members_with_values()
                    .map(|member| match member {
                        MultiMember::Empty => Ok(MultiMember::Empty),
                        MultiMember::Value(polygon) => Ok(polygon
                            .snap_to_reference(engine)?
                            .map_or(MultiMember::Empty, MultiMember::Value)),
                    })
                    .collect::<Result<Vec<_>>>()?;
                Self::MultiPolygon(MultiPolygon::try_new(polygons.axes(), members)?)
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
                for member in lines.members_with_values() {
                    let member = match member {
                        MultiMember::Empty => MultiMember::Empty,
                        MultiMember::Value(line) => {
                            MultiMember::Value(LineSeq::try_new(seq_fn(line)?).map_err(E::from)?)
                        },
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
                Self::MultiLineString(MultiLineString::try_new(axes, members).map_err(E::from)?)
            },
            Self::Polygon(polygon) => Self::Polygon(polygon.try_map_ring_seqs(seq_fn)?),
            Self::MultiPolygon(polygons) => {
                let mut members = Vec::with_capacity(polygons.member_count());
                for member in polygons.members_with_values() {
                    let member = match member {
                        MultiMember::Empty => MultiMember::Empty,
                        MultiMember::Value(polygon) => {
                            MultiMember::Value(polygon.try_map_ring_seqs(seq_fn)?)
                        },
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
                Self::MultiPolygon(MultiPolygon::try_new(axes, members).map_err(E::from)?)
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
        metric_access: Option<&SourceArcMetricAccess<'_>>,
    ) -> Result<Self, crate::error::Error> {
        const MAX_PASSES: usize = 8;
        let mut shape = self.snap_to_grid(size, origin)?;
        for _ in 0..MAX_PASSES {
            if validate_shape_in_frame(&shape, domain)?.is_none() {
                return Ok(shape);
            }
            shape = repair_shape_in_frame(&shape, domain, RepairMethod::Linework, metric_access)?
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
                for member in lines.members_with_values() {
                    let member = match member {
                        MultiMember::Empty => MultiMember::Empty,
                        MultiMember::Value(line) => {
                            let vertices = dedup_line(line);
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
                Self::MultiLineString(MultiLineString::try_new(lines.axes(), members)?)
            },
            Self::Polygon(polygon) => match dedup_ring(&polygon.shell)? {
                Some(shell) => Self::Polygon(Polygon::new(shell, dedup_holes(&polygon)?)),
                None => Self::typed_empty(GeometryKind::Polygon, polygon.axes()),
            },
            Self::MultiPolygon(polygons) => {
                let mut members = Vec::with_capacity(polygons.member_count());
                for member in polygons.members_with_values() {
                    let member = match member {
                        MultiMember::Empty => MultiMember::Empty,
                        MultiMember::Value(polygon) => match dedup_ring(&polygon.shell)? {
                            Some(shell) => {
                                MultiMember::Value(Polygon::new(shell, dedup_holes(polygon)?))
                            },
                            None => MultiMember::Empty,
                        },
                    };
                    members.push(member);
                }
                Self::MultiPolygon(MultiPolygon::try_new(polygons.axes(), members)?)
            },
            Self::GeometryCollection(geometries) => {
                return geometries.try_filter_map_owned_members(|geometry| {
                    let geometry = geometry.collapse_snapped()?;
                    Ok((!geometry.is_empty()).then_some(geometry))
                });
            },
            Self::Point(_) | Self::MultiPoint(_) | Self::Empty(..) => self,
        })
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
    fn indexed_nearest_breaks_a_distance_tie_by_smallest_xy() {
        let mut points = vec![XY::new(-1.0, 0.0), XY::new(1.0, 0.0)];
        points.extend(
            (0..(SNAP_INDEX_MIN_POINTS - points.len()))
                .map(|index| XY::new(10.0 + index as f64, 10.0)),
        );
        let mut reversed = points.clone();
        reversed.swap(0, 1);
        let source = Point::new_raw_axes(0.0, 0.0, ZOrdinate(Some(7.0)), MOrdinate(Some(11.0)));
        for points in [points, reversed] {
            let reference = SnapReference::build(&points, 1.0);
            let snapped = reference.snap_point(source);
            assert_eq!(snapped.x.to_bits(), (-1.0_f64).to_bits());
            assert_eq!(snapped.y.to_bits(), 0.0_f64.to_bits());
            assert_eq!(snapped.z().unwrap().to_bits(), 7.0_f64.to_bits());
            assert_eq!(snapped.m().unwrap().to_bits(), 11.0_f64.to_bits());
        }
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
