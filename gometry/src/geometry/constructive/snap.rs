#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
use super::*;
use crate::NonNegative;

const SNAP_INDEX_MIN_POINTS: usize = 32;

pub(crate) struct SnapReference {
    /// The reference vertices in build order (the brute scan's source; also the
    /// tie-break ordinal = index). Always kept — the tree, when built, holds a
    /// parallel copy keyed for spatial queries.
    points: Box<[Point]>,
    /// Spatial index, built only past [`SNAP_INDEX_MIN_POINTS`]; `None` selects
    /// the linear scan over `points`.
    tree: Option<BulkRTree<rstar::primitives::GeomWithData<[f64; 2], u32>>>,
    tolerance: f64,
    /// `tolerance^2`, `None` when it overflows — absurd-but-finite
    /// tolerances fall back to distance-space comparison over every entry.
    limit: Option<f64>,
}

impl SnapReference {
    pub(crate) fn build(reference_points: &[Point], tolerance: f64) -> Self {
        let limit = tolerance * tolerance;
        let tree = (reference_points.len() >= SNAP_INDEX_MIN_POINTS).then(|| {
            BulkRTree::bulk_load_with_params(
                reference_points
                    .iter()
                    .enumerate()
                    .map(|(ordinal, point)| {
                        rstar::primitives::GeomWithData::new([point.x, point.y], ordinal as u32)
                    })
                    .collect(),
            )
        });
        Self {
            points: reference_points.into(),
            tree,
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
        let mut best: Option<(f64, u32, XY)> = None;
        let mut consider = |distance: f64, ordinal: u32, position: XY| {
            if best.is_none_or(|(leader, at, _)| {
                distance < leader || (distance.to_bits() == leader.to_bits() && ordinal > at)
            }) {
                best = Some((distance, ordinal, position));
            }
        };
        // The metric (squared when the limit is representable, else distance)
        // is uniform across candidates, so `best` is a valid argmin either way.
        let mut test = |reference: XY, ordinal: u32| {
            if let Some(limit) = self.limit {
                let distance_squared = point_distance_squared(point, reference);
                if distance_squared <= limit {
                    consider(distance_squared, ordinal, reference);
                }
            } else {
                let distance = point_distance(point, reference);
                if distance <= self.tolerance {
                    consider(distance, ordinal, reference);
                }
            }
        };
        match (&self.tree, self.limit) {
            // Indexed: the tree pre-filters to the tolerance ball.
            (Some(tree), Some(limit)) => {
                for candidate in tree.locate_within_distance([point.x, point.y], limit) {
                    test(
                        XY::new(candidate.geom()[0], candidate.geom()[1]),
                        candidate.data,
                    );
                }
            },
            (Some(tree), None) => {
                for candidate in tree {
                    test(
                        XY::new(candidate.geom()[0], candidate.geom()[1]),
                        candidate.data,
                    );
                }
            },
            // Brute: scan every reference vertex (small reference).
            (None, _) => {
                for (ordinal, reference) in self.points.iter().enumerate() {
                    test(XY::new(reference.x, reference.y), ordinal as u32);
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
    pub(crate) fn insertions(&self, start: Point, end: Point) -> Vec<Point> {
        if same_point(start, end) {
            return Vec::new();
        }
        let segment = Segment {
            start: start.into(),
            end: end.into(),
        };
        let tolerance = self.tolerance;
        let limit = self.limit;
        let mut candidates: Vec<(f64, u32, Point)> = Vec::new();
        let mut consider = |reference: XY, ordinal: u32| {
            if let Some(limit) = limit {
                if point_distance_squared(reference, start) <= limit
                    || point_distance_squared(reference, end) <= limit
                    || point_segment_distance_squared(
                        Point::new_unchecked_xy(reference.x, reference.y),
                        segment,
                    ) > limit
                {
                    return;
                }
            } else if point_distance(reference, start) <= tolerance
                || point_distance(reference, end) <= tolerance
                || point_segment_distance(
                    Point::new_unchecked_xy(reference.x, reference.y),
                    segment,
                ) > tolerance
            {
                return;
            }
            let fraction = segment_projection_fraction(
                Point::new_unchecked_xy(reference.x, reference.y),
                segment,
            );
            if fraction > 0.0 && fraction < 1.0 {
                let mut point = lerp_point(start, end, fraction);
                point.x = reference.x;
                point.y = reference.y;
                candidates.push((fraction, ordinal, point));
            }
        };
        match &self.tree {
            // Indexed + a finite limit: only vertices in the segment's
            // tolerance-padded bbox can land inside (max-norm superset of the
            // euclidean ball). A non-finite limit still scans the whole tree.
            Some(tree) if limit.is_some() => {
                let (lo_x, hi_x) = (start.x.min(end.x), start.x.max(end.x));
                let (lo_y, hi_y) = (start.y.min(end.y), start.y.max(end.y));
                let window = rstar::AABB::from_corners([lo_x - tolerance, lo_y - tolerance], [
                    hi_x + tolerance,
                    hi_y + tolerance,
                ]);
                for candidate in tree.locate_in_envelope_intersecting(window) {
                    consider(
                        XY::new(candidate.geom()[0], candidate.geom()[1]),
                        candidate.data,
                    );
                }
            },
            Some(tree) => {
                for candidate in tree {
                    consider(
                        XY::new(candidate.geom()[0], candidate.geom()[1]),
                        candidate.data,
                    );
                }
            },
            // Brute: scan every reference vertex (small reference); `consider`
            // applies the exact tolerance/projection filter.
            None => {
                for (ordinal, reference) in self.points.iter().enumerate() {
                    consider(XY::new(reference.x, reference.y), ordinal as u32);
                }
            },
        }
        // Fraction order with build-order ties: identical to the linear
        // scan's input-order stable sort.
        candidates.sort_unstable_by(|a, b| a.0.total_cmp(&b.0).then(a.1.cmp(&b.1)));
        // Collapse only genuine duplicates — references at an identical
        // projected position. A fixed parametric epsilon would merge distinct
        // reference points (e.g. 0.5 and 0.5000000000005) even at tolerance 0.
        #[expect(clippy::float_cmp, reason = "exact projected-position identity")]
        candidates.dedup_by(|right, left| right.2.x == left.2.x && right.2.y == left.2.y);
        candidates.into_iter().map(|(_, _, point)| point).collect()
    }
}

pub(crate) fn snap_points_to_reference<C: Coordinates + ?Sized>(
    points: &C,
    reference: &SnapReference,
) -> Vec<Point> {
    points
        .iter_coords()
        .map(|point| reference.snap_point(point))
        .collect()
}

pub(crate) fn snap_line_to_reference<C: Coordinates + ?Sized>(
    points: &C,
    reference: &SnapReference,
) -> Vec<Point> {
    if points.coord_count() < 2 {
        return snap_points_to_reference(points, reference);
    }

    let mut result = Vec::with_capacity(points.coord_count());
    push_distinct_point(&mut result, reference.snap_point(points.nth_coord(0)));

    for [start, end] in points.segment_pairs() {
        for point in reference.insertions(start, end) {
            push_distinct_point(&mut result, point);
        }
        push_distinct_point(&mut result, reference.snap_point(end));
    }
    if result.len() == 1 {
        return vec![
            reference.snap_point(points.nth_coord(0)),
            reference.snap_point(points.nth_coord(points.coord_count() - 1)),
        ];
    }
    result
}

pub(crate) fn snap_ring_to_reference<C: Coordinates + ?Sized>(
    points: &C,
    reference: &SnapReference,
) -> Vec<Point> {
    let len = points.coord_count();
    let mut result = snap_line_to_reference(points, reference);
    if result.len() > 1 && len > 1 && same_point(points.nth_coord(0), points.nth_coord(len - 1)) {
        let first = result[0];
        if let Some(last) = result.last_mut() {
            *last = first;
        }
    }
    result
}

/// Consecutive-duplicate removal against the LAST KEPT vertex (the GEOS
/// rule): a vertex survives when it is farther than `tolerance` from the
/// previously retained one. Columnar: the keep scan reads only the XY
/// columns; survivors gather every present lane by index, so Z/M ride
/// along untouched — and an input with nothing to drop is returned as a
/// zero-copy share.
pub(crate) fn remove_repeated_points(points: &CoordSeq, tolerance: f64) -> CoordSeq {
    let (xs, ys) = (points.xs(), points.ys());
    if xs.is_empty() {
        return points.clone();
    }
    let limit = tolerance * tolerance;
    let mut keep: Vec<usize> = Vec::with_capacity(xs.len());
    keep.push(0);
    let (mut anchor_x, mut anchor_y) = (xs[0], ys[0]);
    if limit.is_finite() {
        for index in 1..xs.len() {
            let (dx, dy) = (xs[index] - anchor_x, ys[index] - anchor_y);
            if dx * dx + dy * dy > limit {
                keep.push(index);
                (anchor_x, anchor_y) = (xs[index], ys[index]);
            }
        }
    } else {
        // `tolerance * tolerance` overflowed: compare in distance space.
        for index in 1..xs.len() {
            let anchor = XY::new(anchor_x, anchor_y);
            if point_distance(anchor, XY::new(xs[index], ys[index])) > tolerance {
                keep.push(index);
                (anchor_x, anchor_y) = (xs[index], ys[index]);
            }
        }
    }
    if keep.len() == xs.len() {
        return points.clone();
    }
    points.select(keep.iter().copied())
}

pub(crate) fn remove_repeated_line_points(points: &CoordSeq, tolerance: f64) -> CoordSeq {
    let cleaned = remove_repeated_points(points, tolerance);
    if points.len() >= 2 && cleaned.len() == 1 {
        return points.select([0, points.len() - 1].into_iter());
    }
    cleaned
}

pub(crate) fn segmentize_points(points: &CoordSeq, max_segment_length: f64) -> Result<CoordSeq> {
    subdivide_columns(points, "segmentize", "max_segment_length", |segment| {
        // Guarded-sqrt distance (lengths are measurement arithmetic): the
        // libm `hypot` call was 17% of a bulk segmentize profile.
        let length = point_distance(segment.start, segment.end);
        if length == 0.0 {
            1
        } else {
            (length / max_segment_length).ceil() as usize
        }
    })
}

/// Insert `ceil(1 / fraction) - 1` evenly spaced vertices into every segment
/// (the GEOS densify semantics): each segment is split into equal pieces no
/// longer than `fraction` of its own length, so the discrete Hausdorff and
/// Fréchet metrics converge on the continuous ones. `fraction` is in
/// `(0, 1]`; `1` keeps the vertices unchanged.
pub(crate) fn densify_points(points: &CoordSeq, fraction: f64) -> Result<CoordSeq> {
    let steps = (1.0 / fraction).ceil() as usize;
    subdivide_columns(points, "densify", "fraction", |segment| {
        if same_point(segment.start, segment.end) {
            1
        } else {
            steps
        }
    })
}

/// Split every segment of a chain into `steps_for(segment)` equal pieces,
/// keeping every original vertex — the shared engine behind `segmentize`
/// (length-driven) and `densify` (fraction-driven). Columnar: every
/// ordinate interpolates straight from its column into the output column —
/// no 40-byte `Point` staging, no transpose.
pub(crate) fn subdivide_columns(
    points: &CoordSeq,
    operation: &'static str,
    parameter: &'static str,
    steps_for: impl Fn(Segment) -> usize,
) -> Result<CoordSeq> {
    let count = points.len();
    if count < 2 {
        return Ok(points.clone());
    }
    // `steps_for` runs once per segment (not per output vertex); the exact
    // output size keeps every column a single allocation.
    let steps: Vec<usize> = line_segments(points).map(steps_for).collect();
    let mut budget = ExpansionBudget::new(operation, parameter);
    for &segment_steps in &steps {
        budget.add(segment_steps.saturating_sub(1))?;
    }
    let inserted = budget.used();
    if inserted == 0 {
        // Nothing subdivides (every segment already satisfies the bound —
        // the common case once data is denser than the requested length):
        // the input IS the output, share it.
        return Ok(points.clone());
    }
    let total = count.checked_add(inserted).ok_or({
        GeometryErrorKind::GeneratedOutputTooLarge {
            operation,
            parameter,
            produced: usize::MAX,
            limit: GENERATED_ITEM_LIMIT,
        }
    })?;
    // Column-independent fractions, computed once and reused across the XYZM
    // columns: one flat `Vec<f64>` + per-segment CSR offsets instead of a
    // `Vec<Vec<f64>>` (one allocation, not one per segment).
    let mut fraction_offsets: Vec<usize> = Vec::new();
    fraction_offsets
        .try_reserve_exact(steps.len() + 1)
        .map_err(|_| {
            GeometryErrorKind::message(format!(
                "{operation} could not allocate subdivision offsets"
            ))
        })?;
    fraction_offsets.push(0);
    let mut fractions: Vec<f64> = Vec::new();
    fractions
        .try_reserve_exact(total.saturating_sub(steps.len() + 1))
        .map_err(|_| {
            GeometryErrorKind::message(format!(
                "{operation} could not allocate {total} output coordinates"
            ))
        })?;
    for &segment_steps in &steps {
        for step in 1..segment_steps {
            // The division stays: a hoisted reciprocal shifts fractions by an
            // ulp (changing printed vertex coordinates) for a win lost in noise.
            fractions.push(step as f64 / segment_steps as f64);
        }
        fraction_offsets.push(fractions.len());
    }
    let interpolated = |column: &[f64]| -> Result<Box<[f64]>> {
        let mut out = Vec::new();
        out.try_reserve_exact(total).map_err(|_| {
            GeometryErrorKind::message(format!(
                "{operation} could not allocate {total} output coordinates"
            ))
        })?;
        out.push(column[0]);
        for index in 0..steps.len() {
            let (start, end) = (column[index], column[index + 1]);
            let segment_steps = steps[index];
            let fracs = &fractions[fraction_offsets[index]..fraction_offsets[index + 1]];
            for (step, &fraction) in fracs.iter().enumerate() {
                let step = step + 1;
                let value = interpolate_f64(start, end, fraction);
                // Finite-input convex interpolation only overflows at the
                // absolute f64 extreme; fall back to the nearer endpoint
                // (the per-point rescue of `interpolate_segment_point`).
                out.push(if value.is_finite() {
                    value
                } else if step * 2 <= segment_steps {
                    start
                } else {
                    end
                });
            }
            out.push(end);
        }
        Ok(out.into_boxed_slice())
    };
    Ok(CoordSeq::from_columns(
        interpolated(points.xs())?.into(),
        interpolated(points.ys())?.into(),
        points.zs().map(interpolated).transpose()?.map(Into::into),
        points.ms().map(interpolated).transpose()?.map(Into::into),
    ))
}

impl Shape {
    pub fn snap(&self, reference: &Self, tolerance: f64) -> Result<Self> {
        let tolerance = NonNegative::try_new("tolerance", tolerance)?.get();
        // Boxes farther apart than the tolerance snap to the identity: no
        // reference vertex can be within tolerance of any vertex OR segment
        // (both sit inside their bounds), so the scans cannot change a thing.
        if self
            .bounds()
            .zip(reference.bounds())
            .is_some_and(|(left, right)| {
                bounds_distance_squared(left, right) > tolerance * tolerance
            })
        {
            return Ok(self.clone());
        }
        let reference_points = reference.unique_xy_points();
        if reference_points.is_empty() {
            return Ok(self.clone());
        }
        let engine = SnapReference::build(&reference_points, tolerance);
        Ok(match self {
            Self::Point(point) => Self::Point(engine.snap_point(*point)),
            Self::MultiPoint(points) => {
                Self::MultiPoint(snap_points_to_reference(points, &engine).into())
            },
            Self::LineString(points) => Self::LineString(LineSeq::from_trusted(CoordSeq::from(
                snap_line_to_reference(points, &engine),
            ))),
            Self::MultiLineString(lines) => Self::MultiLineString(
                lines
                    .iter()
                    .map(|line| {
                        LineSeq::from_trusted(CoordSeq::from(snap_line_to_reference(line, &engine)))
                    })
                    .collect(),
            ),
            Self::Polygon(polygon) => Self::Polygon(polygon.snap_to_reference(&engine)),
            Self::MultiPolygon(polygons) => Self::MultiPolygon(
                polygons
                    .iter()
                    .map(|polygon| polygon.snap_to_reference(&engine))
                    .collect(),
            ),
            Self::GeometryCollection(geometries) => Self::GeometryCollection(
                geometries
                    .iter()
                    .map(|geometry| geometry.snap(reference, tolerance))
                    .collect::<Result<_, _>>()?,
            ),
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
        Ok(self.snapped_to_grid(size, origin)?.collapse_snapped())
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
    ) -> Result<Self, E> {
        Ok(match self {
            Self::Point(point) => Self::Point(point_fn(point)?),
            Self::MultiPoint(points) => Self::MultiPoint(seq_fn(points)?),
            Self::LineString(points) => Self::LineString(LineSeq::from_trusted(seq_fn(points)?)),
            Self::MultiLineString(lines) => Self::MultiLineString(
                lines
                    .iter()
                    .map(|line| seq_fn(line).map(LineSeq::from_trusted))
                    .collect::<Result<_, _>>()?,
            ),
            Self::Polygon(polygon) => Self::Polygon(polygon.try_map_ring_seqs(seq_fn)?),
            Self::MultiPolygon(polygons) => Self::MultiPolygon(
                polygons
                    .iter()
                    .map(|polygon| polygon.try_map_ring_seqs(seq_fn))
                    .collect::<Result<_, _>>()?,
            ),
            Self::GeometryCollection(geometries) => Self::GeometryCollection(
                geometries
                    .iter()
                    .map(|geometry| geometry.try_map_coordseqs(seq_fn, point_fn))
                    .collect::<Result<_, _>>()?,
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
            let snapped = ((value - origin) / size).round() * size + origin;
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
    pub fn snap_to_grid_repaired(
        &self,
        size: (f64, f64),
        origin: (f64, f64),
        geographic: bool,
    ) -> Result<Self, crate::error::Error> {
        const MAX_PASSES: usize = 8;
        let mut shape = self.snap_to_grid(size, origin)?;
        for _ in 0..MAX_PASSES {
            if validate_shape_in_frame(&shape, geographic).is_none() {
                return Ok(shape);
            }
            shape = repair_shape_in_frame(&shape, geographic, RepairMethod::Linework)?
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
    fn collapse_snapped(self) -> Self {
        fn dedup_line<C: Coordinates + ?Sized>(points: &C) -> Vec<Point> {
            let mut vertices: Vec<Point> = points.iter_coords().collect();
            dedup_consecutive_points(&mut vertices);
            vertices
        }
        fn dedup_ring(ring: &Ring) -> Option<Ring> {
            let mut vertices = dedup_line(ring);
            // Re-close: the closure duplicate just got deduped away.
            if let Some(first) = vertices.first().copied()
                && (!same_point(first, *vertices.last().expect("non-empty")) || vertices.len() == 1)
            {
                vertices.push(first);
            }
            // A ring needs three distinct vertices plus the closure.
            (vertices.len() >= 4).then(|| Ring::from_trusted_closed(vertices))
        }
        match self {
            Self::LineString(points) => {
                let vertices = dedup_line(&points);
                if vertices.len() >= 2 {
                    Self::LineString(
                        LineSeq::try_new(CoordSeq::from(vertices))
                            .expect("deduplicated line keeps at least two vertices"),
                    )
                } else {
                    Self::LineString(LineSeq::empty(points.axes()))
                }
            },
            Self::MultiLineString(lines) => Self::MultiLineString(
                lines
                    .iter()
                    .map(dedup_line::<LineSeq>)
                    .filter(|vertices| vertices.len() >= 2)
                    .map(CoordSeq::from)
                    .map(LineSeq::from_trusted)
                    .collect(),
            ),
            Self::Polygon(polygon) => match dedup_ring(&polygon.shell) {
                Some(shell) => Self::Polygon(Polygon::new(
                    shell,
                    polygon.holes.iter().filter_map(dedup_ring).collect(),
                )),
                None => Self::empty_polygon(),
            },
            Self::MultiPolygon(polygons) => Self::MultiPolygon(
                polygons
                    .iter()
                    .filter_map(|polygon| {
                        dedup_ring(&polygon.shell).map(|shell| {
                            Polygon::new(
                                shell,
                                polygon.holes.iter().filter_map(dedup_ring).collect(),
                            )
                        })
                    })
                    .collect(),
            ),
            Self::GeometryCollection(geometries) => Self::GeometryCollection(
                geometries
                    .into_iter()
                    .map(Self::collapse_snapped)
                    .filter(|geometry| !geometry.is_empty())
                    .collect(),
            ),
            other => other,
        }
    }
}
