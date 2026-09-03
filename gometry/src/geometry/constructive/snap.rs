use crate::NonNegative;
use crate::geographic::GeographicFrame;
use crate::geometry::constructive::Result;
use crate::geometry::{
    BulkRTree, CoordSeq, Coordinates, ExpansionBudget, GENERATED_ITEM_LIMIT, GeometryErrorKind,
    GeometryKind, LineSeq, MultiLineString, MultiMember, MultiPolygon, Point, Polygon,
    RepairMethod, Ring, Segment, SegmentProjection, Shape, TopologyDomain, XY,
    bounds_distance_squared, dedup_consecutive_points, interpolate_f64, line_segments,
    point_distance, point_segment_distance, points_dwithin, push_distinct_point,
    repair_shape_in_frame, same_point, same_topological_coordinate, segment_projection,
    validate_shape_in_frame,
};

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
    /// `tolerance²`, `None` when it overflows — absurd-but-finite
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
            // Honest dwithin — squared false-zero must not treat distinct
            // subnormals as within a zero (or tiny) tolerance.
            if points_dwithin(point, reference, self.tolerance) {
                let distance = point_distance(point, reference);
                consider(distance, ordinal, reference);
            }
        };
        match (&self.tree, self.limit) {
            // Indexed with an honest squared ball (limit > 0): pre-filter.
            (Some(tree), Some(limit)) if limit > 0.0 => {
                for candidate in tree.locate_within_distance([point.x, point.y], limit) {
                    test(
                        XY::new(candidate.geom()[0], candidate.geom()[1]),
                        candidate.data,
                    );
                }
            },
            // Zero/underflowed limit or non-finite tolerance²: full scan +
            // honest points_dwithin (cannot trust squared-space radius).
            (Some(tree), _) => {
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
        let mut candidates: Vec<(SegmentProjection, u32, Point)> = Vec::new();
        let mut consider = |reference: XY, ordinal: u32| {
            let ref_pt = Point::new_raw_xy(reference.x, reference.y);
            // Skip if within tolerance of either endpoint, or farther than
            // tolerance from the open segment (honest distance, not squared).
            if points_dwithin(reference, start, tolerance)
                || points_dwithin(reference, end, tolerance)
                || point_segment_distance(ref_pt, segment) > tolerance
            {
                return;
            }
            let projection = segment_projection(ref_pt, segment);
            if !projection.is_start() && !projection.is_end() {
                let mut point = projection.interpolate_point(start, end);
                point.x = reference.x;
                point.y = reference.y;
                candidates.push((projection, ordinal, point));
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
        candidates.sort_unstable_by(|a, b| a.0.cmp_along(&b.0).then(a.1.cmp(&b.1)));
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

pub(crate) fn segmentize_points_budgeted(
    points: &CoordSeq,
    max_segment_length: f64,
    placement: SegmentPlacement<'_>,
    budget: &mut ExpansionBudget,
) -> Result<CoordSeq> {
    // Measure a segment the same way the vertices will be placed, or the step
    // count and the geometry disagree: a geodesic placement measured planarly
    // would emit pieces longer than requested near the poles.
    let length_of = |segment: Segment| match placement {
        SegmentPlacement::Planar => Ok(point_distance(segment.start, segment.end)),
        SegmentPlacement::Geodesic(geodesic, frame) => {
            let start = frame
                .to_geodesic_point(Point::new_raw_xy(segment.start.x, segment.start.y))
                .expect("geographic subdivision received admitted coordinates");
            let end = frame
                .to_geodesic_point(Point::new_raw_xy(segment.end.x, segment.end.y))
                .expect("geographic subdivision received admitted coordinates");
            crate::crs::geodesic_line_solution_const::<false>(
                geodesic, start.x, start.y, end.x, end.y,
            )
            .map(|(_, _, _, total)| total)
        },
    };
    subdivide_columns(
        points,
        placement,
        "segmentize",
        |segment| length_of(segment).map(|length| segmentize_steps(length, max_segment_length)),
        budget,
    )
}

fn segmentize_steps(length: f64, max_segment_length: f64) -> usize {
    if length == 0.0 || !length.is_finite() || max_segment_length <= 0.0 {
        return 1;
    }
    // A huge quotient must not wrap to a small step count, which would admit
    // output beyond the shared 16M generated-work budget.
    let pieces = (length / max_segment_length).ceil();
    if !pieces.is_finite() || pieces > GENERATED_ITEM_LIMIT as f64 {
        // The sink charges inserted vertices (`pieces - 1`), so its sentinel
        // must remain one past that limit rather than merely one past pieces.
        GENERATED_ITEM_LIMIT.saturating_add(2)
    } else {
        pieces as usize
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
    let steps = (1.0 / fraction).ceil() as usize;
    subdivide_columns(
        points,
        placement,
        "densify",
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
    const fn generated(&self, input_len: usize) -> usize {
        self.emitted.saturating_sub(input_len)
    }
}

impl SubdivisionSink for SubdivisionCount {
    fn source(&mut self, _: usize) -> Result<()> {
        self.emitted = self.emitted.saturating_add(1);
        Ok(())
    }

    fn interior(&mut self, _: usize, steps: usize) -> Result<()> {
        self.emitted = self.emitted.saturating_add(steps.saturating_sub(1));
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
    placement: SegmentPlacement<'a>,
    xs: Vec<f64>,
    ys: Vec<f64>,
    zs: Option<Vec<f64>>,
    ms: Option<Vec<f64>>,
}

impl<'a> SubdivisionColumns<'a> {
    fn new(
        points: &'a CoordSeq,
        placement: SegmentPlacement<'a>,
        operation: &'static str,
        total: usize,
    ) -> Result<Self> {
        let reserve = |column: &str| -> Result<Vec<f64>> {
            let mut values = Vec::new();
            values.try_reserve_exact(total).map_err(|_| {
                GeometryErrorKind::message(format!(
                    "{operation} could not allocate {total} output {column} ordinates"
                ))
            })?;
            Ok(values)
        };
        Ok(Self {
            points,
            placement,
            xs: reserve("x")?,
            ys: reserve("y")?,
            zs: points.zs().map(|_| reserve("z")).transpose()?,
            ms: points.ms().map(|_| reserve("m")).transpose()?,
        })
    }

    fn finish(self) -> CoordSeq {
        CoordSeq::from_columns(
            self.xs.into_boxed_slice().into(),
            self.ys.into_boxed_slice().into(),
            self.zs.map(|column| column.into_boxed_slice().into()),
            self.ms.map(|column| column.into_boxed_slice().into()),
        )
    }

    fn push_interpolated(
        output: &mut Vec<f64>,
        column: &[f64],
        segment_index: usize,
        steps: usize,
    ) {
        let (start, end) = (column[segment_index], column[segment_index + 1]);
        for step in 1..steps {
            let value = interpolate_f64(start, end, step as f64 / steps as f64);
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

impl SubdivisionColumns<'_> {
    /// Place `steps - 1` interior vertices at equal geodesic distances along
    /// the segment. One inverse solve per segment, then one direct solve per
    /// inserted vertex.
    ///
    /// A degenerate segment is handled by the shared geodesic solver. A solver
    /// failure is reported rather than replaced with a planar lon/lat point:
    /// the latter would change the metric curve while preserving its vertex
    /// count.
    fn push_geodesic(
        &mut self,
        geodesic: &geographiclib_rs::Geodesic,
        domain: TopologyDomain,
        segment_index: usize,
        steps: usize,
    ) -> Result<()> {
        let (xs, ys) = (self.points.xs(), self.points.ys());
        let (mut x0, mut y0) = (xs[segment_index], ys[segment_index]);
        let (mut x1, mut y1) = (xs[segment_index + 1], ys[segment_index + 1]);
        if let TopologyDomain::Geographic(frame) = domain {
            let start = frame
                .to_geodesic_point(Point::new_raw_xy(x0, y0))
                .expect("geographic subdivision received admitted coordinates");
            let end = frame
                .to_geodesic_point(Point::new_raw_xy(x1, y1))
                .expect("geographic subdivision received admitted coordinates");
            (x0, y0) = (start.x, start.y);
            (x1, y1) = (end.x, end.y);
        }
        let solution = crate::crs::geodesic_line_solution_const::<false>(geodesic, x0, y0, x1, y1)?;
        for step in 1..steps {
            let fraction = step as f64 / steps as f64;
            let equatorial = y0 == 0.0 && y1 == 0.0 && (x1 - x0).abs() < 180.0;
            let (mut longitude, mut latitude) = if equatorial {
                (interpolate_f64(x0, x1, fraction), 0.0)
            } else {
                let info = crate::crs::geodesic_interpolate_on_line_const::<false, true>(
                    geodesic, solution, fraction,
                )?;
                (info.longitude, info.latitude)
            };
            if let TopologyDomain::Geographic(frame) = domain {
                let point = frame
                    .from_geodesic_point(Point::new_raw_xy(longitude, latitude))
                    .expect("geographic subdivision produced a finite coordinate");
                (longitude, latitude) = (point.x, point.y);
            }
            if !(longitude.is_finite() && latitude.is_finite()) {
                return Err(GeometryErrorKind::message(
                    "geodesic subdivision calculation produced a non-finite coordinate",
                ));
            }
            self.xs.push(longitude);
            self.ys.push(latitude);
        }
        Ok(())
    }
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
        match self.placement {
            SegmentPlacement::Planar => {
                Self::push_interpolated(&mut self.xs, self.points.xs(), segment_index, steps);
                Self::push_interpolated(&mut self.ys, self.points.ys(), segment_index, steps);
            },
            SegmentPlacement::Geodesic(geodesic, frame) => {
                self.push_geodesic(
                    geodesic,
                    TopologyDomain::Geographic(frame),
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

/// Split every segment of a chain into `steps_for(segment)` equal pieces,
/// keeping every original vertex — the shared engine behind `segmentize`
/// (length-driven) and `densify` (fraction-driven). Columnar: every
/// Z/M ordinate interpolates straight from its column into the output column;
/// XY follows `placement` — no 40-byte `Point` staging, no transpose.
pub(crate) fn subdivide_columns(
    points: &CoordSeq,
    placement: SegmentPlacement<'_>,
    operation: &'static str,
    steps_for: impl Fn(Segment) -> Result<usize>,
    budget: &mut ExpansionBudget,
) -> Result<CoordSeq> {
    let count = points.len();
    if count < 2 {
        return Ok(points.clone());
    }
    // `steps_for` runs once per segment (not per output vertex); the exact
    // output size keeps every column a single allocation.
    let steps: Vec<usize> = line_segments(points)
        .map(steps_for)
        .collect::<Result<_>>()?;
    let mut count_sink = SubdivisionCount { emitted: 0 };
    emit_subdivision(&steps, &mut count_sink)?;
    let total = count_sink.emitted;
    // Existing source vertices are input-sized work. Charge only the vertices
    // this operation creates, once for the whole caller-owned budget.
    budget.add(count_sink.generated(count))?;
    if count_sink.generated(count) == 0 {
        // Nothing subdivides (every segment already satisfies the bound —
        // the common case once data is denser than the requested length):
        // the input IS the output, share it.
        return Ok(points.clone());
    }
    let mut output = SubdivisionColumns::new(points, placement, operation, total)?;
    emit_subdivision(&steps, &mut output)?;
    Ok(output.finish())
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
                Self::MultiPoint(points.map_values(|point| engine.snap_point(point)))
            },
            Self::LineString(points) => Self::LineString(
                LineSeq::try_new(CoordSeq::from(snap_line_to_reference(points, &engine)))
                    .expect("line coordinate sequence must contain at least two vertices"),
            ),
            Self::MultiLineString(lines) => {
                Self::MultiLineString(lines.map_values(lines.axes(), |line| {
                    LineSeq::try_new(CoordSeq::from(snap_line_to_reference(line, &engine)))
                        .expect("line coordinate sequence must contain at least two vertices")
                }))
            },
            Self::Polygon(polygon) => Self::Polygon(polygon.snap_to_reference(&engine)),
            Self::MultiPolygon(polygons) => {
                Self::MultiPolygon(polygons.map_values(polygons.axes(), |polygon| {
                    polygon.snap_to_reference(&engine)
                }))
            },
            Self::GeometryCollection(geometries) => Self::GeometryCollection(
                geometries.try_map_members(|geometry| geometry.snap(reference, tolerance))?,
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
            Self::LineString(points) => Self::LineString(
                LineSeq::try_new(seq_fn(points)?)
                    .expect("line coordinate sequence must contain at least two vertices"),
            ),
            Self::MultiLineString(lines) => {
                let members = lines
                    .members()
                    .map(|member| match member {
                        MultiMember::Empty => Ok(MultiMember::Empty),
                        MultiMember::Value(index) => seq_fn(&lines.as_slice()[index])
                            .map(|coords| {
                                LineSeq::try_new(coords)
                                    .expect("coordinate transform preserves line cardinality")
                            })
                            .map(MultiMember::Value),
                    })
                    .collect::<Result<Vec<_>, E>>()?;
                let axes = members
                    .iter()
                    .find_map(|member| match member {
                        MultiMember::Value(line) => Some(line.axes()),
                        MultiMember::Empty => None,
                    })
                    .unwrap_or(lines.axes());
                Self::MultiLineString(MultiLineString::from_values(axes, members))
            },
            Self::Polygon(polygon) => Self::Polygon(polygon.try_map_ring_seqs(seq_fn)?),
            Self::MultiPolygon(polygons) => {
                let members = polygons
                    .members()
                    .map(|member| match member {
                        MultiMember::Empty => Ok(MultiMember::Empty),
                        MultiMember::Value(index) => polygons.as_slice()[index]
                            .try_map_ring_seqs(seq_fn)
                            .map(MultiMember::Value),
                    })
                    .collect::<Result<Vec<_>, E>>()?;
                let axes = members
                    .iter()
                    .find_map(|member| match member {
                        MultiMember::Value(polygon) => Some(polygon.axes()),
                        MultiMember::Empty => None,
                    })
                    .unwrap_or(polygons.axes());
                Self::MultiPolygon(MultiPolygon::from_values(axes, members))
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
            if validate_shape_in_frame(&shape, domain).is_none() {
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
            (vertices.len() >= 4).then(|| Ring::from_closed(vertices))
        }
        Ok(match self {
            Self::LineString(points) => {
                let vertices = dedup_line(&points);
                if vertices.len() >= 2 {
                    Self::LineString(
                        LineSeq::try_new(CoordSeq::from(vertices))
                            .expect("deduplicated line keeps at least two vertices"),
                    )
                } else {
                    Self::typed_empty(GeometryKind::LineString, points.axes())
                }
            },
            Self::MultiLineString(lines) => {
                let members = lines
                    .members()
                    .map(|member| match member {
                        MultiMember::Empty => MultiMember::Empty,
                        MultiMember::Value(index) => {
                            let vertices = dedup_line(&lines.as_slice()[index]);
                            if vertices.len() >= 2 {
                                MultiMember::Value(LineSeq::try_new(CoordSeq::from(vertices)).expect("line coordinate sequence must contain at least two vertices"))
                            } else {
                                MultiMember::Empty
                            }
                        },
                    })
                    .collect();
                Self::MultiLineString(MultiLineString::from_values(lines.axes(), members))
            },
            Self::Polygon(polygon) => match dedup_ring(&polygon.shell) {
                Some(shell) => Self::Polygon(Polygon::new(
                    shell,
                    polygon.holes.iter().filter_map(dedup_ring).collect(),
                )),
                None => Self::typed_empty(GeometryKind::Polygon, polygon.axes()),
            },
            Self::MultiPolygon(polygons) => {
                let members = polygons
                    .members()
                    .map(|member| match member {
                        MultiMember::Empty => MultiMember::Empty,
                        MultiMember::Value(index) => {
                            let polygon = &polygons.as_slice()[index];
                            dedup_ring(&polygon.shell)
                                .map(|shell| {
                                    MultiMember::Value(Polygon::new(
                                        shell,
                                        polygon.holes.iter().filter_map(dedup_ring).collect(),
                                    ))
                                })
                                .unwrap_or(MultiMember::Empty)
                        },
                    })
                    .collect();
                Self::MultiPolygon(MultiPolygon::from_values(polygons.axes(), members))
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
