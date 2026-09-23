#![expect(
    clippy::similar_names,
    reason = "axis and formula symbols name paired quantities here (minx/miny, xs/ys, lat_rad/lam_rad, step/stop), which this lint reads as typos"
)]
//! Linear-referencing and measured-line operations on `Shape`: interpolate /
//! locate / substring (by distance/fraction and by M), plus the geodesic-aware
//! splitting helpers.

use super::linear_3d;
use crate::error::Result;
#[cfg(test)]
use crate::geometry::LineIndex;
use crate::geometry::{
    CoordSeq, CoordinateAxes, Coordinates, Distance3dParts, GeometryErrorKind, GeometryKind, HasM,
    HasZ, LineSeq, Lineal, MColumn, MultiLineString, Ordering, PlanarMetric, Point, PreparedPlan,
    Segment, SegmentMetric, SegmentProjection, Shape, SharedSlice, distance_3d_with_parts,
    interpolate_fraction, point_distance, point_segment_selection, same_point, shape_axes_all_z,
    squared_norm_is_trustworthy,
};
use crate::{Finite, HeapSize};
/// Validated `(start, end)` pair for linear-referencing measure/distance bounds.
///
/// Distance substringing validates finiteness here; the ordered-pair guard for
/// `line_substring` runs after [`normalize_line_distance`] in
/// [`LineIndex::substring`] because negative offsets can reorder the effective
/// span. Measure-based surfaces validate the full finite + `start <= end`
/// invariant at construction.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct MeasureRange {
    start: f64,
    end: f64,
}

impl MeasureRange {
    /// Raw distance bounds for `line_substring` (finiteness only — order is
    /// checked post-normalization in [`LineIndex::substring`]).
    pub(crate) fn substring_distance(start: f64, end: f64) -> Result<Self> {
        let start = Finite::try_new("start", start)?.get();
        let end = Finite::try_new("end", end)?.get();
        Ok(Self { start, end })
    }

    /// Raw measure bounds for `line_substring(basis='m')`.
    pub(crate) fn substring_measure(start: f64, end: f64) -> Result<Self> {
        let start = Finite::try_new("start", start)?.get();
        let end = Finite::try_new("end", end)?.get();
        if start > end {
            return Err(GeometryErrorKind::SubstringOrder(start, end).into());
        }
        Ok(Self { start, end })
    }

    /// Measure ramp bounds for `interpolate_m`.
    ///
    /// Its public parameters really are `start_m`/`end_m` — unlike
    /// `line_substring`, which is `start`/`end` under EVERY `basis`. Name each
    /// after its own signature.
    pub(crate) fn interpolate_m(start: f64, end: f64) -> Result<Self> {
        let start = Finite::try_new("start_m", start)?.get();
        let end = Finite::try_new("end_m", end)?.get();
        if end < start {
            return Err(GeometryErrorKind::InvalidMeasureRange(start, end).into());
        }
        Ok(Self { start, end })
    }

    pub(crate) const fn start(self) -> f64 {
        self.start
    }

    pub(crate) const fn end(self) -> f64 {
        self.end
    }
}

impl Shape {
    #[cfg(test)]
    pub fn line_locate_point(&self, point: Point, normalized: bool) -> Result<f64> {
        let index = LineIndex::build(self, &PlanarMetric)?;
        index.locate_point(point, normalized, &PlanarMetric)
    }

    /// Split this lineal geometry at each splitter point that lies on it.
    ///
    /// `tolerance` (coordinate units, already validated non-negative at the
    /// boundary) governs BOTH which points count as on-line and how near-equal
    /// cut offsets collapse: the default `0.0` is exact topological membership
    /// (a point splits only when it lies exactly on the linework) and
    /// identity deduplication (two cuts collapse only at bit-identical offsets).
    pub fn split(&self, splitter: &Self, tolerance: f64) -> Result<Vec<Self>> {
        self.split_at_points(&splitter.splitter_points()?, tolerance)
    }

    /// [`Self::split`] against split locations the caller already holds.
    pub(crate) fn split_at_points(&self, splitters: &[Point], tolerance: f64) -> Result<Vec<Self>> {
        let mut pieces = Vec::new();
        let lines = match self.lineal().ok_or(GeometryErrorKind::LinealRequired)? {
            Lineal::LineString(line) => std::slice::from_ref(line),
            Lineal::MultiLineString(lines) => &**lines,
            // Nothing to split: no pieces, as for a multipart of empty members.
            Lineal::Empty => &[],
        };
        for line in lines {
            for part in split_line_by_points(line, splitters, tolerance)? {
                pieces.push(Self::LineString(LineSeq::from_proven(
                    CoordSeq::try_from_points(&part)?,
                )));
            }
        }
        Ok(pieces)
    }
}

impl PreparedPlan {
    /// Cached-engine siblings of the `Shape` linear-referencing methods —
    /// the prefix index builds once per handle and serves every later call
    /// (shadowing the `Shape` versions via `Deref`, like distance/dwithin).
    pub(crate) fn line_interpolate_point(&self, distance: f64, normalized: bool) -> Result<Shape> {
        let distance = Finite::try_new("distance", distance)?.get();
        Ok(Shape::Point(self.line_index()?.interpolate(
            distance,
            normalized,
            &PlanarMetric,
        )))
    }

    pub(crate) fn line_substring(&self, range: MeasureRange, normalized: bool) -> Result<Shape> {
        self.line_index()?
            .substring(range, normalized, &PlanarMetric)
    }

    pub(crate) fn line_locate_point(&self, point: Point, normalized: bool) -> Result<f64> {
        self.line_index()?
            .locate_point(point, normalized, &PlanarMetric)
    }
}

pub(crate) fn segment_segment_distance_3d(
    p1: [f64; 3],
    q1: [f64; 3],
    p2: [f64; 3],
    q2: [f64; 3],
) -> Result<f64> {
    linear_3d::segment_segment_distance_3d(p1, q1, p2, q2)
}

impl Shape {
    /// Compute the minimum 3D (Euclidean) distance between two geometries, over their
    /// linework: points/multipoints as degenerate segments, polygons via their
    /// boundary rings (no interior==0 — a 3D surface has no volume). Both
    /// geometries must carry a Z ordinate on every vertex, else
    /// [`GeometryErrorKind::MissingZ`]. Empty operands return infinity. The point-
    /// to-point case keeps the overflow-robust `hypot` form.
    pub fn distance_3d(&self, other: &Self) -> Result<f64> {
        if self.is_empty() || other.is_empty() {
            return Ok(f64::INFINITY);
        }
        if !shape_axes_all_z(self) || !shape_axes_all_z(other) {
            return Err(GeometryErrorKind::MissingZ.into());
        }
        if let (Self::Point(a), Self::Point(b)) = (self, other) {
            let az = a.z().ok_or(GeometryErrorKind::MissingZ)?;
            let bz = b.z().ok_or(GeometryErrorKind::MissingZ)?;
            let (dx, dy, dz) = (b.x - a.x, b.y - a.y, bz - az);
            let squared = dx * dx + dy * dy + dz * dz;
            return Ok(
                if squared_norm_is_trustworthy(squared, dx == 0.0 && dy == 0.0 && dz == 0.0) {
                    squared.sqrt()
                } else {
                    dx.hypot(dy).hypot(dz)
                },
            );
        }
        let left = Distance3dParts::build(self);
        let right = Distance3dParts::build(other);
        distance_3d_with_parts(&left, &right)
    }

    /// Distribute an M ordinate from `start_m` to `end_m` over the arc length
    /// of this lineal geometry, stationing by `segment_length` between
    /// consecutive vertices — continuous across parts (the `PostGIS`
    /// ``ST_AddMeasure`` shape, so multipart M stays usable by the
    /// monotonic-M linear referencing). The CRS-aware surfaces pass a
    /// geodesic `segment_length` (so M reflects real-world distance on a
    /// geographic CRS, like the rest of the LRS family); the planar surface
    /// passes 2D distance. X/Y/Z are untouched — only M is (re)assigned,
    /// and existing M requires `overwrite`.
    pub(crate) fn interpolate_m(
        &self,
        range: MeasureRange,
        overwrite: bool,
        segment_length: impl Fn(Point, Point) -> f64,
    ) -> Result<Self> {
        let (start_m, end_m) = (range.start(), range.end());
        let parts = self.linework()?;
        if parts.iter().all(|part| part.is_empty()) {
            // Nothing to station: the result is the same empty, now measured
            // like every non-empty result.
            let kind = match self {
                Self::MultiLineString(_) => GeometryKind::MultiLineString,
                Self::Empty(kind, _) => *kind,
                _ => GeometryKind::LineString,
            };
            return Ok(Self::Empty(kind, self.axes().union(CoordinateAxes::XYM)));
        }
        if !overwrite && self.has_m() {
            return Err(GeometryErrorKind::MeasureOverwrite.into());
        }
        // Arc length of the drawn linework only: the gap between parts
        // consumes no measure (parts abut in measure space, the PostGIS
        // multipart behavior). Segment lengths are evaluated ONCE into a
        // prefix buffer (was twice — total pass then assign pass — which
        // doubled every geodesic inverse on a geographic CRS).
        let segment_count = parts.iter().map(|part| part.len().saturating_sub(1)).sum();
        let mut segment_lengths = Vec::with_capacity(segment_count);
        let mut part_seg_ends = Vec::with_capacity(parts.len());
        let mut total = 0.0_f64;
        for part in &parts {
            for [start, end] in part.segment_pairs() {
                let length = segment_length(start, end);
                total += length;
                segment_lengths.push(length);
            }
            part_seg_ends.push(segment_lengths.len());
        }
        let span = end_m - start_m;
        let mut traveled = 0.0;
        let mut measured = Vec::with_capacity(parts.len());
        let mut seg_cursor = 0_usize;
        for (part_index, part) in parts.iter().enumerate() {
            let part_end = part_seg_ends[part_index];
            let mut measures = Vec::with_capacity(part.len());
            for index in 0..part.len() {
                if index > 0 {
                    traveled += segment_lengths[seg_cursor];
                    seg_cursor += 1;
                }
                // Per-vertex division is deliberate: `traveled` accumulates
                // to bit-exactly `total`, so the final vertex measures
                // exactly `end_m` — a hoisted `span / total` factor would
                // lose that endpoint exactness for a ~ns-scale win.
                let measure = if total > 0.0 {
                    start_m + span * (traveled / total)
                } else {
                    start_m
                };
                measures.push(measure);
            }
            debug_assert_eq!(seg_cursor, part_end);
            let _ = part_end;
            let columns = part.column_arcs();
            let physical_len = columns.xs.len();
            let window = crate::geometry::CoordWindow::from_range(columns.window, physical_len);
            let m = SharedSlice::from_vec(measures);
            measured.push(LineSeq::from_proven(
                CoordSeq::from_shared_columns_with_window(
                    columns.xs,
                    columns.ys,
                    columns.zs,
                    Some(MColumn::Local {
                        owner: m,
                        offset: 0,
                    }),
                    window,
                )?,
            ));
        }
        if measured.is_empty() {
            return Ok(Self::typed_empty(GeometryKind::of(self), self.axes()));
        }
        let output_axes = CoordinateAxes::new(HasZ(self.axes().has_z()), HasM(true));
        Ok(match self {
            Self::LineString(_) => Self::LineString(measured.into_iter().next().expect("one part")),
            Self::MultiLineString(lines) => {
                let mut measured = measured.into_iter();
                let mut members = Vec::with_capacity(lines.member_count());
                for member in lines.members() {
                    let member = match member {
                        crate::geometry::MultiMember::Empty => {
                            Some(crate::geometry::MultiMember::Empty)
                        },
                        crate::geometry::MultiMember::Value(_) => {
                            measured.next().map(crate::geometry::MultiMember::Value)
                        },
                    };
                    if let Some(member) = member {
                        members.push(member);
                    }
                }
                Self::MultiLineString(MultiLineString::try_new(output_axes, members)?)
            },
            _ => unreachable!("linework returns only line strings"),
        })
    }

    /// Point at measure `m` along this geometry's M ordinate. `m` is clamped to
    /// the `[m_first, m_last]` range.
    #[cfg(test)]
    pub fn line_interpolate_point_m(&self, m: f64) -> Result<Self> {
        self.line_interpolate_point_m_with(m, &PlanarMetric)
    }

    #[cfg(test)]
    pub(crate) fn line_interpolate_point_m_with<M: SegmentMetric>(
        &self,
        m: f64,
        metric: &M,
    ) -> Result<Self> {
        let m = Finite::try_new("m", m)?.get();
        let index = MeasuredLineIndex::build(self)?;
        Ok(Self::Point(index.interpolate_finite(m, metric)?))
    }

    /// Interpolated M ordinate at the location on this geometry nearest
    /// `point`.
    #[cfg(test)]
    pub fn line_locate_point_m(&self, point: Point) -> Result<f64> {
        self.line_locate_point_m_with(point, &PlanarMetric)
    }

    #[cfg(test)]
    pub(crate) fn line_locate_point_m_with<M: SegmentMetric>(
        &self,
        point: Point,
        metric: &M,
    ) -> Result<f64> {
        let index = MeasuredLineIndex::build(self)?;
        index.locate(point, metric)
    }

    /// Sub-line between measures `m0` and `m1` along this geometry's M
    /// ordinate.
    #[cfg(test)]
    pub(crate) fn line_substring_m(&self, range: MeasureRange) -> Result<Self> {
        self.line_substring_m_with(range, &PlanarMetric)
    }

    #[cfg(test)]
    pub(crate) fn line_substring_m_with<M: SegmentMetric>(
        &self,
        range: MeasureRange,
        metric: &M,
    ) -> Result<Self> {
        let index = MeasuredLineIndex::build(self)?;
        index.substring(range, metric)
    }
}

impl PreparedPlan {
    pub(crate) fn line_interpolate_point_m_with<M: SegmentMetric>(
        &self,
        m: f64,
        metric: &M,
    ) -> Result<Shape> {
        let m = Finite::try_new("m", m)?.get();
        Ok(Shape::Point(
            self.measured_line_index()?.interpolate_finite(m, metric)?,
        ))
    }

    pub(crate) fn line_locate_point_m_with<M: SegmentMetric>(
        &self,
        point: Point,
        metric: &M,
    ) -> Result<f64> {
        self.measured_line_index()?.locate(point, metric)
    }

    pub(crate) fn line_substring_m_with<M: SegmentMetric>(
        &self,
        range: MeasureRange,
        metric: &M,
    ) -> Result<Shape> {
        self.measured_line_index()?.substring(range, metric)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum MeasuredLineAdmission {
    NotLineal,
    Empty,
    MissingMeasure,
    NonMonotonic,
}

impl From<MeasuredLineAdmission> for crate::error::Error {
    fn from(admission: MeasuredLineAdmission) -> Self {
        let kind = match admission {
            MeasuredLineAdmission::NotLineal => GeometryErrorKind::LineStringRequired,
            MeasuredLineAdmission::Empty => GeometryErrorKind::EmptyLinework,
            MeasuredLineAdmission::MissingMeasure => GeometryErrorKind::MissingMeasure,
            MeasuredLineAdmission::NonMonotonic => GeometryErrorKind::NonMonotonicMeasure,
        };
        kind.into()
    }
}

/// Immutable M-linear-referencing product for one lineal shape.
///
/// The product retains row/part views over the source columns rather than
/// gathering an AoS point buffer. Iteration deliberately crosses multipart
/// boundaries, matching the flattened vertex walk used by the public M
/// operations.
pub(crate) struct MeasuredLineIndex {
    parts: Box<[CoordSeq]>,
    first: Point,
    last: Point,
}

impl MeasuredLineIndex {
    pub(crate) fn build(shape: &Shape) -> Result<Self> {
        let parts = match shape {
            Shape::LineString(line) => std::slice::from_ref(line),
            Shape::MultiLineString(lines) => &**lines,
            Shape::Empty(GeometryKind::LineString | GeometryKind::MultiLineString, _) => {
                return Err(MeasuredLineAdmission::Empty.into());
            },
            _ => return Err(MeasuredLineAdmission::NotLineal.into()),
        };
        let mut owned = Vec::with_capacity(parts.len());
        owned.extend(parts.iter().map(|line| line.as_coords().clone()));
        let parts = owned.into_boxed_slice();
        let mut first = None;
        let mut last = None;
        let mut previous: Option<Point> = None;
        for part in &parts {
            for point in part {
                if point.m().is_none() {
                    return Err(MeasuredLineAdmission::MissingMeasure.into());
                }
                if let Some(previous) = previous
                    && point.m().unwrap_or(0.0) < previous.m().unwrap_or(0.0)
                {
                    return Err(MeasuredLineAdmission::NonMonotonic.into());
                }
                first.get_or_insert(point);
                last = Some(point);
                previous = Some(point);
            }
        }
        let Some(first) = first else {
            return Err(MeasuredLineAdmission::Empty.into());
        };
        let Some(last) = last else {
            return Err(MeasuredLineAdmission::Empty.into());
        };
        Ok(Self { parts, first, last })
    }

    fn for_each_window(&self, mut visit: impl FnMut(Point, Point) -> Result<bool>) -> Result<()> {
        let mut previous = None;
        for part in &self.parts {
            for point in part {
                if let Some(start) = previous
                    && !visit(start, point)?
                {
                    return Ok(());
                }
                previous = Some(point);
            }
        }
        Ok(())
    }

    fn interpolate_finite<M: SegmentMetric>(&self, target: f64, metric: &M) -> Result<Point> {
        let target = target.clamp(self.first.m().unwrap_or(0.0), self.last.m().unwrap_or(0.0));
        let mut result = None;
        self.for_each_window(|start, end| -> Result<bool> {
            let _ = metric.admitted_length(start, end)?;
            let (start_m, end_m) = (start.m().unwrap_or(0.0), end.m().unwrap_or(0.0));
            if target > end_m {
                return Ok(true);
            }
            #[expect(clippy::float_cmp, reason = "exact M boundary identity")]
            if target == start_m {
                result = Some(start);
                return Ok(false);
            }
            #[expect(clippy::float_cmp, reason = "exact M boundary identity")]
            if target == end_m {
                result = Some(end);
                return Ok(false);
            }
            let fraction = interpolate_fraction(start_m, end_m, target)?;
            result = Some(metric.interpolate(start, end, fraction));
            Ok(false)
        })?;
        Ok(result.unwrap_or(self.last))
    }

    fn locate<M: SegmentMetric>(&self, point: Point, metric: &M) -> Result<f64> {
        if point.x.is_nan() {
            return Ok(f64::NAN);
        }
        let mut best_distance = f64::INFINITY;
        let mut best_measure = self.first.m().unwrap_or(0.0);
        let mut segment_scratch = crate::geometry::GeodesicSegmentScratch::default();
        let mut previous = None;
        for part in &self.parts {
            for end in part {
                let Some(start) = previous else {
                    previous = Some(end);
                    continue;
                };
                let length = if M::LOCATE_WITH_MEASURE_NEEDS_LENGTH {
                    metric.admitted_length(start, end)?
                } else {
                    0.0
                };
                let Some((distance, measure)) = metric.locate_with_measure(
                    point,
                    start,
                    end,
                    length,
                    best_distance,
                    &mut segment_scratch,
                )?
                else {
                    previous = Some(end);
                    continue;
                };
                if distance < best_distance {
                    best_distance = distance;
                    best_measure = measure;
                }
                previous = Some(end);
            }
        }
        Ok(best_measure)
    }

    fn substring<M: SegmentMetric>(&self, range: MeasureRange, metric: &M) -> Result<Shape> {
        let (m0, m1) = (range.start(), range.end());
        let (lo, hi) = (self.first.m().unwrap_or(0.0), self.last.m().unwrap_or(0.0));
        let start = m0.clamp(lo, hi);
        let end = m1.clamp(lo, hi);
        if start.total_cmp(&end) == Ordering::Equal {
            return Ok(Shape::Point(self.interpolate_finite(start, metric)?));
        }
        let mut result = Vec::new();
        self.for_each_window(|segment_start, segment_end| -> Result<bool> {
            let _ = metric.admitted_length(segment_start, segment_end)?;
            let (start_m, end_m) = (
                segment_start.m().unwrap_or(0.0),
                segment_end.m().unwrap_or(0.0),
            );
            if end_m < start || start_m > end {
                return Ok(true);
            }
            let fraction = |measure: f64| interpolate_fraction(start_m, end_m, measure);
            #[expect(clippy::float_cmp, reason = "exact M boundary identity")]
            let at = |measure: f64| -> Result<Point> {
                if measure == start_m {
                    Ok(segment_start)
                } else if measure == end_m {
                    Ok(segment_end)
                } else {
                    Ok(metric.interpolate(segment_start, segment_end, fraction(measure)?))
                }
            };
            push_distinct_point(&mut result, at(start.max(start_m))?);
            push_distinct_point(&mut result, at(end.min(end_m))?);
            Ok(true)
        })?;
        Ok(match result.len() {
            0 => Shape::Point(self.interpolate_finite(start, metric)?),
            1 => Shape::Point(result[0]),
            _ => Shape::LineString(LineSeq::from_proven(CoordSeq::try_from_points(&result)?)),
        })
    }
}

/// Cached admission result for the measured line product.
pub(crate) enum MeasuredLineIndexSlot {
    Index(MeasuredLineIndex),
    Rejected(MeasuredLineAdmission),
}

impl MeasuredLineIndexSlot {
    pub(crate) fn build(shape: &Shape) -> Result<Self> {
        use crate::error::ErrorKind;

        match MeasuredLineIndex::build(shape) {
            Ok(index) => Ok(Self::Index(index)),
            Err(error) => match error.kind() {
                ErrorKind::Geometry(GeometryErrorKind::EmptyLinework) => {
                    Ok(Self::Rejected(MeasuredLineAdmission::Empty))
                },
                ErrorKind::Geometry(GeometryErrorKind::LineStringRequired) => {
                    Ok(Self::Rejected(MeasuredLineAdmission::NotLineal))
                },
                ErrorKind::Geometry(GeometryErrorKind::MissingMeasure) => {
                    Ok(Self::Rejected(MeasuredLineAdmission::MissingMeasure))
                },
                ErrorKind::Geometry(GeometryErrorKind::NonMonotonicMeasure) => {
                    Ok(Self::Rejected(MeasuredLineAdmission::NonMonotonic))
                },
                _ => Err(error),
            },
        }
    }

    pub(crate) fn get(&self) -> Result<&MeasuredLineIndex> {
        match self {
            Self::Index(index) => Ok(index),
            Self::Rejected(admission) => Err((*admission).into()),
        }
    }
}

impl HeapSize for MeasuredLineIndex {
    fn heap_bytes(&self) -> usize {
        self.parts.heap_bytes()
    }
}

impl HeapSize for MeasuredLineIndexSlot {
    fn heap_bytes(&self) -> usize {
        match self {
            Self::Index(index) => index.heap_bytes(),
            Self::Rejected(_) => 0,
        }
    }
}

/// Resolve a linear-referencing distance: a `[0, 1]` fraction of `total` when
/// `normalized`, otherwise an absolute measure clamped into `[0, total]`.
pub(super) fn normalize_line_distance(distance: f64, total: f64, normalized: bool) -> f64 {
    if normalized {
        distance * total
    } else if distance < 0.0 {
        total + distance
    } else {
        distance
    }
    .clamp(0.0, total)
}

/// Split a line at each of `points` within `tolerance` of it, yielding the
/// pieces. `tolerance` (coordinate units) bounds both membership and offset
/// deduplication: `0.0` is exact on-line membership and identity dedup.
pub(super) fn split_line_by_points<C: Coordinates + ?Sized>(
    line: &C,
    points: &[Point],
    tolerance: f64,
) -> crate::error::Result<Vec<Vec<Point>>> {
    let coords: Vec<Point> = line.iter_coords().collect();
    if coords.len() < 2 {
        return Ok(vec![coords]);
    }
    let Some(first_live) = coords
        .array_windows::<2>()
        .position(|pair| point_distance(pair[0], pair[1]) > 0.0)
    else {
        return Ok(vec![coords]);
    };
    let last_live = coords
        .array_windows::<2>()
        .rposition(|pair| point_distance(pair[0], pair[1]) > 0.0)
        .expect("a first live segment has a last live segment");

    let mut cuts = points
        .iter()
        .filter_map(|point| line_point_offset(&coords, *point, tolerance).transpose())
        .collect::<crate::error::Result<Vec<_>>>()?;
    cuts.retain(|(index, projection)| {
        !(*index == first_live && projection.is_start()
            || *index == last_live && projection.is_end())
    });
    crate::geometry::try_sort_by(&mut cuts, |left, right| {
        Ok(left.0.cmp(&right.0).then(left.1.cmp_along(&right.1)?))
    })?;
    let mut keep: Vec<(usize, SegmentProjection)> = Vec::with_capacity(cuts.len());
    for cut in cuts {
        if let Some(previous) = keep.last()
            && previous.0 == cut.0
        {
            if previous.1.cmp_along(&cut.1)?.is_eq() {
                continue;
            }
            if tolerance != 0.0 {
                let start = coords[cut.0];
                let end = coords[cut.0 + 1];
                if point_distance(
                    previous.1.interpolate_point(start, end)?,
                    cut.1.interpolate_point(start, end)?,
                ) <= tolerance
                {
                    continue;
                }
            }
        }
        keep.push(cut);
    }
    let cuts = keep;

    if cuts.is_empty() {
        return Ok(vec![coords]);
    }

    let mut pieces = Vec::with_capacity(cuts.len() + 1);
    let mut piece = vec![coords[0]];
    let mut cuts = cuts.into_iter().peekable();
    for (segment_index, pair) in coords.array_windows::<2>().enumerate() {
        while cuts
            .peek()
            .is_some_and(|(cut_index, _)| *cut_index == segment_index)
        {
            let (_, projection) = cuts.next().expect("peeked cut exists");
            let point = projection.interpolate_point(pair[0], pair[1])?;
            push_distinct_point(&mut piece, point);
            if piece.len() >= 2 {
                pieces.push(std::mem::replace(&mut piece, vec![point]));
            }
        }
        push_distinct_point(&mut piece, pair[1]);
    }
    if piece.len() >= 2 {
        pieces.push(piece);
    }
    Ok(pieces)
}

/// Segment-local position at which `point` lies on `line` within `tolerance`
/// (coordinate units), if any. `tolerance == 0.0` requires exact membership.
pub(super) fn line_point_offset<C: Coordinates + ?Sized>(
    line: &C,
    point: Point,
    tolerance: f64,
) -> crate::error::Result<Option<(usize, SegmentProjection)>> {
    let mut best = None;
    let mut best_key = None;
    for (segment_index, [start, end]) in line.segment_pairs().enumerate() {
        let length = point_distance(start, end);
        if length == 0.0 {
            continue;
        }
        let segment = Segment {
            start: start.xy(),
            end: end.xy(),
        };
        let selection = point_segment_selection(point.xy(), segment)?;
        if selection.distance() > tolerance {
            continue;
        }
        let key = selection.distance_key(point.xy(), segment);
        if tolerance == 0.0 && !key.is_exact_zero()? {
            continue;
        }
        let closer = match best_key.as_ref() {
            None => true,
            Some(incumbent) => key.cmp(incumbent)?.is_lt(),
        };
        if closer {
            let projection = selection.projection(point.xy(), segment)?;
            best_key = Some(key);
            best = Some((segment_index, projection));
        }
    }
    Ok(best)
}

/// Append `point` unless it duplicates the current last vertex.
pub(super) fn push_distinct_point(points: &mut Vec<Point>, point: Point) {
    if points.last().is_none_or(|last| !same_point(*last, point)) {
        points.push(point);
    }
}

#[cfg(test)]
mod measured_window_tests {
    use super::*;

    #[test]
    fn interpolate_m_preserves_multipart_empty_members() {
        use crate::geometry::MultiMember;

        let source =
            crate::io::parse_wkt("MULTILINESTRING (EMPTY, (0 0, 3 0), (8 0, 11 0), EMPTY)")
                .unwrap();
        let range = MeasureRange::interpolate_m(10.0, 40.0).unwrap();
        let Shape::MultiLineString(lines) = source
            .interpolate_m(range, false, point_distance)
            .expect("measured interpolation")
        else {
            panic!("multipart output");
        };
        assert_eq!(lines.member_count(), 4);
        assert!(matches!(lines.members().next(), Some(MultiMember::Empty)));
        assert_eq!(lines.as_slice()[0].ms(), Some(&[10.0, 25.0][..]));
        assert_eq!(lines.as_slice()[1].ms(), Some(&[25.0, 40.0][..]));
    }

    #[test]
    fn interpolate_m_retains_partial_xyz_window_and_local_measures() {
        let coords = CoordSeq::try_from_vecs(
            vec![-10.0, 0.0, 3.0, 6.0, 20.0],
            vec![9.0, 0.0, 0.0, 0.0, 9.0],
            Some(vec![100.0, 11.0, 12.0, 13.0, 200.0]),
            None,
        )
        .unwrap();
        let window = coords.view(crate::geometry::CoordWindow::from_range(1..4, coords.len()));
        let source = Shape::LineString(LineSeq::from_proven(window));
        let output = source
            .interpolate_m(
                MeasureRange::interpolate_m(10.0, 40.0).unwrap(),
                false,
                point_distance,
            )
            .unwrap();
        let Shape::LineString(line) = &output else {
            panic!("line output");
        };
        assert_eq!(line.xs(), &[0.0, 3.0, 6.0]);
        assert_eq!(line.zs(), Some(&[11.0, 12.0, 13.0][..]));
        assert_eq!(line.ms(), Some(&[10.0, 25.0, 40.0][..]));
        assert_eq!(line.xs().as_ptr(), coords.xs()[1..].as_ptr());
        drop(source);
        drop(coords);
        assert_eq!(line.ms(), Some(&[10.0, 25.0, 40.0][..]));
    }
}
