#![allow(
    clippy::similar_names,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
#![allow(
    clippy::arbitrary_source_item_ordering,
    reason = "file-local domain naming, dependency paths, or cohesive item layout is clearer here"
)]
//! Linear-referencing and measured-line operations on `Shape`: interpolate /
//! locate / substring (by distance/fraction and by M), plus the geodesic-aware
//! splitting helpers.

use super::*;
use crate::Finite;
use crate::error::Result;

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
        let start = Finite::try_new("start_distance", start)?.get();
        let end = Finite::try_new("end_distance", end)?.get();
        Ok(Self { start, end })
    }

    /// Raw measure bounds for `line_substring_m`.
    pub(crate) fn substring_measure(start: f64, end: f64) -> Result<Self> {
        let start = Finite::try_new("start_m", start)?.get();
        let end = Finite::try_new("end_m", end)?.get();
        if start > end {
            return Err(GeometryErrorKind::SubstringMeasureOrder(start, end).into());
        }
        Ok(Self { start, end })
    }

    /// Measure ramp bounds for `interpolate_m`.
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
    pub fn line_interpolate_point(&self, distance: f64, normalized: bool) -> Result<Self> {
        let distance = Finite::try_new("distance", distance)?.get();
        let index = LineIndex::build(self, &PlanarMetric)?;
        Ok(Self::Point(index.interpolate(
            distance,
            normalized,
            &PlanarMetric,
        )))
    }

    pub fn line_locate_point(&self, point: Point, normalized: bool) -> Result<f64> {
        let index = LineIndex::build(self, &PlanarMetric)?;
        Ok(index.locate_point(point, normalized, &PlanarMetric))
    }

    /// Split this lineal geometry at each splitter point that lies on it.
    ///
    /// `tolerance` (coordinate units, already validated non-negative at the
    /// boundary) governs BOTH which points count as on-line and how near-equal
    /// cut offsets collapse: the default `0.0` is exact topological membership
    /// (a point splits only when it lies exactly on the linework) and
    /// identity deduplication (two cuts collapse only at bit-identical offsets).
    pub fn split(&self, splitter: &Self, tolerance: f64) -> Result<Vec<Self>> {
        let splitters = splitter.splitter_points()?;
        let mut pieces = Vec::new();
        match self {
            Self::LineString(line) => {
                pieces.extend(
                    split_line_by_points(line, splitters.as_slice(), tolerance)
                        .into_iter()
                        .map(|part| {
                            Self::LineString(
                                LineSeq::try_new(CoordSeq::from(part))
                                    .expect("line split part is lineal"),
                            )
                        }),
                );
            },
            Self::MultiLineString(lines) => {
                pieces.extend(lines.iter().flat_map(|line| {
                    split_line_by_points(line, splitters.as_slice(), tolerance)
                        .into_iter()
                        .map(|part| {
                            Self::LineString(
                                LineSeq::try_new(CoordSeq::from(part))
                                    .expect("line split part is lineal"),
                            )
                        })
                }));
            },
            _ => return Err(GeometryErrorKind::LineStringRequired.into()),
        }
        Ok(pieces)
    }
}

impl ShapeData {
    /// Cached-engine siblings of the `Shape` linear-referencing methods —
    /// the prefix index builds once per handle and serves every later call
    /// (shadowing the `Shape` versions via `Deref`, like distance/dwithin).
    pub fn line_interpolate_point(&self, distance: f64, normalized: bool) -> Result<Shape> {
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

    pub fn line_locate_point(&self, point: Point, normalized: bool) -> Result<f64> {
        Ok(self
            .line_index()?
            .locate_point(point, normalized, &PlanarMetric))
    }
}

/// Minimum Euclidean distance between two 3D segments (Ericson, *Real-Time
/// Collision Detection*, ClosestPtSegmentSegment in distance form). A
/// degenerate endpoint pair (`p == q`) acts as a point, so point/point,
/// point/segment, and segment/segment all share this one kernel.
#[expect(clippy::many_single_char_names, reason = "standard math notation")]
pub(crate) fn segment_segment_distance_3d(
    p1: [f64; 3],
    q1: [f64; 3],
    p2: [f64; 3],
    q2: [f64; 3],
) -> f64 {
    let sub = |a: [f64; 3], b: [f64; 3]| [a[0] - b[0], a[1] - b[1], a[2] - b[2]];
    let dot = |a: [f64; 3], b: [f64; 3]| a[0] * b[0] + a[1] * b[1] + a[2] * b[2];
    let d1 = sub(q1, p1);
    let d2 = sub(q2, p2);
    let r = sub(p1, p2);
    let a = dot(d1, d1);
    let e = dot(d2, d2);
    let f = dot(d2, r);
    let (s, t);
    if a == 0.0 && e == 0.0 {
        s = 0.0;
        t = 0.0;
    } else if a == 0.0 {
        s = 0.0;
        t = (f / e).clamp(0.0, 1.0);
    } else {
        let c = dot(d1, r);
        if e == 0.0 {
            t = 0.0;
            s = (-c / a).clamp(0.0, 1.0);
        } else {
            let b = dot(d1, d2);
            #[expect(
                clippy::suspicious_operation_groupings,
                reason = "textbook segment-segment closest-point denominator a*e - b*b (Ericson, Real-Time Collision Detection); clippy's a*b suggestion is wrong"
            )]
            let denom = a * e - b * b;
            let s0 = if denom == 0.0 {
                0.0
            } else {
                ((b * f - c * e) / denom).clamp(0.0, 1.0)
            };
            let t0 = (b * s0 + f) / e;
            if t0 < 0.0 {
                t = 0.0;
                s = (-c / a).clamp(0.0, 1.0);
            } else if t0 > 1.0 {
                t = 1.0;
                s = ((b - c) / a).clamp(0.0, 1.0);
            } else {
                t = t0;
                s = s0;
            }
        }
    }
    let c1 = [p1[0] + d1[0] * s, p1[1] + d1[1] * s, p1[2] + d1[2] * s];
    let c2 = [p2[0] + d2[0] * t, p2[1] + d2[1] * t, p2[2] + d2[2] * t];
    let diff = sub(c1, c2);
    dot(diff, diff).sqrt()
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
                if squared.is_finite() && (squared != 0.0 || (dx == 0.0 && dy == 0.0 && dz == 0.0))
                {
                    squared.sqrt()
                } else {
                    dx.hypot(dy).hypot(dz)
                },
            );
        }
        let left = Distance3dParts::build(self);
        let right = Distance3dParts::build(other);
        Ok(distance_3d_with_parts(&left, &right))
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
            return Err(GeometryErrorKind::EmptyLinework.into());
        }
        if !overwrite && self.has_m() {
            return Err(GeometryErrorKind::MeasureOverwrite.into());
        }
        // Arc length of the drawn linework only: the gap between parts
        // consumes no measure (parts abut in measure space, the PostGIS
        // multipart behavior). Segment lengths are evaluated ONCE into a
        // prefix buffer (was twice — total pass then assign pass — which
        // doubled every geodesic inverse on a geographic CRS).
        let mut segment_lengths: Vec<f64> = Vec::new();
        let mut part_seg_ends: Vec<usize> = Vec::with_capacity(parts.len());
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
            let full_window = columns.window.start == 0 && columns.window.end == columns.xs.len();
            let xs = if full_window {
                columns.xs
            } else {
                part.xs().into()
            };
            let ys = if full_window {
                columns.ys
            } else {
                part.ys().into()
            };
            let zs = if full_window {
                columns.zs
            } else {
                part.zs().map(Into::into)
            };
            measured.push(LineSeq::from_trusted(CoordSeq::from_columns(
                xs,
                ys,
                zs,
                Some(measures.into()),
            )));
        }
        Ok(match self {
            Self::LineString(_) => Self::LineString(measured.into_iter().next().expect("one part")),
            _ => Self::MultiLineString(measured),
        })
    }

    /// Vertices of this geometry's linework, validated to carry monotonic
    /// non-decreasing M ordinates (the precondition for M linear referencing).
    fn measured_vertices(&self) -> Result<Vec<Point>> {
        let vertices: Vec<Point> = self.linework()?.into_iter().flatten().collect();
        if vertices.is_empty() {
            return Err(GeometryErrorKind::EmptyLinework.into());
        }
        if !vertices.iter().all(|vertex| vertex.m().is_some()) {
            return Err(GeometryErrorKind::MissingMeasure.into());
        }
        if vertices
            .array_windows::<2>()
            .any(|[start, end]| end.m().unwrap_or(0.0) < start.m().unwrap_or(0.0))
        {
            return Err(GeometryErrorKind::NonMonotonicMeasure.into());
        }
        Ok(vertices)
    }

    /// Point at measure `m` along this geometry's M ordinate. `m` is clamped to
    /// the `[m_first, m_last]` range.
    pub fn line_interpolate_point_m(&self, m: f64) -> Result<Self> {
        let m = Finite::try_new("m", m)?.get();
        let vertices = self.measured_vertices()?;
        let target = m.clamp(
            vertices[0].m().unwrap_or(0.0),
            vertices[vertices.len() - 1].m().unwrap_or(0.0),
        );
        for [start, end] in vertices.array_windows::<2>() {
            let (start_m, end_m) = (start.m().unwrap_or(0.0), end.m().unwrap_or(0.0));
            if target <= end_m {
                #[expect(clippy::float_cmp, reason = "exact M boundary identity")]
                if target == start_m {
                    return Ok(Self::Point(*start));
                }
                #[expect(clippy::float_cmp, reason = "exact M boundary identity")]
                if target == end_m {
                    return Ok(Self::Point(*end));
                }
                let span = end_m - start_m;
                let fraction = if span == 0.0 {
                    0.0
                } else {
                    (target - start_m) / span
                };
                return Ok(Self::Point(lerp_point(*start, *end, fraction)));
            }
        }
        Ok(Self::Point(vertices[vertices.len() - 1]))
    }

    /// Interpolated M ordinate at the location on this geometry nearest
    /// `point`.
    pub fn line_locate_point_m(&self, point: Point) -> Result<f64> {
        let vertices = self.measured_vertices()?;
        let mut best_distance = f64::INFINITY;
        let mut best_measure = vertices[0].m().unwrap_or(0.0);
        for [start, end] in vertices.array_windows::<2>() {
            let segment = Segment {
                start: (*start).into(),
                end: (*end).into(),
            };
            let fraction = segment_projection_fraction(point, segment);
            let projected = lerp_point(*start, *end, fraction);
            let distance = point_distance(point, projected);
            if distance < best_distance {
                best_distance = distance;
                best_measure = projected.m().unwrap_or(0.0);
            }
        }
        Ok(best_measure)
    }

    /// Sub-line between measures `m0` and `m1` along this geometry's M
    /// ordinate.
    pub(crate) fn line_substring_m(&self, range: MeasureRange) -> Result<Self> {
        let (m0, m1) = (range.start(), range.end());
        let vertices = self.measured_vertices()?;
        let (lo, hi) = (
            vertices[0].m().unwrap_or(0.0),
            vertices[vertices.len() - 1].m().unwrap_or(0.0),
        );
        let start = m0.clamp(lo, hi);
        let end = m1.clamp(lo, hi);
        if start.total_cmp(&end) == Ordering::Equal {
            return self.line_interpolate_point_m(start);
        }
        let mut result = Vec::new();
        for [segment_start, segment_end] in vertices.array_windows::<2>() {
            let (start_m, end_m) = (
                segment_start.m().unwrap_or(0.0),
                segment_end.m().unwrap_or(0.0),
            );
            if end_m < start || start_m > end {
                continue;
            }
            let span = end_m - start_m;
            let fraction = |measure: f64| {
                if span == 0.0 {
                    0.0
                } else {
                    (measure - start_m) / span
                }
            };
            #[expect(clippy::float_cmp, reason = "exact M boundary identity")]
            let at = |measure: f64| {
                if measure == start_m {
                    *segment_start
                } else if measure == end_m {
                    *segment_end
                } else {
                    lerp_point(*segment_start, *segment_end, fraction(measure))
                }
            };
            push_distinct_point(&mut result, at(start.max(start_m)));
            push_distinct_point(&mut result, at(end.min(end_m)));
        }
        Ok(match result.len() {
            0 => self.line_interpolate_point_m(start)?,
            1 => Self::Point(result[0]),
            _ => Self::LineString(
                LineSeq::try_new(CoordSeq::from(result)).expect("M substring is lineal"),
            ),
        })
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
) -> Vec<Vec<Point>> {
    if line.coord_count() < 2 {
        return vec![line.iter_coords().collect()];
    }
    let total = line_segments(line)
        .map(|segment| point_distance(segment.start, segment.end))
        .sum::<f64>();
    if total == 0.0 {
        return vec![line.iter_coords().collect()];
    }

    let mut offsets = points
        .iter()
        .filter_map(|point| line_point_offset(line, *point, tolerance))
        .filter(|offset| *offset > 0.0 && *offset < total)
        .collect::<Vec<_>>();
    offsets.sort_by(f64::total_cmp);
    offsets.dedup_by(|left, right| (*left - *right).abs() <= tolerance);

    if offsets.is_empty() {
        return vec![line.iter_coords().collect()];
    }

    let mut pieces = Vec::with_capacity(offsets.len() + 1);
    let mut start = 0.0;
    for offset in offsets {
        if let Some(piece) = line_piece_between(line, start, offset) {
            pieces.push(piece);
        }
        start = offset;
    }
    if let Some(piece) = line_piece_between(line, start, total) {
        pieces.push(piece);
    }
    pieces
}

/// Distance along `line` at which `point` lies on it within `tolerance`
/// (coordinate units), if any. `tolerance == 0.0` requires the point to lie
/// exactly on the linework.
pub(super) fn line_point_offset<C: Coordinates + ?Sized>(
    line: &C,
    point: Point,
    tolerance: f64,
) -> Option<f64> {
    let mut offset = 0.0;
    let mut best = None;
    let mut best_distance = f64::INFINITY;
    for [start, end] in line.segment_pairs() {
        let length = point_distance(start, end);
        if length == 0.0 {
            continue;
        }
        let segment = Segment {
            start: start.xy(),
            end: end.xy(),
        };
        let fraction = segment_projection_fraction(point, segment);
        let projected = lerp_point(start, end, fraction);
        let distance = point_distance(point, projected);
        if distance <= tolerance && distance < best_distance {
            best_distance = distance;
            best = Some(length * fraction + offset);
        }
        offset += length;
    }
    best
}

/// Sub-line of `line` between the `start` and `end` offsets along it.
pub(super) fn line_piece_between<C: Coordinates + ?Sized>(
    line: &C,
    start: f64,
    end: f64,
) -> Option<Vec<Point>> {
    if start >= end {
        return None;
    }
    let mut result = Vec::new();
    let mut offset = 0.0;
    for [seg_start, seg_end] in line.segment_pairs() {
        let length = point_distance(seg_start, seg_end);
        if length == 0.0 {
            continue;
        }
        let segment_start = offset;
        let segment_end = offset + length;
        if segment_end < start {
            offset = segment_end;
            continue;
        }
        if segment_start > end {
            break;
        }
        let from = start.max(segment_start);
        let to = end.min(segment_end);
        if from <= to {
            push_distinct_point(
                &mut result,
                lerp_point(seg_start, seg_end, (from - segment_start) / length),
            );
            push_distinct_point(
                &mut result,
                lerp_point(seg_start, seg_end, (to - segment_start) / length),
            );
        }
        offset = segment_end;
    }
    (result.len() >= 2).then_some(result)
}

/// Append `point` unless it duplicates the current last vertex.
pub(super) fn push_distinct_point(points: &mut Vec<Point>, point: Point) {
    if points.last().is_none_or(|last| !same_point(*last, point)) {
        points.push(point);
    }
}
