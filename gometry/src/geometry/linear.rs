#![expect(
    clippy::similar_names,
    reason = "axis and formula symbols name paired quantities here (minx/miny, xs/ys, lat_rad/lam_rad, step/stop), which this lint reads as typos"
)]
//! Linear-referencing and measured-line operations on `Shape`: interpolate /
//! locate / substring (by distance/fraction and by M), plus the geodesic-aware
//! splitting helpers.

use super::line_index::InterpolationProvenance;
use super::linear_3d;
use crate::error::Result;
use crate::geometry::distance::{ChartParts, point_chart_locate};
use crate::geometry::{
    ChartMetric, CoordSeq, CoordinateAxes, Coordinates, Distance3dParts, FacetBvh,
    GeometryErrorKind, GeometryKind, HasM, HasZ, LineSeq, Lineal, MColumn, MultiLineString,
    Ordering, PlanarMetric, Point, PreparedLinework, PreparedPlan, Segment, SegmentMetric,
    SegmentProjection, Shape, SharedSlice, distance_3d_with_parts, interpolate_fraction,
    point_distance, point_segment_selection, push_distinct_active_position, same_point,
    segment_projection, shape_axes_all_z, squared_norm_is_trustworthy,
};
use crate::{Finite, HeapSize};
/// Validated `(start, end)` pair for linear-referencing measure/distance bounds.
///
/// Distance substringing validates finiteness here; the ordered-pair guard for
/// `line_substring` runs after [`normalize_line_distance`] in
/// [`crate::geometry::LineIndex::substring`] because negative offsets can reorder the effective
/// span. Measure-based surfaces validate the full finite + `start <= end`
/// invariant at construction.
#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct MeasureRange {
    start: f64,
    end: f64,
}

impl MeasureRange {
    /// Raw distance bounds for `line_substring` (finiteness only — order is
    /// checked post-normalization in [`crate::geometry::LineIndex::substring`]).
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
        crate::geometry::locate_planar_route(self.linework()?.into_iter(), point, normalized)
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
    /// Cached-engine sibling of the `Shape` linear-referencing method.
    pub(crate) fn line_interpolate_point(&self, distance: f64, normalized: bool) -> Result<Shape> {
        let index = self.line_index()?;
        Ok(Shape::Point(if index.has_zero_length_segments() {
            index.interpolate_with_zero_length_segments(distance, normalized, &PlanarMetric)?
        } else {
            index.interpolate(distance, normalized, &PlanarMetric)?
        }))
    }

    pub(crate) fn line_substring(&self, range: MeasureRange, normalized: bool) -> Result<Shape> {
        self.line_index()?
            .substring(range, normalized, &PlanarMetric)
    }

    pub(crate) fn line_locate_point(&self, point: Point, normalized: bool) -> Result<f64> {
        let index = self.line_index()?;
        index.locate_point_prepared(point, normalized, || self.distance_parts())
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
    /// monotonic-M linear referencing). Resolved geographic metrics pass
    /// ellipsoidal chart-edge length; planar metrics pass 2D coordinate
    /// distance, including an explicit planar override on a geographic CRS.
    /// X/Y/Z are untouched — only M is (re)assigned,
    /// and existing M requires `overwrite`.
    pub(crate) fn interpolate_m(
        &self,
        range: MeasureRange,
        overwrite: bool,
        segment_length: impl Fn(Point, Point) -> Result<f64>,
    ) -> Result<Self> {
        let (start_m, end_m) = (range.start(), range.end());
        let parts = self.linework()?;
        if parts.iter().all(|part| part.is_empty()) {
            // Nothing to station: the result is the same empty, now measured
            // like every non-empty result.
            let kind = match self {
                Self::MultiLineString(_) => GeometryKind::MultiLineString,
                Self::Empty(kind, _) => *kind,
                Self::Point(_) => GeometryKind::Point,
                Self::MultiPoint(_) => GeometryKind::MultiPoint,
                Self::LineString(_) => GeometryKind::LineString,
                Self::Polygon(_) => GeometryKind::Polygon,
                Self::MultiPolygon(_) => GeometryKind::MultiPolygon,
                Self::GeometryCollection(_) => GeometryKind::GeometryCollection,
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
        // doubled every chart-edge measurement on a geographic CRS).
        let segment_count = parts.iter().map(|part| part.len().saturating_sub(1)).sum();
        let mut segment_lengths = Vec::with_capacity(segment_count);
        let mut part_seg_ends = Vec::with_capacity(parts.len());
        let mut total = 0.0_f64;
        for part in &parts {
            for [start, end] in part.segment_pairs() {
                let length = segment_length(start, end)?;
                total += length;
                segment_lengths.push(length);
            }
            part_seg_ends.push(segment_lengths.len());
        }
        let span = end_m - start_m;
        let mut traveled = 0.0;
        let mut measured = Vec::with_capacity(parts.len());
        let mut seg_cursor = 0_usize;
        #[expect(
            clippy::indexing_slicing,
            clippy::arithmetic_side_effects,
            reason = "part ends and segment lengths are emitted alongside each part, so the part and segment cursors stay within synchronized columns"
        )]
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
            Self::Point(_)
            | Self::MultiPoint(_)
            | Self::Polygon(_)
            | Self::MultiPolygon(_)
            | Self::GeometryCollection(_)
            | Self::Empty(..) => unreachable!("linework returns only line strings"),
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
    pub(crate) fn line_locate_point_m_with(
        &self,
        point: Point,
        metric: &PlanarMetric,
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

    pub(crate) fn line_locate_point_m_with(
        &self,
        point: Point,
        metric: &PlanarMetric,
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
    /// Deriving endpoint bounds from parts added 0.06–6.98% instructions in measured workloads.
    first_measure: f64,
    last_measure: f64,
}

impl MeasuredLineIndex {
    pub(crate) fn points(&self) -> impl Iterator<Item = Point> {
        self.parts.iter().flat_map(|part| part.into_iter())
    }

    pub(crate) fn chart_locate<M>(
        &self,
        point: Point,
        parts: &ChartParts,
        metric: &M,
    ) -> Result<f64>
    where
        M: crate::geometry::GeodesicMetric
            + crate::geometry::ChartMetric<PointEdgeScratch = crate::crs::PointEdgeScratch>,
    {
        if point.x.is_nan() {
            return Ok(f64::NAN);
        }
        let Some((ordinal, parameter)) = point_chart_locate(point, parts, metric, |_| true)? else {
            return Ok(self.first_measure);
        };
        let part = parts
            .edges
            .get(ordinal)
            .expect("measured ordinal belongs to the flattened route");
        let length = metric.chart_edge_length(&part.edge)?;
        let fraction = metric.chart_edge_arclength_fraction(&part.edge, parameter, Some(length))?;
        Ok(crate::geometry::interpolate_f64(
            part.start.m().unwrap_or(0.0),
            part.end.m().unwrap_or(0.0),
            fraction,
        ))
    }

    pub(crate) fn build(shape: &Shape) -> Result<Self> {
        let parts = match shape {
            Shape::LineString(line) => std::slice::from_ref(line),
            Shape::MultiLineString(lines) => &**lines,
            Shape::Empty(GeometryKind::LineString | GeometryKind::MultiLineString, _) => {
                return Err(MeasuredLineAdmission::Empty.into());
            },
            Shape::Point(_)
            | Shape::MultiPoint(_)
            | Shape::Polygon(_)
            | Shape::MultiPolygon(_)
            | Shape::GeometryCollection(_)
            | Shape::Empty(..) => return Err(MeasuredLineAdmission::NotLineal.into()),
        };
        let mut owned = Vec::with_capacity(parts.len());
        owned.extend(parts.iter().map(|line| line.as_coords().clone()));
        let parts = owned.into_boxed_slice();
        let mut first_measure = None;
        let mut last_measure = None;
        let mut previous_measure = None;
        for part in &parts {
            for point in part {
                let Some(measure) = point.m() else {
                    return Err(MeasuredLineAdmission::MissingMeasure.into());
                };
                if let Some(previous) = previous_measure
                    && measure < previous
                {
                    return Err(MeasuredLineAdmission::NonMonotonic.into());
                }
                first_measure.get_or_insert(measure);
                last_measure = Some(measure);
                previous_measure = Some(measure);
            }
        }
        let Some(first_measure) = first_measure else {
            return Err(MeasuredLineAdmission::Empty.into());
        };
        let Some(last_measure) = last_measure else {
            return Err(MeasuredLineAdmission::Empty.into());
        };
        Ok(Self {
            parts,
            first_measure,
            last_measure,
        })
    }

    #[expect(
        clippy::expect_used,
        reason = "build rejects measured linework without any coordinate"
    )]
    fn last_point(&self) -> Point {
        self.parts
            .iter()
            .rev()
            .find_map(CoordSeq::last)
            .expect("measured line index has a last point")
    }

    fn for_each_window(
        &self,
        mut visit: impl FnMut(usize, Point, Point) -> Result<bool>,
    ) -> Result<()> {
        let mut previous = None;
        let mut ordinal = 0;
        for part in &self.parts {
            for point in part {
                if let Some(start) = previous
                    && !visit(ordinal, start, point)?
                {
                    return Ok(());
                }
                if previous.is_some() {
                    ordinal = ordinal.wrapping_add(1);
                }
                previous = Some(point);
            }
        }
        Ok(())
    }

    fn interpolate_finite<M: SegmentMetric>(&self, target: f64, metric: &M) -> Result<Point> {
        self.interpolate_using(target, |_, start, end, fraction, provenance| {
            let length = metric.admitted_length(start, end)?;
            metric.interpolate(start, end, length, fraction, provenance)
        })
    }

    pub(crate) fn chart_interpolate(
        &self,
        target: f64,
        parts: &ChartParts,
        metric: &impl ChartMetric,
    ) -> Result<Point> {
        let target = Finite::try_new("m", target)?.get();
        self.interpolate_using(target, |ordinal, _, _, fraction, provenance| {
            let part = parts
                .edges
                .get(ordinal)
                .expect("measured ordinal belongs to the route");
            let length = metric.chart_edge_length(&part.edge)?;
            metric.interpolate_prepared(part, length, fraction, provenance)
        })
    }

    fn interpolate_using(
        &self,
        target: f64,
        mut interpolate: impl FnMut(usize, Point, Point, f64, InterpolationProvenance) -> Result<Point>,
    ) -> Result<Point> {
        let target = target.clamp(self.first_measure, self.last_measure);
        let mut result = None;
        self.for_each_window(|ordinal, start, end| -> Result<bool> {
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
            result = Some(interpolate(
                ordinal,
                start,
                end,
                fraction,
                InterpolationProvenance::Independent,
            )?);
            Ok(false)
        })?;
        Ok(result.unwrap_or_else(|| self.last_point()))
    }

    fn locate(&self, point: Point, metric: &PlanarMetric) -> Result<f64> {
        if point.x.is_nan() {
            return Ok(f64::NAN);
        }
        let mut best_distance = f64::INFINITY;
        let mut best_measure = self.first_measure;
        let mut segment_scratch = ();
        let mut previous = None;
        for part in &self.parts {
            for end in part {
                let Some(start) = previous else {
                    previous = Some(end);
                    continue;
                };
                let Some((distance, measure)) = metric.locate_with_measure(
                    point,
                    start,
                    end,
                    0.0,
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
        self.substring_using(range, |_, start, end, fraction, provenance| {
            let length = metric.admitted_length(start, end)?;
            metric.interpolate(start, end, length, fraction, provenance)
        })
    }

    pub(crate) fn chart_substring(
        &self,
        range: MeasureRange,
        parts: &ChartParts,
        metric: &impl ChartMetric,
    ) -> Result<Shape> {
        self.substring_using(range, |ordinal, _, _, fraction, provenance| {
            let part = parts
                .edges
                .get(ordinal)
                .expect("measured ordinal belongs to the route");
            let length = metric.chart_edge_length(&part.edge)?;
            metric.interpolate_prepared(part, length, fraction, provenance)
        })
    }

    fn substring_using(
        &self,
        range: MeasureRange,
        mut interpolate: impl FnMut(usize, Point, Point, f64, InterpolationProvenance) -> Result<Point>,
    ) -> Result<Shape> {
        let (m0, m1) = (range.start(), range.end());
        let (lo, hi) = (self.first_measure, self.last_measure);
        let start = m0.clamp(lo, hi);
        let end = m1.clamp(lo, hi);
        if start.total_cmp(&end) == Ordering::Equal {
            return Ok(Shape::Point(
                self.interpolate_using(start, &mut interpolate)?,
            ));
        }
        let mut result = Vec::new();
        self.for_each_window(|ordinal, segment_start, segment_end| -> Result<bool> {
            let (start_m, end_m) = (
                segment_start.m().unwrap_or(0.0),
                segment_end.m().unwrap_or(0.0),
            );
            if end_m < start || start_m > end {
                return Ok(true);
            }
            let fraction = |measure: f64| interpolate_fraction(start_m, end_m, measure);
            #[expect(clippy::float_cmp, reason = "exact M boundary identity")]
            let mut at = |measure: f64| -> Result<Point> {
                if measure == start_m {
                    Ok(segment_start)
                } else if measure == end_m {
                    Ok(segment_end)
                } else {
                    interpolate(
                        ordinal,
                        segment_start,
                        segment_end,
                        fraction(measure)?,
                        InterpolationProvenance::Embedded,
                    )
                }
            };
            push_distinct_active_position(&mut result, at(start.max(start_m))?);
            push_distinct_active_position(&mut result, at(end.min(end_m))?);
            Ok(true)
        })?;
        Ok(match result.as_slice() {
            [] => Shape::Point(self.interpolate_using(start, &mut interpolate)?),
            [point] => Shape::Point(*point),
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
                ErrorKind::Geometry(_)
                | ErrorKind::Crs(_)
                | ErrorKind::Io(_)
                | ErrorKind::Frame(_)
                | ErrorKind::Resource(_) => Err(error),
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
    let [first, .., _] = coords.as_slice() else {
        return Ok(vec![coords]);
    };
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

    let mut cuts = if coords.len().saturating_mul(points.len()) <= 256 {
        points
            .iter()
            .filter_map(|point| line_point_offset(&coords, *point, tolerance).transpose())
            .collect::<crate::error::Result<Vec<_>>>()?
    } else {
        let linework = PreparedLinework::from_chains(vec![CoordSeq::try_from_points(&coords)?])?;
        let bvh = FacetBvh::build(&linework).expect("a live line has an indexable facet");
        let mut stack = Vec::new();
        let mut cuts = Vec::new();
        let upper_bound = tolerance.next_up() * tolerance.next_up();
        for point in points {
            let mut nearest = None;
            bvh.nearest_point_with_stack::<false, true, true>(
                &linework,
                *point,
                upper_bound,
                tolerance,
                &|_, segment| !same_point(segment.start, segment.end),
                &mut stack,
                &mut nearest,
            )?;
            if let Some(nearest) = nearest
                && let Some(ordinal) = nearest.segment_ordinal()
            {
                let [start, end] = coords
                    .array_windows::<2>()
                    .nth(ordinal)
                    .expect("the selected source segment has two coordinates");
                cuts.push((
                    ordinal,
                    segment_projection(*point, Segment {
                        start: start.xy(),
                        end: end.xy(),
                    })?,
                ));
            }
        }
        cuts
    };
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
                #[expect(
                    clippy::indexing_slicing,
                    reason = "cut ordinals address existing coordinate pairs in the original line"
                )]
                let (start, end) = (coords[cut.0], coords[cut.0.wrapping_add(1)]);
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

    let mut pieces = Vec::with_capacity(cuts.len().wrapping_add(1));
    let mut piece = vec![*first];
    let mut cuts = cuts.into_iter().peekable();
    for (segment_index, pair) in coords.array_windows::<2>().enumerate() {
        while cuts
            .peek()
            .is_some_and(|(cut_index, _)| *cut_index == segment_index)
        {
            let (_, projection) = cuts.next().expect("peeked cut exists");
            let point = projection.interpolate_point(pair[0], pair[1])?;
            push_distinct_active_position(&mut piece, point);
            if piece.len() >= 2 {
                pieces.push(std::mem::replace(&mut piece, vec![point]));
            }
        }
        push_distinct_active_position(&mut piece, pair[1]);
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
    #[cfg(test)]
    use crate::geometry::facet_bvh::NEAREST_WORK;

    let mut best = None;
    let mut best_key = None;
    for (segment_index, [start, end]) in line.segment_pairs().enumerate() {
        #[cfg(test)]
        NEAREST_WORK.with(|work| work.set(work.get().saturating_add(1)));
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
            .interpolate_m(range, false, |a, b| Ok(point_distance(a, b)))
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
                |a, b| Ok(point_distance(a, b)),
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

    #[test]
    fn indexed_split_uses_local_xyzm_windows_for_quarter_cuts() {
        let owner = CoordSeq::try_from_vecs(
            vec![
                -20.0, 0.0, 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 11.0, 12.0, 13.0,
                14.0, 15.0, 16.0, 40.0,
            ],
            vec![
                -40.0, 0.0, 2.0, 4.0, 6.0, 8.0, 10.0, 12.0, 14.0, 16.0, 18.0, 20.0, 22.0, 24.0,
                26.0, 28.0, 30.0, 32.0, 80.0,
            ],
            Some(vec![
                900.0, 10.0, 14.0, 18.0, 22.0, 26.0, 30.0, 34.0, 38.0, 42.0, 46.0, 50.0, 54.0,
                58.0, 62.0, 66.0, 70.0, 74.0, 1_900.0,
            ]),
            Some(vec![
                9_000.0, 100.0, 106.0, 112.0, 118.0, 124.0, 130.0, 136.0, 142.0, 148.0, 154.0,
                160.0, 166.0, 172.0, 178.0, 184.0, 190.0, 196.0, 19_000.0,
            ]),
        )
        .unwrap();
        let window = owner.view(crate::geometry::CoordWindow::from_range(1..18, owner.len()));
        assert_eq!(window.len(), 17);
        let source = Shape::LineString(LineSeq::from_proven(window));

        let splitters = vec![
            Point::new(0.25, 0.5).unwrap(),
            Point::new(1.25, 2.5).unwrap(),
            Point::new(2.25, 4.5).unwrap(),
            Point::new(3.25, 6.5).unwrap(),
            Point::new(4.25, 8.5).unwrap(),
            Point::new(5.25, 10.5).unwrap(),
            Point::new(6.25, 12.5).unwrap(),
            Point::new(7.25, 14.5).unwrap(),
            Point::new(8.25, 16.5).unwrap(),
            Point::new(9.25, 18.5).unwrap(),
            Point::new(10.25, 20.5).unwrap(),
            Point::new(11.25, 22.5).unwrap(),
            Point::new(12.25, 24.5).unwrap(),
            Point::new(13.25, 26.5).unwrap(),
            Point::new(14.25, 28.5).unwrap(),
            Point::new(15.25, 30.5).unwrap(),
            Point::new(0.0, 0.0).unwrap(),
            Point::new(16.0, 32.0).unwrap(),
        ];
        let output = source.split_at_points(&splitters, 0.0).unwrap();
        let records = |shape: &Shape| {
            let Shape::LineString(line) = shape else {
                panic!("line output");
            };
            line.as_coords()
                .iter()
                .map(|point| {
                    [
                        point.x.to_bits(),
                        point.y.to_bits(),
                        point.z.to_bits(),
                        point.m.to_bits(),
                    ]
                })
                .collect::<Vec<_>>()
        };
        assert_eq!(records(output.first().expect("first split piece")), vec![
            [
                0.0_f64.to_bits(),
                0.0_f64.to_bits(),
                10.0_f64.to_bits(),
                100.0_f64.to_bits(),
            ],
            [
                0.25_f64.to_bits(),
                0.5_f64.to_bits(),
                11.0_f64.to_bits(),
                101.5_f64.to_bits(),
            ],
        ]);
        assert_eq!(output.len(), 17);
        for piece in &output {
            let Shape::LineString(line) = piece else {
                panic!("line output");
            };
            assert_eq!(line.as_coords().axes(), CoordinateAxes::XYZM);
        }

        let mut pieces = output.iter();
        pieces.next().expect("first split piece");
        for _ in 0..7 {
            pieces.next().expect("split piece before middle");
        }
        let middle = pieces.next().expect("middle split piece");
        let last = pieces.last().expect("last split piece");

        assert_eq!(records(middle), vec![
            [
                7.25_f64.to_bits(),
                14.5_f64.to_bits(),
                39.0_f64.to_bits(),
                143.5_f64.to_bits(),
            ],
            [
                8.0_f64.to_bits(),
                16.0_f64.to_bits(),
                42.0_f64.to_bits(),
                148.0_f64.to_bits(),
            ],
            [
                8.25_f64.to_bits(),
                16.5_f64.to_bits(),
                43.0_f64.to_bits(),
                149.5_f64.to_bits(),
            ],
        ]);
        assert_eq!(records(last), vec![
            [
                15.25_f64.to_bits(),
                30.5_f64.to_bits(),
                71.0_f64.to_bits(),
                191.5_f64.to_bits(),
            ],
            [
                16.0_f64.to_bits(),
                32.0_f64.to_bits(),
                74.0_f64.to_bits(),
                196.0_f64.to_bits(),
            ],
        ]);
    }
}
