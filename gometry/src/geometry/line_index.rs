//! Prefix-length linear-referencing engine: one build walks the linework
//! and caches per-segment lengths + cumulative offsets, then interpolate /
//! substring / locate run off the prefix table instead of re-measuring every
//! segment per call. Parameterized by a [`SegmentMetric`] so planar and
//! geographic chart-edge referencing share one lifecycle — each build measures
//! every source edge once and caches cumulative offsets.
//!
//! Zero-length segments stay in the route sequence at their unchanged prefix
//! and contribute their stored vertices to interpolation and substrings.
//! Locate skips them, matching the former projection walk; exact prefix
//! boundaries resolve to the first vertex at that prefix, except the end,
//! which resolves to the last stored vertex.

use crate::HeapSize;
use crate::crs::{ChartEdge, PointEdgeScratch};
use crate::error::Result;
use crate::geometry::distance::{
    ChartEdgePart, ChartParts, coordinate_squared_safe, point_chart_locate, squared_space_safe,
};
use crate::geometry::facet_bvh::update_nearest_candidate;
use crate::geometry::{
    ChartMetric, CoordSeq, Coordinates as _, DistanceParts, GeodesicMetric, GeometryErrorKind,
    LineSeq, MeasureRange, NearestCandidate, Ordering, Point, Segment, Shape, ShapeCountQuery, XY,
    lerp_point, linework_first_point, normalize_line_distance, point_distance,
    point_segment_distance_and_projection, point_segment_selection, push_distinct_active_position,
    segment_projection,
};

#[derive(Clone, Copy, Eq, PartialEq)]
pub(crate) enum InterpolationProvenance {
    Independent,
    Embedded,
}

/// Per-segment measurement for the linear-referencing engine.
pub(crate) trait SegmentMetric {
    /// Length of the segment `start`–`end` in this metric.
    fn admitted_length(&self, start: Point, end: Point) -> Result<f64>;
    /// The point at an arclength fraction, with spelling selected by its
    /// output context.
    fn interpolate(
        &self,
        start: Point,
        end: Point,
        length: f64,
        fraction: f64,
        provenance: InterpolationProvenance,
    ) -> Result<Point>;
    fn interpolate_prepared(
        &self,
        part: &ChartEdgePart,
        length: f64,
        fraction: f64,
        provenance: InterpolationProvenance,
    ) -> Result<Point> {
        self.interpolate(part.start, part.end, length, fraction, provenance)
    }
}

/// Planar (coordinate-unit) metric.
pub(crate) struct PlanarMetric;

impl SegmentMetric for PlanarMetric {
    fn admitted_length(&self, start: Point, end: Point) -> Result<f64> {
        // The guarded sqrt form: a libm `hypot` per segment is the dominant
        // cost of the cold prefix build (38% of `line_interpolate_point` in
        // profile), and lengths are measurement arithmetic.
        Ok(point_distance(start, end))
    }

    fn interpolate(
        &self,
        start: Point,
        end: Point,
        _length: f64,
        fraction: f64,
        _provenance: InterpolationProvenance,
    ) -> Result<Point> {
        Ok(lerp_point(start, end, fraction))
    }
}

impl PlanarMetric {
    pub(crate) fn locate_with_measure(
        &self,
        probe: Point,
        start: Point,
        end: Point,
        _length: f64,
        _best: f64,
        _scratch: &mut (),
    ) -> Result<Option<(f64, f64)>> {
        let (distance, projection) = point_segment_distance_and_projection(probe.xy(), Segment {
            start: start.into(),
            end: end.into(),
        })?;
        // Keep the certified projection ratio until measure interpolation;
        // converting a subnormal ratio to along-distance and back loses it.
        Ok(Some((
            distance,
            projection.interpolate_point(start, end)?.m().unwrap_or(0.0),
        )))
    }
}

/// Locate one query while measuring its route, without a reusable prefix index.
pub(crate) fn locate_planar_route<'a>(
    lines: impl Iterator<Item = &'a CoordSeq>,
    probe: Point,
    normalized: bool,
) -> Result<f64> {
    let mut has_vertex = false;
    let mut total = 0.0;
    let mut best_distance = f64::INFINITY;
    let mut search: Result<Option<NearestCandidate>> = Ok(None);
    let mut stationing = None;
    let mut base = 0_usize;
    for line in lines {
        has_vertex |= !line.is_empty();
        for (local, (xs, ys)) in line
            .xs()
            .array_windows::<2>()
            .zip(line.ys().array_windows::<2>())
            .enumerate()
        {
            let ordinal = base.wrapping_add(local);
            let [ax, bx] = *xs;
            let [ay, by] = *ys;
            let segment = Segment {
                start: XY::new(ax, ay),
                end: XY::new(bx, by),
            };
            let measured = point_distance(segment.start, segment.end);
            if !measured.is_finite() {
                return Err(GeometryErrorKind::message(
                    "line index cannot include a non-finite segment length",
                ));
            }
            let offset = total;
            if measured != 0.0 {
                total += measured;
            }
            if !total.is_finite() {
                return Err(GeometryErrorKind::message(
                    "line index total length must be finite",
                ));
            }
            if measured == 0.0 || probe.x.is_nan() {
                continue;
            }
            let gap = (probe.x - segment.start.x.max(segment.end.x))
                .max(segment.start.x.min(segment.end.x) - probe.x)
                .max(probe.y - segment.start.y.max(segment.end.y))
                .max(segment.start.y.min(segment.end.y) - probe.y);
            if gap.is_normal() && gap > best_distance * 2.0 {
                continue;
            }
            if let Ok(best) = &mut search {
                let candidate =
                    point_segment_selection(probe.xy(), segment).and_then(|selection| {
                        let distance = selection.distance();
                        update_nearest_candidate!(&selection, probe, segment, || ordinal, best)
                            .map(|changed| (changed, distance))
                    });
                match candidate {
                    Ok((true, distance)) => {
                        best_distance = distance;
                        // Subtract accumulated prefixes, as on the prepared path:
                        // at a large station the stored segment length can round.
                        stationing = Some((offset, total - offset, segment));
                    },
                    Ok((false, _)) => {},
                    // Finish source admission before reporting a projection error.
                    Err(error) => search = Err(error),
                }
            }
        }
        base = base.wrapping_add(line.len().saturating_sub(1));
    }
    if !has_vertex {
        return Err(GeometryErrorKind::EmptyLinework.into());
    }
    if probe.x.is_nan() {
        return Ok(f64::NAN);
    }
    if total == 0.0 {
        return Ok(0.0);
    }
    search?;
    let Some((offset, length, segment)) = stationing else {
        return Ok(0.0);
    };
    let projection = segment_projection(probe, segment)?;
    let station = offset + projection.scale(length)?;
    Ok(if normalized { station / total } else { station })
}

/// Geographic linear referencing uses the same chart-edge owner as proximity.
impl<M: ChartMetric> SegmentMetric for M {
    fn admitted_length(&self, start: Point, end: Point) -> Result<f64> {
        let edge = self.chart_edge(start, end)?;
        self.chart_edge_length(&edge)
    }

    fn interpolate(
        &self,
        start: Point,
        end: Point,
        length: f64,
        fraction: f64,
        provenance: InterpolationProvenance,
    ) -> Result<Point> {
        let edge = self.chart_edge(start, end)?;
        interpolate_chart_edge(self, start, end, &edge, length, fraction, provenance)
    }

    fn interpolate_prepared(
        &self,
        part: &ChartEdgePart,
        length: f64,
        fraction: f64,
        provenance: InterpolationProvenance,
    ) -> Result<Point> {
        interpolate_chart_edge(
            self, part.start, part.end, &part.edge, length, fraction, provenance,
        )
    }
}

fn interpolate_chart_edge(
    metric: &impl ChartMetric,
    start: Point,
    end: Point,
    edge: &ChartEdge,
    length: f64,
    fraction: f64,
    provenance: InterpolationProvenance,
) -> Result<Point> {
    let parameter = metric.chart_edge_locate_t(edge, length * fraction, length)?;
    let point = metric.chart_edge_witness(start, end, edge, parameter, Some(length))?;
    if provenance == InterpolationProvenance::Independent {
        return Ok(point);
    }
    let lifted_position = edge.chart_position(parameter)?;
    let longitude = metric
        .chart_frame()
        .longitude()
        .nearest_longitude_alias(lifted_position.x, point.x)
        .map_err(|error| crate::crs::CrsError::message(error.to_string()))?;
    Ok(point.with_xy_raw(longitude, point.y))
}

/// Cache slot for a built (or unbuildable) [`LineIndex`].
///
/// `OnceLock` needs a stored value, and `GeometryErrorKind` is not `Clone`, so
/// the two build failures are remembered as variants and re-raised by the
/// accessor.
pub(crate) enum LineIndexSlot {
    Index(LineIndex),
    NotLineal,
    Empty,
    /// Fallible construction (non-finite segment/total length, etc.).
    Invalid(String),
}

impl LineIndexSlot {
    pub(crate) fn build(shape: &Shape, metric: &impl SegmentMetric) -> Result<Self> {
        use crate::error::ErrorKind;
        match LineIndex::build(shape, metric) {
            Ok(index) => Ok(Self::Index(index)),
            Err(error) => match error.kind() {
                ErrorKind::Geometry(GeometryErrorKind::EmptyLinework) => Ok(Self::Empty),
                ErrorKind::Geometry(GeometryErrorKind::LineStringRequired) => Ok(Self::NotLineal),
                ErrorKind::Resource(_) => Err(error),
                ErrorKind::Geometry(_)
                | ErrorKind::Crs(_)
                | ErrorKind::Io(_)
                | ErrorKind::Frame(_) => Ok(Self::Invalid(error.to_string())),
            },
        }
    }

    pub(crate) fn get(&self) -> Result<&LineIndex> {
        match self {
            Self::Index(index) => Ok(index),
            Self::NotLineal => Err(GeometryErrorKind::LineStringRequired.into()),
            Self::Empty => Err(GeometryErrorKind::EmptyLinework.into()),
            Self::Invalid(msg) => Err(GeometryErrorKind::message(Box::<str>::from(msg.as_str()))),
        }
    }
}

/// The prefix-length index over one shape's linework, in one metric.
/// Each stored segment has FULL ordinate carry — the planar [`Segment`] is
/// XY-only by design, and LRS interpolates Z/M.
#[derive(Clone, Copy)]
pub(crate) struct OrdinateSegment {
    start: Point,
    end: Point,
}

crate::heapless!(OrdinateSegment);

pub(crate) trait LineSegments: HeapSize {
    fn len(&self) -> usize;
    fn segment(&self, index: usize) -> OrdinateSegment;
    fn iter(&self) -> impl Iterator<Item = OrdinateSegment>;
    fn interpolate<M: SegmentMetric>(
        &self,
        index: usize,
        metric: &M,
        length: f64,
        fraction: f64,
        provenance: InterpolationProvenance,
    ) -> Result<Point>;
}

impl LineSegments for Box<[OrdinateSegment]> {
    fn len(&self) -> usize {
        self.as_ref().len()
    }
    fn segment(&self, index: usize) -> OrdinateSegment {
        *self
            .as_ref()
            .get(index)
            .expect("segment ordinal belongs to the route")
    }
    fn iter(&self) -> impl Iterator<Item = OrdinateSegment> {
        self.as_ref().iter().copied()
    }
    fn interpolate<M: SegmentMetric>(
        &self,
        index: usize,
        metric: &M,
        length: f64,
        fraction: f64,
        provenance: InterpolationProvenance,
    ) -> Result<Point> {
        let segment = self.segment(index);
        metric.interpolate(segment.start, segment.end, length, fraction, provenance)
    }
}

impl LineSegments for std::sync::Arc<ChartParts> {
    fn len(&self) -> usize {
        self.edges.len()
    }
    fn segment(&self, index: usize) -> OrdinateSegment {
        let part = self
            .edges
            .get(index)
            .expect("chart-edge ordinal belongs to the route");
        OrdinateSegment {
            start: part.start,
            end: part.end,
        }
    }
    fn iter(&self) -> impl Iterator<Item = OrdinateSegment> {
        self.edges.iter().map(|part| OrdinateSegment {
            start: part.start,
            end: part.end,
        })
    }
    fn interpolate<M: SegmentMetric>(
        &self,
        index: usize,
        metric: &M,
        length: f64,
        fraction: f64,
        provenance: InterpolationProvenance,
    ) -> Result<Point> {
        let part = self
            .edges
            .get(index)
            .expect("chart-edge ordinal belongs to the route");
        metric.interpolate_prepared(part, length, fraction, provenance)
    }
}

pub(crate) type ChartLineIndex = LineIndex<std::sync::Arc<ChartParts>>;

pub(crate) struct LineIndex<S = Box<[OrdinateSegment]>> {
    /// All stored segments, in linework order. Zero-length segments share a
    /// prefix with the surrounding route vertices.
    segments: S,
    /// `prefix[i]` = cumulative length before segment `i`; the last entry is
    /// the cached total length. Lengths are cached here — locate's
    /// `mul_add(length, fraction, offset)` and every interpolation walk read
    /// them back instead of re-measuring.
    prefix: Box<[f64]>,
    /// Stored separately: deriving it added 0.17–2.08% instructions in measured paths.
    total: f64,
    first: Point,
    last: Point,
    has_zero_segments: bool,
}

impl LineIndex {
    pub(crate) const fn has_zero_length_segments(&self) -> bool {
        self.has_zero_segments
    }

    fn storage(upper: Option<usize>) -> (Vec<OrdinateSegment>, Vec<f64>) {
        let mut segments = Vec::new();
        let mut prefix = Vec::new();
        if let Some(upper) = upper {
            let _segments_reservation = segments.try_reserve(upper);
            if let Some(prefix_capacity) = upper.checked_add(1) {
                let _prefix_reservation = prefix.try_reserve(prefix_capacity);
            }
        }
        (segments, prefix)
    }

    /// Build over `shape`'s linework. `LineStringRequired` for non-lineal
    /// shapes and `EmptyLinework` when there is no first vertex. Zero-total
    /// linework retains both endpoint identities.
    pub(crate) fn build(shape: &Shape, metric: &impl SegmentMetric) -> Result<Self> {
        let lines = shape.linework()?;
        let first = linework_first_point(&lines).ok_or(GeometryErrorKind::EmptyLinework)?;
        let upper = ShapeCountQuery::default().segments(shape).ok();
        let (mut segments, mut prefix) = Self::storage(upper);
        prefix.push(0.0);
        let mut total = 0.0;
        let mut last = first;
        let mut has_zero_segments = false;
        for line in &lines {
            for [start, end] in line.segment_pairs() {
                last = end;
                let length = metric.admitted_length(start, end)?;
                // Non-finite lengths (overflowed extreme segments) would poison
                // every subsequent prefix entry — reject construction.
                if !length.is_finite() {
                    return Err(GeometryErrorKind::message(
                        "line index cannot include a non-finite segment length",
                    ));
                }
                if length == 0.0 {
                    has_zero_segments = true;
                    segments.push(OrdinateSegment { start, end });
                    prefix.push(total);
                    continue;
                }
                // Plain sum: blanket Kahan on LRS prefixes is nonfree (+4–33%
                // planar) — N11 closed under free-or-nearly-free. Standalone
                // shape-length folds compensate separately in measure.rs.
                total += length;
                if !total.is_finite() {
                    return Err(GeometryErrorKind::message(
                        "line index total length must be finite",
                    ));
                }
                segments.push(OrdinateSegment { start, end });
                prefix.push(total);
            }
        }
        // The walks fall back to the linework's last vertex (zero-length tail
        // segments included), not the last measured segment's end.
        let last = lines
            .iter()
            .rev()
            .find_map(|line| line.last())
            .unwrap_or(last);
        let segments = segments.into_boxed_slice();
        let prefix = prefix.into_boxed_slice();
        Ok(Self {
            segments,
            prefix,
            total,
            first,
            last,
            has_zero_segments,
        })
    }

    /// Build over one line's zero-copy [`CoordSeq`] window — the packed-`Lines`
    /// batch lane reads CSR rows through this with no per-row `Shape` / `Arc`
    /// traffic (the measured gap vs the `iter_rows` + transient fallback).
    pub(crate) fn build_coordseq(coords: &CoordSeq, metric: &impl SegmentMetric) -> Result<Self> {
        let first = coords.first().ok_or(GeometryErrorKind::EmptyLinework)?;
        let upper = coords.len().saturating_sub(1);
        let (mut segments, mut prefix) = Self::storage(Some(upper));
        prefix.push(0.0);
        let mut total = 0.0;
        let mut last = first;
        let mut has_zero_segments = false;
        for [start, end] in coords.segment_pairs() {
            last = end;
            let length = metric.admitted_length(start, end)?;
            if !length.is_finite() {
                return Err(GeometryErrorKind::message(
                    "line index cannot include a non-finite segment length",
                ));
            }
            if length == 0.0 {
                has_zero_segments = true;
                segments.push(OrdinateSegment { start, end });
                prefix.push(total);
                continue;
            }
            total += length;
            if !total.is_finite() {
                return Err(GeometryErrorKind::message(
                    "line index total length must be finite",
                ));
            }
            segments.push(OrdinateSegment { start, end });
            prefix.push(total);
        }
        let last = coords.last().unwrap_or(last);
        let segments = segments.into_boxed_slice();
        let prefix = prefix.into_boxed_slice();
        Ok(Self {
            segments,
            prefix,
            total,
            first,
            last,
            has_zero_segments,
        })
    }
}

impl ChartLineIndex {
    pub(crate) fn build_chart(shape: &Shape, metric: &impl ChartMetric) -> Result<Self> {
        let parts = std::sync::Arc::new(ChartParts::build(shape, metric)?);
        Self::from_chart_parts(shape, parts, metric)
    }

    pub(crate) fn build_chart_coordseq(
        coords: &CoordSeq,
        metric: &impl ChartMetric,
    ) -> Result<Self> {
        Self::build_chart(
            &Shape::LineString(LineSeq::from_proven(coords.clone())),
            metric,
        )
    }

    pub(crate) fn from_chart_parts(
        shape: &Shape,
        parts: std::sync::Arc<ChartParts>,
        metric: &impl ChartMetric,
    ) -> Result<Self> {
        let lines = shape.linework()?;
        let first = linework_first_point(&lines).ok_or(GeometryErrorKind::EmptyLinework)?;
        let last = lines
            .iter()
            .rev()
            .find_map(|line| line.last())
            .unwrap_or(first);
        let mut prefix = Vec::with_capacity(parts.edges.len().saturating_add(1));
        prefix.push(0.0);
        let mut total = 0.0;
        let mut has_zero_segments = false;
        for part in &parts.edges {
            let length = metric.chart_edge_length(&part.edge)?;
            if !length.is_finite() {
                return Err(GeometryErrorKind::message(
                    "line index cannot include a non-finite segment length",
                ));
            }
            has_zero_segments |= length == 0.0;
            total += length;
            if !total.is_finite() {
                return Err(GeometryErrorKind::message(
                    "line index total length must be finite",
                ));
            }
            prefix.push(total);
        }
        Ok(Self {
            segments: parts,
            prefix: prefix.into_boxed_slice(),
            total,
            first,
            last,
            has_zero_segments,
        })
    }

    pub(crate) fn locate_chart_point<M>(
        &self,
        probe: Point,
        normalized: bool,
        metric: &M,
    ) -> Result<f64>
    where
        M: GeodesicMetric + ChartMetric<PointEdgeScratch = PointEdgeScratch>,
    {
        if probe.x.is_nan() {
            return Ok(f64::NAN);
        }
        if self.total == 0.0 {
            return Ok(0.0);
        }
        let Some((ordinal, parameter)) =
            point_chart_locate(probe, &self.segments, metric, |part| {
                !part.edge.is_degenerate()
            })?
        else {
            return Ok(0.0);
        };
        let part = self
            .segments
            .edges
            .get(ordinal)
            .expect("nearest ordinal belongs to the route");
        let prefix = *self
            .prefix
            .get(ordinal)
            .expect("nearest ordinal has a station prefix");
        let station = prefix + metric.chart_edge_length_to(&part.edge, parameter)?;
        Ok(if normalized {
            station / self.total
        } else {
            station
        })
    }

    pub(crate) const fn parts(&self) -> &std::sync::Arc<ChartParts> {
        &self.segments
    }
}

impl<S: LineSegments> LineIndex<S> {
    /// The segment containing `target` (a clamped `[0, total]` measure) and
    /// the local offset into it. An exact boundary resolves to the segment it
    /// ends, matching the old `target <= length` walk.
    fn segment_at(&self, target: f64) -> Option<(usize, f64)> {
        if self.segments.len() == 0 {
            return None;
        }
        let index = self
            .prefix
            .partition_point(|&boundary| boundary < target)
            .saturating_sub(1)
            .min(self.segments.len().wrapping_sub(1));
        #[expect(
            clippy::indexing_slicing,
            reason = "the nonempty segment slice and one-entry-longer prefix are built together"
        )]
        Some((index, target - self.prefix[index]))
    }

    /// The point at `distance` along the linework (the engine behind
    /// `line_interpolate_point`). Caller has validated finiteness.
    pub(crate) fn interpolate(
        &self,
        distance: f64,
        normalized: bool,
        metric: &impl SegmentMetric,
    ) -> Result<Point> {
        if self.total == 0.0 {
            return Ok(self.first);
        }
        let target = normalize_line_distance(distance, self.total, normalized);
        let Some((index, local)) = self.segment_at(target) else {
            return Ok(self.last);
        };
        let segment = self.segments.segment(index);
        #[expect(
            clippy::indexing_slicing,
            reason = "segment_at returns an index with a following prefix entry"
        )]
        let length = self.prefix[index.wrapping_add(1)] - self.prefix[index];
        if local == 0.0 {
            return Ok(segment.start);
        }
        #[expect(clippy::float_cmp, reason = "exact prefix boundary identity")]
        if local == length {
            return Ok(segment.end);
        }
        self.segments.interpolate(
            index,
            metric,
            length,
            local / length,
            InterpolationProvenance::Independent,
        )
    }

    pub(crate) fn interpolate_route_position(
        &self,
        distance: f64,
        normalized: bool,
        metric: &impl SegmentMetric,
    ) -> Result<Point> {
        if self.has_zero_segments {
            self.interpolate_with_zero_length_segments(distance, normalized, metric)
        } else {
            self.interpolate(distance, normalized, metric)
        }
    }

    pub(crate) fn interpolate_with_zero_length_segments(
        &self,
        distance: f64,
        normalized: bool,
        metric: &impl SegmentMetric,
    ) -> Result<Point> {
        if self.total == 0.0 {
            return Ok(self.interpolate_zero_total(distance, normalized));
        }
        let target = normalize_line_distance(distance, self.total, normalized);
        if target.total_cmp(&self.total) == Ordering::Equal {
            return Ok(self.last);
        }
        let Some((index, local)) = self.segment_at(target) else {
            return Ok(self.last);
        };
        let segment = self.segments.segment(index);
        #[expect(
            clippy::indexing_slicing,
            reason = "segment_at returns an index with a following prefix entry"
        )]
        let length = self.prefix[index.wrapping_add(1)] - self.prefix[index];
        if local == 0.0 {
            return Ok(segment.start);
        }
        #[expect(clippy::float_cmp, reason = "exact prefix boundary identity")]
        if local == length {
            return Ok(segment.end);
        }
        self.segments.interpolate(
            index,
            metric,
            length,
            local / length,
            InterpolationProvenance::Independent,
        )
    }

    fn interpolate_zero_total(&self, distance: f64, normalized: bool) -> Point {
        if normalized && distance >= 1.0 {
            self.last
        } else {
            self.first
        }
    }

    /// The sub-line between two distances (the engine behind
    /// `line_substring`). Caller has validated finiteness. Effective
    /// distances (after fraction/negative-offset resolution) must be
    /// ordered — a swapped pair is the classic argument typo, so it errors
    /// rather than silently reversing; `reverse()` is the deliberate move.
    pub(crate) fn substring(
        &self,
        range: MeasureRange,
        normalized: bool,
        metric: &impl SegmentMetric,
    ) -> Result<Shape> {
        if self.total == 0.0 && normalized && range.start() > range.end() {
            return Err(GeometryErrorKind::SubstringOrder(range.start(), range.end()).into());
        }
        let zero_length_normalized_span = self.total == 0.0
            && normalized
            && range.start().clamp(0.0, 1.0) < range.end().clamp(0.0, 1.0);
        if self.total == 0.0 && !zero_length_normalized_span {
            let endpoint = if normalized && range.start().clamp(0.0, 1.0) >= 1.0 {
                self.last
            } else {
                self.first
            };
            return Ok(Shape::Point(endpoint));
        }
        let (start_distance, end_distance) = (range.start(), range.end());
        let start = normalize_line_distance(start_distance, self.total, normalized);
        let end = normalize_line_distance(end_distance, self.total, normalized);
        if start > end {
            return Err(GeometryErrorKind::SubstringOrder(start_distance, end_distance).into());
        }
        if start.total_cmp(&end) == Ordering::Equal && !zero_length_normalized_span {
            return Ok(Shape::Point(
                self.interpolate_route_position(start, false, metric)?,
            ));
        }

        let mut result = Vec::new();
        // Only the overlapping prefix window is walked; semantics (distinct
        // consecutive points, segment-end offsets) match the full scan.
        let from_index = self.prefix.partition_point(|&boundary| boundary < start);
        for (index, (segment, prefix_pair)) in self
            .segments
            .iter()
            .zip(self.prefix.array_windows::<2>())
            .enumerate()
            .skip(from_index.saturating_sub(1))
        {
            let [segment_start, segment_end] = *prefix_pair;
            if segment_end < start {
                continue;
            }
            if segment_start > end {
                break;
            }
            let from = start.max(segment_start);
            let to = end.min(segment_end);
            if from > to {
                continue;
            }
            let length = segment_end - segment_start;
            if length == 0.0 {
                if start <= segment_start && segment_start <= end {
                    push_distinct_active_position(&mut result, segment.start);
                    push_distinct_active_position(&mut result, segment.end);
                }
                continue;
            }
            // Boundary fractions resolve to the exact stored endpoints — no
            // metric call (the interior windows of a substring walk are all
            // full segments, so this skips their edge evaluations). The
            // comparisons are exact by design: `from`/`to` are clamped to
            // the same prefix values they compare against.
            #[expect(clippy::float_cmp, reason = "exact prefix boundary identity")]
            let at = |target: f64| -> Result<Point> {
                if target == segment_start {
                    Ok(segment.start)
                } else if target == segment_end {
                    Ok(segment.end)
                } else {
                    self.segments.interpolate(
                        index,
                        metric,
                        length,
                        (target - segment_start) / length,
                        InterpolationProvenance::Embedded,
                    )
                }
            };
            push_distinct_active_position(&mut result, at(from)?);
            push_distinct_active_position(&mut result, at(to)?);
        }

        Ok(match result.as_slice() {
            [] => Shape::Point(self.interpolate_route_position(start, false, metric)?),
            [point] => Shape::Point(*point),
            _ => Shape::LineString(LineSeq::from_proven(CoordSeq::try_from_points(&result)?)),
        })
    }
}

impl LineIndex {
    /// The along-line offset of `probe`'s nearest projection (the engine
    /// behind `line_locate_point`). Selection shares proximity's exact key:
    /// source distance, projection provenance, then segment ordinal.
    pub(crate) fn locate_point_prepared<'a>(
        &self,
        probe: Point,
        normalized: bool,
        distance_parts: impl FnOnce() -> Result<&'a DistanceParts>,
    ) -> Result<f64> {
        self.locate_point_with_search(probe, normalized, || {
            self.nearest_planar_prepared(probe, distance_parts()?)
        })
    }

    fn locate_point_with_search(
        &self,
        probe: Point,
        normalized: bool,
        search: impl FnOnce() -> Result<Option<NearestCandidate>>,
    ) -> Result<f64> {
        // POINT EMPTY reaches a locate as the NaN point of a point column.
        if probe.x.is_nan() {
            return Ok(f64::NAN);
        }
        if self.total == 0.0 {
            return Ok(0.0);
        }
        let Some(candidate) = search()? else {
            return Ok(0.0);
        };
        let ordinal = candidate
            .segment_ordinal()
            .expect("locate selects a linework segment");
        let segment = self.segments.segment(ordinal);
        let offset = *self
            .prefix
            .get(ordinal)
            .expect("nearest ordinal has a station prefix");
        let next = *self
            .prefix
            .get(ordinal.wrapping_add(1))
            .expect("nearest ordinal has a trailing station prefix");
        let length = next - offset;
        let source_segment = Segment {
            start: segment.start.xy(),
            end: segment.end.xy(),
        };
        let projection = segment_projection(probe, source_segment)?;
        let station = offset + projection.scale(length)?;
        let result = if normalized {
            station / self.total
        } else {
            station
        };
        Ok(result)
    }

    fn nearest_planar_prepared(
        &self,
        probe: Point,
        parts: &DistanceParts,
    ) -> Result<Option<NearestCandidate>> {
        let squared = squared_space_safe(parts) && coordinate_squared_safe(probe);
        let keep = |_: Point, segment: Segment| {
            !self.has_zero_segments || point_distance(segment.start, segment.end) != 0.0
        };
        let mut best = None;
        match (squared, parts.bvh()) {
            (true, Some(bvh)) => {
                bvh.nearest_to_points::<true>(
                    &parts.linework,
                    std::iter::once(probe),
                    &keep,
                    &mut best,
                )?;
            },
            (false, Some(bvh)) => {
                bvh.nearest_to_points::<false>(
                    &parts.linework,
                    std::iter::once(probe),
                    &keep,
                    &mut best,
                )?;
            },
            (true, None) => {
                parts.linework.nearest_to_points::<true>(
                    std::iter::once(probe),
                    &keep,
                    &mut best,
                )?;
            },
            (false, None) => {
                parts.linework.nearest_to_points::<false>(
                    std::iter::once(probe),
                    &keep,
                    &mut best,
                )?;
            },
        }
        Ok(best)
    }
}

impl HeapSize for LineIndexSlot {
    fn heap_bytes(&self) -> usize {
        match self {
            Self::Index(index) => index.heap_bytes(),
            Self::NotLineal | Self::Empty => 0,
            Self::Invalid(msg) => msg.capacity(),
        }
    }
}

impl<S: LineSegments> HeapSize for LineIndex<S> {
    fn heap_bytes(&self) -> usize {
        self.segments
            .heap_bytes()
            .saturating_add(self.prefix.heap_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn planar_locate_orders_segments_by_source_distance() {
        let points = [
            Point::new_raw_xy(-1.0, (-1.0_f64).next_up()),
            Point::new_raw_xy(1.0, 1.0),
            Point::new_raw_xy(0.5, 0.5),
        ];
        let line = Shape::LineString(
            LineSeq::try_new(CoordSeq::try_from_points(&points).unwrap()).unwrap(),
        );
        assert_eq!(line.line_locate_point(points[2], true).unwrap(), 1.0);
    }

    #[test]
    fn planar_locate_selects_exact_nearest_when_distances_round_to_a_tie() {
        let shape = crate::io::parse_wkt(
            "MULTILINESTRING ((0.000000007450580596923828125 1, 1 1), (0 1, -1 1))",
        )
        .expect("valid multipart line");
        let probe = Point::new_raw_xy(0.0, 0.0);
        let prepared = crate::geometry::PreparedPlan::new(shape);
        let station = prepared
            .line_locate_point(probe, false)
            .expect("prepared nearest search");
        // The nearest endpoint is the start of segment 1. Segment 0 has
        // horizontal length 1 - 2^-27, which is exactly representable.
        assert_eq!(
            station.to_bits(),
            0x3FEF_FFFF_FC00_0000,
            "exact locate selects the second segment despite rounded-distance equality"
        );
        let query = crate::geometry::PreparedPlan::new(Shape::Point(probe));
        let (_, nearest) = query
            .nearest_points(&prepared)
            .expect("proximity query")
            .expect("nonempty nearest pair");
        assert_eq!(nearest.x.to_bits(), 0.0_f64.to_bits());
        assert_eq!(nearest.y.to_bits(), 1.0_f64.to_bits());
    }

    #[test]
    fn planar_locate_retains_a_finite_station_when_query_distance_overflows() {
        use crate::geometry::point_pair_distance_key;
        let coordinates = CoordSeq::try_from_points(&[
            Point::new_raw_xy(1e308, 0.0),
            Point::new_raw_xy(1e308, 1.0),
        ])
        .unwrap();
        let shape = Shape::LineString(LineSeq::try_new(coordinates.clone()).unwrap());
        let probe = Point::new_raw_xy(-1e308, 0.5);
        let prepared = crate::geometry::PreparedPlan::new(shape);
        assert_eq!(
            prepared.line_locate_point(probe, false).unwrap().to_bits(),
            0.5_f64.to_bits(),
        );
        assert_eq!(
            locate_planar_route(std::iter::once(&coordinates), probe, false)
                .unwrap()
                .to_bits(),
            0.5_f64.to_bits(),
        );
        let old_point = Point::new_raw_xy(1e308, 0.0);
        let new_point = Point::new_raw_xy(1e308, 0.5);
        assert_eq!(
            point_distance(probe, old_point).to_bits(),
            f64::INFINITY.to_bits()
        );
        assert_eq!(
            point_distance(probe, new_point).to_bits(),
            f64::INFINITY.to_bits()
        );
        assert!(
            point_pair_distance_key(probe.xy(), new_point.xy())
                .cmp_distance(&point_pair_distance_key(probe.xy(), old_point.xy()))
                .unwrap()
                .is_lt()
        );
        assert_locate_interpolate_matches_proximity(&prepared, probe);
    }

    #[test]
    fn planar_locate_prefers_interior_on_endpoint_interior_exact_tie() {
        let shape = crate::io::parse_wkt("MULTILINESTRING ((0 1, 1 2), (-1 1, 1 1))")
            .expect("valid multipart line");
        let probe = Point::new_raw_xy(0.0, 0.0);
        let prepared = crate::geometry::PreparedPlan::new(shape);
        assert_eq!(
            prepared
                .line_locate_point(probe, false)
                .expect("exact nearest tie")
                .to_bits(),
            2.414_213_562_373_095_f64.to_bits(),
            "segment 1's interior station is the diagonal length sqrt(2) plus one"
        );
    }

    fn assert_locate_interpolate_matches_proximity(
        prepared: &crate::geometry::PreparedPlan,
        probe: Point,
    ) {
        let station = prepared.line_locate_point(probe, false).unwrap();
        let Shape::Point(interpolated) = prepared.line_interpolate_point(station, false).unwrap()
        else {
            unreachable!("line interpolation returns a point")
        };
        let query = crate::geometry::PreparedPlan::new(Shape::Point(probe));
        let (_, nearest) = query.nearest_points(prepared).unwrap().unwrap();
        assert_eq!(interpolated.x.to_bits(), nearest.x.to_bits());
        assert_eq!(interpolated.y.to_bits(), nearest.y.to_bits());
    }

    #[test]
    fn planar_locate_interpolate_matches_proximity_away_from_part_boundaries() {
        for (wkt, probe) in [
            ("LINESTRING (0 0, 8 0)", Point::new_raw_xy(2.0, 1.0)),
            ("LINESTRING (0 0, 0 4, 4 4)", Point::new_raw_xy(2.0, 5.0)),
            (
                "MULTILINESTRING ((0 0, 2 0), (10 0, 14 0))",
                Point::new_raw_xy(12.0, 1.0),
            ),
        ] {
            let prepared = crate::geometry::PreparedPlan::new(crate::io::parse_wkt(wkt).unwrap());
            assert_locate_interpolate_matches_proximity(&prepared, probe);
        }
    }

    #[test]
    fn planar_one_shot_locate_streams_without_route_allocations() {
        let coordinates =
            CoordSeq::try_from_points(&[Point::new_raw_xy(0.0, 0.0), Point::new_raw_xy(8.0, 0.0)])
                .unwrap();
        let (station, allocations) = crate::test_support::allocation_count(|| {
            locate_planar_route(
                std::iter::once(&coordinates),
                Point::new_raw_xy(2.0, 1.0),
                true,
            )
            .unwrap()
        });
        assert_eq!(station.to_bits(), 0.25_f64.to_bits());
        assert_eq!(allocations, 0);
    }

    #[test]
    fn planar_one_shot_locate_admits_source_lengths_before_empty_query() {
        let coordinates = CoordSeq::try_from_points(&[
            Point::new_raw_xy(-1e308, 0.0),
            Point::new_raw_xy(1e308, 0.0),
        ])
        .unwrap();
        let old_error = LineIndex::build_coordseq(&coordinates, &PlanarMetric)
            .err()
            .expect("source segment length overflows")
            .to_string();
        let new_error = locate_planar_route(
            std::iter::once(&coordinates),
            Point::new_raw_xy(f64::NAN, f64::NAN),
            false,
        )
        .unwrap_err()
        .to_string();
        assert_eq!(new_error, old_error);
    }

    #[test]
    fn planar_locate_ordinals_preserve_shared_multipart_occurrences() {
        use crate::geometry::{CoordinateAxes, MultiLineString, MultiMember};
        let repeated = LineSeq::try_new(
            CoordSeq::try_from_points(&[Point::new_raw_xy(0.0, 0.0), Point::new_raw_xy(2.0, 0.0)])
                .unwrap(),
        )
        .unwrap();
        let final_line = LineSeq::try_new(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(10.0, 0.0),
                Point::new_raw_xy(14.0, 0.0),
            ])
            .unwrap(),
        )
        .unwrap();
        let shape = Shape::MultiLineString(
            MultiLineString::try_new(CoordinateAxes::XY, vec![
                MultiMember::Empty,
                MultiMember::Value(repeated.clone()),
                MultiMember::Value(repeated),
                MultiMember::Empty,
                MultiMember::Value(final_line),
            ])
            .unwrap(),
        );
        let prepared = crate::geometry::PreparedPlan::new(shape);
        assert_eq!(
            prepared
                .line_locate_point(Point::new_raw_xy(12.0, 1.0), false)
                .unwrap()
                .to_bits(),
            6.0_f64.to_bits(),
        );
        assert_eq!(
            prepared.distance_parts().unwrap().linework.segment_count(),
            3
        );
    }

    #[test]
    fn prepared_planar_locate_matches_streaming_across_seeded_ties_zero_edges_and_empty_parts() {
        use crate::geometry::{CoordinateAxes, MultiLineString, MultiMember};

        let line = |points: &[Point]| {
            LineSeq::try_new(CoordSeq::try_from_points(points).expect("valid test coordinates"))
                .expect("valid test line")
        };
        let tie_above = line(&[Point::new_raw_xy(-1.0, 1.0), Point::new_raw_xy(1.0, 1.0)]);
        let zero_at_probe = line(&[
            Point::new_raw_xy(100.0, 100.0),
            Point::new_raw_xy(100.0, 100.0),
        ]);
        let tie_below = line(&[Point::new_raw_xy(-1.0, -1.0), Point::new_raw_xy(1.0, -1.0)]);
        let near_zero_probe = line(&[
            Point::new_raw_xy(99.0, 99.0),
            Point::new_raw_xy(101.0, 99.0),
        ]);

        let mut state = 0x317_u64;
        let seeded_points: Vec<_> = (0_usize..=128)
            .map(|index| {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                let y = 1_000.0 + ((state >> 32) % 101) as f64 / 1_000.0;
                Point::new_raw_xy(1_000.0 + index as f64 * 4.0, y)
            })
            .collect();
        let repeated_vertex = *seeded_points
            .get(64)
            .expect("seeded line contains the repeated-vertex site");
        let mut seeded_points = seeded_points;
        seeded_points.insert(64, repeated_vertex);
        let seeded_coordinates =
            CoordSeq::try_from_points(&seeded_points).expect("valid seeded coordinates");
        let seeded_line = LineSeq::try_new(seeded_coordinates.clone()).expect("valid seeded line");
        let shape = Shape::MultiLineString(
            MultiLineString::try_new(CoordinateAxes::XY, vec![
                MultiMember::Value(tie_above),
                MultiMember::Empty,
                MultiMember::Value(zero_at_probe),
                MultiMember::Value(tie_below),
                MultiMember::Value(near_zero_probe),
                MultiMember::Empty,
                MultiMember::Value(line(&[
                    Point::new_raw_xy(2.0_f64.powi(-27), -9.0),
                    Point::new_raw_xy(1.0, -9.0),
                ])),
                MultiMember::Value(line(&[
                    Point::new_raw_xy(0.0, -9.0),
                    Point::new_raw_xy(-1.0, -9.0),
                ])),
                MultiMember::Value(line(&[
                    Point::new_raw_xy(0.0, -19.0),
                    Point::new_raw_xy(1.0, -18.0),
                ])),
                MultiMember::Value(line(&[
                    Point::new_raw_xy(-1.0, -19.0),
                    Point::new_raw_xy(1.0, -19.0),
                ])),
                MultiMember::Value(seeded_line.clone()),
            ])
            .expect("valid multipart line"),
        );
        let prepared = crate::geometry::PreparedPlan::new(shape.clone());
        assert!(prepared.distance_parts().unwrap().bvh().is_some());
        let mut probes = vec![
            Point::new_raw_xy(0.0, 0.0),
            Point::new_raw_xy(100.0, 100.0),
            Point::new_raw_xy(0.0, -10.0),
            Point::new_raw_xy(0.0, -20.0),
        ];
        for ordinal in [0, 7, 31, 63, 64, 127, 128] {
            let start = *seeded_points
                .get(ordinal)
                .expect("seeded segment starts in the line");
            let end = *seeded_points
                .get(ordinal.wrapping_add(1))
                .expect("seeded segment has a following vertex");
            probes.push(Point::new_raw_xy(
                start.x.midpoint(end.x),
                start.y.midpoint(end.y) + 0.25,
            ));
        }

        for probe in probes {
            for normalized in [false, true] {
                let expected =
                    locate_planar_route(shape.linework().unwrap().into_iter(), probe, normalized)
                        .expect("one-shot streaming reference");
                assert_eq!(
                    prepared
                        .line_locate_point(probe, normalized)
                        .expect("prepared nearest search")
                        .to_bits(),
                    expected.to_bits(),
                    "prepared locate differs from streaming at {probe:?}, normalized={normalized}"
                );
            }
        }
        assert_eq!(
            prepared
                .line_locate_point(Point::new_raw_xy(0.0, 0.0), false)
                .expect("exact nearest tie")
                .to_bits(),
            1.0_f64.to_bits(),
            "the first source segment wins an exact distance tie across multipart EMPTY members"
        );
        assert_eq!(
            prepared
                .line_locate_point(Point::new_raw_xy(100.0, 100.0), false)
                .expect("zero-length segment is not a locate candidate")
                .to_bits(),
            5.0_f64.to_bits(),
            "locate skips the coincident zero-length member and stationing crosses its prefix"
        );
        let seeded_shape = Shape::LineString(seeded_line);
        let seeded_prepared = crate::geometry::PreparedPlan::new(seeded_shape);
        for ordinal in [0, 7, 31, 63, 64, 127, 128] {
            let start = *seeded_points
                .get(ordinal)
                .expect("seeded segment starts in the line");
            let end = *seeded_points
                .get(ordinal.wrapping_add(1))
                .expect("seeded segment has a following vertex");
            let probe = Point::new_raw_xy(start.x.midpoint(end.x), start.y.midpoint(end.y) + 0.25);
            let expected = crate::line_locate_coordseq(
                &crate::crs::MetricModel::COORDINATE,
                &seeded_coordinates,
                probe,
                false,
            )
            .expect("packed one-shot reference");
            assert_eq!(
                seeded_prepared
                    .line_locate_point(probe, false)
                    .expect("prepared nearest search")
                    .to_bits(),
                expected.to_bits(),
                "prepared locate differs from packed streaming at {probe:?}"
            );
        }
    }

    #[test]
    fn chart_locate_ordinals_preserve_zero_edges_and_shared_multipart_occurrences() {
        use geographiclib_rs::Geodesic;

        use crate::crs::EllipsoidMetric;
        use crate::geographic::GeographicFrame;
        use crate::geometry::{MultiLineString, MultiMember};

        let chain = LineSeq::try_new(
            CoordSeq::try_from_points(&[
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(0.0, 0.0),
                Point::new_raw_xy(1.0, 0.0),
                Point::new_raw_xy(2.0, 0.0),
            ])
            .unwrap(),
        )
        .unwrap();
        let shape = Shape::MultiLineString(
            MultiLineString::try_new(chain.axes(), vec![
                MultiMember::Value(chain.clone()),
                MultiMember::Value(chain),
            ])
            .unwrap(),
        );
        let geodesic = Geodesic::wgs84();
        let metric = EllipsoidMetric::for_geodesic_at_frame(&geodesic, 0, GeographicFrame::crs84());
        let index = ChartLineIndex::build_chart(&shape, &metric).unwrap();
        assert_eq!(index.parts().edges.len(), 6);
        assert_eq!(index.prefix.len(), 7);
        assert_eq!(index.prefix[0].to_bits(), index.prefix[1].to_bits());
        assert_eq!(index.prefix[3].to_bits(), index.prefix[4].to_bits());
        let probe = Point::new_raw_xy(1.5, 0.0);
        let (ordinal, parameter) = point_chart_locate(probe, index.parts(), &metric, |part| {
            !part.edge.is_degenerate()
        })
        .unwrap()
        .unwrap();
        assert_eq!(ordinal, 2);
        assert!((parameter - 0.5).abs() < 1e-14);
        let station = index.locate_chart_point(probe, false, &metric).unwrap();
        assert!((station - 166_979.236_189_910_36).abs() < 4e-9);
        assert!((index.locate_chart_point(probe, true, &metric).unwrap() - 0.375).abs() < 2e-15);
    }

    #[test]
    fn full_turn_parallel_lrs_round_trips_across_the_circle() {
        use geographiclib_rs::Geodesic;

        use crate::crs::EllipsoidMetric;
        use crate::geographic::GeographicFrame;

        let shape =
            crate::io::parse_wkt("LINESTRING (10 60, 370 60)").expect("one explicit full turn");
        let geodesic = Geodesic::wgs84();
        let metric = EllipsoidMetric::for_geodesic_at_frame(&geodesic, 0, GeographicFrame::crs84());
        let index = ChartLineIndex::build_chart(&shape, &metric).expect("chart-edge line index");
        let cases = [28.0, 100.0, -170.0, -80.0, -8.0];
        let start_ulp = 10.0_f64.next_up() - 10.0;
        let latitude_ulp = 60.0_f64.next_up() - 60.0;
        let mut first_station = None;
        let mut previous_station = f64::NEG_INFINITY;

        for longitude in cases {
            let query = Point::new_raw_xy(longitude, 60.0);
            let station = index
                .locate_chart_point(query, true, &metric)
                .expect("full-turn chart locate");
            assert!(
                station > previous_station,
                "full-turn stations must increase: {previous_station} then {station}"
            );
            if first_station.is_none() {
                first_station = Some(station);
            }
            let interpolated = index
                .interpolate_route_position(station, true, &metric)
                .expect("full-turn chart interpolate");
            let longitude_ulp = longitude.abs().next_up() - longitude.abs();
            let longitude_budget = 4.0 * start_ulp.max(longitude_ulp) + 360.0 * 4.0 * f64::EPSILON;
            let latitude_budget = 4.0 * latitude_ulp;
            assert!(
                (interpolated.x - longitude).abs() <= longitude_budget,
                "round-trip longitude {} differs from {longitude} by more than {longitude_budget}",
                interpolated.x
            );
            assert!(
                (interpolated.y - 60.0).abs() <= latitude_budget,
                "round-trip latitude {} differs from 60 by more than {latitude_budget}",
                interpolated.y
            );
            previous_station = station;
        }
        assert!(first_station.is_some_and(|station| station < 0.1));
        assert!(previous_station > 0.9);
    }

    #[test]
    fn chart_bvh_locate_keeps_earliest_ordinal_when_a_later_leaf_is_visited_first() {
        use geographiclib_rs::Geodesic;

        use crate::crs::EllipsoidMetric;
        use crate::geographic::GeographicFrame;
        let shape = crate::io::parse_wkt(
            "MULTILINESTRING ((0 1, 2 1), (10 20, 11 20), (20 20, 21 20), \
             (30 20, 31 20), (40 20, 41 20), (-40 20, -39 20), \
             (-30 20, -29 20), (-20 20, -19 20), (-10 1, 0 1))",
        )
        .unwrap();
        let geodesic = Geodesic::wgs84();
        let metric = EllipsoidMetric::for_geodesic_at_frame(&geodesic, 0, GeographicFrame::crs84());
        let index = ChartLineIndex::build_chart(&shape, &metric).unwrap();
        let probe = Point::new_raw_xy(0.0, 0.0);
        let mut best = f64::INFINITY;
        let mut visited = Vec::new();
        index
            .parts()
            .bvh()
            .unwrap()
            .visit_point_candidates(
                &index.parts().edges,
                probe,
                &metric,
                &mut best,
                None,
                &mut Vec::new(),
                |ordinal, _, _| {
                    visited.push(ordinal);
                    Ok::<_, crate::error::Error>(None::<()>)
                },
            )
            .unwrap();
        assert!(
            visited.iter().position(|&ordinal| ordinal == 8).unwrap()
                < visited.iter().position(|&ordinal| ordinal == 0).unwrap()
        );
        let (ordinal, _) = point_chart_locate(probe, index.parts(), &metric, |_| true)
            .unwrap()
            .unwrap();
        assert_eq!(ordinal, 0);
        assert_eq!(
            index
                .locate_chart_point(probe, false, &metric)
                .unwrap()
                .to_bits(),
            0.0_f64.to_bits()
        );
    }

    #[test]
    fn cached_chart_locate_preserves_source_then_query_domain_error_precedence() {
        let model = crate::crs::metric_model(Some("OGC:CRS84")).unwrap();
        let invalid = crate::geometry::PreparedPlan::new(
            crate::io::parse_wkt("LINESTRING (0 100, 1 100)").unwrap(),
        );
        let invalid_probe = Point::new_raw_xy(2.0, 101.0);
        let cache = crate::geometry::FrameDependentCaches::default();
        let source_error = crate::crs::ensure_geographic_domain_in_frame(
            crate::geographic::GeographicFrame::crs84(),
            invalid.shape(),
        )
        .unwrap_err()
        .to_string();
        assert_eq!(
            crate::line_locate_shape(&model, &invalid, &cache, invalid_probe, false)
                .unwrap_err()
                .to_string(),
            source_error,
        );
        let valid = crate::geometry::PreparedPlan::new(
            crate::io::parse_wkt("LINESTRING (0 10, 1 10)").unwrap(),
        );
        let query_error = crate::crs::ensure_geographic_lonlat_in_frame(
            crate::geographic::GeographicFrame::crs84(),
            invalid_probe.x,
            invalid_probe.y,
        )
        .unwrap_err()
        .to_string();
        for _ in 0..2 {
            assert_eq!(
                crate::line_locate_shape(&model, &valid, &cache, invalid_probe, false)
                    .unwrap_err()
                    .to_string(),
                query_error,
            );
        }
    }
}
